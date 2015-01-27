package org.apache.cassandra.db.index;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.index.search.OnDiskSA;
import org.apache.cassandra.db.index.search.OnDiskSABuilder;
import org.apache.cassandra.db.index.search.container.TokenTreeBuilder;
import org.apache.cassandra.db.index.search.tokenization.AbstractTokenizer;
import org.apache.cassandra.db.index.search.tokenization.NoOpTokenizer;
import org.apache.cassandra.db.index.search.tokenization.StandardTokenizer;
import org.apache.cassandra.db.index.utils.LazyMergeSortIterator;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.notifications.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.*;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.db.index.utils.LazyMergeSortIterator.OperationType;
import static org.apache.cassandra.db.index.search.container.TokenTree.Token;
import static org.apache.cassandra.db.index.search.OnDiskSABuilder.Mode;

/**
 * Note: currently does not work with cql3 tables (unless 'WITH COMPACT STORAGE' is declared when creating the table).
 *
 * ALos, makes the assumption this will be the only index running on the table as part of the query.
 * SIM tends to shoves all indexed columns into one PerRowSecondaryIndex
 */
public class SuffixArraySecondaryIndex extends PerRowSecondaryIndex implements SSTableWriterListenable
{
    private static final Logger logger = LoggerFactory.getLogger(SuffixArraySecondaryIndex.class);

    private static final Set<AbstractType<?>> TOKENIZABLE_TYPES = new HashSet<AbstractType<?>>()
    {{
        add(UTF8Type.instance);
        add(AsciiType.instance);
    }};

    /**
     * A sanity ceiling on the number of max rows we'll ever return for a query.
     */
    private static final int MAX_ROWS = 10000;

    private static final String FILE_NAME_FORMAT = "SI_%s.db";
    private static final ThreadPoolExecutor INDEX_FLUSHER_MEMTABLE;
    private static final ThreadPoolExecutor INDEX_FLUSHER_GENERAL;

    static
    {
        INDEX_FLUSHER_GENERAL = new JMXEnabledThreadPoolExecutor(1, 8, 60, TimeUnit.SECONDS,
                                                                 new LinkedBlockingQueue<Runnable>(),
                                                                 new NamedThreadFactory("SuffixArrayBuilder-General"),
                                                                 "internal");
        INDEX_FLUSHER_GENERAL.allowCoreThreadTimeOut(true);

        INDEX_FLUSHER_MEMTABLE = new JMXEnabledThreadPoolExecutor(1, 8, 60, TimeUnit.SECONDS,
                                                                 new LinkedBlockingQueue<Runnable>(),
                                                                 new NamedThreadFactory("SuffixArrayBuilder-Memtable"),
                                                                 "internal");
        INDEX_FLUSHER_MEMTABLE.allowCoreThreadTimeOut(true);
    }

    private final BiMap<ByteBuffer, Component> columnDefComponents;

    private final ConcurrentMap<ByteBuffer, SADataTracker> intervalTrees;

    private final Map<ByteBuffer, Mode> indexingModes = new HashMap<>();
    private final ConcurrentMap<Descriptor, CopyOnWriteArrayList<SSTableIndex>> currentIndexes;

    private AbstractType<?> keyComparator;
    private boolean isInitialized;

    public SuffixArraySecondaryIndex()
    {
        columnDefComponents = HashBiMap.create();
        intervalTrees = new ConcurrentHashMap<>();
        currentIndexes = new ConcurrentHashMap<>();
    }

    public void init()
    {
        if (!(StorageService.getPartitioner() instanceof Murmur3Partitioner))
            throw new UnsupportedOperationException("SASI supported only with Murmur3Partitioner.");

        isInitialized = true;

        // init() is called by SIM only on the instance that it will keep around, but will call addColumnDef on any instance
        // that it happens to create (and subsequently/immediately throw away)
        addComponent(columnDefs);

        baseCfs.getDataTracker().subscribe(new DataTrackerConsumer());
    }

    void addColumnDef(ColumnDefinition columnDef)
    {
        super.addColumnDef(columnDef);

        String mode = columnDef.getIndexOptions().get("mode");
        indexingModes.put(columnDef.name, mode == null ? Mode.ORIGINAL : Mode.mode(mode));

        addComponent(Collections.singleton(columnDef));
    }

    private void addComponent(Set<ColumnDefinition> defs)
    {
        // if SI hasn't been initialized that means that this instance
        // was created for validation purposes only, so we don't have do anything here
        if (!isInitialized)
            return;

        // only reason baseCfs would be null is if coming through the CFMetaData.validate() path, which only
        // checks that the SI 'looks' legit, but then throws away any created instances - fml, this 2I api sux
        if (baseCfs == null)
            return;
        else if (keyComparator == null)
            keyComparator = this.baseCfs.metadata.getKeyValidator();

        INDEX_FLUSHER_GENERAL.setCorePoolSize(columnDefs.size());
        INDEX_FLUSHER_GENERAL.setMaximumPoolSize(columnDefs.size() * 2);
        INDEX_FLUSHER_MEMTABLE.setMaximumPoolSize(columnDefs.size());

        AbstractType<?> type = baseCfs.getComparator();
        for (ColumnDefinition def : defs)
        {
            String indexName = String.format(FILE_NAME_FORMAT, type.getString(def.name));
            columnDefComponents.put(def.name, new Component(Component.Type.SECONDARY_INDEX, indexName));

            // on restart, sstables are loaded into DataTracker before 2I are hooked up (and init() invoked),
            // so we need to grab the sstables here
            if (!intervalTrees.containsKey(def.name))
                intervalTrees.put(def.name, new SADataTracker(def.name, baseCfs.getDataTracker().getSSTables()));
        }
    }

    private void addToIntervalTree(Collection<SSTableReader> readers, Collection<ColumnDefinition> defs)
    {
        for (ColumnDefinition columnDefinition : defs)
        {
            SADataTracker dataTracker = intervalTrees.get(columnDefinition.name);
            dataTracker.update(Collections.<SSTableReader>emptyList(), readers);
        }
    }

    private void removeFromIntervalTree(Collection<SSTableReader> readers)
    {
        for (Map.Entry<ByteBuffer, SADataTracker> entry : intervalTrees.entrySet())
        {
            entry.getValue().update(readers, Collections.<SSTableReader>emptyList());
        }
    }

    private ColumnDefinition getColumnDef(Component component)
    {
        //ugly hack to get validator via the columnDefs
        ByteBuffer colName = columnDefComponents.inverse().get(component);
        if (colName != null)
            return getColumnDefinition(colName);
        return null;
    }

    public boolean isIndexBuilt(ByteBuffer columnName)
    {
        return true;
    }

    public void validateOptions() throws ConfigurationException
    {
        // todo: sanity check on valid tokenization options
    }

    public String getIndexName()
    {
        return "RowLevel_SuffixArrayIndex_" + baseCfs.getColumnFamilyName();
    }

    public ColumnFamilyStore getIndexCfs()
    {
        return null;
    }

    public long getLiveSize()
    {
        // Live size means how many bytes from the memtable (or outside Memtable but not yet flushed) is used by index,
        // as we write indexes at the point when Memtable has already been written to disk, we should return 0 from this
        // method to avoid miscalculation of Memtable live size and spurious flushes.
        return 0;
    }

    public void reload()
    {
        // nop, i think
    }

    public void index(ByteBuffer rowKey, ColumnFamily cf)
    {
        // this will index a whole row, or at least, what is passed in
        // called from memtable path, as well as in index rebuild path
        // need to be able to distinguish between the two
        // should be reasonably easy to distinguish is the write is coming form memtable path

        // better yet, the index rebuild path can be bypassed by overriding buildIndexAsync(), and just return
        // some empty Future - all current callers of buildIndexAsync() ignore the returned Future.
        // seems a little dangerous (not future proof) but good enough for a v1
        if (logger.isTraceEnabled())
            logger.trace("received an index() call");
    }

    /**
     * parent class to eliminate the index rebuild
     *
     * @return a future that does and blocks on nothing
     */
    public Future<?> buildIndexAsync()
    {
        return Futures.immediateCheckedFuture(null);
    }

    public void delete(DecoratedKey key)
    {
        // called during 'nodetool cleanup' - can punt on impl'ing this
    }

    public void removeIndex(ByteBuffer columnName)
    {
        // TODO: figure it out
    }

    public void invalidate()
    {
        // TODO: same as above
    }

    public void truncateBlocking(long truncatedAt)
    {
        // nop?? - at least punting for now
    }

    public void forceBlockingFlush()
    {
        //nop, I think, as this 2I will flush with the owning CF's sstable, so we don't need this extra work
    }

    public Collection<Component> getIndexComponents()
    {
        return ImmutableList.<Component>builder().addAll(columnDefComponents.values()).build();
    }

    public SSTableWriterListener getListener(Descriptor descriptor, Source source)
    {
        return new PerSSTableIndexWriter(descriptor, source);
    }

    private AbstractType<?> getValidator(ByteBuffer columnName)
    {
        ColumnDefinition columnDef = getColumnDefinition(columnName);
        return columnDef == null ? null : columnDef.getValidator();
    }

    protected class PerSSTableIndexWriter implements SSTableWriterListener
    {
        private final Descriptor descriptor;
        private final Source source;

        // need one entry for each term we index
        private final Map<ByteBuffer, ColumnIndex> indexPerColumn;

        private DecoratedKey currentKey;
        private long currentKeyPosition;

        public PerSSTableIndexWriter(Descriptor descriptor, Source source)
        {
            this.descriptor = descriptor;
            this.source = source;
            this.indexPerColumn = new HashMap<>();
        }

        public void begin()
        {
            // nop
        }

        public void startRow(DecoratedKey key, long curPosition)
        {
            currentKey = key;
            currentKeyPosition = curPosition;
        }

        public void nextColumn(Column column)
        {
            Component component = columnDefComponents.get(column.name());
            if (component == null)
                return;

            ColumnIndex index = indexPerColumn.get(column.name());
            if (index == null)
            {
                String outputFile = descriptor.filenameFor(component);
                indexPerColumn.put(column.name(), (index = new ColumnIndex(getColumnDef(component), outputFile)));
            }

            index.add(column.value().duplicate(), currentKey, (int) currentKeyPosition);
        }

        public void complete()
        {
            currentKey = null;

            try
            {
                logger.info("about to submit for concurrent SA'ing");
                final CountDownLatch latch = new CountDownLatch(indexPerColumn.size());

                // first, build up a listing per-component (per-index)
                for (final ColumnIndex index : indexPerColumn.values())
                {
                    logger.info("Submitting {} for concurrent SA'ing", index.outputFile);
                    ThreadPoolExecutor executor = source == Source.MEMTABLE ? INDEX_FLUSHER_MEMTABLE : INDEX_FLUSHER_GENERAL;
                    executor.submit(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            try
                            {
                                long flushStart = System.nanoTime();
                                index.blockingFlush();
                                logger.info("Flushing SA index to {} took {} ms.",
                                        index.outputFile,
                                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - flushStart));
                            }
                            finally
                            {
                                latch.countDown();
                            }
                        }
                    });
                }

                Uninterruptibles.awaitUninterruptibly(latch, 10, TimeUnit.MINUTES);
            }
            finally
            {
                // drop this data ASAP
                indexPerColumn.clear();
            }
        }

        public int compareTo(String o)
        {
            return descriptor.generation;
        }

        private class ColumnIndex
        {
            private final ColumnDefinition column;
            private final String outputFile;
            private final AbstractTokenizer tokenizer;
            private final Map<ByteBuffer, TokenTreeBuilder> keysPerTerm;

            // key range of the per-column index
            private DecoratedKey min, max;

            public ColumnIndex(ColumnDefinition column, String outputFile)
            {
                this.column = column;
                this.outputFile = outputFile;
                this.tokenizer = TOKENIZABLE_TYPES.contains(column.getValidator())
                                        ? new StandardTokenizer()
                                        : new NoOpTokenizer();

                this.tokenizer.init(column.getIndexOptions());
                this.keysPerTerm = new HashMap<>();
            }

            private void add(ByteBuffer term, DecoratedKey key, long keyPosition)
            {
                final Long keyToken = ((LongToken) key.getToken()).token;

                tokenizer.reset(term);
                while (tokenizer.hasNext())
                {
                    ByteBuffer token = tokenizer.next();
                    TokenTreeBuilder keys = keysPerTerm.get(token);
                    if (keys == null)
                        keysPerTerm.put(token, (keys = new TokenTreeBuilder()));

                    keys.add(Pair.create(keyToken, keyPosition));
                }

                /* calculate key range (based on actual key values) for current index */

                min = (min == null || keyComparator.compare(min.key, currentKey.key) > 0) ? currentKey : min;
                max = (max == null || keyComparator.compare(max.key, currentKey.key) < 0) ? currentKey : max;
            }

            public void blockingFlush()
            {
                OnDiskSABuilder builder = new OnDiskSABuilder(column.getValidator(), indexingModes.get(column.name));

                for (Map.Entry<ByteBuffer, TokenTreeBuilder> e : keysPerTerm.entrySet())
                    builder.add(e.getKey(), e.getValue());

                // since everything added to the builder, it's time to drop references to the data
                keysPerTerm.clear();

                builder.finish(Pair.create(min, max), new File(outputFile));
            }
        }
    }

    protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns)
    {
        return new LocalSecondaryIndexSearcher(baseCfs.indexManager, columns);
    }

    protected class LocalSecondaryIndexSearcher extends SecondaryIndexSearcher
    {
        private final Phaser phaser;

        protected LocalSecondaryIndexSearcher(SecondaryIndexManager indexManager, Set<ByteBuffer> columns)
        {
            super(indexManager, columns);
            phaser = new Phaser();
        }

        public List<Row> search(ExtendedFilter filter)
        {
            try
            {
                return performSearch(filter);
            }
            catch(Exception e)
            {
                logger.info("error occurred while searching suffix array indexes; ignoring", e);
                return Collections.emptyList();
            }
            finally
            {
                phaser.forceTermination();
            }
        }

        protected List<Row> performSearch(ExtendedFilter filter) throws IOException
        {
            if (filter.getClause().isEmpty()) // not sure how this could happen in the real world, but ...
                return Collections.emptyList();

            AbstractBounds<RowPosition> requestedRange = filter.dataRange.keyRange();

            final int maxRows = Math.min(MAX_ROWS, filter.maxRows());
            List<Row> rows = new ArrayList<>(maxRows);
            List<Pair<ByteBuffer, Expression>> expressions = analyzeQuery(filter.getClause());
            List<Pair<ByteBuffer, Expression>> expressionsImmutable = ImmutableList.copyOf(expressions);

            List<SSTableIndex> primaryIndexes = null;
            Pair<ByteBuffer, Expression> primaryExpression = null;

            for (Pair<ByteBuffer, Expression> e : expressions)
            {
                List<SSTableIndex> indexes = intervalTrees.get(e.left).view.get().match(e.right);
                if (primaryIndexes == null || primaryIndexes.size() > indexes.size())
                {
                    primaryIndexes = indexes;
                    primaryExpression = e;
                }
            }

            if (primaryIndexes == null || primaryIndexes.size() == 0)
                return Collections.emptyList();

            /*
               Once we've figured out what primary index SSTables are,
               next thing we need to do is to bucket them based on overlapping key ranges,
               so instead of searching whole set of indexes we can do it per bucket of
               overlapping primary files which saves a lot of skipTo moves when multiple
               indexes are merged together.

               Also note that the final step in this process is sorting buckets
               based on the "first key" of the backing SSTable which effectively puts them
               into token sorted order because results have to be returned in that order.
             */

            // sort SSTables based on actual key order
            Collections.sort(primaryIndexes, new Comparator<SSTableIndex>()
            {
                @Override
                public int compare(SSTableIndex a, SSTableIndex b)
                {
                    return keyComparator.compare(a.sstable.first.key, b.sstable.first.key);
                }
            });

            List<IndexBucket> primaryBuckets = new ArrayList<>();

            SSTableIndex lastIndex = null;
            IndexBucket currentBucket = new IndexBucket();

            for (final SSTableIndex index : primaryIndexes)
            {
                DecoratedKey min = index.sstable.first;
                DecoratedKey max = index.sstable.last;

                if (!requestedRange.left.isMinimum() && !requestedRange.right.isMinimum())
                {
                    Range<RowPosition> indexRange = new Range<RowPosition>(min, max);

                    // no intersection with primary range, avoid even considering current index
                    if (!requestedRange.intersects(Collections.singleton(indexRange)))
                        continue;
                }

                if (lastIndex != null && currentBucket.compareTo(min) < 0)
                {
                    primaryBuckets.add(currentBucket);
                    currentBucket = new IndexBucket() {{ add(index); }};
                }
                else
                {
                    currentBucket.add(index);
                }

                lastIndex = index;
            }

            primaryBuckets.add(currentBucket);

            // sort buckets based on the token because
            // we need to return data in the token sorted order
            Collections.sort(primaryBuckets);

            expressions.remove(primaryExpression);

            intersection:
            for (IndexBucket bucket : primaryBuckets)
            {
                List<SkippableIterator<Long, Token>> unions = new ArrayList<>();

                unions.add(new SuffixIterator(primaryExpression.right, bucket.bucket));

                for (SSTableIndex primaryIndex : bucket)
                {
                    for (Pair<ByteBuffer, Expression> expression : expressions)
                    {
                        SAView view = intervalTrees.get(expression.left).view.get();
                        unions.add(new SuffixIterator(expression.right, view.match(primaryIndex.minKey(), primaryIndex.maxKey())));
                    }
                }

                SkippableIterator<Long, Token> joiner = null;
                try
                {
                    joiner = new LazyMergeSortIterator<>(OperationType.AND, unions);

                    joiner.skipTo(((LongToken) requestedRange.left.getToken()).token);

                    while (joiner.hasNext())
                    {
                        for (DecoratedKey key : joiner.next())
                        {
                            if (!requestedRange.contains(key) || rows.size() >= maxRows)
                                break intersection;

                            Row row = getRow(key, filter, expressionsImmutable);
                            if (row != null)
                                rows.add(row);
                        }
                    }
                }
                finally
                {
                    FileUtils.closeQuietly(joiner);
                }
            }

            return rows;
        }

        private Row getRow(DecoratedKey key, ExtendedFilter filter, List<Pair<ByteBuffer, Expression>> expressions)
        {
            ReadCommand cmd = ReadCommand.create(baseCfs.keyspace.getName(),
                                                 key.key,
                                                 baseCfs.getColumnFamilyName(),
                                                 System.currentTimeMillis(),
                                                 filter.columnFilter(key.key));

            return new RowReader(cmd, expressions).call();
        }

        private class RowReader implements Callable<Row>
        {
            private final ReadCommand command;
            private final List<Pair<ByteBuffer, Expression>> expressions;

            public RowReader(ReadCommand command, List<Pair<ByteBuffer, Expression>> expressions)
            {
                this.command = command;
                this.expressions = expressions;
                phaser.register();
            }

            public Row call()
            {
                try
                {
                    Row row = command.getRow(Keyspace.open(command.ksName));
                    return satisfiesPredicates(row) ? row : null;
                }
                finally
                {
                    phaser.arriveAndDeregister();
                }
            }

            /**
             * reapply the predicates to see if the row still satisfies the search predicates
             * now that it's been loaded and merged from the LSM storage engine.
             */
            private boolean satisfiesPredicates(Row row)
            {
                if (row.cf == null)
                    return false;
                long now = System.currentTimeMillis();
                for (Pair<ByteBuffer, Expression> entry : expressions)
                {
                    Column col = row.cf.getColumn(entry.left);
                    if (col == null || !col.isLive(now) || !entry.right.contains(col.value()))
                        return false;
                }
                return true;
            }
        }

        public boolean isIndexing(List<IndexExpression> clause)
        {
            // this is a bit weak, currently just checks for the success of one column, not all
            // however, parent SIS.isIndexing only cares if one predicate is covered ... grrrr!!
            for (IndexExpression expression : clause)
            {
                if (columnDefComponents.keySet().contains(expression.column_name))
                    return true;
            }
            return false;
        }

        private List<Pair<ByteBuffer, Expression>> analyzeQuery(List<IndexExpression> expressions)
        {
            // differentiate between equality and inequality expressions while analyzing so we can later put the equality
            // statements at the front of the list of expressions.
            List<Pair<ByteBuffer, Expression>> equalityExpressions = new ArrayList<>();
            List<Pair<ByteBuffer, Expression>> inequalityExpressions = new ArrayList<>();
            for (IndexExpression e : expressions)
            {
                ByteBuffer name = e.bufferForColumn_name();
                List<Pair<ByteBuffer, Expression>> expList = e.op.equals(IndexOperator.EQ) ? equalityExpressions : inequalityExpressions;
                Expression exp = null;
                for (Pair<ByteBuffer, Expression> pair : expList)
                {
                    if (pair.left.equals(name))
                        exp = pair.right;
                }

                if (exp == null)
                {
                    exp = new Expression(getColumnDefinition(name).getValidator());
                    expList.add(Pair.create(name, exp));
                }
                exp.add(e.op, e.bufferForValue());
            }

            equalityExpressions.addAll(inequalityExpressions);
            return equalityExpressions;
        }
    }

    private static class Expression
    {
        private final AbstractType<?> validator;
        private final boolean isSuffix;
        private Bound lower, upper;

        private Expression(AbstractType<?> validator)
        {
            this.validator = validator;
            isSuffix = validator instanceof AsciiType || validator instanceof UTF8Type;
        }

        public void add(IndexOperator op, ByteBuffer value)
        {
            switch (op)
            {
                case EQ:
                    lower = new Bound(value, true);
                    upper = lower;
                    break;

                case LT:
                    upper = new Bound(value, false);
                    break;

                case LTE:
                    upper = new Bound(value, true);
                    break;

                case GT:
                    lower = new Bound(value, false);
                    break;

                case GTE:
                    lower = new Bound(value, true);
                    break;

            }
        }

        public boolean contains(ByteBuffer value)
        {
            if (lower != null)
            {
                // suffix check
                if (isSuffix)
                {
                    if (!ByteBufferUtil.contains(value, lower.value))
                        return false;
                }
                else
                {
                    // range - (mainly) for numeric values
                    int cmp = validator.compare(lower.value, value);
                    if (cmp > 0 || (cmp == 0 && !lower.inclusive))
                        return false;
                }
            }

            if (upper != null && lower != upper)
            {
                // suffix check
                if (isSuffix)
                {
                    if (!ByteBufferUtil.contains(value, upper.value))
                        return false;
                }
                else
                {
                    // range - mainly for numeric values
                    int cmp = validator.compare(upper.value, value);
                    if (cmp < 0 || (cmp == 0 && !upper.inclusive))
                        return false;
                }
            }
            return true;
        }
    }

    private static class Bound
    {
        private final ByteBuffer value;
        private final boolean inclusive;

        public Bound(ByteBuffer value, boolean inclusive)
        {
            this.value = value;
            this.inclusive = inclusive;
        }
    }

    private class SuffixIterator extends AbstractIterator<Token> implements SkippableIterator<Long, Token>
    {
        private final SkippableIterator<Long, Token> union;
        private final List<SSTableIndex> referencedIndexes = new ArrayList<>();

        public SuffixIterator(Expression expression, final List<SSTableIndex> perSSTableIndexes)
        {
            List<SkippableIterator<Long, Token>> keys = new ArrayList<>(perSSTableIndexes.size());

            for (final SSTableIndex index : perSSTableIndexes)
            {
                if (!index.reference())
                    continue;

                ByteBuffer lower = (expression.lower == null) ? null : expression.lower.value;
                ByteBuffer upper = (expression.upper == null) ? null : expression.upper.value;

                keys.add(index.search(lower, lower == null || expression.lower.inclusive,
                                      upper, upper == null || expression.upper.inclusive));

                referencedIndexes.add(index);
            }

            union = new LazyMergeSortIterator<>(OperationType.OR, keys);
        }

        @Override
        protected Token computeNext()
        {
            return union.hasNext() ? union.next() : endOfData();
        }

        @Override
        public void skipTo(Long next)
        {
            union.skipTo(next);
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(union);
            for (SSTableIndex index : referencedIndexes)
                index.release();
        }
    }

    /** a pared-down version of DataTracker and DT.View. need one for each index of each column family */
    private class SADataTracker
    {
        // by using using DT.View, we do get some baggage fields (memtable, compacting, and so on)
        // but always pass in empty list for those fields, we should be ok
        private final AtomicReference<SAView> view = new AtomicReference<>();

        public SADataTracker(ByteBuffer name, Set<SSTableReader> ssTables)
        {
            view.set(new SAView(name, ssTables));
        }

        public void update(Collection<SSTableReader> oldSSTables, Collection<SSTableReader> newSSTables)
        {

            SAView currentView, newView;
            do
            {
                currentView = view.get();

                //TODO: handle this slightly more elegantly, but don't bother trying to remove tables from an empty tree
                if (currentView.keyIntervalTree.intervalCount() == 0 && oldSSTables.size() > 0)
                    return;

                newView = currentView.update(oldSSTables, newSSTables);
                if (newView == null)
                    break;
            }
            while (!view.compareAndSet(currentView, newView));

            for (SSTableReader sstable : oldSSTables)
            {
                CopyOnWriteArrayList<SSTableIndex> indexes = currentIndexes.remove(sstable.descriptor);
                if (indexes == null)
                    continue;

                for (SSTableIndex index : indexes)
                    index.release();
            }
        }
    }

    private class SAView
    {
        private final ByteBuffer col;
        private final AbstractType<?> validator;
        private ByteBuffer minSuffix, maxSuffix;

        private final IntervalTree<ByteBuffer, SSTableIndex, Interval<ByteBuffer, SSTableIndex>> termIntervalTree;
        private final IntervalTree<ByteBuffer, SSTableIndex, Interval<ByteBuffer, SSTableIndex>> keyIntervalTree;

        public SAView(ByteBuffer col, Set<SSTableReader> sstables)
        {
            this(null, col, Collections.<SSTableReader>emptyList(), sstables);
        }

        private SAView(SAView previous, ByteBuffer col, final Collection<SSTableReader> toRemove, Collection<SSTableReader> toAdd)
        {
            this.col = col;
            this.validator = baseCfs.metadata.getColumnDefinitionFromColumnName(col).getValidator();

            String name = baseCfs.getComparator().getString(col);

            Predicate<Interval<ByteBuffer, SSTableIndex>> predicate = new Predicate<Interval<ByteBuffer, SSTableIndex>>()
            {
                public boolean apply(Interval<ByteBuffer, SSTableIndex> interval)
                {
                    return !toRemove.contains(interval.data.sstable);
                }
            };

            List<Interval<ByteBuffer, SSTableIndex>> termIntervals = new ArrayList<>();
            List<Interval<ByteBuffer, SSTableIndex>> keyIntervals = new ArrayList<>();

            // reuse entries from the previously constructed view to avoid reloading from disk or keep (yet another) cache of the sstreader -> intervals
            if (previous != null)
            {
                for (Interval<ByteBuffer, SSTableIndex> interval : Iterables.filter(previous.keyIntervalTree, predicate))
                    keyIntervals.add(interval);

                for (Interval<ByteBuffer, SSTableIndex> interval : Iterables.filter(previous.termIntervalTree, predicate))
                {
                    termIntervals.add(interval);
                    updateRange(interval.data);
                }
            }

            for (SSTableReader sstable : toAdd)
            {
                String columnName = baseCfs.getComparator().getString(col);
                File indexFile = new File(sstable.descriptor.filenameFor(String.format(FILE_NAME_FORMAT, columnName)));
                if (!indexFile.exists())
                    continue;

                SSTableIndex index = new SSTableIndex(col, indexFile, sstable);

                logger.info("Interval.create(field: {}, minSuffix: {}, maxSuffix: {}, minKey: {}, maxKey: {}, sstable: {})",
                            name,
                            validator.getString(index.minSuffix()),
                            validator.getString(index.maxSuffix()),
                            keyComparator.getString(index.minKey()),
                            keyComparator.getString(index.maxKey()),
                            sstable);

                CopyOnWriteArrayList<SSTableIndex> openIndexes = currentIndexes.get(sstable.descriptor);
                if (openIndexes == null)
                {
                    CopyOnWriteArrayList<SSTableIndex> newList = new CopyOnWriteArrayList<>();
                    openIndexes = currentIndexes.putIfAbsent(sstable.descriptor, newList);
                    if (openIndexes == null)
                        openIndexes = newList;
                }

                openIndexes.add(index);

                termIntervals.add(Interval.create(index.minSuffix(), index.maxSuffix(), index));
                keyIntervals.add(Interval.create(index.minKey(), index.maxKey(), index));

                updateRange(index);
            }

            termIntervalTree = IntervalTree.build(termIntervals, new Comparator<ByteBuffer>()
            {
                @Override
                public int compare(ByteBuffer a, ByteBuffer b)
                {
                    return validator.compare(a, b);
                }
            });

            keyIntervalTree = IntervalTree.build(keyIntervals, new Comparator<ByteBuffer>()
            {
                @Override
                public int compare(ByteBuffer a, ByteBuffer b)
                {
                    return keyComparator.compare(a, b);
                }
            });
        }

        public void updateRange(SSTableIndex index)
        {
            minSuffix = minSuffix == null || validator.compare(minSuffix, index.minSuffix()) > 0 ? index.minSuffix() : minSuffix;
            maxSuffix = maxSuffix == null || validator.compare(maxSuffix, index.maxSuffix()) < 0 ? index.maxSuffix() : maxSuffix;
        }

        public List<SSTableIndex> match(Expression expression)
        {
            ByteBuffer min = expression.lower == null ? minSuffix : expression.lower.value;
            ByteBuffer max = expression.upper == null ? maxSuffix : expression.upper.value;

            return termIntervalTree.search(Interval.create(min, max, (SSTableIndex) null));
        }

        public List<SSTableIndex> match(ByteBuffer minKey, ByteBuffer maxKey)
        {
            return keyIntervalTree.search(Interval.create(minKey, maxKey, (SSTableIndex) null));
        }

        public SAView update(Collection<SSTableReader> toRemove, Collection<SSTableReader> toAdd)
        {
            int newKeysSize = assertCorrectSizing(keyIntervalTree, toRemove, toAdd);
            int newTermsSize = assertCorrectSizing(termIntervalTree, toRemove, toAdd);
            assert newKeysSize == newTermsSize : String.format("mismatched sizes for intervals tree for keys vs terms: %d != %d", newKeysSize, newTermsSize);

            return new SAView(this, col, toRemove, toAdd);
        }

        private int assertCorrectSizing(IntervalTree<ByteBuffer, SSTableIndex, Interval<ByteBuffer, SSTableIndex>> tree,
                                        Collection<SSTableReader> oldSSTables, Collection<SSTableReader> newReaders)
        {
            int newSSTablesSize = tree.intervalCount() - oldSSTables.size() + newReaders.size();
            assert newSSTablesSize >= newReaders.size() :
                String.format("Incoherent new size %d replacing %s by %s, sstable size %d", newSSTablesSize, oldSSTables, newReaders, tree.intervalCount());
            return newSSTablesSize;
        }
    }

    private class DataTrackerConsumer implements INotificationConsumer
    {
        public void handleNotification(INotification notification, Object sender)
        {
            // unfortunately, we can only check the type of notification via instaceof :(
            if (notification instanceof SSTableAddedNotification)
            {
                SSTableAddedNotification notif = (SSTableAddedNotification) notification;
                addToIntervalTree(Collections.singletonList(notif.added), getColumnDefs());
            }
            else if (notification instanceof SSTableListChangedNotification)
            {
                SSTableListChangedNotification notif = (SSTableListChangedNotification) notification;
                for (SSTableReader reader : notif.added)
                    addToIntervalTree(Collections.singletonList(reader), getColumnDefs());
                for (SSTableReader reader : notif.removed)
                    removeFromIntervalTree(Collections.singletonList(reader));
            }
            else if (notification instanceof SSTableDeletingNotification)
            {
                SSTableDeletingNotification notif = (SSTableDeletingNotification) notification;
                removeFromIntervalTree(Collections.singletonList(notif.deleting));
            }
        }
    }

    private static class DecoratedKeyFetcher implements Function<Long, DecoratedKey>
    {
        private final SSTableReader sstable;

        DecoratedKeyFetcher(SSTableReader reader)
        {
            sstable = reader;
        }

        @Override
        public DecoratedKey apply(Long offset)
        {
            try
            {
                return sstable.keyAt(offset);
            }
            catch (IOException e)
            {
                throw new FSReadError(e, sstable.getFilename());
            }
        }

        @Override
        public int hashCode()
        {
            return sstable.descriptor.hashCode();
        }

        @Override
        public boolean equals(Object other)
        {
            return other instanceof DecoratedKeyFetcher
                    && sstable.descriptor.equals(((DecoratedKeyFetcher) other).sstable.descriptor);
        }
    }

    private class SSTableIndex
    {
        private final ByteBuffer column;
        private final SSTableReader sstable;
        private final OnDiskSA index;
        private final AtomicInteger references = new AtomicInteger(1);

        public SSTableIndex(ByteBuffer name, File indexFile, SSTableReader referent)
        {
            column = name;
            sstable = referent;

            assert sstable.acquireReference();

            AbstractType<?> validator = getValidator(column);

            assert validator != null;
            assert indexFile.exists() : String.format("SSTable %s should have index %s.",
                                                      sstable.getFilename(),
                                                      baseCfs.getComparator().getString(column));

            index = new OnDiskSA(indexFile, validator, new DecoratedKeyFetcher(sstable));
        }

        public ByteBuffer minSuffix()
        {
            return index.minSuffix();
        }

        public ByteBuffer maxSuffix()
        {
            return index.maxSuffix();
        }

        public ByteBuffer minKey()
        {
            return index.minKey();
        }

        public ByteBuffer maxKey()
        {
            return index.maxKey();
        }

        public SkippableIterator<Long, Token> search(ByteBuffer lower, boolean lowerInclusive,
                                                     ByteBuffer upper, boolean upperInclusive)
        {
            return index.search(lower, lowerInclusive, upper, upperInclusive);
        }

        public boolean reference()
        {
            while (true)
            {
                int n = references.get();
                if (n <= 0)
                    return false;
                if (references.compareAndSet(n, n + 1))
                    return true;
            }
        }

        public void release()
        {
            int n = references.decrementAndGet();
            if (n == 0)
            {
                FileUtils.closeQuietly(index);
                sstable.releaseReference();
            }
        }

        @Override
        public String toString()
        {
            return String.format("SSTableIndex(column: %s, SSTable: %s)", baseCfs.getComparator().getString(column), sstable.descriptor);
        }
    }

    private class IndexBucket implements Comparable<IndexBucket>, Iterable<SSTableIndex>
    {
        private final List<SSTableIndex> bucket;
        private DecoratedKey minKey, maxKey;

        public IndexBucket()
        {
            bucket = new ArrayList<>(1);
        }

        public void add(SSTableIndex index)
        {
            bucket.add(index);

            DecoratedKey firstKey = index.sstable.first;
            DecoratedKey lastKey  = index.sstable.last;

            minKey = minKey == null || keyComparator.compare(minKey.key, firstKey.key) > 0 ? firstKey : minKey;
            maxKey = maxKey == null || keyComparator.compare(maxKey.key, lastKey.key) < 0 ? lastKey : maxKey;
        }

        @Override
        public int compareTo(IndexBucket bucket)
        {
            return minKey.compareTo(bucket.minKey);
        }

        public int compareTo(DecoratedKey key)
        {
            return keyComparator.compare(maxKey.key, key.key);
        }

        @Override
        public Iterator<SSTableIndex> iterator()
        {
            return bucket.iterator();
        }
    }
}

