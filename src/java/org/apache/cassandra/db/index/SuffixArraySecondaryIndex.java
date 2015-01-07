package org.apache.cassandra.db.index;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.search.container.KeyContainer;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.index.search.OnDiskSA;
import org.apache.cassandra.db.index.search.OnDiskSABuilder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;

import com.google.common.base.Predicate;
import com.google.common.util.concurrent.Futures;
import org.roaringbitmap.IntIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.apache.cassandra.db.index.search.OnDiskSA.DataSuffix;
import static org.apache.cassandra.db.index.search.OnDiskSA.IteratorOrder;

/**
 * Note: currently does not work with cql3 tables (unless 'WITH COMPACT STORAGE' is declared when creating the table).
 *
 * ALos, makes the assumption this will be the only index running on the table as part of the query.
 * SIM tends to shoves all indexed columns into one PerRowSecondaryIndex
 */
public class SuffixArraySecondaryIndex extends PerRowSecondaryIndex implements SSTableWriterListenable
{
    protected static final Logger logger = LoggerFactory.getLogger(SuffixArraySecondaryIndex.class);
    public static final String FILE_NAME_FORMAT = "SI_%s.db";

    private static final ThreadPoolExecutor executor = getExecutor();

    private static ThreadPoolExecutor getExecutor()
    {
        ThreadPoolExecutor executor = new JMXEnabledThreadPoolExecutor(1, 8, 60, TimeUnit.SECONDS,
                                                                       new LinkedBlockingQueue<Runnable>(),
                                                                       new NamedThreadFactory("SuffixArrayBuilder"),
                                                                       "internal");
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    /**
     * A sanity ceiling on the number of max rows we'll ever return for a query.
     */
    private static final int MAX_ROWS = 100000;

    //not sure i really need this, tbh
    private final Map<Integer, SSTableWriterListener> openListeners;

    private final BiMap<ByteBuffer, Component> columnDefComponents;

    private AbstractType<?> keyComparator;

    public SuffixArraySecondaryIndex()
    {
        openListeners = new ConcurrentHashMap<>();
        columnDefComponents = HashBiMap.create();
    }

    public void init()
    {
        // init() is called by SIM only on the instance that it will keep around, but will call addColumnDef on any instance
        // that it happens to create (and subsequently/immediately throw away)
        addComponent(columnDefs);

        keyComparator = baseCfs.metadata.getKeyValidator();
    }

    void addColumnDef(ColumnDefinition columnDef)
    {
        super.addColumnDef(columnDef);
        addComponent(Collections.singleton(columnDef));
    }

    private void addComponent(Set<ColumnDefinition> defs)
    {
        // only reason baseCfs would be null is if coming through the CFMetaData.validate() path, which only
        // checks that the SI 'looks' legit, but then throws away any created instances - fml, this 2I api sux
        if (baseCfs == null)
            return;
        assert baseCfs != null : "should not happen - fix code paths to here!!";

        AbstractType<?> type = baseCfs.getComparator();
        for (ColumnDefinition def : defs)
        {
            String indexName = String.format(FILE_NAME_FORMAT, type.getString(def.name));
            columnDefComponents.put(def.name, new Component(Component.Type.SECONDARY_INDEX, indexName));
        }

        executor.setCorePoolSize(columnDefs.size() * 2);
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
        // nop ??
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

    public SSTableWriterListener getListener(Descriptor descriptor)
    {
        LocalSSTableWriterListener listener = new LocalSSTableWriterListener(descriptor);
        openListeners.put(descriptor.generation, listener);
        return listener;
    }

    protected class LocalSSTableWriterListener implements SSTableWriterListener
    {
        private final Descriptor descriptor;

        // need one entry for each term we index
        private final Map<ByteBuffer, IndexInfo> indexPerTerm;
        private final Map<Component, Pair<DecoratedKey, DecoratedKey>> ranges;

        private DecoratedKey curKey;
        private long curFilePosition;

        public LocalSSTableWriterListener(Descriptor descriptor)
        {
            this.descriptor = descriptor;
            this.indexPerTerm = new ConcurrentHashMap<>();
            this.ranges = new ConcurrentHashMap<>();
        }

        public void begin()
        {
            // nop
        }

        public void startRow(DecoratedKey key, long curPosition)
        {
            curKey = key;
            curFilePosition = curPosition;
        }

        public void nextColumn(Column column)
        {
            Component component = columnDefComponents.get(column.name());
            if (component == null)
                return;

            ByteBuffer term = column.value().slice();
            IndexInfo indexInfo = indexPerTerm.get(term);
            if (indexInfo == null)
                indexPerTerm.put(term, (indexInfo = new IndexInfo(component)));

            indexInfo.add(curKey, (int) curFilePosition);

            Pair<DecoratedKey, DecoratedKey> range = ranges.get(component);
            if (range == null)
            {
                ranges.put(component, Pair.create(curKey, curKey));
                return;
            }

            DecoratedKey min = range.left, max = range.right;
            min = (min == null || keyComparator.compare(min.key, curKey.key) > 0) ? curKey : min;
            max = (max == null || keyComparator.compare(max.key, curKey.key) < 0) ? curKey : max;
            ranges.put(component, Pair.create(min, max));
        }

        public void complete()
        {
            curKey = null;
            try
            {
                // first, build up a listing per-component (per-index)
                Map<Component, OnDiskSABuilder> componentBuilders = new HashMap<>(columnDefComponents.size());
                for (Map.Entry<ByteBuffer, IndexInfo> entry : indexPerTerm.entrySet())
                {
                    IndexInfo indexInfo = entry.getValue();
                    assert columnDefComponents.values().contains(indexInfo.component);

                    OnDiskSABuilder builder = componentBuilders.get(indexInfo.component);
                    if (builder == null)
                    {
                        AbstractType<?> validator = getColumnDef(indexInfo.component).getValidator();
                        OnDiskSABuilder.Mode mode = (validator instanceof AsciiType || validator instanceof UTF8Type)
                                                     ? OnDiskSABuilder.Mode.SUFFIX
                                                     : OnDiskSABuilder.Mode.ORIGINAL;

                        builder = new OnDiskSABuilder(keyComparator, validator, mode);
                        componentBuilders.put(indexInfo.component, builder);
                    }

                    builder.add(entry.getKey(), indexInfo.keys);
                }

                indexPerTerm.clear();

                // now do the writing
                logger.info("about to submit for concurrent SA'ing");
                List<Future<Boolean>> futures = new ArrayList<>(componentBuilders.size());
                for (Map.Entry<Component, OnDiskSABuilder> entry : componentBuilders.entrySet())
                {
                    String fileName = descriptor.filenameFor(entry.getKey());
                    logger.info("submitting {} for concurrent SA'ing", fileName);
                    futures.add(executor.submit(new BuilderFinisher(ranges.get(entry.getKey()), entry.getValue(), fileName)));
                }

                for (Future<Boolean> f : futures)
                {
                    try
                    {
                        // set *some* upper bound of wait time
                        f.get(120, TimeUnit.MINUTES);
                    }
                    catch (TimeoutException toe)
                    {
                        logger.warn("timed out waiting for a suffix array index to be created");
                    }
                    catch (Exception e)
                    {
                        throw new IOError(e);
                    }
                }
            }
            finally
            {
                openListeners.remove(descriptor.generation);
                // drop this data asap
                indexPerTerm.clear();
            }
        }

        public int compareTo(String o)
        {
            return descriptor.generation;
        }

        private class IndexInfo
        {
            private final Component component;
            private final TreeMap<DecoratedKey, Integer> keys;

            public IndexInfo(Component component)
            {
                this.component = component;
                this.keys = new TreeMap<>(new Comparator<DecoratedKey>()
                {
                    @Override
                    public int compare(DecoratedKey a, DecoratedKey b)
                    {
                        long tokenA = MurmurHash.hash2_64(a.key, a.key.position(), a.key.remaining(), 0);
                        long tokenB = MurmurHash.hash2_64(b.key, b.key.position(), b.key.remaining(), 0);

                        return Long.compare(tokenA, tokenB);
                    }
                });
            }

            public void add(DecoratedKey key, int offset)
            {
                keys.put(key, offset);
            }
        }
    }

    private class BuilderFinisher implements Callable<Boolean>
    {
        private final Pair<DecoratedKey, DecoratedKey> range;
        private OnDiskSABuilder builder;
        private final String fileName;

        public BuilderFinisher(Pair<DecoratedKey, DecoratedKey> range, OnDiskSABuilder builder, String fileName)
        {
            this.range = range;
            this.builder = builder;
            this.fileName = fileName;
        }

        public Boolean call() throws Exception
        {
            try
            {
                long start = System.nanoTime();
                builder.finish(range, new File(fileName));
                logger.info("finishing SA for {} in {} ms", fileName, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
                return Boolean.TRUE;
            }
            catch (Exception e)
            {
                throw new Exception(String.format("failed to write output file %s", fileName), e);
            }
            finally
            {
                //release the builder and any resources asap
                builder = null;
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

            Map<ByteBuffer, IntervalTree<ByteBuffer, SSTableReader, Interval<ByteBuffer, SSTableReader>>> termIntervalTree = new HashMap<>();
            Map<ByteBuffer, IntervalTree<ByteBuffer, SSTableReader, Interval<ByteBuffer, SSTableReader>>> keyIntervalTree = new HashMap<>();

            for (Pair<ByteBuffer, Expression> e : expressions)
            {
                String name = baseCfs.getComparator().getString(e.left);
                final AbstractType<?> validator = baseCfs.metadata.getColumnDefinitionFromColumnName(e.left).getValidator();

                List<Interval<ByteBuffer, SSTableReader>> termIntervals = new ArrayList<>();
                List<Interval<ByteBuffer, SSTableReader>> keyIntervals = new ArrayList<>();

                for (SSTableReader sstable : baseCfs.getDataTracker().getSSTables())
                {
                    try (RandomAccessFile index = new RandomAccessFile(new File(sstable.descriptor.filenameFor(String.format(FILE_NAME_FORMAT, name))), "r"))
                    {
                        ByteBuffer minTerm = ByteBufferUtil.readWithShortLength(index);
                        ByteBuffer maxTerm = ByteBufferUtil.readWithShortLength(index);

                        logger.info("(term) Interval.create(field: " + name + ", min: " + validator.getString(minTerm) + ", max: " + validator.getString(maxTerm) + ", sstable: " + sstable + ")");
                        termIntervals.add(Interval.create(minTerm, maxTerm, sstable));

                        ByteBuffer minKey = ByteBufferUtil.readWithShortLength(index);
                        ByteBuffer maxKey = ByteBufferUtil.readWithShortLength(index);

                        logger.info("(key) Interval.create(field: " + name + ", min: " + keyComparator.getString(minKey) + ", max: " + keyComparator.getString(maxKey) + ", sstable: " + sstable + ")");
                        keyIntervals.add(Interval.create(minKey, maxKey, sstable));
                    }
                }

                termIntervalTree.put(e.left, IntervalTree.build(termIntervals, new Comparator<ByteBuffer>()
                {
                    @Override
                    public int compare(ByteBuffer a, ByteBuffer b)
                    {
                        return validator.compare(a, b);
                    }
                }));

                keyIntervalTree.put(e.left, IntervalTree.build(keyIntervals, new Comparator<ByteBuffer>()
                {
                    @Override
                    public int compare(ByteBuffer a, ByteBuffer b)
                    {
                        return keyComparator.compare(a, b);
                    }
                }));
            }

            List<SSTableReader> primarySSTables = null;
            Pair<ByteBuffer, Expression> primaryExpression = null;

            for (Pair<ByteBuffer, Expression> e : expressions)
            {
                Expression expression = e.right;
                IntervalTree<ByteBuffer, SSTableReader, Interval<ByteBuffer, SSTableReader>> intervals = termIntervalTree.get(e.left);

                List<SSTableReader> sstables = (expression.lower == null || expression.upper == null)
                            ? intervals.search((expression.lower == null ? expression.upper : expression.lower).value)
                            : intervals.search(Interval.create(expression.lower.value, expression.upper.value, (SSTableReader) null));

                if (primarySSTables == null || primarySSTables.size() > sstables.size())
                {
                    primarySSTables = sstables;
                    primaryExpression = e;
                }
            }

            if (primarySSTables == null)
                return Collections.emptyList();

            expressions.remove(primaryExpression);

            logger.info("Primary: Field: " + baseCfs.getComparator().getString(primaryExpression.left) + ", SSTables: " + primarySSTables);

            final Set<DecoratedKey> evaluatedKeys = new TreeSet<>(DecoratedKey.comparator);

            outermost:
            try (ParallelSuffixIterator primarySuffixes = getSuffixIterator(primarySSTables.iterator(), primaryExpression.left, primaryExpression.right))
            {
                if (primarySuffixes == null)
                    return Collections.emptyList();

                while (primarySuffixes.hasNext())
                {
                    KeyContainer container = primarySuffixes.next().getKeys();
                    for (KeyContainer.Bucket a : container)
                    {
                        IntIterator primaryOffsets = a.getPositions().getIntIterator();

                        keys_loop:
                        while (primaryOffsets.hasNext())
                        {
                            final int keyOffset = primaryOffsets.next();
                            DecoratedKey key = primarySuffixes.currentSSTable().keyAt(keyOffset);

                            if (!requestedRange.contains(key) || !evaluatedKeys.add(key))
                                continue;

                            predicate_loop:
                            for (Pair<ByteBuffer, Expression> predicate : expressions)
                            {
                                List<SSTableReader> candidateSSTables = keyIntervalTree.get(predicate.left).search(key.key);
                                logger.info("candidate SSTables for (column: " + baseCfs.metadata.comparator.getString(predicate.left) + ", key: " + keyComparator.getString(key.key) + ") = " + candidateSSTables);

                                try (ParallelSuffixIterator suffixIterator = getSuffixIterator(candidateSSTables.iterator(), predicate.left, predicate.right))
                                {
                                    if (suffixIterator == null)
                                        continue keys_loop; //it's arguable that we should just bail on the whole query if there's no index for the predicate's column

                                    while (suffixIterator.hasNext())
                                    {
                                        DataSuffix suffix = suffixIterator.next();
                                        SSTableReader currentSSTable = suffixIterator.currentSSTable();

                                        KeyContainer candidates = suffix.getKeys();
                                        for (KeyContainer.Bucket candidate : candidates.intersect(key.key))
                                        {
                                            // if it's the same SSTable we can avoid reading actual keys
                                            // from the index file and match key offsets bitmap directly.
                                            if (primarySuffixes.currentSSTable().equals(currentSSTable))
                                            {
                                                if (candidate.getPositions().contains(keyOffset))
                                                    continue predicate_loop;
                                            }
                                            else // otherwise do a binary search in the external offsets, matching keys
                                            {
                                                int[] offsets = candidate.getPositions().toArray();

                                                int start = 0, end = offsets.length - 1;
                                                while (start <= end)
                                                {
                                                    int middle = start + ((end - start) >> 1);

                                                    int cmp = currentSSTable.keyAt(offsets[middle]).compareTo(key);
                                                    if (cmp == 0)
                                                        continue predicate_loop;

                                                    if (cmp < 0)
                                                        start = middle + 1;
                                                    else
                                                        end = middle - 1;
                                                }
                                            }
                                        }
                                    }
                                }
                                finally
                                {
                                    // do nothing here for now
                                }

                                // if we got here, there's no match for the current predicate, so skip this key
                                continue keys_loop;
                            }

                            Row row = getRow(key, filter, expressionsImmutable);

                            if (row != null)
                                rows.add(row);

                            if (rows.size() >= maxRows)
                                break outermost;
                        }
                    }
                }
            }

            return rows;
        }

        private ParallelSuffixIterator getSuffixIterator(Iterator<SSTableReader> sstables, ByteBuffer indexedColumn, Expression predicate)
        {
            ColumnDefinition columnDef = getColumnDefinition(indexedColumn);
            return columnDef != null ? new ParallelSuffixIterator(sstables, columnDef.name, predicate, columnDef.getValidator()) : null;
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

    private class ParallelSuffixIterator extends AbstractIterator<DataSuffix> implements Closeable
    {
        private final List<SuffixIterator> iterators;
        private Iterator<SuffixIterator> curIterator;
        private SuffixIterator suffixIterator;

        public ParallelSuffixIterator(Iterator<SSTableReader> sstables, ByteBuffer columnName, Expression exp, AbstractType<?> validator)
        {
            iterators = new ArrayList<>();
            while (sstables.hasNext())
                iterators.add(new SuffixIterator(sstables.next(), columnName, exp, validator));
            curIterator = iterators.iterator();
        }

        protected DataSuffix computeNext()
        {
            if (!curIterator.hasNext())
            {
                if (iterators.isEmpty())
                    return endOfData();
                curIterator = iterators.iterator();
            }

            suffixIterator = curIterator.next();
            while (!suffixIterator.hasNext())
            {
                suffixIterator.close();
                curIterator.remove();
                if (!curIterator.hasNext())
                    return computeNext();
                suffixIterator = curIterator.next();
            }
            return suffixIterator.next();
        }

        public SSTableReader currentSSTable()
        {
            return suffixIterator.sstable;
        }

        public void close() throws IOException
        {
            for (SuffixIterator iter : iterators)
                iter.close();
        }
    }

    private class SuffixIterator extends AbstractIterator<DataSuffix> implements Closeable
    {
        private final Expression exp;
        private final SSTableReader sstable;
        private final AbstractType<?> validator;
        private final IteratorOrder order;
        private final String indexFile;

        private OnDiskSA sa;
        private Iterator<DataSuffix> suffixes;

        public SuffixIterator(SSTableReader ssTables, ByteBuffer columnName, Expression exp, AbstractType<?> validator)
        {
            this.sstable = ssTables;
            this.validator = validator;
            this.exp = exp;
            this.order = exp.lower == null ? IteratorOrder.ASC : IteratorOrder.DESC;

            String name = (String) baseCfs.getComparator().compose(columnName);
            this.indexFile = sstable.descriptor.filenameFor(String.format(FILE_NAME_FORMAT, name));
        }

        @Override
        protected DataSuffix computeNext()
        {
            // lazily load the index file
            if (sa == null)
            {
                if (!load())
                    return endOfData();
            }

            if (!suffixes.hasNext())
                return endOfData();

            DataSuffix suffix = suffixes.next();

            if (order == IteratorOrder.DESC && exp.upper != null)
            {
                ByteBuffer s = suffix.getSuffix();
                s.limit(s.position() + exp.upper.value.remaining());

                int cmp = validator.compare(s, exp.upper.value);
                if ((cmp > 0 && exp.upper.inclusive) || (cmp >= 0 && !exp.upper.inclusive))
                    return endOfData();
            }

            return suffix;
        }

        private boolean load()
        {
            try
            {
                File f = new File(indexFile);
                if (f.exists())
                {
                    sa = new OnDiskSA(f, validator);
                    suffixes = (exp.lower == null) // in case query is col <(=) x
                               ? sa.iteratorAt(exp.upper.value, order, exp.upper.inclusive)
                               : sa.iteratorAt(exp.lower.value, order, exp.lower.inclusive);

                    if (suffixes.hasNext())
                        return true;
                }
            }
            catch (IOException e)
            {
                logger.info("failed to open suffix array file {}, skipping", indexFile, e);
            }
            return false;
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(sa);
        }
    }

    private static class BloomFilterPredicate implements Predicate<SSTableReader>
    {
        private final ByteBuffer key;

        private BloomFilterPredicate(ByteBuffer key)
        {
            this.key = key;
        }

        public boolean apply(SSTableReader reader)
        {
            return reader.getBloomFilter().isPresent(key);
        }
    }
}
