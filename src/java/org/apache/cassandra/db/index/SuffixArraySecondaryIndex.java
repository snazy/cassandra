package org.apache.cassandra.db.index;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.search.AbstractTokenizer;
import org.apache.cassandra.db.index.utils.LazyMergeSortIterator;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableDeletingNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
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
import com.google.common.collect.*;
import com.google.common.util.concurrent.Futures;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.db.index.utils.LazyMergeSortIterator.OperationType;

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
    public static final String TOKENIZATION_CLASS_OPTION_NAME = "tokenization_class";

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
    private final Map<ByteBuffer, Class<?>> columnTokenizers;

    private final Map<ByteBuffer, SADataTracker> intervalTrees;

    private AbstractType<?> keyComparator;

    public SuffixArraySecondaryIndex()
    {
        openListeners = new ConcurrentHashMap<>();
        columnDefComponents = HashBiMap.create();
        columnTokenizers = new ConcurrentHashMap<>();
        intervalTrees = new ConcurrentHashMap<>();
    }

    public void init()
    {
        // init() is called by SIM only on the instance that it will keep around, but will call addColumnDef on any instance
        // that it happens to create (and subsequently/immediately throw away)
        addComponent(columnDefs);

        baseCfs.getDataTracker().subscribe(new DataTrackerConsumer());
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
        else if (keyComparator == null)
            keyComparator = this.baseCfs.metadata.getKeyValidator();
        executor.setCorePoolSize(columnDefs.size() * 2);

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

        // todo: where should this actually go?
        try
        {
            for (ColumnDefinition colDef : columnDefs)
            {
                Map<String, String> indexOptions = colDef.getIndexOptions();
                columnTokenizers.put(colDef.name, Class.forName(indexOptions.get(TOKENIZATION_CLASS_OPTION_NAME)));
            }
        }
        catch (Exception e)
        {
            logger.error("Failed to parse index options from column", e);
        }
    }

    private void addToIntervalTree(Collection<SSTableReader> readers, Collection<ColumnDefinition> defs)
    {
        for (ColumnDefinition columnDefinition : defs)
        {
            SADataTracker dataTracker = intervalTrees.get(columnDefinition.name);
            dataTracker.update(Collections.EMPTY_LIST, readers);
        }
    }

    private void removeFromIntervalTree(Collection<SSTableReader> readers)
    {
        for (Map.Entry<ByteBuffer, SADataTracker> entry : intervalTrees.entrySet())
        {
            entry.getValue().update(readers, Collections.EMPTY_LIST);
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
        for (ColumnDefinition colDef : columnDefs)
        {
            if (!colDef.isIndexed())
                continue;

            Map<String, String> indexOptions = colDef.getIndexOptions();

            // if specified, check to make sure the provided tokenization class is valid
            if (indexOptions.get(TOKENIZATION_CLASS_OPTION_NAME) != null)
            {
                try
                {
                    Class<?> clazz = Class.forName(indexOptions.get(TOKENIZATION_CLASS_OPTION_NAME));
                    Object instance = clazz.newInstance();
                    if (!(instance instanceof AbstractTokenizer))
                    {
                        throw new ConfigurationException("Unable to use configured tokenization "
                                + "class [" + clazz.getName() + "] as it does not extend AbstractTokenizer");
                    }
                }
                catch (ClassNotFoundException e)
                {
                    throw new ConfigurationException("Tokenization class "
                            + "[" + indexOptions.get(TOKENIZATION_CLASS_OPTION_NAME) + "] not found");
                }
                catch (IllegalAccessException | InstantiationException e)
                {
                    throw new ConfigurationException("Unable to instantiate an instance of "
                            + "tokenization class [" + indexOptions.get(TOKENIZATION_CLASS_OPTION_NAME) + "]");
                }
            }
        }
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

    private AbstractType<?> getValidator(ByteBuffer columnName)
    {
        ColumnDefinition columnDef = getColumnDefinition(columnName);
        return columnDef == null ? null : columnDef.getValidator();
    }

    private AbstractTokenizer<ByteBuffer> getTokenizer(ByteBuffer columnName, ByteBuffer input)
    {
        Class<?> tokenizerClass = columnTokenizers.get(columnName);
        AbstractTokenizer<ByteBuffer> tokenizer = null;
        if (tokenizerClass != null)
        {
            try {
                tokenizer = (AbstractTokenizer<ByteBuffer>) columnTokenizers.get(columnName).newInstance();
                tokenizer.setOptions(getColumnDefinition(columnName).getIndexOptions());
                tokenizer.setInput(input);
                tokenizer.init();
            }
            catch (Exception e)
            {
                String columnNameToString;
                try
                {
                    columnNameToString = ByteBufferUtil.string(columnName);
                }
                catch (CharacterCodingException e1)
                {
                    logger.error("Failed to get column name as string", e1);
                    columnNameToString = "Unknown";
                }
                logger.error("Failed to get a tokenizer instance for column " +
                        "name [{}]", columnNameToString, e);
            }
        }
        return (tokenizer == null) ? null : tokenizer;
    }

    protected class LocalSSTableWriterListener implements SSTableWriterListener
    {
        private final Descriptor descriptor;

        // need one entry for each term we index
        private final Map<ByteBuffer, IndexInfo> indexPerTerm;
        private final Map<Component, Pair<DecoratedKey, DecoratedKey>> ranges;

        private DecoratedKey curKey;
        private long curFilePosition;

        private AbstractTokenizer<ByteBuffer> tokenizer;

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

            // todo: for performance we should share an instance, but we
            // don't have the column name until here.. and options per column..
            // additionally, bc we are using AbstractIterator now, once endOfData()
            // is called once it appears there is no way to reset it..
            tokenizer = getTokenizer(column.name(), term);

            if (tokenizer != null)
            {
                while (tokenizer.hasNext())
                {
                    ByteBuffer token = tokenizer.next();
                    IndexInfo indexInfo = indexPerTerm.get(token);
                    if (indexInfo == null)
                        indexPerTerm.put(token, (indexInfo = new IndexInfo(component)));

                    indexInfo.add(curKey, (int) curFilePosition);
                }
            }
            else
            {
                IndexInfo indexInfo = indexPerTerm.get(term);
                if (indexInfo == null)
                    indexPerTerm.put(term, (indexInfo = new IndexInfo(component)));

                indexInfo.add(curKey, (int) curFilePosition);
            }

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

            List<SSTableReader> primarySSTables = null;
            Pair<ByteBuffer, Expression> primaryExpression = null;

            for (Pair<ByteBuffer, Expression> e : expressions)
            {
                Expression expression = e.right;
                IntervalTree<ByteBuffer, SSTableReader, Interval<ByteBuffer, SSTableReader>> intervals = intervalTrees.get(e.left).view.get().termIntervalTree;

                List<SSTableReader> sstables = (expression.lower == null || expression.upper == null)
                            ? intervals.search((expression.lower == null ? expression.upper : expression.lower).value)
                            : intervals.search(Interval.create(expression.lower.value, expression.upper.value, (SSTableReader) null));

                if (primarySSTables == null || primarySSTables.size() > sstables.size())
                {
                    primarySSTables = sstables;
                    primaryExpression = e;
                }
            }

            if (primarySSTables == null || primarySSTables.size() == 0)
                return Collections.emptyList();

            expressions.remove(primaryExpression);

            logger.info("Primary: Field: " + baseCfs.getComparator().getString(primaryExpression.left) + ", SSTables: " + primarySSTables);
            Map<ByteBuffer, Set<SSTableReader>> narrowedCandidates = new HashMap<>(expressionsImmutable.size());

            // add primary expression first
            narrowedCandidates.put(primaryExpression.left, new HashSet<>(primarySSTables));

            for (Pair<ByteBuffer, Expression> e : expressions)
            {
                final AbstractType<?> validator = baseCfs.metadata.getColumnDefinitionFromColumnName(e.left).getValidator();
                if (validator.compare(primaryExpression.left, e.left) == 0)
                {
                    narrowedCandidates.put(e.left, new HashSet<>(primarySSTables));
                    continue;
                }

                Set<SSTableReader> readers = new HashSet<>();
                String name = baseCfs.getComparator().getString(e.left);
                IntervalTree<ByteBuffer, SSTableReader, Interval<ByteBuffer, SSTableReader>> it = intervalTrees.get(e.left).view.get().keyIntervalTree;
                for (SSTableReader reader : primarySSTables)
                {
                    try (RandomAccessFile index = new RandomAccessFile(new File(reader.descriptor.filenameFor(String.format(FILE_NAME_FORMAT, name))), "r"))
                    {
                        // read past the min/max terms
                        ByteBufferUtil.readWithShortLength(index);
                        ByteBufferUtil.readWithShortLength(index);
                        ByteBuffer minKey = ByteBufferUtil.readWithShortLength(index);
                        ByteBuffer maxKey = ByteBufferUtil.readWithShortLength(index);

                        readers.addAll(it.search(Interval.create(minKey, maxKey, (SSTableReader) null)));
                    }
                }

                // might not want to fail the entire search if we fail to acquire references - maybe retry (but will need updated SADataTracker) ??
                if (readers.isEmpty() || !SSTableReader.acquireReferences(readers))
                {
                    for (Map.Entry<ByteBuffer, Set<SSTableReader>> entry : narrowedCandidates.entrySet())
                        SSTableReader.releaseReferences(entry.getValue());
                    return Collections.emptyList();
                }

                narrowedCandidates.put(e.left, readers);
            }

            try
            {
                List<SkippableIterator<DecoratedKey>> suffixes = new ArrayList<>(expressionsImmutable.size());
                for (Pair<ByteBuffer, Expression> expression : expressionsImmutable)
                {
                    ByteBuffer name = expression.left;
                    suffixes.add(new SuffixIterator(name, expression.right, narrowedCandidates.get(name)));
                }

                Iterator<DecoratedKey> joiner = new LazyMergeSortIterator<>(DecoratedKey.comparator, OperationType.AND, suffixes);
                while (joiner.hasNext() && rows.size() < maxRows)
                {
                    DecoratedKey key = joiner.next();

                    if (!requestedRange.contains(key))
                        continue;

                    Row row = getRow(key, filter, expressionsImmutable);
                    if (row != null)
                        rows.add(row);
                }
            }
            finally
            {
                for (Map.Entry<ByteBuffer, Set<SSTableReader>> entry : narrowedCandidates.entrySet())
                    SSTableReader.releaseReferences(entry.getValue());
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

    private class SuffixIterator extends AbstractIterator<DecoratedKey> implements SkippableIterator<DecoratedKey>
    {
        private final SkippableIterator<DecoratedKey> union;

        public SuffixIterator(ByteBuffer columnName, Expression expression, Set<SSTableReader> sstables)
        {
            AbstractType<?> validator = getValidator(columnName);
            assert validator != null;

            String name = (String) baseCfs.getComparator().compose(columnName);
            List<SkippableIterator<DecoratedKey>> keys = new ArrayList<>(sstables.size());

            for (SSTableReader reader : sstables)
            {
                File index = new File(reader.descriptor.filenameFor(String.format(FILE_NAME_FORMAT, name)));
                assert index.exists() : "SSTable " + reader.getFilename() + " should have index " + name;

                OnDiskSA sa = null;
                try
                {
                    sa = new OnDiskSA(index, validator);

                    ByteBuffer lower = (expression.lower == null) ? null : expression.lower.value;
                    ByteBuffer upper = (expression.upper == null) ? null : expression.upper.value;

                    keys.add(new DecoratedKeySkippableIterator(sa.search(lower, lower == null || expression.lower.inclusive,
                                                                         upper, upper == null || expression.upper.inclusive),
                                                               reader));
                }
                catch (IOException e)
                {
                    throw new FSReadError(e, index.getAbsolutePath());
                }
                finally
                {
                    FileUtils.closeQuietly(sa);
                }
            }

            union = new LazyMergeSortIterator<>(DecoratedKey.comparator, OperationType.OR, keys);
        }

        @Override
        protected DecoratedKey computeNext()
        {
            return union.hasNext() ? union.next() : endOfData();
        }

        @Override
        public void skipTo(DecoratedKey next)
        {
            union.skipTo(next);
        }
    }

    private class DecoratedKeySkippableIterator extends AbstractIterator<DecoratedKey> implements SkippableIterator<DecoratedKey>
    {
        private final OffsetPeekingIterator offsets;
        private final SSTableReader reader;

        private DecoratedKeySkippableIterator(RoaringBitmap offsets, SSTableReader reader)
        {
            this.reader = reader;
            this.offsets = new OffsetPeekingIterator(offsets);
        }

        @Override
        protected DecoratedKey computeNext()
        {
            try
            {
                return offsets.hasNext() ? reader.keyAt(offsets.next()) : endOfData();
            }
            catch (IOException e)
            {
                throw new FSReadError(e, reader.getFilename());
            }
        }

        @Override
        public void skipTo(DecoratedKey next)
        {
            RowIndexEntry entry = reader.getPosition(next, SSTableReader.Operator.GE);
            if (entry == null)
                return;

            offsets.skipTo((int) entry.position);
        }
    }

    private static class OffsetPeekingIterator extends AbstractIterator<Integer> implements SkippableIterator<Integer>, PeekingIterator<Integer>
    {
        private final Iterator<Integer> offsets;

        private OffsetPeekingIterator(RoaringBitmap offsets)
        {
            this.offsets = offsets == null ? null : offsets.iterator();
        }

        @Override
        protected Integer computeNext()
        {
            return offsets != null && offsets.hasNext() ? offsets.next() : endOfData();
        }

        @Override
        public void skipTo(Integer next)
        {
            while (hasNext())
            {
                int nextOffset = peek();
                if (nextOffset >= next)
                    break;

                next();
            }
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
        }
    }

    private class SAView
    {
        private final ByteBuffer col;
        final IntervalTree<ByteBuffer, SSTableReader, Interval<ByteBuffer, SSTableReader>> termIntervalTree;
        final IntervalTree<ByteBuffer, SSTableReader, Interval<ByteBuffer, SSTableReader>> keyIntervalTree;

        public SAView(ByteBuffer col, Set<SSTableReader> sstables)
        {
            this(null, col, Collections.EMPTY_LIST, sstables);
        }

        private SAView(SAView previous, ByteBuffer col, final Collection<SSTableReader> toRemove, Collection<SSTableReader> toAdd)
        {
            this.col = col;

            String name = baseCfs.getComparator().getString(col);
            final AbstractType<?> validator = baseCfs.metadata.getColumnDefinitionFromColumnName(col).getValidator();

            Predicate<Interval<ByteBuffer, SSTableReader>> predicate = new Predicate<Interval<ByteBuffer, SSTableReader>>()
            {
                public boolean apply(Interval<ByteBuffer, SSTableReader> interval)
                {
                    return !toRemove.contains(interval.data);
                }
            };

            List<Interval<ByteBuffer, SSTableReader>> termIntervals = new ArrayList<>();
            List<Interval<ByteBuffer, SSTableReader>> keyIntervals = new ArrayList<>();

            // reuse entries from the previously constructed view to avoid reloading from disk or keep (yet another) cache of the sstreader -> intervals
            if (previous != null)
            {
                for (Interval<ByteBuffer, SSTableReader> interval : Iterables.filter(previous.keyIntervalTree, predicate))
                    keyIntervals.add(interval);
                for (Interval<ByteBuffer, SSTableReader> interval : Iterables.filter(previous.termIntervalTree, predicate))
                    termIntervals.add(interval);
            }

            for (SSTableReader sstable : toAdd)
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
                catch (IOException ioe)
                {
                    logger.warn("unable to read SA file - ignoring", ioe);
                }
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

        public SAView update(Collection<SSTableReader> toRemove, Collection<SSTableReader> toAdd)
        {
            int newKeysSize = assertCorrectSizing(keyIntervalTree, toRemove, toAdd);
            int newTermsSize = assertCorrectSizing(termIntervalTree, toRemove, toAdd);
            assert newKeysSize == newTermsSize : String.format("mismatched sizes for intervals tree for keys vs terms: %d != %d", newKeysSize, newTermsSize);

            return new SAView(this, col, toRemove, toAdd);
        }

        private int assertCorrectSizing(IntervalTree<ByteBuffer, SSTableReader, Interval<ByteBuffer, SSTableReader>> tree, Collection<SSTableReader> oldSSTables, Collection<SSTableReader> newReaders)
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
}

