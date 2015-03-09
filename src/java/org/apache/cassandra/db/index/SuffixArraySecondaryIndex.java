package org.apache.cassandra.db.index;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.SSTableNamesIterator;
import org.apache.cassandra.db.compaction.*;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.index.search.Expression;
import org.apache.cassandra.db.index.search.OnDiskSA;
import org.apache.cassandra.db.index.search.OnDiskSABuilder;
import org.apache.cassandra.db.index.search.container.TokenTreeBuilder;
import org.apache.cassandra.db.index.search.memory.IndexMemtable;
import org.apache.cassandra.db.index.search.tokenization.AbstractTokenizer;
import org.apache.cassandra.db.index.search.tokenization.NoOpTokenizer;
import org.apache.cassandra.db.index.search.tokenization.StandardTokenizer;
import org.apache.cassandra.db.index.utils.LazyMergeSortIterator;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.SSTableWriterListener.Source;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.notifications.*;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.utils.*;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

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
public class SuffixArraySecondaryIndex extends PerRowSecondaryIndex
{
    private static final Logger logger = LoggerFactory.getLogger(SuffixArraySecondaryIndex.class);

    private static final String INDEX_MODE_OPTION = "mode";
    private static final String INDEX_ANALYZED_OPTION = "analyzed";

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
    private final ConcurrentMap<ByteBuffer, Pair<ColumnDefinition, IndexMode>> indexedColumns;

    private final ConcurrentMap<ByteBuffer, SADataTracker> intervalTrees;

    private final ConcurrentMap<Descriptor, CopyOnWriteArrayList<SSTableIndex>> currentIndexes;

    private final AtomicReference<IndexMemtable> globalMemtable = new AtomicReference<>(new IndexMemtable(this));

    private AbstractType<?> keyComparator;
    private boolean isInitialized;

    public SuffixArraySecondaryIndex()
    {
        columnDefComponents = HashBiMap.create();
        intervalTrees = new ConcurrentHashMap<>();
        currentIndexes = new ConcurrentHashMap<>();
        indexedColumns = new NonBlockingHashMap<>();
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

        Map<String, String> indexOptions = columnDef.getIndexOptions();

        Mode mode = indexOptions.get(INDEX_MODE_OPTION) == null ? Mode.ORIGINAL : Mode.mode(indexOptions.get(INDEX_MODE_OPTION));
        boolean isAnalyzed = indexOptions.get(INDEX_ANALYZED_OPTION) == null ? false : Boolean.valueOf(indexOptions.get(INDEX_ANALYZED_OPTION));

        indexedColumns.put(columnDef.name, Pair.create(columnDef, new IndexMode(mode, isAnalyzed)));
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

        for (ColumnDefinition def : defs)
        {
            String indexName = String.format(FILE_NAME_FORMAT, def.getIndexName());
            columnDefComponents.put(def.name, new Component(Component.Type.SECONDARY_INDEX, indexName));

            // on restart, sstables are loaded into DataTracker before 2I are hooked up (and init() invoked),
            // so we need to grab the sstables here
            if (!intervalTrees.containsKey(def.name))
                intervalTrees.put(def.name, new SADataTracker(def.name, baseCfs.getDataTracker().getSSTables()));
        }
    }

    public Pair<ColumnDefinition, IndexMode> getIndexDefinition(ByteBuffer columnName)
    {
        return indexedColumns.get(columnName);
    }

    private void addToIntervalTree(Collection<SSTableReader> readers, Collection<ColumnDefinition> defs)
    {
        for (ColumnDefinition columnDefinition : defs)
        {
            SADataTracker dataTracker = intervalTrees.get(columnDefinition.name);
            if (dataTracker == null)
            {
                SADataTracker newDataTracker = new SADataTracker(columnDefinition.name, Collections.<SSTableReader>emptySet());
                dataTracker = intervalTrees.putIfAbsent(columnDefinition.name, newDataTracker);
                if (dataTracker == null)
                    dataTracker = newDataTracker;
            }
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
        // nop
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
        return globalMemtable.get().estimateSize();
    }

    public void reload()
    {
        invalidateMemtable();
    }

    public void index(ByteBuffer key, ColumnFamily cf)
    {
        globalMemtable.get().index(key, cf);
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

    @Override
    public void buildIndexes(Collection<SSTableReader> sstables, Set<String> indexNames)
    {
        SortedSet<ByteBuffer> indexes = new TreeSet<>(baseCfs.getComparator());
        for (ColumnDefinition columnDef : columnDefs)
        {
            Iterator<String> iterator = indexNames.iterator();

            while (iterator.hasNext())
            {
                String indexName = iterator.next();
                if (columnDef.getIndexName().equals(indexName))
                {
                    dropIndexData(columnDef.name, System.currentTimeMillis());
                    indexes.add(columnDef.name);
                    iterator.remove();
                    break;
                }
            }
        }

        if (indexes.isEmpty())
            return;

        for (SSTableReader sstable : sstables)
        {
            try
            {
                FBUtilities.waitOnFuture(CompactionManager.instance.submitIndexBuild(new IndexBuilder(sstable, indexes)));
            }
            catch (Exception e)
            {
                logger.error("Failed index build task for " + sstable, e);
            }
        }
    }

    public void candidatesForIndexing(Collection<SSTableReader> initialSstables)
    {
        for (SSTableReader sstable : initialSstables)
        {
            SortedSet<ByteBuffer> missingIndexes = new TreeSet<>();
            for (ColumnDefinition def : getColumnDefs())
            {
                File indexFile = new File(sstable.descriptor.filenameFor(String.format(FILE_NAME_FORMAT, def.getIndexName())));
                if (!indexFile.exists())
                    missingIndexes.add(def.name);
            }

            if (!missingIndexes.isEmpty())
            {
                logger.info("submitting rebuild of missing indexes, count = {}, for sttable {}", missingIndexes.size(), sstable);
                CompactionManager.instance.submitIndexBuild(new IndexBuilder(sstable, missingIndexes));
            }
        }
    }

    public void delete(DecoratedKey key)
    {
        // called during 'nodetool cleanup' - can punt on impl'ing this
    }

    public void removeIndex(ByteBuffer columnName)
    {
        removeIndex(columnName, FBUtilities.timestampMicros());
    }

    public void removeIndex(ByteBuffer columnName, long truncateUntil)
    {
        columnDefComponents.remove(columnName);
        dropIndexData(columnName, truncateUntil);
    }

    private void dropIndexData(ByteBuffer columnName, long truncateUntil)
    {
        SADataTracker dataTracker = intervalTrees.get(columnName);
        if (dataTracker != null)
            dataTracker.dropData(truncateUntil);
    }

    public void invalidate()
    {
        invalidate(true, FBUtilities.timestampMicros());
    }

    public void invalidate(boolean invalidateMemtable, long truncateUntil)
    {
        if (invalidateMemtable)
            invalidateMemtable();

        for (ByteBuffer colName : new ArrayList<>(columnDefComponents.keySet()))
            dropIndexData(colName, truncateUntil);
    }

    private void invalidateMemtable()
    {
        globalMemtable.getAndSet(new IndexMemtable(this));
    }

    public void truncateBlocking(long truncatedAt)
    {
        invalidate(false, truncatedAt);
    }

    public void forceBlockingFlush()
    {
        //nop, as this 2I will flush with the owning CF's sstable, so we don't need this extra work
    }

    public Collection<Component> getIndexComponents()
    {
        return ImmutableList.<Component>builder().addAll(columnDefComponents.values()).build();
    }

    public SSTableWriterListener getWriterListener(Descriptor descriptor, Source source)
    {
        return new PerSSTableIndexWriter(descriptor, source);
    }

    private AbstractType<?> getValidator(ByteBuffer columnName)
    {
        ColumnDefinition columnDef = getColumnDefinition(columnName);
        return columnDef == null ? null : columnDef.getValidator();
    }

    protected SAView getView(ByteBuffer columnName)
    {
        SADataTracker dataTracker = intervalTrees.get(columnName);
        return dataTracker != null ? dataTracker.view.get() : null;
    }

    protected class PerSSTableIndexWriter implements SSTableWriterListener
    {
        private final Descriptor descriptor;
        private final Source source;

        // need one entry for each term we index
        private final Map<ByteBuffer, ColumnIndex> indexPerColumn;

        private DecoratedKey currentKey;
        private long currentKeyPosition;
        private boolean isComplete;

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

            index.add(column.value().duplicate(), currentKey, currentKeyPosition);
        }

        public void complete()
        {
            if (isComplete)
                return;
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
                isComplete = true;
            }
        }

        public int hashCode()
        {
            return descriptor.hashCode();
        }

        public boolean equals(Object o)
        {
            return !(o == null || !(o instanceof PerSSTableIndexWriter)) && descriptor.equals(((PerSSTableIndexWriter) o).descriptor);
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
                this.tokenizer = getTokenizer(indexedColumns.get(column.name));
                this.tokenizer.init(column.getIndexOptions());
                this.keysPerTerm = new HashMap<>();
            }

            private void add(ByteBuffer term, DecoratedKey key, long keyPosition)
            {
                final Long keyToken = ((LongToken) key.getToken()).token;

                if (!validate(key.key, term))
                    return;

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
                Mode mode = getMode(column.name);
                if (mode == null || keysPerTerm.size() == 0)
                    return;

                OnDiskSABuilder builder = new OnDiskSABuilder(column.getValidator(), mode);

                for (Map.Entry<ByteBuffer, TokenTreeBuilder> e : keysPerTerm.entrySet())
                    builder.add(e.getKey(), e.getValue());

                // since everything added to the builder, it's time to drop references to the data
                keysPerTerm.clear();

                builder.finish(Pair.create(min, max), new File(outputFile));
            }

            public boolean validate(ByteBuffer key, ByteBuffer term)
            {
                try
                {
                    column.getValidator().validate(term);
                    return true;
                }
                catch (MarshalException e)
                {
                    logger.error(String.format("Can't add column %s to index for key: %s", baseCfs.getComparator().getString(column.name),
                                                                                           keyComparator.getString(key)),
                                                                                           e);
                }

                return false;
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

            final int maxRows = Math.min(MAX_ROWS, filter.maxRows() == Integer.MAX_VALUE ? filter.maxColumns() : filter.maxRows());
            final Set<Row> rows = new TreeSet<>(new Comparator<Row>()
            {
                @Override
                public int compare(Row a, Row b)
                {
                    return a.key.compareTo(b.key);
                }
            });

            Stack<IndexExpression> group = new Stack<>();
            Stack<SSTableIndexMergeIterator> combinedGroups = new Stack<>();

            List<Expression> expressions = new ArrayList<>();

            for (IndexExpression e : filter.getClause())
            {
                if (e.isSetLogicalOp())
                {
                    OperationType op = OperationType.valueOf(e.logicalOp.name());

                    List<Expression.Column> arguments;
                    switch (group.size())
                    {
                        case 0: // both arguments come from the combined groups stack
                            SSTableIndexMergeIterator sideL = combinedGroups.pop();
                            SSTableIndexMergeIterator sideR = combinedGroups.pop();

                            combinedGroups.push(new SSTableIndexMergeIterator(op, sideL, sideR));
                            break;

                        case 1: // one of the arguments is from group other is from already combined groups stack
                            arguments = analyzeGroup(op, group.pop());

                            sideL = combinedGroups.pop();
                            sideR = getMergeIterator(op, arguments, sideL.getContext());

                            combinedGroups.push(new SSTableIndexMergeIterator(op, sideR, sideL));
                            expressions.addAll(arguments);
                            break;

                        default: // all arguments come from group expressions
                            arguments = analyzeGroup(op, group.pop(), group.pop());
                            combinedGroups.push(getMergeIterator(op, arguments, Collections.<SSTableIndex>emptySet()));
                            expressions.addAll(arguments);
                            break;
                    }

                    expressions.add(new Expression.Logical(op));
                }
                else
                {
                    group.add(e);
                }
            }

            // special case for backward compatibility and/or single expression statements,
            // assumes that all expressions of the group are linked with AND.
            if (group.size() > 0)
            {
                int count = 0;
                IndexExpression[] global = new IndexExpression[group.size()];
                while (!group.empty())
                    global[count++] = group.pop();

                List<Expression.Column> columns = analyzeGroup(OperationType.AND, global);
                expressions.addAll(columns);

                combinedGroups.push(getMergeIterator(OperationType.AND, columns, Collections.<SSTableIndex>emptySet()));
            }

            SSTableIndexMergeIterator joiner = combinedGroups.pop();

            try
            {
                fetch(joiner, requestedRange, filter, expressions, rows, maxRows);
                return Lists.newArrayList(rows);
            }
            finally
            {
                FileUtils.closeQuietly(joiner);
            }
        }

        private Pair<Expression.Column, Set<SSTableIndex>> getPrimaryExpression(List<Expression.Column> expressions)
        {
            Expression.Column expression = null;
            Set<SSTableIndex> primaryIndexes = null;

            for (Expression.Column e : expressions)
            {
                SAView view = getView(e.name);

                if (view == null)
                    continue;

                Set<SSTableIndex> indexes = view.match(e);
                if (primaryIndexes == null || primaryIndexes.size() > indexes.size())
                {
                    primaryIndexes = indexes;
                    expression = e;
                }
            }

            return Pair.create(expression, primaryIndexes);
        }

        /**
         * Get merge iterator for given set of the pre-analyzed expressions,
         * context is only used when operation is set to AND to limit the number of
         * SSTableIndex files read by queries of the same priority.
         *
         * @param op The operation to join expressions on.
         * @param expressions The expression group to join.
         * @param context Set of SSTableIndex files used by the neighbor AND operation.
         *
         * @return The joined iterator over data collected from given expressions.
         */
        private SSTableIndexMergeIterator getMergeIterator(OperationType op,
                                                           List<Expression.Column> expressions,
                                                           Set<SSTableIndex> context)
        {
            Expression.Column primaryExpression;
            Set<SSTableIndex> primaryIndexes;

            final IndexMemtable currentMemtable = globalMemtable.get();

            // try to compute primary only for AND operation
            if (op == OperationType.OR)
            {
                primaryExpression = null;
                primaryIndexes = Collections.emptySet();
            }
            else
            {
                Pair<Expression.Column, Set<SSTableIndex>> primary = getPrimaryExpression(expressions);
                primaryExpression = primary.left;
                primaryIndexes = primary.right != null ? primary.right : Collections.<SSTableIndex>emptySet();

                if (primaryIndexes.isEmpty())
                    primaryIndexes = context;
                else if (!context.isEmpty())
                    primaryIndexes = Sets.intersection(primaryIndexes, context);
            }

            Set<SSTableIndex> allIndexes = new HashSet<>();

            List<SkippableIterator<Long, Token>> unions = new ArrayList<>(expressions.size());
            for (Expression.Column e : expressions)
            {
                if (primaryExpression != null && primaryExpression.equals(e))
                {
                    unions.add(new SuffixIterator(primaryExpression,
                                                  currentMemtable.search(primaryExpression),
                                                  primaryIndexes));
                    continue;
                }

                Set<SSTableIndex> readers = new HashSet<>();
                SAView view = getView(e.name);

                if (view != null && primaryIndexes.size() > 0)
                {
                    for (SSTableIndex index : primaryIndexes)
                        readers.addAll(view.match(index.minKey(), index.maxKey()));
                }
                else if (view != null)
                {
                    readers.addAll(view.match(e));
                }

                unions.add(new SuffixIterator(e, currentMemtable.search(e), readers));
                allIndexes.addAll(readers);
            }

            return new SSTableIndexMergeIterator(op, unions, op == OperationType.AND ? primaryIndexes : allIndexes);
        }

        protected void fetch(SkippableIterator<Long, Token> joiner,
                             AbstractBounds<RowPosition> range,
                             ExtendedFilter filter,
                             List<Expression> expressions,
                             Set<Row> aggregator,
                             int maxRows)
        {

            joiner.skipTo(((LongToken) range.left.getToken()).token);

            intersection:
            while (joiner.hasNext())
            {
                for (DecoratedKey key : joiner.next())
                {
                    if (!range.contains(key) || aggregator.size() >= maxRows)
                        break intersection;

                    Row row = getRow(key, filter, expressions);
                    if (row != null)
                        aggregator.add(row);
                }
            }
        }

        private Row getRow(DecoratedKey key, IDiskAtomFilter columnFilter, List<Expression> expressions)
        {
            ReadCommand cmd = ReadCommand.create(baseCfs.keyspace.getName(),
                                                 key.key,
                                                 baseCfs.getColumnFamilyName(),
                                                 System.currentTimeMillis(),
                                                 columnFilter);

            return new RowReader(cmd, expressions).call();
        }

        private Row getRow(DecoratedKey key, ExtendedFilter filter, List<Expression> expressions)
        {
            return getRow(key, filter.columnFilter(key.key), expressions);
        }

        private class RowReader implements Callable<Row>
        {
            private final ReadCommand command;
            private final List<Expression> expressions;

            public RowReader(ReadCommand command, List<Expression> expressions)
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
             *
             * Expression list has all of the predicates in the polish notation, we use this fact
             * here and try to build a stack machine evaluation per returned indexed column, it works as follows:
             *
             * For each given row iterate through expressions in order and if it's column expression
             * evaluate column value based on expression and returns a boolean,
             * push it to the stack of results, when logical expression is encountered
             * do a boolean logic operation & or | between previously evaluated results (binary operation),
             * once all of the expressions have been evaluated stack will have a single element left
             * which represents a decision for a whole row.
             *
             * Query: f:x OR (a > 5 AND b < 7)
             * Expressions: [b < 7, a > 5, AND, f:x, OR]
             * Row: key1(a:9, b:6, f:y)
             *
             * Evaluation:
             *
             * #1 expression b < 7
             *    - get column b -> b:6
             *    - validate value -> true
             *    - push true
             * #2 expression a > 5
             *    - get column a -> a:9
             *    - validate value -> true
             *    - push true
             * #3 expression AND
             *    - pop -> true
             *    - pop -> true
             *    - apply AND true & true -> true
             *    - push true
             * #4 expression f:x
             *    - get column f -> f:y
             *    - validate value -> false
             *    - push false
             * #5 expression OR
             *    - pop -> false
             *    - pop -> true
             *    - apply OR false | true -> true
             *    - push true <--- this is the final result for the row
             */
            private boolean satisfiesPredicates(Row row)
            {
                if (row == null || row.cf == null || row.cf.isMarkedForDelete())
                    return false;

                final long now = System.currentTimeMillis();
                final Stack<Boolean> conditions = new Stack<>();

                for (Expression e : expressions)
                {
                    if (e.isLogical())
                    {
                        // expression merges the same column bounds from distinct IndexExpressions' (only for AND)
                        // to a single Expression.Column e.g. age > X AND age < Y are going to be
                        // Expression.Column{name: age, lower: X, upper: Y} so situation when logical operation
                        // has a single argument in the queue is expected.
                        if (conditions.size() == 1)
                            continue;

                        switch (e.toLogicalExpression().op)
                        {
                            case AND:
                                conditions.push(conditions.pop() & conditions.pop());
                                break;

                            case OR:
                                conditions.push(conditions.pop() | conditions.pop());
                        }
                    }
                    else
                    {
                        Expression.Column expression = e.toColumnExpression();

                        Column column = row.cf.getColumn(expression.name);
                        if (column == null)
                            throw new IllegalStateException("All indexed columns should be included into the column slice, missing: "
                                                          + baseCfs.getComparator().getString(expression.name));

                        if (!column.isLive(now))
                            return false;

                        conditions.push(expression.contains(column.value()));
                    }
                }

                int conditionCount = conditions.size();
                switch (conditionCount)
                {
                    case 0:
                        throw new AssertionError();

                    case 1:
                        return conditions.pop();

                    default: // backward compatibility case, everything is linked with AND
                        boolean result = false;
                        for (int i = 0; i < conditionCount; i++)
                            result = (i == 0) ? conditions.pop() : result & conditions.pop();

                        return result;
                }
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
    }

    private class SuffixIterator extends AbstractIterator<Token> implements SkippableIterator<Long, Token>
    {
        private final SkippableIterator<Long, Token> union;
        private final List<SSTableIndex> referencedIndexes = new ArrayList<>();

        public SuffixIterator(Expression.Column expression,
                              SkippableIterator<Long, Token> memtableIterator,
                              final Collection<SSTableIndex> perSSTableIndexes)
        {
            List<SkippableIterator<Long, Token>> keys = new ArrayList<>(perSSTableIndexes.size());

            if (memtableIterator != null)
                keys.add(memtableIterator);

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
                {
                    // reference count has already been decremented by marking as obsolete
                    if (!index.isObsolete())
                        index.release();
                }
            }
        }

        public void dropData(long truncateUntil)
        {
            SAView currentView = view.get();
            if (currentView == null)
                return;

            Set<SSTableReader> toRemove = new HashSet<>();
            for (SSTableIndex index : currentView)
            {
                if (index.sstable.getMaxTimestamp() > truncateUntil)
                    continue;

                index.markObsolete();
                toRemove.add(index.sstable);
            }

            update(toRemove, Collections.<SSTableReader>emptyList());
        }
    }

    private class SAView implements Iterable<SSTableIndex>
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
            this.validator = previous == null
                                ? baseCfs.metadata.getColumnDefinitionFromColumnName(col).getValidator()
                                : previous.validator;

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
                String columnName = getColumnDefinition(col).getIndexName();
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

        public Set<SSTableIndex> match(Expression.Column expression)
        {
            if (minSuffix == null) // no data in this view
                return Collections.emptySet();

            ByteBuffer min = expression.lower == null ? minSuffix : expression.lower.value;
            ByteBuffer max = expression.upper == null ? maxSuffix : expression.upper.value;

            if (validator.compare(min, minSuffix) < 0)
                min = minSuffix;

            if (validator.compare(max, minSuffix) < 0)
                max = minSuffix;

            return new HashSet<>(termIntervalTree.search(Interval.create(min, max, (SSTableIndex) null)));
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

        public Iterator<SSTableIndex> iterator()
        {
            return Iterators.transform(keyIntervalTree.iterator(), new Function<Interval<ByteBuffer, SSTableIndex>, SSTableIndex>()
            {
                public SSTableIndex apply(Interval<ByteBuffer, SSTableIndex> i)
                {
                    return i.data;
                }
            });
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
            else if (notification instanceof MemtableRenewedNotification)
            {
                invalidateMemtable();
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
        private final AtomicBoolean obsolete = new AtomicBoolean(false);

        public SSTableIndex(ByteBuffer name, File indexFile, SSTableReader referent)
        {
            column = name;
            sstable = referent;

            if (!sstable.acquireReference())
                throw new IllegalStateException("Couldn't acquire reference to the sstable: " + sstable);

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
                if (obsolete.get())
                    FileUtils.delete(index.getIndexPath());
            }
        }

        public void markObsolete()
        {
            obsolete.getAndSet(true);
            release();
        }

        public boolean isObsolete()
        {
            return obsolete.get();
        }

        @Override
        public boolean equals(Object o)
        {
            // name is not checked on purpose that simplifies context handling between logical operations
            return o instanceof SSTableIndex && sstable.descriptor.equals(((SSTableIndex) o).sstable.descriptor);
        }

        @Override
        public int hashCode()
        {
            return sstable.hashCode();
        }

        @Override
        public String toString()
        {
            return String.format("SSTableIndex(column: %s, SSTable: %s)", baseCfs.getComparator().getString(column), sstable.descriptor);
        }
    }

    private class IndexBuilder extends IndexBuildTask
    {
        private final KeyIterator keys;

        private final SSTableReader sstable;
        private final SortedSet<ByteBuffer> indexNames;
        private final Collection<ColumnDefinition> indexes;
        private final PerSSTableIndexWriter indexWriter;

        public IndexBuilder(SSTableReader sstable, SortedSet<ByteBuffer> indexesToBuild)
        {
            this.keys = new KeyIterator(sstable.descriptor);
            this.sstable = sstable;

            if (!sstable.acquireReference())
                throw new IllegalStateException("Couldn't acquire reference to the sstable: " + sstable);

            this.indexWriter = new PerSSTableIndexWriter(sstable.descriptor.asTemporary(true), SSTableWriterListener.Source.COMPACTION);
            this.indexNames = indexesToBuild;
            this.indexes = new ArrayList<ColumnDefinition>()
            {{
                for (ByteBuffer name : indexNames)
                    add(getColumnDefinition(name));
            }};
        }

        @Override
        public CompactionInfo getCompactionInfo()
        {
            return new CompactionInfo(baseCfs.metadata,
                                      org.apache.cassandra.db.compaction.OperationType.INDEX_BUILD,
                                      keys.getBytesRead(),
                                      keys.getTotalBytes());
        }

        public void build()
        {
            try
            {
                while (keys.hasNext())
                {
                    if (isStopRequested())
                        throw new CompactionInterruptedException(getCompactionInfo());

                    DecoratedKey key = keys.next();

                    indexWriter.startRow(key, keys.getKeyPosition());

                    SSTableNamesIterator columns = new SSTableNamesIterator(sstable, key, indexNames);

                    while (columns.hasNext())
                    {
                        OnDiskAtom atom = columns.next();

                        if (atom != null && atom instanceof Column)
                            indexWriter.nextColumn((Column) atom);
                    }
                }
            }
            finally
            {
                sstable.releaseReference();
            }

            indexWriter.complete();

            for (ColumnDefinition columnDef : indexes)
            {
                String indexName = String.format(FILE_NAME_FORMAT, columnDef.getIndexName());

                File tmpIndex = new File(indexWriter.descriptor.filenameFor(indexName));
                if (!tmpIndex.exists()) // no data was inserted into the index for given sstable
                    continue;

                FileUtils.renameWithConfirm(tmpIndex, new File(sstable.descriptor.filenameFor(indexName)));
            }

            addToIntervalTree(Collections.singletonList(sstable), indexes);
        }
    }

    public Mode getMode(ByteBuffer columnName)
    {
        Pair<ColumnDefinition, IndexMode> definition = indexedColumns.get(columnName);
        return definition == null ? null : definition.right.mode;
    }

    public static AbstractTokenizer getTokenizer(Pair<ColumnDefinition, IndexMode> column)
    {
        assert column != null;
        return column.right.isAnalyzed && TOKENIZABLE_TYPES.contains(column.left.getValidator()) ? new StandardTokenizer() : new NoOpTokenizer();
    }

    private List<Expression.Column> analyzeGroup(OperationType op, IndexExpression... expressions)
    {
        Multimap<ByteBuffer, Expression.Column> analyzed = HashMultimap.create();

        for (final IndexExpression e : expressions)
        {
            if (e.isSetLogicalOp())
                continue;

            ByteBuffer name = ByteBuffer.wrap(e.getColumn_name());
            Pair<ColumnDefinition, IndexMode> column = indexedColumns.get(name);

            if (column == null)
            {
                logger.error("Requested column: " + baseCfs.getComparator().getString(name) + ", wasn't found in the index.");
                continue;
            }

            AbstractType<?> validator = column.left.getValidator();

            Collection<Expression.Column> perColumn = analyzed.get(name);

            switch (e.getOp())
            {
                // '=' can have multiple expressions e.g. text = "Hello World",
                // becomes text = "Hello" AND text = "WORLD"
                // because "space" is always interpreted as a split point.
                case EQ:
                    final AbstractTokenizer tokenizer = getTokenizer(column);
                    tokenizer.init(column.left.getIndexOptions());
                    tokenizer.reset(ByteBuffer.wrap(e.getValue()));
                    while (tokenizer.hasNext())
                    {
                        perColumn.add(new Expression.Column(name, validator)
                        {{
                            add(e.op, tokenizer.next());
                        }});
                    }
                    break;

                // default means "range" operator, combines both bounds together into the single expression,
                // iff operation of the group is AND, otherwise we are forced to create separate expressions
                default:
                    Expression.Column range;
                    if (perColumn.size() == 0 || op != OperationType.AND)
                        perColumn.add((range = new Expression.Column(name, validator)));
                    else
                        range = Iterators.getLast(perColumn.iterator());

                    range.add(e.op, e.bufferForValue());
                    break;
            }
        }

        List<Expression.Column> result = new ArrayList<>();
        for (Map.Entry<ByteBuffer, Expression.Column> e : analyzed.entries())
            result.add(e.getValue());

        return result;
    }

    public class SSTableIndexMergeIterator extends LazyMergeSortIterator<Long, Token>
    {
        private final Set<SSTableIndex> indexes;

        public SSTableIndexMergeIterator(OperationType op, SSTableIndexMergeIterator sideL, SSTableIndexMergeIterator sideR)
        {
            super(op, Arrays.<SkippableIterator<Long, Token>>asList(sideL, sideR));
            indexes = Sets.union(sideL.getContext(), sideR.getContext());
        }

        public SSTableIndexMergeIterator(OperationType op, List<SkippableIterator<Long, Token>> group, Set<SSTableIndex> context)
        {
            super(op, group);
            indexes = context;
        }

        public Set<SSTableIndex> getContext()
        {
            return indexes;
        }
    }

    public static class IndexMode
    {
        public final Mode mode;
        public final boolean isAnalyzed;

        public IndexMode(Mode mode, boolean isAnalyzed)
        {
            this.mode = mode;
            this.isAnalyzed = isAnalyzed;
        }
    }
}

