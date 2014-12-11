package org.apache.cassandra.db.index;

import java.io.Closeable;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataTracker.SSTableIntervalTree;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.index.search.OnDiskSA;
import org.apache.cassandra.db.index.search.OnDiskSABuilder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriterListenable;
import org.apache.cassandra.io.sstable.SSTableWriterListener;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableDeletingNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.Pair;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

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
    private final Map<ByteBuffer, SADataTracker> intervalTrees;

    private DataTrackerConsumer consumer;

    public SuffixArraySecondaryIndex()
    {
        openListeners = new ConcurrentHashMap<>();
        columnDefComponents = HashBiMap.create();
        intervalTrees = new ConcurrentHashMap<>();
    }

    public void init()
    {
        // init() is called by SIM only on the instance that it will keep around, but will call addColumnDef on any instance
        // that it happens to create (and subsequently/immediately throw away)
        addComponent(columnDefs);

        if (consumer == null)
        {
            consumer = new DataTrackerConsumer();
            baseCfs.getDataTracker().subscribe(consumer);
        }
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
        List<ColumnDefinition> toLoad = new ArrayList<>(defs.size());
        for (ColumnDefinition def : defs)
        {
            String indexName = String.format(FILE_NAME_FORMAT, type.getString(def.name));
            columnDefComponents.put(def.name, new Component(Component.Type.SECONDARY_INDEX, indexName));

            // on restart, sstables are loaded into DataTracker before 2I are hooked up (and init() invoked),
            // so we need to grab the sstables here
            if (!intervalTrees.containsKey(def.name))
            {
                intervalTrees.put(def.name, new SADataTracker());
                toLoad.add(def);
            }
        }

        addToIntervalTree(baseCfs.getDataTracker().getSSTables(), toLoad);
        executor.setCorePoolSize(columnDefs.size());
    }

    private void addToIntervalTree(Collection<SSTableReader> readers, Collection<ColumnDefinition> defs)
    {
        for (ColumnDefinition columnDefinition : defs)
        {
            String columnName = (String) baseCfs.getComparator().compose(columnDefinition.name);

            // build collection of Intervals first
            Collection<Interval<RowPosition, SSTableReader>> intervals = new ArrayList<>(readers.size());
            for (SSTableReader reader : readers)
            {
                try
                {
                    String indexFile = reader.descriptor.filenameFor(String.format(FILE_NAME_FORMAT, columnName));
                    File f = new File(indexFile);
                    if (!f.exists())
                        continue;
                    Pair<Integer, Integer> positions = derivePositions(f, columnDefinition);
                    if (null != positions)
                        intervals.add(Interval.<RowPosition, SSTableReader>create
                                           (reader.keyAt(positions.left), reader.keyAt(positions.right), reader));
                }
                catch (IOException e)
                {
                    logger.error("problem opening suffix array index on column {} for sstable {}", columnName, reader, e);
                }
            }
            // now add to indexed column-specific interval tree
            SADataTracker dataTracker = intervalTrees.get(columnDefinition.name);
            dataTracker.replace(Collections.EMPTY_LIST, intervals);
        }
    }

    /** derive the min/max fileOffsets in the index */
    private Pair<Integer, Integer> derivePositions(File indexFile, ColumnDefinition columnDefinition) throws IOException
    {
        int min = Integer.MAX_VALUE;
        int max = -1;

        try (OnDiskSA sa = new OnDiskSA(indexFile, columnDefinition.getValidator()))
        {
            Iterator<DataSuffix> suffixIterator = sa.iterator();
            while (suffixIterator.hasNext())
            {
                DataSuffix suffix = suffixIterator.next();
                IntIterator intIterator = suffix.getKeys().getIntIterator();
                // make the blind assumption that the bitmap is not empty, which is not unreasonable
                int i = intIterator.next();
                min = Math.min(min, i);
                max = Math.max(i, max);
                while (intIterator.hasNext())
                    max = Math.max(max, intIterator.next());
            }
        }
        assert min <= max : String.format("min found position %d is less than the max %d", min, max);
        if (min == Integer.MAX_VALUE || max == -1)
            return null;
        return Pair.create(min, max);
    }

    private ColumnDefinition getColumnDef(Component component)
    {
        //ugly hack to get validator via the columnDefs
        ByteBuffer colName = columnDefComponents.inverse().get(component);
        if (colName != null)
            return getColumnDefinition(colName);
        return null;
    }

    private void removeFromIntervalTree(Collection<SSTableReader> readers)
    {
        for (Map.Entry<ByteBuffer, SADataTracker> entry : intervalTrees.entrySet())
        {
            entry.getValue().replace(readers, Collections.EMPTY_LIST);
        }
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
        long size = 0;
        for (Map.Entry<ByteBuffer, SADataTracker> entry : intervalTrees.entrySet())
        {
            for (SSTableReader reader : entry.getValue().view.get().sstables)
            {
                // get file size for each SA file
                for (ColumnDefinition def : getColumnDefs())
                {
                    String columnName = baseCfs.getComparator().compose(def.name).toString();
                    String indexFile = reader.descriptor.filenameFor(String.format(FILE_NAME_FORMAT, columnName));
                    File f = new File(indexFile);
                    if (f.exists())
                        size += f.length();
                }
            }
        }
        return size;
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
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                //nop
            }
        };
        return new FutureTask<Object>(runnable, null);
    }

    public void delete(DecoratedKey key)
    {
        // called during 'nodetool cleanup' - can punt on impl'ing this
    }

    public void removeIndex(ByteBuffer columnName)
    {
        intervalTrees.remove(columnName);
    }

    public void invalidate()
    {
        // according to CFS.invalidate(), "call when dropping or renaming a CF"
        intervalTrees.clear();
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
        private final Map<ByteBuffer, Pair<Component, RoaringBitmap>> termBitmaps;

        private DecoratedKey curKey;
        private long curFilePosition;

        public LocalSSTableWriterListener(Descriptor descriptor)
        {
            this.descriptor = descriptor;
            termBitmaps = new ConcurrentHashMap<>();
        }

        public void begin()
        {
            //nop
        }

        public void startRow(DecoratedKey key, long curPosition)
        {
            this.curKey = key;
            this.curFilePosition = curPosition;
        }

        public void nextColumn(Column column)
        {
            ByteBuffer indexedCol = column.name();
            if (!columnDefComponents.keySet().contains(indexedCol))
                return;

            ByteBuffer term = column.value().slice();
            Pair<Component, RoaringBitmap> pair = termBitmaps.get(term);
            if (pair == null)
            {
                pair = Pair.create(columnDefComponents.get(indexedCol), new RoaringBitmap());
                termBitmaps.put(term, pair);
            }

            pair.right.add((int)curFilePosition);
        }

        public void complete()
        {
            curKey = null;
            try
            {
                // first, build up a listing per-component (per-index)
                Map<Component, OnDiskSABuilder> componentBuilders = new HashMap<>(columnDefComponents.size());
                for (Map.Entry<ByteBuffer, Pair<Component, RoaringBitmap>> entry : termBitmaps.entrySet())
                {
                    Component component = entry.getValue().left;
                    assert columnDefComponents.values().contains(component);

                    OnDiskSABuilder builder = componentBuilders.get(component);
                    if (builder == null)
                    {
                        AbstractType<?> validator = getColumnDef(component).getValidator();
                        OnDiskSABuilder.Mode mode = (validator instanceof AsciiType || validator instanceof UTF8Type) ?
                               OnDiskSABuilder.Mode.SUFFIX : OnDiskSABuilder.Mode.ORIGINAL;
                        builder = new OnDiskSABuilder(validator, mode);
                        componentBuilders.put(component, builder);
                    }

                    builder.add(entry.getKey(), entry.getValue().right);
                }
                termBitmaps.clear();

                // now do the writing
                logger.info("about to submit for concurrent SA'ing");
                List<Future<Boolean>> futures = new ArrayList<>(componentBuilders.size());
                for (Map.Entry<Component, OnDiskSABuilder> entry : componentBuilders.entrySet())
                {
                    String fileName = descriptor.filenameFor(entry.getKey());
                    logger.info("submitting {} for concurrent SA'ing", fileName);
                    futures.add(executor.submit(new BuilderFinisher(entry.getValue(), fileName)));
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
                termBitmaps.clear();
            }
        }

        public int compareTo(String o)
        {
            return descriptor.generation;
        }
    }

    private class BuilderFinisher implements Callable<Boolean>
    {
        private OnDiskSABuilder builder;
        private final String fileName;

        public BuilderFinisher(OnDiskSABuilder builder, String fileName)
        {
            this.builder = builder;
            this.fileName = fileName;
        }

        public Boolean call() throws Exception
        {
            try
            {
                long start = System.nanoTime();
                builder.finish(new File(fileName));
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
        private final Comparator<OnDiskAtomIterator> comparator = new Comparator<OnDiskAtomIterator>()
        {
            public int compare(OnDiskAtomIterator i1, OnDiskAtomIterator i2)
            {
                return i1.getKey().compareTo(i2.getKey());
            }
        };

        private final long executionTimeLimit;
        private final Phaser phaser;
        private int phaserPhase;
        private long startTime;

        protected LocalSecondaryIndexSearcher(SecondaryIndexManager indexManager, Set<ByteBuffer> columns)
        {
            super(indexManager, columns);
            executionTimeLimit = (long)(DatabaseDescriptor.getRangeRpcTimeout() -
                                        (DatabaseDescriptor.getRangeRpcTimeout() * .1));
            phaser = new Phaser();
        }

        public List<Row> search(ExtendedFilter filter)
        {
            try
            {
                startTime = System.currentTimeMillis();
                phaserPhase = phaser.register();
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

            final int maxRows = Math.min(MAX_ROWS, filter.maxRows());
            List<Row> rows = new ArrayList<>(maxRows);
            List<Future<Row>> loadingRows = new ArrayList<>(maxRows);
            List<Pair<ByteBuffer, Expression>> expressions = analyzeQuery(filter.getClause());
            List<Pair<ByteBuffer, Expression>> expressionsImmutable = ImmutableList.copyOf(expressions);

            // TODO:JEB - this is a Set we get from dataTracker, but we need to order them!!!!
            // TODO: (i think) we need to have a strategy about marking any sstables as referenced, but as we break early
            // on all the for loops, to eagerly determine the keys before loading, we might be ok without it.
            Set<SSTableReader> sstables = baseCfs.getDataTracker().getSSTables();
            final Set<DecoratedKey> evaluatedKeys = new TreeSet<>(DecoratedKey.comparator);

            Pair<ByteBuffer, Expression> eqPredicate = expressions.remove(0);
            try (SuffixIterator eqSuffixes = getSuffixIterator(sstables.iterator(), eqPredicate.left, eqPredicate.right))
            {
                if (eqSuffixes == null)
                    return Collections.emptyList();

                outermost:
                while (eqSuffixes.hasNext())
                {
                    Iterator<DecoratedKey> keys = eqSuffixes.next();

                    keys_loop:
                    while (keys.hasNext() && rows.size() < maxRows)
                    {
                        DecoratedKey key = keys.next();
                        if (!evaluatedKeys.add(key))
                            continue;

                        predicate_loop:
                        for (Pair<ByteBuffer, Expression> predicate : expressions)
                        {
                            Iterator<SSTableReader> candidates = intervalTrees.get(predicate.left).view.get().intervalTree.search(key).iterator();
                            try (SuffixIterator suffixIterator = getSuffixIterator(candidates, predicate.left, predicate.right))
                            {
                                while (suffixIterator.hasNext())
                                {
                                    Iterator<DecoratedKey> candidateKeys = suffixIterator.next();
                                    while (candidateKeys.hasNext())
                                    {
                                        int cmp = candidateKeys.next().compareTo(key);
                                        if (cmp == 0)
                                        {
                                            // we're good for this predicate (in any sstable), so move on to next predicate
                                            continue predicate_loop;
                                        }
                                        else if (cmp > 0)
                                        {
                                            // we're past the range of this suffix, so try the next suffix
                                            break;
                                        }
                                    }
                                }
                            }

                            // if we got here, there's no match for the current predicate, so skip this key
                            continue keys_loop;
                        }

                        // if we got here, all the predicates have been satisfied
                        loadingRows.add(submitRow(key.key, filter, expressionsImmutable));
                        // read exactly up until all results are found and break main loop
                        // otherwise we are going to read a lot more keys from the index then actually required.
                        if (rows.size() + loadingRows.size() >= maxRows)
                        {
                            // check up on loading rows to see who is loaded
                            harvest(rows, loadingRows, maxRows, -1);
                            if (rows.size() >= maxRows)
                                break outermost;
                        }
                    }
                }
            }

            // last call to here in case we didn't max out the row count above (and exited early)
            // if we did hit the max, harvest will clean up/close the rows to be loaded
            harvest(rows, loadingRows, maxRows, startTime);
            return rows;
        }

        private SuffixIterator getSuffixIterator(Iterator<SSTableReader> sstables, ByteBuffer indexedColumn, Expression predicate)
        {
            ColumnDefinition columnDef = getColumnDefinition(indexedColumn);
            if (columnDef == null)
                return null;
            return new SuffixIterator(sstables, indexedColumn, predicate, columnDef.getValidator());

        }

        private void harvest(List<Row> rows, List<Future<Row>> futures, int maxRows, long startTime)
        {
            if (rows.size() >= maxRows)
            {
                for (Iterator<Future<Row>> iter = futures.iterator(); iter.hasNext();)
                {
                    Future<Row> f = iter.next();
                    f.cancel(true);
                    iter.remove();
                }
                return;
            }

            // TODO:JEB there's some clean up work to do here
            boolean lastTime = startTime > 0;
            if (lastTime)
            {
                try
                {
                    long elapsed = System.currentTimeMillis() - startTime;
                    long remainingTime = executionTimeLimit - elapsed;
                    if (remainingTime > 0)
                    {
                        phaser.arrive();
                        phaserPhase = phaser.awaitAdvanceInterruptibly(phaserPhase, remainingTime, TimeUnit.MILLISECONDS);
                    }
                }
                catch (InterruptedException | TimeoutException e)
                {
                    //nop
                }
            }

            for (Iterator<Future<Row>> iter = futures.iterator(); iter.hasNext();)
            {
                Future<Row> f = iter.next();
                if (f.isDone() && !(rows.size() >= maxRows))
                {
                    Row r = Futures.getUnchecked(f);
                    if (r != null)
                        rows.add(r);
                    iter.remove();
                }
                else if (lastTime || rows.size() >= maxRows)
                {
                    f.cancel(true);
                    iter.remove();
                }
            }
        }

        private Future<Row> submitRow(ByteBuffer key, ExtendedFilter filter, List<Pair<ByteBuffer, Expression>> expressions)
        {
            ExecutorService readStage = StageManager.getStage(Stage.READ);
            ReadCommand cmd = ReadCommand.create(baseCfs.keyspace.getName(),
                                                 key,
                                                 baseCfs.getColumnFamilyName(),
                                                 System.currentTimeMillis(),
                                                 filter.columnFilter(key));

            return readStage.submit(new RowReader(cmd, expressions));
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

            public Row call() throws Exception
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

    private class SuffixIterator extends AbstractIterator<Iterator<DecoratedKey>> implements Closeable
    {
        private final Expression exp;
        private final Iterator<SSTableReader> sstables;
        private final AbstractType<?> validator;
        private final IteratorOrder order;
        private final ByteBuffer indexedColumn;
        private final String columnName;

        private OnDiskSA sa;
        private Iterator<DataSuffix> suffixes;
        private SSTableReader currentSSTable;

        public SuffixIterator(Iterator<SSTableReader> ssTables, ByteBuffer indexedColumn, Expression exp, AbstractType<?> validator)
        {
            this.indexedColumn = indexedColumn;
            this.sstables = ssTables;
            this.validator = validator;
            this.exp = exp;
            order = exp.lower == null ? IteratorOrder.ASC : IteratorOrder.DESC;

            ColumnDefinition columnDef = getColumnDefinition(indexedColumn);
            assert columnDef != null;
            columnName = (String) baseCfs.getComparator().compose(columnDef.name);
        }

        @Override
        protected Iterator<DecoratedKey> computeNext()
        {
            if (sa == null || !suffixes.hasNext())
            {
                if (!loadNext())
                    return endOfData();
            }

            DataSuffix suffix = suffixes.next();

            if (order == IteratorOrder.DESC && exp.upper != null)
            {
                ByteBuffer s = suffix.getSuffix();
                s.limit(s.position() + exp.upper.value.remaining());

                int cmp = validator.compare(s, exp.upper.value);
                if ((cmp > 0 && exp.upper.inclusive) || (cmp >= 0 && !exp.upper.inclusive))
                {
                    Iterators.advance(suffixes, Integer.MAX_VALUE);
                    return computeNext();
                }
            }

            return new KeyIterator(currentSSTable, suffix);
        }

        private boolean loadNext()
        {
            // close exhausted iterator and move to the new file
            FileUtils.closeQuietly(sa);

            while (sstables.hasNext())
            {
                SSTableReader ssTable = sstables.next();
                String indexFile = ssTable.descriptor.filenameFor(String.format(FILE_NAME_FORMAT, columnName));

                try
                {
                    File f = new File(indexFile);
                    if (f.exists())
                    {
                        sa = new OnDiskSA(f, validator);
                        currentSSTable = ssTable;
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
            }

            // if there are no more SSTables to check, clear out the currentSSTable and suffixes
            // but leave "sa" in tact so it can be closed by close() method, otherwise there is going to be FB leak.
            currentSSTable = null;
            suffixes = null;

            return false;
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(sa);
        }
    }

    private static class KeyIterator extends AbstractIterator<DecoratedKey>
    {
        private final SSTableReader sstable;
        private final IntIterator positions;

        public KeyIterator(SSTableReader sstable, DataSuffix suffix)
        {
            this.sstable = sstable;

            IntIterator positions = null;

            try
            {
                positions = suffix.getKeys().getIntIterator();
            }
            catch (IOException e)
            {
                logger.error("Failed to get positions from the one data suffixes.", e);
            }

            this.positions = positions;
        }

        @Override
        protected DecoratedKey computeNext()
        {
            if (positions == null)
                return endOfData();

            while (positions.hasNext())
            {
                DecoratedKey key;
                int position = positions.next();

                try
                {
                    if ((key = sstable.keyAt(position)) == null)
                        continue;

                    return key;
                }
                catch (IOException e)
                {
                    logger.warn("failed to read key at position {} from sstable {}; ignoring.", position, sstable, e);
                }
            }

            return endOfData();
        }
    }

    /** a pared-down version of DataTracker and DT.View. need one for each index of each column family */
    private class SADataTracker
    {
        // by using using DT.View, we do get some baggage fields (memtable, compacting, and so on)
        // but always pass in empty list for those fields, we should be ok
        private final AtomicReference<SAView> view = new AtomicReference<>();

        public SADataTracker()
        {
            view.set(new SAView(Collections.EMPTY_SET, SSTableIntervalTree.empty()));
        }

        public void replace(Collection<SSTableReader> oldSSTables, Collection<Interval<RowPosition, SSTableReader>> newReaders)
        {

            SAView currentView, newView;
            do
            {
                currentView = view.get();

                //TODO: handle this slightly more elegantly, but don't bother trying to remove tables from an empty tree
                if (currentView.sstables.size() == 0 && oldSSTables.size() > 0)
                    return;
                newView = currentView.replace(oldSSTables, newReaders);
                if (newView == null)
                    break;
            }
            while (!view.compareAndSet(currentView, newView));
        }
    }

    private class SAView
    {
        public final Set<SSTableReader> sstables;
        public final SSTableIntervalTree intervalTree;

        public SAView(Set<SSTableReader> sstables, SSTableIntervalTree intervalTree)
        {
            this.sstables = sstables;
            this.intervalTree = intervalTree;
        }

        public SAView replace(Collection<SSTableReader> oldSSTables, Collection<Interval<RowPosition, SSTableReader>> newReaders)
        {
            int newSSTablesSize = sstables.size() - oldSSTables.size() + Iterables.size(newReaders);
            assert newSSTablesSize >= Iterables.size(newReaders) :
                    String.format("Incoherent new size %d replacing %s by %s, sstable size %d", newSSTablesSize, oldSSTables, newReaders, sstables.size());

            Map<SSTableReader, Interval<RowPosition, SSTableReader>> intervals = new HashMap<>(newSSTablesSize);
            for (Interval<RowPosition, SSTableReader> i : intervalTree)
                if (!oldSSTables.contains(i.data))
                    intervals.put(i.data, i);

            for (Interval<RowPosition, SSTableReader> interval : newReaders)
                intervals.put(interval.data, interval);

            return new SAView(intervals.keySet(), new SSTableIntervalTree(intervals.values()));
        }
    }

    private class DataTrackerConsumer implements INotificationConsumer
    {
        public void handleNotification(INotification notification, Object sender)
        {
            // unfortunately, we can only check the type of notification via instaceof :(
            if (notification instanceof SSTableAddedNotification)
            {
                SSTableAddedNotification notif = (SSTableAddedNotification)notification;
                addToIntervalTree(Collections.singletonList(notif.added), getColumnDefs());
            }
            else if (notification instanceof SSTableListChangedNotification)
            {
                SSTableListChangedNotification notif = (SSTableListChangedNotification)notification;
                for (SSTableReader reader : notif.added)
                    addToIntervalTree(Collections.singletonList(reader), getColumnDefs());
                for (SSTableReader reader : notif.removed)
                    removeFromIntervalTree(Collections.singletonList(reader));
            }
            else if (notification instanceof SSTableDeletingNotification)
            {
                SSTableDeletingNotification notif = (SSTableDeletingNotification)notification;
                removeFromIntervalTree(Collections.singletonList(notif.deleting));
            }
        }
    }
}
