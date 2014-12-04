package org.apache.cassandra.db.index;

import java.io.Closeable;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
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
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;


import com.google.common.collect.*;
import com.google.common.util.concurrent.Futures;

import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.apache.cassandra.db.index.search.OnDiskSA.IteratorOrder;
import static org.apache.cassandra.db.index.search.OnDiskSA.DataSuffix;

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

    private final Map<ByteBuffer, Component> columnDefComponents;

    // TODO: maybe switch away from Guava table (and opening indices when sstable loads)
    // and use LoadingCache to load only when requested
    private final Table<SSTableReader, ByteBuffer, OnDiskSA> suffixArrays;
    private volatile boolean hasInited;

    public SuffixArraySecondaryIndex()
    {
        openListeners = new ConcurrentHashMap<>();
        columnDefComponents = new ConcurrentHashMap<>();
        suffixArrays = HashBasedTable.create();
    }

    public void init()
    {
        // init() is called by SIM only on the instance that it will keep around, but will call addColumnDef on any instance
        // that it happens to create (and subsequently/immediately throw away)
        hasInited = true;
        addComponent(columnDefs);
    }

    private void addComponent(Set<ColumnDefinition> defs)
    {
        if (baseCfs == null)
            return;
        //TODO:JEB not sure if this is the correct comparator
        AbstractType<?> type = baseCfs.getComparator();
        for (ColumnDefinition def : defs)
        {
            String indexName = String.format(FILE_NAME_FORMAT, type.getString(def.name));
            columnDefComponents.put(def.name, new Component(Component.Type.SECONDARY_INDEX, indexName));
        }

        if (!hasInited)
            return;

        executor.setCorePoolSize(columnDefs.size());

        for (SSTableReader reader : baseCfs.getDataTracker().getSSTables())
            add(reader, defs);
    }

    void addColumnDef(ColumnDefinition columnDef)
    {
        super.addColumnDef(columnDef);
        addComponent(Collections.singleton(columnDef));
    }

    public void add(SSTableReader reader, Collection<ColumnDefinition> toAdd)
    {
//        Descriptor desc = reader.descriptor;
//        for (Component component : reader.getComponents(Component.Type.SECONDARY_INDEX))
//        {
//            String fileName = desc.filenameFor(component);
//            OnDiskSA onDiskSA = null;
//            try
//            {
//                ColumnDefinition cDef = getColumnDef(component);
//                if (cDef == null || !toAdd.contains(cDef))
//                    continue;
//                //because of the truly whacked out way in which 2I instances get the column defs passed in vs. init,
//                // we have this insane check so we don't open the file numerous times .. <sigh>
//                onDiskSA = new OnDiskSA(new File(fileName), cDef.getValidator());
//            }
//            catch (IOException e)
//            {
//                logger.error("problem opening suffix array index {} for sstable {}", fileName, this, e);
//            }
//
//            int start = fileName.indexOf("_") + 1;
//            int end = fileName.lastIndexOf(".");
//            ByteBuffer name = ByteBufferUtil.bytes(fileName.substring(start, end));
//            suffixArrays.put(reader, name, onDiskSA);
//        }
    }

    private ColumnDefinition getColumnDef(Component component)
    {
        //TODO:JEB ugly hack to get validator via the columnDefs
        for (Map.Entry<ByteBuffer, Component> e : columnDefComponents.entrySet())
        {
            if (e.getValue().equals(component))
                return getColumnDefinition(e.getKey());
        }
        return null;
    }

    public void add(SSTableReader reader)
    {
        assert hasInited : "trying to add an sstable to this secondary index before it's been init'ed";
        add(reader, columnDefs);
    }

    public void remove(SSTableReader reader)
    {
        // this ConcurrentModificationException catching is because the suffixArrays as a guava table
        // returns only a HashMap, not anything more concurrency-safe
        while (true)
        {
            try
            {
                for (Map.Entry<ByteBuffer, OnDiskSA> entry : suffixArrays.row(reader).entrySet())
                {
                    suffixArrays.remove(reader, entry.getKey());
                    FileUtils.closeQuietly(entry.getValue());
                }
                break;
            }
            catch (ConcurrentModificationException cme)
            {}
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
        //TODO:JEB
        return 0;
    }

    public void reload()
    {
        // nop, i think
    }

    public void index(ByteBuffer rowKey, ColumnFamily cf)
    {
        //TODO:JEB this will index a whole row, or at least, what is passed in
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
        //TODO:JEB called during 'nodetool cleanup' - can punt on impl'ing this for now
    }

    public void removeIndex(ByteBuffer columnName)
    {
        // nop, this index will be automatically cleaned up as part of the sttable components
    }

    public void invalidate()
    {
        //TODO:JEB according to CFS.invalidate(), "call when dropping or renaming a CF" - so punting on impl'ing
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
        private final Phaser phaser;
        private final long executionTimeLimit;
        private int phaserPhase;

        protected LocalSecondaryIndexSearcher(SecondaryIndexManager indexManager, Set<ByteBuffer> columns)
        {
            super(indexManager, columns);
            phaser = new Phaser();
            executionTimeLimit = (long)(DatabaseDescriptor.getRangeRpcTimeout() -
                                 (DatabaseDescriptor.getRangeRpcTimeout() * .1));
        }

        public List<Row> search(ExtendedFilter filter)
        {
            try
            {
                phaserPhase = phaser.register();
                return performSearch(filter);
            }
            catch(Exception e)
            {
                logger.info("error occurred while searching suffix array indexes; ignoring", e);
                return Collections.EMPTY_LIST;
            }
            finally
            {
                phaser.forceTermination();
            }
        }

        protected List<Row> performSearch(ExtendedFilter filter)
        {
            final int maxRows = Math.min(MAX_ROWS, filter.maxRows());
            long now = System.currentTimeMillis();
            List<Row> rows = new ArrayList<>(maxRows);
            Map<ByteBuffer, Expression> expressions = analyzeQuery(filter.getClause());
            List<Future<Row>> loadingRows = new ArrayList<>(maxRows);

            search_per_sstable_loop:
            for (SSTableReader reader : baseCfs.getDataTracker().getSSTables())
            {
                List<SuffixIterator> iterators = getIterators(expressions, reader);
                if (iterators.isEmpty())
                    continue;

                try
                {
                    while (true)
                    {
                        int exhaustedCount = 0;

                        RoaringBitmap step = null;
                        for (Iterator<RoaringBitmap> positions : iterators)
                        {
                            RoaringBitmap expressionStep = new RoaringBitmap();
                            while (positions.hasNext()) {
                                expressionStep.or(positions.next());
                                if (expressionStep.getCardinality() >= maxRows)
                                    break;
                            }

                            if (step == null)
                                step = expressionStep;
                            else
                                step.and(expressionStep);

                            if (!positions.hasNext())
                                exhaustedCount++;
                        }

                        if (step == null)
                            continue;

                        IntIterator positions = step.getIntIterator();
                        while (positions.hasNext())
                        {
                            ByteBuffer key;
                            if ((key = readKeyFromIndex(reader, positions.next())) == null)
                                continue;

                            loadingRows.add(submitRow(key, filter, expressions));
                            // read exactly up until all results are found and break main loop
                            // otherwise we are going to read a lot more keys from the index then actually required.
                            if (rows.size() + loadingRows.size() >= maxRows)
                            {
                                // check up on loading rows to see who is loaded
                                harvest(rows, loadingRows, -1);
                                if (rows.size() >= maxRows)
                                    break search_per_sstable_loop;
                            }
                        }

                        if (exhaustedCount == iterators.size())
                            break;
                    }
                }
                finally
                {
                    for (SuffixIterator suffixes : iterators)
                        FileUtils.closeQuietly(suffixes);
                }
            }
            harvest(rows, loadingRows, now);
            return rows;
        }

        private List<SuffixIterator> getIterators(Map<ByteBuffer, Expression> expressions, SSTableReader reader)
        {
            List<SuffixIterator> iterators = new ArrayList<>();
            for (Map.Entry<ByteBuffer, Expression> exp : expressions.entrySet())
            {
                ColumnDefinition columnDef = getColumnDefinition(exp.getKey());
                if (columnDef == null)
                    continue;

                String columnName = (String) baseCfs.getComparator().compose(columnDef.name);
                String indexPath = reader.descriptor.filenameFor(String.format(FILE_NAME_FORMAT, columnName));

                SuffixIterator suffixes = getSuffixes(indexPath, exp.getValue(), columnDef.getValidator());

                // if there was a problem when fetching one of the indexes, we skip current SSTable,
                // as it's not going to produce any AND results.
                if (suffixes == null)
                {
                    for (SuffixIterator s : iterators)
                        FileUtils.closeQuietly(s);
                    return Collections.EMPTY_LIST;
                }

                iterators.add(suffixes);
            }
            return iterators;
        }

        private SuffixIterator getSuffixes(final String indexFile, final Expression exp, final AbstractType<?> validator)
        {
            try
            {
                return new SuffixIterator(indexFile, exp, validator);
            }
            catch (IOException e)
            {
                logger.error("Failed to read SA index file: " + indexFile + ", skipping.", e);
                return null;
            }
        }

        private ByteBuffer readKeyFromIndex(SSTableReader reader, int keyPosition)
        {
            try
            {
                return reader.keyAt(keyPosition);
            }
            catch (IOException ioe)
            {
                logger.warn("failed to read key at position {} from sstable {}; ignoring.", keyPosition, reader, ioe);
            }

            return null;
        }

        private void harvest(List<Row> rows, List<Future<Row>> futures, long startTime)
        {
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
                        phaser.awaitAdvanceInterruptibly(phaserPhase, remainingTime, TimeUnit.MILLISECONDS);
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
                if (f.isDone())
                {
                    Row r = Futures.getUnchecked(f);
                    if (r != null)
                        rows.add(r);
                    iter.remove();
                }
                else if (lastTime)
                {
                    f.cancel(true);
                    iter.remove();
                }
            }
        }

        private Future<Row> submitRow(ByteBuffer key, ExtendedFilter filter, Map<ByteBuffer, Expression> expressions)
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
            private final Map<ByteBuffer, Expression> expressions;

            public RowReader(ReadCommand command, Map<ByteBuffer, Expression> expressions)
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
                for (Map.Entry<ByteBuffer, Expression> entry : expressions.entrySet())
                {
                    Column col = row.cf.getColumn(entry.getKey());
                    if (col == null || !col.isLive(now) || !entry.getValue().contains(col.value()))
                        return false;
                }
                return true;
            }
        }

        public boolean isIndexing(List<IndexExpression> clause)
        {
            //TODO:JEB this is a bit weak, currently just checks for the success of one column, not all
            // however, parent SIS.isIndexing only cares if one predicate is covered ... grrrr!!
            for (IndexExpression expression : clause)
            {
                if (columnDefComponents.keySet().contains(expression.column_name))
                    return true;
            }
            return false;
        }

        private Map<ByteBuffer, Expression> analyzeQuery(List<IndexExpression> expressions)
        {
            Map<ByteBuffer, Expression> groups = new HashMap<>();
            for (IndexExpression e : expressions)
            {
                ByteBuffer name = e.bufferForColumn_name();

                Expression exp = groups.get(name);
                if (exp == null)
                {
                    ColumnDefinition columnDef = getColumnDefinition(name);
                    if (columnDef == null)
                        continue;

                    groups.put(name, (exp = new Expression(columnDef.getValidator())));
                }

                exp.add(e.op, e.bufferForValue());
            }

            return groups;
        }
    }

    private static class Expression
    {
        private final AbstractType<?> validator;
        private Bound lower, upper;

        private Expression(AbstractType<?> validator)
        {
            this.validator = validator;
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
                if (!ByteBufferUtil.contains(lower.value, value))
                    return false;

                // range - mainly for numeric values
                int cmp = validator.compare(lower.value, value);
                if ((cmp > 0 && !lower.inclusive) || (cmp <= 0 && lower.inclusive))
                    return false;
            }

            if (upper != null)
            {
                // suffix check
                if (!ByteBufferUtil.contains(upper.value, value))
                    return false;

                // range - mainly for numeric values
                int cmp = validator.compare(upper.value, value);
                if ((cmp < 0 && !upper.inclusive) || (cmp <= 0 && upper.inclusive))
                    return false;
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

    private static class SuffixIterator extends AbstractIterator<RoaringBitmap> implements Closeable
    {
        private final OnDiskSA sa;
        private final String indexFile;
        private final Expression exp;
        private final AbstractType<?> validator;
        private final Iterator<DataSuffix> suffixes;
        private final IteratorOrder order;

        public SuffixIterator(String indexFile, Expression exp, AbstractType<?> validator) throws IOException
        {
            this.indexFile = indexFile;
            this.validator = validator;
            this.exp = exp;
            this.sa = new OnDiskSA(new File(indexFile), validator);
            this.suffixes = (exp.lower == null) // in case query is col <(=) x
                                    ? sa.iteratorAt(exp.upper.value, (order = IteratorOrder.ASC), exp.upper.inclusive)
                                    : sa.iteratorAt(exp.lower.value, (order = IteratorOrder.DESC), exp.lower.inclusive);

        }

        @Override
        protected RoaringBitmap computeNext()
        {
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

            try
            {
                return suffix.getKeys();
            }
            catch (IOException e)
            {
                logger.error("Failed to get positions from the one data suffixes (" + indexFile + ").", e);
                return endOfData();
            }
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(sa);
        }
    }
}
