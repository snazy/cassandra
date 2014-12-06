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
import java.util.ConcurrentModificationException;
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

import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Table;
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
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
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
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.ByteBufferUtil;
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

        protected List<Row> performSearch(ExtendedFilter filter) throws IOException
        {
            System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
            System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^  NEXT SEARCH ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
            System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
            final int maxRows = Math.min(MAX_ROWS, filter.maxRows());
            long now = System.currentTimeMillis();
            List<Row> rows = new ArrayList<>(maxRows);
            List<Future<Row>> loadingRows = new ArrayList<>(maxRows);
            List<Pair<ByteBuffer, Expression>> expressions = analyzeQuery(filter.getClause());
            if (expressions.isEmpty())
            {
                // how the fuck did this happen? either way, bail immediately to conserve any trivial number of CPU cycles from this disaster of a query....
                return Collections.EMPTY_LIST;
            }

            // take first predicate, get (top n) list of matches, and iterate
            //  - get and decorate the key, get list of candidate sstables from IntervalTree, and check the BF for the key, add to matches list
            //  - iterate through remaining predicates
            //      - merge iterate through the matching sstables' indexes for predicate to find any matches on *key* from first predicate match (thus, dereference the candidate's file position to a key)
            //      -- can break out of merge iterator if any predicate is satisfied by any sstable
            //      -- can break out of a given sstable iterator (via endOfData()) if the key's token is greater than predicate value (because row key's tokens are ordered)
            //  - if key matches all predicates, load row and do rest of work...

            // TODO:JEB - this is a Set we get from dataTracker, but we need to order them!!!!
            // also, we might need to mark sstables as referenced at the higher level here
            Set<SSTableReader> sstables = baseCfs.getDataTracker().getSSTables();

            final Set<DecoratedKey> eliminatedKeys = new TreeSet<>(DecoratedKey.comparator);

            Pair<ByteBuffer, Expression> firstPredicate = expressions.remove(0);
            SuffixIterator si = getSuffixIterator(sstables.iterator(), firstPredicate.left, firstPredicate.right);
            if (si == null)
                return Collections.EMPTY_LIST;
            outermost: while (si.hasNext())
            {
                // load next maxRows predicate matches (keys)
                RoaringBitmap predicateMatches = si.next();
                final Set<DecoratedKey> localKeys = new TreeSet<>(DecoratedKey.comparator);
                IntIterator positions = predicateMatches.getIntIterator();
                while (positions.hasNext() && rows.size() < maxRows)
                {
                    DecoratedKey key;
                    if ((key = readKeyFromIndex(si.getCurrentReader(), positions.next())) == null)
                        continue;

                    if (eliminatedKeys.add(key))
                        localKeys.add(key);
                }

                System.out.println("localKeys size: " + localKeys.size());
                keys_loop: for (DecoratedKey decoratedKey : localKeys)
                {
                    System.out.println("\t&&&&&&&&&&&&&&&&&&&& search for matching predicates for key: " + baseCfs.getComparator().compose(decoratedKey.key) + " &&&&&&&&&&&&&&&&&&&&&&&&&&");
                    ColumnFamilyStore.ViewFragment view = baseCfs.markReferenced(decoratedKey);
                    try
                    {
                        //TODO: add in optimization to first look at the sstable from which the row key came
                        // i think there's a good probability other matching columns may be in the same sstable

                        predicate_loop: for (Pair<ByteBuffer, Expression> predicate : expressions)
                        {
                            System.out.println("\tnext predicate for key: " + baseCfs.getComparator().compose(predicate.left));
                            Iterator<SSTableReader> candidates = Iterators.filter(view.sstables.iterator(), new BloomFilterPredicate(decoratedKey.key));
                            System.out.println("candidates size = " + Iterators.size(Iterators.filter(view.sstables.iterator(), new BloomFilterPredicate(decoratedKey.key)))
                                                    + ", view size = " + view.sstables.size());
                            SuffixIterator suffixIterator = getSuffixIterator(candidates, predicate.left, predicate.right);

                            suffix_loop: while (suffixIterator.hasNext())
                            {
                                RoaringBitmap bitmap = suffixIterator.next();
                                IntIterator intIterator = bitmap.getIntIterator();
//                                while (intIterator.hasNext())
//                                {
//                                    int position = intIterator.next();
//                                    DecoratedKey dkey = suffixIterator.curSstable.keyAt(position);
//                                    System.out.println("\there's a predicate for key: " + baseCfs.getComparator().compose(dkey.key) + "/" + baseCfs.getComparator().compose(predicate.left)
//                                                            + ", from sstable " + suffixIterator.curSstable);
//                                    if (dkey.equals(decoratedKey))
//                                    {
//                                        // we're good for this predicate (in any sstable), so move on to next predicate
//                                        System.out.println("\tfound a matching predicate for key: " + baseCfs.getComparator().compose(decoratedKey.key) + "/" + baseCfs.getComparator().compose(predicate.left));
//                                        continue predicate_loop;
//                                    }
////                                    if (dkey.compareTo(decoratedKey) > 0)
////                                    {
////                                        // we're past the range of this suffix, so try the next suffix
////                                        continue suffix_loop;
////                                    }
//                                }
                            }
                            // if we got here, there's no match for the current predicate, so skip this key
                            System.out.println("\t\tdid not find a matching predicate for key: " + baseCfs.getComparator().compose(decoratedKey.key) + "/" + baseCfs.getComparator().compose(predicate.left));
                            continue keys_loop;
                        }
                        // if we got here, all the predicates have been satisfied
                        System.out.println("\t\tloading key: " + baseCfs.getComparator().compose(decoratedKey.key));
                        loadingRows.add(submitRow(decoratedKey.key, filter, analyzeQuery(filter.getClause())));
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
                    finally
                    {
                        SSTableReader.releaseReferences(view.sstables);
                    }
                }
            }

            // last call to here in case we didn't max out the row count above (and exited early)
            // if we did hit the max, harvest will clean up/close the rows to be loaded
            harvest(rows, loadingRows, maxRows, now);
            return rows;
        }

        private class BloomFilterPredicate implements Predicate<SSTableReader>
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

        private SuffixIterator getSuffixIterator(Iterator<SSTableReader> sstables, ByteBuffer indexedColumn, Expression predicate)
        {
            ColumnDefinition columnDef = getColumnDefinition(indexedColumn);
            if (columnDef == null)
                return null;
            return new SuffixIterator(sstables, indexedColumn, predicate, columnDef.getValidator());

        }
        private DecoratedKey readKeyFromIndex(SSTableReader reader, int keyPosition)
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
            //TODO:JEB this is a bit weak, currently just checks for the success of one column, not all
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
            // differentiate between equality and inequlaity expressions while analyizing so we can later put the equality
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
                    //TODO:JEB actually get the validator to pass in
                    expList.add(Pair.create(name, exp = new Expression(null)));

                exp.add(e.op, e.bufferForValue());
            }

            equalityExpressions.addAll(inequalityExpressions);
            return equalityExpressions;
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
            //TODO:JEB fix the comparison checks!!!
            if (lower != null)
            {
                // suffix check
                if (!ByteBufferUtil.contains(lower.value, value))
                    return false;

                // range - mainly for numeric values
//                int cmp = validator.compare(lower.value, value);
//                if ((cmp > 0 && !lower.inclusive) || (cmp >= 0 && lower.inclusive))
//                    return false;
            }

            if (upper != null)
            {
                // suffix check
                if (!ByteBufferUtil.contains(upper.value, value))
                    return false;

                // range - mainly for numeric values
//                int cmp = validator.compare(upper.value, value);
//                if ((cmp < 0 && !upper.inclusive) || (cmp <= 0 && upper.inclusive))
//                    return false;
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

    private class SuffixIterator extends AbstractIterator<RoaringBitmap> implements Closeable
    {
        private final Expression exp;
        private final Iterator<SSTableReader> sstables;
        private final AbstractType<?> validator;
        private final IteratorOrder order;
        private final ByteBuffer indexedColumn;
        private final String columnName;

        private OnDiskSA sa;
        private Iterator<DataSuffix> suffixes;
        private SSTableReader curSstable;

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
        protected RoaringBitmap computeNext()
        {
            if (sa == null || !suffixes.hasNext())
            {
                loadNext();
                if (sa == null)
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

            try
            {
                return suffix.getKeys();
            }
            catch (IOException e)
            {
                logger.error("Failed to get positions from the one data suffixes", e);
                return endOfData();
            }
        }

        private void loadNext()
        {
            FileUtils.closeQuietly(sa);
            while (sstables.hasNext())
            {
                SSTableReader ssTable = sstables.next();
                System.out.println("** loadNext() sstable = " + ssTable + "/" + columnName);
                String indexFile = ssTable.descriptor.filenameFor(String.format(FILE_NAME_FORMAT, columnName));
                try
                {
                    File f = new File(indexFile);
                    if (f.exists())
                    {
                        sa = new OnDiskSA(f, validator);
                        curSstable = ssTable;
                        suffixes = (exp.lower == null) // in case query is col <(=) x
                                   ? sa.iteratorAt(exp.upper.value, order, exp.upper.inclusive)
                                   : sa.iteratorAt(exp.lower.value, order, exp.lower.inclusive);
                        return;
                    }
                }
                catch (IOException e)
                {
                    logger.info("failed to open suffix array file {}, skipping", indexFile, e);
                }
            }
            // if no more sstables, clear out the sa field
            sa = null;
            curSstable = null;
            suffixes = null;
        }

        public SSTableReader getCurrentReader()
        {
            return curSstable;
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(sa);
        }
    }
}
