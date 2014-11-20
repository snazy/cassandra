package org.apache.cassandra.db.index;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;
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
import org.apache.cassandra.db.DataTracker;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
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
import org.roaringbitmap.RoaringBitmap;

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
        Descriptor desc = reader.descriptor;
        for (Component component : reader.getComponents(Component.Type.SECONDARY_INDEX))
        {
            String fileName = desc.filenameFor(component);
            OnDiskSA onDiskSA = null;
            try
            {
                ColumnDefinition cDef = getColumnDef(component);
                if (cDef == null || !toAdd.contains(cDef))
                    continue;
                //because of the truly whacked out way in which 2I instances get the column defs passed in vs. init,
                // we have this insane check so we don't open the file numerous times .. <sigh>

                onDiskSA = new OnDiskSA(new File(fileName), cDef.getValidator());
            }
            catch (IOException e)
            {
                logger.error("problem opening suffix array index {} for sstable {}", fileName, this, e);
            }

            int start = fileName.indexOf("_") + 1;
            int end = fileName.lastIndexOf(".");
            ByteBuffer name = ByteBufferUtil.bytes(fileName.substring(start, end));
            suffixArrays.put(reader, name, onDiskSA);
        }
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
            {
                continue;
            }
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
        logger.debug("received an index() call");
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
                        f.get(60, TimeUnit.MINUTES);
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
        private final OnDiskSABuilder builder;
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
                logger.info("starting SA for {}", fileName);
                builder.finish(new File(fileName));
                logger.info("finishing SA for {}", fileName);
                return Boolean.TRUE;
            }
            catch (Exception e)
            {
                throw new Exception(String.format("failed to write output file %s", fileName), e);
            }
        }
    }

    protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns)
    {
        return new LocalSecondaryIndexSearcher(baseCfs.indexManager, columns);
    }

    protected class LocalSecondaryIndexSearcher extends SecondaryIndexSearcher
    {
        protected LocalSecondaryIndexSearcher(SecondaryIndexManager indexManager, Set<ByteBuffer> columns)
        {
            super(indexManager, columns);
        }

        public List<Row> search(ExtendedFilter filter)
        {
            DataTracker tracker = baseCfs.getDataTracker();

            //TODO punt on memtables for now....

            int maxKeys = Math.min(MAX_ROWS, filter.maxRows());
            Set<ByteBuffer> keys = new TreeSet<>();
            for (SSTableReader reader : tracker.getSSTables())
            {
                if (keys.size() >= maxKeys)
                    break;

                Map<ByteBuffer, OnDiskSA> indices = suffixArrays.row(reader);
                if (indices.isEmpty())
                    continue;

                RoaringBitmap bitmap = new RoaringBitmap();
                List<IndexExpression> l = filter.getClause();
                for (int i = 0; i < l.size(); i++)
                {
                    IndexExpression exp = l.get(i);
                    OnDiskSA sa = indices.get(exp.bufferForColumn_name());
                    if (sa == null)
                        continue;

                    RoaringBitmap matches = search(sa, exp);
                    // this is intended ONLY while we do not support OR between predicates
                    if (matches == null || matches.isEmpty())
                    {
                        bitmap = null;
                        break;
                    }

                    if (i == 0)
                        bitmap.or(matches);
                    else
                        bitmap.and(matches);
                }

                if (bitmap != null)
                    keys.addAll(readKeys(reader, bitmap));
            }

            return loadRows(keys, maxKeys);
        }

        private RoaringBitmap search(OnDiskSA sa, IndexExpression exp)
        {
            try
            {
                final IndexOperator op = exp.getOp();
                if (op == IndexOperator.EQ)
                {
                    return sa.search(exp.bufferForValue());
                }
                else
                {
                    RoaringBitmap bitmap = new RoaringBitmap();
                    OnDiskSA.IteratorOrder order = op == IndexOperator.GTE || op == IndexOperator.GT
                        ? OnDiskSA.IteratorOrder.DESC : OnDiskSA.IteratorOrder.ASC;
                    boolean includeKey = op == IndexOperator.GTE || op == IndexOperator.LTE;
                    for (Iterator<OnDiskSA.DataSuffix> iter = sa.iteratorAt(exp.bufferForValue(), order, includeKey); iter.hasNext(); )
                    {
                        RoaringBitmap bm = iter.next().getKeys();
                        bitmap.or(bm);
                    }
                    return bitmap;
                }
            }
            catch (IOException e)
            {
                logger.warn("failed to read index for bitmap");
                return null;
            }
        }

        private Set<ByteBuffer> readKeys(SSTableReader reader, RoaringBitmap bitmap)
        {
            Set<ByteBuffer> keys = new TreeSet<>();
            for (Integer i : bitmap)
            {
                try
                {
                    DecoratedKey key = reader.keyAt(i);
                    if (key != null)
                        keys.add(key.key);
                }
                catch (IOException ioe)
                {
                    logger.warn("failed to read key at position {} from sstable {}; ignoring.", i, reader, ioe);
                }
            }
            return keys;
        }

        private List<Row> loadRows(Set<ByteBuffer> keys, int maxRows)
        {
            ExecutorService readStage = StageManager.getStage(Stage.READ);
            final long timestamp = System.currentTimeMillis();

            List<Future<Row>> futures = new ArrayList<>(keys.size());
            int cur = 0;
            for (ByteBuffer key : keys)
            {
                ReadCommand cmd = ReadCommand.create(baseCfs.keyspace.getName(), key, baseCfs.getColumnFamilyName(), timestamp, new IdentityQueryFilter());
                futures.add(readStage.submit(new RowReader(cmd)));
                cur++;
                if (cur == maxRows)
                    break;
            }

            List<Row> rows = new ArrayList<>(cur);
            for (Future<Row> future : futures)
            {
                try
                {
                    rows.add(future.get(DatabaseDescriptor.getRangeRpcTimeout(), TimeUnit.MILLISECONDS));
                }
                catch (Exception e)
                {
                    logger.error("problem reading row", e);
                }
            }
            return rows;
        }

        private class RowReader implements Callable<Row>
        {
            private final ReadCommand command;

            public RowReader(ReadCommand command)
            {
                this.command = command;
            }

            public Row call() throws Exception
            {
                return command.getRow(Keyspace.open(command.ksName));
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
    }
}
