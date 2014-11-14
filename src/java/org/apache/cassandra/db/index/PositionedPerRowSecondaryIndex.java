package org.apache.cassandra.db.index;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EmptyColumns;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.index.search.OnDiskSA;
import org.apache.cassandra.db.index.search.OnDiskSABuilder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriterListenable;
import org.apache.cassandra.io.sstable.SSTableWriterListener;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.roaringbitmap.RoaringBitmap;

public class PositionedPerRowSecondaryIndex extends PerRowSecondaryIndex implements SSTableWriterListenable
{
    protected static final Logger logger = LoggerFactory.getLogger(PositionedPerRowSecondaryIndex.class);

    //not sure i really need this, tbh
    private final Map<Integer, SSTableWriterListener> openListeners;

    private String indexName;
    private final List<Component> components;

    private final Set<ByteBuffer> columnDefNames;

    public PositionedPerRowSecondaryIndex()
    {
        openListeners = new ConcurrentHashMap<>();
        components = new ArrayList<>();
        columnDefNames = new HashSet<>();
    }

    public void init()
    {
        logger.info("init'ing a PositionedPerRowSecondaryIndex");
        indexName = "RowLevel_idx.db";
        components.add(new Component(Component.Type.SECONDARY_INDEX, indexName));

        // while init() is called form SIM when the class is first created, and only one columnDef has been set,
        //  we'll loop here just for sanity sake ...
        for (ColumnDefinition col : columnDefs)
            columnDefNames.add(col.name);
    }

    private AbstractType<?> getComparator()
    {
        return baseCfs != null ? baseCfs.getComparator() : null;
    }

    void addColumnDef(ColumnDefinition columnDef)
    {
        super.addColumnDef(columnDef);
        AbstractType<?> type = getComparator();
        logger.info("adding a columnDef: {}, baseCfs = {}", columnDef, baseCfs);
        if (type != null)
            columnDefNames.add(columnDef.name);
    }

    public void validateOptions() throws ConfigurationException
    {
        // nop ??
    }

    public String getIndexName()
    {
        return indexName;
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
        logger.info("received an index() call");
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
        return ImmutableList.<Component>builder().addAll(components).build();
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

        // need one builders for each column (that is, column name) we index
        private final Map<ByteBuffer, Pair<OnDiskSABuilder, RoaringBitmap>> builders;

        private DecoratedKey curKey;
        private long curFilePosition;

        public LocalSSTableWriterListener(Descriptor descriptor)
        {
            this.descriptor = descriptor;
            builders = new ConcurrentHashMap<>();
        }

        public void begin()
        {
            logger.info("received listener.begin() call");
            for (ByteBuffer name : columnDefNames)
                builders.put(name, null);
        }

        public void startRow(DecoratedKey key, long curPosition)
        {
            this.curKey = key;
            this.curFilePosition = curPosition;
        }

        public void nextColumn(Column column)
        {
            if (!builders.containsKey(column.name()))
                return;

            Pair<OnDiskSABuilder, RoaringBitmap> pair = builders.get(column.name());
            if (pair == null)
            {
                pair = Pair.create(new OnDiskSABuilder(getComparator(), OnDiskSABuilder.Mode.SUFFIX),
                                   new RoaringBitmap());
                builders.put(column.name(), pair);
            }

            try
            {
                pair.right.add((int)curFilePosition);
                pair.left.add(column.value(), pair.right);
            }
            catch (IOException e)
            {
                logger.error("failed to add add column to secondary index: {}", column, e);
            }
        }

        public void complete()
        {
            logger.info("received listener.complete() call");
            try
            {
                for (Map.Entry<ByteBuffer, Pair<OnDiskSABuilder, RoaringBitmap>> entry : builders.entrySet())
                {
                    String fileName = null;

                    try
                    {
                        fileName = descriptor.filenameFor(ByteBufferUtil.string(entry.getKey()));
                        entry.getValue().left.finish(new File(fileName));
                    }
                    catch (Exception e)
                    {
                        logger.error("failed to write output file {}", fileName);
                        throw new IOError(e);
                    }
                }
            }
            finally
            {
                openListeners.remove(descriptor.generation);
            }
        }

        public int compareTo(String o)
        {
            return descriptor.generation;
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
            logger.info("received a search() call");
            IndexExpression indexExpression = highestSelectivityPredicate(filter.getClause());

            Map<SSTableReader, Set<OnDiskSA>> candidates = secondaryIndexHolder.getIndexes();
            logger.info("found {} candidate sstables with indices", candidates.keySet().size());
            Map<SSTableReader, Set<RoaringBitmap>> targets = getTargets(candidates, indexExpression.bufferForValue(), filter.maxRows());
            logger.info("found {} target sstables with indices", candidates.keySet().size());

            return loadRows(targets, filter.maxRows());
        }

        protected IndexExpression highestSelectivityPredicate(List<IndexExpression> clause)
        {
            List<IndexExpression> candidates = new ArrayList<>(clause.size());
            for (IndexExpression expression : clause)
            {
                //skip columns belonging to a different index type
                if(expression.op != IndexOperator.EQ || !columns.contains(expression.column_name))
                    continue;

                SecondaryIndex index = indexManager.getIndexForColumn(expression.column_name);
                if (index == null)
                    continue;

                candidates.add(expression);
            }

            //TODO:JEB need better selection process here rather than just the first entry...
            return candidates.isEmpty() ? null : candidates.get(0);
        }

        private Map<SSTableReader, Set<RoaringBitmap>> getTargets(Map<SSTableReader, Set<OnDiskSA>> candidates, ByteBuffer val, int maxRows)
        {
            int curRows = 0;
            Map<SSTableReader, Set<RoaringBitmap>> targets = new HashMap<>();
            for (Map.Entry<SSTableReader, Set<OnDiskSA>> entry : candidates.entrySet())
            {
                boolean inTargets = false;
                for (OnDiskSA sa : entry.getValue())
                {
                    try
                    {
                        RoaringBitmap bitmap = sa.search(val);
                        if (bitmap == null || bitmap.isEmpty())
                            continue;

                        if (!inTargets)
                        {
                            Set<RoaringBitmap> s = new HashSet<>();
                            s.add(bitmap);
                            targets.put(entry.getKey(), s);
                            inTargets = true;
                        }
                        else
                        {
                            targets.get(entry.getKey()).add(bitmap);
                        }

                        // TODO:JEB this is a bit unfortunate, that we have to iterate through the bitmap just to get it's size
                        curRows += Iterables.size(bitmap);
                        if (curRows > maxRows)
                            break;
                    }
                    catch (IOException e)
                    {
                        logger.warn("failed to read index for bitmap {}", entry.getKey());
                    }
                }
            }
            return targets;
        }

        private List<Row> loadRows(Map<SSTableReader, Set<RoaringBitmap>> targets, int maxRows)
        {
            ExecutorService readStage = StageManager.getStage(Stage.READ);
            int cur = 0;

            List<Future<Row>> futures = new ArrayList<>(targets.size());
            outer: for (Map.Entry<SSTableReader, Set<RoaringBitmap>> entry : targets.entrySet())
            {
                for (RoaringBitmap bitmap : entry.getValue())
                {
                    for (Integer i : bitmap)
                    {
                        futures.add(readStage.submit(new RowReader(entry.getKey(), i.longValue())));
                        cur++;
                        if (cur >= maxRows)
                            break outer;
                    }
                }
            }

            List<Row> rows = new ArrayList<>();
            for (Future<Row> future : futures)
            {
                try
                {
                    rows.add(future.get(10, TimeUnit.SECONDS));
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
            private final SSTableReader sstable;
            private final long position;

            public RowReader(SSTableReader sstable, long position)
            {
                this.sstable = sstable;
                this.position = position;
            }

            public Row call() throws Exception
            {
                RandomAccessReader in = sstable.openDataReader();
                in.seek(position);
                DecoratedKey key = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in));

                ColumnFamily columnFamily = EmptyColumns.factory.create(sstable.metadata);
                columnFamily.delete(DeletionTime.serializer.deserialize(in));
                int columnCount = sstable.descriptor.version.hasRowSizeAndColumnCount ? in.readInt() : Integer.MAX_VALUE;
                Iterator<OnDiskAtom> atomIterator = columnFamily.metadata().getOnDiskIterator(in, columnCount, ColumnSerializer.Flag.LOCAL,
                                                                                              (int)(System.currentTimeMillis() / 1000),
                                                                                              sstable.descriptor.version);

                ColumnFamily cf = columnFamily.cloneMeShallow(ArrayBackedSortedColumns.factory, false);
                while (atomIterator.hasNext())
                    cf.addAtom(atomIterator.next());
                return new Row(key, cf);
            }
        }

        public boolean isIndexing(List<IndexExpression> clause)
        {
            for (IndexExpression expression : clause)
            {
                if (columnDefNames.contains(expression.column_name))
                    return true;
            }
            return false;
        }

    }
}
