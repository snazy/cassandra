package org.apache.cassandra.db.index;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
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

/**
 * Note: currently does not work with cql3 tables (unless 'WITH COMPACT STORAGE' is declared when creating the table).
 *
 * ALos, makes the assumption this will be the only index running on the table as part of the query.
 * SIM tends to shoves all indexed columns into one PerRowSecondaryIndex
 */
public class SuffixArraySecondaryIndex extends PerRowSecondaryIndex implements SSTableWriterListenable
{
    protected static final Logger logger = LoggerFactory.getLogger(SuffixArraySecondaryIndex.class);

    /**
     * A sanity ceiling on the number of max rows we'll ever return for a query.
     */
    private static final int MAX_ROWS = 100000;

    //not sure i really need this, tbh
    private final Map<Integer, SSTableWriterListener> openListeners;

    private final Map<ByteBuffer, Component> columnDefComponents;

    public SuffixArraySecondaryIndex()
    {
        openListeners = new ConcurrentHashMap<>();
        columnDefComponents = new ConcurrentHashMap<>();
    }

    public void init()
    {
        logger.info("init'ing a SuffixArraySecondaryIndex");

        // while init() is called form SIM when the class is first created, and only one columnDef has been set,
        //  we'll loop here just for sanity sake ...
        for (ColumnDefinition col : columnDefs)
            addComponent(col);
    }

    private void addComponent(ColumnDefinition def)
    {
        if (getComparator() == null)
            return;

        String indexName = String.format("SecondaryIndex_%s.db", def.getIndexName());
        logger.info("adding a columnDef: {}, baseCfs = {}, component index name = {}", def, baseCfs, indexName);
        columnDefComponents.put(def.name, new Component(Component.Type.SECONDARY_INDEX, indexName));
    }

    void addColumnDef(ColumnDefinition columnDef)
    {
        super.addColumnDef(columnDef);
        addComponent(columnDef);
    }

    private AbstractType<?> getComparator()
    {
        return baseCfs != null ? baseCfs.getComparator() : null;
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
                        builder = new OnDiskSABuilder(getComparator(), OnDiskSABuilder.Mode.SUFFIX);
                        componentBuilders.put(component, builder);
                    }

                    builder.add(entry.getKey(), entry.getValue().right);
                }

                // now do the writing
                for (Map.Entry<Component, OnDiskSABuilder> entry : componentBuilders.entrySet())
                {
                    String fileName = null;
                    try
                    {
                        fileName = descriptor.filenameFor(entry.getKey());
                        entry.getValue().finish(new File(fileName));
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
                // drop this data asap
                termBitmaps.clear();
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
            Map<SSTableReader, Set<Pair<ByteBuffer, OnDiskSA>>> candidates = secondaryIndexHolder.getIndexes();

            Map<SSTableReader, Set<RoaringBitmap>> targets = new HashMap<>(candidates.size());
            for (IndexExpression exp : filter.getClause())
            {
                for (Map.Entry<SSTableReader, Set<Pair<ByteBuffer, OnDiskSA>>> entry : candidates.entrySet())
                {
                    // test if any entries for current index expression
                    for (Pair<ByteBuffer, OnDiskSA> pair : entry.getValue())
                    {
                        if (pair.left.equals(exp.bufferForColumn_name()))
                        {
                            Set<RoaringBitmap> bitmaps = getBitmaps(pair.right, exp);
                            if (bitmaps != null && !bitmaps.isEmpty())
                                targets.put(entry.getKey(), bitmaps);
                        }
                    }
                }
            }
            logger.info("found {} target sstables with indices", candidates.keySet().size());

            final int maxRows = (Math.min(filter.maxRows(), MAX_ROWS));
            return loadRows(targets, maxRows);
        }

        private Set<RoaringBitmap> getBitmaps(OnDiskSA sa, IndexExpression exp)
        {
            Set<RoaringBitmap> bitmaps = null;
            try
            {
                final IndexOperator op = exp.getOp();
                if (op == IndexOperator.EQ)
                {
                    RoaringBitmap bitmap = sa.search(exp.bufferForValue());
                    if (bitmap != null || !bitmap.isEmpty())
                    {
                        if (bitmaps == null)
                            bitmaps = new HashSet<>();
                        bitmaps.add(bitmap);
                    }
                }
                else
                {
                    OnDiskSA.IteratorOrder order = op == IndexOperator.GTE || op == IndexOperator.GT
                        ? OnDiskSA.IteratorOrder.ASC : OnDiskSA.IteratorOrder.DESC;
                    for (Iterator<OnDiskSA.DataSuffix> iter = sa.iteratorAt(exp.bufferForValue(), order); iter.hasNext(); )
                    {
                        if (bitmaps == null)
                            bitmaps = new HashSet<>();
                        bitmaps.add(iter.next().getKeys());
                        // TODO:JEB add some upper bound to the number of entries (via the bitmap) we add
                    }
                }
            }
            catch (IOException e)
            {
                logger.warn("failed to read index for bitmap");
            }
            return bitmaps;
        }

        private List<Row> loadRows(Map<SSTableReader, Set<RoaringBitmap>> targets, int maxRows)
        {
            ExecutorService readStage = StageManager.getStage(Stage.READ);
            int rowsSubmitted = 0;

            //TODO: what would be nice here is to know if we're loading the same row key
            // across sstables, that way we could use merge iterator of some sort instead of
            // reassembling by hand after we've read the rows from the individual sstbles.

            List<Future<Row>> futures = new ArrayList<>(targets.size());
            for (Map.Entry<SSTableReader, Set<RoaringBitmap>> entry : targets.entrySet())
            {
                // remove any possible row dupes from the multiple bitmaps
                Set<Integer> ints = new HashSet<>();
                outer: for (RoaringBitmap bitmap : entry.getValue())
                {
                    for (Integer i : bitmap)
                    {
                        ints.add(i);
                        if (rowsSubmitted + ints.size() >= maxRows)
                            break outer;
                    }
                }

                for (Integer i : ints)
                    futures.add(readStage.submit(new RowReader(entry.getKey(), i.longValue())));
                rowsSubmitted += ints.size();
                if (rowsSubmitted >= maxRows)
                    break;
            }

            // merge the resultant rows to together (if they cam from different sstables)
            Map<DecoratedKey, Row> rows = new HashMap<>();
            for (Future<Row> future : futures)
            {
                try
                {
                    Row result = future.get(DatabaseDescriptor.getRangeRpcTimeout(), TimeUnit.MILLISECONDS);
                    Row row = rows.get(result.key);
                    if (row == null)
                        rows.put(result.key, result);
                    else
                        row.cf.delete(result.cf);
                }
                catch (Exception e)
                {
                    logger.error("problem reading row", e);
                }
            }

            //this is lame, java, very lame
            return new ArrayList<>(rows.values());
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
