package org.apache.cassandra.db.index;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.filter.ExtendedFilter;
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
            Map<SSTableReader, Set<RandomAccessReader>> candidates = secondaryIndexHolder.getIndexes();
            logger.info("found {} candidate sstables with indices", candidates.keySet().size());

            //TODO:JEB plug into pavel's code *here*, get some positions back, and load the rows

            return Collections.EMPTY_LIST;
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

    public SSTableWriterListener getListener(Descriptor descriptor)
    {
        LocalSSTableWriterListener listener = new LocalSSTableWriterListener(descriptor);
        openListeners.put(descriptor.generation, listener);
        return listener;
    }

    protected class LocalSSTableWriterListener implements SSTableWriterListener
    {
        private final Descriptor descriptor;

        private final String fileName;

        // need one builders for each column (that is, column name) we index
        private final Map<ByteBuffer, Pair<OnDiskSABuilder, RoaringBitmap>> builders;

        private DecoratedKey curKey;
        private long curFilePosition;

        public LocalSSTableWriterListener(Descriptor descriptor)
        {
            this.descriptor = descriptor;
            fileName = descriptor.filenameFor(getIndexComponents().iterator().next());
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
            DataOutputStream outputFile;
            try
            {
                outputFile = new DataOutputStream(new FileOutputStream(fileName));

                outputFile.flush();
                outputFile.close();
            }
            catch (Exception e)
            {
                logger.error("failed to write output file {}", fileName);
                throw new IOError(e);
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
}
