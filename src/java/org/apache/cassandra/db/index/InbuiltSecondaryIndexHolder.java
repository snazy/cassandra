package org.apache.cassandra.db.index;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.index.search.OnDiskSA;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;

public class InbuiltSecondaryIndexHolder
{
    private static final Logger logger = LoggerFactory.getLogger(InbuiltSecondaryIndexHolder.class);
    private final ColumnFamilyStore baseCfs;

    // TODO:JEB can I get a COW map??
    private Map<SSTableReader, Set<Pair<ByteBuffer, OnDiskSA>>> inbuiltSecondaryIndexes = new ConcurrentHashMap<>();

    public InbuiltSecondaryIndexHolder(ColumnFamilyStore baseCfs)
    {
        this.baseCfs = baseCfs;
    }

    public void add(SSTableReader sstable)
    {
        assert !inbuiltSecondaryIndexes.containsKey(sstable) : "already have indexes for sstable";
        Set<Component> components = sstable.getComponents(Component.Type.SECONDARY_INDEX);
        if (components.isEmpty())
            return;

        Descriptor desc = sstable.descriptor;
        Set<Pair<ByteBuffer, OnDiskSA>> readers = new HashSet<>();
        for (Component component : components)
        {
            String fileName = desc.filenameFor(component);
            OnDiskSA onDiskSA = null;
            try
            {
                onDiskSA = new OnDiskSA(new File(fileName), sstable.metadata.comparator);
            }
            catch (IOException e)
            {
                logger.error("problem opening index {} for sstable {}", fileName, sstable, e);
            }
            readers.add(Pair.create(parseName(component.name), onDiskSA));
        }
        inbuiltSecondaryIndexes.put(sstable, readers);
    }

    //TODO: this is kinda lame, but no other way to associate index to column name
    private ByteBuffer parseName(String name)
    {
        int start = name.indexOf("_") + 1;
        int end = name.lastIndexOf(".");
        return baseCfs.getComparator().fromString(name.substring(start, end));
    }

    public void remove(SSTableReader sstable)
    {
        Set<Pair<ByteBuffer, OnDiskSA>> readers = inbuiltSecondaryIndexes.get(sstable);
        if (readers == null || readers.isEmpty())
            return;
        for (Pair<ByteBuffer, OnDiskSA> pair : readers)
            FileUtils.closeQuietly(pair.right);
    }

    public Map<SSTableReader, Set<Pair<ByteBuffer, OnDiskSA>>> getIndexes()
    {
        return inbuiltSecondaryIndexes;
    }

    public Set<Pair<ByteBuffer, OnDiskSA>> getIndexes(SSTableReader reader)
    {
        return inbuiltSecondaryIndexes.get(reader);
    }
}
