package org.apache.cassandra.db.index;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.index.search.OnDiskSA;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;

public class InbuiltSecondaryIndexHolder
{
    private static final Logger logger = LoggerFactory.getLogger(InbuiltSecondaryIndexHolder.class);

    // TODO:JEB can I get a COW map??
    private Map<SSTableReader, Set<OnDiskSA>> inbuiltSecondaryIndexes = new ConcurrentHashMap<>();

    public void add(SSTableReader sstable)
    {
        assert !inbuiltSecondaryIndexes.containsKey(sstable) : "already have indexes for sstable";
        Set<Component> components = sstable.getComponents(Component.Type.SECONDARY_INDEX);
        if (components.isEmpty())
            return;

        Set<OnDiskSA> readers = new HashSet<>();
        for (Component component : components)
        {
            Descriptor desc = sstable.descriptor;
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
            readers.add(onDiskSA);
        }
        inbuiltSecondaryIndexes.put(sstable, readers);

    }

    public void remove(SSTableReader sstable)
    {
        Set<OnDiskSA> readers = inbuiltSecondaryIndexes.get(sstable);
        if (readers == null || readers.isEmpty())
            return;
        for (OnDiskSA sa : readers)
            FileUtils.closeQuietly(sa);
    }

    public Map<SSTableReader, Set<OnDiskSA>> getIndexes()
    {
        return inbuiltSecondaryIndexes;
    }
}
