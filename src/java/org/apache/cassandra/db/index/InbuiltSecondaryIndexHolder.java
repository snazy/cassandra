package org.apache.cassandra.db.index;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.RandomAccessReader;

public class InbuiltSecondaryIndexHolder
{
    // TODO:JEB can I get a COW map??
    private Map<SSTableReader, Set<RandomAccessReader>> inbuiltSecondaryIndexes = new ConcurrentHashMap<>();

    public void add(SSTableReader sstable)
    {
        assert !inbuiltSecondaryIndexes.containsKey(sstable) : "already have indexes for sstable";
        Set<Component> components = sstable.getComponents(Component.Type.SECONDARY_INDEX);
        if (components.isEmpty())
            return;

        Set<RandomAccessReader> readers = new HashSet<>();
        for (Component component : components)
        {
            Descriptor desc = sstable.descriptor;
            String fileName = desc.filenameFor(component);

            //open the 2I file here - will (probably) be specific to 2I implementation, so don't attempt to parse
            // what if the file is compressed? where will that be indicated? punting on that decision for now
            RandomAccessReader reader = RandomAccessReader.open(new File(fileName));
            readers.add(reader);
        }
        inbuiltSecondaryIndexes.put(sstable, readers);

    }

    public void remove(SSTableReader sstable)
    {
        Set<RandomAccessReader> readers = inbuiltSecondaryIndexes.get(sstable);
        if (readers == null || readers.isEmpty())
            return;
        for (RandomAccessReader reader : readers)
        {
            reader.close();
        }
    }

    public Map<SSTableReader, Set<RandomAccessReader>> getIndexes()
    {
        return inbuiltSecondaryIndexes;
    }
}
