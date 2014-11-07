package org.apache.cassandra.io.sstable;

public interface SSTableWriterListenable
{
    SSTableWriterListener getListener(Descriptor descriptor);
}
