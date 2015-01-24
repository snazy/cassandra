package org.apache.cassandra.io.sstable;

public interface SSTableWriterListenable
{
    public enum Source { MEMTABLE, COMPACTION }
    SSTableWriterListener getListener(Descriptor descriptor, Source source);
}
