package org.apache.cassandra.io.sstable;

import org.apache.cassandra.db.DecoratedKey;

/**
 * Observer for events in the lifecycle of writing out an sstable.
 */
public interface SSTableWriterListener extends Comparable<String> //?????? with the Comparable<String>   ?????
{
    /**
     * called before writing any data to the sstable.
     */
    void begin();

    /**
     * Called after the row has been written to the sstable, with the offset of the row.
     * @param key The key written
     * @param position the file offset of the row.
     */
    void nextRow(DecoratedKey key, long position);

    /**
     * Called when all data is written to the file and it's ready to be finished up.
     */
    void complete();
}
