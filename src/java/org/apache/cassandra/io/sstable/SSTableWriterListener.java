package org.apache.cassandra.io.sstable;

import org.apache.cassandra.db.Column;
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

    void startRow(DecoratedKey key, long curPosition);

    void nextColumn(Column column);

    /**
     * Called when all data is written to the file and it's ready to be finished up.
     */
    void complete();
}
