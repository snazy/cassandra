package org.apache.cassandra.io.sstable;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.DecoratedKey;

/**
 * Observer for events in the lifecycle of writing out an sstable.
 */
public interface SSTableWriterListener
{
    enum Source { MEMTABLE, COMPACTION }

    /**
     * called before writing any data to the sstable.
     */
    void begin();

    /**
     * called when a new row in being written to the sstable, but before any columns are processed (see {@code nextColumn(Column)}).
     */
    void startRow(DecoratedKey key, long curPosition);

    /**
     * called after the column is written to the sstable. will be preceded by a call to {@code startRow(DecoratedKey, long)}, and the column should be assumed
     * to belong to that row.w
     */
    void nextColumn(Column column);

    /**
     * Called when all data is written to the file and it's ready to be finished up.
     */
    void complete();
}
