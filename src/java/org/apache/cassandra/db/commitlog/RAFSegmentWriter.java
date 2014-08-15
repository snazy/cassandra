package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;

/**
 * Standard implementation for non-encrypted commit logs.
 *
 * When writing to the buffer, each commit log entry is followed by an and-of-segment marker, and then the pointer in
 * the buffer is moved back to the beginning of that marker (so that it is overwritten with the next entry).
 * This is done to help the {@link org.apache.cassandra.db.commitlog.CommitLogReplayer} know when it's reached the end of entries in the log file.
 */
public class RAFSegmentWriter extends SegmentWriter
{
    private static final Logger logger = LoggerFactory.getLogger(RAFSegmentWriter.class);

    public RAFSegmentWriter(File logFile)
    {
        super(logFile);
        logger.debug("created a new commit log segment: {}", logFile);
    }

    protected ByteBuffer getOutputBuffer() throws IOException
    {
        return logFileAccessor.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, DatabaseDescriptor.getCommitLogSegmentSize());
    }

    protected void writeHeader() throws IOException
    {
        assert buffer.position() == 0;
        buffer.putInt(CommitLog.END_OF_SEGMENT_MARKER);
        buffer.position(0);
    }

    public boolean hasCapacityFor(long size)
    {
        return size + CHECKSUM_LEN <= buffer.remaining();
    }

    protected void postWrite() throws IOException
    {
        if (buffer.remaining() >= CommitLog.END_OF_SEGMENT_MARKER_SIZE)
        {
            // writes end of segment marker and rewinds back to position where it starts
            int pos = buffer.position();
            buffer.putInt(CommitLog.END_OF_SEGMENT_MARKER);
            buffer.position(pos);
        }
    }

    public void sync()
    {
        // MappedByteBuffer.force() does not declare IOException but can actually throw it
        ((MappedByteBuffer)buffer).force();
    }

    public int position()
    {
        return buffer.position();
    }

    public void close() throws IOException
    {
        FileUtils.clean((MappedByteBuffer)buffer);
        logFileAccessor.close();
    }
}
