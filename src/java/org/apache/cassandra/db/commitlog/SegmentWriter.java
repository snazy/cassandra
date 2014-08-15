package org.apache.cassandra.db.commitlog;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.Checksum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.ByteBufferOutputStream;
import org.apache.cassandra.io.util.ChecksummedOutputStream;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.PureJavaCrc32;

public abstract class SegmentWriter
{
    private static final Logger logger = LoggerFactory.getLogger(SegmentWriter.class);

    protected static final int CHECKSUM_LEN = 16; // because we write out two longs from the checksum for each mutation entry

    protected final RandomAccessFile logFileAccessor;
    protected final long fileLength;
    protected final Checksum checksum;

    /**
     * Buffers incoming row mutations prior to flushing.
     */
    protected ByteBuffer buffer;
    protected DataOutputStream bufferStream;

    /** reusable byte buffer for the checksum long */
    protected final ByteBuffer longBuffer = ByteBuffer.allocate(8);

    protected SegmentWriter(File logFile)
    {
        checksum = new PureJavaCrc32();

        try
        {
            logFileAccessor = new RandomAccessFile(logFile, "rw");
            // Map the segment, extending or truncating it to the standard segment size
            fileLength = DatabaseDescriptor.getCommitLogSegmentSize();
            logFileAccessor.setLength(fileLength);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, logFile);
        }
    }

    public void init() throws IOException
    {
        setBuffer(getOutputBuffer());
        writeHeader();
    }

    protected void setBuffer(ByteBuffer buffer)
    {
        this.buffer = buffer;
        bufferStream = new DataOutputStream(new ChecksummedOutputStream(new ByteBufferOutputStream(buffer), checksum));
    }

    protected abstract ByteBuffer getOutputBuffer() throws IOException;
    protected abstract void writeHeader() throws IOException;

    public abstract boolean hasCapacityFor(long size);

    void write(RowMutation mutation, int mutationSize) throws IOException
    {
        checksum.reset();

        // checksummed length
        bufferStream.writeInt(mutationSize);
        longBuffer.clear();
        longBuffer.putLong(checksum.getValue());
        longBuffer.position(0);
        buffer.put(longBuffer);
        long cksum = checksum.getValue();

        // checksummed mutation
        RowMutation.serializer.serialize(mutation, bufferStream, MessagingService.current_version);
        longBuffer.clear();
        longBuffer.putLong(checksum.getValue());
        longBuffer.position(0);
        buffer.put(longBuffer);

        postWrite();
    }

    protected void postWrite() throws IOException {};

    public abstract void sync() throws IOException;

    public abstract int position();

    public abstract void close() throws IOException;
}
