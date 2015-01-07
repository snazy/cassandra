package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.TransparentDataEncryptionOptions;
import org.apache.cassandra.io.FSWriteError;

public class EncryptedSegmentWriter extends SegmentWriter
{
    private static final Logger logger = LoggerFactory.getLogger(EncryptedSegmentWriter.class);

    /**
     * a reusable byte array to hold encrypted data before we copy it to the mmap'ed buffer
     */
    private byte[] flushBuffer;

    private final Cipher cipher;
    private final int chunkLength;
    private final String keyAlias;

    public EncryptedSegmentWriter(File logFile, TransparentDataEncryptionOptions tdeOptions)
    {
        super(logFile);
        keyAlias = tdeOptions.key_alias;
        chunkLength = tdeOptions.chunk_length_kb * 1024;
        flushBuffer = new byte[chunkLength];

        try
        {
            cipher = DatabaseDescriptor.getCipherFactory().getEncryptor(tdeOptions.cipher, tdeOptions.key_alias);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, logFile);
        }
        logger.debug("created a new encrypted commit log segment: {}", logFile);
    }

    protected ByteBuffer getOutputBuffer() throws IOException
    {
        ByteBuffer buf = ByteBuffer.allocate(chunkLength);
        buf.mark();
        return buf;
    }

    protected void writeHeader() throws IOException
    {
        assert buffer.position() == 0;
        //write out encryption headers here - IV, key_version (for key rotation needs), cipher_alg
        logFileAccessor.writeShort((short) cipher.getAlgorithm().length());
        logFileAccessor.write(cipher.getAlgorithm().getBytes());
        logFileAccessor.writeShort((short) keyAlias.length());
        logFileAccessor.write(keyAlias.getBytes());

        byte[] iv = cipher.getIV();
        logFileAccessor.write(iv.length);
        logFileAccessor.write(iv);

        long curPos = logFileAccessor.getFilePointer();
        logFileAccessor.write(CommitLog.END_OF_SEGMENT_MARKER);
        logFileAccessor.seek(curPos);
    }

    //check if we have enough space in the file, not just the buffer
    public boolean hasCapacityFor(long size)
    {
        try
        {
            int estimatedSize = (int)size + ENTRY_OVERHEAD_SIZE + buffer.position();
            // bail if there's no remaining space in the file
            if (estimatedSize > fileLength - logFileAccessor.getFilePointer())
                return false;

            if (estimatedSize > buffer.remaining())
            {
                flush(false);
                // if we still don't have enough space in the buffer, reallocate
                if (estimatedSize > buffer.remaining())
                {
                    // this might seem goofy to call flush() again before realloc'ing, but we need to make sure we flush everything
                    // from the current buffer before it's thrown away. also, we're trying to optimize the flushes and maximize buffer use (before encrypting)
                    flush(true);

                    // at this point, buffer should be completely flushed & empty, so make sure buffer is even large enough to handle the mutation
                    if (estimatedSize > buffer.capacity())
                    {
                        ByteBuffer b = ByteBuffer.allocate((int)(estimatedSize * 1.04));
                        b.mark();
                        setBuffer(b);
                    }

                }
            }
            return true;
        }
        catch (IOException ioe)
        {
            throw new IOError(ioe);
        }
    }

    private boolean flush(boolean force) throws IOException
    {
        final int curEnd = buffer.position();
        // rewind back to any mark we may have set
        buffer.reset();
        final int curStart = buffer.position();
        final int preEncryptLen = curEnd - curStart;
        if (preEncryptLen == 0)
            return false;

        //protect against allocating a large, local flush buffer if super.buffer is big by chunking the writes.
        // this helps with memory (re-)allocation as well as keeping the blocks to encrypt at a reasonable/bounded size.
        int startIdx = curStart, endIdx = curStart;
        while (curEnd > endIdx)
        {
            if (endIdx > curStart)
                startIdx = endIdx;
            endIdx = Math.min(endIdx + chunkLength, curEnd);

            int clearTextLen = endIdx - startIdx;
            // if we don't have a full buffer yet, don't bother to encrypt and make the write() syscall
            // but only do so if we're at the last block of a multi-chunk flush (and we've already written at least one block)
            if (startIdx > curStart && clearTextLen < chunkLength && !force)
            {
                // set the buffer's position to the start of the unwritten block, so the next call to flush() is aligned correctly
                buffer.position(startIdx);
                buffer.mark();
                buffer.position(curEnd);
                return true;
            }

            int estLen = cipher.getOutputSize(clearTextLen);
            if (flushBuffer.length < estLen)
                flushBuffer = new byte[estLen];

            int cipherTextLen;
            try
            {
                cipherTextLen = cipher.doFinal(buffer.array(), startIdx, clearTextLen, flushBuffer, 0);
            }
            catch (ShortBufferException | IllegalBlockSizeException | BadPaddingException e)
            {
                throw new IOException("failed to encrypt commit log block", e);
            }

            logFileAccessor.writeInt(cipherTextLen);
            logFileAccessor.write(flushBuffer, 0, cipherTextLen);
        }

        long curPos = logFileAccessor.getFilePointer();
        if (fileLength - curPos >= CommitLog.END_OF_SEGMENT_MARKER_SIZE)
        {
            logFileAccessor.write(CommitLog.END_OF_SEGMENT_MARKER);
            logFileAccessor.seek(curPos);
        }
        buffer.clear();
        buffer.mark();
        return true;
    }

    public void sync() throws IOException
    {
        if (flush(true))
            logFileAccessor.getChannel().force(false);
    }

    public void close() throws IOException
    {
        logFileAccessor.close();
    }

    public int position()
    {
        try
        {
            return (int)logFileAccessor.getFilePointer();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }
}
