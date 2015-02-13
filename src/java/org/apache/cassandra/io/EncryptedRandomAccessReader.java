package org.apache.cassandra.io;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;

import com.google.common.io.CountingInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;

/**
 * NOTE: current his class *only* works when accessed sequentially, as in commit log replay. further work
 * needs to be done to make it thread safe and really legit for random access.
 */
public class EncryptedRandomAccessReader extends RandomAccessReader
{
    private static final Logger logger = LoggerFactory.getLogger(EncryptedRandomAccessReader.class);

    private static final byte[] END_OF_SEGMENT_MARKER = new byte[CommitLog.END_OF_SEGMENT_MARKER_SIZE];

    /**
     * we read the raw encrypted bytes into this buffer, then move the decrypted ones into super.buffer.
     */
    private ByteBuffer encrypted;

    /**
     * a simple, reusable buffer for reading the length of the next encrypted block.
     */
    private final ByteBuffer blockLenBuffer;

    private final Cipher cipher;
    private final int initialOffset;

    private int nextOffset;

    public EncryptedRandomAccessReader(File dataFilePath) throws IOException
    {
        super(new File(dataFilePath.getAbsolutePath()), DEFAULT_BUFFER_SIZE, null);
        encrypted = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        blockLenBuffer = ByteBuffer.allocate(4);
        FileInputStream fis = null;
        try
        {
            CountingInputStream cis = new CountingInputStream(new FileInputStream(dataFilePath));
            DataInputStream dis = new DataInputStream(cis);
            String cipherAlg = dis.readUTF();
            String keyAlias = dis.readUTF();
            int ivLen = dis.readByte();
            byte[] iv = new byte[ivLen];
            dis.read(iv);

            cipher = DatabaseDescriptor.getCipherFactory().getDecryptor(cipherAlg, keyAlias, iv);
            initialOffset = (int)cis.getCount();
            nextOffset = initialOffset;
        }
        catch (IOException ioe)
        {
            throw new FSReadError(ioe, dataFilePath);
        }
        finally
        {
            FileUtils.closeQuietly(fis);
        }
    }

    protected void reBuffer()
    {
        try
        {
            channel.position(nextOffset);
            blockLenBuffer.clear();
            int cnt = channel.read(blockLenBuffer);
            nextOffset += cnt;

            // make sure we read an entire int's worth of data
            if (cnt < blockLenBuffer.capacity())
            {
                buffer = END_OF_SEGMENT_MARKER;
                validBufferBytes = END_OF_SEGMENT_MARKER.length;
                bufferOffset = current;
                return;
            }

            blockLenBuffer.flip();
            final int cipherTextLen = blockLenBuffer.getInt();

            if (cipherTextLen < 0 || cipherTextLen > 1 << 24)
                throw new IllegalStateException(String.format("read an invalid size (%d) for encrypted block length at offset %d",
                                                              cipherTextLen, nextOffset));

            if (cipherTextLen == 0)
            {
                buffer = END_OF_SEGMENT_MARKER;
                validBufferBytes = END_OF_SEGMENT_MARKER.length;
            }
            else
            {
                nextOffset += cipherTextLen;

                if (encrypted.capacity() < cipherTextLen)
                    encrypted = ByteBuffer.allocate(cipherTextLen);
                else
                    encrypted.clear();
                encrypted.limit(cipherTextLen);

                int count = channel.read(encrypted);
                int decryptBufLen = cipher.getOutputSize(count);
                if (buffer.length < decryptBufLen)
                    buffer = new byte[decryptBufLen];
                validBufferBytes = cipher.doFinal(encrypted.array(), 0, count, buffer, 0);
            }
            bufferOffset = current;
        }
        catch (IOException | ShortBufferException  | BadPaddingException | IllegalBlockSizeException e)
        {
            logger.error("problem reading encrypted file at bufferOffset {}, encrypted buf.len {}", bufferOffset, encrypted.capacity());
            throw new FSReadError(e, getPath());
        }
    }

    public void resetBuffer()
    {
        //special case when we want to start reading from the top of the file
        if (current == 0)
            nextOffset = initialOffset;
        current += nextOffset;
        nextOffset = 0;
        super.resetBuffer();
    }

    public int getTotalBufferSize()
    {
        return super.getTotalBufferSize() + encrypted.capacity();
    }

    public String toString()
    {
        return String.format("%s - encrypted file.", getPath());
    }

    public int getInitialOffset()
    {
        return initialOffset;
    }
}
