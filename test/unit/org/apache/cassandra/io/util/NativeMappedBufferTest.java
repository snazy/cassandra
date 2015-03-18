package org.apache.cassandra.io.util;

import org.apache.cassandra.utils.CLibrary;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class NativeMappedBufferTest
{

    @Test
    public void testBasicWriteThenRead() throws Exception
    {
        long numInts                    = 1000; //1000000;
        final File testFile             = createTestFile(numInts);
        final RandomAccessFile rar      = new RandomAccessFile(testFile, "r");
        final NativeMappedBuffer buffer = new NativeMappedBuffer(rar.getFD(), 0, rar.getChannel().size());
        rar.close();

        Assert.assertEquals(0, buffer.position());
        for (long i = 0; i < numInts; i++)
        {
            Assert.assertEquals(i * 8, buffer.position());
            Assert.assertEquals(i, buffer.getLong());
        }

        buffer.position(0);
        for (long i = 0; i < numInts; i++)
        {
            Assert.assertEquals(i, buffer.getLong(i * 8));
            Assert.assertEquals(0, buffer.position());
        }

        // read all the numbers as shorts (all numbers fit into four bytes)
        for (long i = 0; i < numInts; i++)
            Assert.assertEquals(i, buffer.getInt((i * 8) + 4));

        // read all the numbers as shorts (all numbers fit into two bytes)
        for (long i = 0; i < numInts; i++)
            Assert.assertEquals(i, buffer.getShort((i * 8) + 6));

        // read all the numbers that can be represented as a single byte
        for (long i = 0; i < 128; i++)
            Assert.assertEquals(i, buffer.get((i * 8) + 7));

        buffer.unmap();
    }

    @Test
    public void testWithUnAlignedStartOffset() throws Exception
    {
        long numInts                    = 1000;
        final File testFile             = createTestFile(numInts);
        final RandomAccessFile rar      = new RandomAccessFile(testFile, "r");
        final NativeMappedBuffer buffer = new NativeMappedBuffer(rar.getFD(), 32, rar.getChannel().size());
        rar.close();

        buffer.position(0);
        for (long i = 0; i < (numInts - 4); i++)
            Assert.assertEquals(i + 4, buffer.getLong());

        buffer.position(0);
        for (long i = 0; i < (numInts - 4); i++)
            Assert.assertEquals(i + 4, buffer.getLong(i * 8));

    }

    @Test
    public void testDuplicate() throws Exception
    {
        long numInts                     =  10;
        final File testFile              = createTestFile(numInts);
        final RandomAccessFile rar       = new RandomAccessFile(testFile, "r");
        final NativeMappedBuffer buffer1 = new NativeMappedBuffer(rar.getFD(), 0, rar.getChannel().size());
        rar.close();

        Assert.assertEquals(0, buffer1.getLong());
        Assert.assertEquals(1, buffer1.getLong());

        final NativeMappedBuffer buffer2 = buffer1.duplicate();

        Assert.assertEquals(2, buffer1.getLong());
        Assert.assertEquals(2, buffer2.getLong());

        buffer2.position(0);
        Assert.assertEquals(3, buffer1.getLong());
        Assert.assertEquals(0, buffer2.getLong());

    }

    @Test
    public void testLimit() throws Exception
    {
        long numInts                     =  10;
        final File testFile              = createTestFile(numInts);
        final RandomAccessFile rar       = new RandomAccessFile(testFile, "r");
        final NativeMappedBuffer buffer1 = new NativeMappedBuffer(rar.getFD(), 0, rar.getChannel().size());
        rar.close();

        NativeMappedBuffer buffer2 = buffer1.duplicate().position(16).limit(32);
        buffer1.position(0).limit(16);
        List<Long> longs = new ArrayList<>(4);

        while (buffer1.hasRemaining())
            longs.add(buffer1.getLong());

        while (buffer2.hasRemaining())
            longs.add(buffer2.getLong());

        Assert.assertArrayEquals(new Long[]{0L, 1L, 2L, 3L}, longs.toArray());
    }

    @Test
    public void testAsByteBuffer() throws Exception
    {
        long numInts = 1000;
        final File testFile = createTestFile(numInts);
        final RandomAccessFile rar = new RandomAccessFile(testFile, "r");
        final NativeMappedBuffer buffer1 = new NativeMappedBuffer(rar.getFD(), 0, rar.getChannel().size());
        final ByteBuffer buffer2 = buffer1.asByteBuffer();
        rar.close();

        Assert.assertEquals(0, buffer1.position());
        for (long i = 0; i < numInts; i++)
            Assert.assertEquals(i, buffer1.getLong());

        Assert.assertEquals(0, buffer2.position());
        for (long i = 0; i < numInts; i++)
            Assert.assertEquals(i, buffer2.getLong());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPositionGreaterThanLimit() throws Exception
    {
        long numInts = 1;
        final File testFile = createTestFile(numInts);
        final RandomAccessFile rar = new RandomAccessFile(testFile, "r");
        final NativeMappedBuffer buffer = new NativeMappedBuffer(rar.getFD(), 0, rar.getChannel().size());
        rar.close();

        buffer.limit(4);
        try
        {
            buffer.position(buffer.limit() + 1);
        }
        finally
        {
            buffer.unmap();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativePosition() throws Exception
    {
        long numInts = 1;
        final File testFile = createTestFile(numInts);
        final RandomAccessFile rar = new RandomAccessFile(testFile, "r");
        final NativeMappedBuffer buffer = new NativeMappedBuffer(rar.getFD(), 0, rar.getChannel().size());
        rar.close();

        try
        {
            buffer.position(-1);
        }
        finally
        {
            buffer.unmap();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLimitGreaterThanCapacity() throws Exception
    {
        long numInts = 1;
        final File testFile = createTestFile(numInts);
        final RandomAccessFile rar = new RandomAccessFile(testFile, "r");
        final NativeMappedBuffer buffer = new NativeMappedBuffer(rar.getFD(), 0, rar.getChannel().size());
        rar.close();


        try
        {
            buffer.limit(buffer.capacity() + 1);
        }
        finally
        {
            buffer.unmap();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLimitLessThanPosition() throws Exception
    {
        long numInts = 1;
        final File testFile = createTestFile(numInts);
        final RandomAccessFile rar = new RandomAccessFile(testFile, "r");
        final NativeMappedBuffer buffer = new NativeMappedBuffer(rar.getFD(), 0, rar.getChannel().size());
        rar.close();


        buffer.position(1);
        try
        {
            buffer.limit(0);
        }
        finally
        {
            buffer.unmap();
        }
    }

    @Test(expected = BufferUnderflowException.class)
    public void testGetRelativeUnderflow() throws Exception
    {
        long numInts = 1;
        final File testFile = createTestFile(numInts);
        final RandomAccessFile rar = new RandomAccessFile(testFile, "r");
        final NativeMappedBuffer buffer = new NativeMappedBuffer(rar.getFD(), 0, rar.getChannel().size());
        rar.close();

        buffer.position(buffer.limit());
        try
        {
            buffer.get();
        }
        finally
        {
            buffer.unmap();
        }

    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetAbsoluteGreaterThanCapacity() throws Exception
    {
        long numInts = 1;
        final File testFile = createTestFile(numInts);
        final RandomAccessFile rar = new RandomAccessFile(testFile, "r");
        final NativeMappedBuffer buffer = new NativeMappedBuffer(rar.getFD(), 0, rar.getChannel().size());
        rar.close();

        try
        {
            buffer.get(buffer.limit());
        }
        finally
        {
            buffer.unmap();
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetAbsoluteNegativePosition() throws Exception
    {
        long numInts = 1;
        final File testFile = createTestFile(numInts);
        final RandomAccessFile rar = new RandomAccessFile(testFile, "r");
        final NativeMappedBuffer buffer = new NativeMappedBuffer(rar.getFD(), 0, rar.getChannel().size());
        rar.close();

        try
        {
            buffer.get(-1);
        }
        finally
        {
            buffer.unmap();
        }
    }


    @Test(expected = BufferUnderflowException.class)
    public void testGetShortRelativeUnderflow() throws Exception
    {
        long numInts = 1;
        final File testFile = createTestFile(numInts);
        final RandomAccessFile rar = new RandomAccessFile(testFile, "r");
        final NativeMappedBuffer buffer = new NativeMappedBuffer(rar.getFD(), 0, rar.getChannel().size());
        rar.close();

        buffer.position(buffer.capacity() - 1);
        try
        {
            buffer.getShort();
        }
        finally
        {
            buffer.unmap();
        }

    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetShortAbsoluteGreaterThanCapacity() throws Exception
    {
        long numInts = 1;
        final File testFile = createTestFile(numInts);
        final RandomAccessFile rar = new RandomAccessFile(testFile, "r");
        final NativeMappedBuffer buffer = new NativeMappedBuffer(rar.getFD(), 0, rar.getChannel().size());
        rar.close();

        Assert.assertEquals(8, buffer.capacity());
        try
        {
            buffer.getShort(buffer.capacity() - 1);
        }
        finally
        {
            buffer.unmap();
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetShortAbsoluteNegativePosition() throws Exception
    {
        long numInts = 1;
        final File testFile = createTestFile(numInts);
        final RandomAccessFile rar = new RandomAccessFile(testFile, "r");
        final NativeMappedBuffer buffer = new NativeMappedBuffer(rar.getFD(), 0, rar.getChannel().size());
        rar.close();

        try {
            buffer.getShort(-1);
        } finally {
            buffer.unmap();
        }
    }

    @Test(expected = BufferUnderflowException.class)
    public void testGetIntRelativeUnderflow() throws Exception
    {
        long numInts = 1;
        final File testFile = createTestFile(numInts);
        final RandomAccessFile rar = new RandomAccessFile(testFile, "r");
        final NativeMappedBuffer buffer = new NativeMappedBuffer(rar.getFD(), 0, rar.getChannel().size());
        rar.close();

        buffer.position(buffer.capacity() - 3);
        try
        {
            buffer.getInt();
        }
        finally
        {
            buffer.unmap();
        }

    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIntAbsoluteGreaterThanCapacity() throws Exception
    {
        long numInts = 1;
        final File testFile = createTestFile(numInts);
        final RandomAccessFile rar = new RandomAccessFile(testFile, "r");
        final NativeMappedBuffer buffer = new NativeMappedBuffer(rar.getFD(), 0, rar.getChannel().size());
        rar.close();

        Assert.assertEquals(8, buffer.capacity());
        try
        {
            buffer.getInt(buffer.capacity() - 3);
        }
        finally
        {
            buffer.unmap();
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIntAbsoluteNegativePosition() throws Exception
    {
        long numInts = 1;
        final File testFile = createTestFile(numInts);
        final RandomAccessFile rar = new RandomAccessFile(testFile, "r");
        final NativeMappedBuffer buffer = new NativeMappedBuffer(rar.getFD(), 0, rar.getChannel().size());
        rar.close();

        try {
            buffer.getInt(-1);
        } finally {
            buffer.unmap();
        }
    }

    @Test(expected = BufferUnderflowException.class)
    public void testGetLongRelativeUnderflow() throws Exception
    {
        long numInts = 1;
        final File testFile = createTestFile(numInts);
        final RandomAccessFile rar = new RandomAccessFile(testFile, "r");
        final NativeMappedBuffer buffer = new NativeMappedBuffer(rar.getFD(), 0, rar.getChannel().size());
        rar.close();

        buffer.position(buffer.capacity() - 7);
        try
        {
            buffer.getLong();
        }
        finally
        {
            buffer.unmap();
        }

    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetLongAbsoluteGreaterThanCapacity() throws Exception
    {
        long numInts = 1;
        final File testFile = createTestFile(numInts);
        final RandomAccessFile rar = new RandomAccessFile(testFile, "r");
        final NativeMappedBuffer buffer = new NativeMappedBuffer(rar.getFD(), 0, rar.getChannel().size());
        rar.close();

        Assert.assertEquals(8, buffer.capacity());
        try
        {
            buffer.getLong(buffer.capacity() - 7);
        }
        finally
        {
            buffer.unmap();
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetLongAbsoluteNegativePosition() throws Exception
    {
        long numInts = 1;
        final File testFile = createTestFile(numInts);
        final RandomAccessFile rar = new RandomAccessFile(testFile, "r");
        final NativeMappedBuffer buffer = new NativeMappedBuffer(rar.getFD(), 0, rar.getChannel().size());
        rar.close();

        try {
            buffer.getLong(-1);
        } finally {
            buffer.unmap();
        }
    }

    private File createTestFile(long numInts) throws Exception
    {
        final File testFile = File.createTempFile("native-mapped-buffer-test", "");
        testFile.deleteOnExit();

        final SequentialWriter writer = new SequentialWriter(testFile, 4096, false);

        for (long i = 0L; i < numInts; i++)
            writer.writeLong(i);

        writer.close();

        return testFile;
    }

}
