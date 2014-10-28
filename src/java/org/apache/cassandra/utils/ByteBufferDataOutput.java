package org.apache.cassandra.utils;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.util.AbstractDataOutput;

public class ByteBufferDataOutput extends AbstractDataOutput
{
    protected final ByteBuffer buffer;

    public ByteBufferDataOutput(ByteBuffer buffer)
    {
        this.buffer = buffer;
    }

    @Override
    public void write(byte[] buffer, int offset, int count) throws IOException
    {
        this.buffer.put(buffer, offset, count);
    }

    @Override
    public void write(int oneByte) throws IOException
    {
        this.buffer.put((byte) oneByte);
    }

    public int position()
    {
        return buffer.position();
    }

    public int capacity()
    {
        return buffer.capacity();
    }

    public void writeFullyTo(DataOutput out) throws IOException
    {
        for (int i = 0; i < buffer.capacity(); i++)
            out.writeByte(buffer.get(i));
    }
}