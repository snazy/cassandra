package org.apache.cassandra.utils;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.util.AbstractDataInput;

public class ByteBufferDataInput extends AbstractDataInput
{
    private final ByteBuffer buffer;

    public ByteBufferDataInput(ByteBuffer buffer)
    {
        this.buffer = buffer;
    }

    @Override
    protected void seekInternal(int position)
    {
        buffer.position(position);
    }

    @Override
    protected int getPosition()
    {
        return buffer.position();
    }

    @Override
    public int skipBytes(int n) throws IOException
    {
        int len = Math.min(buffer.remaining(), buffer.position() + n);
        buffer.position(len);
        return len;
    }

    @Override
    public int read() throws IOException
    {
        return buffer.get() & 0xff;
    }
}
