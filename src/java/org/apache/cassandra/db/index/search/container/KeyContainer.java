package org.apache.cassandra.db.index.search.container;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.*;

import org.roaringbitmap.RoaringBitmap;

public class KeyContainer
{
    protected final ByteBuffer min, max;
    protected final RoaringBitmap offsets;

    public KeyContainer(RandomAccessReader in) throws FSReadError
    {
        try
        {
            min = ByteBufferUtil.readWithShortLength(in);
            max = ByteBufferUtil.readWithShortLength(in);

            offsets = new RoaringBitmap();
            offsets.deserialize(in);
        }
        catch (IOException e)
        {
            throw new FSReadError(e, in.getPath());
        }
    }

    public RoaringBitmap getOffsets()
    {
        return offsets;
    }
}
