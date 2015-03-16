package org.apache.cassandra.io.util;

import com.sun.jna.Pointer;
import org.apache.cassandra.utils.CLibrary;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.FileDescriptor;
import java.nio.*;

public class NativeMappedBuffer
{
    private static final Logger logger = LoggerFactory.getLogger(NativeMappedBuffer.class);

    private static final long PAGE_SIZE = 4096; // TODO (jwest): get page size from CLibrary?

    // TODO (jwest): add mark
    private final long fileOffset;
    private final Pointer region;
    private final long size;
    private final int skipped;
    private long position;
    private long limit;

    protected NativeMappedBuffer(Pointer region, long fileOffset, int skipped, long size, long position)
    {
        this.region = region;
        this.fileOffset = fileOffset;
        this.skipped = skipped;
        this.size = size;
        this.position = position;
        this.limit = this.size;
    }

    public NativeMappedBuffer(FileDescriptor fd, long offset, long size)
    {
        // the offset is page aligned for mapping but to honor the
        // given offset the distance between the two is stored as is skipped
        // down cast is safe because page size will always fit in
        // an int and the value is smaller than one page
        this(CLibrary.nativeMapping(fd, offset & ~(PAGE_SIZE - 1), size, "r"),
                offset & ~(PAGE_SIZE - 1),
                (int) (offset - (offset & ~(PAGE_SIZE - 1))),
                size,
                0);
    }

    public void unmap()
    {
        //try
        //{
            if (CLibrary.munmap(region, size) != 0)
                throw new RuntimeException("unable to unmap region starting at address" + region.toString());
        //}
        //catch (UnsatisfiedLinkError e)
        //{
//            logger.error("unable to unmap region");
        //}
    }

    public NativeMappedBuffer duplicate()
    {
        return new NativeMappedBuffer(region, fileOffset, skipped, size, position);
    }

    public ByteBuffer asByteBuffer()
    {
        return region.getByteBuffer(skipped + position, remaining()).order(ByteOrder.BIG_ENDIAN);
    }

    public long capacity()
    {
        return size;
    }

    public long position()
    {
        return position;
    }

    public NativeMappedBuffer position(long newPosition)
    {
        if (newPosition < 0 || newPosition > limit)
            throw new IllegalArgumentException("limit must be greater-than or equal to 0 and less than or equal to limit (" + limit + ")");

        position = newPosition;
        return this;
    }

    public long limit()
    {
        return limit;
    }

    public NativeMappedBuffer limit(long newLimit)
    {
        if (newLimit < position || newLimit > size)
            throw new IllegalArgumentException("limit must be greater-than or equal to position (" + position + ") and less than or equal to total capacity (" + size + ")");

        limit = newLimit;
        return this;
    }

    public long remaining()
    {
        return limit - position;
    }

    public boolean hasRemaining()
    {
        return remaining() > 0;
    }

    public byte get()
    {
        if (position >= limit)
            throw new BufferUnderflowException();

        return get(position++);
    }

    public byte get(long pos)
    {
        if (pos < 0)
            throw new IndexOutOfBoundsException("position " + position + " cannot be read");

        if (pos >= limit)
            throw new IndexOutOfBoundsException("position " + pos + " is not less than " + limit);

        return region.getByte(skipped + pos);
    }

    public short getShort()
    {
        if (position > (limit - 2))
            throw new BufferUnderflowException();

        short res = getShort(position);
        position += 2;
        return res;
    }

    public short getShort(long pos)
    {
        if (pos < 0)
            throw new IndexOutOfBoundsException("position " + position + " cannot be read");

        if (pos > (limit - 2))
            throw new IndexOutOfBoundsException("position " + pos + " is not less than or equal to " + (limit - 2));

        return (short) reconstruct(pos, 2);
    }

    public int getInt()
    {
        if (position > (limit - 4))
            throw new BufferUnderflowException();

        int res = getInt(position);
        position += 4;
        return res;
    }

    public int getInt(long pos)
    {
        if (pos < 0)
            throw new IndexOutOfBoundsException("position " + position + " cannot be read");

        if (pos > (limit - 4))
            throw new IndexOutOfBoundsException("position " + pos + " is not less than or equal to " + (limit - 4));

        return (int) reconstruct(pos, 4);
    }

    public long getLong()
    {
        if (position > (limit - 8))
            throw new BufferUnderflowException();

        long res = getLong(position);
        position += 8;
        return res;
    }


    public long getLong(long pos)
    {
        if (pos < 0)
            throw new IndexOutOfBoundsException("position " + position + " cannot be read");

        if (pos > (limit - 8))
            throw new IndexOutOfBoundsException("position " + pos + " is not less than or equal to " + (limit - 8));

        return reconstruct(pos, 8);
    }

    protected long reconstruct(long pos, int numBytes)
    {
        long result = 0;
        for (byte i = 0; i < numBytes; i++)
            result = (result << 8) + (region.getByte((skipped + pos) + i) & 0xff);

        return result;
    }

}
