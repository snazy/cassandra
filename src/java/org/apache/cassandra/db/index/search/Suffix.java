package org.apache.cassandra.db.index.search;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.index.search.OnDiskSABuilder.SuffixSize;
import org.apache.cassandra.io.util.NativeMappedBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class Suffix
{
    protected final NativeMappedBuffer content;
    protected final SuffixSize suffixSize;


    public Suffix(NativeMappedBuffer content, SuffixSize size)
    {
        this.content = content;
        this.suffixSize = size;
    }

    public ByteBuffer getSuffix()
    {
        NativeMappedBuffer dup = content.duplicate();
        int len = suffixSize.isConstant() ? suffixSize.size : dup.getShort();
        dup.limit(dup.position() + len);
        return dup.asByteBuffer();
    }

    public long getDataOffset()
    {
        long position = content.position();
        return position + (suffixSize.isConstant() ? suffixSize.size : 2 + content.getShort(position));
    }

    public int compareTo(AbstractType<?> comparator, ByteBuffer query)
    {
        return compareTo(comparator, query, true);
    }

    public int compareTo(AbstractType<?> comparator, ByteBuffer query, boolean checkFully)
    {
        long position = content.position(), limit = content.limit();
        int padding = suffixSize.isConstant() ? 0 : 2;
        int len = suffixSize.isConstant() ? suffixSize.size : content.getShort(position);

        content.position(position + padding)
                .limit(position + padding + (checkFully ? len : Math.min(len, query.remaining())));

        int cmp = comparator.compare(content.asByteBuffer(), query);
        content.position(position).limit(limit);
        return cmp;

    }
}
