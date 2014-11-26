package org.apache.cassandra.db.index.search;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;

public class Suffix
{
    protected final ByteBuffer content;

    public Suffix(ByteBuffer content)
    {
        this.content = content;
    }

    public ByteBuffer getSuffix()
    {
        ByteBuffer dup = content.duplicate();
        int len = dup.getShort();
        dup.limit(dup.position() + len);
        return dup;
    }

    public int compareTo(AbstractType<?> comparator, ByteBuffer query)
    {
        return compareTo(comparator, query, true);
    }

    public int compareTo(AbstractType<?> comparator, ByteBuffer query, boolean checkFully)
    {
        int position = content.position(), limit = content.limit(), len = content.getShort(position);
        content.position(position + 2).limit(position + 2 + (checkFully ? len : Math.min(len, query.remaining())));
        int cmp = comparator.compare(content, query);
        content.position(position).limit(limit);
        return cmp;

    }
}
