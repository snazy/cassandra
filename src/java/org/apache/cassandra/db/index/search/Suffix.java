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
        int len = dup.getInt();
        dup.limit(dup.position() + len);
        return dup;
    }

    /**
     * TODO: optimize compare/get methods so they don't do any buffer copies
     */

    public int compareTo(AbstractType<?> comparator, ByteBuffer query)
    {
        ByteBuffer dup = content.duplicate();
        int len = dup.getInt();
        dup.limit(dup.position() + Math.min(len, query.remaining()));
        return comparator.compare(dup, query.duplicate());
    }
}
