package org.apache.cassandra.db.index.search.sa;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.index.search.container.TokenTreeBuilder;
import org.apache.cassandra.db.marshal.AbstractType;

public class ByteTerm extends Term<ByteBuffer>
{
    public ByteTerm(int position, ByteBuffer value, TokenTreeBuilder tokens)
    {
        super(position, value, tokens);
    }

    @Override
    public ByteBuffer getTerm()
    {
        return value.duplicate();
    }

    @Override
    public ByteBuffer getSuffix(int start)
    {
        return (ByteBuffer) value.duplicate().position(value.position() + start);
    }

    @Override
    public int compareTo(AbstractType<?> comparator, Term other)
    {
        return comparator.compare(value, (ByteBuffer) other.value);
    }

    @Override
    public int length()
    {
        return value.remaining();
    }
}
