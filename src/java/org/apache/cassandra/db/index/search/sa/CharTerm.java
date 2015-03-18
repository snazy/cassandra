package org.apache.cassandra.db.index.search.sa;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import org.apache.cassandra.db.index.search.container.TokenTreeBuilder;
import org.apache.cassandra.db.marshal.AbstractType;

import com.google.common.base.Charsets;

public class CharTerm extends Term<CharBuffer>
{
    public CharTerm(int position, CharBuffer value, TokenTreeBuilder tokens)
    {
        super(position, value, tokens);
    }

    @Override
    public ByteBuffer getTerm()
    {
        return Charsets.UTF_8.encode(value.duplicate());
    }

    @Override
    public ByteBuffer getSuffix(int start)
    {
        return Charsets.UTF_8.encode(value.subSequence(value.position() + start, value.remaining()));
    }

    @Override
    public int compareTo(AbstractType<?> comparator, Term other)
    {
        return value.compareTo((CharBuffer) other.value);
    }

    @Override
    public int length()
    {
        return value.length();
    }
}
