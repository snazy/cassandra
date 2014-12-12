package org.apache.cassandra.db.index.search;

import java.nio.ByteBuffer;
import java.util.NavigableMap;

import org.apache.cassandra.db.DecoratedKey;

public class Term
{
    public final int position;
    public final ByteBuffer value;
    public final NavigableMap<DecoratedKey, Integer> keys;

    public Term(int position, ByteBuffer value, NavigableMap<DecoratedKey, Integer> keys)
    {
        this.position = position;
        this.value = value;
        this.keys = keys;
    }

    public ByteBuffer getSuffix(int start)
    {
        return (ByteBuffer) value.duplicate().position(start);
    }

    @Override
    public String toString()
    {
        return String.format("Term(value: %s, position: %d, keys: %s)", value, position, keys);
    }
}

