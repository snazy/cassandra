package org.apache.cassandra.db.index.search.container;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.roaringbitmap.RoaringBitmap;

public class KeyContainerBuilder
{
    private final AbstractType<?> keyComparator;
    private final RoaringBitmap offsets = new RoaringBitmap();

    private ByteBuffer min, max;

    public KeyContainerBuilder(AbstractType<?> keyComparator, NavigableMap<DecoratedKey, Integer> keys)
    {
        this.keyComparator = keyComparator;
        this.add(keys);
    }

    public void add(NavigableMap<DecoratedKey, Integer> other)
    {
        for (Map.Entry<DecoratedKey, Integer> e : other.entrySet())
        {
            updateRange(e.getKey());
            addInternal(e.getValue());
        }
    }

    private void addInternal(int keyOffset)
    {
        offsets.add(keyOffset);
    }

    public void serialize(DataOutput out) throws IOException
    {
        ByteBufferUtil.writeWithShortLength(min, out);
        ByteBufferUtil.writeWithShortLength(max, out);

        offsets.serialize(out);
    }

    public int serializedSize()
    {
        return 2 + min.remaining()
             + 2 + max.remaining()
             + offsets.serializedSizeInBytes();
    }

    private void updateRange(DecoratedKey key)
    {
        this.min = (min == null || keyComparator.compare(min, key.key) > 0) ? key.key : min;
        this.max = (max == null || keyComparator.compare(max, key.key) < 0) ? key.key : max;
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder().append(min).append(max).append(offsets).build();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof KeyContainerBuilder))
            return false;

        KeyContainerBuilder other = (KeyContainerBuilder) o;
        return !(!Objects.equals(min, other.min) || !Objects.equals(max, other.max)) && offsets.equals(other.offsets);
    }
}

