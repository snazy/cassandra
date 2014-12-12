package org.apache.cassandra.db.index.search.container;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.MurmurHash;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.roaringbitmap.RoaringBitmap;

public class KeyContainerBuilder
{
    private static final int MAX_PER_BUCKET = 1024;

    private final List<Bucket> buckets;

    private int offset = 0;

    private RowPosition min, max;

    public KeyContainerBuilder()
    {
        buckets = new ArrayList<Bucket>()
        {{
            add(new Bucket());
        }};
    }

    public KeyContainerBuilder(NavigableMap<DecoratedKey, Integer> keys)
    {
        this();
        add(keys);
    }

    public void add(NavigableMap<DecoratedKey, Integer> other)
    {
        for (Map.Entry<DecoratedKey, Integer> e : other.entrySet())
        {
            updateRange(e.getKey());
            addInternal(e.getKey().key, e.getValue());
        }
    }

    private void addInternal(ByteBuffer key, int keyOffset)
    {
        Bucket current = buckets.get(offset);

        if (current.isFull())
        {
            buckets.add((current = new Bucket()));
            offset++;
        }

        current.add(key, keyOffset);
    }

    public void serialize(DataOutput out) throws IOException
    {
        RowPosition.serializer.serialize(min, out);
        RowPosition.serializer.serialize(max, out);

        out.writeInt(buckets.size());

        int offset = 0;
        for (Bucket b : buckets)
        {
            out.writeLong(b.minToken);
            out.writeLong(b.maxToken);
            out.writeInt(offset);

            offset += b.dataSerializedSize();
        }

        for (Bucket b : buckets)
            b.serialize(out);
    }

    public int serializedSize()
    {
        int size = (int) (RowPosition.serializer.serializedSize(min, TypeSizes.NATIVE)
                        + RowPosition.serializer.serializedSize(max, TypeSizes.NATIVE)
                        + 4); // min + max tokens + number of buckets

        for (Bucket b : buckets)
            size += 4 + b.serializedSize(); // offset of the bucket + serialized size

        return size;
    }

    private void updateRange(RowPosition position)
    {
        this.min = (min == null || min.compareTo(position) > 0) ? position : min;
        this.max = (max == null || max.compareTo(position) < 0) ? position : max;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof KeyContainerBuilder))
            return false;

        KeyContainerBuilder other = (KeyContainerBuilder) o;
        if (!Objects.equals(min, other.min) || !Objects.equals(max, other.max))
            return false;

        if (buckets.size() != other.buckets.size())
            return false;

        for (int i = 0; i < buckets.size(); i++)
        {
            if (!buckets.get(i).equals(other.buckets.get(i)))
                return false;
        }

        return true;
    }

    private static class Bucket extends TokenRange
    {
        private final IFilter bf;
        private final RoaringBitmap offsets;

        public Bucket()
        {
            bf = FilterFactory.getFilter(MAX_PER_BUCKET, 0.01, true);
            offsets = new RoaringBitmap();
        }

        public void add(ByteBuffer key, int keyOffset)
        {
            bf.add(key);
            offsets.add(keyOffset);
            updateRange(MurmurHash.hash2_64(key, key.position(), key.remaining(), 0));
        }

        public boolean isFull()
        {
            return offsets.getCardinality() >= MAX_PER_BUCKET;
        }

        public void serialize(DataOutput out) throws IOException
        {
            FilterFactory.serialize(bf, out);
            offsets.serialize(out);
        }

        @Override
        public boolean equals(Object o)
        {
            return o instanceof Bucket && offsets.equals(((Bucket) o).offsets);
        }

        public int dataSerializedSize()
        {
            return (int) bf.serializedSize() + offsets.serializedSizeInBytes();
        }

        public int serializedSize()
        {
            return super.serializedSize() + dataSerializedSize();
        }

        public String toString()
        {
            return String.format("Bucket(bf: %s, offsets: %s)",  bf, offsets);
        }
    }
}

