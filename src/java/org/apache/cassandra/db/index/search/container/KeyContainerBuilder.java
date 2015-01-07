package org.apache.cassandra.db.index.search.container;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.MurmurHash;

import org.apache.commons.lang.builder.HashCodeBuilder;
import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.carrotsearch.hppc.cursors.ObjectIntCursor;
import org.roaringbitmap.RoaringBitmap;

public class KeyContainerBuilder
{
    private static final int MAX_PER_BUCKET = 1024;

    private final AbstractType<?> keyComparator;
    private final List<Bucket> buckets = new ArrayList<>();
    private final ObjectIntOpenHashMap<ByteBuffer> buffer = new ObjectIntOpenHashMap<>();

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
            addInternal(e.getKey().key, e.getValue());
        }
    }

    private void addInternal(ByteBuffer key, Integer keyOffset)
    {
        if (buffer.size() == MAX_PER_BUCKET)
            flush();

        buffer.put(key, keyOffset);
    }

    private void flush()
    {
        buckets.add(new Bucket(buffer));
        buffer.clear();
    }

    public KeyContainerBuilder finish()
    {
        if (buffer.size() > 0)
            flush();

        return this;
    }

    public void serialize(DataOutput out) throws IOException
    {
        ByteBufferUtil.writeWithShortLength(min, out);
        ByteBufferUtil.writeWithShortLength(max, out);

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
        int size = (2 + min.remaining()
                  + 2 + max.remaining()
                  + 4); // min + max tokens + number of buckets

        for (Bucket b : buckets)
            size += 4 + b.serializedSize(); // offset of the bucket + serialized size

        return size;
    }

    private void updateRange(DecoratedKey key)
    {
        this.min = (min == null || keyComparator.compare(min, key.key) > 0) ? key.key : min;
        this.max = (max == null || keyComparator.compare(max, key.key) < 0) ? key.key : max;
    }

    @Override
    public int hashCode()
    {
        HashCodeBuilder builder = new HashCodeBuilder().append(min).append(max);
        for (Bucket bucket : buckets)
            builder.append(bucket.hashCode());

        return builder.toHashCode();
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

        public Bucket(ObjectIntOpenHashMap<ByteBuffer> keys)
        {
            bf = FilterFactory.getFilter(keys.size(), 0.01, true);
            offsets = new RoaringBitmap();

            for (ObjectIntCursor<ByteBuffer> key : keys)
                add(key.key, key.value);
        }

        private void add(ByteBuffer key, int keyOffset)
        {
            bf.add(key);
            offsets.add(keyOffset);
            updateRange(MurmurHash.hash2_64(key, key.position(), key.remaining(), 0));
        }

        public void serialize(DataOutput out) throws IOException
        {
            FilterFactory.serialize(bf, out);
            offsets.serialize(out);
        }

        @Override
        public int hashCode()
        {
            return new HashCodeBuilder().append(offsets.hashCode()).toHashCode();
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
    }
}

