package org.apache.cassandra.db.index.search.container;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.*;

import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import org.roaringbitmap.RoaringBitmap;

public class KeyContainer implements Iterable<KeyContainer.Bucket>
{
    protected final ByteBuffer min, max;

    protected final RandomAccessReader in;
    protected final IntervalTree<Long, Bucket, Interval<Long, Bucket>> buckets;
    protected final long containerStart;

    public KeyContainer(RandomAccessReader file) throws FSReadError
    {
        try
        {
            in = file;

            min = ByteBufferUtil.readWithShortLength(file);
            max = ByteBufferUtil.readWithShortLength(file);

            int numBuckets = file.readInt();
            List<Interval<Long, Bucket>> intervals = new ArrayList<>(numBuckets);

            for (int i = 0; i < numBuckets; i++)
            {
                long min = file.readLong(), max = file.readLong();
                intervals.add(Interval.create(min, max, new Bucket(this, file.readInt())));
            }

            buckets = IntervalTree.build(intervals);

            containerStart = file.getFilePointer();

            for (Interval<Long, Bucket> bucket : intervals)
                bucket.data.load();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, file.getPath());
        }
    }

    public Iterable<Bucket> intersect(final ByteBuffer key)
    {
        return Iterables.filter(buckets.search(MurmurHash.hash2_64(key, key.position(), key.remaining(), 0)),
                                new Predicate<Bucket>()
                                {
                                    @Override
                                    public boolean apply(Bucket bucket)
                                    {
                                        return bucket.isPresent(key);
                                    }
                                });
    }

    public Interval<ByteBuffer, SSTableReader> getRange()
    {
        return Interval.create(min, max);
    }

    public boolean intersects(KeyContainer o, AbstractType<?> comparator)
    {
        IPartitioner partitioner = new LocalPartitioner(comparator);
        Bounds<DecoratedKey> a = new Bounds<>(partitioner.decorateKey(min), partitioner.decorateKey(max), partitioner);
        Bounds<DecoratedKey> b = new Bounds<>(partitioner.decorateKey(o.min), partitioner.decorateKey(o.max), partitioner);
        return a.intersects(b);
    }

    @Override
    public Iterator<Bucket> iterator()
    {
        return new AbstractIterator<Bucket>()
        {
            private final Iterator<Interval<Long, Bucket>> iterator = buckets.iterator();

            @Override
            protected Bucket computeNext()
            {
                if (iterator.hasNext())
                    return iterator.next().data;

                return endOfData();
            }
        };
    }

    public static class Bucket
    {
        private final KeyContainer parent;
        private IFilter bf;
        private long bucketStart, offsetsStart;
        private RoaringBitmap offsets;

        public Bucket(KeyContainer parent, long bucketStart) throws FSReadError
        {
            this.parent = parent;
            this.bucketStart = bucketStart;
        }

        public void load()
        {
            long current = parent.in.getFilePointer();

            try
            {
                parent.in.seek(current + bucketStart);
                bf = FilterFactory.deserialize(parent.in, true);
                offsetsStart = parent.in.getFilePointer();
            }
            catch (IOException e)
            {
                throw new FSReadError(e, parent.in.getPath());
            }
            finally
            {
                parent.in.seek(current);
            }
        }

        public boolean isPresent(ByteBuffer key)
        {
            return bf.isPresent(key);
        }

        public RoaringBitmap getPositions()
        {
            if (offsets != null)
                return offsets;

            long position = parent.in.getFilePointer();

            try
            {
                parent.in.seek(offsetsStart);

                offsets = new RoaringBitmap();
                offsets.deserialize(parent.in);

                return offsets;
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
            finally
            {
                parent.in.seek(position);
            }
        }
    }
}
