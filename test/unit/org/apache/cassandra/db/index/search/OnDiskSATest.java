package org.apache.cassandra.db.index.search;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.utils.Pair;

import com.google.common.collect.Iterators;
import junit.framework.Assert;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

public class OnDiskSATest
{
    @Test
    public void testStringSAConstruction() throws Exception
    {
        Map<ByteBuffer, NavigableMap<DecoratedKey, Integer>> data = new HashMap<ByteBuffer, NavigableMap<DecoratedKey, Integer>>()
        {{
                put(UTF8Type.instance.decompose("scat"), keyBuilder(1));
                put(UTF8Type.instance.decompose("mat"),  keyBuilder(2));
                put(UTF8Type.instance.decompose("fat"),  keyBuilder(3));
                put(UTF8Type.instance.decompose("cat"),  keyBuilder(1, 4));
                put(UTF8Type.instance.decompose("till"), keyBuilder(2, 6));
                put(UTF8Type.instance.decompose("bill"), keyBuilder(5));
                put(UTF8Type.instance.decompose("foo"),  keyBuilder(7));
                put(UTF8Type.instance.decompose("bar"),  keyBuilder(9, 10));
                put(UTF8Type.instance.decompose("michael"), keyBuilder(11, 12, 1));
        }};

        OnDiskSABuilder builder = new OnDiskSABuilder(UTF8Type.instance, UTF8Type.instance, OnDiskSABuilder.Mode.SUFFIX);
        for (Map.Entry<ByteBuffer, NavigableMap<DecoratedKey, Integer>> e : data.entrySet())
            builder.add(e.getKey(), e.getValue());

        File index = File.createTempFile("on-disk-sa-string", "db");
        index.deleteOnExit();

        builder.finish(Pair.create(keyAt(1), keyAt(12)), index);

        OnDiskSA onDisk = new OnDiskSA(index, UTF8Type.instance);

        // first check if we can find exact matches
        for (Map.Entry<ByteBuffer, NavigableMap<DecoratedKey, Integer>> e : data.entrySet())
        {
            if (UTF8Type.instance.getString(e.getKey()).equals("cat"))
                continue; // cat is embedded into scat, we'll test it in next section

            Assert.assertEquals("Key was: " + UTF8Type.instance.compose(e.getKey()), convert(e.getValue()), onDisk.search(e.getKey()));
        }

        // check that cat returns positions for scat & cat
        Assert.assertEquals(RoaringBitmap.bitmapOf(1, 4), onDisk.search(UTF8Type.instance.fromString("cat")));

        // random suffix queries
        Assert.assertEquals(RoaringBitmap.bitmapOf(9, 10), onDisk.search(UTF8Type.instance.fromString("ar")));
        Assert.assertEquals(RoaringBitmap.bitmapOf(1, 2, 3, 4), onDisk.search(UTF8Type.instance.fromString("at")));
        Assert.assertEquals(RoaringBitmap.bitmapOf(1, 11, 12), onDisk.search(UTF8Type.instance.fromString("mic")));
        Assert.assertEquals(RoaringBitmap.bitmapOf(1, 11, 12), onDisk.search(UTF8Type.instance.fromString("ae")));
        Assert.assertEquals(RoaringBitmap.bitmapOf(2, 5, 6), onDisk.search(UTF8Type.instance.fromString("ll")));
        Assert.assertEquals(RoaringBitmap.bitmapOf(1, 2, 5, 6, 11, 12), onDisk.search(UTF8Type.instance.fromString("l")));
        Assert.assertEquals(RoaringBitmap.bitmapOf(7), onDisk.search(UTF8Type.instance.fromString("oo")));
        Assert.assertEquals(RoaringBitmap.bitmapOf(7), onDisk.search(UTF8Type.instance.fromString("o")));
        Assert.assertEquals(RoaringBitmap.bitmapOf(1, 2, 3, 4, 6), onDisk.search(UTF8Type.instance.fromString("t")));

        Assert.assertEquals(null, onDisk.search(UTF8Type.instance.decompose("hello")));

        onDisk.close();
    }

    @Test
    public void testIntegerSAConstruction() throws Exception
    {
        final Map<ByteBuffer, NavigableMap<DecoratedKey, Integer>> data = new HashMap<ByteBuffer, NavigableMap<DecoratedKey, Integer>>()
        {{
                put(Int32Type.instance.decompose(5),  keyBuilder(1));
                put(Int32Type.instance.decompose(7),  keyBuilder(2));
                put(Int32Type.instance.decompose(1),  keyBuilder(3));
                put(Int32Type.instance.decompose(3),  keyBuilder(1, 4));
                put(Int32Type.instance.decompose(8),  keyBuilder(2, 6));
                put(Int32Type.instance.decompose(10), keyBuilder(5));
                put(Int32Type.instance.decompose(6),  keyBuilder(7));
                put(Int32Type.instance.decompose(4),  keyBuilder(9, 10));
                put(Int32Type.instance.decompose(0),  keyBuilder(11, 12, 1));
        }};

        OnDiskSABuilder builder = new OnDiskSABuilder(UTF8Type.instance, Int32Type.instance, OnDiskSABuilder.Mode.ORIGINAL);
        for (Map.Entry<ByteBuffer, NavigableMap<DecoratedKey, Integer>> e : data.entrySet())
            builder.add(e.getKey(), e.getValue());

        File index = File.createTempFile("on-disk-sa-int", "db");
        index.deleteOnExit();

        builder.finish(Pair.create(keyAt(1), keyAt(12)), index);

        OnDiskSA onDisk = new OnDiskSA(index, Int32Type.instance);

        for (Map.Entry<ByteBuffer, NavigableMap<DecoratedKey, Integer>> e : data.entrySet())
        {
            Assert.assertEquals(convert(e.getValue()), onDisk.search(e.getKey()));
        }

        List<ByteBuffer> sortedNumbers = new ArrayList<ByteBuffer>()
        {{
                for (ByteBuffer num : data.keySet())
                    add(num);
        }};

        Collections.sort(sortedNumbers, new Comparator<ByteBuffer>()
        {
            @Override
            public int compare(ByteBuffer a, ByteBuffer b)
            {
                return Int32Type.instance.compare(a, b);
            }
        });

        // test full iteration
        int idx = 0;
        for (OnDiskSA.DataSuffix suffix : onDisk)
        {
            ByteBuffer number = sortedNumbers.get(idx++);
            Assert.assertEquals(number, suffix.getSuffix());
            Assert.assertEquals(convert(data.get(number)), suffix.getOffsets());
        }

        // test partial iteration (descending)
        idx = 3; // start from the 3rd element
        Iterator<OnDiskSA.DataSuffix> partialIter = onDisk.iteratorAt(sortedNumbers.get(idx), OnDiskSA.IteratorOrder.DESC, true);
        while (partialIter.hasNext())
        {
            OnDiskSA.DataSuffix suffix = partialIter.next();
            ByteBuffer number = sortedNumbers.get(idx++);

            Assert.assertEquals(number, suffix.getSuffix());
            Assert.assertEquals(convert(data.get(number)), suffix.getOffsets());
        }

        idx = 3; // start from the 3rd element exclusive
        partialIter = onDisk.iteratorAt(sortedNumbers.get(idx++), OnDiskSA.IteratorOrder.DESC, false);
        while (partialIter.hasNext())
        {
            OnDiskSA.DataSuffix suffix = partialIter.next();
            ByteBuffer number = sortedNumbers.get(idx++);

            Assert.assertEquals(number, suffix.getSuffix());
            Assert.assertEquals(convert(data.get(number)), suffix.getOffsets());
        }

        // test partial iteration (ascending)
        idx = 6; // start from the 6rd element
        partialIter = onDisk.iteratorAt(sortedNumbers.get(idx), OnDiskSA.IteratorOrder.ASC, true);
        while (partialIter.hasNext())
        {
            OnDiskSA.DataSuffix suffix = partialIter.next();
            ByteBuffer number = sortedNumbers.get(idx--);

            Assert.assertEquals(number, suffix.getSuffix());
            Assert.assertEquals(convert(data.get(number)), suffix.getOffsets());
        }

        idx = 6; // start from the 6rd element exclusive
        partialIter = onDisk.iteratorAt(sortedNumbers.get(idx--), OnDiskSA.IteratorOrder.ASC, false);
        while (partialIter.hasNext())
        {
            OnDiskSA.DataSuffix suffix = partialIter.next();
            ByteBuffer number = sortedNumbers.get(idx--);

            Assert.assertEquals(number, suffix.getSuffix());
            Assert.assertEquals(convert(data.get(number)), suffix.getOffsets());
        }

        onDisk.close();

        List<ByteBuffer> iterCheckNums = new ArrayList<ByteBuffer>()
        {{
            add(Int32Type.instance.decompose(3));
            add(Int32Type.instance.decompose(9));
            add(Int32Type.instance.decompose(14));
            add(Int32Type.instance.decompose(42));
        }};

        OnDiskSABuilder iterTest = new OnDiskSABuilder(UTF8Type.instance, Int32Type.instance, OnDiskSABuilder.Mode.ORIGINAL);
        for (int i = 0; i < iterCheckNums.size(); i++)
            iterTest.add(iterCheckNums.get(i), keyBuilder(i));

        File iterIndex = File.createTempFile("sa-iter", ".db");
        iterIndex.deleteOnExit();

        iterTest.finish(Pair.create(keyAt(0), keyAt(4)), iterIndex);

        onDisk = new OnDiskSA(iterIndex, Int32Type.instance);

        ByteBuffer number = Int32Type.instance.decompose(1);
        Assert.assertEquals(0, Iterators.size(onDisk.iteratorAt(number, OnDiskSA.IteratorOrder.ASC, false)));
        Assert.assertEquals(0, Iterators.size(onDisk.iteratorAt(number, OnDiskSA.IteratorOrder.ASC, true)));
        Assert.assertEquals(4, Iterators.size(onDisk.iteratorAt(number, OnDiskSA.IteratorOrder.DESC, false)));
        Assert.assertEquals(4, Iterators.size(onDisk.iteratorAt(number, OnDiskSA.IteratorOrder.DESC, true)));

        number = Int32Type.instance.decompose(44);
        Assert.assertEquals(4, Iterators.size(onDisk.iteratorAt(number, OnDiskSA.IteratorOrder.ASC, false)));
        Assert.assertEquals(4, Iterators.size(onDisk.iteratorAt(number, OnDiskSA.IteratorOrder.ASC, true)));
        Assert.assertEquals(0, Iterators.size(onDisk.iteratorAt(number, OnDiskSA.IteratorOrder.DESC, false)));
        Assert.assertEquals(0, Iterators.size(onDisk.iteratorAt(number, OnDiskSA.IteratorOrder.DESC, true)));

        number = Int32Type.instance.decompose(20);
        Assert.assertEquals(3, Iterators.size(onDisk.iteratorAt(number, OnDiskSA.IteratorOrder.ASC, false)));
        Assert.assertEquals(3, Iterators.size(onDisk.iteratorAt(number, OnDiskSA.IteratorOrder.ASC, true)));
        Assert.assertEquals(1, Iterators.size(onDisk.iteratorAt(number, OnDiskSA.IteratorOrder.DESC, false)));
        Assert.assertEquals(1, Iterators.size(onDisk.iteratorAt(number, OnDiskSA.IteratorOrder.DESC, true)));

        number = Int32Type.instance.decompose(5);
        Assert.assertEquals(1, Iterators.size(onDisk.iteratorAt(number, OnDiskSA.IteratorOrder.ASC, false)));
        Assert.assertEquals(1, Iterators.size(onDisk.iteratorAt(number, OnDiskSA.IteratorOrder.ASC, true)));
        Assert.assertEquals(3, Iterators.size(onDisk.iteratorAt(number, OnDiskSA.IteratorOrder.DESC, false)));
        Assert.assertEquals(3, Iterators.size(onDisk.iteratorAt(number, OnDiskSA.IteratorOrder.DESC, true)));

        number = Int32Type.instance.decompose(10);
        Assert.assertEquals(2, Iterators.size(onDisk.iteratorAt(number, OnDiskSA.IteratorOrder.ASC, false)));
        Assert.assertEquals(2, Iterators.size(onDisk.iteratorAt(number, OnDiskSA.IteratorOrder.ASC, true)));
        Assert.assertEquals(2, Iterators.size(onDisk.iteratorAt(number, OnDiskSA.IteratorOrder.DESC, false)));
        Assert.assertEquals(2, Iterators.size(onDisk.iteratorAt(number, OnDiskSA.IteratorOrder.DESC, true)));

        onDisk.close();
    }

    @Test
    public void testMultiSuffixMatches() throws Exception
    {
        OnDiskSABuilder builder = new OnDiskSABuilder(UTF8Type.instance, UTF8Type.instance, OnDiskSABuilder.Mode.SUFFIX)
        {{
                add(UTF8Type.instance.decompose("Eliza"), keyBuilder(1, 2));
                add(UTF8Type.instance.decompose("Elizabeth"), keyBuilder(3, 4));
                add(UTF8Type.instance.decompose("Aliza"), keyBuilder(5, 6));
                add(UTF8Type.instance.decompose("Taylor"), keyBuilder(7, 8));
                add(UTF8Type.instance.decompose("Pavel"), keyBuilder(9, 10));
        }};

        File index = File.createTempFile("on-disk-sa-multi-suffix-match", ".db");
        index.deleteOnExit();

        builder.finish(Pair.create(keyAt(1), keyAt(10)), index);

        OnDiskSA onDisk = new OnDiskSA(index, UTF8Type.instance);

        Assert.assertEquals(bitMapOf(1, 2, 3, 4, 5, 6), onDisk.search(UTF8Type.instance.decompose("liz")));
        Assert.assertEquals(bitMapOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), onDisk.search(UTF8Type.instance.decompose("a")));
        Assert.assertEquals(bitMapOf(5, 6), onDisk.search(UTF8Type.instance.decompose("A")));
        Assert.assertEquals(bitMapOf(1, 2, 3, 4), onDisk.search(UTF8Type.instance.decompose("E")));
        Assert.assertEquals(bitMapOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), onDisk.search(UTF8Type.instance.decompose("l")));
        Assert.assertEquals(bitMapOf(3, 4), onDisk.search(UTF8Type.instance.decompose("bet")));
        Assert.assertEquals(bitMapOf(3, 4, 9, 10), onDisk.search(UTF8Type.instance.decompose("e")));
        Assert.assertEquals(bitMapOf(7, 8), onDisk.search(UTF8Type.instance.decompose("yl")));
        Assert.assertEquals(bitMapOf(7, 8), onDisk.search(UTF8Type.instance.decompose("T")));
        Assert.assertEquals(bitMapOf(1, 2, 3, 4, 5, 6), onDisk.search(UTF8Type.instance.decompose("za")));
        Assert.assertEquals(bitMapOf(3, 4), onDisk.search(UTF8Type.instance.decompose("ab")));

        Assert.assertNull(onDisk.search(UTF8Type.instance.decompose("Pi")));
        Assert.assertNull(onDisk.search(UTF8Type.instance.decompose("ethz")));
        Assert.assertNull(onDisk.search(UTF8Type.instance.decompose("liw")));
        Assert.assertNull(onDisk.search(UTF8Type.instance.decompose("Taw")));
        Assert.assertNull(onDisk.search(UTF8Type.instance.decompose("Av")));

        onDisk.close();
    }

    /*
    @Test
    public void testRandomLookupPerformance() throws Exception
    {
        final int NUM_ELEMENTS = 1000000;
        final Random random = new Random();
        final long testStartMs = System.currentTimeMillis();

        long start = System.nanoTime();
        OnDiskSABuilder builder = new OnDiskSABuilder(Int32Type.instance, OnDiskSABuilder.Mode.ORIGINAL);
        for (int i = 0; i < NUM_ELEMENTS; i++)
            builder.add(LongType.instance.decompose(testStartMs + i), bitMapOf(i));

        File index = File.createTempFile("on-disk-sa-lookup", "db");
        index.deleteOnExit();

        System.out.println("building of the SA took: " + (System.nanoTime() - start) + " ns.");

        start = System.nanoTime();
        builder.finish(index);
        System.out.println("finish of the SA took: " + (System.nanoTime() - start) + " ns.");

        System.out.println("File size => " + index.length());

        OnDiskSA onDisk = new OnDiskSA(index, Int32Type.instance);

        Histogram h = Metrics.newHistogram(OnDiskSATest.class, "x");
        for (int i = 0; i < NUM_ELEMENTS; i++)
        {
            int idx = random.nextInt(NUM_ELEMENTS);
            ByteBuffer key = LongType.instance.decompose(testStartMs + idx);

            start = System.nanoTime();
            RoaringBitmap result = onDisk.search(key.duplicate());
            h.update(System.nanoTime() - start);

            Assert.assertTrue(result.equals(bitMapOf(idx)));
        }

        Snapshot s = h.getSnapshot();
        System.out.printf("performance (random lookup): median: %f, p75: %f, p95: %f, p98: %f, p99: %f, p999: %f %n",
                s.getMedian(), s.get75thPercentile(), s.get95thPercentile(), s.get98thPercentile(), s.get99thPercentile(), s.get999thPercentile());

        onDisk.close();
    }
    */

    private static RoaringBitmap convert(NavigableMap<DecoratedKey, Integer> keys)
    {
        RoaringBitmap bitmap = new RoaringBitmap();
        for (Integer offset : keys.values())
            bitmap.add(offset);

        return bitmap;
    }

    private static DecoratedKey keyAt(int key)
    {
        return StorageService.getPartitioner().decorateKey(ByteBuffer.wrap(("key" + key).getBytes()));
    }

    private static NavigableMap<DecoratedKey, Integer> keyBuilder(Integer... keys)
    {
        NavigableMap<DecoratedKey, Integer> builder = new TreeMap<>(new Comparator<DecoratedKey>()
        {
            @Override
            public int compare(DecoratedKey a, DecoratedKey b)
            {
                long tokenA = MurmurHash.hash2_64(a.key, a.key.position(), a.key.remaining(), 0);
                long tokenB = MurmurHash.hash2_64(b.key, b.key.position(), b.key.remaining(), 0);

                return Long.compare(tokenA, tokenB);
            }
        });

        for (Integer key : keys)
            builder.put(keyAt(key), key);

        return builder;
    }

    private static RoaringBitmap bitMapOf(int... values)
    {
        RoaringBitmap map = new RoaringBitmap();
        for (int v : values)
            map.add(v);
        return map;
    }
}
