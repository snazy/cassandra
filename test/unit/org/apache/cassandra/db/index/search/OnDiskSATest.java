package org.apache.cassandra.db.index.search;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;

import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.google.common.base.Function;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.search.container.TokenTree;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.utils.Pair;

import com.google.common.collect.Iterators;

import junit.framework.Assert;
import org.junit.Test;


public class OnDiskSATest
{
    @Test
    public void testStringSAConstruction() throws Exception
    {
        Map<ByteBuffer, NavigableMap<Long, LongSet>> data = new HashMap<ByteBuffer, NavigableMap<Long, LongSet>>()
        {{
                put(UTF8Type.instance.decompose("scat"), keyBuilder(1L));
                put(UTF8Type.instance.decompose("mat"),  keyBuilder(2L));
                put(UTF8Type.instance.decompose("fat"),  keyBuilder(3L));
                put(UTF8Type.instance.decompose("cat"),  keyBuilder(1L, 4L));
                put(UTF8Type.instance.decompose("till"), keyBuilder(2L, 6L));
                put(UTF8Type.instance.decompose("bill"), keyBuilder(5L));
                put(UTF8Type.instance.decompose("foo"),  keyBuilder(7L));
                put(UTF8Type.instance.decompose("bar"),  keyBuilder(9L, 10L));
                put(UTF8Type.instance.decompose("michael"), keyBuilder(11L, 12L, 1L));
        }};

        OnDiskSABuilder builder = new OnDiskSABuilder(UTF8Type.instance, OnDiskSABuilder.Mode.SUFFIX);
        for (Map.Entry<ByteBuffer, NavigableMap<Long, LongSet>> e : data.entrySet())
            builder.add(e.getKey(), e.getValue());

        File index = File.createTempFile("on-disk-sa-string", "db");
        index.deleteOnExit();

        builder.finish(Pair.create(keyAt(1), keyAt(12)), index);

        OnDiskSA onDisk = new OnDiskSA(index, UTF8Type.instance, new KeyConverter());

        // first check if we can find exact matches
        for (Map.Entry<ByteBuffer, NavigableMap<Long, LongSet>> e : data.entrySet())
        {
            if (UTF8Type.instance.getString(e.getKey()).equals("cat"))
                continue; // cat is embedded into scat, we'll test it in next section

            Assert.assertEquals("Key was: " + UTF8Type.instance.compose(e.getKey()), convert(e.getValue()), convert(onDisk.search(e.getKey())));
        }

        // check that cat returns positions for scat & cat
        Assert.assertEquals(convert(1, 4), convert(onDisk.search(UTF8Type.instance.fromString("cat"))));

        // random suffix queries
        Assert.assertEquals(convert(9, 10), convert(onDisk.search(UTF8Type.instance.fromString("ar"))));
        Assert.assertEquals(convert(1, 2, 3, 4), convert(onDisk.search(UTF8Type.instance.fromString("at"))));
        Assert.assertEquals(convert(1, 11, 12), convert(onDisk.search(UTF8Type.instance.fromString("mic"))));
        Assert.assertEquals(convert(1, 11, 12), convert(onDisk.search(UTF8Type.instance.fromString("ae"))));
        Assert.assertEquals(convert(2, 5, 6), convert(onDisk.search(UTF8Type.instance.fromString("ll"))));
        Assert.assertEquals(convert(1, 2, 5, 6, 11, 12), convert(onDisk.search(UTF8Type.instance.fromString("l"))));
        Assert.assertEquals(convert(7), convert(onDisk.search(UTF8Type.instance.fromString("oo"))));
        Assert.assertEquals(convert(7), convert(onDisk.search(UTF8Type.instance.fromString("o"))));
        Assert.assertEquals(convert(1, 2, 3, 4, 6), convert(onDisk.search(UTF8Type.instance.fromString("t"))));

        Assert.assertEquals(Collections.emptySet(), convert(onDisk.search(UTF8Type.instance.decompose("hello"))));

        onDisk.close();
    }

    @Test
    public void testIntegerSAConstruction() throws Exception
    {
        final Map<ByteBuffer, NavigableMap<Long, LongSet>> data = new HashMap<ByteBuffer, NavigableMap<Long, LongSet>>()
        {{
                put(Int32Type.instance.decompose(5),  keyBuilder(1L));
                put(Int32Type.instance.decompose(7),  keyBuilder(2L));
                put(Int32Type.instance.decompose(1),  keyBuilder(3L));
                put(Int32Type.instance.decompose(3),  keyBuilder(1L, 4L));
                put(Int32Type.instance.decompose(8),  keyBuilder(2L, 6L));
                put(Int32Type.instance.decompose(10), keyBuilder(5L));
                put(Int32Type.instance.decompose(6),  keyBuilder(7L));
                put(Int32Type.instance.decompose(4),  keyBuilder(9L, 10L));
                put(Int32Type.instance.decompose(0),  keyBuilder(11L, 12L, 1L));
        }};

        OnDiskSABuilder builder = new OnDiskSABuilder(Int32Type.instance, OnDiskSABuilder.Mode.ORIGINAL);
        for (Map.Entry<ByteBuffer, NavigableMap<Long, LongSet>> e : data.entrySet())
            builder.add(e.getKey(), e.getValue());

        File index = File.createTempFile("on-disk-sa-int", "db");
        index.deleteOnExit();

        builder.finish(Pair.create(keyAt(1), keyAt(12)), index);

        OnDiskSA onDisk = new OnDiskSA(index, Int32Type.instance, new KeyConverter());

        for (Map.Entry<ByteBuffer, NavigableMap<Long, LongSet>> e : data.entrySet())
        {
            Assert.assertEquals(convert(e.getValue()), convert(onDisk.search(e.getKey())));
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
            Assert.assertEquals(convert(data.get(number)), convert(suffix.getOffsets()));
        }

        // test partial iteration (descending)
        idx = 3; // start from the 3rd element
        Iterator<OnDiskSA.DataSuffix> partialIter = onDisk.iteratorAt(sortedNumbers.get(idx), OnDiskSA.IteratorOrder.DESC, true);
        while (partialIter.hasNext())
        {
            OnDiskSA.DataSuffix suffix = partialIter.next();
            ByteBuffer number = sortedNumbers.get(idx++);

            Assert.assertEquals(number, suffix.getSuffix());
            Assert.assertEquals(convert(data.get(number)), convert(suffix.getOffsets()));
        }

        idx = 3; // start from the 3rd element exclusive
        partialIter = onDisk.iteratorAt(sortedNumbers.get(idx++), OnDiskSA.IteratorOrder.DESC, false);
        while (partialIter.hasNext())
        {
            OnDiskSA.DataSuffix suffix = partialIter.next();
            ByteBuffer number = sortedNumbers.get(idx++);

            Assert.assertEquals(number, suffix.getSuffix());
            Assert.assertEquals(convert(data.get(number)), convert(suffix.getOffsets()));
        }

        // test partial iteration (ascending)
        idx = 6; // start from the 6rd element
        partialIter = onDisk.iteratorAt(sortedNumbers.get(idx), OnDiskSA.IteratorOrder.ASC, true);
        while (partialIter.hasNext())
        {
            OnDiskSA.DataSuffix suffix = partialIter.next();
            ByteBuffer number = sortedNumbers.get(idx--);

            Assert.assertEquals(number, suffix.getSuffix());
            Assert.assertEquals(convert(data.get(number)), convert(suffix.getOffsets()));
        }

        idx = 6; // start from the 6rd element exclusive
        partialIter = onDisk.iteratorAt(sortedNumbers.get(idx--), OnDiskSA.IteratorOrder.ASC, false);
        while (partialIter.hasNext())
        {
            OnDiskSA.DataSuffix suffix = partialIter.next();
            ByteBuffer number = sortedNumbers.get(idx--);

            Assert.assertEquals(number, suffix.getSuffix());
            Assert.assertEquals(convert(data.get(number)), convert(suffix.getOffsets()));
        }

        onDisk.close();

        List<ByteBuffer> iterCheckNums = new ArrayList<ByteBuffer>()
        {{
            add(Int32Type.instance.decompose(3));
            add(Int32Type.instance.decompose(9));
            add(Int32Type.instance.decompose(14));
            add(Int32Type.instance.decompose(42));
        }};

        OnDiskSABuilder iterTest = new OnDiskSABuilder(Int32Type.instance, OnDiskSABuilder.Mode.ORIGINAL);
        for (int i = 0; i < iterCheckNums.size(); i++)
            iterTest.add(iterCheckNums.get(i), keyBuilder((long) i));

        File iterIndex = File.createTempFile("sa-iter", ".db");
        iterIndex.deleteOnExit();

        iterTest.finish(Pair.create(keyAt(0), keyAt(4)), iterIndex);

        onDisk = new OnDiskSA(iterIndex, Int32Type.instance, new KeyConverter());

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
        OnDiskSABuilder builder = new OnDiskSABuilder(UTF8Type.instance, OnDiskSABuilder.Mode.SUFFIX)
        {{
                add(UTF8Type.instance.decompose("Eliza"), keyBuilder(1L, 2L));
                add(UTF8Type.instance.decompose("Elizabeth"), keyBuilder(3L, 4L));
                add(UTF8Type.instance.decompose("Aliza"), keyBuilder(5L, 6L));
                add(UTF8Type.instance.decompose("Taylor"), keyBuilder(7L, 8L));
                add(UTF8Type.instance.decompose("Pavel"), keyBuilder(9L, 10L));
        }};

        File index = File.createTempFile("on-disk-sa-multi-suffix-match", ".db");
        index.deleteOnExit();

        builder.finish(Pair.create(keyAt(1), keyAt(10)), index);

        OnDiskSA onDisk = new OnDiskSA(index, UTF8Type.instance, new KeyConverter());

        Assert.assertEquals(convert(1, 2, 3, 4, 5, 6), convert(onDisk.search(UTF8Type.instance.decompose("liz"))));
        Assert.assertEquals(convert(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), convert(onDisk.search(UTF8Type.instance.decompose("a"))));
        Assert.assertEquals(convert(5, 6), convert(onDisk.search(UTF8Type.instance.decompose("A"))));
        Assert.assertEquals(convert(1, 2, 3, 4), convert(onDisk.search(UTF8Type.instance.decompose("E"))));
        Assert.assertEquals(convert(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), convert(onDisk.search(UTF8Type.instance.decompose("l"))));
        Assert.assertEquals(convert(3, 4), convert(onDisk.search(UTF8Type.instance.decompose("bet"))));
        Assert.assertEquals(convert(3, 4, 9, 10), convert(onDisk.search(UTF8Type.instance.decompose("e"))));
        Assert.assertEquals(convert(7, 8), convert(onDisk.search(UTF8Type.instance.decompose("yl"))));
        Assert.assertEquals(convert(7, 8), convert(onDisk.search(UTF8Type.instance.decompose("T"))));
        Assert.assertEquals(convert(1, 2, 3, 4, 5, 6), convert(onDisk.search(UTF8Type.instance.decompose("za"))));
        Assert.assertEquals(convert(3, 4), convert(onDisk.search(UTF8Type.instance.decompose("ab"))));

        Assert.assertEquals(Collections.emptySet(), convert(onDisk.search(UTF8Type.instance.decompose("Pi"))));
        Assert.assertEquals(Collections.emptySet(), convert(onDisk.search(UTF8Type.instance.decompose("ethz"))));
        Assert.assertEquals(Collections.emptySet(), convert(onDisk.search(UTF8Type.instance.decompose("liw"))));
        Assert.assertEquals(Collections.emptySet(), convert(onDisk.search(UTF8Type.instance.decompose("Taw"))));
        Assert.assertEquals(Collections.emptySet(), convert(onDisk.search(UTF8Type.instance.decompose("Av"))));

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

    @Test
    public void testSuperBlocks() throws Exception
    {
        Map<ByteBuffer, NavigableMap<Long, LongSet>> terms = new HashMap<>();
        terms.put(UTF8Type.instance.decompose("1234"), keyBuilder(1L, 2L));
        terms.put(UTF8Type.instance.decompose("2345"), keyBuilder(3L, 4L));
        terms.put(UTF8Type.instance.decompose("3456"), keyBuilder(5L, 6L));
        terms.put(UTF8Type.instance.decompose("4567"), keyBuilder(7L, 8L));
        terms.put(UTF8Type.instance.decompose("5678"), keyBuilder(9L, 10L));

        OnDiskSABuilder builder = new OnDiskSABuilder(Int32Type.instance, OnDiskSABuilder.Mode.ORIGINAL);
        for (Map.Entry<ByteBuffer, NavigableMap<Long, LongSet>> entry : terms.entrySet())
            builder.add(entry.getKey(), entry.getValue());

        File index = File.createTempFile("on-disk-sa-multi-suffix-match", ".db");
        index.deleteOnExit();

        builder.finish(Pair.create(keyAt(1), keyAt(10)), index);

        OnDiskSA onDisk = new OnDiskSA(index, UTF8Type.instance, new KeyConverter());
        OnDiskSA.OnDiskSuperBlock superBlock = onDisk.dataLevel.getSuperBlock(0);
        Iterator<TokenTree.Token> iter = superBlock.iterator();
        while (iter.hasNext())
        {
            TokenTree.Token token = iter.next();
            // TODO: add in an assert that is sensible, but for now, knowing that the super block
            // iterator can be loaded is a good first step
            //Assert.assertTrue(terms.containsKey(token));
        }
    }

    private static DecoratedKey keyAt(long key)
    {
        return StorageService.getPartitioner().decorateKey(ByteBuffer.wrap(("key" + key).getBytes()));
    }

    private static NavigableMap<Long, LongSet> keyBuilder(Long... keys)
    {
        NavigableMap<Long, LongSet> builder = new TreeMap<>();

        for (final Long key : keys)
        {
            DecoratedKey dk = keyAt(key);
            builder.put(MurmurHash.hash2_64(dk.key, dk.key.position(), dk.key.remaining(), 0), new LongOpenHashSet() {{ add(key); }});
        }

        return builder;
    }

    private static Set<DecoratedKey> convert(NavigableMap<Long, LongSet> offsets)
    {
        Set<DecoratedKey> result = new HashSet<>();
        for (LongSet v : offsets.values())
        {
            for (LongCursor offset : v)
                result.add(keyAt(offset.value));
        }
        return result;
    }

    private static Set<DecoratedKey> convert(long... keyOffsets)
    {
        Set<DecoratedKey> result = new HashSet<>();
        for (long offset : keyOffsets)
            result.add(keyAt(offset));

        return result;
    }

    private static Set<DecoratedKey> convert(SkippableIterator<Long, TokenTree.Token> results)
    {
        Set<DecoratedKey> keys = new HashSet<>();

        while (results.hasNext())
        {
            for (DecoratedKey key : results.next())
                keys.add(key);
        }

        return keys;
    }

    private static class KeyConverter implements Function<Long, DecoratedKey>
    {
        @Override
        public DecoratedKey apply(Long offset)
        {
            return keyAt(offset);
        }
    }
}
