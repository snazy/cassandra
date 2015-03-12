package org.apache.cassandra.db.index.search;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.search.container.TokenTree;
import org.apache.cassandra.db.index.search.container.TokenTreeBuilder;
import org.apache.cassandra.db.index.search.plan.Expression;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.utils.Pair;

import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import junit.framework.Assert;
import org.junit.Test;


public class OnDiskSATest
{
    @Test
    public void testStringSAConstruction() throws Exception
    {
        Map<ByteBuffer, TokenTreeBuilder> data = new HashMap<ByteBuffer, TokenTreeBuilder>()
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
        for (Map.Entry<ByteBuffer, TokenTreeBuilder> e : data.entrySet())
            builder.add(e.getKey(), e.getValue());

        File index = File.createTempFile("on-disk-sa-string", "db");
        index.deleteOnExit();

        builder.finish(Pair.create(keyAt(1), keyAt(12)), index);

        OnDiskSA onDisk = new OnDiskSA(index, UTF8Type.instance, new KeyConverter());

        // first check if we can find exact matches
        for (Map.Entry<ByteBuffer, TokenTreeBuilder> e : data.entrySet())
        {
            if (UTF8Type.instance.getString(e.getKey()).equals("cat"))
                continue; // cat is embedded into scat, we'll test it in next section

            Assert.assertEquals("Key was: " + UTF8Type.instance.compose(e.getKey()), convert(e.getValue()), convert(onDisk.search(expressionFor(e.getKey()))));
        }

        // check that cat returns positions for scat & cat
        Assert.assertEquals(convert(1, 4), convert(onDisk.search(expressionFor("cat"))));

        // random suffix queries
        Assert.assertEquals(convert(9, 10), convert(onDisk.search(expressionFor("ar"))));
        Assert.assertEquals(convert(1, 2, 3, 4), convert(onDisk.search(expressionFor("at"))));
        Assert.assertEquals(convert(1, 11, 12), convert(onDisk.search(expressionFor("mic"))));
        Assert.assertEquals(convert(1, 11, 12), convert(onDisk.search(expressionFor("ae"))));
        Assert.assertEquals(convert(2, 5, 6), convert(onDisk.search(expressionFor("ll"))));
        Assert.assertEquals(convert(1, 2, 5, 6, 11, 12), convert(onDisk.search(expressionFor("l"))));
        Assert.assertEquals(convert(7), convert(onDisk.search(expressionFor("oo"))));
        Assert.assertEquals(convert(7), convert(onDisk.search(expressionFor("o"))));
        Assert.assertEquals(convert(1, 2, 3, 4, 6), convert(onDisk.search(expressionFor("t"))));

        Assert.assertEquals(Collections.<DecoratedKey>emptySet(), convert(onDisk.search(expressionFor("hello"))));

        onDisk.close();
    }

    @Test
    public void testIntegerSAConstruction() throws Exception
    {
        final Map<ByteBuffer, TokenTreeBuilder> data = new HashMap<ByteBuffer, TokenTreeBuilder>()
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
        for (Map.Entry<ByteBuffer, TokenTreeBuilder> e : data.entrySet())
            builder.add(e.getKey(), e.getValue());

        File index = File.createTempFile("on-disk-sa-int", "db");
        index.deleteOnExit();

        builder.finish(Pair.create(keyAt(1), keyAt(12)), index);

        OnDiskSA onDisk = new OnDiskSA(index, Int32Type.instance, new KeyConverter());

        for (Map.Entry<ByteBuffer, TokenTreeBuilder> e : data.entrySet())
        {
            Assert.assertEquals(convert(e.getValue()), convert(onDisk.search(expressionFor(e.getKey()))));
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
            Assert.assertEquals(convert(data.get(number)), convert(suffix.getTokens()));
        }

        // test partial iteration (descending)
        idx = 3; // start from the 3rd element
        Iterator<OnDiskSA.DataSuffix> partialIter = onDisk.iteratorAt(sortedNumbers.get(idx), OnDiskSA.IteratorOrder.DESC, true);
        while (partialIter.hasNext())
        {
            OnDiskSA.DataSuffix suffix = partialIter.next();
            ByteBuffer number = sortedNumbers.get(idx++);

            Assert.assertEquals(number, suffix.getSuffix());
            Assert.assertEquals(convert(data.get(number)), convert(suffix.getTokens()));
        }

        idx = 3; // start from the 3rd element exclusive
        partialIter = onDisk.iteratorAt(sortedNumbers.get(idx++), OnDiskSA.IteratorOrder.DESC, false);
        while (partialIter.hasNext())
        {
            OnDiskSA.DataSuffix suffix = partialIter.next();
            ByteBuffer number = sortedNumbers.get(idx++);

            Assert.assertEquals(number, suffix.getSuffix());
            Assert.assertEquals(convert(data.get(number)), convert(suffix.getTokens()));
        }

        // test partial iteration (ascending)
        idx = 6; // start from the 6rd element
        partialIter = onDisk.iteratorAt(sortedNumbers.get(idx), OnDiskSA.IteratorOrder.ASC, true);
        while (partialIter.hasNext())
        {
            OnDiskSA.DataSuffix suffix = partialIter.next();
            ByteBuffer number = sortedNumbers.get(idx--);

            Assert.assertEquals(number, suffix.getSuffix());
            Assert.assertEquals(convert(data.get(number)), convert(suffix.getTokens()));
        }

        idx = 6; // start from the 6rd element exclusive
        partialIter = onDisk.iteratorAt(sortedNumbers.get(idx--), OnDiskSA.IteratorOrder.ASC, false);
        while (partialIter.hasNext())
        {
            OnDiskSA.DataSuffix suffix = partialIter.next();
            ByteBuffer number = sortedNumbers.get(idx--);

            Assert.assertEquals(number, suffix.getSuffix());
            Assert.assertEquals(convert(data.get(number)), convert(suffix.getTokens()));
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

        Assert.assertEquals(convert(1, 2, 3, 4, 5, 6), convert(onDisk.search(expressionFor("liz"))));
        Assert.assertEquals(convert(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), convert(onDisk.search(expressionFor("a"))));
        Assert.assertEquals(convert(5, 6), convert(onDisk.search(expressionFor("A"))));
        Assert.assertEquals(convert(1, 2, 3, 4), convert(onDisk.search(expressionFor("E"))));
        Assert.assertEquals(convert(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), convert(onDisk.search(expressionFor("l"))));
        Assert.assertEquals(convert(3, 4), convert(onDisk.search(expressionFor("bet"))));
        Assert.assertEquals(convert(3, 4, 9, 10), convert(onDisk.search(expressionFor("e"))));
        Assert.assertEquals(convert(7, 8), convert(onDisk.search(expressionFor("yl"))));
        Assert.assertEquals(convert(7, 8), convert(onDisk.search(expressionFor("T"))));
        Assert.assertEquals(convert(1, 2, 3, 4, 5, 6), convert(onDisk.search(expressionFor("za"))));
        Assert.assertEquals(convert(3, 4), convert(onDisk.search(expressionFor("ab"))));

        Assert.assertEquals(Collections.<DecoratedKey>emptySet(), convert(onDisk.search(expressionFor("Pi"))));
        Assert.assertEquals(Collections.<DecoratedKey>emptySet(), convert(onDisk.search(expressionFor("ethz"))));
        Assert.assertEquals(Collections.<DecoratedKey>emptySet(), convert(onDisk.search(expressionFor("liw"))));
        Assert.assertEquals(Collections.<DecoratedKey>emptySet(), convert(onDisk.search(expressionFor("Taw"))));
        Assert.assertEquals(Collections.<DecoratedKey>emptySet(), convert(onDisk.search(expressionFor("Av"))));

        onDisk.close();
    }

    @Test
    public void testSparseMode() throws Exception
    {
        OnDiskSABuilder builder = new OnDiskSABuilder(LongType.instance, OnDiskSABuilder.Mode.SPARSE);

        final long start = System.currentTimeMillis();
        final int numIterations = 100000;

        for (long i = 0; i < numIterations; i++)
            builder.add(LongType.instance.decompose(start + i), keyBuilder(i));

        File index = File.createTempFile("on-disk-sa-sparse", "db");
        index.deleteOnExit();

        builder.finish(Pair.create(keyAt(start), keyAt(start + numIterations)), index);

        OnDiskSA onDisk = new OnDiskSA(index, LongType.instance, new KeyConverter());

        ThreadLocalRandom random = ThreadLocalRandom.current();

        for (long step = start; step < (start + numIterations); step += 1000)
        {
            boolean lowerInclusive = random.nextBoolean();
            boolean upperInclusive = random.nextBoolean();

            long limit = random.nextLong(step, start + numIterations);
            SkippableIterator<Long, TokenTree.Token> rows = onDisk.search(expressionFor(step, lowerInclusive, limit, upperInclusive));

            long lowerKey = step - start;
            long upperKey = lowerKey + (limit - step);

            if (!lowerInclusive)
                lowerKey += 1;

            if (upperInclusive)
                upperKey += 1;

            Set<DecoratedKey> actual = convert(rows);
            for (long key = lowerKey; key < upperKey; key++)
                Assert.assertTrue("key" + key + " wasn't found", actual.contains(keyAt(key)));

            Assert.assertEquals((upperKey - lowerKey), actual.size());
        }

        // let's also explicitly test whole range search
        SkippableIterator<Long, TokenTree.Token> rows = onDisk.search(expressionFor(start, true, start + numIterations, true));

        Set<DecoratedKey> actual = convert(rows);
        Assert.assertEquals(numIterations, actual.size());
    }

    @Test
    public void testNotEqualsQueryForStrings() throws Exception
    {
        Map<ByteBuffer, TokenTreeBuilder> data = new HashMap<ByteBuffer, TokenTreeBuilder>()
        {{
                put(UTF8Type.instance.decompose("Pavel"),   keyBuilder(1L, 2L));
                put(UTF8Type.instance.decompose("Jason"),   keyBuilder(3L));
                put(UTF8Type.instance.decompose("Jordan"),  keyBuilder(4L));
                put(UTF8Type.instance.decompose("Michael"), keyBuilder(5L, 6L));
                put(UTF8Type.instance.decompose("Vijay"),   keyBuilder(7L));
                put(UTF8Type.instance.decompose("Travis"),  keyBuilder(8L));
                put(UTF8Type.instance.decompose("Aleksey"), keyBuilder(9L, 10L));
        }};

        OnDiskSABuilder builder = new OnDiskSABuilder(UTF8Type.instance, OnDiskSABuilder.Mode.ORIGINAL);
        for (Map.Entry<ByteBuffer, TokenTreeBuilder> e : data.entrySet())
            builder.add(e.getKey(), e.getValue());

        File index = File.createTempFile("on-disk-sa-except-test", "db");
        index.deleteOnExit();

        builder.finish(Pair.create(keyAt(1), keyAt(10)), index);

        OnDiskSA onDisk = new OnDiskSA(index, UTF8Type.instance, new KeyConverter());

        // test whole words first
        Assert.assertEquals(convert(3, 4, 5, 6, 7, 8, 9, 10), convert(onDisk.search(expressionForNot("Pavel"))));

        Assert.assertEquals(convert(3, 4, 7, 8, 9, 10), convert(onDisk.search(expressionForNot("Pavel", "Michael"))));

        Assert.assertEquals(convert(3, 4, 7, 9, 10), convert(onDisk.search(expressionForNot("Pavel", "Michael", "Travis"))));

        // now test prefixes
        Assert.assertEquals(convert(3, 4, 5, 6, 7, 8, 9, 10), convert(onDisk.search(expressionForNot("Pav"))));

        Assert.assertEquals(convert(3, 4, 7, 8, 9, 10), convert(onDisk.search(expressionForNot("Pavel", "Mic"))));

        Assert.assertEquals(convert(3, 4, 7, 9, 10), convert(onDisk.search(expressionForNot("Pavel", "Micha", "Tr"))));

        onDisk.close();
    }

    @Test
    public void testNotEqualsQueryForNumbers() throws Exception
    {
        final Map<ByteBuffer, TokenTreeBuilder> data = new HashMap<ByteBuffer, TokenTreeBuilder>()
        {{
                put(Int32Type.instance.decompose(5),  keyBuilder(1L));
                put(Int32Type.instance.decompose(7),  keyBuilder(2L));
                put(Int32Type.instance.decompose(1),  keyBuilder(3L));
                put(Int32Type.instance.decompose(3),  keyBuilder(1L, 4L));
                put(Int32Type.instance.decompose(8),  keyBuilder(8L, 6L));
                put(Int32Type.instance.decompose(10), keyBuilder(5L));
                put(Int32Type.instance.decompose(6),  keyBuilder(7L));
                put(Int32Type.instance.decompose(4),  keyBuilder(9L, 10L));
                put(Int32Type.instance.decompose(0),  keyBuilder(11L, 12L, 1L));
        }};

        OnDiskSABuilder builder = new OnDiskSABuilder(Int32Type.instance, OnDiskSABuilder.Mode.ORIGINAL);
        for (Map.Entry<ByteBuffer, TokenTreeBuilder> e : data.entrySet())
            builder.add(e.getKey(), e.getValue());

        File index = File.createTempFile("on-disk-sa-except-int-test", "db");
        index.deleteOnExit();

        builder.finish(Pair.create(keyAt(1), keyAt(12)), index);

        OnDiskSA onDisk = new OnDiskSA(index, Int32Type.instance, new KeyConverter());

        Assert.assertEquals(convert(1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12), convert(onDisk.search(expressionForNot(1))));
        Assert.assertEquals(convert(1, 2, 4, 5, 7, 9, 10, 11, 12), convert(onDisk.search(expressionForNot(1, 8))));
        Assert.assertEquals(convert(1, 2, 4, 5, 7, 11, 12), convert(onDisk.search(expressionForNot(1, 8, 4))));

        onDisk.close();
    }

    @Test
    public void testRangeQueryWithExclusions() throws Exception
    {
        final long lower = 0;
        final long upper = 100000;

        OnDiskSABuilder builder = new OnDiskSABuilder(LongType.instance, OnDiskSABuilder.Mode.SPARSE);
        for (long i = lower; i <= upper; i++)
            builder.add(LongType.instance.decompose(i), keyBuilder(i));

        File index = File.createTempFile("on-disk-sa-except-long-ranges", "db");
        index.deleteOnExit();

        builder.finish(Pair.create(keyAt(lower), keyAt(upper)), index);

        OnDiskSA onDisk = new OnDiskSA(index, LongType.instance, new KeyConverter());

        ThreadLocalRandom random = ThreadLocalRandom.current();

        // single exclusion

        // let's do small range first to figure out if searchPoint works properly
        validateExclusions(onDisk, lower, 50, Sets.newHashSet(42L));
        // now let's do whole data set to test SPARSE searching
        validateExclusions(onDisk, lower, upper, Sets.newHashSet(31337L));

        // pair of exclusions which would generate a split

        validateExclusions(onDisk, lower, random.nextInt(400, 800), Sets.newHashSet(42L, 154L));
        validateExclusions(onDisk, lower, upper, Sets.newHashSet(31337L, 54631L));

        // 3 exclusions which would generate a split and change bounds

        validateExclusions(onDisk, lower, random.nextInt(400, 800), Sets.newHashSet(42L, 154L));
        validateExclusions(onDisk, lower, upper, Sets.newHashSet(31337L, 54631L));

        validateExclusions(onDisk, lower, random.nextLong(400, upper), Sets.newHashSet(42L, 55L));
        validateExclusions(onDisk, lower, random.nextLong(400, upper), Sets.newHashSet(42L, 55L, 93L));
        validateExclusions(onDisk, lower, random.nextLong(400, upper), Sets.newHashSet(42L, 55L, 93L, 205L));

        Set<Long> exclusions = Sets.newHashSet(3L, 12L, 13L, 14L, 27L, 54L, 81L, 125L, 384L, 771L, 1054L, 2048L, 78834L);

        // test that exclusions are properly bound by lower/upper of the expression
        Assert.assertEquals(392, validateExclusions(onDisk, lower, 400, exclusions, false));
        Assert.assertEquals(101, validateExclusions(onDisk, lower, 100, Sets.newHashSet(-10L, -5L, -1L), false));

        validateExclusions(onDisk, lower, upper, exclusions);

        Assert.assertEquals(100000, convert(onDisk.search(new Expression(null, UTF8Type.instance, LongType.instance)
                                                    .add(IndexOperator.NOT_EQ, LongType.instance.decompose(100L)))).size());

        Assert.assertEquals(49, convert(onDisk.search(new Expression(null, UTF8Type.instance, LongType.instance)
                                                    .add(IndexOperator.LT, LongType.instance.decompose(50L))
                                                    .add(IndexOperator.NOT_EQ, LongType.instance.decompose(10L)))).size());

        Assert.assertEquals(99998, convert(onDisk.search(new Expression(null, UTF8Type.instance, LongType.instance)
                                                    .add(IndexOperator.GT, LongType.instance.decompose(1L))
                                                    .add(IndexOperator.NOT_EQ, LongType.instance.decompose(20L)))).size());

        onDisk.close();
    }

    private void validateExclusions(OnDiskSA sa, long lower, long upper, Set<Long> exclusions)
    {
        validateExclusions(sa, lower, upper, exclusions, true);
    }

    private int validateExclusions(OnDiskSA sa, long lower, long upper, Set<Long> exclusions, boolean checkCount)
    {
        int count = 0;
        for (DecoratedKey key : convert(sa.search(rangeWithExclusions(lower, true, upper, true, exclusions))))
        {
            String keyId = UTF8Type.instance.getString(key.key).split("key")[1];
            Assert.assertFalse("key" + keyId + " is present.", exclusions.contains(Long.valueOf(keyId)));
            count++;
        }

        if (checkCount)
            Assert.assertEquals(upper - (lower == 0 ? -1 : lower) - exclusions.size(), count);

        return count;
    }

    @Test
    public void testDescriptor() throws Exception
    {
        final Map<ByteBuffer, TokenTreeBuilder> data = new HashMap<ByteBuffer, TokenTreeBuilder>()
        {{
                put(Int32Type.instance.decompose(5), keyBuilder(1L));
            }};

        OnDiskSABuilder builder1 = new OnDiskSABuilder(Int32Type.instance, OnDiskSABuilder.Mode.ORIGINAL);
        OnDiskSABuilder builder2 = new OnDiskSABuilder(Int32Type.instance, OnDiskSABuilder.Mode.ORIGINAL);
        for (Map.Entry<ByteBuffer, TokenTreeBuilder> e : data.entrySet())
        {
            builder1.add(e.getKey(), e.getValue());
            builder2.add(e.getKey(), e.getValue());
        }

        File index1 = File.createTempFile("on-disk-sa-int", "db");
        File index2 = File.createTempFile("on-disk-sa-int2", "db");
        index1.deleteOnExit();
        index2.deleteOnExit();

        builder1.finish(Pair.create(keyAt(1), keyAt(12)), index1);
        builder2.finish(new Descriptor(Descriptor.VERSION_AA), Pair.create(keyAt(1), keyAt(12)), index2);

        OnDiskSA onDisk1 = new OnDiskSA(index1, Int32Type.instance, new KeyConverter());
        OnDiskSA onDisk2 = new OnDiskSA(index2, Int32Type.instance, new KeyConverter());

        Assert.assertEquals(onDisk1.descriptor.version.version, Descriptor.CURRENT_VERSION);
    }

    @Test
    public void testSuperBlocks() throws Exception
    {
        Map<ByteBuffer, TokenTreeBuilder> terms = new HashMap<>();
        terms.put(UTF8Type.instance.decompose("1234"), keyBuilder(1L, 2L));
        terms.put(UTF8Type.instance.decompose("2345"), keyBuilder(3L, 4L));
        terms.put(UTF8Type.instance.decompose("3456"), keyBuilder(5L, 6L));
        terms.put(UTF8Type.instance.decompose("4567"), keyBuilder(7L, 8L));
        terms.put(UTF8Type.instance.decompose("5678"), keyBuilder(9L, 10L));

        OnDiskSABuilder builder = new OnDiskSABuilder(Int32Type.instance, OnDiskSABuilder.Mode.SPARSE);
        for (Map.Entry<ByteBuffer, TokenTreeBuilder> entry : terms.entrySet())
            builder.add(entry.getKey(), entry.getValue());

        File index = File.createTempFile("on-disk-sa-try-superblocks", ".db");
        index.deleteOnExit();

        builder.finish(Pair.create(keyAt(1), keyAt(10)), index);

        OnDiskSA onDisk = new OnDiskSA(index, Int32Type.instance, new KeyConverter());
        OnDiskSA.OnDiskSuperBlock superBlock = onDisk.dataLevel.getSuperBlock(0);
        Iterator<TokenTree.Token> iter = superBlock.iterator();

        Long lastToken = null;
        while (iter.hasNext())
        {
            TokenTree.Token token = iter.next();

            if (lastToken != null)
                Assert.assertTrue(lastToken.compareTo(token.get()) < 0);

            lastToken = token.get();
        }
    }

    @Test
    public void testSuperBlockRetrieval() throws Exception
    {
        OnDiskSABuilder builder = new OnDiskSABuilder(LongType.instance, OnDiskSABuilder.Mode.SPARSE);
        for (long i = 0; i < 100000; i++)
            builder.add(LongType.instance.decompose(i), keyBuilder((long) i));

        File index = File.createTempFile("on-disk-sa-multi-superblock-match", ".db");
        index.deleteOnExit();

        builder.finish(Pair.create(keyAt(0), keyAt(100000)), index);

        OnDiskSA onDiskSA = new OnDiskSA(index, LongType.instance, new KeyConverter());

        testSearchRangeWithSuperBlocks(onDiskSA, 0, 500);
        testSearchRangeWithSuperBlocks(onDiskSA, 300, 93456);
        testSearchRangeWithSuperBlocks(onDiskSA, 210, 1700);
        testSearchRangeWithSuperBlocks(onDiskSA, 530, 3200);

        Random random = new Random(0xdeadbeef);
        for (int i = 0; i < 100000; i += random.nextInt(1500)) // random steps with max of 1500 elements
        {
            for (int j = 0; j < 3; j++)
                testSearchRangeWithSuperBlocks(onDiskSA, i, ThreadLocalRandom.current().nextInt(i, 100000));
        }
    }

    private void testSearchRangeWithSuperBlocks(OnDiskSA onDiskSA, long start, long end)
    {
        SkippableIterator<Long, TokenTree.Token> tokens = onDiskSA.search(expressionFor(start, true, end, false));

        int keyCount = 0;
        Long lastToken = null;
        while (tokens.hasNext())
        {
            TokenTree.Token token = tokens.next();
            Iterator<DecoratedKey> keys = token.iterator();

            // each of the values should have exactly a single key
            Assert.assertTrue(keys.hasNext());
            keys.next();
            Assert.assertFalse(keys.hasNext());

            // and it's last should always smaller than current
            if (lastToken != null)
                Assert.assertTrue("last should be less than current", lastToken.compareTo(token.get()) < 0);

            lastToken = token.get();
            keyCount++;
        }

        Assert.assertEquals(end - start, keyCount);
    }

    private static DecoratedKey keyAt(long key)
    {
        return StorageService.getPartitioner().decorateKey(ByteBuffer.wrap(("key" + key).getBytes()));
    }

    private static TokenTreeBuilder keyBuilder(Long... keys)
    {
        TokenTreeBuilder builder = new TokenTreeBuilder(Descriptor.CURRENT);

        for (final Long key : keys)
        {
            DecoratedKey dk = keyAt(key);
            builder.add(Pair.create(MurmurHash.hash2_64(dk.key, dk.key.position(), dk.key.remaining(), 0), key));
        }

        return builder.finish();
    }

    private static Set<DecoratedKey> convert(TokenTreeBuilder offsets)
    {
        Set<DecoratedKey> result = new HashSet<>();

        Iterator<Pair<Long, LongSet>> offsetIter = offsets.iterator();
        while (offsetIter.hasNext())
        {
            LongSet v = offsetIter.next().right;

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
        Set<DecoratedKey> keys = new TreeSet<>(DecoratedKey.comparator);

        while (results.hasNext())
        {
            for (DecoratedKey key : results.next())
                keys.add(key);
        }

        return keys;
    }

    private static Expression expressionFor(long lower, boolean lowerInclusive, long upper, boolean upperInclusive)
    {
        Expression expression = new Expression(null, UTF8Type.instance, LongType.instance);
        expression.add(lowerInclusive ? IndexOperator.GTE : IndexOperator.GT, LongType.instance.decompose(lower));
        expression.add(upperInclusive ? IndexOperator.LTE : IndexOperator.LT, LongType.instance.decompose(upper));
        return expression;
    }

    private static Expression expressionFor(ByteBuffer term)
    {
        Expression expression = new Expression(null, UTF8Type.instance, UTF8Type.instance);
        expression.add(IndexOperator.EQ, term);
        return expression;
    }

    private static Expression expressionForNot(AbstractType<?> validator, Iterable<ByteBuffer> terms)
    {
        Expression expression = new Expression(null, UTF8Type.instance, validator);
        for (ByteBuffer term : terms)
            expression.add(IndexOperator.NOT_EQ, term);
        return expression;

    }

    private static Expression expressionForNot(Integer... terms)
    {
        return expressionForNot(Int32Type.instance, Iterables.transform(Arrays.asList(terms), new Function<Integer, ByteBuffer>()
        {
            @Override
            public ByteBuffer apply(Integer term)
            {
                return Int32Type.instance.decompose(term);
            }
        }));
    }

    private static Expression rangeWithExclusions(long lower, boolean lowerInclusive, long upper, boolean upperInclusive, Set<Long> exclusions)
    {
        Expression expression = expressionFor(lower, lowerInclusive, upper, upperInclusive);
        for (long e : exclusions)
            expression.add(IndexOperator.NOT_EQ, LongType.instance.decompose(e));

        return expression;
    }

    private static Expression expressionForNot(String... terms)
    {
        return expressionForNot(UTF8Type.instance, Iterables.transform(Arrays.asList(terms), new Function<String, ByteBuffer>()
        {
            @Override
            public ByteBuffer apply(String term)
            {
                return UTF8Type.instance.decompose(term);
            }
        }));
    }

    private static Expression expressionFor(String term)
    {
        return expressionFor(UTF8Type.instance.decompose(term));
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
