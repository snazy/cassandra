package org.apache.cassandra.db.index.utils;

import java.io.IOException;
import java.util.*;

import org.apache.cassandra.utils.Pair;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.cassandra.db.index.utils.LazyMergeSortIterator.OperationType;

public class LazyMergeSortIteratorTest
{
    private static final Random random = new Random();

    @Test
    public void orLongIterators()
    {
        SkippableIterator<Long, LongValue> it1 = new TestSkippableIterator<>(convert(2L, 3L, 5L, 6L));
        SkippableIterator<Long, LongValue> it2 = new TestSkippableIterator<>(convert(1L, 7L));
        SkippableIterator<Long, LongValue> it3 = new TestSkippableIterator<>(convert(4L, 8L, 9L, 10L));
        List<SkippableIterator<Long, LongValue>> iterators = Arrays.asList(it1,it2,it3);

        List<LongValue> actual = new ArrayList<>();
        SkippableIterator<Long, LongValue> it = new LazyMergeSortIterator<>(OperationType.OR, iterators);
        while(it.hasNext())
            actual.add(it.next());

        Assert.assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), actual);
    }

    @Test
    public void orLongIteratorsWithOverlappingValues()
    {
        SkippableIterator<Long, LongValue> it1 = new TestSkippableIterator<>(convert(2L, 3L, 5L, 6L));
        SkippableIterator<Long, LongValue> it2 = new TestSkippableIterator<>(convert(1L, 4L, 6L, 7L));
        SkippableIterator<Long, LongValue> it3 = new TestSkippableIterator<>(convert(4L, 6L, 8L, 9L, 10L));
        List<SkippableIterator<Long, LongValue>> iterators = Arrays.asList(it1,it2,it3);

        List<LongValue> actual = new ArrayList<>();
        SkippableIterator<Long, LongValue> it = new LazyMergeSortIterator<>(OperationType.OR, iterators);
        while(it.hasNext())
            actual.add(it.next());

        Assert.assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), actual);
    }

    @Test
    public void andLongIterators()
    {
        SkippableIterator<Long, LongValue> it1 = new TestSkippableIterator<>(convert(2L, 3L, 4L, 5L, 6L, 9L));
        SkippableIterator<Long, LongValue> it2 = new TestSkippableIterator<>(convert(1L, 2L, 4L, 9L));
        SkippableIterator<Long, LongValue> it3 = new TestSkippableIterator<>(convert(2L, 4L, 7L, 8L, 9L, 10L));
        List<SkippableIterator<Long, LongValue>> iterators = Arrays.asList(it1,it2,it3);

        List<LongValue> actual = new ArrayList<>();
        SkippableIterator<Long, LongValue> it = new LazyMergeSortIterator<>(OperationType.AND, iterators);
        while(it.hasNext())
            actual.add(it.next());

        Assert.assertEquals(convert(2L, 4L, 9L), actual);
    }

    @Test
    public void orWithSingleIteratorAsInput()
    {
        List<LongValue> input = convert(1L, 2L, 4L, 9L);
        SkippableIterator<Long, LongValue> it = new TestSkippableIterator<>(input);
        List<SkippableIterator<Long, LongValue>> iterators = Collections.singletonList(it);

        List<LongValue> actual = new ArrayList<>();
        SkippableIterator<Long, LongValue> itRes = new LazyMergeSortIterator<>(OperationType.AND, iterators);
        while(itRes.hasNext())
            actual.add(itRes.next());

        Assert.assertEquals(input, actual);
    }

    @Test
    public void orWithTwoIteratorsWithSingleValues()
    {
        SkippableIterator<Long, LongValue> i1 = new TestSkippableIterator<>(convert(1L));
        SkippableIterator<Long, LongValue> i2 = new TestSkippableIterator<>(convert(1L));
        List<SkippableIterator<Long, LongValue>> union = Arrays.asList(i1, i2);

        List<LongValue> found = new ArrayList<>();
        SkippableIterator<Long, LongValue> it = new LazyMergeSortIterator<>(OperationType.OR, union);
        while (it.hasNext())
            found.add(it.next());

        Assert.assertEquals(1, found.size());
    }

    @Test
    public void christopherNolanWouldBeProud()
    {
        List<List<LongValue>> rawMergedLists = new ArrayList<>();
        List<SkippableIterator<Long, Table>> mergeSortIterators = new ArrayList<>();

        for(int i = 0; i < 3; i++)
        {
            List<Table> generatedTables = new ArrayList<>();
            int numTablesToGen = getRandomInt(2, 5);

            // always insert tableids 2,9, and 17 to
            // ensure the AND operation will always contain
            // at least 3 results
            Set<Long> tableIds = new HashSet<>();
            tableIds.add(2l);
            tableIds.add(9l);
            tableIds.add(17l);
            while (tableIds.size() < numTablesToGen)
                tableIds.add((long) random.nextInt(15));

            List<Long> tableIdsList = new ArrayList<>(tableIds);
            Collections.sort(tableIdsList);
            for (int j = 0; j < numTablesToGen; j++)
            {
                List<List<LongValue>> generatedLists = new ArrayList<>();
                for (int k = 0; k < 3; k++)
                {
                    List<LongValue> randomList = getRandomLongArray(getRandomInt(1,6),
                                                                    getRandomInt(15,25), 5l, 21l, 30l);
                    generatedLists.add(randomList);
                }

                List<LongValue> expectedORMergedList = getExpectedORResult(generatedLists);
                rawMergedLists.add(expectedORMergedList);
                generatedTables.add(getTestTable(tableIdsList.get(j), generatedLists));
            }
            SkippableIterator<Long, Table> si = new TestSkippableIterator<>(generatedTables);
            mergeSortIterators.add(si);
        }

        List<LongValue> expected = getExpectedANDResult(rawMergedLists);
        Map<Long, Table> actualTables = new HashMap<>();
        Set<CombinedValue<Long>> actualValues = new HashSet<>();

        Iterator<Table> inceptionItr = new LazyMergeSortIterator<>(OperationType.AND, mergeSortIterators);
        while(inceptionItr.hasNext())
        {
            Table next = inceptionItr.next();
            actualTables.put(next.getTableId(), next);
            SkippableIterator<Long, LongValue> itr = next.getIterator();
            while (itr.hasNext())
                actualValues.add(itr.next());
        }

        // ensure the smallest set of expected values present
        for (CombinedValue<Long> num : expected)
            Assert.assertTrue(actualValues.contains(num));

        Assert.assertTrue(actualTables.containsKey(2l));
        Assert.assertTrue(actualTables.containsKey(9l));
        Assert.assertTrue(actualTables.containsKey(17l));
    }

    @Test
    public void randomGeneratedOr()
    {
        List<List<LongValue>> rawGeneratedLists = new ArrayList<>();
        List<SkippableIterator<Long, LongValue>> iterators = new ArrayList<>();
        for (int i = 0; i < 4; i++)
        {
            List<LongValue> randomArray = getRandomLongArray(1, 25);
            rawGeneratedLists.add(randomArray);
            SkippableIterator<Long, LongValue> it = new TestSkippableIterator<>(randomArray);
            iterators.add(it);
        }

        List<LongValue> expected = getExpectedORResult(rawGeneratedLists);

        LazyMergeSortIterator<Long, LongValue> it = new LazyMergeSortIterator<>(OperationType.OR, iterators);
        List<LongValue> actual = new ArrayList<>();
        while (it.hasNext())
            actual.add(it.next());

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void multiORWithSingleANDTest()
    {
        SkippableIterator<Long, LongValue> f1_1 = new TestSkippableIterator<>(convert(0L,  146L, 218L));
        SkippableIterator<Long, LongValue> f1_2 = new TestSkippableIterator<>(convert(74L, 219L, 292L));
        SkippableIterator<Long, LongValue> f1_3 = new TestSkippableIterator<>(convert(0L, 147L, 222L, 297L));

        SkippableIterator<Long, LongValue> f2_1 = new TestSkippableIterator<>(convert(0L,  146L, 291L));
        SkippableIterator<Long, LongValue> f2_2 = new TestSkippableIterator<>(convert(74L, 218L, 292L));
        SkippableIterator<Long, LongValue> f2_3 = new TestSkippableIterator<>(convert(0L, 74L, 222L, 297L));

        SkippableIterator<Long, LongValue> f1Union = new LazyMergeSortIterator<>(OperationType.OR, Arrays.asList(f1_1, f1_2, f1_3));
        SkippableIterator<Long, LongValue> f2Union = new LazyMergeSortIterator<>(OperationType.OR, Arrays.asList(f2_1, f2_2, f2_3));

        /*   f1    f2
              0     0
             74    74
            146   146
            147   218
            218   222
            219   291
            222   292
            292   297
         */

        SkippableIterator<Long, LongValue> intersection = new LazyMergeSortIterator<>(OperationType.AND, Arrays.asList(f1Union, f2Union));

        List<CombinedValue<Long>> actual = new ArrayList<>();
        while (intersection.hasNext())
            actual.add(intersection.next());

        List<LongValue> expected = convert(0L, 74L, 146L, 218L, 222L, 292L, 297L);
        Assert.assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @Test
    public void theMoviePrimerIsEasierToUnderstand()
    {
        List<List<LongValue>> expectedORLists = new ArrayList<>();
        List<SkippableIterator<Long, LongValue>> iteratorsToAnd = new ArrayList<>();
        int numLazyOrIterators = getRandomInt(3,7);
        for (int i = 0; i < numLazyOrIterators; i++)
        {
            List<List<LongValue>> rawORLists = new ArrayList<>();
            List<SkippableIterator<Long, LongValue>> skippableIterators = new ArrayList<>();
            int numSkippableIterators = getRandomInt(2,7);
            for (int j = 0; j < numSkippableIterators; j++) {
                List<LongValue> list = getRandomLongArray(1, 25);
                rawORLists.add(list);
                SkippableIterator<Long, LongValue> it = new TestSkippableIterator<>(list);
                skippableIterators.add(it);
            }
            expectedORLists.add(getExpectedORResult(rawORLists));
            SkippableIterator<Long, LongValue> unionItr = new LazyMergeSortIterator<>(OperationType.OR, skippableIterators);
            iteratorsToAnd.add(unionItr);
        }

        List<LongValue> expectedANDResult = getExpectedANDResult(expectedORLists);
        SkippableIterator<Long, LongValue> intersection = new LazyMergeSortIterator<>(OperationType.AND, iteratorsToAnd);
        List<LongValue> actual = new ArrayList<>();
        while (intersection.hasNext())
            actual.add(intersection.next());

        Assert.assertEquals(expectedANDResult, actual);
    }

    @Test
    public void testMergingOfTheValues()
    {
        List<TokenValue> s1_1Tokens = new ArrayList<TokenValue>()
        {{
                add(new TokenValue(0, Pair.create("sstable_1", new long[] { 0, 1 })));
                add(new TokenValue(1, Pair.create("sstable_1", new long[] { 2, 5 })));
                add(new TokenValue(5, Pair.create("sstable_1", new long[] { 5 })));
        }};

        // this helps to check merging of the token collisions
        List<TokenValue> s1_2Tokens = new ArrayList<TokenValue>()
        {{
                add(new TokenValue(1, Pair.create("sstable_1", new long[] { 2, 3, 4 })));
        }};

        // this helps to check merging of the token collisions
        List<TokenValue> s1_3Tokens = new ArrayList<TokenValue>()
        {{
                add(new TokenValue(0, Pair.create("sstable_1", new long[] { 2 })));
                add(new TokenValue(1, Pair.create("sstable_1", new long[] { 6 })));
        }};

        List<TokenValue> s2_1Tokens = new ArrayList<TokenValue>()
        {{
                add(new TokenValue(0, Pair.create("sstable_2", new long[] { 10, 11 })));
                add(new TokenValue(1, Pair.create("sstable_2", new long[] { 12 })));
                add(new TokenValue(3, Pair.create("sstable_2", new long[] { 13 })));
                add(new TokenValue(4, Pair.create("sstable_2", new long[] { 14 })));
                add(new TokenValue(5, Pair.create("sstable_2", new long[] { 15 })));
        }};

        List<TokenValue> s2_2Tokens = new ArrayList<TokenValue>()
        {{
                add(new TokenValue(5, Pair.create("sstable_2", new long[] { 15, 16 })));
        }};

        SkippableIterator<Long, TokenValue> s1_1 = new TestSkippableIterator<>(s1_1Tokens);
        SkippableIterator<Long, TokenValue> s1_2 = new TestSkippableIterator<>(s1_2Tokens);
        SkippableIterator<Long, TokenValue> s1_3 = new TestSkippableIterator<>(s1_3Tokens);

        SkippableIterator<Long, TokenValue> s2_1 = new TestSkippableIterator<>(s2_1Tokens);
        SkippableIterator<Long, TokenValue> s2_2 = new TestSkippableIterator<>(s2_2Tokens);

        SkippableIterator<Long, TokenValue> s1Union = new LazyMergeSortIterator<>(OperationType.OR, Arrays.asList(s1_1, s1_2, s1_3));
        SkippableIterator<Long, TokenValue> s2Union = new LazyMergeSortIterator<>(OperationType.OR, Arrays.asList(s2_1, s2_2));

        List<TokenValue> actual = new ArrayList<>();
        SkippableIterator<Long, TokenValue> intersection = new LazyMergeSortIterator<>(OperationType.AND, Arrays.asList(s1Union, s2Union));
        while (intersection.hasNext())
            actual.add(intersection.next());

        List<TokenValue> expected = new ArrayList<TokenValue>(3)
        {{
            add(new TokenValue(0, new HashMap<String, Set<Long>>()
            {{
                    put("sstable_1", Sets.newHashSet(0L,  1L, 2L));
                    put("sstable_2", Sets.newHashSet(10L, 11L));
            }}));

            add(new TokenValue(1, new HashMap<String, Set<Long>>()
            {{
                    put("sstable_1", Sets.newHashSet(2L, 3L, 4L, 5L, 6L));
                    put("sstable_2", Sets.newHashSet(12L));
            }}));

            add(new TokenValue(5, new HashMap<String, Set<Long>>()
            {{
                    put("sstable_1", Sets.newHashSet(5L));
                    put("sstable_2", Sets.newHashSet(15L, 16L));
            }}));
        }};

        Assert.assertEquals(expected, actual);
    }


    private List<LongValue> getExpectedORResult(List<List<LongValue>> rawListsToOR)
    {
        SortedSet<LongValue> expectedOrSet = new TreeSet<>();
        for (List<LongValue> list : rawListsToOR)
            expectedOrSet.addAll(list);

        List<LongValue> expectedOrRes = new ArrayList<>();
        expectedOrRes.addAll(expectedOrSet);

        return expectedOrRes;
    }

    private List<LongValue> getExpectedANDResult(List<List<LongValue>> rawListsToAND)
    {
        int longestListSize = 0;
        int longestListIdx = 0;
        for (int i = 0; i < rawListsToAND.size(); i++)
        {
            if(rawListsToAND.get(i).size() > longestListSize)
                longestListIdx = i;
        }

        List<LongValue> andRes = new ArrayList<>();
        List<LongValue> longest = new ArrayList<>();
        longest.addAll(rawListsToAND.get(longestListIdx));
        Collections.sort(longest);

        for (LongValue val : longest)
        {
            boolean shouldAdd = true;
            for(List<LongValue> list : rawListsToAND)
            {
                if (!list.contains(val))
                    shouldAdd = false;
            }
            if (shouldAdd)
                andRes.add(val);
        }
        return andRes;
    }

    private int getRandomInt(int min, int max)
    {
        int numTablesToGen = 0;
        while (numTablesToGen <= min)
            numTablesToGen = random.nextInt(max);
        return numTablesToGen;
    }

    private Table getTestTable(Long tableId, List<List<LongValue>> lists)
    {
        List<SkippableIterator<Long, LongValue>> iterators = new ArrayList<>();
        for (List<LongValue> list : lists)
        {
            iterators.add(new TestSkippableIterator<>(list));
        }

        LazyMergeSortIterator<Long, LongValue> it = new LazyMergeSortIterator<>(OperationType.OR, iterators);
        return new Table(tableId, it);
    }

    class Table implements CombinedValue<Long>
    {
        private long tableId;
        private LazyMergeSortIterator<Long, LongValue> iterator;

        public Table(long tableId, LazyMergeSortIterator<Long, LongValue> iterator)
        {
            this.tableId = tableId;
            this.iterator = iterator;
        }

        public Long getTableId()
        {
            return tableId;
        }

        public LazyMergeSortIterator<Long, LongValue> getIterator()
        {
            return iterator;
        }

        @Override
        public int compareTo(CombinedValue<Long> b)
        {
            return Long.compare(tableId, b.get());
        }

        @Override
        public int hashCode()
        {
            return new HashCodeBuilder().append(tableId).build();
        }

        @Override
        public String toString()
        {
            return "table_" + tableId;
        }

        @Override
        public void merge(CombinedValue<Long> other)
        {}

        @Override
        public Long get()
        {
            return tableId;
        }
    }

    private List<LongValue> getRandomLongArray(int min, int max, long... explicitlyAdd)
    {
        List<LongValue> ret = new ArrayList<>();
        int numElms = 0;
        while (numElms < 4)
            numElms = random.nextInt(15);

        for(int i = 0; i < numElms; i++)
        {
            long nextRand = 0;
            while (nextRand < min)
                nextRand = (long) random.nextInt(max);
            ret.add(new LongValue(nextRand));
        }

        // add any values that must appear in the otherwise randomly
        // generated list
        for (long toAdd : explicitlyAdd)
                ret.add(new LongValue(toAdd));

        // remove any possible duplicates from the random generation
        // and sort the final list
        Set<LongValue> retSet = new TreeSet<>(ret);

        ret.clear();
        ret.addAll(retSet);

        return ret;
    }

    public List<LongValue> convert(long... values)
    {
        List<LongValue> result = new ArrayList<>(values.length);
        for (long value : values)
            result.add(new LongValue(value));

        return result;
    }

    private static class LongValue implements CombinedValue<Long>
    {
        private final long value;

        public LongValue(long v)
        {
            value = v;
        }

        @Override
        public Long get()
        {
            return value;
        }

        @Override
        public void merge(CombinedValue<Long> other)
        {}

        @Override
        public int compareTo(CombinedValue<Long> o)
        {
            return Long.compare(value, o.get());
        }

        @Override
        public int hashCode()
        {
            return new HashCodeBuilder().append(value).build();
        }

        @Override
        public boolean equals(Object other)
        {
            return other instanceof LongValue && compareTo(((LongValue) other)) == 0;
        }

        @Override
        public String toString()
        {
            return String.format("%d", value);
        }
    }

    // minimal impl of a SkippableIterator for basic test cases
    public class TestSkippableIterator<K extends Comparable<K>, T extends CombinedValue<K>> extends AbstractIterator<T>
            implements SkippableIterator<K, T>
    {
        private final List<T> elements;
        private final Iterator<T> elmsIterator;

        public TestSkippableIterator(List<T> elms)
        {
            this.elements = elms;
            this.elmsIterator = elms.iterator();
        }

        @Override
        public K getMinimum()
        {
            return elements.isEmpty() ? null : elements.get(0).get();
        }

        @Override
        public T computeNext()
        {
            return elmsIterator.hasNext() ? elmsIterator.next() : endOfData();
        }

        @Override
        public void skipTo(K next)
        {
            while (hasNext())
            {
                if (peek().get().compareTo(next) >= 0)
                    break;

                next();
            }
        }

        @Override
        public boolean intersect(T element)
        {
            for (T existing : elements)
            {
                if (existing.get().equals(element.get()))
                {
                    element.merge(existing);
                    return true;
                }
            }

            return false;
        }

        @Override
        public long getCount()
        {
            return elements.size();
        }

        @Override
        public void close() throws IOException
        {
            // nothing to do here
        }
    }

    public static class TokenValue implements CombinedValue<Long>
    {
        private final long token;
        private final Map<String, Set<Long>> offsets;

        public TokenValue(long token, final Pair<String, long[]> offsets)
        {
            this.token = token;
            this.offsets = new HashMap<String, Set<Long>>()
            {{
                put(offsets.left, new HashSet<>(Longs.asList(offsets.right)));
            }};
        }

        public TokenValue(long token, final Map<String, Set<Long>> offsets)
        {
            this.token = token;
            this.offsets = offsets;
        }

        @Override
        public void merge(CombinedValue<Long> other)
        {
            TokenValue tokenValue = (TokenValue) other;
            assert token == tokenValue.token;

            for (Map.Entry<String, Set<Long>> e : tokenValue.offsets.entrySet())
            {
                Set<Long> existing = offsets.get(e.getKey());
                if (existing == null)
                    offsets.put(e.getKey(), e.getValue());
                else
                    existing.addAll(e.getValue());
            }
        }

        @Override
        public Long get()
        {
            return token;
        }

        @Override
        public int compareTo(CombinedValue<Long> o)
        {
            return Long.compare(token, o.get());
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof TokenValue))
                return false;

            TokenValue o = (TokenValue) other;
            return token == o.token && offsets.equals(o.offsets);
        }

        @Override
        public int hashCode()
        {
            return new HashCodeBuilder().append(token).build();
        }

        @Override
        public String toString()
        {
            return String.format("TokenValue(token: %d, offsets: %s)", token, offsets);
        }
    }
}
