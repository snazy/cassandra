package org.apache.cassandra.db.index.search;

import com.google.common.collect.AbstractIterator;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.db.index.utils.LazyMergeSortIterator;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class MergeSortAwareIteratorTest
{
    private static final Random random = new Random();
    private static final Comparator<Long> longComparator = new Comparator<Long>() {
        @Override
        public int compare(Long o1, Long o2) {
            return o1.compareTo(o2);
        }
    };
    private static final Comparator<Table> tableComparator = new Comparator<Table>() {
        @Override
        public int compare(Table o1, Table o2) {
            return o1.getTableId().compareTo(o2.getTableId());
        }
    };

    @Test
    public void orLongIterators()
    {
        SkippableIterator<Long> it1 = new TestSkippableIterator<>(Arrays.asList(2l,3l,5l,6l), longComparator);
        SkippableIterator<Long> it2 = new TestSkippableIterator<>(Arrays.asList(1l,7l), longComparator);
        SkippableIterator<Long> it3 = new TestSkippableIterator<>(Arrays.asList(4l,8l,9l,10l), longComparator);
        List<SkippableIterator<Long>> iterators = Arrays.asList(it1,it2,it3);

        List<Long> actual = new ArrayList<>();
        Iterator<Long> it = new LazyMergeSortIterator<>(longComparator,
                LazyMergeSortIterator.OperationType.OR, iterators);
        while(it.hasNext())
            actual.add(it.next());

        List<Long> expected = Arrays.asList(1l,2l,3l,4l,5l,6l,7l,8l,9l,10l);
        Assert.assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @Test
    public void andLongIterators()
    {
        SkippableIterator<Long> it1 = new TestSkippableIterator<>(Arrays.asList(2l,3l,4l,5l,6l,9l), longComparator);
        SkippableIterator<Long> it2 = new TestSkippableIterator<>(Arrays.asList(1l,2l,4l,9l), longComparator);
        SkippableIterator<Long> it3 = new TestSkippableIterator<>(Arrays.asList(2l,4l,7l,8l,9l,10l), longComparator);
        List<SkippableIterator<Long>> iterators = Arrays.asList(it1,it2,it3);

        List<Long> actual = new ArrayList<>();
        Iterator<Long> it = new LazyMergeSortIterator<>(longComparator,
                LazyMergeSortIterator.OperationType.AND, iterators);
        while(it.hasNext())
            actual.add(it.next());

        List<Long> expected = Arrays.asList(2l,4l,9l);
        Assert.assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @Test
    public void orWithSingleIteratorAsInput()
    {
        List<Long> input = Arrays.asList(1l,2l,4l,9l);
        SkippableIterator<Long> it = new TestSkippableIterator<>(input, longComparator);
        List<SkippableIterator<Long>> iterators = Arrays.asList(it);

        List<Long> actual = new ArrayList<>();
        SkippableIterator<Long> itRes = new LazyMergeSortIterator<>(longComparator,
                LazyMergeSortIterator.OperationType.AND, iterators);
        while(itRes.hasNext())
            actual.add(itRes.next());

        Assert.assertArrayEquals(input.toArray(), actual.toArray());
    }

    @Test
    public void christopherNolanWouldBeProud()
    {
        List<List<Long>> rawMergedLists = new ArrayList<>();
        List<SkippableIterator<Table>> mergeSortIterators = new ArrayList<>();

        for(int i = 0; i < 3; i++) {
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
                List<List<Long>> generatedLists = new ArrayList<>();
                for (int k = 0; k < 3; k++)
                {
                    List<Long> randomList = getRandomLongArray(getRandomInt(1,6),
                            getRandomInt(15,25), 5l, 21l, 30l);
                    generatedLists.add(randomList);
                }

                List<Long> expectedORMergedList = getExpectedORResult(generatedLists);
                rawMergedLists.add(expectedORMergedList);
                generatedTables.add(getTestTable(tableIdsList.get(j), generatedLists));
            }
            SkippableIterator<Table> si = new TestSkippableIterator<>(generatedTables, tableComparator);
            mergeSortIterators.add(si);
        }

        List<Long> expected = getExpectedANDResult(rawMergedLists);
        Map<Long, Table> actualTables = new HashMap<>();
        Set<Long> actualValues = new HashSet<>();

        Iterator<Table> inceptionItr = new LazyMergeSortIterator<>(tableComparator,
                LazyMergeSortIterator.OperationType.AND, mergeSortIterators);
        while(inceptionItr.hasNext())
        {
            Table next = inceptionItr.next();
            actualTables.put(next.tableId, next);
            SkippableIterator<Long> itr = next.getIterator();
            while (itr.hasNext())
                actualValues.add(itr.next());
        }

        // ensure the smallest set of expected values present
        for (Long num : expected)
            Assert.assertTrue(actualValues.contains(num));

        Assert.assertTrue(actualTables.containsKey(2l));
        Assert.assertTrue(actualTables.containsKey(9l));
        Assert.assertTrue(actualTables.containsKey(17l));
    }

    @Test
    public void randomGeneratedOr()
    {
        List<List<Long>> rawGeneratedLists = new ArrayList<>();
        List<SkippableIterator<Long>> iterators = new ArrayList<>();
        for (int i = 0; i < 4; i++)
        {
            List<Long> randomArray = getRandomLongArray(1, 25);
            rawGeneratedLists.add(randomArray);
            SkippableIterator<Long> it = new TestSkippableIterator<>(randomArray, longComparator);
            iterators.add(it);
        }

        List<Long> expected = getExpectedORResult(rawGeneratedLists);

        LazyMergeSortIterator<Long> it = new LazyMergeSortIterator<>(longComparator,
                LazyMergeSortIterator.OperationType.OR, iterators);
        List<Long> actual = new ArrayList<>();
        while (it.hasNext())
            actual.add(it.next());

        Assert.assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @Test
    public void multiORWithSingleANDTest()
    {
        SkippableIterator<Long> f1_1 = new TestSkippableIterator<>(Arrays.asList(0L,  146L, 218L), longComparator);
        SkippableIterator<Long> f1_2 = new TestSkippableIterator<>(Arrays.asList(74L, 219L, 292L), longComparator);
        SkippableIterator<Long> f1_3 = new TestSkippableIterator<>(Arrays.asList(0L,  147L, 222L, 297L), longComparator);

        SkippableIterator<Long> f2_1 = new TestSkippableIterator<>(Arrays.asList(0L,  146L, 291L), longComparator);
        SkippableIterator<Long> f2_2 = new TestSkippableIterator<>(Arrays.asList(74L, 218L, 292L), longComparator);
        SkippableIterator<Long> f2_3 = new TestSkippableIterator<>(Arrays.asList(0L,  74L,  222L, 297L), longComparator);

        SkippableIterator<Long> f1Union = new LazyMergeSortIterator<>(longComparator,
                LazyMergeSortIterator.OperationType.OR, Arrays.asList(f1_1, f1_2, f1_3));
        SkippableIterator<Long> f2Union = new LazyMergeSortIterator<>(longComparator,
                LazyMergeSortIterator.OperationType.OR, Arrays.asList(f2_1, f2_2, f2_3));

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

        SkippableIterator<Long> intersection = new LazyMergeSortIterator<>(longComparator, LazyMergeSortIterator.OperationType.AND, Arrays.asList(f1Union, f2Union));

        List<Long> actual = new ArrayList<>();
        while (intersection.hasNext())
            actual.add(intersection.next());

        List<Long> expected = Arrays.asList(0l, 74l, 146l, 218l, 222l, 292l, 297l);
        Assert.assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @Test
    public void theMoviePrimerIsEasierToUnderstand()
    {
        List<List<Long>> expectedORLists = new ArrayList<>();
        List<SkippableIterator<Long>> iteratorsToAnd = new ArrayList<>();
        int numLazyOrIterators = getRandomInt(3,7);
        for (int i = 0; i < numLazyOrIterators; i++)
        {
            List<List<Long>> rawORLists = new ArrayList<>();
            List<SkippableIterator<Long>> skippableIterators = new ArrayList<>();
            int numSkippableIterators = getRandomInt(2,7);
            for (int j = 0; j < numSkippableIterators; j++) {
                List<Long> list = getRandomLongArray(1, 25);
                rawORLists.add(list);
                SkippableIterator<Long> it = new TestSkippableIterator<>(list, longComparator);
                skippableIterators.add(it);
            }
            expectedORLists.add(getExpectedORResult(rawORLists));
            SkippableIterator<Long> unionItr = new LazyMergeSortIterator<>(longComparator,
                    LazyMergeSortIterator.OperationType.OR, skippableIterators);
            iteratorsToAnd.add(unionItr);
        }

        List<Long> expectedANDResult = getExpectedANDResult(expectedORLists);
        SkippableIterator<Long> intersection = new LazyMergeSortIterator<>(longComparator,
                LazyMergeSortIterator.OperationType.AND, iteratorsToAnd);
        List<Long> actual = new ArrayList<>();
        while (intersection.hasNext())
            actual.add(intersection.next());
        Assert.assertArrayEquals(expectedANDResult.toArray(), actual.toArray());
    }

    private List<Long> getExpectedORResult(List<List<Long>> rawListsToOR)
    {
        Set<Long> expectedOrSet = new HashSet<>();
        for (List<Long> list : rawListsToOR)
            expectedOrSet.addAll(list);

        List<Long> expectedOrRes = new ArrayList<>();
        expectedOrRes.addAll(expectedOrSet);
        Collections.sort(expectedOrRes);
        return expectedOrRes;
    }

    private List<Long> getExpectedANDResult(List<List<Long>> rawListsToAND)
    {
        int longestListSize = 0;
        int longestListIdx = 0;
        for (int i = 0; i < rawListsToAND.size(); i++)
        {
            if(rawListsToAND.get(i).size() > longestListSize)
                longestListIdx = i;
        }

        List<Long> andRes = new ArrayList<>();
        List<Long> longest = new ArrayList<>();
        longest.addAll(rawListsToAND.get(longestListIdx));
        Collections.sort(longest);
        for (Long val : longest)
        {
            boolean shouldAdd = true;
            for(List<Long> list : rawListsToAND)
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

    private Table getTestTable(Long tableId, List<List<Long>> lists)
    {
        List<SkippableIterator<Long>> iterators = new ArrayList<>();
        for (List<Long> list : lists)
        {
            iterators.add(new TestSkippableIterator<>(list, longComparator));
        }

        LazyMergeSortIterator<Long> it = new LazyMergeSortIterator<>(longComparator,
                LazyMergeSortIterator.OperationType.OR, iterators);
        return new Table(tableId, it);
    }

    class Table implements Comparable<Table>
    {
        private Long tableId;
        private LazyMergeSortIterator<Long> iterator;

        public Table(Long tableId, LazyMergeSortIterator<Long> iterator)
        {
            this.tableId = tableId;
            this.iterator = iterator;
        }

        public Long getTableId()
        {
            return tableId;
        }

        public LazyMergeSortIterator<Long> getIterator()
        {
            return iterator;
        }

        @Override
        public int compareTo(Table b)
        {
            return b.getTableId().compareTo(tableId);
        }

        @Override
        public String toString()
        {
            return "table_"+tableId.toString();
        }
    }

    private List<Long> getRandomLongArray(int min, int max, long ... explicitlyAdd)
    {
        List<Long> ret = new ArrayList<>();
        int numElms = 0;
        while (numElms < 4)
            numElms = random.nextInt(15);

        for(int i = 0; i < numElms; i++)
        {
            long nextRand = 0;
            while (nextRand < min)
                nextRand = (long) random.nextInt(max);
            ret.add(nextRand);
        }

        // add any values that must appear in the otherwise randomly
        // generated list
        for (long toAdd : explicitlyAdd)
                ret.add(toAdd);

        // remove any possible duplicates from the random generation
        // and sort the final list
        Set<Long> retSet = new LinkedHashSet<>(ret);
        ret.clear();
        ret.addAll(retSet);
        Collections.sort(ret);
        return ret;
    }

    // minimal impl of a SkippableIterator for basic test cases
    public class TestSkippableIterator<T> extends AbstractIterator<T> implements SkippableIterator<T>
    {
        private Iterator<T> elms;
        private Comparator<T> comparator;

        public TestSkippableIterator(List<T> elms, Comparator<T> comparator)
        {
            this.elms = elms.iterator();
            this.comparator = comparator;
        }

        @Override
        public T computeNext()
        {
            return elms.hasNext() ? elms.next() : endOfData();
        }

        @Override
        public void skipTo(T next)
        {
            while (hasNext())
            {
                if (comparator.compare(peek(), next) >= 0)
                    break;

                next();
            }
        }
    }
}
