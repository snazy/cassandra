package org.apache.cassandra.db.index.search;

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
    public void christopherNolanWouldBeProud()
    {
        List<Set<Long>> rawMergedLists = new ArrayList<>();
        List<SkippableIterator<Table>> mergeSortIterators = new ArrayList<>();

        for(int i = 0; i < 3; i++) {
            List<Table> generatedTables = new ArrayList<>();
            int numTablesToGen = 0;
            while (numTablesToGen <= 0)
                numTablesToGen = random.nextInt(30);

            // generate a list of tableids to use..
            Set<Long> tableIds = new HashSet<>();
            // always insert tableids 2,9, and 17 to
            // ensure the AND operation will always contain
            // at least 3 results
            tableIds.add(2l);
            tableIds.add(9l);
            tableIds.add(17l);
            while (tableIds.size() < numTablesToGen)
                tableIds.add((long) random.nextInt(35));

            List<Long> tableIdsList = new ArrayList<>(tableIds);
            Collections.sort(tableIdsList);
            for (int j = 0; j < numTablesToGen; j++)
            {
                List<Long> randomList1 = getRandomLongArray(1, 25);
                List<Long> randomList2 = getRandomLongArray(10, 50);
                List<Long> randomList3 = getRandomLongArray(1, 40);

                Set<Long> merged = new HashSet<>();
                merged.addAll(randomList1);
                merged.addAll(randomList2);
                merged.addAll(randomList3);
                rawMergedLists.add(merged);

                generatedTables.add(getTestTable(tableIdsList.get(j), randomList1, randomList2, randomList3));
            }
            SkippableIterator<Table> si = new TestSkippableIterator<>(generatedTables, tableComparator);
            mergeSortIterators.add(si);
        }

        int longestListSize = 0;
        int longestListIdx = 0;
        for (int i = 0; i < rawMergedLists.size(); i++)
        {
            if(rawMergedLists.get(i).size() > longestListSize)
                longestListIdx = i;
        }

        List<Long> expected = new ArrayList<>();
        List<Long> longest = new ArrayList<>();
        longest.addAll(rawMergedLists.get(longestListIdx));
        Collections.sort(longest);
        for (Long val : longest)
        {
            boolean shouldAdd = true;
            for(Set<Long> set : rawMergedLists)
            {
                if (!set.contains(val))
                    shouldAdd = false;
            }
            if (shouldAdd)
                expected.add(val);
        }

        Map<Long, Table> actual = new HashMap<>();
        Iterator<Table> inceptionItr = new LazyMergeSortIterator<>(tableComparator,
                LazyMergeSortIterator.OperationType.AND, mergeSortIterators);
        while(inceptionItr.hasNext())
        {
            Table next = inceptionItr.next();
            actual.put(next.tableId, next);
        }

        Assert.assertTrue(actual.containsKey(2l));
        Assert.assertTrue(actual.containsKey(9l));
        Assert.assertTrue(actual.containsKey(17l));
    }

    private Table getTestTable(Long tableId, List<Long> ... lists)
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

    private List<Long> getRandomLongArray(int min, int max)
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

        //remove any possible duplicates from the random generation
        //and sort the final list
        Set<Long> retSet = new LinkedHashSet<>(ret);
        ret.clear();
        ret.addAll(retSet);
        Collections.sort(ret);
        return ret;
    }

    // minimal impl of a SkippableIterator for basic test cases
    public class TestSkippableIterator<T> implements SkippableIterator<T>
    {
        private List<T> elms;
        private Comparator<T> comparator;
        private int pos = 0;

        public TestSkippableIterator(List<T> elms, Comparator<T> comparator)
        {
            this.elms = elms;
            this.comparator = comparator;
        }

        @Override
        public boolean hasNext()
        {
            return elms.size() > pos;
        }

        @Override
        public T next()
        {
            return elms.get(pos++);
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void skipTo(T next)
        {
            if (pos+1 >= elms.size())
                return;

            while(hasNext())
            {
                if (pos+1 < elms.size() && comparator.compare(elms.get(pos+1), next) < 0)
                    pos++;
                else
                    break;
            }
        }
    }
}
