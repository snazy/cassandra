package org.apache.cassandra.db.index.search;

import org.apache.cassandra.db.index.utils.SeekableIterator;
import org.apache.cassandra.db.index.utils.LazyMergeSortIterator;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class MergeSortAwareIteratorTest
{
    private static final Comparator<Long> longComparator = new Comparator<Long>() {
        @Override
        public int compare(Long o1, Long o2) {
            return o1.compareTo(o2);
        }
    };

    @Test
    public void orLongIterators()
    {
        SeekableIterator<Long> it1 = new TestSeekableIterator<>(Arrays.asList(2l,3l,5l,6l));
        SeekableIterator<Long> it2 = new TestSeekableIterator<>(Arrays.asList(1l,7l));
        SeekableIterator<Long> it3 = new TestSeekableIterator<>(Arrays.asList(4l,8l,9l,10l));
        List<SeekableIterator<Long>> iterators = Arrays.asList(it1,it2,it3);

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
        SeekableIterator<Long> it1 = new TestSeekableIterator<>(Arrays.asList(2l,3l,4l,5l,6l,9l));
        SeekableIterator<Long> it2 = new TestSeekableIterator<>(Arrays.asList(1l,2l,4l,9l));
        SeekableIterator<Long> it3 = new TestSeekableIterator<>(Arrays.asList(2l,4l,7l,8l,9l,10l));
        List<SeekableIterator<Long>> iterators = Arrays.asList(it1,it2,it3);

        List<Long> actual = new ArrayList<>();
        Iterator<Long> it = new LazyMergeSortIterator<>(longComparator,
                LazyMergeSortIterator.OperationType.AND, iterators);
        while(it.hasNext())
            actual.add(it.next());

        List<Long> expected = Arrays.asList(2l,4l,9l);
        Assert.assertArrayEquals(expected.toArray(), actual.toArray());
    }

    // minimal impl of a SeekableIterator for basic test cases
    public class TestSeekableIterator<T> implements SeekableIterator<T>
    {
        private List<T> elms;
        private int pos = 0;

        public TestSeekableIterator(List<T> elms)
        {
            this.elms = elms;
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
            while(hasNext())
            {
                if (!elms.get(pos).equals(next))
                    pos++;
                else
                    break;
            }
        }
    }
}
