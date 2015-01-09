package org.apache.cassandra.db.index.utils;

import java.util.*;

/**
 * Returns a single merged and sorted iterator from the provided iterators.
 * Using OperationType.AND will return a iterator with only elements present
 * in all input iterators. OperationType.OR will return all elements from the
 * input iterators in sorted order.
 */
public class LazyMergeSortIterator<T> implements SkippableIterator<T>
{
    public static enum OperationType
    {
        AND, OR
    }

    private Comparator<T> comparator;
    private OperationType opType;
    private List<SkippableIterator<T>> iterators;

    // buffer of elements already taken from an iterator
    // that maybe used in the next iteration
    private List<T> currentPerIterator;
    private T next = null;

    public LazyMergeSortIterator(Comparator<T> comparator, OperationType opType,
                                 List<SkippableIterator<T>> iterators)
    {
        this.comparator = comparator;
        this.opType = opType;
        this.iterators = iterators;
        this.currentPerIterator = new ArrayList<>((List<T>) Collections.nCopies(iterators.size(), null));
    }

    @Override
    public boolean hasNext()
    {
        Element nextElm = findNextElement();
        if (nextElm == null)
            return false;

        T tmp = null;
        if (opType == OperationType.AND)
        {
            while (nextElm != null)
            {
                // if next target element is smaller than the max value
                // seen in the last pass across all iterators, we can tell
                // all iterators to safely skip forwards to the last known max
                if (nextElm.max != null && comparator.compare(nextElm.value, nextElm.max) < 0)
                {
                    for(SkippableIterator<T> iterator : iterators)
                        iterator.skipTo(nextElm.max);
                    nextElm = findNextElement();
                    continue;
                }

                boolean foundInAllIterators = true;
                for (int i = 0; i < iterators.size(); i++)
                {
                    if (nextElm.iteratorIdx == i)
                        continue;

                    int cmpRes = comparator.compare(nextElm.value, currentPerIterator.get(i));
                    if (cmpRes == 0)
                    {
                        // found the same value in this iterator, this satisfies our AND merge
                        tmp = nextElm.value;
                        currentPerIterator.set(i, null);
                    }
                    else
                    {
                        foundInAllIterators = false;
                    }

                }

                if (tmp != null && foundInAllIterators)
                {
                    next = tmp;
                    break;
                }
                else
                {
                    nextElm = findNextElement();
                }
            }
        }
        else if (opType == OperationType.OR && nextElm.value != null)
        {
            // check if the previous value returned equals the next
            // possible value. Because OR operator, elements must be de-duped
            if (next != null && comparator.compare(nextElm.value, next) == 0)
                while (nextElm != null && nextElm.value != null
                        && comparator.compare(nextElm.value, next) == 0)
                    nextElm = findNextElement();
        }

        if (nextElm == null || nextElm.value == null)
            return false;

        next = nextElm.value;
        return true;
    }

    @Override
    public T next()
    {
        return next;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void skipTo(T next)
    {
        for (SkippableIterator<T> itr : iterators)
            itr.skipTo(next);
    }

    private Element findNextElement()
    {
        int iteratorIdx = 0;
        T minVal = null;
        T maxVal = null;
        for (int i = 0; i < iterators.size(); i++)
        {
            T prev = currentPerIterator.get(i);
            if (prev == null && iterators.get(i).hasNext())
            {
                T itNext = iterators.get(i).next();
                currentPerIterator.set(i, itNext);
                prev = itNext;
            }

            if (prev == null)
            {
                // if any one of the iterators has no more elements we can
                // safely bail now as there will never be another match
                if (opType == OperationType.AND)
                    return null;
                else
                    continue;
            }
            else if (minVal == null)
            {
                iteratorIdx = i;
                minVal = prev;
                maxVal = prev;
                continue;
            }

            // found value smaller than previous candidate from other iterator
            if (comparator.compare(prev, minVal) < 0)
            {
                iteratorIdx = i;
                minVal = prev;
            }

            // this iterator has a value larger than seen before?
            if (comparator.compare(prev, maxVal) > 0)
                maxVal = prev;
        }
        currentPerIterator.set(iteratorIdx, null);
        return new Element(iteratorIdx, minVal, maxVal);
    }

    private class Element
    {
        // index of iterator this value is from
        protected int iteratorIdx;
        protected T value;
        protected T max;

        protected Element(int iteratorIdx, T value, T max)
        {
            this.iteratorIdx = iteratorIdx;
            this.value = value;
            this.max = max;
        }
    }
}
