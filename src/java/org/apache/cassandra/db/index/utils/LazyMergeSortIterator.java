package org.apache.cassandra.db.index.utils;

import com.google.common.collect.AbstractIterator;

import java.util.*;

/**
 * Returns a single merged and sorted iterator from the provided iterators.
 * Using OperationType.AND will return a iterator with only elements present
 * in all input iterators. OperationType.OR will return all elements from the
 * input iterators in sorted order.
 */
public class LazyMergeSortIterator<T> extends AbstractIterator<T>
{
    public static enum OperationType
    {
        AND, OR
    }

    private Comparator<T> comparator;
    private OperationType opType;
    private List<SeekableIterator<T>> iterators;

    // buffer of elements already taken from an iterator
    // that maybe used in the next iteration
    private List<T> currentPerIterator;

    public LazyMergeSortIterator(Comparator<T> comparator, OperationType opType,
                                 List<SeekableIterator<T>> iterators)
    {
        this.comparator = comparator;
        this.opType = opType;
        this.iterators = iterators;
        this.currentPerIterator = new ArrayList<>((List<T>) Collections.nCopies(iterators.size(), null));
    }

    @Override
    public T computeNext()
    {
        Element nextElm = findNextElement();
        if (nextElm == null)
            return endOfData();

        T tmp = null;
        if (opType == OperationType.AND)
        {
            // currently, we only need additional handling for AND
            while (nextElm != null)
            {
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
                    else if (cmpRes > 0)
                    {
                        // skip forward in iterator until we find a value equal to
                        // the current potential next value
                        iterators.get(i).skipTo(nextElm.value);
                        if (iterators.get(i).hasNext())
                        {
                            tmp = nextElm.value;
                            currentPerIterator.set(i, null);
                        }
                    }
                    else
                    {
                        foundInAllIterators = false;
                    }
                }

                if (tmp != null && foundInAllIterators)
                {
                    return tmp;
                }
                else
                {
                    nextElm = findNextElement();
                }
            }
        }
        if (nextElm == null || nextElm.value == null)
            return endOfData();
        return nextElm.value;
    }

    private Element findNextElement()
    {
        int iteratorIdx = 0;
        T nextVal = null;
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
                if (opType == OperationType.AND)
                    return null;
                else
                    continue;
            }
            else if (nextVal == null)
            {
                iteratorIdx = i;
                nextVal = prev;
                continue;
            }

            if (comparator.compare(prev, nextVal) <= 0)
            {
                // found value less than our previous candidate in current iterator
                iteratorIdx = i;
                nextVal = prev;
            }
        }
        currentPerIterator.set(iteratorIdx, null);
        return new Element(iteratorIdx, nextVal);
    }

    private class Element
    {
        // index of iterator this value is from
        protected int iteratorIdx;
        protected T value;

        protected Element(int iteratorIdx, T value)
        {
            this.iteratorIdx = iteratorIdx;
            this.value = value;
        }
    }
}
