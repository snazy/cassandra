package org.apache.cassandra.db.index.utils;

import org.apache.cassandra.utils.Pair;

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
                // "expected" behavior when op is AND and only given 1 input is to
                // simply iterate and return all the values from the one iterator
                if (iterators.size() == 1)
                {
                    next = nextElm.currentMinElement.left;
                    break;
                }

                // if next target element is smaller than the max value
                // seen in the last pass across all iterators, we can tell
                // all iterators to safely skip forwards to the last known max
                if (nextElm.currentMaxElement != null
                        && comparator.compare(nextElm.currentMinElement.left, nextElm.currentMaxElement.left) < 0)
                {
                    for(SkippableIterator<T> iterator : iterators)
                        iterator.skipTo(nextElm.currentMaxElement.left);
                    nextElm = findNextElement();
                    continue;
                }

                boolean foundInAllIterators = true;
                for (int i = 0; i < iterators.size(); i++)
                {
                    if (nextElm.currentMinElement.right.equals(i))
                        continue;

                    int cmpRes = comparator.compare(nextElm.currentMinElement.left, currentPerIterator.get(i));
                    if (cmpRes == 0)
                    {
                        // found the same value in this iterator, this satisfies our AND merge
                        tmp = nextElm.currentMinElement.left;
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
        else if (opType == OperationType.OR && nextElm.currentMinElement != null)
        {
            // check if the previous value returned equals the next
            // possible value. Because OR operator, elements must be de-duped
            if (next != null && comparator.compare(nextElm.currentMinElement.left, next) == 0)
                while (nextElm != null && nextElm.currentMinElement != null
                        && comparator.compare(nextElm.currentMinElement.left, next) == 0)
                    nextElm = findNextElement();
        }

        if (nextElm == null || nextElm.currentMinElement == null)
            return false;

        next = nextElm.currentMinElement.left;
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
        Pair<T,Integer> minVal = null;
        Pair<T,Integer> maxVal = null;
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
                minVal = maxVal = Pair.create(prev, i);
                continue;
            }

            // found value smaller than previous candidate from other iterator
            if (comparator.compare(prev, minVal.left) < 0)
                minVal = Pair.create(prev, i);

            // this iterator has a value larger than seen before?
            if (comparator.compare(prev, maxVal.left) > 0)
                maxVal = Pair.create(prev, i);
        }
        if (minVal != null)
            currentPerIterator.set(minVal.right, null);
        return new Element(minVal, maxVal);
    }

    private class Element
    {
        // <left:value,right:index of iterator value from>
        protected Pair<T,Integer> currentMinElement;
        protected Pair<T,Integer> currentMaxElement;

        protected Element(Pair<T,Integer> currentMinElement, Pair<T,Integer> currentMaxElement)
        {
            this.currentMinElement = currentMinElement;
            this.currentMaxElement = currentMaxElement;
        }
    }
}
