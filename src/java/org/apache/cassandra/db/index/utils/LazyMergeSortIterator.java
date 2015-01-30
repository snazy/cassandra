package org.apache.cassandra.db.index.utils;

import java.io.IOException;
import java.util.*;

import org.apache.cassandra.utils.Pair;

/**
 * Returns a single merged and sorted iterator from the provided iterators.
 * Using OperationType.AND will return a iterator with only elements present
 * in all input iterators. OperationType.OR will return all elements from the
 * input iterators in sorted order.
 */
public class LazyMergeSortIterator<K extends Comparable<K>, T extends CombinedValue<K>> implements SkippableIterator<K, T>
{
    public static enum OperationType
    {
        AND, OR
    }

    private OperationType opType;
    private List<SkippableIterator<K, T>> iterators;

    // buffer of elements already taken from an iterator
    // that maybe used in the next iteration
    private List<T> currentPerIterator;
    private T next = null;
    private T lookahead = null;
    private T nextLookahead = null;


    public LazyMergeSortIterator(OperationType opType, List<SkippableIterator<K, T>> iterators)
    {
        this.opType = opType;
        this.iterators = iterators;
        this.currentPerIterator = new ArrayList<>(iterators.size());
        {
            for (int i = 0; i < iterators.size(); i++)
                currentPerIterator.add(null);
        }
    }

    @Override
    public boolean hasNext()
    {
        Element nextElm = findNextElement();

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
                        && nextElm.currentMinElement.left.compareTo(nextElm.currentMaxElement.left) < 0)
                {
                    for(SkippableIterator<K, T> iterator : iterators)
                        iterator.skipTo(nextElm.currentMaxElement.left.get());
                    nextElm = findNextElement();
                    continue;
                }

                boolean foundInAllIterators = true;
                for (int i = 0; i < iterators.size(); i++)
                {
                    if (nextElm.currentMinElement.right.equals(i))
                    {
                        if (tmp == null)
                            tmp = nextElm.currentMinElement.left;
                        else
                            tmp.merge(nextElm.currentMinElement.left);
                        continue;
                    }

                    int cmpRes = nextElm.currentMinElement.left.compareTo(currentPerIterator.get(i));
                    if (cmpRes == 0)
                    {
                        // found the same value in this iterator, this satisfies our AND merge
                        if (tmp == null)
                            tmp = currentPerIterator.get(i);
                        else
                            tmp.merge(currentPerIterator.get(i));

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

            if (nextElm == null)
                return false;
        }
        else if (opType == OperationType.OR)
        {
            // if no more elements to consume from iterators and
            // we've also already consumed any possible lookahead
            // elements, then we are done
            if (nextElm == null && lookahead == null)
                return false;

            // bc merge op is OR, elements must be de-duped so
            // if equal elements are found across iterators, this
            // iterator should return a single merged element
            next = (next == null && nextElm != null)
                    ? nextElm.currentMinElement.left : lookahead;

            if (nextElm == null)
            {
                lookahead = nextLookahead;
                nextLookahead = null;
            }
            else
            {
                if (nextLookahead != null)
                {
                    lookahead = nextLookahead;
                    nextLookahead = nextElm.currentMinElement.left;
                }
                else
                {
                    Element nextElmPlusOne = findNextElement();
                    if (nextElmPlusOne != null)
                    {
                        if (next == null)
                            return false;

                        lookahead = nextElmPlusOne.currentMinElement.left;
                        // if first element in multiple iterators is equal
                        // we need to merge and de-dupe here too

                        while (lookahead.compareTo(next) == 0)
                        {
                            next.merge(lookahead);
                            nextElmPlusOne = findNextElement();
                            if (nextElmPlusOne == null)
                                break;

                            lookahead = nextElmPlusOne.currentMinElement.left;
                        }

                        Element nextElmPlusTwo = findNextElement();
                        nextLookahead = (nextElmPlusTwo != null)
                                ? nextElmPlusTwo.currentMinElement.left : null;
                    }
                }

                // if both next+1 and next+2 are equal, continue merging next+1
                // and next+2 until nextLookahead is greater than lookahead
                while (nextLookahead != null && nextLookahead.compareTo(lookahead) == 0)
                {
                    nextLookahead.merge(lookahead);

                    lookahead = nextLookahead;
                    Element nextElmPlusOne = findNextElement();
                    nextLookahead = (nextElmPlusOne == null)
                                    ? null : nextElmPlusOne.currentMinElement.left;
                }
            }
        }

        return next != null;
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
    public void skipTo(K next)
    {
        for (SkippableIterator<K, T> itr : iterators)
            itr.skipTo(next);
    }

    private Element findNextElement()
    {
        Pair<T, Integer> minVal = null;
        Pair<T, Integer> maxVal = null;
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
            if (prev.compareTo(minVal.left) < 0)
                minVal = Pair.create(prev, i);

            // this iterator has a value larger than seen before?
            if (prev.compareTo(maxVal.left) > 0)
                maxVal = Pair.create(prev, i);
        }
        if (minVal != null)
            currentPerIterator.set(minVal.right, null);
        return (minVal == null) ? null : new Element(minVal, maxVal);
    }

    @Override
    public void close() throws IOException
    {
        for (SkippableIterator<K, T> i : iterators)
            i.close();
    }

    private class Element
    {
        // <left:value,right:index of iterator value from>
        protected Pair<T, Integer> currentMinElement;
        protected Pair<T, Integer> currentMaxElement;

        protected Element(Pair<T, Integer> currentMinElement, Pair<T, Integer> currentMaxElement)
        {
            this.currentMinElement = currentMinElement;
            this.currentMaxElement = currentMaxElement;
        }
    }
}
