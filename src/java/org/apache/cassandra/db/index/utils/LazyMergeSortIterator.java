package org.apache.cassandra.db.index.utils;

import org.apache.cassandra.db.marshal.AbstractType;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Returns a single merged and sorted iterator from the provided iterators.
 * Using OperationType.AND will return a iterator with only elements present
 * in all input iterators. OperationType.OR will return all elements from the
 * input iterators in sorted order.
 */
public class LazyMergeSortIterator<T> implements Iterator<T>
{
    public static enum OperationType
    {
        AND, OR
    }

    private AbstractType<T> comparator;
    private OperationType opType;
    private List<SeekableIterator<T>> iterators;

    private T next = null;
    // buffer of elements already taken from an iterator
    // that maybe used in the next iteration
    private List<T> dequeuedElmBuffer;

    public LazyMergeSortIterator(AbstractType<T> comparator, OperationType opType,
                                 List<SeekableIterator<T>> iterators)
    {
        this.comparator = comparator;
        this.opType = opType;
        this.iterators = iterators;
        this.dequeuedElmBuffer = new ArrayList<>((List<T>) Collections.nCopies(iterators.size(), null));
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
            // currently, we only need additional handling for AND
            while (nextElm != null)
            {
                boolean foundInAllIterators = true;
                for (int i = 0; i < iterators.size(); i++)
                {
                    if (nextElm.iteratorIdx == i)
                        continue;

                    ByteBuffer o1 = (nextElm.value instanceof ByteBuffer)
                            ? (ByteBuffer) nextElm.value
                            : comparator.decompose(nextElm.value);
                    ByteBuffer o2 = (dequeuedElmBuffer.get(i) instanceof ByteBuffer)
                            ? (ByteBuffer) dequeuedElmBuffer.get(i)
                            : comparator.decompose(dequeuedElmBuffer.get(i));
                    int cmpRes = comparator.compare(o1, o2);
                    if (cmpRes == 0)
                    {
                        // found the same value in this iterator, this satisfies our AND merge
                        tmp = nextElm.value;
                        dequeuedElmBuffer.set(i, null);
                    }
                    else if (cmpRes > 0)
                    {
                        // skip forward in iterator until we find a value equal to
                        // the current potential next value
                        iterators.get(i).skipTo(nextElm.value);
                        if (iterators.get(i).hasNext())
                        {
                            tmp = nextElm.value;
                            dequeuedElmBuffer.set(i, null);
                        }
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
        next = (nextElm == null) ? null : nextElm.value;
        return nextElm != null && nextElm.value != null;
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

    private Element findNextElement()
    {
        int iteratorIdx = 0;
        T nextVal = null;
        for (int i = 0; i < iterators.size(); i++)
        {
            T prev = dequeuedElmBuffer.get(i);
            if (prev == null && iterators.get(i).hasNext())
            {
                T itNext = iterators.get(i).next();
                dequeuedElmBuffer.set(i, itNext);
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

            ByteBuffer o1 = (prev instanceof ByteBuffer)
                    ? (ByteBuffer) prev : comparator.decompose(prev);
            ByteBuffer o2 = (nextVal instanceof ByteBuffer)
                    ? (ByteBuffer) nextVal : comparator.decompose(nextVal);
            if (comparator.compare(o1, o2) <= 0)
            {
                // found value less than our previous candidate in current iterator
                iteratorIdx = i;
                nextVal = prev;
            }
        }
        dequeuedElmBuffer.set(iteratorIdx, null);
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
