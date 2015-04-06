package org.apache.cassandra.db.index.utils;

import java.io.Closeable;
import java.util.Iterator;

public interface SkippableIterator<K extends Comparable<K>, T extends CombinedValue<K>> extends Iterator<T>, Closeable
{
    /**
     * @return The minimum of the iterator.
     */
    K getMinimum();

    /**
     * When called, this iterators current position should
     * be skipped forwards until finding either:
     *   1) an element equal to next
     *   2) the greatest element that still evaluates to less than next
     *   3) the end of the iterator
     * @param next value to skip the iterator forward until matching
     */
    void skipTo(K next);

    /**
     * Try to intersect iterator with given element.
     *
     * @param element The element to intersect with.
     *
     * @return true if iterator has given element, false otherwise.
     */
    boolean intersect(T element);

    /**
     * @return number of elements this iterator contains
     */
    long getCount();
}
