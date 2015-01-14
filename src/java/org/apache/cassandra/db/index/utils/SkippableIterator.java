package org.apache.cassandra.db.index.utils;

import java.io.Closeable;
import java.util.Iterator;

public interface SkippableIterator<K extends Comparable<K>, T extends CombinedValue<K>> extends Iterator<T>, Closeable
{
    /**
     * When called, this iterators current position should
     * be skipped forwards until finding either:
     *   1) an element equal to next
     *   2) the greatest element that still evaluates to less than next
     *   3) the end of the iterator
     * @param next value to skip the iterator forward until matching
     */
    public void skipTo(K next);
}
