package org.apache.cassandra.db.index.utils;

import java.util.Iterator;

public interface SeekableIterator<T> extends Iterator<T> {
    /**
     * Skip forward in the iterator until either running
     * out of elements or finding an equal value
     * @param next value to seek the iterator forward until matching
     */
    public void skipTo(T next);
}
