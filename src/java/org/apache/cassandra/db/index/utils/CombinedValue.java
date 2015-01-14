package org.apache.cassandra.db.index.utils;

public interface CombinedValue<V> extends Comparable<CombinedValue<V>>
{
    void merge(CombinedValue<V> other);

    V get();
}
