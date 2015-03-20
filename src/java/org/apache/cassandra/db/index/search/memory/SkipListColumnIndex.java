package org.apache.cassandra.db.index.search.memory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.SuffixArraySecondaryIndex.IndexMode;
import org.apache.cassandra.db.index.search.plan.Expression;
import org.apache.cassandra.db.index.search.container.TokenTree.Token;
import org.apache.cassandra.db.index.utils.LazyMergeSortIterator;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.service.StorageService;

import org.github.jamm.MemoryMeter;

public class SkipListColumnIndex extends ColumnIndex
{
    private final ConcurrentSkipListMap<ByteBuffer, ConcurrentSkipListSet<DecoratedKey>> index;

    public SkipListColumnIndex(IndexMode mode, ColumnDefinition definition)
    {
        super(mode, definition);
        index = new ConcurrentSkipListMap<>(definition.getValidator());
    }

    @Override
    public void add(ByteBuffer value, ByteBuffer key)
    {
        final DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);
        ConcurrentSkipListSet<DecoratedKey> keys = index.get(value);

        if (keys == null)
        {
            ConcurrentSkipListSet<DecoratedKey> newKeys = new ConcurrentSkipListSet<>(DecoratedKey.comparator);
            keys = index.putIfAbsent(value, newKeys);
            if (keys == null)
                keys = newKeys;
        }

        keys.add(dk);
    }

    @Override
    public SkippableIterator<Long, Token> search(Expression expression)
    {
        List<SkippableIterator<Long, Token>> union = new ArrayList<>();

        ByteBuffer min = expression.lower == null ? null : expression.lower.value;
        ByteBuffer max = expression.upper == null ? null : expression.upper.value;

        SortedMap<ByteBuffer, ConcurrentSkipListSet<DecoratedKey>> search;

        if (min == null && max == null)
        {
            throw new IllegalArgumentException();
        }
        if (min != null && max != null)
        {
            search = index.subMap(min, expression.lower.inclusive, max, expression.upper.inclusive);
        }
        else if (min == null)
        {
            search = index.headMap(max, expression.upper.inclusive);
        }
        else
        {
            search = index.tailMap(min, expression.lower.inclusive);
        }

        for (Map.Entry<ByteBuffer, ConcurrentSkipListSet<DecoratedKey>> keys : search.entrySet())
            union.add(new KeySkippableIterator(keys.getValue().iterator()));

        return new LazyMergeSortIterator<>(LazyMergeSortIterator.OperationType.OR, union);
    }

    @Override
    public long estimateSize(MemoryMeter meter)
    {
        return meter.measureDeep(index);
    }
}
