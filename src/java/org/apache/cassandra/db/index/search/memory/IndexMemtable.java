package org.apache.cassandra.db.index.search.memory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.index.SuffixArraySecondaryIndex;
import org.apache.cassandra.db.index.search.container.TokenTree;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.db.index.SuffixArraySecondaryIndex.Expression;

import org.apache.cassandra.utils.Pair;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.github.jamm.MemoryMeter;

public class IndexMemtable
{
    private final MemoryMeter meter;

    private final ConcurrentMap<ByteBuffer, InMemoryIndex> indexes;
    private final SuffixArraySecondaryIndex backend;

    public IndexMemtable(final SuffixArraySecondaryIndex backend)
    {
        this.indexes = new NonBlockingHashMap<>();
        this.backend = backend;
        this.meter = new MemoryMeter().omitSharedBufferOverhead().withTrackerProvider(new Callable<Set<Object>>()
        {
            public Set<Object> call() throws Exception
            {
                // avoid counting this once for each row
                Set<Object> set = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
                set.add(backend.getBaseCfs().metadata);
                return set;
            }
        });
    }

    public long estimateSize()
    {
        long deepSize = 0;
        for (InMemoryIndex index : indexes.values())
            deepSize += index.estimateSize(meter);

        return deepSize;
    }

    public void index(ByteBuffer key, ColumnFamily cf)
    {
        for (ColumnDefinition indexedColumn : backend.getColumnDefs())
        {
            Iterator<Column> itr = cf.iterator();
            Column column = null;
            while (itr.hasNext())
            {
                Column col = itr.next();
                if (col.name().equals(indexedColumn.name))
                {
                    column = col;
                    break;
                }
            }
            if (column == null)
                continue;

            InMemoryIndex index = indexes.get(column.name());
            if (index == null)
            {
                InMemoryIndex newIndex = new InMemoryIndex(indexedColumn, backend.getMode(column.name()));
                index = indexes.putIfAbsent(column.name(), newIndex);
                if (index == null)
                    index = newIndex;
            }

            index.index(column.value(), key);
        }
    }

    public SkippableIterator<Long, TokenTree.Token> search(Pair<ByteBuffer, Expression> expression)
    {
        InMemoryIndex index = indexes.get(expression.left);
        return index == null ? null : index.search(expression.right);
    }
}
