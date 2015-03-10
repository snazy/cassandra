package org.apache.cassandra.db.index.search.memory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.index.SuffixArraySecondaryIndex;
import org.apache.cassandra.db.index.SuffixArraySecondaryIndex.IndexMode;
import org.apache.cassandra.db.index.search.Expression;
import org.apache.cassandra.db.index.search.container.TokenTree;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.db.index.utils.TypeUtil;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Pair;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.github.jamm.MemoryMeter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexMemtable
{
    private static final Logger logger = LoggerFactory.getLogger(IndexMemtable.class);

    private final MemoryMeter meter;

    private final ConcurrentMap<ByteBuffer, ColumnIndex> indexes;
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
        for (ColumnIndex index : indexes.values())
            deepSize += index.estimateSize(meter);

        return deepSize;
    }

    public void index(ByteBuffer key, ColumnFamily cf)
    {
        for (Column column : cf)
        {
            Pair<ColumnDefinition, IndexMode> columnDefinition = backend.getIndexDefinition(column.name());
            if (columnDefinition == null)
                continue;

            ColumnIndex index = indexes.get(column.name());
            if (index == null)
            {
                ColumnIndex newIndex = ColumnIndex.forColumn(columnDefinition.left, columnDefinition.right);
                index = indexes.putIfAbsent(column.name(), newIndex);
                if (index == null)
                    index = newIndex;
            }

            final AbstractType<?> keyValidator = backend.getBaseCfs().metadata.getKeyValidator();
            final AbstractType<?> comparator = backend.getBaseCfs().getComparator();
            final ColumnDefinition definition = columnDefinition.left;

            ByteBuffer value = column.value();

            if (!TypeUtil.isValid(value, definition.getValidator()))
            {
                int size = value.remaining();
                if ((value = TypeUtil.tryUpcast(value, definition.getValidator())) == null)
                {
                    logger.error("Can't add column {} to index for key: {}, value size {} bytes, validator: {}.",
                                 comparator.getString(definition.name),
                                 keyValidator.getString(key),
                                 size,
                                 definition.getValidator());
                    return;
                }
            }

            index.add(value, key);
        }
    }

    public SkippableIterator<Long, TokenTree.Token> search(Expression.Column expression)
    {
        ColumnIndex index = indexes.get(expression.name);
        return index == null ? null : index.search(expression);
    }
}
