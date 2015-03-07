package org.apache.cassandra.db.index.search.memory;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.index.search.Expression;
import org.apache.cassandra.db.index.SuffixArraySecondaryIndex.IndexMode;
import org.apache.cassandra.db.index.search.container.TokenTree.Token;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.github.jamm.MemoryMeter;

public abstract class ColumnIndex
{
    protected final IndexMode mode;
    protected final ColumnDefinition definition;

    protected ColumnIndex(IndexMode mode, ColumnDefinition definition)
    {
        this.mode = mode;
        this.definition = definition;
    }

    public abstract void add(ByteBuffer value, ByteBuffer key);
    public abstract SkippableIterator<Long, Token> search(Expression.Column expression);
    public abstract long estimateSize(MemoryMeter meter);

    public static ColumnIndex forColumn(ColumnDefinition column, IndexMode mode)
    {
        AbstractType<?> v = column.getValidator();
        return (v instanceof AsciiType || v instanceof UTF8Type)
                ? new TrieColumnIndex(mode, column)
                : new SkipListColumnIndex(mode, column);
    }
}
