package org.apache.cassandra.db.index.search.sa;

import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.index.search.container.TokenTreeBuilder;
import org.apache.cassandra.db.marshal.AbstractType;

public abstract class Term<T extends Buffer>
{
    protected final int position;
    protected final T value;
    protected TokenTreeBuilder tokens;


    public Term(int position, T value, TokenTreeBuilder tokens)
    {
        this.position = position;
        this.value = value;
        this.tokens = tokens;
    }

    public int getPosition()
    {
        return position;
    }

    public abstract ByteBuffer getTerm();
    public abstract ByteBuffer getSuffix(int start);

    public TokenTreeBuilder getTokens()
    {
        return tokens;
    }

    public abstract int compareTo(AbstractType<?> comparator, Term other);

    public abstract int length();

}

