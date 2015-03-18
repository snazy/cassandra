package org.apache.cassandra.db.index.search.sa;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.index.search.Descriptor;
import org.apache.cassandra.db.index.search.OnDiskSABuilder.Mode;
import org.apache.cassandra.db.index.search.container.TokenTreeBuilder;
import org.apache.cassandra.db.marshal.AbstractType;

public abstract class SA<T extends Buffer>
{
    protected final AbstractType<?> comparator;
    protected final Mode mode;

    protected final List<Term<T>> terms = new ArrayList<>();
    protected int charCount = 0;

    public SA(AbstractType<?> comparator, Mode mode)
    {
        this.comparator = comparator;
        this.mode = mode;
    }

    public Mode getMode()
    {
        return mode;
    }

    public void add(ByteBuffer termValue, TokenTreeBuilder tokens)
    {
        Term<T> term = getTerm(termValue, tokens);
        terms.add(term);
        charCount += term.length();
    }

    public abstract TermIterator finish(Descriptor descriptor);

    protected abstract Term<T> getTerm(ByteBuffer termValue, TokenTreeBuilder tokens);
}
