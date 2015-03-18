package org.apache.cassandra.db.index.search.sa;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.cassandra.db.index.search.Descriptor;
import org.apache.cassandra.db.index.search.OnDiskSABuilder;
import org.apache.cassandra.db.index.search.container.TokenTreeBuilder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Pair;

public class IntegralSA extends SA<ByteBuffer>
{
    public IntegralSA(AbstractType<?> comparator, OnDiskSABuilder.Mode mode)
    {
        super(comparator, mode);
    }

    @Override
    public Term<ByteBuffer> getTerm(ByteBuffer termValue, TokenTreeBuilder tokens)
    {
        return new ByteTerm(charCount, termValue, tokens);
    }

    @Override
    public TermIterator finish(Descriptor descriptor)
    {
        return new IntegralSuffixIterator();
    }


    private class IntegralSuffixIterator extends TermIterator
    {
        private final Iterator<Term<ByteBuffer>> termIterator;

        public IntegralSuffixIterator()
        {
            Collections.sort(terms, new Comparator<Term<?>>()
            {
                @Override
                public int compare(Term<?> a, Term<?> b)
                {
                    return a.compareTo(comparator, b);
                }
            });

            termIterator = terms.iterator();
        }

        @Override
        public ByteBuffer minTerm()
        {
            return terms.get(0).getTerm();
        }

        @Override
        public ByteBuffer maxTerm()
        {
            return terms.get(terms.size() - 1).getTerm();
        }

        @Override
        protected Pair<ByteBuffer, TokenTreeBuilder> computeNext()
        {
            if (!termIterator.hasNext())
                return endOfData();

            Term<ByteBuffer> term = termIterator.next();
            return Pair.create(term.getTerm(), term.getTokens().finish());
        }
    }
}
