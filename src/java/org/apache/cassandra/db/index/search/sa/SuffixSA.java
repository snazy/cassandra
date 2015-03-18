package org.apache.cassandra.db.index.search.sa;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import org.apache.cassandra.db.index.search.Descriptor;
import org.apache.cassandra.db.index.search.OnDiskSABuilder;
import org.apache.cassandra.db.index.search.container.TokenTreeBuilder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Pair;

import com.google.common.base.Charsets;
import net.mintern.primitive.Primitive;
import net.mintern.primitive.comparators.LongComparator;

public class SuffixSA extends SA<CharBuffer>
{
    public SuffixSA(AbstractType<?> comparator, OnDiskSABuilder.Mode mode)
    {
        super(comparator, mode);
    }

    @Override
    protected Term<CharBuffer> getTerm(ByteBuffer termValue, TokenTreeBuilder tokens)
    {
        return new CharTerm(charCount, Charsets.UTF_8.decode(termValue), tokens);
    }

    @Override
    public TermIterator finish(Descriptor descriptor)
    {
        return new SASuffixIterator(descriptor);
    }

    private class SASuffixIterator extends TermIterator
    {
        private final long[] suffixes;

        private int current = 0;
        private ByteBuffer lastProcessedSuffix;
        private TokenTreeBuilder container;
        private final Descriptor descriptor;

        public SASuffixIterator(Descriptor d)
        {
            // each element has term index and char position encoded as two 32-bit integers
            // to avoid binary search per suffix while sorting suffix array.
            suffixes = new long[charCount];
            descriptor = d;

            long termIndex = -1, currentTermLength = -1;
            for (int i = 0; i < charCount; i++)
            {
                if (i >= currentTermLength || currentTermLength == -1)
                {
                    Term currentTerm = terms.get((int) ++termIndex);
                    currentTermLength = currentTerm.getPosition() + currentTerm.length();
                }

                suffixes[i] = (termIndex << 32) | i;
            }

            Primitive.sort(suffixes, new LongComparator()
            {
                @Override
                public int compare(long a, long b)
                {
                    Term aTerm = terms.get((int) (a >>> 32));
                    Term bTerm = terms.get((int) (b >>> 32));
                    return comparator.compare(aTerm.getSuffix(((int) a) - aTerm.getPosition()),
                                              bTerm.getSuffix(((int) b) - bTerm.getPosition()));
                }
            });
        }

        private Pair<ByteBuffer, TokenTreeBuilder> suffixAt(int position)
        {
            long index = suffixes[position];
            Term term = terms.get((int) (index >>> 32));
            return Pair.create(term.getSuffix(((int) index) - term.getPosition()), term.getTokens());
        }

        @Override
        public ByteBuffer minTerm()
        {
            return suffixAt(0).left;
        }

        @Override
        public ByteBuffer maxTerm()
        {
            return suffixAt(suffixes.length - 1).left;
        }

        @Override
        protected Pair<ByteBuffer, TokenTreeBuilder> computeNext()
        {
            while (true)
            {
                if (current >= suffixes.length)
                {
                    if (lastProcessedSuffix == null)
                        return endOfData();

                    Pair<ByteBuffer, TokenTreeBuilder> result = finishSuffix();

                    lastProcessedSuffix = null;
                    return result;
                }

                Pair<ByteBuffer, TokenTreeBuilder> suffix = suffixAt(current++);

                if (lastProcessedSuffix == null)
                {
                    lastProcessedSuffix = suffix.left;
                    container = new TokenTreeBuilder(descriptor, suffix.right.getTokens());
                }
                else if (comparator.compare(lastProcessedSuffix, suffix.left) == 0)
                {
                    lastProcessedSuffix = suffix.left;
                    container.add(suffix.right.getTokens());
                }
                else
                {
                    Pair<ByteBuffer, TokenTreeBuilder> result = finishSuffix();

                    lastProcessedSuffix = suffix.left;
                    container = new TokenTreeBuilder(descriptor, suffix.right.getTokens());

                    return result;
                }
            }
        }

        private Pair<ByteBuffer, TokenTreeBuilder> finishSuffix()
        {
            return Pair.create(lastProcessedSuffix, container.finish());
        }
    }
}
