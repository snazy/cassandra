package org.apache.cassandra.db.index.search.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.db.index.search.Descriptor;
import org.apache.cassandra.db.index.search.OnDiskSA;
import org.apache.cassandra.db.index.search.OnDiskSA.DataSuffix;
import org.apache.cassandra.db.index.search.OnDiskSABuilder;
import org.apache.cassandra.db.index.search.container.TokenTreeBuilder;
import org.apache.cassandra.db.index.utils.LazyMergeSortIterator;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Pair;

import com.google.common.collect.AbstractIterator;

public class OnDiskSAIterator extends AbstractIterator<CombinedTerm> implements SkippableIterator<DataSuffix, CombinedTerm>
{
    private final AbstractType<?> comparator;
    private final Iterator<DataSuffix> terms;

    public OnDiskSAIterator(AbstractType<?> comparator, Iterator<DataSuffix> terms)
    {
        this.comparator = comparator;
        this.terms = terms;
    }

    public static SkippableIterator<DataSuffix, CombinedTerm> union(OnDiskSA... union)
    {
        List<SkippableIterator<DataSuffix, CombinedTerm>> iterators = new ArrayList<>(union.length);
        for (OnDiskSA e : union)
            iterators.add(new OnDiskSAIterator(e.getComparator(), e.iterator()));

        return new LazyMergeSortIterator<>(LazyMergeSortIterator.OperationType.OR, iterators);
    }

    @Override
    protected CombinedTerm computeNext()
    {
        return terms.hasNext() ? new CombinedTerm(comparator, terms.next()) : endOfData();
    }

    @Override
    public void skipTo(DataSuffix next)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException
    {}

    public static class CombinedSuffixIterator extends OnDiskSABuilder.SuffixIterator
    {
        final Descriptor descriptor;
        final SkippableIterator<DataSuffix, CombinedTerm> union;
        final ByteBuffer min;
        final ByteBuffer max;

        public CombinedSuffixIterator(Descriptor d, OnDiskSA... sas)
        {
            descriptor = d;
            union = union(sas);

            AbstractType<?> comparator = sas[0].getComparator(); // assumes all SAs have same comparator
            ByteBuffer minimum = sas[0].minSuffix();
            ByteBuffer maximum = sas[0].maxSuffix();
            for (int i = 1; i < sas.length; i++)
            {
                minimum = comparator.compare(minimum, sas[i].minSuffix()) > 0 ? sas[i].minSuffix() : minimum;
                maximum = comparator.compare(maximum, sas[i].maxSuffix()) < 0 ? sas[i].maxSuffix() : maximum;
            }

            min = minimum;
            max = maximum;
        }

        public ByteBuffer minSuffix()
        {
            return min;
        }

        public ByteBuffer maxSuffix()
        {
            return max;
        }

        @Override
        protected Pair<ByteBuffer, TokenTreeBuilder> computeNext()
        {
            if (!union.hasNext())
            {
                return endOfData();
            }
            else
            {
                CombinedTerm term = union.next();
                return Pair.create(term.getTerm(), term.getTokenTreeBuilder(descriptor));
            }

        }
    }
}
