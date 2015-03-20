package org.apache.cassandra.db.index.search.utils;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.index.search.Descriptor;
import org.apache.cassandra.db.index.search.OnDiskSA;
import org.apache.cassandra.db.index.search.container.TokenTreeBuilder;
import org.apache.cassandra.db.index.search.sa.TermIterator;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Pair;

public class CombinedTermIterator extends TermIterator
{
    final Descriptor descriptor;
    final SkippableIterator<OnDiskSA.DataSuffix, CombinedTerm> union;
    final ByteBuffer min;
    final ByteBuffer max;

    public CombinedTermIterator(OnDiskSA... sas)
    {
        this(Descriptor.CURRENT, sas);
    }

    public CombinedTermIterator(Descriptor d, OnDiskSA... sas)
    {
        descriptor = d;
        union = OnDiskSAIterator.union(sas);

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

    public ByteBuffer minTerm()
    {
        return min;
    }

    public ByteBuffer maxTerm()
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
