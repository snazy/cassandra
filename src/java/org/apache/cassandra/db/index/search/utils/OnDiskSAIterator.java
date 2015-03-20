package org.apache.cassandra.db.index.search.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.db.index.search.OnDiskSA;
import org.apache.cassandra.db.index.search.OnDiskSA.DataSuffix;
import org.apache.cassandra.db.index.utils.LazyMergeSortIterator;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.db.marshal.AbstractType;

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

}
