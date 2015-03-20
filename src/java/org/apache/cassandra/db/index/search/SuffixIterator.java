package org.apache.cassandra.db.index.search;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.AbstractIterator;
import org.apache.cassandra.db.index.search.container.TokenTree.Token;
import org.apache.cassandra.db.index.search.plan.Expression;
import org.apache.cassandra.db.index.utils.LazyMergeSortIterator;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.io.util.FileUtils;

public class SuffixIterator extends AbstractIterator<Token> implements SkippableIterator<Long, Token>
{
    private final SkippableIterator<Long, Token> union;
    private final List<SSTableIndex> referencedIndexes = new ArrayList<>();

    public SuffixIterator(Expression expression,
                          SkippableIterator<Long, Token> memtableIterator,
                          final Collection<SSTableIndex> perSSTableIndexes)
    {
        List<SkippableIterator<Long, Token>> keys = new ArrayList<>(perSSTableIndexes.size());

        if (memtableIterator != null)
            keys.add(memtableIterator);

        for (SSTableIndex index : perSSTableIndexes)
        {
            if (!index.reference())
                continue;

            keys.add(index.search(expression));
            referencedIndexes.add(index);
        }

        union = new LazyMergeSortIterator<>(LazyMergeSortIterator.OperationType.OR, keys);
    }

    @Override
    protected Token computeNext()
    {
        return union.hasNext() ? union.next() : endOfData();
    }

    @Override
    public void skipTo(Long next)
    {
        union.skipTo(next);
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(union);
        for (SSTableIndex index : referencedIndexes)
            index.release();
    }
}
