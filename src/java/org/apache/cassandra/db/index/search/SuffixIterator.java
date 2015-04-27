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
    private final long count;

    public SuffixIterator(Expression expression,
                          SkippableIterator<Long, Token> memtableIterator,
                          final Collection<SSTableIndex> perSSTableIndexes)
    {
        List<SkippableIterator<Long, Token>> keys = new ArrayList<>(perSSTableIndexes.size());

        long tokenCount = 0;
        if (memtableIterator != null)
        {
            keys.add(memtableIterator);
            tokenCount += memtableIterator.getCount();
        }

        for (SSTableIndex index : perSSTableIndexes)
        {
            if (!index.reference())
                continue;

            SkippableIterator<Long, Token> results = index.search(expression);
            if (results == null)
            {
                index.release();
                continue;
            }

            keys.add(results);
            referencedIndexes.add(index);
            tokenCount += results.getCount();
        }

        union = new LazyMergeSortIterator<>(LazyMergeSortIterator.OperationType.OR, keys);
        count = tokenCount;
    }

    @Override
    public Long getMinimum()
    {
        return union.getMinimum();
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
    public boolean intersect(Token token)
    {
        return union.intersect(token);
    }

    @Override
    public long getCount()
    {
        return count;
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(union);
        for (SSTableIndex index : referencedIndexes)
            index.release();
    }
}
