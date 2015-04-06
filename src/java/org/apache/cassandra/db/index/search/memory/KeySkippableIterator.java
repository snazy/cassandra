package org.apache.cassandra.db.index.search.memory;

import java.io.IOException;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.search.container.TokenTree.Token;
import org.apache.cassandra.db.index.utils.CombinedValue;
import org.apache.cassandra.db.index.utils.SkippableIterator;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;

public class KeySkippableIterator extends AbstractIterator<Token> implements SkippableIterator<Long, Token>
{
    private final DKIterator iterator;
    private final ConcurrentSkipListSet<DecoratedKey> keys;

    public KeySkippableIterator(ConcurrentSkipListSet<DecoratedKey> keys)
    {
        this.keys = keys;
        this.iterator = new DKIterator(keys.iterator());
    }

    @Override
    public Long getMinimum()
    {
        return keys.isEmpty() ? null : (Long) keys.first().getToken().token;
    }

    @Override
    protected Token computeNext()
    {
        return iterator.hasNext() ? new DKToken(iterator.next()) : endOfData();
    }

    @Override
    public void skipTo(Long next)
    {
        while (iterator.hasNext())
        {
            DecoratedKey key = iterator.peek();
            if (Long.compare((long) key.token.token, next) >= 0)
                break;

            // consume smaller key
            iterator.next();
        }
    }

    @Override
    public boolean intersect(Token token)
    {
        final long searchToken = token.get();

        for (DecoratedKey key : keys)
        {
            if (key.getToken().token.equals(searchToken))
                return true;
        }

        return false;
    }

    @Override
    public long getCount()
    {
        return keys.size();
    }

    @Override
    public void close() throws IOException
    {}

    private static class DKIterator extends AbstractIterator<DecoratedKey> implements PeekingIterator<DecoratedKey>
    {
        private final Iterator<DecoratedKey> keys;

        public DKIterator(Iterator<DecoratedKey> keys)
        {
            this.keys = keys;
        }

        @Override
        protected DecoratedKey computeNext()
        {
            return keys.hasNext() ? keys.next() : endOfData();
        }
    }

    private static class DKToken extends Token
    {
        private final SortedSet<DecoratedKey> keys;

        public DKToken(final DecoratedKey key)
        {
            super((long) key.token.token, null);

            keys = new TreeSet<DecoratedKey>(DecoratedKey.comparator)
            {{
                add(key);
            }};
        }

        public void merge(CombinedValue<Long> other)
        {
            if (!(other instanceof Token))
                return;

            Token o = (Token) other;
            assert o.get().equals(token);

            if (o instanceof DKToken)
            {
                keys.addAll(((DKToken) o).keys);
            }
            else
            {
                for (DecoratedKey key : o)
                    keys.add(key);
            }
        }

        @Override
        public Iterator<DecoratedKey> iterator()
        {
            return keys.iterator();
        }
    }
}