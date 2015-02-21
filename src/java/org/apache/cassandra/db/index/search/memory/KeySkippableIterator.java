package org.apache.cassandra.db.index.search.memory;

import java.io.IOException;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.search.container.TokenTree.Token;
import org.apache.cassandra.db.index.utils.CombinedValue;
import org.apache.cassandra.db.index.utils.SkippableIterator;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;

public class KeySkippableIterator extends AbstractIterator<Token> implements SkippableIterator<Long, Token>
{
    private final DKIterator keys;

    public KeySkippableIterator(Iterator<DecoratedKey> keys)
    {
        this.keys = new DKIterator(keys);
    }

    @Override
    protected Token computeNext()
    {
        return keys.hasNext() ? new DKToken(keys.next()) : endOfData();
    }

    @Override
    public void skipTo(Long next)
    {
        while (keys.hasNext())
        {
            DecoratedKey key = keys.peek();
            if (Long.compare((long) key.token.token, next) >= 0)
                break;

            // consume smaller key
            keys.next();
        }
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