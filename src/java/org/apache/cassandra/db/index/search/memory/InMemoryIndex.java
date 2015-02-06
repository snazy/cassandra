package org.apache.cassandra.db.index.search.memory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.PeekingIterator;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.SuffixArraySecondaryIndex;
import org.apache.cassandra.db.index.search.OnDiskSABuilder;
import org.apache.cassandra.db.index.search.container.TokenTree;
import org.apache.cassandra.db.index.search.tokenization.AbstractTokenizer;
import org.apache.cassandra.db.index.utils.CombinedValue;
import org.apache.cassandra.db.index.utils.LazyMergeSortIterator;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.db.index.SuffixArraySecondaryIndex.Expression;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.StorageService;

import com.google.common.collect.AbstractIterator;
import org.ardverk.collection.AbstractKeyAnalyzer;
import org.ardverk.collection.PatriciaTrie;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import org.github.jamm.MemoryMeter;

public class InMemoryIndex
{
    private final OnDiskSABuilder.Mode mode;
    private final ColumnDefinition definition;
    private final NonBlockingHashMapLong<PatriciaTrie<ByteBuffer, SortedSet<DecoratedKey>>> perThreadIndex;

    public InMemoryIndex(ColumnDefinition definition, OnDiskSABuilder.Mode mode)
    {
        this.mode = mode;
        this.definition = definition;
        this.perThreadIndex = new NonBlockingHashMapLong<>();
    }

    public void index(ByteBuffer value, ByteBuffer key)
    {
        final long threadId = Thread.currentThread().getId();
        final DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);

        PatriciaTrie<ByteBuffer, SortedSet<DecoratedKey>> index = perThreadIndex.get(threadId);
        if (index == null)
        {
            PatriciaTrie<ByteBuffer, SortedSet<DecoratedKey>> newIndex = new PatriciaTrie<>(new KeyAnalyzer(definition.getValidator()));
            index = perThreadIndex.putIfAbsent(threadId, newIndex);
            if (index == null)
                index = newIndex;
        }

        AbstractTokenizer tokenizer = SuffixArraySecondaryIndex.getTokenizer(definition.getValidator());
        tokenizer.init(definition.getIndexOptions());
        tokenizer.reset(value.duplicate());
        while (tokenizer.hasNext())
        {
            ByteBuffer term = tokenizer.next();

            switch (mode)
            {
                case SUFFIX:
                    int start = term.position();
                    for (int i = 0; i < term.remaining(); i++)
                        getKeys(index, (ByteBuffer) term.duplicate().position(start + i)).add(dk);
                    break;

                default:
                    getKeys(index, term).add(dk);
                    break;
            }
        }
    }

    public long estimateSize(MemoryMeter meter)
    {
        long deepSize = 0;
        // we are only going to consider keys of the PatriciaTrie instead of whole perThreadIndexMap
        // because first of all perThreadIndex map has constant number of entries which equals to
        // number of writer thread allocated by Cassandra, secondary DecoratedKey overhead is shared
        // between index and Cassandra's memtable, plus 36 which is per entry overhead of the NBHML.
        for (PatriciaTrie<ByteBuffer, SortedSet<DecoratedKey>> e : perThreadIndex.values())
            deepSize += 36 + meter.measureDeep(e.keySet());

        return deepSize;
    }

    public SkippableIterator<Long, TokenTree.Token> search(Expression expression)
    {
        List<SkippableIterator<Long, TokenTree.Token>> union = new ArrayList<>();
        for (PatriciaTrie<ByteBuffer, SortedSet<DecoratedKey>> index : perThreadIndex.values())
        {
            ByteBuffer min = expression.lower == null ? null : expression.lower.value;
            ByteBuffer max = expression.upper == null ? null : expression.upper.value;

            SortedMap<ByteBuffer, SortedSet<DecoratedKey>> search;

            if (min == null && max == null)
            {
                throw new IllegalArgumentException();
            }
            if (min != null && max != null)
            {
                search = (expression.isEquality)
                            ? index.prefixMap(min)
                            : index.subMap(min, expression.lower.inclusive, max, expression.upper.inclusive);
            }
            else if (min == null)
            {
                search = index.headMap(max, expression.upper.inclusive);
            }
            else
            {
                search = index.tailMap(min, expression.lower.inclusive);
            }

            for (Map.Entry<ByteBuffer, SortedSet<DecoratedKey>> keys : search.entrySet())
                union.add(new KeySkippableIterator(keys.getValue().iterator()));
        }

        return new LazyMergeSortIterator<>(LazyMergeSortIterator.OperationType.OR, union);
    }

    private SortedSet<DecoratedKey> getKeys(PatriciaTrie<ByteBuffer, SortedSet<DecoratedKey>> index, ByteBuffer term)
    {
        SortedSet<DecoratedKey> keys = index.get(term);
        if (keys == null)
            index.put(term, (keys = new TreeSet<>()));

        return keys;
    }

    private static class KeyAnalyzer extends AbstractKeyAnalyzer<ByteBuffer>
    {
        private static final int MSB = 1 << Byte.SIZE - 1;

        private final AbstractType<?> comparator;

        public KeyAnalyzer(AbstractType<?> comparator)
        {
            this.comparator = comparator;
        }

        @Override
        public int compare(ByteBuffer a, ByteBuffer b)
        {
            return comparator.compare(a, b);
        }

        @Override
        public int lengthInBits(ByteBuffer value)
        {
            return value.remaining() * Byte.SIZE;
        }

        @Override
        public boolean isBitSet(ByteBuffer key, int bitIndex)
        {
            if (bitIndex >= lengthInBits(key))
                return false;

            int index = bitIndex / Byte.SIZE;
            int bit = bitIndex % Byte.SIZE;

            return (key.get(key.position() + index) & mask(bit)) != 0;
        }

        @Override
        public int bitIndex(ByteBuffer key, ByteBuffer otherKey)
        {
            int length = Math.max(key.remaining(), otherKey.remaining());

            boolean allNull = true;

            int idx1 = key.position(), idx2 = otherKey.position();
            for (int i = 0; i < length; i++)
            {
                byte b1 = (i >= key.remaining()) ? 0 : key.get(idx1++);
                byte b2 = (i >= otherKey.remaining()) ? 0 : otherKey.get(idx2++);

                if (b1 != b2)
                {
                    int xor = b1 ^ b2;
                    for (int j = 0; j < Byte.SIZE; j++)
                    {
                        if ((xor & mask(j)) != 0)
                            return (i * Byte.SIZE) + j;
                    }
                }

                if (b1 != 0)
                    allNull = false;
            }

            return allNull ? NULL_BIT_KEY : EQUAL_BIT_KEY;
        }

        @Override
        public boolean isPrefix(ByteBuffer key, ByteBuffer prefix)
        {
            if (key.remaining() < prefix.remaining())
                return false;

            int idx1 = key.position(), idx2 = prefix.position();
            for (int i = 0; i < prefix.remaining(); i++)
            {
                if (key.get(idx1++) != prefix.get(idx2++))
                    return false;
            }

            return true;
        }

        private static int mask(int bit)
        {
            return MSB >>> bit;
        }
    }

    private static class KeySkippableIterator extends AbstractIterator<TokenTree.Token> implements SkippableIterator<Long, TokenTree.Token>
    {
        private final DKIterator keys;

        public KeySkippableIterator(Iterator<DecoratedKey> keys)
        {
            this.keys = new DKIterator(keys);
        }

        @Override
        protected TokenTree.Token computeNext()
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
    }

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

    private static class DKToken extends TokenTree.Token
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
            DKToken token = (DKToken) other;
            assert this.token == token.token;

            keys.addAll(token.keys);
        }

        @Override
        public Iterator<DecoratedKey> iterator()
        {
            return keys.iterator();
        }
    }
}
