package org.apache.cassandra.db.index.search.memory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.SuffixArraySecondaryIndex;
import org.apache.cassandra.db.index.SuffixArraySecondaryIndex.IndexMode;
import org.apache.cassandra.db.index.search.plan.Expression;
import org.apache.cassandra.db.index.search.container.TokenTree.Token;
import org.apache.cassandra.db.index.search.analyzer.AbstractAnalyzer;
import org.apache.cassandra.db.index.utils.LazyMergeSortIterator;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.service.StorageService;

import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.suffix.ConcurrentSuffixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.SmartArrayBasedNodeFactory;

import org.apache.cassandra.utils.Pair;
import org.github.jamm.MemoryMeter;

public class TrieColumnIndex extends ColumnIndex
{
    private final ConcurrentTrie index;

    public TrieColumnIndex(IndexMode mode, ColumnDefinition definition)
    {
        super(mode, definition);

        switch (mode.mode)
        {
            case SUFFIX:
                index = new ConcurrentSuffixTrie(definition);
                break;

            case ORIGINAL:
                index = new ConcurrentPrefixTrie(definition);
                break;

            default:
                throw new IllegalStateException("Unsupported mode: " + mode);
        }
    }

    @Override
    public void add(ByteBuffer value, ByteBuffer key)
    {
        final DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);

        AbstractAnalyzer analyzer = SuffixArraySecondaryIndex.getAnalyzer(Pair.create(definition, mode));

        analyzer.init(definition.getIndexOptions(), definition.getValidator());
        analyzer.reset(value.duplicate());

        while (analyzer.hasNext())
        {
            ByteBuffer term = analyzer.next();
            index.add(definition.getValidator().getString(term), dk);
        }
    }

    @Override
    public SkippableIterator<Long, Token> search(Expression expression)
    {
        return index.search(expression);
    }

    @Override
    public long estimateSize(MemoryMeter meter)
    {
        return meter.measureDeep(index);
    }

    private static abstract class ConcurrentTrie
    {
        protected final ColumnDefinition definition;

        public ConcurrentTrie(ColumnDefinition column)
        {
            definition = column;
        }

        public void add(String value, DecoratedKey key)
        {
            ConcurrentSkipListSet<DecoratedKey> keys = get(value);
            if (keys == null)
            {
                ConcurrentSkipListSet<DecoratedKey> newKeys = new ConcurrentSkipListSet<>(DecoratedKey.comparator);
                keys = putIfAbsent(value, newKeys);
                if (keys == null)
                    keys = newKeys;
            }

            keys.add(key);
        }

        public SkippableIterator<Long, Token> search(Expression expression)
        {
            List<SkippableIterator<Long, Token>> union = new ArrayList<>();

            assert expression.getOp() == Expression.Op.EQ; // means that min == max

            ByteBuffer prefix = expression.lower == null ? null : expression.lower.value;

            Iterable<ConcurrentSkipListSet<DecoratedKey>> search = search(definition.getValidator().getString(prefix));

            for (ConcurrentSkipListSet<DecoratedKey> keys : search)
                union.add(new KeySkippableIterator(keys.iterator()));

            return new LazyMergeSortIterator<>(LazyMergeSortIterator.OperationType.OR, union);
        }

        protected abstract ConcurrentSkipListSet<DecoratedKey> get(String value);
        protected abstract Iterable<ConcurrentSkipListSet<DecoratedKey>> search(String value);
        protected abstract ConcurrentSkipListSet<DecoratedKey> putIfAbsent(String value, ConcurrentSkipListSet<DecoratedKey> key);
    }

    private static class ConcurrentPrefixTrie extends ConcurrentTrie
    {
        private final ConcurrentRadixTree<ConcurrentSkipListSet<DecoratedKey>> trie;

        private ConcurrentPrefixTrie(ColumnDefinition column)
        {
            super(column);
            trie = new ConcurrentRadixTree<>(new SmartArrayBasedNodeFactory());
        }

        @Override
        public ConcurrentSkipListSet<DecoratedKey> get(String value)
        {
            return trie.getValueForExactKey(value);
        }

        @Override
        public ConcurrentSkipListSet<DecoratedKey> putIfAbsent(String value, ConcurrentSkipListSet<DecoratedKey> newKeys)
        {
            return trie.putIfAbsent(value, newKeys);
        }

        @Override
        public Iterable<ConcurrentSkipListSet<DecoratedKey>> search(String value)
        {
            return trie.getValuesForKeysStartingWith(value);
        }
    }

    private static class ConcurrentSuffixTrie extends ConcurrentTrie
    {
        private final ConcurrentSuffixTree<ConcurrentSkipListSet<DecoratedKey>> trie;

        private ConcurrentSuffixTrie(ColumnDefinition column)
        {
            super(column);
            trie = new ConcurrentSuffixTree<>(new SmartArrayBasedNodeFactory());
        }

        @Override
        public ConcurrentSkipListSet<DecoratedKey> get(String value)
        {
            return trie.getValueForExactKey(value);
        }

        @Override
        public ConcurrentSkipListSet<DecoratedKey> putIfAbsent(String value, ConcurrentSkipListSet<DecoratedKey> newKeys)
        {
            return trie.putIfAbsent(value, newKeys);
        }

        @Override
        public Iterable<ConcurrentSkipListSet<DecoratedKey>> search(String value)
        {
            return trie.getValuesForKeysContaining(value);
        }
    }
}
