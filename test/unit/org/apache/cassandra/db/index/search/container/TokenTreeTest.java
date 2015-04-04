package org.apache.cassandra.db.index.search.container;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.search.Descriptor;
import org.apache.cassandra.db.index.utils.CombinedValue;
import org.apache.cassandra.dht.LongToken;
import org.apache.cassandra.io.util.NativeMappedBuffer;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;

import junit.framework.Assert;
import org.junit.Test;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;


import static org.apache.cassandra.db.index.search.container.TokenTree.Token;

public class TokenTreeTest
{
    private static final Function<Long, DecoratedKey> KEY_CONVERTER = new KeyConverter();

    static LongSet singleOffset = new LongOpenHashSet() {{ add(1); }};
    static LongSet bigSingleOffset = new LongOpenHashSet() {{ add(((long) Integer.MAX_VALUE) + 10); }};
    static LongSet shortPackableCollision = new LongOpenHashSet() {{ add(2L); add(3L); }}; // can pack two shorts
    static LongSet intPackableCollision = new LongOpenHashSet() {{ add(6L); add(((long) Short.MAX_VALUE) + 1); }}; // can pack int & short
    static LongSet multiCollision =  new LongOpenHashSet() {{ add(3L); add(4L); add(5L); }}; // can't pack
    static LongSet unpackableCollision = new LongOpenHashSet() {{ add(((long) Short.MAX_VALUE) + 1); add(((long) Short.MAX_VALUE) + 2); }}; // can't pack

    final static SortedMap<Long, LongSet> simpleTokenMap = new TreeMap<Long, LongSet>()
    {{
            put(1L, bigSingleOffset); put(3L, shortPackableCollision); put(4L, intPackableCollision); put(6L, singleOffset);
            put(9L, multiCollision); put(10L, unpackableCollision); put(12L, singleOffset); put(13L, singleOffset);
            put(15L, singleOffset); put(16L, singleOffset); put(20L, singleOffset); put(22L, singleOffset);
            put(25L, singleOffset); put(26L, singleOffset); put(27L, singleOffset); put(28L, singleOffset);
            put(40L, singleOffset); put(50L, singleOffset); put(100L, singleOffset); put(101L, singleOffset);
            put(102L, singleOffset); put(103L, singleOffset); put(108L, singleOffset); put(110L, singleOffset);
            put(112L, singleOffset); put(115L, singleOffset); put(116L, singleOffset); put(120L, singleOffset);
            put(121L, singleOffset); put(122L, singleOffset); put(123L, singleOffset); put(125L, singleOffset);
    }};

    final static SortedMap<Long, LongSet> bigTokensMap = new TreeMap<Long, LongSet>()
    {{
            for (long i = 0; i < 1000000; i++)
                put(i, singleOffset);
    }};

    final static SortedMap<Long, LongSet> collidingTokensMap = new TreeMap<Long, LongSet>()
    {{
            put(1L, singleOffset); put(7L, singleOffset); put(8L, singleOffset);
    }};

    final static SortedMap<Long, LongSet> tokens = bigTokensMap;

    @Test
    public void buildAndIterate() throws Exception
    {
        final TokenTreeBuilder builder = new TokenTreeBuilder(Descriptor.CURRENT, tokens).finish();
        final Iterator<Pair<Long, LongSet>> tokenIterator = builder.iterator();
        final Iterator<Map.Entry<Long, LongSet>> listIterator = tokens.entrySet().iterator();
        while (tokenIterator.hasNext() && listIterator.hasNext())
        {
            Pair<Long, LongSet> tokenNext = tokenIterator.next();
            Map.Entry<Long, LongSet> listNext = listIterator.next();

            Assert.assertEquals(listNext.getKey(), tokenNext.left);
            Assert.assertEquals(listNext.getValue(), tokenNext.right);
        }

        Assert.assertFalse("token iterator not finished", tokenIterator.hasNext());
        Assert.assertFalse("list iterator not finished", listIterator.hasNext());
    }

    @Test
    public void buildWithMultipleMapsAndIterate() throws Exception
    {
        final SortedMap<Long, LongSet> merged = new TreeMap<>();
        final TokenTreeBuilder builder = new TokenTreeBuilder(Descriptor.CURRENT, simpleTokenMap).finish();
        builder.add(collidingTokensMap);

        merged.putAll(collidingTokensMap);
        for (Map.Entry<Long, LongSet> entry : simpleTokenMap.entrySet())
        {
            if (merged.containsKey(entry.getKey()))
            {
                LongSet mergingOffsets  = entry.getValue();
                LongSet existingOffsets = merged.get(entry.getKey());

                if (mergingOffsets.equals(existingOffsets))
                    continue;

                Set<Long> mergeSet = new HashSet<>();
                for (LongCursor merging : mergingOffsets)
                    mergeSet.add(merging.value);

                for (LongCursor existing : existingOffsets)
                    mergeSet.add(existing.value);

                LongSet mergedResults = new LongOpenHashSet();
                for (Long result : mergeSet)
                    mergedResults.add(result);

                merged.put(entry.getKey(), mergedResults);
            }
            else
            {
                merged.put(entry.getKey(), entry.getValue());
            }
        }

        final Iterator<Pair<Long, LongSet>> tokenIterator = builder.iterator();
        final Iterator<Map.Entry<Long, LongSet>> listIterator = merged.entrySet().iterator();
        while (tokenIterator.hasNext() && listIterator.hasNext())
        {
            Pair<Long, LongSet> tokenNext = tokenIterator.next();
            Map.Entry<Long, LongSet> listNext = listIterator.next();

            Assert.assertEquals(listNext.getKey(), tokenNext.left);
            Assert.assertEquals(listNext.getValue(), tokenNext.right);
        }

        Assert.assertFalse("token iterator not finished", tokenIterator.hasNext());
        Assert.assertFalse("list iterator not finished", listIterator.hasNext());

    }

    @Test
    public void testSerializedSize() throws Exception
    {
        final TokenTreeBuilder builder = new TokenTreeBuilder(Descriptor.CURRENT, tokens).finish();

        final File treeFile = File.createTempFile("token-tree-size-test", "tt");
        treeFile.deleteOnExit();

        final SequentialWriter writer = new SequentialWriter(treeFile, 4096, false);
        builder.write(writer);
        writer.close();


        final RandomAccessReader reader = RandomAccessReader.open(treeFile);
        Assert.assertEquals((int) reader.bytesRemaining(), builder.serializedSize());
    }

    @Test
    public void buildSerializeAndIterate() throws Exception
    {
        final TokenTreeBuilder builder = new TokenTreeBuilder(Descriptor.CURRENT, simpleTokenMap).finish();

        final File treeFile = File.createTempFile("token-tree-iterate-test1", "tt");
        treeFile.deleteOnExit();

        final SequentialWriter writer = new SequentialWriter(treeFile, 4096, false);
        builder.write(writer);
        writer.close();

        final RandomAccessReader reader = RandomAccessReader.open(treeFile);
        final TokenTree tokenTree = new TokenTree(new NativeMappedBuffer(reader.getFD(), 0, reader.length()));

        final Iterator<Token> tokenIterator = tokenTree.iterator(KEY_CONVERTER);
        final Iterator<Map.Entry<Long, LongSet>> listIterator = simpleTokenMap.entrySet().iterator();
        while (tokenIterator.hasNext() && listIterator.hasNext())
        {
            Token treeNext = tokenIterator.next();
            Map.Entry<Long, LongSet> listNext = listIterator.next();

            Assert.assertEquals(listNext.getKey(), treeNext.get());
            Assert.assertEquals(convert(listNext.getValue()), convert(treeNext));
        }

        Assert.assertFalse("token iterator not finished", tokenIterator.hasNext());
        Assert.assertFalse("list iterator not finished", listIterator.hasNext());

        reader.close();
    }

    @Test
    public void buildSerializeAndGet() throws Exception
    {
        final long tokMin = 0;
        final long tokMax = 1000;

        final SortedMap<Long, LongSet> toks = new TreeMap<Long, LongSet>()
        {{
                for (long i = tokMin; i <= tokMax; i++)
                {
                    LongSet offsetSet = new LongOpenHashSet();
                    offsetSet.add(i);
                    put(i, offsetSet);
                }
        }};

        final TokenTreeBuilder builder = new TokenTreeBuilder(Descriptor.CURRENT, toks).finish();
        final File treeFile = File.createTempFile("token-tree-get-test", "tt");
        treeFile.deleteOnExit();

        final SequentialWriter writer = new SequentialWriter(treeFile, 4096, false);
        builder.write(writer);
        writer.close();

        final RandomAccessReader reader = RandomAccessReader.open(treeFile);
        final TokenTree tokenTree = new TokenTree(new NativeMappedBuffer(reader.getFD(), 0, reader.length()));

        for (long i = 0; i <= tokMax; i++)
        {
            Token result = tokenTree.get(i, KEY_CONVERTER);
            Assert.assertNotNull("failed to find object for token " + i, result);

            Set<Long> found = result.getOffsets();
            Assert.assertEquals(1, found.size());
            Assert.assertEquals(i, found.toArray()[0]);
        }

        Assert.assertNull("found missing object", tokenTree.get(tokMax + 10, KEY_CONVERTER));

    }

    @Test
    public void buildSerializeIterateAndSkip() throws Exception
    {
        final TokenTreeBuilder builder = new TokenTreeBuilder(Descriptor.CURRENT, tokens).finish();

        final File treeFile = File.createTempFile("token-tree-iterate-test2", "tt");
        treeFile.deleteOnExit();

        final SequentialWriter writer = new SequentialWriter(treeFile, 4096, false);
        builder.write(writer);
        writer.close();

        final RandomAccessReader reader = RandomAccessReader.open(treeFile);
        final TokenTree tokenTree = new TokenTree(new NativeMappedBuffer(reader.getFD(), 0, reader.length()));

        final SkippableIterator<Long, Token> treeIterator = tokenTree.iterator(KEY_CONVERTER);
        final SkippableIterator<Long, TokenWithOffsets> listIterator = new EntrySetSkippableIterator(tokens);

        long lastToken = 0L;
        while (treeIterator.hasNext() && lastToken < 12)
        {
            Token treeNext = treeIterator.next();
            TokenWithOffsets listNext = listIterator.next();

            Assert.assertEquals(listNext.token, (lastToken = treeNext.get()));
            Assert.assertEquals(convert(listNext.offsets), convert(treeNext));
        }


        treeIterator.skipTo(100548L);
        listIterator.skipTo(100548L);

        while (treeIterator.hasNext() && listIterator.hasNext())
        {
            Token treeNext = treeIterator.next();
            TokenWithOffsets listNext = listIterator.next();

            Assert.assertEquals(listNext.token, (long) treeNext.get());
            Assert.assertEquals(convert(listNext.offsets), convert(treeNext));

        }

        Assert.assertFalse("Tree iterator not completed", treeIterator.hasNext());
        Assert.assertFalse("List iterator not completed", listIterator.hasNext());

        reader.close();
    }

    @Test
    public void skipPastEnd() throws Exception
    {
        final TokenTreeBuilder builder = new TokenTreeBuilder(Descriptor.CURRENT, simpleTokenMap).finish();

        final File treeFile = File.createTempFile("token-tree-skip-past-test", "tt");
        treeFile.deleteOnExit();

        final SequentialWriter writer = new SequentialWriter(treeFile, 4096, false);
        builder.write(writer);
        writer.close();

        final RandomAccessReader reader = RandomAccessReader.open(treeFile);
        final SkippableIterator<Long, Token> tokenTree = new TokenTree(new NativeMappedBuffer(reader.getFD(), 0, reader.length())).iterator(KEY_CONVERTER);

        tokenTree.skipTo(simpleTokenMap.lastKey() + 10);
    }

    private static class EntrySetSkippableIterator extends AbstractIterator<TokenWithOffsets>
            implements SkippableIterator<Long, TokenWithOffsets>
    {
        private final SortedMap<Long, LongSet> tokens;
        private final Iterator<Map.Entry<Long, LongSet>> elements;

        EntrySetSkippableIterator(SortedMap<Long, LongSet> elms)
        {
            tokens = elms;
            elements = elms.entrySet().iterator();
        }

        @Override
        public Long getMinimum()
        {
            return tokens.isEmpty() ? null : tokens.firstKey();
        }

        @Override
        public TokenWithOffsets computeNext()
        {
            if (!elements.hasNext())
                return endOfData();

            Map.Entry<Long, LongSet> next = elements.next();
            return new TokenWithOffsets(next.getKey(), next.getValue());
        }

        @Override
        public void skipTo(Long next)
        {
            while (hasNext())
            {
                if (Long.compare(peek().token, next) >= 0)
                    break;

                next();
            }

        }

        @Override
        public boolean intersect(TokenWithOffsets token)
        {
            LongSet existing = tokens.get(token.get());
            if (existing == null)
                return false;

            token.merge(new TokenWithOffsets(token.get(), existing));
            return true;
        }

        @Override
        public long getCount()
        {
            return tokens.size();
        }

        @Override
        public void close() throws IOException
        {
            // nothing to do here
        }
    }

    public static class TokenWithOffsets implements CombinedValue<Long>
    {
        private final long token;
        private final LongSet offsets;

        public TokenWithOffsets(long token, final LongSet offsets)
        {
            this.token = token;
            this.offsets = offsets;
        }

        @Override
        public void merge(CombinedValue<Long> other)
        {}

        @Override
        public Long get()
        {
            return token;
        }

        @Override
        public int compareTo(CombinedValue<Long> o)
        {
            return Long.compare(token, o.get());
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof TokenWithOffsets))
                return false;

            TokenWithOffsets o = (TokenWithOffsets) other;
            return token == o.token && offsets.equals(o.offsets);
        }

        @Override
        public int hashCode()
        {
            return new HashCodeBuilder().append(token).build();
        }

        @Override
        public String toString()
        {
            return String.format("TokenValue(token: %d, offsets: %s)", token, offsets);
        }
    }

    private static Set<DecoratedKey> convert(LongSet offsets)
    {
        Set<DecoratedKey> keys = new HashSet<>();
        for (LongCursor offset : offsets)
            keys.add(KEY_CONVERTER.apply(offset.value));

        return keys;
    }

    private static Set<DecoratedKey> convert(Token results)
    {
        Set<DecoratedKey> keys = new HashSet<>();
        for (DecoratedKey key : results)
            keys.add(key);

        return keys;
    }

    private static class KeyConverter implements Function<Long, DecoratedKey>
    {
        @Override
        public DecoratedKey apply(Long offset)
        {
            return dk(offset);
        }
    }

    private static DecoratedKey dk(Long token)
    {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(token);
        buf.flip();
        Long hashed = MurmurHash.hash2_64(buf, buf.position(), buf.remaining(), 0);
        return new DecoratedKey(new LongToken(hashed), buf);
    }
}
