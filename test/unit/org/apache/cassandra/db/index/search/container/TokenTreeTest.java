package org.apache.cassandra.db.index.search.container;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.utils.CombinedValue;
import org.apache.cassandra.dht.LongToken;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;

import junit.framework.Assert;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Test;

import static org.apache.cassandra.db.index.search.container.TokenTree.Token;
import static org.apache.cassandra.db.index.SuffixArraySecondaryIndex.TOKEN_COMPARATOR;

public class TokenTreeTest
{
    static long[] singleOffset = {1};
    static long[] shortPackableCollision = {2, 3}; // can pack two shorts
    static long[] intPackableCollision = {6, Short.MAX_VALUE + 1}; // can pack int & short
    static long[] multiCollision = {3, 4, 5}; // can't pack
    static long[] unpackableCollision = {Short.MAX_VALUE + 1, Short.MAX_VALUE + 2}; // can't pack
    static long[] divideOffset = {Integer.MAX_VALUE + 1};

    // TODO (jwest): remove token here, its useless
    static DecoratedKey dk(Long token)
    {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(token);
        buf.flip();
        Long hashed = MurmurHash.hash2_64(buf, buf.position(), buf.remaining(), 0);
        return new DecoratedKey(new LongToken(hashed), buf);
    }

    final static SortedMap<DecoratedKey, long[]> tokens4 = new TreeMap<DecoratedKey, long[]>(TOKEN_COMPARATOR)
    {{
            put(dk(1L), singleOffset); put(dk(3L), singleOffset); put(dk(4L), singleOffset); put(dk(6L), singleOffset);
            put(dk(9L), singleOffset); put(dk(10L), singleOffset); put(dk(12L), singleOffset); put(dk(13L), singleOffset);
            put(dk(15L), singleOffset); put(dk(16L), singleOffset); put(dk(20L), singleOffset); put(dk(22L), singleOffset);
            put(dk(25L), singleOffset); put(dk(26L), singleOffset); put(dk(27L), singleOffset); put(dk(28L), singleOffset);
            put(dk(40L), singleOffset); put(dk(50L), singleOffset); put(dk(100L), singleOffset); put(dk(101L), singleOffset);
            put(dk(102L), singleOffset); put(dk(103L), singleOffset); put(dk(108L), singleOffset); put(dk(110L), singleOffset);
            put(dk(112L), singleOffset); put(dk(115L), singleOffset); put(dk(116L), singleOffset); put(dk(120L), singleOffset);
            put(dk(121L), singleOffset); put(dk(122L), singleOffset); put(dk(123L), singleOffset); put(dk(125L), singleOffset);
    }};

    final static SortedMap<DecoratedKey, long[]> tokens5 = new TreeMap<DecoratedKey, long[]>(TOKEN_COMPARATOR)
    {{
            for (long i = 0; i < 1000000; i++)
                put(dk(i), singleOffset);
    }};

    final static SortedMap<DecoratedKey, long[]> tokens6 = new TreeMap<DecoratedKey, long[]>(TOKEN_COMPARATOR)
    {{
            put(dk(1L), singleOffset); put(dk(7L), singleOffset); put(dk(8L), singleOffset);
    }};

    final static SortedMap<DecoratedKey, long[]> tokens = tokens5;

    @Test
    public void buildAndIterate() throws Exception
    {
        final TokenTreeBuilder builder = new TokenTreeBuilder(tokens);
        final Iterator<Pair<DecoratedKey, long[]>> tokenIterator = builder.iterator();
        final Iterator<Map.Entry<DecoratedKey, long[]>> listIterator = tokens.entrySet().iterator();
        while (tokenIterator.hasNext() && listIterator.hasNext())
        {
            Pair<DecoratedKey, long[]> tokenNext = tokenIterator.next();
            Map.Entry<DecoratedKey, long[]> listNext = listIterator.next();
            Assert.assertEquals(listNext.getKey(), tokenNext.left);
            // TODO (jwest): this should use Arrays.isEqual because its doing object eq.
            Assert.assertEquals(listNext.getValue(), tokenNext.right);
        }

        Assert.assertFalse("token iterator not finished", tokenIterator.hasNext());
        Assert.assertFalse("list iterator not finished", listIterator.hasNext());
    }

    @Test
    public void buildWithMultipleMapsAndIterate() throws Exception
    {
        final SortedMap<DecoratedKey, long[]> merged = new TreeMap<>();
        final TokenTreeBuilder builder = new TokenTreeBuilder(tokens4);
        builder.add(tokens6);

        merged.putAll(tokens6);
        for (Map.Entry<DecoratedKey, long[]> entry : tokens4.entrySet())
        {
            if (merged.containsKey(entry.getKey()))
            {
                long[] mergingOffsets  = entry.getValue();
                long[] existingOffsets = merged.get(entry.getKey());

                if (Arrays.equals(mergingOffsets, existingOffsets))
                    continue;

                Set<Long> mergeSet = new HashSet<>();
                for (Long merging : mergingOffsets)
                    mergeSet.add(merging);
                for (Long existing : existingOffsets)
                    mergeSet.add(existing);

                long[] mergedResults = new long[mergeSet.size()];
                int i = 0;
                for (Long result : mergeSet)
                {
                    mergedResults[i] = result;
                    i++;
                }
                merged.put(entry.getKey(), mergedResults);
            }
            else
            {
                merged.put(entry.getKey(), entry.getValue());
            }
        }

        final Iterator<Pair<DecoratedKey, long[]>> tokenIterator = builder.iterator();
        final Iterator<Map.Entry<DecoratedKey, long[]>> listIterator = merged.entrySet().iterator();
        while (tokenIterator.hasNext() && listIterator.hasNext())
        {
            Pair<DecoratedKey, long[]> tokenNext = tokenIterator.next();
            Map.Entry<DecoratedKey, long[]> listNext = listIterator.next();
            Assert.assertEquals(listNext.getKey(), tokenNext.left);
            Assert.assertEquals(listNext.getValue(), tokenNext.right);
        }

        Assert.assertFalse("token iterator not finished", tokenIterator.hasNext());
        Assert.assertFalse("list iterator not finished", listIterator.hasNext());

    }

    @Test
    public void testSerializedSize() throws Exception
    {
        final TokenTreeBuilder builder = new TokenTreeBuilder(tokens);

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
        final TokenTreeBuilder builder = new TokenTreeBuilder(tokens);

        final File treeFile = File.createTempFile("token-tree-iterate-test1", "tt");
        treeFile.deleteOnExit();

        final SequentialWriter writer = new SequentialWriter(treeFile, 4096, false);
        builder.write(writer);
        writer.close();

        final RandomAccessReader reader = RandomAccessReader.open(treeFile);
        final TokenTree tokenTree = new TokenTree(reader);

//        final Iterator<Long> listIterator = tokens.keySet().iterator();
//        while(tokenIterator.hasNext() && listIterator.hasNext())
 //           Assert.assertEquals(listIterator.next(), tokenIterator.next());

 //       Assert.assertFalse("token iterator not finished", tokenIterator.hasNext());
 //       Assert.assertFalse("list iterator not finished", listIterator.hasNext());

        final Iterator<Token> tokenIterator = tokenTree.iterator(new KeyConverter());
        final Iterator<Map.Entry<DecoratedKey, long[]>> listIterator = tokens.entrySet().iterator();
        while (tokenIterator.hasNext() && listIterator.hasNext())
        {
            Token treeNext = tokenIterator.next();
            Map.Entry<DecoratedKey, long[]> listNext = listIterator.next();
            Assert.assertEquals(((LongToken) listNext.getKey().getToken()).token, treeNext.get());
            Assert.assertEquals(convert(listNext.getValue()), convert(treeNext));
        }

        Assert.assertFalse("token iterator not finished", tokenIterator.hasNext());
        Assert.assertFalse("list iterator not finished", listIterator.hasNext());

        reader.close();
    }

    @Test
    public void buildSerializeIterateAndSkip() throws Exception
    {
        final TokenTreeBuilder builder = new TokenTreeBuilder(tokens);

        final File treeFile = File.createTempFile("token-tree-iterate-test2", "tt");
        treeFile.deleteOnExit();

        final SequentialWriter writer = new SequentialWriter(treeFile, 4096, false);
        builder.write(writer);
        writer.close();

        final RandomAccessReader reader = RandomAccessReader.open(treeFile);
        final TokenTree tokenTree = new TokenTree(reader);

        final SkippableIterator<Long, Token> treeIterator = tokenTree.iterator(new KeyConverter());
        final SkippableIterator<Long, TokenWithOffsets> listIterator = new EntrySetSkippableIterator(tokens.entrySet().iterator());

        long lastToken = 0L;
        while (treeIterator.hasNext() && lastToken < 12)
        {
            Token treeNext = treeIterator.next();
            TokenWithOffsets listNext = listIterator.next();

            Assert.assertEquals(listNext.token, (lastToken = treeNext.get()));
            Assert.assertEquals(convert(listNext.offsets), convert(treeNext));
        }


//        treeIterator.skipTo(100548L);
//        listIterator.skipTo(100548L);
        treeIterator.skipTo(100548L);
        listIterator.skipTo(100548L);

        while (treeIterator.hasNext() && listIterator.hasNext())
        {
            Token treeNext = treeIterator.next();
            TokenWithOffsets listNext = listIterator.next();

            Assert.assertEquals(listNext.token, (long) treeNext.get());

            //Assert.assertTrue(listNext.getKey().equals(treeNext.left));
            //Assert.assertEquals(listNext.getValue()[0], treeNext.right[0]);
            Assert.assertEquals(convert(listNext.offsets), convert(treeNext));

        }

        Assert.assertFalse("Tree iterator not completed", treeIterator.hasNext());
        Assert.assertFalse("List iterator not completed", listIterator.hasNext());

        reader.close();
    }

    @Test
    public void skipPastEnd() throws Exception
    {
        final TokenTreeBuilder builder = new TokenTreeBuilder(tokens4);

        final File treeFile = File.createTempFile("token-tree-skip-past-test", "tt");
        treeFile.deleteOnExit();

        final SequentialWriter writer = new SequentialWriter(treeFile, 4096, false);
        builder.write(writer);
        writer.close();

        final RandomAccessReader reader = RandomAccessReader.open(treeFile);
        final SkippableIterator<Long, Token> tokenTree = new TokenTree(reader).iterator(new KeyConverter());

        // TODO (jwest): dont hardcode this value, get it from tokens4
        tokenTree.skipTo(1000L);
    }

    private static class EntrySetSkippableIterator extends AbstractIterator<TokenWithOffsets>
            implements SkippableIterator<Long, TokenWithOffsets>
    {
        private final Iterator<Map.Entry<DecoratedKey, long[]>> elements;

        EntrySetSkippableIterator(Iterator<Map.Entry<DecoratedKey, long[]>> elms)
        {
            elements = elms;
        }

        @Override
        public TokenWithOffsets computeNext()
        {
            if (!elements.hasNext())
                return endOfData();

            Map.Entry<DecoratedKey, long[]> next = elements.next();
            return new TokenWithOffsets(((LongToken) next.getKey().getToken()).token, next.getValue());
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
        public void close() throws IOException
        {
            // nothing to do here
        }
    }

    public static class TokenWithOffsets implements CombinedValue<Long>
    {
        private final long token;
        private final long[] offsets;

        public TokenWithOffsets(long token, final long[] offsets)
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
            return token == o.token && Arrays.equals(offsets, o.offsets);
        }

        @Override
        public int hashCode()
        {
            return new HashCodeBuilder().append(token).build();
        }

        @Override
        public String toString()
        {
            return String.format("TokenValue(token: %d, offsets: %s)", token, Arrays.toString(offsets));
        }
    }

    private static Set<DecoratedKey> convert(long[] offsets)
    {
        Set<DecoratedKey> keys = new HashSet<>();
        for (long offset : offsets)
            keys.add(dk(offset));

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
}
