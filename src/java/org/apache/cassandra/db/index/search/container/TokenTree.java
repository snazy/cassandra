package org.apache.cassandra.db.index.search.container;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.search.Descriptor;
import org.apache.cassandra.db.index.utils.CombinedValue;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Pair;

import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.google.common.primitives.Longs;
import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import org.apache.commons.lang3.builder.HashCodeBuilder;

// Note: all of the seek-able offsets contained in TokenTree should be sizeof(long)
// even if currently only lower int portion of them if used, because that makes
// it possible to switch to mmap implementation which supports long positions
// without any on-disk format changes and/or re-indexing if one day we'll have a need to.
public class TokenTree
{
    private static final int LONG_BYTES = Long.SIZE / 8;
    private static final int SHORT_BYTES = Short.SIZE / 8;

    private final Descriptor descriptor;
    private final ByteBuffer file;
    private final int startPos;
    private final long treeMinToken;
    private final long treeMaxToken;
    private final long tokenCount;

    public TokenTree(ByteBuffer tokenTree)
    {
        this(Descriptor.CURRENT, tokenTree);
    }

    public TokenTree(Descriptor d, ByteBuffer tokenTree)
    {
        descriptor = d;
        file = tokenTree;
        startPos = file.position();


        file.position(startPos + TokenTreeBuilder.SHARED_HEADER_BYTES);

        if (!validateMagic())
            throw new IllegalArgumentException("invalid token tree");

        tokenCount = file.getLong();
        treeMinToken = file.getLong();
        treeMaxToken = file.getLong();
    }

    public SkippableIterator<Long, Token> iterator(Function<Long, DecoratedKey> keyFetcher)
    {
        return new TokenTreeIterator(file.duplicate(), keyFetcher);
    }

    public Token get(final long searchToken, Function<Long, DecoratedKey> kf)
    {
        seekToLeaf(searchToken, file);
        int leafStart = file.position();
        short leafSize = file.getShort(leafStart + 1); // skip the info byte

        file.position(leafStart + TokenTreeBuilder.BLOCK_HEADER_BYTES); // skip to tokens
        short tokenIndex = searchLeaf(searchToken, leafSize);

        file.position(leafStart + TokenTreeBuilder.BLOCK_HEADER_BYTES);
        Token tok = Token.getTokenAt(tokenIndex, leafSize, file, kf);
        if (tok.get().compareTo(searchToken) == 0)
            return tok;
        else
            return null;
    }

    private boolean validateMagic()
    {
        switch (descriptor.version.toString())
        {
            case Descriptor.VERSION_AA:
                return true;
            case Descriptor.VERSION_AB:
                return TokenTreeBuilder.AB_MAGIC == file.getShort();
            default:
                return false;
        }
    }

    // finds leaf that *could* contain token
    private void seekToLeaf(long token, ByteBuffer file)
    {
        // this loop always seeks forward except for the first iteration
        // where it may seek back to the root
        int blockStart = startPos;
        while (true)
        {
            file.position(blockStart);

            byte info = file.get();
            boolean isLeaf = (info & 1) == 1;

            if (isLeaf)
            {
                file.position(blockStart);
                break;
            }

            short tokenCount = file.getShort();

            long minToken = file.getLong();
            long maxToken = file.getLong();

            int seekBase = blockStart + TokenTreeBuilder.BLOCK_HEADER_BYTES;
            if (minToken > token)
            {
                // seek to beginning of child offsets to locate first child
                file.position(seekBase + tokenCount * LONG_BYTES);
                blockStart = (startPos + (int) file.getLong());
            }
            else if (maxToken < token)
            {
                // seek to end of child offsets to locate last child
                file.position(seekBase + (2 * tokenCount) * LONG_BYTES);
                blockStart = (startPos + (int) file.getLong());
            }
            else
            {
                // skip to end of block header/start of interior block tokens
                file.position(seekBase);

                short offsetIndex = searchBlock(token, tokenCount, file);

                // file pointer is now at beginning of offsets
                if (offsetIndex == tokenCount)
                    file.position(file.position() + (offsetIndex * LONG_BYTES));
                else
                    file.position(file.position() + ((tokenCount - offsetIndex - 1) + offsetIndex) * LONG_BYTES);

                blockStart = (startPos + (int) file.getLong());
            }
        }
    }

    private short searchBlock(long searchToken, short tokenCount, ByteBuffer file)
    {
        short offsetIndex = 0;
        for (int i = 0; i < tokenCount; i++)
        {
            long readToken = file.getLong();
            if (searchToken < readToken)
                break;

            offsetIndex++;
        }

        return offsetIndex;
    }

    private short searchLeaf(long searchToken, short tokenCount)
    {
        int base = file.position();

        int start = 0;
        int end = tokenCount;
        int middle = 0;

        while (start <= end)
        {
            middle = start + ((end - start) >> 1);

            // each entry is 16 bytes wide, token is in bytes 4-11
            long token = file.getLong(base + (middle * (2 * LONG_BYTES) + 4));

            if (token == searchToken)
                break;

            if (token < searchToken)
                start = middle + 1;
            else
                end = middle - 1;
        }

        return (short) middle;
    }

    public class TokenTreeIterator implements SkippableIterator<Long, Token>
    {
        private final Function<Long, DecoratedKey> keyFetcher;

        private int currentLeafStart;
        private int currentTokenIndex;
        private Token lastToken;
        private boolean lastLeaf;
        private short leafSize;
        private long leafMinToken;
        private long leafMaxToken;
        private ByteBuffer file;
        private Long skipToToken;

        TokenTreeIterator(ByteBuffer file, Function<Long, DecoratedKey> keyFetcher)
        {
            this.file = file;
            this.keyFetcher = keyFetcher;

            seekToLeaf(treeMinToken, this.file);
            setupBlock();
        }

        @Override
        public boolean hasNext()
        {
            return (!lastLeaf || currentTokenIndex < leafSize);
        }

        @Override
        public Token next()
        {
            maybeSkip();

            if (currentTokenIndex >= leafSize && lastLeaf)
                return null;

            if (currentTokenIndex < leafSize) // tokens remaining in this leaf
            {
                lastToken = getTokenAt(currentTokenIndex);
                currentTokenIndex++;

                return lastToken;
            }
            else // no more tokens remaining in this leaf
            {
                assert !lastLeaf;

                seekToNextLeaf();
                setupBlock();
                return next();
            }
        }

        private void maybeSkip()
        {
            if (skipToToken == null)
                return;

            performSkipTo(skipToToken);
            skipToToken = null;
        }

        @Override
        public void skipTo(Long token)
        {
            skipToToken = token;
        }

        public void performSkipTo(Long token)
        {
            if (lastToken != null && token <= lastToken.token)
                return;

            if (token <= leafMaxToken) // next is in this leaf block
            {
                searchLeaf(token);
            }
            else // next is in a leaf block that needs to be found
            {
                seekToLeaf(token, file);
                setupBlock();
                findNearest(token);
            }
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        private void setupBlock()
        {
            currentLeafStart = file.position();
            currentTokenIndex = 0;

            lastLeaf = (file.get() & (1 << TokenTreeBuilder.LAST_LEAF_SHIFT)) > 0;
            leafSize = file.getShort();

            leafMinToken = file.getLong();
            leafMaxToken = file.getLong();

            // seek to end of leaf header/start of data
            file.position(currentLeafStart + TokenTreeBuilder.BLOCK_HEADER_BYTES);
        }

        private void findNearest(Long next)
        {
            if (next > leafMaxToken && !lastLeaf)
            {
                seekToNextLeaf();
                setupBlock();
                findNearest(next);
            }
            else if (next > leafMinToken)
                searchLeaf(next);
        }

        private void searchLeaf(long next)
        {
            for (int i = currentTokenIndex; i < leafSize; i++)
            {
                if (compareTokenAt(currentTokenIndex, next) >= 0)
                    break;

                currentTokenIndex++;
            }
        }

        private int compareTokenAt(int idx, long toToken)
        {
            return Long.compare(file.getLong(getTokenPosition(idx)), toToken);
        }

        private Token getTokenAt(int idx)
        {
            return Token.getTokenAt(idx, leafSize, file, keyFetcher);
        }

        private int getTokenPosition(int idx)
        {
            // skip 4 byte entry header to get position pointing directly at the entry's token
            return Token.getEntryPosition(idx, file) + (2 * SHORT_BYTES);
        }

        private void seekToNextLeaf()
        {
            file.position(currentLeafStart + TokenTreeBuilder.BLOCK_BYTES);
        }

        @Override
        public void close() throws IOException
        {
            // nothing to do here
        }
    }

    public static class Token implements CombinedValue<Long>, Iterable<DecoratedKey>
    {
        protected final long token;
        protected final Map<Function<Long, DecoratedKey>, LongSet> offsets;

        public Token(long token, final Pair<Function<Long, DecoratedKey>, long[]> offsets)
        {
            this.token = token;
            this.offsets = new HashMap<Function<Long, DecoratedKey>, LongSet>()
            {{
                    if (offsets != null)
                        put(offsets.left, new LongOpenHashSet() {{ add(offsets.right); }});
            }};
        }

        public static Token getTokenAt(int idx, short max, ByteBuffer file, Function<Long, DecoratedKey> keyFetcher)
        {
            int position = getEntryPosition(idx, file);

            short info = file.getShort(position);
            short offsetShort = file.getShort(position + SHORT_BYTES);
            long token = file.getLong(position + (2 * SHORT_BYTES));
            long[] offsets = reconstructOffset(info, offsetShort, file.getInt(position + (2 * SHORT_BYTES) + LONG_BYTES), file, max);

            return new Token(token, Pair.create(keyFetcher, offsets));
        }

        private static int getEntryPosition(int idx, ByteBuffer file)
        {
            // info (4 bytes) + token (8 bytes) + offset (4 bytes) = 16 bytes
            return file.position() + (idx * (2 * LONG_BYTES));
        }

        private static long[] reconstructOffset(short info, short offsetShort, int offsetInt, ByteBuffer file, short leafSize)
        {
            int type = (info & TokenTreeBuilder.ENTRY_TYPE_MASK);
            if (type == TokenTreeBuilder.EntryType.OVERFLOW.ordinal())
            {
                long[] offsets = new long[offsetShort]; // offsetShort contains count of tokens
                int offsetPos = (file.position() + (2 * (leafSize * LONG_BYTES)) + (offsetInt * LONG_BYTES));
                for (int i = 0; i < offsetShort; i++) {
                    offsets[i] = file.getLong(offsetPos + (i * LONG_BYTES));
                }

                return offsets;
            }
            else if (type == TokenTreeBuilder.EntryType.FACTORED.ordinal())
            {
                return new long[]{(((long) offsetInt) << Short.SIZE) + offsetShort};
            }
            else if(type == TokenTreeBuilder.EntryType.PACKED.ordinal())
            {
                return new long[]{offsetShort, offsetInt};
            }
            else
            {
                return new long[]{offsetInt};
            }
        }


        public Set<Long> getOffsets()
        {
            Set<Long> result = null;
            for (final LongSet entry : offsets.values())
            {
                if (result == null)
                    result = new HashSet<>();

                for (LongCursor offset : entry)
                    result.add(offset.value);
            }

            return result;
        }

        @Override
        public void merge(CombinedValue<Long> other)
        {
            Token token = (Token) other;
            assert this.token == token.token;

            for (Map.Entry<Function<Long, DecoratedKey>, LongSet> e : token.offsets.entrySet())
            {
                LongSet existing = offsets.get(e.getKey());
                if (existing == null)
                {
                    offsets.put(e.getKey(), e.getValue());
                }
                else
                {
                    for (LongCursor offset : e.getValue())
                        existing.add(offset.value);
                }
            }
        }

        @Override
        public Long get()
        {
            return token;
        }

        public Iterator<DecoratedKey> iterator()
        {
            List<Iterator<DecoratedKey>> keys = new ArrayList<>(offsets.size());

            for (Map.Entry<Function<Long, DecoratedKey>, LongSet> e : offsets.entrySet())
                keys.add(new KeyIterator(e.getKey(), e.getValue()));

            return MergeIterator.get(keys, DecoratedKey.comparator, new MergeIterator.Reducer<DecoratedKey, DecoratedKey>()
            {
                DecoratedKey reduced = null;

                @Override
                public boolean trivialReduceIsTrivial()
                {
                    return true;
                }

                @Override
                public void reduce(DecoratedKey current)
                {
                    reduced = current;
                }

                @Override
                protected DecoratedKey getReduced()
                {
                    return reduced;
                }
            });
        }

        @Override
        public int compareTo(CombinedValue<Long> o)
        {
            return Longs.compare(token, ((Token) o).token);
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof Token))
                return false;

            Token o = (Token) other;
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

    private static class KeyIterator extends AbstractIterator<DecoratedKey>
    {
        private final Function<Long, DecoratedKey> keyFetcher;
        private final Iterator<LongCursor> offsets;

        public KeyIterator(Function<Long, DecoratedKey> keyFetcher, LongSet offsets)
        {
            this.keyFetcher = keyFetcher;
            this.offsets = offsets.iterator();
        }

        @Override
        public DecoratedKey computeNext()
        {
            return offsets.hasNext() ? keyFetcher.apply(offsets.next().value) : endOfData();
        }
    }
}