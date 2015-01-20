package org.apache.cassandra.db.index.search.container;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.utils.CombinedValue;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.utils.Pair;

import com.google.common.primitives.Longs;
import org.apache.commons.lang3.builder.HashCodeBuilder;

// Note: all of the seek-able offsets contained in TokenTree should be sizeof(long)
// even if currently only lower int portion of them if used, because that makes
// it possible to switch to mmap implementation which supports log positions
// without any on-disk format changes and/or re-indexing if one day we'll have a need to.
public class TokenTree
{
    private final ByteBuffer file;
    private final int startPos;
    private final TokenRange tokenRange = new TokenRange();
    private final long tokenCount;

    public TokenTree(ByteBuffer tokenTree)
    {

        file = tokenTree;
        startPos = file.position();

        file.position(startPos + 19); // TODO (jwest): don't hardcode block shared header size
        tokenCount = file.getLong();

        long minToken = file.getLong();
        long maxToken = file.getLong();
        tokenRange.setRange(minToken, maxToken);
    }

    public SkippableIterator<Long, Token> iterator(Function<Long, DecoratedKey> keyFetcher)
    {
        return new TokenTreeIterator(keyFetcher);
    }

    /*
    public long getTokenCount()
    {
        return tokenCount;
    }

    public Long minToken()
    {
        return tokenRange.minToken;
    }

    public Long maxToken()
    {
        return tokenRange.maxToken;
    }
    */

    // finds leaf that *could* contain token
    // TODO (jwest): deal w/ when token < minToken or > maxToken
    private void seekToLeaf(long token)
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

            if (minToken > token)
            {
                // seek to beginning of child offsets to locate first child
                int seekTo = blockStart +
                        TokenTreeBuilder.BLOCK_HEADER_BYTES +
                        tokenCount * (Long.SIZE / 8);

                file.position(seekTo);
                blockStart = (startPos + (int) file.getLong());
            }
            else if (maxToken < token)
            {
                // seek to end of child offsets to locate last child
                int seekTo = blockStart +
                        TokenTreeBuilder.BLOCK_HEADER_BYTES +
                        (2 * tokenCount) * (Long.SIZE / 8);

                file.position(seekTo);
                blockStart = (startPos + (int) file.getLong());
            }
            else
            {
                // skip to end of block header/start of interior block tokens
                file.position(blockStart + TokenTreeBuilder.BLOCK_HEADER_BYTES);

                short offsetIndex = searchBlock(token, tokenCount);

                // file pointer is now at beginning of offsets
                file.position(file.position() + ((tokenCount - offsetIndex - 1) + offsetIndex) * (Long.SIZE / 8));
                blockStart = (startPos + (int) file.getLong());
            }
        }
    }

    // TODO (jwest): binary instead of linear search
    private short searchBlock(long searchToken, short tokenCount)
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

    public class TokenTreeIterator implements SkippableIterator<Long, Token>
    {
        private final Function<Long, DecoratedKey> keyFetcher;

        private int currentLeafStart;
        private int currentTokenIndex;
        private List<Token> tokens;
        private Token lastToken;
        private boolean lastLeaf;
        private short leafSize;
        private TokenRange leafRange;

        TokenTreeIterator(Function<Long, DecoratedKey> keyFetcher)
        {
            this.keyFetcher = keyFetcher;

            seekToLeaf(tokenRange.minToken);
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
            if (currentTokenIndex >= leafSize && lastLeaf)
                throw new NoSuchElementException();

            if (currentTokenIndex < leafSize) // tokens remaining in this leaf
            {
                // ensure we always are at the place in the file we expect to be
                if (currentTokenIndex < leafSize) // tokens remaining in this leaf
                {
                    lastToken = tokens.get(currentTokenIndex);
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
            else // no more tokens remaining in this leaf
            {
                assert !lastLeaf;

                seekToNextLeaf();
                setupBlock();
                return next();
            }
        }

        @Override
        public void skipTo(Long token)
        {
            if (lastToken != null && token <= lastToken.token)
                return;

            if (token <= leafRange.maxToken) // next is in this leaf block
            {
                searchLeaf(token);
            }
            else // next is in a leaf block that needs to be found
            {
                seekToLeaf(token);
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

            // TODO (jwest): don't hardcode last leaf indicator pos
            lastLeaf = (file.get() & (1 << 1)) > 0;
            leafSize = file.getShort();

            leafRange = new TokenRange();
            long minToken = file.getLong();
            long maxToken = file.getLong();
            leafRange.setRange(minToken, maxToken);

            // seek to end of leaf header/start of data
            file.position(currentLeafStart + TokenTreeBuilder.BLOCK_HEADER_BYTES);

            tokens = new ArrayList<>(leafSize);
            for (int i = 0; i < leafSize; i++)
            {
                // TODO (jwest): deal with collisions
                int infoAndPack = file.getInt();
                long token = file.getLong();
                int offset = file.getInt();
                tokens.add(new Token(token, Pair.create(keyFetcher, new long[]{ offset })));
            }
        }

        private void findNearest(Long next)
        {
            if (leafRange.maxToken < next && !lastLeaf)
            {
                seekToNextLeaf();
                setupBlock();
                findNearest(next);
            }
            else if (leafRange.minToken > next)
                return;
            else
                searchLeaf(next);
        }

        // TODO (jwest): binary instead of linear search
        private void searchLeaf(Long next)
        {
            for (int i = currentTokenIndex; i < leafSize; i++)
            {
                if (tokens.get(currentTokenIndex).token >= next)
                    break;

                // 4 byte int - collision
                file.getInt();

                currentTokenIndex++;
            }

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
        private final long token;
        private final Map<Function<Long, DecoratedKey>, Set<Long>> offsets;

        public Token(long token, final Pair<Function<Long, DecoratedKey>, long[]> offsets)
        {
            this.token = token;
            this.offsets = new HashMap<Function<Long, DecoratedKey>, Set<Long>>()
            {{
                    put(offsets.left, new HashSet<>(Longs.asList(offsets.right)));
            }};
        }

        @Override
        public void merge(CombinedValue<Long> other)
        {
            Token token = (Token) other;
            assert this.token == token.token;

            for (Map.Entry<Function<Long, DecoratedKey>, Set<Long>> e : token.offsets.entrySet())
            {
                Set<Long> existing = offsets.get(e.getKey());
                if (existing == null)
                    offsets.put(e.getKey(), e.getValue());
                else
                    existing.addAll(e.getValue());
            }
        }

        @Override
        public Long get()
        {
            return token;
        }

        public Iterator<DecoratedKey> iterator()
        {
            return new AbstractIterator<DecoratedKey>()
            {
                private final Iterator<Map.Entry<Function<Long, DecoratedKey>, Set<Long>>> perSSTable = offsets.entrySet().iterator();

                private Iterator<Long> currentOffsets;
                private Function<Long, DecoratedKey> currentFetcher;

                @Override
                protected DecoratedKey computeNext()
                {
                    if (currentOffsets != null && currentOffsets.hasNext())
                        return currentFetcher.apply(currentOffsets.next());

                    while (perSSTable.hasNext())
                    {
                        Map.Entry<Function<Long, DecoratedKey>, Set<Long>> offsets = perSSTable.next();
                        currentFetcher = offsets.getKey();
                        currentOffsets = offsets.getValue().iterator();

                        if (currentOffsets.hasNext())
                            return currentFetcher.apply(currentOffsets.next());
                    }

                    return endOfData();
                }
            };
        }

        @Override
        public int compareTo(CombinedValue<Long> o)
        {
            return Longs.compare(token, o.get());
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
}