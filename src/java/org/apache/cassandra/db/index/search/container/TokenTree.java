package org.apache.cassandra.db.index.search.container;

import java.io.IOException;
import java.util.*;

import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.utils.CombinedValue;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.Pair;

import com.google.common.primitives.Longs;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class TokenTree {

    private final RandomAccessReader file;
    private final long startPos;
    private final TokenRange tokenRange = new TokenRange();
    private final long tokenCount;

    public TokenTree(RandomAccessReader f) throws IOException
    {

        file = f;
        startPos = file.getFilePointer();

        file.skipBytes(19); // TODO (jwest): don't hardcode block shared header size
        tokenCount = file.readLong();

        long minToken = file.readLong();
        long maxToken = file.readLong();
        tokenRange.setRange(minToken, maxToken);
    }

    public SkippableIterator<Long, Token> iterator(Function<Long, DecoratedKey> keyFetcher)
    {
        return new TokenTreeIterator(keyFetcher);
    }

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

    // finds leaf that *could* contain token
    // TODO (jwest): deal w/ when token < minToken or > maxToken
    private void seekToLeaf(long token) throws IOException
    {

        // this loop always seeks forward except for the first iteration
        // where it may seek back to the root
        long blockStart = startPos;
        while (true) {
            file.seek(blockStart);

            byte info = file.readByte();
            boolean isLeaf = (info & 1) == 1;

            if (isLeaf) {
                file.seek(blockStart);
                break;
            }

            short tokenCount = file.readShort();
            long minToken = file.readLong();
            long maxToken = file.readLong();

            if (minToken > token)
            {
                // seek to beginning of child offsets to locate first child
                long seekTo = blockStart +
                        TokenTreeBuilder.BLOCK_HEADER_BYTES +
                        tokenCount * (Long.SIZE / 8);

                file.seek(seekTo);
                blockStart = (startPos + file.readLong());
            }
            else if(maxToken < token)
            {
                // seek to end of child offsets to locate last child
                long seekTo = blockStart +
                        TokenTreeBuilder.BLOCK_HEADER_BYTES +
                        (2 * tokenCount) * (Long.SIZE / 8);

                file.seek(seekTo);
                blockStart = (startPos + file.readLong());
            }
            else
            {
                // skip to end of block header/start of interior block tokens
                file.seek(blockStart + TokenTreeBuilder.BLOCK_HEADER_BYTES);

                short offsetIndex = searchBlock(token, tokenCount);

                // file pointer is now at beginning of offsets
                file.skipBytes(((tokenCount - offsetIndex - 1) + offsetIndex) * (Long.SIZE / 8));
                blockStart = (startPos + file.readLong());
            }
        }
    }

    // TODO (jwest): binary instead of linear search
    private short searchBlock(long searchToken, short tokenCount) throws IOException
    {
        short offsetIndex = 0;
        for (int i = 0; i < tokenCount; i++)
        {
            long readToken = file.readLong();
            if (searchToken < readToken)
                break;

            offsetIndex++;
        }

        return offsetIndex;
    }

    public class TokenTreeIterator implements SkippableIterator<Long, Token>
    {
        private final Function<Long, DecoratedKey> keyFetcher;

        private long currentLeafStart;
        private int currentTokenIndex;
        private List<Token> tokens;
        private Token lastToken;
        private boolean lastLeaf;
        private short leafSize;
        private TokenRange leafRange;

        TokenTreeIterator(Function<Long, DecoratedKey> keyFetcher)
        {
            this.keyFetcher = keyFetcher;

            try
            {
                seekToLeaf(tokenRange.minToken);
                setupBlock();
            }
            catch (IOException e)
            {
                throw new FSReadError(e, file.getPath());
            }
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

            try
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
            catch (IOException e)
            {
                throw new FSReadError(e, file.getPath());
            }
        }

        @Override
        public void skipTo(Long token)
        {
            if (lastToken != null && token <= lastToken.token)
                return;

            try
            {
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
            catch (IOException e)
            {
                throw new FSReadError(e, file.getPath());
            }

        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        private void setupBlock() throws IOException
        {
            currentLeafStart = file.getFilePointer();
            currentTokenIndex = 0;

            // TODO (jwest): don't hardcode last leaf indicator pos
            lastLeaf = (file.readByte() & (1 << 1)) > 0;
            leafSize = file.readShort();

            leafRange = new TokenRange();
            long minToken = file.readLong();
            long maxToken = file.readLong();
            leafRange.setRange(minToken, maxToken);

            // seek to end of leaf header/start of data
            file.seek(currentLeafStart + TokenTreeBuilder.BLOCK_HEADER_BYTES);

            tokens = new ArrayList<>(leafSize);
            for (int i = 0; i < leafSize; i++)
            {
                // TODO (jwest): deal with collisions
                int infoAndPack = file.readInt();
                long token = file.readLong();
                int offset = file.readInt();
                tokens.add(new Token(token, Pair.create(keyFetcher, new long[]{ offset })));
            }

        }

        private void findNearest(Long next) throws IOException
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
        private void searchLeaf(Long next) throws IOException
        {
            for (int i = currentTokenIndex; i < leafSize; i++)
            {
                if (tokens.get(currentTokenIndex).token >= next)
                    break;

                currentTokenIndex++;
            }

        }

        private void seekToNextLeaf()
        {
            file.seek(currentLeafStart + TokenTreeBuilder.BLOCK_BYTES);
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