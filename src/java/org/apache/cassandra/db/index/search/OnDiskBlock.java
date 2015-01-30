package org.apache.cassandra.db.index.search;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.index.search.container.TokenTree;
import org.apache.cassandra.db.marshal.AbstractType;

import static org.apache.cassandra.db.index.search.OnDiskSABuilder.BLOCK_SIZE;

public abstract class OnDiskBlock<T extends Suffix>
{
    // this contains offsets of the suffixes and suffix data
    protected final ByteBuffer blockIndex;
    protected final int blockIndexSize;

    protected final boolean hasCombinedIndex;
    protected final TokenTree combinedIndex;

    public OnDiskBlock(ByteBuffer block)
    {
        blockIndex = block;

        int blockOffset = block.position();
        int combinedIndexOffset = block.getInt(blockOffset + BLOCK_SIZE);

        hasCombinedIndex = (combinedIndexOffset >= 0);
        int blockIndexOffset = blockOffset + BLOCK_SIZE + 4 + combinedIndexOffset;

        combinedIndex = hasCombinedIndex ? new TokenTree((ByteBuffer) blockIndex.duplicate().position(blockIndexOffset)) : null;

        blockIndexSize = block.getInt() * 2;
    }

    public SearchResult<T> search(AbstractType<?> comparator, ByteBuffer query)
    {
        int cmp = -1, start = 0, end = getElementsSize() - 1, middle = 0;

        T element = null;
        while (start <= end)
        {
            middle = start + ((end - start) >> 1);
            element = getElement(middle);

            cmp = element.compareTo(comparator, query);
            if (cmp == 0)
                return new SearchResult<>(element, cmp, middle);
            else if (cmp < 0)
                start = middle + 1;
            else
                end = middle - 1;
        }

        return new SearchResult<>(element, cmp, middle);
    }

    protected T getElement(int index)
    {
        ByteBuffer dup = blockIndex.duplicate();
        int startsAt = getElementPosition(index);
        if (getElementsSize() - 1 == index) // last element
            dup.position(startsAt);
        else
            dup.position(startsAt).limit(getElementPosition(index + 1));

        return cast(dup);
    }

    protected int getElementPosition(int idx)
    {
        return getElementPosition(blockIndex, idx, blockIndexSize);
    }

    protected int getElementsSize()
    {
        return blockIndexSize / 2;
    }

    protected abstract T cast(ByteBuffer data);

    static int getElementPosition(ByteBuffer data, int idx, int indexSize)
    {
        idx *= 2;
        assert idx < indexSize;
        return data.position() + indexSize + data.getShort(data.position() + idx);
    }

    public TokenTree getBlockIndex()
    {
        return combinedIndex;
    }

    public static class SearchResult<T>
    {
        public final T result;
        public final int index, cmp;

        public SearchResult(T result, int cmp, int index)
        {
            this.result = result;
            this.index = index;
            this.cmp = cmp;
        }
    }
}
