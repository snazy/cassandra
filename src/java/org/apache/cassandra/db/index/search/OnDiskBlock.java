package org.apache.cassandra.db.index.search;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.index.search.container.TokenTree;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.NativeMappedBuffer;

import static org.apache.cassandra.db.index.search.OnDiskSABuilder.BLOCK_SIZE;

public abstract class OnDiskBlock<T extends Suffix>
{
    public static enum BlockType
    {
        POINTER, DATA
    }

    // this contains offsets of the suffixes and suffix data
    protected final NativeMappedBuffer blockIndex;
    protected final int blockIndexSize;

    protected final boolean hasCombinedIndex;
    protected final TokenTree combinedIndex;

    public OnDiskBlock(NativeMappedBuffer block, BlockType blockType)
    {
        blockIndex = block;

        if (blockType == BlockType.POINTER)
        {
            hasCombinedIndex = false;
            combinedIndex = null;
            blockIndexSize = block.getInt() * 2;
            return;
        }

        long blockOffset = block.position();
        int combinedIndexOffset = block.getInt(blockOffset + BLOCK_SIZE);

        hasCombinedIndex = (combinedIndexOffset >= 0);
        long blockIndexOffset = blockOffset + BLOCK_SIZE + 4 + combinedIndexOffset;

        combinedIndex = hasCombinedIndex ? new TokenTree(blockIndex.duplicate().position(blockIndexOffset)) : null;
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
        NativeMappedBuffer dup = blockIndex.duplicate();
        long startsAt = getElementPosition(index);
        if (getElementsSize() - 1 == index) // last element
            dup.position(startsAt);
        else
            dup.position(startsAt).limit(getElementPosition(index + 1));

        return cast(dup);
    }

    protected long getElementPosition(int idx)
    {
        return getElementPosition(blockIndex, idx, blockIndexSize);
    }

    protected int getElementsSize()
    {
        return blockIndexSize / 2;
    }

    protected abstract T cast(NativeMappedBuffer data);

    static long getElementPosition(NativeMappedBuffer data, int idx, int indexSize)
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
