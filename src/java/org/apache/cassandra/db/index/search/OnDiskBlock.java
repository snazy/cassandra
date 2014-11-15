package org.apache.cassandra.db.index.search;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;

public abstract class OnDiskBlock<T extends Suffix>
{
    protected final ByteBuffer data;
    protected final int blockIndexSize;

    public OnDiskBlock(ByteBuffer data)
    {
        this.data = data;
        this.blockIndexSize = data.getInt() * 4;
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
            if (cmp == 0) {
                return new SearchResult<>(element, cmp, middle);
            }

            if (cmp < 0)
                start = middle + 1;
            else
                end = middle - 1;
        }

        return new SearchResult<>(element, cmp, middle);
    }

    protected T getElement(int index)
    {
        ByteBuffer dup = data.duplicate();
        int startsAt = getElementPosition(index);
        if (getElementsSize() - 1 == index) // last element
            dup.position(startsAt);
        else
            dup.position(startsAt).limit(getElementPosition(index + 1));

        return cast(dup);
    }

    protected int getElementPosition(int idx)
    {
        return getElementPosition(data, idx, blockIndexSize);
    }

    protected int getElementsSize()
    {
        return blockIndexSize / 4;
    }

    protected abstract T cast(ByteBuffer data);

    static int getElementPosition(ByteBuffer data, int idx, int indexSize)
    {
        idx *= 4;
        assert idx < indexSize;
        return data.position() + indexSize + data.getInt(data.position() + idx);
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
