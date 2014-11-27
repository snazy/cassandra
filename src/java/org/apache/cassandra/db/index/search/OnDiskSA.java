package org.apache.cassandra.db.index.search;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Iterator;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.utils.ByteBufferDataInput;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.FSReadError;

import com.google.common.collect.AbstractIterator;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.roaringbitmap.RoaringBitmap;

import static org.apache.cassandra.db.index.search.OnDiskBlock.SearchResult;

public class OnDiskSA implements Iterable<OnDiskSA.DataSuffix>, Closeable
{
    public static enum IteratorOrder
    {
        DESC, ASC;

        public int startAt(SearchResult<DataSuffix> found, boolean inclusive)
        {
            switch (this)
            {
                case DESC:
                    if (found.cmp < 0)
                        return found.index + 1;

                    return inclusive || found.cmp != 0 ? found.index : found.index + 1;

                case ASC:
                    if (found.cmp < 0) // search term was bigger then whole data set
                        return found.index;
                    return inclusive && (found.cmp == 0 || found.cmp < 0) ? found.index : found.index - 1;

                default:
                    throw new IllegalArgumentException("Unknown order: " + this);
            }
        }
    }

    protected final AbstractType<?> comparator;
    protected final RandomAccessReader file;
    protected final String indexPath;

    protected final PointerLevel[] levels;
    protected final DataLevel dataLevel;

    public OnDiskSA(File index, AbstractType<?> cmp) throws IOException
    {
        comparator = cmp;
        indexPath = index.getAbsolutePath();

        file = RandomAccessReader.open(index, OnDiskSABuilder.BLOCK_SIZE, null);

        file.seek(file.length() - 8);

        // start of the levels
        file.seek(file.readLong());

        int numLevels = file.readInt();
        levels = new PointerLevel[numLevels];
        for (int i = 0; i < levels.length; i++)
        {
            int blockCount = file.readInt();
            levels[i] = new PointerLevel(file.getFilePointer(), blockCount);
            file.skipBytes(blockCount * 8);
        }

        int blockCount = file.readInt();
        dataLevel = new DataLevel(file.getFilePointer(), blockCount);
    }


    public RoaringBitmap search(ByteBuffer query) throws IOException
    {
        if (levels.length == 0) // not enough data to even feel a single block
        {
            return searchDataBlock(query, 0);
        }

        PointerSuffix ptr = findPointer(query);
        return ptr == null ? null : searchDataBlock(query, getBlockIdx(ptr, query));
    }

    public Iterator<DataSuffix> iteratorAt(ByteBuffer query, IteratorOrder order, boolean inclusive)
    {
        int dataBlockIdx = levels.length == 0 ? 0 : getBlockIdx(findPointer(query), query);
        SearchResult<DataSuffix> start = searchIndex(query, dataBlockIdx);

        switch (order)
        {
            case DESC:
                return new DescDataIterator(dataLevel, dataBlockIdx, order.startAt(start, inclusive));

            case ASC:
                return new AscDataIterator(dataLevel, dataBlockIdx, order.startAt(start, inclusive));

            default:
                throw new IllegalArgumentException("Unknown order: " + order);
        }
    }

    @Override
    public Iterator<DataSuffix> iterator()
    {
        return new DescDataIterator(dataLevel, 0, 0);
    }

    @Override
    public void close() throws IOException
    {
        file.close();
    }

    private PointerSuffix findPointer(ByteBuffer query)
    {
        PointerSuffix ptr = null;
        for (PointerLevel level : levels)
        {
            if ((ptr = level.getPointer(ptr, query)) == null)
                return null;
        }

        return ptr;
    }

    private RoaringBitmap searchDataBlock(ByteBuffer query, int blockIdx) throws IOException
    {
        SearchResult<DataSuffix> suffix = dataLevel.getBlock(blockIdx).search(comparator, query);
        if (suffix == null || suffix.result == null)
            return null;

        int cmp = suffix.result.compareTo(comparator, query, false);
        return cmp != 0 ? null : suffix.result.getKeys();
    }

    private SearchResult<DataSuffix> searchIndex(ByteBuffer query, int blockIdx)
    {
        return dataLevel.getBlock(blockIdx).search(comparator, query);
    }

    private int getBlockIdx(PointerSuffix ptr, ByteBuffer query)
    {
        int blockIdx = 0;
        if (ptr != null)
        {
            int cmp = ptr.compareTo(comparator, query);
            blockIdx = (cmp == 0 || cmp > 0) ? ptr.getBlock() : ptr.getBlock() + 1;
        }

        return blockIdx;
    }

    protected class PointerLevel extends Level<PointerBlock>
    {
        public PointerLevel(long offset, int count)
        {
            super(offset, count);
        }

        public PointerSuffix getPointer(PointerSuffix parent, ByteBuffer query)
        {
            return getBlock(getBlockIdx(parent, query)).search(comparator, query).result;
        }

        @Override
        protected PointerBlock cast(ByteBuffer block, RandomAccessReader source)
        {
            return new PointerBlock(block, source);
        }
    }

    protected class DataLevel extends Level<DataBlock>
    {
        public DataLevel(long offset, int count)
        {
            super(offset, count);
        }

        @Override
        protected DataBlock cast(ByteBuffer block, RandomAccessReader source)
        {
            return new DataBlock(block, source);
        }
    }

    protected abstract class Level<T extends OnDiskBlock>
    {
        protected final long blockOffsets;
        protected final int blockCount;

        public Level(long offsets, int count)
        {
            this.blockOffsets = offsets;
            this.blockCount = count;
        }

        public T getBlock(int idx) throws FSReadError
        {
            assert idx >= 0 && idx < blockCount;

            byte[] block = new byte[OnDiskSABuilder.BLOCK_SIZE];

            try
            {
                // seek to the block offset in the level index
                // this is done to save memory for long[] because
                // the number of blocks in the level is dependent on the
                // block size (which, by default, is 4K), as we align all of the blocks
                // and index is pre-faulted on construction this seek shouldn't be
                // performance problem.
                file.seek(blockOffsets + idx * 8);

                // read block offset and move there
                file.seek(file.readLong());
                file.read(block);
            }
            catch (IOException e)
            {
                throw new FSReadError(e, indexPath);
            }

            return cast(ByteBuffer.wrap(block), file);
        }

        protected abstract T cast(ByteBuffer block, RandomAccessReader source);
    }

    protected static class DataBlock extends OnDiskBlock<DataSuffix>
    {
        public DataBlock(ByteBuffer data, RandomAccessReader source)
        {
            super(data, source);
        }

        @Override
        protected DataSuffix cast(ByteBuffer data)
        {
            return new DataSuffix(data, source, auxiliarySectionOffset);
        }
    }

    protected static class PointerBlock extends OnDiskBlock<PointerSuffix>
    {
        public PointerBlock(ByteBuffer block, RandomAccessReader source)
        {
            super(block, source);
        }

        @Override
        protected PointerSuffix cast(ByteBuffer data)
        {
            return new PointerSuffix(data);
        }
    }

    public static class DataSuffix extends Suffix
    {
        private final RandomAccessReader file;
        private final long auxSectionOffset;

        protected DataSuffix(ByteBuffer content, RandomAccessReader file, long auxSectionOffset)
        {
            super(content);
            this.file = file;
            this.auxSectionOffset = auxSectionOffset;
        }

        public int getOffset()
        {
            int position = content.position();
            return content.getInt(position + 2 + content.getShort(position));
        }

        public RoaringBitmap getKeys() throws IOException
        {
            RoaringBitmap keys = new RoaringBitmap();

            file.seek(auxSectionOffset + getOffset());
            keys.deserialize(file);

            return keys;
        }
    }

    protected static class PointerSuffix extends Suffix
    {
        public PointerSuffix(ByteBuffer content)
        {
            super(content);
        }

        public int getBlock()
        {
            int position = content.position();
            return content.getInt(position + 2 + content.getShort(position));
        }
    }

    private static class DescDataIterator extends DataIterator
    {
        protected DescDataIterator(DataLevel level, int block, int index)
        {
            super(level, block, index);
        }

        @Override
        protected int nextBlock()
        {
            return block++;
        }

        @Override
        protected int nextIndex()
        {
            return index++;
        }

        @Override
        protected int startIndex()
        {
            return 0;
        }
    }

    private static class AscDataIterator extends DataIterator
    {
        protected AscDataIterator(DataLevel level, int block, int index)
        {
            super(level, block, index);
        }

        @Override
        protected int nextBlock()
        {
            return block--;
        }

        @Override
        protected int nextIndex()
        {
            return index--;
        }

        @Override
        protected int startIndex()
        {
            return current.getElementsSize() - 1;
        }
    }

    private abstract static class DataIterator extends AbstractIterator<DataSuffix>
    {
        protected final DataLevel level;

        protected DataBlock current;
        protected int block = 0, index = 0;

        protected DataIterator(DataLevel level, int block, int index)
        {
            this.level = level;
            this.block = block;
            this.index = index;
            this.current = level.getBlock(nextBlock());
        }

        @Override
        protected DataSuffix computeNext()
        {
            if (current == null)
                return endOfData();

            if (index >= 0 && index < current.getElementsSize())
                return current.getElement(nextIndex());

            if (block >= 0 && block < level.blockCount)
            {
                current = level.getBlock(nextBlock());
                index = startIndex();
                return computeNext();
            }

            current = null;
            return endOfData();
        }

        protected abstract int nextBlock();
        protected abstract int nextIndex();
        protected abstract int startIndex();
    }
}
