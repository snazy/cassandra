package org.apache.cassandra.db.index.search;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.utils.ByteBufferDataInput;

import com.google.common.collect.AbstractIterator;
import org.roaringbitmap.RoaringBitmap;

public class OnDiskSA implements Iterable<OnDiskSA.DataSuffix>, Closeable
{
    protected final AbstractType<?> comparator;
    protected final RandomAccessFile file;
    protected final String indexPath;

    protected final PointerLevel[] levels;
    protected final DataLevel dataLevel;

    public OnDiskSA(File index, AbstractType<?> cmp) throws IOException
    {
        comparator = cmp;
        indexPath = index.getAbsolutePath();

        file = new RandomAccessFile(index, "r");
        file.seek(file.length() - 4); // to figure out start of the level index as last 8 bytes (long) of the file

        int dataBlockSize = file.readInt();

        file.seek(file.getFilePointer() - 12);

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
        dataLevel = new DataLevel(file.getFilePointer(), blockCount, dataBlockSize);
    }


    public RoaringBitmap search(ByteBuffer query) throws IOException
    {
        if (levels.length == 0) // not enough data to even feel a single block
            return searchDataBlock(query, 0);

        PointerSuffix ptr = findPointer(query);
        return ptr == null ? null : searchDataBlock(query, getBlockIdx(ptr, query));
    }

    public Iterator<DataSuffix> iteratorAt(ByteBuffer query)
    {
        if (levels.length == 0) // not enough data to even feel a single block
            return new DataIterator(dataLevel, 0, searchIndex(query, 0));

        PointerSuffix ptr = findPointer(query);
        int dataBlockIdx = getBlockIdx(ptr, query);
        return new DataIterator(dataLevel, dataBlockIdx, searchIndex(query, dataBlockIdx));
    }

    @Override
    public Iterator<DataSuffix> iterator()
    {
        return new DataIterator(dataLevel);
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
        OnDiskBlock.IntPair<DataSuffix> suffix = dataLevel.getBlock(blockIdx).search(comparator, query);
        return suffix == null || suffix.right == null ? null : suffix.right.getKeys();
    }

    private int searchIndex(ByteBuffer query, int blockIdx)
    {
        return dataLevel.getBlock(blockIdx).search(comparator, query).left;
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
        public PointerLevel(long offset, int count) throws IOException
        {
            super(offset, count, OnDiskSABuilder.INDEX_BLOCK_SIZE);
        }

        public PointerSuffix getPointer(PointerSuffix parent, ByteBuffer query)
        {
            return getBlock(getBlockIdx(parent, query)).search(comparator, query).right;
        }

        @Override
        protected PointerBlock cast(ByteBuffer block)
        {
            return new PointerBlock(block);
        }
    }

    protected class DataLevel extends Level<DataBlock>
    {
        public DataLevel(long offset, int count, int blockSize) throws IOException
        {
            super(offset, count, blockSize);
        }

        @Override
        protected DataBlock cast(ByteBuffer block)
        {
            return new DataBlock(block);
        }
    }

    protected abstract class Level<T extends OnDiskBlock>
    {
        protected final long blockOffsets;
        protected final int blockCount, blockSize;

        public Level(long offsets, int count, int blockSize) throws IOException
        {
            this.blockOffsets = offsets;
            this.blockCount = count;
            this.blockSize = blockSize;
        }

        public T getBlock(int idx) throws FSReadError
        {
            assert idx >= 0 && idx < blockCount;

            byte[] block = new byte[blockSize];

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

            return cast(ByteBuffer.wrap(block));
        }

        protected abstract T cast(ByteBuffer block);
    }

    protected static class DataBlock extends OnDiskBlock<DataSuffix>
    {
        public DataBlock(ByteBuffer data)
        {
            super(data);
        }

        @Override
        protected DataSuffix cast(ByteBuffer data)
        {
            return new DataSuffix(data);
        }
    }

    protected static class PointerBlock extends OnDiskBlock<PointerSuffix>
    {
        public PointerBlock(ByteBuffer data)
        {
            super(data);
        }

        @Override
        protected PointerSuffix cast(ByteBuffer data)
        {
            return new PointerSuffix(data);
        }
    }

    public static class DataSuffix extends Suffix
    {
        protected DataSuffix(ByteBuffer content)
        {
            super(content);
        }

        public RoaringBitmap getKeys() throws IOException
        {
            ByteBuffer dup = content.duplicate();
            int len = dup.getInt();
            dup.position(dup.position() + len);

            RoaringBitmap keys = new RoaringBitmap();
            keys.deserialize(new ByteBufferDataInput(dup));
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
            ByteBuffer dup = content.duplicate();
            int len = dup.getInt();
            dup.position(dup.position() + len);
            return dup.getInt();
        }
    }

    private static class DataIterator extends AbstractIterator<DataSuffix>
    {
        private final DataLevel level;
        private DataBlock current;
        private int block = 0, index = 0;

        protected DataIterator(DataLevel level)
        {
            this(level, 0, 0);
        }

        protected DataIterator(DataLevel level, int block, int index)
        {
            this.level = level;
            this.block = block;
            this.index = index;
            this.current = level.getBlock(this.block++);
        }

        @Override
        protected DataSuffix computeNext()
        {
            if (current == null)
                return endOfData();

            if (index < current.getElementsSize())
                return current.getElement(index++);

            if (block < level.blockCount)
            {
                current = level.getBlock(block++);
                index = 0;
                return computeNext();
            }

            current = null;
            return endOfData();
        }
    }
}
