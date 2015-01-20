package org.apache.cassandra.db.index.search;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.search.container.TokenTree;
import org.apache.cassandra.db.index.utils.LazyMergeSortIterator;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;

import static org.apache.cassandra.db.index.search.container.TokenTree.Token;
import static org.apache.cassandra.db.index.search.OnDiskSABuilder.BLOCK_SIZE;
import static org.apache.cassandra.db.index.search.OnDiskBlock.SearchResult;
import static org.apache.cassandra.db.index.utils.LazyMergeSortIterator.OperationType;

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
    protected final MappedByteBuffer indexFile;
    protected final int indexSize;

    protected final Function<Long, DecoratedKey> keyFetcher;

    protected final String indexPath;

    protected final PointerLevel[] levels;
    protected final DataLevel dataLevel;

    protected final ByteBuffer minSuffix, maxSuffix, minKey, maxKey;

    public OnDiskSA(File index, AbstractType<?> cmp, Function<Long, DecoratedKey> keyReader)
    {
        keyFetcher = keyReader;

        comparator = cmp;
        indexPath = index.getAbsolutePath();

        RandomAccessFile backingFile = null;
        try
        {
            backingFile = new RandomAccessFile(index, "r");

            assert backingFile.length() <= Integer.MAX_VALUE;

            minSuffix = ByteBufferUtil.readWithShortLength(backingFile);
            maxSuffix = ByteBufferUtil.readWithShortLength(backingFile);

            minKey = ByteBufferUtil.readWithShortLength(backingFile);
            maxKey = ByteBufferUtil.readWithShortLength(backingFile);

            indexSize = (int) backingFile.length();
            indexFile = backingFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, indexSize);

            // start of the levels
            indexFile.position((int) indexFile.getLong(indexSize - 8));

            int numLevels = indexFile.getInt();
            levels = new PointerLevel[numLevels];
            for (int i = 0; i < levels.length; i++)
            {
                int blockCount = indexFile.getInt();
                levels[i] = new PointerLevel(indexFile.position(), blockCount);
                indexFile.position(indexFile.position() + blockCount * 8);
            }

            int blockCount = indexFile.getInt();
            dataLevel = new DataLevel(indexFile.position(), blockCount);
        }
        catch (IOException e)
        {
            throw new FSReadError(e, index);
        }
        finally
        {
            FileUtils.closeQuietly(backingFile);
        }
    }

    public ByteBuffer minSuffix()
    {
        return minSuffix;
    }

    public ByteBuffer maxSuffix()
    {
        return maxSuffix;
    }

    public ByteBuffer minKey()
    {
        return minKey;
    }

    public ByteBuffer maxKey()
    {
        return maxKey;
    }

    public SkippableIterator<Long, Token> search(ByteBuffer suffix) throws IOException
    {
        return search(suffix, true, suffix, true);
    }

    public SkippableIterator<Long, Token> search(ByteBuffer lower, boolean lowerInclusive,
                                                 ByteBuffer upper, boolean upperInclusive)
    {
        IteratorOrder order = lower == null ? IteratorOrder.ASC : IteratorOrder.DESC;
        Iterator<DataSuffix> suffixes = lower == null
                                         ? iteratorAt(upper, order, upperInclusive)
                                         : iteratorAt(lower, order, lowerInclusive);


        List<SkippableIterator<Long, Token>> union = new ArrayList<>();
        while (suffixes.hasNext())
        {
            DataSuffix suffix = suffixes.next();

            if (order == IteratorOrder.DESC && upper != null) {
                ByteBuffer s = suffix.getSuffix();
                s.limit(s.position() + upper.remaining());
                int cmp = comparator.compare(s, upper);
                if ((cmp > 0 && upperInclusive) || (cmp >= 0 && !upperInclusive))
                    break;
            }

            union.add(suffix.getOffsets());
        }

        return new LazyMergeSortIterator<>(OperationType.OR, union);
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
    {}

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
        protected PointerBlock cast(ByteBuffer block)
        {
            return new PointerBlock(block);
        }
    }

    protected class DataLevel extends Level<DataBlock>
    {
        public DataLevel(long offset, int count)
        {
            super(offset, count);
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
        protected final int blockCount;

        public Level(long offsets, int count)
        {
            this.blockOffsets = offsets;
            this.blockCount = count;
        }

        public T getBlock(int idx) throws FSReadError
        {
            assert idx >= 0 && idx < blockCount;

            // calculate block offset and move there
            // (long is intentional, we'll just need mmap implementation which supports long positions)
            long blockOffset = indexFile.getLong((int) (blockOffsets + idx * 8));
            return cast((ByteBuffer) indexFile.duplicate().position((int) blockOffset));
        }

        protected abstract T cast(ByteBuffer block);
    }

    protected class DataBlock extends OnDiskBlock<DataSuffix>
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
        public PointerBlock(ByteBuffer block)
        {
            super(block);
        }

        @Override
        protected PointerSuffix cast(ByteBuffer data)
        {
            return new PointerSuffix(data);
        }
    }

    public class DataSuffix extends Suffix
    {
        protected DataSuffix(ByteBuffer content)
        {
            super(content);

        }

        public int getOffset()
        {
            int position = content.position();
            return content.getInt(position + 2 + content.getShort(position));
        }

        public SkippableIterator<Long, Token> getOffsets()
        {
            int blockEnd = (int) FBUtilities.align(content.position(), BLOCK_SIZE);
            return new TokenTree((ByteBuffer) indexFile.duplicate().position(blockEnd + getOffset())).iterator(keyFetcher);
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

    @SuppressWarnings("unused")
    public void printSA(PrintStream out) throws IOException
    {
        int level = 0;
        for (OnDiskSA.PointerLevel l : levels)
        {
            out.println(" !!!! level " + (level++));
            for (int i = 0; i < l.blockCount; i++)
            {
                out.println(" --- block " + i + " ---- ");
                OnDiskSA.PointerBlock block = l.getBlock(i);
                for (int j = 0; j < block.getElementsSize(); j++)
                {
                    OnDiskSA.PointerSuffix p = block.getElement(j);
                    out.printf("PointerSuffix(chars: %s, blockIdx: %d)%n", comparator.compose(p.getSuffix()), p.getBlock());
                }
            }
        }

        out.println(" !!!!! data blocks !!!!! ");
        for (int i = 0; i < dataLevel.blockCount; i++)
        {
            out.println(" --- block " + i + " ---- ");
            OnDiskSA.DataBlock block = dataLevel.getBlock(i);
            for (int j = 0; j < block.getElementsSize(); j++)
            {
                OnDiskSA.DataSuffix p = block.getElement(j);
                out.printf("DataSuffix(chars: %s, offsets: %s)%n", comparator.compose(p.getSuffix()), Iterators.toString(p.getOffsets()));
            }
        }

        out.println(" ***** end of level printout ***** ");
    }
}
