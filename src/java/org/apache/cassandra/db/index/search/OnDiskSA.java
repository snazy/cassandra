package org.apache.cassandra.db.index.search;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.search.OnDiskSABuilder.SuffixSize;
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
import com.google.common.collect.Iterators;

import static org.apache.cassandra.db.index.search.OnDiskSABuilder.SUPER_BLOCK_SIZE;
import static org.apache.cassandra.db.index.search.container.TokenTree.Token;
import static org.apache.cassandra.db.index.search.OnDiskSABuilder.BLOCK_SIZE;
import static org.apache.cassandra.db.index.search.OnDiskBlock.SearchResult;
import static org.apache.cassandra.db.index.utils.LazyMergeSortIterator.OperationType;
import static org.apache.cassandra.db.index.search.OnDiskSABuilder.Mode;

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

    public final Descriptor descriptor;
    protected final Mode mode;
    protected final SuffixSize suffixSize;

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
            descriptor = new Descriptor(backingFile.readUTF());

            suffixSize = SuffixSize.of(backingFile.readShort());

            minSuffix = ByteBufferUtil.readWithShortLength(backingFile);
            maxSuffix = ByteBufferUtil.readWithShortLength(backingFile);

            minKey = ByteBufferUtil.readWithShortLength(backingFile);
            maxKey = ByteBufferUtil.readWithShortLength(backingFile);

            mode = Mode.mode(backingFile.readUTF());

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
        int lowerBlock = lower == null ? 0 : getDataBlock(lower);
        int upperBlock = upper == null
                            ? dataLevel.blockCount - 1
                            // optimization so we don't have to fetch upperBlock when query has lower == upper
                            : (lower != null && comparator.compare(lower, upper) == 0) ? lowerBlock : getDataBlock(upper);

        // 'same' block query has to read all of the matching suffixes
        // as well as when the difference between lower and upper is less than once full block
        if (mode != Mode.SPARSE || lowerBlock == upperBlock || upperBlock - lowerBlock <= 1)
            return searchPoint(lowerBlock, lower, lowerInclusive, upperBlock, upper, upperInclusive);

        // once we have determined that lower block and upper block are at least a distance of
        // one full block away from each other it's time to figure out where lower/upper are
        // located in their designated blocks that will help us to decide if we have to treat
        // lowerBlock and upperBlock as partial blocks or whole block reads.

        // if lower is at the beginning of the block that means we can just do a single iterator per block
        SearchResult<DataSuffix> lowerPosition = (lower == null) ? null : searchIndex(lower, lowerBlock);
        SearchResult<DataSuffix> upperPosition = (upper == null) ? null : searchIndex(upper, upperBlock);

        final List<SkippableIterator<Long, Token>> union = new ArrayList<>();

        // optimistically assume that first and last blocks are full block reads, saves at least 3 'else' conditions
        int firstFullBlockIdx = lowerBlock, lastFullBlockIdx = upperBlock;

        // 'lower' doesn't cover the whole block so we need to do a partial iteration
        // Two reasons why that can happen:
        //   - 'lower' is not the first element of the block
        //   - 'lower' is first element but it's not inclusive in the query
        if (lowerPosition != null && (lowerPosition.index > 0 || !lowerInclusive))
        {
            DataBlock block = dataLevel.getBlock(lowerBlock);
            int start = (lowerInclusive || lowerPosition.cmp != 0) ? lowerPosition.index : lowerPosition.index + 1;

            union.add(block.getRange(start, block.getElementsSize()));
            firstFullBlockIdx = lowerBlock + 1;
        }

        if (upperPosition != null)
        {
            DataBlock block = dataLevel.getBlock(upperBlock);
            int lastIndex = block.getElementsSize() - 1;

            // The save as with 'lower' but here we need to check if the upper is the last element of the block,
            // which means that we only have to get individual results if:
            //  - if it *is not* the last element, or
            //  - it *is* but shouldn't be included (dictated by upperInclusive)
            if (upperPosition.index != lastIndex || !upperInclusive)
            {
                int end = (upperPosition.cmp < 0 || (upperPosition.cmp == 0 && upperInclusive))
                                ? upperPosition.index + 1 : upperPosition.index;

                union.add(block.getRange(0, end));
                lastFullBlockIdx = upperBlock - 1;
            }
        }

        int totalSuperBlocks = (lastFullBlockIdx - firstFullBlockIdx) / SUPER_BLOCK_SIZE;

        // if there are no super-blocks, we can simply read all of the block iterators in sequence
        if (totalSuperBlocks == 0)
        {
            for (int i = firstFullBlockIdx; i <= lastFullBlockIdx; i++)
                union.add(dataLevel.getBlock(i).getBlockIndex().iterator(keyFetcher));

            return new LazyMergeSortIterator<>(OperationType.OR, union);
        }

        // first get all of the blocks which are aligned before the first super-block in the sequence,
        // e.g. if the block range was (1, 9) and super-block-size = 4, we need to read 1, 2, 3, 4 - 7 is covered by
        // super-block, 8, 9 is a remainder.

        int superBlockAlignedStart = firstFullBlockIdx == 0 ? 0 : (int) FBUtilities.align(firstFullBlockIdx, SUPER_BLOCK_SIZE);
        for (int blockIdx = firstFullBlockIdx; blockIdx < Math.min(superBlockAlignedStart, lastFullBlockIdx); blockIdx++)
            union.add(getBlockIterator(blockIdx));

        // now read all of the super-blocks matched by the request, from the previous comment
        // it's a block with index 1 (which covers everything from 4 to 7)

        int superBlockIdx = superBlockAlignedStart / SUPER_BLOCK_SIZE;
        for (int offset = 0; offset < totalSuperBlocks - 1; offset++)
            union.add(dataLevel.getSuperBlock(superBlockIdx++).iterator());

        // now it's time for a remainder read, again from the previous example it's 8, 9 because
        // we have over-shot previous block but didn't request enough to cover next super-block.

        int lastCoveredBlock = superBlockIdx * SUPER_BLOCK_SIZE;
        for (int offset = 0; offset <= (lastFullBlockIdx - lastCoveredBlock); offset++)
            union.add(getBlockIterator(lastCoveredBlock + offset));

        return new LazyMergeSortIterator<>(OperationType.OR, union);
    }

    private SkippableIterator<Long, Token> searchPoint(int lowerBlock, ByteBuffer lower, boolean lowerInclusive,
                                                       int upperBlock, ByteBuffer upper, boolean upperInclusive)
    {
        IteratorOrder order = lower == null ? IteratorOrder.ASC : IteratorOrder.DESC;
        Iterator<DataSuffix> suffixes = lower == null
                                         ? iteratorAt(upperBlock, upper, order, upperInclusive)
                                         : iteratorAt(lowerBlock, lower, order, lowerInclusive);

        List<SkippableIterator<Long, Token>> union = new ArrayList<>();
        while (suffixes.hasNext())
        {
            DataSuffix suffix = suffixes.next();

            if (order == IteratorOrder.DESC && upper != null)
            {
                ByteBuffer s = suffix.getSuffix();
                s.limit(s.position() + upper.remaining());
                int cmp = comparator.compare(s, upper);

                if ((cmp > 0 && upperInclusive) || (cmp >= 0 && !upperInclusive))
                    break;
            }

            union.add(suffix.getTokens());
        }

        return new LazyMergeSortIterator<>(OperationType.OR, union);
    }

    private SkippableIterator<Long, Token> getBlockIterator(int blockIdx)
    {
        DataBlock block = dataLevel.getBlock(blockIdx);
        return (block.hasCombinedIndex)
                ? block.getBlockIndex().iterator(keyFetcher)
                : block.getRange(0, block.getElementsSize());
    }

    public Iterator<DataSuffix> iteratorAt(ByteBuffer query, IteratorOrder order, boolean inclusive)
    {
        int dataBlockIdx = levels.length == 0 ? 0 : getBlockIdx(findPointer(query), query);
        return iteratorAt(dataBlockIdx, query, order, inclusive);
    }

    private Iterator<DataSuffix> iteratorAt(int dataBlockIdx, ByteBuffer query, IteratorOrder order, boolean inclusive)
    {
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

    private int getDataBlock(ByteBuffer query)
    {
        return levels.length == 0 ? 0 : getBlockIdx(findPointer(query), query);
    }

    @Override
    public Iterator<DataSuffix> iterator()
    {
        return new DescDataIterator(dataLevel, 0, 0);
    }

    @Override
    public void close() throws IOException
    {
        if (!FileUtils.isCleanerAvailable())
            return;

        FileUtils.clean(indexFile);
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
        protected final int superBlockCnt;
        protected final long superBlocksOffset;

        public DataLevel(long offset, int count)
        {
            super(offset, count);
            long baseOffset = blockOffsets + blockCount * 8;
            superBlockCnt = indexFile.getInt((int)baseOffset);
            superBlocksOffset = baseOffset + 4;
        }

        @Override
        protected DataBlock cast(ByteBuffer block)
        {
            return new DataBlock(block);
        }

        public OnDiskSuperBlock getSuperBlock(int idx)
        {
            assert idx < superBlockCnt : String.format("requested index %d is greater than super block count %d", idx, superBlockCnt);
            long blockOffset = indexFile.getLong((int) (superBlocksOffset + idx * 8));
            return new OnDiskSuperBlock((ByteBuffer) indexFile.duplicate().position((int) blockOffset));
        }
    }

    protected class OnDiskSuperBlock
    {
        private final TokenTree tokenTree;

        public OnDiskSuperBlock(ByteBuffer buffer)
        {
            tokenTree = new TokenTree(buffer);
        }

        public SkippableIterator<Long, Token> iterator()
        {
            return tokenTree.iterator(keyFetcher);
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
            super(data, BlockType.DATA);
        }

        @Override
        protected DataSuffix cast(ByteBuffer data)
        {
            return new DataSuffix(data, suffixSize, getBlockIndex());
        }

        public SkippableIterator<Long, Token> getRange(int start, int end)
        {
            List<SkippableIterator<Long, Token>> union = new ArrayList<>();
            NavigableMap<Long, Token> sparse = new TreeMap<>();

            for (int i = start; i < end; i++)
            {
                DataSuffix suffix = getElement(i);

                if (suffix.isSparse())
                {
                    NavigableMap<Long, Token> tokens = suffix.getSparseTokens();
                    for (Map.Entry<Long, Token> t : tokens.entrySet())
                    {
                        Token token = sparse.get(t.getKey());
                        if (token == null)
                            sparse.put(t.getKey(), t.getValue());
                        else
                            token.merge(t.getValue());
                    }
                }
                else
                {
                    union.add(suffix.getTokens());
                }
            }

            PrefetchedTokensIterator prefetched = new PrefetchedTokensIterator(sparse);

            if (union.size() == 0)
                return prefetched;

            union.add(prefetched);
            return new LazyMergeSortIterator<>(OperationType.OR, union);
        }
    }

    protected class PointerBlock extends OnDiskBlock<PointerSuffix>
    {
        public PointerBlock(ByteBuffer block)
        {
            super(block, BlockType.POINTER);
        }

        @Override
        protected PointerSuffix cast(ByteBuffer data)
        {
            return new PointerSuffix(data, suffixSize);
        }
    }

    public class DataSuffix extends Suffix
    {
        private final TokenTree perBlockIndex;

        protected DataSuffix(ByteBuffer content, SuffixSize size, TokenTree perBlockIndex)
        {
            super(content, size);
            this.perBlockIndex = perBlockIndex;
        }

        public SkippableIterator<Long, Token> getTokens()
        {
            final int blockEnd = (int) FBUtilities.align(content.position(), BLOCK_SIZE);

            if (isSparse())
                return new PrefetchedTokensIterator(getSparseTokens());

            int offset = blockEnd + 4 + content.getInt(getDataOffset() + 1);
            return new TokenTree((ByteBuffer) indexFile.duplicate().position(offset)).iterator(keyFetcher);
        }

        public boolean isSparse()
        {
            return content.get(getDataOffset()) > 0;
        }

        public NavigableMap<Long, Token> getSparseTokens()
        {
            int ptrOffset = getDataOffset();

            byte size = content.get(ptrOffset);

            assert size > 0;

            NavigableMap<Long, Token> individualTokens = new TreeMap<>();
            for (int i = 0; i < size; i++)
            {
                Token token = perBlockIndex.get(content.getLong(ptrOffset + 1 + (8 * i)), keyFetcher);

                assert token != null;
                individualTokens.put(token.get(), token);
            }

            return individualTokens;
        }
    }

    protected static class PointerSuffix extends Suffix
    {
        public PointerSuffix(ByteBuffer content, SuffixSize size)
        {
            super(content, size);
        }

        public int getBlock()
        {
            return content.getInt(getDataOffset());
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

    private static class PrefetchedTokensIterator extends AbstractIterator<Token> implements SkippableIterator<Long, Token>
    {
        private final NavigableMap<Long, Token> tokens;
        private Iterator<Token> currentIterator;

        public PrefetchedTokensIterator(NavigableMap<Long, Token> tokens)
        {
            this.tokens = tokens;
            this.currentIterator = tokens.values().iterator();
        }

        @Override
        protected Token computeNext()
        {
            return currentIterator != null && currentIterator.hasNext()
                    ? currentIterator.next()
                    : endOfData();
        }

        @Override
        public void skipTo(Long next)
        {
            currentIterator = tokens.tailMap(next, true).values().iterator();
        }

        @Override
        public void close() throws IOException
        {
            endOfData();
        }
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
                out.printf("DataSuffix(chars: %s, offsets: %s)%n", comparator.compose(p.getSuffix()), Iterators.toString(p.getTokens()));
            }
        }

        out.println(" ***** end of level printout ***** ");
    }

    public String getIndexPath()
    {
        return indexPath;
    }
}
