package org.apache.cassandra.db.index.search;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.search.container.TokenTreeBuilder;
import org.apache.cassandra.db.index.search.sa.IntegralSA;
import org.apache.cassandra.db.index.search.sa.SA;
import org.apache.cassandra.db.index.search.sa.TermIterator;
import org.apache.cassandra.db.index.search.sa.SuffixSA;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.ByteBufferDataOutput;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.ShortArrayList;
import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: when serializing PointerSuffix, maybe it makes sense to encode blockIdx into the size of the suffix string
 * which limits number of blocks to 64K (can address 4GB with 64KB size blocks) and size of the suffix to the 64K
 * characters which might be a good space trade-off.
 */
public class OnDiskSABuilder
{
    private static final Logger logger = LoggerFactory.getLogger(OnDiskSABuilder.class);

    public static enum Mode
    {
        SUFFIX, ORIGINAL, SPARSE;

        public static Mode mode(String mode)
        {
            return Mode.valueOf(mode.toUpperCase());
        }
    }

    public static enum SuffixSize
    {
        INT(4), LONG(8), UUID(16), VARIABLE(-1);

        public final int size;

        SuffixSize(int size)
        {
            this.size = size;
        }

        public boolean isConstant()
        {
            return this != VARIABLE;
        }

        public static SuffixSize of(int size)
        {
            switch (size)
            {
                case -1:
                    return VARIABLE;

                case 4:
                    return INT;

                case 8:
                    return LONG;

                case 16:
                    return UUID;

                default:
                    throw new IllegalStateException("unknown state: " + size);
            }
        }

        public static SuffixSize sizeOf(AbstractType<?> comparator)
        {
            if (comparator instanceof Int32Type || comparator instanceof FloatType)
                return INT;

            if (comparator instanceof LongType || comparator instanceof DoubleType
                    || comparator instanceof TimestampType || comparator instanceof DateType)
                return LONG;

            if (comparator instanceof TimeUUIDType || comparator instanceof UUIDType)
                return UUID;

            return VARIABLE;
        }
    }

    public static final int BLOCK_SIZE = 4096;
    public static final int MAX_TERM_SIZE = 1024;
    public static final int SUPER_BLOCK_SIZE = 64;

    private final List<MutableLevel<InMemoryPointerSuffix>> levels = new ArrayList<>();
    private MutableLevel<InMemoryDataSuffix> dataLevel;

    private final SA<?> sa;
    private final SuffixSize suffixSize;

    public OnDiskSABuilder(AbstractType<?> comparator, Mode mode)
    {
        // split terms into suffixes only if it's text, otherwise (even if SUFFIX is set) use terms in original form
        this.sa = ((comparator instanceof UTF8Type || comparator instanceof AsciiType) && mode == Mode.SUFFIX)
                    ? new SuffixSA(comparator, mode) : new IntegralSA(comparator, mode);

        this.suffixSize = SuffixSize.sizeOf(comparator);
    }

    public OnDiskSABuilder add(ByteBuffer term, TokenTreeBuilder keys)
    {
        if (term.remaining() >= MAX_TERM_SIZE)
        {
            logger.error("Rejecting value (value size {}, maximum size {} bytes).", term.remaining(), Short.MAX_VALUE);
            return this;
        }

        sa.add(term, keys);
        return this;
    }

    private void addSuffix(InMemoryDataSuffix suffix, SequentialWriter out) throws IOException
    {
        InMemoryPointerSuffix ptr = dataLevel.add(suffix);
        if (ptr == null)
            return;

        int levelIdx = 0;
        for (;;)
        {
            MutableLevel<InMemoryPointerSuffix> level = getIndexLevel(levelIdx++, out);
            if ((ptr = level.add(ptr)) == null)
                break;
        }
    }

    public void finish(Pair<DecoratedKey, DecoratedKey> range, File file) throws FSWriteError
    {
        finish(Descriptor.CURRENT, range, file);
    }

    @VisibleForTesting
    public void finish(Descriptor descriptor, Pair<DecoratedKey, DecoratedKey> range, File file) throws FSWriteError
    {
        SequentialWriter out = null;

        try
        {
            out = new SequentialWriter(file, BLOCK_SIZE, false);

            TermIterator terms = sa.finish(descriptor);
            out.writeUTF(descriptor.version.toString());

            out.writeShort(suffixSize.size);

            // min, max suffix (useful to find initial scan range from search expressions)
            ByteBufferUtil.writeWithShortLength(terms.minTerm(), out);
            ByteBufferUtil.writeWithShortLength(terms.maxTerm(), out);

            // min, max keys covered by index (useful when searching across multiple indexes)
            ByteBufferUtil.writeWithShortLength(range.left.key, out);
            ByteBufferUtil.writeWithShortLength(range.right.key, out);

            Mode saMode = sa.getMode();

            out.writeUTF(saMode.toString());

            out.skipBytes((int) (BLOCK_SIZE - out.getFilePointer()));

            dataLevel = saMode == Mode.SPARSE ? new DataBuilderLevel(out, new MutableDataBlock(saMode, descriptor), descriptor)
                                              : new MutableLevel<>(out, new MutableDataBlock(saMode, descriptor));
            while (terms.hasNext())
            {
                Pair<ByteBuffer, TokenTreeBuilder> term = terms.next();
                addSuffix(new InMemoryDataSuffix(term.left, term.right), out);
            }

            dataLevel.finalFlush();
            for (MutableLevel l : levels)
                l.flush(); // flush all of the buffers

            // and finally write levels index
            final long levelIndexPosition = out.getFilePointer();

            out.writeInt(levels.size());
            for (int i = levels.size() - 1; i >= 0; i--)
                levels.get(i).flushMetadata();

            dataLevel.flushMetadata();

            out.writeLong(levelIndexPosition);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, file);
        }
        finally
        {
            FileUtils.closeQuietly(out);
        }
    }

    private MutableLevel<InMemoryPointerSuffix> getIndexLevel(int idx, SequentialWriter out)
    {
        if (levels.size() == 0)
            levels.add(new MutableLevel<>(out, new MutableBlock<InMemoryPointerSuffix>()));

        if (levels.size() - 1 < idx)
        {
            int toAdd = idx - (levels.size() - 1);
            for (int i = 0; i < toAdd; i++)
                levels.add(new MutableLevel<>(out, new MutableBlock<InMemoryPointerSuffix>()));
        }

        return levels.get(idx);
    }

    protected static void alignToBlock(SequentialWriter out) throws IOException
    {
        long endOfBlock = out.getFilePointer();
        if ((endOfBlock & (BLOCK_SIZE - 1)) != 0) // align on the block boundary if needed
            out.skipBytes((int) (FBUtilities.align(endOfBlock, BLOCK_SIZE) - endOfBlock));
    }

    private class InMemorySuffix
    {
        protected final ByteBuffer suffix;

        public InMemorySuffix(ByteBuffer suffix)
        {
            this.suffix = suffix;
        }

        public int serializedSize()
        {
            return (suffixSize.isConstant() ? 0 : 2) + suffix.remaining();
        }

        public void serialize(DataOutput out) throws IOException
        {
            if (suffixSize.isConstant())
                ByteBufferUtil.write(suffix, out);
            else
                ByteBufferUtil.writeWithShortLength(suffix, out);
        }
    }

    private class InMemoryPointerSuffix extends InMemorySuffix
    {
        protected final int blockCnt;

        public InMemoryPointerSuffix(ByteBuffer suffix, int blockCnt)
        {
            super(suffix);
            this.blockCnt = blockCnt;
        }

        @Override
        public int serializedSize()
        {
            return super.serializedSize() + 4;
        }

        @Override
        public void serialize(DataOutput out) throws IOException
        {
            super.serialize(out);
            out.writeInt(blockCnt);
        }
    }

    private class InMemoryDataSuffix extends InMemorySuffix
    {
        private TokenTreeBuilder keys;

        public InMemoryDataSuffix(ByteBuffer suffix, TokenTreeBuilder keys)
        {
            super(suffix);
            this.keys = keys;
        }
    }

    private class MutableLevel<T extends InMemorySuffix>
    {
        private final LongArrayList blockOffsets = new LongArrayList();

        protected final SequentialWriter out;

        private final MutableBlock<T> inProcessBlock;
        private InMemoryPointerSuffix lastSuffix;

        public MutableLevel(SequentialWriter out, MutableBlock<T> block)
        {
            this.out = out;
            this.inProcessBlock = block;
        }

        /**
         * @return If we flushed a block, return the last suffix of that block; else, null.
         */
        public InMemoryPointerSuffix add(T suffix) throws IOException
        {
            InMemoryPointerSuffix toPromote = null;

            if (!inProcessBlock.hasSpaceFor(suffix))
            {
                flush();
                toPromote = lastSuffix;
            }

            inProcessBlock.add(suffix);

            lastSuffix = new InMemoryPointerSuffix(suffix.suffix, blockOffsets.size());
            return toPromote;
        }

        public void flush() throws IOException
        {
            blockOffsets.add(out.getFilePointer());
            inProcessBlock.flushAndClear(out);
        }

        public void finalFlush() throws IOException
        {
            flush();
        }

        public void flushMetadata() throws IOException
        {
            flushMetadata(blockOffsets);
        }

        protected void flushMetadata(LongArrayList longArrayList) throws IOException
        {
            out.writeInt(longArrayList.size());
            for (int i = 0; i < longArrayList.size(); i++)
                out.writeLong(longArrayList.get(i));
        }
    }

    /** builds standard data blocks and super blocks, as well */
    private class DataBuilderLevel extends MutableLevel<InMemoryDataSuffix>
    {
        private final LongArrayList superBlockOffsets = new LongArrayList();

        /** count of regular data blocks written since current super block was init'd */
        private int dataBlocksCnt;
        private TokenTreeBuilder superBlockTree;
        private final Descriptor descriptor;

        public DataBuilderLevel(SequentialWriter out, MutableBlock<InMemoryDataSuffix> block, Descriptor d)
        {
            super(out, block);
            descriptor = d;
            superBlockTree = new TokenTreeBuilder(descriptor);
        }

        public InMemoryPointerSuffix add(InMemoryDataSuffix suffix) throws IOException
        {
            InMemoryPointerSuffix ptr = super.add(suffix);
            if (ptr != null)
            {
                dataBlocksCnt++;
                flushSuperBlock(false);
            }
            superBlockTree.add(suffix.keys.getTokens());
            return ptr;
        }

        public void flushSuperBlock(boolean force) throws IOException
        {
            if (dataBlocksCnt == SUPER_BLOCK_SIZE || (force && !superBlockTree.getTokens().isEmpty()))
            {
                superBlockOffsets.add(out.getFilePointer());
                superBlockTree.finish().write(out);
                alignToBlock(out);

                dataBlocksCnt = 0;
                superBlockTree = new TokenTreeBuilder(descriptor);
            }
        }

        public void finalFlush() throws IOException
        {
            super.flush();
            flushSuperBlock(true);
        }

        public void flushMetadata() throws IOException
        {
            super.flushMetadata();
            flushMetadata(superBlockOffsets);
        }
    }

    private static class MutableBlock<T extends InMemorySuffix>
    {
        protected final ByteBufferDataOutput buffer;
        protected final ShortArrayList offsets;

        public MutableBlock()
        {
            buffer = new ByteBufferDataOutput(ByteBuffer.allocate(BLOCK_SIZE));
            offsets = new ShortArrayList();
        }

        public final void add(T suffix) throws IOException
        {
            offsets.add((short) buffer.position());
            addInternal(suffix);
        }

        protected void addInternal(T suffix) throws IOException
        {
            suffix.serialize(buffer);
        }

        public boolean hasSpaceFor(T element)
        {
            return sizeAfter(element) < buffer.capacity();
        }

        protected int sizeAfter(T element)
        {
            return getWatermark() + 4 + element.serializedSize();
        }

        protected int getWatermark()
        {
            return 4 + offsets.size() * 2 + buffer.position();
        }

        public void flushAndClear(SequentialWriter out) throws IOException
        {
            out.writeInt(offsets.size());
            for (int i = 0; i < offsets.size(); i++)
                out.writeShort(offsets.get(i));

            buffer.writeFullyTo(out);

            alignToBlock(out);

            offsets.clear();
            buffer.clear();
        }
    }

    private static class MutableDataBlock extends MutableBlock<InMemoryDataSuffix>
    {
        private final Mode mode;

        private int offset = 0;
        private int sparseValueSuffixes = 0;

        private final List<TokenTreeBuilder> containers = new ArrayList<>();
        private TokenTreeBuilder combinedIndex;
        private final Descriptor descriptor;

        public MutableDataBlock(Mode mode, Descriptor descriptor)
        {
            this.mode = mode;
            this.descriptor = descriptor;
            this.combinedIndex = new TokenTreeBuilder(descriptor);
        }

        @Override
        protected void addInternal(InMemoryDataSuffix suffix) throws IOException
        {
            TokenTreeBuilder keys = suffix.keys;

            if (mode == Mode.SPARSE && keys.getTokenCount() <= 5)
            {
                writeSuffix(suffix, keys);
                sparseValueSuffixes++;
            }
            else
            {
                writeSuffix(suffix, offset);

                offset += keys.serializedSize();
                containers.add(keys);
            }

            if (mode == Mode.SPARSE)
                combinedIndex.add(keys.getTokens());
        }

        @Override
        protected int sizeAfter(InMemoryDataSuffix element)
        {
            return super.sizeAfter(element) + ptrLength(element);
        }

        @Override
        public void flushAndClear(SequentialWriter out) throws IOException
        {
            super.flushAndClear(out);

            out.writeInt((sparseValueSuffixes == 0) ? -1 : offset);

            if (containers.size() > 0)
            {
                for (TokenTreeBuilder tokens : containers)
                    tokens.write(out);
            }

            if (sparseValueSuffixes > 0)
            {
                combinedIndex.finish().write(out);
            }

            alignToBlock(out);

            containers.clear();
            combinedIndex = new TokenTreeBuilder(descriptor);

            offset = 0;
            sparseValueSuffixes = 0;
        }

        private int ptrLength(InMemoryDataSuffix suffix)
        {
            return (suffix.keys.getTokenCount() > 5)
                    ? 5 // 1 byte type + 4 byte offset to the tree
                    : 1 + (8 * (int) suffix.keys.getTokenCount()); // 1 byte size + n 8 byte tokens
        }

        private void writeSuffix(InMemorySuffix suffix, TokenTreeBuilder keys) throws IOException
        {
            suffix.serialize(buffer);
            buffer.writeByte((byte) keys.getTokenCount());

            Iterator<Pair<Long, LongSet>> tokens = keys.iterator();
            while (tokens.hasNext())
                buffer.writeLong(tokens.next().left);
        }

        private void writeSuffix(InMemorySuffix suffix, int offset) throws IOException
        {
            suffix.serialize(buffer);
            buffer.writeByte(0x0);
            buffer.writeInt(offset);
        }
    }
}
