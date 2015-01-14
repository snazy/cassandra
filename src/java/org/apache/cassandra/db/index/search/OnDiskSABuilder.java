package org.apache.cassandra.db.index.search;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.search.container.TokenTreeBuilder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.ByteBufferDataOutput;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.ShortArrayList;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import net.mintern.primitive.Primitive;
import net.mintern.primitive.comparators.IntComparator;
import net.mintern.primitive.comparators.LongComparator;

/**
 * TODO: when serializing PointerSuffix, maybe it makes sense to encode blockIdx into the size of the suffix string
 * which limits number of blocks to 64K (can address 4GB with 64KB size blocks) and size of the suffix to the 64K
 * characters which might be a good space trade-off.
 */
public class OnDiskSABuilder
{
    public static enum Mode
    {
        SUFFIX, ORIGINAL
    }

    public static final int BLOCK_SIZE = 4096;

    private final List<MutableLevel<InMemoryPointerSuffix>> levels = new ArrayList<>();
    private MutableLevel<InMemoryDataSuffix> dataLevel;

    private final SA sa;
    private final Mode mode;

    public OnDiskSABuilder(AbstractType<?> keyComparator, AbstractType<?> comparator, Mode mode)
    {
        this.sa = new SA(keyComparator, comparator, mode);
        this.mode = mode;
    }

    public OnDiskSABuilder add(ByteBuffer term, NavigableMap<DecoratedKey, Long> keys)
    {
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
        SequentialWriter out = null;

        try
        {
            out = new SequentialWriter(file, BLOCK_SIZE, false);

            SuffixIterator suffixes = sa.finish();

            // min, max suffix (useful to find initial scan range from search expressions)
            ByteBufferUtil.writeWithShortLength(suffixes.minSuffix(), out);
            ByteBufferUtil.writeWithShortLength(suffixes.maxSuffix(), out);

            // min, max keys covered by index (useful when searching across multiple indexes)
            ByteBufferUtil.writeWithShortLength(range.left.key, out);
            ByteBufferUtil.writeWithShortLength(range.right.key, out);

            out.skipBytes((int) (BLOCK_SIZE - out.getFilePointer()));

            dataLevel = new MutableLevel<>(out, new MutableDataBlock(mode));
            while (suffixes.hasNext())
            {
                Pair<ByteBuffer, TokenTreeBuilder> suffix = suffixes.next();
                addSuffix(new InMemoryDataSuffix(suffix.left, suffix.right), out);
            }

            dataLevel.flush();
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

    private static class InMemorySuffix
    {
        protected final ByteBuffer suffix;

        public InMemorySuffix(ByteBuffer suffix)
        {
            this.suffix = suffix;
        }

        public int serializedSize()
        {
            return 2 + suffix.remaining();
        }

        public void serialize(DataOutput out) throws IOException
        {
            ByteBufferUtil.writeWithShortLength(suffix, out);
        }
    }

    private static class InMemoryPointerSuffix extends InMemorySuffix
    {
        protected final int blockIdx;

        public InMemoryPointerSuffix(ByteBuffer suffix, int blockIdx)
        {
            super(suffix);
            this.blockIdx = blockIdx;
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
            out.writeInt(blockIdx);
        }
    }

    private static class InMemoryDataSuffix extends InMemorySuffix
    {
        private TokenTreeBuilder keys;

        public InMemoryDataSuffix(ByteBuffer suffix, TokenTreeBuilder keys)
        {
            super(suffix);
            this.keys = keys;
        }
    }

    private static class SA
    {
        private final List<Term> terms = new ArrayList<>();
        private final AbstractType<?> keyComparator, comparator;
        private final Mode mode;

        private int charCount = 0;

        public SA(AbstractType<?> keyComparator, AbstractType<?> comparator, Mode mode)
        {
            this.keyComparator = keyComparator;
            this.comparator = comparator;
            this.mode = mode;
        }

        public void add(ByteBuffer term, NavigableMap<DecoratedKey, Long> keys)
        {
            terms.add(new Term(charCount, term, keys));
            charCount += term.remaining();
        }

        public SuffixIterator finish()
        {
            switch (mode)
            {
                case SUFFIX:
                    return new SASuffixIterator();

                case ORIGINAL:
                    return new IntegralSuffixIterator();

                default:
                    throw new IllegalArgumentException("unknown mode: " + mode);
            }
        }

        private class IntegralSuffixIterator extends SuffixIterator
        {
            private final Iterator<Term> termIterator;

            public IntegralSuffixIterator()
            {
                Collections.sort(terms, new Comparator<Term>()
                {
                    @Override
                    public int compare(Term a, Term b)
                    {
                        return comparator.compare(a.value, b.value);
                    }
                });

                termIterator = terms.iterator();
            }

            @Override
            public ByteBuffer minSuffix()
            {
                return terms.get(0).value;
            }

            @Override
            public ByteBuffer maxSuffix()
            {
                return terms.get(terms.size() - 1).value;
            }

            @Override
            protected Pair<ByteBuffer, TokenTreeBuilder> computeNext()
            {
                if (!termIterator.hasNext())
                    return endOfData();

                Term term = termIterator.next();
                return Pair.create(term.value, new TokenTreeBuilder(transformKeysMap(term.keys)));
            }
        }

        private class SASuffixIterator extends SuffixIterator
        {
            private final long[] suffixes;

            private int current = 0;
            private ByteBuffer lastProcessedSuffix;
            private TokenTreeBuilder container;

            public SASuffixIterator()
            {
                // each element has term index and char position encoded as two 32-bit integers
                // to avoid binary search per suffix while sorting suffix array.
                suffixes = new long[charCount];

                long termIndex = -1, currentTermLength = -1;
                for (int i = 0; i < charCount; i++)
                {
                    if (i >= currentTermLength || currentTermLength == -1)
                    {
                        Term currentTerm = terms.get((int) ++termIndex);
                        currentTermLength = currentTerm.position + currentTerm.value.remaining();
                    }

                    suffixes[i] = (termIndex << 32) | i;
                }

                Primitive.sort(suffixes, new LongComparator()
                {
                    @Override
                    public int compare(long a, long b)
                    {
                        Term aTerm = terms.get((int) (a >>> 32));
                        Term bTerm = terms.get((int) (b >>> 32));
                        return comparator.compare(aTerm.getSuffix(((int) a) - aTerm.position),
                                bTerm.getSuffix(((int) b) - bTerm.position));
                    }
                });
            }

            private Pair<ByteBuffer, NavigableMap<DecoratedKey, Long>> suffixAt(int position)
            {
                long index = suffixes[position];
                Term term = terms.get((int) (index >>> 32));
                return Pair.create(term.getSuffix(((int) index) - term.position), term.keys);
            }

            @Override
            public ByteBuffer minSuffix()
            {
                return suffixAt(0).left;
            }

            @Override
            public ByteBuffer maxSuffix()
            {
                return suffixAt(suffixes.length - 1).left;
            }

            @Override
            protected Pair<ByteBuffer, TokenTreeBuilder> computeNext()
            {
                while (true)
                {
                    if (current >= suffixes.length)
                    {
                        if (lastProcessedSuffix == null)
                            return endOfData();

                        Pair<ByteBuffer, TokenTreeBuilder> result = finishSuffix();

                        lastProcessedSuffix = null;
                        return result;
                    }

                    Pair<ByteBuffer, NavigableMap<DecoratedKey, Long>> suffix = suffixAt(current++);

                    if (lastProcessedSuffix == null)
                    {
                        lastProcessedSuffix = suffix.left;
                        container = new TokenTreeBuilder(transformKeysMap(suffix.right));
                    }
                    else if (comparator.compare(lastProcessedSuffix, suffix.left) == 0)
                    {
                        lastProcessedSuffix = suffix.left;
                        container.add(transformKeysMap(suffix.right));
                    }
                    else
                    {
                        Pair<ByteBuffer, TokenTreeBuilder> result = finishSuffix();

                        lastProcessedSuffix = suffix.left;
                        container = new TokenTreeBuilder(transformKeysMap(suffix.right));

                        return result;
                    }
                }
            }

            private Pair<ByteBuffer, TokenTreeBuilder> finishSuffix()
            {
                return Pair.create(lastProcessedSuffix, container);
            }
        }
    }

    private static class MutableLevel<T extends InMemorySuffix>
    {
        private final LongArrayList blockOffsets = new LongArrayList();

        private final SequentialWriter out;

        private final MutableBlock<T> inProcessBlock;
        private InMemoryPointerSuffix lastSuffix;

        public MutableLevel(SequentialWriter out, MutableBlock<T> block)
        {
            this.out = out;
            this.inProcessBlock = block;
        }

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

        public void flushMetadata() throws IOException
        {
            out.writeInt(blockOffsets.size());
            for (int i = 0; i < blockOffsets.size(); i++)
                out.writeLong(blockOffsets.get(i));
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

        protected void alignToBlock(SequentialWriter out) throws IOException
        {
            long endOfBlock = out.getFilePointer();
            if ((endOfBlock & (BLOCK_SIZE - 1)) != 0) // align on the block boundary if needed
                out.skipBytes((int) (align(endOfBlock, BLOCK_SIZE) - endOfBlock));
        }
    }

    private static class MutableDataBlock extends MutableBlock<InMemoryDataSuffix>
    {
        private final Mode mode;
        // we need a sorted map based on offsets here because it's required to write KeyContainerBuilder in "offset" order.
        // This map helps to find containers much quicker than iterating over whole set as we'd have to do with list.
        private final BiMap<TokenTreeBuilder, Integer> containers = HashBiMap.create();

        private int offset = 0;

        public MutableDataBlock(Mode mode)
        {
            this.mode = mode;
        }

        @Override
        protected void addInternal(InMemoryDataSuffix suffix) throws IOException
        {
            // checking for the equal keys only makes
            // sense in the mode.SUFFIX case because
            // in that mode words are split into multiple suffixes
            // and suffixes are stored separately this way
            // suffixes share keys from the original word which could be optimized.
            if (mode == Mode.SUFFIX)
            {
                Integer existingOffset = containers.get(suffix.keys);
                if (existingOffset != null)
                {
                    writeSuffix(suffix, existingOffset);
                    return;
                }
            }

            containers.put(suffix.keys, offset);
            writeSuffix(suffix, offset);
            offset += suffix.keys.serializedSize();
        }

        @Override
        protected int sizeAfter(InMemoryDataSuffix element)
        {
            return super.sizeAfter(element) + 4;
        }

        @Override
        public void flushAndClear(SequentialWriter out) throws IOException
        {
            super.flushAndClear(out);

            BiMap<Integer, TokenTreeBuilder> inverse = containers.inverse();

            int pos = 0;
            int[] offsets = new int[inverse.size()];
            for (int offset : inverse.keySet())
                offsets[pos++] = offset;

            Primitive.sort(offsets, new IntComparator()
            {
                @Override
                public int compare(int a, int b)
                {
                    return Integer.compare(a, b);
                }
            });

            for (int off : offsets)
                inverse.get(off).write(out);

            super.alignToBlock(out);
            containers.clear();
            offset = 0;
        }

        private void writeSuffix(InMemorySuffix suffix, int offset) throws IOException
        {
            suffix.serialize(buffer);
            buffer.writeInt(offset);
        }
    }

    // TODO (jwest): move somewhere
    public static long align(long val, int boundary)
    {
        return (val + boundary) & ~(boundary - 1);
    }

    public static abstract class SuffixIterator extends AbstractIterator<Pair<ByteBuffer, TokenTreeBuilder>>
    {
        public abstract ByteBuffer minSuffix();
        public abstract ByteBuffer maxSuffix();

        // TODO (jwest): this makes me sad and probably needs to go away
        protected SortedMap<DecoratedKey, long[]> transformKeysMap(SortedMap<DecoratedKey, Long> keys)
        {
            SortedMap<DecoratedKey, long[]> keyMap = new TreeMap<>();
            for (Map.Entry<DecoratedKey, Long> entry : keys.entrySet())
                keyMap.put(entry.getKey(), new long[]{entry.getValue()});

            return keyMap;
        }
    }
}
