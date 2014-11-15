package org.apache.cassandra.db.index.search;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.LongArrayList;
import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferDataOutput;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.PrimitiveIntSort;

import org.roaringbitmap.RoaringBitmap;

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

    public static final int INDEX_BLOCK_SIZE = 4096;

    private final AbstractType<?> comparator;

    private final List<MutableLevel<InMemorySuffix>> levels = new ArrayList<>();
    private InMemoryDataSuffix lastProcessed;

    private final SA sa;

    public OnDiskSABuilder(AbstractType<?> comparator, Mode mode)
    {
        this.comparator = comparator;
        this.sa = new SA(comparator, mode);
    }

    public OnDiskSABuilder add(ByteBuffer term, RoaringBitmap keys)
    {
        sa.add(term, keys);
        return this;
    }

    private void addSuffix(ByteBuffer suffix, RoaringBitmap keys, RandomAccessFile out, int blockSize) throws IOException
    {
        InMemoryDataSuffix current = new InMemoryDataSuffix(suffix, keys);

        if (lastProcessed == null)
        {
            lastProcessed = current;
            return;
        }

        if (comparator.compare(lastProcessed.suffix, current.suffix) == 0)
        {
            lastProcessed.merge(current.keys); // deduplicate dataBlocks from different terms
        }
        else
        {
            addSuffix(lastProcessed, out, blockSize);
            lastProcessed = current;
        }
    }

    private void addSuffix(InMemoryDataSuffix suffix, RandomAccessFile out, int blockSize) throws IOException
    {
        InMemorySuffix ptr = getLevel(0, out, blockSize).add(suffix);
        if (ptr == null)
            return;

        int levelIdx = 1;
        for (;;)
        {
            MutableLevel<InMemorySuffix> level = getLevel(levelIdx++, out, INDEX_BLOCK_SIZE);
            if ((ptr = level.add(ptr)) == null)
                break;
        }
    }

    public void finish(File file) throws FSWriteError
    {
        RandomAccessFile out = null;

        try
        {
            out = new RandomAccessFile(file, "rw");

            Iterator<Pair<ByteBuffer, RoaringBitmap>> suffixes = sa.finish();

            // align biggest element size on the 2 default chunks size boundary
            int dataBlockSize = (int) align(sa.maxElementSize, INDEX_BLOCK_SIZE) * 2;
            while (suffixes.hasNext())
            {
                Pair<ByteBuffer, RoaringBitmap> suffix = suffixes.next();
                addSuffix(suffix.left, suffix.right, out, dataBlockSize);
            }

            // add the very last processed suffix
            addSuffix(lastProcessed, out, dataBlockSize);

            for (MutableLevel l : levels)
                l.flush(); // flush all of the buffers

            // and finally write levels index

            final long levelIndexPosition = out.getFilePointer();

            out.writeInt(levels.size() - 1);
            for (int i = levels.size() - 1; i >= 0; i--)
                levels.get(i).flushMetadata();

            out.writeLong(levelIndexPosition);
            out.writeInt(dataBlockSize);
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

    private MutableLevel<InMemorySuffix> getLevel(int idx, RandomAccessFile out, int blockSize)
    {
        if (levels.size() == 0)
            levels.add(new MutableLevel<>(out, blockSize));

        if (levels.size() - 1 < idx)
        {
            int toAdd = idx - (levels.size() - 1);
            for (int i = 0; i < toAdd; i++)
                levels.add(new MutableLevel<>(out, blockSize));
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
            return 4 + suffix.remaining();
        }

        public void serialize(DataOutput out) throws IOException
        {
            ByteBufferUtil.writeWithLength(suffix, out);
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
        private RoaringBitmap keys;

        public InMemoryDataSuffix(ByteBuffer suffix, RoaringBitmap keys)
        {
            super(suffix);
            this.keys = keys;
        }

        public void merge(RoaringBitmap newKeys)
        {
            keys = RoaringBitmap.or(keys, newKeys);
        }

        @Override
        public int serializedSize()
        {
            return super.serializedSize() + keys.serializedSizeInBytes();
        }

        @Override
        public void serialize(DataOutput out) throws IOException
        {
            super.serialize(out);
            keys.serialize(out);
        }
    }

    private static class SA
    {
        private final List<Term> terms = new ArrayList<>();
        private final AbstractType<?> comparator;
        private final Mode mode;

        private int charCount = 0;
        private int maxElementSize = 0;

        public SA(AbstractType<?> comparator, Mode mode)
        {
            this.comparator = comparator;
            this.mode = mode;
        }

        public void add(ByteBuffer term, RoaringBitmap keys)
        {
            terms.add(new Term(charCount, term, keys));

            // to figure out how big chunk should be
            maxElementSize = Math.max(maxElementSize, 4 + term.remaining() + keys.serializedSizeInBytes());
            charCount += term.remaining();
        }

        public Iterator<Pair<ByteBuffer, RoaringBitmap>> finish()
        {
            switch (mode)
            {
                case SUFFIX:
                    return constructSuffixes();

                case ORIGINAL:
                    return constructOriginal();

                default:
                    throw new IllegalArgumentException("unknown mode: " + mode);
            }
        }

        private Iterator<Pair<ByteBuffer, RoaringBitmap>> constructSuffixes()
        {
            final int[] suffixes = new int[charCount];
            for (int i = 0; i < suffixes.length; i++)
                suffixes[i] = i;

            PrimitiveIntSort.qsort(suffixes, new PrimitiveIntSort.CompareInt()
            {
                @Override
                public boolean lessThan(int a, int b)
                {
                    return comparator.compare(getSuffix(terms, a), getSuffix(terms, b)) < 0;
                }
            });

            return new AbstractIterator<Pair<ByteBuffer, RoaringBitmap>>()
            {
                private int current = 0;

                @Override
                protected Pair<ByteBuffer, RoaringBitmap> computeNext()
                {
                    if (current >= suffixes.length)
                        return endOfData();

                    int startsAt = suffixes[current++];
                    Term term = getTerm(terms, startsAt);
                    return Pair.create(getSuffix(term, startsAt), term.keys);
                }
            };
        }

        private Iterator<Pair<ByteBuffer, RoaringBitmap>> constructOriginal()
        {
            Collections.sort(terms, new Comparator<Term>()
            {
                @Override
                public int compare(Term a, Term b)
                {
                    return comparator.compare(a.value, b.value);
                }
            });

            return new AbstractIterator<Pair<ByteBuffer, RoaringBitmap>>()
            {
                private final Iterator<Term> termIterator = terms.iterator();

                @Override
                protected Pair<ByteBuffer, RoaringBitmap> computeNext()
                {
                    if (!termIterator.hasNext())
                        return endOfData();

                    Term term = termIterator.next();
                    return Pair.create(term.value, term.keys);
                }
            };
        }

        private ByteBuffer getSuffix(List<Term> values, int pos)
        {
            return getSuffix(getTerm(values, pos), pos);
        }

        private ByteBuffer getSuffix(Term t, int pos)
        {
            return t.getSuffix(pos - t.position);
        }

        private Term getTerm(List<Term> values, int idx)
        {
            int start = 0;
            int end = values.size() - 1;

            int currentIndex = 0;
            while (start <= end)
            {
                int mid = start + (end - start) / 2;
                int stopIndex = values.get(mid).position;
                if (stopIndex <= idx)
                {
                    currentIndex = mid;
                    start = mid + 1;
                }
                else
                {
                    end = mid - 1;
                }
            }

            return values.get(currentIndex);
        }
    }

    private static class MutableLevel<T extends InMemorySuffix>
    {
        private final LongArrayList blockOffsets = new LongArrayList();

        private final RandomAccessFile out;

        private final MutableBlock inProcessBlock;
        private InMemoryPointerSuffix lastSuffix;

        public MutableLevel(RandomAccessFile out, int blockSize)
        {
            this.out = out;
            this.inProcessBlock = new MutableBlock(blockSize);
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

    private static class MutableBlock
    {
        private final ByteBufferDataOutput buffer;
        private final IntArrayList offsets = new IntArrayList();

        protected MutableBlock(int blockSize)
        {
            buffer = new ByteBufferDataOutput(ByteBuffer.allocate(blockSize));
        }

        private void add(InMemorySuffix suffix) throws IOException
        {
            offsets.add(buffer.position());
            suffix.serialize(buffer);
        }

        public boolean hasSpaceFor(InMemorySuffix element)
        {
            return getWatermark() + 4 + element.serializedSize() < buffer.capacity();
        }

        private int getWatermark()
        {
            return 4 + offsets.size() * 4 + buffer.position();
        }

        public void flushAndClear(RandomAccessFile out) throws IOException
        {
            out.writeInt(offsets.size());
            for (int i = 0; i < offsets.size(); i++)
                out.writeInt(offsets.get(i));

            buffer.writeFullyTo(out);

            long endOfBlock = out.getFilePointer();
            if ((endOfBlock & (buffer.capacity() - 1)) != 0) // align on the block boundary if needed
                out.seek(align(endOfBlock, buffer.capacity()));

            out.getFD().sync(); // now fsync whole block

            offsets.clear();
            buffer.clear();
        }
    }

    private static long align(long val, int boundary)
    {
        return (val + boundary) & ~(boundary - 1);
    }
}
