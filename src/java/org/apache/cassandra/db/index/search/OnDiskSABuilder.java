package org.apache.cassandra.db.index.search;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.AbstractIterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferDataOutput;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.PrimitiveIntSort;
import org.roaringbitmap.RoaringBitmap;

import com.google.common.annotations.VisibleForTesting;

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

    public static final int CHUNK_SIZE = 128;

    private final AbstractType<?> comparator;

    private final List<MutableLevel<InMemoryPointerSuffix>> levels = new ArrayList<>();
    private final MutableLevel<InMemoryDataSuffix> dataLevel = new MutableLevel<>();
    private InMemoryDataSuffix lastProcessed;

    private final SA sa;

    public OnDiskSABuilder(AbstractType<?> comparator, Mode mode)
    {
        this.comparator = comparator;
        this.sa = new SA(comparator, mode);
    }

    public OnDiskSABuilder add(ByteBuffer term, RoaringBitmap keys) throws IOException
    {
        sa.add(term, keys);
        return this;
    }

    private void addSuffix(ByteBuffer suffix, RoaringBitmap keys) throws IOException
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
            addSuffix(lastProcessed);
            lastProcessed = current;
        }
    }

    private void addSuffix(InMemoryDataSuffix suffix) throws IOException
    {
        InMemoryPointerSuffix ptr = dataLevel.add(suffix);
        if (ptr == null)
            return;

        int levelIdx = 0;
        for (;;)
        {
            MutableLevel<InMemoryPointerSuffix> mutableLevel = getLevel(levelIdx++);
            if ((ptr = mutableLevel.add(ptr)) == null)
                break;
        }
    }

    public void finish(DataOutput out) throws IOException
    {
        Iterator<Pair<ByteBuffer, RoaringBitmap>> suffixes = sa.finish();
        while (suffixes.hasNext())
        {
            Pair<ByteBuffer, RoaringBitmap> suffix = suffixes.next();
            addSuffix(suffix.left, suffix.right);
        }

        // add the very last processed suffix
        addSuffix(lastProcessed);

        serialize(out);
    }

    private void serialize(DataOutput out) throws IOException
    {
        out.writeInt(levels.size());
        for (int i = levels.size() - 1; i >= 0; i--)
            out.writeInt(levels.get(i).serializedSize());

        for (int i = levels.size() - 1; i >= 0; i--)
            levels.get(i).serialize(out);

        dataLevel.serialize(out);
    }

    @VisibleForTesting
    protected int serializedSize()
    {
        int indexSize = 4 + levels.size() * 4; // number of levels + level positions
        for (MutableLevel l : levels)
            indexSize += l.serializedSize();

        return indexSize + dataLevel.serializedSize();
    }

    private MutableLevel<InMemoryPointerSuffix> getLevel(int idx)
    {
        if (levels.size() == 0)
            levels.add(new MutableLevel<InMemoryPointerSuffix>());

        if (levels.size() - 1 < idx)
        {
            int toAdd = idx - (levels.size() - 1);
            for (int i = 0; i < toAdd; i++)
                levels.add(new MutableLevel<InMemoryPointerSuffix>());
        }

        return levels.get(idx);
    }

    private static class MutableLevel<T extends InMemorySuffix>
    {
        protected final List<MutableBlock<T>> blocks = new ArrayList<>();

        protected MutableBlock<T> inProgress = new MutableBlock<>();
        protected T last;

        public MutableLevel()
        {
            this.blocks.add(inProgress);
        }

        public InMemoryPointerSuffix add(T ptr) throws IOException
        {
            InMemoryPointerSuffix toPromote = null;

            if (!inProgress.hasSpaceFor(ptr))
            {
                toPromote = new InMemoryPointerSuffix(last.suffix, blocks.size() - 1);
                blocks.add((inProgress = new MutableBlock<>()));
            }

            inProgress.add(ptr);

            last = ptr;
            return toPromote;
        }

        public void serialize(DataOutput out) throws IOException
        {
            out.writeInt(blocks.size());

            int position = 0;
            for (MutableBlock b : blocks)
            {
                out.writeInt(position);
                position += b.serializedSize();
            }

            for (MutableBlock b : blocks)
                b.serialize(out);
        }

        public int serializedSize()
        {
            int size = 4;
            for (MutableBlock b : blocks)
                size += 4 + b.serializedSize(); // block position + block serialized size
            return size;
        }
    }

    private static class MutableBlock<T extends InMemorySuffix>
    {
        protected final ByteBufferDataOutput block;
        protected final List<Integer> positions = new ArrayList<>(); // TODO: add HPPC to use primitive collections (or bitmap)

        public MutableBlock()
        {
            block = new ByteBufferDataOutput(ByteBuffer.allocate(CHUNK_SIZE));
        }

        public boolean hasSpaceFor(T element)
        {
            return getWatermark() + element.serializedSize() < CHUNK_SIZE;
        }

        protected void add(T element) throws IOException
        {
            positions.add(block.position());
            element.serialize(block);
        }

        public void serialize(DataOutput out) throws IOException
        {
            out.writeInt(positions.size());
            for (int position : positions)
                out.writeInt(position);

            block.writeFullyTo(out);
        }

        public int serializedSize()
        {
            return 4 + positions.size() * 4 + block.capacity();
        }

        private int getWatermark()
        {
            return 4 + positions.size() * 4 + block.position();
        }
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
        private final int blockIdx;

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

        public SA(AbstractType<?> comparator, Mode mode)
        {
            this.comparator = comparator;
            this.mode = mode;
        }

        public void add(ByteBuffer term, RoaringBitmap keys) throws IOException
        {
            terms.add(new Term(charCount, term, keys));
            charCount += term.remaining();
        }

        protected Iterator<Pair<ByteBuffer, RoaringBitmap>> finish()
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
}
