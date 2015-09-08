/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Objects;

import com.google.common.primitives.Ints;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Binary format of {@code RowIndexEntry} is defined as follows:
 * {@code
 * (long) position
 *  (int) serialized size of data that follows
 *  (int) DeletionTime.localDeletionTime
 * (long) DeletionTime.markedForDeletionAt
 *  (int) number of IndexInfo objects
 *    (*) serialized IndexInfo objects, see below
 *    (*) offsets of serialized IndexInfo objects, since version "ma" (3.0)
 * }
 * <p>
 * Each {@link IndexInfo} object is serialized as follows:
 * {@code
 *    (*) IndexInfo.firstName (ClusteringPrefix serializer, either Clustering.serializer.serialize or Slice.Bound.serializer.serialize)
 *    (*) IndexInfo.lastName (ClusteringPrefix serializer)
 * (long) IndexInfo.offset
 * (long) IndexInfo.width
 * (bool) IndexInfo.endOpenMarker != null              (if version.storeRows)
 *  (int) IndexInfo.endOpenMarker.localDeletionTime    (if version.storeRows && IndexInfo.endOpenMarker != null)
 * (long) IndexInfo.endOpenMarker.markedForDeletionAt  (if version.storeRows && IndexInfo.endOpenMarker != null)
 * }
 * </p>
 */
public class RowIndexEntry
{
    private final long position;

    public RowIndexEntry(long position)
    {
        this.position = position;
    }

    public long getPosition()
    {
        return position;
    }

    public int promotedSize(CFMetaData metadata, Version version)
    {
        return 0;
    }

    /**
     * @return true if this index entry contains the row-level tombstone and column summary.  Otherwise,
     * caller should fetch these from the row header.
     */
    public boolean isIndexed()
    {
        return false;
    }

    public int indexCount()
    {
        return 0;
    }

    public IndexInfo indexInfo(int index)
    {
        throw new IllegalStateException();
    }

    public DeletionTime deletionTime()
    {
        throw new UnsupportedOperationException();
    }

    public long blockOffset(int blockIdx)
    {
        throw new UnsupportedOperationException();
    }

    public long blockWidth(int blockIdx)
    {
        throw new UnsupportedOperationException();
    }

    public DeletionTime endOpenMarker(int blockIdx)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * The index of the IndexInfo in which a scan starting with @name should begin.
     *
     * @param name name to search for
     * @param comparator the comparator to use
     * @param reversed whether or not the search is reversed, i.e. we scan forward or backward from name
     * @param lastIndex where to start the search from in indexList
     *
     * @return int index
     */
    public int indexOf(ClusteringPrefix name, ClusteringComparator comparator, boolean reversed, int lastIndex)
    {
        /*
        Take the example from the unit test, and say your index looks like this:
        [0..5][10..15][20..25]
        and you look for the slice [13..17].

        When doing forward slice, we are doing a binary search comparing 13 (the start of the query)
        to the lastName part of the index slot. You'll end up with the "first" slot, going from left to right,
        that may contain the start.

        When doing a reverse slice, we do the same thing, only using as a start column the end of the query,
        i.e. 17 in this example, compared to the firstName part of the index slots.  bsearch will give us the
        first slot where firstName > start ([20..25] here), so we subtract an extra one to get the slot just before.
        */
        int size = indexCount();
        int startIdx = 0;
        int endIdx = size;
        if (reversed)
        {
            if (lastIndex < size - 1)
            {
                endIdx = lastIndex + 1;
            }
        }
        else
        {
            if (lastIndex > 0)
            {
                startIdx = lastIndex;
            }
        }
        int index = binarySearch(name, comparator.indexComparator(reversed), reversed, startIdx, endIdx);
        return index < 0 ? -index - (reversed ? 2 : 1) : index;
    }

    int binarySearch(ClusteringPrefix name, Comparator<IndexInfo> c,
                     boolean reversed, int fromIndex, int toIndex) {
        return -1;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RowIndexEntry that = (RowIndexEntry) o;
        return position == that.position;
    }

    public int hashCode()
    {
        return Objects.hash(position);
    }

    public static RowIndexEntry buildIndex(UnfilteredRowIterator iterator, SequentialWriter writer,
                                           SerializationHeader header, Version version,
                                           CFMetaData metadata, long position, DeletionTime deletionTime)
    {
        assert !iterator.isEmpty() && version.storeRows() && deletionTime != null;

        Builder builder = new Builder(iterator, writer, metadata, header, version, deletionTime);
        try
        {
            return builder.build(position);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public interface IndexSerializer
    {
        void serialize(RowIndexEntry rie, DataOutputPlus out) throws IOException;
        RowIndexEntry deserialize(DataInputPlus in) throws IOException;
        int serializedSize(RowIndexEntry rie);
    }

    public static class Serializer implements IndexSerializer
    {
        private final CFMetaData metadata;
        private final Version version;

        public Serializer(CFMetaData metadata, Version version)
        {
            this.metadata = metadata;
            this.version = version;
        }

        public void serialize(RowIndexEntry rie, DataOutputPlus out) throws IOException
        {
            assert version.storeRows() : "We read old index files but we should never write them";

            out.writeUnsignedVInt(rie.getPosition());

            if (rie.isIndexed())
            {
                IndexedEntry indexed = (IndexedEntry) rie;
                if (indexed.version.equals(version))
                {
                    out.writeUnsignedVInt(indexed.buffer.limit());
                    indexed.buffer.position(0);
                    out.write(indexed.buffer);
                }
                else
                {
                    // manual serialization of older version to latest version

                    out.writeUnsignedVInt(rie.promotedSize(metadata, version));

                    int indexCount = rie.indexCount();
                    DeletionTime.serializer.serialize(rie.deletionTime(), out);
                    out.writeInt(indexCount);
                    int offset = 16;

                    int[] offsets = new int[indexCount];

                    IndexInfo.Serializer idxSerializer = metadata.serializers().indexSerializer(version);
                    for (int i = 0; i < indexCount; i++)
                    {
                        IndexInfo ii = rie.indexInfo(i);
                        offsets[i] = offset;
                        idxSerializer.serialize(ii, out, metadata.clusteringTypes());
                        offset += idxSerializer.serializedSize(ii, metadata.clusteringTypes());
                    }

                    for (int off : offsets)
                        out.writeInt(off);
                }
            }
            else
                out.writeUnsignedVInt(0);
        }

        public int serializedSize(RowIndexEntry rie)
        {
            int promotedSize = rie.promotedSize(metadata, version);

            return version.storeRows()
                   ? TypeSizes.sizeofUnsignedVInt(rie.getPosition()) + TypeSizes.sizeofUnsignedVInt(promotedSize) + promotedSize
                   : TypeSizes.sizeof(rie.getPosition()) + TypeSizes.sizeof(promotedSize) + promotedSize;
        }

        public RowIndexEntry deserialize(DataInputPlus in) throws IOException
        {
            long position;
            int size;

            if (version.storeRows())
            {
                position = in.readUnsignedVInt();
                size = (int) in.readUnsignedVInt();
            }
            else
            {
                position = in.readLong();
                size = in.readInt();
            }

            if (size > 0)
            {
                ByteBuffer buffer = ByteBufferUtil.read(in, size);
                return new IndexedEntry(position, buffer, metadata, version);
            }
            else
            {
                return new RowIndexEntry(position);
            }
        }

        public static long readPosition(DataInputPlus in, Version version) throws IOException
        {
            return version.storeRows()
                   ? in.readUnsignedVInt()
                   : in.readLong();
        }

        public static void skip(DataInputPlus in, Version version) throws IOException
        {
            readPosition(in, version);
            skipPromotedIndex(in, version);
        }

        public static void skipPromotedIndex(DataInputPlus in, Version version) throws IOException
        {
            int size = version.storeRows()
                       ? (int)in.readUnsignedVInt()
                       : in.readInt();
            if (size <= 0)
                return;

            FileUtils.skipBytesFully(in, size);
        }
    }

    /**
     * An entry in the row index for a row whose columns are indexed.
     */
    private static class IndexedEntry extends RowIndexEntry
    {
        // binary representation of serialized RowIndexEntry.IndexedEntry
        private final ByteBuffer buffer;
        // buffer containing IndexInfo offsets - for pre-3.0 indexes this is a separate buffer, as pre-3.0
        // indexes do not contain offsets to IndexInfo objects
        private final ByteBuffer offsets;
        // offset to offsets of IndexInfo objects in this.offsets
        private final int offsetsOffset;
        private final CFMetaData metadata;
        private final IndexInfo.Serializer serializer;
        private final Version version;
        private IndexInfo lastIndexInfo;
        private int lastIndexInfoIndex = -1;

        IndexedEntry(long position, ByteBuffer buffer, CFMetaData metadata, Version version)
        {
            super(position);
            this.buffer = buffer;

            if (version.storeRows())
            {
                this.offsetsOffset = buffer.limit() - indexCount() * 4;
                this.offsets = buffer;
            }
            else
            {
                this.offsetsOffset = 0;
                this.offsets = ByteBuffer.allocate(indexCount() * 4);
            }

            this.metadata = metadata;
            this.version = version;
            this.serializer = metadata.serializers().indexSerializer(version);

        }

        @Override
        public DeletionTime deletionTime()
        {
            return DeletionTime.serializer.deserialize(buffer, 0);
        }

        @Override
        public boolean isIndexed()
        {
            return true;
        }

        @Override
        public int indexCount()
        {
            int offset = (int) DeletionTime.serializer.serializedSize(null);
            return buffer.getInt(offset);
        }

        public long blockOffset(int blockIdx)
        {
            if (lastIndexInfoIndex == blockIdx)
                return getPosition() + lastIndexInfo.getOffset();

            ByteBuffer buf = buffer.duplicate();
            try (DataInputBuffer input = new DataInputBuffer(buf, false))
            {
                indexInfo(blockIdx, false, buf, input);
                return getPosition() + serializer.readOffset(input, metadata.clusteringTypes());
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public long blockWidth(int blockIdx)
        {
            if (lastIndexInfoIndex == blockIdx)
                return lastIndexInfo.getWidth();

            ByteBuffer buf = buffer.duplicate();
            try (DataInputBuffer input = new DataInputBuffer(buf, false))
            {
                indexInfo(blockIdx, false, buf, input);
                return serializer.readWidth(input, metadata.clusteringTypes());
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public DeletionTime endOpenMarker(int blockIdx)
        {
            if (lastIndexInfoIndex == blockIdx)
                return lastIndexInfo.getEndOpenMarker();

            ByteBuffer buf = buffer.duplicate();
            try (DataInputBuffer input = new DataInputBuffer(buf, false))
            {
                indexInfo(blockIdx, false, buf, input);
                return serializer.readEndOpenMarker(input, metadata.clusteringTypes());
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public IndexInfo indexInfo(int index)
        {
            return indexInfo(index, true);
        }

        private IndexInfo indexInfo(int index, boolean deserialize)
        {
            // This method is called often with the same index argument.
            // (see org.apache.cassandra.db.columniterator.AbstractSSTableIterator.IndexState.currentIndex())
            if (lastIndexInfoIndex == index)
                return lastIndexInfo;

            ByteBuffer buf = buffer.duplicate();
            try (DataInputBuffer input = new DataInputBuffer(buf, false))
            {
                return indexInfo(index, deserialize, buf, input);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        private IndexInfo indexInfo(int index, boolean deserialize, ByteBuffer buf, DataInputBuffer input) throws IOException
        {
            int offset = indexInfoOffset(index);
            if (offset > 0)
            {
                // We already know the offset of the requested IndexInfo, so just "seek" and deserialize.
                // This is the only possible code path for 3.0 sstable format "ma".
                buf.position(offset);
            }
            else
            {
                // We do not know the index of the requested IndexInfo object - i.e. it is necessary
                // to scan the serialized index. Discovered IndexInfo offsets will be cached.
                // This is the only possible code path for pre-3.0 sstable formats.

                int i = 0;
                // skip already discovered offsets
                while (i < index && indexInfoOffset(i) != 0)
                    i++;

                // "seek" to last known IndexInfo
                if (i == 0)
                    offset = firstIndexInfoOffset();
                else
                {
                    i--;
                    offset = indexInfoOffset(i);
                }
                buf.position(offset);

                // need to read through all IndexInfo objects until we reach the requested one
                for (; ; i++)
                {
                    // save IndexInfo offset
                    indexInfoOffset(i, buf.position());

                    if (i == index)
                        break;

                    serializer.skip(input, metadata.clusteringTypes());
                }
            }

            if (!deserialize)
                // used by serialize() methods via ensureIndexInfoOffsets() to calculate all IndexInfo offsets
                return null;

            IndexInfo info = serializer.deserialize(input, metadata.clusteringTypes());

            if (!hasIndexInfoOffsets() && indexCount() > index + 1)
            {
                // We know the offset of the next IndexInfo - so store it. (for pre-3.0 sstables)
                indexInfoOffset(index + 1, buf.position());
            }

            lastIndexInfoIndex = index;
            lastIndexInfo = info;
            return info;
        }

        private boolean hasIndexInfoOffsets()
        {
            return buffer == offsets;
        }

        private static int firstIndexInfoOffset()
        {
            return (int) (TypeSizes.sizeof(0)/*columnsCount*/ + DeletionTime.serializer.serializedSize(null));
        }

        private int indexInfoOffset(int index)
        {
            return offsets.getInt(offsetsOffset + index * 4);
        }

        private void indexInfoOffset(int indexIdx, int offset)
        {
            offsets.putInt(offsetsOffset + indexIdx * 4, offset);
        }

        @Override
        public int promotedSize(CFMetaData metadata, Version version)
        {
            if (version.equals(this.version))
                return Ints.checkedCast(buffer.limit());

            int size = 4;
            size += DeletionTime.serializer.serializedSize(null);
            size += 4;

            int indexCount = indexCount();
            IndexInfo.Serializer idxSerializer = metadata.serializers().indexSerializer(version);
            for (int i = 0; i < indexCount; i++)
            {
                IndexInfo ii = indexInfo(i);
                size += idxSerializer.serializedSize(ii, metadata.clusteringTypes());
            }

            size += 4 * indexCount;

            return size;
        }

        int binarySearch(ClusteringPrefix name, Comparator<IndexInfo> c,
                         boolean reversed, int fromIndex, int toIndex) {
            int low = fromIndex;
            int high = toIndex - 1;

            ByteBuffer buf = buffer.duplicate();
            try (DataInputBuffer input = new DataInputBuffer(buf, false))
            {
                while (low <= high) {
                    int mid = (low + high) >>> 1;

                    // Do the comparation previously done via:
                    //                IndexInfo midVal = indexInfo(mid);
                    //                int cmp = c.compare(midVal, key);
                    //
                    // "seek" to start of serialized IndexInfo
                    indexInfo(mid, false, buf, input);
                    if (!reversed)
                        // skip IndexInfo.firstName
                        ClusteringPrefix.serializer.skip(input, version.correspondingMessagingVersion(), metadata.clusteringTypes());
                    ClusteringPrefix c2 = ClusteringPrefix.serializer.deserialize(input, version.correspondingMessagingVersion(), metadata.clusteringTypes());
                    int cmp = -metadata.comparator.compare(name, c2);

                    if (cmp < 0)
                        low = mid + 1;
                    else if (cmp > 0)
                        high = mid - 1;
                    else
                        return mid; // key found
                }
                return -(low + 1);  // key not found
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            IndexedEntry that = (IndexedEntry) o;
            return buffer.equals(that.buffer);
        }

        public int hashCode()
        {
            return Objects.hash(super.hashCode(), buffer);
        }
    }

    /**
     * Help to create an index for a column family based on size of columns,
     * and write said columns to disk.
     */
    private static class Builder
    {
        private final UnfilteredRowIterator iterator;
        private final SequentialWriter writer;
        private final CFMetaData metadata;
        private final SerializationHeader header;
        private final Version version;

        private final long initialPosition;
        private long startPosition = -1;

        private int written;

        private ClusteringPrefix firstClustering;
        private ClusteringPrefix lastClustering;

        private DeletionTime openMarker;

        private DataOutputBuffer buffer;
        private IndexInfo firstIndexInfo;
        private DataOutputBuffer offsetsBuffer;
        private final DeletionTime deletionTime;
        private int indexCount;

        Builder(UnfilteredRowIterator iterator,
                SequentialWriter writer,
                CFMetaData metadata, SerializationHeader header,
                Version version, DeletionTime deletionTime)
        {
            this.iterator = iterator;
            this.writer = writer;
            this.metadata = metadata;
            this.header = header;
            this.version = version;
            this.deletionTime = deletionTime;
            this.initialPosition = writer.getFilePointer();
        }

        private void writePartitionHeader(UnfilteredRowIterator iterator) throws IOException
        {
            ByteBufferUtil.writeWithShortLength(iterator.partitionKey().getKey(), writer);
            DeletionTime.serializer.serialize(iterator.partitionLevelDeletion(), writer);
            if (header.hasStatic())
                UnfilteredSerializer.serializer.serialize(iterator.staticRow(), header, writer, version.correspondingMessagingVersion());
        }

        RowIndexEntry build(long position) throws IOException
        {
            writePartitionHeader(iterator);

            while (iterator.hasNext())
                add(iterator.next());

            UnfilteredSerializer.serializer.writeEndOfPartition(writer);

            // It's possible we add no rows, just a top level deletion
            if (written == 0)
                return new RowIndexEntry(position);

            // the last column may have fallen on an index boundary already.  if not, index it explicitly.
            if (firstClustering != null)
                addIndexBlock();

            // we only consider the columns summary when determining whether to create an IndexedEntry,
            // since if there are insufficient columns to be worth indexing we're going to seek to
            // the beginning of the row anyway, so we might as well read the tombstone there as well.
            if (this.buffer != null)
            {
                ByteBuffer buf = offsetsBuffer.buffer();
                buffer.write(buf);

                buf = buffer.buffer();
                buf.putInt((int) DeletionTime.serializer.serializedSize(null), indexCount);
                return new IndexedEntry(position, buf, metadata, version);
            }
            else
                return new RowIndexEntry(position);
        }

        private long currentPosition()
        {
            return writer.getFilePointer() - initialPosition;
        }

        private void addIndexBlock() throws IOException
        {
            IndexInfo cIndexInfo = new IndexInfo(firstClustering,
                                                 lastClustering,
                                                 startPosition,
                                                 currentPosition() - startPosition,
                                                 openMarker);
            if (buffer == null)
            {
                if (firstIndexInfo != null)
                {
                    this.buffer = new DataOutputBuffer(4096);
                    this.offsetsBuffer = new DataOutputBuffer(1024);

                    DeletionTime.serializer.serialize(deletionTime, buffer);
                    buffer.writeInt(0); // placeholder for number of IndexInfo objects

                    addIndexInfo(firstIndexInfo);

                    firstIndexInfo = null;
                }
                else
                    firstIndexInfo = cIndexInfo;
            }
            if (buffer != null)
            {
                addIndexInfo(cIndexInfo);
            }

            firstClustering = null;
        }

        private void addIndexInfo(IndexInfo cIndexInfo) throws IOException
        {
            offsetsBuffer.writeInt(buffer.getLength());
            metadata.serializers().latestVersionIndexSerializer.serialize(cIndexInfo, buffer, metadata.clusteringTypes());
            indexCount++;
        }

        private void add(Unfiltered unfiltered) throws IOException
        {
            if (firstClustering == null)
            {
                // Beginning of an index block. Remember the start and position
                firstClustering = unfiltered.clustering();
                startPosition = currentPosition();
            }

            UnfilteredSerializer.serializer.serialize(unfiltered, header, writer, version.correspondingMessagingVersion());
            lastClustering = unfiltered.clustering();
            ++written;

            if (unfiltered.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
            {
                RangeTombstoneMarker marker = (RangeTombstoneMarker)unfiltered;
                openMarker = marker.isOpen(false) ? marker.openDeletionTime(false) : null;
            }

            // if we hit the column index size that we have to index after, go ahead and index it.
            if (currentPosition() - startPosition >= DatabaseDescriptor.getColumnIndexSize())
                addIndexBlock();
        }
    }
}
