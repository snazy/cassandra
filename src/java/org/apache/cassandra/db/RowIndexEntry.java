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

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.primitives.Ints;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.sstable.IndexInfo;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

public class RowIndexEntry implements IMeasurableMemory
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new RowIndexEntry(0));

    public final long position;

    public RowIndexEntry(long position)
    {
        this.position = position;
    }

    public int promotedSize(Version version, SerializationHeader header)
    {
        return 0;
    }

    public static RowIndexEntry buildIndex(long position, DeletionTime deletionTime,
                                           UnfilteredRowIterator iterator, SequentialWriter output,
                                           SerializationHeader header, Version version) throws IOException
    {
        assert !iterator.isEmpty() && version.storeRows();
        assert deletionTime != null;

        Builder builder = new Builder(position, deletionTime,
                                      iterator, output, header, version.correspondingMessagingVersion());
        return builder.build();
    }

    /**
     * @return true if this index entry contains the row-level tombstone and column summary.  Otherwise,
     * caller should fetch these from the row header.
     */
    public boolean isIndexed()
    {
        return columnsCount() > 0;
    }

    public DeletionTime deletionTime()
    {
        throw new UnsupportedOperationException();
    }

    public int columnsCount()
    {
        return 0;
    }

    public IndexInfo indexInfo(int indexIdx)
    {
        throw new IndexOutOfBoundsException();
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
        IndexInfo target = new IndexInfo(name, name, 0, 0, null);
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
        int size = columnsCount();
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
        int index = binarySearch(target, comparator.indexComparator(reversed), startIdx, endIdx);
        return index < 0 ? -index - (reversed ? 2 : 1) : index;
    }

    private int binarySearch(IndexInfo key, Comparator<IndexInfo> c,
                             int fromIndex, int toIndex) {
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            IndexInfo midVal = indexInfo(mid);
            int cmp = c.compare(midVal, key);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1);  // key not found
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE;
    }

    public static class Serializer
    {
        private final Version version;
        private final SerializationHeader header;

        public Serializer(Version version, SerializationHeader header)
        {
            this.version = version;
            this.header = header;
        }

        public void serialize(RowIndexEntry rie, DataOutputPlus out) throws IOException
        {
            out.writeLong(rie.position);
            out.writeInt(rie.promotedSize(version, header));

            if (rie.isIndexed())
            {
                DeletionTime.serializer.serialize(rie.deletionTime(), out);
                out.writeInt(rie.columnsCount());
                IndexInfo.Serializer idxSerializer = IndexInfo.indexSerializer(version);
                int sz = rie.columnsCount();
                for (int i = 0; i < sz; i++)
                    idxSerializer.serialize(rie.indexInfo(i), out, header);
            }
        }

        public RowIndexEntry deserialize(DataInputPlus in) throws IOException
        {
            long position = in.readLong();

            int size = in.readInt();
            if (size > 0)
            {
                DeletionTime deletionTime = DeletionTime.serializer.deserialize(in);

                int entries = in.readInt();
                IndexInfo.Serializer idxSerializer = IndexInfo.indexSerializer(version);
                List<IndexInfo> columnsIndex;
                if (entries == 1)
                    columnsIndex = Collections.singletonList(idxSerializer.deserialize(in, header));
                else
                {
                    columnsIndex = new ArrayList<>(entries);
                    for (int i = 0; i < entries; i++)
                        columnsIndex.add(idxSerializer.deserialize(in, header));
                }

                return new IndexedEntry(position, deletionTime, columnsIndex);
            }
            else
            {
                return new RowIndexEntry(position);
            }
        }

        public static void skip(DataInput in) throws IOException
        {
            in.readLong();
            skipPromotedIndex(in);
        }

        public static void skipPromotedIndex(DataInput in) throws IOException
        {
            int size = in.readInt();
            if (size <= 0)
                return;

            FileUtils.skipBytesFully(in, size);
        }

        public int serializedSize(RowIndexEntry rie)
        {
            int size = TypeSizes.sizeof(rie.position) + TypeSizes.sizeof(rie.promotedSize(version, header));

            if (rie.isIndexed())
            {
                size += DeletionTime.serializer.serializedSize(rie.deletionTime());
                size += TypeSizes.sizeof(rie.columnsCount());

                IndexInfo.Serializer idxSerializer = IndexInfo.indexSerializer(version);
                int sz = rie.columnsCount();
                for (int i = 0; i < sz; i++)
                    size += idxSerializer.serializedSize(rie.indexInfo(i), header);
            }

            return size;
        }
    }

    /**
     * An entry in the row index for a row whose columns are indexed.
     */
    private static class IndexedEntry extends RowIndexEntry
    {
        private final DeletionTime deletionTime;
        private final List<IndexInfo> columnsIndex;
        private static final long BASE_SIZE =
                ObjectSizes.measure(new IndexedEntry(0, DeletionTime.LIVE, Arrays.<IndexInfo>asList(null, null)))
              + ObjectSizes.measure(new ArrayList<>(1));

        private IndexedEntry(long position, DeletionTime deletionTime, List<IndexInfo> columnsIndex)
        {
            super(position);
            assert deletionTime != null;
            assert columnsIndex != null && columnsIndex.size() > 1;
            this.deletionTime = deletionTime;
            this.columnsIndex = columnsIndex;
        }

        @Override
        public DeletionTime deletionTime()
        {
            return deletionTime;
        }

        @Override
        public int columnsCount()
        {
            return columnsIndex.size();
        }

        public IndexInfo indexInfo(int indexIdx)
        {
            return columnsIndex.get(indexIdx);
        }

        @Override
        public int promotedSize(Version version, SerializationHeader header)
        {
            long size = DeletionTime.serializer.serializedSize(deletionTime);
            size += TypeSizes.sizeof(columnsIndex.size()); // number of entries
            IndexInfo.Serializer idxSerializer = IndexInfo.indexSerializer(version);
            for (IndexInfo info : columnsIndex)
                size += idxSerializer.serializedSize(info, header);

            return Ints.checkedCast(size);
        }

        @Override
        public long unsharedHeapSize()
        {
            long entrySize = 0;
            for (IndexInfo idx : columnsIndex)
                entrySize += idx.unsharedHeapSize();

            return BASE_SIZE
                   + entrySize
                   + deletionTime.unsharedHeapSize()
                   + ObjectSizes.sizeOfReferenceArray(columnsIndex.size());
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
        private final SerializationHeader header;
        private final int version;

        private final long initialPosition;
        private long startPosition = -1;

        private int written;

        private ClusteringPrefix firstClustering;
        private ClusteringPrefix lastClustering;

        private DeletionTime openMarker;

        private final long position;
        private final DeletionTime deletionTime;
        private final List<IndexInfo> columnsIndex = new ArrayList<>();

        Builder(long position,
                DeletionTime deletionTime,
                UnfilteredRowIterator iterator,
                SequentialWriter writer,
                SerializationHeader header,
                int version)
        {
            this.position = position;
            this.deletionTime = deletionTime;

            this.iterator = iterator;
            this.writer = writer;
            this.header = header;
            this.version = version;

            this.initialPosition = writer.getFilePointer();
        }

        private void writePartitionHeader(UnfilteredRowIterator iterator) throws IOException
        {
            ByteBufferUtil.writeWithShortLength(iterator.partitionKey().getKey(), writer);
            DeletionTime.serializer.serialize(iterator.partitionLevelDeletion(), writer);
            if (header.hasStatic())
                UnfilteredSerializer.serializer.serialize(iterator.staticRow(), header, writer, version);
        }

        public RowIndexEntry build() throws IOException
        {
            writePartitionHeader(iterator);

            while (iterator.hasNext())
                add(iterator.next());

            return close();
        }

        private long currentPosition()
        {
            return writer.getFilePointer() - initialPosition;
        }

        private void addIndexBlock()
        {
            IndexInfo cIndexInfo = new IndexInfo(firstClustering,
                                                 lastClustering,
                                                 startPosition,
                                                 currentPosition() - startPosition,
                                                 openMarker);
            columnsIndex.add(cIndexInfo);
            firstClustering = null;
        }

        private void add(Unfiltered unfiltered) throws IOException
        {
            if (firstClustering == null)
            {
                // Beginning of an index block. Remember the start and position
                firstClustering = unfiltered.clustering();
                startPosition = currentPosition();
            }

            UnfilteredSerializer.serializer.serialize(unfiltered, header, writer, version);
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

        private RowIndexEntry close() throws IOException
        {
            UnfilteredSerializer.serializer.writeEndOfPartition(writer);

            // It's possible we add no rows, just a top level deletion
            if (written == 0)
                return new RowIndexEntry(position);

            // the last column may have fallen on an index boundary already.  if not, index it explicitly.
            if (firstClustering != null)
                addIndexBlock();

            assert !columnsIndex.isEmpty();

            // we only consider the columns summary when determining whether to create an IndexedEntry,
            // since if there are insufficient columns to be worth indexing we're going to seek to
            // the beginning of the row anyway, so we might as well read the tombstone there as well.
            if (columnsIndex.size() > 1)
                return new IndexedEntry(position, deletionTime, columnsIndex);
            else
                return new RowIndexEntry(position);
        }
    }
}
