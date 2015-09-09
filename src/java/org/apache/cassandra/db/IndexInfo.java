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
import java.util.Objects;

import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * {@code IndexInfo} is embedded in the indexed version of {@link RowIndexEntry}.
 * Each instance roughly covers a range of {@link org.apache.cassandra.config.Config#column_index_size_in_kb column_index_size_in_kb} kB
 * and contains the first and last clustering value (or slice bound), its offset in the data file and width in the data file.
 * <p>
 * Each {@code IndexInfo} object is serialized as follows.
 * </p>
 * <p>
 * Serialization format changed in 3.0. First, the {@code endOpenMarker} has been introduced.
 * Second, the <i>order</i> of the fields in serialized representation changed to allow future
 * optimizations to access {@code offset} and {@code width} fields directly without skipping
 * {@code firstName}/{@code lastName}.
 * </p>
 * <p>
 * 3.0 and newer:<br/>
 * {@code
 * (long) IndexInfo.offset
 * (long) IndexInfo.width
 * (bool) IndexInfo.endOpenMarker != null
 *  (int) IndexInfo.endOpenMarker.localDeletionTime    (if IndexInfo.endOpenMarker != null)
 * (long) IndexInfo.endOpenMarker.markedForDeletionAt  (if IndexInfo.endOpenMarker != null)
 *    (*) IndexInfo.lastName (ClusteringPrefix serializer, either Clustering.serializer.serialize or Slice.Bound.serializer.serialize)
 *    (*) IndexInfo.firstName (ClusteringPrefix serializer, either Clustering.serializer.serialize or Slice.Bound.serializer.serialize)
 * }
 * </p>
 * <p>
 * Pre 3.0:<br/>
 * {@code
 *    (*) IndexInfo.firstName (ClusteringPrefix serializer, either Clustering.serializer.serialize or Slice.Bound.serializer.serialize)
 *    (*) IndexInfo.lastName (ClusteringPrefix serializer, either Clustering.serializer.serialize or Slice.Bound.serializer.serialize)
 * (long) IndexInfo.offset
 * (long) IndexInfo.width
 * }
 * </p>
 */
public final class IndexInfo
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new IndexInfo(null, null, 0, 0, null));

    private final long width;
    private final ClusteringPrefix lastName;
    private final ClusteringPrefix firstName;
    private final long offset;

    // If at the end of the index block there is an open range tombstone marker, this marker
    // deletion infos. null otherwise.
    private final DeletionTime endOpenMarker;

    public IndexInfo(ClusteringPrefix firstName,
                     ClusteringPrefix lastName,
                     long offset,
                     long width,
                     DeletionTime endOpenMarker)
    {
        this.firstName = firstName;
        this.lastName = lastName;
        this.offset = offset;
        this.width = width;
        this.endOpenMarker = endOpenMarker;
    }

    public long getWidth()
    {
        return width;
    }

    public ClusteringPrefix getLastName()
    {
        return lastName;
    }

    public ClusteringPrefix getFirstName()
    {
        return firstName;
    }

    public long getOffset()
    {
        return offset;
    }

    public DeletionTime getEndOpenMarker()
    {
        return endOpenMarker;
    }

    public static final class Serializer
    {
        private final boolean legacy;

        public Serializer(boolean legacy)
        {
            this.legacy = legacy;
        }

        public void serialize(IndexInfo info, DataOutputPlus out, ISerializer<ClusteringPrefix> clusteringSerializer) throws IOException
        {
            if (!legacy)
            {
                out.writeLong(info.offset);
                out.writeLong(info.width);

                DeletionTime eom = info.endOpenMarker;
                out.writeBoolean(eom != null);
                if (eom != null)
                    DeletionTime.serializer.serialize(eom, out);

                clusteringSerializer.serialize(info.lastName, out);
                clusteringSerializer.serialize(info.firstName, out);
            }
            else
            {
                clusteringSerializer.serialize(info.firstName, out);
                clusteringSerializer.serialize(info.lastName, out);
                out.writeLong(info.offset);
                out.writeLong(info.width);
            }
        }

        public long readOffset(DataInputBuffer in, ISerializer<ClusteringPrefix> clusteringSerializer) throws IOException
        {
            if (legacy)
            {
                skipNames(in, clusteringSerializer);
            }
            return in.readLong();
        }

        public long readWidth(DataInputBuffer in, ISerializer<ClusteringPrefix> clusteringSerializer) throws IOException
        {
            if (legacy)
            {
                skipNames(in, clusteringSerializer);
            }
            in.skipBytes(8);
            return in.readLong();
        }

        public DeletionTime readEndOpenMarker(DataInputBuffer in, ISerializer<ClusteringPrefix> clusteringSerializer) throws IOException
        {
            if (!legacy)
            {
                in.skipBytes(8 + 8);

                return in.readBoolean()
                       ? DeletionTime.serializer.deserialize(in)
                       : null;
            }
            return null;
        }

        public IndexInfo deserialize(DataInputPlus in, ISerializer<ClusteringPrefix> clusteringSerializer) throws IOException
        {
            long offset;
            long width;
            ClusteringPrefix firstName;
            ClusteringPrefix lastName;
            DeletionTime endOpenMarker;
            if (!legacy)
            {
                offset = in.readLong();
                width = in.readLong();
                endOpenMarker = in.readBoolean()
                                ? DeletionTime.serializer.deserialize(in)
                                : null;
                lastName = clusteringSerializer.deserialize(in);
                firstName = clusteringSerializer.deserialize(in);
            }
            else
            {
                firstName = clusteringSerializer.deserialize(in);
                lastName = clusteringSerializer.deserialize(in);
                offset = in.readLong();
                width = in.readLong();
                endOpenMarker = null;
            }

            return new IndexInfo(firstName, lastName, offset, width, endOpenMarker);
        }

        public void skip(DataInputBuffer in, ISerializer<ClusteringPrefix> clusteringSerializer) throws IOException
        {
            skipPreNames(in);

            skipNames(in, clusteringSerializer);

            if (legacy)
                in.skipBytes(8 + 8);
        }

        public long serializedSize(IndexInfo info, ISerializer<ClusteringPrefix> clusteringSerializer)
        {
            long size = clusteringSerializer.serializedSize(info.firstName)
                        + clusteringSerializer.serializedSize(info.lastName)
                        + TypeSizes.sizeof(info.offset)
                        + TypeSizes.sizeof(info.width);

            if (!legacy)
            {
                size += TypeSizes.sizeof(info.endOpenMarker != null);
                if (info.endOpenMarker != null)
                    size += DeletionTime.serializer.serializedSize(info.endOpenMarker);
            }
            return size;
        }

        public ClusteringPrefix readLastName(DataInputBuffer input, ISerializer<ClusteringPrefix> clusteringSerializer) throws IOException
        {
            skipPreNames(input);
            return clusteringSerializer.deserialize(input);
        }

        public ClusteringPrefix readFirstName(DataInputBuffer input, ISerializer<ClusteringPrefix> clusteringSerializer) throws IOException
        {
            skipPreNames(input);
            clusteringSerializer.skip(input);
            return clusteringSerializer.deserialize(input);
        }

        private static void skipNames(DataInputBuffer in, ISerializer<ClusteringPrefix> clusteringSerializer) throws IOException
        {
            clusteringSerializer.skip(in);
            clusteringSerializer.skip(in);
        }

        private void skipPreNames(DataInputBuffer in) throws IOException
        {
            if (!legacy)
            {
                in.skipBytes(8 + 8);

                if (in.readBoolean())
                    DeletionTime.serializer.skip(in);
            }
        }
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE
               + firstName.unsharedHeapSize()
               + lastName.unsharedHeapSize()
               + (endOpenMarker == null ? 0 : endOpenMarker.unsharedHeapSize());
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexInfo indexInfo = (IndexInfo) o;
        return width == indexInfo.width &&
               offset == indexInfo.offset &&
               Objects.equals(lastName, indexInfo.lastName) &&
               Objects.equals(firstName, indexInfo.firstName) &&
               Objects.equals(endOpenMarker, indexInfo.endOpenMarker);
    }

    public int hashCode()
    {
        return Objects.hash(width, lastName, firstName, offset, endOpenMarker);
    }
}
