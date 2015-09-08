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
import java.util.List;
import java.util.Objects;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ObjectSizes;

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
        private final CFMetaData metadata;
        private final Version version;

        public Serializer(CFMetaData metadata, Version version)
        {
            this.metadata = metadata;
            this.version = version;
        }

        public void serialize(IndexInfo info, DataOutputPlus out, List<AbstractType<?>> clusteringTypes) throws IOException
        {
            ISerializer<ClusteringPrefix> clusteringSerializer = metadata.serializers().clusteringPrefixSerializer(version, clusteringTypes);
            clusteringSerializer.serialize(info.firstName, out);
            clusteringSerializer.serialize(info.lastName, out);
            out.writeLong(info.offset);
            out.writeLong(info.width);

            if (version.storeRows())
            {
                out.writeBoolean(info.endOpenMarker != null);
                if (info.endOpenMarker != null)
                    DeletionTime.serializer.serialize(info.endOpenMarker, out);
            }
        }

        public long readOffset(DataInputBuffer in, List<AbstractType<?>> clusteringTypes) throws IOException
        {
            ISerializer<ClusteringPrefix> clusteringSerializer = metadata.serializers().clusteringPrefixSerializer(version, clusteringTypes);

            clusteringSerializer.skip(in);
            clusteringSerializer.skip(in);
            return in.readLong();
        }

        public long readWidth(DataInputBuffer in, List<AbstractType<?>> clusteringTypes) throws IOException
        {
            ISerializer<ClusteringPrefix> clusteringSerializer = metadata.serializers().clusteringPrefixSerializer(version, clusteringTypes);

            clusteringSerializer.skip(in);
            clusteringSerializer.skip(in);
            in.skipBytes(8);
            return in.readLong();
        }

        public DeletionTime readEndOpenMarker(DataInputBuffer in, List<AbstractType<?>> clusteringTypes) throws IOException
        {
            ISerializer<ClusteringPrefix> clusteringSerializer = metadata.serializers().clusteringPrefixSerializer(version, clusteringTypes);

            clusteringSerializer.skip(in);
            clusteringSerializer.skip(in);
            in.skipBytes(8 + 8);

            return version.storeRows() && in.readBoolean()
                   ? DeletionTime.serializer.deserialize(in)
                   : null;
        }

        public IndexInfo deserialize(DataInputPlus in, List<AbstractType<?>> clusteringTypes) throws IOException
        {
            ISerializer<ClusteringPrefix> clusteringSerializer = metadata.serializers().clusteringPrefixSerializer(version, clusteringTypes);

            ClusteringPrefix firstName = clusteringSerializer.deserialize(in);
            ClusteringPrefix lastName = clusteringSerializer.deserialize(in);
            long offset = in.readLong();
            long width = in.readLong();
            DeletionTime endOpenMarker = version.storeRows() && in.readBoolean()
                                         ? DeletionTime.serializer.deserialize(in)
                                         : null;

            return new IndexInfo(firstName, lastName, offset, width, endOpenMarker);
        }

        public void skip(DataInputBuffer in, List<AbstractType<?>> clusteringTypes) throws IOException
        {
            ISerializer<ClusteringPrefix> clusteringSerializer = metadata.serializers().clusteringPrefixSerializer(version, clusteringTypes);

            clusteringSerializer.skip(in);
            clusteringSerializer.skip(in);
            in.skipBytes(8 + 8);
            if (version.storeRows() && in.readBoolean())
                DeletionTime.serializer.skip(in);
        }

        public long serializedSize(IndexInfo info, List<AbstractType<?>> clusteringTypes)
        {
            ISerializer<ClusteringPrefix> clusteringSerializer = metadata.serializers().clusteringPrefixSerializer(version, clusteringTypes);
            long size = clusteringSerializer.serializedSize(info.firstName)
                        + clusteringSerializer.serializedSize(info.lastName)
                        + TypeSizes.sizeof(info.offset)
                        + TypeSizes.sizeof(info.width);

            if (version.storeRows())
            {
                size += TypeSizes.sizeof(info.endOpenMarker != null);
                if (info.endOpenMarker != null)
                    size += DeletionTime.serializer.serializedSize(info.endOpenMarker);
            }
            return size;
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
