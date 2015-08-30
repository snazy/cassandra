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

package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.MoreObjects;

import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class IndexInfo
{

    private static final AtomicReference<Serializer[]> serializers = new AtomicReference<>(new Serializer[0]);

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

    public static Serializer indexSerializer(Version version)
    {
        // A poor-man's singleton approach to reduce the garbage by new IndexInfo.Serializer instances,
        // since this method is called very often with off-heap key-cache.

        if (version == BigFormat.latestVersion)
            return latestVersionSerializer;

        Serializer[] arr = serializers.get();
        for (Serializer serializer : arr)
        {
            if (serializer.getVersion().equals(version))
                return serializer;
        }

        arr = Arrays.copyOf(arr, arr.length + 1);
        Serializer ser = new Serializer(version);
        arr[arr.length - 1] = ser;
        serializers.set(arr);
        return ser;
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

    public static final Serializer latestVersionSerializer = new Serializer(BigFormat.latestVersion);

    public static final class Serializer
    {
        private final Version version;

        Serializer(Version version)
        {
            this.version = version;
        }

        public Version getVersion()
        {
            return version;
        }

        public void serialize(IndexInfo info, DataOutputPlus out, SerializationHeader header) throws IOException
        {
            ISerializer<ClusteringPrefix> clusteringSerializer = RowIndexEntry.clusteringPrefixSerializer(version, header);
            serialize(info, out, header, version, clusteringSerializer);
        }

        public static void serialize(IndexInfo info, DataOutputPlus out, SerializationHeader header, Version version, ISerializer<ClusteringPrefix> clusteringSerializer) throws IOException
        {
            clusteringSerializer.serialize(info.getFirstName(), out);
            clusteringSerializer.serialize(info.getLastName(), out);
            out.writeLong(info.getOffset());
            out.writeLong(info.getWidth());

            if (version.storeRows())
            {
                out.writeBoolean(info.getEndOpenMarker() != null);
                if (info.getEndOpenMarker() != null)
                    DeletionTime.serializer.serialize(info.getEndOpenMarker(), out);
            }
        }

        public IndexInfo deserialize(DataInputPlus in, SerializationHeader header) throws IOException
        {
            ISerializer<ClusteringPrefix> clusteringSerializer = RowIndexEntry.clusteringPrefixSerializer(version, header);

            ClusteringPrefix firstName = clusteringSerializer.deserialize(in);
            ClusteringPrefix lastName = clusteringSerializer.deserialize(in);
            long offset = in.readLong();
            long width = in.readLong();
            DeletionTime endOpenMarker = version.storeRows() && in.readBoolean()
                                         ? DeletionTime.serializer.deserialize(in)
                                         : null;

            return new IndexInfo(firstName, lastName, offset, width, endOpenMarker);
        }

        public void skip(DataInputPlus in, SerializationHeader header, ISerializer<ClusteringPrefix> clusteringSerializer) throws IOException
        {
            /*ClusteringPrefix firstName*/ ClusteringPrefix.Serializer.skip(in, version.correspondingMessagingVersion(), header.clusteringTypes());
            /*ClusteringPrefix lastName*/ ClusteringPrefix.Serializer.skip(in, version.correspondingMessagingVersion(), header.clusteringTypes());
            in.skipBytes(8 + 8); // offset + width
            if (version.storeRows() && in.readBoolean())
                in.skipBytes((int) DeletionTime.serializer.serializedSize(null));
        }

        public long serializedSize(IndexInfo info, SerializationHeader header)
        {
            ISerializer<ClusteringPrefix> clusteringSerializer = RowIndexEntry.clusteringPrefixSerializer(version, header);
            long size = clusteringSerializer.serializedSize(info.getFirstName())
                        + clusteringSerializer.serializedSize(info.getLastName())
                        + TypeSizes.sizeof(info.getOffset())
                        + TypeSizes.sizeof(info.getWidth());

            if (version.storeRows())
            {
                size += TypeSizes.sizeof(info.getEndOpenMarker() != null);
                if (info.getEndOpenMarker() != null)
                    size += DeletionTime.serializer.serializedSize(info.getEndOpenMarker());
            }
            return size;
        }
    }

    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("width", width)
                          .add("lastName", lastName)
                          .add("firstName", firstName)
                          .add("offset", offset)
                          .add("endOpenMarker", endOpenMarker)
                          .toString();
    }
}
