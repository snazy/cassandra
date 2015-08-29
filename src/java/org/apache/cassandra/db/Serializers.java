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

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.sstable.format.Version;

import static org.apache.cassandra.io.sstable.IndexHelper.IndexInfo;

/**
 * Holds references on serializers that depend on the table definition.
 */
public final class Serializers
{
    private static final AtomicReference<IndexInfo.Serializer[]> serializers = new AtomicReference<>(new IndexInfo.Serializer[]{new IndexInfo.Serializer(BigFormat.latestVersion)});

    private Serializers()
    {
    }

    public static IndexInfo.Serializer indexSerializer(Version version)
    {
        // A poor-man's singleton approach to reduce the garbage by new IndexInfo.Serializer instances,
        // since this method is called very often with off-heap key-cache.

        IndexInfo.Serializer[] arr = serializers.get();
        for (IndexInfo.Serializer serializer : arr)
        {
            if (serializer.getVersion().equals(version))
                return serializer;
        }

        arr = Arrays.copyOf(arr, arr.length + 1);
        IndexInfo.Serializer ser = new IndexInfo.Serializer(version);
        arr[arr.length - 1] = ser;
        serializers.set(arr);
        return ser;
    }

    // Note that for the old layout, this will actually discard the cellname parts that are not strictly
    // part of the clustering prefix. Don't use this if that's not what you want.
    public static ISerializer<ClusteringPrefix> clusteringPrefixSerializer(final Version version, final SerializationHeader header)
    {
        if (!version.storeRows())
            throw new UnsupportedOperationException();

        return new ClusteringPrefixSerializer(version.correspondingMessagingVersion(), header.clusteringTypes());
    }

    private static final class ClusteringPrefixSerializer implements ISerializer<ClusteringPrefix>
    {
        private final int messagingVersion;
        private final List<AbstractType<?>> clusteringTypes;
        ClusteringPrefixSerializer(int messagingVersion, List<AbstractType<?>> clusteringTypes)
        {
            this.messagingVersion = messagingVersion;
            this.clusteringTypes = clusteringTypes;
        }

        public void serialize(ClusteringPrefix clustering, DataOutputPlus out) throws IOException
        {
            ClusteringPrefix.serializer.serialize(clustering, out, messagingVersion, clusteringTypes);
        }

        public ClusteringPrefix deserialize(DataInputPlus in) throws IOException
        {
            return ClusteringPrefix.serializer.deserialize(in, messagingVersion, clusteringTypes);
        }

        public long serializedSize(ClusteringPrefix clustering)
        {
            return ClusteringPrefix.serializer.serializedSize(clustering, messagingVersion, clusteringTypes);
        }
    }
}
