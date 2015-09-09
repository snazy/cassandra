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
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Holds references on serializers that depend on the table definition.
 */
public final class Serializers
{
    private final CFMetaData metadata;
    public final SerializationHeader keyCacheHeader;
    public final IndexInfo.Serializer latestVersionIndexSerializer;
    public final ISerializer<ClusteringPrefix> latestVersionClusteringPrefixSerializer;
    public final RowIndexEntry.IndexSerializer latestVersionRowIndexSerializer;

    public Serializers(CFMetaData metadata)
    {
        this.metadata = metadata;
        this.keyCacheHeader = SerializationHeader.forKeyCache(metadata);
        this.latestVersionIndexSerializer = createIndexSerializer(BigFormat.latestVersion);
        this.latestVersionClusteringPrefixSerializer = createClusteringPrefixSerializer(BigFormat.latestVersion);
        this.latestVersionRowIndexSerializer = createRowIndexSerializer(BigFormat.latestVersion);
    }

    public RowIndexEntry.IndexSerializer getRowIndexSerializer(Version version)
    {
        if (BigFormat.latestVersion.equals(version))
            return latestVersionRowIndexSerializer;
        return createRowIndexSerializer(version);
    }

    private RowIndexEntry.IndexSerializer createRowIndexSerializer(Version version)
    {
        return new RowIndexEntry.Serializer(metadata, version);
    }

    public IndexInfo.Serializer indexSerializer(Version version)
    {
        if (BigFormat.latestVersion.equals(version))
            return latestVersionIndexSerializer;
        return createIndexSerializer(version);
    }

    private IndexInfo.Serializer createIndexSerializer(Version version)
    {
        return new IndexInfo.Serializer(metadata, version);
    }

    // TODO: Once we drop support for old (pre-3.0) sstables, we can drop this method and inline the calls to
    // ClusteringPrefix.serializer in IndexHelper directly. At which point this whole class probably becomes
    // unecessary (since IndexInfo.Serializer won't depend on the metadata either).
    public ISerializer<ClusteringPrefix> clusteringPrefixSerializer(Version version)
    {
        if (version == BigFormat.latestVersion)
            return latestVersionClusteringPrefixSerializer;

        return createClusteringPrefixSerializer(version);
    }

    private ISerializer<ClusteringPrefix> createClusteringPrefixSerializer(Version version)
    {
        if (!version.storeRows())
            return new LegacyClusteringPrefixSerializer(metadata);

        return new ClusteringPrefixSerializer(version.correspondingMessagingVersion(), metadata.clusteringTypes());
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

        public void skip(DataInputPlus in) throws IOException
        {
            ClusteringPrefix.serializer.skip(in, messagingVersion, clusteringTypes);
        }

        public long serializedSize(ClusteringPrefix clustering)
        {
            return ClusteringPrefix.serializer.serializedSize(clustering, messagingVersion, clusteringTypes);
        }
    }

    private static final class LegacyClusteringPrefixSerializer implements ISerializer<ClusteringPrefix>
    {
        private final CFMetaData metadata;

        LegacyClusteringPrefixSerializer(CFMetaData metadata)
        {
            this.metadata = metadata;
        }

        public void serialize(ClusteringPrefix clustering, DataOutputPlus out)
        {
            // We should only use this for reading old sstable, never write new ones.
            throw new UnsupportedOperationException();
        }

        public ClusteringPrefix deserialize(DataInputPlus in) throws IOException
        {
            // We're reading the old cellname/composite
            ByteBuffer bb = ByteBufferUtil.readWithShortLength(in);
            assert bb.hasRemaining(); // empty cellnames were invalid

            int clusteringSize = metadata.clusteringColumns().size();
            // If the table has no clustering column, then the cellname will just be the "column" name, which we ignore here.
            if (clusteringSize == 0)
                return Clustering.EMPTY;

            if (!metadata.isCompound())
                return new Clustering(bb);

            List<ByteBuffer> components = CompositeType.splitName(bb);
            byte eoc = CompositeType.lastEOC(bb);

            if (eoc == 0 || components.size() >= clusteringSize)
            {
                // That's a clustering.
                if (components.size() > clusteringSize)
                    components = components.subList(0, clusteringSize);

                return new Clustering(components.toArray(new ByteBuffer[clusteringSize]));
            }
            else
            {
                // It's a range tombstone bound. It is a start since that's the only part we've ever included
                // in the index entries.
                Slice.Bound.Kind boundKind = eoc > 0
                                             ? Slice.Bound.Kind.EXCL_START_BOUND
                                             : Slice.Bound.Kind.INCL_START_BOUND;

                return Slice.Bound.create(boundKind, components.toArray(new ByteBuffer[components.size()]));
            }
        }

        public void skip(DataInputPlus in) throws IOException
        {
            // We're reading the old cellname/composite
            ByteBufferUtil.skipShortLength(in);
        }

        public long serializedSize(ClusteringPrefix clustering)
        {
            // We should only use this for reading old sstable, never write new ones.
            throw new UnsupportedOperationException();
        }
    }
}
