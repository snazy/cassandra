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

package org.apache.cassandra.cache;

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Optional;
import java.util.UUID;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.caffinitas.ohc.CacheSerializer;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;

public final class OHCKeyCache
{
    private OHCKeyCache()
    {
    }

    public static ICache<KeyCacheKey, RowIndexEntry> create()
    {
        OHCacheBuilder<KeyCacheKey, KeyCacheValue> builder = OHCacheBuilder.newBuilder();
        builder.capacity(DatabaseDescriptor.getKeyCacheSizeInMB() * 1024 * 1024)
               .keySerializer(KeySerializer.instance)
               .valueSerializer(ValueSerializer.instance)
               .throwOOME(true);

        return new OHKeyCacheAdapter(builder.build());
    }

    /**
     * A temporary helper object to pass the {@link KeyCacheKey} to the value serializer.
     * Also allows the {@link org.apache.cassandra.cache.OHCKeyCache.ValueSerializer#serialize(KeyCacheValue, ByteBuffer)}
     * method to reuse the same {@link RowIndexEntry.IndexSerializer} instance that
     * {@link org.apache.cassandra.cache.OHCKeyCache.ValueSerializer#serializedSize(KeyCacheValue)} used.
     */
    private static final class KeyCacheValue
    {
        final KeyCacheKey key;
        final RowIndexEntry value;
        ByteBuffer buffer;

        KeyCacheValue(KeyCacheKey key, RowIndexEntry value)
        {
            this.key = key;
            this.value = value;
        }
    }
    
    private static final class OHKeyCacheAdapter implements ICache<KeyCacheKey, RowIndexEntry>
    {
        private final OHCache<KeyCacheKey, KeyCacheValue> ohCache;

        private OHKeyCacheAdapter(OHCache<KeyCacheKey, KeyCacheValue> ohCache)
        {
            this.ohCache = ohCache;
        }

        public long capacity()
        {
            return ohCache.capacity();
        }

        public void setCapacity(long capacity)
        {
            ohCache.setCapacity(capacity);
        }

        public void put(KeyCacheKey key, RowIndexEntry value)
        {
            assert value != null;
            ohCache.put(key, makeVal(key, value));
        }

        public boolean putIfAbsent(KeyCacheKey key, RowIndexEntry value)
        {
            return ohCache.putIfAbsent(key, makeVal(key, value));
        }

        private static KeyCacheValue makeVal(KeyCacheKey key, RowIndexEntry value)
        {
            assert value != null;
            return new KeyCacheValue(key, value);
        }

        public boolean replace(KeyCacheKey key, RowIndexEntry old, RowIndexEntry value)
        {
            return ohCache.addOrReplace(key, makeVal(key, old), makeVal(key, value));
        }

        public RowIndexEntry get(KeyCacheKey key)
        {
            KeyCacheValue v = ohCache.get(key);
            return v != null ? v.value : null;
        }

        public void remove(KeyCacheKey key)
        {
            ohCache.remove(key);
        }

        public int size()
        {
            return (int) ohCache.size();
        }

        public long weightedSize()
        {
            return ohCache.size();
        }

        public void clear()
        {
            ohCache.clear();
        }

        public Iterator<KeyCacheKey> hotKeyIterator(int n)
        {
            return ohCache.hotKeyIterator(n);
        }

        public Iterator<KeyCacheKey> keyIterator()
        {
            return ohCache.keyIterator();
        }

        public boolean containsKey(KeyCacheKey key)
        {
            return ohCache.containsKey(key);
        }
    }

    private static class ValueSerializer implements CacheSerializer<KeyCacheValue>
    {
        public static CacheSerializer<KeyCacheValue> instance = new ValueSerializer();

        public void serialize(KeyCacheValue rowIndexEntry, ByteBuffer buf)
        {
            buf.put(rowIndexEntry.buffer);
        }

        public int serializedSize(KeyCacheValue rowIndexEntry)
        {
            // TODO remove KeyCacheValue.buffer when using OHC's chunked implementation (CASSANDRA-9929) and move
            // code from serializedSize() to serialize(). (OHC chunked implementation provides a mechanism for a
            // thread-local serialization buffer and won't call serializedSize())
            ByteBuffer buf = ByteBufferUtil.temporaryByteBuffer(0);
            while (true)
            {
                try
                {
                    KeyCacheKey key = rowIndexEntry.key;

                    UUID cfId = key.cfId;
                    buf.putLong(cfId.getMostSignificantBits());
                    buf.putLong(cfId.getLeastSignificantBits());

                    // following basically what CacheService.KeyCacheSerializer does

                    Descriptor desc = key.desc;
                    String ksName = desc.ksname;
                    String cfName = desc.cfname;
                    int iIndexSep = cfName.indexOf(Directories.SECONDARY_INDEX_NAME_SEPARATOR);
                    CFMetaData cfm;
                    if (iIndexSep == -1)
                    {
                        // base table (not a 2i)
                        cfm = Schema.instance.getCFMetaData(ksName, cfName);
                        ByteBufferUtil.writeUTF("", buf);
                    }
                    else
                    {
                        String baseName = cfName.substring(0, iIndexSep);
                        String indexName = cfName.substring(iIndexSep + 1);
                        cfm = Schema.instance.getCFMetaData(ksName, baseName);
                        Optional<IndexMetadata> indexMeta = cfm.getIndexes().get(indexName);
                        if (!indexMeta.isPresent())
                            return 0;
                        cfm = CassandraIndex.indexCfsMetadata(cfm, indexMeta.get());

                        ByteBufferUtil.writeUTF(indexName, buf);
                    }

                    if (cfm == null)
                        return 0;

                    buf.putShort((short) desc.formatType.ordinal());
                    ByteBufferUtil.writeUTF(desc.version.getVersion(), buf);

                    DataOutputBufferFixed out = new DataOutputBufferFixed(buf);
                    RowIndexEntry.IndexSerializer<?> indexSerializer = desc.getFormat().getIndexSerializer(cfm,
                                                                                                           desc.version,
                                                                                                           SerializationHeader.forKeyCache(cfm));
                    try
                    {
                        indexSerializer.serialize(rowIndexEntry.value, out);
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException(e);
                    }

                    rowIndexEntry.buffer = buf;
                    buf.flip();
                    return buf.limit();
                }
                catch (BufferOverflowException resize)
                {
                    buf = ByteBufferUtil.resizeByteBuffer(buf);
                }
            }
        }

        public KeyCacheValue deserialize(ByteBuffer buf)
        {
            long msb = buf.getLong();
            long lsb = buf.getLong();
            UUID cfId = new UUID(msb, lsb);

            CFMetaData cfm = Schema.instance.getCFMetaData(cfId);
            ColumnFamilyStore cfs;
            try
            {
                Keyspace ks = Keyspace.open(cfm.ksName);
                if (ks == null)
                    // keyspace no longer exists
                    return null;
                cfs = ks.getColumnFamilyStore(cfm.cfId);
            }
            catch (IllegalArgumentException e)
            {
                // the table no longer exists.
                return null;
            }

            // either an empty string for base CF or just the index name
            String indexName = ByteBufferUtil.readUTF(buf);
            if (!indexName.isEmpty())
            {
                // special handling for 2i - we need the 2i's CFS

                // get base table CFM
                cfm = Schema.instance.getCFMetaData(cfs.keyspace.getName(), cfs.name);
                if (cfm == null)
                    return null;

                // get index for base table
                Index index = cfs.indexManager.getIndexByName(indexName);
                if (!(index instanceof CassandraIndex))
                    return null;
                cfs = ((CassandraIndex)index).getIndexCfs();
            }
            if (cfs == null)
                return null;

            SSTableFormat.Type formatType = SSTableFormat.Type.values()[buf.getShort()];
            Version version = formatType.info.getVersion(ByteBufferUtil.readUTF(buf));

            DataInputBuffer input = new DataInputBuffer(buf, false);

            // following basically what CacheService.KeyCacheSerializer does

            RowIndexEntry.IndexSerializer<?> indexSerializer;
            indexSerializer = formatType.info.getIndexSerializer(cfs.metadata,
                                                                 version,
                                                                 SerializationHeader.forKeyCache(cfs.metadata));
            try
            {
                RowIndexEntry entry = indexSerializer.deserialize(input);
                return new KeyCacheValue(null, entry);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private static class KeySerializer implements CacheSerializer<KeyCacheKey>
    {
        public static CacheSerializer<KeyCacheKey> instance = new KeySerializer();

        public void serialize(KeyCacheKey keyCacheKey, ByteBuffer buf)
        {
            ByteBufferUtil.writeUTF(keyCacheKey.desc.ksname, buf);
            ByteBufferUtil.writeUTF(keyCacheKey.desc.cfname, buf);
            ByteBufferUtil.writeUTF(keyCacheKey.desc.directory.getPath(), buf);
            buf.putShort((short) keyCacheKey.desc.formatType.ordinal());
            buf.putInt(keyCacheKey.desc.generation);
            ByteBufferUtil.writeUTF(keyCacheKey.desc.version.getVersion(), buf);

            buf.put(keyCacheKey.key);

            assert buf.remaining() == 0;
        }

        public int serializedSize(KeyCacheKey keyCacheKey)
        {
            int sz = 0;

            sz += 2 + ByteBufferUtil.stringSerializedSize(keyCacheKey.desc.ksname);
            sz += 2 + ByteBufferUtil.stringSerializedSize(keyCacheKey.desc.cfname);
            sz += 2 + ByteBufferUtil.stringSerializedSize(keyCacheKey.desc.directory.getPath());
            sz += 2; //keyCacheKey.desc.formatType
            sz += 4; //keyCacheKey.desc.generation;
            sz += 2 + ByteBufferUtil.stringSerializedSize(keyCacheKey.desc.version.getVersion());

            sz += keyCacheKey.key.length;

            return sz;
        }

        public KeyCacheKey deserialize(ByteBuffer buf)
        {
            String ksname = ByteBufferUtil.readUTF(buf);
            String cfname = ByteBufferUtil.readUTF(buf);

            // cannot use cfId here since that will be the same for the table and its secondary indexes
            CFMetaData cfm = Schema.instance.getCFMetaData(ksname, cfname);
            if (cfm == null)
                return null;

            File directory = new File(ByteBufferUtil.readUTF(buf));
            SSTableFormat.Type formatType = SSTableFormat.Type.values()[buf.getShort()];
            int generation = buf.getInt();
            String version = ByteBufferUtil.readUTF(buf);

            Descriptor desc = new Descriptor(version, directory, ksname, cfname, generation, formatType);

            byte[] key = new byte[buf.remaining()];
            buf.get(key);

            assert buf.remaining() == 0;

            return new KeyCacheKey(cfm.cfId, desc, key);
        }
    }
}
