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
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.UUID;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.UnbufferedDataOutputStreamPlus;
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
     * method to reuse the same {@link RowIndexEntry.Serializer} instance that
     * {@link org.apache.cassandra.cache.OHCKeyCache.ValueSerializer#serializedSize(KeyCacheValue)} used.
     */
    private static final class KeyCacheValue
    {
        final KeyCacheKey key;
        final RowIndexEntry value;
        String indexName;

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
            KeyCacheKey key = rowIndexEntry.key;

            UUID cfId = key.cfId;
            buf.putLong(cfId.getMostSignificantBits());
            buf.putLong(cfId.getLeastSignificantBits());

            SerializationUtil.writeUTF(rowIndexEntry.indexName, buf);

            CFMetaData cfm = cfm(rowIndexEntry, cfId);
            if (cfm == null)
                return;

            RowIndexEntry.IndexSerializer indexSerializer = cfm.serializers().latestVersionRowIndexSerializer;
            try
            {
                indexSerializer.serialize(rowIndexEntry.value, new DataOutputBufferFixed(buf));
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public int serializedSize(KeyCacheValue rowIndexEntry)
        {
            String cfName = rowIndexEntry.key.desc.cfname;
            int iIndexSep = cfName.indexOf(Directories.SECONDARY_INDEX_NAME_SEPARATOR);
            String indexName = iIndexSep != -1 ? cfName.substring(iIndexSep + 1) : "";
            rowIndexEntry.indexName = indexName;

            int sz = 16
                     + SerializationUtil.stringSerializedSize(indexName);

            CFMetaData cfm = cfm(rowIndexEntry, rowIndexEntry.key.cfId);
            if (cfm == null)
                return sz;

            sz += cfm.serializers().latestVersionRowIndexSerializer.serializedSize(rowIndexEntry.value);

            return sz;
        }

        private static CFMetaData cfm(KeyCacheValue rowIndexEntry, UUID cfId)
        {
            if (rowIndexEntry.indexName.isEmpty())
            {
                return Schema.instance.getCFMetaData(cfId);
            }
            else
            {
                ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(cfId);
                Index index = cfs.indexManager.getIndexByName(rowIndexEntry.indexName);
                if (!(index instanceof CassandraIndex))
                    return null;
                cfs = ((CassandraIndex)index).getIndexCfs();

                return cfs.metadata;
            }
        }

        public KeyCacheValue deserialize(ByteBuffer buf)
        {
            long msb = buf.getLong();
            long lsb = buf.getLong();
            UUID cfId = new UUID(msb, lsb);

            CFMetaData cfm = Schema.instance.getCFMetaData(cfId);

            // either an empty string for base CF or just the index name
            String indexName = SerializationUtil.readUTF(buf);
            if (!indexName.isEmpty())
            {
                // special handling for 2i - we need the 2i's CFS

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

                // get base table CFM
                cfm = Schema.instance.getCFMetaData(cfs.keyspace.getName(), cfs.name);
                if (cfm == null)
                    return null;

                // get index for base table
                Index index = cfs.indexManager.getIndexByName(indexName);
                if (!(index instanceof CassandraIndex))
                    return null;
                cfs = ((CassandraIndex)index).getIndexCfs();
                cfm = cfs.metadata;
            }
            if (cfm == null)
                return null;

            DataInputBuffer input = new DataInputBuffer(buf, false);

            RowIndexEntry.IndexSerializer indexSerializer = cfm.serializers().getRowIndexSerializer(BigFormat.latestVersion);
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
            SerializationUtil.writeUTF(keyCacheKey.desc.ksname, buf);
            SerializationUtil.writeUTF(keyCacheKey.desc.cfname, buf);
            SerializationUtil.writeUTF(keyCacheKey.desc.directory.getPath(), buf);
            buf.putShort((short) keyCacheKey.desc.formatType.ordinal());
            buf.putInt(keyCacheKey.desc.generation);
            SerializationUtil.writeUTF(keyCacheKey.desc.version.getVersion(), buf);

            buf.put(keyCacheKey.key);

            assert buf.remaining() == 0;
        }

        public int serializedSize(KeyCacheKey keyCacheKey)
        {
            int sz = 0;

            sz += SerializationUtil.stringSerializedSize(keyCacheKey.desc.ksname);
            sz += SerializationUtil.stringSerializedSize(keyCacheKey.desc.cfname);
            sz += SerializationUtil.stringSerializedSize(keyCacheKey.desc.directory.getPath());
            sz += 2; //keyCacheKey.desc.formatType
            sz += 4; //keyCacheKey.desc.generation;
            sz += SerializationUtil.stringSerializedSize(keyCacheKey.desc.version.getVersion());

            sz += keyCacheKey.key.length;

            return sz;
        }

        public KeyCacheKey deserialize(ByteBuffer buf)
        {
            String ksname = SerializationUtil.readUTF(buf);
            String cfname = SerializationUtil.readUTF(buf);

            // cannot use cfId here since that will be the same for the table and its secondary indexes
            CFMetaData cfm = Schema.instance.getCFMetaData(ksname, cfname);
            if (cfm == null)
                return null;

            File directory = new File(SerializationUtil.readUTF(buf));
            SSTableFormat.Type formatType = SSTableFormat.Type.values()[buf.getShort()];
            int generation = buf.getInt();
            String version = SerializationUtil.readUTF(buf);

            Descriptor desc = new Descriptor(version, directory, ksname, cfname, generation, formatType);

            byte[] key = new byte[buf.remaining()];
            buf.get(key);

            assert buf.remaining() == 0;

            return new KeyCacheKey(cfm.cfId, desc, key);
        }
    }

    // separate class as this stuff is going to be removed here in favor of having more optimized versions
    // in ByteBufferUtil with CASSANDRA-10189
    public static final class SerializationUtil
    {
        private SerializationUtil()
        {
        }

        /**
         * Writes given string to a byte buffer (and changes its position) using
         * a {@code short} for string's length in bytes in UTF-8 charset.
         * Each serialized string starts with an unsigned 16 bit length followed by the UTF-8 representation of the string.
         * Note: this implementation uses two bytes for {@code (char)0}.
         */
        public static void writeUTF(String str, ByteBuffer target)
        {
            try
            {
                UnbufferedDataOutputStreamPlus.writeUTF(str, new DataOutputBufferFixed(target));
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }

        /**
         * Reads a string to a byte buffer (and changes its position) using
         * a {@code short} for string's length in bytes in UTF-8 charset.
         * Each serialized string starts with an unsigned 16 bit length followed by the UTF-8 representation of the string.
         * Note: this implementation uses two bytes for {@code (char)0}.
         */
        @SuppressWarnings("resource")
        public static String readUTF(ByteBuffer source)
        {
            try
            {
                return new DataInputBuffer(source, false).readUTF();
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }

        /**
         * Get the length in bytes for a given string using UTF-8.
         * Note: this implementation uses the correct one byte length for {@code (char)0} as {@code StandardCharsets.UTF_8}
         * does, but the custom implementations in Java for streams use <i>two</i> bytes for {@code (char)0}.
         */
        public static int stringSerializedSize(String s)
        {
            int l = s.length();
            int bytes = 2;
            for (int i = 0; i < l; i++)
            {
                char c = s.charAt(i);
                if (c > 0 && c <= 127)
                    // Note: Java's DataOutputStream.writeUTF uses _two_ bytes for (char)0, but UTF-8 defines _one_ byte
                    bytes++;
                else if (c < 0x800)
                    bytes += 2;
                else
                    bytes += 3;
            }
            return bytes;
        }
    }
}
