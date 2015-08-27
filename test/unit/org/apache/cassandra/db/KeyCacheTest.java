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

import java.io.IOError;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.cache.OHCKeyCache;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.lifecycle.TransactionLog;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KeyCacheTest
{
    static
    {
        System.setProperty(Config.PROPERTY_PREFIX + "key_cache_serialization_value_buffer_size", "1");
    }

    private static final String KEYSPACE1 = "KeyCacheTest1";
    private static final String KEY_CACHE_CF = "Standard1";
    private static final String KEY_CACHE_LOAD_CF = "Standard2";
    private static final String HOT_KEY_CACHE_LOAD_CF = "Standard3";
    private static final String FOR_CONFIG_CF = "Standard4";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, KEY_CACHE_CF),
                                    SchemaLoader.standardCFMD(KEYSPACE1, KEY_CACHE_LOAD_CF),
                                    SchemaLoader.standardCFMD(KEYSPACE1, HOT_KEY_CACHE_LOAD_CF),
                                    SchemaLoader.standardCFMD(KEYSPACE1, FOR_CONFIG_CF));
    }

    @AfterClass
    public static void cleanup()
    {
        SchemaLoader.cleanupSavedCaches();
    }

    @Test
    public void testKeyCacheLoad() throws Exception
    {
        keyCacheLoad(Integer.MAX_VALUE, KEY_CACHE_LOAD_CF);
    }

    @Test
    public void testHotKeyCacheLoad() throws Exception
    {
        keyCacheLoad(50, HOT_KEY_CACHE_LOAD_CF);
    }

    private void keyCacheLoad(int amount, String table) throws InterruptedException, ExecutionException
    {

        // just to make sure that everything is clean
        CacheService.instance.invalidateKeyCache();

        CompactionManager.instance.disableAutoCompaction();

        ColumnFamilyStore store = Keyspace.open(KEYSPACE1).getColumnFamilyStore(table);

        // empty the cache
        CacheService.instance.invalidateKeyCache();
        assertKeyCacheSize(0, KEYSPACE1, table);

        // insert data and force to disk
        SchemaLoader.insertData(KEYSPACE1, table, 0, 100);
        store.forceBlockingFlush();

        // populate the cache
        readData(KEYSPACE1, table, 100);
        assertKeyCacheSize(100, KEYSPACE1, table);

        // really? our caches don't implement the map interface? (hence no .addAll)
        Map<KeyCacheKey, RowIndexEntry> savedMap = new HashMap<>();
        Iterator<KeyCacheKey> iter = amount == Integer.MAX_VALUE
                                     ? CacheService.instance.keyCache.keyIterator()
                                     : CacheService.instance.keyCache.hotKeyIterator(amount);
        // OHC likely returns more entries than expected, but the cache-write does not write more than requested
        while (iter.hasNext() && savedMap.size() < amount)
        {
            KeyCacheKey k = iter.next();
            if (k.desc.ksname.equals(KEYSPACE1) && k.desc.cfname.equals(table))
                savedMap.put(k, CacheService.instance.keyCache.get(k));
        }

        // force the cache to disk
        CacheService.instance.keyCache.submitWrite(amount).get();

        CacheService.instance.invalidateKeyCache();
        assertKeyCacheSize(0, KEYSPACE1, table);

        CacheService.instance.keyCache.loadSaved(store);
        assertKeyCacheSize(savedMap.size(), KEYSPACE1, table);

        // probably it's better to add equals/hashCode to RowIndexEntry...
        for (Map.Entry<KeyCacheKey, RowIndexEntry> entry : savedMap.entrySet())
        {
            RowIndexEntry expected = entry.getValue();
            RowIndexEntry actual = CacheService.instance.keyCache.get(entry.getKey());
            assertEquals(expected.position, actual.position);
            assertEquals(expected.columnsIndex(), actual.columnsIndex());
            if (expected.isIndexed())
            {
                assertEquals(expected.deletionTime(), actual.deletionTime());
            }
        }
    }

    @Test
    public void testKeyCacheConfiguration() throws ExecutionException, InterruptedException
    {
        long oldCapacity = CacheService.instance.keyCache.getCapacity();
        int oldKeys = CacheService.instance.getKeyCacheKeysToSave();
        int oldPeriod = CacheService.instance.getKeyCacheSavePeriodInSeconds();
        try
        {
            CompactionManager.instance.disableAutoCompaction();

            ColumnFamilyStore store = Keyspace.open(KEYSPACE1).getColumnFamilyStore(FOR_CONFIG_CF);

            // just to make sure that everything is clean
            CacheService.instance.invalidateKeyCache();

            // KeyCache should start at size 0 if we're caching X% of zero data.
            assertKeyCacheSize(0, KEYSPACE1, FOR_CONFIG_CF);

            // insert data and force to disk
            SchemaLoader.insertData(KEYSPACE1, FOR_CONFIG_CF, 0, 100);
            store.forceBlockingFlush();

            // test "no" key cache config
            CacheService.instance.setKeyCacheCapacityInMB(0L);
            CacheService.instance.invalidateKeyCache();
            readData(KEYSPACE1, FOR_CONFIG_CF, 100);
            assertKeyCacheSize(0, KEYSPACE1, FOR_CONFIG_CF);

            // test small key cache config
            CacheService.instance.setKeyCacheCapacityInMB(256L);
            CacheService.instance.invalidateKeyCache();
            readData(KEYSPACE1, FOR_CONFIG_CF, 100);
            assertKeyCacheSize(100, KEYSPACE1, FOR_CONFIG_CF);

            // again: test "no" key cache config
            CacheService.instance.setKeyCacheCapacityInMB(0L);
            CacheService.instance.invalidateKeyCache();
            readData(KEYSPACE1, FOR_CONFIG_CF, 100);
            assertKeyCacheSize(0, KEYSPACE1, FOR_CONFIG_CF);

            CacheService.instance.setKeyCacheKeysToSave(200);
            assertEquals(200, CacheService.instance.getKeyCacheKeysToSave());
            assertNotNull(CacheService.instance.keyCache.saveTask);

            CacheService.instance.setKeyCacheSavePeriodInSeconds(0); // not at all
            assertNull(CacheService.instance.keyCache.saveTask);
            CacheService.instance.setKeyCacheSavePeriodInSeconds(9999); // not yet
            assertEquals(9999, CacheService.instance.getKeyCacheSavePeriodInSeconds());
            assertNotNull(CacheService.instance.keyCache.saveTask);
            assertTrue(CacheService.instance.keyCache.saveTask.getDelay(TimeUnit.SECONDS) >= 9998);
        }
        finally
        {
            CacheService.instance.setKeyCacheCapacityInMB(oldCapacity);
            CacheService.instance.setKeyCacheKeysToSave(oldKeys);
            CacheService.instance.setKeyCacheSavePeriodInSeconds(oldPeriod);
        }
    }

    @Test
    public void testKeyCache() throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(KEY_CACHE_CF);

        // just to make sure that everything is clean
        CacheService.instance.invalidateKeyCache();

        // KeyCache should start at size 0 if we're caching X% of zero data.
        assertKeyCacheSize(0, KEYSPACE1, KEY_CACHE_CF);

        Mutation rm;

        // inserts
        new RowUpdateBuilder(cfs.metadata, 0, "key1").clustering("1").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "key2").clustering("2").build().applyUnsafe();

        // to make sure we have SSTable
        cfs.forceBlockingFlush();

        // reads to cache key position
        Util.getAll(Util.cmd(cfs, "key1").build());
        Util.getAll(Util.cmd(cfs, "key2").build());

        assertKeyCacheSize(2, KEYSPACE1, KEY_CACHE_CF);

        Set<SSTableReader> readers = cfs.getLiveSSTables();
        Refs<SSTableReader> refs = Refs.tryRef(readers);
        if (refs == null)
            throw new IllegalStateException();

        Util.compactAll(cfs, Integer.MAX_VALUE).get();
        boolean noEarlyOpen = DatabaseDescriptor.getSSTablePreempiveOpenIntervalInMB() < 0;

        // after compaction cache should have entries for new SSTables,
        // but since we have kept a reference to the old sstables,
        // if we had 2 keys in cache previously it should become 4
        assertKeyCacheSize(noEarlyOpen ? 2 : 4, KEYSPACE1, KEY_CACHE_CF);

        refs.release();

        TransactionLog.waitForDeletions();

        // after releasing the reference this should drop to 2
        assertKeyCacheSize(2, KEYSPACE1, KEY_CACHE_CF);

        // re-read same keys to verify that key cache didn't grow further
        Util.getAll(Util.cmd(cfs, "key1").build());
        Util.getAll(Util.cmd(cfs, "key2").build());

        assertKeyCacheSize(noEarlyOpen ? 4 : 2, KEYSPACE1, KEY_CACHE_CF);
    }

    private static void readData(String keyspace, String columnFamily, int numberOfRows)
    {
        ColumnFamilyStore store = Keyspace.open(keyspace).getColumnFamilyStore(columnFamily);
        CFMetaData cfm = Schema.instance.getCFMetaData(keyspace, columnFamily);

        for (int i = 0; i < numberOfRows; i++)
            Util.getAll(Util.cmd(store, "key" + i).includeRow("col" + i).build());
    }


    private void assertKeyCacheSize(int expected, String keyspace, String columnFamily)
    {
        int size = 0;
        for (Iterator<KeyCacheKey> iter = CacheService.instance.keyCache.keyIterator();
             iter.hasNext();)
        {
            KeyCacheKey k = iter.next();
            if (k.desc.ksname.equals(keyspace) && k.desc.cfname.equals(columnFamily))
                size++;
        }
        assertEquals(expected, size);
    }


    @Test
    public void testStringReadWriteChar0()
    {
        assertEquals(4, OHCKeyCache.SerializationUtil.stringSerializedSize("\u0000"));

        // heap/array ByteBuffer
        ByteBuffer bb = ByteBuffer.allocate(4);
        OHCKeyCache.SerializationUtil.writeUTF("\u0000", bb);
        assertArrayEquals(new byte[]{ 0, 2, (byte)0xc0, (byte)0x80 }, bb.array());
        bb.flip();
        assertEquals("\u0000", OHCKeyCache.SerializationUtil.readUTF(bb));
        ByteBuffer bbHeap = bb;

        // direct ByteBuffer
        bb = ByteBuffer.allocateDirect(4);
        OHCKeyCache.SerializationUtil.writeUTF("\u0000", bb);
        assertEquals(bbHeap, bb);
        bb.flip();
        assertEquals("\u0000", OHCKeyCache.SerializationUtil.readUTF(bb));
    }

    @Test
    public void testStringReadWrite()
    {
        String[] strings = {
                           "",
                           "\u00e4\u00f6\u00fc\u00df",
                           "hello world",
                           "k\u00f6lsche jung",
                           "\u07FF",
                           "\u0800",
                           "\u007F",
                           "\u0080",
                           "\uffff",
                           "\u3456\u07ff\u0800\u007f\u0080\uffff foo bar baz",
                           "foo bar baz \u3456\u07ff\u0800\u007f\u0080\uffff"
        };

        // heap/array ByteBuffer
        for (String s : strings)
        {
            int bbuSize = OHCKeyCache.SerializationUtil.stringSerializedSize(s) - 2;
            byte[] refBytes = s.getBytes(StandardCharsets.UTF_8);
            assertEquals(refBytes.length, bbuSize);

            ByteBuffer bb = ByteBuffer.allocate(2 + bbuSize);
            OHCKeyCache.SerializationUtil.writeUTF(s, bb);
            assertEquals(bbuSize + 2, bb.position());
            bb.flip();
            String read = OHCKeyCache.SerializationUtil.readUTF(bb);
            assertEquals(s, read);
            assertEquals(bbuSize + 2, bb.position());
        }

        // direct ByteBuffer
        for (String s : strings)
        {
            int bbuSize = OHCKeyCache.SerializationUtil.stringSerializedSize(s) - 2;
            byte[] refBytes = s.getBytes(StandardCharsets.UTF_8);
            assertEquals(refBytes.length, bbuSize);

            ByteBuffer bb = ByteBuffer.allocateDirect(2 + bbuSize);
            OHCKeyCache.SerializationUtil.writeUTF(s, bb);
            assertEquals(bbuSize + 2, bb.position());
            bb.flip();
            String read = OHCKeyCache.SerializationUtil.readUTF(bb);
            assertEquals(s, read);
            assertEquals(bbuSize + 2, bb.position());
        }
    }

    @Test
    public void testBigStringReadWrite() throws IOException
    {
        String ref = bigString();
        assertEquals(65535, OHCKeyCache.SerializationUtil.stringSerializedSize(ref) - 2);

        ByteBuffer buf = ByteBuffer.allocate(65537);
        OHCKeyCache.SerializationUtil.writeUTF(ref, buf);
        buf.flip();

        String str = OHCKeyCache.SerializationUtil.readUTF(buf);
        assertEquals(ref, str);
    }

    @Test
    public void testBigStringUTFReadWrite() throws IOException
    {
        String ref = bigStringUTF();
        assertEquals(65535, OHCKeyCache.SerializationUtil.stringSerializedSize(ref) - 2);

        ByteBuffer buf = ByteBuffer.allocate(65537);
        OHCKeyCache.SerializationUtil.writeUTF(ref, buf);
        buf.flip();

        String str = OHCKeyCache.SerializationUtil.readUTF(buf);
        assertEquals(ref, str);
    }

    @Test
    public void testStringTooLong() throws IOException
    {
        String tooLong = bigString() + 'a';
        ByteBuffer bb = ByteBuffer.allocate(65538);
        try
        {
            OHCKeyCache.SerializationUtil.writeUTF(tooLong, bb);
            fail();
        }
        catch (IOError e)
        {
            assertTrue(e.getCause() instanceof UTFDataFormatException);
        }
    }

    private static String bigString()
    {
        StringBuilder bigString = new StringBuilder();
        for (int i = 0; i < 65535; i++)
        {
            bigString.append((char)('a' + i % 26));
        }
        return bigString.toString();
    }

    private static String bigStringUTF()
    {
        StringBuilder bigString = new StringBuilder();
        bigString.append("\u00e4\u00f6\u00fc\u00df");
        for (int i = 0; i < 65527; i++)
        {
            bigString.append((char)('a' + i % 26));
        }
        return bigString.toString();
    }

}
