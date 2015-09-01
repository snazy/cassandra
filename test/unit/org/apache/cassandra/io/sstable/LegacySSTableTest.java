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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.rows.SliceableUnfilteredRowIterator;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

/**
 * Tests backwards compatibility for SSTables
 */
public class LegacySSTableTest
{
    private static final Logger logger = LoggerFactory.getLogger(LegacySSTableTest.class);

    public static final String LEGACY_SSTABLE_PROP = "legacy-sstable-root";
    public static final String KSNAME = "Keyspace1";
    public static final String CFNAME = "Standard1";

    public static Set<String> TEST_DATA;
    public static File LEGACY_SSTABLE_ROOT;

    public static final String[] legacyVersions = {"ma", "la", "ka", "jb"};

    // 1200 chars
    static final String longString = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();

        CFMetaData metadata = CFMetaData.Builder.createDense(KSNAME, CFNAME, false, false)
                                                .addPartitionKey("key", BytesType.instance)
                                                .addClusteringColumn("column", BytesType.instance)
                                                .addRegularColumn("value", BytesType.instance)
                                                .build();

        SchemaLoader.createKeyspace(KSNAME,
                                    KeyspaceParams.simple(1),
                                    metadata);
        beforeClass();
    }

    public static void beforeClass()
    {
        Keyspace.setInitialized();
        String scp = System.getProperty(LEGACY_SSTABLE_PROP);
        assert scp != null;
        LEGACY_SSTABLE_ROOT = new File(scp).getAbsoluteFile();
        assert LEGACY_SSTABLE_ROOT.isDirectory();

        TEST_DATA = new HashSet<String>();
        for (int i = 100; i < 1000; ++i)
            TEST_DATA.add(Integer.toString(i));
    }

    /**
     * Get a descriptor for the legacy sstable at the given version.
     */
    protected Descriptor getDescriptor(String ver)
    {
        File directory = new File(LEGACY_SSTABLE_ROOT + File.separator + ver + File.separator + KSNAME);
        return new Descriptor(ver, directory, KSNAME, CFNAME, 0, SSTableFormat.Type.LEGACY);
    }

    /**
     * Generates a test SSTable for use in this classes' tests. Uncomment and run against an older build
     * and the output will be copied to a version subdirectory in 'LEGACY_SSTABLE_ROOT'
     *
     @Test
     public void buildTestSSTable() throws IOException
     {
     // write the output in a version specific directory
     Descriptor dest = getDescriptor(Descriptor.Version.current_version);
     assert dest.directory.mkdirs() : "Could not create " + dest.directory + ". Might it already exist?";

     SSTableReader ssTable = SSTableUtils.prepare().ks(KSNAME).cf(CFNAME).dest(dest).write(TEST_DATA);
     assert ssTable.descriptor.generation == 0 :
     "In order to create a generation 0 sstable, please run this test alone.";
     System.out.println(">>> Wrote " + dest);
     }
     */

    @Test
    public void testStreaming() throws Throwable
    {
        StorageService.instance.initServer();

        for (File version : LEGACY_SSTABLE_ROOT.listFiles())
        {
            if (Version.validate(version.getName()) && SSTableFormat.Type.LEGACY.info.getVersion(version.getName()).isCompatibleForStreaming())
                testStreaming(version.getName());
        }
    }

    private void testStreaming(String version) throws Exception
    {
        SSTableReader sstable = SSTableReader.open(getDescriptor(version));
        IPartitioner p = sstable.getPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes("100"))));
        ranges.add(new Range<>(p.getToken(ByteBufferUtil.bytes("100")), p.getMinimumToken()));
        ArrayList<StreamSession.SSTableStreamingSections> details = new ArrayList<>();
        details.add(new StreamSession.SSTableStreamingSections(sstable.ref(),
                                                               sstable.getPositionsForRanges(ranges),
                                                               sstable.estimatedKeysForRanges(ranges), sstable.getSSTableMetadata().repairedAt));
        new StreamPlan("LegacyStreamingTest").transferFiles(FBUtilities.getBroadcastAddress(), details)
                                             .execute().get();

        ColumnFamilyStore cfs = Keyspace.open(KSNAME).getColumnFamilyStore(CFNAME);
        assert cfs.getLiveSSTables().size() == 1;
        sstable = cfs.getLiveSSTables().iterator().next();
        for (String keystring : TEST_DATA)
        {
            ByteBuffer key = bytes(keystring);

            SliceableUnfilteredRowIterator iter = sstable.iterator(Util.dk(key), ColumnFilter.selectionBuilder().add(cfs.metadata.getColumnDefinition(bytes("name"))).build(), false, false);

            // check not deleted (CASSANDRA-6527)
            assert iter.partitionLevelDeletion().equals(DeletionTime.LIVE);
            assert iter.next().clustering().get(0).equals(key);
        }
        sstable.selfRef().release();
    }

    @Test
    public void testVersions() throws Throwable
    {
        boolean notSkipped = false;

        for (File version : LEGACY_SSTABLE_ROOT.listFiles())
        {
            if (Version.validate(version.getName()) && SSTableFormat.Type.LEGACY.info.getVersion(version.getName()).isCompatible())
            {
                notSkipped = true;
                testVersion(version.getName());
            }
        }

        assert notSkipped;
    }

    public void testVersion(String version) throws Throwable
    {
        try
        {
            ColumnFamilyStore cfs = Keyspace.open(KSNAME).getColumnFamilyStore(CFNAME);


            SSTableReader reader = SSTableReader.open(getDescriptor(version));
            for (String keystring : TEST_DATA)
            {

                ByteBuffer key = bytes(keystring);

                SliceableUnfilteredRowIterator iter = reader.iterator(Util.dk(key), ColumnFilter.selection(cfs.metadata.partitionColumns()), false, false);

                // check not deleted (CASSANDRA-6527)
                assert iter.partitionLevelDeletion().equals(DeletionTime.LIVE);
                assert iter.next().clustering().get(0).equals(key);
            }

            // TODO actually test some reads
        }
        catch (Throwable e)
        {
            System.err.println("Failed to read " + version);
            throw e;
        }
    }

    @Test
    public void testLegacyCqlTables() throws Exception
    {
        QueryProcessor.executeInternal("CREATE KEYSPACE legacy_tables WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");

        loadLegacyTables();
    }

    private void loadLegacyTables() throws IOException
    {
        for (String legacyVersion : legacyVersions)
        {
            logger.info("Preparing legacy version {}", legacyVersion);

            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_simple (pk text PRIMARY KEY, val text)", legacyVersion));
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_simple_counter (pk text PRIMARY KEY, val counter)", legacyVersion));
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_clust (pk text, ck text, val text, PRIMARY KEY (pk, ck))", legacyVersion));
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_clust_counter (pk text, ck text, val counter, PRIMARY KEY (pk, ck))", legacyVersion));

            loadLegacyTable("legacy_%s_simple", legacyVersion);
            loadLegacyTable("legacy_%s_simple_counter", legacyVersion);
            loadLegacyTable("legacy_%s_clust", legacyVersion);
            loadLegacyTable("legacy_%s_clust_counter", legacyVersion);

            for (int ck = 0; ck < 50; ck++)
            {
                String ckValue = Integer.toString(ck) + longString;
                for (int pk = 0; pk < 5; pk++)
                {
                    String pkValue = Integer.toString(pk);
                    UntypedResultSet rs;
                    if (ck == 0)
                    {
                        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_simple WHERE pk=?", legacyVersion), pkValue);
                        Assert.assertNotNull(rs);
                        Assert.assertEquals(1, rs.size());
                        Assert.assertEquals("foo bar baz", rs.one().getString("val"));
                        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_simple_counter WHERE pk=?", legacyVersion), pkValue);
                        Assert.assertNotNull(rs);
                        Assert.assertEquals(1, rs.size());
                        Assert.assertEquals(1L, rs.one().getLong("val"));
                    }
                    rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust WHERE pk=? AND ck=?", legacyVersion), pkValue, ckValue);
                    Assert.assertNotNull(rs);
                    Assert.assertEquals(1, rs.size());
                    Assert.assertEquals(128, rs.one().getString("val").length());
                    rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust_counter WHERE pk=? AND ck=?", legacyVersion), pkValue, ckValue);
                    Assert.assertNotNull(rs);
                    Assert.assertEquals(1, rs.size());
                    Assert.assertEquals(1L, rs.one().getLong("val"));
                }
            }
        }
    }

    private void loadLegacyTable(String tablePattern, String legacyVersion) throws IOException
    {
        String table = String.format(tablePattern, legacyVersion);

        logger.info("Loading legacy table {}", table);

        ColumnFamilyStore cfs = Keyspace.open("legacy_tables").getColumnFamilyStore(table);

        for (File cfDir : cfs.getDirectories().getCFDirectories())
        {
            copySstables(legacyVersion, table, cfDir);
        }

        cfs.loadNewSSTables();
    }

    private static void copySstables(String legacyVersion, String table, File cfDir) throws IOException
    {
        byte[] buf = new byte[65536];

        for (File file : new File(LEGACY_SSTABLE_ROOT, String.format("%s/legacy_tables/%s", legacyVersion, table)).listFiles())
        {
            if (file.isFile())
            {
                File target = new File(cfDir, file.getName());
                int rd;
                FileInputStream is = new FileInputStream(file);
                FileOutputStream os = new FileOutputStream(target);
                while ((rd = is.read(buf)) >= 0)
                    os.write(buf, 0, rd);
            }
        }
    }
}
