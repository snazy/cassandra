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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowAndDeletionMergeIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.FBUtilities;

import org.junit.Assert;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

public class RowIndexEntryTest extends CQLTester
{
    private static final Version[] VERSIONS_NOT_LATEST = {
        BigFormat.instance.getVersion("la"),
        BigFormat.instance.getVersion("ka"),
        BigFormat.instance.getVersion("jb"),
    };
    private static final Version[] VERSIONS_STORE_ROWS = {
        BigFormat.instance.getVersion("ma")
    };


    private static final List<AbstractType<?>> clusterTypes = Collections.<AbstractType<?>>singletonList(LongType.instance);
    private static final ClusteringComparator comp = new ClusteringComparator(clusterTypes);
    private static ClusteringPrefix cn(long l)
    {
        return Util.clustering(comp, l);
    }

    @Test
    public void testArtificialIndexOf() throws IOException
    {
        CFMetaData cfMeta = CFMetaData.compile("CREATE TABLE pipe.dev_null (pk bigint, ck bigint, val text, PRIMARY KEY(pk, ck))", "foo");

        DeletionTime deletionInfo = new DeletionTime(FBUtilities.timestampMicros(), FBUtilities.nowInSeconds());

        DataOutputBuffer dob = new DataOutputBuffer();
        dob.writeLong(42L);
        dob.writeInt(0);
        DeletionTime.serializer.serialize(DeletionTime.LIVE, dob);
        IndexInfo.Serializer indexSerializer = cfMeta.serializers().indexSerializer(BigFormat.latestVersion);
        dob.writeInt(3);
        int off0 = dob.getLength() - 12;
        indexSerializer.serialize(new IndexInfo(cn(0L), cn(5L), 0, 0, deletionInfo), dob, cfMeta.clusteringTypes());
        int off1 = dob.getLength() - 12;
        indexSerializer.serialize(new IndexInfo(cn(10L), cn(15L), 0, 0, deletionInfo), dob, cfMeta.clusteringTypes());
        int off2 = dob.getLength() - 12;
        indexSerializer.serialize(new IndexInfo(cn(20L), cn(25L), 0, 0, deletionInfo), dob, cfMeta.clusteringTypes());
        dob.writeInt(off0);
        dob.writeInt(off1);
        dob.writeInt(off2);
        ByteBuffer buf = dob.buffer();
        buf.putInt(8, buf.limit() - 12);

        RowIndexEntry rie = cfMeta.serializers().getRowIndexSerializer(BigFormat.latestVersion).deserialize(new DataInputBuffer(buf, false));

        Assert.assertEquals(0, rie.indexOf(cn(-1L), comp, false, -1));
        Assert.assertEquals(0, rie.indexOf(cn(5L), comp, false, -1));
        Assert.assertEquals(1, rie.indexOf(cn(12L), comp, false, -1));
        Assert.assertEquals(2, rie.indexOf(cn(17L), comp, false, -1));
        Assert.assertEquals(3, rie.indexOf(cn(100L), comp, false, -1));
        Assert.assertEquals(3, rie.indexOf(cn(100L), comp, false, 0));
        Assert.assertEquals(3, rie.indexOf(cn(100L), comp, false, 1));
        Assert.assertEquals(3, rie.indexOf(cn(100L), comp, false, 2));
        Assert.assertEquals(3, rie.indexOf(cn(100L), comp, false, 3));

        Assert.assertEquals(-1, rie.indexOf(cn(-1L), comp, true, -1));
        Assert.assertEquals(0, rie.indexOf(cn(5L), comp, true, 3));
        Assert.assertEquals(0, rie.indexOf(cn(5L), comp, true, 2));
        Assert.assertEquals(1, rie.indexOf(cn(17L), comp, true, 3));
        Assert.assertEquals(2, rie.indexOf(cn(100L), comp, true, 3));
        Assert.assertEquals(2, rie.indexOf(cn(100L), comp, true, 4));
        Assert.assertEquals(1, rie.indexOf(cn(12L), comp, true, 3));
        Assert.assertEquals(1, rie.indexOf(cn(12L), comp, true, 2));
        Assert.assertEquals(1, rie.indexOf(cn(100L), comp, true, 1));
        Assert.assertEquals(2, rie.indexOf(cn(100L), comp, true, 2));
    }

    @Test
    public void testIndexOf() throws Throwable
    {
        for (Version version : VERSIONS_STORE_ROWS)
        {
            File tempFile = File.createTempFile("row_index_entry_test-indexOf-" + version.getVersion(), null);
            tempFile.deleteOnExit();
            try (SequentialWriter writer = SequentialWriter.open(tempFile))
            {
                CFMetaData cfMeta = CFMetaData.compile("CREATE TABLE foo.bar (pk text, ck int, val text, PRIMARY KEY(pk, ck))", "foo");
                SerializationHeader header = new SerializationHeader(cfMeta,
                                                                     cfMeta.partitionColumns(),
                                                                     EncodingStats.NO_STATS);

                DecoratedKey partitionKey = Util.dk("baz");
                ColumnDefinition columnVal = cfMeta.getColumnDefinition(UTF8Type.instance.fromString("val"));

                PartitionUpdate update = new PartitionUpdate(cfMeta, partitionKey, cfMeta.partitionColumns(), 1);
                for (int i = 0; i < 50000; i++)
                    update.add(BTreeRow.singleCellRow(update.metadata().comparator.make(i),
                                                      BufferCell.live(cfMeta,
                                                                      columnVal,
                                                                      0L,
                                                                      ((AbstractType)columnVal.cellValueType()).decompose(Integer.toOctalString(i)))));
                Iterator<Row> rows = update.iterator();

                UnfilteredRowIterator iterator = new RowAndDeletionMergeIterator(cfMeta,
                                                                                 partitionKey,
                                                                                 DeletionTime.LIVE,
                                                                                 ColumnFilter.all(cfMeta),
                                                                                 Rows.EMPTY_STATIC_ROW,
                                                                                 false,
                                                                                 EncodingStats.NO_STATS,
                                                                                 rows,
                                                                                 Collections.emptyIterator(),
                                                                                 true);

                RowIndexEntry rie = RowIndexEntry.buildIndex(iterator,
                                                             writer,
                                                             header,
                                                             version,
                                                             cfMeta,
                                                             42L,
                                                             DeletionTime.LIVE);
                assertTrue(rie.isIndexed());
                assertEquals(259, rie.indexCount()); // measured value

                // existing clustering keys
                for (int keyToFind = 0; keyToFind < 50000; keyToFind++)
                {
                    Clustering value = new Clustering(Int32Type.instance.decompose(keyToFind));
                    int i = rie.indexOf(value,
                                        cfMeta.comparator,
                                        false,
                                        0);
                    assert i != -1;
                    assertTrue("indexOf()=" + i + " out of range [0.." + rie.indexCount() + "[ for keyToFind=" + keyToFind, i >= 0 && i < rie.indexCount());
                    IndexInfo ii = rie.indexInfo(i);
                    assertTrue("keyToFind:" + keyToFind, cfMeta.comparator.compare(ii.getFirstName(), value) <= 0);
                    assertTrue("keyToFind:" + keyToFind, cfMeta.comparator.compare(ii.getLastName(), value) >= 0);
                }
                // non-existing clustering keys
                for (int keyToFind : new int[]{ -1, 50000, 100000 })
                {
                    Clustering value = new Clustering(Int32Type.instance.decompose(keyToFind));
                    int i = rie.indexOf(value,
                                        cfMeta.comparator,
                                        false,
                                        0);
                    if (i < 0 || i >= rie.indexCount())
                        continue;
                    IndexInfo ii = rie.indexInfo(i);
                    assertTrue("keyToFind:" + keyToFind, cfMeta.comparator.compare(ii.getFirstName(), value) > 0 ||
                                                         cfMeta.comparator.compare(ii.getLastName(), value) < 0);
                }

//                for (Version otherVersion : VERSIONS_NOT_LATEST)
//                {
//                    try
//                    {
//                        DataOutputBuffer dop = new DataOutputBuffer(rie.nativeSize());
//                        rie.serialize(otherVersion, dop);
//
//                        RowIndexEntry rieOther = new RowIndexEntry.Serializer(otherVersion, header)
//                                                 .deserialize(new DataInputBuffer(dop.buffer(), false));
//                    }
//                    catch (Throwable t)
//                    {
//                        throw new RuntimeException("error with primary version " + version + " using serialization version " + otherVersion, t);
//                    }
//                }
            }
        }
    }

    @Test
    public void testSerializedSize() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int, b text, c int, PRIMARY KEY(a, b))");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);

        final RowIndexEntry simple = new RowIndexEntry(123);

        DataOutputBuffer buffer = new DataOutputBuffer();
        SerializationHeader header = new SerializationHeader(cfs.metadata, cfs.metadata.partitionColumns(), EncodingStats.NO_STATS);
        RowIndexEntry.IndexSerializer serializer = cfs.metadata.serializers().getRowIndexSerializer(BigFormat.latestVersion);

        serializer.serialize(simple, buffer);

        assertEquals(12, buffer.getLength()); // as of Cassandra 3.0

        RowIndexEntry reserialized = reserialize(cfs.metadata, simple, header);
        assertFalse(reserialized.isIndexed());
        assertEquals(12, serializedSize(cfs.metadata, reserialized));
        assertEquals(simple.getPosition(), reserialized.getPosition());

        //

        PartitionUpdate update = new PartitionUpdate(cfs.metadata, Int32Type.instance.decompose(42), cfs.metadata.partitionColumns(), 1);
        ColumnDefinition columnVal = cfs.metadata.getColumnDefinition(UTF8Type.instance.fromString("c"));
        update.add(BTreeRow.singleCellRow(update.metadata().comparator.make("42"),
                                          BufferCell.live(cfs.metadata,
                                                          columnVal,
                                                          0L,
                                                          ((AbstractType)columnVal.cellValueType()).decompose(42))));
        RowAndDeletionMergeIterator iterator = new RowAndDeletionMergeIterator(cfs.metadata,
                                                                               Util.dk(UTF8Type.instance.decompose("42")),
                                                                               DeletionTime.LIVE,
                                                                               ColumnFilter.all(cfs.metadata),
                                                                               Rows.EMPTY_STATIC_ROW,
                                                                               false,
                                                                               EncodingStats.NO_STATS,
                                                                               update.iterator(),
                                                                               Collections.emptyIterator(),
                                                                               true);
        File tempFile = File.createTempFile("row_index_entry_test_empty", null);
        tempFile.deleteOnExit();
        SequentialWriter writer = SequentialWriter.open(tempFile);
        RowIndexEntry withoutIndex = RowIndexEntry.buildIndex(iterator, writer, header, BigFormat.latestVersion,
                                                              cfs.metadata, 0xbeefdead, DeletionTime.LIVE);
        assertFalse(withoutIndex.isIndexed());
        assertEquals(12, serializedSize(cfs.metadata, withoutIndex));

        //

        // write enough rows to ensure we get a few column index entries
        for (int i = 0; i <= DatabaseDescriptor.getColumnIndexSize() / 4; i++)
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, "" + i, i);

        buffer = new DataOutputBuffer();
        ImmutableBTreePartition partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs).build());

        tempFile = File.createTempFile("row_index_entry_test", null);
        tempFile.deleteOnExit();
        writer = SequentialWriter.open(tempFile);
        RowIndexEntry withIndex = RowIndexEntry.buildIndex(partition.unfilteredIterator(), writer, header, BigFormat.latestVersion,
                                                           cfs.metadata, 0xdeadbeef, DeletionTime.LIVE);

        // sanity check
        assertTrue(withIndex.indexCount() >= 3);

        serializer.serialize(withIndex, buffer);
        assertEquals(169 + withIndex.indexCount() * 4, // C* 3.0: raw length is 169 bytes + 4 bytes per IndexInfo offset
                     buffer.getLength());

        reserialized = reserialize(cfs.metadata, withIndex, header);
        assertTrue(reserialized.isIndexed());
        assertEquals(buffer.getLength(), serializedSize(cfs.metadata, reserialized));
        assertEquals(withIndex.getPosition(), reserialized.getPosition());
        assertEquals(withIndex.deletionTime(), reserialized.deletionTime());
        assertEquals(withIndex.indexCount(), reserialized.indexCount());
        for (int i = 0; i < reserialized.indexCount(); i++)
        {
            assertEquals(withIndex.indexInfo(i), reserialized.indexInfo(i));
        }
    }

    private static int serializedSize(CFMetaData metadata, RowIndexEntry rie)
    {
        return metadata.serializers().getRowIndexSerializer(BigFormat.latestVersion).serializedSize(rie);
    }

    private static RowIndexEntry reserialize(CFMetaData metadata, RowIndexEntry rie, SerializationHeader header) throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer();
        metadata.serializers().getRowIndexSerializer(BigFormat.latestVersion).serialize(rie, out);
        return metadata.serializers().getRowIndexSerializer(BigFormat.latestVersion).deserialize(new DataInputBuffer(out.buffer(), false));
    }
}
