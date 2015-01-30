package org.apache.cassandra.db.index;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.*;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Test;

public class SuffixArraySecondaryIndexTest extends SchemaLoader
{
    private static final String KS_NAME = "SASecondaryIndex";
    private static final String CF_NAME = "SAIndexed1";

    @After
    public void cleanUp()
    {
        Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME).truncateBlocking();
    }

    @Test
    public void testSingleExpressionQueries() throws Exception
    {
        Map<String, Pair<String, Integer>> data = new HashMap<String, Pair<String, Integer>>()
        {{
            put("key1", Pair.create("Pavel", 14));
            put("key2", Pair.create("Pavel", 26));
            put("key3", Pair.create("Pavel", 27));
            put("key4", Pair.create("Jason", 27));
        }};

        ColumnFamilyStore store = loadData(data);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("av")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("as")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("aw")));
        Assert.assertEquals(rows.toString(), 0, rows.size());

        rows = getIndexed(store, 10, new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(27)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{"key3", "key4"}, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(26)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(13)));
        Assert.assertEquals(rows.toString(), 0, rows.size());
    }

    @Test
    public void testMultiExpressionQueries() throws Exception
    {
        Map<String, Pair<String, Integer>> data = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key1", Pair.create("Pavel", 14));
                put("key2", Pair.create("Pavel", 26));
                put("key3", Pair.create("Pavel", 27));
                put("key4", Pair.create("Jason", 27));
        }};

        ColumnFamilyStore store = loadData(data);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows = getIndexed(store, 10,
                                      new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                                      new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(14)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(27)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{"key1", "key2"}, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                         new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                         new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(14)),
                         new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(27)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                         new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                         new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(12)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key1", "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                         new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                         new IndexExpression(age, IndexOperator.GTE, Int32Type.instance.decompose(13)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                         new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                         new IndexExpression(age, IndexOperator.GTE, Int32Type.instance.decompose(16)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));


        rows = getIndexed(store, 10,
                         new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                         new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(30)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                         new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                         new IndexExpression(age, IndexOperator.LTE, Int32Type.instance.decompose(29)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                         new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                         new IndexExpression(age, IndexOperator.LTE, Int32Type.instance.decompose(25)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key1" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testCrossSSTableQueries() throws Exception
    {
        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key0", Pair.create("Maxie", 43));
                put("key1", Pair.create("Chelsie", 33));
                put("key2", Pair.create("Josephine", 43));
                put("key3", Pair.create("Shanna", 27));
                put("key4", Pair.create("Amiya", 36));
            }};

        loadData(part1); // first sstable

        Map<String, Pair<String, Integer>> part2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key5", Pair.create("Americo", 20));
                put("key6", Pair.create("Fiona", 39));
                put("key7", Pair.create("Francis", 41));
                put("key8", Pair.create("Charley", 21));
                put("key9", Pair.create("Amely", 40));
            }};

        loadData(part2);

        Map<String, Pair<String, Integer>> part3 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key10", Pair.create("Eddie", 42));
                put("key11", Pair.create("Oswaldo", 35));
                put("key12", Pair.create("Susana", 35));
                put("key13", Pair.create("Alivia", 42));
                put("key14", Pair.create("Demario", 28));
            }};

        ColumnFamilyStore store = loadData(part3);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows = getIndexed(store, 10,
                                      new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("Fiona")),
                                      new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(40)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key6" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key0", "key11", "key12", "key13", "key14",
                                                                        "key3", "key4", "key6", "key7", "key8" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 5,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));

        Assert.assertEquals(rows.toString(), 5, rows.size());

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.GTE, Int32Type.instance.decompose(35)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key0", "key11", "key12", "key13", "key4", "key6", "key7" },
                                                         rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(32)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key14", "key3", "key8" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(27)),
                          new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(32)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key14" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(10)));

        Assert.assertEquals(rows.toString(), 10, rows.size());

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.LTE, Int32Type.instance.decompose(50)));

        Assert.assertEquals(rows.toString(), 10, rows.size());
    }

    @Test
    public void testQueriesThatShouldBeTokenized() throws Exception
    {
        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key0", Pair.create("If you can dream it, you can do it.", 43));
                put("key1", Pair.create("What you get by achieving your goals is not " +
                        "as important as what you become by achieving your goals, do it.", 33));
                put("key2", Pair.create("Keep your face always toward the sunshine " +
                        "- and shadows will fall behind you.", 43));
                put("key3", Pair.create("We can't help everyone, but everyone can " +
                        "help someone.", 27));
            }};

        ColumnFamilyStore store = loadData(part1);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows = getIndexed(store, 10,
                new IndexExpression(firstName, IndexOperator.EQ,
                        UTF8Type.instance.decompose("What you get by achieving your goals")),
                new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(32)));

        Assert.assertEquals(rows.toString(), Collections.singleton("key1"), rows);

        rows = getIndexed(store, 10,
                new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("do it.")));

        Assert.assertEquals(rows.toString(), Arrays.asList("key0", "key1"), Lists.newArrayList(rows));
    }

    @Test
    public void testMultiExpressionQueriesWhereRowSplitBetweenSSTables() throws Exception
    {
        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key0", Pair.create("Maxie", -1));
                put("key1", Pair.create("Chelsie", 33));
                put("key2", Pair.create((String)null, 43));
                put("key3", Pair.create("Shanna", 27));
                put("key4", Pair.create("Amiya", 36));
        }};

        loadData(part1); // first sstable

        Map<String, Pair<String, Integer>> part2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key5", Pair.create("Americo", 20));
                put("key6", Pair.create("Fiona", 39));
                put("key7", Pair.create("Francis", 41));
                put("key8", Pair.create("Charley", 21));
                put("key9", Pair.create("Amely", 40));
                put("key14", Pair.create((String)null, 28));
        }};

        loadData(part2);

        Map<String, Pair<String, Integer>> part3 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key0", Pair.create((String)null, 43));
                put("key10", Pair.create("Eddie", 42));
                put("key11", Pair.create("Oswaldo", 35));
                put("key12", Pair.create("Susana", 35));
                put("key13", Pair.create("Alivia", 42));
                put("key14", Pair.create("Demario", -1));
                put("key2", Pair.create("Josephine", -1));
        }};

        ColumnFamilyStore store = loadData(part3);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows = getIndexed(store, 10,
                                      new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("Fiona")),
                                      new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(40)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key6" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key0", "key11", "key12", "key13", "key14",
                                                                        "key3", "key4", "key6", "key7", "key8" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 5,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));

        Assert.assertEquals(rows.toString(), 5, rows.size());

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.GTE, Int32Type.instance.decompose(35)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key0", "key11", "key12", "key13", "key4", "key6", "key7" },
                                                         rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(32)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key14", "key3", "key8" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(27)),
                          new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(32)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key14" }, rows.toArray(new String[rows.size()])));

        Map<String, Pair<String, Integer>> part4 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key12", Pair.create((String)null, 12));
                put("key14", Pair.create("Demario", 42));
                put("key2", Pair.create("Frank", -1));
        }};

        store = loadData(part4);
        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("Susana")),
                          new IndexExpression(age, IndexOperator.LTE, Int32Type.instance.decompose(13)),
                          new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(10)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key12" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("Demario")),
                          new IndexExpression(age, IndexOperator.LTE, Int32Type.instance.decompose(30)));
        Assert.assertTrue(rows.toString(), rows.size() == 0);

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("Josephine")));
        Assert.assertTrue(rows.toString(), rows.size() == 0);

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(10)));

        Assert.assertEquals(rows.toString(), 6, rows.size());

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.LTE, Int32Type.instance.decompose(50)));

        Assert.assertEquals(rows.toString(), 6, rows.size());
    }

    @Test
    public void testPagination()
    {
        // split data into 3 distinct SSTables to test paging with overlapping token intervals.

        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key01", Pair.create("Ali", 33));
                put("key02", Pair.create("Jeremy", 41));
                put("key03", Pair.create("Elvera", 22));
                put("key04", Pair.create("Bailey", 45));
                put("key05", Pair.create("Emerson", 32));
                put("key06", Pair.create("Kadin", 38));
                put("key07", Pair.create("Maggie", 36));
                put("key08", Pair.create("Kailey", 36));
                put("key09", Pair.create("Armand", 21));
                put("key10", Pair.create("Arnold", 35));
        }};

        Map<String, Pair<String, Integer>> part2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key11", Pair.create("Ken", 38));
                put("key12", Pair.create("Penelope", 43));
                put("key13", Pair.create("Wyatt", 34));
                put("key14", Pair.create("Johnpaul", 34));
                put("key15", Pair.create("Trycia", 43));
                put("key16", Pair.create("Aida", 21));
                put("key17", Pair.create("Devon", 42));
        }};

        Map<String, Pair<String, Integer>> part3 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key18", Pair.create("Christina", 20));
                put("key19", Pair.create("Rick", 19));
                put("key20", Pair.create("Fannie", 22));
                put("key21", Pair.create("Keegan", 29));
                put("key22", Pair.create("Ignatius", 36));
                put("key23", Pair.create("Ellis", 26));
                put("key24", Pair.create("Annamarie", 29));
                put("key25", Pair.create("Tianna", 31));
                put("key26", Pair.create("Dennis", 32));
        }};

        ColumnFamilyStore store = loadData(part1);

        loadData(part2);
        loadData(part3);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<DecoratedKey> uniqueKeys = getPaged(store, 4,
                new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                new IndexExpression(age, IndexOperator.GTE, Int32Type.instance.decompose(21)));


        List<String> expected = new ArrayList<String>()
        {{
                add("key25");
                add("key20");
                add("key13");
                add("key22");
                add("key09");
                add("key14");
                add("key16");
                add("key24");
                add("key03");
                add("key04");
                add("key08");
                add("key07");
                add("key15");
                add("key06");
                add("key21");
        }};

        Assert.assertEquals(expected, convert(uniqueKeys));

        // now let's test a single equals condition

        uniqueKeys = getPaged(store, 4, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));

        expected = new ArrayList<String>()
        {{
                add("key25");
                add("key20");
                add("key13");
                add("key22");
                add("key09");
                add("key14");
                add("key16");
                add("key24");
                add("key03");
                add("key04");
                add("key18");
                add("key08");
                add("key07");
                add("key15");
                add("key06");
                add("key21");
        }};

        Assert.assertEquals(expected, convert(uniqueKeys));

        // now let's test something which is smaller than a single page
        uniqueKeys = getPaged(store, 4,
                              new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                              new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(36)));

        expected = new ArrayList<String>()
        {{
                add("key22");
                add("key08");
                add("key07");
        }};

        Assert.assertEquals(expected, convert(uniqueKeys));

        // the same but with the page size of 2 to test minimal pagination windows

        uniqueKeys = getPaged(store, 2,
                              new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                              new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(36)));

        Assert.assertEquals(expected, convert(uniqueKeys));

        // and last but not least, test age range query with pagination

        uniqueKeys = getPaged(store, 4,
                new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(20)),
                new IndexExpression(age, IndexOperator.LTE, Int32Type.instance.decompose(36)));

        expected = new ArrayList<String>()
        {{
                add("key25");
                add("key20");
                add("key13");
                add("key22");
                add("key09");
                add("key14");
                add("key16");
                add("key24");
                add("key03");
                add("key08");
                add("key07");
                add("key21");
        }};

        Assert.assertEquals(expected, convert(uniqueKeys));
    }

    @Test
    public void testColumnNamesWithSlashes()
    {
        RowMutation rm1 = new RowMutation("SASecondaryIndex", AsciiType.instance.decompose("key1"));
        rm1.add("SAIndexed1", ByteBufferUtil.bytes("/data/output/id"), AsciiType.instance.decompose("jason"), System.currentTimeMillis());

        RowMutation rm2 = new RowMutation("SASecondaryIndex", AsciiType.instance.decompose("key2"));
        rm2.add("SAIndexed1", ByteBufferUtil.bytes("/data/output/id"), AsciiType.instance.decompose("pavel"), System.currentTimeMillis());

        RowMutation rm3 = new RowMutation("SASecondaryIndex", AsciiType.instance.decompose("key3"));
        rm3.add("SAIndexed1", ByteBufferUtil.bytes("/data/output/id"), AsciiType.instance.decompose("Aleksey"), System.currentTimeMillis());

        rm1.apply();
        rm2.apply();
        rm3.apply();

        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        store.forceBlockingFlush();

        final ByteBuffer dataOutputId = UTF8Type.instance.decompose("/data/output/id");

        Set<String> rows = getIndexed(store, 10, new IndexExpression(dataOutputId, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(dataOutputId, IndexOperator.EQ, UTF8Type.instance.decompose("A")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key3" }, rows.toArray(new String[rows.size()])));

        store.indexManager.removeIndexedColumn(dataOutputId);

        rows = getIndexed(store, 10, new IndexExpression(dataOutputId, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), rows.isEmpty());

        rows = getIndexed(store, 10, new IndexExpression(dataOutputId, IndexOperator.EQ, UTF8Type.instance.decompose("A")));
        Assert.assertTrue(rows.toString(), rows.isEmpty());
    }

    @Test
    public void testInvalidate() throws Exception
    {
        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key0", Pair.create("Maxie", -1));
                put("key1", Pair.create("Chelsie", 33));
                put("key2", Pair.create((String) null, 43));
                put("key3", Pair.create("Shanna", 27));
                put("key4", Pair.create("Amiya", 36));
        }};

        ColumnFamilyStore store = loadData(part1);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key0", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(33)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key1" }, rows.toArray(new String[rows.size()])));

        store.indexManager.invalidate();

        rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), rows.isEmpty());

        rows = getIndexed(store, 10, new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(33)));
        Assert.assertTrue(rows.toString(), rows.isEmpty());


        Map<String, Pair<String, Integer>> part2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key5", Pair.create("Americo", 20));
                put("key6", Pair.create("Fiona", 39));
                put("key7", Pair.create("Francis", 41));
                put("key8", Pair.create("Fred", 21));
                put("key9", Pair.create("Amely", 40));
                put("key14", Pair.create("Dino", 28));
        }};

        loadData(part2);

        rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key6", "key7" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(40)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key9" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testTruncate()
    {
        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key01", Pair.create("Ali", 33));
                put("key02", Pair.create("Jeremy", 41));
                put("key03", Pair.create("Elvera", 22));
                put("key04", Pair.create("Bailey", 45));
                put("key05", Pair.create("Emerson", 32));
                put("key06", Pair.create("Kadin", 38));
                put("key07", Pair.create("Maggie", 36));
                put("key08", Pair.create("Kailey", 36));
                put("key09", Pair.create("Armand", 21));
                put("key10", Pair.create("Arnold", 35));
        }};

        Map<String, Pair<String, Integer>> part2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key11", Pair.create("Ken", 38));
                put("key12", Pair.create("Penelope", 43));
                put("key13", Pair.create("Wyatt", 34));
                put("key14", Pair.create("Johnpaul", 34));
                put("key15", Pair.create("Trycia", 43));
                put("key16", Pair.create("Aida", 21));
                put("key17", Pair.create("Devon", 42));
        }};

        Map<String, Pair<String, Integer>> part3 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key18", Pair.create("Christina", 20));
                put("key19", Pair.create("Rick", 19));
                put("key20", Pair.create("Fannie", 22));
                put("key21", Pair.create("Keegan", 29));
                put("key22", Pair.create("Ignatius", 36));
                put("key23", Pair.create("Ellis", 26));
                put("key24", Pair.create("Annamarie", 29));
                put("key25", Pair.create("Tianna", 31));
                put("key26", Pair.create("Dennis", 32));
        }};

        ColumnFamilyStore store = loadData(part1, 1000);

        loadData(part2, 2000);
        loadData(part3, 3000);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");

        Set<String> rows = getIndexed(store, 100, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertEquals(rows.toString(), 16, rows.size());

        // make sure we don't prematurely delete anything
        for (SecondaryIndex index : store.indexManager.getIndexes())
            index.truncateBlocking(500);

        rows = getIndexed(store, 100, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertEquals(rows.toString(), 16, rows.size());

        for (SecondaryIndex index : store.indexManager.getIndexes())
            index.truncateBlocking(1500);

        rows = getIndexed(store, 100, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertEquals(rows.toString(), 10, rows.size());

        for (SecondaryIndex index : store.indexManager.getIndexes())
            index.truncateBlocking(2500);

        rows = getIndexed(store, 100, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertEquals(rows.toString(), 6, rows.size());

        for (SecondaryIndex index : store.indexManager.getIndexes())
            index.truncateBlocking(3500);

        rows = getIndexed(store, 100, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertEquals(rows.toString(), 0, rows.size());

        // add back in some data just to make sure it all still works
        Map<String, Pair<String, Integer>> part4 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key40", Pair.create("Tianna", 31));
                put("key41", Pair.create("Dennis", 32));
        }};

        loadData(part4, 4000);

        rows = getIndexed(store, 100, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertEquals(rows.toString(), 1, rows.size());
    }
    private static ColumnFamilyStore loadData(Map<String, Pair<String, Integer>> data)
    {
        return loadData(data, System.currentTimeMillis());
    }

    private static ColumnFamilyStore loadData(Map<String, Pair<String, Integer>> data, long timestamp)
    {
        for (Map.Entry<String, Pair<String, Integer>> e : data.entrySet())
            newMutation(e.getKey(), e.getValue().left, e.getValue().right, timestamp).apply();

        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        store.forceBlockingFlush();

        return store;
    }

    private static Set<String> getIndexed(ColumnFamilyStore store, int maxResults, IndexExpression... expressions)
    {
        return getKeys(getIndexed(store, null, maxResults, expressions));
    }

    private static Set<DecoratedKey> getPaged(ColumnFamilyStore store, int pageSize, IndexExpression... expressions)
    {
        List<Row> currentPage;
        Set<DecoratedKey> uniqueKeys = new TreeSet<>();

        DecoratedKey lastKey = null;
        do
        {
            currentPage = getIndexed(store, lastKey, pageSize, expressions);

            if (currentPage == null)
                break;

            for (Row row : currentPage)
                uniqueKeys.add(row.key);

            Row lastRow = Iterators.getLast(currentPage.iterator(), null);
            if (lastRow == null)
                break;

            lastKey = lastRow.key;
        }
        while (currentPage.size() == pageSize);

        return uniqueKeys;
    }

    private static List<Row> getIndexed(ColumnFamilyStore store, DecoratedKey startKey, int maxResults, IndexExpression... expressions)
    {
        IPartitioner p = StorageService.getPartitioner();
        AbstractBounds<RowPosition> bounds;

        if (startKey == null)
        {
            bounds = new Range<>(p.getMinimumToken(), p.getMinimumToken()).toRowBounds();
        }
        else
        {
            bounds = new Bounds<>(startKey, p.getMinimumToken().maxKeyBound(p));
        }

        return store.indexManager.search(ExtendedFilter.create(store,
                                         new DataRange(bounds, new IdentityQueryFilter()),
                                         Arrays.asList(expressions),
                                         maxResults,
                                         false,
                                         System.currentTimeMillis()));
    }

    private static RowMutation newMutation(String key, String firstName, int age)
    {
        return newMutation(key, firstName, age, System.currentTimeMillis());
    }

    private static RowMutation newMutation(String key, String firstName, int age, long timestamp)
    {
        RowMutation rm = new RowMutation("SASecondaryIndex", AsciiType.instance.decompose(key));
        if (firstName != null)
            rm.add("SAIndexed1", ByteBufferUtil.bytes("first_name"), UTF8Type.instance.decompose(firstName), timestamp);
        if (age >= 0)
            rm.add("SAIndexed1", ByteBufferUtil.bytes("age"), Int32Type.instance.decompose(age), timestamp);

        return rm;
    }

    private static Set<String> getKeys(final List<Row> rows)
    {
        return new TreeSet<String>()
        {{
            for (Row r : rows)
                add(AsciiType.instance.compose(r.key.key));
        }};
    }

    private static List<String> convert(final Set<DecoratedKey> keys)
    {
        return new ArrayList<String>()
        {{
            for (DecoratedKey key : keys)
                add(AsciiType.instance.getString(key.key));
        }};
    }
}
