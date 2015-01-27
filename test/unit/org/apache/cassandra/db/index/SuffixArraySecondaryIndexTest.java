package org.apache.cassandra.db.index;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.*;

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
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));

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
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));
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

        Assert.assertEquals(rows.toString(), 7, rows.size());

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.LTE, Int32Type.instance.decompose(50)));

        Assert.assertEquals(rows.toString(), 7, rows.size());
    }

    private static ColumnFamilyStore loadData(Map<String, Pair<String, Integer>> data)
    {
        for (Map.Entry<String, Pair<String, Integer>> e : data.entrySet())
            newMutation(e.getKey(), e.getValue().left, e.getValue().right).apply();

        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        store.forceBlockingFlush();

        return store;
    }

    private static Set<String> getIndexed(ColumnFamilyStore store, int maxResults, IndexExpression... expressions)
    {
        return getKeys(store.indexManager.search(ExtendedFilter.create(store,
                                                 DataRange.allData(StorageService.getPartitioner()),
                                                 Arrays.asList(expressions),
                                                 maxResults,
                                                 false,
                                                 System.currentTimeMillis())));
    }

    private static RowMutation newMutation(String key, String firstName, int age)
    {

        RowMutation rm = new RowMutation("SASecondaryIndex", AsciiType.instance.decompose(key));
        long timestamp = System.currentTimeMillis();
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
}
