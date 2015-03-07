package org.apache.cassandra.db.index.search.plan;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Sets;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.SuffixArraySecondaryIndex;
import org.apache.cassandra.db.index.utils.LazyMergeSortIterator.OperationType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;

import junit.framework.Assert;
import org.apache.cassandra.thrift.LogicalIndexOperator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.BeforeClass;
import org.junit.Test;

public class OperationTest extends SchemaLoader
{
    @BeforeClass
    public static void loadSchema() throws IOException, ConfigurationException
    {
        System.setProperty("cassandra.config", "cassandra-murmur.yaml");
        loadSchema(false);
    }

    @Test
    public void testAnalyze()
    {
        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        ColumnFamilyStore store = Keyspace.open("sasecondaryindex").getColumnFamilyStore("saindexed1");

        SuffixArraySecondaryIndex index = (SuffixArraySecondaryIndex) store.indexManager.getIndexForColumn(UTF8Type.instance.decompose("first_name"));

        // age != 5 AND age > 1 AND age != 6 AND age <= 10
        Map<Expression.Op, Expression> expressions = convert(Operation.analyzeGroup(index, UTF8Type.instance, OperationType.AND,
                                                                                Arrays.asList(new IndexExpression(age, IndexOperator.NOT_EQ, Int32Type.instance.decompose(5)),
                                                                                              new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(1)),
                                                                                              new IndexExpression(age, IndexOperator.NOT_EQ, Int32Type.instance.decompose(6)),
                                                                                              new IndexExpression(age, IndexOperator.LTE, Int32Type.instance.decompose(10)))));

        Expression expected = new Expression(age, UTF8Type.instance, Int32Type.instance)
        {{
            operation = Op.RANGE;
            lower = new Bound(Int32Type.instance.decompose(1), false);
            upper = new Bound(Int32Type.instance.decompose(10), true);

            exclusions.add(Int32Type.instance.decompose(5));
            exclusions.add(Int32Type.instance.decompose(6));
        }};

        Assert.assertEquals(1, expressions.size());
        Assert.assertEquals(expected, expressions.get(Expression.Op.RANGE));

        // age != 5 OR age >= 7
        expressions = convert(Operation.analyzeGroup(index, UTF8Type.instance, OperationType.OR,
                        Arrays.asList(new IndexExpression(age, IndexOperator.NOT_EQ, Int32Type.instance.decompose(5)),
                                      new IndexExpression(age, IndexOperator.GTE, Int32Type.instance.decompose(7)))));
        Assert.assertEquals(2, expressions.size());

        Assert.assertEquals(new Expression(age, UTF8Type.instance, Int32Type.instance)
                            {{
                                    operation = Op.NOT_EQ;
                                    exclusions.add(Int32Type.instance.decompose(5));
                            }}, expressions.get(Expression.Op.NOT_EQ));

        Assert.assertEquals(new Expression(age, UTF8Type.instance, Int32Type.instance)
                            {{
                                    operation = Op.RANGE;
                                    lower = new Bound(Int32Type.instance.decompose(7), true);
                            }}, expressions.get(Expression.Op.RANGE));

        // age != 5 OR age < 7
        expressions = convert(Operation.analyzeGroup(index, UTF8Type.instance, OperationType.OR,
                        Arrays.asList(new IndexExpression(age, IndexOperator.NOT_EQ, Int32Type.instance.decompose(5)),
                                      new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(7)))));

        Assert.assertEquals(2, expressions.size());
        Assert.assertEquals(new Expression(age, UTF8Type.instance, Int32Type.instance)
                            {{
                                    operation = Op.RANGE;
                                    upper = new Bound(Int32Type.instance.decompose(7), false);
                            }}, expressions.get(Expression.Op.RANGE));
        Assert.assertEquals(new Expression(age, UTF8Type.instance, Int32Type.instance)
                            {{
                                    operation = Op.NOT_EQ;
                                    exclusions.add(Int32Type.instance.decompose(5));
                            }}, expressions.get(Expression.Op.NOT_EQ));

        // age > 1 AND age < 7
        expressions = convert(Operation.analyzeGroup(index, UTF8Type.instance, OperationType.AND,
                        Arrays.asList(new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(1)),
                                      new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(7)))));

        Assert.assertEquals(1, expressions.size());
        Assert.assertEquals(new Expression(age, UTF8Type.instance, Int32Type.instance)
                            {{
                                    operation = Op.RANGE;
                                    lower = new Bound(Int32Type.instance.decompose(1), false);
                                    upper = new Bound(Int32Type.instance.decompose(7), false);
                            }}, expressions.get(Expression.Op.RANGE));

        // first_name = 'a' OR first_name != 'b'
        expressions = convert(Operation.analyzeGroup(index, UTF8Type.instance, OperationType.OR,
                        Arrays.asList(new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                                      new IndexExpression(firstName, IndexOperator.NOT_EQ, UTF8Type.instance.decompose("b")))));

        Assert.assertEquals(2, expressions.size());
        Assert.assertEquals(new Expression(firstName, UTF8Type.instance, UTF8Type.instance)
                            {{
                                    operation = Op.NOT_EQ;
                                    exclusions.add(UTF8Type.instance.decompose("b"));
                            }}, expressions.get(Expression.Op.NOT_EQ));
        Assert.assertEquals(new Expression(firstName, UTF8Type.instance, UTF8Type.instance)
                            {{
                                    operation = Op.EQ;
                                    lower = upper = new Bound(UTF8Type.instance.decompose("a"), true);
                            }}, expressions.get(Expression.Op.EQ));
    }

    @Test
    public void testSatisfiedBy() throws Exception
    {
        final ByteBuffer timestamp = UTF8Type.instance.decompose("timestamp");
        final ByteBuffer age = UTF8Type.instance.decompose("age");
        final IPartitioner<?> partitioner = StorageService.getPartitioner();

        ColumnFamilyStore store = Keyspace.open("sasecondaryindex").getColumnFamilyStore("saindexed1");

        SuffixArraySecondaryIndex index = (SuffixArraySecondaryIndex) store.indexManager.getIndexForColumn(UTF8Type.instance.decompose("first_name"));

        // first let's check that if we haven't called complete it produces false even with valid data
        // simple - age != 5
        Operation op = new Operation(OperationType.AND, UTF8Type.instance, new IndexExpression(age, IndexOperator.NOT_EQ, Int32Type.instance.decompose(5)));

        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.addColumn(new Column(age, Int32Type.instance.decompose(6), System.currentTimeMillis()));

        Assert.assertFalse(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf)));

        // now .complete and it should produce succeed
        op.complete(index);

        Assert.assertTrue(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf)));

        cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.addColumn(new Column(age, Int32Type.instance.decompose(5), System.currentTimeMillis()));

        // and reject incorrect value
        Assert.assertFalse(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf)));

        cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.addColumn(new Column(age, Int32Type.instance.decompose(6), System.currentTimeMillis()));

        Assert.assertTrue(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf)));

        // range with exclusions - age != 5 AND age > 1 AND age != 6 AND age <= 10
        op = new Operation(OperationType.AND, UTF8Type.instance, new IndexExpression(age, IndexOperator.NOT_EQ, Int32Type.instance.decompose(5)),
                                                                 new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(1)),
                                                                 new IndexExpression(age, IndexOperator.NOT_EQ, Int32Type.instance.decompose(6)),
                                                                 new IndexExpression(age, IndexOperator.LTE, Int32Type.instance.decompose(10)));
        op.complete(index);

        Set<Integer> exclusions = Sets.newHashSet(0, 1, 5, 6, 11);
        for (int i = 0; i <= 11; i++)
        {
            cf = ArrayBackedSortedColumns.factory.create(store.metadata);
            cf.addColumn(new Column(age, Int32Type.instance.decompose(i), System.currentTimeMillis()));

            boolean result = op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf));
            Assert.assertTrue(exclusions.contains(i) ? !result : result);
        }

        // now let's do something more complex - age = 5 OR age = 6
        op = new Operation(OperationType.OR, UTF8Type.instance);
        op.setLeft(new Operation(OperationType.AND, UTF8Type.instance, new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(5))));
        op.setRight(new Operation(OperationType.AND, UTF8Type.instance, new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(6))));

        op.complete(index);

        exclusions = Sets.newHashSet(0, 1, 2, 3, 4, 7, 8, 9, 10);
        for (int i = 0; i <= 10; i++)
        {
            cf = ArrayBackedSortedColumns.factory.create(store.metadata);
            cf.addColumn(new Column(age, Int32Type.instance.decompose(i), System.currentTimeMillis()));

            boolean result = op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf));
            Assert.assertTrue(exclusions.contains(i) ? !result : result);
        }

        // now let's test aggregated AND commands
        op = new Operation(OperationType.AND, UTF8Type.instance);

        // logical should be ignored by analyzer, but we still what to make sure that it is
        IndexExpression logical = new IndexExpression(ByteBufferUtil.EMPTY_BYTE_BUFFER, IndexOperator.EQ, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        logical.setLogicalOp(LogicalIndexOperator.AND);

        op.add(logical);
        op.add(new IndexExpression(age, IndexOperator.GTE, Int32Type.instance.decompose(0)));
        op.add(new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(10)));
        op.add(new IndexExpression(age, IndexOperator.NOT_EQ, Int32Type.instance.decompose(7)));

        op.complete(index);

        exclusions = Sets.newHashSet(7);
        for (int i = 0; i < 10; i++)
        {
            cf = ArrayBackedSortedColumns.factory.create(store.metadata);
            cf.addColumn(new Column(age, Int32Type.instance.decompose(i), System.currentTimeMillis()));

            boolean result = op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf));
            Assert.assertTrue(exclusions.contains(i) ? !result : result);
        }

        // multiple analyzed expressions in the Operation timestamp >= 10 AND age = 5
        op = new Operation(OperationType.AND, UTF8Type.instance);
        op.add(new IndexExpression(timestamp, IndexOperator.GTE, LongType.instance.decompose(10L)));
        op.add(new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(5)));

        op.complete(index);

        cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.addColumn(new Column(age, Int32Type.instance.decompose(6), System.currentTimeMillis()));
        cf.addColumn(new Column(timestamp, LongType.instance.decompose(11L), System.currentTimeMillis()));

        Assert.assertFalse(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf)));

        cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.addColumn(new Column(age, Int32Type.instance.decompose(5), System.currentTimeMillis()));
        cf.addColumn(new Column(timestamp, LongType.instance.decompose(22L), System.currentTimeMillis()));

        Assert.assertTrue(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf)));

        cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.addColumn(new Column(age, Int32Type.instance.decompose(5), System.currentTimeMillis()));
        cf.addColumn(new Column(timestamp, LongType.instance.decompose(9L), System.currentTimeMillis()));

        Assert.assertFalse(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf)));

        // operation with internal expressions and right child
        op = new Operation(OperationType.OR, UTF8Type.instance, new IndexExpression(timestamp, IndexOperator.GT, LongType.instance.decompose(10L)));
        op.setRight(new Operation(OperationType.AND, UTF8Type.instance, new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(0)),
                                                                        new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(10))));
        op.complete(index);

        cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.addColumn(new Column(age, Int32Type.instance.decompose(5), System.currentTimeMillis()));
        cf.addColumn(new Column(timestamp, LongType.instance.decompose(9L), System.currentTimeMillis()));

        Assert.assertTrue(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf)));

        cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.addColumn(new Column(age, Int32Type.instance.decompose(20), System.currentTimeMillis()));
        cf.addColumn(new Column(timestamp, LongType.instance.decompose(11L), System.currentTimeMillis()));

        Assert.assertTrue(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf)));

        cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.addColumn(new Column(age, Int32Type.instance.decompose(0), System.currentTimeMillis()));
        cf.addColumn(new Column(timestamp, LongType.instance.decompose(9L), System.currentTimeMillis()));

        Assert.assertFalse(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf)));

        // and for desert let's try out null and deleted rows etc.
        op = new Operation(OperationType.AND, UTF8Type.instance);
        op.add(new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(30)));
        op.complete(index);

        Assert.assertFalse(op.satisfiedBy(null));
        Assert.assertFalse(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), null)));

        long now = System.currentTimeMillis();

        cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.setDeletionInfo(new DeletionInfo(now - 10, (int) (now / 1000)));
        cf.addColumn(new Column(age, Int32Type.instance.decompose(6), System.currentTimeMillis()));

        Assert.assertFalse(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf)));

        cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.addColumn(new DeletedColumn(age, Int32Type.instance.decompose(6), System.currentTimeMillis()));

        Assert.assertFalse(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf)));

        try
        {
            cf = ArrayBackedSortedColumns.factory.create(store.metadata);
            Assert.assertFalse(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf)));
        }
        catch (IllegalStateException e)
        {
            // expected
        }
    }

    private Map<Expression.Op, Expression> convert(List<Expression> expressions)
    {
        Map<Expression.Op, Expression> converted = new HashMap<>();
        for (Expression expression : expressions)
        {
            Expression column = converted.get(expression.getOp());
            assert column == null; // sanity check
            converted.put(expression.getOp(), expression);
        }

        return converted;
    }
}
