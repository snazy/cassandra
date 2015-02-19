package org.apache.cassandra.cql3;

import junit.framework.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.LogicalIndexOperator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.cassandra.cql3.QueryProcessor.process;

public class SelectStatementTest
{
    static String keyspace = "select_statement_test";

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        System.setProperty("cassandra.config", "cassandra-murmur.yaml");
        SchemaLoader.loadSchema(false);
        executeSchemaChange("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        executeSchemaChange("CREATE TABLE %s.test (ts timeuuid, foo text, bar text, first_name text, last_name text, street text, city text, " +
                "state text, country text, num1 int, v1 int, v2 int, v3 int, primary key (ts)) " +
                "WITH COMPACT STORAGE AND compaction = { 'class' : 'LeveledCompactionStrategy' };");
        executeSchemaChange("CREATE CUSTOM INDEX ON %s.test (foo) using 'org.apache.cassandra.db.index.SuffixArraySecondaryIndex';");
        executeSchemaChange("CREATE CUSTOM INDEX ON %s.test (bar) using 'org.apache.cassandra.db.index.SuffixArraySecondaryIndex';");
        executeSchemaChange("CREATE CUSTOM INDEX ON %s.test (first_name) using 'org.apache.cassandra.db.index.SuffixArraySecondaryIndex';");
        executeSchemaChange("CREATE CUSTOM INDEX ON %s.test (last_name) using 'org.apache.cassandra.db.index.SuffixArraySecondaryIndex';");
        executeSchemaChange("CREATE CUSTOM INDEX ON %s.test (street) using 'org.apache.cassandra.db.index.SuffixArraySecondaryIndex';");
        executeSchemaChange("CREATE CUSTOM INDEX ON %s.test (city) using 'org.apache.cassandra.db.index.SuffixArraySecondaryIndex';");
        executeSchemaChange("CREATE CUSTOM INDEX ON %s.test (state) using 'org.apache.cassandra.db.index.SuffixArraySecondaryIndex';");
        executeSchemaChange("CREATE CUSTOM INDEX ON %s.test (country) using 'org.apache.cassandra.db.index.SuffixArraySecondaryIndex';");
        executeSchemaChange("CREATE CUSTOM INDEX ON %s.test (num1) using 'org.apache.cassandra.db.index.SuffixArraySecondaryIndex';");

        executeSchemaChange("CREATE TABLE %s.test2 (k int, v1 int, v2 int, v3 int, PRIMARY KEY (k, v1, v2, v3)) " +
                "WITH COMPACT STORAGE AND compaction = { 'class' : 'LeveledCompactionStrategy' };");
    }

    private static void executeSchemaChange(String query) throws Throwable
    {
        try
        {
            process(String.format(query, keyspace), ConsistencyLevel.ONE);
        } catch (RuntimeException exc)
        {
            throw exc.getCause();
        }
    }

    @Test
    public void parseSimpleORSelectStatementTest() throws Exception
    {
        QueryProcessor.parseStatement("SELECT * FROM test WHERE foo = 'abcd' OR bar = 'xyz';");
    }

    @Test
    public void parseSimpleANDSelectStatementTest() throws Exception
    {
        QueryProcessor.parseStatement("SELECT * FROM test WHERE foo = 'abcd' AND bar = 'xyz';");
    }

    @Test
    public void parseNestedORWithANDSelectStatementTest() throws Exception
    {
        ParsedStatement parsedStatement = QueryProcessor.parseStatement(String.format(
                "SELECT * FROM %s.test WHERE foo = 'abcd' AND (bar = 'xyz' OR bar = 'qwerty') ALLOW FILTERING;", keyspace));
        parsedStatement.prepare();
    }

    @Test
    public void parseComplexMultipleNestedORAndANDSelectTest() throws Exception
    {
        String whereClause = "first_name='Bob' AND ( last_name='Smith' OR num1=25 AND " +
                "street='Infinite_Loop' AND city='Cupertino' ) OR ( foo='this_is_foos_val' " +
                "AND bar='this_is_bars_val' ) OR ( state='CA' AND country='US' ) ";
        ParsedStatement parsedStatement = QueryProcessor.parseStatement("SELECT * FROM "+keyspace+".test " +
                "WHERE " + whereClause + " ALLOW FILTERING;");
        SelectStatement selectStatement = (SelectStatement) parsedStatement.prepare().statement;

        List<IndexExpression> indexExpressions = selectStatement.getIndexExpressions(Collections.<ByteBuffer>emptyList());
        Assert.assertEquals(17, indexExpressions.size());

        List<IndexExpression> expected = new ArrayList<IndexExpression>()
        {{
            add(new IndexExpression().setOp(IndexOperator.EQ).setColumn_name("country".getBytes()).setValue("US".getBytes()));
            add(new IndexExpression().setOp(IndexOperator.EQ).setColumn_name("state".getBytes()).setValue("CA".getBytes()));
            add(makeLogical(LogicalIndexOperator.AND));

            add(new IndexExpression().setOp(IndexOperator.EQ).setColumn_name("bar".getBytes()).setValue("this_is_bars_val".getBytes()));
            add(new IndexExpression().setOp(IndexOperator.EQ).setColumn_name("foo".getBytes()).setValue("this_is_foos_val".getBytes()));
            add(makeLogical(LogicalIndexOperator.AND));

            add(new IndexExpression().setOp(IndexOperator.EQ).setColumn_name("city".getBytes()).setValue("Cupertino".getBytes()));
            add(new IndexExpression().setOp(IndexOperator.EQ).setColumn_name("street".getBytes()).setValue("Infinite_Loop".getBytes()));
            add(makeLogical(LogicalIndexOperator.AND));

            add(new IndexExpression().setOp(IndexOperator.EQ).setColumn_name("num1".getBytes()).setValue(Int32Type.instance.decompose(25)));
            add(makeLogical(LogicalIndexOperator.AND));

            add(new IndexExpression().setOp(IndexOperator.EQ).setColumn_name("last_name".getBytes()).setValue("Smith".getBytes()));
            add(makeLogical(LogicalIndexOperator.OR));

            add(new IndexExpression().setOp(IndexOperator.EQ).setColumn_name("first_name".getBytes()).setValue("Bob".getBytes()));
            add(makeLogical(LogicalIndexOperator.AND));

            add(makeLogical(LogicalIndexOperator.OR));
            add(makeLogical(LogicalIndexOperator.OR));
        }};

        Assert.assertEquals(expected, indexExpressions);
    }

    @Test
    public void parseTupleNotationTest() throws Exception
    {
        ParsedStatement parsedStatement = QueryProcessor.parseStatement("SELECT v1, v2, v3 FROM "+keyspace+".test2 " +
                "WHERE k = 0 AND (v1, v2, v3) >= (1, 0, 1)");
        parsedStatement.prepare();
    }

    private static IndexExpression makeLogical(LogicalIndexOperator op)
    {
        IndexExpression e = new IndexExpression();

        e.setLogicalOp(op);
        e.setOp(IndexOperator.EQ);

        e.setColumn_name(ByteBufferUtil.EMPTY_BYTE_BUFFER);
        e.setValue(ByteBufferUtil.EMPTY_BYTE_BUFFER);

        return e;
    }
}
