package org.apache.cassandra.cql3;

import junit.framework.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexExpression;
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
        ParsedStatement parsedStatement = QueryProcessor.parseStatement("SELECT * FROM "+keyspace+".test WHERE " +
                "foo = 'abcd' AND (bar = 'xyz' OR bar = 'qwerty');");
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
        Assert.assertTrue(indexExpressions.size() == 17);

        // check that List<IndexExpression> created by collapsing
        // relation tree matches correct Reverse Polish Notation order
        int pos = 0;
        List<String> polishNotationOrderedStmtTokens = toReversePolishNotation(whereClause);
        for (String token : polishNotationOrderedStmtTokens)
        {
            String indexExpressionStr = indexExpressionToString(indexExpressions.get(pos++));
            Assert.assertEquals(token, indexExpressionStr);
        }
    }

    @Test
    public void parseTupleNotationTest() throws Exception
    {
        ParsedStatement parsedStatement = QueryProcessor.parseStatement("SELECT v1, v2, v3 FROM "+keyspace+".test2 " +
                "WHERE k = 0 AND (v1, v2, v3) >= (1, 0, 1)");
        SelectStatement selectStatement = (SelectStatement) parsedStatement.prepare().statement;
    }

    private static String indexExpressionToString(IndexExpression indexExpression)
    {
        try
        {
            if (indexExpression.getLogicalOp() != null)
                return indexExpression.getLogicalOp().name();

            StringBuilder sb = new StringBuilder();
            String colName = ByteBufferUtil.string(indexExpression.column_name);
            sb.append(colName);
            sb.append("=");

            String value;
            if (colName.equals("num1"))
            {
                value = Integer.toString(indexExpression.value.getInt());
            }
            else
            {
                value = ByteBufferUtil.string(indexExpression.value);
            }
            if (Character.isDigit(value.charAt(0)))
            {
                sb.append(value);
            }
            else
            {
                sb.append('\'');
                sb.append(value);
                sb.append('\'');
            }
            return sb.toString();
        }
        catch (Exception e)
        {
            return null;
        }
    }

    private static List<String> toReversePolishNotation(String input)
    {
        Stack<String> stack = new Stack<>();
        List<String> out = new ArrayList<>();

        for (String inVal : input.split(" "))
        {
            switch (inVal)
            {
                case "AND":
                case "OR":
                    while (!stack.empty() && stack.peek().equals("AND"))
                        out.add(stack.pop());
                case "(":
                    stack.push(inVal);
                    break;
                case ")":
                    while (!stack.empty())
                    {
                        if (stack.peek().equals("("))
                        {
                            stack.pop();
                            break;
                        }
                        else
                        {
                            out.add(stack.pop());
                        }
                    }
                    break;
                default:
                    out.add(inVal);
                    break;
            }
        }

        if (!stack.empty())
            while (!stack.isEmpty())
                out.add(stack.pop());

        return out;
    }
}
