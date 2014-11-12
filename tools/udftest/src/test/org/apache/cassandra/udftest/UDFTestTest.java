package org.apache.cassandra.udftest;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.exceptions.InvalidRequestException;

public class UDFTestTest
{
    @Test
    public void basicTest() throws Throwable
    {
        UDF udf = UDFs.create("CREATE FUNCTION foo(inp int, d double) RETURNS text LANGUAGE java AS 'return \"foo\";'");
        udf.penetrate();
    }

    @Test(expected = InvalidRequestException.class)
    public void npeTest() throws Throwable
    {
        UDF udf = UDFs.create("CREATE FUNCTION foo(inp int, d double) RETURNS text LANGUAGE java AS 'return inp.toString() + d;'");
        udf.penetrate();
    }

    @Test
    public void executionTest() throws Throwable
    {
        UDF udf = UDFs.create("CREATE FUNCTION foo(inp int, d double) RETURNS text LANGUAGE java AS 'return \"x\" + inp + \"x\" + d + \"x\";'");
        Assert.assertEquals("xnullxnullx", udf.execute(null, null));
        Assert.assertEquals("x5x9.1x", udf.execute(5, 9.1d));
    }
}
