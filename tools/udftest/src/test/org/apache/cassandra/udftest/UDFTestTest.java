package org.apache.cassandra.udftest;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.codahale.metrics.Snapshot;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class UDFTestTest
{
    @Test
    public void basicTest() throws Throwable
    {
        UDF udf = new UDF("CREATE FUNCTION foo(inp int, d double) RETURNS text LANGUAGE java AS 'return \"foo\";'");
        udf.penetrate();
    }

    @Test(expected = InvalidRequestException.class)
    public void npeTest() throws Throwable
    {
        // this UDF fails with a NullPointerException if input parameter 'inp' is null
        UDF udf = new UDF("CREATE FUNCTION foo(inp int, d double) RETURNS text LANGUAGE java AS 'return inp.toString() + d;'");
        udf.penetrate();
    }

    @Test
    public void executionTest() throws Throwable
    {
        UDF udf = new UDF("CREATE FUNCTION foo(inp int, d double) RETURNS text LANGUAGE java AS 'return \"x\" + inp + \"x\" + d + \"x\";'");
        Assert.assertEquals("xnullxnullx", udf.execute(null, null).getResult());
        Assert.assertEquals("x5x9.1x", udf.execute(5, 9.1d).getResult());
    }

    @Test
    public void executionJavascriptTest() throws Throwable
    {
        UDF udf = new UDF("CREATE FUNCTION foo(inp int, d double) RETURNS text LANGUAGE javascript AS '\"x\" + inp + \"x\" + d + \"x\";'");
        Assert.assertEquals("xnullxnullx", udf.execute(null, null).getResult());
        Assert.assertEquals("x5x9.1x", udf.execute(5, 9.1d).getResult());
    }

    @Test
    public void executeLoopsTest() throws Throwable
    {
        UDF udf = new UDF("CREATE FUNCTION foo(inp int, d double) RETURNS text LANGUAGE java AS 'return \"x\" + inp + \"x\" + d + \"x\";'");
        UDFResult result = udf.executeConcurrentLoops(1, 1000, null, null);
        Assert.assertEquals("xnullxnullx", result.getResult());
        dumpResult(result, "Java loops");
        result = udf.executeConcurrentLoops(1, 1000, 5, 9.1d);
        Assert.assertEquals("x5x9.1x", result.getResult());
        dumpResult(result, "Java loops");
    }

    @Test
    public void executeLoopsJavascriptTest() throws Throwable
    {
        UDF udf = new UDF("CREATE FUNCTION foo(inp int, d double) RETURNS text LANGUAGE javascript AS '\"x\" + inp + \"x\" + d + \"x\";'");
        UDFResult result = udf.executeConcurrentLoops(1, 1000, null, null);
        Assert.assertEquals("xnullxnullx", result.getResult());
        dumpResult(result, "Javascript loops");
        result = udf.executeConcurrentLoops(1, 1000, 5, 9.1d);
        Assert.assertEquals("x5x9.1x", result.getResult());
        dumpResult(result, "Javascript loops");
    }

    @Test
    public void executeConcurrentLoopsTest() throws Throwable
    {
        UDF udf = new UDF("CREATE FUNCTION foo(inp int, d double) RETURNS text LANGUAGE java AS 'return \"x\" + inp + \"x\" + d + \"x\";'");
        UDFResult result = udf.executeConcurrentLoops(10, 1000, null, null);
        Assert.assertEquals("xnullxnullx", result.getResult());
        dumpResult(result, "Java concurrent loops");
        result = udf.executeConcurrentLoops(10, 1000, 5, 9.1d);
        Assert.assertEquals("x5x9.1x", result.getResult());
        dumpResult(result, "Java concurrent loops");
    }

    @Test
    public void executeConcurrentTimedTest() throws Throwable
    {
        UDF udf = new UDF("CREATE FUNCTION foo(inp int, d double) RETURNS text LANGUAGE java AS 'return \"x\" + inp + \"x\" + d + \"x\";'");
        UDFResult result = udf.executeConcurrentTimed(10, TimeUnit.MILLISECONDS, 500, null, null);
        Assert.assertEquals("xnullxnullx", result.getResult());
        dumpResult(result, "Java concurrent timed");
        result = udf.executeConcurrentTimed(10, TimeUnit.MILLISECONDS, 500, 5, 9.1d);
        Assert.assertEquals("x5x9.1x", result.getResult());
        dumpResult(result, "Java concurrent timed");
    }

    @Test
    public void executeConcurrentTimedJavascriptTest() throws Throwable
    {
        UDF udf = new UDF("CREATE FUNCTION foo(inp int, d double) RETURNS text LANGUAGE javascript AS '\"x\" + inp + \"x\" + d + \"x\";'");
        UDFResult result = udf.executeConcurrentTimed(10, TimeUnit.MILLISECONDS, 500, null, null);
        Assert.assertEquals("xnullxnullx", result.getResult());
        dumpResult(result, "Javascript concurrent timed");
        result = udf.executeConcurrentTimed(10, TimeUnit.MILLISECONDS, 500, 5, 9.1d);
        Assert.assertEquals("x5x9.1x", result.getResult());
        dumpResult(result, "Javascript concurrent timed");
    }

    private void dumpResult(UDFResult result, String title)
    {
        long cnt = result.getTimer().getCount();
        Snapshot snap = result.getTimer().getSnapshot();
        System.out.printf("Execution time of '%s' per invocation (%d):%n" +
                          "   mean: %.3f \u00b5s%n" +
                          "   95th: %.3f \u00b5s%n" +
                          "   99th: %.3f \u00b5s%n" +
                          "    med: %.3f \u00b5s%n", title,
                          cnt,
                          snap.getMean() / 1000d,
                          snap.get95thPercentile() / 1000d,
                          snap.get99thPercentile() / 1000d,
                          snap.getMedian() / 1000d);
    }
}
