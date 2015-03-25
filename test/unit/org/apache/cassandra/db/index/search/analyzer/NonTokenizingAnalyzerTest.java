package org.apache.cassandra.db.index.search.analyzer;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Tests for the non-tokenizing analyzer
 */
public class NonTokenizingAnalyzerTest
{
    @Test
    public void caseInsensitiveAnalizer() throws Exception
    {
        NonTokenizingAnalyzer analyzer = new NonTokenizingAnalyzer();
        NonTokenizingOptions options = NonTokenizingOptions.DEFAULT;
        options.setCaseSensitive(false);
        analyzer.init(options, UTF8Type.instance);

        String testString = "Nip it in the bud";
        ByteBuffer toAnalyze = ByteBuffer.wrap(testString.getBytes());
        analyzer.reset(toAnalyze);
        ByteBuffer analyzed = null;
        while (analyzer.hasNext())
            analyzed = analyzer.next();
        Assert.assertTrue(testString.toLowerCase().equals(ByteBufferUtil.string(analyzed)));
    }

    @Test
    public void caseSensitiveAnalizer() throws Exception
    {
        NonTokenizingAnalyzer analyzer = new NonTokenizingAnalyzer();
        NonTokenizingOptions options = NonTokenizingOptions.DEFAULT;
        analyzer.init(options, UTF8Type.instance);

        String testString = "Nip it in the bud";
        ByteBuffer toAnalyze = ByteBuffer.wrap(testString.getBytes());
        analyzer.reset(toAnalyze);
        ByteBuffer analyzed = null;
        while (analyzer.hasNext())
            analyzed = analyzer.next();
        Assert.assertFalse(testString.toLowerCase().equals(ByteBufferUtil.string(analyzed)));
    }

    @Test
    public void ensureIncompatibleInputSkipped() throws Exception
    {
        NonTokenizingAnalyzer analyzer = new NonTokenizingAnalyzer();
        NonTokenizingOptions options = NonTokenizingOptions.DEFAULT;
        analyzer.init(options, Int32Type.instance);

        ByteBuffer toAnalyze = ByteBufferUtil.bytes(1);
        analyzer.reset(toAnalyze);
        Assert.assertTrue(!analyzer.hasNext());
    }
}
