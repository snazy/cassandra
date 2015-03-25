package org.apache.cassandra.db.index.search.analyzer;

import org.apache.cassandra.db.marshal.AbstractType;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Default noOp tokenizer. The iterator will iterate only once
 * returning the unmodified input
 */
public class NoOpAnalyzer extends AbstractAnalyzer
{
    private ByteBuffer input;
    private boolean hasNext = false;

    @Override
    public void init(Map<String, String> options, AbstractType validator)
    {
    }

    @Override
    public boolean hasNext()
    {
        if (hasNext)
        {
            this.next = input;
            this.hasNext = false;
            return true;
        }
        return false;
    }

    @Override
    public void reset(ByteBuffer input)
    {
        this.next = null;
        this.input = input;
        this.hasNext = true;
    }
}
