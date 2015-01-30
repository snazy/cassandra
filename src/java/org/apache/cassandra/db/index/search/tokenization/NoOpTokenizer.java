package org.apache.cassandra.db.index.search.tokenization;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Default noOp tokenizer. The iterator will iterate only once
 * returning the unmodified input
 */
public class NoOpTokenizer extends AbstractTokenizer
{
    private ByteBuffer input;
    private boolean hasNext = false;

    @Override
    public void init(Map<String, String> options)
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
