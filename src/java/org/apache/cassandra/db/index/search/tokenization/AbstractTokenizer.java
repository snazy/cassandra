package org.apache.cassandra.db.index.search.tokenization;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

public abstract class AbstractTokenizer implements Iterator<ByteBuffer>
{
    protected ByteBuffer next = null;

    @Override
    public ByteBuffer next()
    {
        return next;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public abstract void init(Map<String, String> options);

    public abstract void reset(ByteBuffer input);
}
