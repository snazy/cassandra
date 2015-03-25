package org.apache.cassandra.db.index.search.analyzer;

import org.apache.cassandra.db.marshal.AbstractType;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

public abstract class AbstractAnalyzer implements Iterator<ByteBuffer>
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

    public abstract void init(Map<String, String> options, AbstractType validator);

    public abstract void reset(ByteBuffer input);
}
