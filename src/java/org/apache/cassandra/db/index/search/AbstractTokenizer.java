package org.apache.cassandra.db.index.search;

import com.google.common.collect.AbstractIterator;

import java.util.Map;

public abstract class AbstractTokenizer<T> extends AbstractIterator<T>
{
    protected T input;
    protected Map<String, String> options;

    public void setInput(T input)
    {
        this.input = input;
    }

    public void setOptions(Map<String, String> options)
    {
        this.options = options;
    }

    public abstract void init();

    public abstract void reset(T input);
}
