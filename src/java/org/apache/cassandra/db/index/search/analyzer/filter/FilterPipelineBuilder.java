package org.apache.cassandra.db.index.search.analyzer.filter;

/**
 * Creates a Pipeline object for applying n pieces of logic
 * from the provided methods to the builder in a guaranteed order
 */
public class FilterPipelineBuilder
{
    private final FilterPipelineTask<?,?> parent;
    private FilterPipelineTask<?,?> current;

    public FilterPipelineBuilder(FilterPipelineTask<?, ?> first)
    {
        this(first, first);
    }

    private FilterPipelineBuilder(FilterPipelineTask<?, ?> first, FilterPipelineTask<?, ?> current)
    {
        this.parent = first;
        this.current = current;
    }

    public FilterPipelineBuilder add(String name, FilterPipelineTask<?,?> nextTask)
    {
        this.current.setLast(name, nextTask);
        this.current = nextTask;
        return this;
    }

    public FilterPipelineTask<?,?> build()
    {
        return this.parent;
    }
}
