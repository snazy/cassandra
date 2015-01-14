package org.apache.cassandra.db.index.search.tokenization.filter;

/**
 * A single task or set of work to process an input
 * and return a single output. Maintains a link to the
 * next task to be executed after itself
 */
public abstract class FilterPipelineTask<F, T>
{
    private String name;
    public FilterPipelineTask<?, ?> next;

    protected <K, V> void setLast(String name, FilterPipelineTask<K, V> last)
    {
        if (last == this)
            throw new IllegalArgumentException("provided last task ["
                    + last.name + "] cannot be set to itself");
        if (this.next == null)
        {
            this.next = last;
            this.name = name;
        }
        else
        {
            this.next.setLast(name, last);
        }
    }

    public abstract T process(F input) throws Exception;

    public String getName()
    {
        return name;
    }
}
