package org.apache.cassandra.db.index.search.analyzer.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes all linked Pipeline Tasks serially and returns
 * output (if exists) from the executed logic
 */
public class FilterPipelineExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(FilterPipelineExecutor.class);

    public static <F,T> T execute(FilterPipelineTask<F, T> task, T initialInput)
    {
        FilterPipelineTask<?, ?> taskPtr = task;
        T result = initialInput;
        try
        {
            while (true)
            {
                FilterPipelineTask<F,T> taskGeneric = (FilterPipelineTask<F,T>) taskPtr;
                result = taskGeneric.process((F) result);
                taskPtr = taskPtr.next;
                if(taskPtr == null)
                    return result;
            }
        }
        catch (Exception e)
        {
            logger.info("An unhandled exception to occurred while processing " +
                    "pipeline [{}]", task.getName(), e);
        }
        return null;
    }
}
