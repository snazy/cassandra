package org.apache.cassandra.db.index.search.analyzer;

import org.apache.cassandra.db.index.search.analyzer.filter.BasicResultFilters;
import org.apache.cassandra.db.index.search.analyzer.filter.FilterPipelineBuilder;
import org.apache.cassandra.db.index.search.analyzer.filter.FilterPipelineExecutor;
import org.apache.cassandra.db.index.search.analyzer.filter.FilterPipelineTask;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Analyzer that does *not* tokenize the input. Optionally will
 * apply filters for the input output as defined in analyzers options
 */
public class NonTokenizingAnalyzer extends AbstractAnalyzer {
    private static final Set<AbstractType<?>> VALID_ANALYZABLE_TYPES = new HashSet<AbstractType<?>>()
    {{
            add(UTF8Type.instance);
            add(AsciiType.instance);
    }};

    private AbstractType validator;
    private NonTokenizingOptions options;
    private FilterPipelineTask filterPipeline;

    private ByteBuffer input;
    private boolean hasNext = false;

    @Override
    public void init(Map<String, String> options, AbstractType validator)
    {
        init(NonTokenizingOptions.buildFromMap(options), validator);
    }

    public void init(NonTokenizingOptions tokenizerOptions, AbstractType validator)
    {
        this.validator = validator;
        this.options = tokenizerOptions;
        this.filterPipeline = getFilterPipeline();
    }

    @Override
    public boolean hasNext()
    {
        // check that we know how to handle the input, otherwise bail
        if (!VALID_ANALYZABLE_TYPES.contains(validator))
            return false;

        if (hasNext)
        {
            String inputStr = (String) validator.compose(input);
            Object pipelineRes = FilterPipelineExecutor.execute(filterPipeline, inputStr);
            this.next = ByteBuffer.wrap(((String) pipelineRes).getBytes());
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

    private FilterPipelineTask getFilterPipeline()
    {
        FilterPipelineBuilder builder = new FilterPipelineBuilder(new BasicResultFilters.NoOperation());
        if (options.isCaseSensitive() && options.shouldLowerCaseOutput())
            builder = builder.add("to_lower", new BasicResultFilters.LowerCase());
        if (options.isCaseSensitive() && options.shouldUpperCaseOutput())
            builder = builder.add("to_upper", new BasicResultFilters.UpperCase());
        if (!options.isCaseSensitive())
            builder = builder.add("to_lower", new BasicResultFilters.LowerCase());
        return builder.build();
    }
}
