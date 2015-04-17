package org.apache.cassandra.db.index.search.analyzer;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.index.search.analyzer.filter.BasicResultFilters;
import org.apache.cassandra.db.index.search.analyzer.filter.FilterPipelineBuilder;
import org.apache.cassandra.db.index.search.analyzer.filter.FilterPipelineExecutor;
import org.apache.cassandra.db.index.search.analyzer.filter.FilterPipelineTask;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analyzer that does *not* tokenize the input. Optionally will
 * apply filters for the input output as defined in analyzers options
 */
public class NonTokenizingAnalyzer extends AbstractAnalyzer
{
    private static final Logger logger = LoggerFactory.getLogger(NonTokenizingAnalyzer.class);

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
            String inputStr;

            try
            {
                inputStr = validator.getString(input);
                if (inputStr == null)
                    throw new MarshalException(String.format("'null' deserialized value for %s with %s", ByteBufferUtil.bytesToHex(input), validator));

                Object pipelineRes = FilterPipelineExecutor.execute(filterPipeline, inputStr);
                if (pipelineRes == null || !(pipelineRes instanceof String))
                    return false;

                next = ByteBuffer.wrap(((String) pipelineRes).getBytes());
                return true;
            }
            catch (MarshalException e)
            {
                logger.error("Failed to deserialize value with " + validator, e);
                return false;
            }
            finally
            {
                hasNext = false;
            }
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
