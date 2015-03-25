package org.apache.cassandra.db.index.search.analyzer;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.index.search.analyzer.filter.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferDataInput;
import org.apache.cassandra.utils.ByteBufferUtil;

public class StandardAnalyzer extends AbstractAnalyzer
{
    public enum TokenType
    {
        EOF(-1),
        ALPHANUM(0),
        NUM(6),
        SOUTHEAST_ASIAN(9),
        IDEOGRAPHIC(10),
        HIRAGANA(11),
        KATAKANA(12),
        HANGUL(13);

        public final int value;

        private static Map<Integer, TokenType> map = new HashMap<>();
        static
        {
            for (TokenType type : TokenType.values())
                map.put(type.value, type);
        }

        private TokenType(int value)
        {
            this.value = value;
        }

        public int getValue()
        {
            return value;
        }

        public static TokenType fromValue(int val)
        {
            return map.get(val);
        }
    }

    private StandardTokenizerInterface scanner;
    private StandardTokenizerOptions options;
    private FilterPipelineTask filterPipeline;

    protected Reader inputReader = null;

    public static final int ALPHANUM = 0;
    public static final int NUM = 6;
    public static final int SOUTHEAST_ASIAN = 9;
    public static final int IDEOGRAPHIC = 10;
    public static final int HIRAGANA = 11;
    public static final int KATAKANA = 12;
    public static final int HANGUL = 13;

    private TokenType currentTokenType;

    public TokenType getCurrentTokenType()
    {
        return currentTokenType;
    }

    public String getToken()
    {
        return scanner.getText();
    }

    public char[] getCharArray()
    {
        return scanner.getArray();
    }

    public final boolean incrementToken() throws IOException
    {
        while(true)
        {
            currentTokenType = TokenType.fromValue(scanner.getNextToken());
            if (currentTokenType == TokenType.EOF)
                return false;
            if (scanner.yylength() <= options.getMaxTokenLength()
                    && scanner.yylength() >= options.getMinTokenLength())
                return true;
        }
    }

    private String getFilteredCurrentToken() throws IOException
    {
        String token = getToken();
        Object pipelineRes;
        while(true)
        {
            pipelineRes = FilterPipelineExecutor.execute(filterPipeline, token);
            if(pipelineRes != null)
            {
                break;
            }
            else
            {
                boolean reachedEOF = incrementToken();
                if (!reachedEOF) {
                    break;
                } else {
                    token = getToken();
                }
            }
        }
        return (String) pipelineRes;
    }

    private FilterPipelineTask getFilterPipeline()
    {
        FilterPipelineBuilder builder = new FilterPipelineBuilder(new BasicResultFilters.NoOperation());
        if (!options.isCaseSensitive() && options.shouldLowerCaseTerms())
            builder = builder.add("to_lower", new BasicResultFilters.LowerCase());
        if (!options.isCaseSensitive() && options.shouldUpperCaseTerms())
            builder = builder.add("to_upper", new BasicResultFilters.UpperCase());
        if (options.shouldStemTerms())
            builder = builder.add("term_stemming", new StemmingFilters.DefaultStemmingFilter(options.getLocale()));
        if (options.shouldIgnoreStopTerms())
            builder = builder.add("skip_stop_words", new StopWordFilters.DefaultStopWordFilter(options.getLocale()));
        return builder.build();
    }

    @Override
    public void init(Map<String, String> options, AbstractType validator)
    {
        init(StandardTokenizerOptions.buildFromMap(options));
    }

    public void init(StandardTokenizerOptions tokenizerOptions)
    {
        this.options = tokenizerOptions;
        this.filterPipeline = getFilterPipeline();

        Reader reader = new InputStreamReader(new ByteBufferDataInput(ByteBufferUtil.EMPTY_BYTE_BUFFER));
        this.scanner = new StandardTokenizerImpl(reader);
        this.inputReader = reader;
    }

    @Override
    public boolean hasNext()
    {
        try
        {
            if (incrementToken())
            {
                if (getFilteredCurrentToken() != null)
                {
                    this.next = ByteBuffer.wrap(getFilteredCurrentToken().getBytes());
                    return true;
                }
            }
        }
        catch (IOException e)
        {}

        return false;
    }

    @Override
    public void reset(ByteBuffer input)
    {
        this.next = null;
        Reader reader = new InputStreamReader(new ByteBufferDataInput(input));
        scanner.yyreset(reader);
        this.inputReader = reader;
    }

    public void reset(InputStream input)
    {
        this.next = null;
        Reader reader = new InputStreamReader(input);
        scanner.yyreset(reader);
        this.inputReader = reader;
    }
}
