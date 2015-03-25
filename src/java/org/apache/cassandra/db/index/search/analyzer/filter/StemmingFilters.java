package org.apache.cassandra.db.index.search.analyzer.filter;

import org.tartarus.snowball.SnowballStemmer;

import java.util.Locale;

/**
 * Filters for performing Stemming on tokens
 */
public class StemmingFilters
{
    public static class DefaultStemmingFilter extends FilterPipelineTask<String, String>
    {
        private SnowballStemmer stemmer;

        public DefaultStemmingFilter(Locale locale)
        {
            stemmer = StemmerFactory.getStemmer(locale);
        }

        @Override
        public String process(String input) throws Exception
        {
            if (stemmer == null)
                return input;
            stemmer.setCurrent(input);
            return (stemmer.stem()) ? stemmer.getCurrent() : input;
        }
    }
}
