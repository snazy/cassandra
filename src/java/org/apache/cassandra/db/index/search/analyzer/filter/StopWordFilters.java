package org.apache.cassandra.db.index.search.analyzer.filter;

import java.util.Locale;
import java.util.Set;

/**
 * Filter implementations for input matching Stop Words
 */
public class StopWordFilters
{
    public static class DefaultStopWordFilter extends FilterPipelineTask<String, String>
    {
        private Set<String> stopWords = null;

        public DefaultStopWordFilter(Locale locale)
        {
            this.stopWords = StopWordFactory.getStopWordsForLanguage(locale);
        }

        @Override
        public String process(String input) throws Exception
        {
            return (stopWords != null && stopWords.contains(input)) ? null : input;
        }
    }
}
