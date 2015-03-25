package org.apache.cassandra.db.index.search.analyzer.filter;

import java.util.Locale;

/**
 * Basic/General Token Filters
 */
public class BasicResultFilters
{
    private static final Locale DEFAULT_LOCALE = Locale.getDefault();

    public static class LowerCase extends FilterPipelineTask<String, String>
    {
        private Locale locale;

        public LowerCase(Locale locale)
        {
            this.locale = locale;
        }

        public LowerCase()
        {
            this.locale = DEFAULT_LOCALE;
        }

        @Override
        public String process(String input) throws Exception
        {
            return input.toLowerCase(locale);
        }
    }

    public static class UpperCase extends FilterPipelineTask<String, String>
    {
        private Locale locale;

        public UpperCase(Locale locale)
        {
            this.locale = locale;
        }

        public UpperCase()
        {
            this.locale = DEFAULT_LOCALE;
        }

        public String process(String input) throws Exception
        {
            return input.toUpperCase(locale);
        }
    }

    public static class NoOperation extends FilterPipelineTask<Object, Object>
    {
        public Object process(Object input) throws Exception
        {
            return input;
        }
    }
}
