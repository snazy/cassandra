package org.apache.cassandra.db.index.search.analyzer;

import java.util.Map;

public class NonTokenizingOptions
{
    public static final String NORMALIZE_LOWERCASE = "normalize_lowercase";
    public static final String NORMALIZE_UPPERCASE = "normalize_uppercase";
    public static final String CASE_SENSITIVE = "case_sensitive";

    public static final NonTokenizingOptions DEFAULT = defaultOptions();

    private boolean caseSensitive;
    private boolean upperCaseOutput;
    private boolean lowerCaseOutput;

    public boolean isCaseSensitive()
    {
        return caseSensitive;
    }

    public void setCaseSensitive(boolean caseSensitive)
    {
        this.caseSensitive = caseSensitive;
    }

    public boolean shouldUpperCaseOutput()
    {
        return upperCaseOutput;
    }

    public void setUpperCaseOutput(boolean upperCaseOutput)
    {
        this.upperCaseOutput = upperCaseOutput;
    }

    public boolean shouldLowerCaseOutput()
    {
        return lowerCaseOutput;
    }

    public void setLowerCaseOutput(boolean lowerCaseOutput)
    {
        this.lowerCaseOutput = lowerCaseOutput;
    }

    public static class OptionsBuilder
    {
        private boolean caseSensitive = true;
        private boolean upperCaseOutput = false;
        private boolean lowerCaseOutput = false;

        public OptionsBuilder()
        {
        }

        public OptionsBuilder caseSensitive(boolean caseSensitive)
        {
            this.caseSensitive = caseSensitive;
            return this;
        }

        public OptionsBuilder upperCaseOutput(boolean upperCaseOutput)
        {
            this.upperCaseOutput = upperCaseOutput;
            return this;
        }

        public OptionsBuilder lowerCaseOutput(boolean lowerCaseOutput)
        {
            this.lowerCaseOutput = lowerCaseOutput;
            return this;
        }

        public NonTokenizingOptions build()
        {
            if (lowerCaseOutput && upperCaseOutput)
                throw new IllegalArgumentException("Options to normalize terms cannot be " +
                        "both uppercase and lowercase at the same time");

            NonTokenizingOptions options = new NonTokenizingOptions();
            options.setCaseSensitive(caseSensitive);
            options.setUpperCaseOutput(upperCaseOutput);
            options.setLowerCaseOutput(lowerCaseOutput);
            return options;
        }
    }

    public static NonTokenizingOptions buildFromMap(Map<String, String> optionsMap)
    {
        OptionsBuilder optionsBuilder = new OptionsBuilder();

        if (optionsMap.containsKey(CASE_SENSITIVE) && (optionsMap.containsKey(NORMALIZE_LOWERCASE)
                || optionsMap.containsKey(NORMALIZE_UPPERCASE)))
            throw new IllegalArgumentException("case_sensitive option cannot be specified together " +
                    "with either normalize_lowercase or normalize_uppercase");

        for (Map.Entry<String, String> entry : optionsMap.entrySet())
        {
            switch (entry.getKey())
            {
                case NORMALIZE_LOWERCASE:
                {
                    boolean bool = Boolean.parseBoolean(entry.getValue());
                    optionsBuilder = optionsBuilder.lowerCaseOutput(bool);
                    break;
                }
                case NORMALIZE_UPPERCASE:
                {
                    boolean bool = Boolean.parseBoolean(entry.getValue());
                    optionsBuilder = optionsBuilder.upperCaseOutput(bool);
                    break;
                }
                case CASE_SENSITIVE:
                {
                    boolean bool = Boolean.parseBoolean(entry.getValue());
                    optionsBuilder = optionsBuilder.caseSensitive(bool);
                    break;
                }
                default:
                {
                }
            }
        }
        return optionsBuilder.build();
    }

    private static NonTokenizingOptions defaultOptions()
    {
        return new OptionsBuilder()
                .caseSensitive(true).lowerCaseOutput(false)
                .upperCaseOutput(false)
                .build();
    }
}
