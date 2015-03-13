package org.apache.cassandra.db.index.search.utils;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

import org.apache.cassandra.db.index.search.OnDiskSA.DataSuffix;
import org.apache.cassandra.db.index.search.container.TokenTree;
import org.apache.cassandra.db.index.utils.CombinedValue;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.db.marshal.AbstractType;

import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.cursors.LongCursor;

public class CombinedTerm implements CombinedValue<DataSuffix>
{
    private final AbstractType<?> comparator;
    private final DataSuffix term;
    private final TreeMap<Long, LongSet> tokens;

    public CombinedTerm(AbstractType<?> comparator, DataSuffix term)
    {
        this.comparator = comparator;
        this.term = term;
        this.tokens = new TreeMap<>();

        SkippableIterator<Long, TokenTree.Token> tokens = term.getTokens();
        while (tokens.hasNext())
        {
            TokenTree.Token current = tokens.next();
            LongSet offsets = this.tokens.get(current.get());
            if (offsets == null)
                this.tokens.put(current.get(), (offsets = new LongOpenHashSet()));

            for (Long offset : current.getOffsets())
                offsets.add(offset);
        }
    }

    public ByteBuffer getTerm()
    {
        return term.getSuffix();
    }

    public Map<Long, LongSet> getTokens()
    {
        return tokens;
    }

    @Override
    public void merge(CombinedValue<DataSuffix> other)
    {
        if (!(other instanceof CombinedTerm))
            return;

        CombinedTerm o = (CombinedTerm) other;

        assert comparator == o.comparator;

        for (Map.Entry<Long, LongSet> token : o.tokens.entrySet())
        {
            LongSet offsets = this.tokens.get(token.getKey());
            if (offsets == null)
                this.tokens.put(token.getKey(), (offsets = new LongOpenHashSet()));

            for (LongCursor offset : token.getValue())
                offsets.add(offset.value);
        }
    }

    @Override
    public DataSuffix get()
    {
        return term;
    }

    @Override
    public int compareTo(CombinedValue<DataSuffix> o)
    {
        return term.compareTo(comparator, o.get().getSuffix());
    }
}