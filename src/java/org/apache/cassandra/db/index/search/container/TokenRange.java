package org.apache.cassandra.db.index.search.container;


import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.LongToken;

public class TokenRange
{
    protected Long minToken, maxToken;

    public void setRange(long min, long max)
    {
        minToken = min;
        maxToken = max;
    }

    public void updateRange(long token)
    {
        minToken = minToken == null ? token : Math.min(minToken, token);
        maxToken = maxToken == null ? token : Math.max(maxToken, token);
    }

    public Bounds<LongToken> getRange()
    {
        return new Bounds<>(new LongToken(minToken), new LongToken(maxToken));
    }

    public boolean intersects(TokenRange other)
    {
        Bounds<LongToken> current = new Bounds<>(new LongToken(minToken), new LongToken(maxToken));
        return current.intersects(new Bounds<>(new LongToken(other.minToken), new LongToken(other.maxToken)));
    }

    public int serializedSize()
    {
        return 16; // min + max (2 * long)
    }
}