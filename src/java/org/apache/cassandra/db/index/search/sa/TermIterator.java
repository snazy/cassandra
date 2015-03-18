package org.apache.cassandra.db.index.search.sa;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.index.search.container.TokenTreeBuilder;
import org.apache.cassandra.utils.Pair;

import com.google.common.collect.AbstractIterator;

public abstract class TermIterator extends AbstractIterator<Pair<ByteBuffer, TokenTreeBuilder>>
{
    public abstract ByteBuffer minTerm();
    public abstract ByteBuffer maxTerm();
}
