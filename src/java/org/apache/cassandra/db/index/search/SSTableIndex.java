package org.apache.cassandra.db.index.search;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.search.container.TokenTree;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;

import com.google.common.base.Function;

public class SSTableIndex
{
    private final ByteBuffer column;
    private final SSTableReader sstable;
    private final OnDiskSA index;
    private final AtomicInteger references = new AtomicInteger(1);
    private final AtomicBoolean obsolete = new AtomicBoolean(false);

    public SSTableIndex(ByteBuffer name, File indexFile, SSTableReader referent)
    {
        column = name;
        sstable = referent;

        if (!sstable.acquireReference())
            throw new IllegalStateException("Couldn't acquire reference to the sstable: " + sstable);

        AbstractType<?> validator = getValidator(column);

        assert validator != null;
        assert indexFile.exists() : String.format("SSTable %s should have index %s.",
                sstable.getFilename(),
                referent.metadata.comparator.getString(column));

        index = new OnDiskSA(indexFile, validator, new DecoratedKeyFetcher(sstable));
    }

    public ByteBuffer minSuffix()
    {
        return index.minSuffix();
    }

    public ByteBuffer maxSuffix()
    {
        return index.maxSuffix();
    }

    public ByteBuffer minKey()
    {
        return index.minKey();
    }

    public ByteBuffer maxKey()
    {
        return index.maxKey();
    }

    public SkippableIterator<Long, TokenTree.Token> search(ByteBuffer lower, boolean lowerInclusive,
                                                           ByteBuffer upper, boolean upperInclusive)
    {
        return index.search(lower, lowerInclusive, upper, upperInclusive);
    }

    public SSTableReader getSSTable()
    {
        return sstable;
    }

    public boolean reference()
    {
        while (true)
        {
            int n = references.get();
            if (n <= 0)
                return false;
            if (references.compareAndSet(n, n + 1))
                return true;
        }
    }

    public void release()
    {
        int n = references.decrementAndGet();
        if (n == 0)
        {
            FileUtils.closeQuietly(index);
            sstable.releaseReference();
            if (obsolete.get())
                FileUtils.delete(index.getIndexPath());
        }
    }

    public void markObsolete()
    {
        obsolete.getAndSet(true);
        release();
    }

    public boolean isObsolete()
    {
        return obsolete.get();
    }

    @Override
    public boolean equals(Object o)
    {
        // name is not checked on purpose that simplifies context handling between logical operations
        return o instanceof SSTableIndex && sstable.descriptor.equals(((SSTableIndex) o).sstable.descriptor);
    }

    @Override
    public int hashCode()
    {
        return sstable.hashCode();
    }

    private AbstractType<?> getValidator(ByteBuffer columnName)
    {
        ColumnDefinition columnDef = sstable.metadata.getColumnDefinition(columnName);
        return columnDef == null ? null : columnDef.getValidator();
    }

    @Override
    public String toString()
    {
        return String.format("SSTableIndex(column: %s, SSTable: %s)", sstable.metadata.comparator.getString(column), sstable.descriptor);
    }

    private static class DecoratedKeyFetcher implements Function<Long, DecoratedKey>
    {
        private final SSTableReader sstable;

        DecoratedKeyFetcher(SSTableReader reader)
        {
            sstable = reader;
        }

        @Override
        public DecoratedKey apply(Long offset)
        {
            try
            {
                return sstable.keyAt(offset);
            }
            catch (IOException e)
            {
                throw new FSReadError(e, sstable.getFilename());
            }
        }

        @Override
        public int hashCode()
        {
            return sstable.descriptor.hashCode();
        }

        @Override
        public boolean equals(Object other)
        {
            return other instanceof DecoratedKeyFetcher
                    && sstable.descriptor.equals(((DecoratedKeyFetcher) other).sstable.descriptor);
        }
    }
}
