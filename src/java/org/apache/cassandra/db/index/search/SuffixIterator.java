package org.apache.cassandra.db.index.search;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.index.search.container.TokenTree.Token;
import org.apache.cassandra.db.index.search.plan.Expression;
import org.apache.cassandra.db.index.utils.LazyMergeSortIterator;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.io.util.FileUtils;

import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SuffixIterator extends AbstractIterator<Token> implements SkippableIterator<Long, Token>
{
    private static final Logger logger = LoggerFactory.getLogger(SuffixIterator.class);

    private static final ThreadLocal<ExecutorService> SEARCH_EXECUTOR = new ThreadLocal<ExecutorService>()
    {
        public ExecutorService initialValue()
        {
            final String currentThread = Thread.currentThread().getName();
            final int concurrencyFactor = DatabaseDescriptor.searchConcurrencyFactor();

            logger.info("Search Concurrency Factor is set to {} for {}", concurrencyFactor, currentThread);

            return (concurrencyFactor <= 1)
                    ? MoreExecutors.sameThreadExecutor()
                    : Executors.newFixedThreadPool(concurrencyFactor, new ThreadFactory()
                      {
                          public final AtomicInteger count = new AtomicInteger();

                          @Override
                          public Thread newThread(Runnable task)
                          {
                              Thread newThread = new Thread(task, currentThread + "-SEARCH-" + count.incrementAndGet());
                              newThread.setDaemon(true);

                              return newThread;
                          }
                      });
        }
    };

    private final SkippableIterator<Long, Token> union;
    private final List<SSTableIndex> referencedIndexes = new CopyOnWriteArrayList<>();
    private final long count;

    public SuffixIterator(final Expression expression,
                          SkippableIterator<Long, Token> memtableIterator,
                          final Collection<SSTableIndex> perSSTableIndexes)
    {
        final AtomicLong tokenCount = new AtomicLong(0);
        final CountDownLatch latch = new CountDownLatch(perSSTableIndexes.size());
        final List<SkippableIterator<Long, Token>> keys = new CopyOnWriteArrayList<>();

        if (memtableIterator != null)
        {
            keys.add(memtableIterator);
            tokenCount.getAndAdd(memtableIterator.getCount());
        }

        for (final SSTableIndex index : perSSTableIndexes)
        {
            if (!index.reference())
            {
                latch.countDown();
                continue;
            }

            SEARCH_EXECUTOR.get().submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        SkippableIterator<Long, Token> results = index.search(expression);
                        if (results == null)
                        {
                            index.release();
                            return;
                        }

                        keys.add(results);
                        referencedIndexes.add(index);
                        tokenCount.getAndAdd(results.getCount());
                    }
                    finally
                    {
                        latch.countDown();
                    }
                }
            });
        }

        Uninterruptibles.awaitUninterruptibly(latch);

        union = new LazyMergeSortIterator<>(LazyMergeSortIterator.OperationType.OR, keys);
        count = tokenCount.get();
    }

    @Override
    public Long getMinimum()
    {
        return union.getMinimum();
    }

    @Override
    protected Token computeNext()
    {
        return union.hasNext() ? union.next() : endOfData();
    }

    @Override
    public void skipTo(Long next)
    {
        union.skipTo(next);
    }

    @Override
    public boolean intersect(Token token)
    {
        return union.intersect(token);
    }

    @Override
    public long getCount()
    {
        return count;
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(union);
        for (SSTableIndex index : referencedIndexes)
            index.release();
    }
}
