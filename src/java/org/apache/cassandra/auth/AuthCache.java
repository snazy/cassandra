/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.auth;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;

import javax.annotation.Nonnull;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import org.apache.cassandra.concurrent.LocalAwareExecutorService;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.utils.MBeanWrapper;

import static com.google.common.base.Preconditions.checkNotNull;

public class AuthCache<K, V> implements AuthCacheMBean
{
    private static final Logger logger = LoggerFactory.getLogger(AuthCache.class);

    private static final String MBEAN_NAME_BASE = "org.apache.cassandra.auth:type=";
    static final String RECORD_CACHE_STATS = "cassandra.authCache.recordStats";

    private static final long CACHE_GET_TIMEOUT = TimeUnit.SECONDS.toMillis(5);
    private static final long CACHE_GETALL_TIMEOUT = TimeUnit.SECONDS.toMillis(30);

    /**
     * Underlying cache. LoadingCache will call underlying load function on {@link #get} if key is not present
     */
    volatile AsyncLoadingCache<K, V> cache;

    private final String name;
    private final IntConsumer setValidityDelegate;
    private final IntSupplier getValidityDelegate;
    private final IntConsumer setUpdateIntervalDelegate;
    private final IntSupplier getUpdateIntervalDelegate;
    private final IntConsumer setMaxEntriesDelegate;
    private final IntSupplier getMaxEntriesDelegate;
    private final IntConsumer setInitialCapacityDelegate;
    private final IntSupplier getInitialCapacityDelegate;
    private final Function<K, V> loadFunction;
    private final Function<Iterable<? extends K>, Map<K, V>> loadAllFunction;
    private final BooleanSupplier enableCache;

    private final AsyncLoader asyncLoader;

    /**
     * @param name Used for MBean
     * @param setValidityDelegate Used to set cache validity period. See {@link com.github.benmanes.caffeine.cache.Policy#expireAfterWrite()}
     * @param getValidityDelegate Getter for validity period
     * @param setUpdateIntervalDelegate Used to set cache update interval. See {@link
     * com.github.benmanes.caffeine.cache.Policy#refreshAfterWrite()}
     * @param getUpdateIntervalDelegate Getter for update interval
     * @param setMaxEntriesDelegate Used to set max # entries in cache. See {@link com.github.benmanes.caffeine.cache.Policy.Eviction#setMaximum(long)}
     * @param getMaxEntriesDelegate Getter for max entries.
     * @param loadFunction Function to load the cache. Called on {@link #get(Object)}
     * @param cacheEnabledDelegate Used to determine if cache is enabled.
     */
    protected AuthCache(String name,
                        IntConsumer setValidityDelegate,
                        IntSupplier getValidityDelegate,
                        IntConsumer setUpdateIntervalDelegate,
                        IntSupplier getUpdateIntervalDelegate,
                        IntConsumer setMaxEntriesDelegate,
                        IntSupplier getMaxEntriesDelegate,
                        IntConsumer setInitialCapacityDelegate,
                        IntSupplier getInitialCapacityDelegate,
                        Function<K, V> loadFunction,
                        Function<Iterable<? extends K>, Map<K, V>> loadAllFunction,
                        BooleanSupplier cacheEnabledDelegate)
    {
        this.name = checkNotNull(name);
        this.setValidityDelegate = checkNotNull(setValidityDelegate);
        this.getValidityDelegate = checkNotNull(getValidityDelegate);
        this.setUpdateIntervalDelegate = checkNotNull(setUpdateIntervalDelegate);
        this.getUpdateIntervalDelegate = checkNotNull(getUpdateIntervalDelegate);
        this.setMaxEntriesDelegate = checkNotNull(setMaxEntriesDelegate);
        this.getMaxEntriesDelegate = checkNotNull(getMaxEntriesDelegate);
        this.setInitialCapacityDelegate = checkNotNull(setInitialCapacityDelegate);
        this.getInitialCapacityDelegate = checkNotNull(getInitialCapacityDelegate);
        this.loadFunction = checkNotNull(loadFunction);
        this.loadAllFunction = checkNotNull(loadAllFunction);
        this.enableCache = checkNotNull(cacheEnabledDelegate);
        this.asyncLoader = new AsyncLoader();
        init();
    }

    /**
     * Do setup for the cache and MBean.
     */
    protected void init()
    {
        cache = initCache(null);
        unregisterMBean();
        MBeanWrapper.instance.registerMBean(this, getObjectName());
    }

    protected void unregisterMBean()
    {
        if (MBeanWrapper.instance.isRegistered(getObjectName()))
            MBeanWrapper.instance.unregisterMBean(getObjectName(), MBeanWrapper.OnException.LOG);
    }

    protected String getObjectName()
    {
        return MBEAN_NAME_BASE + name;
    }

    /**
     * Retrieve a value from the cache. If the entry is not present, or the
     * cache is disabled, then invoke the load function to compute and store
     * the entry.
     */
    protected V get(K k)
    {
        return withTimeout(CACHE_GET_TIMEOUT,
                           cache != null ? cache.get(k)
                                         : asyncLoader.asyncLoad(k, executor()));
    }

    public Map<K, V> getAll(Collection<K> keys)
    {
        return withTimeout(CACHE_GETALL_TIMEOUT,
                           cache != null ? cache.getAll(keys)
                                         : asyncLoader.asyncLoadAll(keys, executor()));
    }

    private <R> R withTimeout(long timeout, CompletableFuture<R> cf)
    {
        try
        {
            return cf.get(timeout, TimeUnit.MILLISECONDS);
        }
        catch (ExecutionException e)
        {
            logger.debug("{} access failed", name, e);
            Throwable c = e.getCause();
            if (c instanceof Error)
                throw (Error) c;
            if (c instanceof RuntimeException)
                throw (RuntimeException) c;
            throw new RuntimeException(c);
        }
        catch (InterruptedException | TimeoutException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected V getIfPresent(K k)
    {
        if (cache == null)
            return null;

        return cache.synchronous().getIfPresent(k);
    }

    protected Map<K, V> getAllPresent(Collection<K> keys)
    {
        if (cache == null)
            return Collections.emptyMap();

        return cache.synchronous().getAllPresent(keys);
    }

    /**
     * Invalidate the entire cache.
     */
    public void invalidate()
    {
        cache = initCache(null);
    }

    /**
     * Invalidate a key
     *
     * @param k key to invalidate
     */
    public void invalidate(K k)
    {
        if (cache != null)
            cache.synchronous().invalidate(k);
    }

    public void invalidateAll()
    {
        if (cache != null)
            cache.synchronous().invalidateAll();
    }

    /**
     * Time in milliseconds that a value in the cache will expire after.
     *
     * @param validityPeriod in milliseconds
     */
    public void setValidity(int validityPeriod)
    {
        if (Boolean.getBoolean("cassandra.disable_auth_caches_remote_configuration"))
            throw new UnsupportedOperationException("Remote configuration of auth caches is disabled");

        setValidityDelegate.accept(validityPeriod);
        cache = initCache(cache);
    }

    public int getValidity()
    {
        return getValidityDelegate.getAsInt();
    }

    /**
     * Time in milliseconds after which an entry in the cache should be refreshed (it's load function called again)
     *
     * @param updateInterval in milliseconds
     */
    public void setUpdateInterval(int updateInterval)
    {
        if (Boolean.getBoolean("cassandra.disable_auth_caches_remote_configuration"))
            throw new UnsupportedOperationException("Remote configuration of auth caches is disabled");

        setUpdateIntervalDelegate.accept(updateInterval);
        cache = initCache(cache);
    }

    public int getUpdateInterval()
    {
        return getUpdateIntervalDelegate.getAsInt();
    }

    /**
     * Set maximum number of entries in the cache.
     */
    public void setMaxEntries(int maxEntries)
    {
        if (Boolean.getBoolean("cassandra.disable_auth_caches_remote_configuration"))
            throw new UnsupportedOperationException("Remote configuration of auth caches is disabled");

        setMaxEntriesDelegate.accept(maxEntries);
        cache = initCache(cache);
    }

    public int getMaxEntries()
    {
        return getMaxEntriesDelegate.getAsInt();
    }

    public void setInitialCapacity(int initialCapacity)
    {
        if (Boolean.getBoolean("cassandra.disable_auth_caches_remote_configuration"))
            throw new UnsupportedOperationException("Remote configuration of auth caches is disabled");

        setInitialCapacityDelegate.accept(initialCapacity);
        cache = initCache(cache);
    }

    public int getInitialCapacity()
    {
        return getInitialCapacityDelegate.getAsInt();
    }

    public Map<String, Number> getCacheStats()
    {
        Map<String, Number> r = new HashMap<>();
        CacheStats stats = cache.synchronous().stats();
        r.put("averageLoadPenalty", stats.averageLoadPenalty());
        r.put("evictionCount", stats.evictionCount());
        r.put("evictionWeight", stats.evictionWeight());
        r.put("hitCount", stats.hitCount());
        r.put("hitRate", stats.hitRate());
        r.put("loadCount", stats.loadCount());
        r.put("loadFailureCount", stats.loadFailureCount());
        r.put("loadFailureRate", stats.loadFailureRate());
        r.put("loadSuccessCount", stats.loadSuccessCount());
        r.put("missCount", stats.missCount());
        r.put("missRate", stats.missRate());
        r.put("requestCount", stats.requestCount());
        r.put("totalLoadTime", stats.totalLoadTime());
        return r;
    }

    /**
     * (Re-)initialise the underlying cache. Will update validity, max entries, and update interval if
     * any have changed. The underlying {@link LoadingCache} will be initiated based on the provided {@code
     * loadFunction}.
     * Note: If you need some unhandled cache setting to be set you should extend {@link AuthCache} and override this
     * method.
     *
     * @param existing If not null will only update cache update validity, max entries, and update interval.
     *
     * @return New {@link LoadingCache} if existing was null, otherwise the existing {@code cache}
     */
    AsyncLoadingCache<K, V> initCache(AsyncLoadingCache<K, V> existing)
    {
        if (!enableCache.getAsBoolean())
        {
            logger.debug("{} cache is not enabled", name);
            return null;
        }

        if (getValidity() <= 0)
        {
            logger.warn("{} cache is configured with a validity of {} ms, disabling cache - this will result in a performance degration!", name, getValidity());
            return null;
        }
        else if (getValidity() <= 60_000)
        {
            logger.info("{} cache is configured with a validity of less than 60 seconds ({} ms). This can result in a performance degration. Consider checking the settings for the auth caches.", name, getValidity());
        }

        logger.info("(Re)initializing {} (validity period/update interval/max entries/initial capacity) ({}/{}/{}/{})",
                    name, getValidity(), getUpdateInterval(), getMaxEntries(), getInitialCapacity());

        // Using an *async* loading cache eliminates concurrent loads of the same keys.

        if (existing == null)
        {
            @SuppressWarnings("rawtypes")
            Caffeine b = Caffeine.newBuilder()
                                 .refreshAfterWrite(getUpdateInterval(), TimeUnit.MILLISECONDS)
                                 .expireAfterWrite(getValidity(), TimeUnit.MILLISECONDS)
                                 .initialCapacity(getInitialCapacity())
                                 .maximumSize(getMaxEntries())
                                 .executor(executor());
            @SuppressWarnings("unchecked") Caffeine<K, V> builder = b;

            if (Boolean.getBoolean(RECORD_CACHE_STATS))
                builder.recordStats();

            return builder.buildAsync(asyncLoader);
        }

        // Always set as mandatory
        LoadingCache<K, V> sync = cache.synchronous();
        sync.policy().refreshAfterWrite().ifPresent(policy ->
                                                            policy.setExpiresAfter(getUpdateInterval(), TimeUnit.MILLISECONDS));
        sync.policy().expireAfterWrite().ifPresent(policy ->
                                                           policy.setExpiresAfter(getValidity(), TimeUnit.MILLISECONDS));
        sync.policy().eviction().ifPresent(policy ->
                                                   policy.setMaximum(getMaxEntries()));
        return cache;
    }

    private LocalAwareExecutorService executor()
    {
        return Stage.AUTH_CACHE.executor();
    }

    private class AsyncLoader implements AsyncCacheLoader<K, V>
    {
        @Nonnull
        @Override
        public CompletableFuture<V> asyncLoad(@Nonnull K key, @Nonnull Executor executor)
        {
            return CompletableFuture.supplyAsync(() -> loadFunction.apply(key), executor);
        }

        @Nonnull
        @Override
        public CompletableFuture<Map<K, V>> asyncLoadAll(@Nonnull Iterable<? extends K> keys, @Nonnull Executor executor)
        {
            return CompletableFuture.supplyAsync(() -> loadAllFunction.apply(keys), executor);
        }
    }
}
