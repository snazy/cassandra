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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AuthCachePrimitiveTest
{
    private AuthCache<String, String> authCache;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Before
    public void setup()
    {
        System.setProperty("dse.authCache.recordStats", "true");
        // limit the number of background I/O threads - 5 should be exactly sufficient
        System.setProperty("cassandra.io.background.max_pool_size", "5");

        authCache = new AuthCache<String, String>("test",
                                                  i -> {}, () -> 1000,
                                                  i -> {}, () -> 1000,
                                                  i -> {}, () -> 1000,
                                                  i -> {}, () -> 4,
                                                  this::loadFunction,
                                                  this::loadAllFunction,
                                                  () -> true);

        loadAllDelay = 1000L;
        loadDelay = 1000L;

        loadAllKeysIndex.set(0);
        loadAllKeys.clear();
        loadedSingleKeys.clear();
        loadedKeys.clear();
        loadCalls.set(0);
        loadAllCalls.set(0);
    }

    private Map<String, String> loadAllFunction(Iterable<? extends String> keys)
    {
        Uninterruptibles.sleepUninterruptibly(loadAllDelay, TimeUnit.MILLISECONDS);

        loadAllCalls.incrementAndGet();
        Map<String, String> r = new HashMap<>();
        keys.forEach(k -> r.put(k, k));
        loadAllKeys.put(loadAllKeysIndex.getAndIncrement(), r.keySet());
        r.keySet().forEach(k ->         loadedKeys.compute(k, (key, old) -> old == null ? 1 : (old + 1)));
        return r;
    }

    private String loadFunction(String k)
    {
        loadCalls.incrementAndGet();

        if (k.equalsIgnoreCase("null"))
            return null;

        loadedSingleKeys.compute(k, (key, old) -> old == null ? 1 : (old + 1));
        loadedKeys.compute(k, (key, old) -> old == null ? 1 : (old + 1));
        Uninterruptibles.sleepUninterruptibly(loadDelay, TimeUnit.MILLISECONDS);
        return k;
    }

    private long loadAllDelay = 1000L;
    private long loadDelay = 1000L;

    private final AtomicInteger loadAllKeysIndex = new AtomicInteger();
    private final Map<Integer, Set<String>> loadAllKeys = new ConcurrentHashMap<>();
    private final Map<String, Integer> loadedSingleKeys = new ConcurrentHashMap<>();
    private final Map<String, Integer> loadedKeys = new ConcurrentHashMap<>();
    private final AtomicInteger loadCalls = new AtomicInteger();
    private final AtomicInteger loadAllCalls = new AtomicInteger();

    /**
     * Verify that the assumptions made in this test class are actually true.
     */
    @Test
    public void sanityCheck()
    {
        boolean error = false;
        try
        {
            // verify that without subscribing the cache is not invoked
            authCache.get("null");

            assertTrue(loadedKeys.isEmpty());
            assertEquals(0, loadCalls.getAndSet(0));
            assertEquals(0, loadAllCalls.getAndSet(0));

            // verify that null values work
            assertNull(authCache.get("null"));

            assertTrue(loadedKeys.isEmpty());
            assertEquals(1, loadCalls.getAndSet(0));
            assertEquals(0, loadAllCalls.getAndSet(0));
            // the expected value 2 is an implementation detail !!!

            // verify that single key loading works in this test
            assertEquals("foo", authCache.get("foo"));

            assertEquals(Sets.newSet("foo"), loadedKeys.keySet());
            loadedKeys.clear();
            assertEquals(1, loadCalls.getAndSet(0));
            assertEquals(0, loadAllCalls.getAndSet(0));
            // the expected value 2 is an implementation detail !!!

            // verify that multiple key loading works in this test
            Map<String, String> map = Maps.asMap(Sets.newSet("abc", "def"), k -> k);
            assertEquals(map, authCache.getAll(map.keySet()));

            assertEquals(Sets.newSet("abc", "def"), loadAllKeys.get(0));
            assertEquals(Sets.newSet("abc", "def"), loadedKeys.keySet());
            loadedKeys.clear();
            assertEquals(0, loadCalls.getAndSet(0));
            assertEquals(1, loadAllCalls.getAndSet(0));
            // the expected value 0 is an implementation detail !!!
        }
        catch (Exception e)
        {
            error = true;
            throw e;
        }
        finally
        {
            if (!error)
            {
                // cache stats can be a little bit delayed...
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                assertEquals(3, authCache.getCacheStats().get("loadCount").intValue());
                assertEquals(2, authCache.getCacheStats().get("loadSuccessCount").intValue());
            }
        }
    }

    /**
     *
     */
    @Test
    public void concurrentLoadsMustNotBlock() throws InterruptedException
    {
        int nThreads = 50;
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        try
        {
            CountDownLatch prepareLatch = new CountDownLatch(50);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch endLatch = new CountDownLatch(50);

            for (int i = 0; i < nThreads; i++)
            {
                executor.submit(() -> {
                    try
                    {
                        prepareLatch.countDown();
                        startLatch.await();

                        authCache.get("foobar");
                        authCache.getAll(Sets.newSet("one", "two", "three", "four"));

                        endLatch.countDown();
                    }
                    catch (InterruptedException e)
                    {
                        throw new RuntimeException(e);
                    }
                });
            }

            // wait for all threads to be ready
            prepareLatch.await();
            // unblock all threads
            startLatch.countDown();
            // wait until all threads have finished
            endLatch.await();

            // There must have been exactly one load for the concurrent cache get() operation.
            assertEquals(1, loadCalls.getAndSet(0));

            // Since this test tries to ensure that the cache get() and getAll() calls happen concurrently,
            // multiple getAll() calls "compete" about loading operations. It may happen that one getAll() call
            // loads the values for all 4 keys, or one for 1 key and another for 3 keys, or two calls for two keys
            // each, or 4 calls for one key each.
            int loadedKeysCount = loadAllKeys.values().stream().mapToInt(Set::size).sum();
            // In total, we must have loaded exactly four keys.
            assertEquals(4, loadedKeysCount);
            // At least one bulk-load operation, and max 4

            assertThat(loadAllKeys.size()).isGreaterThanOrEqualTo(1).isLessThanOrEqualTo(4);
            // Sanity...
            assertEquals(loadAllKeys.size(), loadAllCalls.getAndSet(0));
            // Make sure that we do not have more loads.
            // Seems that the cache statistics are not that reliable and can undercount.
            assertThat(authCache.getCacheStats().get("loadCount").intValue()).isLessThanOrEqualTo(1 + loadAllKeys.size());
            assertThat(authCache.getCacheStats().get("loadSuccessCount").intValue()).isLessThanOrEqualTo(1 + loadAllKeys.size());

            loadAllKeys.clear();
            loadAllKeysIndex.set(0);

            assertEquals(Sets.newSet("one", "two", "three", "four", "foobar"), loadedKeys.keySet());
            loadedKeys.clear();
        }
        finally
        {
            executor.shutdown();
        }
    }
}
