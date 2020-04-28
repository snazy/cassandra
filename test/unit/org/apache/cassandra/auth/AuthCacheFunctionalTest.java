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

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

import static org.assertj.core.api.Assertions.assertThat;

public class AuthCacheFunctionalTest extends CQLTester
{
    private static final long runtimeInSecs = 10L;

    @BeforeClass
    public static void setup()
    {
        System.setProperty("dse.authCache.recordStats", "true");

        System.setProperty("dse.io.sched.min_pool_size", "1");

        requireAuthentication();
        DatabaseDescriptor.setPermissionsValidity(9999);
        DatabaseDescriptor.setPermissionsUpdateInterval(9999);
        requireNetwork();
    }

    @Test
    public void noMoreLoadsThanInvalidations() throws Throwable
    {
        int nThreads = 20;
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        try
        {
            useSuperUser();

            executeNet("CREATE USER a_user WITH PASSWORD 'secret'");

            useUser("a_user", "secret");

            executeNet("SELECT key FROM system.local");

            // run the parallel/concurrent part

            CountDownLatch prepareLatch = new CountDownLatch(nThreads);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch endLatch = new CountDownLatch(nThreads);

            // run for 10 seconds
            long tUntil = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(runtimeInSecs);
            long invalidateInterval = 100L;

            // Re-initialize the caches here to clean up the caches stats
            DatabaseDescriptor.getAuthManager().rolesCache.invalidate();
            DatabaseDescriptor.getAuthManager().permissionsCache.invalidate();
            int invalidations = 1;
            AtomicBoolean active = new AtomicBoolean(true);
            AtomicBoolean outstandingInvalidation = new AtomicBoolean();

            for (int i = 0; i < nThreads; i++)
            {
                executor.submit(() -> {
                    try
                    {
                        prepareLatch.countDown();
                        startLatch.await();

                        // Run this loop while the the main loop (below, "main" thread) is active
                        // or an invalidation is still triggered but not "consumed" by an executeNet().
                        // The latter is to prevent races on small instances running the test.
                        while (active.get())
                            executeNet("SELECT key FROM system.local");

                        endLatch.countDown();
                    }
                    catch (Throwable e)
                    {
                        throw new RuntimeException(e);
                    }
                });
            }

            // wait for all threads to be ready
            prepareLatch.await();
            // unblock all threads
            startLatch.countDown();

            while (true)
            {
                Thread.sleep(invalidateInterval);
                if (System.currentTimeMillis() < tUntil)
                {
                    DatabaseDescriptor.getAuthManager().rolesCache.invalidateAll();
                    DatabaseDescriptor.getAuthManager().permissionsCache.invalidateAll();
                    invalidations++;
                    outstandingInvalidation.set(true);
                }
                else
                {
                    Thread.sleep(invalidateInterval);
                    active.set(false);
                    break;
                }
            }

            // wait until all threads have finished
            endLatch.await();

            Map<String, Number> rolesCacheStats = DatabaseDescriptor.getAuthManager().rolesCache.getCacheStats();
            Map<String, Number> permissionsCacheStats = DatabaseDescriptor.getAuthManager().permissionsCache.getCacheStats();
            // one load for our user "a_user" for each invalidation every 100ms (invalidateInterval)
            // Seems that the cache statistics are not that reliable and can undercount.
            assertThat(rolesCacheStats.get("loadCount").intValue()).isLessThanOrEqualTo(invalidations);
            assertThat(rolesCacheStats.get("loadSuccessCount").intValue()).isLessThanOrEqualTo(invalidations);
            // one load for our user "a_user" for each invalidation every 100ms (invalidateInterval)
            // Seems that the cache statistics are not that reliable and can undercount.
            assertThat(permissionsCacheStats.get("loadCount").intValue()).isLessThanOrEqualTo(invalidations);
            assertThat(permissionsCacheStats.get("loadSuccessCount").intValue()).isLessThanOrEqualTo(invalidations);

        }
        finally
        {
            executor.shutdown();
        }
    }
}
