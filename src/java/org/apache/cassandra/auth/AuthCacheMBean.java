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

public interface AuthCacheMBean
{
    /**
     * Re-initializes the cache.
     */
    public void invalidate();

    /**
     * Clears the cache.
     */
    public void invalidateAll();

    public void setValidity(int validityPeriod);

    public int getValidity();

    public void setUpdateInterval(int updateInterval);

    public int getUpdateInterval();

    public void setMaxEntries(int maxEntries);

    public int getMaxEntries();

    public void setInitialCapacity(int initialCapacity);

    public int getInitialCapacity();

    /**
     * Experimental: retrieve cache statistics.
     *
     * Cache stats are only available, if the system property {@value AuthCache#RECORD_CACHE_STATS} is set to {@¢ode true}.
     *
     * The entries in the returned map are implementation dependent and <em>currently</em> reflect most values in
     * {@link com.github.benmanes.caffeine.cache.stats.CacheStats}. The presence and exact meaning of the returned
     * entries may change both when another cache implementation will be used or the implementation (currently
     * Caffeine) changes.
     */
    public Map<String, Number> getCacheStats();
}
