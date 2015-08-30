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
package org.apache.cassandra.cache;

import java.util.Collections;
import java.util.Iterator;

public class NopCacheProvider<K, V> implements CacheProvider<K, V>
{
    public ICache<K, V> create()
    {
        return new NopCache<>();
    }

    public static class NopCache<K, V> implements ICache<K, V>
    {
        public long capacity()
        {
            return 0;
        }

        public void setCapacity(long capacity)
        {
        }

        public void put(K key, V value)
        {
        }

        public boolean putIfAbsent(K key, V value)
        {
            return false;
        }

        public boolean replace(K key, V old, V value)
        {
            return false;
        }

        public V get(K key)
        {
            return null;
        }

        public void remove(K key)
        {
        }

        public int size()
        {
            return 0;
        }

        public long weightedSize()
        {
            return 0;
        }

        public void clear()
        {
        }

        public Iterator<K> hotKeyIterator(int n)
        {
            return Collections.emptyIterator();
        }

        public Iterator<K> keyIterator()
        {
            return Collections.emptyIterator();
        }

        public boolean containsKey(K key)
        {
            return false;
        }
    }
}
