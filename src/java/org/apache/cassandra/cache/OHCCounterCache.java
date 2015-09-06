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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.UUID;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClockAndCount;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.caffinitas.ohc.CacheSerializer;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;

/**
 * Off-heap counter cache.
 */
public final class OHCCounterCache
{
    private OHCCounterCache()
    {
    }

    public static ICache<CounterCacheKey, ClockAndCount> create()
    {
        OHCacheBuilder<CounterCacheKey, ClockAndCount> builder = OHCacheBuilder.newBuilder();
        builder.capacity(DatabaseDescriptor.getCounterCacheSizeInMB() * 1024 * 1024)
               .keySerializer(KeySerializer.instance)
               .valueSerializer(ValueSerializer.instance)
               .throwOOME(true);

        return new CacheAdapter(builder.build());
    }

    private static final class CacheAdapter implements ICache<CounterCacheKey, ClockAndCount>
    {
        private final OHCache<CounterCacheKey, ClockAndCount> ohCache;

        private CacheAdapter(OHCache<CounterCacheKey, ClockAndCount> ohCache)
        {
            this.ohCache = ohCache;
        }

        public long capacity()
        {
            return ohCache.capacity();
        }

        public void setCapacity(long capacity)
        {
            ohCache.setCapacity(capacity);
        }

        public void put(CounterCacheKey key, ClockAndCount value)
        {
            ohCache.put(key, value);
        }

        public boolean putIfAbsent(CounterCacheKey key, ClockAndCount value)
        {
            return ohCache.putIfAbsent(key, value);
        }

        public boolean replace(CounterCacheKey key, ClockAndCount old, ClockAndCount value)
        {
            return ohCache.addOrReplace(key, old, value);
        }

        public ClockAndCount get(CounterCacheKey key)
        {
            return ohCache.get(key);
        }

        public void remove(CounterCacheKey key)
        {
            ohCache.remove(key);
        }

        public int size()
        {
            return (int) ohCache.size();
        }

        public long weightedSize()
        {
            return ohCache.size();
        }

        public void clear()
        {
            ohCache.clear();
        }

        public Iterator<CounterCacheKey> hotKeyIterator(int n)
        {
            return ohCache.hotKeyIterator(n);
        }

        public Iterator<CounterCacheKey> keyIterator()
        {
            return ohCache.keyIterator();
        }

        public boolean containsKey(CounterCacheKey key)
        {
            return ohCache.containsKey(key);
        }
    }

    private static class ValueSerializer implements CacheSerializer<ClockAndCount>
    {
        public static CacheSerializer<ClockAndCount> instance = new ValueSerializer();

        public void serialize(ClockAndCount clockAndCount, ByteBuffer buf)
        {
            buf.putLong(clockAndCount.clock);
            buf.putLong(clockAndCount.count);
        }

        public int serializedSize(ClockAndCount clockAndCount)
        {
            return 8 + 8;
        }

        public ClockAndCount deserialize(ByteBuffer buf)
        {
            long clock = buf.getLong();
            long count = buf.getLong();
            return ClockAndCount.create(clock, count);
        }
    }

    private static class KeySerializer implements CacheSerializer<CounterCacheKey>
    {
        public static CacheSerializer<CounterCacheKey> instance = new KeySerializer();

        public void serialize(CounterCacheKey key, ByteBuffer buf)
        {
            buf.putLong(key.cfId.getMostSignificantBits());
            buf.putLong(key.cfId.getLeastSignificantBits());
            ByteBufferUtil.writeWithLength(key.partitionKey, buf);
            ByteBufferUtil.writeWithLength(key.cellName, buf);
        }

        public int serializedSize(CounterCacheKey key)
        {
            return 8 + 8 +
                   4 + key.partitionKey.length +
                   4 + key.cellName.length;
        }

        public CounterCacheKey deserialize(ByteBuffer buf)
        {
            long msb = buf.getLong();
            long lsb = buf.getLong();
            ByteBuffer partitionKey = ByteBufferUtil.readWithLength(buf);
            ByteBuffer cellName = ByteBufferUtil.readWithLength(buf);
            return new CounterCacheKey(new UUID(msb, lsb), partitionKey, cellName);
        }
    }
}
