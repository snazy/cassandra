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

package org.apache.cassandra.utils.memory;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;
import sun.misc.Cleaner;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

public final class DirectMemory
{
    private static final Unsafe unsafe;

    private static final Class<?> DIRECT_BYTE_BUFFER_CLASS;
    private static final long DIRECT_BYTE_BUFFER_ADDRESS_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_CAPACITY_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_LIMIT_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_CLEANER_OFFSET;
    private static final long CLEANER_THUNK_OFFSET;
    private static final long CLEANER_NEXT_OFFSET;

    private static final AtomicLong allocated = new AtomicLong();
    private static final AtomicLong allocations = new AtomicLong();

    private static final Cleaner DUMMY_CLEANER;
    public static final int OVERCOUNT = 128;

    static
    {
        try
        {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            ByteBuffer directBuffer = ByteBuffer.allocateDirect(0);
            Class<?> clazz = directBuffer.getClass();
            DIRECT_BYTE_BUFFER_ADDRESS_OFFSET = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("address"));
            DIRECT_BYTE_BUFFER_CAPACITY_OFFSET = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("capacity"));
            DIRECT_BYTE_BUFFER_LIMIT_OFFSET = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("limit"));
            DIRECT_BYTE_BUFFER_CLEANER_OFFSET = unsafe.objectFieldOffset(clazz.getDeclaredField("cleaner"));
            DIRECT_BYTE_BUFFER_CLASS = clazz;

            // release the direct buffer
            ((DirectBuffer) directBuffer).cleaner().clean();

            CLEANER_THUNK_OFFSET = unsafe.objectFieldOffset(Cleaner.class.getDeclaredField("thunk"));
            CLEANER_NEXT_OFFSET = unsafe.objectFieldOffset(Cleaner.class.getDeclaredField("next"));

            DUMMY_CLEANER = Cleaner.create(null, () -> {});
            DUMMY_CLEANER.clean();
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }

        MetricNameFactory nameFactory = new DefaultNameFactory("DirectMemory");
        CassandraMetricsRegistry.Metrics.register(nameFactory.createMetricName("Allocated"), (Gauge<Long>) allocated::longValue);
        CassandraMetricsRegistry.Metrics.register(nameFactory.createMetricName("Allocations"), (Gauge<Long>) allocations::longValue);
        CassandraMetricsRegistry.Metrics.register(nameFactory.createMetricName("MaxDirectMemory"), (Gauge<Long>) DatabaseDescriptor::getMaxDirectMemory);
    }

    private DirectMemory()
    {
    }

    public static ByteBuffer allocateDirect(int capacity)
    {
        if (capacity > Integer.MAX_VALUE || capacity < 0L)
            throw new IllegalArgumentException();

        // Use a somewhat higher capacity to count against max_direct_memory_in_mb to include OS overhead
        // of memory allocations.
        long countCapacity = capacity + OVERCOUNT;

        long max = DatabaseDescriptor.getMaxDirectMemory();
        if (max == 0L)
            // if this method is invoked very early, Config.max_direct_memory_in_mb might not have been initialized
            max = Long.MAX_VALUE;
        while (true)
        {
            long current = allocated.get();
            long expected = current + countCapacity;
            if (expected > max)
                throw new OutOfOffHeapMemoryError("Out of direct memory (" + DatabaseDescriptor.getMaxDirectMemory() + ')');
            if (allocated.compareAndSet(current, expected))
                break;
        }

        allocations.incrementAndGet();

        long address = unsafe.allocateMemory(capacity == 0 ? 1 : capacity);

        try {
            unsafe.setMemory(address, 0L, (byte)0);

            //
            // For safety, we use standard 'Cleaner' to de-reference direct memory.
            // In order to get our Cleaner's Runnable called, we need to set a
            // "dummy" Cleaner.
            //
            Cleaner cleaner = (Cleaner) unsafe.allocateInstance(Cleaner.class);
            unsafe.putObject(cleaner, CLEANER_THUNK_OFFSET, (Runnable) () -> {
                unsafe.freeMemory(address);
                unreserveMemory(countCapacity);
            });
            unsafe.putObject(cleaner, CLEANER_NEXT_OFFSET, DUMMY_CLEANER);

            ByteBuffer bb = (ByteBuffer) unsafe.allocateInstance(DIRECT_BYTE_BUFFER_CLASS);
            unsafe.putLong(bb, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET, address);
            unsafe.putInt(bb, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET, capacity);
            unsafe.putInt(bb, DIRECT_BYTE_BUFFER_LIMIT_OFFSET, capacity);
            unsafe.putObject(bb, DIRECT_BYTE_BUFFER_CLEANER_OFFSET, cleaner);

            bb.order(ByteOrder.BIG_ENDIAN);
            return bb;
        } catch (Error e) {
            throw e;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private static void unreserveMemory(long capacity) {
        allocated.addAndGet(-capacity);

        allocations.decrementAndGet();
    }

    public static boolean freeDirect(ByteBuffer buffer)
    {
        if (buffer == null || buffer.getClass() != DIRECT_BYTE_BUFFER_CLASS)
            return false;
        Cleaner cleaner = (Cleaner) unsafe.getObject(buffer, DIRECT_BYTE_BUFFER_CLEANER_OFFSET);
        if (cleaner == null)
            return false;
        Cleaner next = (Cleaner) unsafe.getObject(cleaner, CLEANER_NEXT_OFFSET);
        if (next != DUMMY_CLEANER)
            return false;

        // yes, one of ours
        Runnable thunk = (Runnable) unsafe.getObject(cleaner, CLEANER_THUNK_OFFSET);
        if (thunk != null)
            thunk.run();

        // just for safety - clean the Cleaner.next and DirectByteBuffer.cleaner fields
        unsafe.putObject(cleaner, CLEANER_NEXT_OFFSET, null);
        unsafe.putObject(buffer, DIRECT_BYTE_BUFFER_CLEANER_OFFSET, null);
        unsafe.putLong(buffer, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET, 0L);
        unsafe.putInt(buffer, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET, 0);
        unsafe.putInt(buffer, DIRECT_BYTE_BUFFER_LIMIT_OFFSET, 0);

        return true;
    }

    public static long allocated()
    {
        return allocated.get();
    }

    public static long allocations()
    {
        return allocations.get();
    }
}
