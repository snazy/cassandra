/**
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
package org.apache.cassandra.test.microbench;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import com.sun.jna.Function;
import com.sun.jna.InvocationMapper;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import sun.misc.Unsafe;

/**
 * Microbenchmark to compare allocation throughput using different strategies:
 * {@code Unsafe}, {@code JNA} and Jemalloc via JNA.
 * Jemalloc must be
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Threads(32)
@Fork(value = 2)
@State(Scope.Benchmark)
public class OffHeapAllocationBench
{
    static final Unsafe unsafe;

    static
    {
        try
        {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (sun.misc.Unsafe) field.get(null);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    interface Allocator
    {
        long alloc(long size);

        void free(long adr);
    }

    static class UnsafeAllocator implements Allocator
    {

        public long alloc(long size)
        {
            return unsafe.allocateMemory(size);
        }

        public void free(long adr)
        {
            unsafe.freeMemory(adr);
        }
    }

    static class JNAAllocator implements Allocator
    {
        public long alloc(long size)
        {
            return Native.malloc(size);
        }

        public void free(long adr)
        {
            Native.free(adr);
        }
    }

    interface JEMLibrary extends Library
    {
        long malloc(long var1);

        void free(long var1);
    }

    static class JemallocJNAAllocator implements Allocator
    {
        private final JEMLibrary library;

        public JemallocJNAAllocator()
        {
            HashMap options = new HashMap();
            options.put("invocation-mapper", new InvocationMapper()
            {
                public InvocationHandler getInvocationHandler(NativeLibrary lib, Method m)
                {
                    final Function f = lib.getFunction(m.getName());
                    return "malloc".equals(m.getName()) ? new InvocationHandler()
                    {
                        public Object invoke(Object proxy, Method method, Object[] args)
                        {
                            return Long.valueOf(f.invokeLong(args));
                        }
                    } : ("free".equals(m.getName()) ? new InvocationHandler()
                    {
                        public Object invoke(Object proxy, Method method, Object[] args)
                        {
                            f.invoke(args);
                            return null;
                        }
                    } : null);
                }
            });
            this.library = (JEMLibrary) Native.loadLibrary("jemalloc", JEMLibrary.class, options);
        }

        public long alloc(long size)
        {
            return library.malloc(size);
        }

        public void free(long adr)
        {
            library.free(adr);
        }
    }

    private final Allocator unsafeAllocator = new UnsafeAllocator();
    private final Allocator jnaAllocator = new JNAAllocator();
    private final Allocator jemallocJnaAllocator = new JemallocJNAAllocator();

    @Benchmark
    public void unsafeAllocate1KB()
    {
        allocate(1, unsafeAllocator);
    }

    @Benchmark
    public void unsafeAllocate8KB()
    {
        allocate(8, unsafeAllocator);
    }

    @Benchmark
    public void unsafeAllocate16KB()
    {
        allocate(16, unsafeAllocator);
    }

    @Benchmark
    public void unsafeAllocate32KB()
    {
        allocate(32, unsafeAllocator);
    }

    @Benchmark
    public void unsafeAllocate48KB()
    {
        allocate(48, unsafeAllocator);
    }

    @Benchmark
    public void unsafeAllocate64KB()
    {
        allocate(64, unsafeAllocator);
    }

    @Benchmark
    public void unsafeAllocate256KB()
    {
        allocate(256, unsafeAllocator);
    }

    @Benchmark
    public void unsafeAllocate1024KB()
    {
        allocate(1024, unsafeAllocator);
    }

    @Benchmark
    public void unsafeAllocate4096KB()
    {
        allocate(4096, unsafeAllocator);
    }

    @Benchmark
    public void jnaAllocate1KB()
    {
        allocate(1, jnaAllocator);
    }

    @Benchmark
    public void jnaAllocate8KB()
    {
        allocate(8, jnaAllocator);
    }

    @Benchmark
    public void jnaAllocate16KB()
    {
        allocate(16, jnaAllocator);
    }

    @Benchmark
    public void jnaAllocate32KB()
    {
        allocate(32, jnaAllocator);
    }

    @Benchmark
    public void jnaAllocate48KB()
    {
        allocate(48, jnaAllocator);
    }

    @Benchmark
    public void jnaAllocate64KB()
    {
        allocate(64, jnaAllocator);
    }

    @Benchmark
    public void jnaAllocate256KB()
    {
        allocate(256, jnaAllocator);
    }

    @Benchmark
    public void jnaAllocate1024KB()
    {
        allocate(1024, jnaAllocator);
    }

    @Benchmark
    public void jnaAllocate4096KB()
    {
        allocate(4096, jnaAllocator);
    }

    @Benchmark
    public void jemallocJnaAllocate1KB()
    {
        allocate(1, jemallocJnaAllocator);
    }

    @Benchmark
    public void jemallocJnaAllocate8KB()
    {
        allocate(8, jemallocJnaAllocator);
    }

    @Benchmark
    public void jemallocJnaAllocate16KB()
    {
        allocate(16, jemallocJnaAllocator);
    }

    @Benchmark
    public void jemallocJnaAllocate32KB()
    {
        allocate(32, jemallocJnaAllocator);
    }

    @Benchmark
    public void jemallocJnaAllocate48KB()
    {
        allocate(48, jemallocJnaAllocator);
    }

    @Benchmark
    public void jemallocJnaAllocate64KB()
    {
        allocate(64, jemallocJnaAllocator);
    }

    @Benchmark
    public void jemallocJnaAllocate256KB()
    {
        allocate(256, jemallocJnaAllocator);
    }

    @Benchmark
    public void jemallocJnaAllocate1024KB()
    {
        allocate(1024, jemallocJnaAllocator);
    }

    @Benchmark
    public void jemallocJnaAllocate4096KB()
    {
        allocate(4096, jemallocJnaAllocator);
    }

    private static void allocate(int kBytes, Allocator allocator)
    {
        int bytes = kBytes * 1024;
        long adr = allocator.alloc(bytes);
        allocator.free(adr);
    }
}
