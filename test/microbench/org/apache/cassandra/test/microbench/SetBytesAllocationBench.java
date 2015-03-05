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

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import com.sun.jna.Native;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

/**
 * Microbenchmark to compare allocation throughput using different strategies:
 * {@code Unsafe}, {@code JNA} and Jemalloc via JNA.
 * Jemalloc must be
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Threads(8)
@Fork(value = 2)
@State(Scope.Benchmark)
public class SetBytesAllocationBench
{
    static final Unsafe unsafe;
    private static final long UNSAFE_COPY_THRESHOLD = 1024 * 1024L; // copied from java.nio.Bits
    private static final long BYTE_ARRAY_BASE_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_ADDRESS_OFFSET;

    static
    {
        try
        {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            BYTE_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(byte[].class);
            DIRECT_BYTE_BUFFER_ADDRESS_OFFSET = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("address"));
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    @State(Scope.Thread)
    public static class Mem {
        public long address;

        public ByteBuffer heap;
        public ByteBuffer direct;

        public Mem() {
            address = Native.malloc(16);
            heap = ByteBuffer.allocate(16);
            direct = ByteBuffer.allocateDirect(16);
        }
    }

    @State(Scope.Thread)
    public static class Toggle {
        public boolean toggle;
    }

    @Benchmark
    @Group("branch")
    public void branchDirect(Mem state)
    {
        setBytesBranch(state.address, state.direct);
    }

    @Benchmark
    @Group("branch")
    public void branchHeap(Mem state)
    {
        setBytesBranch(state.address, state.heap);
    }

    @Benchmark
    @Group("branchMixed")
    public void branchMixed(Mem state, Toggle toggle)
    {
        setBytesBranch(state.address, !(toggle.toggle = !toggle.toggle) ? state.heap : state.direct);
    }

    @Benchmark
    @Group("trunk")
    public void trunkDirect(Mem state)
    {
        setBytesTrunk(state.address, state.direct);
    }

    @Benchmark
    @Group("trunk")
    public void trunkHeap(Mem state)
    {
        setBytesTrunk(state.address, state.heap);
    }

    @Benchmark
    @Group("trunkMixed")
    public void trunkMixed(Mem state, Toggle toggle)
    {
        setBytesTrunk(state.address, !(toggle.toggle = !toggle.toggle) ? state.heap : state.direct);
    }

    public static void setBytesTrunk(long address, ByteBuffer buffer)
    {
        int start = buffer.position();
        int count = buffer.limit() - start;
        if (count == 0)
            return;

        if (buffer.isDirect())
            setBytes(unsafe.getLong(buffer, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET) + start, address, count);
        else
            setBytes(address, buffer.array(), buffer.arrayOffset() + start, count);
    }

    public static void setBytesBranch(long address, ByteBuffer buffer)
    {
        int start = buffer.position();
        int count = buffer.limit() - start;
        if (count == 0)
            return;

        if (buffer.isDirect())
            setBytes(((DirectBuffer)buffer).address() + start, address, count);
        else
            setBytes(address, buffer.array(), buffer.arrayOffset() + start, count);
    }

    public static void setBytes(long address, byte[] buffer, int bufferOffset, int count)
    {
        assert buffer != null;
        assert !(bufferOffset < 0 || count < 0 || bufferOffset + count > buffer.length);
        setBytes(buffer, bufferOffset, address, count);
    }

    public static void setBytes(long src, long trg, long count)
    {
        while (count > 0)
        {
            long size = (count> UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : count;
            unsafe.copyMemory(src, trg, size);
            count -= size;
            src += size;
            trg+= size;
        }
    }

    public static void setBytes(byte[] src, int offset, long trg, long count)
    {
        while (count > 0)
        {
            long size = (count> UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : count;
            unsafe.copyMemory(src, BYTE_ARRAY_BASE_OFFSET + offset, null, trg, size);
            count -= size;
            offset += size;
            trg += size;
        }
    }

}
