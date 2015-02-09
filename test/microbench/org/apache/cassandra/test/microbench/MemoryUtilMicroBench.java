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
package org.apache.cassandra.test.microbench;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import sun.misc.Unsafe;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 1, jvmArgsAppend = "-Xmx512M")
@Threads(1)
@State(Scope.Thread)
public class MemoryUtilMicroBench
{
    private static final Unsafe unsafe;

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
            throw new RuntimeException(e);
        }
    }

    long adr;
    long off;

    static final Pointer pointerHelper = new Pointer(0L);

    static final long memorySize = 1024 * 1024 * 1024; // 1 GB

    private void checkWrap()
    {
        if (off >= memorySize - 8)
            off = 0;
    }

    @Setup
    public void setup() throws Exception
    {
        adr = Native.malloc(memorySize);
    }

    @TearDown
    public void tearDown()
    {
        Native.free(adr);
    }

    @Benchmark
    public void unsafe_getByte()
    {
        unsafe.getByte(adr + off++);
        checkWrap();
    }

    @Benchmark
    public void unsafe_putByte()
    {
        unsafe.putByte(adr + off++, (byte) 0);
        checkWrap();
    }

    @Benchmark
    public void native_getByte()
    {
        pointerHelper.getByte(adr + off++);
        checkWrap();
    }

    @Benchmark
    public void native_putByte()
    {
        pointerHelper.setByte(adr + off++, (byte) 0);
        checkWrap();
    }

    @Benchmark
    public void unsafe_getShort()
    {
        unsafe.getShort(adr + off++);
        checkWrap();
    }

    @Benchmark
    public void unsafe_putShort()
    {
        unsafe.putShort(adr + off++, (short) 0);
        checkWrap();
    }

    @Benchmark
    public void native_getShort()
    {
        pointerHelper.getShort(adr + off++);
        checkWrap();
    }

    @Benchmark
    public void native_putShort()
    {
        pointerHelper.setShort(adr + off++, (short) 0);
        checkWrap();
    }

    @Benchmark
    public void unsafe_getInt()
    {
        unsafe.getInt(adr + off++);
        checkWrap();
    }

    @Benchmark
    public void unsafe_putInt()
    {
        unsafe.putInt(adr + off++, 0);
        checkWrap();
    }

    @Benchmark
    public void native_getInt()
    {
        pointerHelper.getInt(adr + off++);
        checkWrap();
    }

    @Benchmark
    public void native_putInt()
    {
        pointerHelper.setInt(adr + off++, 0);
        checkWrap();
    }

    @Benchmark
    public void unsafe_getLong()
    {
        unsafe.getLong(adr + off++);
        checkWrap();
    }

    @Benchmark
    public void unsafe_putLong()
    {
        unsafe.putLong(adr + off++, 0L);
        checkWrap();
    }

    @Benchmark
    public void native_getLong()
    {
        pointerHelper.getLong(adr + off++);
        checkWrap();
    }

    @Benchmark
    public void native_putLong()
    {
        pointerHelper.setLong(adr + off++, 0L);
        checkWrap();
    }

    @Benchmark
    public void unsafe_getFloat()
    {
        unsafe.getFloat(adr + off++);
        checkWrap();
    }

    @Benchmark
    public void unsafe_putFloat()
    {
        unsafe.putFloat(adr + off++, 0f);
        checkWrap();
    }

    @Benchmark
    public void native_getFloat()
    {
        pointerHelper.getFloat(adr + off++);
        checkWrap();
    }

    @Benchmark
    public void native_putFloat()
    {
        pointerHelper.setFloat(adr + off++, 0f);
        checkWrap();
    }

    @Benchmark
    public void unsafe_getDouble()
    {
        unsafe.getDouble(adr + off++);
        checkWrap();
    }

    @Benchmark
    public void unsafe_putDouble()
    {
        unsafe.putDouble(adr + off++, 0d);
        checkWrap();
    }

    @Benchmark
    public void native_getDouble()
    {
        pointerHelper.getDouble(adr + off++);
        checkWrap();
    }

    @Benchmark
    public void native_putDouble()
    {
        pointerHelper.setDouble(adr + off++, 0d);
        checkWrap();
    }
}
