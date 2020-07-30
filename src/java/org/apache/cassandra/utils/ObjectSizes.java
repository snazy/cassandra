package org.apache.cassandra.utils;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.nio.ByteBuffer;

import org.github.jamm.MemoryMeter;

/**
 * A convenience class for wrapping access to MemoryMeter
 */
public class ObjectSizes
{
    private static final MemoryMeter meter = MemoryMeter.builder()
                                                        .omitSharedBufferOverhead()
                                                        .ignoreKnownSingletons()
                                                        .build();
    private static final MemoryMeter meterShallowBb = MemoryMeter.builder()
                                                                 .onlyShallowByteBuffers()
                                                                 .ignoreKnownSingletons()
                                                                 .build();
    private static final MemoryMeter meterBbHeapOnly = MemoryMeter.builder()
                                                                  .byteBuffersHeapOnlyNoSlice()
                                                                  .ignoreKnownSingletons()
                                                                  .build();

    private static final long BUFFER_EMPTY_SIZE = meter.measure(ByteBufferUtil.EMPTY_BYTE_BUFFER);

    /**
     * Memory a byte array consumes
     * @param bytes byte array to get memory size
     * @return heap-size of the array
     */
    public static long sizeOfArray(byte[] bytes)
    {
        return meter.sizeOfArray(bytes);
    }

    /**
     * Memory a long array consumes
     * @param longs byte array to get memory size
     * @return heap-size of the array
     */
    public static long sizeOfArray(long[] longs)
    {
        return meter.sizeOfArray(longs);
    }

    /**
     * Memory an int array consumes
     * @param ints byte array to get memory size
     * @return heap-size of the array
     */
    public static long sizeOfArray(int[] ints)
    {
        return meter.sizeOfArray(ints);
    }

    /**
     * Memory a reference array consumes
     * @param length the length of the reference array
     * @return heap-size of the array
     */
    public static long sizeOfReferenceArray(int length)
    {
        return meter.sizeOfArray(length, Object.class);
    }

    /**
     * Memory a reference array consumes itself only
     * @param objects the array to size
     * @return heap-size of the array (excluding memory retained by referenced objects)
     */
    public static long sizeOfArray(Object[] objects)
    {
        return meter.sizeOfArray(objects);
    }

    /**
     * Memory a ByteBuffer array consumes.
     */
    public static long sizeOnHeapOf(ByteBuffer[] array)
    {
        return meterBbHeapOnly.measureDeep(array);
    }

    public static long sizeOnHeapExcludingData(ByteBuffer[] array)
    {
        return meterShallowBb.measureDeep(array);
    }

    /**
     * Memory a byte buffer consumes
     * @param buffer ByteBuffer to calculate in memory size
     * @return Total in-memory size of the byte buffer
     */
    public static long sizeOnHeapOf(ByteBuffer buffer)
    {
        return meterBbHeapOnly.measureDeep(buffer);
    }

    public static long sizeOnHeapExcludingData(ByteBuffer buffer)
    {
        return BUFFER_EMPTY_SIZE;
    }

    /**
     * Memory a String consumes
     * @param str String to calculate memory size of
     * @return Total in-memory size of the String
     */
    public static long sizeOf(String str)
    {
        return meter.measure(str);
    }

    /**
     * @param pojo the object to measure
     * @return the size on the heap of the instance and all retained heap referenced by it, excluding portions of
     * ByteBuffer that are not directly referenced by it but including any other referenced that may also be retained
     * by other objects.
     */
    public static long measureDeep(Object pojo)
    {
        return meter.measureDeep(pojo);
    }

    /**
     * @param pojo the object to measure
     * @return the size on the heap of the instance only, excluding any referenced objects
     */
    public static long measure(Object pojo)
    {
        return meter.measure(pojo);
    }
}
