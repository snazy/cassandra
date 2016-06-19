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

import java.nio.ByteBuffer;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class DirectMemoryTest
{
    @BeforeClass
    public static void initTest()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void testDirectMemory()
    {
        assertFalse(DirectMemory.freeDirect(ByteBuffer.allocate(1)));
        assertFalse(DirectMemory.freeDirect(ByteBuffer.allocateDirect(1)));

        long allocations = DirectMemory.allocations();
        long allocated = DirectMemory.allocated();

        ByteBuffer buffer = DirectMemory.allocateDirect(0);
        assertEquals(allocated + DirectMemory.OVERCOUNT, DirectMemory.allocated());
        assertEquals(allocations + 1, DirectMemory.allocations());
        FileUtils.clean(buffer);
        assertEquals(allocated, DirectMemory.allocated());
        assertEquals(allocations, DirectMemory.allocations());

        ByteBuffer[] buffers = new ByteBuffer[100];
        for (int i = 0; i < buffers.length; i++)
        {
            assertEquals(allocated + i * (100L + DirectMemory.OVERCOUNT), DirectMemory.allocated());
            assertEquals(allocations + i, DirectMemory.allocations());
            buffers[i] = DirectMemory.allocateDirect(100);
        }
        for (int i = buffers.length - 1; i >= 0; i--)
        {
            FileUtils.clean(buffers[i]);
            assertEquals(allocated + i * (100L + DirectMemory.OVERCOUNT), DirectMemory.allocated());
            assertEquals(allocations + i, DirectMemory.allocations());
        }
        assertEquals(allocated, DirectMemory.allocated());
        assertEquals(allocations, DirectMemory.allocations());
        // try duplicate free
        for (ByteBuffer buf : buffers)
        {
            FileUtils.clean(buf);
            assertEquals(allocated, DirectMemory.allocated());
            assertEquals(allocations, DirectMemory.allocations());
        }

        // test over-allocation
        buffers = new ByteBuffer[1024 * 1024 / 100];
        DatabaseDescriptor.setMaxDirectMemory(1024 * 1024);
        for (int i = 0; DirectMemory.allocated() + 100 < DatabaseDescriptor.getMaxDirectMemory(); i++)
        {
            buffers[i] = DirectMemory.allocateDirect(100);
        }
        try
        {
            DirectMemory.allocateDirect(0);
            fail();
        }
        catch (OutOfMemoryError e)
        {
            // YES!
        }
    }
}
