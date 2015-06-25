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

package org.apache.cassandra.io.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SegmentedFileTest
{
    @Test
    public void testRoundingBufferSize()
    {
        assertEquals(4096, SegmentedFile.Builder.roundBufferSize(-1L));
        assertEquals(4096, SegmentedFile.Builder.roundBufferSize(0));
        assertEquals(4096, SegmentedFile.Builder.roundBufferSize(1));
        assertEquals(4096, SegmentedFile.Builder.roundBufferSize(2013));
        assertEquals(4096, SegmentedFile.Builder.roundBufferSize(4095));
        assertEquals(4096, SegmentedFile.Builder.roundBufferSize(4096));
        assertEquals(8192, SegmentedFile.Builder.roundBufferSize(4097));
        assertEquals(8192, SegmentedFile.Builder.roundBufferSize(8191));
        assertEquals(8192, SegmentedFile.Builder.roundBufferSize(8192));
        assertEquals(12288, SegmentedFile.Builder.roundBufferSize(8193));
        assertEquals(65536, SegmentedFile.Builder.roundBufferSize(65535));
        assertEquals(65536, SegmentedFile.Builder.roundBufferSize(65536));
        assertEquals(65536, SegmentedFile.Builder.roundBufferSize(65537));
        assertEquals(65536, SegmentedFile.Builder.roundBufferSize(10000000000000000L));
    }
}
