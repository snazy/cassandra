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

package org.apache.cassandra.cql3.validation.entities.udfverify;

import java.nio.ByteBuffer;
import java.util.List;

import com.datastax.driver.core.DataType;
import org.apache.cassandra.cql3.functions.JavaUDF;

/**
 * Used by {@link org.apache.cassandra.cql3.validation.entities.UFVerifierTest}.
 */
public final class GotoEndlessLoopClass extends JavaUDF
{
    public GotoEndlessLoopClass(DataType returnDataType, DataType[] argDataTypes)
    {
        super(returnDataType, argDataTypes);
    }

    protected ByteBuffer executeImpl(int protocolVersion, List<ByteBuffer> params)
    {
        long l = 0; label: do { l++; continue label; } while (l < Long.MAX_VALUE);  return null;
    }

//  // access flags 0x4
//  // signature (ILjava/util/List<Ljava/nio/ByteBuffer;>;)Ljava/nio/ByteBuffer;
//  // declaration: java.nio.ByteBuffer executeImpl(int, java.util.List<java.nio.ByteBuffer>)
//  protected executeImpl(ILjava/util/List;)Ljava/nio/ByteBuffer;
//   L0
//    LINENUMBER 39 L0
//    LCONST_0
//    LSTORE 3
//   L1
//   FRAME APPEND [J]
//    LLOAD 3
//    LCONST_1
//    LADD
//    LSTORE 3
//    LLOAD 3
//    LDC 9223372036854775807
//    LCMP
//    IFLT L1
//    ACONST_NULL
//    ARETURN
//   L2
//    LOCALVARIABLE this Lorg/apache/cassandra/cql3/validation/entities/udfverify/GotoEndlessLoopClass; L0 L2 0
//    LOCALVARIABLE protocolVersion I L0 L2 1
//    LOCALVARIABLE params Ljava/util/List; L0 L2 2
//    // signature Ljava/util/List<Ljava/nio/ByteBuffer;>;
//    // declaration: java.util.List<java.nio.ByteBuffer>
//    LOCALVARIABLE l J L1 L2 3
//    MAXSTACK = 4
//    MAXLOCALS = 5
}
