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
public final class EndlessLoopClass extends JavaUDF
{
    public EndlessLoopClass(DataType returnDataType, DataType[] argDataTypes)
    {
        super(returnDataType, argDataTypes);
    }

    protected ByteBuffer executeImpl(int protocolVersion, List<ByteBuffer> params)
    {
        while (true)
        {
            if (System.currentTimeMillis() % 12345 == 99999)
                break;
            if (System.currentTimeMillis() % 12345 == 99998)
                throw new RuntimeException();
            System.out.println("Hello");
        }
        return null;
//   L0
//    LINENUMBER 50 L0
//   FRAME SAME
//    INVOKESTATIC java/lang/System.currentTimeMillis ()J
//    LDC 12345
//    LREM
//    LDC 99999
//    LCMP
//    IFNE L1
//   L2
//    LINENUMBER 54 L2
//    GOTO L3
//   L1
//    LINENUMBER 64 L1
//   FRAME SAME
//    INVOKESTATIC java/lang/System.currentTimeMillis ()J
//    LDC 12345
//    LREM
//    LDC 99998
//    LCMP
//    IFNE L4
//   L5
//    LINENUMBER 71 L5
//    NEW java/lang/RuntimeException
//    DUP
//    INVOKESPECIAL java/lang/RuntimeException.<init> ()V
//    ATHROW
//   L4
//    LINENUMBER 79 L4
//   FRAME SAME
//    GETSTATIC java/lang/System.out : Ljava/io/PrintStream;
//    LDC "Hello"
//    INVOKEVIRTUAL java/io/PrintStream.println (Ljava/lang/String;)V
//    GOTO L0
//   L3
//    LINENUMBER 86 L3
//   FRAME SAME
//    ACONST_NULL
//    ARETURN
    }
}
