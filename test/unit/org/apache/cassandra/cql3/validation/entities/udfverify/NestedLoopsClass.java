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
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.DataType;
import org.apache.cassandra.cql3.functions.JavaUDF;

/**
 * Used by {@link org.apache.cassandra.cql3.validation.entities.UFVerifierTest}.
 */
public final class NestedLoopsClass extends JavaUDF
{
    public NestedLoopsClass(DataType returnDataType, DataType[] argDataTypes)
    {
        super(returnDataType, argDataTypes);
    }

    protected ByteBuffer executeImpl(int protocolVersion, List<ByteBuffer> params)
    {
        List<String> l = new ArrayList<>();
        while (true)
        {
            l.add("foo");
            for (int i = 0; i < 100; i++)
                l.add("" + i);
            if (System.currentTimeMillis() == Long.MAX_VALUE)
                break;
        }
        return null;
//   L0
//    LINENUMBER 40 L0
//    NEW java/util/ArrayList
//    DUP
//    INVOKESPECIAL java/util/ArrayList.<init> ()V
//    ASTORE 3
//   L1
//    LINENUMBER 43 L1
//   FRAME APPEND [java/util/List]
//    ALOAD 3
//    LDC "foo"
//    INVOKEINTERFACE java/util/List.add (Ljava/lang/Object;)Z
//    POP
//   L2
//    LINENUMBER 44 L2
//    ICONST_0
//    ISTORE 4
//   L3
//   FRAME APPEND [I]
//    ILOAD 4
//    BIPUSH 100
//    IF_ICMPGE L4
//   L5
//    LINENUMBER 46 L5
//    ALOAD 3
//    NEW java/lang/StringBuilder
//    DUP
//    INVOKESPECIAL java/lang/StringBuilder.<init> ()V
//    LDC ""
//    INVOKEVIRTUAL java/lang/StringBuilder.append (Ljava/lang/String;)Ljava/lang/StringBuilder;
//    ILOAD 4
//    INVOKEVIRTUAL java/lang/StringBuilder.append (I)Ljava/lang/StringBuilder;
//    INVOKEVIRTUAL java/lang/StringBuilder.toString ()Ljava/lang/String;
//    INVOKEINTERFACE java/util/List.add (Ljava/lang/Object;)Z
//    POP
//   L6
//    LINENUMBER 44 L6
//    IINC 4 1
//    GOTO L3

//   L4
//    LINENUMBER 49 L4
//   FRAME CHOP 1
//    INVOKESTATIC java/lang/System.currentTimeMillis ()J
//    LDC 9223372036854775807
//    LCMP
//    IFNE L1
//   L7
//    LINENUMBER 50 L7
//    GOTO L8
//   L8
//    LINENUMBER 52 L8
//   FRAME SAME
//    ACONST_NULL
//    ARETURN
    }
}
