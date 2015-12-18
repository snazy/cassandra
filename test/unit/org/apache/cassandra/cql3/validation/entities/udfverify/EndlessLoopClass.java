/*
 * Copyright DataStax, Inc.
 *
 * Modified by DataStax, Inc.
 */

package org.apache.cassandra.cql3.validation.entities.udfverify;

import java.nio.ByteBuffer;
import java.util.List;

import com.datastax.driver.core.TypeCodec;
import org.apache.cassandra.cql3.functions.JavaUDF;
import org.apache.cassandra.cql3.functions.UDFContext;

/**
 * Used by {@link org.apache.cassandra.cql3.validation.entities.UFVerifierTest}.
 */
public final class EndlessLoopClass extends JavaUDF
{
    public EndlessLoopClass(TypeCodec<Object> returnDataType, TypeCodec<Object>[] argDataTypes, UDFContext udfContext)
    {
        super(returnDataType, argDataTypes, udfContext);
    }

    protected ByteBuffer executeImpl(int protocolVersion, List<ByteBuffer> params)
    {
        while (true)
        {
            if (System.currentTimeMillis() % 12345 == 99999)
                break;
            if (System.currentTimeMillis() % 12345 == 99998)
                throw new RuntimeException();
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

    protected Object executeAggregateImpl(int protocolVersion, Object firstParam, List<ByteBuffer> params)
    {
        return null;
    }
}
