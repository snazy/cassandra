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
public final class GotoEndlessLoopClass extends JavaUDF
{
    public GotoEndlessLoopClass(TypeCodec<Object> returnDataType, TypeCodec<Object>[] argDataTypes, UDFContext udfContext)
    {
        super(returnDataType, argDataTypes, udfContext);
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

    protected Object executeAggregateImpl(int protocolVersion, Object firstParam, List<ByteBuffer> params)
    {
        return null;
    }
}
