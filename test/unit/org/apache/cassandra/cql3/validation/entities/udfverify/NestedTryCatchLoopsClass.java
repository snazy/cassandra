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
public final class NestedTryCatchLoopsClass extends JavaUDF
{
    public NestedTryCatchLoopsClass(TypeCodec<Object> returnDataType, TypeCodec<Object>[] argDataTypes, UDFContext udfContext)
    {
        super(returnDataType, argDataTypes, udfContext);
    }

    protected ByteBuffer executeImpl(int protocolVersion, List<ByteBuffer> params)
    {
        outer:
        while (true)
        {
            try
            {
                while (true)
                {
                    if (System.currentTimeMillis() == Long.MAX_VALUE)
                        break;
                }
                return null;
            }
            finally
            {
                continue outer;
            }
        }

//  // access flags 0x4
//  // signature (ILjava/util/List<Ljava/nio/ByteBuffer;>;)Ljava/nio/ByteBuffer;
//  // declaration: java.nio.ByteBuffer executeImpl(int, java.util.List<java.nio.ByteBuffer>)
//  protected executeImpl(ILjava/util/List;)Ljava/nio/ByteBuffer;
//    TRYCATCHBLOCK L0 L1 L2 null
//    TRYCATCHBLOCK L2 L3 L2 null
//   L0
//    LINENUMBER 46 L0
//   FRAME SAME
//    INVOKESTATIC java/lang/System.currentTimeMillis ()J
//    LDC 9223372036854775807
//    LCMP
//    IFNE L0
//   L4
//    LINENUMBER 47 L4
//    GOTO L5
//   L5
//    LINENUMBER 49 L5
//   FRAME SAME
//    ACONST_NULL
//    ASTORE 3
//   L1
//    LINENUMBER 53 L1
//    GOTO L0
//   L2
//   FRAME SAME1 java/lang/Throwable
//    ASTORE 4
//   L3
//    GOTO L0
//   L6
//    LOCALVARIABLE this Lorg/apache/cassandra/cql3/validation/entities/udfverify/NestedTryCatchLoopsClass; L0 L6 0
//    LOCALVARIABLE protocolVersion I L0 L6 1
//    LOCALVARIABLE params Ljava/util/List; L0 L6 2
//    // signature Ljava/util/List<Ljava/nio/ByteBuffer;>;
//    // declaration: java.util.List<java.nio.ByteBuffer>
//    MAXSTACK = 4
//    MAXLOCALS = 5
//}
    }

    protected Object executeAggregateImpl(int protocolVersion, Object firstParam, List<ByteBuffer> params)
    {
        return null;
    }
}
