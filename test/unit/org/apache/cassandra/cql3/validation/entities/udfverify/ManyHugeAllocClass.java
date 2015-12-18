/*
 * Copyright DataStax, Inc.
 *
 * Modified by DataStax, Inc.
 */

package org.apache.cassandra.cql3.validation.entities.udfverify;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.TypeCodec;
import org.apache.cassandra.cql3.functions.JavaUDF;
import org.apache.cassandra.cql3.functions.UDFContext;

/**
 * Used by {@link org.apache.cassandra.cql3.validation.entities.UFVerifierTest}.
 */
public final class ManyHugeAllocClass extends JavaUDF
{
    public ManyHugeAllocClass(TypeCodec<Object> returnDataType, TypeCodec<Object>[] argDataTypes, UDFContext udfContext)
    {
        super(returnDataType, argDataTypes, udfContext);
    }

    protected ByteBuffer executeImpl(int protocolVersion, List<ByteBuffer> params)
    {
        List<ByteBuffer> list = new ArrayList<>();
        for (int i = 0; i < 256; i++)
            list.add(ByteBuffer.allocate(1024 * 1024));
        return list.get((int) (System.currentTimeMillis() & 0xff));
    }

    protected Object executeAggregateImpl(int protocolVersion, Object firstParam, List<ByteBuffer> params)
    {
        return null;
    }
}
