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
package org.apache.cassandra.udftest;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.cql3.statements.CreateFunctionStatement;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.exceptions.InvalidRequestException;

public final class UDF
{
    private final UDFunction udf;

    UDF(CreateFunctionStatement statement) throws InvalidRequestException
    {
        this.udf = statement.functionForTest();
    }

    /**
     * Calls the UDF using many possible combinations of {@code null} and non-null values
     * plus some random data for the specific types. Execution time of this function
     * increases exponentially with the number of UDF arguments.
     */
    public void penetrate() throws InvalidRequestException
    {
        ByteBuffer[][] variations = new ByteBuffer[udf.argTypes().size()][];

        List<ByteBuffer> params = new ArrayList<>(variations.length);

        for (int i = 0; i < variations.length; i++)
        {
            // 1. fill params element
            params.add(null);

            // 2. generate argument type variations
            variations[i] = variationsFor(udf.argTypes().get(i));
        }

        penetrate(variations, 0, params);
    }

    protected ByteBuffer[] variationsFor(AbstractType<?> abstractType)
    {
        if (abstractType instanceof AsciiType)
            return variationsForAsciiType((AsciiType) abstractType);
        if (abstractType instanceof BooleanType)
            return variationsForBooleanType((BooleanType) abstractType);
        if (abstractType instanceof BytesType)
            return variationsForBytesType((BytesType) abstractType);
        if (abstractType instanceof DateType)
            return variationsForDateType((DateType) abstractType);
        if (abstractType instanceof DecimalType)
            return variationsForDecimalType((DecimalType) abstractType);
        if (abstractType instanceof DoubleType)
            return variationsForDoubleType((DoubleType) abstractType);
        if (abstractType instanceof FloatType)
            return variationsForFloatType((FloatType) abstractType);
        if (abstractType instanceof InetAddressType)
            return variationsForInetAddressType((InetAddressType) abstractType);
        if (abstractType instanceof Int32Type)
            return variationsForInt32Type((Int32Type) abstractType);
        if (abstractType instanceof IntegerType)
            return variationsForIntegerType((IntegerType) abstractType);
        if (abstractType instanceof LexicalUUIDType)
            return variationsForLexicalUUIDType((LexicalUUIDType) abstractType);
        if (abstractType instanceof LongType)
            return variationsForLongType((LongType) abstractType);
        if (abstractType instanceof TimeUUIDType)
            return variationsForTimeUUIDType((TimeUUIDType) abstractType);
        if (abstractType instanceof TimestampType)
            return variationsForTimestampType((TimestampType) abstractType);
        if (abstractType instanceof UTF8Type)
            return variationsForUTF8Type((UTF8Type) abstractType);
        if (abstractType instanceof UUIDType)
            return variationsForUUIDType((UUIDType) abstractType);
        return new ByteBuffer[]{ null };
    }

    private ByteBuffer[] variationsForAsciiType(AsciiType abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.fromString("foo")
        };
    }

    private ByteBuffer[] variationsForBooleanType(BooleanType abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.decompose(true),
                               abstractType.decompose(false)
        };
    }

    private ByteBuffer[] variationsForBytesType(BytesType abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.compose(ByteBuffer.wrap(new byte[0])),
                               abstractType.compose(ByteBuffer.wrap(new byte[1])),
                               abstractType.compose(ByteBuffer.wrap(new byte[10]))
        };
    }

    private ByteBuffer[] variationsForDateType(DateType abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.decompose(new Date(0L)),
                               abstractType.decompose(new Date())
        };
    }

    private ByteBuffer[] variationsForDecimalType(DecimalType abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.decompose(BigDecimal.ONE),
                               abstractType.decompose(BigDecimal.TEN),
                               abstractType.decompose(BigDecimal.ZERO),
                               abstractType.decompose(BigDecimal.valueOf(-1d)),
                               abstractType.decompose(BigDecimal.valueOf(Double.NaN)),
                               abstractType.decompose(BigDecimal.valueOf(Double.POSITIVE_INFINITY)),
                               abstractType.decompose(BigDecimal.valueOf(Double.NEGATIVE_INFINITY))
        };
    }

    private ByteBuffer[] variationsForDoubleType(DoubleType abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.decompose(1d),
                               abstractType.decompose(10d),
                               abstractType.decompose(0d),
                               abstractType.decompose(-1d),
                               abstractType.decompose(Double.NaN),
                               abstractType.decompose(Double.POSITIVE_INFINITY),
                               abstractType.decompose(Double.NEGATIVE_INFINITY),
                               abstractType.decompose(Double.MIN_VALUE),
                               abstractType.decompose(Double.MAX_VALUE)
        };
    }

    private ByteBuffer[] variationsForFloatType(FloatType abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.decompose(1f),
                               abstractType.decompose(10f),
                               abstractType.decompose(0f),
                               abstractType.decompose(-1f),
                               abstractType.decompose(Float.NaN),
                               abstractType.decompose(Float.POSITIVE_INFINITY),
                               abstractType.decompose(Float.NEGATIVE_INFINITY),
                               abstractType.decompose(Float.MIN_VALUE),
                               abstractType.decompose(Float.MAX_VALUE)
        };
    }

    private ByteBuffer[] variationsForInetAddressType(InetAddressType abstractType)
    {
        try
        {
            return new ByteBuffer[]{
                                   null,
                                   abstractType.decompose(InetAddress.getLoopbackAddress()),
                                   abstractType.decompose(Inet4Address.getByAddress(new byte[]{ (byte) 255, (byte) 255, (byte) 255, (byte) 255 })),
                                   abstractType.decompose(Inet4Address.getByAddress(new byte[]{ (byte) 0, (byte) 0, (byte) 0, (byte) 0 }))
            };
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private ByteBuffer[] variationsForInt32Type(Int32Type abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.decompose(0),
                               abstractType.decompose(-1),
                               abstractType.decompose(1),
                               abstractType.decompose(Integer.MIN_VALUE),
                               abstractType.decompose(Integer.MAX_VALUE)
        };
    }

    private ByteBuffer[] variationsForIntegerType(IntegerType abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.decompose(BigInteger.ONE),
                               abstractType.decompose(BigInteger.TEN),
                               abstractType.decompose(BigInteger.ZERO),
                               abstractType.decompose(BigInteger.valueOf(-1L))
        };
    }

    private ByteBuffer[] variationsForLexicalUUIDType(LexicalUUIDType abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.decompose(UUID.fromString("foo bar baz"))
        };
    }

    private ByteBuffer[] variationsForLongType(LongType abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.decompose(0L),
                               abstractType.decompose(-1L),
                               abstractType.decompose(1L),
                               abstractType.decompose(Long.MIN_VALUE),
                               abstractType.decompose(Long.MAX_VALUE)
        };
    }

    private ByteBuffer[] variationsForTimeUUIDType(TimeUUIDType abstractType)
    {
        return new ByteBuffer[]{
                               null
        };
    }

    private ByteBuffer[] variationsForTimestampType(TimestampType abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.decompose(new Date(0L)),
                               abstractType.decompose(new Date())
        };
    }

    private ByteBuffer[] variationsForUTF8Type(UTF8Type abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.fromString("foo"),
                               abstractType.fromString("foo \u00e4\u00f6\u00fc\u00df")
        };
    }

    private ByteBuffer[] variationsForUUIDType(UUIDType abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.decompose(UUID.randomUUID())
        };
    }

    private void penetrate(ByteBuffer[][] variations, int off, List<ByteBuffer> params) throws InvalidRequestException
    {
        int mine = variations[off].length;
        for (int i = 0; i < mine; i++)
        {
            params.set(off, variations[off][i]);

            if (off < variations.length - 1)
                penetrate(variations, off + 1, params);
            else
            {
                List<ByteBuffer> args = new ArrayList<>(params.size());
                for (ByteBuffer param : params)
                    args.add(param != null ? param.duplicate() : null);
                udf.execute(args);
            }
        }
    }

    /**
     * Calls the UDF with the given arguments.
     */
    public Object execute(Object... arguments) throws InvalidRequestException
    {
        List<AbstractType<?>> argTypes = udf.argTypes();

        if (arguments.length != argTypes.size())
            throw new IllegalArgumentException("UDF '" + udf + "' expects " + argTypes.size() + " arguments, but " + arguments.length + " given");

        List<ByteBuffer> params = new ArrayList<>();
        for (int i = 0; i < arguments.length; i++)
        {
            if (arguments[i] == null)
            {
                params.add(null);
                continue;
            }
            AbstractType type = argTypes.get(i);
            params.add(type.decompose(arguments[i]));
        }
        ByteBuffer result = udf.execute(params);

        return result != null ? udf.returnType().compose(result) : null;
    }
}
