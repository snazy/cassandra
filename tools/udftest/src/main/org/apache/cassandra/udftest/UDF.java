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
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.codahale.metrics.Timer;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import org.apache.cassandra.cql3.CqlLexer;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.ErrorCollector;
import org.apache.cassandra.cql3.ErrorListener;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.cql3.statements.CreateFunctionStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
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
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.UUIDGen;

/**
 * Wrapper around a user defined function used to test, penetrate and stress UDFs.
 * <p/>
 * Provides simple methods to simply call a UDF implementation,
 * to execute a UDF concurrently and in a loop,
 * to penetrate a UDF with a huge number of parameter value combinations.
 * <p/>
 * All you need is to write a unit test like the following:
 * {@code
 *     @Test
 *     public void basicTest() throws Throwable
 *     {
 *         UDF udf = new UDF("CREATE FUNCTION foo(inp int, d double) RETURNS text LANGUAGE javascript AS '\"x\" + inp + \"x\" + d + \"x\";'");
 *
 *         udf.penetrate();
 *
 *         UDFResult result = udf.execute(5, 2d);
 *
 *         Assert.assertEquals("x5x2x", result.getResult());
 *     }
 * }
 */
public class UDF
{
    private final UDFunction udf;

    private long timeout = 100;
    private TimeUnit timeoutUnit = TimeUnit.MILLISECONDS;
    private int version = Server.VERSION_3;

    public UDF(String cql) throws InvalidRequestException, SyntaxException
    {
        CharStream stream = new ANTLRStringStream(cql);
        CqlLexer lexer = new CqlLexer(stream);

        TokenStream tokenStream = new CommonTokenStream(lexer);
        CqlParser parser = new CqlParser(tokenStream);
        ErrorListener errorListener = new ErrorCollector(cql);
        parser.addErrorListener(errorListener);

        ParsedStatement statement;
        try
        {
            statement = parser.query();
        }
        catch (RuntimeException re)
        {
            throw new SyntaxException(String.format("Failed parsing statement: [%s] reason: %s %s",
                                                    cql,
                                                    re.getClass().getSimpleName(),
                                                    re.getMessage()));
        }
        catch (RecognitionException e)
        {
            throw new SyntaxException("Invalid or malformed CQL query string: " + e.getMessage());
        }
        if (!(statement instanceof CreateFunctionStatement))
            throw new IllegalArgumentException("Must pass a CREATE FUNCTION statement (was " + statement.getClass().getSimpleName() + ")");

        this.udf = ((CreateFunctionStatement) statement).functionForTest();
    }

    public int getVersion()
    {
        return version;
    }

    public void setVersion(int version)
    {
        this.version = version;
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

    /**
     * Override this method if you want to provide different values.
     */
    protected ByteBuffer[] variationsForBooleanType(BooleanType abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.decompose(true),
                               abstractType.decompose(false)
        };
    }

    /**
     * Override this method if you want to provide different values.
     */
    protected ByteBuffer[] variationsForBytesType(BytesType abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.compose(ByteBuffer.wrap(new byte[0])),
                               abstractType.compose(ByteBuffer.wrap(new byte[1])),
                               abstractType.compose(ByteBuffer.wrap(new byte[10]))
        };
    }

    /**
     * Override this method if you want to provide different values.
     */
    protected ByteBuffer[] variationsForDateType(DateType abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.decompose(new Date(0L)),
                               abstractType.decompose(new Date())
        };
    }

    /**
     * Override this method if you want to provide different values.
     */
    protected ByteBuffer[] variationsForDecimalType(DecimalType abstractType)
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

    /**
     * Override this method if you want to provide different values.
     */
    protected ByteBuffer[] variationsForDoubleType(DoubleType abstractType)
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

    /**
     * Override this method if you want to provide different values.
     */
    protected ByteBuffer[] variationsForFloatType(FloatType abstractType)
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

    /**
     * Override this method if you want to provide different values.
     */
    protected ByteBuffer[] variationsForInetAddressType(InetAddressType abstractType)
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

    /**
     * Override this method if you want to provide different values.
     */
    protected ByteBuffer[] variationsForInt32Type(Int32Type abstractType)
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

    /**
     * Override this method if you want to provide different values.
     */
    protected ByteBuffer[] variationsForIntegerType(IntegerType abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.decompose(BigInteger.ONE),
                               abstractType.decompose(BigInteger.TEN),
                               abstractType.decompose(BigInteger.ZERO),
                               abstractType.decompose(BigInteger.valueOf(-1L))
        };
    }

    /**
     * Override this method if you want to provide different values.
     */
    protected ByteBuffer[] variationsForLongType(LongType abstractType)
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

    /**
     * Override this method if you want to provide different values.
     */
    protected ByteBuffer[] variationsForTimestampType(TimestampType abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.decompose(new Date(0L)),
                               abstractType.decompose(new Date())
        };
    }

    /**
     * Override this method if you want to provide different values.
     */
    protected ByteBuffer[] variationsForAsciiType(AsciiType abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.fromString("foo")
        };
    }

    /**
     * Override this method if you want to provide different values.
     */
    protected ByteBuffer[] variationsForUTF8Type(UTF8Type abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.fromString("foo"),
                               abstractType.fromString("foo \u00e4\u00f6\u00fc\u00df")
        };
    }

    /**
     * Override this method if you want to provide different values.
     */
    protected ByteBuffer[] variationsForUUIDType(UUIDType abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.decompose(UUID.randomUUID()),
                               abstractType.decompose(UUID.fromString("foo bar baz")),
                               abstractType.decompose(UUIDGen.getTimeUUID()),
                               abstractType.decompose(UUIDGen.getTimeUUID(0))
        };
    }

    /**
     * Override this method if you want to provide different values.
     */
    protected ByteBuffer[] variationsForLexicalUUIDType(LexicalUUIDType abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.decompose(UUID.fromString("foo bar baz"))
        };
    }

    /**
     * Override this method if you want to provide different values.
     */
    protected ByteBuffer[] variationsForTimeUUIDType(TimeUUIDType abstractType)
    {
        return new ByteBuffer[]{
                               null,
                               abstractType.decompose(UUIDGen.getTimeUUID()),
                               abstractType.decompose(UUIDGen.getTimeUUID(0))
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
                udf.execute(version, args);
            }
        }
    }

    /**
     * Get the current timeout value in {@link #getTimeoutUnit()}.
     * Default value is 100 ms.
     */
    public long getTimeout()
    {
        return timeout;
    }

    /**
     * Set the current timeout value in {@link #getTimeoutUnit()}.
     * Default value is 100 ms.
     */
    public void setTimeout(long timeout)
    {
        if (timeout <= 0)
            throw new IllegalArgumentException("timeout must be positive");
        this.timeout = timeout;
    }

    /**
     * Get the timeout unit for {@link #getTimeout()}.
     * Default value is 100 ms.
     */
    public TimeUnit getTimeoutUnit()
    {
        return timeoutUnit;
    }

    /**
     * Set the timeout unit for {@link #getTimeout()}.
     * Default value is 100 ms.
     */
    public void setTimeoutUnit(TimeUnit timeoutUnit)
    {
        if (timeoutUnit == null)
            throw new NullPointerException();
        this.timeoutUnit = timeoutUnit;
    }

    /**
     * Calls the UDF with the given arguments.
     * Just calls {@link #executeConcurrentLoops(int, int, Object...)} with one thread and one loop.
     */
    public UDFResult execute(Object... arguments) throws InvalidRequestException, TimeoutException
    {
        return executeConcurrentInt(1, 1, null, 0, arguments);
    }

    /**
     * Executes the UDF using multiple concurrent threads and multiple iterations inside one thread to detect
     * whether a UDF is non-deterministic and whether it has unwanted concurrency issues.
     *
     * @param concurrency number of concurrent threads
     * @param loops       number of loops
     * @param arguments   UDF arguments
     * @return return value
     * @throws InvalidRequestException               if an execution failure occured
     * @throws java.util.concurrent.TimeoutException if UDF execution took too long
     * @throws java.lang.RuntimeException            if an unexpected exception occured
     * @see #getTimeout()
     * @see #getTimeoutUnit()
     */
    public UDFResult executeConcurrentLoops(int concurrency, int loops, Object... arguments) throws InvalidRequestException, TimeoutException
    {
        if (loops < 1)
            throw new IllegalArgumentException("Loops must be 1 or higher");

        return executeConcurrentInt(concurrency, loops, null, 0, arguments);
    }

    /**
     * Executes the UDF using multiple concurrent threads and multiple iterations inside one thread to detect
     * whether a UDF is non-deterministic and whether it has unwanted concurrency issues.
     *
     * @param concurrency  number of concurrent threads
     * @param endAfter     end concurrent execution test after this time
     * @param endAfterUnit end concurrent execution test after this time
     * @param arguments    UDF arguments
     * @return return value
     * @throws InvalidRequestException               if an execution failure occured
     * @throws java.util.concurrent.TimeoutException if UDF execution took too long
     * @throws java.lang.RuntimeException            if an unexpected exception occured
     * @see #getTimeout()
     * @see #getTimeoutUnit()
     */
    public UDFResult executeConcurrentTimed(int concurrency, TimeUnit endAfterUnit, long endAfter, Object... arguments) throws InvalidRequestException, TimeoutException
    {
        if (endAfter < 1)
            throw new IllegalArgumentException("endAfter must be 1 or higher");
        if (endAfterUnit == null)
            throw new NullPointerException("endAfterUnit is null");

        return executeConcurrentInt(concurrency, 0, endAfterUnit, endAfter, arguments);
    }

    private UDFResult executeConcurrentInt(int concurrency, final int loops, final TimeUnit endAfterUnit, final long endAfter, final Object... arguments) throws InvalidRequestException, TimeoutException
    {
        if (concurrency < 1)
            throw new IllegalArgumentException("Concurrency must be 1 or higher");

        checkArguments(arguments);

        final Timer timer = new Timer();

        ExecutorService executorService = Executors.newFixedThreadPool(concurrency);
        try
        {
            List<Future<ByteBuffer>> futures = new ArrayList<>(concurrency);
            final CountDownLatch latch = new CountDownLatch(concurrency);
            for (int i = 0; i < concurrency; i++)
                futures.add(executorService.submit(new Callable<ByteBuffer>()
                {
                    private long endAt;

                    public ByteBuffer call() throws Exception
                    {
                        // start when all threads are ready to go
                        latch.countDown();

                        endAt = endAfterUnit != null ? System.currentTimeMillis() + endAfterUnit.toMillis(endAfter) : 0L;

                        List<ByteBuffer> params = buildArguments(arguments);

                        // execute the UDF the first time
                        long t0 = System.nanoTime();
                        ByteBuffer ref = udf.execute(version, params);
                        timer.update(System.nanoTime() - t0, TimeUnit.NANOSECONDS);

                        for (int i = 1; endReached(i); i++)
                        {
                            // reset the input parameters so we can reuse the ByteBuffer instances
                            for (ByteBuffer param : params)
                                if (param != null)
                                    param.rewind();

                            // execute the UDF (again and again
                            t0 = System.nanoTime();
                            ByteBuffer cmp = udf.execute(version, params);
                            timer.update(System.nanoTime() - t0, TimeUnit.NANOSECONDS);

                            // compare the result, if the UDF is deterministic (pure)
                            if (udf.isPure() && !bbEqual(ref, cmp))
                                throw new AssertionError("UDF result is not deterministic");
                        }
                        return ref;
                    }

                    private boolean endReached(int i)
                    {
                        return loops > 0
                               ? i < loops
                               : System.currentTimeMillis() < endAt;
                    }
                }));

            // collect UDF return values by querying the futures
            List<ByteBuffer> results = new ArrayList<>(concurrency);
            long timeoutAtNanos = System.nanoTime() + (loops >= 1
                                                       ? timeoutUnit.toNanos(timeout) * loops
                                                       : (endAfterUnit.toNanos(endAfter) + 250L * 1000L * 1000L));
            for (Future<ByteBuffer> future : futures)
                try
                {
                    results.add(future.get(timeoutAtNanos - System.nanoTime(), TimeUnit.NANOSECONDS));
                }
                catch (InterruptedException | ExecutionException e)
                {
                    if (e.getCause() instanceof InvalidRequestException)
                        throw (InvalidRequestException) e.getCause();
                    throw new RuntimeException(e);
                }

            // use the first result as the return value
            ByteBuffer ref = results.get(0);

            // compare the result, if the UDF is deterministic (pure)
            if (udf.isPure())
                for (int i = 1; i < concurrency; i++)
                    if (!bbEqual(ref, results.get(i)))
                        throw new AssertionError("UDF result is not deterministic");

            return new UDFResult(ref != null ? udf.returnType().compose(ref) : null,
                                 timer);
        }
        finally
        {
            executorService.shutdown();
        }
    }

    private boolean bbEqual(ByteBuffer ref, ByteBuffer cmp)
    {
        return ref == null && cmp == null || !(ref == null || cmp == null) && ref.equals(cmp);
    }

    private void checkArguments(Object[] arguments)
    {
        if (arguments.length != udf.argTypes().size())
            throw new IllegalArgumentException("UDF '" + udf + "' expects " + udf.argTypes().size() + " arguments, but " + arguments.length + " given");
    }

    private List<ByteBuffer> buildArguments(Object[] arguments)
    {
        List<ByteBuffer> params = new ArrayList<>();
        for (int i = 0; i < arguments.length; i++)
        {
            if (arguments[i] == null)
            {
                params.add(null);
                continue;
            }
            AbstractType type = udf.argTypes().get(i);
            params.add(type.decompose(arguments[i]));
        }
        return params;
    }
}
