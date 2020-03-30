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
package org.apache.cassandra.test.junitvintage;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import org.junit.platform.engine.EngineDiscoveryRequest;
import org.junit.platform.engine.ExecutionRequest;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestEngine;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.UniqueId;
import org.junit.vintage.engine.VintageTestEngine;

@SuppressWarnings("unused")
public class AsyncTimeoutVintageEngine implements TestEngine
{
    static final String ENGINE_ID = "timeout-junit-vintage";

    private static final String timeoutProperty = "junit.vintage.execution.timeout.seconds";

    static final long FUTURE_GET_TIMEOUT_MILLIS = 100L;

    private final TestEngine delegate;
    private final int timeoutSeconds;

    // gets a mocked time-source for testing, delegates to System.nanoTime() in prod
    private static LongSupplier nanoTimeSource;
    private static Runnable beforeFutureGet;

    private static volatile long lastAliveTimeNanos;

    public AsyncTimeoutVintageEngine()
    {
        this(new VintageTestEngine(),
             Integer.parseInt(System.getProperty(timeoutProperty, "0")),
             System::nanoTime,
             () -> {});
    }

    AsyncTimeoutVintageEngine(TestEngine delegate, int timeoutSeconds, LongSupplier nanoTimeSource, Runnable beforeFutureGet)
    {
        this.delegate = delegate;
        this.timeoutSeconds = timeoutSeconds;
        AsyncTimeoutVintageEngine.nanoTimeSource = nanoTimeSource;
        AsyncTimeoutVintageEngine.beforeFutureGet = beforeFutureGet;
    }

    /**
     * Receive a "callback" via {@link AsyncTimeoutProgressListener} for every step like "test started",
     * "test finished", "test skipped" so it's possible to only trigger a "test timed out" when there was no
     * progress, i.e. when a single test hangs, but do not trigger a "test timed out" when a whole test class
     * takes extremely long. This also allows shorter timeouts.
     */
    static void testAliveCallback(String uniqueId)
    {
        if (ENGINE_ID.equals(UniqueId.parse(uniqueId).getEngineId().orElse("")))
        {
            lastAliveTimeNanos = nanoTimeSource.getAsLong();
        }
    }

    public ThreadFactory makeThreadFactory()
    {
        AtomicInteger i = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, "vintage-timeout-" + i.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    @Override
    public String getId()
    {
        return ENGINE_ID;
    }

    @Override
    public TestDescriptor discover(EngineDiscoveryRequest discoveryRequest, UniqueId uniqueId)
    {
        return delegate.discover(discoveryRequest, uniqueId);
    }

    @Override
    public void execute(ExecutionRequest request)
    {
        if (timeoutSeconds <= 0)
        {
            delegate.execute(request);
            return;
        }

        InnerClosingEngineExecutionListener wrappedExecutionListener = new InnerClosingEngineExecutionListener(request.getEngineExecutionListener(),
                                                                                                               request.getRootTestDescriptor());

        Runnable asyncExec = () -> delegate.execute(new ExecutionRequest(request.getRootTestDescriptor(),
                                                                         wrappedExecutionListener,
                                                                         request.getConfigurationParameters()));

        ExecutorService executor = Executors.newCachedThreadPool(makeThreadFactory());

        lastAliveTimeNanos = nanoTimeSource.getAsLong();
        Future<?> f = executor.submit(asyncExec);
        try
        {
            while (true)
            {
                try
                {
                    beforeFutureGet.run();
                    f.get(FUTURE_GET_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                    break;
                }
                catch (TimeoutException e)
                {
                    if (noAliveCallbackTimeNanos() >= TimeUnit.SECONDS.toNanos(timeoutSeconds))
                        throw e;
                }
            }
        }
        catch (ExecutionException e)
        {
            wrappedExecutionListener.reportFailure(TestExecutionResult.failed(e.getCause()));
        }
        catch (TimeoutException e)
        {
            f.cancel(true);
            wrappedExecutionListener.reportFailure(TestExecutionResult.failed(new TimeoutException("Test timeout after " + timeoutSeconds + " seconds")));
        }
        catch (Throwable e)
        {
            wrappedExecutionListener.reportFailure(TestExecutionResult.failed(e));
        }
        finally
        {
            wrappedExecutionListener.executionFinished(request.getRootTestDescriptor(), TestExecutionResult.successful());
            executor.shutdown();
        }
    }

    static long noAliveCallbackTimeNanos()
    {
        return nanoTimeSource.getAsLong() - lastAliveTimeNanos;
    }
}
