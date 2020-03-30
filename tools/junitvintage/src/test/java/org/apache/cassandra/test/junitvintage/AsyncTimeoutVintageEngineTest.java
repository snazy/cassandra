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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.platform.engine.EngineDiscoveryRequest;
import org.junit.platform.engine.ExecutionRequest;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestEngine;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.UniqueId;

import static org.apache.cassandra.test.junitvintage.AsyncTimeoutVintageEngine.noAliveCallbackTimeNanos;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class AsyncTimeoutVintageEngineTest
{
    @Test
    public void normalExec() throws Throwable
    {
        UniqueId root = UniqueId.forEngine(AsyncTimeoutVintageEngine.ENGINE_ID);
        TestDescriptor rootDescriptor = Util.mockTestDescriptor(root, TestDescriptor.Type.CONTAINER);

        Util.RecordingEngineExecutionListener recorder = new Util.RecordingEngineExecutionListener();
        InnerClosingEngineExecutionListener listener = new InnerClosingEngineExecutionListener(recorder, rootDescriptor);

        TestDescriptor testClass1 = Util.mockTestDescriptor(root.append("runner", "TestClass1"), TestDescriptor.Type.CONTAINER);
        rootDescriptor.addChild(testClass1);
        TestDescriptor testMethod1 = Util.mockTestDescriptor(testClass1.getUniqueId().append("test", "testSomething"), TestDescriptor.Type.TEST);
        testClass1.addChild(testMethod1);

        AtomicLong time = new AtomicLong();

        AtomicReference<Throwable> failure = new AtomicReference<>();

        AtomicBoolean finishedMarker = new AtomicBoolean();

        Consumer<ExecutionRequest> executeMock = er -> {
            try
            {
                er.getEngineExecutionListener().executionStarted(rootDescriptor);
                er.getEngineExecutionListener().executionStarted(testClass1);
                er.getEngineExecutionListener().executionStarted(testMethod1);
                er.getEngineExecutionListener().executionFinished(testMethod1, TestExecutionResult.successful());
                er.getEngineExecutionListener().executionFinished(testClass1, TestExecutionResult.successful());
                er.getEngineExecutionListener().executionFinished(rootDescriptor, TestExecutionResult.successful());
            }
            catch (Throwable e)
            {
                failure.set(e);
                throw new RuntimeException(e);
            }
            finally
            {
                finishedMarker.set(true);
            }
        };
        TestEngine mock = makeMockTestEngine(executeMock);

        AsyncTimeoutVintageEngine engine = new AsyncTimeoutVintageEngine(mock, 2, time::get, () -> {
        });
        ExecutionRequest request = new ExecutionRequest(rootDescriptor, listener, null);
        engine.execute(request);

        assertTrue(finishedMarker.get(), "Mock TestEngine.execute() has not finished");
        if (failure.get() != null)
            throw failure.get();

        List<Util.RecordedEvent> expected = Arrays.asList(new Util.RecordedEvent(Util.Event.executionStarted, rootDescriptor.getUniqueId()),
                                                          new Util.RecordedEvent(Util.Event.executionStarted, testClass1.getUniqueId()),
                                                          new Util.RecordedEvent(Util.Event.executionStarted, testMethod1.getUniqueId()),
                                                          new Util.RecordedEvent(Util.Event.executionFinished, testMethod1.getUniqueId()),
                                                          new Util.RecordedEvent(Util.Event.executionFinished, testClass1.getUniqueId()),
                                                          new Util.RecordedEvent(Util.Event.executionFinished, rootDescriptor.getUniqueId()));

        assertEquals(expected, recorder.events);
    }

    @Test
    public void noTestLifecycle() throws Throwable
    {
        UniqueId root = UniqueId.forEngine(AsyncTimeoutVintageEngine.ENGINE_ID);
        TestDescriptor rootDescriptor = Util.mockTestDescriptor(root, TestDescriptor.Type.CONTAINER);

        Util.RecordingEngineExecutionListener recorder = new Util.RecordingEngineExecutionListener();
        InnerClosingEngineExecutionListener listener = new InnerClosingEngineExecutionListener(recorder, rootDescriptor);

        AtomicLong time = new AtomicLong();

        AtomicReference<Throwable> failure = new AtomicReference<>();

        AtomicBoolean finishedMarker = new AtomicBoolean();

        Consumer<ExecutionRequest> executeMock = er -> finishedMarker.set(true);

        TestEngine mock = makeMockTestEngine(executeMock);

        AsyncTimeoutVintageEngine engine = new AsyncTimeoutVintageEngine(mock, 2, time::get, () -> {
        });
        ExecutionRequest request = new ExecutionRequest(rootDescriptor, listener, null);
        engine.execute(request);

        assertTrue(finishedMarker.get(), "Mock TestEngine.execute() has not finished");
        if (failure.get() != null)
            throw failure.get();

        List<Util.RecordedEvent> expected = Collections.emptyList();

        assertEquals(expected, recorder.events);
    }

    @Test
    public void skippedTestClass() throws Throwable
    {
        UniqueId root = UniqueId.forEngine(AsyncTimeoutVintageEngine.ENGINE_ID);
        TestDescriptor rootDescriptor = Util.mockTestDescriptor(root, TestDescriptor.Type.CONTAINER);

        Util.RecordingEngineExecutionListener recorder = new Util.RecordingEngineExecutionListener();
        InnerClosingEngineExecutionListener listener = new InnerClosingEngineExecutionListener(recorder, rootDescriptor);

        TestDescriptor testClass1 = Util.mockTestDescriptor(root.append("runner", "TestClass1"), TestDescriptor.Type.CONTAINER);
        rootDescriptor.addChild(testClass1);
        TestDescriptor testMethod1 = Util.mockTestDescriptor(testClass1.getUniqueId().append("test", "testSomething"), TestDescriptor.Type.TEST);
        testClass1.addChild(testMethod1);

        AtomicLong time = new AtomicLong();

        AtomicReference<Throwable> failure = new AtomicReference<>();

        AtomicBoolean finishedMarker = new AtomicBoolean();

        Consumer<ExecutionRequest> executeMock = er -> {
            try
            {
                er.getEngineExecutionListener().executionStarted(rootDescriptor);
                er.getEngineExecutionListener().executionStarted(testClass1);
                er.getEngineExecutionListener().executionStarted(testMethod1);
                er.getEngineExecutionListener().executionSkipped(testClass1, "foo");
                er.getEngineExecutionListener().executionFinished(rootDescriptor, TestExecutionResult.successful());
            }
            catch (Throwable e)
            {
                failure.set(e);
                throw new RuntimeException(e);
            }
            finally
            {
                finishedMarker.set(true);
            }
        };
        TestEngine mock = makeMockTestEngine(executeMock);

        AsyncTimeoutVintageEngine engine = new AsyncTimeoutVintageEngine(mock, 2, time::get, () -> {
        });
        ExecutionRequest request = new ExecutionRequest(rootDescriptor, listener, null);
        engine.execute(request);

        assertTrue(finishedMarker.get(), "Mock TestEngine.execute() has not finished");
        if (failure.get() != null)
            throw failure.get();

        List<Util.RecordedEvent> expected = Arrays.asList(new Util.RecordedEvent(Util.Event.executionStarted, rootDescriptor.getUniqueId()),
                                                          new Util.RecordedEvent(Util.Event.executionStarted, testClass1.getUniqueId()),
                                                          new Util.RecordedEvent(Util.Event.executionStarted, testMethod1.getUniqueId()),
                                                          new Util.RecordedEvent(Util.Event.executionSkipped, testMethod1.getUniqueId()),
                                                          new Util.RecordedEvent(Util.Event.executionSkipped, testClass1.getUniqueId()),
                                                          new Util.RecordedEvent(Util.Event.executionFinished, rootDescriptor.getUniqueId()));

        assertEquals(expected, recorder.events);
    }

    @Test
    public void timeout() throws Throwable
    {
        ExecutorService lateAsync = Executors.newCachedThreadPool();
        try
        {

            UniqueId root = UniqueId.forEngine(AsyncTimeoutVintageEngine.ENGINE_ID);
            TestDescriptor rootDescriptor = Util.mockTestDescriptor(root, TestDescriptor.Type.CONTAINER);

            Util.RecordingEngineExecutionListener recorder = new Util.RecordingEngineExecutionListener();
            InnerClosingEngineExecutionListener listener = new InnerClosingEngineExecutionListener(recorder, rootDescriptor);

            TestDescriptor testClass1 = Util.mockTestDescriptor(root.append("runner", "TestClass1"), TestDescriptor.Type.CONTAINER);
            rootDescriptor.addChild(testClass1);
            TestDescriptor testMethod1 = Util.mockTestDescriptor(testClass1.getUniqueId().append("test", "test1"), TestDescriptor.Type.TEST);
            testClass1.addChild(testMethod1);
            TestDescriptor testMethod2 = Util.mockTestDescriptor(testClass1.getUniqueId().append("test", "test2"), TestDescriptor.Type.TEST);
            testClass1.addChild(testMethod2);
            TestDescriptor testMethod3 = Util.mockTestDescriptor(testClass1.getUniqueId().append("test", "test3"), TestDescriptor.Type.TEST);
            testClass1.addChild(testMethod3);

            AtomicLong timeSource = new AtomicLong();

            // AsyncTimeoutVintageEngine.execute() runs tests in a loop calling Future.get() with a timeout.
            // 'beforeFutureGet' is called from AsyncTimeoutVintageEngine.execute() right before the Future.get().
            // Use a "fresh" count-down-latch to hook into that loop. Calling 'awaitEngineFutureGet' twice ensures
            // that all code in the loop has been exercised at least once.
            AtomicReference<CountDownLatch> latchRef = new AtomicReference<>(new CountDownLatch(1));
            Runnable beforeFutureGet = () -> latchRef.get().countDown();
            Executable awaitEngineFutureGet = () -> {
                CountDownLatch l = new CountDownLatch(1);
                latchRef.set(l);
                l.await();
            };

            // An assertion that verifies whether the last recorded even is one that marks the whole test run as finished,
            // which would indicate a test/code failure.
            Executable assertNotFinished = () -> {
                if (recorder.events.isEmpty())
                    return;
                Util.RecordedEvent last = recorder.events.get(recorder.events.size() - 1);
                if (last.event == Util.Event.executionFinished && last.uniqueId.equals(root))
                    fail("Test engine finished too early");
            };

            AtomicReference<Throwable> failure = new AtomicReference<>();

            CountDownLatch finishedMarker = new CountDownLatch(1);

            // Simulates the situation that the actual engine implementation (junit4 in our case) continues to work
            // although the test has already been reported as failed. This can happen, but no events must be reported.
            CountDownLatch lateResultsCanStart = new CountDownLatch(1);
            CountDownLatch lateResultsFinished = new CountDownLatch(1);
            Consumer<ExecutionRequest> lateResults = er -> {
                try
                {
                    lateResultsCanStart.await();

                    TestDescriptor testMethod4 = Util.mockTestDescriptor(testClass1.getUniqueId().append("test", "test4"), TestDescriptor.Type.TEST);
                    testClass1.addChild(testMethod4);
                    er.getEngineExecutionListener().executionStarted(testMethod4);
                    er.getEngineExecutionListener().executionFinished(testMethod4, TestExecutionResult.successful());

                    TestDescriptor testMethod5 = Util.mockTestDescriptor(testClass1.getUniqueId().append("test", "test4"), TestDescriptor.Type.TEST);
                    testClass1.addChild(testMethod5);
                    er.getEngineExecutionListener().executionStarted(testMethod5);
                    er.getEngineExecutionListener().executionFinished(testMethod5, TestExecutionResult.successful());

                    er.getEngineExecutionListener().executionFinished(testClass1, TestExecutionResult.successful());
                    er.getEngineExecutionListener().executionFinished(er.getRootTestDescriptor(), TestExecutionResult.successful());

                    lateResultsFinished.countDown();
                }
                catch (Throwable e)
                {
                    e.printStackTrace();
                    failure.set(e);
                    throw new RuntimeException(e);
                }
            };

            // 'executeMock' is what the test-engine would do.
            // This mock simulates the start of a test method that times out.
            Consumer<ExecutionRequest> executeMock = er -> {
                try
                {
                    // "Start" the root-container

                    er.getEngineExecutionListener().executionStarted(er.getRootTestDescriptor());
                    AsyncTimeoutVintageEngine.testAliveCallback(er.getRootTestDescriptor().getUniqueId().toString());

                    assertNotFinished.execute();
                    awaitEngineFutureGet.execute();
                    assertNotFinished.execute();
                    awaitEngineFutureGet.execute();

                    // "Start" the test-class

                    er.getEngineExecutionListener().executionStarted(testClass1);
                    AsyncTimeoutVintageEngine.testAliveCallback(er.getRootTestDescriptor().getUniqueId().toString());

                    assertNotFinished.execute();
                    awaitEngineFutureGet.execute();
                    assertNotFinished.execute();
                    awaitEngineFutureGet.execute();

                    // simulate that testMethod1 completes super quick

                    er.getEngineExecutionListener().executionStarted(testMethod1);
                    AsyncTimeoutVintageEngine.testAliveCallback(er.getRootTestDescriptor().getUniqueId().toString());

                    assertNotFinished.execute();
                    awaitEngineFutureGet.execute();
                    assertNotFinished.execute();
                    awaitEngineFutureGet.execute();

                    er.getEngineExecutionListener().executionFinished(testMethod1, TestExecutionResult.successful());
                    AsyncTimeoutVintageEngine.testAliveCallback(er.getRootTestDescriptor().getUniqueId().toString());

                    assertNotFinished.execute();
                    awaitEngineFutureGet.execute();
                    assertNotFinished.execute();
                    awaitEngineFutureGet.execute();

                    // simulate that testMethod2 completes barely within the configured timeout

                    er.getEngineExecutionListener().executionStarted(testMethod2);
                    AsyncTimeoutVintageEngine.testAliveCallback(er.getRootTestDescriptor().getUniqueId().toString());

                    assertNotFinished.execute();
                    awaitEngineFutureGet.execute();
                    timeSource.addAndGet(TimeUnit.SECONDS.toNanos(2) - 1);
                    assertEquals(TimeUnit.SECONDS.toNanos(2) - 1, noAliveCallbackTimeNanos());
                    assertNotFinished.execute();
                    awaitEngineFutureGet.execute();

                    er.getEngineExecutionListener().executionFinished(testMethod2, TestExecutionResult.successful());
                    AsyncTimeoutVintageEngine.testAliveCallback(er.getRootTestDescriptor().getUniqueId().toString());

                    assertNotFinished.execute();
                    awaitEngineFutureGet.execute();
                    assertNotFinished.execute();
                    awaitEngineFutureGet.execute();

                    er.getEngineExecutionListener().executionStarted(testMethod3);
                    AsyncTimeoutVintageEngine.testAliveCallback(er.getRootTestDescriptor().getUniqueId().toString());

                    long timeoutDiv2 = TimeUnit.SECONDS.toNanos(2) / 2;
                    assertNotFinished.execute();
                    awaitEngineFutureGet.execute();
                    timeSource.addAndGet(timeoutDiv2);
                    assertEquals(timeoutDiv2, noAliveCallbackTimeNanos());

                    assertNotFinished.execute();
                    awaitEngineFutureGet.execute();

                    // Trigger the timeout
                    // Need a "fresh" count-down-latch here

                    CountDownLatch l = new CountDownLatch(1);
                    latchRef.set(l);

                    timeSource.addAndGet(TimeUnit.SECONDS.toNanos(2) - timeoutDiv2);
                    assertEquals(TimeUnit.SECONDS.toNanos(2), noAliveCallbackTimeNanos());

                    // AsyncTimeoutVintageEngine shuts down the test execution by shutting down the
                    // executor-service, which runs this test code. Therefore the CountDownLatch.await()
                    // must throw an InterruptedException.
                    assertNotFinished.execute();
                    assertThrows(InterruptedException.class,
                                 l::await);

                    lateAsync.submit(() -> lateResults.accept(er));
                }
                catch (Throwable e)
                {
                    e.printStackTrace();
                    failure.set(e);
                    throw new RuntimeException(e);
                }
                finally
                {
                    finishedMarker.countDown();
                }
            };
            TestEngine mock = makeMockTestEngine(executeMock);

            AsyncTimeoutVintageEngine engine = new AsyncTimeoutVintageEngine(mock, 2, timeSource::get, beforeFutureGet);
            ExecutionRequest request = new ExecutionRequest(rootDescriptor, listener, null);
            engine.execute(request);

            finishedMarker.await();
            if (failure.get() != null)
                throw failure.get();

            List<Util.RecordedEvent> expected = Arrays.asList(new Util.RecordedEvent(Util.Event.executionStarted, rootDescriptor.getUniqueId()),
                                                              new Util.RecordedEvent(Util.Event.executionStarted, testClass1.getUniqueId()),
                                                              new Util.RecordedEvent(Util.Event.executionStarted, testMethod1.getUniqueId()),
                                                              new Util.RecordedEvent(Util.Event.executionFinished, testMethod1.getUniqueId()),
                                                              new Util.RecordedEvent(Util.Event.executionStarted, testMethod2.getUniqueId()),
                                                              new Util.RecordedEvent(Util.Event.executionFinished, testMethod2.getUniqueId()),
                                                              new Util.RecordedEvent(Util.Event.executionStarted, testMethod3.getUniqueId()),
                                                              new Util.RecordedEvent(Util.Event.executionFinished, testMethod3.getUniqueId(), TimeoutException.class),
                                                              new Util.RecordedEvent(Util.Event.executionFinished, testClass1.getUniqueId(), TimeoutException.class),
                                                              new Util.RecordedEvent(Util.Event.executionFinished, rootDescriptor.getUniqueId()));

            assertEquals(expected, recorder.events);

            // The backing test-engine may continue working on the test class execution and produce more events,
            // which must not blocked (not reported upstream).
            lateResultsCanStart.countDown();
            lateResultsFinished.await();
            if (failure.get() != null)
                throw failure.get();
            // Assert that no additional events make it through
            assertEquals(expected, recorder.events);
        }
        finally
        {
            lateAsync.shutdown();
        }
    }

    static TestEngine makeMockTestEngine(Consumer<ExecutionRequest> executeMock)
    {
        return new TestEngine()
        {
            @Override
            public String getId()
            {
                return "";
            }

            @Override
            public TestDescriptor discover(EngineDiscoveryRequest discoveryRequest, UniqueId uniqueId)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void execute(ExecutionRequest request)
            {
                executeMock.accept(request);
            }
        };
    }
}
