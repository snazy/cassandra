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
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.UniqueId;

import org.apache.cassandra.test.junitvintage.Util.RecordedEvent;
import org.apache.cassandra.test.junitvintage.Util.RecordingEngineExecutionListener;
import org.apache.cassandra.test.junitvintage.Util.Event;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InnerClosingEngineExecutionListenerTest
{
    static final String ENGINE_ID = "engine-id";

    @Test
    public void normalLifecycle()
    {
        UniqueId root = UniqueId.forEngine(ENGINE_ID);
        TestDescriptor rootDescriptor = Util.mockTestDescriptor(root, TestDescriptor.Type.CONTAINER);

        RecordingEngineExecutionListener recorder = new RecordingEngineExecutionListener();
        InnerClosingEngineExecutionListener listener = new InnerClosingEngineExecutionListener(recorder, rootDescriptor);

        TestDescriptor testClass1 = Util.mockTestDescriptor(root.append("runner", "TestClass1"), TestDescriptor.Type.CONTAINER);
        rootDescriptor.addChild(testClass1);
        TestDescriptor testMethod1 = Util.mockTestDescriptor(testClass1.getUniqueId().append("test", "testSomething"), TestDescriptor.Type.TEST);
        testClass1.addChild(testMethod1);

        listener.executionStarted(rootDescriptor);
        listener.executionStarted(testClass1);
        listener.executionStarted(testMethod1);
        listener.executionFinished(testMethod1, TestExecutionResult.successful());
        listener.executionFinished(testClass1, TestExecutionResult.successful());
        listener.executionFinished(rootDescriptor, TestExecutionResult.successful());

        List<RecordedEvent> expected = Arrays.asList(new RecordedEvent(Event.executionStarted, rootDescriptor.getUniqueId()),
                                                     new RecordedEvent(Event.executionStarted, testClass1.getUniqueId()),
                                                     new RecordedEvent(Event.executionStarted, testMethod1.getUniqueId()),
                                                     new RecordedEvent(Event.executionFinished, testMethod1.getUniqueId()),
                                                     new RecordedEvent(Event.executionFinished, testClass1.getUniqueId()),
                                                     new RecordedEvent(Event.executionFinished, rootDescriptor.getUniqueId()));

        assertEquals(expected, recorder.events);
    }

    @Test
    public void abortedLifecycle()
    {
        UniqueId root = UniqueId.forEngine(ENGINE_ID);
        TestDescriptor rootDescriptor = Util.mockTestDescriptor(root, TestDescriptor.Type.CONTAINER);

        RecordingEngineExecutionListener recorder = new RecordingEngineExecutionListener();
        InnerClosingEngineExecutionListener listener = new InnerClosingEngineExecutionListener(recorder, rootDescriptor);

        TestDescriptor testClass1 = Util.mockTestDescriptor(root.append("runner", "TestClass1"), TestDescriptor.Type.CONTAINER);
        rootDescriptor.addChild(testClass1);
        TestDescriptor testMethod1 = Util.mockTestDescriptor(testClass1.getUniqueId().append("test", "testSomething"), TestDescriptor.Type.TEST);
        testClass1.addChild(testMethod1);

        listener.executionStarted(rootDescriptor);
        listener.executionStarted(testClass1);
        listener.executionStarted(testMethod1);
        listener.executionFinished(rootDescriptor, TestExecutionResult.successful());

        List<RecordedEvent> expected = Arrays.asList(new RecordedEvent(Event.executionStarted, rootDescriptor.getUniqueId()),
                                                     new RecordedEvent(Event.executionStarted, testClass1.getUniqueId()),
                                                     new RecordedEvent(Event.executionStarted, testMethod1.getUniqueId()),
                                                     new RecordedEvent(Event.executionSkipped, testMethod1.getUniqueId()),
                                                     new RecordedEvent(Event.executionSkipped, testClass1.getUniqueId()),
                                                     new RecordedEvent(Event.executionFinished, rootDescriptor.getUniqueId()));

        assertEquals(expected, recorder.events);
    }

    @Test
    public void abortedLifecycleWithLateEvents()
    {
        UniqueId root = UniqueId.forEngine(ENGINE_ID);
        TestDescriptor rootDescriptor = Util.mockTestDescriptor(root, TestDescriptor.Type.CONTAINER);

        RecordingEngineExecutionListener recorder = new RecordingEngineExecutionListener();
        InnerClosingEngineExecutionListener listener = new InnerClosingEngineExecutionListener(recorder, rootDescriptor);

        TestDescriptor testClass1 = Util.mockTestDescriptor(root.append("runner", "TestClass1"), TestDescriptor.Type.CONTAINER);
        rootDescriptor.addChild(testClass1);
        TestDescriptor testMethod1 = Util.mockTestDescriptor(testClass1.getUniqueId().append("test", "testSomething"), TestDescriptor.Type.TEST);
        testClass1.addChild(testMethod1);

        listener.executionStarted(rootDescriptor);
        listener.executionStarted(testClass1);
        listener.executionStarted(testMethod1);
        listener.executionFinished(rootDescriptor, TestExecutionResult.successful());

        List<RecordedEvent> expected = Arrays.asList(new RecordedEvent(Event.executionStarted, rootDescriptor.getUniqueId()),
                                                     new RecordedEvent(Event.executionStarted, testClass1.getUniqueId()),
                                                     new RecordedEvent(Event.executionStarted, testMethod1.getUniqueId()),
                                                     new RecordedEvent(Event.executionSkipped, testMethod1.getUniqueId()),
                                                     new RecordedEvent(Event.executionSkipped, testClass1.getUniqueId()),
                                                     new RecordedEvent(Event.executionFinished, rootDescriptor.getUniqueId()));

        assertEquals(expected, recorder.events);

        listener.executionFinished(testMethod1, TestExecutionResult.successful());
        listener.executionFinished(testClass1, TestExecutionResult.successful());
        listener.executionFinished(rootDescriptor, TestExecutionResult.successful());

        assertEquals(expected, recorder.events);
    }

    @Test
    public void failedLifecycleWithLateEvents()
    {
        UniqueId root = UniqueId.forEngine(ENGINE_ID);
        TestDescriptor rootDescriptor = Util.mockTestDescriptor(root, TestDescriptor.Type.CONTAINER);

        RecordingEngineExecutionListener recorder = new RecordingEngineExecutionListener();
        InnerClosingEngineExecutionListener listener = new InnerClosingEngineExecutionListener(recorder, rootDescriptor);

        TestDescriptor testClass1 = Util.mockTestDescriptor(root.append("runner", "TestClass1"), TestDescriptor.Type.CONTAINER);
        rootDescriptor.addChild(testClass1);
        TestDescriptor testMethod1 = Util.mockTestDescriptor(testClass1.getUniqueId().append("test", "testSomething"), TestDescriptor.Type.TEST);
        testClass1.addChild(testMethod1);

        listener.executionStarted(rootDescriptor);
        listener.executionStarted(testClass1);
        listener.executionStarted(testMethod1);
        listener.reportFailure(TestExecutionResult.failed(new Exception()));
        listener.executionFinished(rootDescriptor, TestExecutionResult.successful());

        List<RecordedEvent> expected = Arrays.asList(new RecordedEvent(Event.executionStarted, rootDescriptor.getUniqueId()),
                                                     new RecordedEvent(Event.executionStarted, testClass1.getUniqueId()),
                                                     new RecordedEvent(Event.executionStarted, testMethod1.getUniqueId()),
                                                     new RecordedEvent(Event.executionFinished, testMethod1.getUniqueId(), Exception.class),
                                                     new RecordedEvent(Event.executionFinished, testClass1.getUniqueId(), Exception.class),
                                                     new RecordedEvent(Event.executionFinished, rootDescriptor.getUniqueId()));

        assertEquals(expected, recorder.events);

        listener.executionFinished(testMethod1, TestExecutionResult.successful());
        listener.executionFinished(testClass1, TestExecutionResult.successful());
        listener.executionFinished(rootDescriptor, TestExecutionResult.successful());

        assertEquals(expected, recorder.events);
    }
}
