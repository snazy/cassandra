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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.platform.engine.EngineExecutionListener;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.reporting.ReportEntry;
import org.junit.platform.engine.support.descriptor.AbstractTestDescriptor;

public class Util
{

    static TestDescriptor mockTestDescriptor(UniqueId uniqueId, TestDescriptor.Type type)
    {
        return new AbstractTestDescriptor(uniqueId, uniqueId.toString())
        {
            @Override
            public Type getType()
            {
                return type;
            }
        };
    }

    static class RecordingEngineExecutionListener implements EngineExecutionListener
    {
        List<RecordedEvent> events = new CopyOnWriteArrayList<>();

        @Override
        public void dynamicTestRegistered(TestDescriptor testDescriptor)
        {
            events.add(new RecordedEvent(Event.dynamicTestRegistered, testDescriptor.getUniqueId(), null));
        }

        @Override
        public void executionSkipped(TestDescriptor testDescriptor, String reason)
        {
            events.add(new RecordedEvent(Event.executionSkipped, testDescriptor.getUniqueId()));
        }

        @Override
        public void executionStarted(TestDescriptor testDescriptor)
        {
            events.add(new RecordedEvent(Event.executionStarted, testDescriptor.getUniqueId()));
        }

        @Override
        public void executionFinished(TestDescriptor testDescriptor, TestExecutionResult testExecutionResult)
        {
            Class<?> result = testExecutionResult.getThrowable().isPresent() ? testExecutionResult.getThrowable().get().getClass() : null;
            events.add(new RecordedEvent(Event.executionFinished, testDescriptor.getUniqueId(), result));
        }

        @Override
        public void reportingEntryPublished(TestDescriptor testDescriptor, ReportEntry entry)
        {
            events.add(new RecordedEvent(Event.reportingEntryPublished, testDescriptor.getUniqueId()));
        }
    }

    enum Event
    {
        dynamicTestRegistered,
        executionSkipped,
        executionStarted,
        executionFinished,
        reportingEntryPublished
    }

    static class RecordedEvent
    {
        final Event event;
        final UniqueId uniqueId;
        final Class<?> result;

        RecordedEvent(Event event, UniqueId uniqueId)
        {
            this(event, uniqueId, null);
        }

        RecordedEvent(Event event, UniqueId uniqueId, Class<?> result)
        {
            this.event = event;
            this.uniqueId = uniqueId;
            this.result = result;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RecordedEvent that = (RecordedEvent) o;
            return event == that.event &&
                   Objects.equals(uniqueId, that.uniqueId) &&
                   Objects.equals(result, that.result);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(event, uniqueId, result);
        }

        @Override
        public String toString()
        {
            return "{" +
                   "event=" + event +
                   ", uniqueId=" + uniqueId +
                   ", result=" + result +
                   '}';
        }
    }
}
