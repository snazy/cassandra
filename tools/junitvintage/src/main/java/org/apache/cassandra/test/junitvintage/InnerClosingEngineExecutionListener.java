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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.platform.engine.EngineExecutionListener;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.reporting.ReportEntry;

/**
 * JUnit 5 platform's contract for the {@link EngineExecutionListener} is very strict wrt
 * "started" and "stopped" (finished/skipped) callbacks, so we do some things to ensure that:
 * - in case of a timeout, events from the underlying test, which may still be running, are blocked
 * - perform a "skipped" callback for all children, that have been started
 */
final class InnerClosingEngineExecutionListener implements EngineExecutionListener
{
    private final EngineExecutionListener l;
    private final java.util.Set<UniqueId> stopped = new java.util.HashSet<>();
    private final java.util.Map<UniqueId, TestDescriptor> started = new java.util.HashMap<>();
    private final TestDescriptor rootDescriptor;

    volatile TestDescriptor underneathRoot;

    InnerClosingEngineExecutionListener(EngineExecutionListener l, TestDescriptor rootDescriptor)
    {
        this.l = l;
        this.rootDescriptor = rootDescriptor;
    }

    @Override
    public void dynamicTestRegistered(TestDescriptor testDescriptor)
    {
        if (canProcess(testDescriptor, Type.EXEC))
            l.dynamicTestRegistered(testDescriptor);
    }

    @Override
    public void executionSkipped(TestDescriptor testDescriptor, String reason)
    {
        if (canProcess(testDescriptor, Type.STOP))
            l.executionSkipped(testDescriptor, reason);
    }

    @Override
    public void executionStarted(TestDescriptor testDescriptor)
    {
        if (canProcess(testDescriptor, Type.START))
        {
            l.executionStarted(testDescriptor);
            if (testDescriptor.getParent().isPresent() && testDescriptor.getParent().get() == rootDescriptor)
                underneathRoot = testDescriptor;
        }
    }

    @Override
    public void executionFinished(TestDescriptor testDescriptor, TestExecutionResult testExecutionResult)
    {
        if (canProcess(testDescriptor, Type.STOP))
            l.executionFinished(testDescriptor, testExecutionResult);
    }

    @Override
    public void reportingEntryPublished(TestDescriptor testDescriptor, ReportEntry entry)
    {
        if (canProcess(testDescriptor, Type.EXEC))
            l.reportingEntryPublished(testDescriptor, entry);
    }

    private synchronized boolean canProcess(TestDescriptor testDescriptor, Type type)
    {
        UniqueId id = testDescriptor.getUniqueId();

        // If the test-descriptor's ID or any of its parents is already stopped, don't process the event.
        if (stopped.stream().anyMatch(id::hasPrefix))
        {
            return false;
        }

        boolean r = true;

        switch (type)
        {
            case EXEC:
                if (!started.containsKey(id))
                    r = false;
                break;
            case START:
                started.put(id, testDescriptor);
                break;
            case STOP:
                if (!started.containsKey(id))
                    r = false;

                // Mark the TestDescriptor to be stopped as stopped before the processing below, so we do not send an
                // 'executionSkipped' in below.
                stopped.add(id);
                started.remove(id);

                notStopped(id).forEach(this::notifyAsSkipped);
                break;
        }

        return r;
    }

    private Stream<TestDescriptor> notStopped(UniqueId id)
    {
        // Inform the listener that the children are being skipped, if those haven't stopped yet.
        List<TestDescriptor> notStopped = new ArrayList<>();

        // Collect the unique-IDs of the not-yet-stopped test-descriptors first
        for (Map.Entry<UniqueId, TestDescriptor> e : started.entrySet())
            if (e.getKey().hasPrefix(id) && !stopped.contains(e.getKey()))
                notStopped.add(e.getValue());

        // Then process them - "longest" paths (unique-id segments) first
        return notStopped.stream()
                         .sorted(Comparator.comparingInt(a -> Integer.MAX_VALUE - a.getUniqueId().getSegments().size()));
    }

    public synchronized void reportFailure(TestExecutionResult result)
    {
        notStopped(topDescriptor().getUniqueId()).forEach(td ->
                                                          {
                                                              l.executionFinished(td, result);
                                                              stopped.add(td.getUniqueId());
                                                          });
    }

    /**
     * Retrieves the "top" descriptor, which is the descriptor directly underneath the engine (root) descriptor
     * and represents the test class or, if that's not available, the engine (root) descriptor.
     * The descriptor returned by this method is used to report test timeouts and other execution errors.
     */
    private TestDescriptor topDescriptor()
    {
        TestDescriptor top = underneathRoot;
        return top != null ? top : rootDescriptor;
    }

    private void notifyAsSkipped(TestDescriptor td)
    {
        UniqueId id = td.getUniqueId();
        stopped.add(id);
        started.remove(id);
        l.executionSkipped(td, "parent stopped before");
    }

    private enum Type
    {
        EXEC,
        START,
        STOP
    }
}
