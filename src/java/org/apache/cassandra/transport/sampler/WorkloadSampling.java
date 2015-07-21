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

package org.apache.cassandra.transport.sampler;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.transport.Connection;
import org.apache.cassandra.transport.Frame;

/**
 * Manages recording of frames over CQL3 native transport.
 */
public final class WorkloadSampling implements WorkloadSamplingMBean
{
    public static final String[] orderedSchemaTables = { SchemaKeyspace.KEYSPACES,
                                                         SchemaKeyspace.TYPES,
                                                         SchemaKeyspace.FUNCTIONS,
                                                         SchemaKeyspace.AGGREGATES,
                                                         SchemaKeyspace.TABLES,
                                                         SchemaKeyspace.COLUMNS,
                                                         SchemaKeyspace.DROPPED_COLUMNS,
                                                         SchemaKeyspace.MATERIALIZED_VIEWS };
    public static final int OPCODE_KEYSPACE_DEFINITION = 0x10;
    public static final int OPCODE_PREPARED_STATEMENT = 0x11;
    public static final int OPCODE_START_DATA = 0x20;

    public static final String MBEAN_NAME = "org.apache.cassandra.net:type=WorkloadRecording";

    private static final Logger logger = LoggerFactory.getLogger(WorkloadSampling.class);

    private static final AtomicReference<WorkloadSampler> currentRecorder = new AtomicReference<>();

    public WorkloadSampling()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void onMessage(Connection connection, Frame frame)
    {
        WorkloadSampler recorder = currentRecorder.get();
        if (recorder == null)
            return;

        recorder.record(connection, frame);
        if (recorder.isClosed())
            currentRecorder.set(null);
    }

    public void startSamplingMaxSeconds(int maxSeconds)
    {
        startSampling(maxSeconds, 0, 1d, true, false);
    }

    public void startSamplingMaxMB(int maxMBytes)
    {
        startSampling(0, maxMBytes, 1d, true, false);
    }

    public void startSamplingNonBlocking()
    {
        startSampling(0, 0, 1d, true, true);
    }

    public void startSampling()
    {
        startSampling(0, 0, 1d, true, false);
    }

    public void startSampling(int maxSeconds, int maxMBytes, double probability, boolean includeResponses, boolean nonBlocking)
    {
        WorkloadSampler recorder = currentRecorder.get();
        if (recorder != null)
            return;

        try
        {
            recorder = new WorkloadSampler(nextFile(), maxSeconds, maxMBytes, probability, includeResponses, nonBlocking);

            // TODO find a solution to write the recorder header but don't miss any frames (time between writing the header and CAS)
            // it could miss a prepare statement

            if (!currentRecorder.compareAndSet(null, recorder))
            {
                // another recorder is already active - just tear this one down
                recorder.abort();
            }
        }
        catch (Exception e)
        {
            logger.error("Failed to start workload recording", e);
        }
    }

    private static File nextFile()
    {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SS");
        File workloadDir = new File(DatabaseDescriptor.getWorkloadSamplingDirectory());
        return new File(workloadDir, "workload-recording-" + sdf.format(new Date()) + ".bin");
    }

    public boolean isSampling()
    {
        WorkloadSampler recorder = currentRecorder.get();
        return recorder != null && !recorder.isClosed();
    }

    public void stopSampling()
    {
        WorkloadSampler recorder = currentRecorder.get();
        if (recorder != null)
        {
            recorder.close("user stopped recording");
            currentRecorder.compareAndSet(recorder, null);
        }
    }
}
