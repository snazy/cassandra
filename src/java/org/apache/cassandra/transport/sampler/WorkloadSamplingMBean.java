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

public interface WorkloadSamplingMBean
{
    /**
     * Start workload sampling with optional constraints.
     * <p>
     * Workload sampling can be constrainted in the duration in seconds, the file limit in MB,
     * probability (1 meaning all), whether response frames should be included and whether blocking
     * is allowed.
     * </p>
     * <p>
     * In <i>blocking</i> mode, which is the default, all frames will be written to the sample file. This
     * implies waiting for file I/O, if necessary. This may lead to reduced throughput and maybe timeouts.
     * In <i>nonBlocking</i> mode, no two frames will wait for I/O to complete - one sample frame may still
     * have to wait for disk I/O if the implementation's I/O buffers need to be flushed to disk <i>and</i>
     * disk I/O cannot keep up with the amount of data to be written.
     * </p>
     * <p>
     * Implementation detail: disk I/O operations in workload sampling are asynchronous. But asynchronous does
     * not mean to be free. It still depends on the throughput of the file system that where the
     * workload sampling file is written. If possible do not write sampling data to busy disks and prohibit
     * writing to a any NAS or SAN.
     * </p>
     *
     * @param maxSeconds       time in seconds to record. Pass 0 for no time limit. If this time limit is reached,
     *                         recording is automatically stopped and the recorder closed.
     * @param maxMBytes        amount of data to record. Pass 0 for no size limit. If this time limit is reached,
     *                         recording is automatically stopped and the recorder closed.
     * @param probability      probability of frames to be recorded. Pass 0 or 1 to record all frames.
     * @param includeResponses flag whether to record responses
     * @param nonBlocking      flag whether to only record frames, that can be recorded non-blocking.
     */
    public void startSampling(int maxSeconds, int maxMBytes, double probability, boolean includeResponses, boolean nonBlocking);

    /**
     * Convenience method with {@code maxSeconds=0}, {@code probability=1}, {@code includeResponses=true}, {@code nonBlocking=false}
     */
    public void startSamplingMaxSeconds(int maxSeconds);

    /**
     * Convenience method with {@code maxSeconds=0}, {@code probability=1}, {@code includeResponses=true}, {@code nonBlocking=false}
     */
    public void startSamplingMaxMB(int maxMBytes);

    /**
     * Convenience method with {@code maxSeconds=0}, {@code maxMBytes=0}, {@code probability=1}, {@code includeResponses=true}, {@code nonBlocking=true}
     */
    public void startSamplingNonBlocking();

    /**
     * Convenience method with {@code maxSeconds=0}, {@code maxMBytes=0}, {@code probability=1}, {@code includeResponses=true}, {@code nonBlocking=false}
     */
    public void startSampling();

    public boolean isSampling();

    public void stopSampling();
}
