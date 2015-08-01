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

package org.apache.cassandra.cql3.functions;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

import com.sun.management.ThreadMXBean;

/**
 * Maintains the CPU-time and heap-allocation status for a particular UDF invocation, which depends
 * on the thread executing a Java-UDF or Script-UDF.
 */
final class UDFQuotaState
{
    private static final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    static void ensureInitialized()
    {
        // Looks weird?
        // This call "just" links this class to java.lang.management - otherwise UDFs (script UDFs) might fail due to
        //      java.security.AccessControlException: access denied: ("java.lang.RuntimePermission" "accessClassInPackage.java.lang.management")
        // because class loading would be deferred until setup() is executed - but setup() is called with
        // limited privileges.
        threadMXBean.getCurrentThreadCpuTime();
    }

    private final long threadId;

    // CPU time used by the thread before the UDF executes
    private final long timeOffset;
    // fail and warn "CPU time usage"
    private final long failNanos;
    private final long warnNanos;

    // bytes allocated by the thread before the UDF executes
    private final long bytesOffset;
    // fail and warn "bytes allocated on heap"
    private final long failBytes;
    private final long warnBytes;

    private int checkEvery;
    private boolean failed;

    UDFQuotaState(long failTimeoutMillis, long warnTimeoutMillis, long failMemoryMb, long warnMemoryMb)
    {
        this(Thread.currentThread().getId(), failTimeoutMillis, warnTimeoutMillis, failMemoryMb, warnMemoryMb);
    }

    UDFQuotaState(long threadId, long failTimeoutMillis, long warnTimeoutMillis, long failMemoryMb, long warnMemoryMb)
    {
        this.threadId = threadId;
        failNanos = TimeUnit.MILLISECONDS.toNanos(failTimeoutMillis);
        warnNanos = TimeUnit.MILLISECONDS.toNanos(warnTimeoutMillis);
        failBytes = failMemoryMb * 1024 * 1024;
        warnBytes = warnMemoryMb * 1024 * 1024;
        bytesOffset = threadMXBean.getThreadAllocatedBytes(threadId);
        timeOffset = threadMXBean.getThreadCpuTime(threadId);
        assert timeOffset != -1 : "ThreadMXBean does not support thread CPU time";
    }

    /**
     * This method is called regularly from Java UDFs - see {@link JavaUDFByteCodeVerifier} implementation.
     */
    boolean failCheckLazy()
    {
        // only checks every 16th invocation to prevent too many OS roundtrips
        checkEvery++;
        if ((checkEvery & 0xf) == 0xf
            && (runTimeNanos() > failNanos || allocatedBytes() > failBytes))
            failed = true;

        return failed;
    }

    /**
     * Updates {@code result} with the state of this object.
     */
    void afterExec(JavaUDFExecResult result)
    {
        long threadTime = runTimeNanos();
        result.failTimeExceeded = threadTime > failNanos;
        result.warnTimeExceeded = threadTime > warnNanos;
        long threadBytes = allocatedBytes();
        result.failBytesExceeded = threadBytes > failBytes;
        result.warnBytesExceeded = threadBytes > warnBytes;
    }

    /**
     * Get CPU time in nanoseconds since instantiation of this instance.
     */
    long runTimeNanos()
    {
        return threadMXBean.getThreadCpuTime(threadId) - timeOffset;
    }

    /**
     * Get number of bytes allocated on the heap since instantiation of this instance.
     */
    long allocatedBytes()
    {
        return threadMXBean.getThreadAllocatedBytes(threadId) - bytesOffset;
    }
}
