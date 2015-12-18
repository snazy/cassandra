/*
 * Copyright DataStax, Inc.
 *
 * Modified by DataStax, Inc.
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

    /**
     * Each {@code checkInterval}'s callback from an instrumented Java UDF will trigger a check whether runtime/heap quotas are exceeded.
     */
    private static final int checkInterval = Integer.parseInt(System.getProperty("cassandra.java_udf_check_interval", "1000"));

    private final long threadId;

    // CPU time used by the thread before the UDF executes
    private final long cpuTimeOffset;
    // fail and warn "CPU time usage"
    private final long failCpuTimeNanos;
    private final long warnCpuTimeNanos;

    // bytes allocated by the thread before the UDF executes
    private final long bytesOffset;
    // fail and warn "bytes allocated on heap"
    private final long failBytes;
    private final long warnBytes;

    private int checkEvery;
    private boolean failed;

    UDFQuotaState(long failCpuTimeMillis, long warnCpuTimeMillis,
                  long failMemoryMb, long warnMemoryMb)
    {
        threadId = Thread.currentThread().getId();
        failCpuTimeNanos = TimeUnit.MILLISECONDS.toNanos(failCpuTimeMillis);
        warnCpuTimeNanos = TimeUnit.MILLISECONDS.toNanos(warnCpuTimeMillis);
        failBytes = failMemoryMb * 1024 * 1024;
        warnBytes = warnMemoryMb * 1024 * 1024;
        bytesOffset = threadMXBean.getThreadAllocatedBytes(threadId);
        cpuTimeOffset = threadMXBean.getThreadCpuTime(threadId);
        assert cpuTimeOffset != -1 : "ThreadMXBean does not support thread CPU time";
    }

    /**
     * This method is called regularly from Java UDFs - see {@link JavaUDFByteCodeVerifier} implementation.
     */
    boolean failCheckLazy()
    {
        // only checks every Nth invocation to prevent too many OS roundtrips (see field 'checkInterval' above)
        if (checkEvery >= checkInterval
            && (cpuTimeNanos() > failCpuTimeNanos || allocatedBytes() > failBytes))
        {
            failed = true;
            checkEvery = 0;
        }
        else
        {
            checkEvery++;
        }

        return failed;
    }

    /**
     * Updates {@code result} with the state of this object.
     */
    UDFExecResult afterExec(UDFExecResult result)
    {
        long threadCpuTime = cpuTimeNanos();
        result.failCpuTimeExceeded = threadCpuTime > failCpuTimeNanos;
        result.warnCpuTimeExceeded = threadCpuTime > warnCpuTimeNanos;
        long threadBytes = allocatedBytes();
        result.failBytesExceeded = threadBytes > failBytes;
        result.warnBytesExceeded = threadBytes > warnBytes;
        return result;
    }

    /**
     * Get CPU time in nanoseconds since instantiation of this instance.
     */
    long cpuTimeNanos()
    {
        return threadMXBean.getThreadCpuTime(threadId) - cpuTimeOffset;
    }

    /**
     * Get number of bytes allocated on the heap since instantiation of this instance.
     */
    long allocatedBytes()
    {
        return threadMXBean.getThreadAllocatedBytes(threadId) - bytesOffset;
    }
}
