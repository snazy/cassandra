package org.apache.cassandra.concurrent.affinity;

import java.util.concurrent.atomic.AtomicInteger;

import com.higherfrequencytrading.affinity.AffinityLock;
import com.higherfrequencytrading.affinity.AffinitySupport;
import com.higherfrequencytrading.affinity.CpuLayout;

/**
 * Intended to be used with an AbstractExecutorService (ThreadPoolExecutor or ForkJoinPool).
 * This implementation assigns each successively created thread to the next sequential CPU,
 * thus attempting to spread the threads around equally amongst the CPUs.
 * As we have several AbstractExecutorService instances inside of cassandra, this implementation
 * picks a random CPU to start with as this will avoid overloading the lower numbered CPUs.
 */
public class EqualSpreadCpuAffinityStrategy implements CpuAffinityStrategy
{
    private static final CpuLayout CPU_LAYOUT = AffinityLock.cpuLayout();

    private final AtomicInteger cpuAffinityOffset;

    public EqualSpreadCpuAffinityStrategy()
    {
        final int offset = (int)(Math.random() * (double)CPU_LAYOUT.cpus());
        cpuAffinityOffset = new AtomicInteger(offset);
    }

    public void setCpuAffinity(Thread t)
    {
        final int offset = cpuAffinityOffset.getAndIncrement();
        final long cpuId = offset % CPU_LAYOUT.cpus();
        final long mask = 1 << cpuId;
        AffinitySupport.setAffinity(mask);
    }
}
