package org.apache.cassandra.concurrent.affinity;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import com.higherfrequencytrading.affinity.AffinityLock;
import com.higherfrequencytrading.affinity.AffinitySupport;
import com.higherfrequencytrading.affinity.CpuLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Intended to be used with an AbstractExecutorService (ThreadPoolExecutor or ForkJoinPool).
 * This implementation assigns each successively created thread to the next sequential CPU,
 * thus attempting to spread the threads around equally amongst the CPUs.
 * As we have several AbstractExecutorService instances inside of cassandra, this implementation
 * picks a random CPU to start with as this will avoid overloading the lower numbered CPUs.
 */
public class EqualSpreadCpuAffinityStrategy implements CpuAffinityStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(EqualSpreadCpuAffinityStrategy.class);
    private static final CpuLayout CPU_LAYOUT = AffinityLock.cpuLayout();

    private static final int[] bannedCpus = initBannedCpus();

    private final AtomicInteger cpuAffinityOffset;

    /**
     * Allow users to ban CPUs from usage (only when using this strategy). The thought is that the kernel (or admins)
     * will often assign a certain cpu to handle hardware interrupts, and we don't want to pin a thread to the same
     * cpu that's handling all the network interrupts and so on.
     */
    private static int[] initBannedCpus()
    {
        //TODO: is there something better than a jvm property?
        final String bannedCpusProp = System.getProperty("threadAff.bannedCpus");
        if(bannedCpusProp == null)
            return new int[0];

        final String[] cpus = bannedCpusProp.split(",");
        int[] bannedCpus = new int[cpus.length];

        if (bannedCpus.length == CPU_LAYOUT.cpus())
        {
            logger.error("You have chosen to ban cpus {}, which is the same count as number of CPUs in your machine: {}. "
                    + "Ignoring cpu banning.", bannedCpus.length, CPU_LAYOUT.cpus());
            return new int[0];
        }

        int idx = 0;
        for (String cpu : cpus)
        {
            bannedCpus[idx] = Integer.parseInt(cpu.trim());
            idx++;
        }
        logger.info("excluding cpus {} from use in affinity strategy", Arrays.toString(bannedCpus));
        return bannedCpus;
    }

    public EqualSpreadCpuAffinityStrategy()
    {
        final int offset = (int)(Math.random() * (double)CPU_LAYOUT.cpus());
        cpuAffinityOffset = new AtomicInteger(offset);
    }

    public void setCpuAffinity(Thread t)
    {
        long cpuId;
        outer: while (true)
        {
            final int offset = cpuAffinityOffset.getAndIncrement();
            cpuId = offset % CPU_LAYOUT.cpus();
            for (int i = 0; i < bannedCpus.length; i++)
            {
                if(cpuId == bannedCpus[i])
                    continue outer;
            }
            break;
        }

        final long mask = 1 << cpuId;
        AffinitySupport.setAffinity(mask);
    }
}
