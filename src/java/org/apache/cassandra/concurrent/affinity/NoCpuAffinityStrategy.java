package org.apache.cassandra.concurrent.affinity;

/**
 * Standard implementation that will not attempt any cpu affinity.
 */
public class NoCpuAffinityStrategy implements CpuAffinityStrategy
{
    public static final NoCpuAffinityStrategy INSTANCE = new NoCpuAffinityStrategy();

    public void setCpuAffinity(Thread t)
    {
        //nop
    }
}
