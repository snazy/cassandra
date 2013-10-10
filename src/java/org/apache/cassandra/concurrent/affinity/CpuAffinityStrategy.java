package org.apache.cassandra.concurrent.affinity;

public interface CpuAffinityStrategy
{
    void setCpuAffinity(Thread t);
}
