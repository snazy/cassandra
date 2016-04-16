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

package org.apache.cassandra.service.vmpause;

import com.codahale.metrics.Histogram;

public final class HiccupPauseDetector implements VMPauseDetectorMBean
{
    private final Histogram pauseTimeHistogram;

    private long totalPause;
    private long totalCount;

    HiccupPauseDetector(Histogram pauseTimeHistogram)
    {
        this.pauseTimeHistogram = pauseTimeHistogram;

        Thread t = new Thread("HiccupVMPauseDetector")
        {
            public void run()
            {
                while (!Thread.currentThread().isInterrupted())
                {
                    try
                    {
                        long t = System.nanoTime();
                        Thread.sleep(1L);
                        long diff = System.nanoTime() - t;
                        diff /= 1_000_000;
                        if (diff > 1L)
                        {
                            totalPause += diff;
                            totalCount ++;
                            pauseTimeHistogram.update(diff);
                        }
                    }
                    catch (InterruptedException e)
                    {
                        break;
                    }
                }
            }
        };
        t.setDaemon(true);
        t.start();
    }

    public long getLotalPauseTimeMillis()
    {
        return totalPause;
    }

    public long getLotalPauseCount()
    {
        return totalCount;
    }
}
