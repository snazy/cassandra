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
import sun.management.HotspotRuntimeMBean;
import sun.management.ManagementFactoryHelper;

public final class HotspotVMPauseDetector implements VMPauseDetectorMBean
{
    private final HotspotRuntimeMBean hotspotRuntime;
    private final Histogram pauseTimeHistogram;
    private long lastTotal;

    HotspotVMPauseDetector(Histogram pauseTimeHistogram)
    {
        this.pauseTimeHistogram = pauseTimeHistogram;

        hotspotRuntime = ManagementFactoryHelper.getHotspotRuntimeMBean();

        Thread t = new Thread("HotspotVMPauseDetector")
        {
            public void run()
            {
                while (!Thread.currentThread().isInterrupted())
                {
                    tick();
                    try
                    {
                        Thread.sleep(1L);
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

    private void tick()
    {
        long total = hotspotRuntime.getTotalSafepointTime();

        long timeDiff = total - lastTotal;

        if (timeDiff > 0)
        {
            lastTotal = total;
            pauseTimeHistogram.update(timeDiff);
        }
    }

    public long getLotalPauseTimeMillis()
    {
        return lastTotal;
    }

    public long getLotalPauseCount()
    {
        return hotspotRuntime.getSafepointCount();
    }
}
