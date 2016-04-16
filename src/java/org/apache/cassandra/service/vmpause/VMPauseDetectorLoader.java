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

import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Histogram;

import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.MetricName;
import org.apache.cassandra.metrics.DefaultNameFactory;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public final class VMPauseDetectorLoader
{
    private static final Logger logger = LoggerFactory.getLogger(VMPauseDetectorLoader.class);

    public static final VMPauseDetectorMBean vmPauseDetector;

    private static final Histogram pauseHistogram;

    static
    {
        VMPauseDetectorMBean detector;

        MetricName metricName = DefaultNameFactory.createMetricName("VM", "Pause", null);
        pauseHistogram = Metrics.histogram(metricName, false);

        // Try to load HotspotVMPauseDetector class. If this fails, fallback to HiccupPauseDetector
        detector = tryLoad("org.apache.cassandra.service.vmpause.HotspotVMPauseDetector", pauseHistogram);
        if (detector == null)
            detector = new HiccupPauseDetector(pauseHistogram);

        logger.info("Using {} as VM pause detector", detector.getClass().getName());

        vmPauseDetector = detector;

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        try
        {
            mbs.registerMBean(new StandardMBean(detector, VMPauseDetectorMBean.class),
                              new ObjectName(DefaultNameFactory.GROUP_NAME + ":type=VM,name=TotalPause"));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private VMPauseDetectorLoader()
    {
    }

    private static VMPauseDetectorMBean tryLoad(String cls, Histogram pauseHistogram)
    {
        try
        {
            Class c = Class.forName(cls);
            Constructor ctor = c.getDeclaredConstructor(Histogram.class);
            return (VMPauseDetectorMBean) ctor.newInstance(pauseHistogram);
        }
        catch (Exception e)
        {
            logger.debug("Loading {} failed: {}", cls, e);
            return null;
        }
    }
}
