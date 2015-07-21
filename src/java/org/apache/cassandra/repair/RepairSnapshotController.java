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

package org.apache.cassandra.repair;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Controls the interval between snapshots against endpoints for sequential repairs.
 * <p>
 *     If {@link org.apache.cassandra.repair.messages.RepairOption} defines a
 *     {@link org.apache.cassandra.repair.messages.RepairOption#SNAPSHOT_TIME_WINDOW SNAPSHOT_TIME_WINDOW} greater than 0,
 *     this class controls when a {@link SnapshotTask} must be executed against the involved endpoints from
 *     {@link RepairJob} ({@code RepairJob} is per column-famility - and therefore {@code SnapshotTask} is per CF, too.)
 * </p>
 */
public class RepairSnapshotController
{
    public final int snapshotTimeWindow;

    private final Map<String, Map<InetAddress, Long>> columnFamilyLastSnapshotPerNode;

    public RepairSnapshotController(int snapshotTimeWindow, String... cfnames)
    {
        this.snapshotTimeWindow = snapshotTimeWindow;
        Map<String, Map<InetAddress, Long>> map;
        if (snapshotTimeWindow > 0)
        {
            map = new HashMap<>(cfnames.length);
            for (String cfname : cfnames)
            {
                map.put(cfname, new ConcurrentHashMap<InetAddress, Long>());
            }
        }
        else
        {
            map = null;
        }
        this.columnFamilyLastSnapshotPerNode = map;
    }

    public boolean requiresSnapshot(String columnFamily, List<InetAddress> endpoints)
    {
        if (snapshotTimeWindow <= 0)
            return true;

        Map<InetAddress, Long> lastSnapshotPerNode = columnFamilyLastSnapshotPerNode.get(columnFamily);
        long minLastSnapshot = Long.MAX_VALUE;
        for (InetAddress endpoint : endpoints)
        {
            Long lastSnapshot = lastSnapshotPerNode.get(endpoint);
            if (lastSnapshot == null)
            {
                minLastSnapshot = 0L;
                break;
            }
            else if (lastSnapshot < minLastSnapshot)
            {
                minLastSnapshot = lastSnapshot;
            }
        }

        if (minLastSnapshot + snapshotTimeWindow * 1000L < System.currentTimeMillis())
        {
            Long lastSnapshot = System.currentTimeMillis();
            for (InetAddress endpoint : endpoints)
                lastSnapshotPerNode.put(endpoint, lastSnapshot);
            return true;
        }
        return false;
    }
}
