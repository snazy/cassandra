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
package org.apache.cassandra.tracing;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.MigrationListener;
import org.apache.cassandra.service.MigrationManager;

public class TracingFinishedTest extends CQLTester
{
    @Test
    public void testTracingFinished() throws Throwable
    {
        final List<InetAddress> clientAddresses = new ArrayList<>();
        final List<UUID> sessionIds = new ArrayList<>();

        MigrationManager.instance.register(new MigrationListener()
        {
            public void onTraceFinished(InetAddress clientAddress, UUID sessionId)
            {
                clientAddresses.add(clientAddress);
                sessionIds.add(sessionId);
            }
        });

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

        int version = 3;

        Session session = sessionNet(version);
        SimpleStatement statement = new SimpleStatement("SELECT * FROM " + KEYSPACE + '.' + currentTable());
        statement.enableTracing();
        assertRowsNet(version, session.execute(statement));

        Assert.assertEquals(1, clientAddresses.size());
        Assert.assertEquals(InetAddress.getLoopbackAddress(), clientAddresses.get(0));
        Assert.assertNotNull(sessionIds.size());
    }
}
