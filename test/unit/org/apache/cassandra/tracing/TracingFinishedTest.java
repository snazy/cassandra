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

import java.util.Collections;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.RegisterMessage;

public class TracingFinishedTest extends CQLTester
{
    @Test
    public void testTracingFinished() throws Throwable
    {
        sessionNet(3);

        SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort);
        client.connect(false);
        try
        {
            final SynchronousQueue<Event> eventQueue = new SynchronousQueue<>();

            client.setEventHandler(new SimpleClient.EventHandler()
            {
                public void onEvent(Event event)
                {
                    eventQueue.add(event);
                }
            });

            Message.Response resp = client.execute(new RegisterMessage(Collections.singletonList(Event.Type.TRACE_FINISHED)));
            Assert.assertSame(Message.Type.READY, resp.type);

            createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

            QueryMessage query = new QueryMessage("SELECT * FROM " + KEYSPACE + '.' + currentTable(), QueryOptions.DEFAULT);
            query.setTracingRequested();;
            resp = client.execute(query);

            Event event = eventQueue.poll(1, TimeUnit.SECONDS);
            Assert.assertNotNull(event);

            Assert.assertSame(Event.Type.TRACE_FINISHED, event.type);
            Assert.assertEquals(resp.getTracingId(), ((Event.TraceFinished) event).traceSessionId);
        }
        finally
        {
            client.close();
        }
    }
}
