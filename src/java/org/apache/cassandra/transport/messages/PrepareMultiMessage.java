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

package org.apache.cassandra.transport.messages;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.UUIDGen;

public class PrepareMultiMessage extends Message.Request
{
    public static final Codec<PrepareMultiMessage> codec = new Codec<PrepareMultiMessage>()
    {
        public PrepareMultiMessage decode(ByteBuf body, int version)
        {
            List<String> query = CBUtil.readStringList(body);
            return new PrepareMultiMessage(query);
        }

        public void encode(PrepareMultiMessage msg, ByteBuf dest, int version)
        {
            CBUtil.writeStringList(msg.query, dest);
        }

        public int encodedSize(PrepareMultiMessage msg, int version)
        {
            return CBUtil.sizeOfStringList(msg.query);
        }
    };

    private final List<String> query;

    public PrepareMultiMessage(List<String> query)
    {
        super(Type.PREPARE_MULTI);
        this.query = query;
    }

    public Response execute(QueryState state)
    {
        try
        {
            UUID tracingId = null;
            if (isTracingRequested())
            {
                tracingId = UUIDGen.getTimeUUID();
                state.prepareTracingSession(tracingId);
            }

            if (state.traceNextQuery())
            {
                state.createTracingSession(connection);
                Map<String, String> parameters = new HashMap<>();
                for (int i = 0; i < query.size(); i++)
                    parameters.put(String.format("query-%04d", i), query.get(i));
                Tracing.instance.begin("Preparing CQL3 query", state.getClientAddress(), parameters);
            }

            Response response = ClientState.getCQLQueryHandler().prepareMulti(query, state, getCustomPayload());

            if (tracingId != null)
                response.setTracingId(tracingId);

            return response;
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            return ErrorMessage.fromException(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    @Override
    public String toString()
    {
        return "PREPARE_MULTI " + query;
    }
}
