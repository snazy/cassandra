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

package org.apache.cassandra.transport;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.metrics.EstimatedHistogramReservoir;

public final class TransportMetrics
{
    public static final TransportMetrics instance = new TransportMetrics();

    /** Number of messages by type */
    public final Map<Message.Type, Meter> messages;

    /** Number of errors by code */
    public final Map<ExceptionCode, Meter> errors;

    /** Size of request messages */
    public final Histogram requestMessageSize;
    /** Size of response messages */
    public final Histogram responseMessageSize;

    public final AtomicInteger inFlightRequests = new AtomicInteger();
    public final AtomicInteger requestCount = new AtomicInteger();

    private TransportMetrics()
    {
        messages = new EnumMap<>(Message.Type.class);
        for (Message.Type type : Message.Type.values())
            messages.put(type,
                         ClientMetrics.instance.addMeter("Message." + type.name(), new Meter()));

        errors = new EnumMap<>(ExceptionCode.class);
        for (ExceptionCode code : ExceptionCode.values())
            errors.put(code,
                       ClientMetrics.instance.addMeter("Error." + code.name(), new Meter()));

        ClientMetrics.instance.addCounter("RequestsInFlight", inFlightRequests::get);
        ClientMetrics.instance.addCounter("RequestCount", () -> requestCount.getAndSet(0));

        requestMessageSize = ClientMetrics.instance.addHistogram("RequestMessageSize", newHistogram());
        responseMessageSize = ClientMetrics.instance.addHistogram("ResponseMessageSize", newHistogram());
    }

    private static Histogram newHistogram()
    {
        return new Histogram(new EstimatedHistogramReservoir(false));
    }

    public static void init()
    {
        // dummy, empty method to initialize this class
    }
}
