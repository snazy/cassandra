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
package org.apache.cassandra.udftest;

import com.codahale.metrics.Timer;

/**
 * Provides the result plus some metrics of the execution of a user defined function
 * in {@link org.apache.cassandra.udftest.UDF}.
 */
public final class UDFResult
{
    private final Object result;
    private final Timer timer;

    public UDFResult(Object result, Timer timer)
    {
        this.result = result;
        this.timer = timer;
    }

    public Object getResult()
    {
        return result;
    }

    public Timer getTimer()
    {
        return timer;
    }

    public String toString()
    {
        return "UDFResult{" +
               "result=" + result +
               ", totalExecutions=" + timer.getCount() +
               ", meanExecutionTimeNanos=" + timer.getSnapshot().getMean() +
               '}';
    }
}
