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

package org.apache.cassandra.cql3.functions;

/**
 * Handle timeout status of (Java) UDFs - so acting as an interface between Java UDFs, that call
 * {@link #udfExecCall()} regularly (due to byte code injected by {@link JavaUDFByteCodeVerifier})
 * and {@link UDFQuotaState}.
 */
final class JavaUDFQuotaHandler
{
    private static final ThreadLocal<UDFQuotaState> state = new ThreadLocal<>();

    private JavaUDFQuotaHandler()
    {
    }

    /**
     * Pass in actual values and do _not_ use DatabaseDescriptor class here.
     * Reason not to use DatabaseDescriptor is that DatabaseDescriptor is not an allowed to be used from UDFs
     * (class loader restrictions).
     */
    static void beforeStart(long failTimeoutMillis, long warnTimeoutMillis, long failMemoryMb, long warnMemoryMb)
    {
        state.set(new UDFQuotaState(failTimeoutMillis, warnTimeoutMillis, failMemoryMb, warnMemoryMb));
    }

    /**
     * Called during UDF execution (by bytecode manipulated Java UDFs) to check timeout status indirectly
     * via {@link JavaUDF}.
     */
    static boolean udfExecCall()
    {
        return state.get().failCheckLazy();
    }

    /**
     * Called after UDF returns to check quotas status.
     */
    static void afterExec(JavaUDFExecResult result)
    {
        state.get().afterExec(result);
    }
}
