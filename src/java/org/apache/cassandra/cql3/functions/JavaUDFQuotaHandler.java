/*
 * Copyright DataStax, Inc.
 *
 * Modified by DataStax, Inc.
 */
/*
 * Copyright DataStax, Inc.
 *
 * Modified by DataStax, Inc.
 */
package org.apache.cassandra.cql3.functions;

/**
 * Handle timeout status of (Java) UDFs - so acting as an interface between Java UDFs, that call
 * {@link #udfExecCall()} regularly (due to byte code injected by {@link JavaUDFByteCodeVerifier})
 * and {@link UDFQuotaState}.
 */
final class JavaUDFQuotaHandler
{
    private JavaUDFQuotaHandler()
    {
    }

    /**
     * Pass in actual values and do _not_ use DatabaseDescriptor class here.
     * Reason not to use DatabaseDescriptor is that DatabaseDescriptor is not an allowed to be used from UDFs
     * (class loader restrictions).
     */
    static void beforeStart(UDFQuotaState quotaState)
    {
        ThreadAwareSecurityManager.enterSecureSection(quotaState, null, UDFunction::initializeThread);
    }

    /**
     * Called during UDF execution (by bytecode manipulated Java UDFs) to check timeout status indirectly
     * via {@link JavaUDF}.
     */
    static boolean udfExecCall()
    {
        return ThreadAwareSecurityManager.secureSection()
                                         .failCheckLazy();
    }

    /**
     * Called after UDF returns to check quotas status.
     */
    static void afterExec(UDFExecResult result)
    {
        ThreadAwareSecurityManager.leaveSecureSection()
                                  .afterExec(result);
    }
}
