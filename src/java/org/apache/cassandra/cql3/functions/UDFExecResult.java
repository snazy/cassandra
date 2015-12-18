/*
 * Copyright DataStax, Inc.
 *
 * Modified by DataStax, Inc.
 */
package org.apache.cassandra.cql3.functions;

/**
 * Execution result of a Java UDF including flags whether CPU usage or heap usage have exceeded
 * the configured limits (quota).
 */
final class UDFExecResult<T>
{
    T result;
    boolean warnCpuTimeExceeded;
    boolean failCpuTimeExceeded;
    boolean warnBytesExceeded;
    boolean failBytesExceeded;
}
