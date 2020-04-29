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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * State related to a client connection.
 */
public class ClientState
{
    private static final Logger logger = LoggerFactory.getLogger(ClientState.class);

    // Current user for the session
    private volatile AuthenticatedUser user;
    private volatile String keyspace;

    private static final QueryHandler cqlQueryHandler;
    static
    {
        QueryHandler handler = QueryProcessor.instance;
        String customHandlerClass = System.getProperty("cassandra.custom_query_handler_class");
        if (customHandlerClass != null)
        {
            try
            {
                handler = FBUtilities.construct(customHandlerClass, "QueryHandler");
                logger.info("Using {} as query handler for native protocol queries (as requested with -Dcassandra.custom_query_handler_class)", customHandlerClass);
            }
            catch (Exception e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                logger.info("Cannot use class {} as query handler ({}), ignoring by defaulting on normal query handling", customHandlerClass, e.getMessage());
            }
        }
        cqlQueryHandler = handler;
    }

    // isInternal is used to mark ClientState as used by some internal component
    // that should have an ability to modify system keyspace.
    public final boolean isInternal;

    // The remote address of the client - null for internal clients.
    private final InetSocketAddress remoteAddress;

    // Driver String for the client
    private volatile String driverName;
    private volatile String driverVersion;

    // The biggest timestamp that was returned by getTimestamp/assigned to a query. This is global to ensure that the
    // timestamp assigned are strictly monotonic on a node, which is likely what user expect intuitively (more likely,
    // most new user will intuitively expect timestamp to be strictly monotonic cluster-wise, but while that last part
    // is unrealistic expectation, doing it node-wise is easy).
    private static final AtomicLong lastTimestampMicros = new AtomicLong(0);

    private ClientState(AuthenticatedUser user)
    {
        this.isInternal = false;
        this.remoteAddress = null;
        this.user = user;
    }

    protected ClientState(InetSocketAddress remoteAddress)
    {
        this.isInternal = false;
        this.remoteAddress = remoteAddress;
        if (!DatabaseDescriptor.getAuthenticator().requireAuthentication())
            this.user = AuthenticatedUser.ANONYMOUS_USER;
    }

    protected ClientState(ClientState source)
    {
        this.isInternal = source.isInternal;
        this.remoteAddress = source.remoteAddress;
        this.user = source.user;
        this.keyspace = source.keyspace;
        this.driverName = source.driverName;
        this.driverVersion = source.driverVersion;
    }

    /**
     * @return a ClientState object for internal C* calls (not limited by any kind of auth).
     */
    public static ClientState forInternalCalls()
    {
        return forInternalCalls(null, null);
    }

    public static ClientState forInternalCalls(String keyspace)
    {
        return forInternalCalls(keyspace, null);
    }

    public static ClientState forInternalCalls(String keyspace, AuthenticatedUser user)
    {
        ClientState state = new ClientState(user);
        if (keyspace != null)
            state.setKeyspace(keyspace);
        return state;
    }

    /**
     * @return a ClientState object for internal calls with the given user logged in (not limited by any kind of auth).
     */
    public static ClientState forExternalCalls(AuthenticatedUser user)
    {
        return new ClientState(user);
    }

    /**
     * @return a ClientState object for external clients (native protocol users).
     */
    public static ClientState forExternalCalls(SocketAddress remoteAddress)
    {
        return new ClientState((InetSocketAddress)remoteAddress);
    }

    /**
     * Clone this ClientState object, but use the provided keyspace instead of the
     * keyspace in this ClientState object.
     *
     * @return a new ClientState object if the keyspace argument is non-null. Otherwise do not clone
     *   and return this ClientState object.
     */
    public ClientState cloneWithKeyspaceIfSet(String keyspace)
    {
        if (keyspace == null || keyspace.equals(this.keyspace))
            return this;
        ClientState clientState = new ClientState(this);
        clientState.setKeyspace(keyspace);
        return clientState;
    }

    /**
     * This clock guarantees that updates for the same ClientState will be ordered
     * in the sequence seen, even if multiple updates happen in the same millisecond.
     */
    public static long getTimestamp()
    {
        while (true)
        {
            long current = System.currentTimeMillis() * 1000;
            long last = lastTimestampMicros.get();
            long tstamp = last >= current ? last + 1 : current;
            if (lastTimestampMicros.compareAndSet(last, tstamp))
                return tstamp;
        }
    }

    /**
     * Returns a timestamp suitable for paxos given the timestamp of the last known commit (or in progress update).
     * <p>
     * Paxos ensures that the timestamp it uses for commits respects the serial order of those commits. It does so
     * by having each replica reject any proposal whose timestamp is not strictly greater than the last proposal it
     * accepted. So in practice, which timestamp we use for a given proposal doesn't affect correctness but it does
     * affect the chance of making progress (if we pick a timestamp lower than what has been proposed before, our
     * new proposal will just get rejected).
     * <p>
     * As during the prepared phase replica send us the last propose they accepted, a first option would be to take
     * the maximum of those last accepted proposal timestamp plus 1 (and use a default value, say 0, if it's the
     * first known proposal for the partition). This would most work (giving commits the timestamp 0, 1, 2, ...
     * in the order they are commited) up to 2 important caveats:
     *   1) it would give a very poor experience when Paxos and non-Paxos updates are mixed in the same partition,
     *      since paxos operations wouldn't be using microseconds timestamps. And while you shouldn't theoretically
     *      mix the 2 kind of operations, this would still be pretty unintuitive. And what if you started writing
     *      normal updates and realize later you should switch to Paxos to enforce a property you want?
     *   2) this wouldn't actually be safe due to the expiration set on the Paxos state table.
     * <p>
     * So instead, we initially chose to use the current time in microseconds as for normal update. Which works in
     * general but mean that clock skew creates unavailability periods for Paxos updates (either a node has his clock
     * in the past and he may no be able to get commit accepted until its clock catch up, or a node has his clock in
     * the future and then once one of its commit his accepted, other nodes ones won't be until they catch up). This
     * is ok for small clock skew (few ms) but can be pretty bad for large one.
     * <p>
     * Hence our current solution: we mix both approaches. That is, we compare the timestamp of the last known
     * accepted proposal and the local time. If the local time is greater, we use it, thus keeping paxos timestamps
     * locked to the current time in general (making mixing Paxos and non-Paxos more friendly, and behaving correctly
     * when the paxos state expire (as long as your maximum clock skew is lower than the Paxos state expiration
     * time)). Otherwise (the local time is lower than the last proposal, meaning that this last proposal was done
     * with a clock in the future compared to the local one), we use the last proposal timestamp plus 1, ensuring
     * progress.
     *
     * @param minTimestampToUse the max timestamp of the last proposal accepted by replica having responded
     * to the prepare phase of the paxos round this is for. In practice, that's the minimum timestamp this method
     * may return.
     * @return a timestamp suitable for a Paxos proposal (using the reasoning described above). Note that
     * contrarily to the {@link #getTimestamp()} method, the return value is not guaranteed to be unique (nor
     * monotonic) across calls since it can return it's argument (so if the same argument is passed multiple times,
     * it may be returned multiple times). Note that we still ensure Paxos "ballot" are unique (for different
     * proposal) by (securely) randomizing the non-timestamp part of the UUID.
     */
    public long getTimestampForPaxos(long minTimestampToUse)
    {
        while (true)
        {
            long current = Math.max(System.currentTimeMillis() * 1000, minTimestampToUse);
            long last = lastTimestampMicros.get();
            long tstamp = last >= current ? last + 1 : current;
            // Note that if we ended up picking minTimestampMicrosToUse (it was "in the future"), we don't
            // want to change the local clock, otherwise a single node in the future could corrupt the clock
            // of all nodes and for all inserts (since non-paxos inserts also use lastTimestampMicros).
            // See CASSANDRA-11991
            if (tstamp == minTimestampToUse || lastTimestampMicros.compareAndSet(last, tstamp))
                return tstamp;
        }
    }

    public Optional<String> getDriverName()
    {
        return Optional.ofNullable(driverName);
    }

    public Optional<String> getDriverVersion()
    {
        return Optional.ofNullable(driverVersion);
    }

    public void setDriverName(String driverName)
    {
        this.driverName = driverName;
    }

    public void setDriverVersion(String driverVersion)
    {
        this.driverVersion = driverVersion;
    }

    public static QueryHandler getCQLQueryHandler()
    {
        return cqlQueryHandler;
    }

    public InetSocketAddress getRemoteAddress()
    {
        return remoteAddress;
    }

    InetAddress getClientAddress()
    {
        return isInternal ? null : remoteAddress.getAddress();
    }

    public String getRawKeyspace()
    {
        return keyspace;
    }

    public String getKeyspace() throws InvalidRequestException
    {
        if (keyspace == null)
            throw new InvalidRequestException("No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename");
        return keyspace;
    }

    public void setKeyspace(String ks)
    {
        // Skip keyspace validation for non-authenticated users. Apparently, some client libraries
        // call set_keyspace() before calling login(), and we have to handle that.
        if (user != null && Schema.instance.getKeyspaceMetadata(ks) == null)
            throw new InvalidRequestException("Keyspace '" + ks + "' does not exist");
        keyspace = ks;
    }

    /**
     * Attempts to login the given user.
     */
    public void login(AuthenticatedUser user)
    {
        if (user.isAnonymous() || DatabaseDescriptor.getAuthManager().canLogin(user.getPrimaryRole()))
        {
            if (CassandraRoleManager.DEFAULT_SUPERUSER_NAME.equals(user.getName()))
            {
                logger.warn("User '{}' logged in from {}. It is strongly recommended to create and use " +
                            "another user and grant it superuser capabilities and remove the default one.",
                            CassandraRoleManager.DEFAULT_SUPERUSER_NAME,
                            remoteAddress);
            }
            this.user = user;
        }
        else
        {
            throw new AuthenticationException(String.format("%s is not permitted to log in", user.getName()));
        }
    }

    public AuthenticatedUser getUser()
    {
        return user;
    }

    public boolean hasUser()
    {
        return user != null;
    }
}
