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
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.FunctionResource;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.UserRolesAndPermissions;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.dht.Datacenters;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Primarily used as a recorder for server-generated timestamps (timestamp, in microseconds, and nowInSeconds - in, well, seconds).
 *
 * The goal is to be able to use a single consistent server-generated value for both timestamps across the whole request,
 * and later be able to inspect QueryState for the generated values - for logging or other purposes.
 */
public class QueryState
{
    private static final Logger logger = LoggerFactory.getLogger(QueryState.class);

    private final ClientState clientState;

    private long timestamp;
    private int nowInSeconds;

    private final LazyUserRolesAndPermissions userRolesAndPermissions;

    public QueryState(ClientState clientState)
    {
        this(clientState, new LazyUserRolesAndPermissions(clientState), Long.MIN_VALUE, Integer.MIN_VALUE);
    }

    public QueryState(ClientState clientState, UserRolesAndPermissions userRolesAndPermissions)
    {
        this(clientState, userRolesAndPermissions, Long.MIN_VALUE, Integer.MIN_VALUE);
    }

    public QueryState(ClientState clientState, UserRolesAndPermissions userRolesAndPermissions, long timestamp, int nowInSeconds)
    {
        this(clientState, new LazyUserRolesAndPermissions(clientState, userRolesAndPermissions), timestamp, nowInSeconds);
    }

    private QueryState(ClientState clientState, LazyUserRolesAndPermissions userRolesAndPermissions, long timestamp, int nowInSeconds)
    {
        this.clientState = clientState;
        this.userRolesAndPermissions = userRolesAndPermissions;
        this.timestamp = timestamp;
        this.nowInSeconds = nowInSeconds;
    }

    private static CassandraException wrapErrorStandard(AuthenticatedUser user, CassandraException e)
    {
        return new UnauthorizedException("Failed to retrieve roles+permissions for user " + user.getName(), e);
    }

    private static class LazyUserRolesAndPermissions
    {
        private final ClientState clientState;
        private UserRolesAndPermissions memoized;
        private CassandraException failure;

        private LazyUserRolesAndPermissions(ClientState clientState) {
            this.clientState = clientState;
        }

        private LazyUserRolesAndPermissions(ClientState clientState, UserRolesAndPermissions preset) {
            this.clientState = clientState;
            this.memoized = preset;
        }

        UserRolesAndPermissions get()
        {
            return get(QueryState::wrapErrorStandard);
        }

        UserRolesAndPermissions get(BiFunction<AuthenticatedUser, CassandraException, CassandraException> exceptionFunction)
        {
            UserRolesAndPermissions urp = memoized;

            if (urp == null)
            {
                if (failure != null)
                    throw failure;

                AuthenticatedUser user = clientState.getUser();

                if (user == null)
                    return null;

                try
                {
                    urp = memoized = DatabaseDescriptor.getAuthManager()
                                                       .getUserRolesAndPermissions(user);
                }
                catch (CassandraException e)
                {
                    logger.debug("Failed to retrieve roles+permissions for user {}", user, e);
                    CassandraException f = exceptionFunction.apply(user, e);
                    failure = f;
                    throw f;
                }
            }

            urp.ensureDCPermissions(Datacenters.thisDatacenter());

            return urp;
        }
    }

    /**
     * @return a QueryState object for internal C* calls (not limited by any kind of auth).
     */
    public static QueryState forInternalCalls()
    {
        return new QueryState(ClientState.forInternalCalls(null, AuthenticatedUser.SYSTEM_USER));
    }

    /**
     * Generate, cache, and record a timestamp value on the server-side.
     *
     * Used in reads for all live and expiring cells, and all kinds of deletion infos.
     *
     * Shouldn't be used directly. {@link org.apache.cassandra.cql3.QueryOptions#getTimestamp(QueryState)} should be used
     * by all consumers.
     *
     * @return server-generated, recorded timestamp in seconds
     */
    public long getTimestamp()
    {
        if (timestamp == Long.MIN_VALUE)
            timestamp = ClientState.getTimestamp();
        return timestamp;
    }

    /**
     * Generate, cache, and record a nowInSeconds value on the server-side.
     *
     * In writes is used for calculating localDeletionTime for tombstones and expiring cells and other deletion infos.
     * In reads used to determine liveness of expiring cells and rows.
     *
     * Shouldn't be used directly. {@link org.apache.cassandra.cql3.QueryOptions#getNowInSeconds(QueryState)} should be used
     * by all consumers.
     *
     * @return server-generated, recorded timestamp in seconds
     */
    public int getNowInSeconds()
    {
        if (nowInSeconds == Integer.MIN_VALUE)
            nowInSeconds = FBUtilities.nowInSeconds();
        return nowInSeconds;
    }

    /**
     * @return server-generated timestamp value, if one had been requested, or Long.MIN_VALUE otherwise
     */
    public long generatedTimestamp()
    {
        return timestamp;
    }

    /**
     * @return server-generated nowInSeconds value, if one had been requested, or Integer.MIN_VALUE otherwise
     */
    public int generatedNowInSeconds()
    {
        return nowInSeconds;
    }

    public ClientState getClientState()
    {
        return clientState;
    }

    public String getUserName()
    {
        return userRolesAndPermissions.get().getName();
    }

    public AuthenticatedUser getUser()
    {
        return clientState.getUser();
    }

    public boolean hasUser()
    {
        return clientState.hasUser();
    }

    public QueryState cloneWithKeyspaceIfSet(String keyspace)
    {
        ClientState clState = clientState.cloneWithKeyspaceIfSet(keyspace);
        if (clState == clientState)
            // this will be trie, if either 'keyspace' is null or 'keyspace' is equals to the current keyspace
            return this;
        return new QueryState(clState, userRolesAndPermissions, timestamp, nowInSeconds);
    }

    public InetAddress getClientAddress()
    {
        return clientState.getClientAddress();
    }

    /**
     * Checks if this user is an ordinary user (not a super or system user).
     * @return {@code true} if this user is an ordinary user, {@code flase} otherwise.
     */
    public boolean isOrdinaryUser()
    {
        return !userRolesAndPermissions.get().isSuper() && !userRolesAndPermissions.get().isSystem();
    }

    /**
     * Checks if this user is a super user.
     * <p>Only a superuser is allowed to perform CREATE USER and DROP USER queries.
     * Im most cased, though not necessarily, a superuser will have Permission.ALL on every resource
     * (depends on IAuthorizer implementation).</p>
     */
    public boolean isSuper()
    {
        return userRolesAndPermissions.get().isSuper();
    }

    /**
     * Checks if this user is the system user (Apollo).
     * @return {@code true} if this user is the system user, {@code flase} otherwise.
     */
    public boolean isSystem()
    {
        return userRolesAndPermissions.get().isSystem();
    }

    /**
     * Validates that this user is not an anonymous one.
     */
    public void ensureNotAnonymous()
    {
        userRolesAndPermissions.get().ensureNotAnonymous();
    }

    /**
     * Validates that this user is not an anonymous one.
     */
    public void ensureIsSuperuser(String msg)
    {
        userRolesAndPermissions.get().ensureIsSuperuser(msg);
    }

    /**
     * Checks if the user has the specified permission on the data resource.
     * @param resource the resource
     * @param perm the permission
     * @return {@code true} if the user has the permission on the data resource,
     * {@code false} otherwise.
     */
    public boolean hasDataPermission(Permission perm, DataResource resource)
    {
        return userRolesAndPermissions.get().hasDataPermission(perm, resource);
    }

    /**
     * Checks if the user has the specified permission on the function resource.
     * @param resource the resource
     * @param perm the permission
     * @return {@code true} if the user has the permission on the function resource,
     * {@code false} otherwise.
     */
    public final boolean hasFunctionPermission(Permission perm, FunctionResource resource)
    {
        return userRolesAndPermissions.get().hasFunctionPermission(perm, resource);
    }

    /**
     * Checks if the user has the specified permission on the resource.
     * @param resource the resource
     * @param perm the permission
     * @return {@code true} if the user has the permission on the resource,
     * {@code false} otherwise.
     */
    public final boolean hasPermission(Permission perm, IResource resource)
    {
        return userRolesAndPermissions.get().hasPermission(perm, resource);
    }

    /**
     * Checks if the user has the right to grant the permission on the resource.
     * @param resource the resource
     * @param perm the permission
     * @return {@code true} if the user has the right to grant the permission on the resource,
     * {@code false} otherwise.
     */
    public boolean hasGrantPermission(Permission perm, IResource resource)
    {
        return userRolesAndPermissions.get().hasGrantPermission(perm, resource);
    }

    /**
     * Validates that the user has the permission on all the keyspaces.
     * @param perm the permission
     * @throws UnauthorizedException if the user does not have the permission on all the keyspaces
     */
    public final void ensureAllKeyspacesPermission(Permission perm)
    {
        userRolesAndPermissions.get().ensureAllKeyspacesPermission(perm);
    }

    /**
     * Validates that the user has the permission on the keyspace.
     * @param keyspace the keyspace
     * @param perm the permission
     * @throws UnauthorizedException if the user does not have the permission on the keyspace
     */
    public final void ensureKeyspacePermission(Permission perm, String keyspace)
    {
        userRolesAndPermissions.get().ensureKeyspacePermission(perm, keyspace);
    }

    /**
     * Validates that the user has the permission on the table.
     * @param keyspace the table keyspace
     * @param table the table
     * @param perm the permission
     * @throws UnauthorizedException if the user does not have the permission on the table.
     */
    public final void ensureTablePermission(Permission perm, String keyspace, String table)
    {
        userRolesAndPermissions.get().ensureTablePermission(perm, keyspace, table);
    }

    /**
     * Validates that the user has the permission on the table.
     * @param tableRef the table
     * @param perm the permission
     * @throws UnauthorizedException if the user does not have the permission on the table.
     */
    public final void ensureTablePermission(Permission perm, TableMetadataRef tableRef)
    {
        userRolesAndPermissions.get().ensureTablePermission(perm, tableRef);
    }

    /**
     * Validates that the user has the permission on the table.
     * @param table the table metadata
     * @param perm the permission
     * @throws UnauthorizedException if the user does not have the permission on the table.
     */
    public final void ensureTablePermission(Permission perm, TableMetadata table)
    {
        userRolesAndPermissions.get().ensureTablePermission(perm, table);
    }

    /**
     * Validates that the user has the permission on the function.
     * @param function the function
     * @param perm the permission
     * @throws UnauthorizedException if the user does not have the permission on the function.
     */
    public final void ensureFunctionPermission(Permission perm, Function function)
    {
        userRolesAndPermissions.get().ensureFunctionPermission(perm, function);
    }

    /**
     * Validates that the user has the permission on the function.
     * @param perm the permission
     * @param function the function resource
     * @throws UnauthorizedException if the user does not have the permission on the function.
     */
    public final void ensureFunctionPermission(Permission perm, FunctionResource function)
    {
        userRolesAndPermissions.get().ensureFunctionPermission(perm, function);
    }

    public final void ensurePermission(Permission perm, IResource resource)
    {
        userRolesAndPermissions.get().ensurePermission(perm, resource);
    }

    /**
     * Checks that this user has the specified role.
     * @param role the role
     * @return {@code true} if the user has the specified role, {@code false} otherwise.
     */
    public final boolean hasRole(RoleResource role)
    {
        return userRolesAndPermissions.get().hasRole(role);
    }

    /**
     * Checks if the user has the specified permission on the role resource.
     * @param role the resource
     * @param perm the permission
     * @return {@code true} if the user has the permission on the role resource,
     * {@code false} otherwise.
     */
    public boolean hasRolePermission(Permission perm, RoleResource role)
    {
        return userRolesAndPermissions.get().hasRolePermission(perm, role);
    }

    public UserRolesAndPermissions getUserRolesAndPermissions()
    {
        return userRolesAndPermissions.get();
    }
}
