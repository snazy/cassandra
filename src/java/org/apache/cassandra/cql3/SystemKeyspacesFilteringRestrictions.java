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
package org.apache.cassandra.cql3;


import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.Resources;
import org.apache.cassandra.auth.UserRolesAndPermissions;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.restrictions.AuthRestriction;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.virtual.VirtualSchemaKeyspace;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Restricts visible data in the system keyspaces ({@code system_schema} + {@code system} keyspaces) as per
 * permissions granted to a role/user.
 * <p>
 * Internal requests and requests by superusers bypass any permission check.
 * </p>
 * <p>
 * Access to keyspaces {@value SchemaConstants#TRACE_KEYSPACE_NAME}, {@value SchemaConstants#AUTH_KEYSPACE_NAME},
 * {@value SchemaConstants#DISTRIBUTED_KEYSPACE_NAME} are not allowed at all.
 * </p>
 * <p>
 * Access to schema data of other keyspaces is granted if the user has {@link Permission#DESCRIBE} permission.
 * </p>
 * <p>
 * This functionality must be enabled by setting {@link org.apache.cassandra.config.Config#system_keyspaces_filtering}
 * to {@code true} and have authenticator and authorizer configured that require authentication.
 * </p>
 */
public final class SystemKeyspacesFilteringRestrictions
{
    static
    {
        maybeAddReadableSystemResources();
    }

    @SuppressWarnings("deprecation")
    private static void maybeAddReadableSystemResources()
    {
        if (enabled())
        {
            // Add a couple of system keyspace tables to org.apache.cassandra.service.ClientState.READABLE_SYSTEM_RESOURCES.
            // We can control what a user (or driver) can see in these tables.

            Resources.addReadableSystemResource(DataResource.table(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.LEGACY_AVAILABLE_RANGES));
            Resources.addReadableSystemResource(DataResource.table(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.AVAILABLE_RANGES_V2));
            Resources.addReadableSystemResource(DataResource.table(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.LEGACY_SIZE_ESTIMATES));
            Resources.addReadableSystemResource(DataResource.table(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.TABLE_ESTIMATES));

            Resources.addReadableSystemResource(DataResource.table(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.BUILT_INDEXES));
            Resources.addReadableSystemResource(DataResource.table(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.BUILT_VIEWS));
            Resources.addReadableSystemResource(DataResource.table(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.SSTABLE_ACTIVITY));
            Resources.addReadableSystemResource(DataResource.table(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.VIEW_BUILDS_IN_PROGRESS));
        }
    }

    private static boolean enabled()
    {
        return DatabaseDescriptor.isSystemKeyspaceFilteringEnabled();
    }

    private static boolean checkDescribePermissionOnKeyspace(String keyspace, UserRolesAndPermissions userRolesAndPermissions)
    {
        // Check whether the current user has DESCRIBE permission on the keyspace.
        // Intentionally not checking permissions on individual schema objects like tables,
        // types or functions, because that would require checking the whole object _tree_.

        return userRolesAndPermissions.hasPermission(Permission.DESCRIBE, DataResource.keyspace(keyspace));
    }

    public static Server.ChannelFilter getChannelFilter(Event.SchemaChange event)
    {
        String keyspace = event.keyspace;

        // just allow schema change notifications for all system keyspaces (local + distributed)
        if (!enabled()
            || SchemaConstants.isLocalSystemKeyspace(keyspace)
            || SchemaConstants.isReplicatedSystemKeyspace(keyspace))
        {
            return Server.ChannelFilter.NOOP_FILTER;
        }

        return channel -> {
            ClientState clientState = channel.attr(Server.ATTR_KEY_CLIENT_STATE).get();
            AuthenticatedUser user = clientState.getUser();

            UserRolesAndPermissions userRolesAndPermissions = DatabaseDescriptor.getAuthManager()
                                                                                .getUserRolesAndPermissions(user);

            if (checkDescribePermissionOnKeyspace(keyspace, userRolesAndPermissions))
                return Optional.of(channel);
            return Optional.empty();
        };
    }

    @SuppressWarnings("deprecation")
    public static AuthRestriction restrictionsForTable(TableMetadata tableMetadata)
    {
        if (!enabled())
            return null;

        switch (tableMetadata.keyspace)
        {
            case SchemaConstants.SYSTEM_KEYSPACE_NAME:
                switch (tableMetadata.name)
                {
                    case SystemKeyspace.LOCAL:
                    case SystemKeyspace.LEGACY_PEERS:
                    case SystemKeyspace.PEERS_V2:
                        // allow
                        break;
                    case SystemKeyspace.SSTABLE_ACTIVITY:
                    case SystemKeyspace.LEGACY_SIZE_ESTIMATES:
                    case SystemKeyspace.TABLE_ESTIMATES:
                    case SystemKeyspace.BUILT_INDEXES: // note: column 'table_name' is the keyspace_name - duh!
                    case SystemKeyspace.BUILT_VIEWS:
                    case SystemKeyspace.LEGACY_AVAILABLE_RANGES:
                    case SystemKeyspace.AVAILABLE_RANGES_V2:
                    case SystemKeyspace.VIEW_BUILDS_IN_PROGRESS:
                        return new SystemKeyspacesRestriction(tableMetadata, false);
                    default:
                        return new DenyAllRestriction(tableMetadata.partitionKeyColumns());
                }
                break;
            case SchemaConstants.SCHEMA_KEYSPACE_NAME:
                switch (tableMetadata.name)
                {
                    case SchemaKeyspace.TABLES:
                    case SchemaKeyspace.COLUMNS:
                    case SchemaKeyspace.DROPPED_COLUMNS:
                    case SchemaKeyspace.VIEWS:
                        return new SystemKeyspacesRestriction(tableMetadata, true);
                    case SchemaKeyspace.FUNCTIONS:
                    case SchemaKeyspace.AGGREGATES:
                    case SchemaKeyspace.KEYSPACES:
                    case SchemaKeyspace.INDEXES:
                    case SchemaKeyspace.TYPES:
                    case SchemaKeyspace.TRIGGERS:
                    default: // be safe
                        return new SystemKeyspacesRestriction(tableMetadata, false);
                }
                //break; // unreachable, the switch above is exhaustive
            case VirtualSchemaKeyspace.NAME:
                switch (tableMetadata.name)
                {
                    case VirtualSchemaKeyspace.TABLES:
                    case VirtualSchemaKeyspace.COLUMNS:
                        return new SystemKeyspacesRestriction(tableMetadata, true);
                    case VirtualSchemaKeyspace.KEYSPACES:
                        return new SystemKeyspacesRestriction(tableMetadata, false);
                }
                break;
        }
        return null;
    }

    /**
     * Implements restrictions to filter on keyspace_name / table_name depending on
     * the authenticated user's permissions.
     * <br/>
     * <em>NOTE:</em> this implementation can currently only be used on keyspaces
     * using {@code LocalStrategy}, maybe {@code EverywhereStrategy}.
     * We do not need this kind of filtering for other strategies yet.
     */
    private static final class SystemKeyspacesRestriction implements AuthRestriction
    {
        private final ColumnMetadata column;

        SystemKeyspacesRestriction(TableMetadata tableMetadata, boolean withTableInClustering)
        {
            column = withTableInClustering
                     ? tableMetadata.clusteringColumns().iterator().next()
                     : tableMetadata.partitionKeyColumns().iterator().next();
        }

        @Override
        public void addRowFilterTo(RowFilter filter,
                                   QueryState state)
        {
            if (state.isOrdinaryUser())
                filter.addUserExpression(new Expression(column, state));
        }

        private static class Expression extends RowFilter.UserExpression
        {
            private final boolean withTableInClustering;
            private final QueryState state;

            Expression(ColumnMetadata column, QueryState state)
            {
                super(column, Operator.EQ, ByteBufferUtil.EMPTY_BYTE_BUFFER);
                this.state = state;
                this.withTableInClustering = column.isClusteringColumn();
            }

            @Override
            protected void serialize(DataOutputPlus out, int version) throws IOException
            {
                throw new UnsupportedOperationException();
            }

            @Override
            protected long serializedSize(int version)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row)
            {
                // note: does not support MVs (and we do not have those in system/system_schema keyspaces)

                String keyspace = UTF8Serializer.instance.deserialize(partitionKey.getKey().duplicate());

                // Handle SELECT against tables in 'system_schema' keyspace,
                // rows for 'system' and 'system_schema' keyspaces.
                // I.e. _describing_ system keyspaces.
                if (SchemaConstants.SCHEMA_KEYSPACE_NAME.equals(metadata.keyspace))
                {
                    switch (keyspace)
                    {
                        // rows for schema tables in 'system_schema' keyspace
                        case SchemaConstants.SCHEMA_KEYSPACE_NAME:
                            // meaning: always allow reads of schema information of everything in the 'system_schema' keyspace
                            return true;
                        // rows for schema related tables in 'system' keyspace
                        case SchemaConstants.SYSTEM_KEYSPACE_NAME:
                            if (row.isEmpty())
                            {
                                // SELECT DISTINCT keyspace_name FROM ... (only selecting static olumns
                                return true;
                            }

                            if (!withTableInClustering)
                            {
                                return false;
                            }

                            String table = UTF8Serializer.instance.deserialize(row.clustering().clustering().getRawValues()[0]);
                            return checkTables(table);
                    }
                }

                return checkDescribePermissionOnKeyspace(keyspace, state.getUserRolesAndPermissions());
            }

            @SuppressWarnings("deprecation")
            private boolean checkTables(String table)
            {
                // meaning: allow reads of schema information of certain tables in the 'system' keyspace
                switch (table)
                {
                    case SystemKeyspace.LOCAL:
                    case SystemKeyspace.LEGACY_PEERS:
                    case SystemKeyspace.PEERS_V2:
                    case SystemKeyspace.SSTABLE_ACTIVITY:
                    case SystemKeyspace.LEGACY_SIZE_ESTIMATES:
                    case SystemKeyspace.TABLE_ESTIMATES:
                    case SystemKeyspace.BUILT_INDEXES: // note: column 'table_name' is the keyspace_name - duh!
                    case SystemKeyspace.BUILT_VIEWS:
                    case SystemKeyspace.AVAILABLE_RANGES_V2:
                    case SystemKeyspace.LEGACY_AVAILABLE_RANGES:
                    case SystemKeyspace.VIEW_BUILDS_IN_PROGRESS:
                        // meaning: always allow reads of schema information of the above tables in the 'system' keyspace
                        return true;
                    default:
                        // deny
                        return false;
                }
            }
        }
    }

    private static final class DenyAllRestriction implements AuthRestriction
    {
        private final ColumnMetadata column;

        DenyAllRestriction(List<ColumnMetadata> columns)
        {
            this.column = columns.get(0);
        }

        @Override
        public void addRowFilterTo(RowFilter filter,
                                   QueryState state)
        {
            if (state.isOrdinaryUser())
                filter.addUserExpression(new Expression(column));
        }

        private static class Expression extends RowFilter.UserExpression
        {
            Expression(ColumnMetadata keyspaceColumn)
            {
                // A keyspace name cannot be empty - so this should be fine (EQ never yields true)
                super(keyspaceColumn, Operator.EQ, ByteBufferUtil.EMPTY_BYTE_BUFFER);
            }

            @Override
            protected void serialize(DataOutputPlus out, int version) throws IOException
            {
                throw new UnsupportedOperationException();
            }

            @Override
            protected long serializedSize(int version)
            {
                throw new UnsupportedOperationException();
            }

            public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row)
            {
                return false;
            }
        }
    }
}
