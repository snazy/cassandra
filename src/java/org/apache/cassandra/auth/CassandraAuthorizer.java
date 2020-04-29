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
package org.apache.cassandra.auth;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.auth.AuthKeyspace.ROLE_PERMISSIONS;
import static org.apache.cassandra.schema.SchemaConstants.AUTH_KEYSPACE_NAME;

/**
 * CassandraAuthorizer is an IAuthorizer implementation that keeps
 * user permissions internally in C* using the system_auth.role_permissions
 * table.
 */
public class CassandraAuthorizer implements IAuthorizer
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraAuthorizer.class);

    private static final String ROLE = "role";
    private static final String RESOURCE = "resource";
    private static final String PERMISSIONS = "permissions";
    private static final String GRANTABLES = "grantables";
    private static final String RESTRICTED = "restricted";

    private static final String ROLE_PERMISSIONS_TABLE = AUTH_KEYSPACE_NAME + "." + ROLE_PERMISSIONS;

    private SelectStatement permissionsForRolesStatement;

    public CassandraAuthorizer()
    {
    }

    // Called when deleting a role with DROP ROLE query.
    // Internal hook, so no permission checks are needed here.
    public void revokeAllFrom(RoleResource revokee)
    {
        try
        {
            process("DELETE FROM " + ROLE_PERMISSIONS_TABLE + " WHERE role = '%s'",
                    escape(revokee.getRoleName()));
        }
        catch (RequestExecutionException | RequestValidationException e)
        {
            logger.warn("CassandraAuthorizer failed to revoke all permissions of {}: {}",
                        revokee.getRoleName(),
                        e.getMessage());
        }
    }

    // Called after a resource is removed (DROP KEYSPACE, DROP TABLE, etc.).
    // Execute a logged batch removing all the permissions for the resource
    public Set<RoleResource> revokeAllOn(IResource droppedResource)
    {
        try
        {
            Set<String> roles = fetchRolesWithPermissionsOn(droppedResource);
            deletePermissionsFor(droppedResource, roles);
            return roles.stream().map(RoleResource::role).collect(Collectors.toSet());
        }
        catch (RequestExecutionException | RequestValidationException e)
        {
            logger.warn("CassandraAuthorizer failed to revoke all permissions on {}: {}", droppedResource, e.getMessage());
            return Collections.emptySet();
        }
    }

    /**
     * Deletes all the permissions for the specified resource and roles.
     * @param resource the resource
     * @param roles the roles
     */
    private void deletePermissionsFor(IResource resource, Set<String> roles)
    {
        if (roles.isEmpty())
            return;
        process("DELETE FROM " + ROLE_PERMISSIONS_TABLE + " WHERE role IN (%s) AND resource = '%s'",
                roles.stream()
                     .map(CassandraAuthorizer::escape)
                     .collect(Collectors.joining("', '", "'", "'")),
                escape(resource.getName()));
    }

    /**
     * Retrieves all the roles that have some permissions on the specified resource.
     * @param resource the resource for with the roles must be retrieved
     * @return the roles that have some permissions on the specified resource
     */
    private Set<String> fetchRolesWithPermissionsOn(IResource resource)
    {
        UntypedResultSet rows =
                process("SELECT role FROM " + ROLE_PERMISSIONS_TABLE + " WHERE resource = '%s' ALLOW FILTERING",
                        escape(resource.getName()));

        Set<String> roles = new HashSet<>(rows.size());
        for (UntypedResultSet.Row row : rows)
            roles.add(row.getString("role"));

        return roles;
    }

    public Map<IResource, PermissionSets> allPermissionSets(RoleResource role)
    {
        try
        {
            return permissionsForRole(role);
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
        catch (RequestExecutionException e)
        {
            logger.warn("CassandraAuthorizer failed to authorize {}", role);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<RoleResource, Map<IResource, PermissionSets>> allPermissionSetsMany(Iterable<? extends RoleResource> roles)
    {
        try
        {
            return permissionsForRoles(roles);
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
        catch (RequestExecutionException e)
        {
            logger.warn("CassandraAuthorizer failed to authorize {}", roles);
            throw new RuntimeException(e);
        }
    }

    private Map<IResource, PermissionSets> permissionsForRole(RoleResource role)
    {
        return permissionsForRoles(Collections.singleton(role)).get(role);
    }

    private Map<RoleResource, Map<IResource, PermissionSets>> permissionsForRoles(Iterable<? extends RoleResource> roles)
    {
        Map<RoleResource, Map<IResource, PermissionSets>> roleResourcePermissions = new HashMap<>();
        List<String> names = new ArrayList<>();
        for (RoleResource role : roles)
        {
            names.add(role.getRoleName());
            roleResourcePermissions.put(role, new HashMap<>());
        }
        ByteBuffer roleNames = ListSerializer.getInstance(UTF8Serializer.instance).serialize(names);

        QueryOptions options = QueryOptions.forInternalCalls(ConsistencyLevel.LOCAL_ONE,
                                                             Collections.singletonList(roleNames));


        ResultMessage.Rows rows = select(permissionsForRolesStatement, options);
        UntypedResultSet result = UntypedResultSet.create(rows.result);

        if (result.isEmpty())
            return roleResourcePermissions;

        for (UntypedResultSet.Row row : result)
        {
            IResource resource = Resources.fromName(row.getString(RESOURCE));
            PermissionSets.Builder builder = PermissionSets.builder();
            addPermissionsFromRow(row, builder);
            RoleResource role = RoleResource.role(row.getString(ROLE));
            Map<IResource, PermissionSets> resourcePermissions = roleResourcePermissions.get(role);
            resourcePermissions.put(resource, builder.build());
        }

        return roleResourcePermissions;
    }

    private static void addPermissionsFromRow(UntypedResultSet.Row row, PermissionSets.Builder perms)
    {
        permissionsFromRow(row, PERMISSIONS, perms::addGranted);
        permissionsFromRow(row, RESTRICTED, perms::addRestricted);
        permissionsFromRow(row, GRANTABLES, perms::addGrantable);
    }

    private static void permissionsFromRow(UntypedResultSet.Row row, String column, Consumer<Permission> perms)
    {
        if (!row.has(column))
            return;
        row.getSet(column, UTF8Type.instance)
           .stream()
           .map(Permission::byName)
           .forEach(perms);
    }

    public Set<Permission> grant(AuthenticatedUser performer,
                                 Set<Permission> permissions,
                                 IResource resource,
                                 RoleResource grantee,
                                 GrantMode... grantModes)
    {
        if (ArrayUtils.isEmpty(grantModes))
            throw new IllegalArgumentException("Must specify at least one grantMode");

        String roleName = escape(grantee.getRoleName());
        String resourceName = escape(resource.getName());

        Set<Permission> grantedPermissions = com.google.common.collect.Sets.newHashSetWithExpectedSize(permissions.size());
        for (GrantMode grantMode : grantModes)
        {
            String grantModeColumn = columnForGrantMode(grantMode);
            Set<Permission> nonExistingPermissions = new HashSet<>(permissions);
            nonExistingPermissions.removeAll(getExistingPermissions(roleName, resourceName, grantModeColumn, permissions));

            if (!nonExistingPermissions.isEmpty())
            {
                String perms = nonExistingPermissions.stream()
                                                     .map(Permission::name)
                                                     .collect(Collectors.joining("','", "'", "'"));

                updatePermissions(roleName, resourceName, grantModeColumn, "+", perms);
                grantedPermissions.addAll(nonExistingPermissions);
            }
        }
        return grantedPermissions;
    }

    public Set<Permission> revoke(AuthenticatedUser performer,
                                  Set<Permission> permissions,
                                  IResource resource,
                                  RoleResource revokee,
                                  GrantMode... grantModes)
    {
        if (ArrayUtils.isEmpty(grantModes))
            throw new IllegalArgumentException("Must specify at least one grantMode");

        String roleName = escape(revokee.getRoleName());
        String resourceName = escape(resource.getName());

        Set<Permission> revokedPermissions = new HashSet<>();
        for (GrantMode grantMode : grantModes)
        {
            String grantModeColumn = columnForGrantMode(grantMode);
            Set<Permission> existingPermissions = getExistingPermissions(roleName, resourceName, grantModeColumn, permissions);
            if (!existingPermissions.isEmpty())
            {
                String perms = existingPermissions.stream()
                                                  .map(Permission::name)
                                                  .collect(Collectors.joining("','", "'", "'"));

                updatePermissions(roleName, resourceName, grantModeColumn, "-", perms);
                revokedPermissions.addAll(existingPermissions);
            }
        }
        return revokedPermissions;
    }

    private void updatePermissions(String roleName,
                                   String resourceName,
                                   String grantModeColumn,
                                   String op,
                                   String perms)
    {
        process("UPDATE " + ROLE_PERMISSIONS_TABLE + " SET %s = %s %s { %s } WHERE role = '%s' AND resource = '%s'",
                grantModeColumn, grantModeColumn, op, perms,
                roleName,
                resourceName);
    }

    /**
     * Checks that the specified role has at least one of the expected permissions on the resource.
     *
     * @param roleName the role name
     * @param resourceName the resource name
     * @param grantModeColumn the grant mode column
     * @param expectedPermissions the permissions to check for
     * @return {@code true} if the role has at least one of the expected permissions on the resource, {@code false} otherwise.
     */
    private Set<Permission> getExistingPermissions(String roleName,
                                                   String resourceName,
                                                   String grantModeColumn,
                                                   Set<Permission> expectedPermissions)
    {
        UntypedResultSet rs = process("SELECT %s FROM " + ROLE_PERMISSIONS_TABLE + " WHERE role = '%s' AND resource = '%s'",
                                      grantModeColumn,
                                      roleName,
                                      resourceName);

        if (rs.isEmpty())
            return Collections.emptySet();

        UntypedResultSet.Row one = rs.one();

        if (!one.has(grantModeColumn))
            return Collections.emptySet();

        Set<Permission> existingPermissions = Sets.newHashSetWithExpectedSize(expectedPermissions.size());
        for (String permissionName : one.getSet(grantModeColumn, UTF8Type.instance))
        {
            Permission permission = Permission.permission(permissionName);
            if (expectedPermissions.contains(permission))
                existingPermissions.add(permission);
        }
        return existingPermissions;
    }

    private static String columnForGrantMode(GrantMode grantMode)
    {
        switch (grantMode)
        {
            case GRANT:
                return PERMISSIONS;
            case RESTRICT:
                return RESTRICTED;
            case GRANTABLE:
                return GRANTABLES;
            default:
                throw new AssertionError(); // make compiler happy
        }
    }

    public Set<PermissionDetails> list(Set<Permission> permissions,
                                       IResource resource,
                                       RoleResource grantee)
    {
        // 'grantee' can be null - in that case everyone's permissions have been requested. Otherwise only single user's.
        Set<RoleResource> roles = grantee != null
                                  ? DatabaseDescriptor.getRoleManager().getRoles(grantee, true)
                                  : Collections.emptySet();

        Set<PermissionDetails> details = new HashSet<>();
        // If it exists, try the legacy user permissions table first. This is to handle the case
        // where the cluster is being upgraded and so is running with mixed versions of the perms table
        for (UntypedResultSet.Row row : process("%s", buildListQuery(resource, roles)))
        {
            PermissionSets.Builder permsBuilder = PermissionSets.builder();
            addPermissionsFromRow(row, permsBuilder);
            PermissionSets perms = permsBuilder.build();

            String rowRole = row.getString(ROLE);
            IResource rowResource = Resources.fromName(row.getString(RESOURCE));

            for (Permission p : perms.allContainedPermissions())
            {
                if (permissions.contains(p))
                {
                    details.add(new PermissionDetails(rowRole,
                                                      rowResource,
                                                      p,
                                                      perms.grantModesFor(p)));
                }
            }
        }
        return details;
    }

    private String buildListQuery(IResource resource, Set<RoleResource> roles)
    {
        StringBuilder builder =
                new StringBuilder("SELECT " + ROLE + ", "
                                  + RESOURCE + ", "
                                  + PERMISSIONS + ", "
                                  + RESTRICTED + ", "
                                  + GRANTABLES
                                  + " FROM " +  ROLE_PERMISSIONS_TABLE);

        boolean hasResource = resource != null;
        boolean hasRoles = roles != null && !roles.isEmpty();

        if (hasResource)
        {
            builder.append(" WHERE resource = '")
                   .append(escape(resource.getName()))
                   .append('\'');
        }
        if (hasRoles)
        {
            builder.append(hasResource ? " AND " : " WHERE ")
                   .append(ROLE + " IN ")
                   .append(roles.stream()
                                .map(r -> escape(r.getRoleName()))
                                .collect(Collectors.joining("', '", "('", "')")));
        }
        builder.append(" ALLOW FILTERING");
        return builder.toString();
    }

    public Set<DataResource> protectedResources()
    {
        return ImmutableSet.of(DataResource.table(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS));
    }

    public void validateConfiguration() throws ConfigurationException
    {
    }

    public void setup()
    {
        String cql = "SELECT "
                     + ROLE + ", "
                     + RESOURCE + ", "
                     + PERMISSIONS + ", "
                     + RESTRICTED + ", "
                     + GRANTABLES
                     + " FROM " + ROLE_PERMISSIONS_TABLE
                     + " WHERE " + ROLE + " IN ?";

        permissionsForRolesStatement = (SelectStatement) QueryProcessor.getStatement(cql, QueryState.forInternalCalls());
    }

    // We only worry about one character ('). Make sure it's properly escaped.
    private static String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }

    @VisibleForTesting
    UntypedResultSet process(String query, Object... arguments) throws RequestExecutionException
    {
        String cql = String.format(query, arguments);
        return QueryProcessor.process(cql, ConsistencyLevel.LOCAL_ONE);
    }

    @VisibleForTesting
    ResultMessage.Rows select(SelectStatement statement, QueryOptions options)
    {
        return statement.execute(QueryState.forInternalCalls(), options, System.nanoTime());
    }
}
