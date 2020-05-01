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

package org.apache.cassandra.cql3.statements;

import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.QueryState;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * Base class for all permissions related authorization statements -
 * i.e. {@code GRANT}, {@code REVOKE}, {@code RESTRICT}, {@code UNRESTRICT} and {@code LIST (permissions)}
 * statements.
 */
public abstract class PermissionsRelatedStatement extends AuthorizationStatement
{
    /**
     * Flag if the statement specified {@code ALL PERMISSIONS}.
     */
    protected final boolean allPermissions;
    /**
     * The permissions as specified in the statement. Will be {@code null}, for {@code ALL PERMISSIONS}.
     */
    public final Set<Permission> permissions;
    /**
     * The outcome of the call to {@link #filterPermissions()}. Never {@code null}.
     */
    public final Set<Permission> filteredPermissions;
    /**
     * The resource specified in the statement - or {@code null}, if no resource has been specified.
     */
    public IResource resource;
    /**
     * The grantee/revokee specified in the statement - or {@code null}, if not specified.
     */
    public final RoleResource grantee;
    protected final Consumer<String> recognitionError;

    protected PermissionsRelatedStatement(boolean allPermissions,
                                          Set<Permission> permissions,
                                          IResource resource,
                                          RoleName grantee,
                                          Consumer<String> recognitionError)
    {
        this.allPermissions = allPermissions;
        this.permissions = permissions;
        this.resource = resource;
        this.grantee = grantee.hasName() ? RoleResource.role(grantee.getName()) : null;
        // Need to filter the permissions here in the constructor, so that recognition errors can be emitted while
        // the statement is being parsed.
        this.recognitionError = recognitionError;
        this.filteredPermissions = filterPermissions();
    }

    private Set<Permission> filterPermissions()
    {
        if (resource == null)
            return permissionsIfNoResourceGiven();

        if (allPermissions)
            return allPermissions();

        Set<Permission> filtered = authorizer().filterApplicablePermissions(resource, permissions);

        if (filtered.size() != permissions.size())
        {
            String unsupported = permissions.stream()
                                            .filter(p -> !filtered.contains(p))
                                            .map(Permission::name)
                                            .collect(Collectors.joining(","));
            recognitionError.accept("Resource type " + resource.getClass().getSimpleName() +
                                    " does not support the requested permissions: " + unsupported);
        }

        return filtered;
    }

    protected IAuthorizer authorizer()
    {
        return DatabaseDescriptor.getAuthorizer();
    }

    /**
     * The permissions to use for {@code ALL PERMISSIONS} with a resource specified in a {@code GRANT/REVOKE/LIST} statement.
     */
    protected Set<Permission> allPermissions()
    {
        return Permission.ALL;
    }

    /**
     * The permissions to use if no resource has been specified in a {@code GRANT/REVOKE/LIST} statement.
     */
    protected Set<Permission> permissionsIfNoResourceGiven()
    {
        return Permission.ALL;
    }

    public void validate(QueryState state) throws RequestValidationException
    {
        if (!DatabaseDescriptor.getAuthorizer().requireAuthorization())
            throw invalidRequest(String.format("%s operation is not supported by the %s if it is not enabled",
                                               operation(),
                                               DatabaseDescriptor.getAuthorizer().implementation().getClass().getSimpleName()));
    }

    protected abstract String operation();
}
