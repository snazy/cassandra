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

import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public abstract class PermissionsManagementStatement extends PermissionsRelatedStatement
{
    public final GrantMode grantMode;

    protected PermissionsManagementStatement(boolean allPermissions,
                                             Set<Permission> permissions,
                                             IResource resource,
                                             RoleName grantee,
                                             GrantMode grantMode,
                                             Consumer<String> recognitionError)
    {
        super(allPermissions, permissions, resource, grantee, recognitionError);
        this.grantMode = grantMode;
    }

    public void validate(QueryState state) throws RequestValidationException
    {
        if (!DatabaseDescriptor.getAuthorizer().requireAuthorization())
            throw invalidRequest("%s operation is not supported by the %s if it is not enabled",
                                 operation(),
                                 DatabaseDescriptor.getAuthorizer().implementation().getClass().getSimpleName());

        // validate login here before authorize to avoid leaking user existence to anonymous users.
        state.ensureNotAnonymous();

        if (!DatabaseDescriptor.getRoleManager().isExistingRole(grantee))
            throw new InvalidRequestException(String.format("Role %s doesn't exist", grantee.getRoleName()));

        // if a keyspace is omitted when GRANT/REVOKE ON TABLE <table>, we need to correct the resource.
        // called both here and in authorize(), as in some cases we do not call the latter.
        resource = maybeCorrectResource(resource, state);

        // altering permissions on builtin functions is not supported
        if (resource instanceof FunctionResource
            && SchemaConstants.SYSTEM_KEYSPACE_NAME.equals(((FunctionResource)resource).getKeyspace()))
        {
            throw new InvalidRequestException("Altering permissions on builtin functions is not supported");
        }

        if (!resource.exists())
            throw new InvalidRequestException(String.format("Resource %s doesn't exist", resource));
    }

    protected abstract String operation();

    public void authorize(QueryState state) throws UnauthorizedException
    {
        // if a keyspace is omitted when GRANT/REVOKE ON TABLE <table>, we need to correct the resource.
        resource = maybeCorrectResource(resource, state);

        if (state.isSuper())
            // Nobody can stop superman
            return;

        // validate login here to avoid leaking user existence to anonymous users.
        state.ensureNotAnonymous();

        if (grantMode == GrantMode.RESTRICT)
            throw new UnauthorizedException("Only superusers are allowed to RESTRICT/UNRESTRICT");

        Set<Permission> missingPermissions = Permission.setOf();
        try
        {
            // check that the user has AUTHORIZE permission on the resource or its parents, otherwise reject GRANT/REVOKE.
            state.ensurePermission(Permission.AUTHORIZE, resource);

            // check that the user has [a single permission or all in case of ALL] on the resource or its parents.
            Set<Permission> supersededGranted = Permission.setOf();
            for (Permission p : filteredPermissions)
            {
                try
                {
                    state.ensurePermission(p, resource);
                    if (p.supersedes() != null)
                        supersededGranted.add(p.supersedes());
                }
                catch (UnauthorizedException noAuthorizePermission)
                {
                    missingPermissions.add(p);
                }
            }

            // In case we are dealing with a statement using ALL permissions, we can safely ignore.
            //
            // A REVOKE ALL FROM KEYSPACE ... has 'Permissions.all()' in 'filteredPermissions' (really all permissions).
            // However, GRANT ALL ON KEYSPACE ... only has the applicable and non-deprecated permissions.
            //
            // I.e. a GRANT ALL ON KEYSPACE followed by a REVOKE ALL ON KEYSPACE would fail, because the MODIFY
            // permission hasn't been granted, but is checked during revoke. So we do a simple trick here and
            // ignore the missing and superseded permissions for the missing-permisssions check.
            //
            // In other words, the 'REVOKE ALL ON KEYSPACE ks_user1 FROM roleX' statement in
            // org.apache.cassandra.auth.GrantAndRevokeTest.testGrantRevokeAll would otherwise fail.
            if (!missingPermissions.isEmpty() && allPermissions && !supersededGranted.isEmpty())
            {
                ClientWarn.instance.warn(String.format("Not considering permission %s for %s ALL statement", supersededGranted, operation()));
                missingPermissions.removeAll(supersededGranted);
            }
        }
        catch (UnauthorizedException noAuthorizePermission)
        {
            missingPermissions.add(Permission.AUTHORIZE);
        }

        if (!missingPermissions.isEmpty())
        {
            if (grantMode == GrantMode.GRANTABLE)
            {
                throw new UnauthorizedException(String.format("User %s must not grant AUTHORIZE FOR %s permission on %s",
                                                              state.getUserName(),
                                                              StringUtils.join(missingPermissions, ", "),
                                                              resource));
            }

            Set<Permission> missingGrantables = Permission.setOf();

            // Check that the user has grant-option on all permissions to be
            // granted for the resource
            for (Permission p : filteredPermissions)
                if (!state.hasGrantPermission(p, resource))
                {
                    missingGrantables.add(p);
                }

            if (!missingGrantables.isEmpty())
                throw new UnauthorizedException(String.format("User %s has no %s permission nor AUTHORIZE FOR %s permission on %s or any of its parents",
                                                              state.getUserName(),
                                                              StringUtils.join(missingPermissions, ", "),
                                                              StringUtils.join(missingGrantables, ", "),
                                                              resource));

            // This prevents a user to grant a permission to himself or
            // to any of the roles the user belongs to. This check
            // assumes that getRoles() returns a role for the user himself
            // (i.e. like "RoleResource(username)" as for AuthenticatedUser.role).
            if (state.hasRole(grantee))
                throw new UnauthorizedException(String.format("User %s has grant privilege for %s permission(s) on %s but must not grant/revoke for him/herself",
                                                              state.getUserName(),
                                                              StringUtils.join(permissions, ", "),
                                                              resource));

        }
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
