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
import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;

import org.junit.*;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.auth.UserRolesAndPermissions;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.locator.SimpleSnitch;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.apache.cassandra.auth.Role.NULL_ROLE;

public class QueryStateTest
{
    private static IAuthorizer authorizer;

    @Before
    public void setupTest()
    {
        DatabaseDescriptor.setConfig(new Config());
        DatabaseDescriptor.setAuthenticator(new TestAuthenticator());
        DatabaseDescriptor.setAuthManager(new AuthManager(new TestRoleManager(), new TestAuthorizer(), new TestNetworkAuthorizer()));
        DatabaseDescriptor.setEndpointSnitch(new SimpleSnitch());

        authorizer = DatabaseDescriptor.getAuthorizer();
    }

    @Test
    public void testGrantRevokeWithoutCache()
    {
        DatabaseDescriptor.setPermissionsValidity(0);
        DatabaseDescriptor.setRolesValidity(0);

        grantRevoke();
    }

    @Test
    public void testGrantRevokeWithCache()
    {
        DatabaseDescriptor.setPermissionsValidity(100);
        DatabaseDescriptor.setRolesValidity(100);

        grantRevoke();
    }

    private void grantRevoke()
    {
        AuthenticatedUser user = new AuthenticatedUser("user");
        DataResource ks = DataResource.keyspace("ks");

        System.out.println("before queryState 1");
        QueryState queryState = getQueryState(user);

        System.out.println("before grant EXECUTE");
        authorizer.grant(null, Collections.singleton(Permission.EXECUTE), ks, user.getPrimaryRole(), GrantMode.GRANT);

        System.out.println("test 1");
        assertFalse(queryState.hasDataPermission(Permission.EXECUTE, ks));

        System.out.println("before queryState 2");
        queryState = getQueryState(user);
        System.out.println("test 1");
        assertTrue(queryState.hasDataPermission(Permission.EXECUTE, ks));

        //

        System.out.println("before grant SELECT");
        authorizer.grant(null, Collections.singleton(Permission.SELECT), ks, user.getPrimaryRole(), GrantMode.GRANT);
        System.out.println("test 1");
        assertTrue(queryState.hasDataPermission(Permission.EXECUTE, ks));
        System.out.println("test 2");
        assertFalse(queryState.hasDataPermission(Permission.SELECT, ks));

        System.out.println("before queryState 3");
        queryState = getQueryState(user);
        System.out.println("test 1");
        assertTrue(queryState.hasDataPermission(Permission.EXECUTE, ks));
        System.out.println("test 2");
        assertTrue(queryState.hasDataPermission(Permission.SELECT, ks));

        //

        System.out.println("before revoke EXECUTE");
        authorizer.revoke(null, Collections.singleton(Permission.EXECUTE), ks, user.getPrimaryRole(), GrantMode.GRANT);
        System.out.println("test 1");
        assertTrue(queryState.hasDataPermission(Permission.EXECUTE, ks));
        System.out.println("test 2");
        assertTrue(queryState.hasDataPermission(Permission.SELECT, ks));

        System.out.println("before queryState 4");
        queryState = getQueryState(user);
        System.out.println("test 1");
        assertFalse(queryState.hasDataPermission(Permission.EXECUTE, ks));
        System.out.println("test 2");
        assertTrue(queryState.hasDataPermission(Permission.SELECT, ks));
    }

    private QueryState getQueryState(AuthenticatedUser user)
    {
        UserRolesAndPermissions userRolesAndPermissions = DatabaseDescriptor.getAuthManager()
                                                                            .getUserRolesAndPermissions(user);
        QueryState queryState = new QueryState(ClientState.forExternalCalls(user), userRolesAndPermissions);
        return queryState;
    }

    public static class TestRoleManager implements IRoleManager
    {
        final Map<RoleResource, Role> roles = new HashMap<>();

        public Set<Option> supportedOptions()
        {
            throw new UnsupportedOperationException();
        }

        public Set<Option> alterableOptions()
        {
            throw new UnsupportedOperationException();
        }

        public void createRole(AuthenticatedUser performer, RoleResource role, RoleOptions options) throws RequestValidationException, RequestExecutionException
        {
            Role r = new Role(role.getRoleName(),
                              ImmutableSet.of(),
                              options.getSuperuser().or(false),
                              options.getLogin().or(false),
                              ImmutableMap.of(),
                              options.getPassword().or(""));
            roles.put(role, r);
        }

        public void dropRole(AuthenticatedUser performer, RoleResource role) throws RequestValidationException, RequestExecutionException
        {
            roles.remove(role);
        }

        public void alterRole(AuthenticatedUser performer, RoleResource role, RoleOptions options) throws RequestValidationException, RequestExecutionException
        {
            throw new UnsupportedOperationException();
        }

        public void grantRole(AuthenticatedUser performer, RoleResource role, RoleResource grantee) throws RequestValidationException, RequestExecutionException
        {
            Role r = roles.get(grantee);
            r.memberOf.add(role);
        }

        public void revokeRole(AuthenticatedUser performer, RoleResource role, RoleResource revokee) throws RequestValidationException, RequestExecutionException
        {
            Role r = roles.get(revokee);
            r.memberOf.remove(role);
        }

        public Set<RoleResource> getRoles(RoleResource grantee, boolean includeInherited) throws RequestValidationException, RequestExecutionException
        {
            Set<RoleResource> all = new HashSet<>();
            Role r = getRoleData(grantee);
            if (r != Role.NULL_ROLE)
            {
                all.add(grantee);
                collectRoles(r, all, includeInherited);
            }
            return all;
        }

        /*
         * Retrieve all roles granted to the given role. includeInherited specifies
         * whether to include only those roles granted directly or all inherited roles.
         */
        private void collectRoles(Role role, Set<RoleResource> collected, boolean includeInherited) throws RequestValidationException, RequestExecutionException
        {
            for (RoleResource memberOf : role.memberOf)
            {
                Role granted = getRoleData(memberOf);
                if (granted.equals(NULL_ROLE))
                    continue;
                collected.add(RoleResource.role(granted.name));
                if (includeInherited)
                    collectRoles(granted, collected, true);
            }
        }

        private void addRoles(Set<RoleResource> toAdd, Set<RoleResource> all)
        {
            for (RoleResource resource : toAdd)
            {
                all.add(resource);
                Role r = roles.get(resource);
                addRoles(r.memberOf, all);
            }
        }

        public Set<RoleResource> getAllRoles() throws RequestValidationException, RequestExecutionException
        {
            return roles.keySet();
        }

        public boolean isSuper(RoleResource role)
        {
            Role r = roles.get(role);
            return r != null ? r.isSuper : false;
        }

        public boolean canLogin(RoleResource role)
        {
            Role r = roles.get(role);
            return r != null ? r.canLogin : false;
        }

        public Map<String, String> getCustomOptions(RoleResource role)
        {
            return Collections.emptyMap();
        }

        public Set<RoleResource> filterExistingRoleNames(List<String> roleNames)
        {
            return roleNames.stream().map(RoleResource::role).collect(Collectors.toSet());
        }

        public boolean isExistingRole(RoleResource role)
        {
            return roles.containsKey(role);
        }

        public Role getRoleData(RoleResource role)
        {
            Role r = roles.get(role);
            return r == null ? Role.NULL_ROLE : roles.get(role);
        }

        public Set<? extends IResource> protectedResources()
        {
            throw new UnsupportedOperationException();
        }

        public void validateConfiguration() throws ConfigurationException
        {
        }

        public Future<?> setup()
        {
            return Futures.immediateFuture(null);
        }
    }

    public static class TestAuthorizer implements IAuthorizer
    {
        final Map<RoleResource, Map<IResource, PermissionSets>> roleResourcePermissions = new HashMap<>();

        public Map<IResource, PermissionSets> allPermissionSets(RoleResource role)
        {
            Map<IResource, PermissionSets> rolePerms = roleResourcePermissions.get(role);
            System.out.println("allPermissionSets " + role + " = " + rolePerms);
            return rolePerms;
        }

        public Set<Permission> grant(AuthenticatedUser performer,
                                     Set<Permission> permissions,
                                     IResource resource,
                                     RoleResource grantee,
                                     GrantMode... grantModes)
        {
            Map<IResource, PermissionSets> resourcePermissions = roleResourcePermissions.computeIfAbsent(grantee, (k) -> new HashMap<>());
            resourcePermissions = new HashMap<>(resourcePermissions);
            PermissionSets originalPermissionSets = resourcePermissions.computeIfAbsent(resource, (k) -> PermissionSets.EMPTY);

            PermissionSets.Builder builder = originalPermissionSets.unbuild();
            Set<Permission> granted = new HashSet<>();
            for (GrantMode grantMode : grantModes)
            {
                Set<Permission> nonExisting = new HashSet<>(permissions);
                switch (grantMode)
                {
                    case GRANT:
                        nonExisting.removeAll(originalPermissionSets.granted);
                        builder.addGranted(nonExisting);
                        break;
                    case GRANTABLE:
                        nonExisting.removeAll(originalPermissionSets.grantables);
                        builder.addGrantables(nonExisting);
                        break;
                    case RESTRICT:
                        nonExisting.removeAll(originalPermissionSets.restricted);
                        builder.addRestricted(nonExisting);
                        break;
                }
                granted.addAll(nonExisting);
            }
            PermissionSets permissionSets = builder.build();
            if (!permissionSets.isEmpty())
            {
                resourcePermissions.put(resource, builder.build());
                roleResourcePermissions.put(grantee, Collections.unmodifiableMap(resourcePermissions));
            }
            return granted;
        }

        public Set<Permission> revoke(AuthenticatedUser performer,
                                      Set<Permission> permissions,
                                      IResource resource,
                                      RoleResource revokee,
                                      GrantMode... grantModes)
        {
            Map<IResource, PermissionSets> resourcePermissions = roleResourcePermissions.computeIfAbsent(revokee, (k) -> new HashMap<>());
            if (resourcePermissions.isEmpty())
                return Collections.emptySet();

            resourcePermissions = new HashMap<>(resourcePermissions);
            PermissionSets originalPermissionSets = resourcePermissions.computeIfAbsent(resource, (k) -> PermissionSets.EMPTY);

            PermissionSets.Builder builder = originalPermissionSets.unbuild();
            Set<Permission> revoked = new HashSet<>();
            for (GrantMode grantMode : grantModes)
            {
                Set<Permission> existing = new HashSet<>(permissions);
                switch (grantMode)
                {
                    case GRANT:
                        existing.retainAll(originalPermissionSets.granted);
                        builder.removeGranted(existing);
                        break;
                    case GRANTABLE:
                        existing.retainAll(originalPermissionSets.grantables);
                        builder.removeGrantables(existing);
                        break;
                    case RESTRICT:
                        existing.retainAll(originalPermissionSets.restricted);
                        builder.removeRestricted(existing);
                        break;
                }
                revoked.addAll(existing);
            }

            PermissionSets permissionSets = builder.build();
            if (permissionSets.isEmpty())
                resourcePermissions.remove(resource);
            else
                resourcePermissions.put(resource, permissionSets);

            if (resourcePermissions.isEmpty())
                roleResourcePermissions.remove(revokee);
            else
                roleResourcePermissions.put(revokee, Collections.unmodifiableMap(resourcePermissions));

            return revoked;
        }

        public Set<PermissionDetails> list(Set<Permission> permissions, IResource resource, RoleResource grantee) throws RequestValidationException, RequestExecutionException
        {
            throw new UnsupportedOperationException();
        }

        public void revokeAllFrom(RoleResource revokee)
        {
            throw new UnsupportedOperationException();
        }

        public void revokeAllOn(IResource droppedResource)
        {
            throw new UnsupportedOperationException();
        }

        public Set<? extends IResource> protectedResources()
        {
            throw new UnsupportedOperationException();
        }

        public void validateConfiguration() throws ConfigurationException
        {
        }

        public void setup()
        {
        }
    }

    public static class TestAuthenticator implements IAuthenticator
    {
        public boolean requireAuthentication()
        {
            return true;
        }

        public Set<? extends IResource> protectedResources()
        {
            throw new UnsupportedOperationException();
        }

        public void validateConfiguration() throws ConfigurationException
        {
        }

        public void setup()
        {
        }

        public SaslNegotiator newSaslNegotiator(InetAddress clientAddress)
        {
            throw new UnsupportedOperationException();
        }

        public AuthenticatedUser legacyAuthenticate(Map<String, String> credentials) throws AuthenticationException
        {
            throw new UnsupportedOperationException();
        }
    }

    private class TestNetworkAuthorizer implements INetworkAuthorizer {
        @Override
        public boolean requireAuthorization()
        {
            return false;
        }

        @Override
        public void setup()
        {
        }

        @Override
        public DCPermissions authorize(RoleResource role)
        {
            return DCPermissions.all();
        }

        @Override
        public void setRoleDatacenters(RoleResource role, DCPermissions permissions)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void drop(RoleResource role)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void validateConfiguration() throws ConfigurationException
        {
        }
    }
}
