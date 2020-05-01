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

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.auth.Permission.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class RestrictFunctionalTest extends CQLTester
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.toolInitialization(false);
        SchemaLoader.setupAuth(new LocalCassandraRoleManager(), new AllowAllAuthenticator(), new StubAuthorizer(), new AllowAllNetworkAuthorizer());
    }

    @Test
    public void testPermissionCombinations()
    {
        @SuppressWarnings("unchecked") Map<RoleResource, Map<IResource, PermissionSets>> rolePermsMap = rolePermissionsMap(
                roleResourcePermissions("user", resourcePermissions("data/keyspace", PermissionSets.builder().addGranted(SELECT))),
                roleResourcePermissions("role1", resourcePermissions("data/keyspace", PermissionSets.builder().addRestricted(SELECT))),
                roleResourcePermissions("role2", resourcePermissions("data/keyspace", PermissionSets.builder().addGrantable(SELECT)))
                                                                                                                          );
        UserRolesAndPermissions urp = UserRolesAndPermissions.newNormalUserRolesAndPermissions("user",
                                                                                               roles("role1", "role2"),
                                                                                               DCPermissions.all(),
                                                                                               rolePermsMap);

        assertFalse(urp.hasDataPermission(SELECT, DataResource.table("keyspace", "table")));
        assertUnauthorized("Access for user user on <table keyspace.table> or any of its parents with SELECT permission is restricted",
                           () -> urp.ensureDataPermission(SELECT, DataResource.table("keyspace", "table")));
    }

    private void assertUnauthorized(String msg, Void call)
    {
        try
        {
            call.call();
            fail("Test didn't raise any exception");
        }
        catch (UnauthorizedException e)
        {
            assertEquals(msg, e.getMessage());
        }
    }

    @FunctionalInterface
    public interface Void
    {
        void call();
    }

    @SuppressWarnings("unchecked")
    private static Pair<RoleResource, Map<IResource, PermissionSets>> roleResourcePermissions(String role, Pair<IResource, PermissionSets>... resourcePermissions)
    {
        return Pair.create(RoleResource.role(role),
                           Arrays.stream(resourcePermissions).collect(Collectors.toMap(p -> p.left, p -> p.right)));
    }

    private static Pair<IResource, PermissionSets> resourcePermissions(String resourceName, PermissionSets.Builder psb)
    {
        return Pair.create(Resources.fromName(resourceName), psb.build());
    }

    @SuppressWarnings("unchecked")
    private static Map<RoleResource, Map<IResource, PermissionSets>> rolePermissionsMap(Pair<RoleResource, Map<IResource, PermissionSets>>... roleResourcePermissions)
    {
        return Arrays.stream(roleResourcePermissions).collect(Collectors.toMap(p -> p.left, p -> p.right));
    }

    private static Set<RoleResource> roles(String... roles)
    {
        return Arrays.stream(roles).map(RoleResource::role).collect(Collectors.toSet());
    }
}
