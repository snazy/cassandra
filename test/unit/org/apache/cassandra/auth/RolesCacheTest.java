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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.auth.RoleResource.role;
import static org.apache.cassandra.auth.Permission.ALTER;
import static org.apache.cassandra.auth.Permission.CREATE;
import static org.apache.cassandra.auth.Permission.DROP;
import static org.apache.cassandra.auth.Permission.SELECT;
import static org.apache.cassandra.auth.Permission.UPDATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Simulates loading of role hierarchies.
 *
 * Used to verify the number of loads, that circular role dependencies are detected.
 */
public class RolesCacheTest
{
    private static final Logger logger = LoggerFactory.getLogger(RolesCacheTest.class);

    private Map<RoleResource, Set<RoleResource>> loadResults;

    private int loadAllInvocations;
    private int loadAllKeys;
    private int loadInvocations;
    private RolesCache cache;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setAuthenticator(new PasswordAuthenticator());
        DatabaseDescriptor.setAuthManager(new AuthManager(new CassandraRoleManager(), new StubAuthorizer(), new LocalCassandraNetworkAuthorizer()));
    }

    @Before
    public void setup()
    {
        loadResults = new HashMap<>();
        clear();
    }

    private void clear()
    {
        clearStats();
        cache = new RolesCache(this::loadFunction, this::loadAllFunction, ()  -> true);
    }

    private void clearStats()
    {
        loadAllInvocations = 0;
        loadAllKeys = 0;
        loadInvocations = 0;
    }

    @Test
    public void loadShouldRetrieveRolesAndAttributes()
    {
        makeRole("r1", "r2", "r3");
        makeRole("r2");
        makeRole("r3");

        Map<RoleResource, Role> roles = assertLoadCounts("r1", 1, 2, 1);
        assertEquals(3, roles.size());
        Role attrs = roles.get(role("r1"));
        assertEquals(2, attrs.memberOf.size());
        assertTrue(attrs.memberOf.contains(role("r2")));
        assertTrue(attrs.memberOf.contains(role("r3")));
    }

    @Test
    public void collectRolesShouldUseTheLeastAmountOfBulkLoads_simple()
    {
        // user_1
        makeRole("user_1");

        // load user_1
        // --> memberOf[]
        assertLoadCounts("user_1", 0, 0, 1);
        // cached, so nothing to load
        assertLoadCounts("user_1", 0, 0, 0);
    }

    @Test
    public void collectRolesShouldUseTheLeastAmountOfBulkLoads_singleRole()
    {
        // user_2
        // --> role_1
        makeRole("role_1");
        makeRole("user_2", "role_1");

        // load user_2
        // --> memberOf[role_1]
        // --> loadAll [role_1]
        assertLoadCounts("user_2", 1, 1, 1);
        // cached, so nothing to load
        assertLoadCounts("user_2", 0, 0, 0);
    }

    @Test
    public void collectRolesShouldUseTheLeastAmountOfBulkLoads_twoRoles()
    {
        // user_3
        // --> role_2
        // --> role_1
        makeRole("role_2", "role_1");
        makeRole("user_3", "role_2");

        makeRole("role_1");
        assertLoadCounts("role_1", 0, 0, 1);

        // load of user_3, memberOf[role_2]
        // --> loadAll [role_2]
        // --> memberOf[role_1], already cached
        assertLoadCounts("user_3", 1, 1, 1);
        assertLoadCounts("user_3", 0, 0, 0);

        clear();

        // load user_3
        // --> memberOf[role_2]
        // --> loadAll [role_2]
        // --> memberOf[role_1]
        // --> loadAll [role_1]
        assertLoadCounts("user_3", 2, 2, 1);
        assertLoadCounts("user_3", 0, 0, 0);
    }

    @Test
    public void collectRolesShouldUseTheLeastAmountOfBulkLoads_complex()
    {
        // user_4
        // --> role_3 --> role_4
        // --> role_5 --> role_4
        // --> role_6 --> role_3
        // --> role_7 --> role_8
        makeRole("user_4", "role_3", "role_5", "role_6", "role_7");
        makeRole("role_3", "role_4");
        makeRole("role_4");
        makeRole("role_5", "role_4");
        makeRole("role_6", "role_3");
        makeRole("role_7", "role_8");
        makeRole("role_8");

        // load user_4
        // --> memberOf[role_3, role_5, role_6, role_7]
        // --> loadAll [role_3, role_5, role_6, role_7]
        // --> role_3.memberOf[role_4] role_5.memberOf[role_4] role_6.memberOf[role_3] role_7.memberOf[role_8]
        // --> loadAll [role_4, role_8]
        // --> role_4.memberOf[] role_8.memberOf[]
        assertLoadCounts("user_4", 2, 6, 1);
        assertLoadCounts("user_4", 0, 0, 0);
    }

    @Test
    public void collectRolesShouldUseTheLeastAmountOfBulkLoads_veryComplex()
    {
        // user_5
        // --> role_3 --> role_4
        // --> role_5 --> role_4
        // --> role_6 --> role_3
        // --> role_7 --> role_8
        // --> role_9 --> role_3
        // --> role_a --> role_b
        // --> role_b --> role_c, role_e
        // --> role_c --> role_d    // CYCLE!
        // --> role_d --> role_c    // CYCLE!
        // --> role_e --> role_f --> role_b     // CYCLE!
        makeRole("user_5", "role_3", "role_5", "role_6", "role_7", "role_9", "role_a", "role_b", "role_c", "role_d", "role_e");
        makeRole("role_3", "role_4");
        makeRole("role_4");
        makeRole("role_5", "role_4");
        makeRole("role_6", "role_3");
        makeRole("role_7", "role_8");
        makeRole("role_8");
        makeRole("role_9", "role_3");
        makeRole("role_a", "role_b");
        makeRole("role_b", "role_c", "role_e");
        makeRole("role_c", "role_d");
        makeRole("role_d", "role_c");
        makeRole("role_e", "role_f");
        makeRole("role_f", "role_b");

        // load user_5
        // --> memberOf[role_3, role_5, role_6, role_7, role_9, role_a, role_b, role_c, role_d, role_e]
        // --> loadAll [role_3, role_5, role_6, role_7, role_9, role_a, role_b, role_c, role_d, role_e]
        // --> role_3.memberOf[role_4] role_5.memberOf[role_4] role_6.memberOf[role_3] role_7.memberOf[role_8]
        //     role_9.memberOf[role_3] role_a.memberOf[role_b] role_b.memberOf[role_c] role_c.memberOf[role_d]
        //     role_d.memberOf[role_c] role_e.memberOf[role_f]
        // --> loadAll [role_4, role_8, role_f]
        // --> role_4.memberOf[] role_8.memberOf[] role_f[role_b]
        assertLoadCounts("user_5", 2, 13, 1);
        Map<RoleResource, Role> roles = assertLoadCounts("user_5", 0, 0, 0);

        // also verify that permission checks work with "crazy" role hierarchies

        Map<RoleResource, Map<IResource, PermissionSets>> permissions = new HashMap<>();
        addPermissions(permissions, "role_f", DataResource.keyspace("ks_1"), SELECT, UPDATE);
        addPermissions(permissions, "role_e", DataResource.keyspace("ks_2"), ALTER);
        addPermissions(permissions, "role_8", DataResource.keyspace("ks_3"), DROP);
        addPermissions(permissions, "user_5", DataResource.keyspace("ks_4"), CREATE);
        UserRolesAndPermissions urp = UserRolesAndPermissions.newNormalUserRolesAndPermissions("user_5", roles.keySet(), DCPermissions.all(), permissions);

        assertTrue(urp.hasPermission(SELECT, DataResource.keyspace("ks_1")));
        assertTrue(urp.hasPermission(UPDATE, DataResource.keyspace("ks_1")));
        assertFalse(urp.hasPermission(ALTER, DataResource.keyspace("ks_1")));
        assertTrue(urp.hasPermission(ALTER, DataResource.keyspace("ks_2")));
        assertFalse(urp.hasPermission(DROP, DataResource.keyspace("ks_2")));
        assertTrue(urp.hasPermission(DROP, DataResource.keyspace("ks_3")));
        assertFalse(urp.hasPermission(CREATE, DataResource.keyspace("ks_3")));
        assertTrue(urp.hasPermission(CREATE, DataResource.keyspace("ks_4")));
        assertFalse(urp.hasPermission(SELECT, DataResource.keyspace("ks_4")));
    }

    private void addPermissions(Map<RoleResource, Map<IResource, PermissionSets>> permissions, String role, IResource res, Permission... perm)
    {
        PermissionSets.Builder psb = PermissionSets.builder();
        Arrays.stream(perm).forEach(psb::addGranted);

        permissions.put(role(role), Collections.singletonMap(res, psb.build()));
    }

    private Map<RoleResource, Role> assertLoadCounts(String role, int expectedLoadAllInvocations, int expectedLoadAllKeys, int expectedLoadInvocations)
    {
        Map<RoleResource, Role> roles = cache.getRoles(role(role));
        assertEquals("load invocations for " + role + " (loadAll, loadALlKeys, load)",
                     expectedLoadAllInvocations + "/" + expectedLoadAllKeys + "/" + expectedLoadInvocations,
                     loadAllInvocations + "/" + loadAllKeys + "/" + loadInvocations);
        clearStats();
        return roles;
    }

    /**
     * Add a role and it's "memberOf" into {@link #loadResults}.
     */
    private void makeRole(String role, String... memberOf)
    {
        Set<RoleResource> memberOfSet = Arrays.stream(memberOf).map(RoleResource::role).collect(Collectors.toSet());
        loadResults.put(role(role), memberOfSet);
    }

    private Map<RoleResource, Role> loadAllFunction(Iterable<? extends RoleResource> roleResources)
    {
        Map<RoleResource, Role> result = new HashMap<>();
        roleResources.forEach(r -> result.put(r, createLoadResult(r)));
        logger.info("loadAll for {}", result.keySet());
        loadAllInvocations++;
        loadAllKeys += result.size();
        return result;
    }

    private Role loadFunction(RoleResource roleResource)
    {
        logger.info("load for {}", roleResource);
        loadInvocations++;
        return createLoadResult(roleResource);
    }

    private Role createLoadResult(RoleResource r)
    {
        Set<RoleResource> memberOf = loadResults.get(r);
        assertNotNull(r.toString(), memberOf);
        return new Role(r.getRoleName(),
                        ImmutableSet.copyOf(memberOf),
                        false, false, ImmutableMap.of(), "");
    }
}
