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
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.schema.SchemaConstants;

import static org.apache.cassandra.auth.Permission.ALTER;
import static org.apache.cassandra.auth.Permission.AUTHORIZE;
import static org.apache.cassandra.auth.Permission.CREATE;
import static org.apache.cassandra.auth.Permission.DESCRIBE;
import static org.apache.cassandra.auth.Permission.DROP;
import static org.apache.cassandra.auth.Permission.MODIFY;
import static org.apache.cassandra.auth.Permission.SELECT;
import static org.apache.cassandra.auth.Permission.TRUNCATE;
import static org.apache.cassandra.auth.Permission.UPDATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GrantAndRevokeTest extends CQLTester
{
    private static final String role = "role";
    private static final String user = "user";
    private static final String otherUser = "user3";
    private static final String pass = "12345";
    
    @BeforeClass
    public static void setup()
    {
        requireAuthentication();
        requireNetwork();
    }

    @After
    public void removeRoles() throws Throwable
    {
        useSuperUser();

        executeNet(String.format("DROP ROLE IF EXISTS %s", role));
        executeNet(String.format("DROP ROLE IF EXISTS %s", user));
        executeNet(String.format("DROP ROLE IF EXISTS %s", otherUser));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testMultiplePermissionsInGrantRevokeList() throws Throwable
    {
        useSuperUser();

        String tab = createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");

        executeNet(String.format("CREATE ROLE %s", role));

        executeNet(String.format("GRANT SELECT ON TABLE %s.%s TO %s", KEYSPACE, tab, role));

        assertInvalidMessage("Resource type DataResource does not support the requested permissions: DESCRIBE",
                             String.format("GRANT DESCRIBE ON TABLE %s.%s TO %s", KEYSPACE, tab, role));

        executeNet(String.format("GRANT SELECT, MODIFY ON TABLE %s.%s TO %s", KEYSPACE, tab, role));

        assertInvalidMessage("Resource type DataResource does not support the requested permissions: DESCRIBE",
                             String.format("GRANT SELECT, DESCRIBE ON TABLE %s.%s TO %s", KEYSPACE, tab, role));

        IResource res = DataResource.table(KEYSPACE, tab);

        assertPermissions(String.format("LIST ALL PERMISSIONS ON TABLE %s.%s OF %s", KEYSPACE, tab, role),
                          role, res, SELECT, MODIFY);
        assertListPermissions(String.format("LIST SELECT ON TABLE %s.%s OF %s", KEYSPACE, tab, role),
                              role, res, SELECT);
        assertInvalidMessage("Resource type DataResource does not support the requested permissions: DESCRIBE",
                             String.format("LIST DESCRIBE ON TABLE %s.%s OF %s", KEYSPACE, tab, role));
        assertInvalidMessage("Resource type DataResource does not support the requested permissions: DESCRIBE",
                             String.format("LIST SELECT, DESCRIBE ON TABLE %s.%s OF %s", KEYSPACE, tab, role));

        executeNet(String.format("GRANT PERMISSIONS ON TABLE %s.%s TO %s", KEYSPACE, tab, role));

        assertPermissions(String.format("LIST ALL PERMISSIONS ON TABLE %s.%s OF %s", KEYSPACE, tab, role),
                          role, res,
                          ALTER, DROP, SELECT, MODIFY, AUTHORIZE, UPDATE, TRUNCATE);

        executeNet(String.format("REVOKE ALL PERMISSIONS ON TABLE %s.%s FROM %s", KEYSPACE, tab, role));

        assertPermissions(String.format("LIST ALL PERMISSIONS ON TABLE %s.%s OF %s", KEYSPACE, tab, role),
                          role, res // no permissions
        );
    }

    @Test
    public void testGrantRevokeAll() throws Throwable
    {
        useSuperUser();
        executeNet("CREATE USER user1 WITH PASSWORD 'drowssap'");
        executeNet("CREATE ROLE roleX");
        try
        {
            executeNet("GRANT CREATE ON ALL KEYSPACES TO user1");

            useUser("user1", "drowssap");
            executeNet("CREATE KEYSPACE ks_user1 WITH replication={'class': 'SimpleStrategy', 'replication_factor': 1}");
            executeNet("GRANT ALL ON KEYSPACE ks_user1 TO roleX");

            useSuperUser();
            // just interested _whether_ the LIST ALL PERMISSIONS works
            executeNet("LIST ALL PERMISSIONS ON KEYSPACE ks_user1 OF user1");
            executeNet("LIST ALL PERMISSIONS ON KEYSPACE ks_user1 OF roleX");

            useUser("user1", "drowssap");
            executeNet("REVOKE ALL ON KEYSPACE ks_user1 FROM roleX");

            useSuperUser();
            // just interested _whether_ the LIST ALL PERMISSIONS works
            executeNet("LIST ALL PERMISSIONS ON KEYSPACE ks_user1 OF user1");
            executeNet("LIST ALL PERMISSIONS ON KEYSPACE ks_user1 OF roleX");
        }
        finally
        {
            // Cleanup after test
            useSuperUser();
            executeNet("DROP ROLE user1");
            executeNet("DROP ROLE roleX");
        }
    }
    /**
     * Verify that the granted permissions do not contain any deprecated permissions.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testGrantRevokeAllPermissions() throws Throwable
    {
        String tab = createTable(KEYSPACE_PER_TEST, "CREATE TABLE %s (id int PRIMARY KEY, val text)");

        useSuperUser();

        executeNet(String.format("CREATE ROLE %s WITH LOGIN = TRUE AND password='%s'", user, pass));
        executeNet(String.format("CREATE ROLE %s", role));

        IResource ksRes = DataResource.keyspace(KEYSPACE_PER_TEST);
        IResource tabRes = DataResource.table(KEYSPACE_PER_TEST, tab);
        IResource roleRes = RoleResource.role(role);

        // assume this works
        executeNet(String.format("REVOKE ALL ON KEYSPACE %s FROM cassandra", KEYSPACE_PER_TEST));
        executeNet(String.format("REVOKE ALL ON TABLE %s.%s FROM cassandra", KEYSPACE_PER_TEST, tab));
        executeNet(String.format("REVOKE ALL ON ROLE %s FROM cassandra", user));
        executeNet(String.format("REVOKE ALL ON ROLE %s FROM cassandra", role));

        // for keyspace

        executeNet(String.format("GRANT ALL ON KEYSPACE %s TO %s", KEYSPACE_PER_TEST, user));
        assertListPermissionsPermutations(String.format("ON KEYSPACE %s", KEYSPACE_PER_TEST),
                                          user, ksRes, CREATE, ALTER, DROP, SELECT, AUTHORIZE, DESCRIBE, UPDATE, TRUNCATE);

        executeNet(String.format("GRANT MODIFY ON KEYSPACE %s TO %s", KEYSPACE_PER_TEST, user));
        assertListPermissionsPermutations(String.format("ON KEYSPACE %s", KEYSPACE_PER_TEST),
                                          user, ksRes, CREATE, ALTER, DROP, SELECT, AUTHORIZE, DESCRIBE, UPDATE, TRUNCATE, MODIFY);

        executeNet(String.format("REVOKE ALL ON KEYSPACE %s FROM %s", KEYSPACE_PER_TEST, user));
        assertListPermissionsPermutations(String.format("ON KEYSPACE %s", KEYSPACE_PER_TEST),
                                          user, ksRes);

        // for table

        executeNet(String.format("GRANT ALL ON TABLE %s.%s TO %s", KEYSPACE_PER_TEST, tab, user));
        assertListPermissionsPermutations(String.format("ON TABLE %s.%s", KEYSPACE_PER_TEST, tab),
                                          user, tabRes, ALTER, DROP, SELECT, AUTHORIZE, UPDATE, TRUNCATE);

        executeNet(String.format("GRANT MODIFY ON TABLE %s.%s TO %s", KEYSPACE_PER_TEST, tab, user));
        assertListPermissionsPermutations(String.format("ON TABLE %s.%s", KEYSPACE_PER_TEST, tab),
                                          user, tabRes, ALTER, DROP, SELECT, AUTHORIZE, UPDATE, TRUNCATE, MODIFY);

        executeNet(String.format("REVOKE ALL ON TABLE %s.%s FROM %s", KEYSPACE_PER_TEST, tab, user));
        assertListPermissionsPermutations(String.format("ON TABLE %s.%s", KEYSPACE_PER_TEST, tab),
                                          user, tabRes);

        // for role

        executeNet(String.format("GRANT ALL ON ROLE %s TO %s", role, user));
        assertListPermissionsPermutations(String.format("ON ROLE %s", role),
                                          user, roleRes, ALTER, DROP, AUTHORIZE);

        executeNet(String.format("REVOKE ALL ON ROLE %s FROM %s", role, user));
        assertListPermissionsPermutations(String.format("ON ROLE %s", role),
                                          user, roleRes);
    }

    /**
     * Verify that the granted permissions do not contain any deprecated permissions.
     */
    @Test
    public void testGrantedPermissions() throws Throwable
    {
        useSuperUser();

        executeNet(String.format("CREATE ROLE %s WITH LOGIN = TRUE AND password='%s'", user, pass));
        executeNet(String.format("GRANT CREATE ON ALL KEYSPACES TO %s", user));

        useUser(user, pass);

        String ks = "my_ks";
        String tab = "my_tab";
        String f = "my_func";
        String a = "my_aggr";

        executeNet(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", ks));
        executeNet(String.format("CREATE TABLE %s.%s (pk int, ck int, val int, PRIMARY KEY (pk, ck))", ks, tab));
        executeNet(String.format("CREATE OR REPLACE FUNCTION %s.%s(state int, val int) " +
                                 "CALLED ON NULL INPUT " +
                                 "RETURNS int " +
                                 "LANGUAGE java " +
                                 "AS 'return 0;'", ks, f));
        executeNet(String.format("CREATE AGGREGATE %s.%s(int) " +
                                 "SFUNC " + shortFunctionName(f) + " " +
                                 "STYPE int", ks, a));

        useSuperUser();

        executeNet(String.format("REVOKE CREATE ON ALL KEYSPACES FROM %s", user));

        useSuperUser();

        DataResource ksResource = DataResource.keyspace(ks);
        assertPermissions(String.format("LIST ALL PERMISSIONS ON KEYSPACE %s OF %s", ks, user), user, ksResource,
                          ksResource.applicablePermissions().stream().filter(p->!p.deprecated()).toArray(Permission[]::new));

        DataResource tabResource = DataResource.table(ks, tab);
        assertPermissions(String.format("LIST ALL PERMISSIONS ON TABLE %s.%s OF %s NORECURSIVE", ks, tab, user), user, tabResource,
                          tabResource.applicablePermissions().stream().filter(p -> !p.deprecated()).toArray(Permission[]::new));

        FunctionResource fResource = FunctionResource.function(ks, shortFunctionName(f), Arrays.asList(Int32Type.instance, Int32Type.instance));
        assertPermissions(String.format("LIST ALL PERMISSIONS ON FUNCTION %s.%s(int,int) OF %s NORECURSIVE", ks, shortFunctionName(f), user), user, fResource,
                          fResource.applicablePermissions().stream().filter(p -> !p.deprecated()).toArray(Permission[]::new));

        FunctionResource aResource = FunctionResource.function(ks, shortFunctionName(a), Collections.singletonList(Int32Type.instance));
        assertPermissions(String.format("LIST ALL PERMISSIONS ON FUNCTION %s.%s(int) OF %s NORECURSIVE", ks, shortFunctionName(a), user), user, aResource,
                          aResource.applicablePermissions().stream().filter(p -> !p.deprecated()).toArray(Permission[]::new));

        executeNet(String.format("CREATE ROLE %s WITH LOGIN = TRUE AND password='%s'", otherUser, pass));

        executeNet(String.format("GRANT ALL ON KEYSPACE %s TO %s", ks, otherUser));

        assertPermissions(String.format("LIST ALL PERMISSIONS ON KEYSPACE %s OF %s", ks, otherUser), otherUser, ksResource,
                          ksResource.applicablePermissions().stream().filter(p -> !p.deprecated()).toArray(Permission[]::new));
    }

    /**
     * Migration scenario from {@link Permission#MODIFY} to {@link Permission#UPDATE} + {@link Permission#TRUNCATE}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testMigrationFromModifyToUpdateAndTruncatePermissionsWorks() throws Throwable
    {
        useSuperUser();

        String tab = createTable("CREATE TABLE %s (pk int, ck int, val text, PRIMARY KEY (pk, ck))");

        executeNet(String.format("CREATE ROLE %s WITH LOGIN = TRUE AND password='%s'", user, pass));

        ResultSet rs = executeNet(String.format("GRANT SELECT, MODIFY ON TABLE %s.%s TO %s", KEYSPACE, tab, user));
        ExecutionInfo ei = rs.getExecutionInfo();

        assertTrue(ei.getWarnings().stream().anyMatch(("The permission MODIFY is deprecated and has been replaced " +
                                                       "with the UPDATE, TRUNCATE permissions. Please migrate to the new permission(s).")::equals));

        // User as SELECT + MODIFY permissions. Can read, write and truncate.

        IResource res = DataResource.table(KEYSPACE, tab);
        assertPermissions(String.format("LIST ALL PERMISSIONS ON TABLE %s.%s OF %s", KEYSPACE, tab, user),
                          user, res, SELECT, MODIFY);

        useUser(user, pass);

        executeNet(String.format("TRUNCATE TABLE %s.%s", KEYSPACE, tab));

        executeNet(String.format("INSERT INTO %s.%s (pk, ck, val) VALUES (1, 1, 'one')", KEYSPACE, tab));
        executeNet(String.format("INSERT INTO %s.%s (pk, ck, val) VALUES (1, 2, 'two')", KEYSPACE, tab));
        executeNet(String.format("INSERT INTO %s.%s (pk, ck, val) VALUES (1, 3, 'three')", KEYSPACE, tab));

        assertRowsNet(executeNet(String.format("SELECT * FROM %s.%s WHERE pk = 1", KEYSPACE, tab)),
                      new Object[]{1, 1, "one"},
                      new Object[]{1, 2, "two"},
                      new Object[]{1, 3, "three"});

        executeNet(String.format("UPDATE %s.%s SET val = 'eins' WHERE pk = 1 AND ck = 1", KEYSPACE, tab));
        executeNet(String.format("UPDATE %s.%s SET val = 'zwei' WHERE pk = 1 AND ck = 2", KEYSPACE, tab));
        executeNet(String.format("UPDATE %s.%s SET val = 'drei' WHERE pk = 1 AND ck = 3", KEYSPACE, tab));

        assertRowsNet(executeNet(String.format("SELECT * FROM %s.%s WHERE pk = 1", KEYSPACE, tab)),
                      new Object[]{1, 1, "eins"},
                      new Object[]{1, 2, "zwei"},
                      new Object[]{1, 3, "drei"});

        useSuperUser();

        // Grant UPDATE permission and revoke MODIFY

        executeNet(String.format("GRANT UPDATE ON TABLE %s.%s TO %s", KEYSPACE, tab, user));

        executeNet(String.format("REVOKE MODIFY ON TABLE %s.%s FROM %s", KEYSPACE, tab, user));

        // User has SELECT + UPDATE permissions. Can read, write but not truncate.

        assertPermissions(String.format("LIST ALL PERMISSIONS ON TABLE %s.%s OF %s", KEYSPACE, tab, user), user, res, SELECT, UPDATE);

        useUser(user, pass);

        executeNet(String.format("UPDATE %s.%s SET val = 'un' WHERE pk = 1 AND ck = 1", KEYSPACE, tab));
        executeNet(String.format("UPDATE %s.%s SET val = 'deux' WHERE pk = 1 AND ck = 2", KEYSPACE, tab));
        executeNet(String.format("UPDATE %s.%s SET val = 'trois' WHERE pk = 1 AND ck = 3", KEYSPACE, tab));

        assertRowsNet(executeNet(String.format("SELECT * FROM %s.%s WHERE pk = 1", KEYSPACE, tab)),
                      new Object[]{1, 1, "un"},
                      new Object[]{1, 2, "deux"},
                      new Object[]{1, 3, "trois"});

        assertUnauthorizedQuery(
                String.format("User %s has no TRUNCATE permission on <table %s.%s> or any of its parents", user, KEYSPACE, tab),
                String.format("TRUNCATE TABLE %s.%s", KEYSPACE, tab));

        useSuperUser();

        // Also grant the TRUNCATE permission

        executeNet(String.format("GRANT TRUNCATE ON TABLE %s.%s TO %s", KEYSPACE, tab, user));

        // User has SELECT + UPDATE + TRUNCATE permissions. Can read, write and truncate.

        assertPermissions(String.format("LIST ALL PERMISSIONS ON TABLE %s.%s OF %s", KEYSPACE, tab, user),
                          user, res, SELECT, UPDATE, TRUNCATE);

        useUser(user, pass);

        executeNet(String.format("TRUNCATE TABLE %s.%s", KEYSPACE, tab));
    }

    private void assertListPermissionsPermutations(String onResource,
                                                   String role, IResource res, Permission... permissions) throws Throwable
    {
        String dcl = "LIST ALL PERMISSIONS ";
        String ofRole = String.format(" OF %s", role);
        assertPermissions(dcl,
                          role, res, permissions);
        assertPermissions(dcl + onResource,
                          role, res, permissions);
        assertPermissions(dcl + ofRole,
                          role, res, permissions);
        assertPermissions(dcl + onResource + ofRole,
                          role, res, permissions);
    }

    private void assertPermissions(String dcl, String user, IResource res, Permission... permissions) throws Throwable
    {
        assertListPermissions(dcl, user, res, permissions);

        // query system_auth.role_permissions

        // only works for Permission
        String expected = Arrays.stream(permissions)
                                .map(p -> Arrays.asList(user, user, res.getName(), p.name(), true, false, false))
                                .sorted(Comparator.comparing(Object::toString))
                                .map(List::toString)
                                .collect(Collectors.joining(",\n ", "\n", "\n"));

        String rows = executeNet(String.format("SELECT role, resource, permissions FROM %s.%s WHERE role='%s' AND resource='%s'",
                                               SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS, user, res.getName()))
                .all()
                .stream()
                .flatMap(r -> r.getSet(2, String.class).stream()
                               .map(p -> Arrays.asList(r.getString(0), r.getString(0), r.getString(1), p, true, false, false)))
                .sorted(Comparator.comparing(Object::toString))
                .map(List::toString)
                .collect(Collectors.joining(",\n ", "\n", "\n"));

        assertEquals("Permissions in table " + Arrays.toString(permissions) + " for '" + user + "' for resource '" + res.getName() + '\'',
                     expected, rows);
    }

    private void assertListPermissions(String dcl, String user, IResource res, Permission... permissions) throws Throwable
    {
        // query LIST PERMISSIONS

        // only works for Permission
        String expected = Arrays.stream(permissions)
                                .map(p -> Arrays.asList(user, user, res.toString(), p.name(), true, false, false))
                                .sorted(Comparator.comparing(Object::toString))
                                .map(List::toString)
                                .collect(Collectors.joining(",\n ", "\n", "\n"));

        ResultSet rs = executeNet(dcl);
        String result = rs.all().stream()
                          .map(r -> Arrays.asList(r.getString(0), r.getString(1), r.getString(2), r.getString(3), r.getBool(4), r.getBool(5), r.getBool(6)))
                          .sorted(Comparator.comparing(Object::toString))
                          .map(List::toString)
                          .collect(Collectors.joining(",\n ", "\n", "\n"));

        assertEquals("Permissions via DCL '" + dcl + "' " + Arrays.toString(permissions) + " for '" + user + "' for resource '" + res + '\'',
                     expected, result);
    }
}
