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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

public class GrantAndRevokeTest extends CQLTester
{
    @BeforeClass
    public static void setup()
    {
        requireAuthentication();
        requireNetwork();
    }

    @Test
    public void testMultiplePermissionsInGrantRevokeList() throws Throwable
    {
        useSuperUser();

        String tab = createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");

        String role = "role1";
        executeNet(String.format("CREATE ROLE %s", role));

        executeNet(String.format("GRANT SELECT ON TABLE %s.%s TO %s", KEYSPACE, tab, role));

        assertInvalidMessage("Resource type DataResource does not support the requested permissions: DESCRIBE",
                             String.format("GRANT DESCRIBE ON TABLE %s.%s TO %s", KEYSPACE, tab, role));

        executeNet(String.format("GRANT SELECT, MODIFY ON TABLE %s.%s TO %s", KEYSPACE, tab, role));

        assertInvalidMessage("Resource type DataResource does not support the requested permissions: DESCRIBE",
                             String.format("GRANT SELECT, DESCRIBE ON TABLE %s.%s TO %s", KEYSPACE, tab, role));

        String res = DataResource.table(KEYSPACE, tab).toString();

        assertRowsNet(executeNet(String.format("LIST ALL PERMISSIONS ON TABLE %s.%s OF %s", KEYSPACE, tab, role)),
                new Object[]{ role, role, res, "SELECT", true, false, false},
                new Object[]{ role, role, res, "MODIFY", true, false, false}
                );
        assertRowsNet(executeNet(String.format("LIST SELECT ON TABLE %s.%s OF %s", KEYSPACE, tab, role)),
                new Object[]{ role, role, res, "SELECT", true, false, false}
                );
        assertInvalidMessage("Resource type DataResource does not support the requested permissions: DESCRIBE",
                             String.format("LIST DESCRIBE ON TABLE %s.%s OF %s", KEYSPACE, tab, role));
        assertInvalidMessage("Resource type DataResource does not support the requested permissions: DESCRIBE",
                             String.format("LIST SELECT, DESCRIBE ON TABLE %s.%s OF %s", KEYSPACE, tab, role));

        executeNet(String.format("GRANT PERMISSIONS ON TABLE %s.%s TO %s", KEYSPACE, tab, role));

        assertRowsNet(executeNet(String.format("LIST ALL PERMISSIONS ON TABLE %s.%s OF %s", KEYSPACE, tab, role)),
                new Object[]{ role, role, res, "ALTER", true, false, false},
                new Object[]{ role, role, res, "DROP", true, false, false},
                new Object[]{ role, role, res, "SELECT", true, false, false},
                new Object[]{ role, role, res, "MODIFY", true, false, false},
                new Object[]{ role, role, res, "AUTHORIZE", true, false, false}
        );

        executeNet(String.format("REVOKE ALL PERMISSIONS ON TABLE %s.%s FROM %s", KEYSPACE, tab, role));

        assertRowsNet(executeNet(String.format("LIST ALL PERMISSIONS ON TABLE %s.%s OF %s", KEYSPACE, tab, role))
                // empty
        );
    }

    @Test
    public void testGrantRevokeAll() throws Throwable
    {
        useSuperUser();
        executeNet("CREATE USER user1 WITH PASSWORD 'drowssap'");
        executeNet("CREATE ROLE roleX");
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
}
