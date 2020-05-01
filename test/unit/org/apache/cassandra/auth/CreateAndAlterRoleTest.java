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

import static org.mindrot.jbcrypt.BCrypt.gensalt;
import static org.mindrot.jbcrypt.BCrypt.hashpw;

public class CreateAndAlterRoleTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
        requireAuthentication();
        requireNetwork();
    }

    @Test
    public void nestedCreateRole() throws Throwable
    {
        String mainRole = "nested_main";
        String subRole = "nested_sub";
        String otherRole = "nested_other";
        String nopeRole = "nested_nope";
        String password = "secret";

        useSuperUser();

        executeNet(String.format("CREATE ROLE %s WITH login=true AND password='%s'", mainRole, password));
        executeNet(String.format("GRANT CREATE ON ALL ROLES TO %s", mainRole));
        executeNet(String.format("CREATE ROLE %s WITH login=true AND password='%s'", otherRole, password));

        executeNet(String.format("LIST ALL PERMISSIONS OF %s", mainRole));
        executeNet(String.format("LIST ALL PERMISSIONS OF %s", otherRole));

        useUser(mainRole, password);

        executeNet(String.format("CREATE ROLE %s WITH login = true AND password='%s'", subRole, password));

        executeNet(String.format("LIST ALL PERMISSIONS OF %s", mainRole));
        executeNet(String.format("LIST ALL PERMISSIONS OF %s", subRole));
        assertInvalidMessageNet(String.format("You are not authorized to view %s's permissions", otherRole),
                                String.format("LIST ALL PERMISSIONS OF %s", otherRole));

        useUser(subRole, password);

        assertInvalidMessageNet(String.format("You are not authorized to view %s's permissions", mainRole),
                                String.format("LIST ALL PERMISSIONS OF %s", mainRole));
        executeNet(String.format("LIST ALL PERMISSIONS OF %s", subRole));
        assertInvalidMessageNet(String.format("You are not authorized to view %s's permissions", otherRole),
                                String.format("LIST ALL PERMISSIONS OF %s", otherRole));
        assertInvalidMessageNet(String.format("User %s does not have sufficient privileges to perform the requested operation", subRole),
                                String.format("CREATE ROLE %s WITH login=true AND password='%s'", nopeRole, password));

        useUser(mainRole, password);

        executeNet(String.format("ALTER ROLE %s WITH login = false", subRole));
        executeNet(String.format("DROP ROLE %s", subRole));
    }

    @Test
    public void createAlterRoleWithHashedPassword() throws Throwable
    {
        String username = "hashed_pw_role";
        String usernamePlain = "pw_role";
        String plaintextPassword = "super_secret_thing";
        String otherPassword = "much_safer_password";
        String hashedPassword = hashpw(plaintextPassword, gensalt(4));
        String hashedOtherPassword = hashpw(otherPassword, gensalt(4));

        useSuperUser();

        assertInvalidMessage("Invalid hashed password value",
                             String.format("CREATE ROLE %s WITH login=true AND hashed password='%s'",
                                           username, "this_is_an_invalid_hash"));
        assertInvalidMessage("Options 'password' and 'hashed password' are mutually exclusive",
                             String.format("CREATE ROLE %s WITH login=true AND password='%s' AND hashed password='%s'",
                                           username, plaintextPassword, hashedPassword));
        executeNet(String.format("CREATE ROLE %s WITH login=true AND hashed password='%s'",
                                 username, hashedPassword));
        executeNet(String.format("CREATE ROLE %s WITH login=true AND password='%s'",
                                 usernamePlain, plaintextPassword));

        useUser(username, plaintextPassword);

        executeNet("SELECT key FROM system.local");

        useUser(usernamePlain, plaintextPassword);

        executeNet("SELECT key FROM system.local");

        useSuperUser();

        assertInvalidMessage("Options 'password' and 'hashed password' are mutually exclusive",
                             String.format("ALTER ROLE %s WITH password='%s' AND hashed password='%s'",
                                           username, otherPassword, hashedOtherPassword));
        executeNet(String.format("ALTER ROLE %s WITH password='%s'",
                                 username, otherPassword));
        executeNet(String.format("ALTER ROLE %s WITH hashed password='%s'",
                                 usernamePlain, hashedOtherPassword));

        useUser(username, otherPassword);

        executeNet("SELECT key FROM system.local");

        useUser(usernamePlain, otherPassword);

        executeNet("SELECT key FROM system.local");
    }

    @Test
    public void createAlterUserWithHashedPassword() throws Throwable
    {
        String username = "hashed_pw_user";
        String usernamePlain = "pw_user";
        String plaintextPassword = "super_secret_thing";
        String otherPassword = "much_safer_password";
        String hashedPassword = hashpw(plaintextPassword, gensalt(4));
        String hashedOtherPassword = hashpw(otherPassword, gensalt(4));

        useSuperUser();

        assertInvalidMessage("Invalid hashed password value",
                             String.format("CREATE USER %s WITH hashed password '%s'",
                                           username, "this_is_an_invalid_hash"));
        executeNet(String.format("CREATE USER %s WITH hashed password '%s'",
                                 username, hashedPassword));
        executeNet(String.format("CREATE USER %s WITH password '%s'",
                                 usernamePlain, plaintextPassword));

        useUser(username, plaintextPassword);

        executeNet("SELECT key FROM system.local");

        useUser(usernamePlain, plaintextPassword);

        executeNet("SELECT key FROM system.local");

        useSuperUser();

        executeNet(String.format("ALTER USER %s WITH password '%s'",
                                 username, otherPassword));
        executeNet(String.format("ALTER USER %s WITH hashed password '%s'",
                                 usernamePlain, hashedOtherPassword));

        useUser(username, otherPassword);

        executeNet("SELECT key FROM system.local");

        useUser(usernamePlain, otherPassword);

        executeNet("SELECT key FROM system.local");
    }
}
