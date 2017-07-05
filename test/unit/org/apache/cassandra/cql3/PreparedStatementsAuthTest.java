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

import java.lang.reflect.Field;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.auth.AuthConfig;
import org.apache.cassandra.auth.CassandraAuthorizer;
import org.apache.cassandra.auth.CassandraRoleManager;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.fail;

public class PreparedStatementsAuthTest
{
    @BeforeClass
    public static void setup()
    {
        CQLTester.setUpClass();

        requireAuthentication();
        AuthConfig.applyAuth();

        CQLTester.startNetworking();
    }

    @AfterClass
    public static void tearDownClass()
    {
        CQLTester.tearDownClass();
    }

    private static void requireAuthentication()
    {
        setDDField("authenticator", new PasswordAuthenticator());
        setDDField("authorizer", new CassandraAuthorizer());
        setDDField("roleManager", new CassandraRoleManager());

        System.setProperty("cassandra.superuser_setup_delay_ms", "0");
    }

    private static void setDDField(String fieldName, Object value)
    {
        Field field = FBUtilities.getProtectedField(DatabaseDescriptor.class, fieldName);
        try
        {
            field.set(null, value);
        }
        catch (IllegalAccessException e)
        {
            fail("Error setting " + field.getType().getSimpleName() + " instance for test");
        }
    }

    @Test
    public void testListStatements()
    {
        try (Cluster cluster = Cluster.builder()
                                      .withoutJMXReporting()
                                      .addContactPoints(CQLTester.nativeAddr)
                                      .withClusterName("Test Cluster")
                                      .withPort(CQLTester.nativePort)
                                      .withCredentials("cassandra", "cassandra")
                                      .allowBetaProtocolVersion()
                                      .build())
        {
            try (Session session = cluster.connect())
            {
                session.execute("LIST ALL PERMISSIONS").all();
                session.execute("LIST ROLES").all();
                session.execute("LIST USERS").all();

                PreparedStatement pstmt = session.prepare("LIST ALL PERMISSIONS");
                session.execute(pstmt.bind()).all();

                pstmt = session.prepare("LIST ROLES");
                session.execute(pstmt.bind()).all();

                pstmt = session.prepare("LIST USERS");
                session.execute(pstmt.bind()).all();
            }
        }
    }
}
