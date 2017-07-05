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

import com.datastax.driver.core.*;
import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.*;

public class PreparedStatementsMetadataTest
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
    public void testListStatementsV4()
    {
        testListStatements(ProtocolVersion.V4, false);
    }

    @Test
    public void testListStatementsV5()
    {
        testListStatements(ProtocolVersion.V5, true);
    }

    @Test
    public void testCasStatementsV4()
    {
        testCasStatements(ProtocolVersion.V4, false);
    }

    @Test
    public void testCasStatementsV5()
    {
        testCasStatements(ProtocolVersion.V5, true);
    }

    private void testListStatements(ProtocolVersion protocolVersion, boolean beta)
    {
        Cluster.Builder builder = Cluster.builder()
                                         .withoutJMXReporting()
                                         .addContactPoints(CQLTester.nativeAddr)
                                         .withClusterName("Test Cluster")
                                         .withPort(CQLTester.nativePort)
                                         .withCredentials("cassandra", "cassandra");
        if (beta)
            builder.allowBetaProtocolVersion();
        else
            builder.withProtocolVersion(protocolVersion);

        try (Cluster cluster = builder.build())
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

    private void testCasStatements(ProtocolVersion protocolVersion, boolean beta)
    {
        Cluster.Builder builder = Cluster.builder()
                                         .withoutJMXReporting()
                                         .addContactPoints(CQLTester.nativeAddr)
                                         .withClusterName("Test Cluster")
                                         .withPort(CQLTester.nativePort)
                                         .withCredentials("cassandra", "cassandra");
        if (beta)
            builder.allowBetaProtocolVersion();
        else
            builder.withProtocolVersion(protocolVersion);

        try (Cluster cluster = builder.build())
        {
            try (Session session = cluster.connect())
            {
                session.execute("CREATE KEYSPACE IF NOT EXISTS ks WITH replication={'class': 'SimpleStrategy', 'replication_factor': '1'}");
                session.execute("CREATE TABLE IF NOT EXISTS ks.tab (id int PRIMARY KEY, a int, b int, c int)");
                session.execute("TRUNCATE TABLE ks.tab");

                PreparedStatement pstmtInsert = session.prepare("INSERT INTO ks.tab (id, a, b, c) VALUES (?, ?, ?, ?) IF NOT EXISTS");
                PreparedStatement pstmtUpdate = session.prepare("UPDATE ks.tab SET b = ? WHERE id = ? IF b = ?");
                PreparedStatement pstmtDelete = session.prepare("DELETE FROM ks.tab WHERE id = ? IF EXISTS");

                ResultSet rset = session.execute(pstmtInsert.bind(1, 1, 1, 1));
                Row row = rset.one();
                assertTrue(row.getBool("[applied]"));
                rset = session.execute(pstmtInsert.bind(2, 2, 2, 2));
                row = rset.one();
                assertTrue(row.getBool("[applied]"));

                rset = session.execute(pstmtUpdate.bind(2, 1, 1));
                assertTrue(rset.one().getBool("[applied]"));

                rset = session.execute(pstmtUpdate.bind(2, 1, 1));
                row = rset.one();
                assertFalse(row.getBool("[applied]"));
                assertEquals(2, row.getInt("b"));

                rset = session.execute(pstmtDelete.bind(3));
                row = rset.one();
                assertFalse(row.getBool("[applied]"));
                rset = session.execute(pstmtDelete.bind(2));
                row = rset.one();
                assertTrue(row.getBool("[applied]"));
            }
        }
    }
}
