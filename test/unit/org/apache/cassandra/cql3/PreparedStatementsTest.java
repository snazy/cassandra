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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.SyntaxError;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.transport.ProtocolVersion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PreparedStatementsTest extends CQLTester
{
    private static final String KEYSPACE = "prepared_stmt_cleanup";
    private static final String createKsStatement = "CREATE KEYSPACE " + KEYSPACE +
                                                    " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
    private static final String dropKsStatement = "DROP KEYSPACE IF EXISTS " + KEYSPACE;

    @Before
    public void setup()
    {
        requireNetwork();
    }

    @Test
    public void testInvalidatePreparedStatementsOnDrop()
    {
        Session session = sessions.get(ProtocolVersion.V5);
        session.execute(dropKsStatement);
        session.execute(createKsStatement);

        String createTableStatement = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".qp_cleanup (id int PRIMARY KEY, cid int, val text);";
        String dropTableStatement = "DROP TABLE IF EXISTS " + KEYSPACE + ".qp_cleanup;";

        session.execute(createTableStatement);

        PreparedStatement prepared = session.prepare("INSERT INTO " + KEYSPACE + ".qp_cleanup (id, cid, val) VALUES (?, ?, ?)");
        PreparedStatement preparedBatch = session.prepare("BEGIN BATCH " +
                                                          "INSERT INTO " + KEYSPACE + ".qp_cleanup (id, cid, val) VALUES (?, ?, ?);" +
                                                          "APPLY BATCH;");
        session.execute(dropTableStatement);
        session.execute(createTableStatement);
        session.execute(prepared.bind(1, 1, "value"));
        session.execute(preparedBatch.bind(2, 2, "value2"));

        session.execute(dropKsStatement);
        session.execute(createKsStatement);
        session.execute(createTableStatement);

        // The driver will get a response about the prepared statement being invalid, causing it to transparently
        // re-prepare the statement.  We'll rely on the fact that we get no errors while executing this to show that
        // the statements have been invalidated.
        session.execute(prepared.bind(1, 1, "value"));
        session.execute(preparedBatch.bind(2, 2, "value2"));
        session.execute(dropKsStatement);
    }

    @Test
    public void testInvalidatePreparedStatementOnAlterV5() throws Throwable
    {
        testInvalidatePreparedStatementOnAlter(ProtocolVersion.V5, true);
    }

    @Test
    public void testInvalidatePreparedStatementOnAlterV4() throws Throwable
    {
        testInvalidatePreparedStatementOnAlter(ProtocolVersion.V4, false);
    }

    private void testInvalidatePreparedStatementOnAlter(ProtocolVersion version, boolean supportsMetadataChange) throws Throwable
    {
        requireNetwork();
        Session session = sessions.get(version);
        String createTableStatement = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".qp_cleanup (a int PRIMARY KEY, b int, c int);";
        String alterTableStatement = "ALTER TABLE " + KEYSPACE + ".qp_cleanup ADD d int;";

        session.execute(dropKsStatement);
        session.execute(createKsStatement);
        session.execute(createTableStatement);

        PreparedStatement preparedSelect = session.prepare("SELECT * FROM " + KEYSPACE + ".qp_cleanup");
        session.execute("INSERT INTO " + KEYSPACE + ".qp_cleanup (a, b, c) VALUES (?, ?, ?);",
                        1, 2, 3);
        session.execute("INSERT INTO " + KEYSPACE + ".qp_cleanup (a, b, c) VALUES (?, ?, ?);",
                        2, 3, 4);

        Iterator<Row> resultSet = session.execute(preparedSelect.bind()).all().iterator();
        Row row1 = resultSet.next();
        assertEquals(1, row1.getInt("a"));
        assertEquals(2, row1.getInt("b"));
        assertEquals(3, row1.getInt("c"));

        Row row2 = resultSet.next();
        assertEquals(2, row2.getInt("a"));
        assertEquals(3, row2.getInt("b"));
        assertEquals(4, row2.getInt("c"));

        ExecutorService executor = Executors.newFixedThreadPool(8);
        List<Future<Object>> futures = new ArrayList<>();
        AtomicBoolean stopIt = new AtomicBoolean();
        for (int i = 0; i < 8; i++)
        {
            futures.add(executor.submit(() -> {
                while (!stopIt.get())
                {
                    session.execute(preparedSelect.bind()).all();
                }
                return null;
            }));
        }

        session.execute(alterTableStatement);

        Thread.sleep(500);
        stopIt.set(true);
        for (Future<Object> future : futures)
        {
            Uninterruptibles.getUninterruptibly(future, 500, TimeUnit.MILLISECONDS);
        }
        executor.shutdown();

        session.execute("INSERT INTO " + KEYSPACE + ".qp_cleanup (a, b, c, d) VALUES (?, ?, ?, ?);",
                        3, 4, 5, 6);
        resultSet = session.execute(preparedSelect.bind()).all().iterator();

        row1 = resultSet.next();
        assertEquals(1, row1.getInt("a"));
        assertEquals(2, row1.getInt("b"));
        assertEquals(3, row1.getInt("c"));
        if (supportsMetadataChange)
            assertEquals(0, row1.getInt("d"));

        row2 = resultSet.next();
        assertEquals(2, row2.getInt("a"));
        assertEquals(3, row2.getInt("b"));
        assertEquals(4, row2.getInt("c"));
        if (supportsMetadataChange)
            assertEquals(0, row2.getInt("d"));

        Row row3 = resultSet.next();
        assertEquals(3, row3.getInt("a"));
        assertEquals(4, row3.getInt("b"));
        assertEquals(5, row3.getInt("c"));
        if (supportsMetadataChange)
            assertEquals(6, row3.getInt("d"));
        session.execute(dropKsStatement);
    }

    @Test
    public void testInvalidatePreparedStatementOnAlterUnchangedMetadataV4() throws Throwable
    {
        testInvalidatePreparedStatementOnAlterUnchangedMetadata(ProtocolVersion.V4);
    }

    @Test
    public void testInvalidatePreparedStatementOnAlterUnchangedMetadataV5() throws Throwable
    {
        testInvalidatePreparedStatementOnAlterUnchangedMetadata(ProtocolVersion.V5);
    }

    private void testInvalidatePreparedStatementOnAlterUnchangedMetadata(ProtocolVersion version) throws Throwable
    {
        Session session = sessions.get(version);
        String createTableStatement = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".qp_cleanup (a int PRIMARY KEY, b int, c int);";
        String alterTableStatement = "ALTER TABLE " + KEYSPACE + ".qp_cleanup ADD d int;";

        session.execute(dropKsStatement);
        session.execute(createKsStatement);
        session.execute(createTableStatement);

        PreparedStatement preparedSelect = session.prepare("SELECT a, b, c FROM " + KEYSPACE + ".qp_cleanup");
        session.execute("INSERT INTO " + KEYSPACE + ".qp_cleanup (a, b, c) VALUES (?, ?, ?);",
                        1, 2, 3);
        session.execute("INSERT INTO " + KEYSPACE + ".qp_cleanup (a, b, c) VALUES (?, ?, ?);",
                        2, 3, 4);

        Iterator<Row> resultSet = session.execute(preparedSelect.bind()).all().iterator();
        Row row1 = resultSet.next();
        assertEquals(1, row1.getInt("a"));
        assertEquals(2, row1.getInt("b"));
        assertEquals(3, row1.getInt("c"));

        Row row2 = resultSet.next();
        assertEquals(2, row2.getInt("a"));
        assertEquals(3, row2.getInt("b"));
        assertEquals(4, row2.getInt("c"));

        ExecutorService executor = Executors.newFixedThreadPool(8);
        List<Future<Object>> futures = new ArrayList<>();
        AtomicBoolean stopIt = new AtomicBoolean();
        for (int i = 0; i < 8; i++)
        {
            futures.add(executor.submit(() -> {
                while (!stopIt.get())
                {
                    session.execute(preparedSelect.bind()).all();
                }
                return null;
            }));
        }

        session.execute(alterTableStatement);

        Thread.sleep(500);
        stopIt.set(true);
        for (Future<Object> future : futures)
        {
            Uninterruptibles.getUninterruptibly(future, 500, TimeUnit.MILLISECONDS);
        }
        executor.shutdown();

        session.execute("INSERT INTO " + KEYSPACE + ".qp_cleanup (a, b, c, d) VALUES (?, ?, ?, ?);",
                        3, 4, 5, 6);
        resultSet = session.execute(preparedSelect.bind()).all().iterator();

        row1 = resultSet.next();
        assertEquals(1, row1.getInt("a"));
        assertEquals(2, row1.getInt("b"));
        assertEquals(3, row1.getInt("c"));

        row2 = resultSet.next();
        assertEquals(2, row2.getInt("a"));
        assertEquals(3, row2.getInt("b"));
        assertEquals(4, row2.getInt("c"));

        Row row3 = resultSet.next();
        assertEquals(3, row3.getInt("a"));
        assertEquals(4, row3.getInt("b"));
        assertEquals(5, row3.getInt("c"));
        session.execute(dropKsStatement);
    }

    @Test
    public void testStatementRePreparationOnReconnect()
    {
        Session session = sessions.get(ProtocolVersion.V5);

        session.execute(dropKsStatement);
        session.execute(createKsStatement);

        session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".qp_test (id int PRIMARY KEY, cid int, val text);");

        String insertCQL = "INSERT INTO " + KEYSPACE + ".qp_test (id, cid, val) VALUES (?, ?, ?)";
        String selectCQL = "Select * from " + KEYSPACE + ".qp_test where id = ?";

        PreparedStatement preparedInsert = session.prepare(insertCQL);
        PreparedStatement preparedSelect = session.prepare(selectCQL);

        session.execute(preparedInsert.bind(1, 1, "value"));
        assertEquals(1, session.execute(preparedSelect.bind(1)).all().size());

        try (Cluster cluster = Cluster.builder()
                                 .addContactPoints(nativeAddr)
                                 .withClusterName("Test Cluster")
                                 .withPort(nativePort)
                                 .withoutJMXReporting()
                                 .allowBetaProtocolVersion()
                                 .build())
        {
            try(Session newSession = cluster.connect())
            {
                preparedInsert = newSession.prepare(insertCQL);
                preparedSelect = newSession.prepare(selectCQL);
                session.execute(preparedInsert.bind(1, 1, "value"));

                assertEquals(1, session.execute(preparedSelect.bind(1)).all().size());
            }
        }
    }

    @Test
    public void prepareAndExecuteWithCustomExpressions() throws Throwable
    {
        Session session = sessions.get(ProtocolVersion.V5);

        session.execute(dropKsStatement);
        session.execute(createKsStatement);
        String table = "custom_expr_test";
        String index = "custom_index";

        session.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (id int PRIMARY KEY, cid int, val text);",
                                      KEYSPACE, table));
        session.execute(String.format("CREATE CUSTOM INDEX %s ON %s.%s(val) USING '%s'",
                                      index, KEYSPACE, table, StubIndex.class.getName()));
        session.execute(String.format("INSERT INTO %s.%s(id, cid, val) VALUES (0, 0, 'test')", KEYSPACE, table));

        PreparedStatement prepared1 = session.prepare(String.format("SELECT * FROM %s.%s WHERE expr(%s, 'foo')",
                                                                    KEYSPACE, table, index));
        assertEquals(1, session.execute(prepared1.bind()).all().size());

        PreparedStatement prepared2 = session.prepare(String.format("SELECT * FROM %s.%s WHERE expr(%s, ?)",
                                                                    KEYSPACE, table, index));
        assertEquals(1, session.execute(prepared2.bind("foo bar baz")).all().size());

        try
        {
            session.prepare(String.format("SELECT * FROM %s.%s WHERE expr(?, 'foo bar baz')", KEYSPACE, table));
            fail("Expected syntax exception, but none was thrown");
        }
        catch(SyntaxError e)
        {
            assertEquals("Bind variables cannot be used for index names", e.getMessage());
        }
    }
}
