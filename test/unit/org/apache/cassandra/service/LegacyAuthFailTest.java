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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Joiner;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;
import org.slf4j.helpers.NOPLogger;

import org.apache.cassandra.ForwardingLogger;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.SchemaConstants;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LegacyAuthFailTest extends CQLTester
{
    @Test
    public void testLegacyTablesCheck() throws Throwable
    {
        createKeyspace();

        // no legacy tables found
        MockLogger l = new MockLogger();
        StartupChecks.checkLegacyAuthTables.execute(l);
        assertTrue(l.warnings.isEmpty());

        FailureTester tester = (tables) ->
        {
            MockLogger logger = new MockLogger();
            StartupChecks.checkLegacyAuthTables.execute(logger);
            assertEquals(1, logger.warnings.size());
            String msg = logger.warnings.get(0);
            String expectedMsg = String.format("Legacy auth tables %s in keyspace "
                                               + SchemaConstants.AUTH_KEYSPACE_NAME
                                               + " still exist and have not been properly migrated.",
                                               Joiner.on(", ").join(tables));
            assertEquals(expectedMsg, msg);
        };

        testCheckFailure(new ArrayList<>(SchemaConstants.LEGACY_AUTH_TABLES), tester);
    }

    @Test
    public void testObsoleteTablesCheck() throws Throwable
    {
        createKeyspace();

        // no obsolete tables found
        MockLogger l = new MockLogger();
        StartupChecks.checkObsoleteAuthTables.execute(l);
        assertTrue(l.warnings.isEmpty());

        FailureTester tester = (tables) ->
        {
            MockLogger logger = new MockLogger();
            StartupChecks.checkObsoleteAuthTables.execute(logger);
            assertEquals(1, logger.warnings.size());
            String msg = logger.warnings.get(0);
            String expectedMsg = String.format("Auth tables %s in keyspace "
                                               + SchemaConstants.AUTH_KEYSPACE_NAME
                                               + " exist but can safely be dropped.",
                                               Joiner.on(", ").join(tables));
            assertEquals(expectedMsg, msg);
        };

        testCheckFailure(new ArrayList<>(SchemaConstants.OBSOLETE_AUTH_TABLES), tester);
    }

    public void testCheckFailure(List<String> tables, FailureTester tester) throws Throwable
    {
        // test reporting for individual tables
        for (String table : tables)
        {
            createLegacyTable(table);
            tester.executeCheckAndValidateOutput(table);
            dropLegacyTable(table);
        }

        // test reporting of multiple existing tables
        for (String legacyTable : tables)
            createLegacyTable(legacyTable);

        while (!tables.isEmpty())
        {
            tester.executeCheckAndValidateOutput(tables);
            dropLegacyTable(tables.remove(0));
        }
    }

    private void dropLegacyTable(String table) throws Throwable
    {
        execute(format("DROP TABLE %s.%s", SchemaConstants.AUTH_KEYSPACE_NAME, table));
    }

    private void createLegacyTable(String table) throws Throwable
    {
        execute(format("CREATE TABLE %s.%s (id int PRIMARY KEY, val text)", SchemaConstants.AUTH_KEYSPACE_NAME, table));
    }

    private void createKeyspace() throws Throwable
    {
        execute(format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", SchemaConstants.AUTH_KEYSPACE_NAME));
    }

    private interface FailureTester
    {
        default void executeCheckAndValidateOutput(String legacyTable) throws Exception
        {
            executeCheckAndValidateOutput(Collections.singletonList(legacyTable));
        }

        void executeCheckAndValidateOutput(List<String> legacyTables) throws Exception;
    }

    /**
     * Mock Logger used to capture the warnings written by the checks
     */
    private static class MockLogger extends ForwardingLogger
    {
        public List<String> warnings = new ArrayList<>();

        public MockLogger()
        {
        }

        @Override
        public void warn(String format, Object arg1, Object arg2)
        {
            warnings.add(MessageFormatter.format(format, arg1, arg2).getMessage());
            super.warn(format, arg1, arg2);
        }

        @Override
        protected Logger delegate()
        {
            return NOPLogger.NOP_LOGGER;
        }
    }
}
