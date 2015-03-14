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
package org.apache.cassandra.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.MigrationListener;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.transport.messages.ResultMessage;

public class SchemaChangeTest extends CQLTester
{
    @Test
    public void testSchemaChangeResultAndEvent()
    {
        final List<UUID> migrationListenerVersion = new ArrayList<>();

        // implementation of MigrationListener simulates what org.apache.cassandra.transport.Server.EventNotifier
        // does to "broadcast" schema change events
        MigrationListener listener = new MigrationListener()
        {
            void updated()
            {
                migrationListenerVersion.add(Schema.instance.getVersion());
            }

            public void onCreateAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes)
            {
                updated();
            }

            public void onCreateColumnFamily(String ksName, String cfName)
            {
                updated();
            }

            public void onCreateFunction(String ksName, String functionName, List<AbstractType<?>> argTypes)
            {
                updated();
            }

            public void onCreateKeyspace(String ksName)
            {
                updated();
            }

            public void onCreateUserType(String ksName, String typeName)
            {
                updated();
            }

            public void onDropAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes)
            {
                updated();
            }

            public void onDropColumnFamily(String ksName, String cfName)
            {
                updated();
            }

            public void onDropFunction(String ksName, String functionName, List<AbstractType<?>> argTypes)
            {
                updated();
            }

            public void onDropKeyspace(String ksName)
            {
                updated();
            }

            public void onDropUserType(String ksName, String typeName)
            {
                updated();
            }

            public void onUpdateAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes)
            {
                updated();
            }

            public void onUpdateColumnFamily(String ksName, String cfName, boolean columnsDidChange)
            {
                updated();
            }

            public void onUpdateFunction(String ksName, String functionName, List<AbstractType<?>> argTypes)
            {
                updated();
            }

            public void onUpdateKeyspace(String ksName)
            {
                updated();
            }

            public void onUpdateUserType(String ksName, String typeName)
            {
                updated();
            }
        };

        MigrationManager.instance.register(listener);
        try
        {

            Schema.instance.updateVersion();
            UUID version = Schema.instance.getVersion();

            // CREATE TABLE
            version = assertSchemaVersion(version, migrationListenerVersion, "CREATE TABLE %s.atable (pk int PRIMARY KEY, v text)");
            // ALTER TABLE
            version = assertSchemaVersion(version, migrationListenerVersion, "ALTER TABLE %s.atable ADD addCol bigint");
            version = assertSchemaVersion(version, migrationListenerVersion, "ALTER TABLE %s.atable RENAME pk TO primKey");
            version = assertSchemaVersion(version, migrationListenerVersion, "ALTER TABLE %s.atable DROP addCol");
            version = assertSchemaVersion(version, migrationListenerVersion, "ALTER TABLE %s.atable WITH gc_grace_seconds=555");
            // DROP TABLE
            version = assertSchemaVersion(version, migrationListenerVersion, "DROP TABLE %s.atable");

            // CREATE TYPE
            version = assertSchemaVersion(version, migrationListenerVersion, "CREATE TYPE %s.atype (i int, t text)");
            // ALTER TYPE
            version = assertSchemaVersion(version, migrationListenerVersion, "ALTER TYPE %s.atype ADD addCol bigint");
            version = assertSchemaVersion(version, migrationListenerVersion, "ALTER TYPE %s.atype RENAME addCol TO col");
            // DROP TYPE
            version = assertSchemaVersion(version, migrationListenerVersion, "DROP TYPE %s.atype");

            // CREATE FUNCTION
            version = assertSchemaVersion(version, migrationListenerVersion, "CREATE FUNCTION %s.afunc(state double, val double) " +
                                                                             "RETURNS double " +
                                                                             "LANGUAGE javascript " +
                                                                             "AS '\"string\";'");
            // CREATE OR REPLACE FUNCTION
            version = assertSchemaVersion(version, migrationListenerVersion, "CREATE OR REPLACE FUNCTION %s.afunc(state double, val double) " +
                                                                             "RETURNS double " +
                                                                             "LANGUAGE javascript " +
                                                                             "AS '\"other string\";'");
            // DROP FUNCTION
            version = assertSchemaVersion(version, migrationListenerVersion, "DROP FUNCTION %s.afunc");

            // CREATE AGGREGATE
            version = assertSchemaVersion(version, migrationListenerVersion, "CREATE FUNCTION %s.afunc(state double, val double) " +
                                                                             "RETURNS double " +
                                                                             "LANGUAGE javascript " +
                                                                             "AS '\"string\";'");
            version = assertSchemaVersion(version, migrationListenerVersion, "CREATE AGGREGATE %s.aaggr(double) " +
                                                                             "SFUNC afunc " +
                                                                             "STYPE double");
            // CREATE OR REPLACE AGGREGATE
            version = assertSchemaVersion(version, migrationListenerVersion, "CREATE FUNCTION %s.afunc2(state double) " +
                                                                             "RETURNS double " +
                                                                             "LANGUAGE javascript " +
                                                                             "AS '\"string\";'");
            version = assertSchemaVersion(version, migrationListenerVersion, "CREATE OR REPLACE AGGREGATE %s.aaggr(double) " +
                                                                             "SFUNC afunc " +
                                                                             "STYPE double " +
                                                                             "FINALFUNC afunc2");
            // DROP AGGREGATE
            version = assertSchemaVersion(version, migrationListenerVersion, "DROP AGGREGATE %s.aaggr");
            version = assertSchemaVersion(version, migrationListenerVersion, "DROP FUNCTION %s.afunc2");
            version = assertSchemaVersion(version, migrationListenerVersion, "DROP FUNCTION %s.afunc");

        }
        finally
        {
            MigrationManager.instance.unregister(listener);
        }
    }

    private UUID assertSchemaVersion(UUID previousVersion, List<UUID> migrationListenerVersion, String ddl)
    {
        ddl = String.format(ddl, KEYSPACE);
        schemaChange(ddl);

        Schema.instance.updateVersion();
        UUID newVersion = Schema.instance.getVersion();

        Assert.assertFalse("schema version did not change for " + ddl, previousVersion.equals(newVersion));

        Assert.assertEquals("schema change result message contains wrong schema version for " + ddl, newVersion, ((ResultMessage.SchemaChange) lastSchemaChangeResult).change.schemaVersion);

        Assert.assertFalse("no schema change events for " + ddl, migrationListenerVersion.isEmpty());
        for (UUID uuid : migrationListenerVersion)
        {
            Assert.assertEquals("schema change event contains wrong schema version for " + ddl, newVersion, uuid);
        }

        migrationListenerVersion.clear();

        return newVersion;
    }
}
