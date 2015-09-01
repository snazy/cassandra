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

package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.service.StorageService;

/**
 * Not really a test, but generates "legacy" sstables in the current, latest version.
 */
public class GenerateLegacyTablesTest
{
    public static final String LEGACY_SSTABLE_PROP = "legacy-sstable-root";

    public static File LEGACY_SSTABLE_ROOT;

    // 1200 chars
    static final String longString = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        String scp = System.getProperty(LEGACY_SSTABLE_PROP);
        assert scp != null;
        LEGACY_SSTABLE_ROOT = new File(scp).getAbsoluteFile();
        assert LEGACY_SSTABLE_ROOT.isDirectory();

        // do not make files larger than necessary
        DatabaseDescriptor.setColumnIndexSize(1024);

        SchemaLoader.mkdirs();
        SchemaLoader.cleanup();
        SchemaLoader.loadSchema();

        QueryProcessor.process("CREATE KEYSPACE legacy_tables WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", ConsistencyLevel.ONE);
    }

    @Test
    @Ignore
    public void testGenerateSstables() throws Throwable
    {
        QueryProcessor.process(String.format("CREATE TABLE legacy_tables.legacy_%s_simple (pk text PRIMARY KEY, val text)", BigFormat.latestVersion), ConsistencyLevel.ONE);
        QueryProcessor.process(String.format("CREATE TABLE legacy_tables.legacy_%s_simple_counter (pk text PRIMARY KEY, val counter)", BigFormat.latestVersion), ConsistencyLevel.ONE);
        QueryProcessor.process(String.format("CREATE TABLE legacy_tables.legacy_%s_clust (pk text, ck text, val text, PRIMARY KEY (pk, ck))", BigFormat.latestVersion), ConsistencyLevel.ONE);
        QueryProcessor.process(String.format("CREATE TABLE legacy_tables.legacy_%s_clust_counter (pk text, ck text, val counter, PRIMARY KEY (pk, ck))", BigFormat.latestVersion), ConsistencyLevel.ONE);

        Random rand = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 128; i++)
        {
            sb.append((char)('a' + rand.nextInt(26)));
        }
        String randomString = sb.toString();

        for (int pk = 0; pk < 5; pk++)
        {
            String valPk = Integer.toString(pk);
            QueryProcessor.executeInternal(String.format("INSERT INTO legacy_tables.legacy_%s_simple (pk, val) VALUES ('%s', '%s')",
                                                         BigFormat.latestVersion, valPk, "foo bar baz"));

            QueryProcessor.executeInternal(String.format("UPDATE legacy_tables.legacy_%s_simple_counter SET val = val + 1 WHERE pk = '%s'",
                                                         BigFormat.latestVersion, valPk));

            for (int ck = 0; ck < 50; ck++)
            {
                String valCk = Integer.toString(ck);

                QueryProcessor.executeInternal(String.format("INSERT INTO legacy_tables.legacy_%s_clust (pk, ck, val) VALUES ('%s', '%s', '%s')",
                                                             BigFormat.latestVersion, valPk, valCk + longString, randomString));

                QueryProcessor.executeInternal(String.format("UPDATE legacy_tables.legacy_%s_clust_counter SET val = val + 1 WHERE pk = '%s' AND ck='%s'",
                                                             BigFormat.latestVersion, valPk, valCk + longString));

            }
        }

        StorageService.instance.forceKeyspaceFlush("legacy_tables");

        File ksDir = new File(LEGACY_SSTABLE_ROOT, String.format("%s/legacy_tables", BigFormat.latestVersion));
        ksDir.mkdirs();
        copySstables(String.format("legacy_%s_simple", BigFormat.latestVersion), ksDir);
        copySstables(String.format("legacy_%s_simple_counter", BigFormat.latestVersion), ksDir);
        copySstables(String.format("legacy_%s_clust", BigFormat.latestVersion), ksDir);
        copySstables(String.format("legacy_%s_clust_counter", BigFormat.latestVersion), ksDir);
    }

    private void copySstables(String table, File ksDir) throws IOException
    {
        File cfDir = new File(ksDir, table);
        cfDir.mkdir();

        byte[] buf = new byte[65536];

        for (File srcDir : Keyspace.open("legacy_tables").getColumnFamilyStore(table).getDirectories().getCFDirectories())
        {
            for (File file : srcDir.listFiles())
            {
                if (file.isFile())
                {
                    File target = new File(cfDir, file.getName());
                    int rd;
                    FileInputStream is = new FileInputStream(file);
                    FileOutputStream os = new FileOutputStream(target);
                    while ((rd = is.read(buf)) >= 0)
                        os.write(buf, 0, rd);
                }
            }
        }
    }
}
