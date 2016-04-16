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
package org.apache.cassandra.tools;

import java.io.PrintStream;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * Reset level to 0 on a given set of sstables
 */
public class SSTableLevelResetter
{
    private static final String TOOL_NAME = "sstablelevelreset";
    private static final String HELP_OPTION  = "help";
    private static final String FORCE_RUN_OPTION  = "really-reset";

    /**
     * @param args a list of sstables whose metadata we are changing
     */
    public static void main(String[] args)
    {
        PrintStream out = System.out;

        Options options = Options.parseArgs(args);

        Util.initDatabaseDescriptor();

        if (!options.forceRun)
            Util.cassandraDaemonCheckAndExit(TOOL_NAME);

        // TODO several daemon threads will run from here.
        // So we have to explicitly call System.exit.
        try
        {
            // load keyspace descriptions.
            Schema.instance.loadFromDisk(false);

            String keyspaceName = args[1];
            String columnfamily = args[2];
            // validate columnfamily
            if (Schema.instance.getCFMetaData(keyspaceName, columnfamily) == null)
            {
                System.err.println("ColumnFamily not found: " + keyspaceName + "/" + columnfamily);
                System.exit(1);
            }

            Keyspace keyspace = Keyspace.openWithoutSSTables(keyspaceName);
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(columnfamily);
            boolean foundSSTable = false;
            for (Map.Entry<Descriptor, Set<Component>> sstable : cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW).list().entrySet())
            {
                if (sstable.getValue().contains(Component.STATS))
                {
                    foundSSTable = true;
                    Descriptor descriptor = sstable.getKey();
                    StatsMetadata metadata = (StatsMetadata) descriptor.getMetadataSerializer().deserialize(descriptor, MetadataType.STATS);
                    if (metadata.sstableLevel > 0)
                    {
                        out.println("Changing level from " + metadata.sstableLevel + " to 0 on " + descriptor.filenameFor(Component.DATA));
                        descriptor.getMetadataSerializer().mutateLevel(descriptor, 0);
                    }
                    else
                    {
                        out.println("Skipped " + descriptor.filenameFor(Component.DATA) + " since it is already on level 0");
                    }
                }
            }

            if (!foundSSTable)
            {
                out.println("Found no sstables, did you give the correct keyspace/table?");
            }
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            t.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }

    private static class Options
    {
        public final String keyspaceName;
        public final String cfName;

        public boolean debug;
        public boolean forceRun;

        private Options(String keyspaceName, String cfName)
        {
            this.keyspaceName = keyspaceName;
            this.cfName = cfName;
        }

        public static Options parseArgs(String cmdArgs[])
        {
            CommandLineParser parser = new GnuParser();
            BulkLoader.CmdLineOptions options = getCmdLineOptions();
            try
            {
                CommandLine cmd = parser.parse(options, cmdArgs, false);

                if (cmd.hasOption(HELP_OPTION))
                {
                    printUsage(options);
                    System.exit(0);
                }

                String[] args = cmd.getArgs();
                if (args.length != 2)
                {
                    String msg = args.length < 2 ? "Missing arguments" : "Too many arguments";
                    System.err.println(msg);
                    printUsage(options);
                    System.exit(1);
                }

                String keyspaceName = args[0];
                String cfName = args[1];

                Options opts = new Options(keyspaceName, cfName);

                opts.forceRun = cmd.hasOption(FORCE_RUN_OPTION);

                return opts;
            }
            catch (ParseException e)
            {
                errorMsg(e.getMessage(), options);
                return null;
            }
        }

        private static void errorMsg(String msg, BulkLoader.CmdLineOptions options)
        {
            System.err.println(msg);
            printUsage(options);
            System.exit(1);
        }

        private static BulkLoader.CmdLineOptions getCmdLineOptions()
        {
            BulkLoader.CmdLineOptions options = new BulkLoader.CmdLineOptions();
            options.addOption(null, FORCE_RUN_OPTION,      "run even if Cassandra is running, which is really not recommended");
            return options;
        }

        public static void printUsage(BulkLoader.CmdLineOptions options)
        {
            String usage = String.format("%s [options] <keyspace> <column_family>", TOOL_NAME);
            StringBuilder header = new StringBuilder();
            header.append("--\n");
            header.append("Reset the level of the sstables for the provided table." );
            header.append("\n--\n");
            header.append("Options are:");
            new HelpFormatter().printHelp(usage, header.toString(), options, "");
        }
    }
}
