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

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * During compaction we can drop entire sstables if they only contain expired tombstones and if it is guaranteed
 * to not cover anything in other sstables. An expired sstable can be blocked from getting dropped if its newest
 * timestamp is newer than the oldest data in another sstable.
 *
 * This class outputs all sstables that are blocking other sstables from getting dropped so that a user can
 * figure out why certain sstables are still on disk.
 */
public class SSTableExpiredBlockers
{
    private static final String TOOL_NAME = "sstablescrub";
    private static final String HELP_OPTION  = "help";
    private static final String FORCE_RUN_OPTION  = "run-even-if-cassandra-is-running";

    public static void main(String[] args) throws IOException
    {
        Options options = Options.parseArgs(args);
        Util.initDatabaseDescriptor();

        if (!options.forceRun)
            Util.cassandraDaemonCheckAndExit(TOOL_NAME);

        PrintStream out = System.out;

        Schema.instance.loadFromDisk(false);

        CFMetaData metadata = Schema.instance.getCFMetaData(options.keyspaceName, options.cfName);
        if (metadata == null)
            throw new IllegalArgumentException(String.format("Unknown keyspace/table %s.%s",
                                                             options.keyspaceName,
                                                             options.cfName));

        Keyspace ks = Keyspace.openWithoutSSTables(options.keyspaceName);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(options.cfName);
        Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW).skipTemporary(true);
        Set<SSTableReader> sstables = new HashSet<>();
        for (Map.Entry<Descriptor, Set<Component>> sstable : lister.list().entrySet())
        {
            if (sstable.getKey() != null)
            {
                try
                {
                    SSTableReader reader = SSTableReader.open(sstable.getKey());
                    sstables.add(reader);
                }
                catch (Throwable t)
                {
                    out.println("Couldn't open sstable: " + sstable.getKey().filenameFor(Component.DATA)+" ("+t.getMessage()+")");
                }
            }
        }
        if (sstables.isEmpty())
        {
            out.println("No sstables for " + options.keyspaceName + "." + options.cfName);
            System.exit(1);
        }

        int gcBefore = (int)(System.currentTimeMillis()/1000) - metadata.params.gcGraceSeconds;
        Multimap<SSTableReader, SSTableReader> blockers = checkForExpiredSSTableBlockers(sstables, gcBefore);
        for (SSTableReader blocker : blockers.keySet())
        {
            out.println(String.format("%s blocks %d expired sstables from getting dropped: %s%n",
                                    formatForExpiryTracing(Collections.singleton(blocker)),
                                    blockers.get(blocker).size(),
                                    formatForExpiryTracing(blockers.get(blocker))));
        }

        System.exit(0);
    }

    public static Multimap<SSTableReader, SSTableReader> checkForExpiredSSTableBlockers(Iterable<SSTableReader> sstables, int gcBefore)
    {
        Multimap<SSTableReader, SSTableReader> blockers = ArrayListMultimap.create();
        for (SSTableReader sstable : sstables)
        {
            if (sstable.getSSTableMetadata().maxLocalDeletionTime < gcBefore)
            {
                for (SSTableReader potentialBlocker : sstables)
                {
                    if (!potentialBlocker.equals(sstable) &&
                        potentialBlocker.getMinTimestamp() <= sstable.getMaxTimestamp() &&
                        potentialBlocker.getSSTableMetadata().maxLocalDeletionTime > gcBefore)
                        blockers.put(potentialBlocker, sstable);
                }
            }
        }
        return blockers;
    }

    private static String formatForExpiryTracing(Iterable<SSTableReader> sstables)
    {
        StringBuilder sb = new StringBuilder();

        for (SSTableReader sstable : sstables)
            sb.append(String.format("[%s (minTS = %d, maxTS = %d, maxLDT = %d)]", sstable, sstable.getMinTimestamp(), sstable.getMaxTimestamp(), sstable.getSSTableMetadata().maxLocalDeletionTime)).append(", ");

        return sb.toString();
    }

    private static class Options
    {
        public final String keyspaceName;
        public final String cfName;
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
            options.addOption("h",  HELP_OPTION,           "display this help message");
            options.addOption(null, FORCE_RUN_OPTION,      "run even if Cassandra is running, which is really not recommended");
            return options;
        }

        public static void printUsage(BulkLoader.CmdLineOptions options)
        {
            String usage = String.format("%s [options] <keyspace> <table>", TOOL_NAME);
            StringBuilder header = new StringBuilder();
            header.append("--\n");
            header.append("Options are:");
            new HelpFormatter().printHelp(usage, header.toString(), options, "");
        }
    }
}
