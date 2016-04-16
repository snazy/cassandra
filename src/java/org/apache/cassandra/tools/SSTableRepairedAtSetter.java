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

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.service.ActiveRepairService;

/**
 * Set repairedAt status on a given set of sstables.
 *
 * If you pass --is-repaired, it will set the repairedAt time to the last modified time.
 *
 * If you know you ran repair 2 weeks ago, you can do something like
 *
 * {@code
 * sstablerepairset --is-repaired -f <(find /var/lib/cassandra/data/.../ -iname "*Data.db*" -mtime +14)
 * }
 */
public class SSTableRepairedAtSetter
{
    private static final String TOOL_NAME = "sstablerepairedset";
    private static final String FORCE_RUN_OPTION = "really-set";
    private static final String FILES_OPTION = "files";
    private static final String SET_REPAIRED_OPTION = "is-repaired";
    private static final String SET_UNREPAIRED_OPTION = "is-unrepaired";
    private static final String HELP_OPTION  = "help";

    /**
     * @param args a list of sstables whose metadata we are changing
     */
    public static void main(final String[] args) throws IOException
    {
        PrintStream out = System.out;
        if (args.length == 0)
        {
            out.println("This command should be run with Cassandra stopped!");
            out.println("Usage: sstablerepairedset [--is-repaired | --is-unrepaired] [-f <sstable-list> | <sstables>]");
            System.exit(1);
        }

        if (args.length < 3 || !args[0].equals("--really-set") || (!args[1].equals("--is-repaired") && !args[1].equals("--is-unrepaired")))
        {
            out.println("This command should be run with Cassandra stopped, otherwise you will get very strange behavior");
            out.println("Verify that Cassandra is not running and then execute the command like this:");
            out.println("Usage: sstablerepairedset --really-set [--is-repaired | --is-unrepaired] [-f <sstable-list> | <sstables>]");
            System.exit(1);
        }

        // Necessary since BufferPool used in RandomAccessReader needs to access DatabaseDescriptor
        Config.setClientMode(true);

        Options options = Options.parseArgs(args);

        Util.initDatabaseDescriptor();

        if (!options.forceRun)
            Util.cassandraDaemonCheckAndExit(TOOL_NAME);

        List<String> fileNames;
        if (options.file != null)
        {
            fileNames = Files.readAllLines(Paths.get(options.file), Charset.defaultCharset());
        }
        else
        {
            fileNames = options.sstables;
        }

        for (String fname: fileNames)
        {
            Descriptor descriptor = Descriptor.fromFilename(fname);
            if (descriptor.version.hasRepairedAt())
            {
                if (options.setRepaired)
                {
                    FileTime f = Files.getLastModifiedTime(new File(descriptor.filenameFor(Component.DATA)).toPath());
                    descriptor.getMetadataSerializer().mutateRepairedAt(descriptor, f.toMillis());
                }
                else
                {
                    descriptor.getMetadataSerializer().mutateRepairedAt(descriptor, ActiveRepairService.UNREPAIRED_SSTABLE);
                }
            }
            else
            {
                System.err.println("SSTable " + fname + " does not have repaired property, run upgradesstables");
            }
        }
    }

    private static class Options
    {
        public final List<String> sstables;
        public final String file;

        public boolean debug;
        public boolean forceRun;
        public boolean setRepaired;
        public boolean setUnrepaired;

        private Options(List<String> sstables, String file)
        {
            this.sstables = sstables;
            this.file = file;
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

                String file = cmd.getOptionValue(FILES_OPTION);

                String[] args = cmd.getArgs();
                if (args.length == 1 && file == null)
                {
                    System.err.println("Missing arguments");
                    printUsage(options);
                    System.exit(1);
                }

                List<String> sstables = args.length > 0 ? Arrays.asList(args)  : null;

                Options opts = new Options(sstables, file);

                opts.forceRun = cmd.hasOption(FORCE_RUN_OPTION);
                opts.setRepaired = cmd.hasOption(SET_REPAIRED_OPTION);
                opts.setUnrepaired = cmd.hasOption(SET_UNREPAIRED_OPTION);

                if (opts.setRepaired && opts.setUnrepaired)
                {
                    System.err.println("Options " + SET_REPAIRED_OPTION + " and " + SET_UNREPAIRED_OPTION + " are exclusive");
                    printUsage(options);
                    System.exit(1);
                }

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
            options.addOption(null, SET_REPAIRED_OPTION,   "mark sstables as repaired");
            options.addOption(null, SET_UNREPAIRED_OPTION, "mark sstables as unrepaired");
            options.addOption("f",  FILES_OPTION,          "file with sstables");
            return options;
        }

        public static void printUsage(BulkLoader.CmdLineOptions options)
        {
            String usage = String.format("%s [options] <sstable>", TOOL_NAME);
            StringBuilder header = new StringBuilder();
            header.append("--\n");
            header.append("Mark sstables as repaired or unrepaired. The sstable argument is optional, if -f option is provided." );
            header.append("\n--\n");
            header.append("Options are:");
            new HelpFormatter().printHelp(usage, header.toString(), options, "");
        }
    }
}
