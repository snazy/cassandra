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

package org.apache.cassandra.tools.workload;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.transport.sampler.WorkloadFileHeader;
import org.apache.cassandra.transport.sampler.WorkloadSampling;
import org.apache.cassandra.utils.MD5Digest;

/**
 * Workload record file dump tool.
 */
public final class WorkloadDump
{
    private WorkloadDump()
    {
    }

    public static void main(String[] args)
    {
        PrintStream out = System.out;
        if (args.length == 0)
        {
            out.println("Usage: workloaddump <workload-file>");
            System.exit(1);
        }

        try (DataInputStream is = new DataInputStream(new BufferedInputStream(new FileInputStream(new File(args[0])), 65536)))
        {
            WorkloadFileHeader fileHeader = new WorkloadFileHeader(is);
            System.out.printf("   Workload recording started at: %tY-%<tm-%<td-%<tH:%<tM:%<tS.%<tL %<tZ%n" +
                              "          with Cassandra version: %s%n" +
                              "  on node with broadcast address: %s%n" +
                              "               and local address: %s%n",
                             new Date(fileHeader.getTimestamp()),
                             fileHeader.getVersion(),
                             fileHeader.getBroadcastAddress(),
                             fileHeader.getLocalAddress());

            outer: while (true)
            {
                int opcode = is.readInt();
                switch (opcode)
                {
                    case WorkloadSampling.OPCODE_KEYSPACE_DEFINITION:
                        Map<String, List<Map<String, byte[]>>> keyspaceSchema = readKeyspaceSchema(is);
                        break;
                    case WorkloadSampling.OPCODE_PREPARED_STATEMENT:
                        MD5Digest md5Digest = MD5Digest.wrap(readByteArray(is));
                        String queryString = readString(is);
                        break;
                    case WorkloadSampling.OPCODE_START_DATA:
                        break outer;
                    default:
                        throw new IllegalArgumentException("Invalid opcode in file: " + Integer.toHexString(opcode));
                }
            }

            while (is.available() > 0)
            {
                int hLen = is.readUnsignedShort();
                long offsetNanos = is.readLong();
                int port = is.readUnsignedShort();
                int addrLen = hLen - 2 - 8;
                byte[] addr = new byte[addrLen];
                is.readFully(addr);
                InetAddress remote = InetAddress.getByAddress(addr);
                System.out.printf("From %s : %d   at %,d\u00b5s%n",
                                  remote,
                                  port,
                                  offsetNanos / 1000L);

                // TODO Frame.Decoder

                break;
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private static Map<String, List<Map<String, byte[]>>> readKeyspaceSchema(DataInputStream is) throws IOException
    {
        Map<String, List<Map<String, byte[]>>> keyspaceSchema;
        keyspaceSchema = new HashMap<>();
        for (String schemaTable : WorkloadSampling.orderedSchemaTables)
        {
            int rowCount = is.readInt();
            List<Map<String, byte[]>> rows = new ArrayList<>();
            for (int i = 0; i < rowCount; i++)
            {
                int colCount = is.readInt();
                Map<String, byte[]> row = new HashMap<>();
                for (int col = 0; col < colCount; col++)
                {
                    String colName = readString(is);
                    byte[] data = readByteArray(is);
                    row.put(colName, data);
                }
                rows.add(row);
            }
            keyspaceSchema.put(schemaTable, rows);
        }
        return keyspaceSchema;
    }

    private static String readString(DataInputStream is) throws IOException
    {
        int len = is.readInt();
        return len == -1 ? null : new String(readByteArray(is, len), StandardCharsets.UTF_8);
    }

    private static byte[] readByteArray(DataInputStream is) throws IOException
    {
        int len = is.readInt();
        return len == -1 ? null : readByteArray(is, len);
    }

    private static byte[] readByteArray(DataInputStream is, int len) throws IOException
    {
        byte[] arr = new byte[len];
        is.readFully(arr);
        return arr;
    }
}
