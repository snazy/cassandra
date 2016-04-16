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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;

public final class Util
{
    private Util()
    {
    }

    /**
     * This is used by standalone tools to force static initialization of DatabaseDescriptor, and fail if configuration
     * is bad.
     */
    public static void initDatabaseDescriptor()
    {
        try
        {
            DatabaseDescriptor.toolInitialization();
        }
        catch (Throwable e)
        {
            boolean logStackTrace = !(e instanceof ConfigurationException) || ((ConfigurationException) e).logStackTrace;
            System.out.println("Exception (" + e.getClass().getName() + ") encountered during startup: " + e.getMessage());

            if (logStackTrace)
            {
                e.printStackTrace();
                System.exit(3);
            }
            else
            {
                System.err.println(e.getMessage());
                System.exit(3);
            }
        }
    }

    public static String cassandraDaemonCheckPorts()
    {
        // Uses an executor to test all all ports at once.
        // Otherwise there might be a time penalty to wait for TCP.

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        try
        {
            InetAddress listenAddress = replaceAnyAddress(DatabaseDescriptor.getListenAddress());
            InetAddress rpcAddress = replaceAnyAddress(DatabaseDescriptor.getRpcAddress());

            List<Future<String>> futures = new ArrayList<>(5);
            futures.add(executorService.submit(() -> portAlive(listenAddress, DatabaseDescriptor.getStoragePort(), "Storage/Gossip")));
            futures.add(executorService.submit(() -> portAlive(listenAddress, DatabaseDescriptor.getSSLStoragePort(), "Storage/Gossip SSL")));
            futures.add(executorService.submit(() -> portAlive(rpcAddress, DatabaseDescriptor.getRpcPort(), "RPC")));
            futures.add(executorService.submit(() -> portAlive(rpcAddress, DatabaseDescriptor.getNativeTransportPort(), "Native transport")));
            futures.add(executorService.submit(() -> portAlive(rpcAddress, DatabaseDescriptor.getNativeTransportPortSSL(), "Native transport SSL")));

            for (Future<String> future : futures)
            {
                try
                {
                    String response = future.get();
                    if (response != null)
                        return response;
                }
                catch (Exception ignored)
                {
                }
            }
        }
        finally
        {
            executorService.shutdown();
        }

        return null;
    }

    private static String portAlive(InetAddress address, int port, String meaning)
    {
        try (Socket socket = new Socket())
        {
            // address is host-local, so a short timeout should be sufficient
            InetSocketAddress sockAddr = new InetSocketAddress(address, port);
            socket.connect(sockAddr, 200);

            // someone's listening on the socket
            return meaning + " port at " + sockAddr;
        }
        catch (IOException ignored)
        {
        }
        return null;
    }

    private static InetAddress replaceAnyAddress(InetAddress address)
    {
        return address.isAnyLocalAddress() ? InetAddress.getLoopbackAddress() : address;
    }

    public static void cassandraDaemonCheckAndExit(String toolName)
    {
        cassandraDaemonCheckAndExit(toolName, null);
    }

    public static void cassandraDaemonCheckAndExit(String toolName, String additionalHelp)
    {
        String adrPortAlive = Util.cassandraDaemonCheckPorts();
        if (adrPortAlive != null)
        {
            System.err.println(adrPortAlive + " is accepting connections, assuming Cassandra is running.");
            System.err.println("It is strongly recommended to NOT run " + toolName + " while Cassandra is running!");
            if (additionalHelp != null)
            {
                System.err.println();
                System.err.println(additionalHelp);
            }
            System.exit(2);
        }
    }
}
