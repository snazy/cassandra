/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;

import java.io.File;
import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Joiner;

public class OffsetAwareConfigurationLoader extends YamlConfigurationLoader
{

    static final String OFFSET_PROPERTY = "cassandra.test.offsetseed";
    private int offset;

    private static final int LOCK_PORT_OFFSET = 60000;
    private static ServerSocket lockSocket;
    private static boolean didCleanup;

    public OffsetAwareConfigurationLoader()
    {
        String offsetStr = System.getProperty(OFFSET_PROPERTY);

        offset = offsetStr == null ? -1 : Integer.parseInt(offsetStr);
    }

    private static synchronized int allocateLockSocket(Config config)
    {
        if (lockSocket != null)
            return lockSocket.getLocalPort() - LOCK_PORT_OFFSET;

        InetAddress localhost;
        try
        {
            localhost = InetAddress.getByName("127.0.0.1");
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException("Oops, failed to resolve 127.0.0.1, there something seriously wrong", e);
        }

        int[] portsToTry = new int[]{ config.storage_port, config.native_transport_port, config.ssl_storage_port };

        for (int i = 0; i < 50; i++)
        {
            try
            {
                for (int tryPort : portsToTry)
                    if (!portAvailable(localhost, tryPort + i))
                        throw new Exception("just continue...");

                int port = LOCK_PORT_OFFSET + i;
                lockSocket = new ServerSocket(port, 10, localhost);

                return i;
            }
            catch (Exception e)
            {
                // just continue
            }
        }

        throw new RuntimeException("Neither offset property " + OFFSET_PROPERTY + " set, nor found an available 'lock port'");
    }

    private static boolean portAvailable(InetAddress localhost, int port)
    {
        try
        {
            new ServerSocket(port, 10, localhost).close();
            return true;
        }
        catch (Exception e)
        {
            return false;
        }
    }

    @Override
    public Config loadConfig() throws ConfigurationException
    {
        Config config = super.loadConfig();

        if (offset < 0)
            offset = allocateLockSocket(config);

        String sep = File.pathSeparator;

        config.native_transport_port += offset;
        config.storage_port += offset;
        config.ssl_storage_port += offset;

        //Rewrite the seed ports string
        String[] hosts = config.seed_provider.parameters.get("seeds").split(",", -1);
        String rewrittenSeeds = Joiner.on(", ").join(Arrays.stream(hosts).map(host -> {
            StringBuilder sb = new StringBuilder();
            try
            {
                InetAddressAndPort address = InetAddressAndPort.getByName(host.trim());
                if (address.address instanceof Inet6Address)
                {
                    sb.append('[').append(address.address.getHostAddress()).append(']');
                }
                else
                {
                    sb.append(address.address.getHostAddress());
                }
                sb.append(':').append(address.port + offset);
                return sb.toString();
            }
            catch (UnknownHostException e)
            {
                throw new ConfigurationException("Error in OffsetAwareConfigurationLoader reworking seed list", e);
            }
        }).collect(Collectors.toList()));
        config.seed_provider.parameters.put("seeds", rewrittenSeeds);

        config.commitlog_directory += sep + offset;
        config.saved_caches_directory += sep + offset;
        config.hints_directory += sep + offset;

        config.cdc_raw_directory += sep + offset;

        for (int i = 0; i < config.data_file_directories.length; i++)
            config.data_file_directories[i] += sep + offset;

        if (!didCleanup)
        {
            cleanupDirectories(config);
            didCleanup = true;
        }

        return config;
    }

    private void cleanupDirectories(Config config)
    {
        Stream.concat(Stream.of(config.commitlog_directory,
                                config.saved_caches_directory,
                                config.hints_directory,
                                config.cdc_raw_directory),
                      Arrays.stream(config.data_file_directories))
              .forEach(OffsetAwareConfigurationLoader::prepareDirectory);
    }


    private static void prepareDirectory(String directory)
    {
        Path rootPath = Paths.get(directory).toAbsolutePath();

        deleteTree(rootPath);

        if (!Files.isDirectory(rootPath))
        {
            try
            {
                Files.createDirectories(rootPath);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Failed to create directory " + rootPath, e);
            }
        }
    }

    private static void deleteTree(Path root)
    {
        if (root == null)
            return;
        try
        {
            if (Files.isDirectory(root))
            {
                try (Stream<Path> contents = Files.list(root))
                {
                    contents.forEach(OffsetAwareConfigurationLoader::deleteTree);
                }
            }

            Files.deleteIfExists(root);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to delete " + root, e);
        }
    }
}
