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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.util.HashMap;
import java.util.Map;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.RMIServerSocketFactoryImpl;
import org.apache.cassandra.utils.RMISslServerSocketFactoryImpl;

/**
 * Initializes the {@link JMXConnectorServer} for {@link CassandraDaemon}.
 * Extracted as a separate class to ease unit testing.
 */
public final class JMXConnectorServerFactory
{
    private static final Logger logger = LoggerFactory.getLogger(JMXConnectorServerFactory.class);

    private JMXConnectorServerFactory() {}

    public static JMXConnectorServer maybeInitJmx() throws IOException
    {
        String jmxPort = System.getProperty("com.sun.management.jmxremote.port");

        if (jmxPort == null)
        {
            logger.warn("JMX is not enabled to receive remote connections. Please see cassandra-env.sh for more info.");

            jmxPort = System.getProperty("cassandra.jmx.local.port");

            if (jmxPort == null)
            {
                logger.error("cassandra.jmx.local.port missing from cassandra-env.sh, unable to start local JMX service.");
            }
            else
            {
                System.setProperty("java.rmi.server.hostname", "127.0.0.1");

                InetAddress listenAddress = InetAddress.getLoopbackAddress();
                String address = System.getProperty("cassandra.jmx.local.address");
                if (address != null && !address.trim().isEmpty())
                {
                    try
                    {
                        listenAddress = InetAddress.getByName(address);
                        address = listenAddress.getHostName();
                        if (listenAddress instanceof Inet6Address && listenAddress.getHostAddress().equals(address))
                            address = '[' + address + ']';
                        System.setProperty("java.rmi.server.hostname", address);
                    }
                    catch (UnknownHostException unk)
                    {
                        throw new IOException("Failed to resolve JMX listen address '" + listenAddress + '\'', unk);
                    }
                }
                else
                    address = "localhost";

                boolean ssl = "true".equals(System.getProperty("cassandra.jmx.ssl"));

                RMIServerSocketFactory serverFactory;
                RMIClientSocketFactory clientFactory = null;
                if (ssl)
                {
                    String truststorePath = System.getProperty("cassandra.jmx.truststore.path");
                    String truststorePasswordPath = System.getProperty("cassandra.jmx.truststore.password.path");
                    String keystorePath = System.getProperty("cassandra.jmx.keystore.path");
                    String keystorePasswordPath = System.getProperty("cassandra.jmx.keystore.password.path");
                    if (truststorePath == null || truststorePasswordPath == null ||
                        keystorePath == null || keystorePasswordPath == null)
                    {
                        throw new IOException("Missing parameter(s) to initialize JMX SSL server socket - check " +
                                              "cassandra.jmx.truststore.path, cassandra.jmx.truststore.password.path, cassandra.jmx.keystore.path, cassandra.jmx.keystore.password.path");
                    }

                    serverFactory = new RMISslServerSocketFactoryImpl(listenAddress,
                                                                   truststorePath, truststorePasswordPath,
                                                                   keystorePath, keystorePasswordPath);
                    clientFactory = ((RMISslServerSocketFactoryImpl)serverFactory).createClientFactory();
                }
                else
                    serverFactory = new RMIServerSocketFactoryImpl(listenAddress);
                LocateRegistry.createRegistry(Integer.valueOf(jmxPort), clientFactory, serverFactory);

                Map<String, Object> env = new HashMap<>();

                env.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE, serverFactory);
                if (clientFactory != null)
                {
                    env.put(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE, clientFactory);
                    env.put("com.sun.jndi.rmi.factory.socket", clientFactory);
                }
                if ("true".equals(System.getProperty("com.sun.management.jmxremote.authenticate")))
                    env.put("jmx.remote.x.password.file", System.getProperty("com.sun.management.jmxremote.password.file"));

                RMIConnectorServer jmxServer = new RMIConnectorServer(
                                                                     new JMXServiceURL(jmxUrl(jmxPort, address)),
                                                                     env,
                                                                     ManagementFactory.getPlatformMBeanServer()
                );

                jmxServer.start();

                return jmxServer;
            }
        }
        else
        {
            logger.info("JMX is enabled to receive remote connections on port: " + jmxPort);
        }

        return null;
    }

    public static String jmxUrl(String jmxPort, String address)
    {
        return "service:jmx:" +
            "rmi://" + address + ':' + jmxPort + "/jndi/" +
            "rmi://" + address + ':' + jmxPort + "/jmxrmi";
    }
}
