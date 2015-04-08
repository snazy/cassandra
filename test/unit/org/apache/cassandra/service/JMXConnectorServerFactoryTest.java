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
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnector;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.utils.RMISslClientSocketFactoryImpl;

public class JMXConnectorServerFactoryTest
{
    @Before
    public void removeJmxSystemProperties()
    {
        Properties p = System.getProperties();
        for (Iterator<Map.Entry<Object, Object>> i = p.entrySet().iterator();
             i.hasNext(); )
        {
            Map.Entry<Object, Object> entry = i.next();

            String key = ((String) entry.getKey());
            if (key.startsWith("com.sun.management.") || key.startsWith("cassandra.jmx."))
                i.remove();
        }
    }

    static int randomTcpPort()
    {
        try
        {
            try (ServerSocket serverSocket = new ServerSocket(0))
            {
                return serverSocket.getLocalPort();
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testWithoutAnyProperties() throws Exception
    {
        JMXConnectorServer jmx = JMXConnectorServerFactory.maybeInitJmx();
        Assert.assertNull(jmx);
    }

    @Test
    public void testWithLocalPortOnly() throws Exception
    {
        checkCombinations(null, false);
    }

    @Test
    public void testWithLocalPortAndAddress() throws Exception
    {
        for (String address : nonLoopbackAddress())
        {
            removeJmxSystemProperties();

            checkCombinations(address, false);
        }
    }

    @Test
    public void testSslWithoutAnyProperties() throws Exception
    {
        setupSsl();

        JMXConnectorServer jmx = JMXConnectorServerFactory.maybeInitJmx();
        Assert.assertNull(jmx);
    }

    @Test
    public void testSslWithLocalPortOnly() throws Exception
    {
        setupSsl();

        checkCombinations(null, true);
    }

    @Test
    public void testSslMissingProperties() throws Exception
    {
        for (int i = 0; i < 4; i++)
        {
            removeJmxSystemProperties();

            if (i != 0)
                System.setProperty("cassandra.jmx.truststore.path", "-");
            if (i != 1)
                System.setProperty("cassandra.jmx.truststore.password.path", "-");
            if (i != 2)
                System.setProperty("cassandra.jmx.keystore.path", "-");
            if (i != 3)
                System.setProperty("cassandra.jmx.keystore.password.path", "-");

            System.setProperty("cassandra.jmx.ssl", "true");

            try
            {
                checkCombinations(null, true);
            }
            catch (Exception e)
            {
                Assert.assertTrue(e instanceof IOException);
                Assert.assertTrue(e.getMessage().startsWith("Missing parameter(s) to initialize JMX SSL server socket"));
            }
        }
    }

    @Test
    public void testSslWithLocalPortAndAddress() throws Exception
    {
        for (String address : nonLoopbackAddress())
        {
            removeJmxSystemProperties();

            setupSsl();

            checkCombinations(address, true);
        }
    }

    private static void setupSsl()
    {
        System.setProperty("cassandra.jmx.ssl", "true");

        System.setProperty("cassandra.jmx.truststore.path", resourceFile("unittest-jmx.truststore"));
        System.setProperty("cassandra.jmx.truststore.password.path", resourceFile("unittest-jmx.truststore.password"));
        System.setProperty("cassandra.jmx.keystore.path", resourceFile("unittest-jmx.keystore"));
        System.setProperty("cassandra.jmx.keystore.password.path", resourceFile("unittest-jmx.keystore.password"));
    }

    private static void checkCombinations(String address, boolean ssl) throws Exception
    {
        String port = String.valueOf(randomTcpPort());
        checkWithAddressAndPort(port, address, ssl);

        port = String.valueOf(randomTcpPort());
        checkWithAddressAndPortAndAuth(port, address, ssl);
    }

    private static void checkWithAddressAndPort(String port, String address, boolean ssl) throws Exception
    {
        address = hostPortEnv(port, address);

        JMXConnectorServer jmx = JMXConnectorServerFactory.maybeInitJmx();
        Assert.assertNotNull(jmx);
        try
        {
            checkJmxWorking(new JMXServiceURL(JMXConnectorServerFactory.jmxUrl(port, address)), null, ssl);
        }
        finally
        {
            jmx.stop();
        }
    }

    private static void checkWithAddressAndPortAndAuth(String port, String address, boolean ssl) throws Exception
    {
        address = hostPortEnv(port, address);

        System.setProperty("com.sun.management.jmxremote.password.file", resourceFile("unittest-jmxremote.password"));
        System.setProperty("com.sun.management.jmxremote.authenticate", "true");

        JMXConnectorServer jmx = JMXConnectorServerFactory.maybeInitJmx();
        Assert.assertNotNull(jmx);
        try
        {
            try
            {
                checkJmxWorking(new JMXServiceURL(JMXConnectorServerFactory.jmxUrl(port, address)), null, ssl);
            }
            catch (SecurityException e)
            {
                // no credentials
            }

            try
            {
                checkJmxWorking(new JMXServiceURL(JMXConnectorServerFactory.jmxUrl(port, address)),
                                Collections.singletonMap("jmx.remote.credentials", new String[]{ "wrong", "password" }), ssl);
            }
            catch (SecurityException e)
            {
                // wrong credentials
            }

            checkJmxWorking(new JMXServiceURL(JMXConnectorServerFactory.jmxUrl(port, address)),
                            Collections.singletonMap("jmx.remote.credentials", new String[]{ "user", "foo" }), ssl);

            checkJmxWorking(new JMXServiceURL(JMXConnectorServerFactory.jmxUrl(port, address)),
                            Collections.singletonMap("jmx.remote.credentials", new String[]{ "bart", "42" }), ssl);
        }
        finally
        {
            jmx.stop();
        }
    }

    private static String hostPortEnv(String port, String address)
    {
        if (port != null)
            System.setProperty("cassandra.jmx.local.port", port);
        if (address != null)
            System.setProperty("cassandra.jmx.local.address", address);
        else
            address = "localhost";
        return address;
    }

    private static List<String> nonLoopbackAddress() throws Exception
    {
        List<String> r = new ArrayList<>();
        for (Enumeration<NetworkInterface> nie = NetworkInterface.getNetworkInterfaces();
             nie.hasMoreElements(); )
        {
            NetworkInterface ni = nie.nextElement();
            if (!ni.isLoopback() && ni.isUp())
            {
                for (Enumeration<InetAddress> ias = ni.getInetAddresses();
                     ias.hasMoreElements(); )
                {
                    InetAddress ia = ias.nextElement();
                    if (ia.isLinkLocalAddress() || ia.isLoopbackAddress())
                        continue;

                    if (ia instanceof Inet6Address)
                        r.add('[' + ia.getHostAddress() + ']');
                    else
                        r.add(ia.getHostAddress());
                }
            }
        }
        Assert.assertFalse(r.isEmpty());
        return r;
    }

    private static void checkJmxWorking(JMXServiceURL rmiAddress, Map env, boolean ssl) throws Exception
    {
        if (ssl)
        {
            env = env != null ? new HashMap<>(env) : new HashMap();
            String truststorePath = System.getProperty("cassandra.jmx.truststore.path");
            String truststorePasswordPath = System.getProperty("cassandra.jmx.truststore.password.path");
            RMISslClientSocketFactoryImpl clientFactory = new RMISslClientSocketFactoryImpl(truststorePath, truststorePasswordPath);
            env.put(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE, clientFactory);
            env.put("com.sun.jndi.rmi.factory.socket", clientFactory);
        }

        try (RMIConnector connector = new RMIConnector(rmiAddress, env))
        {
            connector.connect();
            MBeanServerConnection msc = connector.getMBeanServerConnection();
            Assert.assertNotNull(msc);

            MBeanInfo mbeanInfo = msc.getMBeanInfo(new ObjectName(ManagementFactory.RUNTIME_MXBEAN_NAME));
            Assert.assertNotNull(mbeanInfo);
        }
    }

    private static String resourceFile(String resource)
    {
        URL url = JMXConnectorServerFactoryTest.class.getClassLoader().getResource(resource);
        Assert.assertNotNull(url);
        return url.getFile();
    }
}
