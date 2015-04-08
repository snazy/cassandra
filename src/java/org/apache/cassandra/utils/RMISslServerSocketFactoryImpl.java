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

package org.apache.cassandra.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.security.KeyStore;
import java.security.SecureRandom;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.apache.cassandra.io.util.FileUtils;


public class RMISslServerSocketFactoryImpl implements RMIServerSocketFactory
{

    private final InetAddress listenAddress;
    private final String truststorePath;
    private final String truststorePasswordPath;
    private final SSLContext context;

    public RMISslServerSocketFactoryImpl(InetAddress listenAddress,
                                         String truststorePath, String truststorePasswordPath,
                                         String keystorePath, String keystorePasswordPath) throws IOException
    {
        this.listenAddress = listenAddress;
        this.truststorePath = truststorePath;
        this.truststorePasswordPath = truststorePasswordPath;

        this.context = initContext(truststorePath, truststorePasswordPath,
                                   keystorePath, keystorePasswordPath);
    }

    static SSLContext initContext(String truststorePath, String truststorePasswordPath,
                                  String keystorePath, String keystorePasswordPath) throws IOException
    {
        FileInputStream tsf = null;
        FileInputStream ksf = null;
        try
        {
            tsf = new FileInputStream(truststorePath);

            SSLContext ctx = SSLContext.getInstance("SSL");

            String truststorePassword = org.apache.commons.io.FileUtils.readFileToString(new File(truststorePasswordPath));
            KeyStore ts = KeyStore.getInstance("JKS");
            ts.load(tsf, truststorePassword.toCharArray());
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ts);

            KeyManager[] keyManagers = null;
            if (keystorePath != null && keystorePasswordPath != null)
            {
                ksf = new FileInputStream(keystorePath);
                String keystorePassword = org.apache.commons.io.FileUtils.readFileToString(new File(keystorePasswordPath));
                KeyStore ks = KeyStore.getInstance("JKS");
                ks.load(ksf, keystorePassword.toCharArray());
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(ks, keystorePassword.toCharArray());
                keyManagers = kmf.getKeyManagers();
            }

            ctx.init(keyManagers, tmf.getTrustManagers(), new SecureRandom());

            return ctx;
        }
        catch (Exception e)
        {
            throw new IOException("Failed to initialze SSL context", e);
        }
        finally
        {
            FileUtils.closeQuietly(tsf);
            FileUtils.closeQuietly(ksf);
        }
    }

    public ServerSocket createServerSocket(final int pPort) throws IOException
    {
        return context.getServerSocketFactory().createServerSocket(pPort, 0, listenAddress);
    }

    public boolean equals(Object obj)
    {
        return obj != null && (obj == this || obj.getClass().equals(getClass()));
    }

    public int hashCode()
    {
        return RMISslServerSocketFactoryImpl.class.hashCode();
    }

    public RMIClientSocketFactory createClientFactory() throws IOException
    {
        return new RMISslClientSocketFactoryImpl(truststorePath, truststorePasswordPath);
    }
}
