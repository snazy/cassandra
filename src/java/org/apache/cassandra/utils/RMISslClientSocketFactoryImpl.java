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

import java.io.IOException;
import java.net.Socket;
import java.util.StringTokenizer;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.rmi.ssl.SslRMIClientSocketFactory;


public class RMISslClientSocketFactoryImpl extends SslRMIClientSocketFactory
{
    private transient SSLContext context;

    private final String truststorePath;
    private final String truststorePasswordPath;

    public RMISslClientSocketFactoryImpl(String truststorePath, String truststorePasswordPath) throws IOException
    {
        this.truststorePath = truststorePath;
        this.truststorePasswordPath = truststorePasswordPath;

        initContext();
    }

    private void initContext() throws IOException
    {
        context = RMISslServerSocketFactoryImpl.initContext(truststorePath, truststorePasswordPath, null, null);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        in.defaultReadObject();
        initContext();
    }

    public Socket createSocket(String host, int port) throws IOException
    {
        // Retrieve the SSLSocketFactory
        //
        final SocketFactory sslSocketFactory = context.getSocketFactory();
        // Create the SSLSocket
        //
        final SSLSocket sslSocket = (SSLSocket)
                                    sslSocketFactory.createSocket(host, port);
        // Set the SSLSocket Enabled Cipher Suites
        //
        final String enabledCipherSuites =
        System.getProperty("javax.rmi.ssl.client.enabledCipherSuites");
        if (enabledCipherSuites != null)
        {
            StringTokenizer st = new StringTokenizer(enabledCipherSuites, ",");
            int tokens = st.countTokens();
            String[] enabledCipherSuitesList = new String[tokens];
            for (int i = 0; i < tokens; i++)
            {
                enabledCipherSuitesList[i] = st.nextToken();
            }
            try
            {
                sslSocket.setEnabledCipherSuites(enabledCipherSuitesList);
            }
            catch (IllegalArgumentException e)
            {
                throw (IOException)
                      new IOException(e.getMessage()).initCause(e);
            }
        }
        // Set the SSLSocket Enabled Protocols
        //
        final String enabledProtocols =
        System.getProperty("javax.rmi.ssl.client.enabledProtocols");
        if (enabledProtocols != null)
        {
            StringTokenizer st = new StringTokenizer(enabledProtocols, ",");
            int tokens = st.countTokens();
            String[] enabledProtocolsList = new String[tokens];
            for (int i = 0; i < tokens; i++)
            {
                enabledProtocolsList[i] = st.nextToken();
            }
            try
            {
                sslSocket.setEnabledProtocols(enabledProtocolsList);
            }
            catch (IllegalArgumentException e)
            {
                throw (IOException)
                      new IOException(e.getMessage()).initCause(e);
            }
        }
        // Return the preconfigured SSLSocket
        //
        return sslSocket;
    }
}
