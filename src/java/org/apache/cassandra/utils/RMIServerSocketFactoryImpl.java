package org.apache.cassandra.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.rmi.server.RMIServerSocketFactory;
import javax.net.ServerSocketFactory;


public class RMIServerSocketFactoryImpl implements RMIServerSocketFactory
{

    private final InetAddress listenAddress;

    public RMIServerSocketFactoryImpl(InetAddress listenAddress)
    {
        this.listenAddress = listenAddress;
    }

    public ServerSocket createServerSocket(final int pPort) throws IOException
    {
        ServerSocketFactory factory = ServerSocketFactory.getDefault();
        return factory.createServerSocket(pPort, 0, listenAddress);
    }

    public boolean equals(Object obj)
    {
        return obj != null && (obj == this || obj.getClass().equals(getClass()));
    }

    public int hashCode()
    {
        return RMIServerSocketFactoryImpl.class.hashCode();
    }
}
