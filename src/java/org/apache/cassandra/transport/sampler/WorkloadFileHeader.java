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

package org.apache.cassandra.transport.sampler;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import com.google.common.base.MoreObjects;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Workload sampling file header.
 * Used for both writing and reading in workload sampling tools bundled with Cassandra.
 */
public class WorkloadFileHeader
{
    public static short MAGIC = (short) 0xCA55;
    public static short CURRENT_VERSION = 0x0010;

    private final long timestamp;
    private final String version;
    private final InetAddress broadcastAddress;
    private final InetAddress localAddress;
    private final int maxSeconds;
    private final int maxMBytes;
    private final double probability;
    private final boolean includeResponses;
    private final boolean nonBlocking;

    public WorkloadFileHeader(int maxSeconds, int maxMBytes, double probability, boolean includeResponses, boolean nonBlocking)
    {
        this.timestamp = System.currentTimeMillis();
        this.version = FBUtilities.getReleaseVersionString();
        broadcastAddress = FBUtilities.getBroadcastAddress();
        localAddress = FBUtilities.getLocalAddress();
        this.maxSeconds = maxSeconds;
        this.maxMBytes = maxMBytes;
        this.probability = probability;
        this.includeResponses = includeResponses;
        this.nonBlocking = nonBlocking;
    }

    public WorkloadFileHeader(DataInputStream input) throws IOException
    {
        if (input.readShort() != MAGIC)
            throw new IllegalArgumentException("Not a workload recorder file");
        short fileVersion = input.readShort();
        if (fileVersion != CURRENT_VERSION)
            throw new IllegalArgumentException(String.format("Unsupport workload recorder file version %x", fileVersion));
        timestamp = input.readLong();
        version = new String(readByteArray(input));
        try
        {
            broadcastAddress = InetAddress.getByAddress(readByteArray(input));
            localAddress = InetAddress.getByAddress(readByteArray(input));
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
        maxSeconds = input.readInt();
        maxMBytes = input.readInt();
        probability = input.readDouble();
        includeResponses = input.readBoolean();
        nonBlocking = input.readBoolean();
    }

    public void encode(ByteBuf headerBuffer)
    {
        headerBuffer.writeShort(MAGIC); // some magic
        headerBuffer.writeShort(CURRENT_VERSION); // some version
        headerBuffer.writeLong(timestamp); // timestamp (millis)
        writeByteArray(headerBuffer, version.getBytes());
        writeByteArray(headerBuffer, broadcastAddress.getAddress());
        writeByteArray(headerBuffer, localAddress.getAddress());
        headerBuffer.writeInt(maxSeconds);
        headerBuffer.writeInt(maxMBytes);
        headerBuffer.writeDouble(probability);
        headerBuffer.writeBoolean(includeResponses);
        headerBuffer.writeBoolean(nonBlocking);
    }

    private static void writeByteArray(ByteBuf headerBuffer, byte[] array)
    {
        headerBuffer.writeShort(array.length);
        headerBuffer.writeBytes(array);
    }

    private static byte[] readByteArray(DataInputStream input) throws IOException
    {
        int len = input.readUnsignedShort();
        byte[] arr = new byte[len];
        input.readFully(arr);
        return arr;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public String getVersion()
    {
        return version;
    }

    public InetAddress getBroadcastAddress()
    {
        return broadcastAddress;
    }

    public InetAddress getLocalAddress()
    {
        return localAddress;
    }

    public int getMaxSeconds()
    {
        return maxSeconds;
    }

    public int getMaxMBytes()
    {
        return maxMBytes;
    }

    public double getProbability()
    {
        return probability;
    }

    public boolean isIncludeResponses()
    {
        return includeResponses;
    }

    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("timestamp", timestamp)
                          .add("version", version)
                          .add("broadcastAddress", broadcastAddress)
                          .add("localAddress", localAddress)
                          .add("maxSeconds", maxSeconds)
                          .add("maxMBytes", maxMBytes)
                          .add("probability", probability)
                          .add("includeResponses", includeResponses)
                          .add("nonBlocking", nonBlocking)
                          .toString();
    }
}
