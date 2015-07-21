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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.transport.Connection;
import org.apache.cassandra.transport.Frame;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.utils.MD5Digest;

final class WorkloadSampler
{
    private static final Logger logger = LoggerFactory.getLogger(WorkloadSampler.class);

    private static final int bufferSize = 65536;

    private ByteBuffer writeBuffer;
    private ByteBuffer flushBuffer;
    private Future<Integer> currentFlush;
    private final ByteBuf headerBuffer;
    private final AsynchronousFileChannel channel;
    private final File file;
    private long bytesWritten;

    private final double probability;
    private final long maxBytes;
    private final boolean includeResponses;
    private final boolean nonBlocking;

    private final long startTimeNanos;

    private boolean closed;
    private long bytesBuffered;

    private final ReentrantLock lock = new ReentrantLock();

    WorkloadSampler(File file, int recordMaxSeconds, int maxMBytes, double probability, boolean includeResponses, boolean nonBlocking) throws IOException
    {
        long maxBytes = maxMBytes * 1024L * 1024L;
        long recordUntilTimestamp = recordMaxSeconds > 0 ? System.currentTimeMillis() + recordMaxSeconds * 1000L : 0L;
        probability = probability == 0d ? 1d : probability;

        //

        this.file = file;
        this.maxBytes = maxBytes;
        this.probability = probability;
        this.includeResponses = includeResponses;
        this.nonBlocking = nonBlocking;

        channel = AsynchronousFileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        headerBuffer = UnpooledByteBufAllocator.DEFAULT.heapBuffer(8192); // enough room to serialize a whole pstmt and schema table row
        writeBuffer = ByteBuffer.allocate(bufferSize);
        flushBuffer = ByteBuffer.allocate(bufferSize);

        // workload recording file header
        new WorkloadFileHeader(recordMaxSeconds, maxMBytes, probability, includeResponses, nonBlocking).encode(headerBuffer);
        writeByteBuf(headerBuffer);

        writeSchema();
        writePreparedStatements();

        headerBuffer.writeInt(WorkloadSampling.OPCODE_START_DATA);
        writeByteBuf(headerBuffer);

        startTimeNanos = System.nanoTime();

        if (recordUntilTimestamp > 0)
            ScheduledExecutors.nonPeriodicTasks.schedule(() -> close("record-until timestamp has been reached."),
                                                         Math.max(1000, recordUntilTimestamp - System.currentTimeMillis()),
                                                         TimeUnit.MILLISECONDS);

        logger.info("Start workload recording to file '{}'", file.getName());
    }

    private void writePreparedStatements()
    {
        // write prepared statements
        // for each prepared statement:
        //      int     OPCODE_PREPARED_STATEMENT
        //      bytes   MD5 digest
        //      string  query string (CQL statement)
        for (Map.Entry<MD5Digest, ParsedStatement.Prepared> entry : QueryProcessor.preparedStatements().entrySet())
        {
            String queryString = entry.getValue().queryString;
            if (queryString == null)
                continue;

            headerBuffer.writeInt(WorkloadSampling.OPCODE_PREPARED_STATEMENT);
            writeBytes(headerBuffer, entry.getKey().bytes);
            writeString(headerBuffer, entry.getValue().queryString);
            writeByteBuf(headerBuffer);
        }
    }

    private void writeSchema()
    {
        // write schema
        // for each keyspace
        //      int     OPCODE_KEYSPACE_DEFINITION
        //      for-each { table in orderedSchemaTables }
        //          int     number of rows of SELECT over keyspace
        //          for-each { row in rows }
        //              int     number of columns
        //              bytes   column name
        //              bytes   value (serialized using the protocol version for the current C* version)
        for (String ksName : Schema.instance.getKeyspaces())
        {
            if (Schema.isSystemKeyspace(ksName)
                || AuthKeyspace.NAME.equals(ksName)
                || TraceKeyspace.NAME.equals(ksName)
                || SystemDistributedKeyspace.NAME.equals(ksName))
                continue;

            headerBuffer.writeInt(WorkloadSampling.OPCODE_KEYSPACE_DEFINITION);
            for (String schemaTable : WorkloadSampling.orderedSchemaTables)
            {
                writeSchemaTable(ksName, schemaTable);
            }
        }
        writeByteBuf(headerBuffer);
    }

    private void writeSchemaTable(String ksName, String schemaTable)
    {
        UntypedResultSet rows = QueryProcessor.executeInternal(String.format("SELECT * FROM %s.%s WHERE keyspace_name=?",
                                                                             SchemaKeyspace.NAME, schemaTable),
                                                               ksName);
        assert rows != null;
        headerBuffer.writeInt(rows.size());
        rows.forEach((row) -> {
            headerBuffer.writeInt(row.getColumns().size());
            for (ColumnSpecification columnSpecification : row.getColumns())
            {
                writeString(headerBuffer, columnSpecification.name.toString());
                writeBytes(headerBuffer, row.getBytes(columnSpecification.name.toString()));
            }
            writeByteBuf(headerBuffer);
        });
    }

    private static void writeString(ByteBuf headerBuffer, String str)
    {
        writeBytes(headerBuffer, str == null
                                 ? null
                                 : str.getBytes(StandardCharsets.UTF_8));
    }

    private static void writeBytes(ByteBuf headerBuffer, byte[] bytes)
    {
        if (bytes == null)
            headerBuffer.writeInt(-1);
        else
        {
            headerBuffer.writeInt(bytes.length);
            headerBuffer.writeBytes(bytes);
        }
    }

    private static void writeBytes(ByteBuf headerBuffer, ByteBuffer bytes)
    {
        if (bytes == null)
            headerBuffer.writeInt(-1);
        else
        {
            headerBuffer.writeInt(bytes.remaining());
            headerBuffer.writeBytes(bytes);
        }
    }

    boolean isClosed()
    {
        return closed;
    }

    void record(Connection connection, Frame frame)
    {
        if (probability < 1d && ThreadLocalRandom.current().nextDouble(1d) < probability)
            return;

        if (!includeResponses && frame.header.type.direction == Message.Direction.RESPONSE)
            return;

        long offsetNanos = System.nanoTime() - startTimeNanos;
        InetSocketAddress isa = (InetSocketAddress) connection.channel().remoteAddress();
        byte[] remoteAddress = isa.getAddress().getAddress();

        // Intention of non-blocking mode is to prevent requests or responses "hanging" here waiting for
        // too-slow sampling file I/O.
        if (nonBlocking)
        {
            try
            {
                // 1 millisecond - do not give up too fast
                if (!lock.tryLock(1, TimeUnit.MILLISECONDS))
                    return;
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                return;
            }
        }
        else
        {
            // The default mode is to block, assuming there is enough I/O bandwidth for sampling file I/O.
            lock.lock();
        }
        try
        {
            if (closed)
                return;

            // write workload header:
            // short    length of timestamp + remote port + remote address bytes
            // long     offset in nanoseconds since start of recording
            // short    remote port
            // bytes    remote address
            headerBuffer.clear();
            headerBuffer.writeShort(8 + remoteAddress.length + 2);
            headerBuffer.writeLong(offsetNanos);
            headerBuffer.writeShort(isa.getPort());
            headerBuffer.writeBytes(remoteAddress);
            //
            // write native protocol header
            Frame.Encoder.encode(frame, headerBuffer);
            writeByteBuf(headerBuffer);
            //
            // write native protocol body
            writeByteBuf(frame.body);

            if (maxBytes != 0L && bytesBuffered >= maxBytes)
            {
                close("max-bytes to record has been reached.");
            }
        }
        finally
        {
            lock.unlock();
        }
    }

    // must be called while holing 'lock'
    private void writeByteBuf(ByteBuf body)
    {
        checkCurrentFlush();

        int readIndex = body.readerIndex();
        int toWrite = body.readableBytes();
        int avail = writeBuffer.remaining();
        while (toWrite > 0)
        {
            if (avail > toWrite)
            {
                // writeBuffer has enough free space to take whole frame.body
                int limit = writeBuffer.limit();
                int pos = writeBuffer.position();
                writeBuffer.limit(pos + toWrite);
                body.getBytes(readIndex, writeBuffer);
                readIndex += toWrite;
                writeBuffer.limit(limit);
                break;
            }

            body.getBytes(readIndex, writeBuffer);
            readIndex += avail;
            toWrite -= avail;
            flushWriteBuffer();
            avail = writeBuffer.remaining();
        }
        body.readerIndex(readIndex);
    }

    void close(String reason)
    {
        if (reason != null)
            logger.info("Closing workload recorder file '{}' since {}", file.getName(), reason);

        lock.lock();
        try
        {
            if (closed)
                return;

            // flush two times (one time for each buffer)
            flushWriteBuffer();
            flushWriteBuffer();

            channel.close();
        }
        catch (IOException e)
        {
            logger.error("Closing workload record file failed", e);
        }
        finally
        {
            closed = true;
            lock.unlock();
        }
    }

    /**
     * Used to abort a never-active recorder in WorkloadRecording
     */
    void abort()
    {
        try
        {
            channel.close();
        }
        catch (IOException e)
        {
            // ignore
        }
        file.delete();
    }

    // must be called while holing 'lock'
    private void flushWriteBuffer()
    {
        if (!closed)
        {
            while (currentFlush != null)
                completeCurrentFlush();

            bytesBuffered += writeBuffer.position();

            // get buffer to flush and flip to other buffer for buffering
            logger.debug("switch current buffer");
            ByteBuffer c = writeBuffer;
            writeBuffer = flushBuffer;
            writeBuffer.clear();

            flushBuffer = c;
            c.flip();
            currentFlush = channel.write(c, bytesWritten);
        }
    }

    // must be called while holing 'lock'
    private void checkCurrentFlush()
    {
        if (currentFlush == null)
            return;

        if (currentFlush.isDone())
            completeCurrentFlush();
    }

    // must be called while holing 'lock'
    private void completeCurrentFlush()
    {
        try
        {
            Integer wr = currentFlush.get();
            logger.debug("async write completed {}", wr);
            bytesWritten += wr;
            if (flushBuffer.remaining() > 0)
            {
                currentFlush = channel.write(flushBuffer, bytesWritten);
            }
            else
            {
                currentFlush = null;
            }
        }
        catch (InterruptedException | ExecutionException e)
        {
            logger.error("Failed to flush workload recorder buffer", e);
            close("Failure: " + e);
        }
    }
}
