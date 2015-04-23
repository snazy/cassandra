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

package org.apache.cassandra.test.microbench;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Uninterruptibles;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.Server;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = { "-Xms2048m", "-Xmx2048m", "-Dstorage-config=test/conf" })
@Threads(10)
@State(Scope.Benchmark)
public class ConcurrentPrepareBench
{

    public static final String KEYSPACE = "bench_test_keyspace";
    private static final String TABLE = "table_a";

    protected static int nativePort;
    protected static InetAddress nativeAddr;
    private Server server;
    private static final AtomicInteger tearDownLatch = new AtomicInteger();

    @Setup
    public void setup() throws Exception
    {
        tearDownLatch.incrementAndGet();

        if (server != null)
            return;

        System.err.println("Starting Cassandra");

        if (nativePort == 0)
        {
            nativeAddr = InetAddress.getLoopbackAddress();

            try
            {
                try (ServerSocket serverSocket = new ServerSocket(0))
                {
                    nativePort = serverSocket.getLocalPort();
                }
                Thread.sleep(250);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }

            System.setProperty("cassandra.native_transport_port", Integer.toString(nativePort));
            System.setProperty("com.sun.management.jmxremote.port", "99999");
        }

        try
        {
            try
            {
                SchemaLoader.cleanup();
            }
            catch (RuntimeException e)
            {
                // ignore
            }

            // Once per-JVM is enough
            SchemaLoader.prepareServer();

            SystemKeyspace.finishStartup();
            StorageService.instance.initServer();
            SchemaLoader.startGossiper();

            server = new org.apache.cassandra.transport.Server(nativeAddr, nativePort);
            server.start();

            try (Cluster cluster = Cluster.builder()
                                          .addContactPoints(nativeAddr)
                                          .withPort(nativePort)
                                          .withProtocolVersion(ProtocolVersion.V3)
                                          .build())
            {
                try (Session session = cluster.connect())
                {
                    session.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
                    session.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s " +
                                                  "(pk int PRIMARY KEY, " +
                                                  "v_text text, " +
                                                  "v_int int, " +
                                                  "v_double double, " +
                                                  "v_long bigint)", KEYSPACE, TABLE));
                }
            }
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            throw new RuntimeException("STARTUP FAILURE", t);
        }
        System.err.println("Cassandra started");
    }

    @TearDown
    public void tearDown() throws Exception
    {
        if (tearDownLatch.decrementAndGet() > 0)
            return;

        System.err.println("Shutdown Cassandra");

        try
        {
            server.stop();
            server = null;

            StorageService.instance.onShutdown();
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            throw new RuntimeException("STARTUP FAILURE", t);
        }
    }

    /**
     * State that contains a Java driver cluster and session per thread and
     * benchmark invocation.
     */
    @State(Scope.Thread)
    public static class ThreadState
    {
        Cluster cluster;
        Session session;

        @Setup(Level.Iteration)
        public void setup()
        {
            cluster = Cluster.builder()
                             .addContactPoints(nativeAddr)
                             .withPort(nativePort)
                             .withProtocolVersion(ProtocolVersion.V3)
                             .build();
            session = cluster.connect();
        }

        @TearDown(Level.Iteration)
        public void tearDown() throws Exception
        {
            session.close();
            cluster.close();
            Thread.sleep(1000L);
        }
    }

    @Benchmark
    public void prepareSingle(ThreadState state)
    {
        state.session.prepare(String.format("SELECT * FROM %s.%s", KEYSPACE, TABLE));
        state.session.prepare(String.format("SELECT pk FROM %s.%s", KEYSPACE, TABLE));
        state.session.prepare(String.format("SELECT v_text FROM %s.%s", KEYSPACE, TABLE));
        state.session.prepare(String.format("SELECT v_int FROM %s.%s", KEYSPACE, TABLE));
        state.session.prepare(String.format("SELECT v_double FROM %s.%s", KEYSPACE, TABLE));
        state.session.prepare(String.format("SELECT v_long FROM %s.%s", KEYSPACE, TABLE));
        state.session.prepare(String.format("SELECT pk, v_text FROM %s.%s", KEYSPACE, TABLE));
        state.session.prepare(String.format("SELECT pk, v_int FROM %s.%s", KEYSPACE, TABLE));
        state.session.prepare(String.format("SELECT pk, v_double FROM %s.%s", KEYSPACE, TABLE));
        state.session.prepare(String.format("SELECT pk, v_long FROM %s.%s", KEYSPACE, TABLE));
        state.session.prepare(String.format("SELECT pk, v_text, v_int, v_double, v_long FROM %s.%s", KEYSPACE, TABLE));
    }

    @Benchmark
    public void prepareConcurrent(ThreadState state) throws Exception
    {
        List<Future> futures = new ArrayList<>();
        futures.add(state.session.prepareAsync(String.format("SELECT * FROM %s.%s", KEYSPACE, TABLE)));
        futures.add(state.session.prepareAsync(String.format("SELECT pk FROM %s.%s", KEYSPACE, TABLE)));
        futures.add(state.session.prepareAsync(String.format("SELECT v_text FROM %s.%s", KEYSPACE, TABLE)));
        futures.add(state.session.prepareAsync(String.format("SELECT v_int FROM %s.%s", KEYSPACE, TABLE)));
        futures.add(state.session.prepareAsync(String.format("SELECT v_double FROM %s.%s", KEYSPACE, TABLE)));
        futures.add(state.session.prepareAsync(String.format("SELECT v_long FROM %s.%s", KEYSPACE, TABLE)));
        futures.add(state.session.prepareAsync(String.format("SELECT pk, v_text FROM %s.%s", KEYSPACE, TABLE)));
        futures.add(state.session.prepareAsync(String.format("SELECT pk, v_int FROM %s.%s", KEYSPACE, TABLE)));
        futures.add(state.session.prepareAsync(String.format("SELECT pk, v_double FROM %s.%s", KEYSPACE, TABLE)));
        futures.add(state.session.prepareAsync(String.format("SELECT pk, v_long FROM %s.%s", KEYSPACE, TABLE)));
        futures.add(state.session.prepareAsync(String.format("SELECT pk, v_text, v_int, v_double, v_long FROM %s.%s", KEYSPACE, TABLE)));
        for (Future future : futures)
        {
            Uninterruptibles.getUninterruptibly(future);
        }
    }

    @Benchmark
    public void prepareMulti(ThreadState state) throws Exception
    {
        List<String> l = Arrays.asList(
                                      String.format("SELECT * FROM %s.%s", KEYSPACE, TABLE),
                                      String.format("SELECT pk FROM %s.%s", KEYSPACE, TABLE),
                                      String.format("SELECT v_text FROM %s.%s", KEYSPACE, TABLE),
                                      String.format("SELECT v_int FROM %s.%s", KEYSPACE, TABLE),
                                      String.format("SELECT v_double FROM %s.%s", KEYSPACE, TABLE),
                                      String.format("SELECT v_long FROM %s.%s", KEYSPACE, TABLE),
                                      String.format("SELECT pk, v_text FROM %s.%s", KEYSPACE, TABLE),
                                      String.format("SELECT pk, v_int FROM %s.%s", KEYSPACE, TABLE),
                                      String.format("SELECT pk, v_double FROM %s.%s", KEYSPACE, TABLE),
                                      String.format("SELECT pk, v_long FROM %s.%s", KEYSPACE, TABLE),
                                      String.format("SELECT pk, v_text, v_int, v_double, v_long FROM %s.%s", KEYSPACE, TABLE)
        );

        Uninterruptibles.getUninterruptibly(state.session.prepareMultipleAsync(l));
    }
}
