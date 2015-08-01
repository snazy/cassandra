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

package org.apache.cassandra.cql3.validation.entities;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.datastax.driver.core.DataType;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.functions.JavaUDFByteCodeVerifier;
import org.apache.cassandra.cql3.validation.entities.udfverify.CallClone;
import org.apache.cassandra.cql3.validation.entities.udfverify.CallComDatastax;
import org.apache.cassandra.cql3.validation.entities.udfverify.CallFinalize;
import org.apache.cassandra.cql3.validation.entities.udfverify.CallOrgApache;
import org.apache.cassandra.cql3.validation.entities.udfverify.ClassWithField;
import org.apache.cassandra.cql3.validation.entities.udfverify.ClassWithInitializer;
import org.apache.cassandra.cql3.validation.entities.udfverify.ClassWithInitializer2;
import org.apache.cassandra.cql3.validation.entities.udfverify.ClassWithInitializer3;
import org.apache.cassandra.cql3.validation.entities.udfverify.ClassWithStaticInitializer;
import org.apache.cassandra.cql3.validation.entities.udfverify.EndlessLoopClass;
import org.apache.cassandra.cql3.validation.entities.udfverify.GoodClass;
import org.apache.cassandra.cql3.validation.entities.udfverify.GotoEndlessLoopClass;
import org.apache.cassandra.cql3.validation.entities.udfverify.HugeAllocClass;
import org.apache.cassandra.cql3.validation.entities.udfverify.ManyHugeAllocClass;
import org.apache.cassandra.cql3.validation.entities.udfverify.NestedLoopsClass;
import org.apache.cassandra.cql3.validation.entities.udfverify.NestedTryCatchLoopsClass;
import org.apache.cassandra.cql3.validation.entities.udfverify.UseOfSynchronized;
import org.apache.cassandra.cql3.validation.entities.udfverify.UseOfSynchronizedWithNotify;
import org.apache.cassandra.cql3.validation.entities.udfverify.UseOfSynchronizedWithNotifyAll;
import org.apache.cassandra.cql3.validation.entities.udfverify.UseOfSynchronizedWithWait;
import org.apache.cassandra.cql3.validation.entities.udfverify.UseOfSynchronizedWithWaitL;
import org.apache.cassandra.cql3.validation.entities.udfverify.UseOfSynchronizedWithWaitLI;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.util.TraceClassVisitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test the Java UDF byte code verifier.
 */
public class UFVerifierTest extends CQLTester
{
    @Test
    public void testByteCodeVerifier()
    {
        new JavaUDFByteCodeVerifier().verify(readClass(GoodClass.class));
    }

    @Test
    public void testClassWithField()
    {
        assertEquals(new HashSet<>(Collections.singletonList("field declared: field")),
                     new JavaUDFByteCodeVerifier().verify(readClass(ClassWithField.class)).errors);
    }

    @Test
    public void testClassWithInitializer()
    {
        assertEquals(new HashSet<>(Arrays.asList("field declared: field",
                                                 "initializer declared")),
                     new JavaUDFByteCodeVerifier().verify(readClass(ClassWithInitializer.class)).errors);
    }

    @Test
    public void testClassWithInitializer2()
    {
        assertEquals(new HashSet<>(Arrays.asList("field declared: field",
                                                 "initializer declared")),
                     new JavaUDFByteCodeVerifier().verify(readClass(ClassWithInitializer2.class)).errors);
    }

    @Test
    public void testClassWithInitializer3()
    {
        assertEquals(new HashSet<>(Collections.singletonList("initializer declared")),
                     new JavaUDFByteCodeVerifier().verify(readClass(ClassWithInitializer3.class)).errors);
    }

    @Test
    public void testClassWithStaticInitializer()
    {
        assertEquals(new HashSet<>(Collections.singletonList("static initializer declared")),
                     new JavaUDFByteCodeVerifier().verify(readClass(ClassWithStaticInitializer.class)).errors);
    }

    @Test
    public void testUseOfSynchronized()
    {
        assertEquals(new HashSet<>(Collections.singletonList("use of synchronized")),
                     new JavaUDFByteCodeVerifier().verify(readClass(UseOfSynchronized.class)).errors);
    }

    @Test
    public void testUseOfSynchronizedWithNotify()
    {
        assertEquals(new HashSet<>(Arrays.asList("use of synchronized", "call to notify()")),
                     new JavaUDFByteCodeVerifier().verify(readClass(UseOfSynchronizedWithNotify.class)).errors);
    }

    @Test
    public void testUseOfSynchronizedWithNotifyAll()
    {
        assertEquals(new HashSet<>(Arrays.asList("use of synchronized", "call to notifyAll()")),
                     new JavaUDFByteCodeVerifier().verify(readClass(UseOfSynchronizedWithNotifyAll.class)).errors);
    }

    @Test
    public void testUseOfSynchronizedWithWait()
    {
        assertEquals(new HashSet<>(Arrays.asList("use of synchronized", "call to wait()")),
                     new JavaUDFByteCodeVerifier().verify(readClass(UseOfSynchronizedWithWait.class)).errors);
    }

    @Test
    public void testUseOfSynchronizedWithWaitL()
    {
        assertEquals(new HashSet<>(Arrays.asList("use of synchronized", "call to wait()")),
                     new JavaUDFByteCodeVerifier().verify(readClass(UseOfSynchronizedWithWaitL.class)).errors);
    }

    @Test
    public void testUseOfSynchronizedWithWaitI()
    {
        assertEquals(new HashSet<>(Arrays.asList("use of synchronized", "call to wait()")),
                     new JavaUDFByteCodeVerifier().verify(readClass(UseOfSynchronizedWithWaitLI.class)).errors);
    }

    @Test
    public void testCallClone()
    {
        assertEquals(new HashSet<>(Collections.singletonList("call to clone()")),
                     new JavaUDFByteCodeVerifier().verify(readClass(CallClone.class)).errors);
    }

    @Test
    public void testCallFinalize()
    {
        assertEquals(new HashSet<>(Collections.singletonList("call to finalize()")),
                     new JavaUDFByteCodeVerifier().verify(readClass(CallFinalize.class)).errors);
    }

    @Test
    public void testCallComDatastax()
    {
        assertEquals(new HashSet<>(Collections.singletonList("call to com/datastax/driver/core/DataType.cint()")),
                     new JavaUDFByteCodeVerifier().addDisallowedPackage("com/").verify(readClass(CallComDatastax.class)).errors);
    }

    @Test
    public void testCallOrgApache()
    {
        assertEquals(new HashSet<>(Collections.singletonList("call to org/apache/cassandra/config/DatabaseDescriptor.getClusterName()")),
                     new JavaUDFByteCodeVerifier().addDisallowedPackage("org/").verify(readClass(CallOrgApache.class)).errors);
    }

    @SuppressWarnings("resource")
    private static byte[] readClass(Class<?> clazz)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        URL res = clazz.getClassLoader().getResource(clazz.getName().replace('.', '/') + ".class");
        assert res != null;
        try (InputStream input = res.openConnection().getInputStream())
        {
            int i;
            while ((i = input.read()) != -1)
                out.write(i);
            return out.toByteArray();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testInvalidByteCodeUDFs() throws Throwable
    {
        assertInvalidByteCode("try\n" +
                              "{\n" +
                              "    clone();\n" +
                              "}\n" +
                              "catch (CloneNotSupportedException e)\n" +
                              "{\n" +
                              "    throw new RuntimeException(e);\n" +
                              "}\n" +
                              "return 0d;", "Java UDF validation failed: [call to clone()]");
        assertInvalidByteCode("try\n" +
                              "{\n" +
                              "    finalize();\n" +
                              "}\n" +
                              "catch (Throwable e)\n" +
                              "{\n" +
                              "    throw new RuntimeException(e);\n" +
                              "}\n" +
                              "return 0d;", "Java UDF validation failed: [call to finalize()]");
        assertInvalidByteCode('\n' +
                              "return 0d;\n" +
                              "    }\n" +
                              '\n' +
                              "    Object field;\n" +
                              '\n' +
                              "    {", "Java UDF validation failed: [field declared: field]");
        assertInvalidByteCode('\n' +
                              "return 0d;\n" +
                              "    }\n" +
                              '\n' +
                              "    final Object field;\n" +
                              '\n' +
                              "    {\n" +
                              "field = new Object();", "Java UDF validation failed: [field declared: field, initializer declared]");
        assertInvalidByteCode('\n' +
                              "return 0d;\n" +
                              "    }\n" +
                              '\n' +
                              "    Object field = new Object();\n" +
                              '\n' +
                              "    {\n" +
                              "Math.sin(1d);", "Java UDF validation failed: [field declared: field, initializer declared]");
        assertInvalidByteCode('\n' +
                              "return 0d;\n" +
                              "    }\n" +
                              '\n' +
                              "    {\n" +
                              "Math.sin(1d);", "Java UDF validation failed: [initializer declared]");
        assertInvalidByteCode('\n' +
                              "return 0d;\n" +
                              "    }\n" +
                              '\n' +
                              "    static\n" +
                              "    {\n" +
                              "Math.sin(1d);", "Java UDF validation failed: [static initializer declared]");
        assertInvalidByteCode("synchronized (this)\n" +
                              "{\n" +
                              "    Math.sin(1d);\n" +
                              "}\n" +
                              "return 0d;", "Java UDF validation failed: [use of synchronized]");
        assertInvalidByteCode("synchronized (this)\n" +
                              "{\n" +
                              "    notify();\n" +
                              "}\n" +
                              "return 0d;", "Java UDF validation failed: [call to notify(), use of synchronized]");
        assertInvalidByteCode("synchronized (this)\n" +
                              "{\n" +
                              "    notifyAll();\n" +
                              "}\n" +
                              "return 0d;", "Java UDF validation failed: [call to notifyAll(), use of synchronized]");
        assertInvalidByteCode("synchronized (this)\n" +
                              "{\n" +
                              "    try\n" +
                              "    {\n" +
                              "        wait();\n" +
                              "    }\n" +
                              "    catch (InterruptedException e)\n" +
                              "    {\n" +
                              "        throw new RuntimeException(e);\n" +
                              "    }\n" +
                              "}\n" +
                              "return 0d;", "Java UDF validation failed: [call to wait(), use of synchronized]");
        assertInvalidByteCode("synchronized (this)\n" +
                              "{\n" +
                              "    try\n" +
                              "    {\n" +
                              "        wait(1000L);\n" +
                              "    }\n" +
                              "    catch (InterruptedException e)\n" +
                              "    {\n" +
                              "        throw new RuntimeException(e);\n" +
                              "    }\n" +
                              "}\n" +
                              "return 0d;", "Java UDF validation failed: [call to wait(), use of synchronized]");
        assertInvalidByteCode("synchronized (this)\n" +
                              "{\n" +
                              "    try\n" +
                              "    {\n" +
                              "        wait(1000L, 100);\n" +
                              "    }\n" +
                              "    catch (InterruptedException e)\n" +
                              "    {\n" +
                              "        throw new RuntimeException(e);\n" +
                              "    }\n" +
                              "}\n" +
                              "return 0d;", "Java UDF validation failed: [call to wait(), use of synchronized]");
    }

    private void assertInvalidByteCode(String body, String error) throws Throwable
    {
        assertInvalidMessage(error,
                             "CREATE FUNCTION " + KEYSPACE + ".mustBeInvalid ( input double ) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS double " +
                             "LANGUAGE java AS $$" + body + "$$");
    }

    //
    // advanced timeout detection
    //

    static class UFVerifierTestClassLoader extends ClassLoader
    {
        private final byte[] bytecode;
        private final String name;

        UFVerifierTestClassLoader(String name, byte[] bytecode)
        {
            super(Thread.currentThread().getContextClassLoader());
            this.name = name;
            this.bytecode = bytecode;
        }

        public Class<?> loadClass(String name) throws ClassNotFoundException
        {
            if (this.name.equals(name))
                return defineClass(name, bytecode, 0, bytecode.length);
            return super.loadClass(name);
        }
    }

    private static void testAmokClass(Class<?> clazz) throws Exception
    {
        byte[] bytecode = readClass(clazz);

//        dumpBytecode("original " + clazz.getCanonicalName(), bytecode);

        JavaUDFByteCodeVerifier.ClassAndErrors result = new JavaUDFByteCodeVerifier().verify(bytecode);
        assertEquals(Collections.emptySet(), result.errors);

        dumpBytecode("transformed " + clazz.getCanonicalName(), result.bytecode);

        Class<?> secured = Class.forName(clazz.getName(),
                                         true,
                                         new UFVerifierTestClassLoader(clazz.getName(), result.bytecode));
        Method mExecImpl = secured.getDeclaredMethod("executeImpl", int.class, List.class);
        mExecImpl.setAccessible(true);
        Object instance = secured.getDeclaredConstructor(DataType.class, DataType[].class).newInstance(null, null);

        Class<?> cTimeoutHandler = Class.forName("org.apache.cassandra.cql3.functions.JavaUDFQuotaHandler");
        Method mBeforeStart = cTimeoutHandler.getDeclaredMethod("beforeStart", long.class, long.class, long.class, long.class);
        mBeforeStart.setAccessible(true);
        Method mUdfExecCall = cTimeoutHandler.getDeclaredMethod("udfExecCall");
        mUdfExecCall.setAccessible(true);

        ExecutorService executor = Executors.newFixedThreadPool(1);
        try
        {
            Future<Object> future = executor.submit(() -> {
                try
                {
                    mBeforeStart.invoke(null, 20L, 0L, 20L, 0L);
                    return mExecImpl.invoke(instance, 3, null);
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
                finally
                {
                    for (int i = 0; i < 16; i++)
                        if ((Boolean)mUdfExecCall.invoke(null))
                            throw new RuntimeException("failure detected");
                }
            });

            try
            {
                future.get(2500, TimeUnit.MILLISECONDS);
                fail();
            }
            catch (ExecutionException ee)
            {
                Throwable cause = ee.getCause();
                assertEquals("failure detected", cause.getMessage());
            }
        }
        finally
        {
            executor.shutdown();
        }
    }

    private static void dumpBytecode(String title, byte[] bytecode)
    {
        ClassReader classReader = new ClassReader(bytecode);

        System.out.println("******************* byte code for " + title);
        TraceClassVisitor traceClassVisitor = new TraceClassVisitor(new PrintWriter(System.out));
        classReader.accept(traceClassVisitor, 0);
    }

    @Test
    public void testEndlessLoopClass() throws Exception
    {
        testAmokClass(EndlessLoopClass.class);
    }

    @Test
    public void testGotoEndlessLoopClass() throws Exception
    {
        testAmokClass(GotoEndlessLoopClass.class);
    }

    @Test
    public void testNestedLoopsClass() throws Exception
    {
        testAmokClass(NestedLoopsClass.class);
    }

    @Test
    public void testNestedTryCatchLoopsClass() throws Exception
    {
        testAmokClass(NestedTryCatchLoopsClass.class);
    }

    @Test
    public void testAllocWarn() throws Exception
    {
        testAmokClass(HugeAllocClass.class);
    }

    @Test
    public void testManyAllocWarn() throws Exception
    {
        testAmokClass(ManyHugeAllocClass.class);
    }
}
