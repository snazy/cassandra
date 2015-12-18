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
/*
 * Copyright DataStax, Inc.
 *
 * Modified by DataStax, Inc.
 */
package org.apache.cassandra.cql3.validation.entities;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.concurrent.*;

import org.junit.Test;

import com.datastax.driver.core.TypeCodec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.functions.JavaUDFByteCodeVerifier;
import org.apache.cassandra.cql3.functions.UDFContext;
import org.apache.cassandra.cql3.validation.entities.udfverify.*;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.util.TraceClassVisitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test the Java UDF byte code verifier.
 */
public class UFVerifierTest extends CQLTester
{

    private static Set<String> verify(Class cls)
    {
        return new JavaUDFByteCodeVerifier().verifyAndInstrument(cls.getName(), readClass(cls)).errors;
    }

    private static Set<String> verify(String disallowedPkg, Class cls)
    {
        return new JavaUDFByteCodeVerifier().addDisallowedPackage(disallowedPkg).verifyAndInstrument(cls.getName(), readClass(cls)).errors;
    }

    @Test
    public void testByteCodeVerifier()
    {
        verify(GoodClass.class);
    }

    @Test
    public void testClassWithField()
    {
        assertEquals(new HashSet<>(Collections.singletonList("field declared: field")),
                     verify(ClassWithField.class));
    }

    @Test
    public void testClassWithInitializer()
    {
        assertEquals(new HashSet<>(Arrays.asList("field declared: field",
                                                 "initializer declared")),
                     verify(ClassWithInitializer.class));
    }

    @Test
    public void testClassWithInitializer2()
    {
        assertEquals(new HashSet<>(Arrays.asList("field declared: field",
                                                 "initializer declared")),
                     verify(ClassWithInitializer2.class));
    }

    @Test
    public void testClassWithInitializer3()
    {
        assertEquals(new HashSet<>(Collections.singletonList("initializer declared")),
                     verify(ClassWithInitializer3.class));
    }

    @Test
    public void testClassWithStaticInitializer()
    {
        assertEquals(new HashSet<>(Collections.singletonList("static initializer declared")),
                     verify(ClassWithStaticInitializer.class));
    }

    @Test
    public void testUseOfSynchronized()
    {
        assertEquals(new HashSet<>(Collections.singletonList("use of synchronized")),
                     verify(UseOfSynchronized.class));
    }

    @Test
    public void testUseOfSynchronizedWithNotify()
    {
        assertEquals(new HashSet<>(Arrays.asList("use of synchronized", "call to java.lang.Object.notify()")),
                     verify(UseOfSynchronizedWithNotify.class));
    }

    @Test
    public void testUseOfSynchronizedWithNotifyAll()
    {
        assertEquals(new HashSet<>(Arrays.asList("use of synchronized", "call to java.lang.Object.notifyAll()")),
                     verify(UseOfSynchronizedWithNotifyAll.class));
    }

    @Test
    public void testUseOfSynchronizedWithWait()
    {
        assertEquals(new HashSet<>(Arrays.asList("use of synchronized", "call to java.lang.Object.wait()")),
                     verify(UseOfSynchronizedWithWait.class));
    }

    @Test
    public void testUseOfSynchronizedWithWaitL()
    {
        assertEquals(new HashSet<>(Arrays.asList("use of synchronized", "call to java.lang.Object.wait()")),
                     verify(UseOfSynchronizedWithWaitL.class));
    }

    @Test
    public void testUseOfSynchronizedWithWaitI()
    {
        assertEquals(new HashSet<>(Arrays.asList("use of synchronized", "call to java.lang.Object.wait()")),
                     verify(UseOfSynchronizedWithWaitLI.class));
    }

    @Test
    public void testCallClone()
    {
        assertEquals(new HashSet<>(Collections.singletonList("call to java.lang.Object.clone()")),
                     verify(CallClone.class));
    }

    @Test
    public void testCallFinalize()
    {
        assertEquals(new HashSet<>(Collections.singletonList("call to java.lang.Object.finalize()")),
                     verify(CallFinalize.class));
    }

    @Test
    public void testCallComDatastax()
    {
        assertEquals(new HashSet<>(Collections.singletonList("call to com.datastax.driver.core.DataType.cint()")),
                     verify("com/", CallComDatastax.class));
    }

    @Test
    public void testCallOrgApache()
    {
        assertEquals(new HashSet<>(Collections.singletonList("call to org.apache.cassandra.config.DatabaseDescriptor.getClusterName()")),
                     verify("org/", CallOrgApache.class));
    }

    @Test
    public void testClassStaticInnerClass()
    {
        assertEquals(new HashSet<>(Collections.singletonList("class declared as inner class")),
                     verify(ClassWithStaticInnerClass.class));
    }

    @Test
    public void testUsingMapEntry()
    {
        assertEquals(Collections.emptySet(),
                     verify(UsingMapEntry.class));
    }

    @Test
    public void testClassInnerClass()
    {
        assertEquals(new HashSet<>(Collections.singletonList("class declared as inner class")),
                     verify(ClassWithInnerClass.class));
    }

    @Test
    public void testClassInnerClass2()
    {
        assertEquals(Collections.emptySet(),
                     verify(ClassWithInnerClass2.class));
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
                              "return 0d;", "Java UDF validation failed: [call to java.lang.Object.clone()]");
        assertInvalidByteCode("try\n" +
                              "{\n" +
                              "    finalize();\n" +
                              "}\n" +
                              "catch (Throwable e)\n" +
                              "{\n" +
                              "    throw new RuntimeException(e);\n" +
                              "}\n" +
                              "return 0d;", "Java UDF validation failed: [call to java.lang.Object.finalize()]");
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
                              "return 0d;", "Java UDF validation failed: [call to java.lang.Object.notify(), use of synchronized]");
        assertInvalidByteCode("synchronized (this)\n" +
                              "{\n" +
                              "    notifyAll();\n" +
                              "}\n" +
                              "return 0d;", "Java UDF validation failed: [call to java.lang.Object.notifyAll(), use of synchronized]");
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
                              "return 0d;", "Java UDF validation failed: [call to java.lang.Object.wait(), use of synchronized]");
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
                              "return 0d;", "Java UDF validation failed: [call to java.lang.Object.wait(), use of synchronized]");
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
                              "return 0d;", "Java UDF validation failed: [call to java.lang.Object.wait(), use of synchronized]");
        assertInvalidByteCode("try {" +
                              "     java.nio.ByteBuffer.allocateDirect(123); return 0d;" +
                              "} catch (Exception t) {" +
                              "     throw new RuntimeException(t);" +
                              '}', "Java UDF validation failed: [call to java.nio.ByteBuffer.allocateDirect()]");
        assertInvalidByteCode("try {" +
                              "     java.net.InetAddress.getLocalHost(); return 0d;" +
                              "} catch (Exception t) {" +
                              "     throw new RuntimeException(t);" +
                              '}', "Java UDF validation failed: [call to java.net.InetAddress.getLocalHost()]");
        assertInvalidByteCode("try {" +
                              "     java.net.InetAddress.getAllByName(\"localhost\"); return 0d;" +
                              "} catch (Exception t) {" +
                              "     throw new RuntimeException(t);" +
                              '}', "Java UDF validation failed: [call to java.net.InetAddress.getAllByName()]");
        assertInvalidByteCode("try {" +
                              "     java.net.Inet4Address.getByName(\"127.0.0.1\"); return 0d;" +
                              "} catch (Exception t) {" +
                              "     throw new RuntimeException(t);" +
                              '}', "Java UDF validation failed: [call to java.net.Inet4Address.getByName()]");
        assertInvalidByteCode("try {" +
                              "     java.net.Inet6Address.getByAddress(new byte[]{127,0,0,1}); return 0d;" +
                              "} catch (Exception t) {" +
                              "     throw new RuntimeException(t);" +
                              '}', "Java UDF validation failed: [call to java.net.Inet6Address.getByAddress()]");
        assertInvalidByteCode("try {" +
                              "     java.net.NetworkInterface.getNetworkInterfaces(); return 0d;" +
                              "} catch (Exception t) {" +
                              "     throw new RuntimeException(t);" +
                              '}', "Java UDF validation failed: [call to java.net.NetworkInterface.getNetworkInterfaces()]");
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

        JavaUDFByteCodeVerifier.ClassAndErrors result = new JavaUDFByteCodeVerifier().verifyAndInstrument(clazz.getName(), bytecode);
        assertEquals(Collections.emptySet(), result.errors);

        dumpBytecode("transformed " + clazz.getCanonicalName(), result.bytecode);

        Class<?> secured = Class.forName(clazz.getName(),
                                         true,
                                         new UFVerifierTestClassLoader(clazz.getName(), result.bytecode));
        Method mExecImpl = secured.getDeclaredMethod("executeImpl", int.class, List.class);
        mExecImpl.setAccessible(true);
        Object instance = secured.getDeclaredConstructor(TypeCodec.class, TypeCodec[].class, UDFContext.class).newInstance(null, null, null);

        Class<?> cQuotaState = Class.forName("org.apache.cassandra.cql3.functions.UDFQuotaState");
        Constructor<?> cQuotaStateCtor = cQuotaState.getDeclaredConstructor(long.class, long.class, long.class, long.class);
        cQuotaStateCtor.setAccessible(true);

        Class<?> cTimeoutHandler = Class.forName("org.apache.cassandra.cql3.functions.JavaUDFQuotaHandler");
        Method mBeforeStart = cTimeoutHandler.getDeclaredMethod("beforeStart", cQuotaState);
        mBeforeStart.setAccessible(true);
        Method mUdfExecCall = cTimeoutHandler.getDeclaredMethod("udfExecCall");
        mUdfExecCall.setAccessible(true);

        ExecutorService executor = Executors.newFixedThreadPool(1);
        try
        {
            Future<Object> future = executor.submit(() -> {
                try
                {
                    Object quotaState = cQuotaStateCtor.newInstance(20L, 0L, 20L, 0L);
                    mBeforeStart.invoke(null, quotaState);
                    return mExecImpl.invoke(instance, 3, null);
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
                finally
                {
                    for (int i = 0; i < 1000; i++)
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
