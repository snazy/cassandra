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
package org.apache.cassandra.cql3.functions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.*;
import java.nio.ByteBuffer;
import java.security.*;
import java.security.cert.Certificate;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.script.*;

import com.datastax.driver.core.DataType;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.FunctionExecutionException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.JVMStabilityInspector;

final class ScriptBasedUDFunction extends UDFunction
{

    static final Map<String, Compilable> scriptEngines = new HashMap<>();

    private static final ProtectionDomain protectionDomain;
    private static final AccessControlContext accessControlContext;

    //
    // For scripted UDFs we have to rely on the security mechanisms of the scripting engine and
    // SecurityManager - especially SecurityManager.checkPackageAccess(). Unlike Java-UDFs, strict checking
    // of class access via the UDF class loader is not possible, since e.g. Nashorn builds its own class loader
    // (jdk.nashorn.internal.runtime.ScriptLoader / jdk.nashorn.internal.runtime.NashornLoader) configured with
    // a system class loader.
    //
    private static final String[] allowedPackagesArray =
    {
    // following required by jdk.nashorn.internal.objects.Global.initJavaAccess()
    "",
    "com",
    "edu",
    "java",
    "javax",
    "javafx",
    "org",
    // following required by Nashorn runtime
    "java.lang",
    "java.lang.invoke",
    "java.lang.reflect",
    "java.nio.charset",
    "java.util",
    "java.util.concurrent",
    "javax.script",
    "sun.reflect",
    "jdk.internal.org.objectweb.asm.commons",
    "jdk.nashorn.internal.runtime",
    "jdk.nashorn.internal.runtime.linker",
    // following required by Java Driver
    "java.math",
    "java.nio",
    "java.text",
    "com.google.common.base",
    "com.google.common.collect",
    "com.google.common.reflect",
    // following required by UDF
    "com.datastax.driver.core",
    "com.datastax.driver.core.utils"
    };

    // use a JVM standard ExecutorService as DebuggableThreadPoolExecutor references internal
    // classes, which triggers AccessControlException from the UDF sandbox
    private static final UDFExecutorService executor =
        new UDFExecutorService(new NamedThreadFactory("UserDefinedScriptFunctions",
                                                      Thread.MIN_PRIORITY,
                                                      udfClassLoader,
                                                      new SecurityThreadGroup("UserDefinedScriptFunctions",
                                                                              Collections.unmodifiableSet(new HashSet<>(Arrays.asList(allowedPackagesArray))),
                                                                              UDFunction::initializeThread)),
                               "userscripts");

    static
    {
        ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
        for (ScriptEngineFactory scriptEngineFactory : scriptEngineManager.getEngineFactories())
        {
            ScriptEngine scriptEngine = scriptEngineFactory.getScriptEngine();
            boolean compilable = scriptEngine instanceof Compilable;
            if (compilable)
            {
                logger.info("Found scripting engine {} {} - {} {} - language names: {}",
                            scriptEngineFactory.getEngineName(), scriptEngineFactory.getEngineVersion(),
                            scriptEngineFactory.getLanguageName(), scriptEngineFactory.getLanguageVersion(),
                            scriptEngineFactory.getNames());
                for (String name : scriptEngineFactory.getNames())
                    scriptEngines.put(name, (Compilable) scriptEngine);
            }
        }

        try
        {
            protectionDomain = new ProtectionDomain(new CodeSource(new URL("udf", "localhost", 0, "/script", new URLStreamHandler()
            {
                protected URLConnection openConnection(URL u)
                {
                    return null;
                }
            }), (Certificate[]) null), ThreadAwareSecurityManager.noPermissions);
        }
        catch (MalformedURLException e)
        {
            throw new RuntimeException(e);
        }
        accessControlContext = new AccessControlContext(new ProtectionDomain[]{ protectionDomain });
    }

    private final CompiledScript script;

    ScriptBasedUDFunction(FunctionName name,
                          List<ColumnIdentifier> argNames,
                          List<AbstractType<?>> argTypes,
                          AbstractType<?> returnType,
                          boolean calledOnNullInput,
                          String language,
                          String body,
                          boolean trusted)
    {
        super(name, argNames, argTypes, returnType, calledOnNullInput, language, body, trusted);

        Compilable scriptEngine = scriptEngines.get(language);
        if (scriptEngine == null)
            throw new InvalidRequestException(String.format("Invalid language '%s' for function '%s'", language, name));

        this.script = trusted
                      ? setupTrusted(scriptEngine, body)
                      : setupUntrusted(scriptEngine, body);

    }

    private CompiledScript setupTrusted(Compilable scriptEngine, String body)
    {
        // execute compilation with no-permissions to prevent evil code e.g. via "static code blocks" / "class initialization"
        try
        {
            return scriptEngine.compile(body);
        }
        catch (ScriptException e)
        {
            logger.info("Failed to compile function '{}' for language {}: ", name, language, e);
            throw new InvalidRequestException(
                                             String.format("Failed to compile function '%s' for language %s: %s", name, language, e));
        }
    }

    private CompiledScript setupUntrusted(Compilable scriptEngine, String body)
    {
        // execute compilation with no-permissions to prevent evil code e.g. via "static code blocks" / "class initialization"
        try
        {
            return AccessController.doPrivileged((PrivilegedExceptionAction<CompiledScript>) () -> scriptEngine.compile(body),
                                                 accessControlContext);
        }
        catch (PrivilegedActionException x)
        {
            Throwable e = x.getCause();
            logger.info("Failed to compile function '{}' for language {}: ", name, language, e);
            throw new InvalidRequestException(
                                             String.format("Failed to compile function '%s' for language %s: %s", name, language, e));
        }
    }

    protected ByteBuffer executeAsync(int protocolVersion, List<ByteBuffer> parameters)
    {
        QuotaHolder quotaHolder = new QuotaHolder();

        Future<ByteBuffer> future = executor.submit(() -> {
            quotaHolder.setup();
            return executeUserDefined(protocolVersion, parameters);
        });

        try
        {
            if (DatabaseDescriptor.getUserDefinedFunctionWarnTimeout() > 0)
                try
                {
                    return future.get(DatabaseDescriptor.getUserDefinedFunctionWarnTimeout(), TimeUnit.MILLISECONDS);
                }
                catch (TimeoutException e)
                {
                    // log and emit a warning that UDF execution took long
                    String warn = String.format("User defined function %s ran longer than %dms", this, DatabaseDescriptor.getUserDefinedFunctionWarnTimeout());
                    logger.warn(warn);
                    ClientWarn.warn(warn);
                }

            // retry with difference of warn-timeout to fail-timeout
            return future.get(DatabaseDescriptor.getUserDefinedFunctionFailTimeout() - DatabaseDescriptor.getUserDefinedFunctionWarnTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            Throwable c = e.getCause();
            if (c instanceof RuntimeException)
                throw (RuntimeException) c;
            throw new RuntimeException(c);
        }
        catch (TimeoutException e)
        {
            // retry a last time with the difference of UDF-fail-timeout to consumed CPU time (just in case execution hit a badly timed GC)
            try
            {
                long cpuTimeMillis = quotaHolder.quotaState == null ? 0L : quotaHolder.quotaState.runTimeNanos() / 1000000L;

                return future.get(Math.max(DatabaseDescriptor.getUserDefinedFunctionFailTimeout() - cpuTimeMillis, 0L),
                                  TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e1)
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            catch (ExecutionException e1)
            {
                Throwable c = e.getCause();
                if (c instanceof RuntimeException)
                    throw (RuntimeException) c;
                throw new RuntimeException(c);
            }
            catch (TimeoutException e1)
            {
                TimeoutException cause = new TimeoutException(String.format("User defined function %s ran longer than %dms%s",
                                                                            this,
                                                                            DatabaseDescriptor.getUserDefinedFunctionFailTimeout(),
                                                                            DatabaseDescriptor.getUserFunctionFailPolicy() == Config.UserFunctionFailPolicy.ignore
                                                                            ? "" : " - will stop Cassandra VM"));
                FunctionExecutionException fe = FunctionExecutionException.create(this, cause);
                JVMStabilityInspector.userFunctionTimeout(cause);
                throw fe;
            }
        }
        finally
        {
            long threadBytes = quotaHolder.quotaState == null ? 0L : quotaHolder.quotaState.allocatedBytes();
            if (threadBytes > DatabaseDescriptor.getUserDefinedFunctionWarnMb() * 1024 * 1024)
            {
                String warn = String.format("User defined function %s allocated more than %dMB heap", this, DatabaseDescriptor.getUserDefinedFunctionWarnMb());
                logger.warn(warn);
                ClientWarn.warn(warn);
            }
            if (threadBytes > DatabaseDescriptor.getUserDefinedFunctionFailMb() * 1024 * 1024)
            {
                TimeoutException cause = new TimeoutException(String.format("User defined function %s allocated more than %dMB heap",
                                                                            this,
                                                                            DatabaseDescriptor.getUserDefinedFunctionFailMb()));
                throw FunctionExecutionException.create(this, cause);
            }
        }
    }

    // cannot be migrated to an AtomicReference since the _class_ needs reference to the code
    private static final class QuotaHolder
    {
        UDFQuotaState quotaState;

        QuotaHolder()
        {
            // Looks weird?
            // This call "just" links this class to java.lang.management - otherwise UDFs (script UDFs) might fail due to
            //      java.security.AccessControlException: access denied:
            //      ("java.lang.RuntimePermission" "accessClassInPackage.java.lang.management" /
            //      "accessClassInPackage.org.apache.cassandra.cql3.functions")
            // because class loading would be deferred until setup() is executed - but setup() is called with
            // limited privileges.
            UDFQuotaState.ensureInitialized();
            //
            // Get the TypeCodec stuff in Java Driver initialized.
            UDHelper.codecFor(DataType.inet()).format(InetAddress.getLoopbackAddress());
            UDHelper.codecFor(DataType.list(DataType.ascii())).format(Collections.emptyList());
        }

        void setup()
        {
            this.quotaState = new UDFQuotaState(Thread.currentThread().getId(),
                                                DatabaseDescriptor.getUserDefinedFunctionFailTimeout(),
                                                DatabaseDescriptor.getUserDefinedFunctionWarnTimeout(),
                                                DatabaseDescriptor.getUserDefinedFunctionFailMb(),
                                                DatabaseDescriptor.getUserDefinedFunctionWarnMb());
        }
    }

    public ByteBuffer executeUserDefined(int protocolVersion, List<ByteBuffer> parameters)
    {
        Object[] params = new Object[argTypes.size()];
        for (int i = 0; i < params.length; i++)
            params[i] = compose(protocolVersion, i, parameters.get(i));

        ScriptContext scriptContext = new SimpleScriptContext();
        scriptContext.setAttribute("javax.script.filename", this.name.toString(), ScriptContext.ENGINE_SCOPE);
        Bindings bindings = scriptContext.getBindings(ScriptContext.ENGINE_SCOPE);
        for (int i = 0; i < params.length; i++)
            bindings.put(argNames.get(i).toString(), params[i]);

        Object result;
        try
        {
            // How to prevent Class.forName() _without_ "help" from the script engine ?
            // NOTE: Nashorn enforces a special permission to allow class-loading, which is not granted - so it's fine.

            result = script.eval(scriptContext);
        }
        catch (ScriptException e)
        {
            throw new RuntimeException(e);
        }
        if (result == null)
            return null;

        Class<?> javaReturnType = UDHelper.asJavaClass(returnDataType);
        Class<?> resultType = result.getClass();
        if (!javaReturnType.isAssignableFrom(resultType))
        {
            if (result instanceof Number)
            {
                Number rNumber = (Number) result;
                if (javaReturnType == Integer.class)
                    result = rNumber.intValue();
                else if (javaReturnType == Long.class)
                    result = rNumber.longValue();
                else if (javaReturnType == Short.class)
                    result = rNumber.shortValue();
                else if (javaReturnType == Byte.class)
                    result = rNumber.byteValue();
                else if (javaReturnType == Float.class)
                    result = rNumber.floatValue();
                else if (javaReturnType == Double.class)
                    result = rNumber.doubleValue();
                else if (javaReturnType == BigInteger.class)
                {
                    if (javaReturnType == Integer.class)
                        result = rNumber.intValue();
                    else if (javaReturnType == Short.class)
                        result = rNumber.shortValue();
                    else if (javaReturnType == Byte.class)
                        result = rNumber.byteValue();
                    else if (javaReturnType == Long.class)
                        result = rNumber.longValue();
                    else if (javaReturnType == Float.class)
                        result = rNumber.floatValue();
                    else if (javaReturnType == Double.class)
                        result = rNumber.doubleValue();
                    else if (javaReturnType == BigInteger.class)
                    {
                        if (rNumber instanceof BigDecimal)
                            result = ((BigDecimal) rNumber).toBigInteger();
                        else if (rNumber instanceof Double || rNumber instanceof Float)
                            result = new BigDecimal(rNumber.toString()).toBigInteger();
                        else
                            result = BigInteger.valueOf(rNumber.longValue());
                    }
                    else if (javaReturnType == BigDecimal.class)
                        // String c'tor of BigDecimal is more accurate than valueOf(double)
                        result = new BigDecimal(rNumber.toString());
                }
                else if (javaReturnType == BigDecimal.class)
                    // String c'tor of BigDecimal is more accurate than valueOf(double)
                    result = new BigDecimal(rNumber.toString());
            }
        }

        return decompose(protocolVersion, result);
    }
}
