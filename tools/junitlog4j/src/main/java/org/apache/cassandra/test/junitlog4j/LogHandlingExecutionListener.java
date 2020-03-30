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
package org.apache.cassandra.test.junitlog4j;

import java.net.URL;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.util.ContextInitializer;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.support.descriptor.ClassSource;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogHandlingExecutionListener implements TestExecutionListener
{
    @Override
    public void executionStarted(TestIdentifier testIdentifier)
    {
        if (!testIdentifier.getSource().isPresent())
            return;

        TestSource source = testIdentifier.getSource().get();
        switch (testIdentifier.getType())
        {
            case CONTAINER:
                if (source instanceof ClassSource)
                {
                    ClassSource classSource = (ClassSource) source;
                    String className = classSource.getClassName();
                    System.setProperty("suitename", className);
                    new DeferredLogbackInit().logContainerStarted(className);
                }
                break;
            case TEST:
                if (source instanceof MethodSource)
                {
                    MethodSource methodSource = (MethodSource) source;
                    String className = methodSource.getClassName();
                    String testName = testIdentifier.getLegacyReportingName();

                    new DeferredLogbackInit().logTestStarted(className, testName);
                }
                break;
        }
    }

    /**
     * Class initialization is ... tricky.
     * We have to prevent that {@link #executionStarted(TestIdentifier)} initializes the {@link LoggerFactory}
     * class (i.e. initializes logback), so it does not get into the situation that the {@code suitename} system
     * property is not present and therefore creates a {@code TEST-suitename_IS_UNDEFINED.log} logfile (in addition
     * to the "right" log files).
     */
    private class DeferredLogbackInit
    {
        private void logContainerStarted(String className)
        {
            LoggerFactory.getLogger(className).info("\n\n############################################################\n" +
                                                    "\n                 Container started: {}\n\n\n", className);
        }

        private void logTestStarted(String className, String testName)
        {
            LoggerFactory.getLogger(className).info("\n\n############################################################\n" +
                                                    "\n                 Test started: {} - {}\n\n\n", className, testName);
        }
    }

    @Override
    public void executionSkipped(TestIdentifier testIdentifier, String reason)
    {
        if (!testIdentifier.getSource().isPresent())
            return;

        TestSource source = testIdentifier.getSource().get();
        switch (testIdentifier.getType())
        {
            case CONTAINER:
                if (source instanceof ClassSource)
                {
                    ClassSource classSource = (ClassSource) source;
                    String className = classSource.getClassName();
                    System.setProperty("suitename", className);

                    LoggerFactory.getLogger(className).info("\n\n########################################################################################################################\n" +
                                                            "\n                 Container skipped: {}", className);

                    // In case the same JVM runs multiple tests, reset the LoggerContext to reflect the changed suitename
                    // Can't put this in DeferredLogbackInit.logContainerStarted() as it would result in duplicate log entries
                    // (duplicate logback initialization)
                    reloadLoggerConfiguration();
                }
                break;
            case TEST:
                if (source instanceof MethodSource)
                {
                    MethodSource methodSource = (MethodSource) source;
                    String className = methodSource.getClassName();
                    String testName = testIdentifier.getLegacyReportingName();

                    LoggerFactory.getLogger(className).info("\n\n########################################################################################################################\n" +
                                                            "\n                 Test skipped: {} - {}", className, testName);
                }
                break;
        }
    }

    private static void reloadLoggerConfiguration()
    {
        try {
            LoggerContext loggerContext = (LoggerContext)LoggerFactory.getILoggerFactory();

            ContextInitializer contextInitializer = new ContextInitializer(loggerContext);
            URL url = contextInitializer.findURLOfDefaultConfigurationFile(true);

            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(loggerContext);
            configurator.doConfigure(url);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult testExecutionResult)
    {
        if (!testIdentifier.getSource().isPresent())
            return;

        TestSource source = testIdentifier.getSource().get();
        switch (testIdentifier.getType())
        {
            case CONTAINER:
                if (source instanceof ClassSource)
                {
                    ClassSource classSource = (ClassSource) source;
                    String className = classSource.getClassName();

                    LoggerFactory.getLogger(className).info("\n\n########################################################################################################################\n" +
                                                            "\n                 Container finished: {}", className);

                    // In case the same JVM runs multiple tests, reset the LoggerContext to reflect the changed suitename
                    // Can't put this in DeferredLogbackInit.logContainerStarted() as it would result in duplicate log entries
                    // (duplicate logback initialization)
                    reloadLoggerConfiguration();
                }
                break;
            case TEST:
                if (source instanceof MethodSource)
                {
                    MethodSource methodSource = (MethodSource) source;
                    String className = methodSource.getClassName();
                    String testName = testIdentifier.getLegacyReportingName();

                    Logger logger = LoggerFactory.getLogger(className);
                    if (!testExecutionResult.getThrowable().isPresent())
                        logger.info("\n\n########################################################################################################################\n" +
                                    "                 Test finished: {} : {} - {}", testExecutionResult.getStatus().name(), className, testName);
                    else
                        logger.error("\n\n########################################################################################################################\n" +
                                    "                 Test finished: {} : {} - {}", testExecutionResult.getStatus().name(), className, testName, testExecutionResult.getThrowable().get());
                }
                break;
        }
    }
}
