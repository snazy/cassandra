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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import ch.qos.logback.core.rolling.RollingFileAppender;

/**
 * Lazily evaluates the actual log file name. No functional difference to the previous method (i.e. via build.xml),
 * but helps with Gradle as the test class name is not known when the test-worker JVM starts.
 * Works with {@link LogHandlingExecutionListener} to get the {@code suitename} system property.
 *
 * Log information that arrives before the test class name (i.e. the {@code suitename} system property) is known
 * will be buffered in memory.
 */
public class PerSuiteRollingFileAppender<E> extends RollingFileAppender<E>
{
    /**
     * Buffer for log information that arrives before the test class name is known.
     * Can be log information that's generated during test discovery (generate data for parameterized tests).
     */
    private volatile ByteArrayOutputStream memBuffer;

    @Override
    public String getFile()
    {
        String fn = super.getFile();
        String suitename = System.getProperty("suitename");
        if (suitename != null)
            fn = makeLogFileName(fn, suitename);
        return fn;
    }

    @Override
    public void openFile(String file_name) throws IOException
    {
        String suitename = System.getProperty("suitename");
        ByteArrayOutputStream out = memBuffer;
        if (suitename != null)
        {
            file_name = makeLogFileName(file_name, suitename);

            super.openFile(file_name);

            if (out != null)
            {
                getOutputStream().write(memBuffer.toByteArray());
                memBuffer = null;
            }
        }
        else
        {
            if (out == null)
            {
                out = memBuffer = new ByteArrayOutputStream();
                setOutputStream(out);
            }
        }
    }

    private String makeLogFileName(String fn, String suitename)
    {
        return fn.replace("%suitename%", suitename);
    }
}
