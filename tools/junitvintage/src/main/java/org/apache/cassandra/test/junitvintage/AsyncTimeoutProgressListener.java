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
package org.apache.cassandra.test.junitvintage;

import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;

public class AsyncTimeoutProgressListener implements TestExecutionListener
{
    @Override
    public void executionSkipped(TestIdentifier testIdentifier, String reason)
    {
        AsyncTimeoutVintageEngine.testAliveCallback(testIdentifier.getUniqueId());
    }

    @Override
    public void executionStarted(TestIdentifier testIdentifier)
    {
        AsyncTimeoutVintageEngine.testAliveCallback(testIdentifier.getUniqueId());
    }

    @Override
    public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult testExecutionResult)
    {
        AsyncTimeoutVintageEngine.testAliveCallback(testIdentifier.getUniqueId());
    }
}
