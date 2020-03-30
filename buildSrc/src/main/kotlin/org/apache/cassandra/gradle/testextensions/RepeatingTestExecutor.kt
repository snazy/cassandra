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
package org.apache.cassandra.gradle.testextensions

import org.gradle.api.internal.tasks.testing.*
import org.gradle.api.tasks.testing.Test
import java.util.*

internal class RepeatingTestExecutor(private val delegate: TestExecuter<JvmTestExecutionSpec>, private val repetitions: Int) : TestExecuter<JvmTestExecutionSpec> {
    override fun execute(testExecutionSpec: JvmTestExecutionSpec, testResultProcessor: TestResultProcessor) {
        val forkOptions = testExecutionSpec.javaForkOptions
        val savedSystemProps: Map<String, Any?> = HashMap(forkOptions.systemProperties)
        try {
            val wrappedProcessor = RepeatingTestResultProcessor(testResultProcessor)
            for (i in 0 until repetitions) {
                if (i == repetitions - 1)
                    wrappedProcessor.lastRun = true
                delegate.execute(testExecutionSpec, wrappedProcessor)
            }
        } finally {
            forkOptions.systemProperties = savedSystemProps
        }
    }

    override fun stopNow() {
        delegate.stopNow()
    }

    companion object {
        fun applyToTest(test: Test, repetitions: Int) {
            setTestExecutor(test, RepeatingTestExecutor(getOrCreateTestExecutor(test), repetitions))
        }

        private fun getOrCreateTestExecutor(test: Test): TestExecuter<JvmTestExecutionSpec> {
            return try {
                val createTestExecuter = Test::class.java.getDeclaredMethod("createTestExecuter")
                createTestExecuter.isAccessible = true
                @Suppress("UNCHECKED_CAST")
                createTestExecuter.invoke(test) as TestExecuter<JvmTestExecutionSpec>
            } catch (e: Exception) {
                throw RuntimeException(e)
            }
        }

        private fun setTestExecutor(test: Test, testExecuter: TestExecuter<JvmTestExecutionSpec>) {
            try {
                val setTestExecuter = Test::class.java.getDeclaredMethod("setTestExecuter", TestExecuter::class.java)
                setTestExecuter.isAccessible = true
                setTestExecuter.invoke(test, testExecuter)
            } catch (e: Exception) {
                throw RuntimeException(e)
            }
        }
    }
}

internal class RepeatingTestResultProcessor(private val delegate: TestResultProcessor) : TestResultProcessor by delegate {
    var lastRun: Boolean = false
    private var rootTestDescriptorId: Any? = null

    override fun started(descriptor: TestDescriptorInternal, testStartEvent: TestStartEvent) {
        if (rootTestDescriptorId == null) {
            rootTestDescriptorId = descriptor.id
            delegate.started(descriptor, testStartEvent)
        } else if (descriptor.id != rootTestDescriptorId) {
            delegate.started(descriptor, testStartEvent)
        }
    }

    override fun completed(testId: Any?, testCompleteEvent: TestCompleteEvent) {
        if (testId != rootTestDescriptorId || lastRun) {
            delegate.completed(testId, testCompleteEvent)
        }
    }
}
