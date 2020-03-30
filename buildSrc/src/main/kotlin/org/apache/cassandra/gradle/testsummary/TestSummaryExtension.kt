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

package org.apache.cassandra.gradle.testsummary

import org.gradle.api.Project
import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.testing.TestDescriptor
import org.gradle.api.tasks.testing.TestListener
import org.gradle.api.tasks.testing.TestResult
import org.gradle.kotlin.dsl.withType
import java.time.Duration
import java.util.*

open class TestSummaryExtension(val project: Project) {
    fun configureTestTasks(project: Project) = project.run {
        tasks.withType<Test>().configureEach {
            val test = this
            addTestListener(SummaryTestListener(test))
        }
    }

    inner class SummaryTestListener(val test: Test) : TestListener {
        override fun beforeTest(testDescriptor: TestDescriptor?) {
        }

        override fun afterSuite(suite: TestDescriptor?, result: TestResult?) {
            if (result != null && suite != null) {
                if (result.testCount > 0 && suite.parent == null) {
                    handleResult(test, suite, result)
                }
            }
        }

        override fun beforeSuite(suite: TestDescriptor?) {
        }

        override fun afterTest(testDescriptor: TestDescriptor?, result: TestResult?) {
        }

        private fun handleResult(test: Test, suite: TestDescriptor, result: TestResult) {
            val summary = """
                    |  ${suite.name}
                    |  Result: ${result.resultType}  (${result.testCount} tests, ${result.successfulTestCount} successes, ${result.failedTestCount} failures, ${result.skippedTestCount} skipped, ${result.exceptions.size} exceptions)
                    |  Duration: ${Duration.ofMillis(result.endTime - result.startTime).toString().substring(2).toLowerCase(Locale.ENGLISH)}
                    |  maxParallelForks: ${test.maxParallelForks}
                """.trimIndent()

            test.logger.lifecycle("${"-".repeat(120)}\n$summary\n${"-".repeat(120)}")
        }
    }
}