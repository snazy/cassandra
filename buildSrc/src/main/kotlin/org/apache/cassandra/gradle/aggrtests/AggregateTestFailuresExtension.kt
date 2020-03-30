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

package org.apache.cassandra.gradle.aggrtests

import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.testing.TestDescriptor
import org.gradle.api.tasks.testing.TestListener
import org.gradle.api.tasks.testing.TestResult
import org.gradle.kotlin.dsl.*

open class AggregateTestFailuresExtension {
    internal val resultListener = ResultListener()

    val taskName = "aggregateTestFailures"

    fun configureTestTasks(project: Project): Unit = project.run {

        tasks.register(taskName, PostCheckTestFailures::class)

        tasks.withType<Test>().configureEach {
            applyToTest(this)
        }
    }

    private fun applyToTest(test: Test) {
        var prj = test.project
        while (true) {
            val ext = prj.extensions.findByType(AggregateTestFailuresExtension::class)

            if (ext != null) {
                test.finalizedBy(prj.tasks.named(taskName, PostCheckTestFailures::class))
                test.addTestListener(ext.resultListener)
            }

            if (prj.parent != null)
                prj = prj.parent!!
            else
                break
        }

        test.ignoreFailures = true
    }
}

class ResultListener : TestListener {
    @Internal
    var hasFailures: Boolean = false

    override fun beforeTest(testDescriptor: TestDescriptor?) {
    }

    override fun afterTest(testDescriptor: TestDescriptor?, result: TestResult?) {
    }

    override fun beforeSuite(suite: TestDescriptor?) {
    }

    override fun afterSuite(suite: TestDescriptor?, result: TestResult?) {
        if (result != null && result.resultType == TestResult.ResultType.FAILURE)
            hasFailures = true
    }
}

open class PostCheckTestFailures : DefaultTask() {
    @TaskAction
    fun checkTestFailures() {
        val ext = project.extensions.getByType(AggregateTestFailuresExtension::class)

        if (ext.resultListener.hasFailures)
            throw GradleException("Some tests have failed")
    }
}