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
package org.apache.cassandra.gradle.testrunner

import com.sun.management.OperatingSystemMXBean
import org.apache.cassandra.gradle.util.JavaAgentArgumentProvider
import org.apache.cassandra.gradle.withJUnitPlatform
import org.gradle.api.JavaVersion
import org.gradle.api.Project
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.create
import org.gradle.kotlin.dsl.getByType
import org.gradle.kotlin.dsl.withType
import org.gradle.process.CommandLineArgumentProvider
import java.io.File
import java.io.IOException
import java.lang.management.ManagementFactory
import java.nio.file.Files
import java.util.*
import kotlin.collections.ArrayList
import kotlin.math.floor
import kotlin.math.max
import kotlin.math.min

open class TestRunnerExtension(val project: Project) {

    @Suppress("MemberVisibilityCanBePrivate")
    var timeoutSeconds: Int = 0

    fun configureTestTasks(project: Project): Unit = project.run {
        tasks.withType<Test>().configureEach {
            applyToTest(this)
        }
    }

    private fun applyToTest(test: Test) {
        val taskExt = test.extensions.create<TestRunnerTaskExtension>("testCommon")

        val testCommon = project.extensions.getByType<TestRunnerExtension>()

        test.withJUnitPlatform {
            includeEngines("timeout-junit-vintage", "junit-jupiter")
        }

        // Required by JUnit 5 to load extensions via the service-loader mechanism (META-INF/services/...)
        test.systemProperty("junit.jupiter.extensions.autodetection.enabled", "true")

        test.doFirst {

            if (!test.systemProperties.containsKey("cassandra.testtag"))
                test.systemProperty("cassandra.testtag", "")

            val timeoutSeconds: Int = taskExt.timeoutSeconds ?: timeoutSeconds
            if (timeoutSeconds > 0) {
                // see https://junit.org/junit5/docs/current/user-guide/#writing-tests-declarative-timeouts
                test.systemProperty("junit.jupiter.execution.timeout.default", "${timeoutSeconds}s")
                test.systemProperty("junit.vintage.execution.timeout.seconds", timeoutSeconds.toString())
            }

            val tempDir = tempDir!!
            val p = project.file(tempDir).toPath()
            if (!Files.isDirectory(p)) {
                try {
                    Files.createDirectories(project.file(tempDir).toPath())
                } catch (e: IOException) {
                    throw RuntimeException(e)
                }
            }

            // This is mostly taken from the javascript part in build.xml's 'testparallelhelper', except that the
            // sqrt(cores) has been replaced with 'cores / 1.75'
            val os = ManagementFactory.getOperatingSystemMXBean() as OperatingSystemMXBean
            val cores = project.findProperty("cores.count")?.toString()?.toInt() ?: os.availableProcessors
            val mem = project.findProperty("mem.size")?.toString()?.toLong() ?: os.totalPhysicalMemorySize

            val numRunners =
                    if (taskExt.noParallelTests) {
                        1
                    } else if (project.hasProperty("test.runners")) {
                        project.property("test.runners").toString().trim { it <= ' ' }.toInt()
                    } else if (cores > 0 && mem > 0) {
                        val guess = min(
                                floor(cores / 1.75),
                                floor(mem.toDouble() / (4L * 1024 * 1024 * 1024))
                        )
                        max(1, guess.toInt())
                    } else {
                        1
                    }

            if (JavaVersion.current().isJava8)
                test.classpath += project.files(System.getProperty("java.home") + "/lib/tools.jar", System.getProperty("java.home") + "/../lib/tools.jar")

            if (JavaVersion.current().isJava11Compatible)
                test.jvmArgs(testCommon.java11Args)

            // Need to spin up a new test-worker JVM for every test (think: static initialization, no proper tear-down)
            test.minHeapSize = "512m"
            test.maxHeapSize = "1536m"
            test.systemProperties(testCommon.systemProperties)
            test.jvmArgs(testCommon.jvmArgs)

            val forkEvery = if (test.name.endsWith("Unit")) 0 else project.findProperty("fork.every")?.toString()?.toLong()
                    ?: 1L

            test.logger.info("Running {} with {} runners, forkEvery={}, num-cores={}, mem={}M",
                    test.name,
                    numRunners,
                    forkEvery,
                    cores,
                    mem / 1024 / 1024)

            test.setForkEvery(forkEvery)
            test.maxParallelForks = numRunners

            test.jvmArgumentProviders.add(JavaAgentArgumentProvider(project.configurations.getByName("jamm")))
            test.jvmArgumentProviders.add(TmpDirProvider(testCommon.tempDir))
        }
    }

    @Suppress("MemberVisibilityCanBePrivate")
    var tempDir: File? = null

    @Suppress("MemberVisibilityCanBePrivate")
    var systemProperties: Map<String, *> = LinkedHashMap<String, Any>()

    @Suppress("MemberVisibilityCanBePrivate")
    var jvmArgs: List<String> = ArrayList()

    @Suppress("MemberVisibilityCanBePrivate")
    var java11Args: List<String> = ArrayList()
}

class TmpDirProvider(@Internal private val tempDir: File?) : CommandLineArgumentProvider {
    override fun asArguments(): MutableIterable<String> {
        return mutableListOf("-Djava.io.tmpdir=$tempDir")
    }
}
