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
package org.apache.cassandra.gradle.microbench

import org.apache.cassandra.gradle.testrunner.TestRunnerExtension
import org.apache.cassandra.gradle.testrunner.TestRunnerPlugin
import org.gradle.api.DefaultTask
import org.gradle.api.JavaVersion
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.internal.jvm.ClassDirectoryBinaryNamingScheme
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.bundling.Jar
import org.gradle.kotlin.dsl.apply
import org.gradle.kotlin.dsl.create
import org.gradle.kotlin.dsl.getByType
import org.gradle.kotlin.dsl.register

@Suppress("unused")
class MicrobenchPlugin : Plugin<Project> {
    override fun apply(project: Project): Unit = project.run {
        val ext = extensions.create<MicrobenchExtension>("microbench")

        plugins.apply(TestRunnerPlugin::class)

        val sourceSets = extensions.getByType<SourceSetContainer>()

        val mainSourceSet = sourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME)
        val testSourceSet = sourceSets.getByName(SourceSet.TEST_SOURCE_SET_NAME)

        val sourceSet = sourceSets.register("microbench") {
            java.srcDir("test/microbench")
            resources.srcDir("test/resources")
        }
        val namingScheme = ClassDirectoryBinaryNamingScheme(sourceSet.name)

        configurations.named(namingScheme.getTaskName(null, "implementation")) {
            extendsFrom(configurations.getByName(testSourceSet.implementationConfigurationName))
            dependencies.add(project.dependencies.create(mainSourceSet.output))
            dependencies.add(project.dependencies.create(testSourceSet.output))
            dependencies.add(project.dependencies.create("org.openjdk.jmh:jmh-core:${ext.jmhVersion}"))
        }
        configurations.named(namingScheme.getTaskName(null, "compileOnly")) {
            dependencies.add(project.dependencies.create("org.openjdk.jmh:jmh-generator-annprocess:${ext.jmhVersion}"))
        }
        configurations.named(namingScheme.getTaskName(null, "annotationProcessor")) {
            dependencies.add(project.dependencies.create("org.openjdk.jmh:jmh-generator-annprocess:${ext.jmhVersion}"))
        }

        val jar = tasks.register<Jar>(namingScheme.getTaskName(null, "jar")) {
            group = "Build"
            description = "Assembles a jar archive containing the microbench classes and triggers ."
            destinationDirectory.fileValue(buildDir.resolve("tools/lib"))
            archiveFileName.set("microbench.jar")
            from(sourceSet.get().output)
            finalizedBy(tasks.named("microbench"))
        }

        tasks.register<DefaultTask>("microbench") {
            val scriptFile = buildDir.resolve("microbench")
            group = "build"
            description = "Generate the $scriptFile shell wrapper to run microbenchmarks"
            dependsOn(jar)
            outputs.file(scriptFile)
            doFirst {
                val testCommon = project.extensions.getByType<TestRunnerExtension>()
                val classpathFiles = (listOf(projectDir.resolve("test/conf")) +
                        configurations.named(sourceSet.get().runtimeClasspathConfigurationName).get().files).map { f ->
                    if (f.startsWith(projectDir))
                        "${'$'}{BASE}/${f.relativeTo(projectDir)}"
                    else
                        f.absolutePath
                }
                val script = """#!/bin/bash
#
# GENERATED FILE
#
# JMH wrapper shell script for ${rootProject.name} for Java ${JavaVersion.current().majorVersion}
#
# DO NOT EDIT, ALL CHANGES IN THE ORIGINAL LOCATION WILL BE OVERWRITTEN (or copy it to a safe place)
#

BASE="$projectDir"

CLASSPATH="${jar.get().archiveFile.get().asFile}"
${classpathFiles.joinToString("\n") { f -> """CLASSPATH="${'$'}{CLASSPATH}:$f"""" }}

java -cp ${"$"}{CLASSPATH} \
    ${if (JavaVersion.current().isJava11Compatible) testCommon.java11Args.joinToString(" \\\n    ") else ""} \
    ${'$'}{JVM_ARGS:"-Xms2g -Xmx2g"} \
    org.openjdk.jmh.Main \
    "$@"
"""

                scriptFile.writeText(script)
                scriptFile.setExecutable(true)
                logger.lifecycle("Wrote executable {}", scriptFile)
            }
        }
    }
}