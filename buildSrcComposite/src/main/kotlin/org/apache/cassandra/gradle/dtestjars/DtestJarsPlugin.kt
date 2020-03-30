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
package org.apache.cassandra.gradle.dtestjars

import java.io.File

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.file.RelativePath
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.Exec
import org.gradle.api.tasks.Sync
import org.gradle.api.tasks.TaskProvider
import org.gradle.internal.os.OperatingSystem
import org.gradle.kotlin.dsl.create
import org.gradle.kotlin.dsl.invoke
import org.gradle.kotlin.dsl.register

import de.undercouch.gradle.tasks.download.Download

import com.datastax.junitpytest.gradleplugin.GitSource
import com.datastax.junitpytest.gradleplugin.LocalSource
import org.gradle.api.GradleException
import org.gradle.api.JavaVersion

@Suppress("unused")
class DtestJarsPlugin : Plugin<Project> {
    override fun apply(project: Project): Unit = project.run {
        val ext = extensions.create(DtestJarsExtension::class,
                "dtests",
                DtestJarsExtension::class)

        val downloadAnt = tasks.register("downloadAnt", Download::class)
        val extractAnt = tasks.register("extractAnt", Sync::class)
        val antDir = buildDir.resolve("apache-ant/ant")

        afterEvaluate {
            val tar = buildDir.resolve("apache-ant/apache-ant-1.10.8-bin.tar.gz")
            downloadAnt {
                src("https://downloads.apache.org//ant/binaries/apache-ant-1.10.8-bin.tar.gz")
                dest(tar)
                overwrite(false)
            }
            extractAnt {
                dependsOn(downloadAnt)

                from(tarTree(tar))
                into(antDir)

                eachFile {
                    relativePath = RelativePath(true, *relativePath.segments.drop(1).toTypedArray())
                    if (relativePath.segments[0] == "manual")
                        exclude()
                }
            }
        }

        ext.all { setupDtestJar(project, this, extractAnt, antDir) }
    }

    private fun setupDtestJar(project: Project, dtestJar: DtestJar, extractAnt: TaskProvider<Sync>, antDir: File) {
        val sourceTaskName = "source${dtestJar.name.capitalize()}"

        // TODO add support for local-sources
//        val repoSource = if (check-some-project-properties) {
//            project.tasks.register(sourceTaskName, LocalSource::class) {
//                eggName.set(dtestJar.name)
//                sourceDirectory.set(project.file(dtestJar.localSource!!))
//            }
//        } else {
            val repoSource =     project.tasks.register(sourceTaskName, GitSource::class) {
                eggName.set(dtestJar.name)
                repository.convention("https://github.com/apache/cassandra")
                branch.set("cassandra-${dtestJar.name}")
            }
//        }

        val buildTask = project.tasks.register("build${dtestJar.name.capitalize()}", Exec::class) {
            dependsOn(extractAnt, repoSource)

            group = "misc"
            description = "Create C* ${dtestJar.name} dtest-jar"

            val execExt = if (OperatingSystem.current().isWindows) ".bat" else ""

            doFirst {
                if (!JavaVersion.current().isJava8) {
                    val java8home = System.getenv("JAVA8_HOME")
                            ?: throw GradleException("JAVA8_HOME environment must be configured when building dtest-jar for ${dtestJar.name}")

                    environment("PATH", "$java8home/bin${File.pathSeparator}${System.getenv("PATH")}")
                    environment("JAVACMD", "$java8home/bin/java$execExt")
                    environment("JAVA_HOME", java8home)
                }
            }

            workingDir = repoSource.get().targetDirectory.get().asFile
            commandLine = listOf("$antDir/bin/ant$execExt", "dtest-jar")

            // TODO cannot use inputs.dir() this way, because the input's then modified as it's being built
            // inputs.dir(repoSource.get().targetDirectory).withPathSensitivity(PathSensitivity.RELATIVE)
            // TODO the output file isn't cached
            // TODO dtest-jar is always built
        }

        val copyDtestJar = project.tasks.register("copyDtestJar${dtestJar.name.capitalize()}", Copy::class) {
            dependsOn(buildTask)

            group = "misc"
            description = "Copy dtest-jar for ${dtestJar.name}"

            val targetBuildDir = repoSource.get().targetDirectory.get().asFile.resolve("build")

            into(project.buildDir)
            from(targetBuildDir)
            include("dtest-*.jar")

            doLast {
                project.tasks.named("testDistributed") {
                    val filenameFilter = { _: File?, name: String? ->
                        val n = name ?: throw GradleException("Null name")
                        n.startsWith("dtest-") && n.endsWith(".jar")
                    }
                    val producedDtestJarFile = targetBuildDir
                            .list(filenameFilter)!!
                            .sortedArrayDescending()[0]
                    inputs.file(project.buildDir.resolve(producedDtestJarFile))
                }
            }
        }

        project.tasks.named("testDistributed") {
            dependsOn(copyDtestJar)
        }
    }
}
