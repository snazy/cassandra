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

package org.apache.cassandra.gradle.gendoc

import org.apache.cassandra.gradle.currentGID
import org.apache.cassandra.gradle.currentUID
import org.gradle.api.JavaVersion
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.Delete
import org.gradle.api.tasks.Exec
import org.gradle.api.tasks.PathSensitivity
import org.gradle.kotlin.dsl.getValue
import org.gradle.kotlin.dsl.provideDelegate
import org.gradle.kotlin.dsl.register
import org.gradle.kotlin.dsl.registering

@Suppress("unused")
class GendocPlugin : Plugin<Project> {
    override fun apply(project: Project): Unit = project.run {

        val cleanGendoc by tasks.registering(Delete::class) {
            doFirst {
                delete("doc/build")
            }
        }

        tasks.register<Exec>("gendoc") {
            dependsOn(tasks.named("jar"))

            mustRunAfter(cleanGendoc)

            description = "Generate C* HTML documentation"
            group = "Documentation"

            workingDir = file("doc")
            executable = "docker-compose"
            args("run",
                    "--rm",
                    "-e", "USE_UID=${currentUID()}",
                    "-e", "USE_GID=${currentGID()}")
            if (JavaVersion.current().isJava11Compatible)
                args("-e", "CASSANDRA_USE_JDK11=true")
            args("build-docs")

            inputs.file("doc/Dockerfile").withPathSensitivity(PathSensitivity.RELATIVE)
            inputs.file("doc/convert_yaml_to_rst.py").withPathSensitivity(PathSensitivity.RELATIVE)
            inputs.file("doc/docker-compose.yml").withPathSensitivity(PathSensitivity.RELATIVE)
            inputs.file("doc/Makefile").withPathSensitivity(PathSensitivity.RELATIVE)
            inputs.dir("doc/source").withPathSensitivity(PathSensitivity.RELATIVE)
            outputs.dir("doc/build")
            outputs.cacheIf { true }

            doFirst {
                exec {
                    workingDir = file("doc")
                    executable = "docker-compose"
                    args("build")
                    if (JavaVersion.current().isJava11Compatible)
                        args("--build-arg", "JVM_VERSION=11")
                    args("build-docs")
                }
            }
        }
    }
}