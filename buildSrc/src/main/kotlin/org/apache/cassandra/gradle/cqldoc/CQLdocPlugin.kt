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

package org.apache.cassandra.gradle.cqldoc

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.Delete
import org.gradle.api.tasks.PathSensitivity
import org.gradle.kotlin.dsl.getValue
import org.gradle.kotlin.dsl.provideDelegate
import org.gradle.kotlin.dsl.registering
import org.gradle.kotlin.dsl.withGroovyBuilder

@Suppress("unused")
class CQLdocPlugin : Plugin<Project> {
    override fun apply(project: Project): Unit = project.run {

        val cql3DocDir = "doc/cql3"

        val cleanCqldoc by tasks.registering(Delete::class) {
            doFirst {
                delete(fileTree(cql3DocDir) { include("*.html") })
            }
        }

        tasks.register("cqldoc") {
            description = "Generate CQL HTML documentation"
            group = "Documentation"

            mustRunAfter(cleanCqldoc)

            outputs.files(fileTree(cql3DocDir) {
                include("*.html", "*.css")
            })
            outputs.cacheIf { true }
            inputs.files(fileTree(cql3DocDir) {
                include("*.textile")
            }).withPathSensitivity(PathSensitivity.RELATIVE)

            doLast {
                val wikitext = project.configurations.getByName("wikitext")

                ant.withGroovyBuilder {
                    "taskdef"("resource" to "wikitexttasks.properties", "classpath" to wikitext.asPath)
                    "wikitext-to-html"("markupLanguage" to "Textile") {
                        "fileset"("dir" to projectDir) {
                            "include"("name" to "$cql3DocDir/*.textile")
                        }
                    }
                }
            }
        }
    }
}