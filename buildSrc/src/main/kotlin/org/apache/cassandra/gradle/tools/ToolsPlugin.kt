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
package org.apache.cassandra.gradle.tools

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.internal.jvm.ClassDirectoryBinaryNamingScheme
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.create
import org.gradle.kotlin.dsl.getByType
import org.gradle.kotlin.dsl.named
import org.gradle.kotlin.dsl.register
import java.io.File

@Suppress("unused")
class ToolsPlugin : Plugin<Project> {
    override fun apply(project: Project): Unit = project.run {
        val ext = extensions.create(CassandraToolsExtension::class,
                "cassandraTools",
                CassandraToolsExtension::class)
        ext.all { setupTool(project, this) }
    }

    private fun setupTool(project: Project, tool: CassandraTool) = project.run {
        val sourceSets = extensions.getByType<SourceSetContainer>()

        val mainSourceSet = sourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME)
        val testSourceSet = sourceSets.getByName(SourceSet.TEST_SOURCE_SET_NAME)

        val srcSet = sourceSets.register(tool.name) {
            java.srcDir("tools/${tool.name}/src")
            java.outputDir = File(buildDir, "classes/${tool.name}")
            resources.srcDir("tools/${tool.name}/src/resources")
        }
        val namingScheme = ClassDirectoryBinaryNamingScheme(srcSet.name)
        val srcSetTest = sourceSets.register("${tool.name}Test") {
            java.srcDir("tools/${tool.name}/test/unit")
            resources.srcDir("test/conf")
        }
        val namingSchemeTest = ClassDirectoryBinaryNamingScheme(srcSetTest.name)

        val cfg = configurations.named(namingScheme.getTaskName(null, "implementation")) {
            extendsFrom(project.configurations.getByName(mainSourceSet.implementationConfigurationName))
        }

        configurations.named(namingSchemeTest.getTaskName(null, "implementation")) {
            extendsFrom(cfg.get())
            extendsFrom(project.configurations.getByName(testSourceSet.implementationConfigurationName))
        }

        configurations.named(namingScheme.getTaskName(null, "implementation")) {
            dependencies.add(project.dependencies.create(mainSourceSet.output))
        }
        configurations.named(namingSchemeTest.getTaskName(null, "implementation")) {
            dependencies.add(project.dependencies.create(srcSet.get().output))
            dependencies.add(project.dependencies.create(testSourceSet.output))
        }

        tasks.register<Test>(namingScheme.getTaskName("test")) {
            group = "verification"
            description = "Run ${tool.name} tests"
            testClassesDirs = srcSetTest.get().output
            classpath = classpath + srcSet.get().runtimeClasspath + testClassesDirs
        }

        val toolJar = tasks.register<Jar>(namingScheme.getTaskName(null, "jar")) {
            group = "build"
            description = "Assembles a jar archive containing the ${tool.name} classes."
            destinationDirectory.fileValue(File(project.buildDir, "tools/lib"))
            archiveFileName.set("${tool.name}.jar")
            from(srcSet.get().output)
        }

        tasks.named<Jar>("jar") {
            dependsOn(toolJar)
        }
    }
}