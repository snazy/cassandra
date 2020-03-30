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
package org.apache.cassandra.gradle.specialtests

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.internal.jvm.ClassDirectoryBinaryNamingScheme
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.create
import org.gradle.kotlin.dsl.getByType
import org.gradle.kotlin.dsl.register

@Suppress("unused")
class SpecialTestsPlugin : Plugin<Project> {
    override fun apply(project: Project): Unit = project.run {
        val ext = extensions.create(SpecialTestsExtension::class,
                "specialTests",
                SpecialTestsExtension::class)
        ext.all { setupSpecialTests(project, this) }
    }

    private fun setupSpecialTests(project: Project, special: SpecialTests) {
        val name: String = special.name

        val sourceSets = project.extensions.getByType<SourceSetContainer>()
        val mainSourceSet = sourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME)
        val testSourceSet = sourceSets.getByName(SourceSet.TEST_SOURCE_SET_NAME)

        val srcSet = sourceSets.register("${name}Test") {
            java.srcDir("test/$name")
            resources.srcDirs("test/resources", "test/conf")
        }
        val namingScheme = ClassDirectoryBinaryNamingScheme(srcSet.name)

        project.configurations.named(namingScheme.getTaskName(null, "implementation")) {
            extendsFrom(project.configurations.getByName(testSourceSet.implementationConfigurationName))
        }

        project.configurations.named(namingScheme.getTaskName(null, "implementation")) {
            dependencies.add(project.dependencies.create(mainSourceSet.output))
            dependencies.add(project.dependencies.create(testSourceSet.output))
        }

        project.tasks.register<Test>(mainSourceSet.getTaskName("test", name)) {
            group = "verification"
            description = "Run $name tests"
            testClassesDirs = srcSet.get().output
            classpath = classpath
                    .plus(srcSet.get().runtimeClasspath)
                    .plus(testClassesDirs)
        }
    }
}