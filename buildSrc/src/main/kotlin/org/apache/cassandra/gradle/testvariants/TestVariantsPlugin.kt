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
package org.apache.cassandra.gradle.testvariants

import org.apache.cassandra.gradle.util.ConcatFiles
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.internal.file.FileOperations
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.create
import org.gradle.kotlin.dsl.getByType
import org.gradle.kotlin.dsl.register
import javax.inject.Inject

@Suppress("unused")
open class TestVariantsPlugin : Plugin<Project> {
    @get:Inject
    open val fileOperations: FileOperations
        get() {
            throw UnsupportedOperationException()
        }

    override fun apply(project: Project): Unit = project.run {
        val ext = extensions.create(TestVariantsExtension::class,
                "testVariants",
                TestVariantsExtension::class)
        ext.all { setupVariant(project, this) }
    }

    private fun setupVariant(project: Project, variant: TestVariant): Unit = project.run {
        val srcSet = extensions.getByType<SourceSetContainer>().getByName(SourceSet.MAIN_SOURCE_SET_NAME)
        tasks.register<Test>(srcSet.getTaskName("test", variant.name)) {
            group = "verification"
            description = "Run the unit tests with $name config"
            val output = fileOperations.fileResolver.newResolver(project.buildDir).resolve(variant.cassandraYaml)
            doFirst {
                val input = variant.sourceConfigFiles.stream().map { path: Any? -> fileOperations.file(path!!) }
                ConcatFiles.concatFiles(input, output)
            }
            systemProperty("cassandra.config", output.absolutePath)
            val test = this
            variant.configActions().forEach { a -> a.execute(test) }
        }
    }
}