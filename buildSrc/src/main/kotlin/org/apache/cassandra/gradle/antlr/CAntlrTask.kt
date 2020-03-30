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
package org.apache.cassandra.gradle.antlr

import org.gradle.api.Incubating
import org.gradle.api.file.SourceDirectorySet
import org.gradle.api.plugins.antlr.AntlrTask
import org.gradle.api.plugins.antlr.internal.AntlrResult
import org.gradle.api.plugins.antlr.internal.AntlrSourceGenerationException
import org.gradle.api.plugins.antlr.internal.AntlrSpecFactory
import org.gradle.api.plugins.antlr.internal.AntlrWorkerManager
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.TaskAction
import org.gradle.work.InputChanges
import java.io.File
import java.util.*
import java.util.stream.Collectors

/**
 * Workaround to deal with antlr include files.
 * See https://github.com/gradle/gradle/issues/13005
 */
open class CAntlrTask : AntlrTask() {
    @get:Input
    var includeFiles: Set<String> = LinkedHashSet()
    private var antlrTask: AntlrTask? = null

    fun configureFrom(antlrTask: AntlrTask) {
        this.antlrTask = antlrTask
        source = antlrTask.source
    }

    @Suppress("UnstableApiUsage")
    @Incubating
    @TaskAction
    override fun execute(inputChanges: InputChanges) {
        val stableSources = stableSources
        // Note: cannot really handle incremental grammar generation (but doesn't actually matter, as there's
        // just one Cql.g file in C* anyway).
        val grammarFiles: Set<File> = HashSet(stableSources.files)
        val sourceDirectorySet: SourceDirectorySet
        try {
            val f = AntlrTask::class.java.getDeclaredField("sourceDirectorySet")
            f.isAccessible = true
            sourceDirectorySet = f[antlrTask] as SourceDirectorySet
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
        val processGrammarFiles = grammarFiles.stream()
                .filter { f: File -> !includeFiles.contains(f.name) }
                .collect(Collectors.toSet())
        val manager = AntlrWorkerManager()
        val spec = AntlrSpecFactory().create(this, processGrammarFiles, sourceDirectorySet)
        val projectDir = projectLayout.projectDirectory.asFile
        val result = manager.runWorker(projectDir, workerProcessBuilderFactory, antlrClasspath, spec)
        evaluate(result)
    }

    private fun evaluate(result: AntlrResult) {
        val errorCount = result.errorCount
        when {
            errorCount < 0 -> {
                throw AntlrSourceGenerationException("There were errors during grammar generation", result.exception)
            }
            errorCount == 1 -> {
                throw AntlrSourceGenerationException("There was 1 error during grammar generation", result.exception)
            }
            errorCount > 1 -> {
                throw AntlrSourceGenerationException("There were "
                        + errorCount
                        + " errors during grammar generation", result.exception)
            }
        }
    }
}