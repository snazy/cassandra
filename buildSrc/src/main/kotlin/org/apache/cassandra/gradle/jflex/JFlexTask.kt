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

// Copied from https://github.com/jprante/gradle-plugin-jflex
// Change: make task cacheable + input-directory-path relative

package org.apache.cassandra.gradle.jflex

import jflex.GeneratorException
import jflex.Main
import org.gradle.api.DefaultTask
import org.gradle.api.file.FileTree
import org.gradle.api.tasks.*
import java.io.File

@CacheableTask
open class JFlexTask : DefaultTask() {

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    @Optional
    var source: FileTree = project.fileTree("src/main/jflex").apply {
        include("**/*.jflex")
    }

    @OutputDirectory
    @Optional
    var generateDir: File = project.buildDir.resolve("generated-src/jflex")

    init {
        outputs.cacheIf { true }
    }

    @TaskAction
    fun generateAndTransformJflex() {
        project.delete(generateDir)
        project.mkdir(generateDir)
        generateJflex()
    }

    private fun generateJflex() {
        if (source.filter { !it.isDirectory }.isEmpty) {
            logger.warn("no flex files found")
        } else {
            source.visit {
                if (!isDirectory) {
                    try {
                        val args = listOf(
                                "-q", file.absolutePath,
                                "-d", project.file("$generateDir/${relativePath.parent}").absolutePath + '/')

                        logger.debug("running jflex {}", args)

                        Main.generate(args.toTypedArray())

                        logger.info("Java code generated from JFlex file : {}", relativePath)
                    } catch (e: GeneratorException) {
                        logger.error("JFlex $e.message", e)
                        throw StopActionException("error occurred during JFlex code generation")
                    }
                }
            }
        }
    }
}
