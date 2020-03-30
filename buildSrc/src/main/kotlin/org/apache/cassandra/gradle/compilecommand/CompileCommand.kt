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
package org.apache.cassandra.gradle.compilecommand

import org.gradle.api.Project
import org.gradle.api.provider.Provider
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.kotlin.dsl.getByType
import java.io.File

class CompileCommand {
    @Suppress("MemberVisibilityCanBePrivate")
    var sourceSet: Any = SourceSet.MAIN_SOURCE_SET_NAME
    var outputFile: File? = null
    var intermediateFilesDirectory = "compile-command-incr"

    fun resolveSourceSet(project: Project): SourceSet? {
        var o: Any? = sourceSet
        if (o is Provider<*>) o = o.get()
        @Suppress("NULLABILITY_MISMATCH_BASED_ON_JAVA_ANNOTATIONS")
        if (o is String) o = project.extensions.getByType<SourceSetContainer>().getByName(o)
        return o as SourceSet?
    }
}