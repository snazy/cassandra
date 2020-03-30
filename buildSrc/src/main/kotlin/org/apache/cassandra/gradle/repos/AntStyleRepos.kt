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

package org.apache.cassandra.gradle.repos

import org.gradle.api.Plugin
import org.gradle.api.Project
import java.net.URI
import java.util.*

@Suppress("unused")
class AntStyleRepos : Plugin<Project> {
    override fun apply(project: Project): Unit = project.run {

        var buildProperties = rootProject.file("build.properties")
        if (!buildProperties.exists())
            buildProperties = rootProject.file("build.properties.default")
        if (buildProperties.exists()) {
            val buildProps = Properties()
            buildProperties.reader().use {
                buildProps.load(it)
            }
            buildProps.forEach { k, v ->
                val key = k.toString()
                if (key.startsWith("artifact.remoteRepository.")) {
                    project.repositories.maven {
                        name = key.substring("artifact.remoteRepository.".length)
                        url = URI.create(v.toString())
                        logger.info("Adding maven repository '${name}' for $v")
                    }
                }
            }
        } else {
            repositories.mavenCentral()
        }
    }
}