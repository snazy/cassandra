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

package org.apache.cassandra.gradle

import org.gradle.api.Action
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.Dependency
import org.gradle.api.artifacts.ExternalModuleDependency
import org.gradle.api.publish.maven.MavenPom
import org.gradle.api.tasks.TaskCollection
import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.testing.junitplatform.JUnitPlatformOptions
import org.gradle.internal.Actions
import org.gradle.internal.os.OperatingSystem
import org.gradle.kotlin.dsl.named
import java.nio.charset.StandardCharsets
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.function.Supplier

fun Test.withJUnitPlatform(testFrameworkConfigure: Action<in JUnitPlatformOptions>?) {
    val action = testFrameworkConfigure ?: Actions.doNothing()
    val currentOptions = this.options
    if (currentOptions is JUnitPlatformOptions)
        action.execute(currentOptions)
    else
        this.useJUnitPlatform(action)
}

inline fun <reified T : Task> TaskCollection<out Task>.configureEach(vararg taskNames: String, noinline configuration: T.() -> Unit): Unit = this.run {
    for (taskName in taskNames) {
        named(taskName, configuration)
    }
}

fun commonCassandraPom(pom: MavenPom, parentGroup: Any, parentArtifact: Any, parentVersion: Any, project: Project): Unit = pom.run {
    inceptionYear.set("2009")
    name.set("Apache Cassandra")
    url.set("https://cassandra.apache.org")
    licenses {
        license {
            name.set("The Apache Software License, Version 2.0")
            url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
        }
    }
    developers {
        val props = Properties()
        project.file("src/developers.properties").reader().use {
            props.load(it)
        }
        props.forEach { i, n ->
            developer {
                id.set(i as String)
                name.set(n as String)
            }
        }
    }
    scm {
        connection.set("scm:https://gitbox.apache.org/repos/asf/cassandra.git")
        developerConnection.set("scm:https://gitbox.apache.org/repos/asf/cassandra.git")
        url.set("https://gitbox.apache.org/repos/asf?p=cassandra.git;a=tree")
    }
    mailingLists {
        mapOf("user" to "User", "dev" to "Developer", "commits" to "Commits").forEach { (ml, n) ->
            mailingList {
                name.set(n)
                subscribe.set("$ml-subscribe@cassandra.apache.org")
                unsubscribe.set("$ml-unsubscribe@cassandra.apache.org")
                post.set("$ml@cassandra.apache.org")
                archive.set("https://lists.apache.org/list.html?$ml@cassandra.apache.org")
                otherArchives.set(listOf("http://www.mail-archive.com/$ml@cassandra.apache.org/"))
            }
        }
    }
    issueManagement {
        system.set("JIRA")
        url.set("https://issues.apache.org/jira/browse/CASSANDRA")
    }
    withXml {
        val elParent = asNode().appendNode("parent")
        elParent.appendNode("artifactId", parentArtifact)
        elParent.appendNode("groupId", parentGroup)
        elParent.appendNode("version", parentVersion)
    }
}

fun MavenPom.generateParentPom(allDepsFunction: Supplier<Set<Dependency>>) {
    withXml {
        // Gradle doesn't have a concept of a "parent pom"
        // The C* parent pom contains the dependency management for all scopes, but without actually
        // mentioning the scope.

        val dependencies = asNode().appendNode("dependencyManagement").appendNode("dependencies")

        allDepsFunction.get().filterIsInstance<ExternalModuleDependency>().forEach { d ->
            val dep = dependencies.appendNode("dependency")
            dep.appendNode("groupId", d.group)
            dep.appendNode("artifactId", d.name)
            dep.appendNode("version", d.version)
            val artifact = d.artifacts.find { a ->
                @Suppress("SENSELESS_COMPARISON") // classifier IS usually null here
                a.classifier != null
            }
            if (artifact != null)
                dep.appendNode("classifier", artifact.classifier)
            d.artifacts.forEach { a -> a.classifier }
            if (d.excludeRules.isNotEmpty()) {
                val exclusions = dep.appendNode("exclusions")
                d.excludeRules.forEach { ex ->
                    val exclusion = exclusions.appendNode("exclusion")
                    @Suppress("USELESS_ELVIS") // nope - elvis isn't useless (just dead)
                    exclusion.appendNode("artifactId", ex.module ?: "*")
                    exclusion.appendNode("groupId", ex.group)
                }
            }
        }
    }
}

fun Project.property(name: String, defaultValue: Any): Any {
    return if (hasProperty(name))
        property(name)!!
    else
        defaultValue
}

fun currentUID(): Int {
    if (OperatingSystem.current().isWindows) return 0

    try {
        val p = ProcessBuilder("id", "-u").start()
        p.waitFor(2, TimeUnit.SECONDS)
        val stdout = p.inputStream.use {
            it.readBytes().toString(StandardCharsets.UTF_8).trim()
        }
        return stdout.toInt()
    } catch (e: Exception) {
        throw RuntimeException(e)
    }
}

fun currentGID(): Int {
    if (OperatingSystem.current().isWindows) return 0

    try {
        val p = ProcessBuilder("id", "-g").start()
        p.waitFor(2, TimeUnit.SECONDS)
        val stdout = p.inputStream.use {
            it.readBytes().toString(StandardCharsets.UTF_8).trim()
        }
        return stdout.toInt()
    } catch (e: Exception) {
        throw RuntimeException(e)
    }
}
