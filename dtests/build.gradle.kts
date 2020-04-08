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
import com.datastax.junitpytest.gradleplugin.GitSource
import com.datastax.junitpytest.gradleplugin.LocalSource
import com.datastax.junitpytest.gradleplugin.Pytest
import com.datastax.junitpytest.gradleplugin.PytestDiscovery
import com.datastax.junitpytest.gradleplugin.RepoSource
import org.apache.cassandra.gradle.property
import org.gradle.api.tasks.PathSensitivity.RELATIVE

plugins {
    id("org.apache.cassandra.repos")
    id("org.caffinitas.gradle.testrerun")
    id("org.caffinitas.gradle.aggregatetestresults")
    id("org.caffinitas.gradle.testsummary")
    id("com.datastax.junitpytest")
    id("com.gradle.enterprise.test-distribution")
}

// The following code allows to configure the sources for dtests, ccm and cassandra-driver.
//
// By default it refers to the standard git repos and branches.
//
// To use a local directory or a different git repo, run `./gradlew tasks` or look into gradle/additional-help.txt.
//
// A local directory is preferred over a git branch.

fun sourceTask(src: String, repo: String, branchName: String, syncCfg: Action<Sync>): TaskProvider<out RepoSource> =
    if (!hasProperty("dtests.source.$src"))
        tasks.register<GitSource>("git${src.capitalize()}") {
            repository.set(project.property("dtests.source.$src.repo", repo).toString())
            eggName.set(src)
            branch.set(project.property("dtests.source.$src.branch", branchName).toString())
        }
    else
        tasks.register<LocalSource>("local${src.capitalize()}") {
            repository.set(project.property("dtests.source.$src").toString())
            eggName.set(src)
            sourceDirectory.set(file(project.property("dtests.source.$src").toString()))
            exclude(".git")
            exclude("__pycache__")
            exclude(".pytest_cache")
            exclude("venv")
            syncCfg.execute(this)
        }

val sourceDtest = sourceTask("dtest", "https://gitbox.apache.org/repos/asf/cassandra-dtest.git/", "master") {
    exclude("logs")
    exclude("cassandra.logdir_IS_UNDEFINED")
}
val sourceCcm = sourceTask("ccm", "https://github.com/riptano/ccm.git", "cassandra-test") {}
val sourceCassandraDriver = sourceTask("cassandraDriver", "https://github.com/datastax/python-driver.git", "cassandra-test") {}

pytest {
    pipEnvironment.set(mutableMapOf("CASS_DRIVER_NO_CYTHON" to "true")) // cython setup takes long with python-cassandra-driver cython stuff (~ 2 minutes)

    requirementsSource.set(sourceDtest.get().targetDirectory.file("requirements-base.txt"))
    sourceRequirementsTasks.set(listOf(sourceCcm, sourceCassandraDriver))

    pytestDirectorySet.srcDir(sourceDtest.get().targetDirectory)
}

tasks.named("createVirtualenv") {
    dependsOn(sourceDtest)
    inputs.dir(sourceDtest.get().targetDirectory)
}

val rootPrj = evaluationDependsOn(":")

tasks.named<PytestDiscovery>("discoverPytest") {
    // pytestOptions (and more properties) are shared between the discoverPytest and pytest tasks.
    // Since test discovery runs before test execution, the pytestOptions need to be set here.

    val options = mutableListOf<String>()
    options.add("--cassandra-dir=${project.rootDir.absolutePath}")
    if (project.hasProperty("dtestOptions"))
        options.addAll(project.property("dtestOptions").toString().split(' '))
    if (!project.hasProperty("dtestNoVnodes"))
        options.add("--use-vnodes")
    if (project.hasProperty("dtestUpgrade"))
        options.add("--execute-upgrade-tests")
    if (project.hasProperty("dtestLarge"))
        options.add("--force-resource-intensive-tests")
    if (project.hasProperty("dtestNoLarge"))
        options.add("--skip-resource-intensive-tests")
    pytestOptions.set(options)

    // need the lib/ directory populated
    dependsOn(rootPrj.tasks.named("syncLibDir")) // syncLibDir dependsOn jar
}
tasks.named<Pytest>("pytest") {
    inputs.files(
            rootProject.projectDir.resolve("bin"),
            rootProject.projectDir.resolve("build/apache-cassandra-${project.version}.jar"),
            rootProject.projectDir.resolve("build/lib/jars"),
            rootProject.projectDir.resolve("build/tools/lib"),
            rootProject.projectDir.resolve("lib"),
            rootProject.projectDir.resolve("pylib"),
            rootProject.projectDir.resolve("conf"),
            rootProject.projectDir.resolve("tools/bin"),
            rootProject.projectDir.resolve("tools/fqltool"),
            rootProject.projectDir.resolve("tools/stress"),
            rootProject.projectDir.resolve("build.xml"),
            rootProject.projectDir.listFiles { _, fn -> fn.endsWith(".yaml") }
    ).withPathSensitivity(RELATIVE)

    maxParallelForks = 1
    distribution {
        enabled.set(project.hasProperty("withDistribution"))
        if (project.hasProperty("noLocalExecutors") || project.hasProperty("noLocalDtests"))
            maxLocalExecutors.set(0)
        maxRemoteExecutors.set(project.property("remoteExecutors", "20").toString().toInt())
        requirements.addAll(listOf("python=3", "jdk=8", "jdk=11"))
    }
}

tasks.withType(Pytest::class).configureEach {

}