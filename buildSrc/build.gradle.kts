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

import java.util.*

plugins {
    `java-gradle-plugin`
    `kotlin-dsl`
}

project.version = "4.0-alpha4"

repositories {
    var buildProperties = file("../build.properties")
    if (!buildProperties.exists())
        buildProperties = file("../build.properties.default")
    if (buildProperties.exists()) {
        val buildProps = Properties()
        buildProperties.reader().use {
            buildProps.load(it)
        }
        buildProps.forEach { k, v ->
            val key = k.toString()
            if (key.startsWith("artifact.remoteRepository.")) {
                maven(v) {
                    name = key.substring("artifact.remoteRepository.".length)
                    logger.info("Adding maven repository '${name}' for $v")
                }
            }
        }
    } else {
        mavenCentral()
    }
}

dependencies {
    implementation("de.jflex:jflex:1.7.0")
}

gradlePlugin {
    plugins {
        create("antStyleRepos") {
            id = "org.apache.cassandra.repos"
            implementationClass = "org.apache.cassandra.gradle.repos.AntStyleRepos"
            description = "Configures the repositories from build.properties(.default)"
        }
        create("testExtensions") {
            id = "org.apache.cassandra.testextensions"
            implementationClass = "org.apache.cassandra.gradle.testextensions.TestExtensionsPlugin"
            description = "Provides repeating test execution and test-reruns"
        }
        create("aggregateTestTasks") {
            id = "org.apache.cassandra.aggregateTestFailures"
            implementationClass = "org.apache.cassandra.gradle.aggrtests.AggregateTestFailuresPlugin"
            description = "Run all test tasks, even if some fail - fail build at the end, if one test task fails"
        }
        create("compileCommand") {
            id = "org.apache.cassandra.compilecommand"
            implementationClass = "org.apache.cassandra.gradle.compilecommand.CompileCommandPlugin"
            description = "Provides the Gradle-side of incremental compilation support for the compile-command APT"
        }
        create("testRunner") {
            id = "org.apache.cassandra.testrunner"
            implementationClass = "org.apache.cassandra.gradle.testrunner.TestRunnerPlugin"
            description = "C* specific test task runner configuration"
        }
        create("testSummary") {
            id = "org.apache.cassandra.testsummary"
            implementationClass = "org.apache.cassandra.gradle.testsummary.TestSummaryPlugin"
            description = "Show test task summaries"
        }
        create("testVariants") {
            id = "org.apache.cassandra.testvariants"
            implementationClass = "org.apache.cassandra.gradle.testvariants.TestVariantsPlugin"
            description = "Generate variants of the 'test'"
        }
        create("specialTests") {
            id = "org.apache.cassandra.specialtests"
            implementationClass = "org.apache.cassandra.gradle.specialtests.SpecialTestsPlugin"
            description = "Generate 'test' tasks for the additional test source directories in the test/ subfolder"
        }
        create("cassandraTools") {
            id = "org.apache.cassandra.tools"
            implementationClass = "org.apache.cassandra.gradle.tools.ToolsPlugin"
            description = "Generate source sets and accompanying tasks for tools"
        }
        create("cassandraMicrobench") {
            id = "org.apache.cassandra.microbench"
            implementationClass = "org.apache.cassandra.gradle.microbench.MicrobenchPlugin"
            description = "Boilerplate code for microbenchmarks"
        }
        create("jflex") {
            id = "org.apache.cassandra.jflex"
            implementationClass = "org.apache.cassandra.gradle.jflex.JFlexPlugin"
            description = "JFlex plugin"
        }
        create("cqldoc") {
            id = "org.apache.cassandra.cqldoc"
            implementationClass = "org.apache.cassandra.gradle.cqldoc.CQLdocPlugin"
            description = "C* CQLdoc plugin"
        }
        create("gendoc") {
            id = "org.apache.cassandra.gendoc"
            implementationClass = "org.apache.cassandra.gradle.gendoc.GendocPlugin"
            description = "C* Gendoc plugin"
        }
        create("distArchives") {
            id = "org.apache.cassandra.distarchives"
            implementationClass = "org.apache.cassandra.gradle.distarchives.DistArchivesPlugin"
            description = "C* specific dist-archives processing"
        }
    }
}
