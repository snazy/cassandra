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

plugins {
    `java-library`
    id("org.apache.cassandra.repos")
    id("org.caffinitas.gradle.testrerun")
    id("org.caffinitas.gradle.aggregatetestresults")
}

dependencies {
    testImplementation(gradleTestKit())
    testImplementation("org.junit.platform:junit-platform-launcher:1.6.2")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.2")
    testImplementation("org.assertj:assertj-core:3.15.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.6.2")
}

tasks.named("test") {
    enabled = false
}

tasks.register<Test>("testGradleBuild") {
    group = "verification"
    description = "Test the main Gradle build scripts"

    inputs.file(rootProject.file("build.gradle.kts"))
    inputs.file(rootProject.file("settings.gradle.kts"))
    inputs.file(rootProject.file("buildSrcComposite/build.gradle.kts"))
    inputs.file(rootProject.file("buildSrcComposite/settings.gradle.kts"))
    inputs.files(rootProject.file("buildSrcComposite/src"))

    useJUnitPlatform {
        includeEngines("junit-jupiter")
    }
    setForkEvery(0)
    systemProperty("rootProject.projectDirectory", rootProject.projectDir.relativeTo(projectDir))
    systemProperty("rootProject.version", rootProject.version)
}
