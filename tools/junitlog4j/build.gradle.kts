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

val junitPlatformVersion: String by rootProject.extra
val junitJupiterVersion: String by rootProject.extra
val logbackVersion: String by rootProject.extra

dependencies {
    implementation("org.junit.platform:junit-platform-launcher:${junitPlatformVersion}")
    implementation("ch.qos.logback:logback-core:${logbackVersion}")
    implementation("ch.qos.logback:logback-classic:${logbackVersion}")

    testImplementation("org.junit.jupiter:junit-jupiter-api:${junitJupiterVersion}")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:${junitJupiterVersion}")
}
