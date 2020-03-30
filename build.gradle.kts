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

/**
 * Gradle script for Apache Cassandra.
 *
 * This is *not* a replacement for ant (at least at this point) but an opt-in for developers.
 * Means: you can use Gradle, but you don't have to.
 *
 * After cloning github.com/apache/cassandra just open the project in IntelliJ IDEA and it works out of the box.
 * If you want to use the legacy {@code ant generate-idea-files} approach, just run that ant task.
 * It is *not* recommended to run `./gradlew idea` - just open the project in IDEA
 *
 * It does not matter whether you use Java 8 or Java >= 11 in your IDE or from the command line. The script
 * just detects the Java versions and adds the necessary JVM options.
 *
 * Not implemented yet:
 * - release-packages (RPM / DEB) (use https://github.com/nebula-plugins/gradle-ospackage-plugin/wiki ?) + signing
 * - forking in-jvm-dtests
 *
 * Maintaining dependencies in ant (2 times) and Gradle will not work for a long time. But it feels possible
 * to let the Gradle build generate the poms.
 */

import java.time.Duration
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.apache.cassandra.gradle.commonCassandraPom
import org.apache.cassandra.gradle.configureEach
import org.apache.cassandra.gradle.generateParentPom
import org.apache.cassandra.gradle.withJUnitPlatform
import org.apache.cassandra.gradle.testrunner.TestRunnerTaskExtension
import org.apache.cassandra.gradle.util.JavaAgentArgumentProvider
import org.caffinitas.gradle.jflex.JFlexTask
import org.jetbrains.gradle.ext.*
import org.jetbrains.gradle.ext.Application
import org.nosphere.apache.rat.RatTask

plugins {
    `java-library`
    jacoco
    `maven-publish`
    signing
    eclipse
    idea
    id("org.jetbrains.gradle.plugin.idea-ext") version "0.7"
    id("org.caffinitas.gradle.aggregatetestresults") version "0.1"
    id("org.caffinitas.gradle.compilecommand") version "0.1.2"
    id("org.caffinitas.gradle.jflex") version "0.1.1"
    id("org.caffinitas.gradle.antlr") version "0.1"
    id("org.caffinitas.gradle.testrerun") version "0.1"
    id("org.caffinitas.gradle.testsummary") version "0.1.1"
    id("org.caffinitas.gradle.microbench") version "0.1.1"
    id("de.marcphilipp.nexus-publish") version "0.4.0"
    id("org.nosphere.apache.rat") version "0.6.0"
    id("com.github.johnrengelman.shadow") version "5.2.0"
    // plugins in buildSrc/
    id("org.apache.cassandra.testvariants")
    id("org.apache.cassandra.testrunner")
    id("org.apache.cassandra.specialtests")
    id("org.apache.cassandra.tools")
    id("org.apache.cassandra.cqldoc")
    id("org.apache.cassandra.gendoc")
    id("org.apache.cassandra.repos")
    id("org.apache.cassandra.distarchives")
    id("org.apache.cassandra.dtestjars")
}

tasks.configureEach<Task>("tasks", "help") {
    doLast { println(projectDir.resolve("gradle/additional-help.txt").readText()) }
}

val jamm: Configuration by configurations.creating
val wikitext: Configuration by configurations.creating
val javaAllocationInstrumenter: Configuration by configurations.creating
val byteman: Configuration by configurations.creating
configurations.named("testImplementation") {
    extendsFrom(byteman)
}

/**
 * Just setup the "main" + "test" source-sets here. The source-sets, tasks, configurations, etc are setup
 * below for tools (e.g. stress, fqltool) and additional test sources (distributed, long, burn, memory).
 */
sourceSets {
    main {
        java.outputDir = buildDir.resolve("classes/main")
        java.srcDir("src/java")
        resources.srcDir("src/resources")
    }
    test {
        java.srcDir("test/unit")
        resources.srcDirs("test/resources", "test/conf")
    }
}

/**
 * Convenience tasks that depends on all compile-tasks.
 */
val compileAll by tasks.registering {
    group = "Build"
    description = "Compile all sources."

    dependsOn(tasks.withType<AbstractCompile>())
}

/**
 * Sets up source-sets, configurations, jar + test tasks for tools.
 */
cassandraTools {
    register("fqltool")
    register("stress")
}

/**
 * Sets up source-sets, configurations, test tasks for the additional test source directories in the test/ directory.
 */
specialTests {
    register("long")
    register("burn")
    register("memory")
    register("distributed")
}
tasks.named<Test>("testLong") {
    extensions.configure<TestRunnerTaskExtension> {
        timeoutSeconds = 480
    }
}
tasks.named<Test>("testBurn") {
    jvmArgs("-Dlogback.configurationFile=test/conf/logback-burntest.xml")
    extensions.configure<TestRunnerTaskExtension> {
        timeoutSeconds = 60000
    }
}
tasks.named<Test>("testMemory") {
    jvmArgumentProviders.add(JavaAgentArgumentProvider(javaAllocationInstrumenter))
    extensions.configure<TestRunnerTaskExtension> {
        timeoutSeconds = 480
    }
}
val testDistributed = tasks.named<Test>("testDistributed") {
    extensions.configure<TestRunnerTaskExtension> {
        timeoutSeconds = 360
        noParallelTests = true
    }
}

/**
 * Dependencies
 */

val bytemanVersion by extra("4.0.6")
val jammVersion by extra("0.3.2")
val ohcVersion by extra("0.5.1")
val asmVersion by extra("7.1")
val allocationInstrumenterVersion by extra("3.2.0")
val logbackVersion by extra("1.2.3")
val junitPlatformVersion by extra("1.6.2")
val junitJupiterVersion by extra("5.6.2")

dependencies {
    antlr("org.antlr:antlr:3.5.2") {
        exclude(group = "org.antlr", module = "stringtemplate")
    }

    implementation("org.xerial.snappy:snappy-java:1.1.2.6")
    implementation("org.lz4:lz4-java:1.7.1")
    testImplementation("com.ning:compress-lzf:0.8.4")
    implementation("com.github.luben:zstd-jni:1.3.8-5")
    implementation("com.google.guava:guava:27.0-jre") {
        exclude(group = "com.google.guava", module = "failureaccess")
        exclude(group = "com.google.guava", module = "listenablefuture")
        exclude(group = "com.google.code.findbugs", module = "jsr305")
        exclude(group = "org.checkerframework", module = "checker-qual")
        exclude(group = "com.google.errorprone", module = "error_prone_annotations")
        exclude(group = "com.google.j2objc", module = "j2objc-annotations")
        exclude(group = "org.codehaus.mojo", module = "animal-sniffer-annotations")
    }
    testImplementation("org.checkerframework:checker-qual:2.0.0")
    implementation("com.google.j2objc:j2objc-annotations:1.3")
    implementation("org.hdrhistogram:HdrHistogram:2.1.9")
    implementation("commons-cli:commons-cli:1.1")
    implementation("commons-codec:commons-codec:1.9")
    testImplementation("commons-io:commons-io:2.2")
    testImplementation("commons-lang:commons-lang:2.4")
    implementation("org.apache.commons:commons-lang3:3.1")
    implementation("org.apache.commons:commons-math3:3.2")
    testImplementation("commons-collections:commons-collections:3.2.1")
    implementation("org.antlr:antlr-runtime:3.5.2") {
        exclude(group = "org.antlr", module = "stringtemplate")
    }
    implementation("org.slf4j:slf4j-api:1.7.25")
    implementation("org.slf4j:log4j-over-slf4j:1.7.25")
    implementation("org.slf4j:jcl-over-slf4j:1.7.25")
    implementation("ch.qos.logback:logback-core:${logbackVersion}")
    implementation("ch.qos.logback:logback-classic:${logbackVersion}")
    implementation("com.fasterxml.jackson.core:jackson-core:2.9.5")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.9.5")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.9.5")
    implementation("com.googlecode.json-simple:json-simple:1.1")
    implementation("com.boundary:high-scale-lib:1.0.6")
    implementation("com.github.jbellis:jamm:${jammVersion}")
    jamm("com.github.jbellis:jamm:${jammVersion}")
    implementation("org.psjava:psjava:0.1.19")

    implementation("org.yaml:snakeyaml:1.11")
    testImplementation("org.quicktheories:quicktheories:0.25")
    compileOnly("org.apache.hadoop:hadoop-core:1.0.3") {
        exclude(group = "org.mortbay.jetty", module = "servlet-api")
        exclude(group = "commons-logging", module = "commons-logging")
        exclude(group = "org.eclipse.jdt", module = "core")
        exclude(group = "ant", module = "ant")
        exclude(group = "junit", module = "junit")
        exclude(group = "org.slf4j", module = "slf4j-api")
    }
    compileOnly("org.apache.hadoop:hadoop-minicluster:1.0.3") {
        exclude(group = "asm", module = "asm") // this is the outdated version 3.1
        exclude(group = "org.slf4j", module = "slf4j-api")
    }
    testImplementation("org.apache.hadoop:hadoop-core:1.0.3")
    testImplementation("org.apache.hadoop:hadoop-minicluster:1.0.3")
    implementation("net.java.dev.jna:jna:4.2.2")

    compileOnly("org.jboss.byteman:byteman:${bytemanVersion}")
    byteman("org.jboss.byteman:byteman-install:${bytemanVersion}")
    byteman("org.jboss.byteman:byteman:${bytemanVersion}")
    byteman("org.jboss.byteman:byteman-submit:${bytemanVersion}")
    byteman("org.jboss.byteman:byteman-bmunit:${bytemanVersion}")

    implementation("io.dropwizard.metrics:metrics-core:3.1.5")
    implementation("io.dropwizard.metrics:metrics-logback:3.1.5")
    implementation("io.dropwizard.metrics:metrics-jvm:3.1.5")
    implementation("com.addthis.metrics:reporter-config3:3.0.3") {
        exclude(group = "org.hibernate", module = "hibernate-validator")
    }
    implementation("org.mindrot:jbcrypt:0.3m")
    implementation("io.airlift:airline:0.8") {
        exclude(group = "javax.inject", module = "javax.inject")
        exclude(group = "com.google.code.findbugs", module = "jsr305")
        exclude(group = "com.google.guava", module = "guava")
    }
    implementation("javax.inject:javax.inject:1")
    implementation("io.netty:netty-all:4.1.37.Final")
    implementation("io.netty:netty-tcnative-boringssl-static:2.0.25.Final")
    // https://mvnrepository.com/artifact/net.openhft/chronicle-bom/1.16.23
    implementation("net.openhft:chronicle-queue:4.16.3") {
        exclude(group = "net.openhft", module = "affinity")
        exclude(group = "net.java.dev.jna")
        exclude(group = "com.intellij", module = "annotations")
    }
    compileOnly("com.google.code.findbugs:jsr305:2.0.2")
    testImplementation("com.google.code.findbugs:jsr305:2.0.2")
    implementation("com.clearspring.analytics:stream:2.5.2") {
        exclude(group = "it.unimi.dsi", module = "fastutil")
    }
    implementation("com.datastax.cassandra:cassandra-driver-core:3.6.0:shaded") {
        exclude(group = "io.netty", module = "netty-buffer")
        exclude(group = "io.netty", module = "netty-codec")
        exclude(group = "io.netty", module = "netty-handler")
        exclude(group = "io.netty", module = "netty-transport")
        exclude(group = "org.slf4j", module = "slf4j-api")
        exclude(group = "org.ow2.asm")
        exclude(group = "com.github.jnr")
        exclude(group = "com.google.guava", module = "guava")
    }
    implementation("org.eclipse.jdt.core.compiler:ecj:4.6.1")
    implementation("org.caffinitas.ohc:ohc-core:${ohcVersion}") {
        exclude(group = "org.slf4j", module = "slf4j-api")
        exclude(group = "com.google.guava", module = "guava")
    }
    implementation("org.caffinitas.ohc:ohc-core-j8:${ohcVersion}") {
        exclude(group = "com.google.guava", module = "guava")
    }
    implementation("org.fusesource:sigar:1.6.4") {
        exclude(group = "log4j", module = "log4j")
    }
    implementation("com.carrotsearch:hppc:0.8.1")
    implementation("de.jflex:jflex:1.6.0") {
        exclude(group = "org.apache.ant")
    }
    implementation("com.github.rholder:snowball-stemmer:1.3.0.581.1")
    implementation("com.googlecode.concurrent-trees:concurrent-trees:2.4.0")
    implementation("com.github.ben-manes.caffeine:caffeine:2.3.5")
    implementation("org.jctools:jctools-core:1.2.1")
    implementation("org.ow2.asm:asm:${asmVersion}")
    compileOnly("org.ow2.asm:asm-tree:${asmVersion}")
    compileOnly("org.ow2.asm:asm-commons:${asmVersion}")
    implementation("org.gridkit.jvmtool:sjk-cli:0.14")
    implementation("org.gridkit.jvmtool:sjk-core:0.14") {
        exclude(group = "org.gridkit.lab")
        exclude(group = "org.gridkit.jvmtool")
        exclude(group = "org.perfkit.sjk.parsers")
    }
    implementation("org.gridkit.jvmtool:sjk-stacktrace:0.14")
    implementation("org.gridkit.jvmtool:mxdump:0.14") {
        exclude(group = "org.gridkit.jvmtool")
    }
    implementation("org.gridkit.lab:jvm-attach-api:1.5")
    implementation("org.gridkit.jvmtool:sjk-json:0.14")

    implementation("com.beust:jcommander:1.30")

    testImplementation("org.apache.cassandra:dtest-api:0.0.2")

    testImplementation("junit:junit:4.12")
    testImplementation("org.mockito:mockito-core:3.2.4")

    testImplementation("org.apache.ant:ant-junit:1.9.7")

    testImplementation("org.assertj:assertj-core:3.15.0")

    testRuntimeOnly("org.caffinitas.junitvintagetimeout:junitvintagetimeout:0.1.1")
    testRuntimeOnly(project(":tools:junitlog4j"))

    javaAllocationInstrumenter("com.google.code.java-allocation-instrumenter:java-allocation-instrumenter:${allocationInstrumenterVersion}")
    add("memoryTestImplementation", "com.google.code.java-allocation-instrumenter:java-allocation-instrumenter:${allocationInstrumenterVersion}")

    wikitext("com.datastax.wikitext:wikitext-core-ant:1.3")
    wikitext("org.fusesource.wikitext:textile-core:1.3")
}

configurations.named("distributedTestImplementation") {
    dependencies.add(project.dependencies.create(sourceSets.getByName("fqltool").output))
}

configurations.named("testImplementation") {
    extendsFrom(configurations[JacocoPlugin.ANT_CONFIGURATION_NAME])
    dependencies.add(project.dependencies.create(sourceSets.getByName("stress").output))
}

configurations.all {
    resolutionStrategy {
        force("commons-cli:commons-cli:1.1")
        force("org.yaml:snakeyaml:1.11")
        force("net.java.dev.jna:jna:4.2.2")
        force("com.google.code.findbugs:jsr305:2.0.2")
        force("io.dropwizard.metrics:metrics-core:3.1.5")
        force("io.dropwizard.metrics:metrics-jvm:3.1.5")
        // https://mvnrepository.com/artifact/net.openhft/chronicle-bom/1.16.23
        force("net.openhft:chronicle-core:1.16.4")
        force("net.openhft:chronicle-bytes:1.16.3")
        force("net.openhft:chronicle-wire:1.16.1")
        force("net.openhft:chronicle-threads:1.16.0")
        force("org.checkerframework:checker-qual:2.0.0")
    }
}

tasks.named<JFlexTask>("jflex") {
    source = fileTree("src/java") {
        include("**/*.jflex")
    }
}

cantlr {
    val generateGrammarSourceCassandra = registerCAntlr("generateGrammarSource") {
        outputDirectory = project.file("${antlrTask.get().outputDirectory}/org/apache/cassandra/cql3")
        maxHeapSize = "1g"
        arguments.addAll(listOf(
                "-Xconversiontimeout",
                "10000",
                "-Xmaxinlinedfastates",
                "10",
                // Hack to have the correct directories in there
                "-fo",
                "${outputDirectory.relativeTo(project.projectDir)}"
        ))
        includeFiles = setOf("Lexer.g", "Parser.g")
    }
    tasks.named("compileJava") {
        dependsOn(generateGrammarSourceCassandra)
    }
}

jacoco {
    // intentionally using the jacoco version brought with the jacoco plugin here
    reportsDir = buildDir.resolve("jacoco")
}
tasks.named<JacocoReport>("jacocoTestReport") {
    enabled = project.hasProperty("codecoverage")
    reports {
        // Use the same files + directories as in build.xml
        xml.isEnabled = true
        xml.destination = buildDir.resolve("jacoco/report.xml")
        csv.isEnabled = true
        csv.destination = buildDir.resolve("jacoco/report.csv")
        html.isEnabled = true
        html.destination = buildDir.resolve("jacoco")
    }
}
tasks.withType<Test>().configureEach {
    // Additional directories for Gradle distributed testing
    inputs.dir("test/conf")
    inputs.dir("test/data")
    inputs.dir("conf")
    outputs.dir(buildDir.resolve("test/logs"))
    outputs.dir(buildDir.resolve("cdc/logs"))
    outputs.dir(buildDir.resolve("compression/logs"))
    outputs.dir(buildDir.resolve("compressionZstd/logs"))
    configure<JacocoTaskExtension> {
        isEnabled = project.hasProperty("codecoverage")
    }
}

/**
 * Generates additional test-tasks for CDC + compression configurations.
 */
testVariants {
    register("cdc") {
        sourceConfigFiles = listOf("test/conf/cassandra.yaml", "test/conf/cdc.yaml")
        cassandraYaml = "test/cassandra.cdc.yaml"
        configure {
            systemProperties(
                    "cassandra.testtag" to "cdc"
            )
        }
    }
    register("compressionZst") {
        sourceConfigFiles = listOf("test/conf/cassandra.yaml", "test/conf/commitlog_compression_Zstd.yaml")
        cassandraYaml = "test/cassandra.compressed.zstd.yaml"
        configure {
            systemProperties(
                    "cassandra.test.compression" to true,
                    "cassandra.testtag" to "compressionZstd")
        }
    }
    register("compressionLZ4") {
        sourceConfigFiles = listOf("test/conf/cassandra.yaml", "test/conf/commitlog_compression_LZ4.yaml")
        cassandraYaml = "test/cassandra.compressed.lz4.yaml"
        configure {
            systemProperties(
                    "cassandra.test.compression" to true,
                    "cassandra.testtag" to "compression")
        }
    }
}

val testUnit by tasks.registering(Test::class) {
    withJUnitPlatform {
        includeTags("org.apache.cassandra.test.tags.Unit")
    }
    systemProperty("cassandra.test.pureunit", "true")

    // Convenience - apply command-line test patterns from "main" test task to "testUnit" as well.
    // E.g. a `./gradlew :test --tests UF*Test` would also apply `UF*Test` as the command-line pattern to `:testUnit`
    doFirst {
        val f = filter as org.gradle.api.internal.tasks.testing.filter.DefaultTestFilter
        if (f.commandLineIncludePatterns.isEmpty()) {
            for (m in listOf("test", "testCdc", "testCompressionLZ4", "testCompressionZst")) {
                val main = tasks.named<Test>(m).get().filter as org.gradle.api.internal.tasks.testing.filter.DefaultTestFilter
                if (main.commandLineIncludePatterns.isNotEmpty())
                    f.setCommandLineIncludePatterns(main.commandLineIncludePatterns)
            }
        }
    }
}
tasks.configureEach<Test>("test", "testCdc", "testCompressionLZ4", "testCompressionZst") {
    dependsOn(testUnit)
    withJUnitPlatform {
        excludeTags("org.apache.cassandra.test.tags.Unit")
    }
}

tasks.configureEach<Test>("testUnit", "test", "testLong", "testBurn", "testMemory", "testDistributed", "testFqltool", "testStress", "testCdc", "testCompressionLZ4", "testCompressionZst") {
    filter.isFailOnNoMatchingTests = false
}

tasks.register<DefaultTask>("testAll") {
    group = "verification"
    description = "Run all test* tasks"

    dependsOn(tasks.withType(Test::class))
}

/**
 * Common configuration for all test-tasks
 */
testRunner {
    timeoutSeconds = 480

    java11Args = listOf(
            "-Djdk.attach.allowAttachSelf=true",

            "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
            "--add-exports", "java.base/jdk.internal.ref=ALL-UNNAMED",
            "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-exports", "java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED",
            "--add-exports", "java.rmi/sun.rmi.registry=ALL-UNNAMED",
            "--add-exports", "java.rmi/sun.rmi.server=ALL-UNNAMED",
            "--add-exports", "java.sql/java.sql=ALL-UNNAMED",

            "--add-opens", "java.base/java.lang.module=ALL-UNNAMED",
            "--add-opens", "java.base/java.net=ALL-UNNAMED",
            "--add-opens", "java.base/jdk.internal.loader=ALL-UNNAMED",
            "--add-opens", "java.base/jdk.internal.ref=ALL-UNNAMED",
            "--add-opens", "java.base/jdk.internal.reflect=ALL-UNNAMED",
            "--add-opens", "java.base/jdk.internal.math=ALL-UNNAMED",
            "--add-opens", "java.base/jdk.internal.module=ALL-UNNAMED",
            "--add-opens", "java.base/jdk.internal.util.jar=ALL-UNNAMED",
            "--add-opens", "jdk.management/com.sun.management.internal=ALL-UNNAMED")

    tempDir = file(System.getProperty("tmp.dir", buildDir.resolve("test/tmp.dir").absolutePath))

    jvmArgs = listOf("-ea",
            "-Xss256k",
            /* When we do classloader manipulation SoftReferences can cause memory leaks
                 that can OOM our test runs. The next two settings informs our GC
                 algorithm to limit the metaspace size and clean up SoftReferences
                 more aggressively rather than waiting. See CASSANDRA-14922 for more details.
            */
            "-XX:MaxMetaspaceSize=384M",
            "-XX:MetaspaceSize=128M",
            "-XX:MaxMetaspaceExpansion=64M",
            "-XX:SoftRefLRUPolicyMSPerMB=0")

    systemProperties = mapOf(
            "storage-config" to "${file("test/conf").relativeTo(projectDir)}",
            "java.awt.headless" to "true",
            "cassandra.debugrefcount" to "true",
            "cassandra.memtable_row_overhead_computation_step" to "100",
            "cassandra.test.use_prepared" to "${System.getProperty("cassandra.test.use_prepared", "true")!!.toBoolean()}",
            "cassandra.test.sstableformatdevelopment" to "true",
            // The first time SecureRandom initializes can be slow if it blocks on /dev/random
            "java.security.egd" to "file:/dev/urandom",
            "cassandra.keepBriefBrief" to "${System.getProperty("cassandra.keepBriefBrief", "true")!!.toBoolean()}",
            "cassandra.strict.runtime.checks" to "true",
            // disable shrinks in quicktheories CASSANDRA-15554 -->
            "QT_SHRINKS" to "0",

            "legacy-sstable-root" to "${file("test/data").relativeTo(projectDir)}/legacy-sstables",
            "invalid-legacy-sstable-root" to "${file("test/data").relativeTo(projectDir)}/invalid-legacy-sstables",
            "cassandra.ring_delay_ms" to "1000",
            "cassandra.tolerate_sstable_size" to "true",
            "cassandra.config.loader" to "org.apache.cassandra.OffsetAwareConfigurationLoader",
            "cassandra.skip_sync" to "true")
}

java {
    @Suppress("UnstableApiUsage")
    withJavadocJar()
    @Suppress("UnstableApiUsage")
    withSourcesJar()
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = if (JavaVersion.current().isJava11Compatible) {
        JavaVersion.VERSION_11
    } else {
        JavaVersion.VERSION_1_8
    }
}

val sourcesJar by tasks.named<Jar>("sourcesJar") {
    group = "build"
    description = "Creates the sources-jar"

    archiveClassifier.set("sources")
    from({ sourceSets.main.get().allSource })
}
val javadoc = tasks.named<Javadoc>("javadoc") {
    isFailOnError = false
    title = "Apache Cassandra ${project.version}"

    if (JavaVersion.current().isJava11Compatible) {
        options {
            val customOptionsFile = file("gradle/javadoc-java11.options")
            inputs.file(customOptionsFile).withPathSensitivity(PathSensitivity.RELATIVE)
            optionFiles(customOptionsFile)
        }
    }

    options {
        showFromPackage()
        encoding = "UTF-8"
        windowTitle = "Apache Cassandra  ${project.version}"
        header = "Apache&reg; Cassandra&trade; ${project.version} - APIs are subject to chage without notice"
    }
}
val javadocJar by tasks.named<Jar>("javadocJar") {
    group = "build"
    description = "Creates the javadoc-jar"

    archiveClassifier.set("javadoc")
    from(javadoc)
}

val jar = tasks.named<Jar>("jar")

microbench {
    if (JavaVersion.current().isJava11Compatible)
        jvmOptions.set(testRunner.java11Args)
}

publishing {
    publications.register<MavenPublication>("cassandra") {
        artifactId = "cassandra-all"
        pom {
            commonCassandraPom(this, project.group, "cassandra-parent", project.version, project)
        }
        from(components.getByName("java"))
    }
    publications.register<MavenPublication>("cassandraParent") {
        artifactId = "cassandra-parent"
        pom {
            commonCassandraPom(this, "org.apache", "apache", "22", project)
            generateParentPom {
                val microbenchSourceSet = sourceSets.getByName("microbench")

                // Need to resolve these first
                configurations.getByName(JacocoPlugin.ANT_CONFIGURATION_NAME).resolve()
                configurations.getByName(JacocoPlugin.AGENT_CONFIGURATION_NAME).resolve()

                setOf<Dependency>() +
                        configurations.runtimeClasspath.get().allDependencies +
                        configurations.annotationProcessor.get().allDependencies +
                        configurations.testRuntimeClasspath.get().allDependencies +
                        project.dependencies.create("org.apache.rat:apache-rat:0.10") + // the rat version declared by ant, the rat plugin (0.6.0) uses 0.13
                        configurations.getByName(JacocoPlugin.ANT_CONFIGURATION_NAME).allDependencies +
                        configurations.getByName(JacocoPlugin.AGENT_CONFIGURATION_NAME).allDependencies +
                        configurations.getByName(microbenchSourceSet.implementationConfigurationName).allDependencies +
                        configurations.getByName(microbenchSourceSet.annotationProcessorConfigurationName).allDependencies
            }
        }
    }
}
val copyCassandraPoms by tasks.registering(Copy::class) {
    dependsOn(tasks.named("generatePomFileForCassandraPublication"))

    from(buildDir.resolve("publications/cassandra")) {
        include("pom-default.xml*")
        rename { n: String ->
            when (n) {
                "pom-default.xml" -> jar.get().archiveFileName.get().replace(".jar", ".pom")
                "pom-default.xml.asc" -> jar.get().archiveFileName.get().replace(".jar", ".pom.asc")
                else -> n
            }
        }
    }
    into(buildDir)
}
val copyCassandraParentPoms by tasks.registering(Copy::class) {
    dependsOn(tasks.named("generatePomFileForCassandraParentPublication"))

    from(buildDir.resolve("publications/cassandraParent")) {
        include("pom-default.xml*")
        rename { n: String ->
            when (n) {
                "pom-default.xml" -> jar.get().archiveFileName.get().replace(".jar", "-parent.pom")
                "pom-default.xml.asc" -> jar.get().archiveFileName.get().replace(".jar", "-parent.pom.asc")
                else -> n
            }
        }
    }
    into(buildDir)
}
tasks.named("generatePomFileForCassandraPublication") {
    // Probably want both
    finalizedBy(copyCassandraPoms, tasks.named("generatePomFileForCassandraParentPublication"))
}
tasks.named("generatePomFileForCassandraParentPublication") {
    // Probably want both
    dependsOn(tasks.named("generatePomFileForCassandraPublication"))
    finalizedBy(copyCassandraParentPoms)
}

if (project.hasProperty("withSigning")) {
    // Only sign artifacts if the `withSigning` project property is present.
    // Signing depends on publications (for distributions), which includes javadoc-jar, sources-jar and more
    // stuff and since it's meant to sign artifacts, it would also sign the "ordinary jar" - i.e. a
    // `./gradelw jar` would run javadoc and perform signing as well.
    // Think of this `-PwithSigning` toggle as an optimization to speed up day-to-day developer workflows.
    copyCassandraPoms {
        dependsOn(tasks.named("signCassandraPublication"))
    }
    copyCassandraParentPoms {
        dependsOn(tasks.named("signCassandraParentPublication"))
    }
    tasks.named("generatePomFileForCassandraPublication") {
        finalizedBy(tasks.named("signCassandraPublication"))
    }
    tasks.named("generatePomFileForCassandraParentPublication") {
        finalizedBy(tasks.named("signCassandraParentPublication"))
    }
    signing {
        useGpgCmd()
        afterEvaluate {
            sign(publishing.publications["cassandra"])
            sign(publishing.publications["cassandraParent"])
        }
    }
    tasks.withType<Sign> {
        onlyIf { project.hasProperty("signing.gnupg.keyName") }
    }
}

nexusPublishing {
    clientTimeout.set(Duration.ofMinutes(3))
    repositories {
        sonatype()
    }
}

/**
 * Take the various hotspot_compiler file snippets and produces a single file.
 */
compileCommands {
    add {
        outputFile = project.file("conf/hotspot_compiler")
        sourceSet = sourceSets.main
    }
}

val generateVersionProperties by tasks.registering(WriteProperties::class) {
    outputFile = file("src/resources/org/apache/cassandra/config/version.properties")
    property("CassandraVersion", version)
}

tasks.named<JavaCompile>(sourceSets.main.get().compileJavaTaskName) {
    dependsOn(generateVersionProperties)
}

/**
 * Common settings for all Java compilation tasks.
 */
tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"

    if (JavaVersion.current().isJava11Compatible)
        options.compilerArgs.addAll(listOf("--add-exports", "java.rmi/sun.rmi.registry=ALL-UNNAMED"))
}

val syncLibDir by tasks.registering(Sync::class) {
    group = "Build"
    description = "Synchronizes the contents of the lib/ directoriy with the declared runtime dependencies."

    dependsOn(jar)

    destinationDir = project.file("lib")
    duplicatesStrategy = DuplicatesStrategy.INCLUDE

    from(configurations.runtimeClasspath)

    exclude("antlr-3*.jar")

    rename { n: String -> if ("javax.inject-1.jar" == n) "javax.inject.jar" else n }

    preserve {
        include("licenses/**")
        include("*.zip")
        include("sigar-bin/**")
        include("jstackjunit-0.0.1.jar") // not in maven-central
    }
}

val populateBuildLibJarsForDtest by tasks.registering(Sync::class) {
    destinationDir = project.buildDir.resolve("lib/jars")

    from(byteman)
}

/**
 * Common settings for all jar tasks.
 */
tasks.withType<Jar>().configureEach {
    @Suppress("UnstableApiUsage")
    manifest {
        attributes["Implementation-Title"] = "Cassandra"
        attributes["Implementation-Version"] = project.version
        attributes["Implementation-Vendor"] = "Apache"
        attributes["Implementation-URL"] = "https://cassandra.apache.org"
        if (project.hasProperty("release")) {
            attributes["Build-Jdk"] = "${System.getProperty("java.runtime.version")} (${System.getProperty("java.vendor")} ${System.getProperty("java.vendor.version")} ${System.getProperty("java.version.date")})"
            attributes["X-Build-Timestamp"] = System.currentTimeMillis()
            attributes["X-Build-OS"] = "${System.getProperty("os.name")} ${System.getProperty("os.version")}"
        }
    }
}

dtests {
    register("2.2")
    register("3.0")
    register("3.11")
}
val dtestJar by tasks.registering(ShadowJar::class) {
    dependsOn(tasks.named("classes"),
            tasks.named("testClasses"),
            tasks.named("processResources"),
            tasks.named("processTestResources"))

    destinationDirectory.set(project.buildDir)
    archiveBaseName.set("dtest")
    archiveVersion.set(project.version.toString().replace("-SNAPSHOT", ""))

    from("build/classes/main") {
        exclude("compile-command-incr")
    }
    from("build/classes/java/test")
    from("build/classes/java/distributedTest")
    from("build/resources/main")
    from("build/resources/distributedTest")

    configurations = listOf(project.configurations.getByName("runtimeClasspath"))
}
testDistributed {
    dependsOn(dtestJar)
    inputs.file(dtestJar.get().archiveFile)
}

jar {
    destinationDirectory.fileValue(buildDir)

    exclude("compile-command-incr/**")

    finalizedBy(syncLibDir)
    dependsOn(populateBuildLibJarsForDtest)
    dependsOn(tasks.named("generatePomFileForCassandraPublication"))

    into("META-INF/maven/${project.group}/${project.name}") {
        from(tasks.named("generatePomFileForCassandraPublication"))
        rename("pom-default.xml", "pom.xml")
    }
}

val gendoc by tasks.existing
val cleanGendoc by tasks.existing
val cqldoc by tasks.existing
val cleanCqldoc by tasks.existing

tasks.named("clean") {
    dependsOn(cleanCqldoc)
    dependsOn(cleanGendoc)
}

tasks.named("assemble") {
    dependsOn(cqldoc)
    dependsOn(gendoc)
}

distributions {
    main {
        contents {
            from(".") {
                include("*.txt",
                        "*.asc",
                        "*.md",
                        "bin/**",
                        "conf/**",
                        "pylib/**",
                        "tools/bin/**",
                        "tools/*.yaml")
                exclude("pylib/**/*.pyc")
            }

            into("tools/lib") {
                from(tasks.named("fqltoolJar"),
                        tasks.named("stressJar"))
            }

            into("lib") {
                from("lib",
                        jar)
            }
            into("javadoc") {
                from(javadoc)
            }
            into("doc") {
                into("cql3") {
                    from(cqldoc)
                }
                from("doc") {
                    include("SASI.md")
                }
                from(gendoc)
            }
        }
    }
    register("src") {
        contents {
            from(projectDir) {
                exclude("build/**",
                        "*/build/**",
                        "src/gen-java/**",
                        ".git/**",
                        ".project",
                        ".classpath",
                        ".settings/**",
                        ".idea/**",
                        "out",
                        "*/out",
                        ".gradle/**",
                        "buildSrc/.gradle/**",
                        ".externalToolBuilders/**",
                        "venv",
                        "logs",
                        "data",
                        // artifacts from tests
                        "audit/**",
                        "*.log")
            }
        }
    }
}

tasks.named<RatTask>("rat") {
    exclude(file(".rat-excludes").readLines().filter { l -> l.isNotEmpty() })
    doLast {
        copy {
            from(reportDir) {
                include("rat-report.txt")
                rename { "rat-report.log" }
            }
            into(buildDir)
        }
    }
}

tasks.register<JavaExec>("runCassandra") {
    group = "run"
    description = "Run CassandraDaemon"

    dependsOn(jar)

    debug = true

    mainClass.set("org.apache.cassandra.service.CassandraDaemon")
    if (JavaVersion.current().isJava11Compatible) {
        jvmArgs("@${project.projectDir}/conf/jvm-server.options",
                "@${project.projectDir}/conf/jvm11-server.options",
                "-Xms2g",
                "-Xmx2g")
    }
    else {
        listOf("conf/jvm-server.options",
               "conf/jvm8-server.options").forEach { f ->
            jvmArgs(file(f).readLines().map { s -> s.trim() }.filter { s -> s.isNotEmpty() && !s.startsWith("#") })
        }
        jvmArgs("-Xms2g",
                "-Xmx2g")
    }

    systemProperties("cassandra.config" to "file://${project.projectDir}/conf/cassandra.yaml",
            "cassandra.storagedir" to "${project.projectDir}/data",
            "logback.configurationFile" to "file://${project.projectDir}/conf/logback.xml",
            "cassandra.logdir" to "${project.projectDir}/logs",
            "java.library.path" to "${project.projectDir}/lib/sigar-bin",
            "QT_SHRINKS" to "0")

    classpath = sourceSets.main.get().runtimeClasspath
}

val ideName = "Apache Cassandra ${rootProject.version.toString().replace(Regex("^([0-9.]+).*"), "$1")}"
idea {
    module {
        name = ideName
        isDownloadSources = true // this is the default BTW
        excludeDirs.add(file("doc/build"))
        excludeDirs.add(file("logs"))
        excludeDirs.add(file("data"))
        inheritOutputDirs = true
    }

    project {
        withGroovyBuilder {
            "settings" {
                val copyright: CopyrightConfiguration = getProperty("copyright") as CopyrightConfiguration
                val encodings: EncodingConfiguration = getProperty("encodings") as EncodingConfiguration
                val delegateActions: ActionDelegationConfig = getProperty("delegateActions") as ActionDelegationConfig
                val runConfigurations: RunConfigurationContainer = getProperty("runConfigurations") as RunConfigurationContainer

                runConfigurations.defaults(Application::class.java) {
                    jvmArgs = "-Dcassandra.config=file://\$PROJECT_DIR\$/conf/cassandra.yaml " +
                            "-Dcassandra.storagedir=\$PROJECT_DIR\$/data " +
                            "-Dlogback.configurationFile=file://\$PROJECT_DIR\$/conf/logback.xml " +
                            "-Dcassandra.logdir=\$PROJECT_DIR\$/data/logs " +
                            "-Djava.library.path=\$PROJECT_DIR\$/lib/sigar-bin " +
                            "-DQT_SHRINKS=0 " +
                            "-ea " +
                            (if (JavaVersion.current().isJava11Compatible) testRunner.java11Args.joinToString(" ") else "")
                    moduleName = "apache-cassandra"
                }

                runConfigurations.defaults(JUnit::class.java) {
                    vmParameters = testRunner.jvmArgs.joinToString(" ") +
                            "-Dcassandra.testtag= " +
                            (if (JavaVersion.current().isJava11Compatible) testRunner.java11Args.joinToString(" ") else "")
                }

                delegateActions.testRunner = ActionDelegationConfig.TestRunner.CHOOSE_PER_TEST

                encodings.encoding = "UTF-8"
                encodings.properties.encoding = "UTF-8"

                copyright.useDefault = "Apache"
                copyright.profiles.create("Apache") {
                    notice = file("gradle/license.txt").readText()
                }
            }
        }
    }
}
// There's no proper way to set the name of the IDEA project (when "just importing" or syncing the Gradle project)
val ideaDir = projectDir.resolve(".idea")
if (ideaDir.isDirectory)
    ideaDir.resolve(".name").writeText(ideName)

eclipse {
    project {
        name = ideName
    }
}
