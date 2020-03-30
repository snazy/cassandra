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

package org.apache.cassandra.buildtest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.Objects;

import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.BuildTask;
import org.gradle.testkit.runner.GradleRunner;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.gradle.testkit.runner.TaskOutcome.SUCCESS;
import static org.gradle.testkit.runner.TaskOutcome.UP_TO_DATE;

@SuppressWarnings("ConstantConditions")
@TestMethodOrder(OrderAnnotation.class)
public class GradleBuildTest
{
    private final String rootProjectDirectory = System.getProperty("rootProject.projectDirectory");
    private final String rootProjectVersion = System.getProperty("rootProject.version");

    @Test
    @Order(0)
    public void smoketest()
    {
        BuildResult result = GradleRunner.create()
                                         .withProjectDir(rootProjectDir())
                                         .withArguments(":clean", ":tasks", "--stacktrace", "--info", "--no-scan")
                                         .build();

        assertThat(Objects.requireNonNull(result.task(":tasks")))
                .extracting(BuildTask::getOutcome).isIn(SUCCESS);
    }

    private File rootProjectDir()
    {
        return new File(rootProjectDirectory).getAbsoluteFile();
    }

    @Test
    public void jar()
    {
        BuildResult result = GradleRunner.create()
                                         .withProjectDir(rootProjectDir())
                                         .withArguments(":clean", ":jar", "--stacktrace", "--info", "--no-scan")
                                         .build();

        assertThat(Arrays.asList(":jar",
                                 ":syncLibDir"))
                .extracting(result::task)
                .extracting(BuildTask::getOutcome)
                .allMatch(outcome -> outcome == SUCCESS || outcome == UP_TO_DATE);

        assertThat(Arrays.asList("build/apache-cassandra-" + rootProjectVersion + ".jar",
                                 "build/apache-cassandra-" + rootProjectVersion + ".pom",
                                 "build/apache-cassandra-" + rootProjectVersion + "-parent.pom",
                                 "build/tools/lib/stress.jar",
                                 "build/tools/lib/fqltool.jar"))
                .allMatch(file -> new File(rootProjectDir(), file).exists());
    }

    @Test
    public void antlrIncludeFileChangeTriggersAntlrTask() throws IOException
    {
        assertThat(GradleRunner.create()
                               .withProjectDir(rootProjectDir())
                               .withArguments(":clean", ":generateGrammarSourceCassandra")
                               .build()
                               .task(":generateGrammarSourceCassandra")
                               .getOutcome())
                .isIn(SUCCESS, UP_TO_DATE);

        // Re-run - expect "up to date"
        assertThat(GradleRunner.create()
                               .withProjectDir(rootProjectDir())
                               .withArguments(":generateGrammarSourceCassandra", "--stacktrace", "--info", "--no-scan")
                               .build()
                               .task(":generateGrammarSourceCassandra")
                               .getOutcome())
                .isEqualTo(UP_TO_DATE);

        for (String antlrFile : Arrays.asList("src/antlr/Lexer.g", "src/antlr/Cql.g"))
        {
            Path antlrFilePath = rootProjectDir().toPath().resolve(antlrFile);
            FileTime lastModified = Files.getLastModifiedTime(antlrFilePath);
            byte[] original = Files.readAllBytes(antlrFilePath);
            try
            {
                Files.write(antlrFilePath,
                            ("\n\n/* Some additional content " + System.nanoTime() + "*/\n").getBytes(),
                            StandardOpenOption.APPEND);

                // include file has changed, must have worked
                assertThat(GradleRunner.create()
                                       .withProjectDir(rootProjectDir())
                                       .withArguments(":generateGrammarSourceCassandra")
                                       .build()
                                       .task(":generateGrammarSourceCassandra")
                                       .getOutcome())
                        .describedAs("initial :generateGrammarSourceCassandra for {} with modified content", antlrFile)
                        .isEqualTo(SUCCESS);

                assertThat(GradleRunner.create()
                                       .withProjectDir(rootProjectDir())
                                       .withArguments(":generateGrammarSourceCassandra")
                                       .build()
                                       .task(":generateGrammarSourceCassandra")
                                       .getOutcome())
                        .describedAs("re-run :generateGrammarSourceCassandra for {} with modified content", antlrFile)
                        .isEqualTo(UP_TO_DATE);

                // revert original content
                Files.write(antlrFilePath,
                            original,
                            StandardOpenOption.TRUNCATE_EXISTING);
                assertThat(GradleRunner.create()
                                       .withProjectDir(rootProjectDir())
                                       .withArguments(":generateGrammarSourceCassandra")
                                       .build()
                                       .task(":generateGrammarSourceCassandra")
                                       .getOutcome())
                        .describedAs("re-initial :generateGrammarSourceCassandra for {} with original content", antlrFile)
                        .isEqualTo(SUCCESS);

                assertThat(GradleRunner.create()
                                       .withProjectDir(rootProjectDir())
                                       .withArguments(":generateGrammarSourceCassandra")
                                       .build()
                                       .task(":generateGrammarSourceCassandra")
                                       .getOutcome())
                        .describedAs("re-run :generateGrammarSourceCassandra for {} with original content", antlrFile)
                        .isEqualTo(UP_TO_DATE);
            }
            finally
            {
                Files.write(antlrFilePath, original);
                Files.setLastModifiedTime(antlrFilePath, lastModified);
            }
        }
    }

    @Test
    public void assemble()
    {
        BuildResult result = GradleRunner.create()
                                         .withProjectDir(rootProjectDir())
                                         .withArguments(":clean", ":assemble", "--stacktrace", "--info", "--no-scan")
                                         .build();

        assertThat(Arrays.asList(":jar",
                                 ":syncLibDir",
                                 ":gendoc",
                                 ":javadoc",
                                 ":distTar",
                                 ":distZip",
                                 ":srcDistTar",
                                 ":srcDistZip"))
                .extracting(result::task)
                .extracting(BuildTask::getOutcome)
                .allMatch(outcome -> outcome == SUCCESS || outcome == UP_TO_DATE);

        assertThat(Arrays.asList("build/apache-cassandra-" + rootProjectVersion + ".jar",
                                 "build/apache-cassandra-" + rootProjectVersion + ".pom",
                                 "build/apache-cassandra-" + rootProjectVersion + "-parent.pom",
                                 "build/apache-cassandra-" + rootProjectVersion + "-bin.tar.gz",
                                 "build/apache-cassandra-" + rootProjectVersion + "-bin.tar.gz.sha256",
                                 "build/apache-cassandra-" + rootProjectVersion + "-bin.tar.gz.sha512",
                                 "build/apache-cassandra-" + rootProjectVersion + "-bin.zip",
                                 "build/apache-cassandra-" + rootProjectVersion + "-bin.zip.sha256",
                                 "build/apache-cassandra-" + rootProjectVersion + "-bin.zip.sha512",
                                 "build/apache-cassandra-" + rootProjectVersion + "-src.tar.gz",
                                 "build/apache-cassandra-" + rootProjectVersion + "-src.tar.gz.sha256",
                                 "build/apache-cassandra-" + rootProjectVersion + "-src.tar.gz.sha512",
                                 "build/apache-cassandra-" + rootProjectVersion + "-src.zip",
                                 "build/apache-cassandra-" + rootProjectVersion + "-src.zip.sha256",
                                 "build/apache-cassandra-" + rootProjectVersion + "-src.zip.sha512",
                                 "build/docs/javadoc/index.html",
                                 "build/libs/apache-cassandra-" + rootProjectVersion + "-sources.jar",
                                 "build/libs/apache-cassandra-" + rootProjectVersion + "-javadoc.jar",
                                 "doc/build/html/index.html",
                                 "doc/build/html/tools/cqlsh.html"))
                .allMatch(file -> new File(rootProjectDir(), file).exists());
    }

    @Test
    public void compileAll()
    {
        BuildResult result = GradleRunner.create()
                                         .withProjectDir(rootProjectDir())
                                         .withArguments(":clean", ":compileAll", "--stacktrace", "--info", "--no-scan")
                                         .build();

        assertThat(Arrays.asList(":compileBurnTestJava",
                                 ":compileDistributedTestJava",
                                 ":compileFqltoolJava",
                                 ":compileFqltoolTestJava",
                                 ":compileLongTestJava",
                                 ":compileMemoryTestJava",
                                 ":compileMicrobenchJava",
                                 ":compileStressJava",
                                 ":compileStressTestJava",
                                 ":generateGrammarSourceCassandra",
                                 ":jflex",
                                 ":compileAll"))
                .extracting(result::task)
                .extracting(BuildTask::getOutcome)
                .allMatch(outcome -> outcome == SUCCESS || outcome == UP_TO_DATE);
    }

    @Test
    public void microbench()
    {
        assertThat(GradleRunner.create()
                               .withProjectDir(rootProjectDir())
                               .withArguments(":clean", ":microbench", "--stacktrace", "--info", "--no-scan")
                               .build()
                               .task(":microbench")
                               .getOutcome())
                .isIn(SUCCESS, UP_TO_DATE);
    }
}
