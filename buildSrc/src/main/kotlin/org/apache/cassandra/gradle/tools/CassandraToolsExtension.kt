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
package org.apache.cassandra.gradle.tools

import org.gradle.api.Namer
import org.gradle.api.internal.AbstractValidatingNamedDomainObjectContainer
import org.gradle.api.internal.CollectionCallbackActionDecorator
import org.gradle.api.model.ObjectFactory
import org.gradle.internal.reflect.DirectInstantiator
import javax.inject.Inject

/**
 * Boilerplate code to setup test tasks that "extend" the "normal" test tasks - i.e. the stuff that's needed
 * to setup the `testCdc` and `testCompression*` tasks.
 */
open class CassandraToolsExtension : AbstractValidatingNamedDomainObjectContainer<CassandraTool>(CassandraTool::class.java, DirectInstantiator.INSTANCE, TestConfigNamer(), CollectionCallbackActionDecorator.NOOP) {
    @get:Inject
    open val objectFactory: ObjectFactory
        get() {
            throw UnsupportedOperationException()
        }

    override fun doCreate(name: String): CassandraTool {
        return objectFactory.newInstance(CassandraTool::class.java, name)
    }

    private class TestConfigNamer : Namer<CassandraTool> {
        override fun determineName(cfg: CassandraTool): String {
            return cfg.name
        }
    }
}