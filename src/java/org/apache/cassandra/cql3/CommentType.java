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

package org.apache.cassandra.cql3;

import java.util.List;

import org.apache.cassandra.auth.FunctionResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.ClientState;

public enum CommentType
{
    KEYSPACE("keyspace")
    {
        public void checkAccess(ClientState state, String keyspace, String entityName, String entitySpec, List<AbstractType<?>> argTypes)
        {
            state.hasKeyspaceAccess(keyspace, Permission.ALTER);
        }
    },

    TABLE("table")
    {
        public void checkAccess(ClientState state, String keyspace, String entityName, String entitySpec, List<AbstractType<?>> argTypes)
        {
            state.hasColumnFamilyAccess(keyspace, entityName, Permission.ALTER);
        }
    },

    COLUMN("column")
    {
        public void checkAccess(ClientState state, String keyspace, String entityName, String entitySpec, List<AbstractType<?>> argTypes)
        {
            state.hasColumnFamilyAccess(keyspace, entityName, Permission.ALTER);
        }
    },

    TYPE("type")
    {
        public void checkAccess(ClientState state, String keyspace, String entityName, String entitySpec, List<AbstractType<?>> argTypes)
        {
            state.hasKeyspaceAccess(keyspace, Permission.ALTER);
        }
    },

    ROLE("role")
    {
        public void checkAccess(ClientState state, String keyspace, String entityName, String entitySpec, List<AbstractType<?>> argTypes)
        {
            if (state.getUser().isSuper())
                return;
            state.ensureHasPermission(Permission.ALTER, RoleResource.role(entityName));
        }
    },

    INDEX("index")
    {
        public void checkAccess(ClientState state, String keyspace, String entityName, String entitySpec, List<AbstractType<?>> argTypes)
        {
            // TODO need to use the index's table name and pass it to hasColumnFamilyAccess (entityName parameter contains the index name)
            state.hasColumnFamilyAccess(keyspace, entityName, Permission.ALTER);
        }
    },

    MATERIALIZED_VIEW("materialized_view")
    {
        public void checkAccess(ClientState state, String keyspace, String entityName, String entitySpec, List<AbstractType<?>> argTypes)
        {
            state.hasColumnFamilyAccess(keyspace, entityName, Permission.ALTER);
        }
    },

    FUNCTION("function")
    {
        public void checkAccess(ClientState state, String keyspace, String entityName, String entitySpec, List<AbstractType<?>> argTypes)
        {
            state.ensureHasPermission(Permission.ALTER, FunctionResource.function(keyspace, entityName, argTypes));
        }
    },

    AGGREGATE("aggregate")
    {
        public void checkAccess(ClientState state, String keyspace, String entityName, String entitySpec, List<AbstractType<?>> argTypes)
        {
            state.ensureHasPermission(Permission.ALTER, FunctionResource.function(keyspace, entityName, argTypes));
        }
    };

    public final String entityType;

    private CommentType(String entityType)
    {
        this.entityType = entityType;
    }

    public abstract void checkAccess(ClientState state, String keyspace, String entityName, String entitySpec, List<AbstractType<?>> argTypes);
}
