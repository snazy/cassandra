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

package org.apache.cassandra.cql3.statements;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CommentType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event;

public class CommentOnStatement extends SchemaAlteringStatement
{
    private final CommentType type;
    private final String keyspace;
    private final String entityName;
    private final String entitySpec;
    private final List<CQL3Type.Raw> argRawTypes;
    private final String comment;

    private List<AbstractType<?>> argTypes;

    public CommentOnStatement(CommentType type, String keyspace, String entityName, String entitySpec, List<CQL3Type.Raw> argRawTypes, String comment)
    {
        this.type = type;
        this.keyspace = keyspace;
        this.entityName = entityName;
        this.entitySpec = entitySpec;
        this.argRawTypes = argRawTypes;
        this.comment = comment;
    }

    public Prepared prepare()
    {
        if (argRawTypes != null)
        {
            argTypes = new ArrayList<>(argRawTypes.size());
            for (CQL3Type.Raw rawType : argRawTypes)
                argTypes.add(RawType.prepareType(keyspace, "arguments", rawType));
        }

        return super.prepare();
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        type.checkAccess(state, keyspace, entityName, entitySpec, argTypes);
    }

    public void validate(ClientState state) throws RequestValidationException
    {

    }

    public Event.SchemaChange announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        return null;
    }
}
