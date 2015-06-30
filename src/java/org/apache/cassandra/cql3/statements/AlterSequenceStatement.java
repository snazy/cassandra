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

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.SequenceResource;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.sequences.SequenceDef;
import org.apache.cassandra.cql3.sequences.SequenceName;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.sequences.Sequence;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * TODO add javadoc explaining the purpose of this class/interface/annotation
 */
public class AlterSequenceStatement extends SchemaAlteringStatement
{
    private final SequenceDef definition;
    private final boolean ifExists;

    public AlterSequenceStatement(SequenceName sequenceName, Constants.Literal incrBy, Constants.Literal minVal,
                                  Constants.Literal maxVal, Constants.Literal cache, Constants.Literal cacheLocal,
                                  ConsistencyLevel clSerial, ConsistencyLevel cl, boolean ifExists)
    {
        this.ifExists = ifExists;
        this.definition = new SequenceDef(sequenceName, incrBy, minVal, maxVal, null, cache, cacheLocal, clSerial, cl);
    }

    public void prepareKeyspace(ClientState state) throws InvalidRequestException
    {
        if (!definition.getName().hasKeyspace() && state.getRawKeyspace() != null)
            definition.setName(new SequenceName(state.getRawKeyspace(), definition.getName().name));

        if (!definition.getName().hasKeyspace())
            throw new InvalidRequestException("Functions must be fully qualified with a keyspace name if a keyspace is not set for the session");

        ThriftValidation.validateKeyspaceNotSystem(definition.getName().keyspace);
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(definition.getName().keyspace);
        assert ksm != null; // should haven't validate otherwise
        if (ksm.sequences.get(ByteBufferUtil.bytes(definition.getName().name)).isPresent())
            state.ensureHasPermission(Permission.ALTER, SequenceResource.sequence(definition.getName().keyspace,
                                                                                  definition.getName().name));
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        if (Schema.instance.getKSMetaData(definition.getName().keyspace) == null)
            throw new InvalidRequestException(String.format("Cannot add sequence '%s' to non existing keyspace '%s'.", definition.getName().name, definition.getName().keyspace));
    }

    public Event.SchemaChange changeEvent()
    {
        return new Event.SchemaChange(Event.SchemaChange.Change.UPDATED,
                                      Event.SchemaChange.Target.SEQUENCE,
                                      definition.getName().keyspace, definition.getName().name);
    }

    public boolean announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(definition.getName().keyspace);
        assert ksm != null; // should haven't validate otherwise

        Sequence old = ksm.sequences.getNullable(ByteBufferUtil.bytes(definition.getName().name));
        if (old == null)
        {
            if (ifExists)
                return false;
            else
                throw new InvalidRequestException(String.format("Cannot alter non-existing sequence '%s'.", definition.getName()));
        }

        definition.setStartWith(old.startWith);

        old.testUpdate(definition);

        MigrationManager.announceSequenceUpdate(new Sequence(definition), isLocalOnly);

        return true;
    }

}
