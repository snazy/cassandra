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

import java.util.Optional;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.SequenceResource;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.sequences.SequenceName;
import org.apache.cassandra.db.sequences.Sequence;
import org.apache.cassandra.db.sequences.SequenceManager;
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
public class DropSequenceStatement extends SchemaAlteringStatement
{
    private SequenceName sequenceName;
    private final boolean ifExists;

    public DropSequenceStatement(SequenceName sequenceName, boolean ifExists)
    {
        this.sequenceName = sequenceName;
        this.ifExists = ifExists;
    }

    public void prepareKeyspace(ClientState state) throws InvalidRequestException
    {
        if (!sequenceName.hasKeyspace() && state.getRawKeyspace() != null)
            sequenceName = new SequenceName(state.getRawKeyspace(), sequenceName.name);

        if (!sequenceName.hasKeyspace())
            throw new InvalidRequestException("Sequences must be fully qualified with a keyspace name if a keyspace is not set for the session");

        ThriftValidation.validateKeyspaceNotSystem(sequenceName.keyspace);
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(sequenceName.keyspace);
        assert ksm != null; // should haven't validate otherwise
        if (ksm.sequences.get(ByteBufferUtil.bytes(sequenceName.name)).isPresent())
            state.ensureHasPermission(Permission.ALTER, SequenceResource.sequence(sequenceName.keyspace,
                                                                                  sequenceName.name));
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        if (Schema.instance.getKSMetaData(sequenceName.keyspace) == null)
            throw new InvalidRequestException(String.format("Cannot add sequence '%s' to non existing keyspace '%s'.", sequenceName.name, sequenceName.keyspace));
    }

    public Event.SchemaChange changeEvent()
    {
        return new Event.SchemaChange(Event.SchemaChange.Change.DROPPED,
                                      Event.SchemaChange.Target.FUNCTION,
                                      sequenceName.keyspace, sequenceName.name);
    }

    public boolean announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(sequenceName.keyspace);
        assert ksm != null; // should haven't validate otherwise

        Optional<Sequence> seq = ksm.sequences.get(ByteBufferUtil.bytes(sequenceName.name));
        if (!seq.isPresent())
        {
            if (ifExists)
                return false;
            else
                throw new InvalidRequestException(String.format("Cannot alter non-existing sequence '%s'.", sequenceName));
        }

        MigrationManager.announceSequenceDrop(sequenceName, isLocalOnly);

        // perform the global part only once on the coordinator executing the statement
        SequenceManager.getInstance().removeSequenceGlobal(seq.get());

        return true;
    }

}
