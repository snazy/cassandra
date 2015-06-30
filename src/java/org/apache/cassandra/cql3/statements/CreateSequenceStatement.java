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

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.SequenceResource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.sequences.SequenceDef;
import org.apache.cassandra.cql3.sequences.SequenceName;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.sequences.Sequence;
import org.apache.cassandra.db.sequences.SequenceManager;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * TODO add javadoc explaining the purpose of this class/interface/annotation
 */
public class CreateSequenceStatement extends SchemaAlteringStatement
{
    private final SequenceDef definition;
    private final boolean ifNotExists;

    public CreateSequenceStatement(SequenceName sequenceName, Constants.Literal incrBy, Constants.Literal minVal,
                                   Constants.Literal maxVal, Constants.Literal startWith,
                                   Constants.Literal cache, Constants.Literal cacheLocal,
                                   ConsistencyLevel clSerial, ConsistencyLevel cl,
                                   boolean ifNotExists)
    {
        this.ifNotExists = ifNotExists;
        this.definition = new SequenceDef(sequenceName, incrBy, minVal, maxVal, startWith, cache, cacheLocal, clSerial, cl);
    }

    public void prepareKeyspace(ClientState state) throws InvalidRequestException
    {
        if (!definition.getName().hasKeyspace() && state.getRawKeyspace() != null)
            definition.setName(new SequenceName(state.getRawKeyspace(), definition.getName().name));

        if (!definition.getName().hasKeyspace())
            throw new InvalidRequestException("Functions must be fully qualified with a keyspace name if a keyspace is not set for the session");

        ThriftValidation.validateKeyspaceNotSystem(definition.getName().keyspace);
    }

    protected void grantPermissionsToCreator(QueryState state)
    {
        try
        {
            IResource resource = SequenceResource.sequence(definition.getName().keyspace, definition.getName().name);
            DatabaseDescriptor.getAuthorizer().grant(AuthenticatedUser.SYSTEM_USER,
                                                     resource.applicablePermissions(),
                                                     resource,
                                                     RoleResource.role(state.getClientState().getUser().getName()));
        }
        catch (RequestExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.ensureHasPermission(Permission.CREATE, SequenceResource.keyspace(definition.getName().keyspace));
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        if (Schema.instance.getKSMetaData(definition.getName().keyspace) == null)
            throw new InvalidRequestException(String.format("Cannot add sequence '%s' to non existing keyspace '%s'.", definition.getName().name, definition.getName().keyspace));
    }

    public Event.SchemaChange changeEvent()
    {
        return new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.SEQUENCE,
                                      definition.getName().keyspace, definition.getName().name);
    }

    public boolean announceMigration(boolean isLocalOnly) throws InvalidRequestException, ConfigurationException
    {
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(definition.getName().keyspace);
        assert ksm != null; // should haven't validate otherwise

        // Can happen with ifNotExists
        if (ksm.types.get(ByteBufferUtil.bytes(definition.getName().name)).isPresent())
        {
            if (!ifNotExists)
                throw new InvalidRequestException(String.format("Sequence %s already exists", definition.getName()));

            return false;
        }

        Sequence seq = new Sequence(definition);

        // perform the global part only once on the coordinator executing the statement
        SequenceManager.getInstance().createSequenceGlobal(seq);

        MigrationManager.announceNewSequence(seq, isLocalOnly);
        return true;
    }

}
