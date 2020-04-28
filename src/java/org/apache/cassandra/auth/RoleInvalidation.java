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
package org.apache.cassandra.auth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;

/**
 * Internode messaging payload to invalidate cached role data.
 */
public class RoleInvalidation
{
    public static final RoleInvalidation.Serializer serializer = new RoleInvalidation.Serializer();

    public static final class Serializer implements IVersionedSerializer<RoleInvalidation>
    {
        private Serializer()
        {
        }

        public void serialize(RoleInvalidation schema, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(schema.roles.size());
            for (RoleResource role : schema.roles)
                out.writeUTF(role.getName());
        }

        public RoleInvalidation deserialize(DataInputPlus in, int version) throws IOException
        {
            int count = in.readInt();
            Collection<RoleResource> roles = new ArrayList<>(count);

            for (int i = 0; i < count; i++)
                roles.add(RoleResource.fromName(in.readUTF()));

            return new RoleInvalidation(roles);
        }

        public long serializedSize(RoleInvalidation schema, int version)
        {
            long size = TypeSizes.sizeof(schema.roles.size());
            for (RoleResource role : schema.roles)
                size += TypeSizes.sizeof(role.getName());
            return size;
        }
    }

    public final Collection<RoleResource> roles;

    RoleInvalidation(Collection<RoleResource> roles)
    {
        this.roles = roles;
    }

    public static class RoleInvalidationVerbHandler implements IVerbHandler<RoleInvalidation>
    {
        public static final RoleInvalidationVerbHandler instance = new RoleInvalidationVerbHandler();

        @Override
        public void doVerb(Message<RoleInvalidation> message)
        {
            DatabaseDescriptor.getAuthManager().handleRoleInvalidation(message.payload);
        }
    }
}
