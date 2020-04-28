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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.virtual.VirtualSchemaKeyspace;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.utils.Hex;

public final class Resources
{
    private static final Set<IResource> READABLE_SYSTEM_RESOURCES = new HashSet<>();
    private static final Set<IResource> PROTECTED_AUTH_RESOURCES = new HashSet<>();
    private static final Set<IResource> DROPPABLE_SYSTEM_TABLES = new HashSet<>();

    static
    {
        // We want these system cfs to be always readable to authenticated users since many tools rely on them
        // (nodetool, cqlsh, bulkloader, etc.)
        for (String cf : Arrays.asList(SystemKeyspace.LOCAL, SystemKeyspace.LEGACY_PEERS, SystemKeyspace.PEERS_V2))
            READABLE_SYSTEM_RESOURCES.add(DataResource.table(SchemaConstants.SYSTEM_KEYSPACE_NAME, cf));

        // make all schema tables readable by default (required by the drivers)
        SchemaKeyspace.ALL.forEach(table -> READABLE_SYSTEM_RESOURCES.add(DataResource.table(SchemaConstants.SCHEMA_KEYSPACE_NAME, table)));

        // make all virtual schema tables readable by default as well
        VirtualSchemaKeyspace.instance.tables().forEach(t -> READABLE_SYSTEM_RESOURCES.add(t.metadata().resource));

        // neither clients nor tools need authentication/authorization
        if (DatabaseDescriptor.isDaemonInitialized())
        {
            PROTECTED_AUTH_RESOURCES.addAll(DatabaseDescriptor.getAuthenticator().protectedResources());
            PROTECTED_AUTH_RESOURCES.addAll(DatabaseDescriptor.getAuthorizer().protectedResources());
            PROTECTED_AUTH_RESOURCES.addAll(DatabaseDescriptor.getRoleManager().protectedResources());
        }

        SchemaConstants.OBSOLETE_AUTH_TABLES.forEach(t -> DROPPABLE_SYSTEM_TABLES.add(DataResource.table(SchemaConstants.AUTH_KEYSPACE_NAME, t)));
    }

    /**
     * Construct a chain of resource parents starting with the resource and ending with the root.
     *
     * @param resource The staring point.
     * @return list of resource in the chain form start to the root.
     */
    public static List<? extends IResource> chain(IResource resource)
    {
        List<IResource> chain = new ArrayList<IResource>();
        while (true)
        {
           chain.add(resource);
           if (!resource.hasParent())
               break;
           resource = resource.getParent();
        }
        return chain;
    }

    /**
     * Creates an IResource instance from its external name.
     * Resource implementation class is inferred by matching against the known IResource
     * impls' root level resources.
     * @param name
     * @return an IResource instance created from the name
     */
    public static IResource fromName(String name)
    {
        if (name.startsWith(RoleResource.root().getName()))
            return RoleResource.fromName(name);

        if (name.startsWith(DataResource.root().getName()))
            return DataResource.fromName(name);

        if (name.startsWith(FunctionResource.root().getName()))
            return FunctionResource.fromName(name);

        if (name.startsWith(JMXResource.root().getName()))
            return JMXResource.fromName(name);

        throw new IllegalArgumentException(String.format("Name %s is not valid for any resource type", name));
    }

    public static boolean isAlwaysReadable(IResource resource)
    {
        return READABLE_SYSTEM_RESOURCES.contains(resource);
    }

    public static boolean isProtected(IResource resource)
    {
        return PROTECTED_AUTH_RESOURCES.contains(resource);
    }

    public static boolean isDroppable(IResource resource)
    {
        return DROPPABLE_SYSTEM_TABLES.contains(resource);
    }

    @Deprecated
    public final static String ROOT = "cassandra";
    @Deprecated
    public final static String KEYSPACES = "keyspaces";

    @Deprecated
    public static String toString(List<Object> resource)
    {
        StringBuilder buff = new StringBuilder();
        for (Object component : resource)
        {
            buff.append("/");
            if (component instanceof byte[])
                buff.append(Hex.bytesToHex((byte[])component));
            else
                buff.append(component);
        }
        return buff.toString();
    }
}
