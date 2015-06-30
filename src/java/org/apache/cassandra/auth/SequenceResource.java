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

import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * IResource implementation representing sequences.
 * <p/>
 * The root level "sequences" resource represents the collection of all sequences.
 * "sequences"                          - root level resource representing all sequences defined across every keyspace
 * "sequences/keyspace"                 - keyspace level resource to apply permissions to all sequences within a keyspace
 * "sequences/keyspace/sequence"        - a specific sequence, scoped to a given keyspace
 */
public class SequenceResource implements IResource
{
    enum Level
    {
        ROOT, KEYSPACE, SEQUENCE
    }

    // permissions which may be granted on either a resource representing some collection of sequences
    // i.e. the root resource (all sequences) or a keyspace level resource (all sequences in a given keyspace)
    private static final Set<Permission> COLLECTION_LEVEL_PERMISSIONS = Sets.immutableEnumSet(Permission.CREATE,
                                                                                              Permission.ALTER,
                                                                                              Permission.DROP,
                                                                                              Permission.AUTHORIZE,
                                                                                              Permission.EXECUTE);

    private static final Set<Permission> SEQUENCE_PERMISSIONS = Sets.immutableEnumSet(Permission.ALTER,
                                                                                      Permission.DROP,
                                                                                      Permission.AUTHORIZE,
                                                                                      Permission.EXECUTE);

    private static final String ROOT_NAME = "sequences";
    private static final SequenceResource ROOT_RESOURCE = new SequenceResource();

    private final Level level;
    private final String keyspace;
    private final String name;

    private SequenceResource()
    {
        level = Level.ROOT;
        keyspace = null;
        name = null;
    }

    private SequenceResource(String keyspace)
    {
        level = Level.KEYSPACE;
        this.keyspace = keyspace;
        name = null;
    }

    private SequenceResource(String keyspace, String name)
    {
        level = Level.SEQUENCE;
        this.keyspace = keyspace;
        this.name = name;
    }

    /**
     * @return the root-level resource.
     */
    public static SequenceResource root()
    {
        return ROOT_RESOURCE;
    }

    /**
     * Creates a SequenceResource representing the collection of sequences scoped
     * to a specific keyspace.
     *
     * @param keyspace name of the keyspace
     * @return SequenceResource instance representing all of the keyspace's sequences
     */
    public static SequenceResource keyspace(String keyspace)
    {
        return new SequenceResource(keyspace);
    }

    /**
     * Creates a SequenceResource representing a specific, keyspace-scoped sequence.
     *
     * @param keyspace the keyspace in which the function is scoped
     * @param name     name of the sequence.
     * @return SequenceResource instance reresenting the function.
     */
    public static SequenceResource sequence(String keyspace, String name)
    {
        return new SequenceResource(keyspace, name);
    }

    /**
     * Creates a SequenceResource representing a specific, keyspace-scoped sequence.
     * This variant is used to create an instance during parsing of a CQL statement.
     * It includes transposition of the arg types from CQL types to AbstractType
     * implementations
     *
     * @param keyspace the keyspace in which the sequence is scoped
     * @param name     name of the sequence.
     * @return SequenceResource instance reresenting the sequence.
     */
    public static SequenceResource sequenceFromCql(String keyspace, String name)
    {
        return new SequenceResource(keyspace, name);
    }

    /**
     * Parses a resource name into a SequenceResource instance.
     *
     * @param name Name of the sequence resource.
     * @return SequenceResource instance matching the name.
     */
    public static SequenceResource fromName(String name)
    {
        String[] parts = StringUtils.split(name, '/');

        if (!parts[0].equals(ROOT_NAME) || parts.length > 3)
            throw new IllegalArgumentException(String.format("%s is not a valid sequence resource name", name));

        if (parts.length == 1)
            return root();

        if (parts.length == 2)
            return keyspace(parts[1]);

        String[] nameAndArgs = StringUtils.split(parts[2], "[|]");
        return sequence(parts[1], nameAndArgs[0]);
    }

    /**
     * @return Printable name of the resource.
     */
    public String getName()
    {
        switch (level)
        {
            case ROOT:
                return ROOT_NAME;
            case KEYSPACE:
                return String.format("%s/%s", ROOT_NAME, keyspace);
            case SEQUENCE:
                return String.format("%s/%s/%s", ROOT_NAME, keyspace, name);
        }
        throw new AssertionError();
    }

    /**
     * Get the name of the keyspace this resource relates to. In the case of the
     * global root resource, return null
     *
     * @return the keyspace name of this resource, or null for the root resource
     */
    public String getKeyspace()
    {
        return keyspace;
    }

    /**
     * @return Parent of the resource, if any. Throws IllegalStateException if it's the root-level resource.
     */
    public IResource getParent()
    {
        switch (level)
        {
            case KEYSPACE:
                return root();
            case SEQUENCE:
                return keyspace(keyspace);
        }
        throw new IllegalStateException("Root-level resource can't have a parent");
    }

    public boolean hasParent()
    {
        return level != Level.ROOT;
    }

    public boolean exists()
    {
        switch (level)
        {
            case ROOT:
                return true;
            case KEYSPACE:
                return Schema.instance.getKeyspaces().contains(keyspace);
            case SEQUENCE:
                return Schema.instance.getKeyspaces().contains(keyspace)
                       && Schema.instance.getKeyspaceInstance(keyspace).getMetadata().sequences.get(ByteBufferUtil.bytes(name)).isPresent();
        }
        throw new AssertionError();
    }

    public Set<Permission> applicablePermissions()
    {
        switch (level)
        {
            case ROOT:
            case KEYSPACE:
                return COLLECTION_LEVEL_PERMISSIONS;
            case SEQUENCE:
            {
                assert exists() : "Unable to find sequence object for resource " + toString();
                return SEQUENCE_PERMISSIONS;
            }
        }
        throw new AssertionError();
    }

    public int compareTo(SequenceResource o)
    {
        return this.name.compareTo(o.name);
    }

    @Override
    public String toString()
    {
        switch (level)
        {
            case ROOT:
                return "<all functions>";
            case KEYSPACE:
                return String.format("<all sequences in %s>", keyspace);
            case SEQUENCE:
                return String.format("<sequence %s.%s>",
                                     keyspace,
                                     name);
        }
        throw new AssertionError();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof SequenceResource))
            return false;

        SequenceResource f = (SequenceResource) o;

        return Objects.equal(level, f.level)
               && Objects.equal(keyspace, f.keyspace)
               && Objects.equal(name, f.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(level, keyspace, name);
    }
}
