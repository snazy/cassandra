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

/**
 * IResource implementation representing the resource for <i>trusted functions</i>.
 *
 * The root level "functions" resource represents the collection of all Functions.
 * "trustedFunctions"                   - root level resource representing all trusted functions
 */
public final class TrustedFunctionResource implements IResource
{
    enum Level
    {
        ROOT
    }

    // permissions which may be granted on either a resource representing some collection of functions
    // i.e. the root resource (all functions) or a keyspace level resource (all functions in a given keyspace)
    private static final Set<Permission> COLLECTION_LEVEL_PERMISSIONS = Sets.immutableEnumSet(Permission.CREATE,
                                                                                              Permission.ALTER,
                                                                                              Permission.EXECUTE);

    private static final String ROOT_NAME = "functions";
    private static final TrustedFunctionResource ROOT_RESOURCE = new TrustedFunctionResource();

    private final Level level;

    private TrustedFunctionResource()
    {
        level = Level.ROOT;
    }

    /**
     * @return the root-level resource.
     */
    public static TrustedFunctionResource root()
    {
        return ROOT_RESOURCE;
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
        }
        throw new AssertionError();
    }

    /**
     * @return Parent of the resource, if any. Throws IllegalStateException if it's the root-level resource.
     */
    public IResource getParent()
    {
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
        }
        throw new AssertionError();
    }

    public Set<Permission> applicablePermissions()
    {
        switch (level)
        {
            case ROOT:
                return COLLECTION_LEVEL_PERMISSIONS;
        }
        throw new AssertionError();
    }

    @Override
    public String toString()
    {
        switch (level)
        {
            case ROOT:
                return "<trusted functions>";
        }
        throw new AssertionError();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof TrustedFunctionResource))
            return false;

        TrustedFunctionResource f = (TrustedFunctionResource) o;

        return Objects.equal(level, f.level);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(level);
    }

}
