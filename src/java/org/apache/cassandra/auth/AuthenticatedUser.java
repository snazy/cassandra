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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Returned from IAuthenticator#authenticate(), represents an authenticated user everywhere internally.
 *
 * Holds the name of the user and the roles that have been granted to the user. The roles will be cached
 * for roles_validity_in_ms.
 */
public class AuthenticatedUser
{
    public static final String SYSTEM_USERNAME = "system";
    public static final AuthenticatedUser SYSTEM_USER = new AuthenticatedUser(SYSTEM_USERNAME);

    public static final String ANONYMOUS_USERNAME = "anonymous";
    public static final AuthenticatedUser ANONYMOUS_USER = new AuthenticatedUser(ANONYMOUS_USERNAME);

    /**
     * We need a global map of artificial, internal users, since DSE creates its own internal users, too.
     * For example in {@code DseAuthenticator.InProcAuthenticatedUser} ({@code InProcessSaslNegotiator})
     */
    private static Map<RoleResource, Role> internalUserRoles = Collections.emptyMap();
    static
    {
        registerInternalUserRole(SYSTEM_USER, new Role(SYSTEM_USERNAME,
                                                       ImmutableSet.of(),
                                                       true,
                                                       false,
                                                       ImmutableMap.of(),
                                                       ""));
        registerInternalUserRole(ANONYMOUS_USER, new Role(ANONYMOUS_USERNAME,
                                                          ImmutableSet.of(),
                                                          false,
                                                          false,
                                                          ImmutableMap.of(),
                                                          ""));
    }

    protected static synchronized void registerInternalUserRole(AuthenticatedUser internalUser, Role internalRole)
    {
        IdentityHashMap<RoleResource, Role> newMap = new IdentityHashMap<>(internalUserRoles);
        if (newMap.putIfAbsent(internalUser.getPrimaryRole(), internalRole) != null)
            throw new IllegalArgumentException("Duplicate internal user assignment for " + internalUser);
        internalUserRoles = newMap;
    }

    public static Role maybeGetInternalUserRole(RoleResource role)
    {
        return internalUserRoles.get(role);
    }

    private final String name;
    // primary Role of the logged in user
    private final RoleResource role;

    public AuthenticatedUser(String name)
    {
        this.name = name;
        this.role = RoleResource.role(name);
    }

    public String getName()
    {
        return name;
    }

    /**
     * The primay role of this user. Proxy users return the role of the authorized user.
     */
    public RoleResource getPrimaryRole()
    {
        return role;
    }

    /**
     * Normal users just return the primary role. Proxy users return the authenticated user's primary role.
     */
    public RoleResource getLoginRole()
    {
        return role;
    }

    /**
     * If IAuthenticator doesn't require authentication, this method may return true.
     */
    public boolean isAnonymous()
    {
        return this == ANONYMOUS_USER;
    }

    /**
     * Some internal operations are performed on behalf of Cassandra itself, in those cases
     * the system user should be used where an identity is required
     * see CreateRoleStatement#execute() and overrides of AlterSchemaStatement#createdResources()
     */
    public boolean isSystem()
    {
        return this == SYSTEM_USER;
    }

    @Override
    public String toString()
    {
        return String.format("#<User %s>", name);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof AuthenticatedUser))
            return false;

        AuthenticatedUser u = (AuthenticatedUser) o;

        return Objects.equal(name, u.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name);
    }

}
