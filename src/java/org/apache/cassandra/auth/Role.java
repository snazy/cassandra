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

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Represents all attributes of a role.
 */
public final class Role
{
    // NullObject
    public static final Role NULL_ROLE = new Role("", ImmutableSet.of(), false, false, ImmutableMap.of(), "");

    public final String name;
    public final ImmutableSet<RoleResource> memberOf;
    public final boolean isSuper;
    public final boolean canLogin;
    public final ImmutableMap<String, String> options;
    final String hashedPassword;

    public Role withOptions(ImmutableMap<String, String> options)
    {
        return new Role(name, memberOf, isSuper, canLogin, options, hashedPassword);
    }

    public Role withRoles(ImmutableSet<RoleResource> memberOf)
    {
        return new Role(name, memberOf, isSuper, canLogin, options, hashedPassword);
    }

    public Role(String name,
                ImmutableSet<RoleResource> memberOf,
                boolean isSuper,
                boolean canLogin,
                ImmutableMap<String, String> options,
                String hashedPassword)
    {
        this.name = name;
        this.memberOf = memberOf;
        this.isSuper = isSuper;
        this.canLogin = canLogin;
        this.options = ImmutableMap.copyOf(options);
        this.hashedPassword = hashedPassword;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("name", name)
                          .add("memberOf", memberOf)
                          .add("isSuper", isSuper)
                          .add("canLogin", canLogin)
                          .add("options", options)
                          .toString();
    }
}
