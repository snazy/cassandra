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

import java.util.*;
import java.util.stream.Collectors;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.utils.Pair;

public class StubAuthorizer implements IAuthorizer
{
    private final Map<Pair<String, IResource>, PermissionSets> userPermissions = new HashMap<>();

    public void clear()
    {
        userPermissions.clear();
    }

    public Map<IResource, PermissionSets> allPermissionSets(RoleResource role)
    {
        return userPermissions.entrySet()
                              .stream()
                              .filter(e -> e.getKey().left.equals(role.getRoleName()))
                              .collect(Collectors.toMap(e -> e.getKey().right, Map.Entry::getValue));
    }

    public Set<Permission> grant(AuthenticatedUser performer,
                                 Set<Permission> permissions,
                                 IResource resource,
                                 RoleResource grantee,
                                 GrantMode... grantModes)
    {
        Pair<String, IResource> key = Pair.create(grantee.getRoleName(), resource);
        PermissionSets oldPermissions = userPermissions.get(key);

        if (oldPermissions == null)
            oldPermissions = PermissionSets.EMPTY;

        PermissionSets.Builder builder = oldPermissions.unbuild();
        Set<Permission> granted = new HashSet<>();
        for (GrantMode grantMode : grantModes)
        {
            Set<Permission> nonExisting = new HashSet<>(permissions);
            switch (grantMode)
            {
                case GRANT:
                    nonExisting.removeAll(oldPermissions.granted);
                    builder.addGranted(nonExisting);
                    break;
                case RESTRICT:
                    nonExisting.removeAll(oldPermissions.restricted);
                    builder.addRestricted(nonExisting);
                    break;
                case GRANTABLE:
                    nonExisting.removeAll(oldPermissions.grantables);
                    builder.addGrantables(nonExisting);
                    break;
            }
            granted.addAll(nonExisting);
        }

        PermissionSets newPermissions = builder.build();
        if (!newPermissions.isEmpty())
            userPermissions.put(key, newPermissions);
        return granted;
    }

    public Set<Permission> revoke(AuthenticatedUser performer,
                                  Set<Permission> permissions,
                                  IResource resource,
                                  RoleResource revokee,
                                  GrantMode... grantModes)
    {
        Pair<String, IResource> key = Pair.create(revokee.getRoleName(), resource);
        PermissionSets oldPermissions = userPermissions.get(key);
        if (oldPermissions == null)
            return Collections.emptySet();

        PermissionSets.Builder builder = oldPermissions.unbuild();
        Set<Permission> revoked = new HashSet<>();
        for (GrantMode grantMode : grantModes)
        {
            Set<Permission> existing = new HashSet<>(permissions);
            switch (grantMode)
            {
                case GRANT:
                    existing.retainAll(oldPermissions.granted);
                    builder.removeGranted(existing);
                    break;
                case RESTRICT:
                    existing.retainAll(oldPermissions.restricted);
                    builder.removeRestricted(existing);
                    break;
                case GRANTABLE:
                    existing.retainAll(oldPermissions.grantables);
                    builder.removeGrantables(existing);
                    break;
            }
            revoked.addAll(existing);
        }

        PermissionSets newPermissions = builder.build();
        if (newPermissions.isEmpty())
        {
            userPermissions.remove(key);
        }
        else
        {
            userPermissions.put(key, newPermissions);
        }
        return revoked;
    }

    public Set<PermissionDetails> list(Set<Permission> permissions,
                                       IResource resource,
                                       RoleResource grantee) throws RequestValidationException, RequestExecutionException
    {
        return userPermissions.entrySet()
                              .stream()
                              .filter(entry -> entry.getKey().left.equals(grantee.getRoleName())
                                               && (resource == null || entry.getKey().right.equals(resource))
                                               && containsAny(entry.getValue(), permissions))
                              .flatMap(entry -> permissions.stream()
                                                           .map(p -> new PermissionDetails(entry.getKey().left,
                                                                                           entry.getKey().right,
                                                                                           p,
                                                                                           entry.getValue().grantModesFor(p))))
                              .collect(Collectors.toSet());
    }

    private static boolean containsAny(PermissionSets value, Set<Permission> permissions)
    {
        return permissions.stream()
                          .anyMatch(p -> value.granted.contains(p) || value.restricted.contains(p) || value.grantables.contains(p));
    }

    public void revokeAllFrom(RoleResource revokee)
    {
        userPermissions.keySet()
                       .removeAll(userPermissions.keySet()
                                                 .stream()
                                                 .filter(key -> key.left.equals(revokee.getRoleName())).collect(Collectors.toList()));
    }

    public void revokeAllOn(IResource droppedResource)
    {
        userPermissions.keySet()
                       .removeAll(userPermissions.keySet()
                                                 .stream()
                                                 .filter(key -> key.right.equals(droppedResource)).collect(Collectors.toList()));
    }

    public Set<? extends IResource> protectedResources()
    {
        return Collections.emptySet();
    }

    public void validateConfiguration() throws ConfigurationException
    {
    }

    public void setup()
    {
    }
}
