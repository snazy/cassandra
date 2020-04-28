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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;

public class RolesCache extends AuthCache<RoleResource, Role>
{
    public RolesCache(IRoleManager roleManager)
    {
        this(roleManager::getRoleData,
             roleManager::getRolesData,
             () -> DatabaseDescriptor.getAuthenticator().requireAuthentication());
    }

    @VisibleForTesting
    public RolesCache(Function<RoleResource, Role> loadFunction,
                      Function<Iterable<? extends RoleResource>, Map<RoleResource, Role>> loadAllFunction,
                      BooleanSupplier enabledFunction)
    {
        super("RolesCache",
              DatabaseDescriptor::setRolesValidity,
              DatabaseDescriptor::getRolesValidity,
              DatabaseDescriptor::setRolesUpdateInterval,
              DatabaseDescriptor::getRolesUpdateInterval,
              DatabaseDescriptor::setRolesCacheMaxEntries,
              DatabaseDescriptor::getRolesCacheMaxEntries,
              DatabaseDescriptor::setRolesCacheInitialCapacity,
              DatabaseDescriptor::getRolesCacheInitialCapacity,
              loadFunction,
              loadAllFunction,
              enabledFunction);
    }

    public Map<RoleResource, Role> getRoles(RoleResource primaryRole)
    {
        Role role = get(primaryRole);

        if (role == null)
        {
            return Collections.emptyMap();
        }

        if (role.memberOf.isEmpty())
        {
            return Collections.singletonMap(primaryRole, role);
        }

        Map<RoleResource, Role> map = new HashMap<>();
        map.put(primaryRole, role);
        Set<RoleResource> remaining = new HashSet<>(role.memberOf);

        return collectRoles(Collections.synchronizedSet(remaining),
                            Collections.synchronizedMap(map));
    }

    private Map<RoleResource, Role> collectRoles(Set<RoleResource> needToVisit,
                                                 Map<RoleResource, Role> workMap)
    {
        while (!needToVisit.isEmpty())
        {
            // handle the ones present in the cache first
            Map<RoleResource, Role> presentRoles = getAllPresent(needToVisit);
            handleCollectRoles(needToVisit, workMap, presentRoles);

            Map<RoleResource, Role> loadedRoles = getAll(needToVisit);
            // handle the roles that actually need to be loaded
            handleCollectRoles(needToVisit, workMap, loadedRoles);
            collectRoles(needToVisit, workMap);
        }

        return Collections.unmodifiableMap(workMap);
    }

    private void handleCollectRoles(Set<RoleResource> needToVisit, Map<RoleResource, Role> workMap, Map<RoleResource, Role> result)
    {
        for (Map.Entry<RoleResource, Role> entry : result.entrySet())
        {
            Role role = entry.getValue();
            if (workMap.put(entry.getKey(), role) == null)
            {
                for (RoleResource memberOf : role.memberOf)
                {
                    if (!workMap.containsKey(memberOf))
                    {
                        needToVisit.add(memberOf);
                    }
                }
            }
            needToVisit.remove(entry.getKey());
        }
    }
}
