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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.Collections.unmodifiableSet;

/**
 * The {@code AuthManager} provides a caching layer on top of the {@code IRoleManager} and {@code IAuthorizer}.
 * <p>If the requested data is not in the cache an the calling Thread is a TPC thread, the {@code AuthManager} will
 * defer the retrieval of data and schedule it on an IO Thread. If the calling thread is not a TPC thread or the
 * data is within the cache the call will be executed synchonously and will be blocking.</p>
 * <p>As the returned {@code Single} might have been scheduled on an IO thread, make sure the operations performed on
 * it are scheduled to be run on the Scheduler they should use.</p>
 */
public final class AuthManager
{
    private static final Logger logger = LoggerFactory.getLogger(AuthManager.class);

    /**
     * The role manager.
     */
    private final IRoleManager roleManager;

    /**
     * The role cache.
     */
    final RolesCache rolesCache;

    /**
     * The network auth cache.
     */
    private final NetworkAuthCache networkAuthCache;

    /**
     * The authorizer.
     */
    private final IAuthorizer authorizer;

    private final INetworkAuthorizer networkAuthorizer;

    /**
     * The permission cache.
     */
    final PermissionsCache permissionsCache;

    public AuthManager(IRoleManager roleManager, IAuthorizer authorizer, INetworkAuthorizer networkAuthorizer)
    {
        this.rolesCache = new RolesCache(roleManager);
        this.permissionsCache = new PermissionsCache(authorizer);
        this.networkAuthCache = new NetworkAuthCache(networkAuthorizer);
        this.roleManager = new RoleManagerCacheInvalidator(roleManager, authorizer);
        this.authorizer = new AuthorizerInvalidator(authorizer);

        // Don't trigger role invalidation here (would be duplicate work).
        // Even not just duplicate work, but since role invalidation has already been performed
        // by org.apache.cassandra.auth.AuthManager.RoleManagerCacheInvalidator.dropRole (above),
        // there are no more roles to invalidate from any cache, and also since no roles exist,
        // the logic in org.apache.cassandra.auth.AuthManager.invalidateRoles would then invalidate
        // the whole caches.
        this.networkAuthorizer = networkAuthorizer;
    }

    /**
     * Returns the roles and permissions associated to the specified user.
     * <p>If the role and permission data were not cached and the calling thread is a TPC thread,
     * the returned {@code Single} might be a deferred one which is scheduled to run on an IO thread.</p>
     *
     * @param user the user for which the roles and permissions must be retrieved.
     *
     * @return the role and permissions associated to the specified user.
     */
    public UserRolesAndPermissions getUserRolesAndPermissions(AuthenticatedUser user)
    {
        if (user == null)
        {
            NullPointerException ex = new NullPointerException("Null 'user' passed to AuthManager.getUserRolesAndPermissions");
            logger.error("Null 'user' passed to AuthManager.getUserRolesAndPermissions. This is clearly a bug.", ex);
            throw ex;
        }

        if (user.isSystem())
            return UserRolesAndPermissions.SYSTEM;

        if (user.isAnonymous())
            return UserRolesAndPermissions.ANONYMOUS;

        return getUserRolesAndPermissions(user.getName(), user.getPrimaryRole());
    }

    /**
     * Returns the roles and permissions associated to the specified user.
     * <p>If the role and permission data were not cached and the calling thread is a TPC thread,
     * the returned {@code Single} might be a deferred one which is scheduled to run on an IO thread.</p>
     *
     * @param name the user name.
     *
     * @return the role and permissions associated to the specified user.
     */
    public UserRolesAndPermissions getUserRolesAndPermissions(String name)
    {
        return getUserRolesAndPermissions(name, RoleResource.role(name));
    }

    /**
     * Returns the roles and permissions associated to the specified user.
     * <p>If the role and permission data were not cached and the calling thread is a TPC thread,
     * the returned {@code Single} might be a deferred one which is scheduled to run on an IO thread.</p>
     *
     * @param name the user name.
     * @param primaryRole thuser primary role
     *
     * @return the role and permissions associated to the specified user.
     */
    public UserRolesAndPermissions getUserRolesAndPermissions(String name, RoleResource primaryRole)
    {
        return loadRolesAndPermissionsIfNeeded(name, primaryRole);
    }

    /**
     * Retrieves the roles and permissions from the cache allowing it to fetch them from the disk if needed.
     *
     * @param name the user name
     * @param primaryRole the user primary role
     *
     * @return roles and permissions for the specified user
     */
    private UserRolesAndPermissions loadRolesAndPermissionsIfNeeded(String name, RoleResource primaryRole)
    {
        Map<RoleResource, Role> rolePerResource = rolesCache.getRoles(primaryRole);

        DCPermissions dcPermissions = networkAuthCache.get(primaryRole);

        Set<RoleResource> roleResources = rolePerResource.keySet();

        if (checkIsSuperuser(rolePerResource))
            return UserRolesAndPermissions.createSuperUserRolesAndPermissions(name,
                                                                              roleResources);

        if (!authorizer.requireAuthorization())
            return UserRolesAndPermissions.newNormalUserRoles(name,
                                                              roleResources,
                                                              dcPermissions);

        return UserRolesAndPermissions.newNormalUserRolesAndPermissions(name,
                                                                        roleResources,
                                                                        dcPermissions,
                                                                        permissionsCache.getAll(roleResources));
    }

    /**
     * Returns the credentials for the supplied role.
     * <p>If the credentials data are not cached and the calling thread is a TPC thread,
     * the returned {@code Single} might be a deferred one which is scheduled to run on an IO thread.</p>
     *
     * @param username the user name
     */
    String getCredentials(String username)
    {
        RoleResource resource = RoleResource.role(username);

        return rolesCache.get(resource).hashedPassword;
    }

    /**
     * Get all roles granted to the supplied Role, including both directly granted
     * and inherited roles.
     * <p>If the role data are not cached and the calling thread is a TPC thread,
     * the returned {@code Single} might be a deferred one which is scheduled to run on an IO thread.</p>
     *
     * @param primaryRole the Role
     *
     * @return set of all granted Roles for the primary Role
     */
    public Set<RoleResource> getRoles(RoleResource primaryRole)
    {
        return unmodifiableSet(rolesCache.getRoles(primaryRole).keySet());
    }

    /**
     * Returns {@code true} if the supplied role is allowed to login.
     *
     * @param role the primary role
     *
     * @return {@code true} if the role is allowed to login, {@code false} otherwise
     */
    public boolean canLogin(RoleResource role)
    {
        return rolesCache.get(role).canLogin;
    }

    public boolean isSuperuser(RoleResource role)
    {
        Map<RoleResource, Role> rolePerResource = rolesCache.getRoles(role);
        return checkIsSuperuser(rolePerResource);
    }

    private boolean checkIsSuperuser(Map<RoleResource, Role> rolePerResource)
    {
        for (Role r : rolePerResource.values())
        {
            if (r.isSuper)
                return true;
        }
        return false;
    }

    /**
     * Returns a decorated {@code RoleManager} that will makes sure that cache data are properly invalidated when
     * roles are modified.
     */
    public IRoleManager getRoleManager()
    {
        return roleManager;
    }

    /**
     * Returns a decorated {@code IAuthorizer} that will makes sure that cache data are properly invalidated when
     * permissions are modified.
     */
    public IAuthorizer getAuthorizer()
    {
        return authorizer;
    }

    /**
     * Returns a decorated {@code IAuthorizer} that will makes sure that cache data are properly invalidated when
     * permissions are modified.
     */
    public INetworkAuthorizer getNetworkAuthorizer()
    {
        return networkAuthorizer;
    }

    public void handleRoleInvalidation(RoleInvalidation invalidation)
    {
        invalidate(invalidation.roles);
    }

    public void invalidateRoles(Collection<RoleResource> roles)
    {
        invalidate(roles);
        pushRoleInvalidation(roles);
    }

    private void pushRoleInvalidation(Collection<RoleResource> roles)
    {
        RoleInvalidation invalidation = new RoleInvalidation(roles);
        for (InetAddressAndPort endpoint : Gossiper.instance.getLiveMembers())
        {// only push schema to nodes with known and equal versions
            if (!endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
            {
                MessagingService.instance().send(Message.out(Verb.INVALIDATE_ROLE, invalidation), endpoint);
            }
        }
    }

    @VisibleForTesting
    public void invalidateCaches()
    {
        logger.debug("Invalidating all caches");
        permissionsCache.invalidate();
        rolesCache.invalidate();
        networkAuthCache.invalidate();
    }

    private void invalidate(Collection<RoleResource> roles)
    {
        if (roles.isEmpty())
        {
            invalidateCaches();
        }
        else
        {
            logger.debug("Invalidating roles {}", roles);
            for (RoleResource role : roles)
            {
                permissionsCache.invalidate(role);
                rolesCache.invalidate(role);
                networkAuthCache.invalidate(role);
            }
        }
    }

    public void setup(IAuthenticator authenticator)
    {
        boolean requireAuth = authenticator.requireAuthentication() ||
                              networkAuthorizer.requireAuthorization() ||
                              authorizer.requireAuthorization();

        if (requireAuth)
        {
            try
            {
                DatabaseDescriptor.getRoleManager().setup().get();
            }
            catch (InterruptedException | ExecutionException e)
            {
                throw new RuntimeException(e);
            }
            DatabaseDescriptor.getAuthenticator().setup();
            DatabaseDescriptor.getAuthorizer().setup();
            DatabaseDescriptor.getNetworkAuthorizer().setup();
        }
    }

    /**
     * Decorator used to invalidate cache data on role modifications
     */
    private class RoleManagerCacheInvalidator implements IRoleManager
    {
        private final IRoleManager roleManager;

        private final IAuthorizer authorizer;

        RoleManagerCacheInvalidator(IRoleManager roleManager, IAuthorizer authorizer)
        {
            this.roleManager = roleManager;
            this.authorizer = authorizer;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T extends IRoleManager> T implementation()
        {
            return (T) roleManager;
        }

        @Override
        public Set<Option> supportedOptions()
        {
            return roleManager.supportedOptions();
        }

        @Override
        public Set<Option> alterableOptions()
        {
            return roleManager.alterableOptions();
        }

        @Override
        public void createRole(AuthenticatedUser performer,
                               RoleResource role,
                               RoleOptions options)
        {
            roleManager.createRole(performer, role, options);
            invalidateRoles(Collections.singleton(role));
        }

        @Override
        public void dropRole(AuthenticatedUser performer,
                             RoleResource role)
        {
            // We do not want to load those roles within the cache so we have to fetch them directly from
            // the IRoleManager
            Set<RoleResource> roles = getRoles(role, true);
            roleManager.dropRole(performer, role);
            authorizer.revokeAllFrom(role);
            authorizer.revokeAllOn(role);
            invalidateRoles(roles);
        }

        @Override
        public void alterRole(AuthenticatedUser performer,
                              RoleResource role,
                              RoleOptions options)
        {
            // We do not want to load those roles within the cache so we have to fetch them directly from
            // the IRoleManager
            Set<RoleResource> roles = getRoles(role, true);
            roleManager.alterRole(performer, role, options);
            invalidateRoles(roles);
        }

        @Override
        public void grantRole(AuthenticatedUser performer,
                              RoleResource role,
                              RoleResource grantee)
        {
            // We do not want to load those roles within the cache so we have to fetch them directly from
            // the IRoleManager
            Set<RoleResource> roles = getRoles(role, true);
            roles.addAll(getRoles(grantee, true));
            roleManager.grantRole(performer, role, grantee);
            invalidateRoles(roles);
        }

        @Override
        public void revokeRole(AuthenticatedUser performer,
                               RoleResource role,
                               RoleResource revokee)
        {
            // We do not want to load those roles within the cache so we have to fetch them directly from
            // the IRoleManager
            Set<RoleResource> roles = getRoles(role, true);
            roles.addAll(getRoles(revokee, true));
            roleManager.revokeRole(performer, role, revokee);
            invalidateRoles(roles);
        }

        @Override
        public Set<RoleResource> getRoles(RoleResource grantee,
                                          boolean includeInherited)
        {
            return roleManager.getRoles(grantee, includeInherited);
        }

        @Override
        public Set<RoleResource> getAllRoles()
        {
            return roleManager.getAllRoles();
        }

        @Override
        public boolean isSuper(RoleResource role)
        {
            return roleManager.isSuper(role);
        }

        @Override
        public boolean canLogin(RoleResource role)
        {
            return roleManager.canLogin(role);
        }

        @Override
        public Map<String, String> getCustomOptions(RoleResource role)
        {
            return roleManager.getCustomOptions(role);
        }

        @Override
        public boolean isExistingRole(RoleResource role)
        {
            return roleManager.isExistingRole(role);
        }

        @Override
        public Set<RoleResource> filterExistingRoleNames(List<String> roleNames)
        {
            return roleManager.filterExistingRoleNames(roleNames);
        }

        @Override
        public Role getRoleData(RoleResource role)
        {
            return roleManager.getRoleData(role);
        }

        @Override
        public Set<? extends IResource> protectedResources()
        {
            return roleManager.protectedResources();
        }

        @Override
        public void validateConfiguration() throws ConfigurationException
        {
            roleManager.validateConfiguration();
        }

        @Override
        public Future<?> setup()
        {
            return roleManager.setup();
        }

        @Override
        public boolean hasSuperuserStatus(RoleResource role)
        {
            return roleManager.hasSuperuserStatus(role);
        }
    }

    /**
     * Decorator used to invalidate cache data on role modifications
     */
    private class AuthorizerInvalidator implements IAuthorizer
    {
        private final IAuthorizer authorizer;

        AuthorizerInvalidator(IAuthorizer authorizer)
        {
            this.authorizer = authorizer;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T extends IAuthorizer> T implementation()
        {
            return (T) authorizer;
        }

        @Override
        public boolean requireAuthorization()
        {
            return authorizer.requireAuthorization();
        }

        @Override
        public Map<IResource, PermissionSets> allPermissionSets(RoleResource role)
        {
            return authorizer.allPermissionSets(role);
        }

        @Override
        public Set<Permission> grant(AuthenticatedUser performer,
                                     Set<Permission> permissions,
                                     IResource resource,
                                     RoleResource grantee,
                                     GrantMode... grantModes)
        {
            // We do not want to load those roles within the cache so we have to fetch them directly from
            // the IRoleManager
            Set<RoleResource> roles = roleManager.getRoles(grantee, true);
            Set<Permission> granted = authorizer.grant(performer, permissions, resource, grantee, grantModes);
            if (!granted.isEmpty())
                invalidateRoles(roles);
            return granted;
        }

        @Override
        public Set<Permission> revoke(AuthenticatedUser performer,
                                      Set<Permission> permissions,
                                      IResource resource,
                                      RoleResource revokee,
                                      GrantMode... grantModes)
        {
            // We do not want to load those roles within the cache so we have to fetch them directly from
            // the IRoleManager
            Set<RoleResource> roles = roleManager.getRoles(revokee, true);
            Set<Permission> revoked = authorizer.revoke(performer, permissions, resource, revokee, grantModes);
            if (!revoked.isEmpty())
                invalidateRoles(roles);
            return revoked;
        }

        @Override
        public Set<PermissionDetails> list(Set<Permission> permissions,
                                           IResource resource,
                                           RoleResource grantee)
        {
            return authorizer.list(permissions, resource, grantee);
        }

        @Override
        public void revokeAllFrom(RoleResource revokee)
        {
            // We do not want to load those roles within the cache so we have to fetch them directly from
            // the IRoleManager
            Set<RoleResource> roles = roleManager.getRoles(revokee, true);
            authorizer.revokeAllFrom(revokee);
            invalidateRoles(roles);
        }

        @Override
        public void revokeAllOn(IResource droppedResource)
        {
            authorizer.revokeAllOn(droppedResource);
        }

        @Override
        public Set<? extends IResource> protectedResources()
        {
            return authorizer.protectedResources();
        }

        @Override
        public void validateConfiguration() throws ConfigurationException
        {
            authorizer.validateConfiguration();
        }

        @Override
        public void setup()
        {
            authorizer.setup();
        }

        @Override
        public Set<Permission> applicablePermissions(IResource resource)
        {
            return authorizer.applicablePermissions(resource);
        }
    }
}
