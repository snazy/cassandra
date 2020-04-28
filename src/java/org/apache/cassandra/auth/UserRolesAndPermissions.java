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

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;

/**
 * The roles and permissions of an authentified user.
 */
public abstract class UserRolesAndPermissions
{
    private static final Logger logger = LoggerFactory.getLogger(UserRolesAndPermissions.class);

    /**
     * The roles and permissions of the system.
     */
    public static final UserRolesAndPermissions SYSTEM = new UserRolesAndPermissions(AuthenticatedUser.SYSTEM_USERNAME, Collections.emptySet())
    {
        @Override
        public boolean hasGrantPermission(Permission perm, IResource resource)
        {
            return false;
        }

        @Override
        public boolean hasDataPermission(Permission perm, DataResource resource)
        {
            return true;
        }

        @Override
        public boolean isSystem()
        {
            return true;
        }

        @Override
        protected void ensurePermissionOnResourceChain(Permission perm, IResource resource)
        {
        }

        @Override
        protected boolean hasPermissionOnResourceChain(Permission perm, IResource resource)
        {
            return true;
        }

        @Override
        public boolean hasJMXPermission(Permission permission, MBeanServer mbs, ObjectName object)
        {
            return true;
        }
    };

    /**
     * The roles and permissions of an anonymous user.
     * <p>Anonymous users only exists in transitional mode (when the permission are being set up).
     * An anonymous user can access any table data but is blocked from performing any role or permissions operations.
     * </p>
     */
    public static final UserRolesAndPermissions ANONYMOUS = new UserRolesAndPermissions(AuthenticatedUser.ANONYMOUS_USERNAME, Collections.emptySet())
    {
        @Override
        public void ensureNotAnonymous()
        {
            throw new UnauthorizedException("Anonymous users are not authorized to perform this request");
        }

        @Override
        public void ensureIsSuperuser(String msg)
        {
        }

        @Override
        public boolean hasGrantPermission(Permission perm, IResource resource)
        {
            return false;
        }

        @Override
        protected boolean hasPermissionOnResourceChain(Permission perm, IResource resource)
        {
            return Permission.AUTHORIZE != perm;
        }

        @Override
        public boolean hasJMXPermission(Permission permission, MBeanServer mbs, ObjectName object)
        {
            return true;
        }

        @Override
        protected void ensurePermissionOnResourceChain(Permission perm, IResource resource)
        {
            if (Permission.AUTHORIZE == perm)
                throw new UnauthorizedException("Anonymous users are not authorized to perform this request");
        }
    };

    /**
     * The user name.
     */
    private final String name;

    /**
     * The user roles.
     */
    protected final Set<RoleResource> roles;

    private UserRolesAndPermissions(String name, Set<RoleResource> roles)
    {
        this.name = name;
        this.roles = roles;
    }

    /**
     * Creates a new user with the specified name and roles.
     * @param name the user name
     * @param roles the user roles
     * @return a new user
     */
    public static UserRolesAndPermissions newNormalUserRoles(String name,
                                                             Set<RoleResource> roles,
                                                             DCPermissions dcPermissions)
    {
        assert !DatabaseDescriptor.getAuthorizer().requireAuthorization() : "An user without permissions can only created when the authorization are not required";
        return new NormalUserRoles(name, roles, dcPermissions);
    }

    /**
     * Creates a new user with the specified name, roles and permissions.
     * @param name the user name
     * @param roles the user roles
     * @param permissions the permissions per role and resources.
     * @return a new user
     */
    public static UserRolesAndPermissions newNormalUserRolesAndPermissions(String name,
                                                                           Set<RoleResource> roles,
                                                                           DCPermissions dcPermissions,
                                                                           Map<RoleResource, Map<IResource, PermissionSets>> permissions)
    {
        return new NormalUserWithPermissions(name, roles, dcPermissions, permissions);
    }

    /**
     * Creates a new super user with the specified name and roles.
     * @param name the user name
     * @param roles the user roles
     * @return a new super user
     */
    public static UserRolesAndPermissions createSuperUserRolesAndPermissions(String name, Set<RoleResource> roles)
    {
        return new SuperUserRoleAndPermissions(name, roles);
    }

    /**
     * Returns the user name.
     * @return the user name.
     */
    public final String getName()
    {
        return name;
    }

    /**
     * Checks if this user is a super user.
     * <p>Only a superuser is allowed to perform CREATE USER and DROP USER queries.
     * Im most cased, though not necessarily, a superuser will have Permission.ALL on every resource
     * (depends on IAuthorizer implementation).</p>
     */
    public boolean isSuper()
    {
        return false;
    }

    /**
     * Checks if this user is the system user (Apollo).
     * @return {@code true} if this user is the system user, {@code flase} otherwise.
     */
    public boolean isSystem()
    {
        return false;
    }

    /**
     * Validates that this user is not an anonymous one.
     */
    public void ensureNotAnonymous()
    {
    }

    public void ensureIsSuperuser(String msg)
    {
    }

    public void ensureDCPermissions(String dc)
    {
    }

    /**
     * Checks if the user has the specified permission on the data resource.
     * @param perm the permission
     * @param resource the resource
     * @return {@code true} if the user has the permission on the data resource,
     * {@code false} otherwise.
     */
    public boolean hasDataPermission(Permission perm, DataResource resource)
    {
        if (!DataResource.root().equals(resource))
        {
            // we only care about schema modification.
            if (isSchemaModification(perm))
            {
                String keyspace = resource.getKeyspace();

                try
                {
                    preventSystemKSSchemaModification(perm, resource);
                }
                catch (UnauthorizedException e)
                {
                    return false;
                }

                if (SchemaConstants.isReplicatedSystemKeyspace(keyspace)
                    && perm != Permission.ALTER
                    && !(perm == Permission.DROP && Resources.isDroppable(resource)))
                {
                    return false;
                }
            }

            if (perm == Permission.SELECT && Resources.isAlwaysReadable(resource))
                return true;

            if (Resources.isProtected(resource) && isSchemaModification(perm))
                return false;
        }

        return hasPermissionOnResourceChain(perm, resource);
    }

    /**
     * Checks if the user has the specified permission on the role resource.
     * @param perm the permission
     * @param resource the resource
     * @return {@code true} if the user has the permission on the role resource,
     * {@code false} otherwise.
     */
    public final boolean hasRolePermission(Permission perm, RoleResource resource)
    {
        return hasPermissionOnResourceChain(perm, resource);
    }

    /**
     * Checks if the user has the specified permission on the function resource.
     * @param perm the permission
     * @param resource the resource
     * @return {@code true} if the user has the permission on the function resource,
     * {@code false} otherwise.
     */
    public final boolean hasFunctionPermission(Permission perm, FunctionResource resource)
    {
        // Access to built in functions is unrestricted
        if (resource.hasParent() && isNativeFunction(resource))
            return true;

        return hasPermissionOnResourceChain(perm, resource);
    }

    /**
     * Checks if the user has the specified permission on the JMX object.
     * @param permission the permission
     * @param mbs The MBeanServer
     * @param object the object
     * @return {@code true} if the user has the permission on the function resource,
     * {@code false} otherwise.
     */
    public abstract boolean hasJMXPermission(Permission permission, MBeanServer mbs, ObjectName object);

    /**
     * Checks if the user has the specified permission on the resource.
     * @param perm the permission
     * @param resource the resource
     * @return {@code true} if the user has the permission on the resource,
     * {@code false} otherwise.
     */
    public final boolean hasPermission(Permission perm, IResource resource)
    {
        if (resource instanceof DataResource)
            return hasDataPermission(perm, (DataResource) resource);

        if (resource instanceof FunctionResource)
            return hasFunctionPermission(perm, (FunctionResource) resource);

        return hasPermissionOnResourceChain(perm, resource);
    }

    /**
     * Checks if the user has the right to grant the permission on the resource.
     * @param perm the permission
     * @param resource the resource
     * @return {@code true} if the user has the right to grant the permission on the resource,
     * {@code false} otherwise.
     */
    public abstract boolean hasGrantPermission(Permission perm, IResource resource);

    /**
     * Validates that the user has the permission on the data resource.
     * @param perm the permission
     * @param resource the resource
     * @throws UnauthorizedException if the user does not have the permission on the data resource
     */
    public final void ensureDataPermission(Permission perm, DataResource resource)
    {
        if (!DataResource.root().equals(resource))
        {
            preventSystemKSSchemaModification(perm, resource);

            if (perm == Permission.SELECT && Resources.isAlwaysReadable(resource))
                return;

            if (Resources.isProtected(resource))
                if (isSchemaModification(perm))
                    throw new UnauthorizedException(String.format("%s schema is protected", resource));
        }

        ensurePermissionOnResourceChain(perm, resource);
    }

    private void preventSystemKSSchemaModification(Permission perm, DataResource resource)
    {
        String keyspace = resource.getKeyspace();
        validateKeyspace(keyspace);

        // we only care about schema modification.
        if (!isSchemaModification(perm))
            return;

        // prevent system keyspace modification (not only because this could be dangerous, but also because this just
        // wouldn't work with the way the schema of those system keyspace/table schema is hard-coded)
        if (SchemaConstants.isLocalSystemKeyspace(keyspace))
            throw new UnauthorizedException(keyspace + " keyspace is not user-modifiable.");

        if (SchemaConstants.isReplicatedSystemKeyspace(keyspace))
        {
            // allow users with sufficient privileges to alter replication params of replicated system keyspaces
            if (perm == Permission.ALTER && resource.isKeyspaceLevel())
                return;

            // prevent all other modifications of replicated system keyspaces
            throw new UnauthorizedException(String.format("Cannot %s %s", perm, resource));
        }
    }

    /**
     * Checks if the specified permission is for a schema modification.
     * @param perm the permission to check
     * @return {@code true} if the permission is for a schema modification, {@code false} otherwise.
     */
    private boolean isSchemaModification(Permission perm)
    {
        return (perm == Permission.CREATE) || (perm == Permission.ALTER) || (perm == Permission.DROP);
    }

    /**
     * Validates that the user has the permission on all the keyspaces.
     * @param perm the permission
     * @throws UnauthorizedException if the user does not have the permission on all the keyspaces
     */
    public final void ensureAllKeyspacesPermission(Permission perm)
    {
        ensureDataPermission(perm, DataResource.root());
    }

    /**
     * Validates that the user has the permission on the keyspace.
     * @param perm the permission
     * @param keyspace the keyspace
     * @throws UnauthorizedException if the user does not have the permission on the keyspace
     */
    public final void ensureKeyspacePermission(Permission perm, String keyspace)
    {
        validateKeyspace(keyspace);
        ensureDataPermission(perm, DataResource.keyspace(keyspace));
    }

    /**
     * Validates that the specified keyspace is not {@code null}.
     * @param keyspace the keyspace to check.
     */
    protected static void validateKeyspace(String keyspace)
    {
        if (keyspace == null)
            throw new InvalidRequestException("You have not set a keyspace for this session");
    }

    /**
     * Validates that the user has the permission on the table.
     * @param perm the permission
     * @param keyspace the table keyspace
     * @param table the table
     * @throws UnauthorizedException if the user does not have the permission on the table.
     */
    public final void ensureTablePermission(Permission perm, String keyspace, String table)
    {
        ensureDataPermission(perm, DataResource.table(keyspace, table));
    }

    /**
     * Validates that the user has the permission on the table.
     * @param perm the permission
     * @param tableRef the table
     * @throws UnauthorizedException if the user does not have the permission on the table.
     */
    public final void ensureTablePermission(Permission perm, TableMetadataRef tableRef)
    {
        ensureTablePermission(perm, tableRef.get());
    }

    /**
     * Validates that the user has the permission on the table.
     * @param perm the permission
     * @param table the table metadata
     * @throws UnauthorizedException if the user does not have the permission on the table.
     */
    public final void ensureTablePermission(Permission perm, TableMetadata table)
    {
        ensureDataPermission(perm, table.resource);
    }

    /**
     * Validates that the user has the permission on the function.
     * @param perm the permission
     * @param function the function
     * @throws UnauthorizedException if the user does not have the permission on the function.
     */
    public final void ensureFunctionPermission(Permission perm, Function function)
    {
        // built in functions are always available to all
        if (function.isNative())
            return;

        ensurePermissionOnResourceChain(perm, FunctionResource.function(function.name().keyspace,
                                                                        function.name().name,
                                                                        function.argTypes()));
    }

    /**
     * Validates that the user has the permission on the function.
     * @param perm the function resource
     * @param resource the resource
     * @throws UnauthorizedException if the user does not have the permission on the function.
     */
    public final void ensureFunctionPermission(Permission perm, FunctionResource resource)
    {
        // Access to built in functions is unrestricted
        if(resource.hasParent() && isNativeFunction(resource))
            return;

        ensurePermissionOnResourceChain(perm, resource);
    }

    private boolean isNativeFunction(FunctionResource resource)
    {
        return resource.getKeyspace().equals(SchemaConstants.SYSTEM_KEYSPACE_NAME);
    }

    public final void ensurePermission(Permission perm, IResource resource)
    {
        if (resource instanceof DataResource)
        {
            ensureDataPermission(perm, (DataResource) resource);
        }
        else if (resource instanceof FunctionResource)
        {
            ensureFunctionPermission(perm, (FunctionResource) resource);
        }
        else
        {
            ensurePermissionOnResourceChain(perm, resource);
        }
    }

    protected abstract void ensurePermissionOnResourceChain(Permission perm, IResource resource);

    protected abstract boolean hasPermissionOnResourceChain(Permission perm, IResource resource);

    /**
     * Checks that this user has the specified role.
     * @param role the role
     * @return {@code true} if the user has the specified role, {@code false} otherwise.
     */
    public final boolean hasRole(RoleResource role)
    {
        return roles.contains(role);
    }

    public final Set<RoleResource> getRoles()
    {
        return roles;
    }

    /**
     * A super user.
     */
    private static final class SuperUserRoleAndPermissions extends UserRolesAndPermissions
    {
        public SuperUserRoleAndPermissions(String name, Set<RoleResource> roles)
        {
            super(name, roles);
        }

        @Override
        public boolean isSuper()
        {
            return true;
        }

        @Override
        public boolean hasGrantPermission(Permission perm, IResource resource)
        {
            return true;
        }

        @Override
        protected void ensurePermissionOnResourceChain(Permission perm, IResource resource)
        {
        }

        @Override
        protected boolean hasPermissionOnResourceChain(Permission perm, IResource resource)
        {
            return true;
        }

        @Override
        public boolean hasJMXPermission(Permission permission, MBeanServer mbs, ObjectName object)
        {
            return true;
        }
    }

    /**
     * A normal user with all his roles and permissions.
     */
    private static abstract class AbstractNormalUser extends UserRolesAndPermissions
    {
        private final DCPermissions dcPermissions;

        public AbstractNormalUser(String name,
                                         Set<RoleResource> roles,
                                         DCPermissions dcPermissions)
        {
            super(name, roles);
            this.dcPermissions = dcPermissions;

        }

        @Override
        public void ensureDCPermissions(String dc)
        {
            if (dcPermissions != null && !dcPermissions.canAccess(dc))
                throw new UnauthorizedException(String.format("You do not have access to this datacenter (%s)", dc));
        }
    }

    /**
     * A normal user with all his roles and permissions.
     */
    private static final class NormalUserWithPermissions extends AbstractNormalUser
    {
        /**
         * The user permissions per role and resources.
         */
        private final Map<RoleResource, Map<IResource, PermissionSets>> permissions;

        public NormalUserWithPermissions(String name,
                                         Set<RoleResource> roles,
                                         DCPermissions dcPermissions,
                                         Map<RoleResource, Map<IResource, PermissionSets>> permissions)
        {
            super(name, roles, dcPermissions);
            this.permissions = permissions;
        }

        @Override
        public void ensureIsSuperuser(String msg)
        {
            throw new UnauthorizedException(msg);
        }

        @Override
        public boolean hasGrantPermission(Permission perm, IResource resource)
        {
            for (PermissionSets permissions : getAllPermissionSetsFor(resource))
            {
                if (permissions.grantables.contains(perm))
                    return true;
            }
            return false;
        }

        @Override
        protected void ensurePermissionOnResourceChain(Permission perm, IResource resource)
        {
            boolean granted = false;
            for (PermissionSets permissions : getAllPermissionSetsFor(resource))
            {
                granted |= permissions.granted.contains(perm);
                if (permissions.restricted.contains(perm))
                    throw new UnauthorizedException(String.format("Access for user %s on %s or any of its parents with %s permission is restricted",
                                                                  getName(),
                                                                  resource,
                                                                  perm));
            }

            if (!granted)
                throw new UnauthorizedException(String.format("User %s has no %s permission on %s or any of its parents",
                                                              getName(),
                                                              perm,
                                                              resource));
        }

        private List<PermissionSets> getAllPermissionSetsFor(IResource resource)
        {
            List<? extends IResource> chain = Resources.chain(resource);

            List<PermissionSets> list = new ArrayList<>(chain.size() * (super.roles.size() + 1));

            for (RoleResource roleResource : super.roles)
            {
                for (IResource res : chain)
                {
                    PermissionSets permissions = getPermissions(roleResource, res);
                    if (permissions != null)
                        list.add(permissions);
                }
            }

            return list;
        }

        @Override
        protected boolean hasPermissionOnResourceChain(Permission perm, IResource resource)
        {
            boolean granted = false;
            for (PermissionSets permissions : getAllPermissionSetsFor(resource))
            {
                granted |= permissions.granted.contains(perm);
                if (permissions.restricted.contains(perm))
                    return false;
            }
            return granted;
        }

        private PermissionSets getPermissions(RoleResource roleResource, IResource res)
        {
            Map<IResource, PermissionSets> map = this.permissions.get(roleResource);
            return map == null ? null : map.get(res);
        }

        @Override
        public boolean hasJMXPermission(Permission permission, MBeanServer mbs, ObjectName object)
        {
            for (Map<IResource, PermissionSets> permissionsPerResource : permissions.values())
            {
                PermissionSets rootPerms = permissionsPerResource.get(JMXResource.root());
                if (rootPerms != null)
                {
                    if (rootPerms.restricted.contains(permission))
                    {
                        // this role is _restricted_ the required permission on the JMX root resource
                        // I.e. restricted 'permission' on _any_ JMX resource.
                        return false;
                    }
                    if (rootPerms.granted.contains(permission))
                    {
                        // This role is _granted_ (and not restricted) the required permission on the JMX resource root.
                        // I.e. granted 'permission' on _any_ JMX resource.
                        return true;
                    }
                }
            }

            // Check for individual JMX resources.
            //
            // Collecting the (permitted) JMXResource instances before checking the bean names feels
            // cheaper than vice versa - especially for 'checkPattern', which performs a call to
            // javax.management.MBeanServer.queryNames().

            Set<JMXResource> permittedResources = collectPermittedJmxResources(permission);

            if (permittedResources.isEmpty())
                return false;

            // finally, check the JMXResource from the grants to see if we have either
            // an exact match or a wildcard match for the target resource, whichever is
            // applicable
            return object.isPattern()
                   ? checkPattern(mbs, object, permittedResources)
                   : checkExact(object, permittedResources);
        }

        /**
         * Given a set of JMXResources upon which the Subject has been granted a particular permission, check whether
         * any match the pattern-type ObjectName representing the target of the method invocation. At this point, we are
         * sure that whatever the required permission, the Subject has definitely been granted it against this set of
         * JMXResources. The job of this method is only to verify that the target of the invocation is covered by the
         * members of the set.
         *
         * @return true if all registered beans which match the target can also be matched by the JMXResources the
         *         subject has been granted permissions on; false otherwise
         */
        private boolean checkPattern(MBeanServer mbs, ObjectName target, Set<JMXResource> permittedResources)
        {
            // Get the full set of beans which match the target pattern
            Set<ObjectName> targetNames = mbs.queryNames(target, null);

            // Iterate over the resources the permission has been granted on. Some of these may
            // be patterns, so query the server to retrieve the full list of matching names and
            // remove those from the target set. Once the target set is empty (i.e. all required
            // matches have been satisfied), the requirement is met.
            // If there are still unsatisfied targets after all the JMXResources have been processed,
            // there are insufficient grants to permit the operation.
            for (JMXResource resource : permittedResources)
            {
                try
                {
                    Set<ObjectName> matchingNames = mbs.queryNames(ObjectName.getInstance(resource.getObjectName()),
                                                                   null);
                    targetNames.removeAll(matchingNames);
                    if (targetNames.isEmpty())
                        return true;
                }
                catch (MalformedObjectNameException e)
                {
                    logger.warn("Permissions for JMX resource contains invalid ObjectName {}",
                                resource.getObjectName());
                }
            }

            return false;
        }

        /**
         * Given a set of JMXResources upon which the Subject has been granted a particular permission, check whether
         * any match the ObjectName representing the target of the method invocation. At this point, we are sure that
         * whatever the required permission, the Subject has definitely been granted it against this set of
         * JMXResources. The job of this method is only to verify that the target of the invocation is matched by a
         * member of the set.
         *
         * @return true if at least one of the permitted resources matches the target; false otherwise
         */
        private boolean checkExact(ObjectName target, Set<JMXResource> permittedResources)
        {
            for (JMXResource resource : permittedResources)
            {
                try
                {
                    if (ObjectName.getInstance(resource.getObjectName()).apply(target))
                        return true;
                }
                catch (MalformedObjectNameException e)
                {
                    logger.warn("Permissions for JMX resource contains invalid ObjectName {}",
                                resource.getObjectName());
                }
            }

            logger.trace("Subject does not have sufficient permissions on target MBean {}", target);
            return false;
        }

        private Set<JMXResource> collectPermittedJmxResources(Permission permission)
        {
            Set<JMXResource> permittedResources = new HashSet<>();
            for (Map<IResource, PermissionSets> permissionsPerResource : permissions.values())
            {
                for (Map.Entry<IResource, PermissionSets> resourcePermissionSets : permissionsPerResource.entrySet())
                {
                    if (resourcePermissionSets.getKey() instanceof JMXResource)
                    {
                        if (resourcePermissionSets.getValue().hasEffectivePermission(permission))
                            permittedResources.add((JMXResource) resourcePermissionSets.getKey());
                    }
                }
            }
            return permittedResources;
        }
    }

    /**
     * A normal user with his roles but without permissions.
     * <p>This user should only be used when {@link IAuthorizer#requireAuthorization()} is {@code false}.</p>
     */
    private static final class NormalUserRoles extends AbstractNormalUser
    {
        public NormalUserRoles(String name, Set<RoleResource> roles, DCPermissions dcPermissions)
        {
            super(name, roles, dcPermissions);
        }

        @Override
        public void ensureIsSuperuser(String msg)
        {
            throw new UnauthorizedException(msg);
        }

        @Override
        public boolean hasGrantPermission(Permission perm, IResource resource)
        {
            return true;
        }

        @Override
        protected void ensurePermissionOnResourceChain(Permission perm, IResource resource)
        {
        }

        @Override
        protected boolean hasPermissionOnResourceChain(Permission perm, IResource resource)
        {
            return true;
        }

        @Override
        public boolean hasJMXPermission(Permission permission, MBeanServer mbs, ObjectName object)
        {
            return true;
        }
    }
}
