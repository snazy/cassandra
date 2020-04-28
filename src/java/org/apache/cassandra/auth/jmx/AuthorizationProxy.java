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

package org.apache.cassandra.auth.jmx;

import java.lang.reflect.*;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.security.auth.Subject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.auth.UserRolesAndPermissions;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;

/**
 * Provides a proxy interface to the platform's MBeanServer instance to perform
 * role-based authorization on method invocation.
 *
 * When used in conjunction with a suitable JMXAuthenticator, which attaches a CassandraPrincipal
 * to authenticated Subjects, this class uses the configured IAuthorizer to verify that the
 * subject has the required permissions to execute methods on the MBeanServer and the MBeans it
 * manages.
 *
 * Because an ObjectName may contain wildcards, meaning it represents a set of individual MBeans,
 * JMX resources don't fit well with the hierarchical approach modelled by other IResource
 * implementations and utilised by ClientState::ensurePermission etc. To enable grants to use
 * pattern-type ObjectNames, this class performs its own custom matching and filtering of resources
 * rather than pushing that down to the configured IAuthorizer. To that end, during authorization
 * it pulls back all permissions for the active subject, filtering them to retain only grants on
 * JMXResources. It then uses ObjectName::apply to assert whether the target MBeans are wholly
 * represented by the resources with permissions. This means that it cannot use the PermissionsCache
 * as IAuthorizer can, so it manages its own cache locally.
 *
 * Methods are split into 2 categories; those which are to be invoked on the MBeanServer itself
 * and those which apply to MBean instances. Actually, this is somewhat of a construct as in fact
 * *all* invocations are performed on the MBeanServer instance, the distinction is made here on
 * those methods which take an ObjectName as their first argument and those which do not.
 * Invoking a method of the former type, e.g. MBeanServer::getAttribute(ObjectName name, String attribute),
 * implies that the caller is concerned with a specific MBean. Conversely, invoking a method such as
 * MBeanServer::getDomains is primarily a function of the MBeanServer itself. This class makes
 * such a distinction in order to identify which JMXResource the subject requires permissions on.
 *
 * Certain operations are never allowed for users and these are recorded in a blacklist so that we
 * can short circuit authorization process if one is attempted by a remote subject.
 *
 */
public class AuthorizationProxy implements InvocationHandler
{
    private static final Logger logger = LoggerFactory.getLogger(AuthorizationProxy.class);

    /*
     A whitelist of permitted methods on the MBeanServer interface which *do not* take an ObjectName
     as their first argument. These methods can be thought of as relating to the MBeanServer itself,
     rather than to the MBeans it manages. All of the whitelisted methods are essentially descriptive,
     hence they require the Subject to have the DESCRIBE permission on the root JMX resource.
     */
    private static final Set<String> MBEAN_SERVER_METHOD_WHITELIST = ImmutableSet.of("getDefaultDomain",
                                                                                     "getDomains",
                                                                                     "getMBeanCount",
                                                                                     "hashCode",
                                                                                     "queryMBeans",
                                                                                     "queryNames",
                                                                                     "toString");

    /*
     A blacklist of method names which are never permitted to be executed by a remote user,
     regardless of privileges they may be granted.
     */
    private static final Set<String> METHOD_BLACKLIST = ImmutableSet.of("createMBean",
                                                                        "deserialize",
                                                                        "getClassLoader",
                                                                        "getClassLoaderFor",
                                                                        "instantiate",
                                                                        "registerMBean",
                                                                        "unregisterMBean");

    private MBeanServer mbs;

    /*
     Used to check whether the Role associated with the authenticated Subject has superuser
     status. By default, just delegates to Roles::hasSuperuserStatus, but can be overridden for testing.
     */
    protected Function<String, UserRolesAndPermissions> userRolesAndPermissions = (name) ->
    {
        if (name == null)
            return UserRolesAndPermissions.SYSTEM;

        return DatabaseDescriptor.getAuthManager()
                                 .getUserRolesAndPermissions(name);
    };

    /*
     Used to decide whether authorization is enabled or not, usually this depends on the configured
     IAuthorizer, but can be overridden for testing.
     */
    protected BooleanSupplier isAuthzRequired = DatabaseDescriptor.getAuthorizer()::requireAuthorization;

    /*
     Used to find matching MBeans when the invocation target is a pattern type ObjectName.
     Defaults to querying the MBeanServer but can be overridden for testing. See checkPattern for usage.
     */
    protected Function<ObjectName, Set<ObjectName>> queryNames = (name) -> mbs.queryNames(name, null);

    /*
     Used to determine whether auth setup has completed so we know whether the expect the IAuthorizer
     to be ready. Can be overridden for testing.
     */
    protected BooleanSupplier isAuthSetupComplete = StorageService.instance::isAuthSetupComplete;

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable
    {
        String methodName = method.getName();

        if ("getMBeanServer".equals(methodName))
            throw new SecurityException("Access denied");

        // Retrieve Subject from current AccessControlContext
        AccessControlContext acc = AccessController.getContext();
        Subject subject = Subject.getSubject(acc);

        // Allow setMBeanServer iff performed on behalf of the connector server itself
        if (("setMBeanServer").equals(methodName))
        {
            if (subject != null)
                throw new SecurityException("Access denied");

            if (args[0] == null)
                throw new IllegalArgumentException("Null MBeanServer");

            if (mbs != null)
                throw new IllegalArgumentException("MBeanServer already initialized");

            mbs = (MBeanServer) args[0];
            return null;
        }

        if (authorize(subject, methodName, args))
            return invoke(method, args);

        throw new SecurityException("Access Denied");
    }

    /**
     * Performs the actual authorization of an identified subject to execute a remote method invocation.
     * @param subject The principal making the execution request. A null value represents a local invocation
     *                from the JMX connector itself
     * @param methodName Name of the method being invoked
     * @param args Array containing invocation argument. If the first element is an ObjectName instance, for
     *             authz purposes we consider this an invocation of an MBean method, otherwise it is treated
     *             as an invocation of a method on the MBeanServer.
     */
    @VisibleForTesting
    boolean authorize(Subject subject, String methodName, Object[] args)
    {
        logger.trace("Authorizing JMX method invocation {} for {}",
                     methodName,
                     subject == null ? "" :subject.toString().replaceAll("\\n", " "));

        if (!isAuthSetupComplete.getAsBoolean())
        {
            logger.trace("Auth setup is not complete, refusing access");
            return false;
        }

        // Permissive authorization is enabled
        if (!isAuthzRequired.getAsBoolean())
            return true;

        // Allow operations performed locally on behalf of the connector server itself
        if (subject == null)
            return true;

        // Restrict access to certain methods by any remote user
        if (METHOD_BLACKLIST.contains(methodName))
        {
            logger.trace("Access denied to blacklisted method {}", methodName);
            return false;
        }

        // Reject if the user has not authenticated
        Set<Principal> principals = subject.getPrincipals();
        if (principals == null || principals.isEmpty())
            return false;

        // Currently, we assume that the first Principal returned from the Subject
        // is the one to use for authorization. It would be good to make this more
        // robust, but we have no control over which Principals a given LoginModule
        // might choose to associate with the Subject following successful authentication
        String name = principals.iterator().next().getName();
        UserRolesAndPermissions user = userRolesAndPermissions.apply(name);
        // A role with superuser status can do anything
        if (user.isSuper())
            return true;

        // The method being invoked may be a method on an MBean, or it could belong
        // to the MBeanServer itself
        if (args != null && args[0] instanceof ObjectName)
            return authorizeMBeanMethod(user, methodName, args);

        return authorizeMBeanServerMethod(user, methodName);
    }

    /**
     * Authorize execution of a method on the MBeanServer which does not take an MBean ObjectName
     * as its first argument. The whitelisted methods that match this criteria are generally
     * descriptive methods concerned with the MBeanServer itself, rather than with any particular
     * set of MBeans managed by the server and so we check the DESCRIBE permission on the root
     * JMXResource (representing the MBeanServer)
     *
     * @param user
     * @param methodName
     * @return the result of the method invocation, if authorized
     * @throws Throwable
     * @throws SecurityException if authorization fails
     */
    private boolean authorizeMBeanServerMethod(UserRolesAndPermissions user, String methodName)
    {
        logger.trace("JMX invocation of {} on MBeanServer requires permission {}", methodName, Permission.DESCRIBE);
        return (MBEAN_SERVER_METHOD_WHITELIST.contains(methodName) &&
                user.hasPermission(Permission.DESCRIBE, JMXResource.root()));
    }

    /**
     * Authorize execution of a method on an MBean (or set of MBeans) which may be
     * managed by the MBeanServer. Note that this also includes the queryMBeans and queryNames
     * methods of MBeanServer as those both take an ObjectName (possibly a pattern containing
     * wildcards) as their first argument. They both of those methods also accept null arguments,
     * in which case they will be handled by authorizedMBeanServerMethod
     *
     * @param user
     * @param methodName
     * @param args
     * @return the result of the method invocation, if authorized
     * @throws Throwable
     * @throws SecurityException if authorization fails
     */
    private boolean authorizeMBeanMethod(UserRolesAndPermissions user, String methodName, Object[] args)
    {
        ObjectName targetBean = (ObjectName)args[0];

        // work out which permission we need to execute the method being called on the mbean
        Permission requiredPermission = getRequiredPermission(methodName);
        if (null == requiredPermission)
            return false;

        logger.trace("JMX invocation of {} on {} requires permission {}", methodName, targetBean, requiredPermission);

        boolean hasJmxPermission = user.hasJMXPermission(requiredPermission, mbs, targetBean);

        if (!hasJmxPermission)
            logger.trace("Subject does not have sufficient permissions on target MBean {}", targetBean);

        return hasJmxPermission;
    }

    /**
     * Mapping between method names and the permission required to invoke them. Note, these
     * names refer to methods on MBean instances invoked via the MBeanServer.
     * @param methodName
     * @return
     */
    private static Permission getRequiredPermission(String methodName)
    {
        switch (methodName)
        {
            case "getAttribute":
            case "getAttributes":
                return Permission.SELECT;
            case "setAttribute":
            case "setAttributes":
                return Permission.MODIFY;
            case "invoke":
                return Permission.EXECUTE;
            case "getInstanceOf":
            case "getMBeanInfo":
            case "hashCode":
            case "isInstanceOf":
            case "isRegistered":
            case "queryMBeans":
            case "queryNames":
                return Permission.DESCRIBE;
            default:
                logger.debug("Access denied, method name {} does not map to any defined permission", methodName);
                return null;
        }
    }

    /**
     * Invoke a method on the MBeanServer instance. This is called when authorization is not required (because
     * AllowAllAuthorizer is configured, or because the invocation is being performed by the JMXConnector
     * itself rather than by a connected client), and also when a call from an authenticated subject
     * has been successfully authorized
     *
     * @param method
     * @param args
     * @return
     * @throws Throwable
     */
    private Object invoke(Method method, Object[] args) throws Throwable
    {
        try
        {
            return method.invoke(mbs, args);
        }
        catch (InvocationTargetException e) //Catch any exception that might have been thrown by the mbeans
        {
            Throwable t = e.getCause(); //Throw the exception that nodetool etc expects
            throw t;
        }
    }
}
