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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Only purpose is to Initialize authentication/authorization via {@link #applyAuth()}.
 * This is in this separate class as it implicitly initializes schema stuff (via classes referenced in here).
 */
public final class AuthConfig
{
    private static final Logger logger = LoggerFactory.getLogger(AuthConfig.class);

    private static boolean initialized;

    public static void applyAuth()
    {
        // some tests need this
        if (initialized)
            return;

        initialized = true;

        Config conf = DatabaseDescriptor.getRawConfig();

        IAuthenticator authenticator = new AllowAllAuthenticator();

        /* Authentication, authorization and role management backend, implementing IAuthenticator, IAuthorizer & IRoleMapper*/
        if (conf.authenticator != null)
            authenticator = FBUtilities.newAuthenticator(conf.authenticator);

        DatabaseDescriptor.setAuthenticator(authenticator);

        // authorizer

        IAuthorizer authorizer = new AllowAllAuthorizer();

        if (conf.authorizer != null)
            authorizer = FBUtilities.newAuthorizer(conf.authorizer);

        if (!authenticator.requireAuthentication() && authorizer.requireAuthorization())
            throw new ConfigurationException(conf.authenticator + " can't be used with " + conf.authorizer, false);

        // role manager

        IRoleManager roleManager;
        if (conf.role_manager != null)
            roleManager = FBUtilities.newRoleManager(conf.role_manager);
        else
            roleManager = new CassandraRoleManager();

        if (authenticator instanceof PasswordAuthenticator && !(roleManager instanceof CassandraRoleManager))
            throw new ConfigurationException("CassandraRoleManager must be used with PasswordAuthenticator", false);

        // authenticator

        if (conf.internode_authenticator != null)
            DatabaseDescriptor.setInternodeAuthenticator(FBUtilities.construct(conf.internode_authenticator, "internode_authenticator"));

        // network authorizer
        INetworkAuthorizer networkAuthorizer = FBUtilities.newNetworkAuthorizer(conf.network_authorizer);
        if (networkAuthorizer.requireAuthorization() && !authenticator.requireAuthentication())
        {
            throw new ConfigurationException(conf.network_authorizer + " can't be used with " + conf.authenticator, false);
        }

        // Validate at last to have authenticator, authorizer, role-manager and internode-auth setup
        // in case these rely on each other.

        authenticator.validateConfiguration();
        authorizer.validateConfiguration();
        roleManager.validateConfiguration();
        networkAuthorizer.validateConfiguration();
        DatabaseDescriptor.getInternodeAuthenticator().validateConfiguration();

        DatabaseDescriptor.setAuthManager(new AuthManager(roleManager, authorizer, networkAuthorizer));

        if (DatabaseDescriptor.isSystemKeyspaceFilteringEnabled())
        {
            if (!DatabaseDescriptor.getAuthorizer().requireAuthorization())
            {
                logger.error("In order to use system keyspace filtering, an authorizer that requires authorization must be configured.");
                throw new ConfigurationException("In order to use system keyspace filtering, an authorizer that requires authorization must be configured.");
            }
            if (!DatabaseDescriptor.getAuthenticator().requireAuthentication())
            {
                logger.error("In order to use system keyspace filtering, an authenticator that requires authentication must be configured.");
                throw new ConfigurationException("In order to use system keyspace filtering, an authenticator that requires authentication must be configured.");
            }

            logger.info("System keyspaces filtering enabled.");
        }
        else
        {
            logger.info("System keyspaces filtering not enabled.");
        }
    }
}
