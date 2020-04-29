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

/**
 * Specifies whether the permission shall be {@link #GRANT granted} on a resource,
 * {@link #RESTRICT restricted} on a resource or {@link #GRANTABLE grantable} to others on a resource.
 */
public enum GrantMode
{
    /**
     * GRANT/REVOKE permission on the resource.
     */
    GRANT
    {
        @Override
        public String grantOperationName()
        {
            return "GRANT";
        }

        @Override
        public String revokeOperationName()
        {
            return "REVOKE";
        }

        @Override
        public String revokeWarningMessage(String roleName, IResource resource, String permissions)
        {
            return String.format("Role '%s' was not granted %s on %s", roleName, permissions, resource);
        }

        @Override
        public String grantWarningMessage(String roleName, IResource resource, String permissions)
        {
            return String.format("Role '%s' was already granted %s on %s", roleName, permissions, resource);
        }
    },
    /**
     * RESTRICT/UNRESTRICT permission on the resource.
     */
    RESTRICT
    {
        @Override
        public String grantOperationName()
        {
            return "RESTRICT";
        }

        @Override
        public String revokeOperationName()
        {
            return "UNRESTRICT";
        }

        @Override
        public String revokeWarningMessage(String roleName, IResource resource, String permissions)
        {
            return String.format("Role '%s' was not restricted %s on %s", roleName, permissions, resource);
        }

        @Override
        public String grantWarningMessage(String roleName, IResource resource, String permissions)
        {
            return String.format("Role '%s' was already restricted %s on %s", roleName, permissions, resource);
        }
    },
    /**
     * GRANT/REVOKE grant/authorize permission on the resource.
     */
    GRANTABLE
    {
        @Override
        public String grantOperationName()
        {
            return "GRANT AUTHORIZE FOR";
        }

        @Override
        public String revokeOperationName()
        {
            return "REVOKE AUTHORIZE FOR";
        }

        @Override
        public String revokeWarningMessage(String roleName, IResource resource, String permissions)
        {
            return String.format("Role '%s' was not granted AUTHORIZE FOR %s on %s", roleName, permissions, resource);
        }

        @Override
        public String grantWarningMessage(String roleName, IResource resource, String permissions)
        {
            return String.format("Role '%s' was already granted AUTHORIZE FOR %s on %s", roleName, permissions, resource);
        }
    };

    /**
     * Returns the name of the grant operation for this {@code GrantMode}.
     * @return the name of the grant operation for this {@code GrantMode}.
     */
    public abstract String grantOperationName();

    /**
     * Returns the name of the revoke operation for this {@code GrantMode}.
     * @return the name of the revoke operation for this {@code GrantMode}.
     */
    public abstract String revokeOperationName();

    /**
     * Returns the warning message to send to the user when a revoke or unrestrict operation had not the intended effect.
     * @param roleName the role for which the operation was performed
     * @param resource the resource
     * @param permissions the permissions that should have been revoked/unrestricted
     * @return the warning message to send to the user when a revoke or unrestrict operation had not the intended effect.
     */
    public abstract String revokeWarningMessage(String roleName, IResource resource, String permissions);

    /**
     * Returns the warning message to send to the user when a grant or restrict operation had not the intended effect.
     * @param roleName the role for which the operation was performed
     * @param resource the resource
     * @param permissions the permissions that should have been granted/restricted
     * @return the warning message to send to the user when a grant or restrict operation had not the intended effect.
     */
    public abstract String grantWarningMessage(String roleName, IResource resource, String permissions);
}
