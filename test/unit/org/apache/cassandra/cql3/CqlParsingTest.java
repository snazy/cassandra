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

package org.apache.cassandra.cql3;

import java.util.EnumSet;
import java.util.function.Consumer;

import org.assertj.core.api.ObjectAssert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.GrantMode;
import org.apache.cassandra.auth.LocalCassandraNetworkAuthorizer;
import org.apache.cassandra.auth.LocalCassandraRoleManager;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.StubAuthorizer;
import org.apache.cassandra.cql3.statements.RevokePermissionsStatement;
import org.apache.cassandra.exceptions.SyntaxException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

public class CqlParsingTest extends CQLTester
{
    @BeforeClass
    public static void setup()
    {
        SchemaLoader.setupAuth(new LocalCassandraRoleManager(), new PasswordAuthenticator(), new StubAuthorizer(), new LocalCassandraNetworkAuthorizer());
    }

    @Test
    public void revokeCreateOnKeyspace()
    {
        verifyQuery("REVOKE CREATE ON KEYSPACE revoke_yeah FROM revoked",
                    RevokePermissionsStatement.class,
                    a -> a.extracting(s -> s.grantee,
                                      s -> s.grantMode,
                                      s -> s.permissions,
                                      s -> s.resource)
                          .containsExactly(RoleResource.role("revoked"),
                                           GrantMode.GRANT,
                                           EnumSet.of(Permission.CREATE),
                                           DataResource.keyspace("revoke_yeah")));
    }

    @Test
    public void revokeAuthorizeForCreateOnKeyspace()
    {
        verifyQuery("REVOKE AUTHORIZE FOR CREATE ON KEYSPACE ks_name FROM the_grantee",
                    RevokePermissionsStatement.class,
                    a -> a.extracting(s -> s.grantee,
                                      s -> s.grantMode,
                                      s -> s.permissions,
                                      s -> s.resource)
                          .containsExactly(RoleResource.role("the_grantee"),
                                           GrantMode.GRANTABLE,
                                           EnumSet.of(Permission.CREATE),
                                           DataResource.keyspace("ks_name")));
    }

    @Test
    public void grantInvalidPermission()
    {
        invalidQuery("GRANT UPDATE ON KEYSPACE ks_name TO the_grantee",
                     SyntaxException.class,
                     a -> a.extracting(Throwable::getMessage).isEqualTo("Failed parsing query: [GRANT UPDATE ON KEYSPACE ks_name TO the_grantee] reason: SyntaxException line 1:6 no viable alternative at input 'UPDATE' ([GRANT] UPDATE...)")
                    );
    }

    @Test
    public void grantInvalidPermission2()
    {
        invalidQuery("GRANT SELECT, UPDATE ON KEYSPACE ks_name TO the_grantee",
                     SyntaxException.class,
                     a -> a.extracting(Throwable::getMessage).isEqualTo("Failed parsing query: [GRANT SELECT, UPDATE ON KEYSPACE ks_name TO the_grantee] reason: SyntaxException line 1:14 no viable alternative at input 'UPDATE' (GRANT SELECT, [UPDATE]...)")
                    );
    }

    @Test
    public void grantAuthorizeForInvalidPermission()
    {
        invalidQuery("GRANT AUTHORIZE FOR UPDATE ON KEYSPACE ks_name TO the_grantee",
                     SyntaxException.class,
                     a -> a.extracting(Throwable::getMessage).isEqualTo("Failed parsing query: [GRANT AUTHORIZE FOR UPDATE ON KEYSPACE ks_name TO the_grantee] reason: SyntaxException line 1:20 no viable alternative at input 'UPDATE' (GRANT AUTHORIZE FOR [UPDATE]...)")
                    );
    }

    @Test
    public void grantAuthorizeForInvalidPermission2()
    {
        invalidQuery("GRANT AUTHORIZE FOR SELECT, UPDATE ON KEYSPACE ks_name TO the_grantee",
                     SyntaxException.class,
                     a -> a.extracting(Throwable::getMessage).isEqualTo("Failed parsing query: [GRANT AUTHORIZE FOR SELECT, UPDATE ON KEYSPACE ks_name TO the_grantee] reason: SyntaxException line 1:28 no viable alternative at input 'UPDATE' (GRANT AUTHORIZE FOR SELECT, [UPDATE]...)")
                    );
    }

    <X extends Throwable> void invalidQuery(String cql,
                                            Class<X> error,
                                            Consumer<ObjectAssert<X>> checks)
    {
        ObjectAssert<X> asserts = assertThatThrownBy(() -> CQLFragmentParser.parseAny(CqlParser::query, cql, "query")).asInstanceOf(type(error));
        checks.accept(asserts);
    }

    <T> void verifyQuery(String cql,
                         Class<T> type,
                         Consumer<ObjectAssert<T>> checks)
    {
        CQLStatement.Raw stmt = CQLFragmentParser.parseAny(CqlParser::query, cql, "query");
        ObjectAssert<T> asserts = assertThat(stmt).isNotNull().asInstanceOf(type(type));
        checks.accept(asserts);
    }
}
