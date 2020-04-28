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

import java.util.concurrent.Future;

import com.google.common.util.concurrent.Futures;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

/**
 * This just extends the internal IRoleManager implementation to ensure that
 * all access to underlying tables is made via
 * QueryProcessor.executeOnceInternal/CQLStatement.executeInternal and not
 * StorageProxy so that it can be used in unit tests.
 */
public class LocalCassandraRoleManager extends CassandraRoleManager
{
    @Override
    protected ResultMessage.Rows select(SelectStatement statement, QueryOptions options)
    {
        return statement.executeLocally(QueryState.forInternalCalls(), options);
    }

    @Override
    protected UntypedResultSet process(String query, ConsistencyLevel consistencyLevel)
    {
        return QueryProcessor.executeInternal(query);
    }

    @Override
    protected Future<?> scheduleSetupTask(Runnable setupTask)
    {
        return Futures.immediateFuture(null);
    }
}
