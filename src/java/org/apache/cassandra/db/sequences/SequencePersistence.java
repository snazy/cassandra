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

package org.apache.cassandra.db.sequences;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.Pair;

/**
 * TODO add javadoc explaining the purpose of this class/interface/annotation
 */
public class SequencePersistence
{
    private final ParsedStatement.Prepared pstmtSelectSequenceLocal;
    private final ParsedStatement.Prepared pstmtUpdateSequenceLocal;
    private final ParsedStatement.Prepared pstmtRemoveSequenceLocal;

    private final ParsedStatement.Prepared pstmtCreateSequencePeer;
    private final ParsedStatement.Prepared pstmtSelectSequencePeer;
    private final ParsedStatement.Prepared pstmtUpdateSequencePeer;
    private final ParsedStatement.Prepared pstmtRemoveSequencePeer;

    public SequencePersistence()
    {
        QueryHandler handler = ClientState.getCQLQueryHandler();
        MD5Digest id = handler.prepare("SELECT next_val, reserved" +
                                       " FROM " + SystemKeyspace.NAME + '.' + SystemKeyspace.SEQUENCE_LOCAL +
                                       " WHERE keyspace_name =?" +
                                       " AND sequence_name =?" +
                                       " AND peer =?", QueryState.forInternalCalls(), null).statementId;
        pstmtSelectSequenceLocal = handler.getPrepared(id);
        id = handler.prepare("UPDATE " + SystemKeyspace.NAME + '.' + SystemKeyspace.SEQUENCE_LOCAL +
                             " SET next_val =?, reserved =?" +
                             " WHERE keyspace_name =?" +
                             " AND sequence_name =?" +
                             " AND peer =?", QueryState.forInternalCalls(), null).statementId;
        pstmtUpdateSequenceLocal = handler.getPrepared(id);
        id = handler.prepare("DELETE FROM " + SystemKeyspace.NAME + '.' + SystemKeyspace.SEQUENCE_LOCAL +
                             " WHERE keyspace_name =?" +
                             " AND sequence_name =?" +
                             " AND peer =?", QueryState.forInternalCalls(), null).statementId;
        pstmtRemoveSequenceLocal = handler.getPrepared(id);

        id = handler.prepare("INSERT INTO " + SystemDistributedKeyspace.NAME + '.' + SystemDistributedKeyspace.SEQ_RESERVATIONS +
                             " (keyspace_name, sequence_name, next_val, exhausted) VALUES (?, ?, ?, false)" +
                             " IF NOT EXISTS", QueryState.forInternalCalls(), null).statementId;
        pstmtCreateSequencePeer = handler.getPrepared(id);
        id = handler.prepare("SELECT next_val, exhausted" +
                             " FROM " + SystemDistributedKeyspace.NAME + '.' + SystemDistributedKeyspace.SEQ_RESERVATIONS +
                             " WHERE keyspace_name =?" +
                             " AND sequence_name =?", QueryState.forInternalCalls(), null).statementId;
        pstmtSelectSequencePeer = handler.getPrepared(id);
        id = handler.prepare("UPDATE " + SystemDistributedKeyspace.NAME + '.' + SystemDistributedKeyspace.SEQ_RESERVATIONS +
                             " SET next_val =?, exhausted =? " +
                             " WHERE keyspace_name =?" +
                             " AND sequence_name =? " +
                             " IF next_val =?", QueryState.forInternalCalls(), null).statementId;
        pstmtUpdateSequencePeer = handler.getPrepared(id);
        id = handler.prepare("DELETE FROM " + SystemDistributedKeyspace.NAME + '.' + SystemDistributedKeyspace.SEQ_RESERVATIONS +
                             " WHERE keyspace_name =?" +
                             " AND sequence_name =?", QueryState.forInternalCalls(), null).statementId;
        pstmtRemoveSequencePeer = handler.getPrepared(id);
    }

    public Pair<Long, Long> nextLocalCached(String keyspace, ByteBuffer name)
    {
        CQLStatement stmt = pstmtSelectSequenceLocal.statement;
        QueryOptions options = QueryOptions.forInternalCalls(
                                                            Arrays.asList(
                                                                         UTF8Type.instance.fromString(keyspace),
                                                                         name,
                                                                         InetAddressType.instance.decompose(FBUtilities.getBroadcastAddress())
                                                            ));
        ResultMessage result = QueryProcessor.instance.processPrepared(stmt, QueryState.forInternalCalls(), options);
        if (!(result instanceof ResultMessage.Rows))
            return null;

        ResultMessage.Rows rows = (ResultMessage.Rows) result;
        if (rows.result.isEmpty())
            return null;

        List<ByteBuffer> row = rows.result.rows.get(0);
        return Pair.create(LongType.instance.compose(row.get(0)), LongType.instance.compose(row.get(1)));
    }

    public void removeLocalCached(String keyspace, ByteBuffer name)
    {
        QueryOptions options = QueryOptions.forInternalCalls(Arrays.asList(UTF8Type.instance.fromString(keyspace),
                                                                           name,
                                                                           InetAddressType.instance.decompose(FBUtilities.getBroadcastAddress())));
        QueryProcessor.instance.processPrepared(pstmtRemoveSequenceLocal.statement, QueryState.forInternalCalls(), options);
    }

    public void updateLocalCached(String keyspace, ByteBuffer name, long nextVal, long reserved)
    {
        QueryOptions options = QueryOptions.forInternalCalls(
                                                            Arrays.asList(
                                                                         LongType.instance.decompose(nextVal),
                                                                         LongType.instance.decompose(reserved),
                                                                         UTF8Type.instance.fromString(keyspace),
                                                                         name,
                                                                         InetAddressType.instance.decompose(FBUtilities.getBroadcastAddress())
                                                            ));
        QueryProcessor.instance.processPrepared(pstmtUpdateSequenceLocal.statement, QueryState.forInternalCalls(), options);
    }

    public long nextPeerRange(String keyspace, ByteBuffer name, SequenceState sequence, ConsistencyLevel clSerial, ConsistencyLevel cl)
    {
        while (true)
        {
            // TODO this is a very expensive CAS operation

            QueryOptions options = QueryOptions.forInternalCalls(cl, Arrays.asList(UTF8Type.instance.fromString(keyspace),
                                                                                   name));
            ResultMessage result = QueryProcessor.instance.processPrepared(pstmtSelectSequencePeer.statement, QueryState.forInternalCalls(), options);
            ResultSet rows = ((ResultMessage.Rows) result).result;
            if (rows.isEmpty())
            {
                // a concurrent DROP SEQUENCE removes the row
                throw new InvalidRequestException("Sequence " + keyspace + '.' + UTF8Type.instance.compose(name) + " does not exist");
            }
            List<ByteBuffer> row = rows.rows.get(0);
            long nextVal = LongType.instance.compose(row.get(0));
            boolean exhausted = row.get(1) != null && BooleanType.instance.compose(row.get(1));
            if (exhausted)
                throw new SequenceExhaustedException("Sequence " + keyspace + '.' + UTF8Type.instance.compose(name) + " exhausted");
            long incrementedVal;
            try
            {
                incrementedVal = sequence.peerAdder(nextVal);
            }
            catch (SequenceExhaustedException exh)
            {
                incrementedVal = nextVal;
                exhausted = true;
            }

            options = QueryOptions.create(cl,
                                          Arrays.asList(
                                                       LongType.instance.decompose(incrementedVal),
                                                       BooleanType.instance.decompose(exhausted),
                                                       UTF8Type.instance.fromString(keyspace),
                                                       name,
                                                       LongType.instance.decompose(nextVal)
                                          ),
                                          false,
                                          -1,
                                          null,
                                          clSerial);
            result = QueryProcessor.instance.processPrepared(pstmtUpdateSequencePeer.statement, QueryState.forInternalCalls(), options);
            row = ((ResultMessage.Rows) result).result.rows.get(0);
            if (BooleanType.instance.compose(row.get(0)))
                return nextVal;

            try
            {
                // TODO add some better backoff
                Thread.sleep(ThreadLocalRandom.current().nextLong(200L));
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public void removeSequence(String keyspace, ByteBuffer name, ConsistencyLevel cl)
    {
        removeLocalCached(keyspace, name);

        QueryOptions options = QueryOptions.forInternalCalls(cl, Arrays.asList(UTF8Type.instance.fromString(keyspace),
                                                                               name));
        QueryProcessor.instance.processPrepared(pstmtRemoveSequencePeer.statement, QueryState.forInternalCalls(), options);
    }

    public boolean createPeer(String keyspace, ByteBuffer name, long startWith, ConsistencyLevel clSerial, ConsistencyLevel cl)
    {
        QueryOptions options = QueryOptions.create(cl,
                                                   Arrays.asList(UTF8Type.instance.fromString(keyspace),
                                                                 name,
                                                                 LongType.instance.decompose(startWith)),
                                                   false,
                                                   -1,
                                                   null,
                                                   clSerial);
        ResultMessage result = QueryProcessor.instance.processPrepared(pstmtCreateSequencePeer.statement, QueryState.forInternalCalls(), options);
        List<ByteBuffer> row = ((ResultMessage.Rows) result).result.rows.get(0);
        ByteBuffer applied = row.get(0);
        return BooleanType.instance.compose(applied);
    }
}
