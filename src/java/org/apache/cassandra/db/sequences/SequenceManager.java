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

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.sequences.SequenceName;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationListener;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Manages the runtime status of all sequences.
 */
public class SequenceManager
{
    private static final Logger logger = LoggerFactory.getLogger(SequenceManager.class);

    private static SequenceManager instance;

    private final MigrationListener migrationListener = new MigrationListener()
    {
        public void onCreateSequence(String ksName, String seqName)
        {
            KeyspaceMetadata ksm = Schema.instance.getKSMetaData(ksName);
            assert ksm != null;
            Optional<Sequence> seq = ksm.sequences.get(ByteBufferUtil.bytes(seqName));
            assert seq.isPresent();
            addInternal(seq.get());
        }

        public void onUpdateSequence(String ksName, String seqName)
        {
            KeyspaceMetadata ksm = Schema.instance.getKSMetaData(ksName);
            assert ksm != null;
            Optional<Sequence> seq = ksm.sequences.get(ByteBufferUtil.bytes(seqName));
            assert seq.isPresent();
            updateInternal(seq.get());
        }

        public void onDropSequence(String ksName, String seqName)
        {
            removeInternal(new SequenceName(ksName, seqName));
            persistence.removeLocalCached(ksName, UTF8Type.instance.fromString(seqName));
        }
    };

    public static SequenceManager getInstance()
    {
        assert instance != null;
        return instance;
    }

    public static void initialize()
    {
        if (instance != null)
            // hack for unit tests
            return;
        instance = new SequenceManager(new SequencePersistence());
        instance.setupProductive();
        logger.debug("Sequences handler initialized with production persistence");
    }

    private boolean shutdown;

    private final SequencePersistence persistence;

    private final ConcurrentMap<SequenceName, SequenceState> sequences = new ConcurrentHashMap<>();

    @VisibleForTesting
    public SequenceManager(SequencePersistence persistence)
    {
        this.persistence = persistence;
    }

    private void setupProductive()
    {
        Schema.instance.getKeyspaces().forEach((ksName) -> Schema.instance.getKSMetaData(ksName).sequences.forEach(this::addInternal));
        MigrationManager.instance.register(migrationListener);
    }

    /**
     * Creates the global part of the sequence in {@code system_distributed} keyspace.
     * Does not add the sequence to the internal structures or per-node sequence status table.
     */
    public void createSequenceGlobal(Sequence seq)
    {
        if (persistence.createPeer(seq.keyspace, seq.name, seq.startWith, seq.clSerial, seq.cl))
        {
            if (logger.isDebugEnabled())
                logger.debug("Sequence {}.{} initialized in distributed table", seq.keyspace, seq.getNameAsString());
        }
        else
            throw new InvalidRequestException("Sequence " + seq.keyspace + '.' + seq.getNameAsString() + " already exists");
    }

    /**
     * Used by {@link org.apache.cassandra.cql3.statements.DropSequenceStatement} to remove the global state of the sequence.
     */
    public void removeSequenceGlobal(Sequence seq)
    {
        persistence.removeSequence(seq.keyspace, seq.name, seq.cl);
    }

    public SequenceState find(SequenceName seqName)
    {
        return sequences.get(seqName);
    }

    /**
     * VISIBLE FOR TESTING ONLY!
     * Add given sequence to internal sequence-stage map.
     */
    @VisibleForTesting
    public void addInternal(Sequence seq)
    {
        SequenceState seqState = new SequenceState(seq);
        seqState.initialize(persistence);
        assert sequences.putIfAbsent(seq.sequenceName(), seqState) == null;
    }

    private void updateInternal(Sequence sequence)
    {
        SequenceState seqState = find(sequence.sequenceName());
        seqState.update(persistence, sequence);
    }

    private void removeInternal(SequenceName seq)
    {
        SequenceState old = sequences.remove(seq);
        if (old != null)
            old.shutdown(persistence);
    }

    public void shutdown()
    {
        logger.debug("Shutting down sequences handler");

        if (shutdown)
            return;
        shutdown = true;

        MigrationManager.instance.unregister(migrationListener);

        sequences.values().forEach((seqState) -> seqState.shutdown(persistence));
        sequences.clear();

        // needed for unit tests
        if (instance == this)
            instance = null;
    }

    public long nextval(ClientState clientState, String cfKeyspace, SequenceName seqName, long timeoutMicros)
    {
        logger.trace("Retrieving nextval for {} with timeout {}", seqName, timeoutMicros);

        assert !shutdown;

        SequenceState seq = null;
        SequenceName resolvedName = seqName;
        if (seqName.hasKeyspace())
            seq = find(seqName);
        else if (cfKeyspace != null)
        {
            resolvedName = new SequenceName(cfKeyspace, seqName.name);
            seq = find(resolvedName);
            if (seq == null && clientState != null)
            {
                resolvedName = new SequenceName(clientState.getRawKeyspace(), seqName.name);
                seq = find(resolvedName);
            }
        }

        if (seq == null)
            throw new InvalidRequestException("Sequence " + seqName + " does not exists");

        if (clientState != null)
            clientState.ensureHasPermission(Permission.EXECUTE, resolvedName);
        try
        {
            return seq.nextval(persistence, timeoutMicros);
        }
        catch (TimeoutException e)
        {
            throw new InvalidRequestException("Acquire of next value of sequence " + resolvedName + " timed out");
        }
    }
}
