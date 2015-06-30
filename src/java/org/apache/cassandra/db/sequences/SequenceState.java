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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.sequences.SequenceName;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.utils.Pair;

/**
 * Keeps runtime state of a sequence.
 * We need a separate sequence-state class because {@link Sequence} is immutable and only <i>describing</i>
 * a sequence.
 */
public final class SequenceState
{
    private static final Logger logger = LoggerFactory.getLogger(SequenceState.class);

    private final String keyspace;
    private final ByteBuffer name;
    private final SequenceName sequenceName;

    private long minVal;
    private long maxVal;
    private long startWith;
    private long incrBy;
    private long cache;
    private long cacheLocal;
    private ConsistencyLevel clSerial;
    private ConsistencyLevel cl;

    private final Lock lock = new ReentrantLock();
    private boolean shutdown;
    /**
     * Flag whether a row in {@code system.sequence_local} exists.
     */
    private boolean peerPersisted;
    private final Range peerRange = new Range();
    private final Range vmRange = new Range();

    public SequenceState(Sequence seq)
    {
        this.keyspace = seq.keyspace;
        this.name = seq.name;
        this.sequenceName = seq.sequenceName();

        apply(seq);
    }

    private void apply(Sequence seq)
    {
        this.minVal = seq.minVal;
        this.maxVal = seq.maxVal;
        this.startWith = seq.startWith;
        this.incrBy = seq.incrBy;
        this.cache = seq.cache;
        this.cacheLocal = seq.cacheLocal;
        this.clSerial = seq.clSerial;
        this.cl = seq.cl;
    }

    public void initialize(SequencePersistence persistence)
    {
        Pair<Long, Long> nextEnd = persistence.nextLocalCached(keyspace, name);
        if (nextEnd != null)
        {
            vmRange.set(nextEnd.left, nextEnd.right);
            peerRange.set(nextEnd.right, nextEnd.right);
            peerRange.avail = false;
            persistence.removeLocalCached(keyspace, name);
            peerPersisted = false;
            logger.info("Sequence {} initialized with peer-local cache {}", sequenceName, vmRange);
        }
        else
            logger.info("Sequence {} initialized", sequenceName);
    }

    public void update(SequencePersistence persistence, Sequence seq)
    {
        assert sequenceName.equals(seq.sequenceName());

        lock.lock();
        try
        {
            assert !shutdown;

            apply(seq);

            // invalidate cached ranges

            persistence.removeLocalCached(keyspace, name);
            vmRange.reset();
            peerRange.reset();

            // TODO optimize to eventually not invalidate the cached ranges

        }
        finally
        {
            lock.unlock();
        }
    }

    public void shutdown(SequencePersistence persistence)
    {
        lock.lock();
        try
        {
            if (shutdown)
                return;

            shutdown = true;

            logger.debug("Shutting down sequence {}", sequenceName);
            if (vmRange.avail && cacheLocal > 1L)
            {
                logger.info("Persisting sequence {} peer-local cache with {}", sequenceName, vmRange);

                // update system.sequence_local with vmNext..peerEnd to not loose already allocated range
                persistence.updateLocalCached(keyspace, name, vmRange.next, peerRange.end);
            }
        }
        finally
        {
            lock.unlock();
        }
    }

    //

    long nextval(SequencePersistence persistence, long timeoutMicros) throws TimeoutException
    {
        logger.debug("Acquiring nextval for sequence {}", this);

        try
        {
            if (!lock.tryLock(timeoutMicros, TimeUnit.MICROSECONDS))
                throw new TimeoutException();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        try
        {
            assert !shutdown;

            if (vmRange.avail)
            {
                return nextvalFromVm();
            }

            if (!peerRange.avail)
            {
                // allocate a new peer-range

                logger.trace("Acquiring peer range for sequence {}", sequenceName);

                peerRange.update(persistence.nextPeerRange(keyspace, name, this, clSerial, cl), cache, maxVal, minVal);

                logger.trace("Acquired peer range {} for sequence {}", peerRange, sequenceName);
            }

            if (!vmRange.avail)
            {
                // allocate a new JVM cached range from current peer-range

                logger.trace("Updating JVM cached range for sequence {}", sequenceName);

                vmRange.pull(peerRange, cacheLocal, peerRange.end);

                peerRange.next = boundedAdd(vmRange.end, incrBy, peerRange.end, peerRange.end);

                updateLocal(persistence);
            }

            return nextvalFromVm();
        }
        finally
        {
            lock.unlock();
        }
    }

    private long nextvalFromVm()
    {
        logger.trace("Acquired local range {} for sequence {}", vmRange, sequenceName);

        long next = vmRange.pull();

        logger.trace("Acquire nextval for sequence {} returns {} (vmRange={}, peerRange={})", sequenceName, next, vmRange, peerRange);
        return next;
    }

    /**
     * Called from SequencesPersistence to increment/decrement a value.
     */
    public long peerAdder(long value)
    {

        // TODO exhausted-exception is thrown too early - e.g. INCREMENT BY 1 ... MAXVALUE 3 does only get values 1 + 2 - exhausted-exception is thrown for nextval that would return 3

        long r = value + cache * incrBy;

        if (incrBy > 0)
        {

            // positive

            if (r < value || r > maxVal)
            {
                // integer overflow
                throw new SequenceExhaustedException("Sequence " + sequenceName + " exhausted");
            }

            return r;
        }

        // negative

        if (r > value || r < minVal)
        {
            // integer overflow
            throw new SequenceExhaustedException("Sequence " + sequenceName + " exhausted");
        }

        return r;
    }

    @VisibleForTesting
    public long boundedAdd(long val, long add, long positiveMax, long negativeMin)
    {
        long n = val + add;

        if (incrBy > 0)
        {
            if (n < val || n > positiveMax)
            {
                // prevent overflow
                n = positiveMax;
            }
            else
            {
                n = Math.min(n, positiveMax);
            }
        }
        else
        {
            if (n > val || n < negativeMin)
            {
                // prevent overflow
                n = negativeMin;
            }
            else
            {
                n = Math.max(n, negativeMin);
            }
        }
        return n;
    }

    private void updateLocal(SequencePersistence persistence)
    {
        if (cacheLocal == 1L)
            return;

        if (!peerRange.avail)
        {
            logger.debug("Peer-local cached range exhausted for sequence {}", sequenceName);
            if (peerPersisted)
            {
                persistence.removeLocalCached(keyspace, name);
                peerPersisted = false;
            }
        }
        else
        {
            logger.trace("Updating peer-local cached range for sequence {} to peerRange={}", sequenceName, peerRange);
            persistence.updateLocalCached(keyspace, name, peerRange.next, peerRange.end);
            peerPersisted = true;
        }
    }

    public String toString()
    {
        return Objects.toStringHelper(this)
                      .add("seq", sequenceName)
                      .add("vmRange", vmRange)
                      .add("peerRange", peerRange)
                      .toString();
    }

    @VisibleForTesting
    public long getVmNext()
    {
        return vmRange.next;
    }

    @VisibleForTesting
    public long getVmEnd()
    {
        return vmRange.end;
    }

    public boolean isVmAvailable()
    {
        return vmRange.avail;
    }

    @VisibleForTesting
    public long getPeerNext()
    {
        return peerRange.next;
    }

    @VisibleForTesting
    public long getPeerEnd()
    {
        return peerRange.end;
    }

    @VisibleForTesting
    public boolean isPeerAvailable()
    {
        return peerRange.avail;
    }

    final class Range
    {
        private long next;
        private long end;
        private boolean avail;

        void reset()
        {
            next = end = 0L;
            avail = false;
        }

        void set(long next, long end)
        {
            this.next = next;
            this.end = end;
            this.avail = true;
        }

        void update(long next, long count, long positiveMax, long negativeMin)
        {
            this.next = next;
            this.end = boundedAdd(next, (count - 1) * incrBy, positiveMax, negativeMin);
            this.avail = true;
        }

        public String toString()
        {
            return next + ".." + end + '(' + avail + ')';
        }

        long pull()
        {
            assert avail;

            long r = next;
            if (r == end)
                avail = false;
            else
                next += incrBy;
            return r;
        }

        void pull(Range source, long count, long end)
        {
            assert source.avail;

            this.next = source.next;
            this.end = boundedAdd(this.next, (count - 1) * incrBy, end, end);
            if (this.end == end)
                source.avail = false;
            else
                source.next = this.end + incrBy;
            this.avail = true;
        }
    }
}
