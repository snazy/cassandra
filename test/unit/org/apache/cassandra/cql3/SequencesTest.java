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

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.sequences.SequenceDef;
import org.apache.cassandra.cql3.sequences.SequenceName;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.sequences.Sequence;
import org.apache.cassandra.db.sequences.SequenceExhaustedException;
import org.apache.cassandra.db.sequences.SequenceManager;
import org.apache.cassandra.db.sequences.SequencePersistence;
import org.apache.cassandra.db.sequences.SequenceState;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.LegacySchemaTables;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for sequences.
 * Contains multiple categories of tests:
 * <ol>
 * <li>tests that test internal methods,</li>
 * <li>tests that use a mocked storage implementation,</li>
 * <li>tests that test the production persistence,</li>
 * <li>tests that use the production implementation,</li>
 * <li>tests that use CQL to test sequences.</li>
 * </ol>
 */
public class SequencesTest extends CQLTester
{
    @BeforeClass
    public static void setup()
    {
        requireNetwork();

        QueryProcessor.process("ALTER KEYSPACE system_distributed WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }", ConsistencyLevel.ONE);
    }

    @Test
    public void testSequenceDefs()
    {
        // CREATE SEQUENCE with no explicit parameters
        SequenceDef def = createDefinition(new SequenceName("foo", "bar"),
                                                  null, // incrBy
                                                  null, // minVal
                                                  null, // maxVal
                                                  null, // startWith
                                                  null, // cache
                                                  null // cacheLocal
        );
        assertEquals(1L, def.getIncrBy());
        assertEquals(1L, def.getMinVal());
        assertEquals(Long.MAX_VALUE, def.getMaxVal());
        assertEquals(1L, def.getStartWith());
        assertEquals(1L, def.getCache());
        assertEquals(1L, def.getCacheLocal());


        // CREATE SEQUENCE with positive INCREMENT BY
        def = createDefinition(new SequenceName("foo", "bar"),
                               "3", // incrBy
                               null, // minVal
                               null, // maxVal
                               null, // startWith
                               null, // cache
                               null // cacheLocal
        );
        assertEquals(3L, def.getIncrBy());
        assertEquals(1L, def.getMinVal());
        assertEquals(Long.MAX_VALUE, def.getMaxVal());
        assertEquals(1L, def.getStartWith());
        assertEquals(1L, def.getCache());
        assertEquals(1L, def.getCacheLocal());


        // CREATE SEQUENCE with negative INCREMENT BY
        def = createDefinition(new SequenceName("foo", "bar"),
                               "-3", // incrBy
                               null, // minVal
                               null, // maxVal
                               null, // startWith
                               null, // cache
                               null // cacheLocal
        );
        assertEquals(-3L, def.getIncrBy());
        assertEquals(Long.MIN_VALUE, def.getMinVal());
        assertEquals(-1L, def.getMaxVal());
        assertEquals(-1L, def.getStartWith());
        assertEquals(1L, def.getCache());
        assertEquals(1L, def.getCacheLocal());


        // CREATE SEQUENCE with negative INCREMENT BY
        def = createDefinition(new SequenceName("foo", "bar"),
                               null, // incrBy
                               null, // minVal
                               null, // maxVal
                               "10", // startWith
                               null, // cache
                               null // cacheLocal
        );
        assertEquals(1, def.getIncrBy());
        assertEquals(1, def.getMinVal());
        assertEquals(Long.MAX_VALUE, def.getMaxVal());
        assertEquals(10, def.getStartWith());
        assertEquals(1L, def.getCache());
        assertEquals(1L, def.getCacheLocal());

        // biggest possible range
        def = createDefinition(new SequenceName("foo", "bar"),
                               null, // incrBy
                               Long.toString(Long.MIN_VALUE), // minVal
                               Long.toString(Long.MAX_VALUE), // maxVal
                               null, // startWith
                               null, // cache
                               null // cacheLocal
        );
        assertEquals(1L, def.getIncrBy());
        assertEquals(Long.MIN_VALUE, def.getMinVal());
        assertEquals(Long.MAX_VALUE, def.getMaxVal());
        assertEquals(Long.MIN_VALUE, def.getStartWith());
        assertEquals(1L, def.getCache());
        assertEquals(1L, def.getCacheLocal());

        def = createDefinition(new SequenceName("foo", "bar"),
                               "-1", // incrBy
                               Long.toString(Long.MIN_VALUE), // minVal
                               Long.toString(Long.MAX_VALUE), // maxVal
                               null, // startWith
                               null, // cache
                               null // cacheLocal
        );
        assertEquals(-1L, def.getIncrBy());
        assertEquals(Long.MIN_VALUE, def.getMinVal());
        assertEquals(Long.MAX_VALUE, def.getMaxVal());
        assertEquals(Long.MAX_VALUE, def.getStartWith());
        assertEquals(1L, def.getCache());
        assertEquals(1L, def.getCacheLocal());

        // invalid definitions

        // maxVal < minVal
        invalidDefinition(null, "5", "3", null, null, null);
        // maxVal < minVal
        invalidDefinition("-1", "-3", "-5", null, null, null);
        // cache < cacheLocal
        invalidDefinition(null, null, null, null, "10", "100");
        // cache < cacheLocal
        invalidDefinition("2", null, null, null, null, "3");
        // startWith out of range
        invalidDefinition("-3", null, null, "10", null, null);
        // startWith out of range
        invalidDefinition("3", null, null, "-10", null, null);
        // cache too big
        invalidDefinition("3", "-10", "10", "-10", "8", null);
        invalidDefinition("3", "-10", "10", "-10", "8", "7");
    }

    private static SequenceDef createDefinition(SequenceName name, String incrBy, String minVal, String maxVal, String startWith, String cache, String cacheLocal)
    {
        return new SequenceDef(name,
                                      incrBy == null ? null : Constants.Literal.integer(incrBy),
                                      minVal == null ? null : Constants.Literal.integer(minVal),
                                      maxVal == null ? null : Constants.Literal.integer(maxVal),
                                      startWith == null ? null : Constants.Literal.integer(startWith),
                                      cache == null ? null : Constants.Literal.integer(cache),
                                      cacheLocal == null ? null : Constants.Literal.integer(cacheLocal),
                                      null,
                                      null
        );
    }

    private static void invalidDefinition(String incrBy, String minVal, String maxVal, String startWith, String cache, String cacheLocal)
    {
        try
        {
            createDefinition(new SequenceName("foo", "bar"), incrBy, minVal, maxVal, startWith, cache, cacheLocal);
        }
        catch (InvalidRequestException e)
        {
            // fine
            return;
        }
        fail();
    }

    @Test(expected = InvalidRequestException.class)
    public void testDuplicateSequence()
    {
        SequenceManager sequences = new SequenceManager(new MockedSequencesPersistence());

        SequenceName name = new SequenceName("foo", "one");
        createAndAddSequence(sequences, createDefinition(name, null, null, null, null, null, null));
        createAndAddSequence(sequences, createDefinition(name, null, null, null, null, null, null));
    }

//    @Test(expected = AssertionError.class)
//    public void testNonExistingSequenceUpdate()
//    {
//        SequenceManager sequences = new SequenceManager(new MockedSequencesPersistence());
//
//        SequenceName name = new SequenceName("foo", "one");
//        sequences.updateSequence(createDefinition(name, null, null, null, null, null, null));
//    }
//
//    @Test(expected = AssertionError.class)
//    public void testNonExistingSequenceRemove()
//    {
//        SequenceManager sequences = new SequenceManager(new MockedSequencesPersistence());
//
//        SequenceName name = new SequenceName("foo", "one");
//        sequences.removeSequence(name);
//    }

    //
    // Test against mock persistence
    //

    @Test
    public void testBasic()
    {
        MockedSequencesPersistence persistence = new MockedSequencesPersistence();
        SequenceManager sequences = new SequenceManager(persistence);

        // ascending

        SequenceName name = new SequenceName("foo", "ascending");
        createAndAddSequence(sequences, createDefinition(name, null, null, null, null, null, null));

        assertEquals(1L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 1, 1, false, 1, 1, false);
        assertEquals(2L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 2, 2, false, 2, 2, false);
        assertEquals(3L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 3, 3, false, 3, 3, false);

        // descending

        name = new SequenceName("foo", "descending ");
        createAndAddSequence(sequences, createDefinition(name, "-1", null, null, null, null, null));

        assertEquals(-1L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -1, -1, false, -1, -1, false);
        assertEquals(-2L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -2, -2, false, -2, -2, false);
        assertEquals(-3L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -3, -3, false, -3, -3, false);
    }

    @Test
    public void testBasicOver0()
    {
        for (int incr = 1; incr <= 5; incr++)
        {
            MockedSequencesPersistence persistence = new MockedSequencesPersistence();
            SequenceManager sequences = new SequenceManager(persistence);

            // ascending

            SequenceName name = new SequenceName("foo", "ascending");
            createAndAddSequence(sequences, createDefinition(name, Integer.toString(incr), "-10", "10", null, null, null));

            for (int i = -10; i <= 10 - (incr - 1); i += incr)
                assertEquals("incr=" + incr, i, sequences.nextval(null, null, name, Long.MAX_VALUE));

            // descending

            name = new SequenceName("foo", "descending ");
            createAndAddSequence(sequences, createDefinition(name, Integer.toString(-incr), "-10", "10", null, null, null));

            for (int i = 10; i >= -10 + (incr - 1); i -= incr)
                assertEquals("incr=" + incr, i, sequences.nextval(null, null, name, Long.MAX_VALUE));
        }
    }

    @Test
    public void testBasic5()
    {
        MockedSequencesPersistence persistence = new MockedSequencesPersistence();
        SequenceManager sequences = new SequenceManager(persistence);

        // ascending

        SequenceName name = new SequenceName("foo", "ascending");
        createAndAddSequence(sequences, createDefinition(name, "5", null, null, null, null, null));

        assertEquals(1L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 1, 1, false, 1, 1, false);
        assertEquals(6L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 6, 6, false, 6, 6, false);
        assertEquals(11L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 11, 11, false, 11, 11, false);

        // descending

        name = new SequenceName("foo", "descending ");
        createAndAddSequence(sequences, createDefinition(name, "-5", null, null, null, null, null));

        assertEquals(-1L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -1, -1, false, -1, -1, false);
        assertEquals(-6L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -6, -6, false, -6, -6, false);
        assertEquals(-11L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -11, -11, false, -11, -11, false);
    }

    @Test
    public void testBiggestPossible()
    {
        MockedSequencesPersistence persistence = new MockedSequencesPersistence();
        SequenceManager sequences = new SequenceManager(persistence);

        // ascending

        SequenceName name = new SequenceName("foo", "ascending");
        createAndAddSequence(sequences, createDefinition(name, null, Long.toString(Long.MIN_VALUE), Long.toString(Long.MAX_VALUE), null, null, null));

        long v = Long.MIN_VALUE;
        assertEquals(v, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, v, v, false, v, v, false);
        v++;
        assertEquals(v, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, v, v, false, v, v, false);
        v++;
        assertEquals(v, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, v, v, false, v, v, false);

        // descending

        name = new SequenceName("foo", "descending ");
        createAndAddSequence(sequences, createDefinition(name, "-1", Long.toString(Long.MIN_VALUE), Long.toString(Long.MAX_VALUE), null, null, null));

        v = Long.MAX_VALUE;
        assertEquals(v, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, v, v, false, v, v, false);
        v--;
        assertEquals(v, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, v, v, false, v, v, false);
        v--;
        assertEquals(v, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, v, v, false, v, v, false);
    }

    @Test
    public void testExhausted()
    {
        MockedSequencesPersistence persistence = new MockedSequencesPersistence();
        SequenceManager sequences = new SequenceManager(persistence);

        // ascending

        SequenceName name = new SequenceName("foo", "ascending");
        createAndAddSequence(sequences, createDefinition(name, null, null, "3", null, null, null));

        assertEquals(1L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 1, 1, false, 1, 1, false);
        assertEquals(2L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 2, 2, false, 2, 2, false);
        assertEquals(3L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 3, 3, false, 3, 3, false);
        try
        {
            sequences.nextval(null, null, name, Long.MAX_VALUE);
            fail();
        }
        catch (CassandraException e)
        {
            // OK
        }
        assertSequenceState(sequences, name, 3, 3, false, 3, 3, false);

        // descending

        name = new SequenceName("foo", "descending ");
        createAndAddSequence(sequences, createDefinition(name, "-1", "-3", null, null, null, null));

        assertEquals(-1L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -1, -1, false, -1, -1, false);
        assertEquals(-2L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -2, -2, false, -2, -2, false);
        assertEquals(-3L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -3, -3, false, -3, -3, false);
        try
        {
            sequences.nextval(null, null, name, Long.MAX_VALUE);
            fail();
        }
        catch (CassandraException e)
        {
            // OK
        }
        assertSequenceState(sequences, name, -3, -3, false, -3, -3, false);
    }

    @Test
    public void testCachedExhausted()
    {
        MockedSequencesPersistence persistence = new MockedSequencesPersistence();
        SequenceManager sequences = new SequenceManager(persistence);

        // ascending

        SequenceName name = new SequenceName("foo", "ascending");
        createAndAddSequence(sequences, createDefinition(name, null, null, "3", null, "3", "3"));

        assertEquals(1L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 2, 3, true, 3, 3, false);
        assertEquals(2L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 3, 3, true, 3, 3, false);
        assertEquals(3L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 3, 3, false, 3, 3, false);
        try
        {
            sequences.nextval(null, null, name, Long.MAX_VALUE);
            fail();
        }
        catch (CassandraException e)
        {
            // OK
        }

        // descending

        name = new SequenceName("foo", "descending ");
        createAndAddSequence(sequences, createDefinition(name, "-1", "-3", null, null, "3", "3"));

        assertEquals(-1L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -2, -3, true, -3, -3, false);
        assertEquals(-2L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -3, -3, true, -3, -3, false);
        assertEquals(-3L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -3, -3, false, -3, -3, false);
        try
        {
            sequences.nextval(null, null, name, Long.MAX_VALUE);
            fail();
        }
        catch (CassandraException e)
        {
            // OK
        }
    }

    @Test
    public void testExhaustedLimits()
    {
        MockedSequencesPersistence persistence = new MockedSequencesPersistence();
        SequenceManager sequences = new SequenceManager(persistence);

        // ascending

        SequenceName name = new SequenceName("foo", "ascending");
        long v = Long.MAX_VALUE - 2;
        createAndAddSequence(sequences, createDefinition(name, null, null, null, Long.toString(v), null, null));

        assertEquals(v, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, v, v, false, v, v, false);
        v++;
        assertEquals(v, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, v, v, false, v, v, false);
        v++;
        assertEquals(v, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, v, v, false, v, v, false);
        try
        {
            sequences.nextval(null, null, name, Long.MAX_VALUE);
            fail();
        }
        catch (CassandraException e)
        {
            // OK
        }
        assertSequenceState(sequences, name, v, v, false, v, v, false);
        assertEquals(Long.MAX_VALUE, v);

        // descending

        name = new SequenceName("foo", "descending ");
        v = Long.MIN_VALUE + 2;
        createAndAddSequence(sequences, createDefinition(name, "-1", null, null, Long.toString(v), null, null));

        assertEquals(v, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, v, v, false, v, v, false);
        v--;
        assertEquals(v, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, v, v, false, v, v, false);
        v--;
        assertEquals(v, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, v, v, false, v, v, false);
        try
        {
            sequences.nextval(null, null, name, Long.MAX_VALUE);
            fail();
        }
        catch (CassandraException e)
        {
            // OK
        }
        assertSequenceState(sequences, name, v, v, false, v, v, false);
        assertEquals(Long.MIN_VALUE, v);
    }

    @Test
    public void testCachedExhaustedLimits()
    {
        MockedSequencesPersistence persistence = new MockedSequencesPersistence();
        SequenceManager sequences = new SequenceManager(persistence);

        // ascending

        SequenceName name = new SequenceName("foo", "ascending");
        long v = Long.MAX_VALUE - 2;
        createAndAddSequence(sequences, createDefinition(name, null, null, null, Long.toString(v), "3", "3"));

        assertEquals(v, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, v + 1, v + 2, true, Long.MAX_VALUE, Long.MAX_VALUE, false);
        v++;
        assertEquals(v, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, Long.MAX_VALUE, Long.MAX_VALUE, true, Long.MAX_VALUE, Long.MAX_VALUE, false);
        v++;
        assertEquals(v, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, Long.MAX_VALUE, Long.MAX_VALUE, false, Long.MAX_VALUE, Long.MAX_VALUE, false);
        try
        {
            sequences.nextval(null, null, name, Long.MAX_VALUE);
            fail();
        }
        catch (CassandraException e)
        {
            // OK
        }
        assertEquals(Long.MAX_VALUE, v);

        // descending

        name = new SequenceName("foo", "descending ");
        v = Long.MIN_VALUE + 2;
        createAndAddSequence(sequences, createDefinition(name, "-1", null, null, Long.toString(v), "3", "3"));

        assertEquals(v, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, v - 1, Long.MIN_VALUE, true, Long.MIN_VALUE, Long.MIN_VALUE, false);
        v--;
        assertEquals(v, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, Long.MIN_VALUE, Long.MIN_VALUE, true, Long.MIN_VALUE, Long.MIN_VALUE, false);
        v--;
        assertEquals(v, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, Long.MIN_VALUE, Long.MIN_VALUE, false, Long.MIN_VALUE, Long.MIN_VALUE, false);
        try
        {
            sequences.nextval(null, null, name, Long.MAX_VALUE);
            fail();
        }
        catch (CassandraException e)
        {
            // OK
        }
        assertEquals(Long.MIN_VALUE, v);
    }

    @Test
    public void testBoundedAdd()
    {
        MockedSequencesPersistence persistence = new MockedSequencesPersistence();
        SequenceManager sequences = new SequenceManager(persistence);

        // ascending

        SequenceName name = new SequenceName("foo", "ascending");
        createAndAddSequence(sequences, createDefinition(name, null, null, null, null, null, null));

        SequenceState seq = sequences.find(name);
        assertEquals(Long.MAX_VALUE, seq.boundedAdd(Long.MAX_VALUE, 1, Long.MAX_VALUE, Long.MIN_VALUE));
        assertEquals(10, seq.boundedAdd(10, 1, 10, Long.MIN_VALUE));

        // descending

        name = new SequenceName("foo", "descending");
        createAndAddSequence(sequences, createDefinition(name, "-1", null, null, null, null, null));

        seq = sequences.find(name);
        assertEquals(Long.MIN_VALUE, seq.boundedAdd(Long.MIN_VALUE, 1, Long.MAX_VALUE, Long.MIN_VALUE));
        assertEquals(-10, seq.boundedAdd(-10, 1, Long.MAX_VALUE, -10));
    }

    @Test
    public void testPeerCache()
    {
        MockedSequencesPersistence persistence = new MockedSequencesPersistence();
        SequenceManager sequences = new SequenceManager(persistence);

        // ascending

        SequenceName name = new SequenceName("foo", "ascending");
        createAndAddSequence(sequences, createDefinition(name, null, null, null, null, "10", "1"));

        assertEquals(1L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 1, 1, false, 2, 10, true);
        assertEquals(2L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 2, 2, false, 3, 10, true);
        assertEquals(3L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 3, 3, false, 4, 10, true);
        assertEquals(4L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 4, 4, false, 5, 10, true);
        assertEquals(5L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 5, 5, false, 6, 10, true);
        assertEquals(6L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 6, 6, false, 7, 10, true);
        assertEquals(7L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 7, 7, false, 8, 10, true);
        assertEquals(8L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 8, 8, false, 9, 10, true);
        assertEquals(9L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 9, 9, false, 10, 10, true);
        assertEquals(10L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 10, 10, false, 10, 10, false);
        assertEquals(11L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 11, 11, false, 12, 20, true);
        assertEquals(12L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 12, 12, false, 13, 20, true);

        // descending

        name = new SequenceName("foo", "descending");
        createAndAddSequence(sequences, createDefinition(name, "-1", null, null, null, "10", "1"));

        assertEquals(-1L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -1, -1, false, -2, -10, true);
        assertEquals(-2L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -2, -2, false, -3, -10, true);
        assertEquals(-3L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -3, -3, false, -4, -10, true);
        assertEquals(-4L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -4, -4, false, -5, -10, true);
        assertEquals(-5L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -5, -5, false, -6, -10, true);
        assertEquals(-6L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -6, -6, false, -7, -10, true);
        assertEquals(-7L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -7, -7, false, -8, -10, true);
        assertEquals(-8L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -8, -8, false, -9, -10, true);
        assertEquals(-9L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -9, -9, false, -10, -10, true);
        assertEquals(-10L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -10, -10, false, -10, -10, false);
        assertEquals(-11L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -11, -11, false, -12, -20, true);
        assertEquals(-12L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -12, -12, false, -13, -20, true);
    }

    @Test
    public void testVmAndPeerCache()
    {
        MockedSequencesPersistence persistence = new MockedSequencesPersistence();
        SequenceManager sequences = new SequenceManager(persistence);

        // ascending

        SequenceName name = new SequenceName("foo", "ascending");
        createAndAddSequence(sequences, createDefinition(name, null, null, null, null, "10", "10"));

        assertEquals(1L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 2, 10, true, 10, 10, false);
        assertEquals(2L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 3, 10, true, 10, 10, false);
        assertEquals(3L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 4, 10, true, 10, 10, false);
        assertEquals(4L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 5, 10, true, 10, 10, false);
        assertEquals(5L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 6, 10, true, 10, 10, false);
        assertEquals(6L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 7, 10, true, 10, 10, false);
        assertEquals(7L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 8, 10, true, 10, 10, false);
        assertEquals(8L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 9, 10, true, 10, 10, false);
        assertEquals(9L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 10, 10, true, 10, 10, false);
        assertEquals(10L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 10, 10, false, 10, 10, false);
        assertEquals(11L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 12, 20, true, 20, 20, false);
        assertEquals(12L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 13, 20, true, 20, 20, false);

        // descending

        name = new SequenceName("foo", "descending");
        createAndAddSequence(sequences, createDefinition(name, "-1", null, null, null, "10", "10"));

        assertEquals(-1L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -2, -10, true, -10, -10, false);
        assertEquals(-2L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -3, -10, true, -10, -10, false);
        assertEquals(-3L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -4, -10, true, -10, -10, false);
        assertEquals(-4L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -5, -10, true, -10, -10, false);
        assertEquals(-5L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -6, -10, true, -10, -10, false);
        assertEquals(-6L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -7, -10, true, -10, -10, false);
        assertEquals(-7L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -8, -10, true, -10, -10, false);
        assertEquals(-8L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -9, -10, true, -10, -10, false);
        assertEquals(-9L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -10, -10, true, -10, -10, false);
        assertEquals(-10L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -10, -10, false, -10, -10, false);
        assertEquals(-11L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -12, -20, true, -20, -20, false);
        assertEquals(-12L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -13, -20, true, -20, -20, false);
    }

    @Test
    public void testSmallVmAndPeerCache()
    {
        MockedSequencesPersistence persistence = new MockedSequencesPersistence();
        SequenceManager sequences = new SequenceManager(persistence);

        // ascending

        SequenceName name = new SequenceName("foo", "ascending");
        createAndAddSequence(sequences, createDefinition(name, null, null, null, null, "10", "5"));

        assertEquals(1L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 2, 5, true, 6, 10, true);
        assertEquals(2L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 3, 5, true, 6, 10, true);
        assertEquals(3L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 4, 5, true, 6, 10, true);
        assertEquals(4L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 5, 5, true, 6, 10, true);
        assertEquals(5L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 5, 5, false, 6, 10, true);
        assertEquals(6L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 7, 10, true, 10, 10, false);
        assertEquals(7L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 8, 10, true, 10, 10, false);
        assertEquals(8L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 9, 10, true, 10, 10, false);
        assertEquals(9L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 10, 10, true, 10, 10, false);
        assertEquals(10L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 10, 10, false, 10, 10, false);
        assertEquals(11L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 12, 15, true, 16, 20, true);
        assertEquals(12L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, 13, 15, true, 16, 20, true);

        // descending

        name = new SequenceName("foo", "descending");
        createAndAddSequence(sequences, createDefinition(name, "-1", null, null, null, "10", "5"));

        assertEquals(-1L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -2, -5, true, -6, -10, true);
        assertEquals(-2L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -3, -5, true, -6, -10, true);
        assertEquals(-3L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -4, -5, true, -6, -10, true);
        assertEquals(-4L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -5, -5, true, -6, -10, true);
        assertEquals(-5L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -5, -5, false, -6, -10, true);
        assertEquals(-6L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -7, -10, true, -10, -10, false);
        assertEquals(-7L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -8, -10, true, -10, -10, false);
        assertEquals(-8L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -9, -10, true, -10, -10, false);
        assertEquals(-9L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -10, -10, true, -10, -10, false);
        assertEquals(-10L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -10, -10, false, -10, -10, false);
        assertEquals(-11L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -12, -15, true, -16, -20, true);
        assertEquals(-12L, sequences.nextval(null, null, name, Long.MAX_VALUE));
        assertSequenceState(sequences, name, -13, -15, true, -16, -20, true);
    }

    //
    // Test production persistence
    //

    @Test
    public void testProductionPersistence() throws Throwable
    {
        SequencePersistence persistence = new SequencePersistence();
        SequenceName name = new SequenceName("foo", "seq001");
        Sequence seq = new Sequence(createDefinition(name, null, null, null, null, null, null));

        assertTrue(persistence.createPeer(seq.keyspace, seq.name, 1, ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM));
        assertFalse(persistence.createPeer(seq.keyspace, seq.name, 1, ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM));

        assertNull(persistence.nextLocalCached(seq.keyspace, seq.name));
        persistence.updateLocalCached(seq.keyspace, seq.name, 5, 10);
        assertEquals(Pair.create(5L, 10L), persistence.nextLocalCached(seq.keyspace, seq.name));
        persistence.updateLocalCached(seq.keyspace, seq.name, 5, 10);
        assertEquals(Pair.create(5L, 10L), persistence.nextLocalCached(seq.keyspace, seq.name));
        persistence.removeLocalCached(seq.keyspace, seq.name);
        assertNull(persistence.nextLocalCached(seq.keyspace, seq.name));

        SequenceState seqState = new SequenceState(seq);

        persistence.removeSequence(seq.keyspace, seq.name, ConsistencyLevel.QUORUM);
        try
        {
            persistence.nextPeerRange(seq.keyspace, seq.name, seqState, ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);
            fail();
        }
        catch (InvalidRequestException e)
        {
            // OK
        }
        assertNull(persistence.nextLocalCached(seq.keyspace, seq.name));
    }

    //
    // Test against production persistence
    //

    @Test
    public void testWithProductionPersistence()
    {
        for (int incr = 1; incr <= 5; incr++)
        {
            SequenceManager sequences = SequenceManager.getInstance();

            // ascending

            SequenceName name = new SequenceName("foo", "seq002-" + incr);
            createAndAddSequence(sequences, createDefinition(name, Integer.toString(incr), "-10", "10", null, null, null));

            for (int i = -10; i <= 10 - (incr - 1); i += incr)
                assertEquals("incr=" + incr, i, sequences.nextval(null, null, name, Long.MAX_VALUE));

            // descending

            name = new SequenceName("foo", "seq003-" + incr);
            createAndAddSequence(sequences, createDefinition(name, Integer.toString(-incr), "-10", "10", null, null, null));

            for (int i = 10; i >= -10 + (incr - 1); i -= incr)
                assertEquals("incr=" + incr, i, sequences.nextval(null, null, name, Long.MAX_VALUE));
        }
    }

    //
    // Test via CQL
    //

    @Test
    public void testCqlBasic() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY)");
        execute("INSERT INTO %s (pk) VALUES (42)");

        String seq = createSequence("CREATE SEQUENCE %s");

        assertRows(execute("SELECT minval,maxval,start_with,incr_by,cache,cache_local FROM system.schema_sequences WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(1L, Long.MAX_VALUE, 1L, 1L, 1L, 1L));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(1L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(1L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(2L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(2L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(3L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(3L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(4L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(4L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(5L, false));

        execute("DROP SEQUENCE " + KEYSPACE + '.' + seq);
        assertRows(execute("SELECT minval,maxval,start_with,incr_by,cache,cache_local FROM system.schema_sequences WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq));
    }

    @Test
    public void testCqlExhaust() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY)");
        execute("INSERT INTO %s (pk) VALUES (42)");

        String seq = createSequence("CREATE SEQUENCE %s MAXVALUE 3");

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(1L, 42));
        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(2L, 42));
        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(3L, 42));
        try
        {
            execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s");
            fail();
        }
        catch (SequenceExhaustedException ex)
        {
            // OK
        }

        execute("DROP SEQUENCE " + KEYSPACE + '.' + seq);
        assertRows(execute("SELECT minval,maxval,start_with,incr_by,cache,cache_local FROM system.schema_sequences WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq));
    }

    @Test
    public void testCqlAfterRestart() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY)");
        execute("INSERT INTO %s (pk) VALUES (42)");

        String seq = createSequence("CREATE SEQUENCE %s MAXVALUE 3");

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(1L, 42));
        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(2L, 42));

        // shutdown Sequences
        SequenceManager.getInstance().shutdown();

        // restart Sequences
        LegacySchemaTables.readSchemaFromSystemTables();
        SequenceManager.initialize();

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(3L, 42));
        try
        {
            execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s");
            fail();
        }
        catch (SequenceExhaustedException ex)
        {
            // OK
        }

        execute("DROP SEQUENCE " + KEYSPACE + '.' + seq);
        assertRows(execute("SELECT minval,maxval,start_with,incr_by,cache,cache_local FROM system.schema_sequences WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq));
    }

    @Test
    public void testCqlCache() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY)");
        execute("INSERT INTO %s (pk) VALUES (42)");

        String seq = createSequence("CREATE SEQUENCE %s CACHE 10 CACHE LOCAL 5");

        assertRows(execute("SELECT minval,maxval,start_with,incr_by,cache,cache_local FROM system.schema_sequences WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(1L, Long.MAX_VALUE, 1L, 1L, 10L, 5L));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(1L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(1L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()),
                   row(6L, 10L));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(2L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()),
                   row(6L, 10L));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(3L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()),
                   row(6L, 10L));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(4L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()),
                   row(6L, 10L));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(5L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()),
                   row(6L, 10L));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(6L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(7L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(8L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(9L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(10L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(11L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()),
                   row(16L, 20L));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(21L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(12L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()),
                   row(16L, 20L));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(21L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(13L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()),
                   row(16L, 20L));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(21L, false));

        execute("DROP SEQUENCE " + KEYSPACE + '.' + seq);
        assertRows(execute("SELECT minval,maxval,start_with,incr_by,cache,cache_local FROM system.schema_sequences WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq));
    }

    @Test
    public void testCqlCacheAfterRestart_10_5() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY)");
        execute("INSERT INTO %s (pk) VALUES (42)");

        String seq = createSequence("CREATE SEQUENCE %s CACHE 10 CACHE LOCAL 5");

        assertRows(execute("SELECT minval,maxval,start_with,incr_by,cache,cache_local FROM system.schema_sequences WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(1L, Long.MAX_VALUE, 1L, 1L, 10L, 5L));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(1L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(1L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()),
                   row(6L, 10L));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(2L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()),
                   row(6L, 10L));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(3L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()),
                   row(6L, 10L));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        // shutdown Sequences
        SequenceManager.getInstance().shutdown();

        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()),
                   row(4L, 10L));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        // restart Sequences
        SequenceManager.initialize();
        LegacySchemaTables.readSchemaFromSystemTables();

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(4L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(5L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(6L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        // shutdown Sequences
        SequenceManager.getInstance().shutdown();

        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()),
                   row(7L, 10L));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        // restart Sequences
        SequenceManager.initialize();
        LegacySchemaTables.readSchemaFromSystemTables();

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(7L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(8L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(9L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(10L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(11L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()),
                   row(16L, 20L));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(21L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(12L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()),
                   row(16L, 20L));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(21L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(13L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()),
                   row(16L, 20L));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(21L, false));

        execute("DROP SEQUENCE " + KEYSPACE + '.' + seq);
        assertRows(execute("SELECT minval,maxval,start_with,incr_by,cache,cache_local FROM system.schema_sequences WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq));
    }

    @Test
    public void testCqlCacheAfterRestart_10_10() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY)");
        execute("INSERT INTO %s (pk) VALUES (42)");

        String seq = createSequence("CREATE SEQUENCE %s CACHE 10 CACHE LOCAL 10");

        assertRows(execute("SELECT minval,maxval,start_with,incr_by,cache,cache_local FROM system.schema_sequences WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(1L, Long.MAX_VALUE, 1L, 1L, 10L, 10L));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(1L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(1L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(2L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(3L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        // shutdown Sequences
        SequenceManager.getInstance().shutdown();

        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()),
                   row(4L, 10L));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        // restart Sequences
        SequenceManager.initialize();
        LegacySchemaTables.readSchemaFromSystemTables();

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(4L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(5L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(6L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        // shutdown Sequences
        SequenceManager.getInstance().shutdown();

        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()),
                   row(7L, 10L));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        // restart Sequences
        SequenceManager.initialize();
        LegacySchemaTables.readSchemaFromSystemTables();

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(7L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(8L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(9L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(10L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(11L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(11L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(21L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(12L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(21L, false));

        assertRows(execute("SELECT nextval(" + KEYSPACE + '.' + seq + "), pk FROM %s"),
                   row(13L, 42));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq),
                   row(21L, false));

        execute("DROP SEQUENCE " + KEYSPACE + '.' + seq);
        assertRows(execute("SELECT minval,maxval,start_with,incr_by,cache,cache_local FROM system.schema_sequences WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq));
        assertRows(execute("SELECT next_val,reserved FROM system.sequence_local WHERE keyspace_name=? AND sequence_name=? AND peer=?", KEYSPACE, seq, FBUtilities.getBroadcastAddress()));
        assertRows(execute("SELECT next_val,exhausted FROM system_distributed.seq_reservations WHERE keyspace_name=? AND sequence_name=?", KEYSPACE, seq));
    }

    //
    // Auth tests
    //

    // TODO add authorization tests (see UFAuthTest)

    //
    // Helper methods
    //

    private static void createAndAddSequence(SequenceManager sequences, SequenceDef definition)
    {
        Sequence seq = new Sequence(definition);
        sequences.createSequenceGlobal(seq);
        sequences.addInternal(seq);
    }

    private static void assertSequenceState(SequenceManager sequences, SequenceName name, long vmNext, long vmEnd, boolean vmAvailable, long peerNext, long peerEnd, boolean peerAvailable)
    {
        SequenceState seq = sequences.find(name);
        assertEquals("vmNext mismatch " + seq, vmNext, seq.getVmNext());
        assertEquals("vmEnd mismatch " + seq, vmEnd, seq.getVmEnd());
        assertEquals("vmAvailable mismatch " + seq, vmAvailable, seq.isVmAvailable());
        assertEquals("peerNext mismatch " + seq, peerNext, seq.getPeerNext());
        assertEquals("peerEnd mismatch " + seq, peerEnd, seq.getPeerEnd());
        assertEquals("peerAvailable mismatch " + seq, peerAvailable, seq.isPeerAvailable());
    }

    private static class MockedSequencesPersistence extends SequencePersistence
    {
        final ConcurrentMap<ByteBuffer, Pair<Long, Long>> localCached = new ConcurrentHashMap<>();
        final ConcurrentMap<ByteBuffer, Pair<Long, Boolean>> peerCached = new ConcurrentHashMap<>();

        @Override
        public Pair<Long, Long> nextLocalCached(String keyspace, ByteBuffer name)
        {
            return localCached.get(name);
        }

        @Override
        public void removeLocalCached(String keyspace, ByteBuffer name)
        {
            localCached.remove(name);
        }

        @Override
        public void updateLocalCached(String keyspace, ByteBuffer name, long nextVal, long reserved)
        {
            localCached.put(name, Pair.create(nextVal, reserved));
        }

        @Override
        public long nextPeerRange(String keyspace, ByteBuffer name, SequenceState sequence, ConsistencyLevel clSerial, ConsistencyLevel cl)
        {
            while (true)
            {
                Pair<Long, Boolean> current = peerCached.get(name);
                if (current.right)
                    throw new SequenceExhaustedException("Sequence " + name + " exhausted");
                try
                {
                    Long next = sequence.peerAdder(current.left);
                    if (peerCached.replace(name, current, Pair.create(next, false)))
                        return current.left;
                }
                catch (SequenceExhaustedException e)
                {
                    if (peerCached.replace(name, current, Pair.create(current.left, true)))
                        return current.left;
                }
            }
        }

        @Override
        public void removeSequence(String keyspace, ByteBuffer name, ConsistencyLevel cl)
        {
            localCached.remove(name);
            peerCached.remove(name);
        }

        @Override
        public boolean createPeer(String keyspace, ByteBuffer name, long startWith, ConsistencyLevel clSerial, ConsistencyLevel cl)
        {
            return peerCached.putIfAbsent(name, Pair.create(startWith, false)) == null;
        }
    }
}
