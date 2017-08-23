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
package org.apache.cassandra.db.rows;

import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.*;

import static java.util.Arrays.asList;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AbstractTypeVersionComparatorTest
{
    private UserType udtWith2Fields;
    private UserType udtWith3Fields;

    private TupleType tupleWith2Fields;
    private TupleType tupleWith3Fields;

    private SetType<String> set1;
    private SetType<InetAddress> set2;

    private ListType<String> list1;
    private ListType<InetAddress> list2;

    private MapType<String, BigInteger> map1;
    private MapType<InetAddress, BigInteger> map2;

    private MapType<BigInteger, String> map3;
    private MapType<BigInteger, InetAddress> map4;

    @Before
    public void setUp()
    {
        udtWith2Fields = new UserType("ks",
                                      bytes("myType"),
                                      asList(bytes("a"), bytes("b")),
                                      asList(Int32Type.instance, Int32Type.instance));
        udtWith3Fields = new UserType("ks",
                                      bytes("myType"),
                                      asList(bytes("a"), bytes("b"), bytes("c")),
                                      asList(Int32Type.instance, Int32Type.instance, Int32Type.instance));

        tupleWith2Fields = new TupleType(asList(Int32Type.instance, Int32Type.instance));
        tupleWith3Fields = new TupleType(asList(Int32Type.instance, Int32Type.instance, Int32Type.instance));

        set1 = SetType.getInstance(UTF8Type.instance, true);
        set2 = SetType.getInstance(InetAddressType.instance, true);

        list1 = ListType.getInstance(UTF8Type.instance, true);
        list2 = ListType.getInstance(InetAddressType.instance, true);

        map1 = MapType.getInstance(UTF8Type.instance, IntegerType.instance, true);
        map2 = MapType.getInstance(InetAddressType.instance, IntegerType.instance, true);

        map3 = MapType.getInstance(IntegerType.instance, UTF8Type.instance, true);
        map4 = MapType.getInstance(IntegerType.instance, InetAddressType.instance, true);
    }

    @After
    public void tearDown()
    {
        udtWith2Fields = null;
        udtWith3Fields = null;
        tupleWith2Fields = null;
        tupleWith3Fields = null;
    }

    @Test
    public void testWithSets()
    {
        checkComparisonResults(set1, set2, "Trying to compare 2 different types: org.apache.cassandra.db.marshal.UTF8Type and org.apache.cassandra.db.marshal.InetAddressType");
    }

    @Test
    public void testWithLists()
    {
        checkComparisonResults(list1, list2, "Trying to compare 2 different types: org.apache.cassandra.db.marshal.UTF8Type and org.apache.cassandra.db.marshal.InetAddressType");
    }

    @Test
    public void testWithMaps12()
    {
        checkComparisonResults(map1, map2, "Trying to compare 2 different types: org.apache.cassandra.db.marshal.UTF8Type and org.apache.cassandra.db.marshal.InetAddressType");
    }

    @Test
    public void testWithMaps34()
    {
        checkComparisonResults(map3, map4, "Trying to compare 2 different types: org.apache.cassandra.db.marshal.UTF8Type and org.apache.cassandra.db.marshal.InetAddressType");
    }

    @Test
    public void testWithTuples()
    {
        checkComparisonResults(tupleWith2Fields, tupleWith3Fields);
    }

    @Test
    public void testWithUDTs()
    {
        checkComparisonResults(udtWith2Fields, udtWith3Fields);
    }

    @Test
    public void testWithUDTsNestedWithinSet()
    {
        for (boolean isMultiCell : new boolean[]{false, true})
        {
            SetType<ByteBuffer> set1 = SetType.getInstance(udtWith2Fields, isMultiCell);
            SetType<ByteBuffer> set2 = SetType.getInstance(udtWith3Fields, isMultiCell);
            checkComparisonResults(set1, set2);
        }
    }

    @Test
    public void testWithUDTsNestedWithinList()
    {
        for (boolean isMultiCell : new boolean[]{false, true})
        {
            ListType<ByteBuffer> list1 = ListType.getInstance(udtWith2Fields, isMultiCell);
            ListType<ByteBuffer> list2 = ListType.getInstance(udtWith3Fields, isMultiCell);
            checkComparisonResults(list1, list2);
        }
    }

    @Test
    public void testWithUDTsNestedWithinMap()
    {
        for (boolean isMultiCell : new boolean[]{false, true})
        {
            MapType<ByteBuffer, Integer> map1 = MapType.getInstance(udtWith2Fields, Int32Type.instance, isMultiCell);
            MapType<ByteBuffer, Integer> map2 = MapType.getInstance(udtWith3Fields, Int32Type.instance, isMultiCell);
            checkComparisonResults(map1, map2);
        }

        for (boolean isMultiCell : new boolean[]{false, true})
        {
            MapType<Integer, ByteBuffer> map1 = MapType.getInstance(Int32Type.instance, udtWith2Fields, isMultiCell);
            MapType<Integer, ByteBuffer> map2 = MapType.getInstance(Int32Type.instance, udtWith3Fields, isMultiCell);
            checkComparisonResults(map1, map2);
        }
    }

    @Test
    public void testWithUDTsNestedWithinTuple()
    {
        TupleType tuple1 = new TupleType(asList(udtWith2Fields, Int32Type.instance));
        TupleType tuple2 = new TupleType(asList(udtWith3Fields, Int32Type.instance));
        checkComparisonResults(tuple1, tuple2);
    }

    @Test
    public void testWithUDTsNestedWithinComposite()
    {
        CompositeType composite1 = CompositeType.getInstance(asList(udtWith2Fields, Int32Type.instance));
        CompositeType composite2 = CompositeType.getInstance(asList(udtWith3Fields, Int32Type.instance));
        checkComparisonResults(composite1, composite2);
    }

    @Test
    public void testWithDeeplyNestedUDT()
    {
        for (boolean isMultiCell : new boolean[]{false, true})
        {
            ListType<Set<ByteBuffer>> list1 = ListType.getInstance(SetType.getInstance(new TupleType(asList(udtWith2Fields, Int32Type.instance)), isMultiCell), isMultiCell);
            ListType<Set<ByteBuffer>> list2 = ListType.getInstance(SetType.getInstance(new TupleType(asList(udtWith3Fields, Int32Type.instance)), isMultiCell), isMultiCell);
            checkComparisonResults(list1, list2);
        }
    }

    @Test
    public void testInvalidComparison()
    {
        checkComparisonResults(udtWith2Fields, Int32Type.instance, "Trying to compare 2 different types: org.apache.cassandra.db.marshal.UserType(ks,6d7954797065,61:org.apache.cassandra.db.marshal.Int32Type,62:org.apache.cassandra.db.marshal.Int32Type) and org.apache.cassandra.db.marshal.Int32Type");
    }

    private void checkComparisonResults(AbstractType<?> oldVersion, AbstractType<?> newVersion, String expectedMessage)
    {
        try
        {
            checkComparisonResults(oldVersion, newVersion);
            fail("comparison doesn't throw expected IllegalArgumentException: " + expectedMessage);
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(e.getMessage(), expectedMessage);
        }
    }

    private void checkComparisonResults(AbstractType<?> oldVersion, AbstractType<?> newVersion)
    {
        assertEquals(0, compare(oldVersion, oldVersion));
        assertEquals(0, compare(newVersion, newVersion));
        assertEquals(-1, compare(oldVersion, newVersion));
        assertEquals(1, compare(newVersion, oldVersion));
    }

    private int compare(AbstractType<?> left, AbstractType<?> right)
    {
        return AbstractTypeVersionComparator.INSTANCE.compare(left, right);
    }
}
