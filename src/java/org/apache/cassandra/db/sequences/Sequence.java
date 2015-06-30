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

import org.apache.cassandra.cql3.sequences.SequenceDef;
import org.apache.cassandra.cql3.sequences.SequenceName;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Describes the definition of a sequence.
 */
public class Sequence
{
    public final String keyspace;
    public final ByteBuffer name;
    public final long minVal;
    public final long maxVal;
    public final long startWith;
    public final long incrBy;
    public final long cache;
    public final long cacheLocal;
    public final ConsistencyLevel clSerial;
    public final ConsistencyLevel cl;

    public Sequence(String keyspace, ByteBuffer name, long minVal, long maxVal, long startWith, long incrBy,
                              long cache, long cacheLocal, ConsistencyLevel clSerial, ConsistencyLevel cl)
    {
        this.keyspace = keyspace;
        this.name = name;
        this.minVal = minVal;
        this.maxVal = maxVal;
        this.startWith = startWith;
        this.incrBy = incrBy;
        this.cache = cache;
        this.cacheLocal = cacheLocal;
        this.clSerial = clSerial;
        this.cl = cl;
    }

    public Sequence(SequenceDef def)
    {
        this(def.getName().keyspace, ByteBufferUtil.bytes(def.getName().name),
             def.getMinVal(), def.getMaxVal(), def.getStartWith(), def.getIncrBy(),
             def.getCache(), def.getCacheLocal(), def.getClSerial(), def.getCl());
    }

    public String getNameAsString()
    {
        return UTF8Type.instance.compose(name);
    }

    public SequenceName sequenceName()
    {
        return new SequenceName(keyspace, UTF8Type.instance.compose(name));
    }

    public void testUpdate(SequenceDef definition) throws InvalidRequestException
    {
        // TODO test if requested update can be applied
    }
}
