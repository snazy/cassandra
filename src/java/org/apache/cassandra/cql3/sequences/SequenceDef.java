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

package org.apache.cassandra.cql3.sequences;

import java.math.BigInteger;

import com.google.common.base.Objects;

import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * TODO add javadoc explaining the purpose of this class/interface/annotation
 */
public class SequenceDef
{
    private SequenceName name;

    private long minVal;
    private long maxVal;
    private long startWith;
    private long incrBy;
    private long cache;
    private long cacheLocal;
    private ConsistencyLevel clSerial;
    private ConsistencyLevel cl;

    public SequenceDef(SequenceName name, long minVal, long maxVal, long startWith, long incrBy,
                              long cache, long cacheLocal, ConsistencyLevel clSerial, ConsistencyLevel cl)
    {
        assert name.hasKeyspace();
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

    public SequenceDef(SequenceName name, Constants.Literal incrBy, Constants.Literal minVal, Constants.Literal maxVal,
                              Constants.Literal startWith, Constants.Literal cache, Constants.Literal cacheLocal,
                              ConsistencyLevel clSerial, ConsistencyLevel cl)
    {
        this.name = name; // name might not have a keyspace
        this.incrBy = incrBy != null ? Long.parseLong(incrBy.getRawText()) : 1L;
        this.minVal = minVal != null ? Long.parseLong(minVal.getRawText()) : (this.incrBy > 0 ? 1L : Long.MIN_VALUE);
        this.maxVal = maxVal != null ? Long.parseLong(maxVal.getRawText()) : (this.incrBy > 0 ? Long.MAX_VALUE : -1L);
        this.startWith = startWith != null ? Long.parseLong(startWith.getRawText()) : (this.incrBy > 0 ? this.minVal : this.maxVal);
        this.cache = Math.abs(cache != null ? Long.parseLong(cache.getRawText()) : 1L);
        this.cacheLocal = Math.abs(cacheLocal != null ? Long.parseLong(cacheLocal.getRawText()) : this.cache);
        this.clSerial = clSerial != null ? clSerial : ConsistencyLevel.SERIAL;
        this.cl = cl != null ? cl : ConsistencyLevel.QUORUM;

        if (this.incrBy == 0L)
            throw new InvalidRequestException("Sequence must have non-zero increment - is " + this.incrBy);
        if (this.maxVal <= this.minVal)
            throw new InvalidRequestException("Sequence minVal " + this.minVal + " must be less than maxVal " + this.maxVal);
        if (this.startWith < this.minVal || this.startWith > this.maxVal)
            throw new InvalidRequestException("Sequence startWith " + this.startWith + " must be between minVal " + this.minVal + " and maxVal " + this.maxVal);
        if (this.cache < 1L)
            throw new InvalidRequestException("Sequence cache " + this.cache + " value must be greater or equal to 1");
        if (this.cacheLocal < 1L)
            throw new InvalidRequestException("Sequence cacheLocal " + this.cache + " value must be greater or equal to 1");
        if (this.cacheLocal > this.cache)
            throw new InvalidRequestException("Sequence cacheLocal " + this.cacheLocal + " must not be greater than cache " + this.cache);
        BigInteger possibleIncrements = BigInteger.valueOf(this.maxVal).subtract(BigInteger.valueOf(this.minVal)).add(BigInteger.ONE);
        possibleIncrements = possibleIncrements.divide(BigInteger.valueOf(this.incrBy > 0 ? this.incrBy : -this.incrBy));
        if (possibleIncrements.compareTo(BigInteger.valueOf(this.cache)) < 0)
            throw new InvalidRequestException("Sequence cache " + this.cache + " must not be greater than max possible increments " + possibleIncrements);
        if (possibleIncrements.compareTo(BigInteger.valueOf(this.cacheLocal)) < 0)
            throw new InvalidRequestException("Sequence cacheLocal " + this.cacheLocal + " must not be greater than max possible increments " + possibleIncrements);
    }

    public SequenceName getName()
    {
        return name;
    }

    public void setName(SequenceName name)
    {
        this.name = name;
    }

    public long getMinVal()
    {
        return minVal;
    }

    public long getMaxVal()
    {
        return maxVal;
    }

    public long getStartWith()
    {
        return startWith;
    }

    public void setStartWith(long startWith)
    {
        this.startWith = startWith;
    }

    public long getIncrBy()
    {
        return incrBy;
    }

    public long getCache()
    {
        return cache;
    }

    public long getCacheLocal()
    {
        return cacheLocal;
    }

    public ConsistencyLevel getClSerial()
    {
        return clSerial;
    }

    public ConsistencyLevel getCl()
    {
        return cl;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SequenceDef that = (SequenceDef) o;

        if (minVal != that.minVal) return false;
        if (maxVal != that.maxVal) return false;
        if (startWith != that.startWith) return false;
        if (incrBy != that.incrBy) return false;
        if (cache != that.cache) return false;
        if (cacheLocal != that.cacheLocal) return false;
        if (clSerial != that.clSerial) return false;
        if (cl != that.cl) return false;
        return name.equals(that.name);
    }

    public int hashCode()
    {
        int result = name.hashCode();
        result = 31 * result + (int) (minVal ^ (minVal >>> 32));
        result = 31 * result + (int) (maxVal ^ (maxVal >>> 32));
        result = 31 * result + (int) (startWith ^ (startWith >>> 32));
        result = 31 * result + (int) (incrBy ^ (incrBy >>> 32));
        result = 31 * result + (int) (cache ^ (cache >>> 32));
        result = 31 * result + (int) (cacheLocal ^ (cacheLocal >>> 32));
        result = 31 * result + clSerial.hashCode();
        result = 31 * result + cl.hashCode();
        return result;
    }

    public String toString()
    {
        return Objects.toStringHelper(this)
                      .add("name", name)
                      .add("minVal", minVal)
                      .add("maxVal", maxVal)
                      .add("startWith", startWith)
                      .add("incrBy", incrBy)
                      .add("cache", cache)
                      .add("cacheLocal", cacheLocal)
                      .add("clSerial", clSerial)
                      .add("cl", cl)
                      .toString();
    }
}
