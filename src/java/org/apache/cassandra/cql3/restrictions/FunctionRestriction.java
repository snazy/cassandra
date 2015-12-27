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

package org.apache.cassandra.cql3.restrictions;

import java.util.Collection;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.MultiCBuilder;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.SecondaryIndexManager;

public class FunctionRestriction extends AbstractRestriction
{
    private final Term function;
    private final Operator operator;
    private final Term value;

    public FunctionRestriction(Term function, Operator operator, Term value)
    {
        this.function = function;
        this.operator = operator;
        this.value = value;
    }

    public ColumnDefinition getFirstColumn()
    {
        throw new UnsupportedOperationException();
    }

    public ColumnDefinition getLastColumn()
    {
        throw new UnsupportedOperationException();
    }

    public Collection<ColumnDefinition> getColumnDefs()
    {
        throw new UnsupportedOperationException();
    }

    public Iterable<Function> getFunctions()
    {
        throw new UnsupportedOperationException();
    }

    public Restriction mergeWith(Restriction otherRestriction) throws InvalidRequestException
    {
        throw new UnsupportedOperationException();
    }

    public boolean hasSupportingIndex(SecondaryIndexManager indexManager)
    {
        throw new UnsupportedOperationException();
    }

    public void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options) throws InvalidRequestException
    {
        throw new UnsupportedOperationException();
    }

    public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
    {
        throw new UnsupportedOperationException();
    }
}
