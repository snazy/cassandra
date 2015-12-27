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

import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.restrictions.FunctionRestriction;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class FunctionRelation extends Relation
{
    private final Term.Raw function;
    private final Term.Raw value;

    public FunctionRelation(Term.Raw function, Operator operator, Term.Raw value)
    {
        this.function = function;
        super.relationType = operator;
        this.value = value;
    }

    @Override
    public String toString()
    {
        return function.getText() + operator() + value.getText();
    }

    @Override
    public List<? extends Term.Raw> getInValues()
    {
        return null;
    }

    @Override
    public Term.Raw getValue()
    {
        return value;
    }

    @Override
    public Relation renameIdentifier(ColumnIdentifier.Raw from, ColumnIdentifier.Raw to)
    {
        return this;
    }

    @Override
    protected Term toTerm(List<? extends ColumnSpecification> receivers, Term.Raw raw, String keyspace, VariableSpecifications boundNames) throws InvalidRequestException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Restriction newContainsRestriction(CFMetaData cfm, VariableSpecifications boundNames, boolean isKey) throws InvalidRequestException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Restriction newEQRestriction(CFMetaData cfm, VariableSpecifications boundNames) throws InvalidRequestException
    {
        // TODO hm - no 'receiver' for prepare of value and no 'receiver' for prepare of function :(
        AbstractType<?> type = null;
        ColumnSpecification spec = new ColumnSpecification(cfm.ksName, cfm.cfName, new ColumnIdentifier(toString(), true), type);
        return new FunctionRestriction(function.prepare(cfm.ksName, spec), operator(), value.prepare(cfm.ksName, spec));
    }

    @Override
    protected Restriction newSliceRestriction(CFMetaData cfm, VariableSpecifications boundNames, Bound bound, boolean inclusive) throws InvalidRequestException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Restriction newINRestriction(CFMetaData cfm, VariableSpecifications boundNames) throws InvalidRequestException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Restriction newIsNotRestriction(CFMetaData cfm, VariableSpecifications boundNames) throws InvalidRequestException
    {
        throw new UnsupportedOperationException();
    }
}
