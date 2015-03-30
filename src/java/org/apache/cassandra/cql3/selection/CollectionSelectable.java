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

package org.apache.cassandra.cql3.selection;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * Selectable implementation for collection single element or slice selections.
 */
public class CollectionSelectable extends Selectable
{
    private final ColumnIdentifier id;
    private final boolean slice;
    private final Term.Raw fromRaw;
    private final Term.Raw toRaw;

    // the collection column this selectable relates to
    private ColumnDefinition columnDefinition;
    // the column specification for this selectable
    private ColumnSpecification termColSpec;
    // derived from fromRaw in prepareInternal()
    private Term fromTerm;
    // derived from toRaw in prepareInternal()
    private Term toTerm;
    // if fromRaw is a constant, fromValue will receive its value in prepareInternal()
    private ByteBuffer fromValue;
    // if toRaw is a constant, toValue will receive its value in prepareInternal()
    private ByteBuffer toValue;

    CollectionSelectable(ColumnIdentifier id, boolean slice, Term.Raw fromRaw, Term.Raw toRaw)
    {
        this.id = id;
        this.slice = slice;
        this.fromRaw = fromRaw;
        this.toRaw = toRaw;
    }

    @Override
    public String toString()
    {
        return id.toString() + '[' + (fromRaw != null ? fromRaw : "") + (slice ? ".." + (toRaw != null ? toRaw : "") : "") + ']';
    }

    @Override
    public SelectionRestriction newSelectionRestriction(CFMetaData cfm)
    {
        prepareInternal(cfm);

        if (!columnDefinition.type.isMultiCell()) // filtering cells on frozen collections ... no
            return SelectionRestriction.FULL;

        // If the terms are not dynamic (e.g. not bind markers, not function calls), we
        // can use a "static" restriction that does not need to be evaluated for each execution.
        // The SelectStatement uses this mechanism to create the column filter for all executions.
        boolean dynamic = (fromRaw != null && !fromRaw.isConstant())
                          || (toRaw != null && !toRaw.isConstant());

        if (!dynamic)
            return SelectionRestriction.nonDynamic(slice, fromValue(null), toValue(null), columnDefinition);

        return new SelectionRestriction()
        {
            public boolean restricted()
            {
                return true;
            }

            public boolean dynamic()
            {
                return true;
            }

            public boolean slice()
            {
                return slice;
            }

            public CellPath from(QueryOptions options)
            {
                if (fromTerm == null)
                    return CellPath.BOTTOM;
                ByteBuffer v = fromValue(options);
                if (v == null)
                    return CellPath.BOTTOM;
                return CellPath.create(v);
            }

            public CellPath to(QueryOptions options)
            {
                if (toTerm == null)
                    return CellPath.TOP;
                ByteBuffer v = toValue(options);
                if (v == null)
                    return CellPath.TOP;
                return CellPath.create(v);
            }

            public ColumnDefinition referencedColumn()
            {
                return columnDefinition;
            }

            @Override
            public String toString()
            {
                if (slice)
                    return "SelectonRestriction.Dynamic('" + fromRaw + "' .. '" + toRaw + "' on " + columnDefinition + ')';
                return "SelectonRestriction.Dynamic('" + fromRaw + "' on " + columnDefinition + ')';
            }
        };
    }

    public Selector.Factory newSelectorFactory(CFMetaData cfm, List<ColumnDefinition> defs) throws InvalidRequestException
    {
        prepareInternal(cfm);

        int index = addAndGetIndex(columnDefinition, defs);

        return new Selector.Factory()
        {
            public Selector newInstance() throws InvalidRequestException
            {
                return new Selector()
                {
                    private ByteBuffer current;

                    // Flag whether fromBB & toBB fields have been filled.
                    private boolean keyOrIndexPrepared;
                    // The value for the "from" and "to" bounds will be constant for the execution - so we can cache it here.
                    // But we cannot just rely on the fromValue/toValue values as these are only set for constatn terms
                    // for a whole statement.
                    private ByteBuffer fromBB;
                    private ByteBuffer toBB;

                    public ByteBuffer getOutput(int protocolVersion) throws InvalidRequestException
                    {
                        return current;
                    }

                    public AbstractType<?> getType()
                    {
                        return getReturnType();
                    }

                    public void reset()
                    {
                        current = null;
                        // Need to reset the values since these values can change for each row.
                        // For example if a column is passed into a UDF used to determine the slice bounds.
                        keyOrIndexPrepared = false;
                        fromBB = null;
                        toBB = null;
                    }

                    public void addInput(QueryOptions options, Selection.ResultSetBuilder rs) throws InvalidRequestException
                    {
                        CollectionType<?> type = (CollectionType<?>) columnDefinition.type;

                        if (type.isMultiCell())
                        {
                            // for complex columns (non-frozen sets + maps) we can filter the cells by the slice/from/to parameters

                            ComplexColumnData complex = rs.currentComplex(index);
                            if (complex == null)
                                return;

                            maybePrepare(options);

                            current = type.serializeForNativeProtocol(complex.iterator(), options.getProtocolVersion(),
                                                                      slice, fromBB, toBB);
                            return;
                        }

                        // frozen columns need to be filtered during reserialization

                        ByteBuffer value = rs.current(index, columnDefinition.type, options);
                        if (value == null)
                            return;

                        maybePrepare(options);

                        current = type.reserializeForNativeProtocol(value, options.getProtocolVersion(),
                                                                    slice, fromBB, toBB);
                    }

                    private void maybePrepare(QueryOptions options)
                    {
                        if (!keyOrIndexPrepared)
                        {
                            fromBB = fromTerm != null ? fromValue(options) : null;
                            toBB = toTerm != null ? toValue(options) : null;
                            keyOrIndexPrepared = true;
                        }
                    }

                    @Override
                    public String toString()
                    {
                        return getColumnName();
                    }
                };
            }

            protected String getColumnName()
            {
                return termColSpec.name.toString();
            }

            protected AbstractType<?> getReturnType()
            {
                return slice ? columnDefinition.type : valueType();
            }

            protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn)
            {
                ColumnDefinition def = ColumnDefinition.regularDef(cfm.ksName, cfm.cfName, termColSpec.name.toString(), getReturnType());
                mapping.addMapping(resultsColumn, def);
            }
        };
    }

    // Setup fields used by both newSelectorFactory() and newSelectionRestriction() which can both be
    // called individually. newSelectionRestriction() is not neccessarily called before newSelectorFactory().
    private void prepareInternal(CFMetaData cfm)
    {
        if (columnDefinition != null)
            return;

        columnDefinition = cfm.getColumnDefinition(id);
        if (columnDefinition == null)
            throw new InvalidRequestException(String.format("Undefined name %s in selection clause", id));
        if (!columnDefinition.type.isCollection())
            throw new InvalidRequestException(String.format("Can only use '%s' syntax on collections", toString()));

        AbstractType<?> argType = argType();

        if (argType instanceof ListType)
            throw new InvalidRequestException(String.format("Element and slice selections on lists for column %s is not supported", id));

        termColSpec = new ColumnSpecification(cfm.ksName, cfm.cfName,
                                              new ColumnIdentifier(this.toString(), true),
                                              argType);

        if (fromRaw != null)
        {
            fromTerm = fromRaw.prepare(cfm.ksName, termColSpec);
            if (fromRaw.isConstant())
                fromValue = termValue(fromTerm, null);
        }
        if (toRaw != null)
        {
            toTerm = toRaw.prepare(cfm.ksName, termColSpec);
            if (toRaw.isConstant())
                toValue = termValue(toTerm, null);
        }
    }

    private AbstractType<?> valueType()
    {
        AbstractType<?> type = columnDefinition.type;
        if (type instanceof MapType)
            return ((MapType) type).getValuesType();
        if (type instanceof SetType)
            return ((SetType) type).getElementsType();
        throw new AssertionError("Collection element and slice collections not supported for " + type);
    }

    private AbstractType<?> argType()
    {
        AbstractType<?> type = columnDefinition.type;
        if (type instanceof MapType)
            return ((MapType) type).getKeysType();
        if (type instanceof SetType)
            return ((SetType) type).getElementsType();
        throw new AssertionError("Collection element and slice collections not supported for " + type);
    }

    public static final class Raw implements Selectable.Raw
    {
        public final ColumnIdentifier.Raw id;
        public final boolean slice;
        public final Term.Raw fromRaw;
        public final Term.Raw toRaw;

        public Raw(ColumnIdentifier.Raw id, boolean slice, Term.Raw fromRaw, Term.Raw toRaw)
        {
            this.id = id;
            this.slice = slice;
            this.fromRaw = fromRaw;
            this.toRaw = toRaw;
        }

        public CollectionSelectable prepare(CFMetaData cfm)
        {
            return new CollectionSelectable(id.prepare(cfm), slice, fromRaw, toRaw);
        }

        public boolean processesSelection()
        {
            return true;
        }
    }

    private ByteBuffer fromValue(QueryOptions options)
    {
        // returns the constant "from" bound or derives the value from the term
        return value(fromValue, fromTerm, options);
    }

    private ByteBuffer toValue(QueryOptions options)
    {
        // returns the constant "to" bound or derives the value from the term
        return value(toValue, toTerm, options);
    }

    private static ByteBuffer value(ByteBuffer value, Term term, QueryOptions options)
    {
        return value != null ? value : termValue(term, options);
    }

    private static ByteBuffer termValue(Term term, QueryOptions options)
    {
        return term != null ? term.bindAndGet(options) : null;
    }
}
