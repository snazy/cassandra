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

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.rows.CellPath;

/**
 * Glues slice/element selections on collections (and maybe non-frozen UDTs in the future)
 * and {@link org.apache.cassandra.db.filter.ColumnFilter}.
 *
 * Instances of this class are collected in {@link Selection} and used in {@link org.apache.cassandra.cql3.statements.SelectStatement}
 * to construct the {@link org.apache.cassandra.db.filter.ColumnFilter}.
 */
public interface SelectionRestriction
{
    SelectionRestriction FULL = new SelectionRestriction()
    {
        public CellPath from(QueryOptions options)
        {
            return CellPath.BOTTOM;
        }

        public CellPath to(QueryOptions options)
        {
            return CellPath.TOP;
        }

        public boolean restricted()
        {
            return false;
        }

        public boolean slice()
        {
            return false;
        }

        public boolean dynamic()
        {
            return false;
        }

        public ColumnDefinition referencedColumn()
        {
            return null;
        }

        @Override
        public String toString()
        {
            return "SelectionRestriction.FULL";
        }
    };

    /**
     * Check whether this instance represents a restriction.
     * (i.e. use
     * {@link org.apache.cassandra.db.filter.ColumnFilter.Builder#select(ColumnDefinition, CellPath)} for collection
     * element selection or
     * {@link org.apache.cassandra.db.filter.ColumnFilter.Builder#slice(ColumnDefinition, CellPath, CellPath)} for
     * collection slice selection.
     * Unrestricted selection restriction uses {@link org.apache.cassandra.db.filter.ColumnFilter.Builder#add(ColumnDefinition)}.
     */
    boolean restricted();

    /**
     * {@code true}, if values returned by {@link #from(QueryOptions)} or {@link #to(QueryOptions)} are not
     * constants and need to be evaluated for each execution of the statement. Bind parameters and function calls
     * result in dynamic ones.
     */
    boolean dynamic();

    /**
     * Whether it's a slice or single element select.
     */
    boolean slice();

    /**
     * Get the cell path of the lower/from slice boundary (inclusive).
     * {@link CellPath#BOTTOM} can be used as an "infinite" upper bound.
     *
     * @param options If {@link #dynamic()} is {@code true}, this will be the query options of the current execution.
     *                Otherwise, this parameter will be {@code null}.
     * @return the non-{@code null} cell path. Must return {@link CellPath#BOTTOM} to represent a non-existing bound.
     */
    CellPath from(QueryOptions options);

    /**
     * Get the cell path of the upper/to slice boundary (inclusive).
     * {@link CellPath#TOP} can be used as an "infinite" upper bound.
     *
     * @param options If {@link #dynamic()} is {@code true}, this will be the query options of the current execution.
     *                Otherwise, this parameter will be {@code null}.
     * @return the non-{@code null} cell path. Must return {@link CellPath#TOP} to represent a non-existing bound.
     */
    CellPath to(QueryOptions options);

    ColumnDefinition referencedColumn();

    /**
     * Create a {@link SelectionRestriction} instance using {@code from} and {@code to} as single-element
     * cell paths. Values for {@code from} and {@code to} can be {@code null}.
     */
    static SelectionRestriction nonDynamic(boolean slice, ByteBuffer from, ByteBuffer to, ColumnDefinition def)
    {
        return nonDynamic(slice,
                          from != null ? CellPath.create(from) : null,
                          to != null ? CellPath.create(to) : null,
                          def);
    }

    /**
     * Create a {@link SelectionRestriction} instance using {@code from} and {@code to} as the
     * cell paths. Values for {@code from} and {@code to} can be {@code null}, in which case these
     * will be translated to {@link CellPath#BOTTOM} and {@link CellPath#TOP}.
     */
    static SelectionRestriction nonDynamic(boolean slice, CellPath from, CellPath to, ColumnDefinition def)
    {
        if (from == null)
            from = CellPath.BOTTOM;
        if (to == null)
            to = CellPath.TOP;
        if (slice && from == CellPath.BOTTOM && to == CellPath.TOP)
            // an unbounded slice is a an unrestricted SelectionRestriction (no need to use ColumnFilter's slice)
            return FULL;
        return new NonDynamic(slice, from, to, def);
    }

    final class NonDynamic implements SelectionRestriction
    {
        private final boolean slice;
        private final CellPath from;
        private final CellPath to;
        private final ColumnDefinition def;

        private NonDynamic(boolean slice, CellPath from, CellPath to, ColumnDefinition def)
        {
            this.slice = slice;
            this.from = from;
            this.to = to;
            this.def = def;
        }

        @Override
        public String toString()
        {
            if (slice)
                return "SelectonRestriction.NonDynamic(slice: '" + from + "' .. '" + to + "' on " + def + ')';
            return "SelectonRestriction.NonDynamic(element: '" + from + "' on " + def + ')';
        }

        public boolean restricted()
        {
            return true;
        }

        public boolean dynamic()
        {
            return false;
        }

        public boolean slice()
        {
            return slice;
        }

        public CellPath from(QueryOptions options)
        {
            return from;
        }

        public CellPath to(QueryOptions options)
        {
            return to;
        }

        public ColumnDefinition referencedColumn()
        {
            return def;
        }
    }
}
