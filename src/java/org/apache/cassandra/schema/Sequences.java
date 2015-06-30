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

package org.apache.cassandra.schema;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Optional;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.sequences.Sequence;

import static com.google.common.collect.Iterables.filter;

/**
 * An immutable container for a keyspace's sequences.
 */
public final class Sequences implements Iterable<Sequence>
{
    private final ImmutableMap<ByteBuffer, Sequence> sequences;

    private Sequences(Builder builder)
    {
        sequences = builder.sequences.build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Sequences none()
    {
        return builder().build();
    }

    public Iterator<Sequence> iterator()
    {
        return sequences.values().iterator();
    }

    /**
     * Get the sequence with the specified name
     *
     * @param name a non-qualified sequence name
     * @return an empty {@link Optional} if the sequence name is not found; a non-empty optional of {@link Sequence} otherwise
     */
    public Optional<Sequence> get(ByteBuffer name)
    {
        return Optional.ofNullable(sequences.get(name));
    }

    /**
     * Get the sequence with the specified name
     *
     * @param name a non-qualified sequence name
     * @return null if the sequence name is not found; the found {@link Sequence} otherwise
     */
    @Nullable
    public Sequence getNullable(ByteBuffer name)
    {
        return sequences.get(name);
    }

    /**
     * Create a Sequences instance with the provided sequence added
     */
    public Sequences with(Sequence sequence)
    {
        if (get(sequence.name).isPresent())
            throw new IllegalStateException(String.format("Sequence %s already exists", sequence.name));

        return builder().add(this).add(sequence).build();
    }

    /**
     * Creates a Sequences instance with the sequence with the provided name removed
     */
    public Sequences without(ByteBuffer name)
    {
        Sequence sequence =
            get(name).orElseThrow(() -> new IllegalStateException(String.format("Sequence %s doesn't exists", name)));

        return builder().add(filter(this, t -> t != sequence)).build();
    }

    @Override
    public boolean equals(Object o)
    {
        return this == o || (o instanceof Sequences && sequences.equals(((Sequences) o).sequences));
    }

    @Override
    public int hashCode()
    {
        return sequences.hashCode();
    }

    @Override
    public String toString()
    {
        return sequences.values().toString();
    }

    public static final class Builder
    {
        final ImmutableMap.Builder<ByteBuffer, Sequence> sequences = new ImmutableMap.Builder<>();

        private Builder()
        {
        }

        public Sequences build()
        {
            return new Sequences(this);
        }

        public Builder add(Sequence sequence)
        {
            sequences.put(sequence.name, sequence);
            return this;
        }

        public Builder add(Iterable<Sequence> sequences)
        {
            sequences.forEach(this::add);
            return this;
        }
    }
}
