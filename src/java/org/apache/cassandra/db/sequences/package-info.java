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

/**
 * Sequence implementation.
 * <p>
 *     Sequence definitions are maintained in the legacy schema table {@code system.schema_sequences} and takes
 *     part in the usual migraion process.
 * </p>
 * <p>
 *     The distributed table {@code system_distributed.seq_reservations} provides the next available sequence value
 *     and the exhausted flag. One row per sequence is present as long as the sequence exists.
 * </p>
 * <p>
 *     The per-node local table {@code system.sequence_local} contains the currently allocated range for the node
 *     and consists of the columns {@code next_val} and {@code reserved} (inclusive) that define the globally reserved but
 *     yet unused range. If no row for a sequence exists, no range has been reserved for the node.
 * </p>
 */
package org.apache.cassandra.db.sequences;
