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

package org.apache.flink.table.store.file.predicate;

import org.apache.flink.table.store.format.FieldStats;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.List;

/** Function to test a field with a literal. */
public abstract class LeafBinaryFunction implements LeafFunction {

    private static final long serialVersionUID = 1L;

    public abstract boolean test(LogicalType type, Object field, Object literal);

    public abstract boolean test(
            LogicalType type, long rowCount, FieldStats fieldStats, Object literal);

    @Override
    public boolean test(LogicalType type, Object field, List<Object> literals) {
        return test(type, field, literals.get(0));
    }

    @Override
    public boolean test(
            LogicalType type, long rowCount, FieldStats fieldStats, List<Object> literals) {
        return test(type, rowCount, fieldStats, literals.get(0));
    }
}
