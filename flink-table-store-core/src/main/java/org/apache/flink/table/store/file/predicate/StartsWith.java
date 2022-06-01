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

import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.store.file.stats.FieldStats;

import java.util.Optional;

/**
 * A {@link LeafPredicate.Function} to evaluate {@code filter like 'abc%' or filter like 'abc_'}.
 */
public class StartsWith implements LeafPredicate.Function {

    public static final StartsWith INSTANCE = new StartsWith();

    private StartsWith() {}

    @Override
    public boolean test(Object[] values, int index, Literal patternLiteral) {
        BinaryStringData field = (BinaryStringData) values[index];
        return field != null && field.startsWith((BinaryStringData) patternLiteral.value());
    }

    @Override
    public boolean test(long rowCount, FieldStats[] fieldStats, int index, Literal patternLiteral) {
        FieldStats stats = fieldStats[index];
        if (rowCount == stats.nullCount()) {
            return false;
        }
        BinaryStringData min = (BinaryStringData) stats.minValue();
        BinaryStringData max = (BinaryStringData) stats.maxValue();
        BinaryStringData pattern = (BinaryStringData) patternLiteral.value();
        return (min.startsWith(pattern) || min.compareTo(pattern) <= 0)
                && (max.startsWith(pattern) || max.compareTo(pattern) >= 0);
    }

    @Override
    public Optional<Predicate> negate(int index, Literal literal) {
        return Optional.empty();
    }
}
