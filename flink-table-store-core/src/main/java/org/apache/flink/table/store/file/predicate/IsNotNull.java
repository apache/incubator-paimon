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

import org.apache.flink.table.store.file.stats.FieldStats;

import java.util.Objects;
import java.util.Optional;

/** A {@link Predicate} to eval is not null. */
public class IsNotNull implements Predicate {

    private static final long serialVersionUID = 1L;

    private final int index;

    public IsNotNull(int index) {
        this.index = index;
    }

    @Override
    public boolean test(Object[] values) {
        return values[index] != null;
    }

    @Override
    public boolean test(long rowCount, FieldStats[] fieldStats) {
        return fieldStats[index].nullCount() < rowCount;
    }

    @Override
    public Optional<Predicate> negate() {
        return Optional.of(new IsNull(index));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IsNotNull)) {
            return false;
        }
        IsNotNull isNotNull = (IsNotNull) o;
        return index == isNotNull.index;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index);
    }
}
