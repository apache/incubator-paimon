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

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Non-leaf node in a {@link Predicate} tree. Its evaluation result depends on the results of its
 * children.
 */
public class CompoundPredicate implements Predicate {

    private final Function function;
    private final List<Predicate> children;

    public CompoundPredicate(Function function, List<Predicate> children) {
        this.function = function;
        this.children = children;
    }

    public Function function() {
        return function;
    }

    public List<Predicate> children() {
        return children;
    }

    @Override
    public boolean test(Object[] values) {
        return function.test(values, children);
    }

    @Override
    public boolean test(long rowCount, FieldStats[] fieldStats) {
        return function.test(rowCount, fieldStats, children);
    }

    @Override
    public Optional<Predicate> negate() {
        return function.negate(children);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CompoundPredicate)) {
            return false;
        }
        CompoundPredicate that = (CompoundPredicate) o;
        return Objects.equals(function, that.function) && Objects.equals(children, that.children);
    }

    /** Evaluate the predicate result based on multiple {@link Predicate}s. */
    public interface Function extends Serializable {

        boolean test(Object[] values, List<Predicate> children);

        boolean test(long rowCount, FieldStats[] fieldStats, List<Predicate> children);

        Optional<Predicate> negate(List<Predicate> children);
    }
}
