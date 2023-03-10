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

package org.apache.flink.table.store.file.mergetree.compact;

import org.apache.flink.table.store.file.KeyValue;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * A {@link MergeFunction} for lookup, this wrapper only consider the latest high level record,
 * because each merge will query the old merged record, so the latest high level record should be
 * the final merged value.
 */
public class LookupMergeFunction implements MergeFunction<KeyValue> {

    private final MergeFunction<KeyValue> mergeFunction;
    private final LinkedList<KeyValue> candidates = new LinkedList<>();

    KeyValue highLevel;
    boolean containLevel0;

    public LookupMergeFunction(MergeFunction<KeyValue> mergeFunction) {
        this.mergeFunction = mergeFunction;
    }

    @Override
    public void reset() {
        candidates.clear();
        highLevel = null;
        containLevel0 = false;
    }

    @Override
    public void add(KeyValue kv) {
        candidates.add(kv);
    }

    @Override
    public KeyValue getResult() {
        // 1. Find the latest high level record
        Iterator<KeyValue> descending = candidates.descendingIterator();
        while (descending.hasNext()) {
            KeyValue kv = descending.next();
            if (kv.level() > 0) {
                if (highLevel != null) {
                    descending.remove();
                } else {
                    highLevel = kv;
                }
            } else {
                containLevel0 = true;
            }
        }

        // 2. Do the merge for inputs
        mergeFunction.reset();
        candidates.forEach(mergeFunction::add);
        return mergeFunction.getResult();
    }

    public static MergeFunctionFactory<KeyValue> wrap(MergeFunctionFactory<KeyValue> wrapped) {
        return new Factory(wrapped);
    }

    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;

        private final MergeFunctionFactory<KeyValue> wrapped;

        private Factory(MergeFunctionFactory<KeyValue> wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            return new LookupMergeFunction(wrapped.create(projection));
        }
    }
}
