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

package org.apache.paimon.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/** Ordered packing for input items. */
public class OrderedPacking {
    private OrderedPacking() {}

    public static <T> List<List<T>> pack(
            Iterable<T> items, Function<T, Long> weightFunc, long targetWeight) {
        List<List<T>> packed = new ArrayList<>();

        List<T> binItems = new ArrayList<>();
        long binWeight = 0L;

        for (T item : items) {
            long weight = weightFunc.apply(item);
            // when get a much big item or total weight enough, we check the binItems size. If
            // greater than zero, we pack it
            if (binWeight + weight > targetWeight && binItems.size() > 0) {
                packed.add(binItems);
                binItems = new ArrayList<>();
                binWeight = 0;
            }

            binWeight += weight;
            binItems.add(item);
        }

        if (binItems.size() > 0) {
            packed.add(binItems);
        }
        return packed;
    }
}
