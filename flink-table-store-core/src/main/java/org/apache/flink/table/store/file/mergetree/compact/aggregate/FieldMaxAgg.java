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

package org.apache.flink.table.store.file.mergetree.compact.aggregate;

import org.apache.flink.table.store.utils.RowDataUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

/** max aggregate a field of a row. */
public class FieldMaxAgg extends FieldAggregator {
    public FieldMaxAgg(LogicalType logicalType) {
        super(logicalType);
    }

    @Override
    Object agg(Object accumulator, Object inputField) {
        Object max;

        if (accumulator == null || inputField == null) {
            max = (accumulator == null ? inputField : accumulator);
        } else {
            LogicalTypeRoot type = fieldType.getTypeRoot();
            if (RowDataUtils.compare(accumulator, inputField, type) < 0) {
                max = inputField;
            } else {
                max = accumulator;
            }
        }
        return max;
    }
}
