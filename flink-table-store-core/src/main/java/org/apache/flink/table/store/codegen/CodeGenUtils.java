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

package org.apache.flink.table.store.codegen;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinaryRowDataUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

/** Utils for code generations. */
public class CodeGenUtils {

    public static final Projection<RowData, BinaryRowData> EMPTY_PROJECTION =
            input -> BinaryRowDataUtil.EMPTY_ROW;

    public static Projection<RowData, BinaryRowData> newProjection(
            RowType inputType, int[] mapping) {
        if (mapping.length == 0) {
            return EMPTY_PROJECTION;
        }

        @SuppressWarnings("unchecked")
        Projection<RowData, BinaryRowData> projection =
                CodeGenLoader.getInstance()
                        .discover(CodeGenerator.class)
                        .generateProjection(new TableConfig(), "Projection", inputType, mapping)
                        .newInstance(Thread.currentThread().getContextClassLoader());
        return projection;
    }

    public static NormalizedKeyComputer newNormalizedKeyComputer(
            TableConfig tableConfig, List<LogicalType> fieldTypes, String name) {
        return CodeGenLoader.getInstance()
                .discover(CodeGenerator.class)
                .generateNormalizedKeyComputer(tableConfig, fieldTypes, name)
                .newInstance(Thread.currentThread().getContextClassLoader());
    }

    public static GeneratedRecordComparator generateRecordComparator(
            TableConfig tableConfig, List<LogicalType> fieldTypes, String name) {
        return CodeGenLoader.getInstance()
                .discover(CodeGenerator.class)
                .generateRecordComparator(tableConfig, fieldTypes, name);
    }

    public static RecordComparator newRecordComparator(
            TableConfig tableConfig, List<LogicalType> fieldTypes, String name) {
        return generateRecordComparator(tableConfig, fieldTypes, name)
                .newInstance(Thread.currentThread().getContextClassLoader());
    }
}
