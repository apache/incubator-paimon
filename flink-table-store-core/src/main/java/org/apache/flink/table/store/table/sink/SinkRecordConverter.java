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

package org.apache.flink.table.store.table.sink;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.store.codegen.CodeGenUtils;
import org.apache.flink.table.store.file.schema.Schema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.stream.IntStream;

/** Converter for converting {@link RowData} to {@link SinkRecord}. */
public class SinkRecordConverter {

    private final int numBucket;

    private final Projection<RowData, BinaryRowData> allProjection;

    private final Projection<RowData, BinaryRowData> partProjection;

    private final Projection<RowData, BinaryRowData> pkProjection;

    @Nullable private final Projection<RowData, BinaryRowData> logPkProjection;

    public SinkRecordConverter(int numBucket, Schema schema) {
        this(
                numBucket,
                schema.logicalRowType(),
                schema.projection(schema.partitionKeys()),
                schema.projection(schema.trimmedPrimaryKeys()),
                schema.projection(schema.primaryKeys()));
    }

    public SinkRecordConverter(
            int numBucket,
            RowType inputType,
            int[] partitions,
            int[] primaryKeys,
            int[] logPrimaryKeys) {
        this.numBucket = numBucket;
        this.allProjection =
                CodeGenUtils.newProjection(
                        inputType, IntStream.range(0, inputType.getFieldCount()).toArray());
        this.partProjection = CodeGenUtils.newProjection(inputType, partitions);
        this.pkProjection = CodeGenUtils.newProjection(inputType, primaryKeys);
        this.logPkProjection =
                Arrays.equals(primaryKeys, logPrimaryKeys)
                        ? null
                        : CodeGenUtils.newProjection(inputType, logPrimaryKeys);
    }

    public SinkRecord convert(RowData row) {
        BinaryRowData partition = partProjection.apply(row);
        BinaryRowData primaryKey = primaryKey(row);
        int bucket = bucket(row, primaryKey);
        return new SinkRecord(partition, bucket, primaryKey, row);
    }

    public SinkRecord convertToLogSinkRecord(SinkRecord record) {
        if (logPkProjection == null) {
            return record;
        }
        BinaryRowData logPrimaryKey = logPrimaryKey(record.row());
        return new SinkRecord(record.partition(), record.bucket(), logPrimaryKey, record.row());
    }

    public BinaryRowData primaryKey(RowData row) {
        return pkProjection.apply(row);
    }

    private BinaryRowData logPrimaryKey(RowData row) {
        assert logPkProjection != null;
        return logPkProjection.apply(row);
    }

    public int bucket(RowData row, BinaryRowData primaryKey) {
        int hash = primaryKey.getArity() == 0 ? hashRow(row) : primaryKey.hashCode();
        return Math.abs(hash % numBucket);
    }

    private int hashRow(RowData row) {
        if (row instanceof BinaryRowData) {
            RowKind rowKind = row.getRowKind();
            row.setRowKind(RowKind.INSERT);
            int hash = row.hashCode();
            row.setRowKind(rowKind);
            return hash;
        } else {
            return allProjection.apply(row).hashCode();
        }
    }
}
