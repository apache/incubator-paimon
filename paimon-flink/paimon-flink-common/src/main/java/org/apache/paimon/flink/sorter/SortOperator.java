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

package org.apache.paimon.flink.sorter;

import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.NormalizedKeyComputer;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.sort.BinaryInMemorySortBuffer;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.MutableObjectIterator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.runtime.operators.TableStreamOperator;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.disk.IOManagerImpl.splitPaths;

/** SortOperator to sort the `InternalRow`s by the `KeyType`. */
public class SortOperator extends TableStreamOperator<InternalRow>
        implements OneInputStreamOperator<InternalRow, InternalRow>, BoundedOneInput {

    private final RowType keyRowType;
    private final RowType valueRowType;
    private transient BinaryExternalSortBuffer buffer;
    private transient BinaryRow reuseBinaryRow;

    public SortOperator(RowType keyType, RowType valueRowType) {
        this.keyRowType = keyType;
        this.valueRowType = valueRowType;
    }

    @Override
    public void open() throws Exception {
        super.open();

        List<DataField> keyFields = keyRowType.getFields();
        List<DataField> dataFields = valueRowType.getFields();

        List<DataField> fields = new ArrayList<>();
        fields.addAll(keyFields);
        fields.addAll(dataFields);

        RowType rowType = new RowType(fields);
        reuseBinaryRow = new BinaryRow(rowType.getFieldCount());

        InternalRowSerializer serializer = InternalSerializers.create(rowType);
        NormalizedKeyComputer normalizedKeyComputer =
                CodeGenUtils.newNormalizedKeyComputer(
                        rowType.getFieldTypes(), "MemTableKeyComputer");
        RecordComparator keyComparator =
                CodeGenUtils.newRecordComparator(rowType.getFieldTypes(), "MemTableComparator");

        MemorySegmentPool memoryPool =
                new HeapMemorySegmentPool(
                        MemorySize.parse("512 mb").getBytes(),
                        (int) MemorySize.parse("64 kb").getBytes());

        BinaryInMemorySortBuffer inMemorySortBuffer =
                BinaryInMemorySortBuffer.createBuffer(
                        normalizedKeyComputer, serializer, keyComparator, memoryPool);

        Configuration jobConfig = getContainingTask().getJobConfiguration();

        buffer =
                new BinaryExternalSortBuffer(
                        new BinaryRowSerializer(serializer.getArity()),
                        keyComparator,
                        memoryPool.pageSize(),
                        inMemorySortBuffer,
                        new IOManagerImpl(splitPaths(jobConfig.get(CoreOptions.TMP_DIRS))),
                        jobConfig.getInteger(
                                ExecutionConfigOptions.TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES));
    }

    @Override
    public void endInput() throws Exception {
        if (buffer.size() > 0) {
            MutableObjectIterator<BinaryRow> iterator = buffer.sortedIterator();
            while ((iterator.next(reuseBinaryRow)) != null) {
                output.collect(new StreamRecord<>(reuseBinaryRow));
            }
        }
    }

    @Override
    public void processElement(StreamRecord<InternalRow> element) throws Exception {
        buffer.write(element.getValue());
    }
}
