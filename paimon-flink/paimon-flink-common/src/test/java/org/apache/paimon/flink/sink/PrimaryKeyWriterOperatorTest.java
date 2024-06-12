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

package org.apache.paimon.flink.sink;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.utils.TestingMetricUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** test class for {@link TableWriteOperator} with primary key writer. */
public class PrimaryKeyWriterOperatorTest extends WriterOperatorTestBase {

    @Override
    protected void setTableConfig(Options options) {
        options.set("primary-key", "a,b");
        options.set("partition", "a");
        options.set("bucket", "1");
        options.set("bucket-key", "b");
        options.set("write-buffer-size", "256 b");
        options.set("page-size", "32 b");
    }

    @Test
    public void testNumWritersMetric() throws Exception {
        String tableName = tablePath.getName();
        FileStoreTable fileStoreTable = createFileStoreTable();
        TableCommitImpl commit = fileStoreTable.newCommit(COMMIT_USER);

        RowDataStoreWriteOperator rowDataStoreWriteOperator =
                getRowDataStoreWriteOperator(fileStoreTable);
        OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                createWriteOperatorHarness(fileStoreTable, rowDataStoreWriteOperator);

        TypeSerializer<Committable> serializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        harness.setup(serializer);
        harness.open();

        OperatorMetricGroup metricGroup = rowDataStoreWriteOperator.getMetricGroup();
        MetricGroup writerBufferMetricGroup =
                metricGroup
                        .addGroup("paimon")
                        .addGroup("table", tableName)
                        .addGroup("writerBuffer");

        Gauge<Integer> numWriters =
                TestingMetricUtils.getGauge(writerBufferMetricGroup, "numWriters");

        // write into three partitions
        harness.processElement(GenericRow.of(1, 1), 1);
        harness.processElement(GenericRow.of(2, 2), 2);
        harness.processElement(GenericRow.of(3, 3), 3);
        assertThat(numWriters.getValue()).isEqualTo(3);

        // commit messages in three partitions, no writer should be cleaned
        harness.prepareSnapshotPreBarrier(1);
        harness.snapshot(1, 10);
        harness.notifyOfCompletedCheckpoint(1);
        commit.commit(
                1,
                harness.extractOutputValues().stream()
                        .map(c -> (CommitMessage) c.wrappedCommittable())
                        .collect(Collectors.toList()));
        assertThat(numWriters.getValue()).isEqualTo(3);

        // write into two partitions
        harness.processElement(GenericRow.of(1, 11), 11);
        harness.processElement(GenericRow.of(3, 13), 13);
        // checkpoint has not come yet, so no writer should be cleaned
        assertThat(numWriters.getValue()).isEqualTo(3);

        // checkpoint comes, partition 2 has nothing to write, so it should be cleaned
        harness.prepareSnapshotPreBarrier(2);
        harness.snapshot(2, 20);
        harness.notifyOfCompletedCheckpoint(2);
        assertThat(numWriters.getValue()).isEqualTo(2);

        harness.endInput();
        harness.close();
        commit.close();
    }
}
