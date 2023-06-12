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

import org.apache.paimon.append.AppendOnlyCompactionTask;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.utils.SerializableFunction;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;

/** Compaction Sink for unaware-bucket table. */
public class UnawareBucketCompactionSink extends FlinkSink<AppendOnlyCompactionTask> {

    private static final String COMPACTION_WORKER_NAME = "Compaction Worker";

    private AppendOnlyFileStoreTable table;
    private Integer compactionWorkerParallelism;

    public UnawareBucketCompactionSink(
            AppendOnlyFileStoreTable table, Integer compactionWorkerParallelism) {
        super(table, true);
        this.table = table;
        this.compactionWorkerParallelism = compactionWorkerParallelism;
    }

    @Override
    protected SingleOutputStreamOperator<Committable> handleInput(
            DataStream<AppendOnlyCompactionTask> input, boolean isStreaming, String commitUser) {
        CommittableTypeInfo committableTypeInfo = new CommittableTypeInfo();
        SingleOutputStreamOperator<Committable> compacted =
                input.transform(
                        COMPACTION_WORKER_NAME + " -> " + table.name(),
                        committableTypeInfo,
                        new AppendOnlyTableCompactionWorkerOperator(table, commitUser));

        if (compactionWorkerParallelism != null) {
            compacted.setParallelism(compactionWorkerParallelism);
        }

        return compacted;
    }

    public static DataStreamSink<?> sink(
            AppendOnlyFileStoreTable table,
            Integer compactionWorkerParallelism,
            DataStream<AppendOnlyCompactionTask> input) {
        return new UnawareBucketCompactionSink(table, compactionWorkerParallelism).sinkFrom(input);
    }

    @Override
    protected OneInputStreamOperator<AppendOnlyCompactionTask, Committable> createWriteOperator(
            StoreSinkWrite.Provider writeProvider, boolean isStreaming, String commitUser) {
        return null;
    }

    @Override
    protected SerializableFunction<String, Committer<Committable, ManifestCommittable>>
            createCommitterFactory(boolean streamingCheckpointEnabled) {
        return s -> new StoreCommitter(table.newCommit(s));
    }

    @Override
    protected CommittableStateManager<ManifestCommittable> createCommittableStateManager() {
        return new NoopCommittableStateManager();
    }
}
