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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.VersionedSerializerWrapper;
import org.apache.paimon.flink.utils.StreamExecutionEnvironmentUtils;
import org.apache.paimon.manifest.WrappedManifestCommittable;
import org.apache.paimon.options.Options;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.UUID;

import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_MANAGED_WRITER_BUFFER_MEMORY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_USE_MANAGED_MEMORY;
import static org.apache.paimon.flink.utils.ManagedMemoryUtils.declareManagedMemory;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A sink for processing multi-tables in dedicated compaction job. */
public class CombinedTableCompactorSink implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final String WRITER_NAME = "Writer";
    private static final String GLOBAL_COMMITTER_NAME = "Global Committer";

    private final Catalog.Loader catalogLoader;
    private final boolean ignorePreviousFiles;

    private final Options options;

    public CombinedTableCompactorSink(Catalog.Loader catalogLoader, Options options) {
        this.catalogLoader = catalogLoader;
        this.ignorePreviousFiles = false;
        this.options = options;
    }

    public DataStreamSink<?> sinkFrom(
            DataStream<RowData> multiBucketTableSource,
            DataStream<AppendOnlyCompactionTask> unawareBucketTableSource) {
        // This commitUser is valid only for new jobs.
        // After the job starts, this commitUser will be recorded into the states of write and
        // commit operators.
        // When the job restarts, commitUser will be recovered from states and this value is
        // ignored.
        String initialCommitUser = UUID.randomUUID().toString();
        return sinkFrom(multiBucketTableSource, unawareBucketTableSource, initialCommitUser);
    }

    public DataStreamSink<?> sinkFrom(
            DataStream<RowData> multiBucketTableSource,
            DataStream<AppendOnlyCompactionTask> unawareBucketTableSource,
            String initialCommitUser) {
        // do the actually writing action, no snapshot generated in this stage
        DataStream<MultiTableCommittable> written =
                doWrite(multiBucketTableSource, unawareBucketTableSource, initialCommitUser);

        // commit the committable to generate a new snapshot
        return doCommit(written, initialCommitUser);
    }

    public DataStream<MultiTableCommittable> doWrite(
            DataStream<RowData> multiBucketTableSource,
            DataStream<AppendOnlyCompactionTask> unawareBucketTableSource,
            String commitUser) {
        StreamExecutionEnvironment env = multiBucketTableSource.getExecutionEnvironment();
        boolean isStreaming =
                StreamExecutionEnvironmentUtils.getConfiguration(env)
                                .get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;

        SingleOutputStreamOperator<MultiTableCommittable> multiBucketTableRewriter =
                multiBucketTableSource
                        .transform(
                                String.format("%s-%s", "Multi-Bucket-Table", WRITER_NAME),
                                new MultiTableCommittableTypeInfo(),
                                createWriteOperator(
                                        env.getCheckpointConfig(), isStreaming, commitUser))
                        .setParallelism(multiBucketTableSource.getParallelism());

        SingleOutputStreamOperator<MultiTableCommittable> unawareBucketTableRewriter =
                unawareBucketTableSource
                        .transform(
                                String.format("%s-%s", "Unaware-Bucket-Table", WRITER_NAME),
                                new MultiTableCommittableTypeInfo(),
                                new UnawareCombinedCompactionWorkerOperator(
                                        catalogLoader, commitUser, options))
                        .setParallelism(unawareBucketTableSource.getParallelism());

        if (!isStreaming) {
            assertBatchConfiguration(env, multiBucketTableRewriter.getParallelism());
            assertBatchConfiguration(env, unawareBucketTableRewriter.getParallelism());
        }

        if (options.get(SINK_USE_MANAGED_MEMORY)) {
            declareManagedMemory(
                    multiBucketTableRewriter, options.get(SINK_MANAGED_WRITER_BUFFER_MEMORY));
            declareManagedMemory(
                    unawareBucketTableRewriter, options.get(SINK_MANAGED_WRITER_BUFFER_MEMORY));
        }
        return multiBucketTableRewriter.union(unawareBucketTableRewriter);
    }

    protected DataStreamSink<?> doCommit(
            DataStream<MultiTableCommittable> written, String commitUser) {
        StreamExecutionEnvironment env = written.getExecutionEnvironment();
        ReadableConfig conf = StreamExecutionEnvironmentUtils.getConfiguration(env);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        boolean isStreaming =
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING;
        boolean streamingCheckpointEnabled =
                isStreaming && checkpointConfig.isCheckpointingEnabled();
        if (streamingCheckpointEnabled) {
            assertStreamingConfiguration(env);
        }

        SingleOutputStreamOperator<?> committed =
                written.transform(
                                GLOBAL_COMMITTER_NAME,
                                new MultiTableCommittableTypeInfo(),
                                new CommitterOperator<>(
                                        streamingCheckpointEnabled,
                                        commitUser,
                                        createCommitterFactory(),
                                        createCommittableStateManager()))
                        .setParallelism(1)
                        .setMaxParallelism(1);
        return committed.addSink(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    public static void assertStreamingConfiguration(StreamExecutionEnvironment env) {
        checkArgument(
                !env.getCheckpointConfig().isUnalignedCheckpointsEnabled(),
                "Paimon sink currently does not support unaligned checkpoints. Please set "
                        + ExecutionCheckpointingOptions.ENABLE_UNALIGNED.key()
                        + " to false.");
        checkArgument(
                env.getCheckpointConfig().getCheckpointingMode() == CheckpointingMode.EXACTLY_ONCE,
                "Paimon sink currently only supports EXACTLY_ONCE checkpoint mode. Please set "
                        + ExecutionCheckpointingOptions.CHECKPOINTING_MODE.key()
                        + " to exactly-once");
    }

    private void assertBatchConfiguration(StreamExecutionEnvironment env, int sinkParallelism) {
        try {
            checkArgument(
                    sinkParallelism != -1 || !AdaptiveParallelism.isEnabled(env),
                    "Paimon Sink does not support Flink's Adaptive Parallelism mode. "
                            + "Please manually turn it off or set Paimon `sink.parallelism` manually.");
        } catch (NoClassDefFoundError ignored) {
            // before 1.17, there is no adaptive parallelism
        }
    }

    // TODO:refactor FlinkSink to adopt this sink
    protected OneInputStreamOperator<RowData, MultiTableCommittable> createWriteOperator(
            CheckpointConfig checkpointConfig, boolean isStreaming, String commitUser) {
        return new MultiTablesStoreCompactOperator(
                catalogLoader,
                commitUser,
                checkpointConfig,
                isStreaming,
                ignorePreviousFiles,
                options);
    }

    protected Committer.Factory<MultiTableCommittable, WrappedManifestCommittable>
            createCommitterFactory() {
        return (user, metricGroup) ->
                new StoreMultiCommitter(catalogLoader, user, metricGroup, true);
    }

    protected CommittableStateManager<WrappedManifestCommittable> createCommittableStateManager() {
        return new RestoreAndFailCommittableStateManager<>(
                () -> new VersionedSerializerWrapper<>(new WrappedManifestCommittableSerializer()));
    }
}
