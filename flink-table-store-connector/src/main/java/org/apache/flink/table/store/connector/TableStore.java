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

package org.apache.flink.table.store.connector;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogLock;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.store.connector.sink.BucketStreamPartitioner;
import org.apache.flink.table.store.connector.sink.StoreSink;
import org.apache.flink.table.store.connector.sink.global.GlobalCommittingSinkTranslator;
import org.apache.flink.table.store.connector.source.FileStoreSource;
import org.apache.flink.table.store.connector.source.LogHybridSourceFactory;
import org.apache.flink.table.store.connector.source.StaticFileStoreSplitEnumerator;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.FileStoreImpl;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.FileStoreOptions.MergeEngine;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.log.LogOptions.LogStartupMode;
import org.apache.flink.table.store.log.LogSinkProvider;
import org.apache.flink.table.store.log.LogSourceProvider;
import org.apache.flink.table.store.utils.TypeUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.file.FileStoreOptions.BUCKET;
import static org.apache.flink.table.store.file.FileStoreOptions.CONTINUOUS_DISCOVERY_INTERVAL;
import static org.apache.flink.table.store.file.FileStoreOptions.MERGE_ENGINE;
import static org.apache.flink.table.store.file.FileStoreOptions.MergeEngine.PARTIAL_UPDATE;
import static org.apache.flink.table.store.log.LogOptions.LOG_PREFIX;
import static org.apache.flink.table.store.log.LogOptions.SCAN;

/** A table store api to create source and sink. */
@Experimental
public class TableStore {
    private static final RowType EMPTY_KEY_TYPE = RowType.of();

    private final Configuration options;

    /** commit user, default uuid. */
    private String user = UUID.randomUUID().toString();

    /** partition keys, default no partition. */
    private int[] partitions = new int[0];

    /** file store primary keys which exclude partition fields if partitioned, default no key. */
    private int[] primaryKeys = new int[0];

    /** log store primary keys which include partition fields if partitioned, default no key. */
    private int[] logPrimaryKeys = new int[0];

    private RowType type;

    private ObjectIdentifier tableIdentifier;

    public TableStore(Configuration options) {
        this.options = options;
    }

    public TableStore withUser(String user) {
        this.user = user;
        return this;
    }

    public TableStore withSchema(RowType type) {
        this.type = type;
        return this;
    }

    public TableStore withPartitions(int[] partitions) {
        this.partitions = partitions;
        adjustIndexAndValidate();
        return this;
    }

    public TableStore withPrimaryKeys(int[] primaryKeys) {
        this.primaryKeys = primaryKeys;
        this.logPrimaryKeys = primaryKeys;
        adjustIndexAndValidate();
        return this;
    }

    public TableStore withTableIdentifier(ObjectIdentifier tableIdentifier) {
        this.tableIdentifier = tableIdentifier;
        return this;
    }

    public boolean partitioned() {
        return partitions.length > 0;
    }

    public boolean valueCountMode() {
        return primaryKeys.length == 0;
    }

    public List<String> fieldNames() {
        return type.getFieldNames();
    }

    public List<String> partitionKeys() {
        RowType partitionType = TypeUtils.project(type, partitions);
        return partitionType.getFieldNames();
    }

    @VisibleForTesting
    List<String> primaryKeys() {
        RowType primaryKeyType = TypeUtils.project(type, primaryKeys);
        return primaryKeyType.getFieldNames();
    }

    public Configuration options() {
        return options;
    }

    public Configuration logOptions() {
        return new DelegatingConfiguration(options, LOG_PREFIX);
    }

    public SourceBuilder sourceBuilder() {
        return new SourceBuilder();
    }

    public SinkBuilder sinkBuilder() {
        return new SinkBuilder();
    }

    private MergeEngine mergeEngine() {
        return options.get(MERGE_ENGINE);
    }

    private FileStore buildAppendOnlyStore() {
        FileStoreOptions fileStoreOptions = new FileStoreOptions(options);

        return new FileStoreImpl(
                fileStoreOptions.path(tableIdentifier).toString(),
                fileStoreOptions,
                WriteMode.APPEND_ONLY,
                user,
                TypeUtils.project(type, partitions),
                EMPTY_KEY_TYPE,
                type,
                null);
    }

    private FileStore buildLSMStore() {
        RowType partitionType = TypeUtils.project(type, partitions);
        FileStoreOptions fileStoreOptions = new FileStoreOptions(options);

        if (primaryKeys.length == 0) {
            return FileStoreImpl.createWithValueCount(
                    fileStoreOptions.path(tableIdentifier).toString(),
                    fileStoreOptions,
                    user,
                    partitionType,
                    type);
        } else {
            return FileStoreImpl.createWithPrimaryKey(
                    fileStoreOptions.path(tableIdentifier).toString(),
                    fileStoreOptions,
                    user,
                    partitionType,
                    TypeUtils.project(type, primaryKeys),
                    type,
                    mergeEngine());
        }
    }

    private FileStore buildFileStore() {
        WriteMode writeMode = options.get(TableStoreFactoryOptions.WRITE_MODE);

        switch (writeMode) {
            case CHANGE_LOG:
                return buildLSMStore();

            case APPEND_ONLY:
                return buildAppendOnlyStore();

            default:
                throw new UnsupportedOperationException("Unknown write mode: " + writeMode);
        }
    }

    private void adjustIndexAndValidate() {
        if (logPrimaryKeys.length > 0 && partitions.length > 0) {
            List<Integer> pkList =
                    Arrays.stream(logPrimaryKeys).boxed().collect(Collectors.toList());
            List<Integer> partitionList =
                    Arrays.stream(partitions).boxed().collect(Collectors.toList());

            String pkInfo =
                    type == null
                            ? pkList.toString()
                            : TypeUtils.project(type, logPrimaryKeys).getFieldNames().toString();
            String partitionInfo =
                    type == null
                            ? partitionList.toString()
                            : TypeUtils.project(type, partitions).getFieldNames().toString();
            Preconditions.checkState(
                    pkList.containsAll(partitionList),
                    String.format(
                            "Primary key constraint %s should include all partition fields %s",
                            pkInfo, partitionInfo));
            primaryKeys =
                    Arrays.stream(logPrimaryKeys)
                            .filter(pk -> !partitionList.contains(pk))
                            .toArray();

            Preconditions.checkState(
                    primaryKeys.length > 0,
                    String.format(
                            "Primary key constraint %s should not be same with partition fields %s, this will result in only one record in a partition",
                            pkInfo, partitionInfo));
        }
    }

    /** Source builder to build a flink {@link Source}. */
    public class SourceBuilder {

        private boolean isContinuous = false;

        private StreamExecutionEnvironment env;

        @Nullable private int[][] projectedFields;

        @Nullable private Predicate partitionPredicate;

        @Nullable private Predicate fieldPredicate;

        @Nullable private LogSourceProvider logSourceProvider;

        @Nullable private Integer parallelism;

        public SourceBuilder withEnv(StreamExecutionEnvironment env) {
            this.env = env;
            return this;
        }

        public SourceBuilder withProjection(int[][] projectedFields) {
            this.projectedFields = projectedFields;
            return this;
        }

        public SourceBuilder withPartitionPredicate(Predicate partitionPredicate) {
            this.partitionPredicate = partitionPredicate;
            return this;
        }

        public SourceBuilder withFieldPredicate(Predicate fieldPredicate) {
            this.fieldPredicate = fieldPredicate;
            return this;
        }

        public SourceBuilder withContinuousMode(boolean isContinuous) {
            this.isContinuous = isContinuous;
            return this;
        }

        public SourceBuilder withLogSourceProvider(LogSourceProvider logSourceProvider) {
            this.logSourceProvider = logSourceProvider;
            return this;
        }

        public SourceBuilder withParallelism(@Nullable Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        private long discoveryIntervalMills() {
            return options.get(CONTINUOUS_DISCOVERY_INTERVAL).toMillis();
        }

        private boolean getValueCountMode(WriteMode writeMode) {
            // Decide the value count mode based on the write mode and primary key definitions.
            boolean valueCountMode;
            switch (writeMode) {
                case APPEND_ONLY:
                    valueCountMode = false;
                    break;

                case CHANGE_LOG:
                    valueCountMode = primaryKeys.length == 0;
                    break;

                default:
                    throw new UnsupportedOperationException("Unknown write mode: " + writeMode);
            }
            return valueCountMode;
        }

        private FileStoreSource buildFileSource(
                boolean isContinuous, WriteMode writeMode, boolean continuousScanLatest) {

            return new FileStoreSource(
                    buildFileStore(),
                    writeMode,
                    getValueCountMode(writeMode),
                    isContinuous,
                    discoveryIntervalMills(),
                    continuousScanLatest,
                    projectedFields,
                    partitionPredicate,
                    fieldPredicate,
                    null);
        }

        private Source<RowData, ?, ?> buildSource() {
            WriteMode writeMode = options.get(TableStoreFactoryOptions.WRITE_MODE);
            if (isContinuous) {
                if (primaryKeys.length > 0 && mergeEngine() == PARTIAL_UPDATE) {
                    throw new ValidationException(
                            "Partial update continuous reading is not supported.");
                }

                LogStartupMode startupMode = logOptions().get(SCAN);
                if (logSourceProvider == null) {
                    return buildFileSource(true, writeMode, startupMode == LogStartupMode.LATEST);
                } else {
                    if (startupMode != LogStartupMode.FULL) {
                        return logSourceProvider.createSource(null);
                    }
                    return HybridSource.<RowData, StaticFileStoreSplitEnumerator>builder(
                                    buildFileSource(false, writeMode, false))
                            .addSource(
                                    new LogHybridSourceFactory(logSourceProvider),
                                    Boundedness.CONTINUOUS_UNBOUNDED)
                            .build();
                }
            } else {
                return buildFileSource(false, writeMode, false);
            }
        }

        public DataStreamSource<RowData> build() {
            if (env == null) {
                throw new IllegalArgumentException(
                        "StreamExecutionEnvironment should not be null.");
            }

            LogicalType produceType =
                    Optional.ofNullable(projectedFields)
                            .map(Projection::of)
                            .map(p -> p.project(type))
                            .orElse(type);
            DataStreamSource<RowData> dataStream =
                    env.fromSource(
                            buildSource(),
                            WatermarkStrategy.noWatermarks(),
                            tableIdentifier.asSummaryString(),
                            InternalTypeInfo.of(produceType));
            if (parallelism != null) {
                dataStream.setParallelism(parallelism);
            }
            return dataStream;
        }
    }

    /** Sink builder to build a flink sink from input. */
    public class SinkBuilder {

        private DataStream<RowData> input;

        @Nullable private CatalogLock.Factory lockFactory;

        @Nullable private Map<String, String> overwritePartition;

        @Nullable private LogSinkProvider logSinkProvider;

        @Nullable private Integer parallelism;

        private WriteMode writeMode = WriteMode.CHANGE_LOG;

        public SinkBuilder withInput(DataStream<RowData> input) {
            this.input = input;
            return this;
        }

        public SinkBuilder withLockFactory(CatalogLock.Factory lockFactory) {
            this.lockFactory = lockFactory;
            return this;
        }

        public SinkBuilder withOverwritePartition(Map<String, String> overwritePartition) {
            this.overwritePartition = overwritePartition;
            return this;
        }

        public SinkBuilder withLogSinkProvider(LogSinkProvider logSinkProvider) {
            this.logSinkProvider = logSinkProvider;
            return this;
        }

        public SinkBuilder withParallelism(@Nullable Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public SinkBuilder withWriteMode(WriteMode writeMode) {
            this.writeMode = writeMode;
            return this;
        }

        public DataStreamSink<?> build() {
            FileStore fileStore = buildFileStore();
            int numBucket = options.get(BUCKET);

            BucketStreamPartitioner partitioner =
                    new BucketStreamPartitioner(
                            numBucket, type, partitions, primaryKeys, logPrimaryKeys);
            PartitionTransformation<RowData> partitioned =
                    new PartitionTransformation<>(input.getTransformation(), partitioner);
            if (parallelism != null) {
                partitioned.setParallelism(parallelism);
            }

            StoreSink<?, ?> sink =
                    new StoreSink<>(
                            tableIdentifier,
                            fileStore,
                            writeMode,
                            partitions,
                            primaryKeys,
                            logPrimaryKeys,
                            numBucket,
                            lockFactory,
                            overwritePartition,
                            logSinkProvider);
            return GlobalCommittingSinkTranslator.translate(
                    new DataStream<>(input.getExecutionEnvironment(), partitioned), sink);
        }
    }
}
