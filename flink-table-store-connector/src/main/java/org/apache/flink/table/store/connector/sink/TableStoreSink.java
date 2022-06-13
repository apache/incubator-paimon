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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogLock;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.RequireCatalogLock;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.store.connector.TableStoreFactoryOptions;
import org.apache.flink.table.store.log.LogOptions;
import org.apache.flink.table.store.log.LogSinkProvider;
import org.apache.flink.table.store.log.LogStoreTableFactory;
import org.apache.flink.table.store.table.AppendOnlyFileStoreTable;
import org.apache.flink.table.store.table.ChangelogValueCountFileStoreTable;
import org.apache.flink.table.store.table.ChangelogWithKeyFileStoreTable;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/** Table sink to create {@link StoreSink}. */
public class TableStoreSink
        implements DynamicTableSink, SupportsOverwrite, SupportsPartitioning, RequireCatalogLock {

    private final ObjectIdentifier tableIdentifier;
    private final FileStoreTable table;
    private final DynamicTableFactory.Context logStoreContext;
    @Nullable private final LogStoreTableFactory logStoreTableFactory;

    private Map<String, String> staticPartitions = new HashMap<>();
    private boolean overwrite = false;
    @Nullable private CatalogLock.Factory lockFactory;

    public TableStoreSink(
            ObjectIdentifier tableIdentifier,
            FileStoreTable table,
            DynamicTableFactory.Context logStoreContext,
            @Nullable LogStoreTableFactory logStoreTableFactory) {
        this.tableIdentifier = tableIdentifier;
        this.table = table;
        this.logStoreContext = logStoreContext;
        this.logStoreTableFactory = logStoreTableFactory;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if (table instanceof AppendOnlyFileStoreTable) {
            return ChangelogMode.insertOnly();
        } else if (table instanceof ChangelogValueCountFileStoreTable) {
            // no primary key, sink all changelogs
            return requestedMode;
        } else if (table instanceof ChangelogWithKeyFileStoreTable) {
            Configuration logOptions =
                    new DelegatingConfiguration(
                            Configuration.fromMap(table.schema().options()), LogOptions.LOG_PREFIX);
            if (logOptions.get(LogOptions.CHANGELOG_MODE) != LogOptions.LogChangelogMode.ALL) {
                // with primary key, default sink upsert
                ChangelogMode.Builder builder = ChangelogMode.newBuilder();
                for (RowKind kind : requestedMode.getContainedKinds()) {
                    if (kind != RowKind.UPDATE_BEFORE) {
                        builder.addContainedKind(kind);
                    }
                }
                return builder.build();
            }

            // all changelog mode configured
            if (!requestedMode.contains(RowKind.UPDATE_BEFORE)
                    || !requestedMode.contains(RowKind.UPDATE_AFTER)) {
                throw new ValidationException(
                        "You cannot insert incomplete data into a table that "
                                + "has primary key and declares all changelog mode.");
            }
            return requestedMode;
        } else {
            throw new UnsupportedOperationException(
                    "Unknown FileStoreTable subclass " + table.getClass().getName());
        }
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        LogSinkProvider logSinkProvider = null;
        if (logStoreTableFactory != null) {
            logSinkProvider =
                    logStoreTableFactory.createSinkProvider(
                            logStoreContext,
                            new LogStoreTableFactory.SinkContext() {
                                @Override
                                public boolean isBounded() {
                                    return context.isBounded();
                                }

                                @Override
                                public <T> TypeInformation<T> createTypeInformation(
                                        DataType consumedDataType) {
                                    return context.createTypeInformation(consumedDataType);
                                }

                                @Override
                                public <T> TypeInformation<T> createTypeInformation(
                                        LogicalType consumedLogicalType) {
                                    return context.createTypeInformation(consumedLogicalType);
                                }

                                @Override
                                public DynamicTableSink.DataStructureConverter
                                        createDataStructureConverter(DataType consumedDataType) {
                                    return context.createDataStructureConverter(consumedDataType);
                                }
                            });
        }

        Configuration conf = Configuration.fromMap(table.schema().options());
        // Do not sink to log store when overwrite mode
        final LogSinkProvider finalLogSinkProvider =
                overwrite || conf.get(TableStoreFactoryOptions.COMPACTION_MANUAL_TRIGGERED)
                        ? null
                        : logSinkProvider;
        return (DataStreamSinkProvider)
                (providerContext, dataStream) ->
                        new FlinkSinkBuilder(tableIdentifier, table)
                                .withInput(
                                        new DataStream<>(
                                                dataStream.getExecutionEnvironment(),
                                                dataStream.getTransformation()))
                                .withLockFactory(lockFactory)
                                .withLogSinkProvider(finalLogSinkProvider)
                                .withOverwritePartition(overwrite ? staticPartitions : null)
                                .withParallelism(
                                        conf.get(TableStoreFactoryOptions.SINK_PARALLELISM))
                                .build();
    }

    @Override
    public DynamicTableSink copy() {
        TableStoreSink copied =
                new TableStoreSink(tableIdentifier, table, logStoreContext, logStoreTableFactory);
        copied.staticPartitions = new HashMap<>(staticPartitions);
        copied.overwrite = overwrite;
        copied.lockFactory = lockFactory;
        return copied;
    }

    @Override
    public String asSummaryString() {
        return "TableStoreSink";
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        table.schema()
                .partitionKeys()
                .forEach(
                        partitionKey -> {
                            if (partition.containsKey(partitionKey)) {
                                this.staticPartitions.put(
                                        partitionKey, partition.get(partitionKey));
                            }
                        });
    }

    @Override
    public void applyOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    @Override
    public void setLockFactory(@Nullable CatalogLock.Factory lockFactory) {
        this.lockFactory = lockFactory;
    }
}
