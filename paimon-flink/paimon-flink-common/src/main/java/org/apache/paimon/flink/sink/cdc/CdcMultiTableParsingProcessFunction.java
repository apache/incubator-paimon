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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link ProcessFunction} to parse CDC change event to either a list of {@link DataField}s or
 * {@link CdcRecord} and send them to different side outputs according to table name.
 *
 * <p>This {@link ProcessFunction} can handle records for different tables at the same time.
 *
 * @param <T> CDC change event type
 */
public class CdcMultiTableParsingProcessFunction<T> extends ProcessFunction<T, Void> {

    private final EventParser.Factory<T> parserFactory;
    private final Set<String> initialTables;
    private final String database;
    private final Catalog catalog;

    private transient EventParser<T> parser;
    private transient Map<String, OutputTag<List<DataField>>> updatedDataFieldsOutputTags;
    private transient Map<String, OutputTag<CdcRecord>> recordOutputTags;
    public static final OutputTag<CdcRecord> NEW_TABLE_OUTPUT_TAG =
            new OutputTag<>("paimon-newly-added-table", TypeInformation.of(CdcRecord.class));
    public static final OutputTag<Tuple2<Identifier, List<DataField>>>
            NEW_TABLE_SCHEMA_CHANGE_OUTPUT_TAG =
                    new OutputTag<>(
                            "paimon-newly-added-table-schema-change",
                            TypeInformation.of(
                                    new TypeHint<Tuple2<Identifier, List<DataField>>>() {}));

    public CdcMultiTableParsingProcessFunction(
            String database,
            Catalog catalog,
            List<FileStoreTable> tables,
            EventParser.Factory<T> parserFactory) {
        // for now, only support single database
        this.database = database;
        this.catalog = catalog;
        this.initialTables = tables.stream().map(FileStoreTable::name).collect(Collectors.toSet());
        this.parserFactory = parserFactory;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        parser = parserFactory.create();
        updatedDataFieldsOutputTags = new HashMap<>();
        recordOutputTags = new HashMap<>();
    }

    @Override
    public void processElement(T raw, Context context, Collector<Void> collector) throws Exception {
        parser.setRawEvent(raw);
        String tableName = parser.tableName();

        // CDC Ingestion only supports single database at this time being.
        //    In the future, there will be a mapping between source databases
        //    and target paimon databases
        String databaseName = parser.databaseName();

        if (parser.isUpdatedDataFields()) {
            // check for newly added table
            parser.getNewlyAddedTableSchema(database)
                    .ifPresent(
                            schema -> {
                                Identifier identifier =
                                        new Identifier(database, parser.tableName());
                                try {
                                    catalog.createTable(identifier, schema, true);
                                } catch (Throwable ignored) {
                                }
                            });
            parser.getUpdatedDataFields()
                    .ifPresent(
                            t -> {
                                if (isTableNewlyAdded(tableName)) {
                                    context.output(
                                            NEW_TABLE_SCHEMA_CHANGE_OUTPUT_TAG,
                                            Tuple2.of(Identifier.create(database, tableName), t));

                                } else {
                                    context.output(getUpdatedDataFieldsOutputTag(tableName), t);
                                }
                            });
        } else {
            for (CdcRecord record : parser.getRecords()) {
                // Get the output tag for a given table. Need to differentiate whether the table
                //     is newly discovered during runtime.
                if (isTableNewlyAdded(tableName)) {
                    context.output(NEW_TABLE_OUTPUT_TAG, wrapRecord(database, tableName, record));
                } else {
                    context.output(getRecordOutputTag(tableName), record);
                }
            }
        }
    }

    private CdcRecord wrapRecord(String databaseName, String tableName, CdcRecord record) {
        return MultiplexCdcRecord.fromCdcRecord(databaseName, tableName, record);
    }

    private OutputTag<List<DataField>> getUpdatedDataFieldsOutputTag(String tableName) {
        return updatedDataFieldsOutputTags.computeIfAbsent(
                tableName, CdcMultiTableParsingProcessFunction::createUpdatedDataFieldsOutputTag);
    }

    public static OutputTag<List<DataField>> createUpdatedDataFieldsOutputTag(String tableName) {
        return new OutputTag<>(
                "new-data-field-list-" + tableName, new ListTypeInfo<>(DataField.class));
    }

    private OutputTag<CdcRecord> getRecordOutputTag(String tableName) {
        return recordOutputTags.computeIfAbsent(
                tableName, CdcMultiTableParsingProcessFunction::createRecordOutputTag);
    }

    private boolean isTableNewlyAdded(String tableName) {
        return !initialTables.contains(tableName);
    }

    public static OutputTag<CdcRecord> createRecordOutputTag(String tableName) {
        return new OutputTag<>("record-" + tableName, TypeInformation.of(CdcRecord.class));
    }
}
