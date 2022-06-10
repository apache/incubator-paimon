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

package org.apache.flink.table.store.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.FileStoreImpl;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.PartialUpdateMergeFunction;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.store.file.schema.Schema;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.table.store.table.sink.SinkRecord;
import org.apache.flink.table.store.table.sink.SinkRecordConverter;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.table.source.TableScan;
import org.apache.flink.table.store.table.source.ValueContentRowDataRecordIterator;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** {@link FileStoreTable} for {@link WriteMode#CHANGE_LOG} write mode with primary keys. */
public class ChangelogWithKeyFileStoreTable extends AbstractFileStoreTable {

    private static final long serialVersionUID = 1L;

    private final FileStoreImpl store;

    ChangelogWithKeyFileStoreTable(String name, Schema schema, String user) {
        super(name, schema);
        RowType rowType = schema.logicalRowType();

        // add _KEY_ prefix to avoid conflict with value
        RowType keyType =
                new RowType(
                        schema.logicalTrimmedPrimaryKeysType().getFields().stream()
                                .map(
                                        f ->
                                                new RowType.RowField(
                                                        "_KEY_" + f.getName(),
                                                        f.getType(),
                                                        f.getDescription().orElse(null)))
                                .collect(Collectors.toList()));

        Configuration conf = Configuration.fromMap(schema.options());

        FileStoreOptions.MergeEngine mergeEngine = conf.get(FileStoreOptions.MERGE_ENGINE);
        MergeFunction mergeFunction;
        switch (mergeEngine) {
            case DEDUPLICATE:
                mergeFunction = new DeduplicateMergeFunction();
                break;
            case PARTIAL_UPDATE:
                List<LogicalType> fieldTypes = rowType.getChildren();
                RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.size()];
                for (int i = 0; i < fieldTypes.size(); i++) {
                    fieldGetters[i] = RowData.createFieldGetter(fieldTypes.get(i), i);
                }
                mergeFunction = new PartialUpdateMergeFunction(fieldGetters);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported merge engine: " + mergeEngine);
        }

        this.store =
                new FileStoreImpl(
                        schema.id(),
                        new FileStoreOptions(conf),
                        WriteMode.CHANGE_LOG,
                        user,
                        schema.logicalPartitionType(),
                        keyType,
                        rowType,
                        mergeFunction);
    }

    @Override
    public TableScan newScan() {
        return new TableScan(store.newScan(), schema, store.pathFactory()) {
            @Override
            protected void withNonPartitionFilter(Predicate predicate) {
                // currently we can only perform filter push down on keys
                // consider this case:
                //   data file 1: insert key = a, value = 1
                //   data file 2: update key = a, value = 2
                //   filter: value = 1
                // if we perform filter push down on values, data file 1 will be chosen, but data
                // file 2 will be ignored, and the final result will be key = a, value = 1 while the
                // correct result is an empty set
                // TODO support value filter
                List<String> trimmedPrimaryKeys = schema.trimmedPrimaryKeys();
                int[] fieldIdxToKeyIdx =
                        schema.fields().stream()
                                .mapToInt(f -> trimmedPrimaryKeys.indexOf(f.name()))
                                .toArray();
                List<Predicate> keyFilters = new ArrayList<>();
                for (Predicate p : PredicateBuilder.splitAnd(predicate)) {
                    Optional<Predicate> mapped = mapFilterFields(p, fieldIdxToKeyIdx);
                    mapped.ifPresent(keyFilters::add);
                }
                if (keyFilters.size() > 0) {
                    scan.withKeyFilter(PredicateBuilder.and(keyFilters));
                }
            }
        };
    }

    @Override
    public TableRead newRead() {
        return new TableRead(store.newRead()) {
            @Override
            public TableRead withProjection(int[][] projection) {
                read.withValueProjection(projection);
                return this;
            }

            @Override
            public TableRead withIncremental(boolean isIncremental) {
                read.withDropDelete(!isIncremental);
                return this;
            }

            @Override
            protected RecordReader.RecordIterator<RowData> rowDataRecordIteratorFromKv(
                    RecordReader.RecordIterator<KeyValue> kvRecordIterator) {
                return new ValueContentRowDataRecordIterator(kvRecordIterator);
            }
        };
    }

    @Override
    public TableWrite newWrite() {
        SinkRecordConverter recordConverter =
                new SinkRecordConverter(store.options().bucket(), schema);
        return new TableWrite(store.newWrite(), recordConverter) {
            @Override
            protected void writeSinkRecord(SinkRecord record, RecordWriter writer)
                    throws Exception {
                switch (record.row().getRowKind()) {
                    case INSERT:
                    case UPDATE_AFTER:
                        writer.write(ValueKind.ADD, record.primaryKey(), record.row());
                        break;
                    case UPDATE_BEFORE:
                    case DELETE:
                        writer.write(ValueKind.DELETE, record.primaryKey(), record.row());
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unknown row kind " + record.row().getRowKind());
                }
            }
        };
    }

    @Override
    public FileStore store() {
        return store;
    }
}
