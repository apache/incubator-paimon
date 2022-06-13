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

package org.apache.flink.table.store.connector.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.mergetree.MergeTreeOptions;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.operation.FileStoreRead;
import org.apache.flink.table.store.file.operation.FileStoreReadImpl;
import org.apache.flink.table.store.file.operation.FileStoreWriteImpl;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.table.source.ValueContentRowDataRecordIterator;
import org.apache.flink.table.store.table.source.ValueCountRowDataRecordIterator;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static java.util.Collections.singletonList;

/** Util class to read and write data for source tests. */
public class TestChangelogDataReadWrite {

    private static final RowType KEY_TYPE =
            new RowType(singletonList(new RowType.RowField("k", new BigIntType())));
    private static final RowType VALUE_TYPE =
            new RowType(singletonList(new RowType.RowField("v", new BigIntType())));
    private static final Comparator<RowData> COMPARATOR =
            Comparator.comparingLong(o -> o.getLong(0));

    private final FileFormat avro;
    private final Path tablePath;
    private final FileStorePathFactory pathFactory;
    private final SnapshotManager snapshotManager;
    private final ExecutorService service;

    public TestChangelogDataReadWrite(String root, ExecutorService service) {
        this.avro =
                FileFormat.fromIdentifier(
                        Thread.currentThread().getContextClassLoader(),
                        "avro",
                        new Configuration());
        this.tablePath = new Path(root);
        this.pathFactory =
                new FileStorePathFactory(
                        tablePath,
                        RowType.of(new IntType()),
                        "default",
                        FileStoreOptions.FILE_FORMAT.defaultValue());
        this.snapshotManager = new SnapshotManager(new Path(root));
        this.service = service;
    }

    public TableRead createReadWithKey() {
        return createRead(ValueContentRowDataRecordIterator::new);
    }

    public TableRead createReadWithValueCount() {
        return createRead(it -> new ValueCountRowDataRecordIterator(it, null));
    }

    private TableRead createRead(
            Function<RecordReader.RecordIterator<KeyValue>, RecordReader.RecordIterator<RowData>>
                    rowDataIteratorCreator) {
        FileStoreRead read =
                new FileStoreReadImpl(
                        new SchemaManager(tablePath),
                        0,
                        WriteMode.CHANGE_LOG,
                        KEY_TYPE,
                        VALUE_TYPE,
                        COMPARATOR,
                        new DeduplicateMergeFunction(),
                        avro,
                        pathFactory);
        return new TableRead(read) {
            @Override
            public TableRead withProjection(int[][] projection) {
                throw new UnsupportedOperationException();
            }

            @Override
            public TableRead withIncremental(boolean isIncremental) {
                read.withDropDelete(!isIncremental);
                return this;
            }

            @Override
            protected RecordReader.RecordIterator<RowData> rowDataRecordIteratorFromKv(
                    RecordReader.RecordIterator<KeyValue> kvRecordIterator) {
                return rowDataIteratorCreator.apply(kvRecordIterator);
            }
        };
    }

    public List<DataFileMeta> writeFiles(
            BinaryRowData partition, int bucket, List<Tuple2<Long, Long>> kvs) throws Exception {
        Preconditions.checkNotNull(
                service, "ExecutorService must be provided if writeFiles is needed");
        RecordWriter writer = createMergeTreeWriter(partition, bucket);
        for (Tuple2<Long, Long> tuple2 : kvs) {
            writer.write(ValueKind.ADD, GenericRowData.of(tuple2.f0), GenericRowData.of(tuple2.f1));
        }
        List<DataFileMeta> files = writer.prepareCommit().newFiles();
        writer.close();
        return new ArrayList<>(files);
    }

    public RecordWriter createMergeTreeWriter(BinaryRowData partition, int bucket) {
        MergeTreeOptions options = new MergeTreeOptions(new Configuration());
        return new FileStoreWriteImpl(
                        WriteMode.CHANGE_LOG,
                        new SchemaManager(tablePath),
                        0,
                        KEY_TYPE,
                        VALUE_TYPE,
                        () -> COMPARATOR,
                        new DeduplicateMergeFunction(),
                        avro,
                        pathFactory,
                        snapshotManager,
                        null, // not used, we only create an empty writer
                        options)
                .createEmptyWriter(partition, bucket, service);
    }
}
