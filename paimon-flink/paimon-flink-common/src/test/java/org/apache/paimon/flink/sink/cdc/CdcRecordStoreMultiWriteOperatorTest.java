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
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.FileSystemCatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.MultiTableCommittableTypeInfo;
import org.apache.paimon.flink.sink.StoreSinkWriteImpl;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TraceableFileIO;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.JavaSerializer;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CdcRecordStoreMultiWriteOperator}. */
public class CdcRecordStoreMultiWriteOperatorTest {

    @TempDir java.nio.file.Path tempDir;

    private String commitUser;
    private Path warehouse;
    private String databaseName;
    private Identifier firstTable;
    private Catalog catalog;
    private Identifier secondTable;

    @BeforeEach
    public void before() throws Exception {
        warehouse = new Path(TraceableFileIO.SCHEME + "://" + tempDir.toString());
        commitUser = UUID.randomUUID().toString();

        databaseName = "test_db";
        firstTable = Identifier.create(databaseName, "test_table1");
        secondTable = Identifier.create(databaseName, "test_table2");

        catalog = createTestCatalog();
        catalog.createDatabase(databaseName, true);
        Options conf = new Options();
        conf.set(CdcRecordStoreWriteOperator.RETRY_SLEEP_TIME, Duration.ofMillis(10));

        RowType rowType1 =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"pt", "k", "v"});
        RowType rowType2 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.FLOAT(),
                            DataTypes.VARCHAR(5),
                            DataTypes.VARBINARY(5)
                        },
                        new String[] {"k", "v1", "v2", "v3", "v4"});

        createTestTables(
                catalog,
                Tuple2.of(
                        firstTable,
                        new Schema(
                                rowType1.getFields(),
                                Collections.singletonList("pt"),
                                Arrays.asList("pt", "k"),
                                conf.toMap(),
                                "")),
                Tuple2.of(
                        secondTable,
                        new Schema(
                                rowType2.getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("k"),
                                conf.toMap(),
                                "")));
    }

    private void createTestTables(Catalog catalog, Tuple2<Identifier, Schema>... tableSpecs)
            throws Exception {

        for (Tuple2<Identifier, Schema> spec : tableSpecs) {
            catalog.createTable(spec.f0, spec.f1, false);
        }
    }

    @AfterEach
    public void after() {
        // assert all connections are closed
        Predicate<Path> pathPredicate = path -> path.toString().contains(tempDir.toString());
        assertThat(TraceableFileIO.openInputStreams(pathPredicate)).isEmpty();
        assertThat(TraceableFileIO.openOutputStreams(pathPredicate)).isEmpty();
    }

    private static FileIO getFileIO(CatalogContext catalogContext, Path warehouse) {
        FileIO fileIO;
        try {
            fileIO = FileIO.get(warehouse, catalogContext);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return fileIO;
    }

    @Test
    @Timeout(30)
    public void testSingleTableAddColumn() throws Exception {

        Identifier tableId = firstTable;
        FileStoreTable table = (FileStoreTable) catalog.getTable(tableId);

        OneInputStreamOperatorTestHarness<CdcMultiplexRecord, Committable> harness =
                createTestHarness(catalog);

        harness.open();

        Runner runner = new Runner(harness);
        Thread t = new Thread(runner);
        t.start();

        // check that records with compatible schema can be processed immediately

        Map<String, String> fields = new HashMap<>();
        fields.put("pt", "0");
        fields.put("k", "1");
        fields.put("v", "10");

        CdcMultiplexRecord expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        tableId.getObjectName(),
                        new CdcRecord(RowKind.INSERT, fields));
        runner.offer(expected);
        CdcMultiplexRecord actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        fields = new HashMap<>();
        fields.put("pt", "0");
        fields.put("k", "2");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        tableId.getObjectName(),
                        new CdcRecord(RowKind.INSERT, fields));
        runner.offer(expected);
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // check that records with new fields should be processed after schema is updated

        fields = new HashMap<>();
        fields.put("pt", "0");
        fields.put("k", "3");
        fields.put("v", "30");
        fields.put("v2", "300");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        tableId.getObjectName(),
                        new CdcRecord(RowKind.INSERT, fields));
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        schemaManager.commitChanges(SchemaChange.addColumn("v2", DataTypes.INT()));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        runner.stop();
        t.join();
        harness.close();
    }

    private Catalog createTestCatalog() {

        CatalogContext catalogContext = createCatalogContext(warehouse);
        FileIO fileIO = getFileIO(catalogContext, warehouse);
        return new FileSystemCatalogFactory().create(fileIO, warehouse, catalogContext);
    }

    private CatalogContext createCatalogContext(Path warehouse) {
        Options conf = new Options();
        conf.set(CatalogOptions.WAREHOUSE, warehouse.getPath());
        conf.set(CatalogOptions.URI, "");

        // create CatalogContext using the options
        return CatalogContext.create(conf);
    }

    @Test
    @Timeout(30)
    public void testSingleTableUpdateColumnType() throws Exception {
        Identifier tableId = secondTable;
        FileStoreTable table = (FileStoreTable) catalog.getTable(tableId);

        OneInputStreamOperatorTestHarness<CdcMultiplexRecord, Committable> harness =
                createTestHarness(catalog);
        harness.open();

        Runner runner = new Runner(harness);
        Thread t = new Thread(runner);
        t.start();

        // check that records with compatible schema can be processed immediately

        Map<String, String> fields = new HashMap<>();
        fields.put("k", "1");
        fields.put("v1", "10");
        fields.put("v2", "0.625");
        fields.put("v3", "one");
        fields.put("v4", "b_one");
        CdcMultiplexRecord expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        tableId.getObjectName(),
                        new CdcRecord(RowKind.INSERT, fields));
        runner.offer(expected);
        CdcMultiplexRecord actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // check that records with new fields should be processed after schema is updated

        // int -> bigint

        fields = new HashMap<>();
        fields.put("k", "2");
        fields.put("v1", "12345678987654321");
        fields.put("v2", "0.25");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        tableId.getObjectName(),
                        new CdcRecord(RowKind.INSERT, fields));
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        schemaManager.commitChanges(SchemaChange.updateColumnType("v1", DataTypes.BIGINT()));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // float -> double

        fields = new HashMap<>();
        fields.put("k", "3");
        fields.put("v1", "100");
        fields.put("v2", "1.0000000000009095");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        tableId.getObjectName(),
                        new CdcRecord(RowKind.INSERT, fields));
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        schemaManager.commitChanges(SchemaChange.updateColumnType("v2", DataTypes.DOUBLE()));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // varchar(5) -> varchar(10)

        fields = new HashMap<>();
        fields.put("k", "4");
        fields.put("v1", "40");
        fields.put("v3", "long four");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        tableId.getObjectName(),
                        new CdcRecord(RowKind.INSERT, fields));
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        schemaManager.commitChanges(SchemaChange.updateColumnType("v3", DataTypes.VARCHAR(10)));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // varbinary(5) -> varbinary(10)

        fields = new HashMap<>();
        fields.put("k", "5");
        fields.put("v1", "50");
        fields.put("v4", "long five~");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        tableId.getObjectName(),
                        new CdcRecord(RowKind.INSERT, fields));
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        schemaManager.commitChanges(SchemaChange.updateColumnType("v4", DataTypes.VARBINARY(10)));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        runner.stop();
        t.join();
        harness.close();
    }

    @Test
    @Timeout(30)
    public void testMultiTableUpdateColumnType() throws Exception {
        FileStoreTable table1 = (FileStoreTable) catalog.getTable(firstTable);
        FileStoreTable table2 = (FileStoreTable) catalog.getTable(secondTable);

        OneInputStreamOperatorTestHarness<CdcMultiplexRecord, Committable> harness =
                createTestHarness(catalog);
        harness.open();

        Runner runner = new Runner(harness);
        Thread t = new Thread(runner);
        t.start();

        // check that records with compatible schema from different tables
        //     can be processed immediately

        Map<String, String> fields;

        // first table record
        fields = new HashMap<>();
        fields.put("pt", "0");
        fields.put("k", "1");
        fields.put("v", "10");

        CdcMultiplexRecord expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        firstTable.getObjectName(),
                        new CdcRecord(RowKind.INSERT, fields));
        runner.offer(expected);
        CdcMultiplexRecord actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // second table record
        fields = new HashMap<>();
        fields.put("k", "1");
        fields.put("v1", "10");
        fields.put("v2", "0.625");
        fields.put("v3", "one");
        fields.put("v4", "b_one");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        secondTable.getObjectName(),
                        new CdcRecord(RowKind.INSERT, fields));
        runner.offer(expected);
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // check that records with new fields should be processed after schema is updated

        // int -> bigint
        SchemaManager schemaManager;
        // first table
        fields = new HashMap<>();
        fields.put("pt", "1");
        fields.put("k", "123456789876543211");
        fields.put("v", "varchar");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        firstTable.getObjectName(),
                        new CdcRecord(RowKind.INSERT, fields));
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        schemaManager = new SchemaManager(table1.fileIO(), table1.location());
        schemaManager.commitChanges(SchemaChange.updateColumnType("k", DataTypes.BIGINT()));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // second table
        fields = new HashMap<>();
        fields.put("k", "2");
        fields.put("v1", "12345678987654321");
        fields.put("v2", "0.25");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        secondTable.getObjectName(),
                        new CdcRecord(RowKind.INSERT, fields));
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        schemaManager = new SchemaManager(table2.fileIO(), table2.location());
        schemaManager.commitChanges(SchemaChange.updateColumnType("v1", DataTypes.BIGINT()));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // below are schema changes only from the second table
        // float -> double

        fields = new HashMap<>();
        fields.put("k", "3");
        fields.put("v1", "100");
        fields.put("v2", "1.0000000000009095");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        secondTable.getObjectName(),
                        new CdcRecord(RowKind.INSERT, fields));
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        schemaManager = new SchemaManager(table2.fileIO(), table2.location());
        schemaManager.commitChanges(SchemaChange.updateColumnType("v2", DataTypes.DOUBLE()));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // varchar(5) -> varchar(10)

        fields = new HashMap<>();
        fields.put("k", "4");
        fields.put("v1", "40");
        fields.put("v3", "long four");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        secondTable.getObjectName(),
                        new CdcRecord(RowKind.INSERT, fields));
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        schemaManager = new SchemaManager(table2.fileIO(), table2.location());
        schemaManager.commitChanges(SchemaChange.updateColumnType("v3", DataTypes.VARCHAR(10)));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // varbinary(5) -> varbinary(10)

        fields = new HashMap<>();
        fields.put("k", "5");
        fields.put("v1", "50");
        fields.put("v4", "long five~");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        secondTable.getObjectName(),
                        new CdcRecord(RowKind.INSERT, fields));
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        schemaManager.commitChanges(SchemaChange.updateColumnType("v4", DataTypes.VARBINARY(10)));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        runner.stop();
        t.join();
        harness.close();
    }

    private OneInputStreamOperatorTestHarness<CdcMultiplexRecord, Committable> createTestHarness(
            Catalog catalog) throws Exception {
        CdcRecordStoreMultiWriteOperator operator =
                new CdcRecordStoreMultiWriteOperator(
                        catalog,
                        (t, commitUser, state, ioManager) ->
                                new StoreSinkWriteImpl(
                                        t, commitUser, state, ioManager, false, false),
                        commitUser);
        TypeSerializer<CdcMultiplexRecord> inputSerializer = new JavaSerializer<>();
        TypeSerializer<Committable> outputSerializer =
                new MultiTableCommittableTypeInfo().createSerializer(new ExecutionConfig());
        OneInputStreamOperatorTestHarness<CdcMultiplexRecord, Committable> harness =
                new OneInputStreamOperatorTestHarness<>(operator, inputSerializer);
        harness.setup(outputSerializer);
        return harness;
    }

    private static class Runner implements Runnable {

        private final OneInputStreamOperatorTestHarness<CdcMultiplexRecord, Committable> harness;
        private final BlockingQueue<CdcMultiplexRecord> toProcess = new LinkedBlockingQueue<>();
        private final BlockingQueue<CdcMultiplexRecord> processed = new LinkedBlockingQueue<>();
        private final AtomicBoolean running = new AtomicBoolean(true);

        private Runner(OneInputStreamOperatorTestHarness<CdcMultiplexRecord, Committable> harness) {
            this.harness = harness;
        }

        private void offer(CdcMultiplexRecord record) {
            toProcess.offer(record);
        }

        private CdcMultiplexRecord take() throws Exception {
            return processed.take();
        }

        private CdcMultiplexRecord poll(long seconds) throws Exception {
            return processed.poll(seconds, TimeUnit.SECONDS);
        }

        private void stop() {
            running.set(false);
        }

        @Override
        public void run() {
            long timestamp = 0;
            try {
                while (running.get()) {
                    if (toProcess.isEmpty()) {
                        Thread.sleep(10);
                        continue;
                    }

                    CdcMultiplexRecord record = toProcess.poll();
                    harness.processElement(record, ++timestamp);
                    processed.offer(record);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
