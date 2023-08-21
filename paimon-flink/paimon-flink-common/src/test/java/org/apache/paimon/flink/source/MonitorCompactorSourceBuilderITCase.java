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

package org.apache.paimon.flink.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.assertj.core.api.Assertions.assertThat;

/** this is a test. */
public class MonitorCompactorSourceBuilderITCase extends AbstractTestBase {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {
                        DataTypes.INT(), DataTypes.INT(), DataTypes.STRING(), DataTypes.INT()
                    },
                    new String[] {"k", "v", "dt", "hh"});

    private final DataFileMetaSerializer dataFileMetaSerializer = new DataFileMetaSerializer();

    private Path tablePath;
    private String commitUser;

    @BeforeEach
    public void before() throws IOException {
        tablePath = new Path(getTempDirPath());
        commitUser = UUID.randomUUID().toString();
    }

    @ParameterizedTest(name = "defaultOptions = {0}")
    @ValueSource(booleans = {true, false})
    public void testBatchRead(boolean defaultOptions) throws Exception {
        FileStoreTable table1 = createFileStoreTable("table1");
        FileStoreTable table2 = createFileStoreTable("table2");
        if (!defaultOptions) {
            // change options to test whether CompactorSourceBuilder work normally
            table1 = table1.copy(Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "2"));
            table2 = table2.copy(Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "3"));
        }
        StreamWriteBuilder streamWriteBuilder =
                table1.newStreamWriteBuilder().withCommitUser(commitUser);
        StreamTableWrite write = streamWriteBuilder.newWrite();
        StreamTableCommit commit = streamWriteBuilder.newCommit();

        StreamWriteBuilder streamWriteBuilder2 =
                table2.newStreamWriteBuilder().withCommitUser(commitUser);
        StreamTableWrite write2 = streamWriteBuilder2.newWrite();
        StreamTableCommit commit2 = streamWriteBuilder2.newCommit();

        write.write(rowData(1, 1510, BinaryString.fromString("20221208"), 15));
        write.write(rowData(2, 1620, BinaryString.fromString("20221208"), 16));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(1, 1511, BinaryString.fromString("20221208"), 15));
        write.write(rowData(1, 1510, BinaryString.fromString("20221209"), 15));
        commit.commit(1, write.prepareCommit(true, 1));

        write2.write(rowData(1, 1510, BinaryString.fromString("20221208"), 15));
        write2.write(rowData(2, 1620, BinaryString.fromString("20221208"), 16));
        commit2.commit(0, write2.prepareCommit(true, 0));

        write2.write(rowData(1, 1511, BinaryString.fromString("20221208"), 15));
        write2.write(rowData(1, 1510, BinaryString.fromString("20221209"), 15));
        commit2.commit(1, write2.prepareCommit(true, 1));

        write2.write(rowData(1, 1512, BinaryString.fromString("20221208"), 15));
        write2.write(rowData(1, 1510, BinaryString.fromString("20221209"), 16));
        commit2.commit(2, write2.prepareCommit(true, 2));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<RowData> compactorSource =
                new MonitorCompactorSourceBuilder(
                                "testdb", Stream.of(table1, table2).collect(Collectors.toList()))
                        .withContinuousMode(false)
                        .withEnv(env)
                        .build();
        CloseableIterator<RowData> it = compactorSource.executeAndCollect();

        List<String> actual = new ArrayList<>();
        while (it.hasNext()) {
            actual.add(toString2(it.next()));
        }
        assertThat(actual)
                .hasSameElementsAs(
                        Arrays.asList(
                                "+I 3|20221208|15|0|0|testdb|table2",
                                "+I 3|20221208|16|0|0|testdb|table2",
                                "+I 3|20221209|15|0|0|testdb|table2",
                                "+I 3|20221209|16|0|0|testdb|table2",
                                "+I 2|20221208|15|0|0|testdb|table1",
                                "+I 2|20221208|16|0|0|testdb|table1",
                                "+I 2|20221209|15|0|0|testdb|table1"));

        write.close();
        commit.close();
        it.close();
    }

    @ParameterizedTest(name = "defaultOptions = {0}")
    @ValueSource(booleans = {true, false})
    public void testStreamingRead(boolean defaultOptions) throws Exception {
        FileStoreTable table = createFileStoreTable("table1");
        FileStoreTable table2 = createFileStoreTable("table2");
        if (!defaultOptions) {
            // change options to test whether CompactorSourceBuilder work normally
            Map<String, String> dynamicOptions = new HashMap<>();
            dynamicOptions.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), "2");
            dynamicOptions.put(
                    CoreOptions.CHANGELOG_PRODUCER.key(),
                    CoreOptions.ChangelogProducer.NONE.toString());
            dynamicOptions.put(CoreOptions.SCAN_BOUNDED_WATERMARK.key(), "0");
            table = table.copy(dynamicOptions);

            table2 = table2.copy(dynamicOptions);
        }
        StreamWriteBuilder streamWriteBuilder =
                table.newStreamWriteBuilder().withCommitUser(commitUser);
        StreamTableWrite write = streamWriteBuilder.newWrite();
        StreamTableCommit commit = streamWriteBuilder.newCommit();

        StreamWriteBuilder streamWriteBuilder2 =
                table2.newStreamWriteBuilder().withCommitUser(commitUser);
        StreamTableWrite write2 = streamWriteBuilder2.newWrite();
        StreamTableCommit commit2 = streamWriteBuilder2.newCommit();

        write.write(rowData(1, 1510, BinaryString.fromString("20221208"), 15));
        write.write(rowData(2, 1620, BinaryString.fromString("20221208"), 16));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(1, 1511, BinaryString.fromString("20221208"), 15));
        write.write(rowData(1, 1510, BinaryString.fromString("20221209"), 15));
        write.compact(binaryRow("20221208", 15), 0, true);
        write.compact(binaryRow("20221209", 15), 0, true);
        commit.commit(1, write.prepareCommit(true, 1));

        write.write(rowData(2, 1520, BinaryString.fromString("20221208"), 15));
        write.write(rowData(2, 1621, BinaryString.fromString("20221208"), 16));
        commit.commit(2, write.prepareCommit(true, 2));

        write.write(rowData(1, 1512, BinaryString.fromString("20221208"), 15));
        write.write(rowData(2, 1620, BinaryString.fromString("20221209"), 16));
        commit.commit(3, write.prepareCommit(true, 3));

        // ************************ for table2

        write2.write(rowData(1, 1510, BinaryString.fromString("20221208"), 15));
        write2.write(rowData(2, 1620, BinaryString.fromString("20221208"), 16));
        commit2.commit(0, write2.prepareCommit(true, 0));

        write2.write(rowData(1, 1511, BinaryString.fromString("20221208"), 15));
        write2.write(rowData(1, 1510, BinaryString.fromString("20221209"), 15));
        write2.compact(binaryRow("20221208", 15), 0, true);
        write2.compact(binaryRow("20221209", 15), 0, true);
        commit2.commit(1, write2.prepareCommit(true, 1));

        write2.write(rowData(2, 1520, BinaryString.fromString("20221208"), 15));
        write2.write(rowData(2, 1621, BinaryString.fromString("20221208"), 16));
        commit2.commit(2, write2.prepareCommit(true, 2));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<RowData> compactorSource =
                new MonitorCompactorSourceBuilder(
                                "test", Stream.of(table, table2).collect(Collectors.toList()))
                        .withContinuousMode(true)
                        .withEnv(env)
                        .build();
        CloseableIterator<RowData> it = compactorSource.executeAndCollect();

        List<String> actual = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            actual.add(toString2(it.next()));
        }
        //
        assertThat(actual)
                .hasSameElementsAs(
                        Arrays.asList(
                                "+I 4|20221208|15|0|1|test|table1",
                                "+I 4|20221208|16|0|1|test|table1",
                                "+I 4|20221208|15|0|1|test|table2",
                                "+I 4|20221208|16|0|1|test|table2",
                                "+I 5|20221209|16|0|1|test|table1",
                                "+I 5|20221208|15|0|1|test|table1"));

        write.write(rowData(2, 1520, BinaryString.fromString("20221209"), 15));
        write.write(rowData(1, 1510, BinaryString.fromString("20221208"), 16));
        write.write(rowData(1, 1511, BinaryString.fromString("20221209"), 15));
        commit.commit(4, write.prepareCommit(true, 4));

        write2.write(rowData(2, 1520, BinaryString.fromString("20221209"), 15));
        write2.write(rowData(1, 1510, BinaryString.fromString("20221208"), 16));
        write2.write(rowData(1, 1511, BinaryString.fromString("20221209"), 15));
        commit2.commit(3, write2.prepareCommit(true, 3));

        actual.clear();
        for (int i = 0; i < 4; i++) {
            actual.add(toString2(it.next()));
        }
        assertThat(actual)
                .hasSameElementsAs(
                        Arrays.asList(
                                "+I 6|20221208|16|0|1|test|table1",
                                "+I 6|20221209|15|0|1|test|table1",
                                "+I 5|20221208|16|0|1|test|table2",
                                "+I 5|20221209|15|0|1|test|table2"));

        write.close();
        commit.close();
        it.close();
    }

    @Test
    public void testStreamingPartitionSpec() throws Exception {
        testPartitionSpec(
                true,
                getSpecifiedPartitions(),
                Arrays.asList(
                        "+I 1|20221208|16|0|1",
                        "+I 2|20221209|15|0|1",
                        "+I 3|20221208|16|0|1",
                        "+I 3|20221209|15|0|1"));
    }

    @Test
    public void testBatchPartitionSpec() throws Exception {
        testPartitionSpec(
                false,
                getSpecifiedPartitions(),
                Arrays.asList("+I 3|20221208|16|0|0", "+I 3|20221209|15|0|0"));
    }

    private List<Map<String, String>> getSpecifiedPartitions() {
        Map<String, String> partition1 = new HashMap<>();
        partition1.put("dt", "20221208");
        partition1.put("hh", "16");

        Map<String, String> partition2 = new HashMap<>();
        partition2.put("dt", "20221209");
        partition2.put("hh", "15");

        return Arrays.asList(partition1, partition2);
    }

    private void testPartitionSpec(
            boolean isStreaming,
            List<Map<String, String>> specifiedPartitions,
            List<String> expected)
            throws Exception {
        FileStoreTable table = createFileStoreTable();
        StreamWriteBuilder streamWriteBuilder =
                table.newStreamWriteBuilder().withCommitUser(commitUser);
        StreamTableWrite write = streamWriteBuilder.newWrite();
        StreamTableCommit commit = streamWriteBuilder.newCommit();

        write.write(rowData(1, 1510, BinaryString.fromString("20221208"), 15));
        write.write(rowData(2, 1620, BinaryString.fromString("20221208"), 16));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(2, 1520, BinaryString.fromString("20221208"), 15));
        write.write(rowData(2, 1520, BinaryString.fromString("20221209"), 15));
        commit.commit(1, write.prepareCommit(true, 1));

        write.write(rowData(1, 1511, BinaryString.fromString("20221208"), 15));
        write.write(rowData(1, 1610, BinaryString.fromString("20221208"), 16));
        write.write(rowData(1, 1510, BinaryString.fromString("20221209"), 15));
        commit.commit(2, write.prepareCommit(true, 2));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<RowData> compactorSource =
                new MonitorCompactorSourceBuilder("test", Collections.singletonList(table))
                        .withContinuousMode(isStreaming)
                        .withEnv(env)
                        .withPartitions(specifiedPartitions)
                        .build();
        CloseableIterator<RowData> it = compactorSource.executeAndCollect();

        List<String> actual = new ArrayList<>();
        for (int i = 0; i < expected.size(); i++) {
            actual.add(toString(it.next()));
        }
        assertThat(actual).hasSameElementsAs(expected);

        write.close();
        commit.close();
        it.close();
    }

    private String toString(RowData rowData) {
        int numFiles;
        try {
            numFiles = dataFileMetaSerializer.deserializeList(rowData.getBinary(3)).size();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        BinaryRow partition = deserializeBinaryRow(rowData.getBinary(1));

        return String.format(
                "%s %d|%s|%d|%d|%d",
                rowData.getRowKind().shortString(),
                rowData.getLong(0),
                partition.getString(0),
                partition.getInt(1),
                rowData.getInt(2),
                numFiles);
    }

    private String toString2(RowData rowData) {
        int numFiles;
        try {
            numFiles = dataFileMetaSerializer.deserializeList(rowData.getBinary(3)).size();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        BinaryRow partition = deserializeBinaryRow(rowData.getBinary(1));

        return String.format(
                "%s %d|%s|%d|%d|%d|%s|%s",
                rowData.getRowKind().shortString(),
                rowData.getLong(0),
                partition.getString(0),
                partition.getInt(1),
                rowData.getInt(2),
                numFiles,
                rowData.getString(4),
                rowData.getString(5));
    }

    private GenericRow rowData(Object... values) {
        return GenericRow.of(values);
    }

    private BinaryRow binaryRow(String dt, int hh) {
        BinaryRow b = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(b);
        writer.writeString(0, BinaryString.fromString(dt));
        writer.writeInt(1, hh);
        writer.complete();
        return b;
    }

    private FileStoreTable createFileStoreTable() throws Exception {
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);
        TableSchema tableSchema =
                schemaManager.createTable(
                        new Schema(
                                ROW_TYPE.getFields(),
                                Arrays.asList("dt", "hh"),
                                Arrays.asList("dt", "hh", "k"),
                                Collections.emptyMap(),
                                ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
    }

    private FileStoreTable createFileStoreTable(String path) throws Exception {
        path = tablePath.toString() + "/" + path;
        Path location = new Path(path);
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), location);
        TableSchema tableSchema =
                schemaManager.createTable(
                        new Schema(
                                ROW_TYPE.getFields(),
                                Arrays.asList("dt", "hh"),
                                Arrays.asList("dt", "hh", "k"),
                                Collections.emptyMap(),
                                ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), location, tableSchema);
    }
}
