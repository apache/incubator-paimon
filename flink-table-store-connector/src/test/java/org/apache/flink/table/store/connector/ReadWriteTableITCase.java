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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.store.connector.sink.TableStoreSink;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.utils.BlockingIterator;
import org.apache.flink.table.store.kafka.KafkaTableTestBase;
import org.apache.flink.table.store.log.LogOptions;
import org.apache.flink.types.Row;

import org.junit.Test;

import javax.annotation.Nullable;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.flink.table.planner.factories.TestValuesTableFactory.registerData;
import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.LOG_SYSTEM;
import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.SCAN_PARALLELISM;
import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.SINK_PARALLELISM;
import static org.apache.flink.table.store.kafka.KafkaLogOptions.BOOTSTRAP_SERVERS;
import static org.apache.flink.table.store.log.LogOptions.LOG_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for managed table dml. */
public class ReadWriteTableITCase extends KafkaTableTestBase {

    private String rootPath;

    @Test
    public void testBatchWriteWithPartitionedRecordsWithPk() throws Exception {
        // input is dailyRates()
        List<Row> expectedRecords =
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02"));
        // test batch read
        String managedTable =
                collectAndCheckBatchReadWrite(
                        true, true, null, Collections.emptyList(), expectedRecords);
        checkFileStorePath(tEnv, managedTable);

        // test streaming read
        final StreamTableEnvironment streamTableEnv =
                StreamTableEnvironment.create(buildStreamEnv());
        registerTable(streamTableEnv, managedTable);
        BlockingIterator<Row, Row> streamIter =
                collectAndCheck(
                        streamTableEnv, managedTable, Collections.emptyMap(), expectedRecords);

        // overwrite static partition 2022-01-02
        prepareEnvAndOverwrite(
                managedTable,
                Collections.singletonMap("dt", "'2022-01-02'"),
                Arrays.asList(new String[] {"'Euro'", "100"}, new String[] {"'Yen'", "1"}));

        // streaming iter will not receive any changelog
        assertNoMoreRecords(streamIter);

        // batch read to check partition refresh
        collectAndCheck(
                        tEnv,
                        managedTable,
                        Collections.emptyMap(),
                        Arrays.asList(
                                // part = 2022-01-01
                                changelogRow("+I", "US Dollar", 114L, "2022-01-01"),
                                changelogRow("+I", "Yen", 1L, "2022-01-01"),
                                changelogRow("+I", "Euro", 114L, "2022-01-01"),
                                // part = 2022-01-02
                                changelogRow("+I", "Euro", 100L, "2022-01-02"),
                                changelogRow("+I", "Yen", 1L, "2022-01-02")))
                .close();

        // test partition filter
        List<Row> expected =
                Arrays.asList(
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01"));
        collectAndCheckBatchReadWrite(
                true, true, "dt <> '2022-01-02'", Collections.emptyList(), expected);

        collectAndCheckBatchReadWrite(
                true, true, "dt = '2022-01-01'", Collections.emptyList(), expected);

        // test field filter
        collectAndCheckBatchReadWrite(
                true,
                true,
                "rate >= 100",
                Collections.emptyList(),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02")));

        // test partition and field filter
        collectAndCheckBatchReadWrite(
                true,
                true,
                "rate >= 100 AND dt = '2022-01-02'",
                Collections.emptyList(),
                Collections.singletonList(changelogRow("+I", "Euro", 119L, "2022-01-02")));

        // test projection
        collectAndCheckBatchReadWrite(
                true,
                true,
                null,
                Collections.singletonList("dt"),
                Arrays.asList(
                        changelogRow("+I", "2022-01-01"), // US Dollar
                        changelogRow("+I", "2022-01-01"), // Yen
                        changelogRow("+I", "2022-01-01"), // Euro
                        changelogRow("+I", "2022-01-02"))); // Euro

        collectAndCheckBatchReadWrite(
                true,
                true,
                null,
                Collections.singletonList("dt, currency, rate"),
                Arrays.asList(
                        changelogRow("+I", "2022-01-01", "US Dollar", 114L), // US Dollar
                        changelogRow("+I", "2022-01-01", "Yen", 1L), // Yen
                        changelogRow("+I", "2022-01-01", "Euro", 114L), // Euro
                        changelogRow("+I", "2022-01-02", "Euro", 119L))); // Euro

        // test projection and filter
        collectAndCheckBatchReadWrite(
                true,
                true,
                "rate = 114",
                Collections.singletonList("rate"),
                Arrays.asList(
                        changelogRow("+I", 114L), // US Dollar
                        changelogRow("+I", 114L) // Euro
                        ));
    }

    @Test
    public void testBatchWriteWithPartitionedRecordsWithoutPk() throws Exception {
        // input is dailyRates()

        // test batch read
        String managedTable =
                collectAndCheckBatchReadWrite(
                        true, false, null, Collections.emptyList(), dailyRates());
        checkFileStorePath(tEnv, managedTable);

        // overwrite dynamic partition
        prepareEnvAndOverwrite(
                managedTable,
                Collections.emptyMap(),
                Arrays.asList(
                        new String[] {"'Euro'", "90", "'2022-01-01'"},
                        new String[] {"'Yen'", "2", "'2022-01-02'"}));

        // test streaming read
        final StreamTableEnvironment streamTableEnv =
                StreamTableEnvironment.create(buildStreamEnv());
        registerTable(streamTableEnv, managedTable);
        collectAndCheck(
                        streamTableEnv,
                        managedTable,
                        Collections.emptyMap(),
                        Arrays.asList(
                                // part = 2022-01-01
                                changelogRow("+I", "Euro", 90L, "2022-01-01"),
                                // part = 2022-01-02
                                changelogRow("+I", "Yen", 2L, "2022-01-02")))
                .close();

        // test partition filter
        collectAndCheckBatchReadWrite(
                true, false, "dt >= '2022-01-01'", Collections.emptyList(), dailyRates());

        // test field filter
        collectAndCheckBatchReadWrite(
                true,
                false,
                "currency = 'US Dollar'",
                Collections.emptyList(),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 114L, "2022-01-01")));

        // test partition and field filter
        collectAndCheckBatchReadWrite(
                true,
                false,
                "dt = '2022-01-01' OR rate > 115",
                Collections.emptyList(),
                dailyRates());

        // test projection
        collectAndCheckBatchReadWrite(
                true,
                false,
                null,
                Collections.singletonList("currency"),
                Arrays.asList(
                        changelogRow("+I", "US Dollar"),
                        changelogRow("+I", "US Dollar"),
                        changelogRow("+I", "Yen"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Euro")));

        // test projection and filter
        collectAndCheckBatchReadWrite(
                true,
                false,
                "rate = 119",
                Arrays.asList("currency", "dt"),
                Collections.singletonList(changelogRow("+I", "Euro", "2022-01-02")));
    }

    @Test
    public void testBatchWriteWithNonPartitionedRecordsWithPk() throws Exception {
        // input is rates()
        String managedTable =
                collectAndCheckBatchReadWrite(
                        false,
                        true,
                        null,
                        Collections.emptyList(),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L),
                                changelogRow("+I", "Yen", 1L),
                                changelogRow("+I", "Euro", 119L)));
        checkFileStorePath(tEnv, managedTable);

        // overwrite the whole table
        prepareEnvAndOverwrite(
                managedTable,
                Collections.emptyMap(),
                Collections.singletonList(new String[] {"'Euro'", "100"}));
        collectAndCheck(
                tEnv,
                managedTable,
                Collections.emptyMap(),
                Collections.singletonList(changelogRow("+I", "Euro", 100L)));

        // test field filter
        collectAndCheckBatchReadWrite(
                false,
                true,
                "currency = 'Euro'",
                Collections.emptyList(),
                Collections.singletonList(changelogRow("+I", "Euro", 119L)));

        collectAndCheckBatchReadWrite(
                false,
                true,
                "119 >= rate AND 102 < rate",
                Collections.emptyList(),
                Collections.singletonList(changelogRow("+I", "Euro", 119L)));

        // test projection
        collectAndCheckBatchReadWrite(
                false,
                true,
                null,
                Arrays.asList("rate", "currency"),
                Arrays.asList(
                        changelogRow("+I", 102L, "US Dollar"),
                        changelogRow("+I", 1L, "Yen"),
                        changelogRow("+I", 119L, "Euro")));

        // test projection and filter
        collectAndCheckBatchReadWrite(
                false,
                true,
                "currency = 'Yen'",
                Collections.singletonList("rate"),
                Collections.singletonList(changelogRow("+I", 1L)));
    }

    @Test
    public void testBatchWriteNonPartitionedRecordsWithoutPk() throws Exception {
        // input is rates()
        String managedTable =
                collectAndCheckBatchReadWrite(false, false, null, Collections.emptyList(), rates());
        checkFileStorePath(tEnv, managedTable);

        // test field filter
        collectAndCheckBatchReadWrite(
                false,
                false,
                "currency = 'Euro'",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Euro", 119L)));

        collectAndCheckBatchReadWrite(false, false, "rate >= 1", Collections.emptyList(), rates());

        // test projection
        collectAndCheckBatchReadWrite(
                false,
                false,
                null,
                Collections.singletonList("currency"),
                Arrays.asList(
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Yen"),
                        changelogRow("+I", "US Dollar")));

        // test projection and filter
        collectAndCheckBatchReadWrite(
                false,
                false,
                "rate > 100 OR currency = 'Yen'",
                Collections.singletonList("currency"),
                Arrays.asList(
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Yen"),
                        changelogRow("+I", "US Dollar")));
    }

    @Test
    public void testEnableLogAndStreamingReadWritePartitionedRecordsWithPk() throws Exception {
        // input is dailyRatesChangelogWithoutUB()
        // test hybrid read
        Tuple2<String, BlockingIterator<Row, Row>> tuple =
                collectAndCheckStreamingReadWriteWithoutClose(
                        Collections.emptyMap(),
                        "dt >= '2022-01-01' AND dt <= '2022-01-03' OR currency = 'HK Dollar'",
                        Collections.emptyList(),
                        Arrays.asList(
                                // part = 2022-01-01
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                                // part = 2022-01-02
                                changelogRow("+I", "Euro", 119L, "2022-01-02")));
        String managedTable = tuple.f0;
        checkFileStorePath(tEnv, managedTable);
        BlockingIterator<Row, Row> streamIter = tuple.f1;

        // test log store in hybrid mode accepts all filters
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO `%s` PARTITION (dt = '2022-01-03')\n"
                                        + "VALUES('HK Dollar', 100), ('Yen', 20)\n",
                                managedTable))
                .await();

        tEnv.executeSql(
                        String.format(
                                "INSERT INTO `%s` PARTITION (dt = '2022-01-04')\n"
                                        + "VALUES('Yen', 20)\n",
                                managedTable))
                .await();

        assertThat(streamIter.collect(2, 10, TimeUnit.SECONDS))
                .containsExactlyInAnyOrderElementsOf(
                        Arrays.asList(
                                changelogRow("+I", "HK Dollar", 100L, "2022-01-03"),
                                changelogRow("+I", "Yen", 20L, "2022-01-03")));

        // overwrite partition 2022-01-02
        prepareEnvAndOverwrite(
                managedTable,
                Collections.singletonMap("dt", "'2022-01-02'"),
                Arrays.asList(new String[] {"'Euro'", "100"}, new String[] {"'Yen'", "1"}));

        // batch read to check data refresh
        final StreamTableEnvironment batchTableEnv =
                StreamTableEnvironment.create(buildBatchEnv(), EnvironmentSettings.inBatchMode());
        registerTable(batchTableEnv, managedTable);
        collectAndCheck(
                batchTableEnv,
                managedTable,
                Collections.emptyMap(),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 100L, "2022-01-02"),
                        changelogRow("+I", "Yen", 1L, "2022-01-02"),
                        // part = 2022-01-03
                        changelogRow("+I", "HK Dollar", 100L, "2022-01-03"),
                        changelogRow("+I", "Yen", 20L, "2022-01-03"),
                        // part = 2022-01-04
                        changelogRow("+I", "Yen", 20L, "2022-01-04")));

        // check no changelog generated for streaming read
        assertNoMoreRecords(streamIter);

        // filter on partition
        collectAndCheckStreamingReadWriteWithClose(
                true,
                true,
                true,
                Collections.emptyMap(),
                "dt = '2022-01-01'",
                Collections.emptyList(),
                Collections.singletonList(changelogRow("+I", "US Dollar", 102L, "2022-01-01")));

        // test field filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                true,
                true,
                Collections.emptyMap(),
                "currency = 'US Dollar'",
                Collections.emptyList(),
                Collections.singletonList(changelogRow("+I", "US Dollar", 102L, "2022-01-01")));

        // test partition and field filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                true,
                true,
                Collections.emptyMap(),
                "dt = '2022-01-01' AND rate = 1",
                Collections.emptyList(),
                Collections.emptyList());

        // test projection and filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                true,
                true,
                Collections.emptyMap(),
                "dt = '2022-01-02' AND currency = 'Euro'",
                Arrays.asList("rate", "dt", "currency"),
                Collections.singletonList(changelogRow("+I", 119L, "2022-01-02", "Euro")));
    }

    @Test
    public void testDisableLogAndStreamingReadWritePartitionedRecordsWithPk() throws Exception {
        // input is dailyRatesChangelogWithoutUB()
        // file store continuous read
        // will not merge, at least collect two records
        checkFileStorePath(
                tEnv,
                collectAndCheckStreamingReadWriteWithClose(
                        false,
                        true,
                        true,
                        Collections.emptyMap(),
                        null,
                        Collections.emptyList(),
                        Arrays.asList(
                                // part = 2022-01-01
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                                // part = 2022-01-02
                                changelogRow("+I", "Euro", 119L, "2022-01-02"))));

        // test partition filter
        collectAndCheckStreamingReadWriteWithClose(
                false,
                true,
                true,
                Collections.emptyMap(),
                "dt < '2022-01-02'",
                Collections.emptyList(),
                Collections.singletonList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01")));

        // test field filter
        collectAndCheckStreamingReadWriteWithClose(
                false,
                true,
                true,
                Collections.emptyMap(),
                "rate = 102",
                Collections.emptyList(),
                Collections.singletonList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01")));

        // test partition and field filter
        collectAndCheckStreamingReadWriteWithClose(
                false,
                true,
                true,
                Collections.emptyMap(),
                "rate = 102 or dt < '2022-01-02'",
                Collections.emptyList(),
                Collections.singletonList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01")));

        // test projection and filter
        collectAndCheckStreamingReadWriteWithClose(
                false,
                true,
                true,
                Collections.emptyMap(),
                "rate = 102 or dt < '2022-01-02'",
                Collections.singletonList("currency"),
                Collections.singletonList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar")));
    }

    @Test
    public void testStreamingReadWritePartitionedRecordsWithoutPk() throws Exception {
        // input is dailyRatesChangelogWithUB()
        // enable log store, file store bounded read with merge
        checkFileStorePath(
                tEnv,
                collectAndCheckStreamingReadWriteWithClose(
                        true,
                        true,
                        false,
                        Collections.emptyMap(),
                        null,
                        Collections.emptyList(),
                        Arrays.asList(
                                // part = 2022-01-01
                                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                                // part = 2022-01-02
                                changelogRow("+I", "Euro", 115L, "2022-01-02"))));

        // test partition filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                true,
                false,
                Collections.emptyMap(),
                "dt IS NOT NULL",
                Collections.emptyList(),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 115L, "2022-01-02")));

        collectAndCheckStreamingReadWriteWithClose(
                true,
                true,
                false,
                Collections.emptyMap(),
                "dt IS NULL",
                Collections.emptyList(),
                Collections.emptyList());

        // test field filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                true,
                false,
                Collections.emptyMap(),
                "currency = 'US Dollar' OR rate = 115",
                Collections.emptyList(),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 115L, "2022-01-02")));

        // test partition and field filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                true,
                false,
                Collections.emptyMap(),
                "(dt = '2022-01-02' AND currency = 'US Dollar') OR (dt = '2022-01-01' AND rate = 115)",
                Collections.emptyList(),
                Collections.emptyList());

        // test projection
        collectAndCheckStreamingReadWriteWithClose(
                true,
                true,
                false,
                Collections.emptyMap(),
                null,
                Collections.singletonList("rate"),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", 102L),
                        // part = 2022-01-02
                        changelogRow("+I", 115L)));

        // test projection and filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                true,
                false,
                Collections.emptyMap(),
                "dt <> '2022-01-01'",
                Collections.singletonList("rate"),
                Collections.singletonList(
                        // part = 2022-01-02, Euro
                        changelogRow("+I", 115L)));
    }

    @Test
    public void testStreamingReadWriteNonPartitionedRecordsWithPk() throws Exception {
        // input is ratesChangelogWithoutUB()
        // enable log store, file store bounded read with merge
        checkFileStorePath(
                tEnv,
                collectAndCheckStreamingReadWriteWithClose(
                        true,
                        false,
                        true,
                        Collections.emptyMap(),
                        null,
                        Collections.emptyList(),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L),
                                changelogRow("+I", "Euro", 119L))));

        // test field filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                false,
                true,
                Collections.emptyMap(),
                "currency = 'Yen'",
                Collections.emptyList(),
                Collections.emptyList());

        // test projection
        collectAndCheckStreamingReadWriteWithClose(
                true,
                false,
                true,
                Collections.emptyMap(),
                null,
                Collections.singletonList("currency"),
                Arrays.asList(changelogRow("+I", "US Dollar"), changelogRow("+I", "Euro")));

        // test projection and filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                false,
                true,
                Collections.emptyMap(),
                "rate = 102",
                Collections.singletonList("currency"),
                Collections.singletonList(changelogRow("+I", "US Dollar")));
    }

    @Test
    public void testStreamingReadWriteNonPartitionedRecordsWithoutPk() throws Exception {
        // input is ratesChangelogWithUB()
        // enable log store, with default full scan mode, will merge
        checkFileStorePath(
                tEnv,
                collectAndCheckStreamingReadWriteWithClose(
                        true,
                        false,
                        false,
                        Collections.emptyMap(),
                        null,
                        Collections.emptyList(),
                        Arrays.asList(
                                changelogRow("+I", "US Dollar", 102L),
                                changelogRow("+I", "Euro", 119L),
                                changelogRow("+I", null, 100L),
                                changelogRow("+I", "HK Dollar", null))));

        // test field filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                false,
                false,
                Collections.emptyMap(),
                "currency IS NOT NULL",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 119L),
                        changelogRow("+I", "HK Dollar", null)));

        collectAndCheckStreamingReadWriteWithClose(
                true,
                false,
                false,
                Collections.emptyMap(),
                "rate IS NOT NULL",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 119L),
                        changelogRow("+I", null, 100L)));

        // test projection and filter
        collectAndCheckStreamingReadWriteWithClose(
                true,
                false,
                false,
                Collections.emptyMap(),
                "currency IS NOT NULL AND rate is NOT NULL",
                Collections.singletonList("rate"),
                Arrays.asList(
                        changelogRow("+I", 102L), // US Dollar
                        changelogRow("+I", 119L)) // Euro
                );
    }

    @Test
    public void testReadLatestChangelogOfPartitionedRecordsWithPk() throws Exception {
        // input is dailyRatesChangelogWithoutUB()
        collectLatestLogAndCheck(
                false,
                true,
                true,
                null,
                Collections.emptyList(),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("-U", "Euro", 114L, "2022-01-01"),
                        changelogRow("+U", "Euro", 116L, "2022-01-01"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01"),
                        changelogRow("-D", "Euro", 116L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02")));

        // test partition filter
        collectLatestLogAndCheck(
                false,
                true,
                true,
                "dt = '2022-01-01'",
                Collections.emptyList(),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("-U", "Euro", 114L, "2022-01-01"),
                        changelogRow("+U", "Euro", 116L, "2022-01-01"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01"),
                        changelogRow("-D", "Euro", 116L, "2022-01-01")));

        // test field filter
        collectLatestLogAndCheck(
                false,
                true,
                true,
                "currency = 'Yen'",
                Collections.emptyList(),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01")));

        collectLatestLogAndCheck(
                false,
                true,
                true,
                "rate = 114",
                Collections.emptyList(),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("-U", "Euro", 114L, "2022-01-01")));

        // test partition and field filter
        collectLatestLogAndCheck(
                false,
                true,
                true,
                "rate = 114 AND dt = '2022-01-02'",
                Collections.emptyList(),
                Collections.emptyList());

        // test projection
        collectLatestLogAndCheck(
                false,
                true,
                true,
                null,
                Collections.singletonList("rate"),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", 102L), // US Dollar
                        changelogRow("+I", 114L), // Euro
                        changelogRow("+I", 1L), // Yen
                        changelogRow("-U", 114L), // Euro
                        changelogRow("+U", 116L), // Euro
                        changelogRow("-D", 1L), // Yen
                        changelogRow("-D", 116L), // Euro
                        // part = 2022-01-02
                        changelogRow("+I", 119L) // Euro
                        ));

        // test projection and filter
        collectLatestLogAndCheck(
                false,
                true,
                true,
                "dt = '2022-01-02'",
                Collections.singletonList("rate"),
                Collections.singletonList(
                        // part = 2022-01-02, Euro
                        changelogRow("+I", 119L)));
    }

    @Test
    public void testReadLatestChangelogOfPartitionedRecordsWithoutPk() throws Exception {
        // input is dailyRatesChangelogWithUB()
        collectLatestLogAndCheck(
                false,
                true,
                false,
                null,
                Collections.emptyList(),
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Euro", 116L, "2022-01-01"),
                        changelogRow("-D", "Euro", 116L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 114L, "2022-01-02"),
                        changelogRow("-D", "Euro", 114L, "2022-01-02"),
                        changelogRow("+I", "Euro", 119L, "2022-01-02"),
                        changelogRow("-D", "Euro", 119L, "2022-01-02"),
                        changelogRow("+I", "Euro", 115L, "2022-01-02")));
    }

    @Test
    public void testReadLatestChangelogOfNonPartitionedRecordsWithPk() throws Exception {
        // input is ratesChangelogWithoutUB()
        collectLatestLogAndCheck(
                false,
                false,
                true,
                null,
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Yen", 1L),
                        changelogRow("-U", "Euro", 114L),
                        changelogRow("+U", "Euro", 116L),
                        changelogRow("-D", "Euro", 116L),
                        changelogRow("+I", "Euro", 119L),
                        changelogRow("-D", "Yen", 1L)));

        // test field filter
        collectLatestLogAndCheck(
                false,
                false,
                true,
                "currency = 'Euro'",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("-U", "Euro", 114L),
                        changelogRow("+U", "Euro", 116L),
                        changelogRow("-D", "Euro", 116L),
                        changelogRow("+I", "Euro", 119L)));

        // test projection
        collectLatestLogAndCheck(
                false,
                false,
                true,
                null,
                Collections.singletonList("currency"),
                Arrays.asList(
                        changelogRow("+I", "US Dollar"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Yen"),
                        changelogRow("-D", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("-D", "Yen")));

        // test projection and filter
        collectLatestLogAndCheck(
                false,
                false,
                true,
                "currency = 'Euro'",
                Collections.singletonList("rate"),
                Arrays.asList(
                        changelogRow("+I", 114L),
                        changelogRow("-U", 114L),
                        changelogRow("+U", 116L),
                        changelogRow("-D", 116L),
                        changelogRow("+I", 119L)));
    }

    @Test
    public void testReadLatestChangelogOfNonPartitionedRecordsWithoutPk() throws Exception {
        // input is ratesChangelogWithUB()
        collectLatestLogAndCheck(
                false,
                false,
                false,
                null,
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Yen", 1L),
                        changelogRow("-D", "Euro", 114L),
                        changelogRow("+I", "Euro", 116L),
                        changelogRow("-D", "Euro", 116L),
                        changelogRow("+I", "Euro", 119L),
                        changelogRow("-D", "Euro", 119L),
                        changelogRow("+I", "Euro", 119L),
                        changelogRow("-D", "Yen", 1L),
                        changelogRow("+I", null, 100L),
                        changelogRow("+I", "HK Dollar", null)));

        // test field filter
        collectLatestLogAndCheck(
                false,
                false,
                false,
                "currency = 'Euro'",
                Collections.emptyList(),
                Arrays.asList(
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("-D", "Euro", 114L),
                        changelogRow("+I", "Euro", 116L),
                        changelogRow("-D", "Euro", 116L),
                        changelogRow("+I", "Euro", 119L),
                        changelogRow("-D", "Euro", 119L),
                        changelogRow("+I", "Euro", 119L)));

        // test projection
        collectLatestLogAndCheck(
                false,
                false,
                false,
                null,
                Arrays.asList("currency", "rate"),
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Yen", 1L),
                        changelogRow("-D", "Euro", 114L),
                        changelogRow("+I", "Euro", 116L),
                        changelogRow("-D", "Euro", 116L),
                        changelogRow("+I", "Euro", 119L),
                        changelogRow("-D", "Euro", 119L),
                        changelogRow("+I", "Euro", 119L),
                        changelogRow("-D", "Yen", 1L),
                        changelogRow("+I", null, 100L),
                        changelogRow("+I", "HK Dollar", null)));

        // test projection and filter
        collectLatestLogAndCheck(
                false,
                false,
                false,
                "currency IS NOT NULL",
                Collections.singletonList("currency"),
                Arrays.asList(
                        changelogRow("+I", "US Dollar"),
                        changelogRow("+I", "Euro"),
                        changelogRow("+I", "Yen"),
                        changelogRow("-D", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("-D", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("-D", "Euro"),
                        changelogRow("+I", "Euro"),
                        changelogRow("-D", "Yen"),
                        changelogRow("+I", "HK Dollar")));

        collectLatestLogAndCheck(
                false,
                false,
                false,
                "rate = 119",
                Collections.singletonList("currency"),
                Arrays.asList(
                        changelogRow("+I", "Euro"),
                        changelogRow("-D", "Euro"),
                        changelogRow("+I", "Euro")));
    }

    @Test
    public void testReadLatestChangelogOfInsertOnlyRecords() throws Exception {
        // input is rates()
        List<Row> expected =
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Yen", 1L),
                        changelogRow("-U", "Euro", 114L),
                        changelogRow("+U", "Euro", 119L));

        // currency as pk
        collectLatestLogAndCheck(true, false, true, null, Collections.emptyList(), expected);

        // without pk
        collectLatestLogAndCheck(true, false, true, null, Collections.emptyList(), expected);

        // test field filter
        collectLatestLogAndCheck(
                true,
                false,
                true,
                "rate = 114",
                Collections.emptyList(),
                Arrays.asList(changelogRow("+I", "Euro", 114L), changelogRow("-U", "Euro", 114L)));

        // test projection
        collectLatestLogAndCheck(
                true,
                false,
                true,
                null,
                Collections.singletonList("rate"),
                Arrays.asList(
                        changelogRow("+I", 102L),
                        changelogRow("+I", 114L),
                        changelogRow("+I", 1L),
                        changelogRow("-U", 114L),
                        changelogRow("+U", 119L)));

        // test projection and filter
        collectLatestLogAndCheck(
                true,
                false,
                true,
                "rate = 114",
                Collections.singletonList("currency"),
                Arrays.asList(changelogRow("+I", "Euro"), changelogRow("-U", "Euro")));
    }

    @Test
    public void testReadInsertOnlyChangelogFromTimestamp() throws Exception {
        // input is dailyRates()
        collectChangelogFromTimestampAndCheck(
                true,
                true,
                true,
                0,
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("-U", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+U", "US Dollar", 114L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02")));

        collectChangelogFromTimestampAndCheck(true, true, false, 0, dailyRates());

        // input is rates()
        collectChangelogFromTimestampAndCheck(
                true,
                false,
                true,
                0,
                Arrays.asList(
                        changelogRow("+I", "US Dollar", 102L),
                        changelogRow("+I", "Euro", 114L),
                        changelogRow("+I", "Yen", 1L),
                        changelogRow("-U", "Euro", 114L),
                        changelogRow("+U", "Euro", 119L)));

        collectChangelogFromTimestampAndCheck(true, false, false, 0, rates());

        collectChangelogFromTimestampAndCheck(
                true, false, false, Long.MAX_VALUE - 1, Collections.emptyList());
    }

    @Test
    public void testReadRetractChangelogFromTimestamp() throws Exception {
        // input is dailyRatesChangelogWithUB()
        collectChangelogFromTimestampAndCheck(
                false,
                true,
                false,
                0,
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Euro", 116L, "2022-01-01"),
                        changelogRow("-D", "Euro", 116L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 114L, "2022-01-02"),
                        changelogRow("-D", "Euro", 114L, "2022-01-02"),
                        changelogRow("+I", "Euro", 119L, "2022-01-02"),
                        changelogRow("-D", "Euro", 119L, "2022-01-02"),
                        changelogRow("+I", "Euro", 115L, "2022-01-02")));

        // input is dailyRatesChangelogWithoutUB()
        collectChangelogFromTimestampAndCheck(
                false,
                true,
                true,
                0,
                Arrays.asList(
                        // part = 2022-01-01
                        changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                        changelogRow("+I", "Euro", 114L, "2022-01-01"),
                        changelogRow("+I", "Yen", 1L, "2022-01-01"),
                        changelogRow("-U", "Euro", 114L, "2022-01-01"),
                        changelogRow("+U", "Euro", 116L, "2022-01-01"),
                        changelogRow("-D", "Yen", 1L, "2022-01-01"),
                        changelogRow("-D", "Euro", 116L, "2022-01-01"),
                        // part = 2022-01-02
                        changelogRow("+I", "Euro", 119L, "2022-01-02")));
    }

    private String collectAndCheckBatchReadWrite(
            boolean partitioned,
            boolean hasPk,
            @Nullable String filter,
            List<String> projection,
            List<Row> expected)
            throws Exception {
        return collectAndCheckUnderSameEnv(
                        false,
                        false,
                        true,
                        partitioned,
                        hasPk,
                        true,
                        Collections.emptyMap(),
                        filter,
                        projection,
                        expected)
                .f0;
    }

    private String collectAndCheckStreamingReadWriteWithClose(
            boolean enableLogStore,
            boolean partitioned,
            boolean hasPk,
            Map<String, String> readHints,
            @Nullable String filter,
            List<String> projection,
            List<Row> expected)
            throws Exception {
        Tuple2<String, BlockingIterator<Row, Row>> tuple =
                collectAndCheckUnderSameEnv(
                        true,
                        enableLogStore,
                        false,
                        partitioned,
                        hasPk,
                        true,
                        readHints,
                        filter,
                        projection,
                        expected);
        tuple.f1.close();
        return tuple.f0;
    }

    private Tuple2<String, BlockingIterator<Row, Row>>
            collectAndCheckStreamingReadWriteWithoutClose(
                    Map<String, String> readHints,
                    @Nullable String filter,
                    List<String> projection,
                    List<Row> expected)
                    throws Exception {
        return collectAndCheckUnderSameEnv(
                true, true, false, true, true, true, readHints, filter, projection, expected);
    }

    private void collectLatestLogAndCheck(
            boolean insertOnly,
            boolean partitioned,
            boolean hasPk,
            @Nullable String filter,
            List<String> projection,
            List<Row> expected)
            throws Exception {
        Map<String, String> hints = new HashMap<>();
        hints.put(
                LOG_PREFIX + LogOptions.SCAN.key(),
                LogOptions.LogStartupMode.LATEST.name().toLowerCase());
        collectAndCheckUnderSameEnv(
                        true,
                        true,
                        insertOnly,
                        partitioned,
                        hasPk,
                        false,
                        hints,
                        filter,
                        projection,
                        expected)
                .f1
                .close();
    }

    private void collectChangelogFromTimestampAndCheck(
            boolean insertOnly,
            boolean partitioned,
            boolean hasPk,
            long timestamp,
            List<Row> expected)
            throws Exception {
        Map<String, String> hints = new HashMap<>();
        hints.put(LOG_PREFIX + LogOptions.SCAN.key(), "from-timestamp");
        hints.put(LOG_PREFIX + LogOptions.SCAN_TIMESTAMP_MILLS.key(), String.valueOf(timestamp));
        collectAndCheckUnderSameEnv(
                        true,
                        true,
                        insertOnly,
                        partitioned,
                        hasPk,
                        true,
                        hints,
                        null,
                        Collections.emptyList(),
                        expected)
                .f1
                .close();
    }

    private Tuple2<String, String> createSourceAndManagedTable(
            boolean streaming,
            boolean enableLogStore,
            boolean insertOnly,
            boolean partitioned,
            boolean hasPk)
            throws Exception {
        Map<String, String> tableOptions = new HashMap<>();
        rootPath = TEMPORARY_FOLDER.newFolder().getPath();
        tableOptions.put(FileStoreOptions.FILE_PATH.key(), rootPath);
        if (enableLogStore) {
            tableOptions.put(LOG_SYSTEM.key(), "kafka");
            tableOptions.put(LOG_PREFIX + BOOTSTRAP_SERVERS.key(), getBootstrapServers());
        }
        String sourceTable = "source_table_" + UUID.randomUUID();
        String managedTable = "managed_table_" + UUID.randomUUID();
        EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance().inStreamingMode();
        String helperTableDdl;
        if (streaming) {
            helperTableDdl =
                    insertOnly
                            ? prepareHelperSourceWithInsertOnlyRecords(
                                    sourceTable, partitioned, hasPk)
                            : prepareHelperSourceWithChangelogRecords(
                                    sourceTable, partitioned, hasPk);
            env = buildStreamEnv();
            builder.inStreamingMode();
        } else {
            helperTableDdl =
                    prepareHelperSourceWithInsertOnlyRecords(sourceTable, partitioned, hasPk);
            env = buildBatchEnv();
            builder.inBatchMode();
        }
        String managedTableDdl = prepareManagedTableDdl(sourceTable, managedTable, tableOptions);

        tEnv = StreamTableEnvironment.create(env, builder.build());
        tEnv.executeSql(helperTableDdl);
        tEnv.executeSql(managedTableDdl);
        return new Tuple2<>(sourceTable, managedTable);
    }

    private Tuple2<String, BlockingIterator<Row, Row>> collectAndCheckUnderSameEnv(
            boolean streaming,
            boolean enableLogStore,
            boolean insertOnly,
            boolean partitioned,
            boolean hasPk,
            boolean writeFirst,
            Map<String, String> readHints,
            @Nullable String filter,
            List<String> projection,
            List<Row> expected)
            throws Exception {
        Tuple2<String, String> tables =
                createSourceAndManagedTable(
                        streaming, enableLogStore, insertOnly, partitioned, hasPk);
        String sourceTable = tables.f0;
        String managedTable = tables.f1;

        String insertQuery = prepareInsertIntoQuery(sourceTable, managedTable);
        String selectQuery = prepareSelectQuery(managedTable, readHints, filter, projection);

        BlockingIterator<Row, Row> iterator;
        if (writeFirst) {
            tEnv.executeSql(insertQuery).await();
            iterator = BlockingIterator.of(tEnv.executeSql(selectQuery).collect());
        } else {
            iterator = BlockingIterator.of(tEnv.executeSql(selectQuery).collect());
            tEnv.executeSql(insertQuery).await();
        }
        if (expected.isEmpty()) {
            assertNoMoreRecords(iterator);
        } else {
            assertThat(iterator.collect(expected.size(), 10, TimeUnit.SECONDS))
                    .containsExactlyInAnyOrderElementsOf(expected);
        }
        return Tuple2.of(managedTable, iterator);
    }

    private void prepareEnvAndOverwrite(
            String managedTable,
            Map<String, String> staticPartitions,
            List<String[]> overwriteRecords)
            throws Exception {
        final StreamTableEnvironment batchEnv =
                StreamTableEnvironment.create(buildBatchEnv(), EnvironmentSettings.inBatchMode());
        registerTable(batchEnv, managedTable);
        String insertQuery =
                prepareInsertOverwriteQuery(managedTable, staticPartitions, overwriteRecords);
        batchEnv.executeSql(insertQuery).await();
    }

    private void registerTable(StreamTableEnvironment tEnvToRegister, String managedTable)
            throws Exception {
        String cat = this.tEnv.getCurrentCatalog();
        String db = this.tEnv.getCurrentDatabase();
        ObjectPath objectPath = new ObjectPath(db, managedTable);
        CatalogBaseTable table = this.tEnv.getCatalog(cat).get().getTable(objectPath);
        tEnvToRegister.getCatalog(cat).get().createTable(objectPath, table, false);
    }

    private BlockingIterator<Row, Row> collect(
            StreamTableEnvironment tEnv, String selectQuery, int expectedSize, List<Row> actual)
            throws Exception {
        TableResult result = tEnv.executeSql(selectQuery);
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(result.collect());
        actual.addAll(iterator.collect(expectedSize));
        return iterator;
    }

    private BlockingIterator<Row, Row> collectAndCheck(
            StreamTableEnvironment tEnv,
            String managedTable,
            Map<String, String> hints,
            List<Row> expectedRecords)
            throws Exception {
        List<Row> actual = new ArrayList<>();
        BlockingIterator<Row, Row> iterator =
                collect(
                        tEnv,
                        prepareSimpleSelectQuery(managedTable, hints),
                        expectedRecords.size(),
                        actual);
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expectedRecords);
        return iterator;
    }

    private void checkFileStorePath(StreamTableEnvironment tEnv, String managedTable) {
        String relativeFilePath =
                FileStoreOptions.relativeTablePath(
                        ObjectIdentifier.of(
                                tEnv.getCurrentCatalog(), tEnv.getCurrentDatabase(), managedTable));
        // check snapshot file path
        assertThat(Paths.get(rootPath, relativeFilePath, "snapshot")).exists();
        // check manifest file path
        assertThat(Paths.get(rootPath, relativeFilePath, "manifest")).exists();
    }

    private static void assertNoMoreRecords(BlockingIterator<Row, Row> iterator) {
        List<Row> expectedRecords = Collections.emptyList();
        try {
            // set expectation size to 1 to let time pass by until timeout
            // just wait 5s to avoid too long time
            expectedRecords = iterator.collect(1, 5L, TimeUnit.SECONDS);
            iterator.close();
        } catch (Exception ignored) {
            // don't throw exception
        }
        assertThat(expectedRecords).isEmpty();
    }

    private static String prepareManagedTableDdl(
            String sourceTableName, String managedTableName, Map<String, String> tableOptions) {
        StringBuilder ddl =
                new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                        .append(String.format("`%s`", managedTableName));
        ddl.append(prepareOptions(tableOptions))
                .append(String.format(" LIKE `%s` (EXCLUDING OPTIONS)\n", sourceTableName));
        return ddl.toString();
    }

    private static String prepareOptions(Map<String, String> tableOptions) {
        StringBuilder with = new StringBuilder();
        if (tableOptions.size() > 0) {
            with.append(" WITH (\n");
            tableOptions.forEach(
                    (k, v) ->
                            with.append("  ")
                                    .append(String.format("'%s'", k))
                                    .append(" = ")
                                    .append(String.format("'%s',\n", v)));
            int len = with.length();
            with.delete(len - 2, len);
            with.append(")");
        }
        return with.toString();
    }

    private static String prepareInsertIntoQuery(String sourceTableName, String managedTableName) {
        return prepareInsertIntoQuery(
                sourceTableName, managedTableName, Collections.emptyMap(), Collections.emptyMap());
    }

    private static String prepareInsertIntoQuery(
            String sourceTableName,
            String managedTableName,
            Map<String, String> partitions,
            Map<String, String> hints) {
        StringBuilder insertDmlBuilder =
                new StringBuilder(String.format("INSERT INTO `%s`", managedTableName));
        if (partitions.size() > 0) {
            insertDmlBuilder.append(" PARTITION (");
            partitions.forEach(
                    (k, v) -> {
                        insertDmlBuilder.append(String.format("'%s'", k));
                        insertDmlBuilder.append(" = ");
                        insertDmlBuilder.append(String.format("'%s', ", v));
                    });
            int len = insertDmlBuilder.length();
            insertDmlBuilder.deleteCharAt(len - 1);
            insertDmlBuilder.append(")");
        }
        insertDmlBuilder.append(String.format("\n SELECT * FROM `%s`", sourceTableName));
        insertDmlBuilder.append(buildHints(hints));

        return insertDmlBuilder.toString();
    }

    private static String prepareInsertOverwriteQuery(
            String managedTableName,
            Map<String, String> staticPartitions,
            List<String[]> overwriteRecords) {
        StringBuilder insertDmlBuilder =
                new StringBuilder(String.format("INSERT OVERWRITE `%s`", managedTableName));
        if (staticPartitions.size() > 0) {
            insertDmlBuilder.append(" PARTITION (");
            staticPartitions.forEach(
                    (k, v) -> {
                        insertDmlBuilder.append(String.format("%s", k));
                        insertDmlBuilder.append(" = ");
                        insertDmlBuilder.append(String.format("%s, ", v));
                    });
            int len = insertDmlBuilder.length();
            insertDmlBuilder.delete(len - 2, len);
            insertDmlBuilder.append(")");
        }
        insertDmlBuilder.append("\n VALUES ");
        overwriteRecords.forEach(
                record -> {
                    int arity = record.length;
                    insertDmlBuilder.append("(");
                    IntStream.range(0, arity)
                            .forEach(i -> insertDmlBuilder.append(record[i]).append(", "));

                    if (arity > 0) {
                        int len = insertDmlBuilder.length();
                        insertDmlBuilder.delete(len - 2, len);
                    }
                    insertDmlBuilder.append("), ");
                });
        int len = insertDmlBuilder.length();
        insertDmlBuilder.delete(len - 2, len);
        return insertDmlBuilder.toString();
    }

    private static String prepareSimpleSelectQuery(String tableName, Map<String, String> hints) {
        return prepareSelectQuery(tableName, hints, null, Collections.emptyList());
    }

    private static String prepareSelectQuery(
            String tableName,
            Map<String, String> hints,
            @Nullable String filter,
            List<String> projections) {
        StringBuilder queryBuilder =
                new StringBuilder(
                        String.format(
                                "SELECT %s FROM `%s` %s",
                                projections.isEmpty() ? "*" : String.join(", ", projections),
                                tableName,
                                buildHints(hints)));
        if (filter != null) {
            queryBuilder.append("\nWHERE ").append(filter);
        }
        return queryBuilder.toString();
    }

    private static String buildHints(Map<String, String> hints) {
        if (hints.size() > 0) {
            String hintString =
                    hints.entrySet().stream()
                            .map(
                                    entry ->
                                            String.format(
                                                    "'%s' = '%s'",
                                                    entry.getKey(), entry.getValue()))
                            .collect(Collectors.joining(", "));
            return "/*+ OPTIONS (" + hintString + ") */";
        }
        return "";
    }

    private static String prepareHelperSourceWithInsertOnlyRecords(
            String sourceTable, boolean partitioned, boolean hasPk) {
        return prepareHelperSourceRecords(
                RuntimeExecutionMode.BATCH, sourceTable, partitioned, hasPk);
    }

    private static String prepareHelperSourceWithChangelogRecords(
            String sourceTable, boolean partitioned, boolean hasPk) {
        return prepareHelperSourceRecords(
                RuntimeExecutionMode.STREAMING, sourceTable, partitioned, hasPk);
    }

    /**
     * Prepare helper source table ddl according to different input parameter.
     *
     * <pre> E.g. pk with partition
     *   {@code
     *   CREATE TABLE source_table (
     *     currency STRING,
     *     rate BIGINT,
     *     dt STRING) PARTITIONED BY (dt)
     *    WITH (
     *      'connector' = 'values',
     *      'bounded' = executionMode == RuntimeExecutionMode.BATCH,
     *      'partition-list' = '...'
     *     )
     *   }
     * </pre>
     *
     * @param executionMode is used to calculate {@code bounded}
     * @param sourceTable source table name
     * @param partitioned is used to calculate {@code partition-list}
     * @param hasPk
     * @return helper source ddl
     */
    private static String prepareHelperSourceRecords(
            RuntimeExecutionMode executionMode,
            String sourceTable,
            boolean partitioned,
            boolean hasPk) {
        boolean bounded = executionMode == RuntimeExecutionMode.BATCH;
        String changelogMode = bounded ? "I" : hasPk ? "I,UA,D" : "I,UA,UB,D";
        StringBuilder ddlBuilder =
                new StringBuilder(String.format("CREATE TABLE `%s` (\n", sourceTable))
                        .append("  currency STRING,\n")
                        .append("  rate BIGINT");
        if (partitioned) {
            ddlBuilder.append(",\n dt STRING");
        }
        if (hasPk) {
            ddlBuilder.append(", \n PRIMARY KEY (currency");
            if (partitioned) {
                ddlBuilder.append(", dt");
            }
            ddlBuilder.append(") NOT ENFORCED\n");
        } else {
            ddlBuilder.append("\n");
        }
        ddlBuilder.append(")");
        if (partitioned) {
            ddlBuilder.append(" PARTITIONED BY (dt)\n");
        }
        List<Row> input;
        if (bounded) {
            input = partitioned ? dailyRates() : rates();
        } else {
            if (hasPk) {
                input = partitioned ? dailyRatesChangelogWithoutUB() : ratesChangelogWithoutUB();
            } else {
                input = partitioned ? dailyRatesChangelogWithUB() : ratesChangelogWithUB();
            }
        }
        ddlBuilder.append(
                String.format(
                        "  WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = '%s',\n"
                                + "  'data-id' = '%s',\n"
                                + "  'changelog-mode' = '%s',\n"
                                + "  'disable-lookup' = 'true',\n"
                                + "  'partition-list' = '%s'\n"
                                + ")",
                        bounded,
                        registerData(input),
                        changelogMode,
                        partitioned ? "dt:2022-01-01;dt:2022-01-02" : ""));
        return ddlBuilder.toString();
    }

    private static List<Row> rates() {
        return Arrays.asList(
                changelogRow("+I", "US Dollar", 102L),
                changelogRow("+I", "Euro", 114L),
                changelogRow("+I", "Yen", 1L),
                changelogRow("+I", "Euro", 114L),
                changelogRow("+I", "Euro", 119L));
    }

    private static List<Row> dailyRates() {
        return Arrays.asList(
                // part = 2022-01-01
                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                changelogRow("+I", "Euro", 114L, "2022-01-01"),
                changelogRow("+I", "Yen", 1L, "2022-01-01"),
                changelogRow("+I", "Euro", 114L, "2022-01-01"),
                changelogRow("+I", "US Dollar", 114L, "2022-01-01"),
                // part = 2022-01-02
                changelogRow("+I", "Euro", 119L, "2022-01-02"));
    }

    static List<Row> ratesChangelogWithoutUB() {
        return Arrays.asList(
                changelogRow("+I", "US Dollar", 102L),
                changelogRow("+I", "Euro", 114L),
                changelogRow("+I", "Yen", 1L),
                changelogRow("+U", "Euro", 116L),
                changelogRow("-D", "Euro", 116L),
                changelogRow("+I", "Euro", 119L),
                changelogRow("+U", "Euro", 119L),
                changelogRow("-D", "Yen", 1L));
    }

    static List<Row> dailyRatesChangelogWithoutUB() {
        return Arrays.asList(
                // part = 2022-01-01
                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                changelogRow("+I", "Euro", 114L, "2022-01-01"),
                changelogRow("+I", "Yen", 1L, "2022-01-01"),
                changelogRow("+U", "Euro", 116L, "2022-01-01"),
                changelogRow("-D", "Yen", 1L, "2022-01-01"),
                changelogRow("-D", "Euro", 116L, "2022-01-01"),
                // part = 2022-01-02
                changelogRow("+I", "Euro", 119L, "2022-01-02"),
                changelogRow("+U", "Euro", 119L, "2022-01-02"));
    }

    private static List<Row> ratesChangelogWithUB() {
        return Arrays.asList(
                changelogRow("+I", "US Dollar", 102L),
                changelogRow("+I", "Euro", 114L),
                changelogRow("+I", "Yen", 1L),
                changelogRow("-U", "Euro", 114L),
                changelogRow("+U", "Euro", 116L),
                changelogRow("-D", "Euro", 116L),
                changelogRow("+I", "Euro", 119L),
                changelogRow("-U", "Euro", 119L),
                changelogRow("+U", "Euro", 119L),
                changelogRow("-D", "Yen", 1L),
                changelogRow("+I", null, 100L),
                changelogRow("+I", "HK Dollar", null));
    }

    private static List<Row> dailyRatesChangelogWithUB() {
        return Arrays.asList(
                // part = 2022-01-01
                changelogRow("+I", "US Dollar", 102L, "2022-01-01"),
                changelogRow("+I", "Euro", 116L, "2022-01-01"),
                changelogRow("-D", "Euro", 116L, "2022-01-01"),
                changelogRow("+I", "Yen", 1L, "2022-01-01"),
                changelogRow("-D", "Yen", 1L, "2022-01-01"),
                // part = 2022-01-02
                changelogRow("+I", "Euro", 114L, "2022-01-02"),
                changelogRow("-U", "Euro", 114L, "2022-01-02"),
                changelogRow("+U", "Euro", 119L, "2022-01-02"),
                changelogRow("-D", "Euro", 119L, "2022-01-02"),
                changelogRow("+I", "Euro", 115L, "2022-01-02"));
    }

    private static StreamExecutionEnvironment buildStreamEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(100);
        env.setParallelism(2);
        return env;
    }

    private static StreamExecutionEnvironment buildBatchEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(2);
        return env;
    }

    @Test
    public void testSourceParallelism() throws Exception {
        String managedTable = createSourceAndManagedTable(false, false, false, false, false).f1;

        // without hint
        String query = prepareSimpleSelectQuery(managedTable, Collections.emptyMap());
        assertThat(sourceParallelism(query)).isEqualTo(env.getParallelism());

        // with hint
        query =
                prepareSimpleSelectQuery(
                        managedTable, Collections.singletonMap(SCAN_PARALLELISM.key(), "66"));
        assertThat(sourceParallelism(query)).isEqualTo(66);
    }

    private int sourceParallelism(String sql) {
        DataStream<Row> stream = tEnv.toChangelogStream(tEnv.sqlQuery(sql));
        return stream.getParallelism();
    }

    @Test
    public void testSinkParallelism() {
        testSinkParallelism(null, env.getParallelism());
        testSinkParallelism(23, 23);
    }

    private void testSinkParallelism(Integer configParallelism, int expectedParallelism) {
        // 1. create a mock table sink
        ResolvedSchema schema = ResolvedSchema.of(Column.physical("a", DataTypes.STRING()));
        Map<String, String> options = new HashMap<>();
        if (configParallelism != null) {
            options.put(SINK_PARALLELISM.key(), configParallelism.toString());
        }

        Context context =
                new FactoryUtil.DefaultDynamicTableContext(
                        ObjectIdentifier.of("default", "default", "t1"),
                        new ResolvedCatalogTable(
                                CatalogTable.of(
                                        Schema.newBuilder().fromResolvedSchema(schema).build(),
                                        "mock context",
                                        Collections.emptyList(),
                                        options),
                                schema),
                        Collections.emptyMap(),
                        new Configuration(),
                        Thread.currentThread().getContextClassLoader(),
                        false);
        DynamicTableSink tableSink = new TableStoreFactory().createDynamicTableSink(context);
        assertThat(tableSink).isInstanceOf(TableStoreSink.class);

        // 2. get sink provider
        DynamicTableSink.SinkRuntimeProvider provider =
                tableSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        assertThat(provider).isInstanceOf(DataStreamSinkProvider.class);
        DataStreamSinkProvider sinkProvider = (DataStreamSinkProvider) provider;

        // 3. assert parallelism from transformation
        DataStream<RowData> mockSource =
                env.fromCollection(Collections.singletonList(GenericRowData.of()));
        DataStreamSink<?> sink = sinkProvider.consumeDataStream(null, mockSource);
        Transformation<?> transformation = sink.getTransformation();
        // until a PartitionTransformation, see TableStore.SinkBuilder.build()
        while (!(transformation instanceof PartitionTransformation)) {
            assertThat(transformation.getParallelism()).isIn(1, expectedParallelism);
            transformation = transformation.getInputs().get(0);
        }
    }
}
