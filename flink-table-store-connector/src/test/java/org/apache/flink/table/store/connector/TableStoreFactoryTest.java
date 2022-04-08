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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.log.LogOptions;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.apache.flink.table.store.connector.TableStoreTestBase.createResolvedTable;
import static org.apache.flink.table.store.file.FileStoreOptions.BUCKET;
import static org.apache.flink.table.store.file.FileStoreOptions.PATH;
import static org.apache.flink.table.store.file.FileStoreOptions.TABLE_STORE_PREFIX;
import static org.apache.flink.table.store.file.FileStoreOptions.relativeTablePath;
import static org.apache.flink.table.store.kafka.KafkaLogOptions.BOOTSTRAP_SERVERS;
import static org.apache.flink.table.store.log.LogOptions.CONSISTENCY;
import static org.apache.flink.table.store.log.LogOptions.LOG_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test cases for {@link TableStoreFactory}. */
public class TableStoreFactoryTest {

    private static final ObjectIdentifier TABLE_IDENTIFIER =
            ObjectIdentifier.of("catalog", "database", "table");

    private final TableStoreFactory tableStoreFactory = new TableStoreFactory();

    @TempDir private static java.nio.file.Path sharedTempDir;
    private DynamicTableFactory.Context context;

    @ParameterizedTest
    @MethodSource("providingOptions")
    public void testEnrichOptions(
            Map<String, String> sessionOptions,
            Map<String, String> tableOptions,
            Map<String, String> expectedEnrichedOptions) {
        context = createTableContext(sessionOptions, tableOptions);
        Map<String, String> actualEnrichedOptions = tableStoreFactory.enrichOptions(context);
        assertThat(actualEnrichedOptions)
                .containsExactlyInAnyOrderEntriesOf(expectedEnrichedOptions);
    }

    @ParameterizedTest
    @MethodSource("providingEnrichedOptionsForCreation")
    public void testOnCreateTable(Map<String, String> enrichedOptions, boolean ignoreIfExists) {
        context = createTableContext(Collections.emptyMap(), enrichedOptions);
        Path expectedPath =
                Paths.get(
                        sharedTempDir.toAbsolutePath().toString(),
                        relativeTablePath(TABLE_IDENTIFIER));
        boolean exist = expectedPath.toFile().exists();
        if (ignoreIfExists || !exist) {
            tableStoreFactory.onCreateTable(context, ignoreIfExists);
            assertThat(expectedPath).exists();
        } else {
            assertThatThrownBy(() -> tableStoreFactory.onCreateTable(context, false))
                    .isInstanceOf(TableException.class)
                    .hasMessageContaining(
                            String.format(
                                    "Failed to create file store path. "
                                            + "Reason: directory %s exists for table %s. "
                                            + "Suggestion: please try `DESCRIBE TABLE %s` to "
                                            + "first check whether table exists in current catalog. "
                                            + "If table exists in catalog, and data files under current path "
                                            + "are valid, please use `CREATE TABLE IF NOT EXISTS` ddl instead. "
                                            + "Otherwise, please choose another table name "
                                            + "or manually delete the current path and try again.",
                                    expectedPath,
                                    TABLE_IDENTIFIER.asSerializableString(),
                                    TABLE_IDENTIFIER.asSerializableString()));
        }
    }

    @ParameterizedTest
    @MethodSource("providingEnrichedOptionsForDrop")
    public void testOnDropTable(Map<String, String> enrichedOptions, boolean ignoreIfNotExists) {
        context = createTableContext(Collections.emptyMap(), enrichedOptions);
        Path expectedPath =
                Paths.get(
                        sharedTempDir.toAbsolutePath().toString(),
                        relativeTablePath(TABLE_IDENTIFIER));
        boolean exist = expectedPath.toFile().exists();
        if (exist || ignoreIfNotExists) {
            tableStoreFactory.onDropTable(context, ignoreIfNotExists);
            assertThat(expectedPath).doesNotExist();
        } else {
            assertThatThrownBy(() -> tableStoreFactory.onDropTable(context, false))
                    .isInstanceOf(TableException.class)
                    .hasMessageContaining(
                            String.format(
                                    "Failed to delete file store path. "
                                            + "Reason: directory %s doesn't exist for table %s. "
                                            + "Suggestion: please try `DROP TABLE IF EXISTS` ddl instead.",
                                    expectedPath, TABLE_IDENTIFIER.asSerializableString()));
        }
    }

    @Test
    public void testFilterLogStoreOptions() {
        // mix invalid key and leave value to empty to emphasize the deferred validation
        Map<String, String> expectedLogOptions =
                of(
                        LogOptions.SCAN.key(),
                        "",
                        LogOptions.RETENTION.key(),
                        "",
                        "dummy.key",
                        "",
                        LogOptions.CHANGELOG_MODE.key(),
                        "");
        Map<String, String> enrichedOptions =
                addPrefix(expectedLogOptions, LOG_PREFIX, (key) -> true);
        enrichedOptions.put("foo", "bar");

        assertThat(TableStoreFactory.filterLogStoreOptions(enrichedOptions))
                .containsExactlyInAnyOrderEntriesOf(expectedLogOptions);
    }

    @ParameterizedTest
    @MethodSource("providingResolvedTable")
    public void testBuildTableStore(
            RowType rowType,
            List<String> partitions,
            List<String> primaryKeys,
            TableStoreTestBase.ExpectedResult expectedResult) {
        ResolvedCatalogTable catalogTable =
                createResolvedTable(Collections.emptyMap(), rowType, partitions, primaryKeys);
        context =
                new FactoryUtil.DefaultDynamicTableContext(
                        TABLE_IDENTIFIER,
                        catalogTable,
                        Collections.emptyMap(),
                        Configuration.fromMap(Collections.emptyMap()),
                        Thread.currentThread().getContextClassLoader(),
                        false);
        if (expectedResult.success) {
            TableStore tableStore = tableStoreFactory.buildTableStore(context);
            assertThat(tableStore.partitioned()).isEqualTo(catalogTable.isPartitioned());
            assertThat(tableStore.valueCountMode())
                    .isEqualTo(catalogTable.getResolvedSchema().getPrimaryKeyIndexes().length == 0);

            // check primary key doesn't contain partition
            if (tableStore.partitioned() && !tableStore.valueCountMode()) {
                assertThat(
                                tableStore.primaryKeys().stream()
                                        .noneMatch(pk -> tableStore.partitionKeys().contains(pk)))
                        .isTrue();
            }
        } else {
            assertThatThrownBy(() -> tableStoreFactory.buildTableStore(context))
                    .isInstanceOf(expectedResult.expectedType)
                    .hasMessageContaining(expectedResult.expectedMessage);
        }
    }

    // ~ Tools ------------------------------------------------------------------

    private static Stream<Arguments> providingOptions() {
        Map<String, String> enrichedOptions =
                of(
                        BUCKET.key(),
                        BUCKET.defaultValue().toString(),
                        PATH.key(),
                        sharedTempDir.toString(),
                        LOG_PREFIX + BOOTSTRAP_SERVERS.key(),
                        "localhost:9092",
                        LOG_PREFIX + CONSISTENCY.key(),
                        CONSISTENCY.defaultValue().name());

        // default
        Arguments arg0 =
                Arguments.of(
                        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());

        // set configuration under session level
        Arguments arg1 =
                Arguments.of(
                        addPrefix(enrichedOptions, TABLE_STORE_PREFIX, (key) -> true),
                        Collections.emptyMap(),
                        enrichedOptions);

        // set configuration under table level
        Arguments arg2 = Arguments.of(Collections.emptyMap(), enrichedOptions, enrichedOptions);

        // set both session and table level configuration to test options combination
        Map<String, String> tableOptions = new HashMap<>(enrichedOptions);
        tableOptions.remove(PATH.key());
        tableOptions.remove(CONSISTENCY.key());
        Arguments arg3 =
                Arguments.of(
                        addPrefix(
                                enrichedOptions,
                                TABLE_STORE_PREFIX,
                                (key) -> !tableOptions.containsKey(key)),
                        tableOptions,
                        enrichedOptions);

        // set same key with different value to test table configuration take precedence
        Map<String, String> sessionOptions = new HashMap<>();
        sessionOptions.put(
                TABLE_STORE_PREFIX + BUCKET.key(), String.valueOf(BUCKET.defaultValue() + 1));
        Arguments arg4 = Arguments.of(sessionOptions, enrichedOptions, enrichedOptions);
        return Stream.of(arg0, arg1, arg2, arg3, arg4);
    }

    private static Stream<Arguments> providingEnrichedOptionsForCreation() {
        Map<String, String> enrichedOptions = new HashMap<>();
        enrichedOptions.put(PATH.key(), sharedTempDir.toAbsolutePath().toString());
        return Stream.of(
                Arguments.of(enrichedOptions, false),
                Arguments.of(enrichedOptions, true),
                Arguments.of(enrichedOptions, false));
    }

    private static Stream<Arguments> providingEnrichedOptionsForDrop() {
        File tablePath =
                Paths.get(
                                sharedTempDir.toAbsolutePath().toString(),
                                TABLE_IDENTIFIER.asSummaryString())
                        .toFile();
        if (!tablePath.exists()) {
            tablePath.mkdirs();
        }
        Map<String, String> enrichedOptions = new HashMap<>();
        enrichedOptions.put(PATH.key(), sharedTempDir.toAbsolutePath().toString());
        return Stream.of(
                Arguments.of(enrichedOptions, false),
                Arguments.of(enrichedOptions, true),
                Arguments.of(enrichedOptions, false));
    }

    private static Stream<Arguments> providingResolvedTable() {
        RowType rowType = TestKeyValueGenerator.ROW_TYPE;
        // success case
        Arguments arg0 =
                Arguments.of(
                        rowType,
                        Arrays.asList("dt", "hr"),
                        Arrays.asList("dt", "hr", "shopId"), // pk is [dt, hr, shopId]
                        new TableStoreTestBase.ExpectedResult().success(true));

        // failed case: pk doesn't contain partition key
        Arguments arg1 =
                Arguments.of(
                        rowType,
                        Arrays.asList("dt", "hr"),
                        Collections.singletonList("shopId"), // pk is [shopId]
                        new TableStoreTestBase.ExpectedResult()
                                .success(false)
                                .expectedType(IllegalStateException.class)
                                .expectedMessage(
                                        "Primary key constraint [shopId] should include all partition fields [dt, hr]"));

        // failed case: pk is same as partition key
        Arguments arg2 =
                Arguments.of(
                        rowType,
                        Arrays.asList("dt", "hr", "shopId"),
                        Arrays.asList("dt", "hr", "shopId"), // pk is [dt, hr, shopId]
                        new TableStoreTestBase.ExpectedResult()
                                .success(false)
                                .expectedType(IllegalStateException.class)
                                .expectedMessage(
                                        "Primary key constraint [dt, hr, shopId] should not be same with partition fields [dt, hr, shopId],"
                                                + " this will result in only one record in a partition"));

        return Stream.of(arg0, arg1, arg2);
    }

    private static Map<String, String> addPrefix(
            Map<String, String> options, String prefix, Predicate<String> predicate) {
        Map<String, String> newOptions = new HashMap<>();
        options.forEach(
                (k, v) -> {
                    if (predicate.test(k)) {
                        newOptions.put(prefix + k, v);
                    }
                });
        return newOptions;
    }

    private static DynamicTableFactory.Context createTableContext(
            Map<String, String> sessionOptions, Map<String, String> tableOptions) {
        ResolvedCatalogTable resolvedCatalogTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.derived(),
                                "a comment",
                                Collections.emptyList(),
                                tableOptions),
                        ResolvedSchema.of(Collections.emptyList()));
        return new FactoryUtil.DefaultDynamicTableContext(
                TABLE_IDENTIFIER,
                resolvedCatalogTable,
                Collections.emptyMap(),
                Configuration.fromMap(sessionOptions),
                Thread.currentThread().getContextClassLoader(),
                false);
    }

    private static Map<String, String> of(String... kvs) {
        assert kvs != null && kvs.length % 2 == 0;
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < kvs.length - 1; i += 2) {
            map.put(kvs[i], kvs[i + 1]);
        }
        return map;
    }
}
