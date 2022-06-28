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

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.catalog.CatalogLock;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.connector.StatefulPrecommittingSinkWriter;
import org.apache.flink.table.store.connector.sink.TestFileStore.TestRecordWriter;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.UserCodeClassLoader;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.Callable;

import static org.apache.flink.table.store.file.mergetree.compact.CompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link StoreSink}. */
@RunWith(Parameterized.class)
public class StoreSinkTest {

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    private final boolean hasPk;

    private final boolean partitioned;

    private final ObjectIdentifier identifier =
            ObjectIdentifier.of("my_catalog", "my_database", "my_table");

    private final TestLock lock = new TestLock();

    private TestFileStore fileStore;
    private TestFileStoreTable table;

    public StoreSinkTest(boolean hasPk, boolean partitioned) {
        this.hasPk = hasPk;
        this.partitioned = partitioned;
    }

    @Before
    public void before() throws Exception {
        Path path = new Path(tempFolder.newFolder().toURI().toString());
        TableSchema tableSchema =
                new SchemaManager(path)
                        .commitNewVersion(
                                new UpdateSchema(
                                        RowType.of(
                                                new LogicalType[] {
                                                    new IntType(), new IntType(), new IntType()
                                                },
                                                new String[] {"a", "b", "c"}),
                                        partitioned
                                                ? Collections.singletonList("a")
                                                : Collections.emptyList(),
                                        hasPk ? Arrays.asList("a", "b") : Collections.emptyList(),
                                        new HashMap<>(),
                                        ""));

        RowType partitionType = tableSchema.logicalPartitionType();
        fileStore = new TestFileStore(hasPk, partitionType);
        table = new TestFileStoreTable(path, fileStore, tableSchema);
    }

    @Parameterized.Parameters(name = "hasPk-{0}, partitioned-{1}")
    public static List<Boolean[]> data() {
        return Arrays.asList(
                new Boolean[] {true, true},
                new Boolean[] {true, false},
                new Boolean[] {false, false},
                new Boolean[] {false, true});
    }

    @Test
    public void testChangelogs() throws Exception {
        Assume.assumeTrue(hasPk && partitioned);
        StoreSink<?, ?> sink = newSink(null);
        writeAndCommit(
                sink,
                GenericRowData.ofKind(RowKind.INSERT, 0, 0, 1),
                GenericRowData.ofKind(RowKind.UPDATE_BEFORE, 0, 2, 3),
                GenericRowData.ofKind(RowKind.UPDATE_AFTER, 0, 7, 5),
                GenericRowData.ofKind(RowKind.DELETE, 1, 0, 1));
        assertThat(fileStore.committedFiles.get(row(1)).get(1))
                .isEqualTo(Collections.singletonList("DELETE-key-0-value-1/0/1"));
        assertThat(fileStore.committedFiles.get(row(0)).get(0))
                .isEqualTo(Collections.singletonList("DELETE-key-2-value-0/2/3"));
        assertThat(fileStore.committedFiles.get(row(0)).get(1))
                .isEqualTo(Arrays.asList("INSERT-key-0-value-0/0/1", "INSERT-key-7-value-0/7/5"));
    }

    @Test
    public void testNoKeyChangelogs() throws Exception {
        Assume.assumeTrue(!hasPk && partitioned);
        StoreSink<?, ?> sink =
                new StoreSink<>(identifier, table, false, null, () -> lock, new HashMap<>(), null);
        writeAndCommit(
                sink,
                GenericRowData.ofKind(RowKind.INSERT, 0, 0, 1),
                GenericRowData.ofKind(RowKind.UPDATE_BEFORE, 0, 2, 3),
                GenericRowData.ofKind(RowKind.UPDATE_AFTER, 0, 4, 5),
                GenericRowData.ofKind(RowKind.DELETE, 1, 0, 1));
        assertThat(fileStore.committedFiles.get(row(1)).get(0))
                .isEqualTo(Collections.singletonList("INSERT-key-1/0/1-value--1"));
        assertThat(fileStore.committedFiles.get(row(0)).get(0))
                .isEqualTo(Collections.singletonList("INSERT-key-0/4/5-value-1"));
        assertThat(fileStore.committedFiles.get(row(0)).get(1))
                .isEqualTo(Arrays.asList("INSERT-key-0/0/1-value-1", "INSERT-key-0/2/3-value--1"));
    }

    @Test
    public void testAppend() throws Exception {
        Assume.assumeTrue(hasPk && partitioned);
        StoreSink<?, ?> sink = newSink(null);
        writeAndAssert(sink);

        writeAndCommit(sink, GenericRowData.of(0, 8, 9), GenericRowData.of(1, 10, 11));
        assertThat(fileStore.committedFiles.get(row(1)).get(0))
                .isEqualTo(Collections.singletonList("INSERT-key-10-value-1/10/11"));
        assertThat(fileStore.committedFiles.get(row(0)).get(0))
                .isEqualTo(Arrays.asList("INSERT-key-2-value-0/2/3", "INSERT-key-8-value-0/8/9"));
    }

    @Test
    public void testOverwrite() throws Exception {
        Assume.assumeTrue(hasPk && partitioned);
        StoreSink<?, ?> sink = newSink(new HashMap<>());
        writeAndAssert(sink);

        writeAndCommit(sink, GenericRowData.of(0, 8, 9), GenericRowData.of(1, 10, 11));
        assertThat(fileStore.committedFiles.get(row(1)).get(1)).isNull();
        assertThat(fileStore.committedFiles.get(row(1)).get(0))
                .isEqualTo(Collections.singletonList("INSERT-key-10-value-1/10/11"));
        assertThat(fileStore.committedFiles.get(row(0)).get(0))
                .isEqualTo(Collections.singletonList("INSERT-key-8-value-0/8/9"));
    }

    @Test
    public void testOverwritePartition() throws Exception {
        Assume.assumeTrue(hasPk && partitioned);
        HashMap<String, String> partition = new HashMap<>();
        partition.put("part", "0");
        StoreSink<?, ?> sink = newSink(partition);
        writeAndAssert(sink);

        writeAndCommit(sink, GenericRowData.of(0, 8, 9), GenericRowData.of(1, 10, 11));
        assertThat(fileStore.committedFiles.get(row(1)).get(1))
                .isEqualTo(Collections.singletonList("INSERT-key-0-value-1/0/1"));
        assertThat(fileStore.committedFiles.get(row(1)).get(0))
                .isEqualTo(Collections.singletonList("INSERT-key-10-value-1/10/11"));
        assertThat(fileStore.committedFiles.get(row(0)).get(0))
                .isEqualTo(Collections.singletonList("INSERT-key-8-value-0/8/9"));
    }

    private void writeAndAssert(StoreSink<?, ?> sink) throws Exception {
        writeAndCommit(
                sink,
                GenericRowData.of(0, 0, 1),
                GenericRowData.of(0, 2, 3),
                GenericRowData.of(0, 7, 5),
                GenericRowData.of(1, 0, 1));
        assertThat(fileStore.committedFiles.get(row(1)).get(1))
                .isEqualTo(Collections.singletonList("INSERT-key-0-value-1/0/1"));
        assertThat(fileStore.committedFiles.get(row(0)).get(0))
                .isEqualTo(Collections.singletonList("INSERT-key-2-value-0/2/3"));
        assertThat(fileStore.committedFiles.get(row(0)).get(1))
                .isEqualTo(Arrays.asList("INSERT-key-0-value-0/0/1", "INSERT-key-7-value-0/7/5"));
    }

    private void writeAndCommit(StoreSink<?, ?> sink, RowData... rows) throws Exception {
        commit(sink, write(sink, rows));
    }

    private List<Committable> write(StoreSink<?, ?> sink, RowData... rows) throws Exception {
        StatefulPrecommittingSinkWriter<?> writer = sink.createWriter(null);
        for (RowData row : rows) {
            writer.write(row, null);
        }

        List<Committable> committables = ((StoreSinkWriter) writer).prepareCommit();
        Map<BinaryRowData, Map<Integer, RecordWriter<KeyValue>>> writers =
                new HashMap<>(((StoreSinkWriter) writer).writers());
        assertThat(writers.size()).isGreaterThan(0);

        writer.close();
        writers.forEach(
                (part, map) ->
                        map.forEach(
                                (bucket, recordWriter) ->
                                        assertThat(((TestRecordWriter) recordWriter).closed)
                                                .isTrue()));
        return committables;
    }

    private void commit(StoreSink<?, ?> sink, List<Committable> fileCommittables) throws Exception {
        StoreGlobalCommitter committer = sink.createGlobalCommitter();
        ManifestCommittable committable = committer.combine(0, fileCommittables);

        fileStore.expired = false;
        lock.locked = false;
        committer.commit(Collections.singletonList(committable));
        assertThat(fileStore.expired).isTrue();
        assertThat(lock.locked).isTrue();

        assertThat(
                        committer
                                .filterRecoveredCommittables(Collections.singletonList(committable))
                                .size())
                .isEqualTo(0);

        lock.closed = false;
        committer.close();
        assertThat(lock.closed).isTrue();
    }

    private StoreSink<?, ?> newSink(@Nullable Map<String, String> overwritePartition) {
        return new StoreSink<>(
                identifier, table, false, null, () -> lock, overwritePartition, null);
    }

    private class TestLock implements CatalogLock {

        private boolean locked = false;

        private boolean closed = false;

        @Override
        public <T> T runWithLock(String database, String table, Callable<T> callable)
                throws Exception {
            assertThat(database).isEqualTo(identifier.getDatabaseName());
            assertThat(table).isEqualTo(identifier.getObjectName());
            locked = true;
            return callable.call();
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private Sink.InitContext initContext() {
        return new Sink.InitContext() {
            @Override
            public UserCodeClassLoader getUserCodeClassLoader() {
                return null;
            }

            @Override
            public MailboxExecutor getMailboxExecutor() {
                return null;
            }

            @Override
            public ProcessingTimeService getProcessingTimeService() {
                return null;
            }

            @Override
            public int getSubtaskId() {
                return 0;
            }

            @Override
            public int getNumberOfParallelSubtasks() {
                return 1;
            }

            @Override
            public SinkWriterMetricGroup metricGroup() {
                return null;
            }

            @Override
            public OptionalLong getRestoredCheckpointId() {
                return OptionalLong.empty();
            }

            @Override
            public SerializationSchema.InitializationContext
                    asSerializationSchemaInitializationContext() {
                return null;
            }
        };
    }
}
