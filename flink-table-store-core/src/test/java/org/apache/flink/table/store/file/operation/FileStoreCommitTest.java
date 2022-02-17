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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.FileFormat;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.utils.FailingAtomicRenameFileSystem;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.TestAtomicRenameFileSystem;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileStoreCommitImpl}. */
public class FileStoreCommitTest {

    private static final Logger LOG = LoggerFactory.getLogger(FileStoreCommitTest.class);

    private final FileFormat avro =
            FileFormat.fromIdentifier(
                    FileStoreCommitTest.class.getClassLoader(), "avro", new Configuration());

    private TestKeyValueGenerator gen;
    @TempDir java.nio.file.Path tempDir;

    @BeforeEach
    public void beforeEach() throws IOException {
        gen = new TestKeyValueGenerator();
        Path root = new Path(tempDir.toString());
        root.getFileSystem().mkdirs(new Path(root + "/snapshot"));
        // for failure tests
        FailingAtomicRenameFileSystem.resetFailCounter(100);
        FailingAtomicRenameFileSystem.setFailPossibility(5000);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {TestAtomicRenameFileSystem.SCHEME, FailingAtomicRenameFileSystem.SCHEME})
    public void testSingleCommitUser(String scheme) throws Exception {
        testRandomConcurrentNoConflict(1, scheme);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {TestAtomicRenameFileSystem.SCHEME, FailingAtomicRenameFileSystem.SCHEME})
    public void testManyCommitUsersNoConflict(String scheme) throws Exception {
        testRandomConcurrentNoConflict(ThreadLocalRandom.current().nextInt(3) + 2, scheme);
    }

    protected void testRandomConcurrentNoConflict(int numThreads, String scheme) throws Exception {
        // prepare test data
        Map<BinaryRowData, List<KeyValue>> data =
                generateData(ThreadLocalRandom.current().nextInt(1000) + 1);
        logData(
                () ->
                        data.values().stream()
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList()),
                "input");

        List<Map<BinaryRowData, List<KeyValue>>> dataPerThread = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            dataPerThread.add(new HashMap<>());
        }
        for (Map.Entry<BinaryRowData, List<KeyValue>> entry : data.entrySet()) {
            dataPerThread
                    .get(ThreadLocalRandom.current().nextInt(numThreads))
                    .put(entry.getKey(), entry.getValue());
        }

        // concurrent commits
        List<TestCommitThread> threads = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            TestCommitThread thread =
                    new TestCommitThread(
                            dataPerThread.get(i),
                            OperationTestUtils.createPathFactory(scheme, tempDir.toString()),
                            OperationTestUtils.createPathFactory(
                                    TestAtomicRenameFileSystem.SCHEME, tempDir.toString()));
            thread.start();
            threads.add(thread);
        }

        // calculate expected results
        Map<BinaryRowData, List<KeyValue>> threadResults = new HashMap<>();
        for (TestCommitThread thread : threads) {
            thread.join();
            for (Map.Entry<BinaryRowData, List<KeyValue>> entry : thread.getResult().entrySet()) {
                threadResults.put(entry.getKey(), entry.getValue());
            }
        }
        Map<BinaryRowData, BinaryRowData> expected =
                OperationTestUtils.toKvMap(
                        threadResults.values().stream()
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList()));

        // read actual data and compare
        FileStorePathFactory safePathFactory =
                OperationTestUtils.createPathFactory(
                        TestAtomicRenameFileSystem.SCHEME, tempDir.toString());
        Long snapshotId = safePathFactory.latestSnapshotId();
        assertThat(snapshotId).isNotNull();
        List<KeyValue> actualKvs =
                OperationTestUtils.readKvsFromSnapshot(snapshotId, avro, safePathFactory);
        gen.sort(actualKvs);
        logData(() -> actualKvs, "raw read results");
        Map<BinaryRowData, BinaryRowData> actual = OperationTestUtils.toKvMap(actualKvs);
        logData(() -> kvMapToKvList(expected), "expected");
        logData(() -> kvMapToKvList(actual), "actual");
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testOverwritePartialCommit() throws Exception {
        Map<BinaryRowData, List<KeyValue>> data1 =
                generateData(ThreadLocalRandom.current().nextInt(1000) + 1);
        logData(
                () ->
                        data1.values().stream()
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList()),
                "data1");

        FileStorePathFactory pathFactory =
                OperationTestUtils.createPathFactory(
                        TestAtomicRenameFileSystem.SCHEME, tempDir.toString());
        OperationTestUtils.commitData(
                data1.values().stream().flatMap(Collection::stream).collect(Collectors.toList()),
                gen::getPartition,
                kv -> 0,
                avro,
                pathFactory);

        ThreadLocalRandom random = ThreadLocalRandom.current();
        String dtToOverwrite =
                new ArrayList<>(data1.keySet())
                        .get(random.nextInt(data1.size()))
                        .getString(0)
                        .toString();
        Map<String, String> partitionToOverwrite = new HashMap<>();
        partitionToOverwrite.put("dt", dtToOverwrite);
        if (LOG.isDebugEnabled()) {
            LOG.debug("dtToOverwrite " + dtToOverwrite);
        }

        Map<BinaryRowData, List<KeyValue>> data2 =
                generateData(ThreadLocalRandom.current().nextInt(1000) + 1);
        // remove all records not belonging to dtToOverwrite
        data2.entrySet().removeIf(e -> !dtToOverwrite.equals(e.getKey().getString(0).toString()));
        logData(
                () ->
                        data2.values().stream()
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList()),
                "data2");
        OperationTestUtils.overwriteData(
                data2.values().stream().flatMap(Collection::stream).collect(Collectors.toList()),
                gen::getPartition,
                kv -> 0,
                avro,
                pathFactory,
                partitionToOverwrite);

        List<KeyValue> expectedKvs = new ArrayList<>();
        for (Map.Entry<BinaryRowData, List<KeyValue>> entry : data1.entrySet()) {
            if (dtToOverwrite.equals(entry.getKey().getString(0).toString())) {
                continue;
            }
            expectedKvs.addAll(entry.getValue());
        }
        data2.values().forEach(expectedKvs::addAll);
        gen.sort(expectedKvs);
        Map<BinaryRowData, BinaryRowData> expected = OperationTestUtils.toKvMap(expectedKvs);

        List<KeyValue> actualKvs =
                OperationTestUtils.readKvsFromSnapshot(
                        pathFactory.latestSnapshotId(), avro, pathFactory);
        gen.sort(actualKvs);
        Map<BinaryRowData, BinaryRowData> actual = OperationTestUtils.toKvMap(actualKvs);

        logData(() -> kvMapToKvList(expected), "expected");
        logData(() -> kvMapToKvList(actual), "actual");
        assertThat(actual).isEqualTo(expected);
    }

    private Map<BinaryRowData, List<KeyValue>> generateData(int numRecords) {
        Map<BinaryRowData, List<KeyValue>> data = new HashMap<>();
        for (int i = 0; i < numRecords; i++) {
            KeyValue kv = gen.next();
            data.compute(gen.getPartition(kv), (p, l) -> l == null ? new ArrayList<>() : l).add(kv);
        }
        return data;
    }

    private List<KeyValue> kvMapToKvList(Map<BinaryRowData, BinaryRowData> map) {
        return map.entrySet().stream()
                .map(e -> new KeyValue().replace(e.getKey(), -1, ValueKind.ADD, e.getValue()))
                .collect(Collectors.toList());
    }

    private void logData(Supplier<List<KeyValue>> supplier, String name) {
        if (!LOG.isDebugEnabled()) {
            return;
        }

        LOG.debug("========== Beginning of " + name + " ==========");
        for (KeyValue kv : supplier.get()) {
            LOG.debug(kv.toString(TestKeyValueGenerator.KEY_TYPE, TestKeyValueGenerator.ROW_TYPE));
        }
        LOG.debug("========== End of " + name + " ==========");
    }
}
