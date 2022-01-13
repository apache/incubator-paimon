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

package org.apache.flink.table.store.file.mergetree.sst;

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.stats.FieldStatsCollector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Random {@link SstFileMeta} generator. */
public class SstTestDataGenerator {

    private final int numBuckets;
    private final int memTableCapacity;

    private final List<Map<BinaryRowData, List<KeyValue>>> memTables;
    private final TestKeyValueGenerator gen;

    private SstTestDataGenerator(int numBuckets, int memTableCapacity) {
        this.numBuckets = numBuckets;
        this.memTableCapacity = memTableCapacity;

        this.memTables = new ArrayList<>();
        for (int i = 0; i < numBuckets; i++) {
            memTables.add(new HashMap<>());
        }
        this.gen = new TestKeyValueGenerator();
    }

    public SstFile next() {
        while (true) {
            KeyValue kv = gen.next();
            BinaryRowData key = (BinaryRowData) kv.key();
            BinaryRowData partition = gen.getPartition(kv);
            int bucket = (key.hashCode() % numBuckets + numBuckets) % numBuckets;
            List<KeyValue> memTable =
                    memTables.get(bucket).computeIfAbsent(partition, k -> new ArrayList<>());
            memTable.add(kv);

            if (memTable.size() >= memTableCapacity) {
                List<SstFile> result = createSstFiles(memTable, 0, partition, bucket);
                memTable.clear();
                assert result.size() == 1;
                return result.get(0);
            }
        }
    }

    public List<SstFile> createSstFiles(
            List<KeyValue> kvs, int level, BinaryRowData partition, int bucket) {
        gen.sort(kvs);
        List<KeyValue> combined = new ArrayList<>();
        for (int i = 0; i + 1 < kvs.size(); i++) {
            KeyValue now = kvs.get(i);
            KeyValue next = kvs.get(i + 1);
            if (!now.key().equals(next.key())) {
                combined.add(now);
            }
        }
        combined.add(kvs.get(kvs.size() - 1));

        int capacity = memTableCapacity;
        for (int i = 0; i < level; i++) {
            capacity *= memTableCapacity;
        }
        List<SstFile> result = new ArrayList<>();
        for (int i = 0; i < combined.size(); i += capacity) {
            result.add(
                    createSstFile(
                            combined.subList(i, Math.min(i + capacity, combined.size())),
                            level,
                            partition,
                            bucket));
        }
        return result;
    }

    private SstFile createSstFile(
            List<KeyValue> kvs, int level, BinaryRowData partition, int bucket) {
        FieldStatsCollector collector = new FieldStatsCollector(TestKeyValueGenerator.ROW_TYPE);
        long totalSize = 0;
        BinaryRowData minKey = null;
        BinaryRowData maxKey = null;
        long minSequenceNumber = Long.MAX_VALUE;
        long maxSequenceNumber = Long.MIN_VALUE;
        for (KeyValue kv : kvs) {
            BinaryRowData key = (BinaryRowData) kv.key();
            BinaryRowData value = (BinaryRowData) kv.value();
            totalSize += key.getSizeInBytes() + value.getSizeInBytes();
            collector.collect(value);
            if (minKey == null || gen.compareKeys(key, minKey) < 0) {
                minKey = key;
            }
            if (maxKey == null || gen.compareKeys(key, maxKey) > 0) {
                maxKey = key;
            }
            minSequenceNumber = Math.min(minSequenceNumber, kv.sequenceNumber());
            maxSequenceNumber = Math.max(maxSequenceNumber, kv.sequenceNumber());
        }

        return new SstFile(
                partition,
                bucket,
                new SstFileMeta(
                        "sst-" + UUID.randomUUID(),
                        totalSize,
                        kvs.size(),
                        minKey,
                        maxKey,
                        collector.extract(),
                        minSequenceNumber,
                        maxSequenceNumber,
                        level),
                kvs);
    }

    /** An in-memory SST file. */
    public static class SstFile {
        public final BinaryRowData partition;
        public final int bucket;
        public final SstFileMeta meta;
        public final List<KeyValue> content;

        private SstFile(
                BinaryRowData partition, int bucket, SstFileMeta meta, List<KeyValue> content) {
            this.partition = partition;
            this.bucket = bucket;
            this.meta = meta;
            this.content = content;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link SstTestDataGenerator}. */
    public static class Builder {
        private int numBuckets = 3;
        private int memTableCapacity = 3;

        public Builder numBuckets(int value) {
            this.numBuckets = value;
            return this;
        }

        public Builder memTableCapacity(int value) {
            this.memTableCapacity = value;
            return this;
        }

        public SstTestDataGenerator build() {
            return new SstTestDataGenerator(numBuckets, memTableCapacity);
        }
    }
}
