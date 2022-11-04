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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.utils.RecordWriter;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.table.sink.FileCommittable;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Base {@link FileStoreWrite} implementation.
 *
 * @param <T> type of record to write.
 */
public abstract class AbstractFileStoreWrite<T> implements FileStoreWrite<T> {

    private final String commitUser;
    private final SnapshotManager snapshotManager;
    private final FileStoreScan scan;

    @Nullable protected IOManager ioManager;

    protected final Map<BinaryRowData, Map<Integer, WriterAndCommit<T>>> writers;
    private final ExecutorService compactExecutor;

    private boolean overwrite = false;

    protected AbstractFileStoreWrite(
            String commitUser, SnapshotManager snapshotManager, FileStoreScan scan) {
        this.commitUser = commitUser;
        this.snapshotManager = snapshotManager;
        this.scan = scan;

        this.writers = new HashMap<>();
        this.compactExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("compaction-thread"));
    }

    @Override
    public FileStoreWrite<T> withIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
        return this;
    }

    protected List<DataFileMeta> scanExistingFileMetas(BinaryRowData partition, int bucket) {
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        List<DataFileMeta> existingFileMetas = Lists.newArrayList();
        if (latestSnapshotId != null) {
            // Concat all the DataFileMeta of existing files into existingFileMetas.
            scan.withSnapshot(latestSnapshotId)
                    .withPartitionFilter(Collections.singletonList(partition)).withBucket(bucket)
                    .plan().files().stream()
                    .map(ManifestEntry::file)
                    .forEach(existingFileMetas::add);
        }
        return existingFileMetas;
    }

    public void withOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    @Override
    public void write(BinaryRowData partition, int bucket, T data) throws Exception {
        RecordWriter<T> writer = getWriter(partition, bucket);
        writer.write(data);
    }

    @Override
    public void compact(BinaryRowData partition, int bucket) throws Exception {
        getWriter(partition, bucket).fullCompaction();
    }

    @Override
    public List<FileCommittable> prepareCommit(boolean endOfInput, long commitIdentifier)
            throws Exception {
        long latestCommittedIdentifier =
                snapshotManager
                        .latestSnapshotOfUser(commitUser)
                        .map(Snapshot::commitIdentifier)
                        .orElse(Long.MIN_VALUE);

        List<FileCommittable> result = new ArrayList<>();

        Iterator<Map.Entry<BinaryRowData, Map<Integer, WriterAndCommit<T>>>> partIter =
                writers.entrySet().iterator();
        while (partIter.hasNext()) {
            Map.Entry<BinaryRowData, Map<Integer, WriterAndCommit<T>>> partEntry = partIter.next();
            BinaryRowData partition = partEntry.getKey();
            Iterator<Map.Entry<Integer, WriterAndCommit<T>>> bucketIter =
                    partEntry.getValue().entrySet().iterator();
            while (bucketIter.hasNext()) {
                Map.Entry<Integer, WriterAndCommit<T>> entry = bucketIter.next();
                int bucket = entry.getKey();
                WriterAndCommit<T> writerAndCommit = entry.getValue();

                RecordWriter.CommitIncrement increment =
                        writerAndCommit.writer.prepareCommit(endOfInput);
                FileCommittable committable =
                        new FileCommittable(
                                partition,
                                bucket,
                                increment.newFilesIncrement(),
                                increment.compactIncrement());
                result.add(committable);

                if (committable.isEmpty()) {
                    if (writerAndCommit.lastModifiedCommitIdentifier <= latestCommittedIdentifier) {
                        // Clear writer if no update, and if its latest modification has committed.
                        //
                        // We need a mechanism to clear writers, otherwise there will be more and
                        // more such as yesterday's partition that no longer needs to be written.
                        writerAndCommit.writer.close();
                        bucketIter.remove();
                    }
                } else {
                    writerAndCommit.lastModifiedCommitIdentifier = commitIdentifier;
                }
            }

            if (partEntry.getValue().isEmpty()) {
                partIter.remove();
            }
        }

        return result;
    }

    @Override
    public void close() throws Exception {
        for (Map<Integer, WriterAndCommit<T>> bucketWriters : writers.values()) {
            for (WriterAndCommit<T> writerAndCommit : bucketWriters.values()) {
                writerAndCommit.writer.close();
            }
        }
        writers.clear();
        compactExecutor.shutdownNow();
    }

    private RecordWriter<T> getWriter(BinaryRowData partition, int bucket) {
        Map<Integer, WriterAndCommit<T>> buckets = writers.get(partition);
        if (buckets == null) {
            buckets = new HashMap<>();
            writers.put(partition.copy(), buckets);
        }
        return buckets.computeIfAbsent(bucket, k -> createWriter(partition.copy(), bucket)).writer;
    }

    private WriterAndCommit<T> createWriter(BinaryRowData partition, int bucket) {
        RecordWriter<T> writer =
                overwrite
                        ? createEmptyWriter(partition.copy(), bucket, compactExecutor)
                        : createWriter(partition.copy(), bucket, compactExecutor);
        notifyNewWriter(writer);
        return new WriterAndCommit<>(writer);
    }

    protected void notifyNewWriter(RecordWriter<T> writer) {}

    /** Create a {@link RecordWriter} from partition and bucket. */
    @VisibleForTesting
    public abstract RecordWriter<T> createWriter(
            BinaryRowData partition, int bucket, ExecutorService compactExecutor);

    /** Create an empty {@link RecordWriter} from partition and bucket. */
    @VisibleForTesting
    public abstract RecordWriter<T> createEmptyWriter(
            BinaryRowData partition, int bucket, ExecutorService compactExecutor);

    /** {@link RecordWriter} with identifier of its last modified commit. */
    protected static class WriterAndCommit<T> {

        protected final RecordWriter<T> writer;
        private long lastModifiedCommitIdentifier;

        private WriterAndCommit(RecordWriter<T> writer) {
            this.writer = writer;
            this.lastModifiedCommitIdentifier = Long.MIN_VALUE;
        }
    }
}
