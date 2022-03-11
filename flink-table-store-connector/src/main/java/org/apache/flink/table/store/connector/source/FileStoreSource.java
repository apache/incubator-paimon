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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.operation.FileStoreRead;
import org.apache.flink.table.store.file.operation.FileStoreScan;
import org.apache.flink.table.store.file.predicate.Predicate;

import javax.annotation.Nullable;

import static org.apache.flink.table.store.connector.source.PendingSplitsCheckpoint.INVALID_SNAPSHOT;

/** {@link Source} of file store. */
public class FileStoreSource
        implements Source<RowData, FileStoreSourceSplit, PendingSplitsCheckpoint> {

    private static final long serialVersionUID = 1L;

    private final FileStore fileStore;

    private final boolean valueCountMode;

    @Nullable private final int[][] projectedFields;

    @Nullable private final Predicate predicate;

    public FileStoreSource(
            FileStore fileStore,
            boolean valueCountMode,
            @Nullable int[][] projectedFields,
            @Nullable Predicate predicate) {
        this.fileStore = fileStore;
        this.valueCountMode = valueCountMode;
        this.projectedFields = projectedFields;
        this.predicate = predicate;
    }

    @Override
    public Boundedness getBoundedness() {
        // TODO supports streaming reading for file store
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, FileStoreSourceSplit> createReader(SourceReaderContext context) {
        FileStoreRead read = fileStore.newRead();
        if (projectedFields != null) {
            if (valueCountMode) {
                // TODO don't project keys, and add key projection to split reader
                read.withKeyProjection(projectedFields);
            } else {
                read.withValueProjection(projectedFields);
            }
        }
        return new FileStoreSourceReader(context, read, valueCountMode);
    }

    @Override
    public SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> createEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context) {
        FileStoreScan scan = fileStore.newScan();
        if (predicate != null) {
            // TODO split predicate into partitionPredicate and fieldsPredicate
            //            scan.withPartitionFilter(partitionPredicate);
            //            if (keyAsRecord) {
            //                scan.withKeyFilter(fieldsPredicate);
            //            } else {
            //                scan.withValueFilter(fieldsPredicate);
            //            }
        }
        return new StaticFileStoreSplitEnumerator(context, scan);
    }

    @Override
    public SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            PendingSplitsCheckpoint checkpoint) {
        Snapshot snapshot = null;
        if (checkpoint.currentSnapshotId() != INVALID_SNAPSHOT) {
            snapshot = fileStore.newScan().snapshot(checkpoint.currentSnapshotId());
        }
        return new StaticFileStoreSplitEnumerator(context, snapshot, checkpoint.splits());
    }

    @Override
    public FileStoreSourceSplitSerializer getSplitSerializer() {
        return new FileStoreSourceSplitSerializer(
                fileStore.partitionType(), fileStore.keyType(), fileStore.valueType());
    }

    @Override
    public PendingSplitsCheckpointSerializer getEnumeratorCheckpointSerializer() {
        return new PendingSplitsCheckpointSerializer(getSplitSerializer());
    }
}
