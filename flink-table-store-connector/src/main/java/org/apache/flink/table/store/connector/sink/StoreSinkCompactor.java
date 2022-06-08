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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.operation.FileStoreScan;
import org.apache.flink.table.store.file.predicate.PredicateConverter;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.writer.RecordWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** A dedicated {@link SinkWriter} for manual triggered compaction. */
public class StoreSinkCompactor<WriterStateT> extends StoreSinkWriterBase<WriterStateT> {

    private static final Logger LOG = LoggerFactory.getLogger(StoreSinkCompactor.class);

    private final int subTaskId;
    private final int numOfParallelInstances;

    private final FileStore fileStore;
    private final Map<String, String> partitionSpec;

    public StoreSinkCompactor(
            int subTaskId,
            int numOfParallelInstances,
            FileStore fileStore,
            Map<String, String> partitionSpec) {
        this.subTaskId = subTaskId;
        this.numOfParallelInstances = numOfParallelInstances;
        this.fileStore = fileStore;
        this.partitionSpec = partitionSpec;
    }

    @Override
    public void flush(boolean endOfInput) {
        if (endOfInput) {
            FileStoreScan.Plan plan =
                    fileStore
                            .newScan()
                            .withPartitionFilter(
                                    PredicateConverter.CONVERTER.fromMap(
                                            partitionSpec, fileStore.partitionType()))
                            .plan();
            for (Map.Entry<BinaryRowData, Map<Integer, List<DataFileMeta>>> partEntry :
                    plan.groupByPartFiles().entrySet()) {
                BinaryRowData partition = partEntry.getKey();
                for (Map.Entry<Integer, List<DataFileMeta>> bucketEntry :
                        partEntry.getValue().entrySet()) {
                    int bucket = bucketEntry.getKey();
                    if (select(partition, bucket)) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "Assign partition {}, bucket {} to subtask {}",
                                    FileStorePathFactory.getPartitionComputer(
                                                    fileStore.partitionType(),
                                                    FileSystemConnectorOptions
                                                            .PARTITION_DEFAULT_NAME
                                                            .defaultValue())
                                            .generatePartValues(partition),
                                    bucket,
                                    subTaskId);
                        }
                        RecordWriter writer = getWriter(partition, bucket, bucketEntry.getValue());
                        try {
                            writer.endInput();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void write(RowData element, Context context) throws IOException, InterruptedException {
        // nothing to write
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    private RecordWriter getWriter(BinaryRowData partition, int bucket, List<DataFileMeta> files) {
        Map<Integer, RecordWriter> buckets = writers.get(partition);
        if (buckets == null) {
            buckets = new HashMap<>();
            writers.put(partition.copy(), buckets);
        }
        return buckets.computeIfAbsent(
                bucket,
                k -> fileStore.newWrite().createCompactWriter(partition.copy(), bucket, files));
    }

    @VisibleForTesting
    boolean select(BinaryRowData partition, int bucket) {
        return subTaskId == Math.abs(Objects.hash(partition, bucket) % numOfParallelInstances);
    }
}
