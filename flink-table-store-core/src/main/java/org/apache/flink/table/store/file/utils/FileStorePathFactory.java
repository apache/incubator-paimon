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

package org.apache.flink.table.store.file.utils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.connector.file.table.RowDataPartitionComputer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.mergetree.sst.SstPathFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;
import org.apache.flink.table.utils.PartitionPathUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.UUID;

/** Factory which produces {@link Path}s for each type of files. */
public class FileStorePathFactory {

    private final Path root;
    private final String uuid;
    @Nullable private final RowDataPartitionComputer partitionComputer;

    private int manifestFileCount;
    private int manifestListCount;

    public FileStorePathFactory(Path root) {
        this(root, null, FileSystemConnectorOptions.PARTITION_DEFAULT_NAME.defaultValue());
    }

    public FileStorePathFactory(
            Path root, @Nullable RowType partitionType, String defaultPartValue) {
        this.root = root;
        this.uuid = UUID.randomUUID().toString();

        if (partitionType == null) {
            this.partitionComputer = null;
        } else {
            String[] partitionColumns = partitionType.getFieldNames().toArray(new String[0]);
            this.partitionComputer =
                    new RowDataPartitionComputer(
                            defaultPartValue,
                            partitionColumns,
                            partitionType.getFields().stream()
                                    .map(f -> LogicalTypeDataTypeConverter.toDataType(f.getType()))
                                    .toArray(DataType[]::new),
                            partitionColumns);
        }

        this.manifestFileCount = 0;
        this.manifestListCount = 0;
    }

    public Path newManifestFile() {
        return new Path(root + "/manifest/manifest-" + uuid + "-" + (manifestFileCount++));
    }

    public Path newManifestList() {
        return new Path(root + "/manifest/manifest-list-" + uuid + "-" + (manifestListCount++));
    }

    public Path toManifestFilePath(String manifestFileName) {
        return new Path(root + "/manifest/" + manifestFileName);
    }

    public Path toManifestListPath(String manifestListName) {
        return new Path(root + "/manifest/" + manifestListName);
    }

    public Path toSnapshotPath(long id) {
        return new Path(root + "/snapshot/snapshot-" + id);
    }

    public SstPathFactory createSstPathFactory(@Nullable BinaryRowData partition, int bucket) {
        return new SstPathFactory(root, getPartitionString(partition), bucket);
    }

    public @Nullable String getPartitionString(@Nullable BinaryRowData partition) {
        if (partitionComputer == null) {
            return null;
        }
        return PartitionPathUtils.generatePartitionPath(
                partitionComputer.generatePartValues(
                        Preconditions.checkNotNull(
                                partition, "Partition row data is null. This is unexpected.")));
    }

    @VisibleForTesting
    public String uuid() {
        return uuid;
    }
}
