/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.file.manifest;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.FileFormat;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * This file includes several {@link ManifestFileMeta}, representing all data of the whole table at
 * the corresponding snapshot.
 */
public class ManifestList {

    private static final Logger LOG = LoggerFactory.getLogger(ManifestList.class);

    private final ManifestFileMetaSerializer serializer;
    private final BulkFormat<RowData, FileSourceSplit> readerFactory;
    private final BulkWriter.Factory<RowData> writerFactory;
    private final FileStorePathFactory pathFactory;

    public ManifestList(
            RowType partitionType, FileFormat fileFormat, FileStorePathFactory pathFactory) {
        this.serializer = new ManifestFileMetaSerializer(partitionType);
        RowType metaType = ManifestFileMeta.schema(partitionType);
        this.readerFactory = fileFormat.createReaderFactory(metaType);
        this.writerFactory = fileFormat.createWriterFactory(metaType);
        this.pathFactory = pathFactory;
    }

    public List<ManifestFileMeta> read(String fileName) throws IOException {
        return FileUtils.readListFromFile(
                pathFactory.toManifestListPath(fileName), serializer, readerFactory);
    }

    /**
     * Write several {@link ManifestFileMeta}s into a manifest list.
     *
     * <p>NOTE: This method is atomic.
     */
    public String write(List<ManifestFileMeta> metas) throws IOException {
        Preconditions.checkArgument(
                metas.size() > 0, "Manifest file metas to write must not be empty.");

        Path path = pathFactory.newManifestList();
        try {
            return write(metas, path);
        } catch (Throwable e) {
            LOG.warn("Exception occurs when writing manifest list " + path + ". Cleaning up.", e);
            FileUtils.deleteOrWarn(path);
            throw e;
        }
    }

    private String write(List<ManifestFileMeta> metas, Path path) throws IOException {
        FSDataOutputStream out =
                path.getFileSystem().create(path, FileSystem.WriteMode.NO_OVERWRITE);
        BulkWriter<RowData> writer = writerFactory.create(out);
        for (ManifestFileMeta manifest : metas) {
            writer.addElement(serializer.toRow(manifest));
        }
        writer.finish();
        out.close();
        return path.getName();
    }

    public void delete(String fileName) {
        FileUtils.deleteOrWarn(pathFactory.toManifestListPath(fileName));
    }
}
