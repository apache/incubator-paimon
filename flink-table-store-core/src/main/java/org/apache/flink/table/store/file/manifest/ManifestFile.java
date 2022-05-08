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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.stats.FieldStatsCollector;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.utils.VersionedObjectSerializer;
import org.apache.flink.table.store.file.writer.BaseBulkWriter;
import org.apache.flink.table.store.file.writer.BaseFileWriter;
import org.apache.flink.table.store.file.writer.RollingFileWriter;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Supplier;

/**
 * This file includes several {@link ManifestEntry}s, representing the additional changes since last
 * snapshot.
 */
public class ManifestFile {

    private static final Logger LOG = LoggerFactory.getLogger(ManifestFile.class);

    private final RowType partitionType;
    private final ManifestEntrySerializer serializer;
    private final BulkFormat<RowData, FileSourceSplit> readerFactory;
    private final BulkWriter.Factory<RowData> writerFactory;
    private final FileStorePathFactory pathFactory;
    private final long suggestedFileSize;

    private ManifestFile(
            RowType partitionType,
            ManifestEntrySerializer serializer,
            BulkFormat<RowData, FileSourceSplit> readerFactory,
            BulkWriter.Factory<RowData> writerFactory,
            FileStorePathFactory pathFactory,
            long suggestedFileSize) {
        this.partitionType = partitionType;
        this.serializer = serializer;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.pathFactory = pathFactory;
        this.suggestedFileSize = suggestedFileSize;
    }

    @VisibleForTesting
    public long suggestedFileSize() {
        return suggestedFileSize;
    }

    public List<ManifestEntry> read(String fileName) {
        try {
            return FileUtils.readListFromFile(
                    pathFactory.toManifestFilePath(fileName), serializer, readerFactory);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read manifest file " + fileName, e);
        }
    }

    /**
     * Write several {@link ManifestEntry}s into manifest files.
     *
     * <p>NOTE: This method is atomic.
     */
    public List<ManifestFileMeta> write(List<ManifestEntry> entries) {

        ManifestRollingWriter rollingWriter = createManifestRollingWriter(suggestedFileSize);
        try (ManifestRollingWriter writer = rollingWriter) {
            writer.write(entries);

        } catch (Exception e) {
            LOG.warn("Exception occurs when writing manifest files. Cleaning up.", e);

            rollingWriter.abort();
            throw new RuntimeException(e);
        }

        return rollingWriter.result();
    }

    public void delete(String fileName) {
        FileUtils.deleteOrWarn(pathFactory.toManifestFilePath(fileName));
    }

    private class ManifestEntryBulkWriterFactory implements BulkWriter.Factory<ManifestEntry> {

        @Override
        public BulkWriter<ManifestEntry> create(FSDataOutputStream out) throws IOException {
            return new BaseBulkWriter<>(writerFactory.create(out), serializer::toRow);
        }
    }

    private class ManifestEntryWriter extends BaseFileWriter<ManifestEntry, ManifestFileMeta> {

        private final FieldStatsCollector statsCollector;

        private long numAddedFiles = 0;
        private long numDeletedFiles = 0;

        ManifestEntryWriter(BulkWriter.Factory<ManifestEntry> writerFactory, Path path)
                throws IOException {
            super(writerFactory, path);

            this.statsCollector = new FieldStatsCollector(partitionType);
        }

        @Override
        public void write(ManifestEntry entry) throws IOException {
            super.write(entry);

            switch (entry.kind()) {
                case ADD:
                    numAddedFiles++;
                    break;
                case DELETE:
                    numDeletedFiles++;
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown entry kind: " + entry.kind());
            }

            statsCollector.collect(entry.partition());
        }

        @Override
        protected ManifestFileMeta createFileMeta(Path path) throws IOException {
            return new ManifestFileMeta(
                    path.getName(),
                    path.getFileSystem().getFileStatus(path).getLen(),
                    numAddedFiles,
                    numDeletedFiles,
                    statsCollector.extract());
        }
    }

    private static class ManifestRollingWriter
            extends RollingFileWriter<ManifestEntry, ManifestFileMeta> {

        public ManifestRollingWriter(
                Supplier<ManifestEntryWriter> writerFactory, long targetFileSize) {
            super(writerFactory, targetFileSize);
        }
    }

    private Supplier<ManifestEntryWriter> createWriterFactory() {
        return () -> {
            try {
                return new ManifestEntryWriter(
                        new ManifestEntryBulkWriterFactory(), pathFactory.newManifestFile());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    private ManifestRollingWriter createManifestRollingWriter(long targetFileSize) {
        return new ManifestRollingWriter(createWriterFactory(), targetFileSize);
    }

    /**
     * Creator of {@link ManifestFile}. It reueses {@link BulkFormat} and {@link BulkWriter.Factory}
     * from {@link FileFormat}.
     */
    public static class Factory {

        private final RowType partitionType;
        private final RowType keyType;
        private final RowType valueType;
        private final FileFormat fileFormat;
        private final FileStorePathFactory pathFactory;
        private final long suggestedFileSize;

        public Factory(
                RowType partitionType,
                RowType keyType,
                RowType valueType,
                FileFormat fileFormat,
                FileStorePathFactory pathFactory,
                long suggestedFileSize) {
            this.partitionType = partitionType;
            this.keyType = keyType;
            this.valueType = valueType;
            this.fileFormat = fileFormat;
            this.pathFactory = pathFactory;
            this.suggestedFileSize = suggestedFileSize;
        }

        public ManifestFile create() {
            RowType entryType =
                    VersionedObjectSerializer.versionType(
                            ManifestEntry.schema(partitionType, keyType, valueType));
            return new ManifestFile(
                    partitionType,
                    new ManifestEntrySerializer(partitionType, keyType, valueType),
                    fileFormat.createReaderFactory(entryType),
                    fileFormat.createWriterFactory(entryType),
                    pathFactory,
                    suggestedFileSize);
        }
    }
}
