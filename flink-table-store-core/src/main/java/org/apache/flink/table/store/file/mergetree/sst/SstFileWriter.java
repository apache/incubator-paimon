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

package org.apache.flink.table.store.file.mergetree.sst;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.KeyValueSerializer;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.stats.FieldStats;
import org.apache.flink.table.store.file.stats.FieldStatsCollector;
import org.apache.flink.table.store.file.stats.FileStatsExtractor;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.utils.RollingFile;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Writes {@link KeyValue}s into sst files. */
public class SstFileWriter {

    private static final Logger LOG = LoggerFactory.getLogger(SstFileWriter.class);

    private final RowType keyType;
    private final RowType valueType;
    private final BulkWriter.Factory<RowData> writerFactory;
    private final FileStatsExtractor fileStatsExtractor;
    private final SstPathFactory pathFactory;
    private final long suggestedFileSize;

    private SstFileWriter(
            RowType keyType,
            RowType valueType,
            BulkWriter.Factory<RowData> writerFactory,
            FileStatsExtractor fileStatsExtractor,
            SstPathFactory pathFactory,
            long suggestedFileSize) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.writerFactory = writerFactory;
        this.fileStatsExtractor = fileStatsExtractor;
        this.pathFactory = pathFactory;
        this.suggestedFileSize = suggestedFileSize;
    }

    public RowType keyType() {
        return keyType;
    }

    public RowType valueType() {
        return valueType;
    }

    @VisibleForTesting
    public long suggestedFileSize() {
        return suggestedFileSize;
    }

    @VisibleForTesting
    public SstPathFactory pathFactory() {
        return pathFactory;
    }

    /**
     * Write several {@link KeyValue}s into an sst file of a given level.
     *
     * <p>NOTE: This method is atomic.
     */
    public List<SstFileMeta> write(CloseableIterator<KeyValue> iterator, int level)
            throws Exception {
        SstRollingFile rollingFile =
                fileStatsExtractor == null
                        ? new StatsCollectingRollingFile(level)
                        : new FileExtractingRollingFile(level);
        List<SstFileMeta> result = new ArrayList<>();
        List<Path> filesToCleanUp = new ArrayList<>();
        try {
            rollingFile.write(iterator, result, filesToCleanUp);
        } catch (Throwable e) {
            LOG.warn("Exception occurs when writing sst files. Cleaning up.", e);
            for (Path path : filesToCleanUp) {
                FileUtils.deleteOrWarn(path);
            }
            throw e;
        } finally {
            iterator.close();
        }
        return result;
    }

    public void delete(SstFileMeta file) {
        FileUtils.deleteOrWarn(pathFactory.toPath(file.fileName()));
    }

    private abstract class SstRollingFile extends RollingFile<KeyValue, SstFileMeta> {

        private final int level;
        private final KeyValueSerializer serializer;
        private final RowDataSerializer keySerializer;

        private long rowCount;
        private BinaryRowData minKey;
        private RowData maxKey;
        private long minSequenceNumber;
        private long maxSequenceNumber;

        private SstRollingFile(int level) {
            super(suggestedFileSize);
            this.level = level;
            this.serializer = new KeyValueSerializer(keyType, valueType);
            this.keySerializer = new RowDataSerializer(keyType);
            resetMeta();
        }

        @Override
        protected Path newPath() {
            return pathFactory.newPath();
        }

        @Override
        protected BulkWriter<RowData> newWriter(FSDataOutputStream out) throws IOException {
            return writerFactory.create(out);
        }

        @Override
        protected RowData toRowData(KeyValue kv) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Writing key-value to sst file, kv: " + kv.toString(keyType, valueType));
            }

            rowCount++;
            if (minKey == null) {
                minKey = keySerializer.toBinaryRow(kv.key()).copy();
            }
            maxKey = kv.key();
            minSequenceNumber = Math.min(minSequenceNumber, kv.sequenceNumber());
            maxSequenceNumber = Math.max(maxSequenceNumber, kv.sequenceNumber());

            return serializer.toRow(kv);
        }

        @Override
        protected SstFileMeta collectFile(Path path) throws IOException {
            KeyAndValueStats stats = extractStats(path);
            SstFileMeta result =
                    new SstFileMeta(
                            path.getName(),
                            FileUtils.getFileSize(path),
                            rowCount,
                            minKey,
                            keySerializer.toBinaryRow(maxKey).copy(),
                            stats.keyStats,
                            stats.valueStats,
                            minSequenceNumber,
                            maxSequenceNumber,
                            level);
            resetMeta();
            return result;
        }

        protected void resetMeta() {
            rowCount = 0;
            minKey = null;
            maxKey = null;
            minSequenceNumber = Long.MAX_VALUE;
            maxSequenceNumber = Long.MIN_VALUE;
        }

        protected abstract KeyAndValueStats extractStats(Path path);
    }

    private class FileExtractingRollingFile extends SstRollingFile {

        private FileExtractingRollingFile(int level) {
            super(level);
        }

        @Override
        protected KeyAndValueStats extractStats(Path path) {
            FieldStats[] rawStats;
            try {
                rawStats = fileStatsExtractor.extract(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            int numKeyFields = keyType.getFieldCount();
            return new KeyAndValueStats(
                    Arrays.copyOfRange(rawStats, 0, numKeyFields),
                    Arrays.copyOfRange(rawStats, numKeyFields + 2, rawStats.length));
        }
    }

    private class StatsCollectingRollingFile extends SstRollingFile {

        private FieldStatsCollector keyStatsCollector;
        private FieldStatsCollector valueStatsCollector;

        private StatsCollectingRollingFile(int level) {
            super(level);
        }

        @Override
        protected RowData toRowData(KeyValue kv) {
            keyStatsCollector.collect(kv.key());
            valueStatsCollector.collect(kv.value());
            return super.toRowData(kv);
        }

        @Override
        protected KeyAndValueStats extractStats(Path path) {
            return new KeyAndValueStats(keyStatsCollector.extract(), valueStatsCollector.extract());
        }

        @Override
        protected void resetMeta() {
            super.resetMeta();
            keyStatsCollector = new FieldStatsCollector(keyType);
            valueStatsCollector = new FieldStatsCollector(valueType);
        }
    }

    private static class KeyAndValueStats {

        private final FieldStats[] keyStats;
        private final FieldStats[] valueStats;

        private KeyAndValueStats(FieldStats[] keyStats, FieldStats[] valueStats) {
            this.keyStats = keyStats;
            this.valueStats = valueStats;
        }
    }

    /** Creates {@link SstFileWriter}. */
    public static class Factory {

        private final RowType keyType;
        private final RowType valueType;
        private final FileFormat fileFormat;
        private final FileStorePathFactory pathFactory;
        private final long suggestedFileSize;

        public Factory(
                RowType keyType,
                RowType valueType,
                FileFormat fileFormat,
                FileStorePathFactory pathFactory,
                long suggestedFileSize) {
            this.keyType = keyType;
            this.valueType = valueType;
            this.fileFormat = fileFormat;
            this.pathFactory = pathFactory;
            this.suggestedFileSize = suggestedFileSize;
        }

        public SstFileWriter create(BinaryRowData partition, int bucket) {
            RowType recordType = KeyValue.schema(keyType, valueType);
            return new SstFileWriter(
                    keyType,
                    valueType,
                    fileFormat.createWriterFactory(recordType),
                    fileFormat.createStatsExtractor(recordType).orElse(null),
                    pathFactory.createSstPathFactory(partition, bucket),
                    suggestedFileSize);
        }
    }
}
