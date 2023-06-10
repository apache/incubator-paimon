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

package org.apache.paimon.io;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.ColumnStatisticsCollectSkipper;
import org.apache.paimon.format.FieldStats;
import org.apache.paimon.format.FileStatsExtractor;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.stats.BinaryTableStats;
import org.apache.paimon.stats.FieldStatsArraySerializer;
import org.apache.paimon.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;

/**
 * A {@link StatsCollectingSingleFileWriter} to write data files containing {@link KeyValue}s. Also
 * produces {@link DataFileMeta} after writing a file.
 *
 * <p>NOTE: records given to the writer must be sorted because it does not compare the min max keys
 * to produce {@link DataFileMeta}.
 */
public class KeyValueDataFileWriter
        extends StatsCollectingSingleFileWriter<KeyValue, DataFileMeta> {

    private static final Logger LOG = LoggerFactory.getLogger(KeyValueDataFileWriter.class);

    private final RowType keyType;
    private final RowType valueType;
    private final long schemaId;
    private final int level;

    private final FieldStatsArraySerializer keyStatsConverter;
    private final FieldStatsArraySerializer valueStatsConverter;
    private final InternalRowSerializer keySerializer;

    private BinaryRow minKey = null;
    private InternalRow maxKey = null;
    private long minSeqNumber = Long.MAX_VALUE;
    private long maxSeqNumber = Long.MIN_VALUE;

    public KeyValueDataFileWriter(
            FileIO fileIO,
            FormatWriterFactory factory,
            Path path,
            Function<KeyValue, InternalRow> converter,
            RowType keyType,
            RowType valueType,
            @Nullable
                    Function<ColumnStatisticsCollectSkipper, FileStatsExtractor> fileStatsExtractor,
            long schemaId,
            int level,
            String compression,
            ColumnStatisticsCollectSkipper columnStatisticsCollectSkipper) {
        super(
                fileIO,
                factory,
                path,
                converter,
                KeyValue.schema(keyType, valueType),
                fileStatsExtractor,
                compression,
                columnStatisticsCollectSkipper);

        this.keyType = keyType;
        this.valueType = valueType;
        this.schemaId = schemaId;
        this.level = level;

        this.keyStatsConverter = new FieldStatsArraySerializer(keyType);
        this.valueStatsConverter = new FieldStatsArraySerializer(valueType);
        this.keySerializer = new InternalRowSerializer(keyType);
    }

    @Override
    public void write(KeyValue kv) throws IOException {
        super.write(kv);

        updateMinKey(kv);
        updateMaxKey(kv);

        updateMinSeqNumber(kv);
        updateMaxSeqNumber(kv);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Write key value " + kv.toString(keyType, valueType));
        }
    }

    private void updateMinKey(KeyValue kv) {
        if (minKey == null) {
            minKey = keySerializer.toBinaryRow(kv.key()).copy();
        }
    }

    private void updateMaxKey(KeyValue kv) {
        maxKey = kv.key();
    }

    private void updateMinSeqNumber(KeyValue kv) {
        minSeqNumber = Math.min(minSeqNumber, kv.sequenceNumber());
    }

    private void updateMaxSeqNumber(KeyValue kv) {
        maxSeqNumber = Math.max(maxSeqNumber, kv.sequenceNumber());
    }

    @Override
    @Nullable
    public DataFileMeta result() throws IOException {
        if (recordCount() == 0) {
            return null;
        }

        FieldStats[] rowStats = fieldStats();
        int numKeyFields = keyType.getFieldCount();

        FieldStats[] keyFieldStats = Arrays.copyOfRange(rowStats, 0, numKeyFields);
        BinaryTableStats keyStats = keyStatsConverter.toBinary(keyFieldStats);

        FieldStats[] valFieldStats =
                Arrays.copyOfRange(rowStats, numKeyFields + 2, rowStats.length);
        BinaryTableStats valueStats = valueStatsConverter.toBinary(valFieldStats);

        return new DataFileMeta(
                path.getName(),
                fileIO.getFileSize(path),
                recordCount(),
                minKey,
                keySerializer.toBinaryRow(maxKey).copy(),
                keyStats,
                valueStats,
                minSeqNumber,
                maxSeqNumber,
                schemaId,
                level);
    }
}
