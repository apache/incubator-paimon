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

package org.apache.flink.table.store.format.parquet;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.store.format.FileStatsExtractor;
import org.apache.flink.table.store.utils.Projection;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.formats.parquet.ParquetFileFormatFactory.UTC_TIMEZONE;
import static org.apache.flink.table.store.format.parquet.ParquetFileFormatFactory.IDENTIFIER;

/** Parquet {@link FileFormat}. */
public class ParquetFileFormat extends FileFormat {

    private final Configuration formatOptions;

    public ParquetFileFormat(Configuration formatOptions) {
        super(IDENTIFIER);
        this.formatOptions = formatOptions;
    }

    @VisibleForTesting
    Configuration formatOptions() {
        return formatOptions;
    }

    @Override
    public BulkFormat<RowData, FileSourceSplit> createReaderFactory(
            RowType type, int[][] projection, List<Predicate> filters) {
        return ParquetInputFormatFactory.create(
                getParquetConfiguration(formatOptions),
                (RowType) Projection.of(projection).project(type),
                InternalTypeInfo.of(type),
                formatOptions.get(UTC_TIMEZONE));
    }

    @Override
    public BulkWriter.Factory<RowData> createWriterFactory(RowType type) {
        return ParquetRowDataBuilder.createWriterFactory(
                type, getParquetConfiguration(formatOptions), formatOptions.get(UTC_TIMEZONE));
    }

    @Override
    public Optional<FileStatsExtractor> createStatsExtractor(RowType type) {
        return Optional.of(new ParquetFileStatsExtractor(type));
    }

    public static org.apache.hadoop.conf.Configuration getParquetConfiguration(
            ReadableConfig options) {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        Properties properties = new Properties();
        ((org.apache.flink.configuration.Configuration) options).addAllToProperties(properties);
        properties.forEach(
                (k, v) -> {
                    conf.set(IDENTIFIER + "." + k, v.toString());
                });
        return conf;
    }
}
