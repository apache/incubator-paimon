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

package org.apache.flink.table.store.file.format;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.store.file.stats.FileStatsExtractor;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

/** Factory class which creates reader and writer factories for specific file format. */
public interface FileFormat {

    /**
     * Create a {@link BulkFormat} from the type, with projection pushed down.
     *
     * @param type Type without projection.
     * @param projection See {@link org.apache.flink.table.connector.Projection#toNestedIndexes()}.
     * @param filters A list of filters in conjunctive form for filtering on a best-effort basis.
     */
    BulkFormat<RowData, FileSourceSplit> createReaderFactory(
            RowType type, int[][] projection, List<ResolvedExpression> filters);

    /** Create a {@link BulkWriter.Factory} from the type. */
    BulkWriter.Factory<RowData> createWriterFactory(RowType type);

    default BulkFormat<RowData, FileSourceSplit> createReaderFactory(RowType rowType) {
        int[][] projection = new int[rowType.getFieldCount()][];
        for (int i = 0; i < projection.length; i++) {
            projection[i] = new int[] {i};
        }
        return createReaderFactory(rowType, projection);
    }

    default BulkFormat<RowData, FileSourceSplit> createReaderFactory(
            RowType rowType, int[][] projection) {
        return createReaderFactory(rowType, projection, new ArrayList<>());
    }

    default Optional<FileStatsExtractor> createStatsExtractor(RowType type) {
        return Optional.empty();
    }

    /** Create a {@link FileFormatImpl} from table options. */
    static FileFormat fromTableOptions(
            ClassLoader classLoader,
            Configuration tableOptions,
            ConfigOption<String> formatOption) {
        String formatIdentifier = tableOptions.get(formatOption);
        String formatPrefix = FactoryUtil.getFormatPrefix(formatOption, formatIdentifier);
        ReadableConfig formatOptions = new DelegatingConfiguration(tableOptions, formatPrefix);
        return fromIdentifier(classLoader, formatIdentifier, formatOptions);
    }

    /** Create a {@link FileFormatImpl} from format identifier and format options. */
    static FileFormat fromIdentifier(
            ClassLoader classLoader, String formatIdentifier, ReadableConfig formatOptions) {
        ServiceLoader<FileFormatFactory> serviceLoader =
                ServiceLoader.load(FileFormatFactory.class);
        for (FileFormatFactory factory : serviceLoader) {
            if (factory.identifier().equals(formatIdentifier.toLowerCase())) {
                return factory.create(classLoader, formatOptions);
            }
        }
        return new FileFormatImpl(classLoader, formatIdentifier, formatOptions);
    }
}
