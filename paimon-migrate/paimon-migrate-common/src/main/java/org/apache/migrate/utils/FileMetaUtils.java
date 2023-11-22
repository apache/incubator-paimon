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

package org.apache.migrate.utils;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.format.FieldStats;
import org.apache.paimon.format.TableStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.NewFilesIncrement;
import org.apache.paimon.statistics.FieldStatsCollector;
import org.apache.paimon.stats.BinaryTableStats;
import org.apache.paimon.stats.FieldStatsArraySerializer;
import org.apache.paimon.table.AbstractFileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.StatsCollectorFactories;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** To construct file meta data for external files. */
public class FileMetaUtils {

    public static List<DataFileMeta> construct(
            FileIO fileIO,
            String format,
            String location,
            Table paimonTable,
            Predicate<FileStatus> filter)
            throws IOException {
        List<FileStatus> fileStatuses =
                Arrays.stream(fileIO.listStatus(new Path(location)))
                        .filter(s -> !s.isDir())
                        .filter(filter)
                        .collect(Collectors.toList());

        return fileStatuses.stream()
                .map(status -> constructFileMeta(format, location, status, fileIO, paimonTable))
                .collect(Collectors.toList());
    }

    public static CommitMessage commitFile(BinaryRow partition, List<DataFileMeta> dataFileMetas) {
        return new CommitMessageImpl(
                partition,
                0,
                new NewFilesIncrement(dataFileMetas, Collections.emptyList()),
                new CompactIncrement(
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
    }

    // -----------------------------private method---------------------------------------------

    private static DataFileMeta constructFileMeta(
            String format, String location, FileStatus fileStatus, FileIO fileIO, Table table) {

        try {
            FieldStatsCollector.Factory[] factories =
                    StatsCollectorFactories.createStatsFactories(
                            ((AbstractFileStoreTable) table).coreOptions(),
                            table.rowType().getFieldNames());
            TableStatsExtractor tableStatsExtractor =
                    ((AbstractFileStoreTable) table)
                            .coreOptions()
                            .fileFormat()
                            .createStatsExtractor(table.rowType(), factories)
                            .get();
            return constructFileMeta(
                    location, format, fileStatus, tableStatsExtractor, fileIO, table);
        } catch (IOException e) {
            throw new RuntimeException("error when construct file meta", e);
        }
    }

    private static DataFileMeta constructFileMeta(
            String location,
            String format,
            FileStatus fileStatus,
            TableStatsExtractor tableStatsExtractor,
            FileIO fileIO,
            Table table)
            throws IOException {
        Path path = fileStatus.getPath();
        FieldStatsArraySerializer statsArraySerializer =
                new FieldStatsArraySerializer(table.rowType());

        Pair<FieldStats[], TableStatsExtractor.FileInfo> fileInfo =
                tableStatsExtractor.extractWithFileInfo(fileIO, path);
        BinaryTableStats stats = statsArraySerializer.toBinary(fileInfo.getLeft());

        return DataFileMeta.forAppend(
                path.getName(),
                fileStatus.getLen(),
                fileInfo.getRight().getRowCount(),
                stats,
                0,
                0,
                ((AbstractFileStoreTable) table).schema().id(),
                location,
                format);
    }

    public static BinaryRow writePartitionValue(
            RowType partitionRowType,
            Map<String, String> partitionValues,
            List<DataConverter> dataConverters) {

        BinaryRow binaryRow = new BinaryRow(partitionRowType.getFieldCount());
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(binaryRow);

        List<DataField> fields = partitionRowType.getFields();

        for (int i = 0; i < fields.size(); i++) {
            dataConverters
                    .get(i)
                    .write(binaryRowWriter, i, partitionValues.get(fields.get(i).name()));
        }
        binaryRowWriter.complete();
        return binaryRow;
    }
}
