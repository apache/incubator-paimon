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

package org.apache.paimon.operation;

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexPredicate;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatKey;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.FileRecordReader;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.IndexCastMapping;
import org.apache.paimon.schema.SchemaEvolutionUtil;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BulkFormatMapping;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Projection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;

/** {@link FileStoreRead} for {@link AppendOnlyFileStore}. */
public class AppendOnlyFileStoreRead implements FileStoreRead<InternalRow> {

    private static final Logger LOG = LoggerFactory.getLogger(AppendOnlyFileStoreRead.class);

    private final FileIO fileIO;
    private final SchemaManager schemaManager;
    private final TableSchema schema;
    private final FileFormatDiscover formatDiscover;
    private final FileStorePathFactory pathFactory;
    private final Map<FormatKey, BulkFormatMapping> bulkFormatMappings;
    private final boolean fileIndexReadEnabled;

    private int[][] projection;

    @Nullable private List<Predicate> filters;

    public AppendOnlyFileStoreRead(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            RowType rowType,
            FileFormatDiscover formatDiscover,
            FileStorePathFactory pathFactory,
            boolean fileIndexReadEnabled) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.schema = schema;
        this.formatDiscover = formatDiscover;
        this.pathFactory = pathFactory;
        this.bulkFormatMappings = new HashMap<>();
        this.fileIndexReadEnabled = fileIndexReadEnabled;

        this.projection = Projection.range(0, rowType.getFieldCount()).toNestedIndexes();
    }

    public FileStoreRead<InternalRow> withProjection(int[][] projectedFields) {
        projection = projectedFields;
        return this;
    }

    @Override
    public FileStoreRead<InternalRow> withFilter(Predicate predicate) {
        this.filters = splitAnd(predicate);
        return this;
    }

    @Override
    public RecordReader<InternalRow> createReader(DataSplit split) throws IOException {
        DataFilePathFactory dataFilePathFactory =
                pathFactory.createDataFilePathFactory(split.partition(), split.bucket());
        List<ConcatRecordReader.ReaderSupplier<InternalRow>> suppliers = new ArrayList<>();
        if (split.beforeFiles().size() > 0) {
            LOG.info("Ignore split before files: " + split.beforeFiles());
        }
        // use this to cache evolved predicates
        Map<FormatKey, List<Predicate>> filePredicates = new HashMap<>();
        Map<FormatKey, TableSchema> fileSchema = new HashMap<>();
        for (DataFileMeta file : split.dataFiles()) {
            String formatIdentifier = DataFilePathFactory.formatIdentifier(file.fileName());
            FormatKey formatKey = new FormatKey(file.schemaId(), formatIdentifier);
            BulkFormatMapping bulkFormatMapping =
                    bulkFormatMappings.computeIfAbsent(
                            formatKey,
                            key -> {
                                TableSchema tableSchema = schema;
                                TableSchema dataSchema =
                                        key.schemaId == schema.id()
                                                ? schema
                                                : schemaManager.schema(key.schemaId);

                                // projection to data schema
                                int[][] dataProjection =
                                        SchemaEvolutionUtil.createDataProjection(
                                                tableSchema.fields(),
                                                dataSchema.fields(),
                                                projection);

                                IndexCastMapping indexCastMapping =
                                        SchemaEvolutionUtil.createIndexCastMapping(
                                                Projection.of(projection).toTopLevelIndexes(),
                                                tableSchema.fields(),
                                                Projection.of(dataProjection).toTopLevelIndexes(),
                                                dataSchema.fields());

                                List<Predicate> dataFilters =
                                        this.schema.id() == key.schemaId
                                                ? filters
                                                : SchemaEvolutionUtil.createDataFilters(
                                                        tableSchema.fields(),
                                                        dataSchema.fields(),
                                                        filters);

                                Pair<int[], RowType> partitionPair = null;
                                if (!dataSchema.partitionKeys().isEmpty()) {
                                    Pair<int[], int[][]> partitionMapping =
                                            PartitionUtils.constructPartitionMapping(
                                                    dataSchema, dataProjection);
                                    // if partition fields are not selected, we just do nothing
                                    if (partitionMapping != null) {
                                        dataProjection = partitionMapping.getRight();
                                        partitionPair =
                                                Pair.of(
                                                        partitionMapping.getLeft(),
                                                        dataSchema.projectedLogicalRowType(
                                                                dataSchema.partitionKeys()));
                                    }
                                }

                                RowType projectedRowType =
                                        Projection.of(dataProjection)
                                                .project(dataSchema.logicalRowType());

                                fileSchema.put(key, dataSchema);
                                if (dataFilters != null) {
                                    filePredicates.put(key, dataFilters);
                                }
                                return new BulkFormatMapping(
                                        indexCastMapping.getIndexMapping(),
                                        indexCastMapping.getCastMapping(),
                                        partitionPair,
                                        formatDiscover
                                                .discover(formatIdentifier)
                                                .createReaderFactory(
                                                        projectedRowType, dataFilters));
                            });

            List<Predicate> dataFilter = filePredicates.getOrDefault(formatKey, null);
            if (dataFilter != null && !dataFilter.isEmpty()) {
                List<String> indexFiles =
                        file.extraFiles().stream()
                                .filter(
                                        name ->
                                                name.startsWith(
                                                        DataFilePathFactory.INDEX_PATH_PREFIX))
                                .collect(Collectors.toList());
                if (fileIndexReadEnabled && !indexFiles.isEmpty()) {
                    // go to file index check
                    try (FileIndexPredicate predicate =
                            new FileIndexPredicate(
                                    dataFilePathFactory.toPath(indexFiles.get(0)),
                                    fileIO,
                                    fileSchema.get(formatKey).logicalRowType())) {
                        if (!predicate.testPredicate(
                                PredicateBuilder.and(dataFilter.toArray(new Predicate[0])))) {
                            continue;
                        }
                    }
                }
            }

            final BinaryRow partition = split.partition();
            suppliers.add(
                    () ->
                            new FileRecordReader(
                                    bulkFormatMapping.getReaderFactory(),
                                    new FormatReaderContext(
                                            fileIO,
                                            dataFilePathFactory.toPath(file.fileName()),
                                            file.fileSize()),
                                    bulkFormatMapping.getIndexMapping(),
                                    bulkFormatMapping.getCastMapping(),
                                    PartitionUtils.create(
                                            bulkFormatMapping.getPartitionPair(), partition)));
        }

        return ConcatRecordReader.create(suppliers);
    }
}
