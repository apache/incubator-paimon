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

package org.apache.paimon.flink.compact;

import org.apache.paimon.append.AppendOnlyCompactionTask;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static org.apache.paimon.flink.utils.MultiTablesCompactorUtil.shouldCompactTable;

/**
 * This class is responsible for implementing the scanning logic for the table of different type
 * buckets during compaction.
 *
 * @param <T> the result of scanning file :
 *     <ol>
 *       <li>{@link Split} for the table with multi buckets, such as dynamic or fixed bucket table.
 *       <li>{@link AppendOnlyCompactionTask} for the table witch fixed single bucket ,such as
 *           unaware bucket table.
 *     </ol>
 */
public abstract class AbstractBucketScanLogic<T> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractBucketScanLogic.class);
    protected final Catalog.Loader catalogLoader;
    protected final Pattern includingPattern;
    protected final Pattern excludingPattern;
    protected final Pattern databasePattern;

    protected transient Catalog catalog;

    protected AtomicBoolean isRunning;
    protected boolean isStreaming;

    public AbstractBucketScanLogic(
            Catalog.Loader catalogLoader,
            Pattern includingPattern,
            Pattern excludingPattern,
            Pattern databasePattern,
            boolean isStreaming,
            AtomicBoolean isRunning) {
        this.catalogLoader = catalogLoader;
        catalog = catalogLoader.load();

        this.includingPattern = includingPattern;
        this.excludingPattern = excludingPattern;
        this.databasePattern = databasePattern;
        this.isRunning = isRunning;
        this.isStreaming = isStreaming;
    }

    protected void updateTableMap()
            throws Catalog.DatabaseNotExistException, Catalog.TableNotExistException {
        List<String> databases = catalog.listDatabases();

        for (String databaseName : databases) {
            if (databasePattern.matcher(databaseName).matches()) {
                List<String> tables = catalog.listTables(databaseName);
                for (String tableName : tables) {
                    Identifier identifier = Identifier.create(databaseName, tableName);
                    if (shouldCompactTable(identifier, includingPattern, excludingPattern)
                            && (!tableScanned(identifier))) {
                        Table table = catalog.getTable(identifier);
                        if (!(table instanceof FileStoreTable)) {
                            LOG.error(
                                    String.format(
                                            "Only FileStoreTable supports compact action. The table type is '%s'.",
                                            table.getClass().getName()));
                            continue;
                        }

                        FileStoreTable fileStoreTable = (FileStoreTable) table;
                        addScanTable(fileStoreTable, identifier);
                    }
                }
            }
        }
    }

    abstract Boolean collectFiles(SourceFunction.SourceContext<T> ctx)
            throws Catalog.TableNotExistException, Catalog.DatabaseNotExistException;

    /** Check if table has been scanned. */
    abstract boolean tableScanned(Identifier identifier);

    /** Add the scan table to the table map. */
    abstract void addScanTable(FileStoreTable fileStoreTable, Identifier identifier);

    abstract String bucketType();
}
