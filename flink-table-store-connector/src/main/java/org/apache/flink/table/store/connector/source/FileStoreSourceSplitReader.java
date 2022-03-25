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

package org.apache.flink.table.store.connector.source;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.file.src.impl.FileRecords;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.MutableRecordAndPosition;
import org.apache.flink.connector.file.src.util.Pool;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.operation.FileStoreRead;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;

/** The {@link SplitReader} implementation for the file store source. */
public class FileStoreSourceSplitReader
        implements SplitReader<RecordAndPosition<RowData>, FileStoreSourceSplit> {

    private final FileStoreRead fileStoreRead;
    private final boolean valueCountMode;
    @Nullable private final int[][] valueCountModeProjects;

    private final Queue<FileStoreSourceSplit> splits;

    private final Pool<FileStoreRecordIterator> pool;

    @Nullable private RecordReader currentReader;
    @Nullable private String currentSplitId;
    private long currentNumRead;
    private RecordReader.RecordIterator currentFirstBatch;

    @VisibleForTesting
    public FileStoreSourceSplitReader(FileStoreRead fileStoreRead, boolean valueCountMode) {
        this(fileStoreRead, valueCountMode, null);
    }

    public FileStoreSourceSplitReader(
            FileStoreRead fileStoreRead,
            boolean valueCountMode,
            @Nullable int[][] valueCountModeProjects) {
        this.fileStoreRead = fileStoreRead;
        this.valueCountMode = valueCountMode;
        this.valueCountModeProjects = valueCountModeProjects;
        this.splits = new LinkedList<>();
        this.pool = new Pool<>(1);
        this.pool.add(
                valueCountMode ? new ValueCountRecordIterator() : new PrimaryKeyRecordIterator());
    }

    @Override
    public RecordsWithSplitIds<RecordAndPosition<RowData>> fetch() throws IOException {
        checkSplitOrStartNext();

        // pool first, pool size is 1, the underlying implementation does not allow multiple batches
        // to be read at the same time
        FileStoreRecordIterator iterator = pool();

        RecordReader.RecordIterator nextBatch;
        if (currentFirstBatch != null) {
            nextBatch = currentFirstBatch;
            currentFirstBatch = null;
        } else {
            nextBatch = currentReader.readBatch();
        }
        if (nextBatch == null) {
            pool.recycler().recycle(iterator);
            return finishSplit();
        }
        return FileRecords.forRecords(currentSplitId, iterator.replace(nextBatch));
    }

    private FileStoreRecordIterator pool() throws IOException {
        try {
            return this.pool.pollEntry();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted");
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<FileStoreSourceSplit> splitsChange) {
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChange.getClass()));
        }

        splits.addAll(splitsChange.splits());
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        if (currentReader != null) {
            currentReader.close();
        }
    }

    private void checkSplitOrStartNext() throws IOException {
        if (currentReader != null) {
            return;
        }

        final FileStoreSourceSplit nextSplit = splits.poll();
        if (nextSplit == null) {
            throw new IOException("Cannot fetch from another split - no split remaining");
        }

        currentSplitId = nextSplit.splitId();
        currentReader =
                fileStoreRead.createReader(
                        nextSplit.partition(), nextSplit.bucket(), nextSplit.files());
        currentNumRead = nextSplit.recordsToSkip();
        if (currentNumRead > 0) {
            seek(currentNumRead);
        }
    }

    private void seek(long toSkip) throws IOException {
        while (true) {
            RecordReader.RecordIterator nextBatch = currentReader.readBatch();
            if (nextBatch == null) {
                throw new RuntimeException(
                        String.format(
                                "skip(%s) more than the number of remaining elements.", toSkip));
            }
            KeyValue keyValue;
            while (toSkip > 0 && (keyValue = nextBatch.next()) != null) {
                if (valueCountMode) {
                    toSkip -= Math.abs(keyValue.value().getLong(0));
                } else {
                    toSkip--;
                }
            }
            if (toSkip == 0) {
                currentFirstBatch = nextBatch;
                return;
            }
            nextBatch.releaseBatch();
        }
    }

    private FileRecords<RowData> finishSplit() throws IOException {
        if (currentReader != null) {
            currentReader.close();
            currentReader = null;
        }

        final FileRecords<RowData> finishRecords = FileRecords.finishedSplit(currentSplitId);
        currentSplitId = null;
        return finishRecords;
    }

    private abstract class FileStoreRecordIterator implements BulkFormat.RecordIterator<RowData> {

        private RecordReader.RecordIterator iterator;

        protected final MutableRecordAndPosition<RowData> recordAndPosition =
                new MutableRecordAndPosition<>();

        public FileStoreRecordIterator replace(RecordReader.RecordIterator iterator) {
            this.iterator = iterator;
            this.recordAndPosition.set(null, RecordAndPosition.NO_OFFSET, currentNumRead);
            return this;
        }

        protected KeyValue nextKeyValue() throws IOException {
            // The RowData is reused in iterator, we should set back to insert kind
            if (recordAndPosition.getRecord() != null) {
                recordAndPosition.getRecord().setRowKind(RowKind.INSERT);
            }

            return iterator.next();
        }

        @Override
        public void releaseBatch() {
            this.iterator.releaseBatch();
            pool.recycler().recycle(this);
        }
    }

    private class PrimaryKeyRecordIterator extends FileStoreRecordIterator {

        @Nullable
        @Override
        public RecordAndPosition<RowData> next() {
            try {
                KeyValue kv = nextKeyValue();
                if (kv == null) {
                    return null;
                }
                RowData row = kv.value();
                if (kv.valueKind() == ValueKind.DELETE) {
                    row.setRowKind(RowKind.DELETE);
                }
                recordAndPosition.setNext(row);
                currentNumRead++;
                return recordAndPosition;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private class ValueCountRecordIterator extends FileStoreRecordIterator {

        private long count = 0;

        @Nullable
        private final ProjectedRowData projectedRow =
                Optional.ofNullable(valueCountModeProjects)
                        .map(ProjectedRowData::from)
                        .orElse(null);

        @Nullable
        @Override
        public RecordAndPosition<RowData> next() {
            try {
                if (count == 0) {
                    KeyValue kv = nextKeyValue();
                    if (kv == null) {
                        return null;
                    }

                    long value = kv.value().getLong(0);
                    count = Math.abs(value);
                    if (count == 0) {
                        throw new IllegalStateException("count can not be zero.");
                    }

                    RowData row = kv.key();
                    if (value < 0) {
                        row.setRowKind(RowKind.DELETE);
                    }
                    setNext(row);
                } else {
                    // move forward recordSkipCount
                    recordAndPosition.setNext(recordAndPosition.getRecord());
                }
                count--;
                currentNumRead++;
                return recordAndPosition;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void setNext(RowData row) {
            row = projectedRow == null ? row : projectedRow.replaceRow(row);
            recordAndPosition.setNext(row);
        }
    }
}
