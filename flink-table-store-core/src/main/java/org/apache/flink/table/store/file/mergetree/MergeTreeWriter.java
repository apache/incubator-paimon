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

package org.apache.flink.table.store.file.mergetree;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.mergetree.compact.Accumulator;
import org.apache.flink.table.store.file.mergetree.compact.CompactManager;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;
import org.apache.flink.table.store.file.mergetree.sst.SstFileWriter;
import org.apache.flink.table.store.file.utils.RecordWriter;
import org.apache.flink.util.CloseableIterator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/** A {@link RecordWriter} to write records and generate {@link Increment}. */
public class MergeTreeWriter implements RecordWriter {

    private final MemTable memTable;

    private final CompactManager compactManager;

    private final Levels levels;

    private final Comparator<RowData> keyComparator;

    private final Accumulator accumulator;

    private final SstFileWriter sstFileWriter;

    private final boolean commitForceCompact;

    private final LinkedHashSet<SstFileMeta> newFiles;

    private final LinkedHashMap<String, SstFileMeta> compactBefore;

    private final LinkedHashSet<SstFileMeta> compactAfter;

    private long newSequenceNumber;

    public MergeTreeWriter(
            MemTable memTable,
            CompactManager compactManager,
            Levels levels,
            long maxSequenceNumber,
            Comparator<RowData> keyComparator,
            Accumulator accumulator,
            SstFileWriter sstFileWriter,
            boolean commitForceCompact) {
        this.memTable = memTable;
        this.compactManager = compactManager;
        this.levels = levels;
        this.newSequenceNumber = maxSequenceNumber + 1;
        this.keyComparator = keyComparator;
        this.accumulator = accumulator;
        this.sstFileWriter = sstFileWriter;
        this.commitForceCompact = commitForceCompact;
        this.newFiles = new LinkedHashSet<>();
        this.compactBefore = new LinkedHashMap<>();
        this.compactAfter = new LinkedHashSet<>();
    }

    private long newSequenceNumber() {
        return newSequenceNumber++;
    }

    @VisibleForTesting
    Levels levels() {
        return levels;
    }

    @Override
    public void write(ValueKind valueKind, RowData key, RowData value) throws Exception {
        long sequenceNumber = newSequenceNumber();
        boolean success = memTable.put(sequenceNumber, valueKind, key, value);
        if (!success) {
            flush();
            success = memTable.put(sequenceNumber, valueKind, key, value);
            if (!success) {
                throw new RuntimeException("Mem table is too small to hold a single element.");
            }
        }
    }

    private void flush() throws Exception {
        if (memTable.size() > 0) {
            finishCompaction();
            Iterator<KeyValue> iterator = memTable.iterator(keyComparator, accumulator);
            List<SstFileMeta> files =
                    sstFileWriter.write(CloseableIterator.adapterForIterator(iterator), 0);
            newFiles.addAll(files);
            files.forEach(levels::addLevel0File);
            memTable.clear();
            submitCompaction();
        }
    }

    @Override
    public Increment prepareCommit() throws Exception {
        flush();
        if (commitForceCompact) {
            finishCompaction();
        }
        return drainIncrement();
    }

    @Override
    public void sync() throws Exception {
        finishCompaction();
    }

    private Increment drainIncrement() {
        Increment increment =
                new Increment(
                        new ArrayList<>(newFiles),
                        new ArrayList<>(compactBefore.values()),
                        new ArrayList<>(compactAfter));
        newFiles.clear();
        compactBefore.clear();
        compactAfter.clear();
        return increment;
    }

    private void updateCompactResult(CompactManager.CompactResult result) {
        Set<String> afterFiles =
                result.after().stream().map(SstFileMeta::fileName).collect(Collectors.toSet());
        for (SstFileMeta file : result.before()) {
            if (compactAfter.remove(file)) {
                // This is an intermediate file (not a new data file), which is no longer needed
                // after compaction and can be deleted directly, but upgrade file is required by
                // previous snapshot and following snapshot, so we should ensure:
                // 1. This file is not the output of upgraded.
                // 2. This file is not the input of upgraded.
                if (!compactBefore.containsKey(file.fileName())
                        && !afterFiles.contains(file.fileName())) {
                    sstFileWriter.delete(file);
                }
            } else {
                compactBefore.put(file.fileName(), file);
            }
        }
        compactAfter.addAll(result.after());
    }

    private void submitCompaction() {
        compactManager.submitCompaction(levels);
    }

    private void finishCompaction() throws ExecutionException, InterruptedException {
        Optional<CompactManager.CompactResult> result = compactManager.finishCompaction(levels);
        result.ifPresent(this::updateCompactResult);
    }

    @Override
    public List<SstFileMeta> close() throws Exception {
        sync();
        // delete temporary files
        List<SstFileMeta> delete = new ArrayList<>(newFiles);
        for (SstFileMeta file : compactAfter) {
            // upgrade file is required by previous snapshot, so we should ensure that this file is
            // not the output of upgraded.
            if (!compactBefore.containsKey(file.fileName())) {
                delete.add(file);
            }
        }
        for (SstFileMeta file : delete) {
            sstFileWriter.delete(file);
        }
        newFiles.clear();
        compactAfter.clear();
        return delete;
    }
}
