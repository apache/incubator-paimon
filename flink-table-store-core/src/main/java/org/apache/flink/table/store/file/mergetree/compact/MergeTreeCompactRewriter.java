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

package org.apache.flink.table.store.file.mergetree.compact;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.compact.CompactResult;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.io.KeyValueFileReaderFactory;
import org.apache.flink.table.store.file.io.KeyValueFileWriterFactory;
import org.apache.flink.table.store.file.io.RollingFileWriter;
import org.apache.flink.table.store.file.mergetree.DropDeleteReader;
import org.apache.flink.table.store.file.mergetree.MergeTreeReaders;
import org.apache.flink.table.store.file.mergetree.SortedRun;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;

import java.util.Comparator;
import java.util.List;

/** Default {@link CompactRewriter} for merge trees. */
public class MergeTreeCompactRewriter extends AbstractCompactRewriter {

    protected final KeyValueFileReaderFactory readerFactory;
    protected final KeyValueFileWriterFactory writerFactory;
    protected final Comparator<RowData> keyComparator;
    protected final MergeFunction<KeyValue> mergeFunction;

    public MergeTreeCompactRewriter(
            KeyValueFileReaderFactory readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<RowData> keyComparator,
            MergeFunction<KeyValue> mergeFunction) {
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.keyComparator = keyComparator;
        this.mergeFunction = mergeFunction;
    }

    @Override
    public void rewrite(
            int outputLevel,
            boolean dropDelete,
            List<List<SortedRun>> sections,
            CompactResult toUpdate)
            throws Exception {
        addBefore(sections, toUpdate);
        rewriteCompaction(outputLevel, dropDelete, sections, toUpdate);
    }

    protected void rewriteCompaction(
            int outputLevel,
            boolean dropDelete,
            List<List<SortedRun>> sections,
            CompactResult toUpdate)
            throws Exception {
        RollingFileWriter<KeyValue, DataFileMeta> writer =
                writerFactory.createRollingMergeTreeFileWriter(outputLevel);
        RecordReader<KeyValue> sectionsReader =
                MergeTreeReaders.readerForSections(
                        sections,
                        readerFactory,
                        keyComparator,
                        new ReducerMergeFunctionWrapper(mergeFunction.copy()));
        if (dropDelete) {
            sectionsReader = new DropDeleteReader(sectionsReader);
        }
        writer.write(new RecordReaderIterator<>(sectionsReader));
        writer.close();
        toUpdate.addAfter(writer.result());
    }

    @Override
    public void upgrade(int outputLevel, DataFileMeta file, CompactResult toUpdate)
            throws Exception {
        toUpdate.addBefore(file);
        toUpdate.addAfter(file.upgrade(outputLevel));
    }
}
