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

package org.apache.flink.table.store.table.sink;

import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.operation.FileStoreWrite;

import javax.annotation.Nullable;

import java.util.List;

/**
 * An abstraction layer above {@link org.apache.flink.table.store.file.operation.FileStoreWrite} to
 * provide {@link RowData} writing.
 */
public interface TableWrite extends AutoCloseable {

    TableWrite withOverwrite(boolean overwrite);

    TableWrite withIOManager(IOManager ioManager);

    SinkRecord write(RowData rowData) throws Exception;

    /** Log record need to preserve original pk (which includes partition fields). */
    SinkRecord toLogRecord(SinkRecord record);

    default void compact(BinaryRowData partition, int bucket, boolean fullCompaction)
            throws Exception {
        compact(partition, bucket, fullCompaction, null);
    }

    void compact(
            BinaryRowData partition,
            int bucket,
            boolean fullCompaction,
            @Nullable FileStoreWrite.ExtraCompactFiles extraCompactFiles)
            throws Exception;

    List<FileCommittable> prepareCommit(boolean blocking, long commitIdentifier) throws Exception;

    void close() throws Exception;
}
