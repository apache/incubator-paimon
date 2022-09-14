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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.store.file.append.AppendOnlyCompactManager;
import org.apache.flink.table.store.file.append.AppendOnlyWriter;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.io.DataFilePathFactory;
import org.apache.flink.table.store.file.io.KeyValueFileReadWriteTest;
import org.apache.flink.table.store.file.io.KeyValueFileWriterFactory;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.LinkedList;
import java.util.List;

/** test file format suffix. */
public class FileFormatSuffixTest extends KeyValueFileReadWriteTest {

    private static final RowType SCHEMA =
            RowType.of(
                    new LogicalType[] {new IntType(), new VarCharType(), new VarCharType()},
                    new String[] {"id", "name", "dt"});

    @Test
    public void testFileSuffix(@TempDir java.nio.file.Path tempDir) throws Exception {
        String format = "avro";
        KeyValueFileWriterFactory writerFactory = createWriterFactory(tempDir.toString(), format);
        Path path = writerFactory.pathFactory().newPath();
        Assertions.assertTrue(path.getPath().endsWith(format));

        DataFilePathFactory dataFilePathFactory =
                new DataFilePathFactory(new Path(tempDir.toString()), "dt=1", 1, format);
        FileFormat fileFormat = FileFormat.fromIdentifier(format, new Configuration());
        LinkedList<DataFileMeta> toCompact = new LinkedList<>();
        AppendOnlyWriter appendOnlyWriter =
                new AppendOnlyWriter(
                        0,
                        fileFormat,
                        10,
                        SCHEMA,
                        toCompact,
                        new AppendOnlyCompactManager(null, toCompact, 4, 10, 10, null), // not used
                        false,
                        dataFilePathFactory);
        appendOnlyWriter.write(
                GenericRowData.of(1, StringData.fromString("aaa"), StringData.fromString("1")));
        List<DataFileMeta> result = appendOnlyWriter.close();

        DataFileMeta meta = result.get(0);
        Assertions.assertTrue(meta.fileName().endsWith(format));
    }
}
