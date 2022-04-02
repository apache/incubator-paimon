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

package org.apache.flink.table.store.file;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.Utils;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.format.FileFormatImpl;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.avro.AvroRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FileFormatImpl}. */
public class FileFormatTest {

    @Test
    public void testWriteRead(@TempDir java.nio.file.Path tempDir) throws IOException {
        FileFormat avro = createFileFormat("snappy");
        RowType rowType = RowType.of(new IntType(), new IntType());

        Path path = new Path(tempDir.toUri().toString(), "1.avro");
        FileSystem fs = path.getFileSystem();

        // write

        List<RowData> expected = new ArrayList<>();
        expected.add(GenericRowData.of(1, 1));
        expected.add(GenericRowData.of(2, 2));
        expected.add(GenericRowData.of(3, 3));
        FSDataOutputStream out = fs.create(path, FileSystem.WriteMode.NO_OVERWRITE);
        BulkWriter<RowData> writer = avro.createWriterFactory(rowType).create(out);
        for (RowData row : expected) {
            writer.addElement(row);
        }
        writer.finish();
        out.close();

        // read
        BulkFormat.Reader<RowData> reader =
                avro.createReaderFactory(rowType)
                        .createReader(
                                new Configuration(),
                                new FileSourceSplit("", path, 0, fs.getFileStatus(path).getLen()));
        List<RowData> result = new ArrayList<>();
        Utils.forEachRemaining(
                reader,
                rowData -> result.add(GenericRowData.of(rowData.getInt(0), rowData.getInt(0))));

        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testUnsupportedOption(@TempDir java.nio.file.Path tempDir) {
        BulkWriter.Factory<RowData> writerFactory =
                createFileFormat("_unsupported").createWriterFactory(RowType.of(new IntType()));
        Path path = new Path(tempDir.toUri().toString(), "1.avro");
        Assertions.assertThrows(
                AvroRuntimeException.class,
                () ->
                        writerFactory.create(
                                path.getFileSystem()
                                        .create(path, FileSystem.WriteMode.NO_OVERWRITE)),
                "Unrecognized codec: _unsupported");
    }

    public FileFormat createFileFormat(String codec) {
        Configuration tableOptions = new Configuration();
        tableOptions.set(FileStoreOptions.FILE_FORMAT, "avro");
        tableOptions.setString("avro.codec", codec);
        return FileFormat.fromTableOptions(
                this.getClass().getClassLoader(), tableOptions, FileStoreOptions.FILE_FORMAT);
    }
}
