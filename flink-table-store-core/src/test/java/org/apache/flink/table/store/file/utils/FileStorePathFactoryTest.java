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

package org.apache.flink.table.store.file.utils;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.store.file.mergetree.sst.SstPathFactory;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileStorePathFactory}. */
public class FileStorePathFactoryTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testManifestPaths() {
        FileStorePathFactory pathFactory = new FileStorePathFactory(new Path(tempDir.toString()));
        String uuid = pathFactory.uuid();

        for (int i = 0; i < 20; i++) {
            assertThat(pathFactory.newManifestFile())
                    .isEqualTo(
                            new Path(tempDir.toString() + "/manifest/manifest-" + uuid + "-" + i));
        }
        assertThat(pathFactory.toManifestFilePath("my-manifest-file-name"))
                .isEqualTo(new Path(tempDir.toString() + "/manifest/my-manifest-file-name"));

        for (int i = 0; i < 20; i++) {
            assertThat(pathFactory.newManifestList())
                    .isEqualTo(
                            new Path(
                                    tempDir.toString()
                                            + "/manifest/manifest-list-"
                                            + uuid
                                            + "-"
                                            + i));
        }
        assertThat(pathFactory.toManifestListPath("my-manifest-list-file-name"))
                .isEqualTo(new Path(tempDir.toString() + "/manifest/my-manifest-list-file-name"));
    }

    @Test
    public void testSnapshotPath() {
        FileStorePathFactory pathFactory = new FileStorePathFactory(new Path(tempDir.toString()));
        for (int i = 0; i < 20; i++) {
            assertThat(pathFactory.toSnapshotPath(i))
                    .isEqualTo(new Path(tempDir.toString() + "/snapshot/snapshot-" + i));
        }
    }

    @Test
    public void testCreateSstPathFactoryNoPartition() {
        FileStorePathFactory pathFactory = new FileStorePathFactory(new Path(tempDir.toString()));
        SstPathFactory sstPathFactory = pathFactory.createSstPathFactory(new BinaryRowData(0), 123);
        assertThat(sstPathFactory.toPath("my-sst-file-name"))
                .isEqualTo(new Path(tempDir.toString() + "/bucket-123/my-sst-file-name"));
    }

    @Test
    public void testCreateSstPathFactoryWithPartition() {
        FileStorePathFactory pathFactory =
                new FileStorePathFactory(
                        new Path(tempDir.toString()),
                        RowType.of(
                                new LogicalType[] {new VarCharType(10), new IntType()},
                                new String[] {"dt", "hr"}),
                        "default");

        assertPartition("20211224", 16, pathFactory, "/dt=20211224/hr=16");
        assertPartition("20211224", null, pathFactory, "/dt=20211224/hr=default");
        assertPartition(null, 16, pathFactory, "/dt=default/hr=16");
        assertPartition(null, null, pathFactory, "/dt=default/hr=default");
    }

    private void assertPartition(
            String dt, Integer hr, FileStorePathFactory pathFactory, String expected) {
        BinaryRowData partition = new BinaryRowData(2);
        BinaryRowWriter writer = new BinaryRowWriter(partition);
        if (dt != null) {
            writer.writeString(0, StringData.fromString(dt));
        } else {
            writer.setNullAt(0);
        }
        if (hr != null) {
            writer.writeInt(1, 16);
        } else {
            writer.setNullAt(1);
        }
        writer.complete();
        SstPathFactory sstPathFactory = pathFactory.createSstPathFactory(partition, 123);
        assertThat(sstPathFactory.toPath("my-sst-file-name"))
                .isEqualTo(
                        new Path(tempDir.toString() + expected + "/bucket-123/my-sst-file-name"));
    }
}
