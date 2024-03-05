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

package org.apache.paimon.flink.source;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

/** Unit tests for the {@link FileStoreSourceReader}. */
public class FileStoreSourceReaderFromBranchTest extends FileStoreSourceReaderTest {

    @BeforeEach
    public void beforeEach() throws Exception {
        branch = "testBranch-" + UUID.randomUUID();
        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(tempDir.toUri()), branch);
        schemaManager.createTable(
                new Schema(
                        new RowType(
                                        Arrays.asList(
                                                new DataField(0, "k", new BigIntType()),
                                                new DataField(1, "v", new BigIntType()),
                                                new DataField(2, "default", new IntType())))
                                .getFields(),
                        Collections.singletonList("default"),
                        Arrays.asList("k", "default"),
                        Collections.emptyMap(),
                        null));
    }
}
