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

package org.apache.flink.table.store.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.table.store.file.FileStoreOptions.PATH;

/** Factory to create {@link FileStoreTable}. */
public class FileStoreTableFactory {

    public static FileStoreTable create(Path path) {
        Configuration conf = new Configuration();
        conf.set(PATH, path.toString());
        return create(conf);
    }

    public static FileStoreTable create(Configuration conf) {
        return create(conf, UUID.randomUUID().toString());
    }

    public static FileStoreTable create(Configuration conf, String user) {
        Path tablePath = FileStoreOptions.path(conf);
        SchemaManager schemaManager = new SchemaManager(tablePath);
        TableSchema tableSchema =
                schemaManager
                        .latest()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Schema file not found in location "
                                                        + tablePath
                                                        + ". Please create table first."));

        // merge dynamic options into schema.options
        Map<String, String> newOptions = new HashMap<>(tableSchema.options());
        newOptions.putAll(conf.toMap());
        tableSchema = tableSchema.copy(newOptions);

        if (conf.get(FileStoreOptions.WRITE_MODE) == WriteMode.APPEND_ONLY) {
            return new AppendOnlyFileStoreTable(tablePath, schemaManager, tableSchema, user);
        } else {
            if (tableSchema.primaryKeys().isEmpty()) {
                return new ChangelogValueCountFileStoreTable(
                        tablePath, schemaManager, tableSchema, user);
            } else {
                return new ChangelogWithKeyFileStoreTable(
                        tablePath, schemaManager, tableSchema, user);
            }
        }
    }
}
