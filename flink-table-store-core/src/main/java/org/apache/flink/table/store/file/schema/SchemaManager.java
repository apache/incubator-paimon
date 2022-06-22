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

package org.apache.flink.table.store.file.schema;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.file.operation.Lock;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.utils.JsonSerdeUtil;
import org.apache.flink.table.store.file.utils.MetaFileWriter;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.file.utils.FileUtils.listVersionedFiles;

/** Schema Manager to manage schema versions. */
public class SchemaManager implements Serializable {

    private static final String SCHEMA_PREFIX = "schema-";

    private final Path tableRoot;

    public SchemaManager(Path tableRoot) {
        this.tableRoot = tableRoot;
    }

    /** @return latest schema. */
    public Optional<Schema> latest() {
        try {
            return listVersionedFiles(schemaDirectory(), SCHEMA_PREFIX)
                    .reduce(Math::max)
                    .map(this::schema);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** List all schema. */
    public List<Schema> listAll() {
        return listAllIds().stream().map(this::schema).collect(Collectors.toList());
    }

    /** List all schema IDs. */
    public List<Long> listAllIds() {
        try {
            return listVersionedFiles(schemaDirectory(), SCHEMA_PREFIX)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Create a new schema from {@link UpdateSchema}. */
    public Schema commitNewVersion(UpdateSchema updateSchema) throws Exception {
        return commitNewVersion(Callable::call, updateSchema);
    }

    /** Create a new schema from {@link UpdateSchema}. */
    public Schema commitNewVersion(Lock lock, UpdateSchema updateSchema) throws Exception {
        RowType rowType = updateSchema.rowType();
        List<String> partitionKeys = updateSchema.partitionKeys();
        List<String> primaryKeys = updateSchema.primaryKeys();
        Map<String, String> options = updateSchema.options();

        while (true) {
            long id;
            int highestFieldId;
            List<DataField> fields;
            Optional<Schema> latest = latest();
            if (latest.isPresent()) {
                Schema oldSchema = latest.get();
                Preconditions.checkArgument(
                        oldSchema.primaryKeys().equals(primaryKeys),
                        "Primary key modification is not supported, "
                                + "old primaryKeys is %s, new primaryKeys is %s",
                        oldSchema.primaryKeys(),
                        primaryKeys);

                if (!updateSchema
                                .rowType()
                                .getFields()
                                .equals(oldSchema.logicalRowType().getFields())
                        || !updateSchema.partitionKeys().equals(oldSchema.partitionKeys())) {
                    throw new UnsupportedOperationException(
                            "TODO: support update field types and partition keys. ");
                }

                fields = oldSchema.fields();
                id = oldSchema.id() + 1;
                highestFieldId = oldSchema.highestFieldId();
            } else {
                fields = Schema.newFields(rowType);
                highestFieldId = Schema.currentHighestFieldId(fields);
                id = 0;
            }

            Schema schema =
                    new Schema(
                            id,
                            fields,
                            highestFieldId,
                            partitionKeys,
                            primaryKeys,
                            options,
                            updateSchema.comment());

            Path schemaPath = toSchemaPath(id);

            FileSystem fs = schemaPath.getFileSystem();
            boolean success =
                    lock.runWithLock(
                            () -> {
                                if (fs.exists(schemaPath)) {
                                    return false;
                                }

                                return MetaFileWriter.writeFileSafety(
                                        schemaPath, schema.toString());
                            });
            if (success) {
                return schema;
            }
        }
    }

    /** Read schema for schema id. */
    public Schema schema(long id) {
        try {
            return JsonSerdeUtil.fromJson(FileUtils.readFileUtf8(toSchemaPath(id)), Schema.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path schemaDirectory() {
        return new Path(tableRoot + "/schema");
    }

    private Path toSchemaPath(long id) {
        return new Path(tableRoot + "/schema/" + SCHEMA_PREFIX + id);
    }
}
