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

import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.schema.Schema;
import org.apache.flink.table.store.table.sink.TableCommit;
import org.apache.flink.table.types.logical.RowType;

/** Abstract {@link FileStoreTable}. */
public abstract class AbstractFileStoreTable implements FileStoreTable {

    private static final long serialVersionUID = 1L;

    private final String name;
    protected final Schema schema;

    public AbstractFileStoreTable(String name, Schema schema) {
        this.name = name;
        this.schema = schema;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public RowType rowType() {
        return schema.logicalRowType();
    }

    @Override
    public TableCommit newCommit() {
        return new TableCommit(store().newCommit(), store().newExpire());
    }

    protected abstract FileStore store();
}
