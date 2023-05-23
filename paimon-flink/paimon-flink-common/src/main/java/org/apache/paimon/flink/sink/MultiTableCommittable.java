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

package org.apache.paimon.flink.sink;

import org.apache.paimon.catalog.Identifier;

/**
 * MultiTableCommittable produced by {@link PrepareCommitOperator}. This type of Committable will
 * only be produced by multiplexed operators that handles data from multiple tables. Thus, the
 * database, table, and commit user of each table is included in the committable.
 */
public class MultiTableCommittable extends Committable {

    private final String database;
    private final String table;
    private final String commitUser;

    public MultiTableCommittable(
            String database,
            String table,
            String commitUser,
            long checkpointId,
            Kind kind,
            Object wrappedCommittable) {
        super(checkpointId, kind, wrappedCommittable);
        this.database = database;
        this.table = table;
        this.commitUser = commitUser;
    }

    public static MultiTableCommittable fromCommittable(
            Identifier id, String commitUser, Committable committable) {
        return new MultiTableCommittable(
                id.getDatabaseName(),
                id.getTableName(),
                commitUser,
                committable.checkpointId(),
                committable.kind(),
                committable.wrappedCommittable());
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getCommitUser() {
        return commitUser;
    }
}
