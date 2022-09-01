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

package org.apache.flink.table.store.connector.lookup;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

/** A lookup table which provides get and refresh. */
public interface LookupTable {

    List<RowData> get(RowData key) throws IOException;

    void refresh(Iterator<RowData> incremental) throws IOException;

    static LookupTable create(
            RocksDBStateFactory stateFactory,
            RowType rowType,
            List<String> primaryKey,
            List<String> joinKey,
            Predicate<RowData> recordFilter)
            throws IOException {
        if (new HashSet<>(primaryKey).equals(new HashSet<>(joinKey))) {
            return new PrimaryKeyLookupTable(stateFactory, rowType, joinKey, recordFilter);
        } else {
            return new SecondaryIndexLookupTable(
                    stateFactory, rowType, primaryKey, joinKey, recordFilter);
        }
    }
}
