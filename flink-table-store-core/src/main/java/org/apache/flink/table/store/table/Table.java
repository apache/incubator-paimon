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

import org.apache.flink.table.store.annotation.Experimental;
import org.apache.flink.table.store.table.sink.WriteBuilder;
import org.apache.flink.table.store.table.source.ReadBuilder;
import org.apache.flink.table.store.types.RowType;

import java.io.Serializable;
import java.util.Map;

/**
 * A table provides basic abstraction for table type and table scan and table read.
 *
 * @since 0.4.0
 */
@Experimental
public interface Table extends Serializable {

    /** A name to identify this table. */
    String name();

    /** Returns the row type of this table. */
    RowType rowType();

    /** Copy this table with adding dynamic options. */
    Table copy(Map<String, String> dynamicOptions);

    /** Returns a new read builder. */
    ReadBuilder newReadBuilder();

    /** Returns a new write builder. */
    WriteBuilder newWriteBuilder();
}
