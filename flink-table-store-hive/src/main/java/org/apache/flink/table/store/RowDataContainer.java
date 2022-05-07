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

package org.apache.flink.table.store;

import org.apache.flink.table.data.RowData;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** A reusable object to hold {@link RowData}. */
public class RowDataContainer implements Writable {

    private RowData rowData;

    public RowData get() {
        return rowData;
    }

    public void set(RowData rowData) {
        this.rowData = rowData;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        throw new UnsupportedOperationException();
    }
}
