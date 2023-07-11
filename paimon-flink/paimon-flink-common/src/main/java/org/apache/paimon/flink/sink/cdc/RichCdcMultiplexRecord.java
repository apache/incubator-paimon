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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/** Compared to {@link CdcMultiplexRecord}, this contains schema information. */
public class RichCdcMultiplexRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private final RowKind kind;
    private final Map<String, DataType> fieldTypes;
    private final Map<String, String> fieldValues;
    private final String databaseName;
    private final String tableName;

    public RichCdcMultiplexRecord(
            RowKind kind,
            Map<String, DataType> fieldTypes,
            Map<String, String> fieldValues,
            String databaseName,
            String tableName) {
        this.kind = kind;
        this.fieldTypes = fieldTypes;
        this.fieldValues = fieldValues;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public Map<String, DataType> fieldTypes() {
        return fieldTypes;
    }

    public Map<String, String> fieldValues() {
        return fieldValues;
    }

    public String tableName() {
        return tableName;
    }

    public CdcRecord toCdcRecord() {
        return new CdcRecord(kind, fieldValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, fieldTypes, fieldValues, databaseName, tableName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RichCdcMultiplexRecord that = (RichCdcMultiplexRecord) o;
        return kind == that.kind
                && Objects.equals(fieldTypes, that.fieldTypes)
                && Objects.equals(fieldValues, that.fieldValues)
                && databaseName.equals(that.databaseName)
                && tableName.equals(that.tableName);
    }

    @Override
    public String toString() {
        return "{"
                + "kind="
                + kind
                + ", fieldTypes="
                + fieldTypes
                + ", fieldValues="
                + fieldValues
                + ", databaseName="
                + databaseName
                + ", tableName="
                + tableName
                + '}';
    }
}
