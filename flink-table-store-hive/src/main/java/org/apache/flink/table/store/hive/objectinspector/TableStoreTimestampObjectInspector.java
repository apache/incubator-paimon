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

package org.apache.flink.table.store.hive.objectinspector;

import org.apache.flink.table.data.TimestampData;

import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.sql.Timestamp;

/** {@link AbstractPrimitiveJavaObjectInspector} for TIMESTAMP type. */
public class TableStoreTimestampObjectInspector extends AbstractPrimitiveJavaObjectInspector
        implements TimestampObjectInspector {

    public TableStoreTimestampObjectInspector() {
        super(TypeInfoFactory.timestampTypeInfo);
    }

    @Override
    public Timestamp getPrimitiveJavaObject(Object o) {
        return o == null ? null : ((TimestampData) o).toTimestamp();
    }

    @Override
    public TimestampWritable getPrimitiveWritableObject(Object o) {
        Timestamp ts = getPrimitiveJavaObject(o);
        return ts == null ? null : new TimestampWritable(ts);
    }

    @Override
    public Object copyObject(Object o) {
        if (o instanceof TimestampData) {
            TimestampData timestampData = (TimestampData) o;
            return TimestampData.fromLocalDateTime(timestampData.toLocalDateTime());
        } else if (o instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) o;
            return new Timestamp(timestamp.getTime());
        } else {
            return o;
        }
    }
}
