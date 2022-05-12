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

import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.StringData;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableStoreMapObjectInspector}. */
public class TableStoreMapObjectInspectorTest {

    @Test
    public void testCategoryAndTypeName() {
        TableStoreMapObjectInspector oi =
                new TableStoreMapObjectInspector(
                        TypeInfoFactory.stringTypeInfo, TypeInfoFactory.longTypeInfo);

        assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.MAP);
        assertThat(oi.getTypeName()).isEqualTo("map<string,bigint>");
    }

    @Test
    public void testGetMapAndValue() {
        TableStoreMapObjectInspector oi =
                new TableStoreMapObjectInspector(
                        TypeInfoFactory.stringTypeInfo, TypeInfoFactory.longTypeInfo);

        StringData[] keyArray =
                new StringData[] {
                    StringData.fromString("Hi"),
                    StringData.fromString("Hello"),
                    StringData.fromString("Test")
                };
        Long[] valueArray = new Long[] {1L, null, 2L};
        Map<StringData, Long> javaMap = new HashMap<>();
        for (int i = 0; i < keyArray.length; i++) {
            javaMap.put(keyArray[i], valueArray[i]);
        }
        GenericMapData mapData = new GenericMapData(javaMap);

        assertThat(oi.getMapSize(mapData)).isEqualTo(3);
        for (int i = 0; i < keyArray.length; i++) {
            assertThat(oi.getMapValueElement(mapData, keyArray[i])).isEqualTo(valueArray[i]);
        }
        assertThat(oi.getMapValueElement(mapData, StringData.fromString("NotKey"))).isNull();
        assertThat(oi.getMapValueElement(mapData, null)).isNull();
        assertThat(oi.getMap(mapData)).isEqualTo(javaMap);

        assertThat(oi.getMapSize(null)).isEqualTo(-1);
        assertThat(oi.getMapValueElement(null, StringData.fromString("Hi"))).isNull();
        assertThat(oi.getMap(null)).isNull();
    }
}
