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

package org.apache.flink.table.store.file.data;

import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.utils.ObjectSerializerTestBase;

/** Tests for {@link DataFileMetaSerializer}. */
public class DataFileMetaSerializerTest extends ObjectSerializerTestBase<DataFileMeta> {

    private final DataFileTestDataGenerator gen = DataFileTestDataGenerator.builder().build();

    @Override
    protected DataFileMetaSerializer serializer() {
        return new DataFileMetaSerializer(
                TestKeyValueGenerator.KEY_TYPE, TestKeyValueGenerator.ROW_TYPE);
    }

    @Override
    protected DataFileMeta object() {
        return gen.next().meta;
    }
}
