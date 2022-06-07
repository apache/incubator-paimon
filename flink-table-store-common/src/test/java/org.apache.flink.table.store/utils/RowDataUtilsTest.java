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

package org.apache.flink.table.store.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.table.RandomGeneratorVisitor;
import org.apache.flink.connector.datagen.table.types.RowDataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RowDataUtils}. */
public class RowDataUtilsTest {

    public static final RowType ROW_TYPE =
            (RowType)
                    TableSchema.builder()
                            .field("id", DataTypes.INT().notNull())
                            .field("name", DataTypes.STRING()) /* optional by default */
                            .field("salary", DataTypes.DOUBLE().notNull())
                            .field("strArray", DataTypes.ARRAY(DataTypes.STRING()).nullable())
                            .field("intArray", DataTypes.ARRAY(DataTypes.INT()).nullable())
                            .field("char", DataTypes.CHAR(10).notNull())
                            .field("varchar", DataTypes.VARCHAR(10).notNull())
                            .field("boolean", DataTypes.BOOLEAN().nullable())
                            .field("tinyint", DataTypes.TINYINT())
                            .field("smallint", DataTypes.SMALLINT())
                            .field("bigint", DataTypes.BIGINT())
                            .field("timestampWithoutZone", DataTypes.TIMESTAMP())
                            .field("timestampWithZone", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())
                            .field("date", DataTypes.DATE())
                            .field("decimal", DataTypes.DECIMAL(2, 2))
                            .field("decimal2", DataTypes.DECIMAL(38, 2))
                            .field("decimal3", DataTypes.DECIMAL(10, 1))
                            .build()
                            .toRowDataType()
                            .getLogicalType();

    private RowDataGenerator rowDataGenerator;

    private RowDataSerializer serializer;

    @BeforeEach
    public void before() throws Exception {
        DataGenerator[] generators =
                ROW_TYPE.getFields().stream()
                        .map(
                                field ->
                                        field.getType()
                                                .accept(
                                                        new RandomGeneratorVisitor(
                                                                field.getName(),
                                                                new Configuration()))
                                                .getGenerator())
                        .toArray(DataGenerator[]::new);
        this.rowDataGenerator = new RowDataGenerator(generators, ROW_TYPE.getFieldNames());
        this.rowDataGenerator.open(null, null, null);
        this.serializer = new RowDataSerializer(ROW_TYPE);
    }

    @Test
    public void testCopy() {
        for (int i = 0; i < 10; i++) {
            RowData row = rowDataGenerator.next();
            RowData copied = RowDataUtils.copyRowData(row, ROW_TYPE);
            assertThat(toBinary(copied)).isEqualTo(toBinary(row));
            RowData copied2 = serializer.copy(row);

            // check copied
            for (int j = 0; j < copied.getArity(); j++) {
                Object origin = RowDataUtils.get(row, j, ROW_TYPE.getTypeAt(j));
                Object field1 = RowDataUtils.get(copied, j, ROW_TYPE.getTypeAt(j));
                Object field2 = RowDataUtils.get(copied2, j, ROW_TYPE.getTypeAt(j));

                if (field2 != origin) {
                    assertThat(field1).isNotSameAs(origin);
                }
            }
        }
    }

    private BinaryRowData toBinary(RowData row) {
        return serializer.toBinaryRow(row).copy();
    }
}
