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

package org.apache.flink.table.store.hive;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/** Util class for generating random {@link GenericRowData}. */
public class RandomGenericRowDataGenerator {

    public static final List<TypeInfo> TYPE_INFOS =
            Arrays.asList(
                    TypeInfoFactory.booleanTypeInfo,
                    TypeInfoFactory.byteTypeInfo,
                    TypeInfoFactory.shortTypeInfo,
                    TypeInfoFactory.intTypeInfo,
                    TypeInfoFactory.longTypeInfo,
                    TypeInfoFactory.floatTypeInfo,
                    TypeInfoFactory.doubleTypeInfo,
                    TypeInfoFactory.getDecimalTypeInfo(5, 3),
                    TypeInfoFactory.getDecimalTypeInfo(28, 6),
                    TypeInfoFactory.getCharTypeInfo(10),
                    TypeInfoFactory.getVarcharTypeInfo(10),
                    TypeInfoFactory.stringTypeInfo,
                    TypeInfoFactory.binaryTypeInfo,
                    TypeInfoFactory.dateTypeInfo,
                    TypeInfoFactory.timestampTypeInfo,
                    TypeInfoFactory.getListTypeInfo(TypeInfoFactory.longTypeInfo),
                    TypeInfoFactory.getMapTypeInfo(
                            TypeInfoFactory.stringTypeInfo, TypeInfoFactory.intTypeInfo));

    public static final List<String> TYPE_NAMES =
            Arrays.asList(
                    "boolean",
                    "tinyint",
                    "smallint",
                    "int",
                    "bigint",
                    "float",
                    "double",
                    "decimal(5,3)",
                    "decimal(28,6)",
                    "char(10)",
                    "varchar(10)",
                    "string",
                    "binary",
                    "date",
                    "timestamp",
                    "array<bigint>",
                    "map<string,int>");

    public static final List<String> FIELD_NAMES =
            Arrays.asList(
                    "f_boolean",
                    "f_byte",
                    "f_short",
                    "f_int",
                    "f_long",
                    "f_float",
                    "f_double",
                    "f_decimal_5_3",
                    "f_decimal_28_6",
                    "f_char_10",
                    "f_varchar_10",
                    "f_string",
                    "f_binary",
                    "f_date",
                    "f_timestamp",
                    "f_list_long",
                    "f_map_string_int");

    public static final List<String> FIELD_COMMENTS =
            Arrays.asList(
                    "comment_boolean",
                    "comment_byte",
                    "comment_short",
                    "comment_int",
                    "comment_long",
                    "comment_float",
                    "comment_double",
                    "comment_decimal_5_3",
                    "comment_decimal_28_6",
                    "comment_char_10",
                    "comment_varchar_10",
                    "comment_string",
                    "comment_binary",
                    "comment_date",
                    "comment_timestamp",
                    "comment_list_long",
                    "comment_map_string_int");

    public static GenericRowData generate() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        byte[] randomBytes = new byte[random.nextInt(20)];
        random.nextBytes(randomBytes);
        Long[] randomLongArray = new Long[random.nextInt(20)];
        for (int i = 0; i < randomLongArray.length; i++) {
            if (random.nextBoolean()) {
                randomLongArray[i] = null;
            } else {
                randomLongArray[i] = random.nextLong();
            }
        }
        Map<StringData, Integer> randomMap = new HashMap<>();
        for (int i = random.nextInt(20); i > 0; i--) {
            randomMap.put(StringData.fromString(randomString(20)), random.nextInt());
        }
        GenericRowData rowData =
                GenericRowData.of(
                        random.nextBoolean(),
                        (byte) random.nextInt(Byte.MIN_VALUE, Byte.MAX_VALUE + 1),
                        (short) random.nextInt(Short.MIN_VALUE, Short.MAX_VALUE + 1),
                        random.nextInt(),
                        random.nextLong(),
                        random.nextFloat(),
                        random.nextDouble(),
                        DecimalData.fromBigDecimal(randomBigDecimal(5, 3), 5, 3),
                        DecimalData.fromBigDecimal(randomBigDecimal(28, 6), 28, 6),
                        StringData.fromString(randomString(10)),
                        StringData.fromString(randomString(10)),
                        StringData.fromString(randomString(100)),
                        randomBytes,
                        random.nextInt(10000),
                        TimestampData.fromEpochMillis(random.nextLong(Integer.MAX_VALUE)),
                        new GenericArrayData(randomLongArray),
                        new GenericMapData(randomMap));
        for (int i = 0; i < rowData.getArity(); i++) {
            if (random.nextBoolean()) {
                rowData.setField(i, null);
            }
        }
        return rowData;
    }

    private static String randomString(int lengthBound) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        StringBuilder builder = new StringBuilder();
        for (int i = random.nextInt(lengthBound); i >= 0; i--) {
            builder.append((char) random.nextInt('a', 'z' + 1));
        }
        return builder.toString();
    }

    private static BigDecimal randomBigDecimal(int precision, int scale) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < precision - scale; i++) {
            builder.append((char) (random.nextInt(10) + '0'));
        }
        builder.append('.');
        for (int i = 0; i < scale; i++) {
            builder.append((char) (random.nextInt(10) + '0'));
        }
        return new BigDecimal(builder.toString());
    }
}
