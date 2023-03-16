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

package org.apache.flink.table.store.presto;

import org.apache.flink.table.store.types.ArrayType;
import org.apache.flink.table.store.types.BigIntType;
import org.apache.flink.table.store.types.BinaryType;
import org.apache.flink.table.store.types.BooleanType;
import org.apache.flink.table.store.types.CharType;
import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.store.types.DateType;
import org.apache.flink.table.store.types.DecimalType;
import org.apache.flink.table.store.types.DoubleType;
import org.apache.flink.table.store.types.FloatType;
import org.apache.flink.table.store.types.IntType;
import org.apache.flink.table.store.types.LocalZonedTimestampType;
import org.apache.flink.table.store.types.MapType;
import org.apache.flink.table.store.types.MultisetType;
import org.apache.flink.table.store.types.SmallIntType;
import org.apache.flink.table.store.types.TimeType;
import org.apache.flink.table.store.types.TimestampType;
import org.apache.flink.table.store.types.TinyIntType;
import org.apache.flink.table.store.types.VarBinaryType;
import org.apache.flink.table.store.types.VarCharType;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;

import java.util.Objects;

import static java.lang.String.format;

/** Presto type from Paimon Type. */
public class PrestoTypeUtils {

    private PrestoTypeUtils() {}

    public static Type toPrestoType(DataType paimonType, TypeManager typeManager) {
        if (paimonType instanceof CharType) {
            return com.facebook.presto.common.type.CharType.createCharType(
                    Math.min(
                            com.facebook.presto.common.type.CharType.MAX_LENGTH,
                            ((CharType) paimonType).getLength()));
        } else if (paimonType instanceof VarCharType) {
            return VarcharType.createUnboundedVarcharType();
        } else if (paimonType instanceof BooleanType) {
            return com.facebook.presto.common.type.BooleanType.BOOLEAN;
        } else if (paimonType instanceof BinaryType) {
            return VarbinaryType.VARBINARY;
        } else if (paimonType instanceof VarBinaryType) {
            return VarbinaryType.VARBINARY;
        } else if (paimonType instanceof DecimalType) {
            return com.facebook.presto.common.type.DecimalType.createDecimalType(
                    ((DecimalType) paimonType).getPrecision(),
                    ((DecimalType) paimonType).getScale());
        } else if (paimonType instanceof TinyIntType) {
            return TinyintType.TINYINT;
        } else if (paimonType instanceof SmallIntType) {
            return SmallintType.SMALLINT;
        } else if (paimonType instanceof IntType) {
            return IntegerType.INTEGER;
        } else if (paimonType instanceof BigIntType) {
            return BigintType.BIGINT;
        } else if (paimonType instanceof FloatType) {
            return RealType.REAL;
        } else if (paimonType instanceof DoubleType) {
            return com.facebook.presto.common.type.DoubleType.DOUBLE;
        } else if (paimonType instanceof DateType) {
            return com.facebook.presto.common.type.DateType.DATE;
        } else if (paimonType instanceof TimeType) {
            return com.facebook.presto.common.type.TimeType.TIME;
        } else if (paimonType instanceof TimestampType) {
            return com.facebook.presto.common.type.TimestampType.TIMESTAMP;
        } else if (paimonType instanceof LocalZonedTimestampType) {
            return TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
        } else if (paimonType instanceof ArrayType) {
            DataType elementType = ((ArrayType) paimonType).getElementType();
            return new com.facebook.presto.common.type.ArrayType(
                    Objects.requireNonNull(toPrestoType(elementType, typeManager)));
        } else if (paimonType instanceof MultisetType) {
            return null;
        } else if (paimonType instanceof MapType) {
            MapType paimonMapType = (MapType) paimonType;
            TypeSignature keyType =
                    Objects.requireNonNull(toPrestoType(paimonMapType.getKeyType(), typeManager))
                            .getTypeSignature();
            TypeSignature valueType =
                    Objects.requireNonNull(toPrestoType(paimonMapType.getValueType(), typeManager))
                            .getTypeSignature();
            return typeManager.getParameterizedType(
                    StandardTypes.MAP,
                    ImmutableList.of(
                            TypeSignatureParameter.of(keyType),
                            TypeSignatureParameter.of(valueType)));
        } else {
            throw new UnsupportedOperationException(
                    format("Cannot convert from Paimon type '%s' to Presto type", paimonType));
        }
    }
}
