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

package org.apache.paimon.flink.action.cdc.postgresql;

import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.sink.cdc.UpdatedDataFieldsProcessFunction;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utils for PostgreSqlAction. */
public class PostgreSqlActionUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PostgreSqlActionUtils.class);

    static Connection getConnection(Configuration postgreSqlConfig) throws Exception {
        return DriverManager.getConnection(
                String.format(
                        "jdbc:postgresql://%s:%d/%s",
                        postgreSqlConfig.get(PostgreSqlSourceOptions.HOSTNAME),
                        postgreSqlConfig.get(PostgreSqlSourceOptions.PORT),
                        postgreSqlConfig.get(PostgreSqlSourceOptions.DATABASE_NAME)),
                postgreSqlConfig.get(PostgreSqlSourceOptions.USERNAME),
                postgreSqlConfig.get(PostgreSqlSourceOptions.PASSWORD));
    }

    static boolean schemaCompatible(TableSchema paimonSchema, Schema postgreSqlSchema) {
        for (DataField field : postgreSqlSchema.fields()) {
            int idx = paimonSchema.fieldNames().indexOf(field.name());
            if (idx < 0) {
                LOG.info("Cannot find field '{}' in Paimon table.", field.name());
                return false;
            }
            DataType type = paimonSchema.fields().get(idx).type();
            if (UpdatedDataFieldsProcessFunction.canConvert(field.type(), type)
                    != UpdatedDataFieldsProcessFunction.ConvertAction.CONVERT) {
                LOG.info(
                        "Cannot convert field '{}' from PostgreSQL type '{}' to Paimon type '{}'.",
                        field.name(),
                        field.type(),
                        type);
                return false;
            }
        }
        return true;
    }

    static Schema buildPaimonSchema(
            PostgreSqlSchema postgreSqlSchema,
            List<String> specifiedPartitionKeys,
            List<String> specifiedPrimaryKeys,
            List<ComputedColumn> computedColumns,
            Map<String, String> paimonConfig,
            boolean caseSensitive) {
        Schema.Builder builder = Schema.newBuilder();
        builder.options(paimonConfig);

        // build columns and primary keys from postgreSqlSchema
        LinkedHashMap<String, Tuple2<DataType, String>> postgreSqlFields;
        List<String> postgreSqlPrimaryKeys;
        if (caseSensitive) {
            postgreSqlFields = postgreSqlSchema.fields();
            postgreSqlPrimaryKeys = postgreSqlSchema.primaryKeys();
        } else {
            postgreSqlFields = new LinkedHashMap<>();
            for (Map.Entry<String, Tuple2<DataType, String>> entry :
                    postgreSqlSchema.fields().entrySet()) {
                String fieldName = entry.getKey();
                checkArgument(
                        !postgreSqlFields.containsKey(fieldName.toLowerCase()),
                        String.format(
                                "Duplicate key '%s' in table '%s.%s' appears when converting fields map keys to case-insensitive form.",
                                fieldName, postgreSqlSchema.schemaName(), postgreSqlSchema.tableName()));
                postgreSqlFields.put(fieldName.toLowerCase(), entry.getValue());
            }
            postgreSqlPrimaryKeys =
                    postgreSqlSchema.primaryKeys().stream()
                            .map(String::toLowerCase)
                            .collect(Collectors.toList());
        }

        for (Map.Entry<String, Tuple2<DataType, String>> entry : postgreSqlFields.entrySet()) {
            builder.column(entry.getKey(), entry.getValue().f0, entry.getValue().f1);
        }

        for (ComputedColumn computedColumn : computedColumns) {
            builder.column(computedColumn.columnName(), computedColumn.columnType());
        }

        if (specifiedPrimaryKeys.size() > 0) {
            for (String key : specifiedPrimaryKeys) {
                if (!postgreSqlFields.containsKey(key)
                        && computedColumns.stream().noneMatch(c -> c.columnName().equals(key))) {
                    throw new IllegalArgumentException(
                            "Specified primary key "
                                    + key
                                    + " does not exist in PostgreSQL tables or computed columns.");
                }
            }
            builder.primaryKey(specifiedPrimaryKeys);
        } else if (postgreSqlPrimaryKeys.size() > 0) {
            builder.primaryKey(postgreSqlPrimaryKeys);
        } else {
            throw new IllegalArgumentException(
                    "Primary keys are not specified. "
                            + "Also, can't infer primary keys from PostgreSQL table schemas because "
                            + "PostgreSQL tables have no primary keys or have different primary keys.");
        }

        if (specifiedPartitionKeys.size() > 0) {
            builder.partitionKeys(specifiedPartitionKeys);
        }

        return builder.build();
    }

    static SourceFunction<String> buildPostgreSqlSource(Configuration postgreSqlConfig) {
        validatePostgreSqlConfig(postgreSqlConfig);
        PostgreSQLSource.Builder<String> sourceBuilder = PostgreSQLSource.builder();

        String schemaName = postgreSqlConfig.get(PostgreSqlSourceOptions.SCHEMA_NAME);
        String tableName = postgreSqlConfig.get(PostgreSqlSourceOptions.TABLE_NAME);
        sourceBuilder
                .hostname(postgreSqlConfig.get(PostgreSqlSourceOptions.HOSTNAME))
                .port(postgreSqlConfig.get(PostgreSqlSourceOptions.PORT))
                .username(postgreSqlConfig.get(PostgreSqlSourceOptions.USERNAME))
                .password(postgreSqlConfig.get(PostgreSqlSourceOptions.PASSWORD))
                .database(postgreSqlConfig.get(PostgreSqlSourceOptions.DATABASE_NAME))
                .schemaList(schemaName)
                .tableList(schemaName + "." + tableName);

        Map<String, Object> customConverterConfigs = new HashMap<>();
        customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
        JsonDebeziumDeserializationSchema schema =
                new JsonDebeziumDeserializationSchema(true, customConverterConfigs);
        return sourceBuilder.deserializer(schema).build();
    }

    private static void validatePostgreSqlConfig(Configuration postgreSqlConfig) {
        checkArgument(
                postgreSqlConfig.get(PostgreSqlSourceOptions.HOSTNAME) != null,
                String.format(
                        "postgresql-conf [%s] must be specified.", PostgreSqlSourceOptions.HOSTNAME.key()));

        checkArgument(
                postgreSqlConfig.get(PostgreSqlSourceOptions.USERNAME) != null,
                String.format(
                        "postgresql-conf [%s] must be specified.", PostgreSqlSourceOptions.USERNAME.key()));

        checkArgument(
                postgreSqlConfig.get(PostgreSqlSourceOptions.PASSWORD) != null,
                String.format(
                        "postgresql-conf [%s] must be specified.", PostgreSqlSourceOptions.PASSWORD.key()));

        checkArgument(
                postgreSqlConfig.get(PostgreSqlSourceOptions.DATABASE_NAME) != null,
                String.format(
                        "postgresql-conf [%s] must be specified.",
                        PostgreSqlSourceOptions.DATABASE_NAME.key()));

        checkArgument(
                postgreSqlConfig.get(PostgreSqlSourceOptions.SCHEMA_NAME) != null,
                String.format(
                        "postgresql-conf [%s] must be specified.",
                        PostgreSqlSourceOptions.SCHEMA_NAME.key()));

        checkArgument(
                postgreSqlConfig.get(PostgreSqlSourceOptions.TABLE_NAME) != null,
                String.format(
                        "postgresql-conf [%s] must be specified.", PostgreSqlSourceOptions.TABLE_NAME.key()));
    }
}
