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

package org.apache.paimon.flink.action.cdc.kafka.formats.canal;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.kafka.KafkaSchema;
import org.apache.paimon.flink.action.cdc.kafka.formats.RecordParser;
import org.apache.paimon.flink.action.cdc.mysql.MySqlTypeUtils;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * The {@code CanalRecordParser} class is responsible for parsing records from the Canal-JSON
 * format. Canal is a database binlog multi-platform consumer, which is used to synchronize data
 * across databases. This parser extracts relevant information from the Canal-JSON format and
 * transforms it into a list of {@link RichCdcMultiplexRecord} objects, which represent the changes
 * captured in the database.
 *
 * <p>The class handles different types of database operations such as INSERT, UPDATE, and DELETE,
 * and generates corresponding {@link RichCdcMultiplexRecord} objects for each operation.
 *
 * <p>Additionally, the parser supports schema extraction, which can be used to understand the
 * structure of the incoming data and its corresponding field types.
 */
public class CanalRecordParser extends RecordParser {

    private static final Charset CHARSET_ISO_8859_1 = StandardCharsets.ISO_8859_1;
    private static final String IS_DDL_FIELD = "isDdl";
    private static final String FIELD_SQL = "sql";
    private static final String FIELD_MYSQL_TYPE = "mysqlType";
    private static final String FIELD_PRIMARY_KEYS = "pkNames";
    private static final String FIELD_TYPE = "type";
    private static final String FIELD_DATA = "data";
    private static final String FIELD_OLD = "old";
    private static final String OP_UPDATE = "UPDATE";
    private static final String OP_INSERT = "INSERT";
    private static final String OP_DELETE = "DELETE";

    private final List<ComputedColumn> computedColumns;

    public CanalRecordParser(
            boolean caseSensitive,
            TableNameConverter tableNameConverter,
            List<ComputedColumn> computedColumns) {
        super(tableNameConverter, caseSensitive);
        this.computedColumns = computedColumns;
    }

    @Override
    public List<RichCdcMultiplexRecord> extractRecords() {
        List<RichCdcMultiplexRecord> records = new ArrayList<>();

        if (isDdl()) {
            return records;
        }

        List<String> primaryKeys = extractPrimaryKeys();
        LinkedHashMap<String, String> mySqlFieldTypes = extractFieldTypesFromMySqlType();
        LinkedHashMap<String, DataType> paimonFieldTypes =
                convertToPaimonFieldTypes(mySqlFieldTypes);

        String type = extractString(FIELD_TYPE);
        ArrayNode data = JsonSerdeUtil.getNodeAs(root, FIELD_DATA, ArrayNode.class);

        switch (type) {
            case OP_UPDATE:
                handleUpdateOperation(
                        data, mySqlFieldTypes, paimonFieldTypes, primaryKeys, records);
                break;
            case OP_INSERT:
                handleDataOperation(
                        data,
                        mySqlFieldTypes,
                        paimonFieldTypes,
                        primaryKeys,
                        records,
                        RowKind.INSERT);
                break;
            case OP_DELETE:
                handleDataOperation(
                        data,
                        mySqlFieldTypes,
                        paimonFieldTypes,
                        primaryKeys,
                        records,
                        RowKind.DELETE);
                break;
            default:
                throw new UnsupportedOperationException("Unknown record type: " + type);
        }

        return records;
    }

    @Override
    public KafkaSchema getKafkaSchema(String record) {
        try {
            root = OBJECT_MAPPER.readValue(record, JsonNode.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        validateFormat();

        if (isDdl()) {
            return null;
        }

        LinkedHashMap<String, String> mySqlFieldTypes = extractFieldTypesFromMySqlType();
        LinkedHashMap<String, DataType> paimonFieldTypes = new LinkedHashMap<>();
        mySqlFieldTypes.forEach(
                (name, type) -> paimonFieldTypes.put(name, MySqlTypeUtils.toDataType(type)));

        return new KafkaSchema(
                extractString(FIELD_DATABASE),
                extractString(FIELD_TABLE),
                paimonFieldTypes,
                extractPrimaryKeys());
    }

    @Override
    protected void validateFormat() {
        String errorMessageTemplate =
                "Didn't find '%s' node in json. Only supports canal-json format,"
                        + "please make sure your topic's format is correct.";

        checkNotNull(root.get(FIELD_DATABASE), errorMessageTemplate, FIELD_DATABASE);
        checkNotNull(root.get(FIELD_TABLE), errorMessageTemplate, FIELD_TABLE);
        checkNotNull(root.get(FIELD_TYPE), errorMessageTemplate, FIELD_TYPE);
        checkNotNull(root.get(FIELD_DATA), errorMessageTemplate, FIELD_DATA);

        if (isDdl()) {
            checkNotNull(root.get(FIELD_SQL), errorMessageTemplate, FIELD_SQL);
        } else {
            checkNotNull(root.get(FIELD_MYSQL_TYPE), errorMessageTemplate, FIELD_MYSQL_TYPE);
            checkNotNull(root.get(FIELD_PRIMARY_KEYS), errorMessageTemplate, FIELD_PRIMARY_KEYS);
        }
    }

    @Override
    protected String extractString(String key) {
        return root.get(key).asText();
    }

    private boolean isDdl() {
        JsonNode isDdlNode = root.get(IS_DDL_FIELD);
        if (isDdlNode == null) {
            throw new IllegalArgumentException("Expected 'isDdl' field in the JSON but not found.");
        }
        return isDdlNode.asBoolean();
    }

    private List<String> extractPrimaryKeys() {
        ArrayNode pkNames = JsonSerdeUtil.getNodeAs(root, FIELD_PRIMARY_KEYS, ArrayNode.class);
        return StreamSupport.stream(pkNames.spliterator(), false)
                .map(pk -> toFieldName(pk.asText()))
                .collect(Collectors.toList());
    }

    private LinkedHashMap<String, String> extractFieldTypesFromMySqlType() {
        JsonNode schema = JsonSerdeUtil.getNodeAs(root, FIELD_MYSQL_TYPE, JsonNode.class);
        LinkedHashMap<String, String> fieldTypes = new LinkedHashMap<>();

        schema.fieldNames()
                .forEachRemaining(
                        fieldName -> {
                            String fieldType = schema.get(fieldName).asText();
                            fieldTypes.put(toFieldName(fieldName), fieldType);
                        });

        return fieldTypes;
    }

    /**
     * Extracts data from a given JSON node and transforms it based on provided MySQL and Paimon
     * field types.
     *
     * @param record The JSON node containing the data.
     * @param mySqlFieldTypes A map of MySQL field types.
     * @param paimonFieldTypes A map of Paimon field types.
     * @return A map of extracted and transformed data.
     */
    private Map<String, String> extractRowFromJson(
            JsonNode record,
            Map<String, String> mySqlFieldTypes,
            LinkedHashMap<String, DataType> paimonFieldTypes) {

        Map<String, Object> jsonMap =
                OBJECT_MAPPER.convertValue(record, new TypeReference<Map<String, Object>>() {});
        if (jsonMap == null) {
            return new HashMap<>();
        }

        Map<String, String> resultMap =
                mySqlFieldTypes.entrySet().stream()
                        .filter(
                                entry ->
                                        jsonMap.containsKey(entry.getKey())
                                                && jsonMap.get(entry.getKey()) != null)
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry ->
                                                transformValue(
                                                        jsonMap.get(entry.getKey()).toString(),
                                                        entry.getValue())));

        // generate values for computed columns
        for (ComputedColumn computedColumn : computedColumns) {
            resultMap.put(
                    computedColumn.columnName(),
                    computedColumn.eval(resultMap.get(computedColumn.fieldReference())));
            paimonFieldTypes.put(computedColumn.columnName(), computedColumn.columnType());
        }

        return resultMap;
    }

    private String transformValue(String oldValue, String mySqlType) {
        String newValue = oldValue;
        String shortType = MySqlTypeUtils.getShortType(mySqlType);

        if (MySqlTypeUtils.isSetType(shortType)) {
            newValue = CanalFieldParser.convertSet(newValue, mySqlType);
        } else if (MySqlTypeUtils.isEnumType(shortType)) {
            newValue = CanalFieldParser.convertEnum(newValue, mySqlType);
        } else if (MySqlTypeUtils.isGeoType(shortType)) {
            try {
                byte[] wkb =
                        CanalFieldParser.convertGeoType2WkbArray(
                                oldValue.getBytes(CHARSET_ISO_8859_1));
                newValue = MySqlTypeUtils.convertWkbArray(wkb);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        String.format("Failed to convert %s to geometry JSON.", oldValue), e);
            }
        }
        return newValue;
    }

    private void handleDataOperation(
            ArrayNode data,
            Map<String, String> mySqlFieldTypes,
            LinkedHashMap<String, DataType> paimonFieldTypes,
            List<String> primaryKeys,
            List<RichCdcMultiplexRecord> records,
            RowKind kind) {
        for (JsonNode datum : data) {
            Map<String, String> rowData =
                    extractRowFromJson(datum, mySqlFieldTypes, paimonFieldTypes);
            rowData = adjustCase(rowData);

            records.add(
                    new RichCdcMultiplexRecord(
                            databaseName,
                            tableName,
                            paimonFieldTypes,
                            primaryKeys,
                            new CdcRecord(kind, rowData)));
        }
    }

    private void handleUpdateOperation(
            ArrayNode data,
            Map<String, String> mySqlFieldTypes,
            LinkedHashMap<String, DataType> paimonFieldTypes,
            List<String> primaryKeys,
            List<RichCdcMultiplexRecord> records) {
        ArrayNode old = JsonSerdeUtil.getNodeAs(root, FIELD_OLD, ArrayNode.class);
        for (int i = 0; i < data.size(); i++) {
            Map<String, String> after =
                    extractRowFromJson(data.get(i), mySqlFieldTypes, paimonFieldTypes);

            if (old != null && i < old.size()) {
                Map<String, String> before =
                        extractRowFromJson(old.get(i), mySqlFieldTypes, paimonFieldTypes);

                // Fields in "old" (before) means the fields are changed.
                // Fields not in "old" (before) means the fields are not changed,
                // so we just copy the not changed fields into before.
                for (Map.Entry<String, String> entry : after.entrySet()) {
                    before.putIfAbsent(entry.getKey(), entry.getValue());
                }

                before = adjustCase(before);
                records.add(
                        new RichCdcMultiplexRecord(
                                databaseName,
                                tableName,
                                paimonFieldTypes,
                                primaryKeys,
                                new CdcRecord(RowKind.DELETE, before)));
            }

            after = adjustCase(after);
            records.add(
                    new RichCdcMultiplexRecord(
                            databaseName,
                            tableName,
                            paimonFieldTypes,
                            primaryKeys,
                            new CdcRecord(RowKind.INSERT, after)));
        }
    }

    private LinkedHashMap<String, DataType> convertToPaimonFieldTypes(
            Map<String, String> mySqlFieldTypes) {
        LinkedHashMap<String, DataType> paimonFieldTypes = new LinkedHashMap<>();
        mySqlFieldTypes.forEach(
                (name, type) -> paimonFieldTypes.put(name, MySqlTypeUtils.toDataType(type)));
        return paimonFieldTypes;
    }
}
