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

package org.apache.flink.table.store.kafka;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.log.LogOptions.LogConsistency;
import org.apache.flink.table.store.log.LogOptions.LogStartupMode;
import org.apache.flink.table.store.log.LogSourceProvider;
import org.apache.flink.table.types.DataType;

import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;

/** A Kafka {@link LogSourceProvider}. */
public class KafkaLogSourceProvider implements LogSourceProvider {

    private static final long serialVersionUID = 1L;

    private final String topic;

    private final Properties properties;

    private final DataType physicalType;

    private final int[] primaryKey;

    @Nullable private final DeserializationSchema<RowData> keyDeserializer;

    private final DeserializationSchema<RowData> valueDeserializer;

    private final LogConsistency consistency;

    private final LogStartupMode scanMode;

    @Nullable private final Long timestampMills;

    public KafkaLogSourceProvider(
            String topic,
            Properties properties,
            DataType physicalType,
            int[] primaryKey,
            @Nullable DeserializationSchema<RowData> keyDeserializer,
            DeserializationSchema<RowData> valueDeserializer,
            LogConsistency consistency,
            LogStartupMode scanMode,
            @Nullable Long timestampMills) {
        this.topic = topic;
        this.properties = properties;
        this.physicalType = physicalType;
        this.primaryKey = primaryKey;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.consistency = consistency;
        this.scanMode = scanMode;
        this.timestampMills = timestampMills;
    }

    @Override
    public KafkaSource<RowData> createSource(@Nullable Map<Integer, Long> bucketOffsets) {
        switch (consistency) {
            case TRANSACTIONAL:
                // Add read committed for transactional consistency mode.
                properties.setProperty(ISOLATION_LEVEL_CONFIG, "read_committed");
                break;
            case EVENTUAL:
                if (keyDeserializer == null) {
                    throw new IllegalArgumentException(
                            "Can not use EVENTUAL consistency mode for non-pk table.");
                }
                properties.setProperty(ISOLATION_LEVEL_CONFIG, "read_uncommitted");
                break;
        }

        return KafkaSource.<RowData>builder()
                .setTopics(topic)
                .setStartingOffsets(toOffsetsInitializer(bucketOffsets))
                .setProperties(properties)
                .setDeserializer(createDeserializationSchema())
                .build();
    }

    @VisibleForTesting
    KafkaRecordDeserializationSchema<RowData> createDeserializationSchema() {
        return primaryKey.length > 0
                ? KafkaRecordDeserializationSchema.of(
                        new KafkaLogKeyedDeserializationSchema(
                                physicalType, primaryKey, keyDeserializer, valueDeserializer))
                : KafkaRecordDeserializationSchema.valueOnly(valueDeserializer);
    }

    private OffsetsInitializer toOffsetsInitializer(@Nullable Map<Integer, Long> bucketOffsets) {
        switch (scanMode) {
            case FULL:
                return bucketOffsets == null
                        ? OffsetsInitializer.earliest()
                        : OffsetsInitializer.offsets(toKafkaOffsets(bucketOffsets));
            case LATEST:
                return OffsetsInitializer.latest();
            case FROM_TIMESTAMP:
                if (timestampMills == null) {
                    throw new NullPointerException(
                            "Must specify a timestamp if you choose timestamp startup mode.");
                }
                return OffsetsInitializer.timestamp(timestampMills);
            default:
                throw new UnsupportedOperationException("Unsupported mode: " + scanMode);
        }
    }

    private Map<TopicPartition, Long> toKafkaOffsets(Map<Integer, Long> bucketOffsets) {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        bucketOffsets.forEach(
                (bucket, offset) -> offsets.put(new TopicPartition(topic, bucket), offset));
        return offsets;
    }
}
