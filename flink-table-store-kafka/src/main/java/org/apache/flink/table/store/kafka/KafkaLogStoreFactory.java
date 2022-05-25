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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.store.log.LogStoreTableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;
import static org.apache.flink.table.store.kafka.KafkaLogOptions.BOOTSTRAP_SERVERS;
import static org.apache.flink.table.store.kafka.KafkaLogOptions.TOPIC;
import static org.apache.flink.table.store.log.LogOptions.CHANGELOG_MODE;
import static org.apache.flink.table.store.log.LogOptions.CONSISTENCY;
import static org.apache.flink.table.store.log.LogOptions.FORMAT;
import static org.apache.flink.table.store.log.LogOptions.KEY_FORMAT;
import static org.apache.flink.table.store.log.LogOptions.LogConsistency;
import static org.apache.flink.table.store.log.LogOptions.RETENTION;
import static org.apache.flink.table.store.log.LogOptions.SCAN;
import static org.apache.flink.table.store.log.LogOptions.SCAN_TIMESTAMP_MILLS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;

/** The Kafka {@link LogStoreTableFactory} implementation. */
public class KafkaLogStoreFactory implements LogStoreTableFactory {

    public static final String IDENTIFIER = "kafka";

    public static final String KAFKA_PREFIX = IDENTIFIER + ".";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BOOTSTRAP_SERVERS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCAN);
        options.add(TOPIC);
        options.add(SCAN_TIMESTAMP_MILLS);
        options.add(RETENTION);
        options.add(CONSISTENCY);
        options.add(CHANGELOG_MODE);
        options.add(KEY_FORMAT);
        options.add(FORMAT);
        return options;
    }

    @Override
    public Map<String, String> enrichOptions(Context context) {
        Map<String, String> options = new HashMap<>(context.getCatalogTable().getOptions());
        Preconditions.checkArgument(
                !options.containsKey(TOPIC.key()),
                "Managed table can not contain custom topic. "
                        + "You need to remove topic in table options or session config.");

        String topic = context.getObjectIdentifier().asSummaryString();
        options.put(TOPIC.key(), topic);
        return options;
    }

    private String topic(Context context) {
        return context.getCatalogTable().getOptions().get(TOPIC.key());
    }

    @Override
    public void onCreateTable(Context context, int numBucket, boolean ignoreIfExists) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validateExcept(KAFKA_PREFIX);
        try (AdminClient adminClient = AdminClient.create(toKafkaProperties(helper.getOptions()))) {
            Map<String, String> configs = new HashMap<>();
            helper.getOptions()
                    .getOptional(RETENTION)
                    .ifPresent(
                            retention ->
                                    configs.put(
                                            TopicConfig.RETENTION_MS_CONFIG,
                                            String.valueOf(retention.toMillis())));
            NewTopic topicObj =
                    new NewTopic(topic(context), Optional.of(numBucket), Optional.empty())
                            .configs(configs);
            adminClient.createTopics(Collections.singleton(topicObj)).all().get();
        } catch (ExecutionException | InterruptedException e) {
            if (e.getCause() instanceof TopicExistsException) {
                if (ignoreIfExists) {
                    return;
                }
                throw new TableException(
                        String.format(
                                "Failed to create kafka topic. "
                                        + "Reason: topic %s exists for table %s. "
                                        + "Suggestion: please try `DESCRIBE TABLE %s` to "
                                        + "check whether table exists in current catalog. "
                                        + "If table exists and the DDL needs to be executed "
                                        + "multiple times, please use `CREATE TABLE IF NOT EXISTS` ddl instead. "
                                        + "Otherwise, please choose another table name "
                                        + "or manually delete the current topic and try again.",
                                topic(context),
                                context.getObjectIdentifier().asSerializableString(),
                                context.getObjectIdentifier().asSerializableString()));
            }
            throw new TableException("Error in createTopic", e);
        }
    }

    @Override
    public void onDropTable(Context context, boolean ignoreIfNotExists) {
        try (AdminClient adminClient =
                AdminClient.create(
                        toKafkaProperties(createTableFactoryHelper(this, context).getOptions()))) {
            adminClient.deleteTopics(Collections.singleton(topic(context))).all().get();
        } catch (ExecutionException e) {
            // check the cause to ignore topic not exists conditionally
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                if (ignoreIfNotExists) {
                    return;
                }
                throw new TableException(
                        String.format(
                                "Failed to delete kafka topic. "
                                        + "Reason: topic %s doesn't exist for table %s. "
                                        + "Suggestion: please try `DROP TABLE IF EXISTS` ddl instead.",
                                topic(context),
                                context.getObjectIdentifier().asSerializableString()));
            }
            throw new TableException("Error in deleteTopic", e);
        } catch (InterruptedException e) {
            throw new TableException("Error in deleteTopic", e);
        }
    }

    @Override
    public KafkaLogSourceProvider createSourceProvider(
            DynamicTableFactory.Context context,
            SourceContext sourceContext,
            @Nullable int[][] projectFields) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        DataType physicalType = schema.toPhysicalRowDataType();
        DeserializationSchema<RowData> primaryKeyDeserializer = null;
        int[] primaryKey = schema.getPrimaryKeyIndexes();
        if (primaryKey.length > 0) {
            DataType keyType = DataTypeUtils.projectRow(physicalType, primaryKey);
            primaryKeyDeserializer =
                    LogStoreTableFactory.getKeyDecodingFormat(helper)
                            .createRuntimeDecoder(sourceContext, keyType);
        }
        DeserializationSchema<RowData> valueDeserializer =
                LogStoreTableFactory.getValueDecodingFormat(helper)
                        .createRuntimeDecoder(sourceContext, physicalType);
        return new KafkaLogSourceProvider(
                topic(context),
                toKafkaProperties(helper.getOptions()),
                physicalType,
                primaryKey,
                primaryKeyDeserializer,
                valueDeserializer,
                projectFields,
                helper.getOptions().get(CONSISTENCY),
                helper.getOptions().get(SCAN),
                helper.getOptions().get(SCAN_TIMESTAMP_MILLS));
    }

    @Override
    public KafkaLogSinkProvider createSinkProvider(
            DynamicTableFactory.Context context, SinkContext sinkContext) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        DataType physicalType = schema.toPhysicalRowDataType();
        SerializationSchema<RowData> primaryKeySerializer = null;
        int[] primaryKey = schema.getPrimaryKeyIndexes();
        if (primaryKey.length > 0) {
            DataType keyType = DataTypeUtils.projectRow(physicalType, primaryKey);
            primaryKeySerializer =
                    LogStoreTableFactory.getKeyEncodingFormat(helper)
                            .createRuntimeEncoder(sinkContext, keyType);
        }
        SerializationSchema<RowData> valueSerializer =
                LogStoreTableFactory.getValueEncodingFormat(helper)
                        .createRuntimeEncoder(sinkContext, physicalType);
        return new KafkaLogSinkProvider(
                topic(context),
                toKafkaProperties(helper.getOptions()),
                primaryKeySerializer,
                valueSerializer,
                helper.getOptions().get(CONSISTENCY),
                helper.getOptions().get(CHANGELOG_MODE));
    }

    public static Properties toKafkaProperties(ReadableConfig options) {
        Properties properties = new Properties();
        Map<String, String> optionMap = ((Configuration) options).toMap();
        optionMap.keySet().stream()
                .filter(key -> key.startsWith(KAFKA_PREFIX))
                .forEach(
                        key ->
                                properties.put(
                                        key.substring((KAFKA_PREFIX).length()),
                                        optionMap.get(key)));

        // Add read committed for transactional consistency mode.
        if (options.get(CONSISTENCY) == LogConsistency.TRANSACTIONAL) {
            properties.setProperty(ISOLATION_LEVEL_CONFIG, "read_committed");
        }
        return properties;
    }
}
