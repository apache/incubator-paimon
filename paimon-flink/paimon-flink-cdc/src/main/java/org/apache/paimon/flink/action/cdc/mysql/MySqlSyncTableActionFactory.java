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

package org.apache.paimon.flink.action.cdc.mysql;

import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionFactory;
import org.apache.paimon.flink.action.MultipleParameterToolAdapter;
import org.apache.paimon.flink.action.cdc.TypeMapping;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Optional;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.COMPUTED_COLUMN;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.METADATA_COLUMN;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.MYSQL_CONF;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.PARTITION_KEYS;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.PRIMARY_KEYS;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.TYPE_MAPPING;

/** Factory to create {@link MySqlSyncTableAction}. */
public class MySqlSyncTableActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "mysql_sync_table";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        Tuple3<String, String, String> tablePath = getTablePath(params);
        checkRequiredArgument(params, MYSQL_CONF);

        MySqlSyncTableAction action =
                new MySqlSyncTableAction(
                        tablePath.f0,
                        tablePath.f1,
                        tablePath.f2,
                        optionalConfigMap(params, CATALOG_CONF),
                        optionalConfigMap(params, MYSQL_CONF));

        action.withTableConfig(optionalConfigMap(params, TABLE_CONF));

        if (params.has(PARTITION_KEYS)) {
            action.withPartitionKeys(params.get(PARTITION_KEYS).split(","));
        }

        if (params.has(PRIMARY_KEYS)) {
            action.withPrimaryKeys(params.get(PRIMARY_KEYS).split(","));
        }

        if (params.has(COMPUTED_COLUMN)) {
            action.withComputedColumnArgs(
                    new ArrayList<>(params.getMultiParameter(COMPUTED_COLUMN)));
        }

        if (params.has(METADATA_COLUMN)) {
            action.withMetadataColumns(new ArrayList<>(params.getMultiParameter(METADATA_COLUMN)));
        }

        if (params.has(TYPE_MAPPING)) {
            String[] options = params.get(TYPE_MAPPING).split(",");
            action.withTypeMapping(TypeMapping.parse(options));
        }

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"mysql-sync-table\" creates a streaming job "
                        + "with a Flink MySQL CDC source and a Paimon table sink to consume CDC events.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  mysql-sync-table --warehouse <warehouse-path> --database <database-name> "
                        + "--table <table-name> "
                        + "[--partition-keys <partition-keys>] "
                        + "[--primary-keys <primary-keys>] "
                        + "[--type-mapping <option1,option2...>] "
                        + "[--computed-column <'column-name=expr-name(args[, ...])'> [--computed-column ...]] "
                        + "[--metadata-column <metadata-column>] "
                        + "[--mysql-conf <mysql-cdc-source-conf> [--mysql-conf <mysql-cdc-source-conf> ...]] "
                        + "[--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] "
                        + "[--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]");
        System.out.println();

        System.out.println("Partition keys syntax:");
        System.out.println("  key1,key2,...");
        System.out.println(
                "If partition key is not defined and the specified Paimon table does not exist, "
                        + "this action will automatically create an unpartitioned Paimon table.");
        System.out.println();

        System.out.println("Primary keys syntax:");
        System.out.println("  key1,key2,...");
        System.out.println("Primary keys will be derived from MySQL tables if not specified.");
        System.out.println();

        System.out.println(
                "--type-mapping is used to specify how to map MySQL type to Paimon type. Please see the doc for usage.");
        System.out.println();

        System.out.println("Please see doc for usage of --computed-column.");
        System.out.println();

        System.out.println(
                "--metadata-column is used to specify which metadata columns to include in the output schema of the connector. Please see the doc for usage.");
        System.out.println();

        System.out.println("MySQL CDC source conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "'hostname', 'username', 'password', 'database-name' and 'table-name' "
                        + "are required configurations, others are optional.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html#connector-options");
        System.out.println();

        System.out.println("Paimon catalog and table sink conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://paimon.apache.org/docs/master/maintenance/configurations/");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  mysql-sync-table \\\n"
                        + "    --warehouse hdfs:///path/to/warehouse \\\n"
                        + "    --database test_db \\\n"
                        + "    --table test_table \\\n"
                        + "    --partition-keys pt \\\n"
                        + "    --primary-keys pt,uid \\\n"
                        + "    --mysql-conf hostname=127.0.0.1 \\\n"
                        + "    --mysql-conf username=root \\\n"
                        + "    --mysql-conf password=123456 \\\n"
                        + "    --mysql-conf database-name=source_db \\\n"
                        + "    --mysql-conf table-name='source_table' \\\n"
                        + "    --catalog-conf metastore=hive \\\n"
                        + "    --catalog-conf uri=thrift://hive-metastore:9083 \\\n"
                        + "    --table-conf bucket=4 \\\n"
                        + "    --table-conf changelog-producer=input \\\n"
                        + "    --table-conf sink.parallelism=4");
    }
}
