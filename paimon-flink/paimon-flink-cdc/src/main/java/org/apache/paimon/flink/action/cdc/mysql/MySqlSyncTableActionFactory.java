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
import org.apache.paimon.flink.action.cdc.TypeMapping;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.MultipleParameterTool;

import java.util.ArrayList;
import java.util.Optional;

/** Factory to create {@link MySqlSyncTableAction}. */
public class MySqlSyncTableActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "mysql-sync-table";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterTool params) {
        Tuple3<String, String, String> tablePath = getTablePath(params);
        checkRequiredArgument(params, "mysql-conf");

        MySqlSyncTableAction action =
                new MySqlSyncTableAction(
                        tablePath.f0,
                        tablePath.f1,
                        tablePath.f2,
                        optionalConfigMap(params, "catalog-conf"),
                        optionalConfigMap(params, "mysql-conf"));

        action.withTableConfig(optionalConfigMap(params, "table-conf"));

        if (params.has("partition-keys")) {
            action.withPartitionKeys(params.get("partition-keys").split(","));
        }

        if (params.has("primary-keys")) {
            action.withPrimaryKeys(params.get("primary-keys").split(","));
        }

        if (params.has("computed-column")) {
            action.withComputedColumnArgs(
                    new ArrayList<>(params.getMultiParameter("computed-column")));
        }

        if (params.has("metadata-column")) {
            action.withMetadataColumns(
                    new ArrayList<>(params.getMultiParameter("metadata-column")));
        }

        if (params.has("type-mapping")) {
            String[] options = params.get("type-mapping").split(",");
            action.withTypeMapping(TypeMapping.parse(options));
        }

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"mysql_sync_table\" creates a streaming job "
                        + "with a Flink MySQL CDC source and a Paimon table sink to consume CDC events.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  mysql_sync_table --warehouse <warehouse_path> --database <database_name> "
                        + "--table <table_name> "
                        + "[--partition_keys <partition_keys>] "
                        + "[--primary_keys <primary_keys>] "
                        + "[--type_mapping <option1,option2...>] "
                        + "[--computed_column <'column_name=expr_name(args[, ...])'> [--computed_column ...]] "
                        + "[--metadata_column <metadata_column>] "
                        + "[--mysql_conf <mysql_cdc_source_conf> [--mysql_conf <mysql_cdc_source_conf> ...]] "
                        + "[--catalog_conf <paimon_catalog_conf> [--catalog_conf <paimon_catalog_conf> ...]] "
                        + "[--table_conf <paimon_table_sink_conf> [--table_conf <paimon_table_sink_conf> ...]]");
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
                "--type_mapping is used to specify how to map MySQL type to Paimon type. Please see the doc for usage.");
        System.out.println();

        System.out.println("Please see doc for usage of --computed_column.");
        System.out.println();

        System.out.println(
                "--metadata_column is used to specify which metadata columns to include in the output schema of the connector. Please see the doc for usage.");
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
                "  mysql_sync_table \\\n"
                        + "    --warehouse hdfs:///path/to/warehouse \\\n"
                        + "    --database test_db \\\n"
                        + "    --table test_table \\\n"
                        + "    --partition_keys pt \\\n"
                        + "    --primary_keys pt,uid \\\n"
                        + "    --mysql_conf hostname=127.0.0.1 \\\n"
                        + "    --mysql_conf username=root \\\n"
                        + "    --mysql_conf password=123456 \\\n"
                        + "    --mysql_conf database-name=source_db \\\n"
                        + "    --mysql_conf table-name='source_table' \\\n"
                        + "    --catalog_conf metastore=hive \\\n"
                        + "    --catalog_conf uri=thrift://hive-metastore:9083 \\\n"
                        + "    --table_conf bucket=4 \\\n"
                        + "    --table_conf changelog-producer=input \\\n"
                        + "    --table_conf sink.parallelism=4");
    }
}
