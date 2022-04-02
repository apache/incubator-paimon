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

package org.apache.flink.table.store.connector;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

/** Options for {@link TableStoreFactory}. */
public class TableStoreFactoryOptions {

    public static final ConfigOption<String> LOG_SYSTEM =
            ConfigOptions.key("log.system")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The log system used to keep changes of the table.");

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

    public static final ConfigOption<Integer> SCAN_PARALLELISM =
            ConfigOptions.key("scan.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines a custom parallelism for the scan source. "
                                    + "By default, if this option is not defined, the planner will derive the parallelism "
                                    + "for each statement individually by also considering the global configuration.");

    public static Set<ConfigOption<?>> allOptions() {
        Set<ConfigOption<?>> allOptions = new HashSet<>();
        allOptions.add(LOG_SYSTEM);
        allOptions.add(SINK_PARALLELISM);
        allOptions.add(SCAN_PARALLELISM);
        return allOptions;
    }
}
