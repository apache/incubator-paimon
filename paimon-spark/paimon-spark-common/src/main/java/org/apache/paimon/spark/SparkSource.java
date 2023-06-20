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

package org.apache.paimon.spark;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTableFactory;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.SessionConfigSupport;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

/** The spark source for paimon. */
public class SparkSource implements DataSourceRegister, SessionConfigSupport {

    /** Not use 'paimon' here, the '-' is not allowed in SQL. */
    private static final String SHORT_NAME = "paimon";

    @Override
    public String shortName() {
        return SHORT_NAME;
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        // ignore schema.
        // getTable will get schema by itself.
        return null;
    }

    @Override
    public Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
        // ignore partition.
        // getTable will get partition by itself.
        return null;
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }

    @Override
    public Table getTable(
            StructType schema, Transform[] partitioning, Map<String, String> options) {
        CatalogContext catalogContext =
                CatalogContext.create(
                        Options.fromMap(options),
                        SparkSession.active().sessionState().newHadoopConf());
        return new SparkTable(FileStoreTableFactory.create(catalogContext));
    }

    @Override
    public String keyPrefix() {
        return SHORT_NAME;
    }
}
