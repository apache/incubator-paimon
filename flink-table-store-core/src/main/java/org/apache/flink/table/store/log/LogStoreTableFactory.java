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

package org.apache.flink.table.store.log;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.types.RowKind;

/**
 * Base interface for configuring a default log table connector. The log table is used by managed
 * table factory.
 *
 * <p>Log tables are for processing only unbounded data. Support streaming reading and streaming
 * writing.
 */
public interface LogStoreTableFactory extends DynamicTableFactory {

    /** Notifies the listener that a table creation occurred. */
    void onCreateTable(Context context, int numBucket, boolean ignoreIfExists);

    /** Notifies the listener that a table drop occurred. */
    void onDropTable(Context context, boolean ignoreIfNotExists);

    /**
     * Creates a {@link LogSourceProvider} instance from a {@link CatalogTable} and additional
     * context information.
     */
    LogSourceProvider createSourceProvider(Context context, SourceContext sourceContext);

    /**
     * Creates a {@link LogSinkProvider} instance from a {@link CatalogTable} and additional context
     * information.
     */
    LogSinkProvider createSinkProvider(Context context, SinkContext sinkContext);

    /** Context for create runtime source. */
    interface SourceContext extends DynamicTableSource.Context {}

    /** Context for create runtime sink. */
    interface SinkContext extends DynamicTableSink.Context {}

    // --------------------------------------------------------------------------------------------

    static LogStoreTableFactory discoverLogStoreFactory(ClassLoader cl, String identifier) {
        return FactoryUtil.discoverFactory(cl, LogStoreTableFactory.class, identifier);
    }

    static DecodingFormat<DeserializationSchema<RowData>> getKeyDecodingFormat(
            TableFactoryHelper helper) {
        DecodingFormat<DeserializationSchema<RowData>> format =
                helper.discoverDecodingFormat(
                        DeserializationFormatFactory.class, LogOptions.KEY_FORMAT);
        validateKeyFormat(format, helper.getOptions().get(LogOptions.KEY_FORMAT));
        return format;
    }

    static EncodingFormat<SerializationSchema<RowData>> getKeyEncodingFormat(
            TableFactoryHelper helper) {
        EncodingFormat<SerializationSchema<RowData>> format =
                helper.discoverEncodingFormat(
                        SerializationFormatFactory.class, LogOptions.KEY_FORMAT);
        validateKeyFormat(format, helper.getOptions().get(LogOptions.KEY_FORMAT));
        return format;
    }

    static DecodingFormat<DeserializationSchema<RowData>> getValueDecodingFormat(
            TableFactoryHelper helper) {
        DecodingFormat<DeserializationSchema<RowData>> format =
                helper.discoverDecodingFormat(
                        DeserializationFormatFactory.class, LogOptions.FORMAT);
        validateValueFormat(format, helper.getOptions().get(LogOptions.FORMAT));
        return format;
    }

    static EncodingFormat<SerializationSchema<RowData>> getValueEncodingFormat(
            TableFactoryHelper helper) {
        EncodingFormat<SerializationSchema<RowData>> format =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, LogOptions.FORMAT);
        validateValueFormat(format, helper.getOptions().get(LogOptions.FORMAT));
        return format;
    }

    static void validateKeyFormat(Format format, String name) {
        if (!format.getChangelogMode().containsOnly(RowKind.INSERT)) {
            throw new ValidationException(
                    String.format(
                            "A key format should only deal with INSERT-only records. "
                                    + "But %s has a changelog mode of %s.",
                            name, format.getChangelogMode()));
        }
    }

    static void validateValueFormat(Format format, String name) {
        if (!format.getChangelogMode().equals(ChangelogMode.all())) {
            throw new ValidationException(
                    String.format(
                            "A value format should deal with all records. "
                                    + "But %s has a changelog mode of %s.",
                            name, format.getChangelogMode()));
        }
    }
}
