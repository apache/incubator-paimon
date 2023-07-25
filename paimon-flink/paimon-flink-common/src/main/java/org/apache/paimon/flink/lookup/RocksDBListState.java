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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.Serializer;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** RocksDB state for key -> List of value. */
public class RocksDBListState extends RocksDBState<List<InternalRow>> {

    private final ListDelimitedSerializer listSerializer = new ListDelimitedSerializer();

    private static final List<InternalRow> EMPTY = Collections.emptyList();

    public RocksDBListState(
            RocksDB db,
            ColumnFamilyHandle columnFamily,
            Serializer<InternalRow> keySerializer,
            Serializer<InternalRow> valueSerializer,
            long lruCacheSize) {
        super(db, columnFamily, keySerializer, valueSerializer, lruCacheSize);
    }

    public void add(InternalRow key, InternalRow value) throws IOException {
        byte[] keyBytes = serializeKey(key);
        byte[] valueBytes = serializeValue(value);
        try {
            db.merge(columnFamily, writeOptions, keyBytes, valueBytes);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
        cache.invalidate(wrap(keyBytes));
    }

    public List<InternalRow> get(InternalRow key) throws IOException {
        byte[] keyBytes = serializeKey(key);
        return cache.get(
                wrap(keyBytes),
                k -> {
                    byte[] valueBytes;
                    try {
                        valueBytes = db.get(columnFamily, keyBytes);
                    } catch (RocksDBException e) {
                        throw new RuntimeException(e);
                    }
                    List<InternalRow> rows =
                            listSerializer.deserializeList(valueBytes, valueSerializer);
                    if (rows == null) {
                        return EMPTY;
                    }
                    return rows;
                });
    }

    private byte[] serializeValue(InternalRow value) throws IOException {
        valueOutputView.clear();
        valueSerializer.serialize(value, valueOutputView);
        return valueOutputView.getCopyOfBuffer();
    }
}
