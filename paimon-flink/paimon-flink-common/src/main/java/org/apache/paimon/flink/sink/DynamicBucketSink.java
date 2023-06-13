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

package org.apache.paimon.flink.sink;

import org.apache.paimon.operation.Lock;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.PartitionKeyExtractor;
import org.apache.paimon.utils.SerializableFunction;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.partition;

/** Sink for dynamic bucket table. */
public abstract class DynamicBucketSink<T> extends FlinkWriteSink<Tuple2<T, Integer>> {

    private static final long serialVersionUID = 1L;

    public DynamicBucketSink(
            FileStoreTable table,
            Lock.Factory lockFactory,
            @Nullable Map<String, String> overwritePartition) {
        super(table, overwritePartition, lockFactory);
    }

    protected abstract ChannelComputer<T> channelComputer1();

    protected abstract ChannelComputer<Tuple2<T, Integer>> channelComputer2();

    protected abstract SerializableFunction<TableSchema, PartitionKeyExtractor<T>>
            extractorFunction();

    public DataStreamSink<?> build(DataStream<T> input, @Nullable Integer parallelism) {
        String initialCommitUser = UUID.randomUUID().toString();

        // Topology:
        // input -- shuffle by key hash --> bucket-assigner -- shuffle by bucket --> writer -->
        // committer

        // 1. shuffle by key hash
        DataStream<T> partitionByKeyHash = partition(input, channelComputer1(), parallelism);

        // 2. bucket-assigner
        HashBucketAssignerOperator<T> assignerOperator =
                new HashBucketAssignerOperator<>(initialCommitUser, table, extractorFunction());
        TupleTypeInfo<Tuple2<T, Integer>> rowWithBucketType =
                new TupleTypeInfo<>(partitionByKeyHash.getType(), BasicTypeInfo.INT_TYPE_INFO);
        DataStream<Tuple2<T, Integer>> bucketAssigned =
                partitionByKeyHash.transform(
                        "dynamic-bucket-assigner", rowWithBucketType, assignerOperator);

        // 3. shuffle by bucket
        DataStream<Tuple2<T, Integer>> partitionByBucket =
                partition(bucketAssigned, channelComputer2(), parallelism);

        // 4. writer and committer
        return sinkFrom(partitionByBucket, initialCommitUser);
    }
}
