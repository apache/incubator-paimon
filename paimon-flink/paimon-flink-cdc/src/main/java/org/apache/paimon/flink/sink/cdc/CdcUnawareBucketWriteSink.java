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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.flink.compact.UnawareBucketCompactionTopoBuilder;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.LogSinkFunction;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.UnawareBucketWriteSink;
import org.apache.paimon.table.AppendOnlyFileStoreTable;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;

import java.util.Map;

/** Sink for unaware bucket table. */
public class CdcUnawareBucketWriteSink extends UnawareBucketWriteSink {

    public CdcUnawareBucketWriteSink(AppendOnlyFileStoreTable table, Integer parallelism) {
        super(table, null, null, parallelism, true);
    }

    public CdcUnawareBucketWriteSink(
            AppendOnlyFileStoreTable table,
            Integer parallelism,
            Map<String, String> overwritePartitions,
            LogSinkFunction logSinkFunction,
            boolean boundedInput) {
        super(table, overwritePartitions, logSinkFunction, parallelism, boundedInput);
    }

    @Override
    protected OneInputStreamOperator createWriteOperator(
            StoreSinkWrite.Provider writeProvider, String commitUser) {
        return new CdcUnawareBucketWriteOperator(table, writeProvider, commitUser);
    }

    @Override
    public DataStreamSink<?> sinkFrom(DataStream input, String initialCommitUser) {
        // do the actually writing action, no snapshot generated in this stage
        DataStream<Committable> written = doWrite(input, initialCommitUser, parallelism);

        boolean isStreamingMode =
                input.getExecutionEnvironment()
                                .getConfiguration()
                                .get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;

        // if enable compaction, we need to add compaction topology to this job
        if (enableCompaction && isStreamingMode && !boundedInput) {
            // if streaming mode with bounded input, we disable compaction topology
            UnawareBucketCompactionTopoBuilder builder =
                    new UnawareBucketCompactionTopoBuilder(
                            input.getExecutionEnvironment(), table.name(), table);
            builder.withContinuousMode(true);
            written = written.union(builder.fetchUncommitted(initialCommitUser));
        }

        // commit the committable to generate a new snapshot
        return doCommit(written, initialCommitUser);
    }
}
