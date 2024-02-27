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

package org.apache.paimon.flink.compact;

import org.apache.paimon.append.AppendOnlyCompactionTask;
import org.apache.paimon.table.source.Split;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The class is response for scanning the file which need compaction.
 *
 * @param <T> the result of scanning file :
 *     <ol>
 *       <li>{@link Split} for the table with multi buckets, such as dynamic or fixed bucket table.
 *       <li>{@link AppendOnlyCompactionTask} for the table witch fixed single bucket ,such as
 *           unaware bucket table.
 *     </ol>
 */
public abstract class CompactionFileScanner<T> {
    protected final AtomicBoolean isRunning;

    protected final AbstractBucketScanLogic<T> tableScanLogic;

    public CompactionFileScanner(
            AtomicBoolean isRunning, AbstractBucketScanLogic<T> tableScanLogic) {
        this.isRunning = isRunning;
        this.tableScanLogic = tableScanLogic;
    }

    public abstract void scan(SourceFunction.SourceContext<T> ctx) throws Exception;
}
