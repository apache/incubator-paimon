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

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class is responsible for scanning files that need to be compact by batch method {@link
 * CompactionFileScanner}.
 */
public class BatchFileScanner<T> extends CompactionFileScanner<T> {

    public BatchFileScanner(AtomicBoolean isRunning, AbstractTableScanLogic<T> tableScanLogic) {
        super(isRunning, tableScanLogic);
    }

    @Override
    public void scan(SourceFunction.SourceContext<T> ctx) throws Exception {
        if (isRunning.get()) {
            Boolean isEmpty = tableScanLogic.collectFiles(ctx);
            if (isEmpty == null) {
                return;
            }
            if (isEmpty) {
                throw new Exception(
                        "No file were collected. Please ensure there are tables detected after pattern matching");
            }
        }
    }
}
