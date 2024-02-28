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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class is responsible for scanning files that need to be compact by batch method {@link
 * CompactionFileScanner}.
 */
public class BatchFileScanner<T> extends CompactionFileScanner<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchFileScanner.class);

    public BatchFileScanner(AtomicBoolean isRunning, AbstractBucketScanLogic<T> tableScanLogic) {
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
                // Currently, in the combined mode, there are two scan tasks for the table of two
                // different bucket type (multi bucket & unaware bucket)  are
                // running concurrently.
                // There will be a situation that there is only one task compaction , therefore this
                // should not be thrown exception here.
                LOGGER.info(
                        "No file were collected for the table of {}", tableScanLogic.bucketType());
            }
        }
    }
}
