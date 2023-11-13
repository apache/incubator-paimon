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

package org.apache.paimon.flink.source;

import org.apache.paimon.Snapshot;
import org.apache.paimon.flink.source.assigners.DynamicPartitionPruningAssigner;
import org.apache.paimon.flink.source.assigners.SplitAssigner;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.connector.source.DynamicFilteringEvent;

import javax.annotation.Nullable;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** A {@link SplitEnumerator} implementation for {@link StaticFileStoreSource} input. */
public class StaticFileStoreSplitEnumerator extends StaticFileStoreSplitEnumeratorBase {

    @Nullable private final DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo;

    public StaticFileStoreSplitEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            @Nullable Snapshot snapshot,
            SplitAssigner splitAssigner) {
        this(context, snapshot, splitAssigner, null);
    }

    public StaticFileStoreSplitEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            @Nullable Snapshot snapshot,
            SplitAssigner splitAssigner,
            @Nullable DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo) {
        super(context, snapshot, splitAssigner);
        this.dynamicPartitionFilteringInfo = dynamicPartitionFilteringInfo;
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof DynamicFilteringEvent) {
            DynamicFilteringData dynamicFilteringData =
                    ((DynamicFilteringEvent) sourceEvent).getData();
            LOG.info(
                    "Received DynamicFilteringEvent: {}, is filtering: {}.",
                    subtaskId,
                    dynamicFilteringData.isFiltering());

            checkNotNull(
                    dynamicPartitionFilteringInfo,
                    "Cannot apply dynamic filtering because dynamicPartitionFilteringInfo hasn't been set.");

            if (dynamicFilteringData.isFiltering()) {
                this.splitAssigner =
                        new DynamicPartitionPruningAssigner(
                                splitAssigner,
                                dynamicPartitionFilteringInfo.getPartitionRowProjection(),
                                dynamicFilteringData);
            }
        } else {
            super.handleSourceEvent(subtaskId, sourceEvent);
        }
    }
}
