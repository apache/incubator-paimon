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

package org.apache.paimon.index;

import org.apache.paimon.data.BinaryRow;

import java.util.HashMap;
import java.util.Map;

/** Only for batch sort compact bucket assign. */
public class SimpleBucketAssigner implements BucketAssigner {

    private final int numAssigners;
    private final int assignId;
    private final long targetBucketRowNumber;

    private final Map<BinaryRow, SimplePartitionIndex> partitionIndex;

    public SimpleBucketAssigner(int numAssigners, int assignId, long targetBucketRowNumber) {
        this.numAssigners = numAssigners;
        this.assignId = assignId;
        this.targetBucketRowNumber = targetBucketRowNumber;
        this.partitionIndex = new HashMap<>();
    }

    @Override
    public int assign(BinaryRow partition, int hash) {
        SimplePartitionIndex index =
                this.partitionIndex.computeIfAbsent(partition, p -> new SimplePartitionIndex());
        return index.assign();
    }

    @Override
    public void prepareCommit(long commitIdentifier) {
        // do nothing
    }

    /** Simple partition bucket assigner. */
    private class SimplePartitionIndex {

        private final Map<Integer, Long> bucketInformation;
        private int currentBucket;

        private SimplePartitionIndex() {
            bucketInformation = new HashMap<>();
            loadNewBucket();
        }

        public int assign() {
            Long num = bucketInformation.computeIfAbsent(currentBucket, i -> 0L);
            if (num >= targetBucketRowNumber) {
                loadNewBucket();
            }
            bucketInformation.compute(currentBucket, (i, l) -> l == null ? 1L : l + 1);
            return currentBucket;
        }

        private void loadNewBucket() {
            for (int i = 0; i < Short.MAX_VALUE; i++) {
                if (i % numAssigners == assignId && !bucketInformation.containsKey(i)) {
                    currentBucket = i;
                    return;
                }
            }
            throw new RuntimeException(
                    "Can't find a suitable bucket to assign, all the bucket are assigned?");
        }
    }
}
