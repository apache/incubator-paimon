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

package org.apache.paimon.spark.commands

import org.apache.paimon.data.BinaryRow
import org.apache.paimon.table.source.DeletionFile
import org.apache.paimon.utils.{FileStorePathFactory, SerializationUtils}

/**
 * This class will be used as Dataset's pattern type. So here use Array[Byte] instead of BinaryRow
 * or DeletionVector.
 */
case class SparkDeletionVector(
    partition: Array[Byte],
    bucket: Int,
    file: String,
    deletionVector: Array[Byte]) {

  def relativePath(fileStorePathFactory: FileStorePathFactory): String = {
    fileStorePathFactory
      .relativePartitionAndBucketPath(SerializationUtils.deserializeBinaryRow(partition), bucket)
      .toUri
      .toString + "/" + file
  }
}

case class SparkDeletionVectors(
    partitionAndBucket: String,
    partition: Array[Byte],
    bucket: Int,
    fileAndDVs: List[(String, Array[Byte])]
)

case class SparkDeletionFile(partition: BinaryRow, bucket: Int, deletionFile: Option[DeletionFile])
