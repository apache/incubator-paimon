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
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogUtils}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan}
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference}
import org.apache.spark.sql.execution.command.CommandUtils
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{BinaryType, BooleanType, DataType, DatetimeType, DecimalType, DoubleType, FloatType, IntegralType, StringType}
import org.apache.spark.util.{Utils => SparkUtils}

import java.net.URI

/**
 * Some classes or methods defined in the spark project are marked as private under
 * [[org.apache.spark.sql]] package, Hence, use this class to adapt then so that we can use them
 * indirectly.
 */
object Utils {

  /**
   * In the streaming write case, An "Queries with streaming sources must be executed with
   * writeStream.start()" error will occur if we transform [[DataFrame]] first and then use it.
   *
   * That's because the new [[DataFrame]] has a streaming source that is not supported, see the
   * detail: SPARK-14473. So we can create a new [[DataFrame]] using the origin, planned
   * [[org.apache.spark.sql.execution.SparkPlan]].
   *
   * By the way, the origin [[DataFrame]] has been planned by
   * [[org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy]] before call
   * [[org.apache.spark.sql.execution.streaming.Sink.addBatch]].
   */
  def createNewDataFrame(data: DataFrame): DataFrame = {
    data.sqlContext.internalCreateDataFrame(data.queryExecution.toRdd, data.schema)
  }

  def createDataset(sparkSession: SparkSession, logicalPlan: LogicalPlan): Dataset[Row] = {
    Dataset.ofRows(sparkSession, logicalPlan)
  }

  def normalizeExprs(exprs: Seq[Expression], attributes: Seq[Attribute]): Seq[Expression] = {
    DataSourceStrategy.normalizeExprs(exprs, attributes)
  }

  def translateFilter(
      predicate: Expression,
      supportNestedPredicatePushdown: Boolean): Option[Filter] = {
    DataSourceStrategy.translateFilter(predicate, supportNestedPredicatePushdown)
  }

  def fieldReference(name: String): NamedReference = {
    FieldReference.column(name)
  }

  def bytesToString(size: Long): String = {
    SparkUtils.bytesToString(size)
  }

  // for analyze
  def calculateTotalSize(
      sessionState: SessionState,
      tableName: String,
      locationUri: Option[URI]): Long = {
    CommandUtils.calculateSingleLocationSize(
      sessionState,
      new TableIdentifier(tableName),
      locationUri)
  }

  def computeColumnStats(
      sparkSession: SparkSession,
      relation: LogicalPlan,
      columns: Seq[Attribute]): (Long, Map[Attribute, ColumnStat]) = {
    CommandUtils.computeColumnStats(sparkSession, relation, columns)
  }

  def analyzeSupportsType(dataType: DataType): Boolean = dataType match {
    case _: IntegralType => true
    case _: DecimalType => true
    case DoubleType | FloatType => true
    case BooleanType => true
    case _: DatetimeType => true
    case BinaryType | StringType => true
    case _ => false
  }

  def hasMinMax(dataType: DataType): Boolean = dataType match {
    case _: IntegralType => true
    case _: DecimalType => true
    case DoubleType | FloatType => true
    case BooleanType => true
    case _: DatetimeType => true
    case _ => false
  }
}
