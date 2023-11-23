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
package org.apache.paimon.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal, Or}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.assertj.core.api.Assertions.assertThat

class SparkPushDownTest extends PaimonSparkTestBase {

  test(s"Paimon push down: apply partition filter push down with non-partitioned table") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, pt STRING)
                 |TBLPROPERTIES ('primary-key'='id, pt', 'bucket'='2')
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', 'p1'), (2, 'b', 'p1'), (3, 'c', 'p2')")

    assertThat(spark.sql("SELECT * FROM T WHERE pt = 'p1'").queryExecution.optimizedPlan.exists {
      case Filter(c: Expression, _) =>
        c.exists {
          case EqualTo(a: AttributeReference, l: Literal) =>
            a.name.equals("pt") && l.value.toString.equals("p1")
          case _ => false
        }
      case _ => false
    }).isTrue
  }

  test(s"Paimon push down: apply partition filter push down with partitioned table") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, pt STRING)
                 |TBLPROPERTIES ('primary-key'='id, pt', 'bucket'='2')
                 |PARTITIONED BY (pt)
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', 'p1'), (2, 'b', 'p1'), (3, 'c', 'p2'), (4, 'd', 'p3')")

    // partition filter push down did not hit
    assertThat(spark.sql("SELECT * FROM T WHERE id = '1'").queryExecution.optimizedPlan.exists {
      case Filter(_: Expression, _) => true
      case _ => false
    }).isTrue
    checkAnswer(spark.sql("SELECT * FROM T WHERE id = '1'"), Row(1, "a", "p1") :: Nil)

    assertThat(
      spark.sql("SELECT * FROM T WHERE id = '1' or pt = 'p1'").queryExecution.optimizedPlan.exists {
        case Filter(_: Or, _) => true
        case _ => false
      }).isTrue
    checkAnswer(
      spark.sql("SELECT * FROM T WHERE id = '1' or pt = 'p1'"),
      Row(1, "a", "p1") :: Row(2, "b", "p1") :: Nil)

    // partition filter push down hit
    assertThat(spark.sql("SELECT * FROM T WHERE pt = 'p1'").queryExecution.optimizedPlan.exists {
      case Filter(_: Expression, _) => true
      case _ => false
    }).isFalse
    checkAnswer(
      spark.sql("SELECT * FROM T WHERE pt = 'p1'"),
      Row(1, "a", "p1") :: Row(2, "b", "p1") :: Nil)

    assertThat(
      spark
        .sql("SELECT * FROM T WHERE id = '1' and pt = 'p1'")
        .queryExecution
        .optimizedPlan
        .exists {
          case Filter(c: Expression, _) =>
            c.exists {
              case EqualTo(a: AttributeReference, l: Literal) =>
                a.name.equals("pt") && l.value.toString.equals("p1")
              case _ => false
            }
          case _ => false
        }).isFalse
    checkAnswer(spark.sql("SELECT * FROM T WHERE id = '1' and pt = 'p1'"), Row(1, "a", "p1") :: Nil)

    assertThat(spark.sql("SELECT * FROM T WHERE pt < 'p3'").queryExecution.optimizedPlan.exists {
      case Filter(_: Expression, _) => true
      case _ => false
    }).isFalse
    checkAnswer(
      spark.sql("SELECT * FROM T WHERE pt < 'p3'"),
      Row(1, "a", "p1") :: Row(2, "b", "p1") :: Row(3, "c", "p2") :: Nil)
  }

}
