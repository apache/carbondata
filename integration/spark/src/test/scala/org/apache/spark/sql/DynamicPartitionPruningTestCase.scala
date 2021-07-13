/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.strategy.CarbonDataSourceScan
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.util.SparkUtil
import org.scalatest.BeforeAndAfterEach

import org.apache.carbondata.spark.rdd.CarbonScanRDD

class DynamicPartitionPruningTestCase extends QueryTest with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = {
    sql("drop table if exists dpp_table1")
    sql("drop table if exists dpp_table2")
  }

  override protected def afterEach(): Unit = {
    sql("drop table if exists dpp_table1")
    sql("drop table if exists dpp_table2")
  }

  test("test partition pruning: carbon join other format table") {
    sql("create table dpp_table1(col1 int) partitioned by (col2 string) stored as carbondata")
    sql("insert into dpp_table1 values(1, 'a'),(2, 'b'),(3, 'c'),(4, 'd')")
    sql("create table dpp_table2(col1 int, col2 string) ")
    sql("insert into dpp_table2 values(1, 'a'),(2, 'b'),(3, 'c'),(4, 'd')")

    // partition table without filter
    verifyPartitionPruning(
      "select t1.col1 from dpp_table1 t1, dpp_table2 t2 " +
      "where t1.col2=t2.col2 and t2.col1 in (1,2)",
      "dpp_table1",
      2,
      4)

    // partition table with filter on partition column
    verifyPartitionPruning(
      "select t1.col1 from dpp_table1 t1, dpp_table2 t2 " +
      "where t1.col2=t2.col2 and t2.col2 in ('b','c') and t2.col1 in (1,2)",
      "dpp_table1",
      1,
      2)

    // partition table with filter on normal column
    verifyPartitionPruning(
      "select t1.col1 from dpp_table1 t1, dpp_table2 t2 " +
      "where t1.col2=t2.col2 and t1.col1 in (2, 3) and t2.col1 in (1,2)",
      "dpp_table1",
      2,
      4)

    // partition table with filter on normal column and partition column
    verifyPartitionPruning(
      "select t1.col1 from dpp_table1 t1, dpp_table2 t2 " +
      "where t1.col2=t2.col2 and t1.col1 in (2, 3) and t1.col2 in ('b', 'c') and t2.col1 in (1,2)",
      "dpp_table1",
      1,
      2)
  }

  test("test partition pruning: carbon join format table") {
    sql("create table dpp_table1(col1 int) partitioned by (col2 string) stored as carbondata")
    sql("insert into dpp_table1 values(1, 'a'),(2, 'b'),(3, 'c'),(4, 'd')")
    sql("create table dpp_table2(col1 int, col2 string) stored as carbondata")
    sql("insert into dpp_table2 values(1, 'a'),(2, 'b'),(3, 'c'),(4, 'd')")

    // partition table without filter
    verifyPartitionPruning(
      "select t1.col1 from dpp_table1 t1, dpp_table2 t2 " +
      "where t1.col2=t2.col2 and t2.col1 in (1,2)",
      "dpp_table1",
      2,
      4)

    // partition table with filter on partition column
    verifyPartitionPruning(
      "select t1.col1 from dpp_table1 t1, dpp_table2 t2 " +
      "where t1.col2=t2.col2 and t2.col2 in ('b','c') and t2.col1 in (1,2)",
      "dpp_table1",
      1,
      2)

    // partition table with filter on normal column
    verifyPartitionPruning(
      "select t1.col1 from dpp_table1 t1, dpp_table2 t2 " +
      "where t1.col2=t2.col2 and t1.col1 in (2, 3) and t2.col1 in (1,2)",
      "dpp_table1",
      2,
      4)

    // partition table with filter on normal column and partition column
    verifyPartitionPruning(
      "select t1.col1 from dpp_table1 t1, dpp_table2 t2 " +
      "where t1.col2=t2.col2 and t1.col1 in (2, 3) and t1.col2 in ('b', 'c') and t2.col1 in (1,2)",
      "dpp_table1",
      1,
      2)
  }

  test("test partition pruning: partitioned table join partitioned table") {
    sql("create table dpp_table1(col1 int) partitioned by (col2 string) stored as carbondata")
    sql("insert into dpp_table1 values(1, 'a'),(2, 'b'),(3, 'c'),(4, 'd')")
    sql("create table dpp_table2(col1 int) partitioned by (col2 string) stored as carbondata")
    sql("insert into dpp_table2 values(1, 'a'),(2, 'b'),(3, 'c'),(4, 'd')")

    // partition table without filter
    verifyPartitionPruning(
      "select t1.col1 from dpp_table1 t1, dpp_table2 t2 " +
      "where t1.col2=t2.col2 and t2.col1 in (1,2)",
      "dpp_table1",
      2,
      4)

    // right table without filter
    verifyPartitionPruning(
      "select /** BROADCAST(t1) */ t1.col1 from dpp_table1 t1, dpp_table2 t2 " +
      "where t1.col2=t2.col2 and t1.col1 in (1,2)",
      "dpp_table2",
      4,
      4)

    // left table with filter on partition column
    verifyPartitionPruning(
      "select t1.col1 from dpp_table1 t1, dpp_table2 t2 " +
      "where t1.col2=t2.col2 and t1.col2 in ('b', 'c') and t2.col1 in (1,2)",
      "dpp_table1",
      1,
      2)

    // right table with filter on partition column
    verifyPartitionPruning(
      "select t1.col1 from dpp_table1 t1, dpp_table2 t2 " +
      "where t1.col2=t2.col2 and t2.col2 in ('b', 'c') and t1.col1 in (1,2)",
      "dpp_table2",
      2,
      2,
      false)

    // left table with filter on normal column
    verifyPartitionPruning(
      "select t1.col1 from dpp_table1 t1, dpp_table2 t2 " +
      "where t1.col2=t2.col2 and t1.col1 in (2, 3) and t2.col1 in (1,2)",
      "dpp_table1",
      2,
      4)

    // right table with filter on normal column
    verifyPartitionPruning(
      "select t1.col1 from dpp_table1 t1, dpp_table2 t2 " +
      "where t1.col2=t2.col2 and t2.col1 in (2, 3) and t1.col1 in (1,2)",
      "dpp_table2",
      4,
      4,
      false)

    // left table with filter on normal column and partition column
    verifyPartitionPruning(
      "select t1.col1 from dpp_table1 t1, dpp_table2 t2 " +
      "where t1.col2=t2.col2 and t1.col1 in (2, 3) and t1.col2 in ('b', 'c') and t2.col1 in (1,2)",
      "dpp_table1",
      1,
      2)

    // right table with filter on normal column and partition column
    verifyPartitionPruning(
      "select t1.col1 from dpp_table1 t1, dpp_table2 t2 " +
      "where t1.col2=t2.col2 and t2.col1 in (2, 3) and t2.col2 in ('b', 'c') and t1.col1 in (1,2)",
      "dpp_table2",
      2,
      2,
      false)

    // both tables with filter on normal column and partition column
    verifyPartitionPruning(
      "select t1.col1 from dpp_table1 t1, dpp_table2 t2 " +
      "where t1.col2=t2.col2 and  t1.col1 in (2, 3) and t1.col2 in ('a', 'b', 'c') and " +
      "t2.col1 = 2 and t2.col2 in ('a', 'b')",
      "dpp_table1",
      1,
      2)

    verifyPartitionPruning(
      "select t1.col1 from dpp_table1 t1, dpp_table2 t2 " +
      "where t1.col2=t2.col2 and  t2.col1 in (2, 3) and t2.col2 in ('a', 'b', 'c') and " +
      "t1.col1 = 2 and t2.col2 in ('a', 'b')",
      "dpp_table2",
      2,
      2,
      false)
  }

  private def verifyPartitionPruning(sqlText: String,
      tableName: String,
      dynamicPartitionPruning: Int,
      staticPartitionPruning: Int,
      hasDynamicPruning: Boolean = true): Unit = {
    if (SPARK_VERSION.startsWith("3")) {
      val df = sql(sqlText)
      df.collect()
      val ds = df.queryExecution.executedPlan.find { plan =>
        plan.isInstanceOf[CarbonDataSourceScan] &&
        plan.asInstanceOf[CarbonDataSourceScan].tableIdentifier.get.table.equals(tableName)
      }
      val carbonDs = ds.get.asInstanceOf[CarbonDataSourceScan]
      val carbonRdd = carbonDs.inputRDDs().head.asInstanceOf[CarbonScanRDD[InternalRow]]
      assert(carbonDs.metadata.contains("PartitionFilters"))
      if (SparkUtil.isSparkVersionXAndAbove("2.4")) {
        if (hasDynamicPruning) {
          assert(carbonDs.metadata("PartitionFilters").contains("dynamicpruning"))
        }
        assertResult(dynamicPartitionPruning)(carbonRdd.partitionNames.size)
      } else {
        assertResult(staticPartitionPruning)(carbonRdd.partitionNames.size)
      }
    }
  }
}
