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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.execution.{RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.strategy.CarbonDataSourceScan
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.rdd.CarbonScanRDD

/**
 * BroadCastFilterPushJoinTestCase
 */
class RuntimeFilterTestCase extends QueryTest with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    defaultConfig()
    dropTable()
    sqlContext.setConf("spark.carbon.pushdown.join.as.filter", "true")
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_PUSH_LEFTSEMIEXIST_JOIN_AS_IN_FILTER, "true")
  }

  override protected def afterAll(): Unit = {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_PUSH_LEFTSEMIEXIST_JOIN_AS_IN_FILTER, "false")
    dropTable()
  }

  private def dropTable(): Unit = {
    sql("drop table if exists joins_left")
    sql("drop table if exists joins_right")
    sql("drop table if exists main")
    sql("drop table if exists main_c2")
  }

  test("push down in subquery for leftsemi equi-join") {
    sql("create table main(c1 int, c2 array<string>) stored as carbondata")
    sql("insert into main select 1, array('a', 'b')")
    sql("insert into main select 2, array('aa', 'bb')")
    sql("create table main_c2(c2 string, c1 int) stored as carbondata")
    sql("insert into main_c2 select explode(c2), c1 from main")
    // check result
    checkAnswer(
      sql("select * from main where array_contains(c2, 'bb')"),
      sql("select * from main where c1 in (select c1 from main_c2 where c2 = 'bb')")
    )
    // check plan, in filter has value list
    val df = sql("select * from main where c1 in (select c1 from main_c2 where c2 = 'bb')")
    val scan = df
      .queryExecution
      .executedPlan
      .find(_.isInstanceOf[RowDataSourceScanExec])
      .get
      .asInstanceOf[RowDataSourceScanExec]
    // filter is null
    assertResult(true)(scan.rdd.asInstanceOf[CarbonScanRDD[Row]].indexFilter == null)
    df.collect()
    // push down in subquery as filter in runtime
    assertResult(true)(scan.rdd.asInstanceOf[CarbonScanRDD[Row]].indexFilter != null)

    // check plan, in filter has no values
    val df2 = sql("select * from main where c1 in (select c1 from main_c2 where c2 = 'bbb')")
    val scan2 = df2
      .queryExecution
      .executedPlan
      .find(_.isInstanceOf[RowDataSourceScanExec])
      .get
      .asInstanceOf[RowDataSourceScanExec]
    // filter is null
    assertResult(true)(scan2.rdd.asInstanceOf[CarbonScanRDD[Row]].splits == null)
    df2.collect()
    // push down in subquery as filter in runtime
    assertResult(true)(scan2.rdd.asInstanceOf[CarbonScanRDD[Row]].splits != null)
  }

  test("push down broadcast as filter for inner equi-join") {
    prepareTable("joins_left", 11)
    prepareTable("joins_right", 1)
    val df1 =
      sql("select a.empno from joins_left a, joins_right b where a.empno = b.empno and b.empno = 20")
    assertResult(true)(isPushedDownJoin(df1.queryExecution.executedPlan, "joins_right"))

    val df2 =
      sql("select a.empno from joins_right a, joins_left b where a.empno = b.empno and b.empno = 20")
    assertResult(true)(isPushedDownJoin(df2.queryExecution.executedPlan, "joins_left"))

    val df3 =
      sql("select a.empno from joins_right a, joins_left b where a.empno = b.empno")
    assertResult(true)(isPushedDownJoin(df3.queryExecution.executedPlan, "joins_left"))
  }

  def prepareTable(tableName: String, numLoads: Int): Unit = {
    sql(
      s"""CREATE TABLE $tableName (
         | empno Int,
         | empname String,
         | designation String,
         | doj Timestamp,
         | workgroupcategory Int,
         | workgroupcategoryname String,
         | deptno Int,
         | deptname String,
         | projectcode Int,
         | projectjoindate Timestamp,
         | projectenddate Timestamp,
         | attendance Int,
         | utilization Int,
         | salary Int
         | ) STORED AS carbondata""".stripMargin)

    (1 to numLoads).foreach { _ =>
      sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv'
              | INTO TABLE $tableName
              | OPTIONS(
              | 'DELIMITER'=',',
              | 'BAD_RECORDS_LOGGER_ENABLE'='FALSE',
              | 'BAD_RECORDS_ACTION'='FORCE'
              | )""".stripMargin)
    }
  }

  def isPushedDownJoin(sparkPlan: SparkPlan, broadCastTable: String): Boolean = {
    val pushJoin = sparkPlan.find(_.isInstanceOf[BroadCastFilterPushJoin])
    if (pushJoin.isDefined) {
      val carbonScan = pushJoin.get.asInstanceOf[BroadCastFilterPushJoin].right
        .find(_.isInstanceOf[CarbonDataSourceScan])
      if (carbonScan.isDefined) {
        broadCastTable.equalsIgnoreCase(
          carbonScan.get.asInstanceOf[CarbonDataSourceScan].tableIdentifier.get.table)
      } else {
        false
      }
    } else {
      false
    }
  }
}
