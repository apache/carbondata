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
package org.apache.carbondata.spark.testsuite.secondaryindex

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.secondaryindex.joins.BroadCastSIFilterPushJoin
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, Ignore}

class TestSIWithPartition extends QueryTest with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    sql("drop table if exists uniqdata1")
    sql(
      "CREATE TABLE uniqdata1 (CUST_ID INT,CUST_NAME STRING,DOB timestamp,DOJ timestamp," +
      "BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 DECIMAL(30, 10)," +
      "DECIMAL_COLUMN2 DECIMAL(36, 10),Double_COLUMN1 double, Double_COLUMN2 double," +
      "INTEGER_COLUMN1 int) PARTITIONED BY(ACTIVE_EMUI_VERSION string) STORED AS carbondata " +
      "TBLPROPERTIES('TABLE_BLOCKSIZE'='256 MB')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data_2000.csv' INTO " +
        "TABLE uniqdata1 partition(ACTIVE_EMUI_VERSION='abc') OPTIONS('DELIMITER'=',', " +
        "'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID," +
        "CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1," +
        "DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data_2000.csv' INTO " +
        "TABLE uniqdata1 partition(ACTIVE_EMUI_VERSION='abc') OPTIONS('DELIMITER'=',', " +
        "'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID," +
        "CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1," +
        "DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data_2000.csv' INTO " +
        "TABLE uniqdata1 partition(ACTIVE_EMUI_VERSION='abc') OPTIONS('DELIMITER'=',', " +
        "'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID," +
        "CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1," +
        "DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data_2000.csv' INTO " +
        "TABLE uniqdata1 partition(ACTIVE_EMUI_VERSION='abc') OPTIONS('DELIMITER'=',', " +
        "'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID," +
        "CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1," +
        "DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
  }

  test("Testing SI on partition column") {
    sql("drop index if exists indextable1 on uniqdata1")
    intercept[UnsupportedOperationException] {
      sql("create index indextable1 on table uniqdata1 (ACTIVE_EMUI_VERSION) AS 'carbondata'")
    }
  }

  test("Testing SI without partition column") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")
    val withoutIndex =
      sql("select * from uniqdata1 where ni(CUST_NAME='CUST_NAME_00108')")
        .collect().toSeq

    checkAnswer(sql("select * from uniqdata1 where CUST_NAME='CUST_NAME_00108'"),
      withoutIndex)

    val df = sql("select * from uniqdata1 where CUST_NAME='CUST_NAME_00108'")
      .queryExecution
      .sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
  }

  test("Testing SI with partition column[where clause]") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")
    val withoutIndex =
      sql(
        "select * from uniqdata1 where ni(CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = " +
        "'abc')")
        .collect().toSeq

    checkAnswer(sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = 'abc'"),
      withoutIndex)

    val df = sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = 'abc'")
      .queryExecution
      .sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
  }

  test("Testing SI on partition table with OR condition") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")
    val withoutIndex =
      sql(
        "select * from uniqdata1 where ni(CUST_NAME='CUST_NAME_00108' OR ACTIVE_EMUI_VERSION = " +
        "'abc')")
        .collect().toSeq

    checkAnswer(sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' OR ACTIVE_EMUI_VERSION = 'abc'"),
      withoutIndex)

    val df = sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' OR ACTIVE_EMUI_VERSION = 'abc'")
      .queryExecution
      .sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(true)
    } else {
      assert(false)
    }
  }

  test("Testing SI on partition table with combination of OR OR") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")
    val withoutIndex =
      sql(
        "select * from uniqdata1 where ni(CUST_NAME='CUST_NAME_00108' OR CUST_ID='9000' OR " +
        "ACTIVE_EMUI_VERSION = " +
        "'abc')")
        .collect().toSeq

    checkAnswer(sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' OR CUST_ID='9000' OR " +
      "ACTIVE_EMUI_VERSION = 'abc'"),
      withoutIndex)

    val df = sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' OR CUST_ID='9000' OR " +
      "ACTIVE_EMUI_VERSION = 'abc'")
      .queryExecution
      .sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(true)
    } else {
      assert(false)
    }
  }

  test("Testing SI on partition table with combination of OR AND") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")
    val withoutIndex =
      sql(
        "select * from uniqdata1 where ni(CUST_NAME='CUST_NAME_00108' OR CUST_ID='9000' AND " +
        "ACTIVE_EMUI_VERSION = " +
        "'abc')")
        .collect().toSeq

    checkAnswer(sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' OR CUST_ID='9000' AND " +
      "ACTIVE_EMUI_VERSION = 'abc'"),
      withoutIndex)

    val df = sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' OR CUST_ID='9000' AND " +
      "ACTIVE_EMUI_VERSION = 'abc'")
      .queryExecution
      .sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(true)
    } else {
      assert(false)
    }
  }

  test("Testing SI on partition table with combination of AND OR") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")
    val withoutIndex =
      sql(
        "select * from uniqdata1 where ni(CUST_NAME='CUST_NAME_00108' AND CUST_ID='9000' OR " +
        "ACTIVE_EMUI_VERSION = " +
        "'abc')")
        .collect().toSeq

    checkAnswer(sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' AND CUST_ID='9000' OR " +
      "ACTIVE_EMUI_VERSION = 'abc'"),
      withoutIndex)

    val df = sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' AND CUST_ID='9000' OR " +
      "ACTIVE_EMUI_VERSION = 'abc'")
      .queryExecution
      .sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(true)
    } else {
      assert(false)
    }
  }

  test("Testing SI on partition table with combination of AND AND") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")
    val withoutIndex =
      sql(
        "select * from uniqdata1 where ni(CUST_NAME='CUST_NAME_00108' AND CUST_ID='9000' AND " +
        "ACTIVE_EMUI_VERSION = " +
        "'abc')")
        .collect().toSeq

    checkAnswer(sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' AND CUST_ID='9000' AND " +
      "ACTIVE_EMUI_VERSION = 'abc'"),
      withoutIndex)

    val df = sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' AND CUST_ID='9000' AND " +
      "ACTIVE_EMUI_VERSION = 'abc'")
      .queryExecution
      .sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
  }

  test("Testing SI on partition table with major compaction") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")
    val withoutIndex =
      sql(
        "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = " +
        "'abc'")
        .collect().toSeq

    sql("alter table uniqdata1 compact 'major'")

    checkAnswer(sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = 'abc'"),
      withoutIndex)

    val df = sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = 'abc'")
      .queryExecution
      .sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
  }

  test("Testing SI on partition table with minor compaction") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")

    val withoutIndex =
      sql(
        "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = " +
        "'abc'")
        .collect().toSeq

    sql("alter table uniqdata1 compact 'minor'")

    checkAnswer(sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = 'abc'"),
      withoutIndex)

    val df = sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = 'abc'")
      .queryExecution
      .sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
  }

  test("Testing SI on partition table with delete") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")

    checkAnswer(sql(
      "select count(*) from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION =" +
      " 'abc'"),
      Seq(Row(4)))

    sql("delete from uniqdata1 where CUST_NAME='CUST_NAME_00108'").show()

    checkAnswer(sql(
      "select count(*) from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION =" +
      " 'abc'"),
      Seq(Row(0)))

    val df = sql(
      "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = 'abc'")
      .queryExecution
      .sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
  }

  test("Testing SI on partition table with update") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")

    checkAnswer(sql(
      "select count(*) from uniqdata1 where CUST_ID='9000' and ACTIVE_EMUI_VERSION = 'abc'"),
      Seq(Row(4)))
    intercept[RuntimeException] {
      sql("update uniqdata1 d set (d.CUST_ID) = ('8000')  where d.CUST_ID = '9000'").show()
    }
  }

  test("Testing SI on partition table with rename") {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("create index indextable1 on table uniqdata1 (DOB, CUST_NAME) AS 'carbondata'")

    val withoutIndex =
      sql(
        "select * from uniqdata1 where CUST_NAME='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = " +
        "'abc'")
        .collect().toSeq

    sql("alter table uniqdata1 change CUST_NAME test string")

    checkAnswer(sql(
      "select * from uniqdata1 where test='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = 'abc'"),
      withoutIndex)

    val df = sql(
      "select * from uniqdata1 where test='CUST_NAME_00108' and ACTIVE_EMUI_VERSION = 'abc'")
      .queryExecution
      .sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
  }

  override protected def afterAll(): Unit = {
    sql("drop index if exists indextable1 on uniqdata1")
    sql("drop table if exists uniqdata1")
  }

  /**
   * Method to check whether the filter is push down to SI table or not
   *
   * @param sparkPlan
   * @return
   */
  private def isFilterPushedDownToSI(sparkPlan: SparkPlan): Boolean = {
    var isValidPlan = false
    sparkPlan.transform {
      case broadCastSIFilterPushDown: BroadCastSIFilterPushJoin =>
        isValidPlan = true
        broadCastSIFilterPushDown
    }
    isValidPlan
  }
}
