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

package org.apache.carbondata.integration.spark.testsuite.dataload

import java.io.File

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class LoadDataWithBadRecordsTest extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  override def beforeEach(): Unit = {
    sql("drop table if exists sales")
    sql("drop table if exists int_table")
    sql("drop table if exists boolean_table")
    sql(
      """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
                  actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED BY 'carbondata'""")
    sql("CREATE TABLE if not exists int_table(intField INT) STORED BY 'carbondata'")
    sql("CREATE TABLE if not exists boolean_table(booleanField INT) STORED BY 'carbondata'")
  }

  override def beforeAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC,
        new File("./target/test/badRecords")
          .getCanonicalPath)
  }

  override def afterAll(): Unit = {
    sql("drop table if exists sales")
    sql("drop table if exists int_table")
    sql("drop table if exists boolean_table")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC, CarbonCommonConstants.CARBON_BADRECORDS_LOC_DEFAULT_VAL)
  }

  val rootPath = new File(this.getClass.getResource("/").getPath
    + "../../../../").getCanonicalPath

  val path = s"$rootPath/integration/spark-common-test/src/test/resources/badrecords/datasample.csv"

  test("bad record: FORCE") {
    sql("LOAD DATA local inpath '" + path + "' INTO TABLE sales OPTIONS" +
      "('bad_records_logger_enable'='true','bad_records_action'='FORCE', 'DELIMITER'=" +
      " ',', 'QUOTECHAR'= '\"')");

    checkAnswer(sql("select count(*) from sales"),
      Seq(Row(6)))
  }

  test("bad record: REDIRECT") {
    sql("LOAD DATA local inpath '" + path + "' INTO TABLE sales OPTIONS" +
      "('bad_records_logger_enable'='true','bad_records_action'='REDIRECT', 'DELIMITER'=" +
      " ',', 'QUOTECHAR'= '\"')");
    checkAnswer(sql("select count(*) from sales"),
      Seq(Row(2)))
  }

  test("bad record: IGNORE") {
    sql("LOAD DATA local inpath '" + path + "' INTO TABLE sales OPTIONS" +
      "('bad_records_logger_enable'='true','bad_records_action'='IGNORE', 'DELIMITER'=" +
      " ',', 'QUOTECHAR'= '\"')");
    checkAnswer(sql("select count(*) from sales"),
      Seq(Row(2)))
  }

  test("bad record: FAIL") {
    val exception_insert: Exception = intercept[Exception] {
      sql("LOAD DATA local inpath '" + path + "' INTO TABLE sales OPTIONS" +
        "('bad_records_logger_enable'='true','bad_records_action'='FAIL', 'DELIMITER'=" +
        " ',', 'QUOTECHAR'= '\"')");
    }
    assert(exception_insert.getMessage.contains("Data load failed due to bad record"))
  }

  val fileLocation = s"$rootPath/integration/spark-common-test/src/test/resources/badrecords/intTest.csv"

  test("Loading table: int, bad_records_action is FORCE") {
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$fileLocation'
         | INTO TABLE int_table
         | OPTIONS('FILEHEADER' = 'intField','bad_records_logger_enable'='true','bad_records_action'='FORCE')
       """.stripMargin)

    checkAnswer(sql("select * from int_table where intField = 1"),
      Seq(Row(1), Row(1)))
  }

  test("Loading table: int, bad_records_action is IGNORE") {
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$fileLocation'
         | INTO TABLE int_table
         | OPTIONS('FILEHEADER' = 'intField','bad_records_logger_enable'='true','bad_records_action'='IGNORE')
       """.stripMargin)

    checkAnswer(sql("select * from int_table where intField = 1"),
      Seq(Row(1), Row(1)))
    checkAnswer(sql("select * from int_table"),
      Seq(Row(1), Row(2), Row(5), Row(4), Row(1), Row(2)))
    checkAnswer(sql("select count(*) from int_table"),
      Seq(Row(6)))
  }

  test("Loading table: int, bad_records_action is REDIRECT") {
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$fileLocation'
         | INTO TABLE int_table
         | OPTIONS('FILEHEADER' = 'intField','bad_records_logger_enable'='true','bad_records_action'='REDIRECT')
       """.stripMargin)

    checkAnswer(sql("select * from int_table where intField = 1"),
      Seq(Row(1), Row(1)))
    checkAnswer(sql("select * from int_table"),
      Seq(Row(1), Row(2), Row(5), Row(4), Row(1), Row(2)))
    checkAnswer(sql("select count(*) from int_table"),
      Seq(Row(6)))
  }

  test("default sort_columns: should be success") {
    var csvFilePath = s"$resourcesPath/badrecords/datasample.csv"
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS"
      +
      "('bad_records_logger_enable'='true','bad_records_action'='redirect', 'DELIMITER'=" +
      " ',', 'QUOTECHAR'= '\"')");

    checkAnswer(
      sql("select count(*) from sales"),
      Seq(Row(2)
      )
    )
  }

  test("sort_columns is null, should be success") {
    sql("drop table if exists sales")
    sql(
      """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2))
          STORED BY 'carbondata'
          TBLPROPERTIES('sort_columns'='')""")

    var csvFilePath = s"$resourcesPath/badrecords/datasample.csv"
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS"
      +
      "('bad_records_logger_enable'='true','bad_records_action'='redirect', 'DELIMITER'=" +
      " ',', 'QUOTECHAR'= '\"')");

    checkAnswer(
      sql("select count(*) from sales"),
      Seq(Row(2)
      )
    )
  }
}
