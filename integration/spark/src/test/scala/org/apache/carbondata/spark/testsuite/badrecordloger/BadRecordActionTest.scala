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

package org.apache.carbondata.spark.testsuite.badrecordloger

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterEach

import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class BadRecordActionTest extends QueryTest {


  val csvFilePath = s"$resourcesPath/badrecords/datasample.csv"
  val badRecordFilePath = new File(currentPath + "/target/test/badRecords")
  initCarbonProperties

  private def initCarbonProperties = {
    defaultConfig()
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, LoggerAction.FORCE.name())
    badRecordFilePath.mkdirs()
  }

  test("test load for bad_record_action=force") {
    sql("drop table if exists sales")
    sql(
      """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata""")
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS" +
        "('bad_records_action'='force', 'DELIMITER'=" +
        " ',', 'QUOTECHAR'= '\"','timestampformat'='yyyy/MM/dd')")
    checkAnswer(sql("select count(*) from sales"),
      Seq(Row(6)))

  }

  test("test load for bad_record_action=FORCE") {
    sql("drop table if exists sales")
    sql(
      """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata""")
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS" +
        "('bad_records_action'='FORCE', 'DELIMITER'=" +
        " ',', 'QUOTECHAR'= '\"','timestampformat'='yyyy/MM/dd')")
    checkAnswer(sql("select count(*) from sales"),
      Seq(Row(6)))
  }

  test("test load for bad_record_action=fail") {
    sql("drop table if exists sales")
    sql(
      """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata""")
    val exception = intercept[Exception] {
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS" +
          "('bad_records_action'='fail', 'DELIMITER'=" +
          " ',', 'QUOTECHAR'= '\"','timestampformat'='yyyy/MM/dd')")
    }
    assert(exception.getMessage
      .contains(
        "Data load failed due to bad record: The value with column name date and column data " +
        "type TIMESTAMP is not a valid TIMESTAMP type.Please enable bad record logger to know" +
        " the detail reason"))

  }

  test("test load for bad_record_action=FAIL") {
    sql("drop table if exists sales")
    sql(
      """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata""")
    val exception = intercept[Exception] {
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS" +
          "('bad_records_action'='FAIL', 'DELIMITER'=" +
          " ',', 'QUOTECHAR'= '\"','timestampformat'='yyyy/MM/dd')")
    }
    assert(exception.getMessage
      .contains(
        "Data load failed due to bad record: The value with column name date and column data " +
        "type TIMESTAMP is not a valid TIMESTAMP type.Please enable bad record logger to know" +
        " the detail reason"))
  }


  test("test load for bad_record_action=ignore") {
    sql("drop table if exists sales")
    sql(
      """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata""")
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS" +
        "('bad_records_action'='ignore', 'DELIMITER'=" +
        " ',', 'QUOTECHAR'= '\"','timestampformat'='yyyy/MM/dd')")
    checkAnswer(sql("select count(*) from sales"),
      Seq(Row(2)))
  }

  test("test load for bad_record_action=IGNORE") {
    sql("drop table if exists sales")
    sql(
      """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata""")
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS" +
        "('bad_records_action'='IGNORE', 'DELIMITER'=" +
        " ',', 'QUOTECHAR'= '\"','timestampformat'='yyyy/MM/dd')")
    checkAnswer(sql("select count(*) from sales"),
      Seq(Row(2)))
  }

  test("test bad record REDIRECT but not having empty location in option should throw exception") {
    sql("drop table if exists sales")
    sql(
      """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata""")
    val badRecordLocation = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC,
      CarbonCommonConstants.CARBON_BADRECORDS_LOC_DEFAULT_VAL)
    try {
      val exMessage = intercept[Exception] {
        sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS" +
            "('bad_records_action'='REDIRECT', 'DELIMITER'=" +
            " ',', 'QUOTECHAR'= '\"','timestampformat'='yyyy/MM/dd')")
      }
      assert(exMessage.getMessage
        .contains("Cannot redirect bad records as bad record location is not provided."))
    }
    finally {
      CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC,
        badRecordLocation)
    }
  }

  test("test bad record is REDIRECT with location in carbon properties should pass") {
    sql("drop table if exists sales")
    sql(
      """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata""")
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS" +
        "('bad_records_action'='REDIRECT', 'DELIMITER'=" +
        " ',', 'QUOTECHAR'= '\"','timestampformat'='yyyy/MM/dd')")
    checkAnswer(sql("select count(*) from sales"),
      Seq(Row(2)))
  }

  test("test bad record is redirect with location in option while data loading should pass") {
    sql("drop table if exists sales")
    sql(
      """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata""")
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS" +
        "('bad_records_action'='REDIRECT', 'DELIMITER'=" +
        " ',', 'QUOTECHAR'= '\"', 'BAD_RECORD_PATH'='" + { badRecordFilePath.getCanonicalPath } +
        "','timestampformat'='yyyy/MM/dd')")
    checkAnswer(sql("select count(*) from sales"),
      Seq(Row(2)))
  }

  test("test bad record FORCE option with no_sort as sort scope ") {
    sql("drop table if exists sales_no_sort")
    sql(
      """CREATE TABLE IF NOT EXISTS sales_no_sort(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata tblproperties('sort_scope'='NO_SORT')""")

    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales_no_sort OPTIONS" +
        "('bad_records_action'='FORCE', 'DELIMITER'=" +
        " ',', 'QUOTECHAR'= '\"','timestampformat'='yyyy/MM/dd')")
    checkAnswer(sql("select count(*) from sales_no_sort"),
      Seq(Row(6)))
  }

  test("test bad record REDIRECT option with location and no_sort as sort scope ") {
    sql("drop table if exists sales_no_sort")
    sql(
      """CREATE TABLE IF NOT EXISTS sales_no_sort(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata tblproperties('sort_scope'='NO_SORT')""")

    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales_no_sort OPTIONS" +
        "('bad_records_action'='REDIRECT', 'DELIMITER'=" +
        " ',', 'QUOTECHAR'= '\"', 'BAD_RECORD_PATH'='" + { badRecordFilePath.getCanonicalPath } +
        "','timestampformat'='yyyy/MM/dd')")
    checkAnswer(sql("select count(*) from sales_no_sort"),
      Seq(Row(2)))
  }

  test("test bad record IGNORE option with no_sort as sort scope ") {
    sql("drop table if exists sales_no_sort")
    sql(
      """CREATE TABLE IF NOT EXISTS sales_no_sort(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata tblproperties('sort_scope'='NO_SORT')""")

    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales_no_sort OPTIONS" +
        "('bad_records_action'='IGNORE', 'DELIMITER'=" +
        " ',', 'QUOTECHAR'= '\"','timestampformat'='yyyy/MM/dd')")
    checkAnswer(sql("select count(*) from sales_no_sort"),
      Seq(Row(2)))
  }

  test("test bad record with FAIL option with location and no_sort as sort scope ") {
    sql("drop table if exists sales_no_sort")
    sql(
      """CREATE TABLE IF NOT EXISTS sales_no_sort(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata tblproperties('sort_scope'='NO_SORT')""")

    val exception = intercept[Exception] {
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales_no_sort OPTIONS" +
          "('bad_records_action'='FAIL', 'DELIMITER'=" +
          " ',', 'QUOTECHAR'= '\"','timestampformat'='yyyy/MM/dd')")
    }
    assert(exception.getMessage
      .contains(
        "Data load failed due to bad record: The value with column name date and column data " +
        "type TIMESTAMP is not a valid TIMESTAMP type.Please enable bad record logger to know" +
        " the detail reason"))
  }

  test("test bad record with IGNORE option and sort scope as NO_SORT for bucketed table") {
    sql("drop table if exists sales_bucket")
    sql("CREATE TABLE IF NOT EXISTS sales_bucket(ID BigInt, date Timestamp, country String," +
          "actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata TBLPROPERTIES ('BUCKETNUMBER'='2', 'BUCKETCOLUMNS'='country','sort_scope'='NO_SORT')")
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales_bucket OPTIONS" +
        "('bad_records_action'='IGNORE', 'DELIMITER'=" +
        " ',', 'QUOTECHAR'= '\"','timestampformat'='yyyy/MM/dd')")
    checkAnswer(sql("select count(*) from sales_bucket"),
      Seq(Row(2)))
  }

  test("test bad record with REDIRECT option and sort scope as NO_SORT for bucketed table") {
    sql("drop table if exists sales_bucket")
    sql("CREATE TABLE IF NOT EXISTS sales_bucket(ID BigInt, date Timestamp, country String," +
        "actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata TBLPROPERTIES ('BUCKETNUMBER'='2', 'BUCKETCOLUMNS'='country', 'sort_scope'='NO_SORT')")
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales_bucket OPTIONS" +
        "('bad_records_action'='REDIRECT', 'DELIMITER'=" +
        " ',', 'QUOTECHAR'= '\"', 'BAD_RECORD_PATH'='" + { badRecordFilePath.getCanonicalPath } +
        "','timestampformat'='yyyy/MM/dd')")
    checkAnswer(sql("select count(*) from sales_bucket"),
      Seq(Row(2)))
  }

  test("test bad record IGNORE with complex data types") {
    val timeStampFormat = CarbonProperties.getInstance().getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table if exists complextable")
    sql("create table complextable(arrayColumn array<timestamp>, structColumn struct<s1:int,s2:timestamp>,arraystruct array<Struct<as1:int,as2:timestamp>>) stored as carbondata")
    sql(s"LOAD DATA local inpath '$resourcesPath/badrecords/complexdata.csv' INTO TABLE complextable OPTIONS('bad_records_action'='ignore', 'DELIMITER'=',', " +
        "'QUOTECHAR'= '\"','COMPLEX_DELIMITER_LEVEL_1'='$','COMPLEX_DELIMITER_LEVEL_2'='#')")
    checkAnswer(sql("select count(*) from complextable"), Seq(Row(5)))
    sql("drop table if exists complextable")
    if(null != timeStampFormat) {
      CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, timeStampFormat)
    }
  }


  private def currentPath: String = {
    new File(this.getClass.getResource("/").getPath + "../../")
      .getCanonicalPath
  }
}