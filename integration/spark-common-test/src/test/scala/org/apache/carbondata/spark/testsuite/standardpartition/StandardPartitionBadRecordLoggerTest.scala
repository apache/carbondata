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

package org.apache.carbondata.spark.testsuite.standardpartition

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test Class for detailed query on timestamp datatypes
 *
 *
 */
class StandardPartitionBadRecordLoggerTest extends QueryTest with BeforeAndAfterAll {
  var hiveContext: HiveContext = _

  override def beforeAll {
    drop()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
  }

  test("test partition redirect") {
    sql(
      s"""CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) partitioned by (country String) STORED AS carbondata TBLPROPERTIES('BAD_RECORD_PATH'='$warehouse')""")

    val csvFilePath = s"$resourcesPath/badrecords/datasample.csv"
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS"
        +
        "('bad_records_logger_enable'='true','bad_records_action'='redirect', 'DELIMITER'=" +
        " ',', 'QUOTECHAR'= '\"')")
    checkAnswer(
      sql("select count(*) from sales"),
      Seq(Row(2)
      )
    )
  }

  test("test partition serializable_values") {
    // 1.0 "\N" which should be treated as NULL
    // 1.1 Time stamp "\N" which should be treated as NULL
    val csvFilePath = s"$resourcesPath/badrecords/seriazableValue.csv"
    sql(
      """CREATE TABLE IF NOT EXISTS serializable_values(ID BigInt, date Timestamp,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) partitioned by (country String) STORED AS carbondata
        """)
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE serializable_values OPTIONS"
        +
        "('bad_records_logger_enable'='true', 'bad_records_action'='ignore', " +
        "'DELIMITER'= ',', 'QUOTECHAR'= '\"')")
    checkAnswer(
      sql("select count(*) from serializable_values"),
      Seq(Row(2)
      )
    )
  }

  test("test partition serializable_values_false") {
    val csvFilePath = s"$resourcesPath/badrecords/seriazableValue.csv"
    // load with bad_records_logger_enable false
    sql(
      """CREATE TABLE IF NOT EXISTS serializable_values_false(ID BigInt, date Timestamp,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) partitioned by (country String) STORED AS carbondata
        """)
    sql(
      "LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE serializable_values_false OPTIONS"
      + "('bad_records_logger_enable'='false', 'DELIMITER'= ',', 'QUOTECHAR'= '\"')");
    checkAnswer(
      sql("select count(*) from serializable_values_false"),
      Seq(Row(2)
      )
    )
  }

  test("test partition with empty_timestamp") {
    // 4.1 Time stamp empty data - Bad records/Null value based on configuration
    // 5. non-parsable data - Bad records/Null value based on configuration
    // 6. empty line(check current one) - Bad records/Null value based on configuration
    // only one value should be loadded.
    sql(
      """CREATE TABLE IF NOT EXISTS empty_timestamp(ID BigInt, date Timestamp,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) partitioned by (country String) STORED AS carbondata
        """)
    val csvFilePath = s"$resourcesPath/badrecords/emptyTimeStampValue.csv"
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE empty_timestamp OPTIONS"
        +
        "('bad_records_logger_enable'='true','IS_EMPTY_DATA_BAD_RECORD'='true' ," +
        "'bad_records_action'='ignore', " +
        "'DELIMITER'= ',', 'QUOTECHAR'= '\"')")
    checkAnswer(
      sql("select count(*) from empty_timestamp"),
      Seq(Row(1)
      )
    )
  }

  test("test partition with insufficientColumn") {
    // 2. insufficient columns - Bad records/Null value based on configuration
    sql(
      """CREATE TABLE IF NOT EXISTS insufficientColumn(date Timestamp,country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) partitioned by (ID BigInt) STORED AS carbondata
        """)
    val csvFilePath = s"$resourcesPath/badrecords/insufficientColumns.csv"
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE insufficientColumn OPTIONS"
        +
        "('bad_records_logger_enable'='true', 'bad_records_action'='ignore', " +
        "'DELIMITER'= ',', 'QUOTECHAR'= '\"')")
    checkAnswer(
      sql("select count(*) from insufficientColumn"),
      Seq(Row(3)
      )
    )
  }

  test("test partition with insufficientColumn_false") {
    // load with bad_records_logger_enable false
    sql(
      """CREATE TABLE IF NOT EXISTS insufficientColumn_false(date Timestamp,country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) partitioned by (ID BigInt) STORED AS carbondata
        """)
    val csvFilePath = s"$resourcesPath/badrecords/insufficientColumns.csv"
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE insufficientColumn_false OPTIONS"
        + "('bad_records_logger_enable'='false', 'DELIMITER'= ',', 'QUOTECHAR'= '\"')")
    checkAnswer(
      sql("select count(*) from insufficientColumn_false"),
      Seq(Row(3)
      )
    )
  }


  test("test partition with emptyColumnValues") {
    // 3. empty data for string data type - take empty value
    // 4. empty data for non-string data type - Bad records/Null value based on configuration
    //table should have only two records.
    sql(
      """CREATE TABLE IF NOT EXISTS emptyColumnValues(date Timestamp,country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) partitioned by (ID BigInt) STORED AS carbondata
        """)
    val csvFilePath = s"$resourcesPath/badrecords/emptyValues.csv"
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE emptyColumnValues OPTIONS"
        +
        "('bad_records_logger_enable'='true','IS_EMPTY_DATA_BAD_RECORD'='true'," +
        " 'bad_records_action'='ignore', " +
        "'DELIMITER'= ',', 'QUOTECHAR'= '\"')")
    checkAnswer(
      sql("select count(*) from emptyColumnValues"),
      Seq(Row(2)
      )
    )
  }

  test("test partition with emptyColumnValues_false") {
    // load with bad_records_logger_enable to false
    sql(
      """CREATE TABLE IF NOT EXISTS emptyColumnValues_false(date Timestamp,country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) partitioned by (ID BigInt) STORED AS carbondata
        """)
    val csvFilePath = s"$resourcesPath/badrecords/emptyValues.csv"
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE emptyColumnValues_false OPTIONS"
        + "('bad_records_logger_enable'='false', 'DELIMITER'= ',', 'QUOTECHAR'= '\"')");
    checkAnswer(
      sql("select count(*) from emptyColumnValues_false"),
      Seq(Row(7)
      )
    )
  }

  test("test partition with empty_timestamp_false") {
    // load with bad_records_logger_enable to false
    sql(
      """CREATE TABLE IF NOT EXISTS empty_timestamp_false(ID BigInt, date Timestamp,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) partitioned by (country String) STORED AS carbondata
        """)
    val csvFilePath = s"$resourcesPath/badrecords/emptyTimeStampValue.csv"
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE empty_timestamp_false OPTIONS"
        + "('bad_records_logger_enable'='false', 'DELIMITER'= ',', 'QUOTECHAR'= '\"')");
    checkAnswer(
      sql("select count(*) from empty_timestamp_false"),
      Seq(Row(7)
      )
    )
  }

  test("test load ddl command") {
    sql(
      """CREATE TABLE IF NOT EXISTS dataloadOptionTests(ID BigInt, date Timestamp, country
           String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata
      """)
    val csvFilePath = s"$resourcesPath/badrecords/emptyTimeStampValue.csv"
    try {
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE dataloadOptionTests OPTIONS"
        + "('bad_records_action'='FORCA', 'DELIMITER'= ',', 'QUOTECHAR'= '\"')");
    } catch {
      case ex: Exception =>
        assert("option BAD_RECORDS_ACTION can have only either FORCE or IGNORE or REDIRECT or FAIL"
          .equals(ex.getMessage))
    }
  }

  def drop(): Unit = {
    sql("drop table IF EXISTS sales")
    sql("drop table IF EXISTS serializable_values")
    sql("drop table IF EXISTS serializable_values_false")
    sql("drop table IF EXISTS insufficientColumn")
    sql("drop table IF EXISTS insufficientColumn_false")
    sql("drop table IF EXISTS emptyColumnValues")
    sql("drop table IF EXISTS emptyColumnValues_false")
    sql("drop table IF EXISTS empty_timestamp")
    sql("drop table IF EXISTS empty_timestamp_false")
    sql("drop table IF EXISTS dataloadOptionTests")
  }

  override def afterAll {
    drop()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }
}