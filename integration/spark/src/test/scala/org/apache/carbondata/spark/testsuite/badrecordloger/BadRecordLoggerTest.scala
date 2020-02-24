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

import java.io.{File, FileFilter}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.{CarbonCommonConstants}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.core.datastore.impl.FileFactory

/**
 * Test Class for detailed query on timestamp datatypes
 *
 *
 */
class BadRecordLoggerTest extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    defaultConfig()
    try {
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
      sql("drop table IF EXISTS sales_test")
      sql(
        """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata""")

      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
      var csvFilePath = s"$resourcesPath/badrecords/datasample.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS"
          +
          "('bad_records_logger_enable'='true','bad_records_action'='redirect', 'DELIMITER'=" +
          " ',', 'QUOTECHAR'= '\"')");

      // 1.0 "\N" which should be treated as NULL
      // 1.1 Time stamp "\N" which should be treated as NULL
      csvFilePath = s"$resourcesPath/badrecords/seriazableValue.csv"
      sql(
        """CREATE TABLE IF NOT EXISTS serializable_values(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata
        """)
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE serializable_values OPTIONS"
          +
          "('bad_records_logger_enable'='true', 'bad_records_action'='ignore', " +
          "'DELIMITER'= ',', 'QUOTECHAR'= '\"')");
      // load with bad_records_logger_enable false
      sql(
        """CREATE TABLE IF NOT EXISTS serializable_values_false(ID BigInt, date Timestamp,
           country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata
        """)
      sql(
        "LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE serializable_values_false OPTIONS"
        + "('bad_records_logger_enable'='false', 'DELIMITER'= ',', 'QUOTECHAR'= '\"')");
      // 2. insufficient columns - Bad records/Null value based on configuration
      sql(
        """CREATE TABLE IF NOT EXISTS insufficientColumn(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata
        """)
      csvFilePath = s"$resourcesPath/badrecords/insufficientColumns.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE insufficientColumn OPTIONS"
          +
          "('bad_records_logger_enable'='true', 'bad_records_action'='ignore', " +
          "'DELIMITER'= ',', 'QUOTECHAR'= '\"')");
      // load with bad_records_logger_enable false
      sql(
        """CREATE TABLE IF NOT EXISTS insufficientColumn_false(ID BigInt, date Timestamp, country
            String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata
        """)
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE insufficientColumn_false OPTIONS"
          + "('bad_records_logger_enable'='false', 'DELIMITER'= ',', 'QUOTECHAR'= '\"')");

      // 3. empty data for string data type - take empty value
      // 4. empty data for non-string data type - Bad records/Null value based on configuration
      //table should have only two records.
      sql(
        """CREATE TABLE IF NOT EXISTS emptyColumnValues(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata
        """)
      csvFilePath = s"$resourcesPath/badrecords/emptyValues.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE emptyColumnValues OPTIONS"
          +
          "('bad_records_logger_enable'='true','IS_EMPTY_DATA_BAD_RECORD'='true'," +
          " 'bad_records_action'='ignore', " +
          "'DELIMITER'= ',', 'QUOTECHAR'= '\"')");
      // load with bad_records_logger_enable to false
      sql(
        """CREATE TABLE IF NOT EXISTS emptyColumnValues_false(ID BigInt, date Timestamp, country
           String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata
        """)
      csvFilePath = s"$resourcesPath/badrecords/emptyValues.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE emptyColumnValues_false OPTIONS"
          + "('bad_records_logger_enable'='false', 'DELIMITER'= ',', 'QUOTECHAR'= '\"')");


      // 4.1 Time stamp empty data - Bad records/Null value based on configuration
      // 5. non-parsable data - Bad records/Null value based on configuration
      // 6. empty line(check current one) - Bad records/Null value based on configuration
      // only one value should be loadded.
      sql(
        """CREATE TABLE IF NOT EXISTS empty_timestamp(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata
        """)
      csvFilePath = s"$resourcesPath/badrecords/emptyTimeStampValue.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE empty_timestamp OPTIONS"
          +
          "('bad_records_logger_enable'='true','IS_EMPTY_DATA_BAD_RECORD'='true' ," +
          "'bad_records_action'='ignore', " +
          "'DELIMITER'= ',', 'QUOTECHAR'= '\"')");
      // load with bad_records_logger_enable to false
      sql(
        """CREATE TABLE IF NOT EXISTS empty_timestamp_false(ID BigInt, date Timestamp, country
           String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata
        """)
      csvFilePath = s"$resourcesPath/badrecords/emptyTimeStampValue.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE empty_timestamp_false OPTIONS"
          + "('bad_records_logger_enable'='false', 'DELIMITER'= ',', 'QUOTECHAR'= '\"')");


    } catch {
      case x: Throwable => CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    }
  }

  test("select count(*) from sales") {
    checkAnswer(
      sql("select count(*) from sales"),
      Seq(Row(2)
      )
    )
  }

  test("select count(*) from serializable_values") {
    checkAnswer(
      sql("select count(*) from serializable_values"),
      Seq(Row(2)
      )
    )
  }

  test("select count(*) from serializable_values_false") {
    checkAnswer(
      sql("select count(*) from serializable_values_false"),
      Seq(Row(2)
      )
    )
  }

  test("select count(*) from empty_timestamp") {
    checkAnswer(
      sql("select count(*) from empty_timestamp"),
      Seq(Row(1)
      )
    )
  }

  test("select count(*) from insufficientColumn") {
    checkAnswer(
      sql("select count(*) from insufficientColumn"),
      Seq(Row(3)
      )
    )
  }

  test("select count(*) from insufficientColumn_false") {
    checkAnswer(
      sql("select count(*) from insufficientColumn_false"),
      Seq(Row(3)
      )
    )
  }


  test("select count(*) from emptyColumnValues") {
    checkAnswer(
      sql("select count(*) from emptyColumnValues"),
      Seq(Row(2)
      )
    )
  }

  test("select count(*) from emptyColumnValues_false") {
    checkAnswer(
      sql("select count(*) from emptyColumnValues_false"),
      Seq(Row(7)
      )
    )
  }

  test("select count(*) from empty_timestamp_false") {
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

  test("validate redirected data") {
    cleanBadRecordPath("default", "sales_test")
    val csvFilePath = s"$resourcesPath/badrecords/datasample.csv"
    sql(
      """CREATE TABLE IF NOT EXISTS sales_test(ID BigInt, date long, country int,
          actual_price Double, Quantity String, sold_price Decimal(19,2)) STORED AS carbondata""")

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    try {
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales_test OPTIONS" +
          "('bad_records_logger_enable'='false','bad_records_action'='redirect', 'DELIMITER'=" +
          " ',', 'QUOTECHAR'= '\"')");
    } catch {
      case e: Exception => {
        assert(true)
      }
    }
    val redirectCsvPath = getRedirectCsvPath("default", "sales_test", "0", "0")
    assert(checkRedirectedCsvContentAvailableInSource(csvFilePath, redirectCsvPath))
  }

  test("test load ddl command with improper value") {
    sql("drop table IF EXISTS dataLoadOptionTests")
    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS dataLoadOptionTests(
         |   ID BigInt,
         |   date Timestamp,
         |   country String,
         |   actual_price Double,
         |   Quantity int,
         |   sold_price Decimal(19,2)
         | ) STORED AS carbondata
      """.stripMargin.trim)
    val csvFilePath = s"$resourcesPath/badrecords/emptyTimeStampValue.csv"
    try {
      sql(
        s"""
           | LOAD DATA local inpath '" + $csvFilePath + "' INTO TABLE dataLoadOptionTests
           | OPTIONS(
           |   'bad_records_logger_enable'='fals',
           |   'DELIMITER'= ',',
           |   'QUOTECHAR'= '\"'
           | )""".stripMargin.trim);
      assert(false)
    } catch {
      case ex: Exception =>
        assert(ex.getMessage.contains(
          "option BAD_RECORDS_LOGGER_ENABLE can have only either TRUE or FALSE, " +
            "It shouldn't be fals"
        ))
    } finally {
      sql("drop table IF EXISTS dataLoadOptionTests")
    }
  }

  def getRedirectCsvPath(dbName: String, tableName: String, segment: String, task: String) = {
    var badRecordLocation = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC)
    badRecordLocation = badRecordLocation + "/" + dbName + "/" + tableName + "/" + segment + "/" +
                        task
    val listFiles = new File(badRecordLocation).listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = {
        pathname.getPath.endsWith(".csv")
      }
    })
    listFiles(0)
  }

  /**
   *
   * @param csvFilePath
   * @param redirectCsvPath
   */
  def checkRedirectedCsvContentAvailableInSource(csvFilePath: String,
      redirectCsvPath: File): Boolean = {
    val origFileLineList = FileUtils.readLines(new File(csvFilePath))
    val redirectedFileLineList = FileUtils.readLines(redirectCsvPath)
    val iterator = redirectedFileLineList.iterator()
    while (iterator.hasNext) {
      if (!origFileLineList.contains(iterator.next())) {
        return false;
      }
    }
    return true
  }

  def cleanBadRecordPath(dbName: String, tableName: String) = {
    var badRecordLocation = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC)
    badRecordLocation = badRecordLocation + "/" + dbName + "/" + tableName
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(badRecordLocation))
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table IF EXISTS sales")
    sql("drop table IF EXISTS sales_test")
    sql("drop table IF EXISTS serializable_values")
    sql("drop table IF EXISTS serializable_values_false")
    sql("drop table IF EXISTS insufficientColumn")
    sql("drop table IF EXISTS insufficientColumn_false")
    sql("drop table IF EXISTS emptyColumnValues")
    sql("drop table IF EXISTS emptyColumnValues_false")
    sql("drop table IF EXISTS empty_timestamp")
    sql("drop table IF EXISTS empty_timestamp_false")
    sql("drop table IF EXISTS dataloadOptionTests")
    sql("drop table IF EXISTS loadIssue")
  }
}