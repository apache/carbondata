/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.spark.testsuite.badrecordloger

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties


/**
 * Test Class for detailed query on timestamp datatypes
 *
 *
 */
class BadRecordLoggerTest extends QueryTest with BeforeAndAfterAll {
  var hiveContext: HiveContext = _

  override def beforeAll {
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
      sql(
        """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED BY 'carbondata'""")

      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC,
          new File("./target/test/badRecords")
            .getCanonicalPath)

      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
      val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
        .getCanonicalPath
      var csvFilePath = currentDirectory + "/src/test/resources/badrecords/datasample.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS"
          +
          "('bad_records_logger_enable'='true','bad_records_logger_action'='redirect', 'DELIMITER'=" +
          " ',', 'QUOTECHAR'= '\"')");

      // 1.0 "\N" which should be treated as NULL
      // 1.1 Time stamp "\N" which should be treated as NULL
      csvFilePath = currentDirectory +
                    "/src/test/resources/badrecords/seriazableValue.csv"
      sql(
        """CREATE TABLE IF NOT EXISTS serializable_values(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED BY 'carbondata'
        """)
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE serializable_values OPTIONS"
          +
          "('bad_records_logger_enable'='true', 'bad_records_logger_action'='ignore', " +
          "'DELIMITER'= ',', 'QUOTECHAR'= '\"')");
      // load with bad_records_logger_enable false
      sql(
        """CREATE TABLE IF NOT EXISTS serializable_values_false(ID BigInt, date Timestamp,
           country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED BY 'carbondata'
        """)
      sql(
        "LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE serializable_values_false OPTIONS"
        + "('bad_records_logger_enable'='false', 'DELIMITER'= ',', 'QUOTECHAR'= '\"')");
      // 2. insufficient columns - Bad records/Null value based on configuration
      sql(
        """CREATE TABLE IF NOT EXISTS insufficientColumn(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED BY 'carbondata'
        """)
      csvFilePath = currentDirectory +
                    "/src/test/resources/badrecords/insufficientColumns.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE insufficientColumn OPTIONS"
          +
          "('bad_records_logger_enable'='true', 'bad_records_logger_action'='ignore', " +
          "'DELIMITER'= ',', 'QUOTECHAR'= '\"')");
      // load with bad_records_logger_enable false
      sql(
        """CREATE TABLE IF NOT EXISTS insufficientColumn_false(ID BigInt, date Timestamp, country
            String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED BY 'carbondata'
        """)
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE insufficientColumn_false OPTIONS"
          + "('bad_records_logger_enable'='false', 'DELIMITER'= ',', 'QUOTECHAR'= '\"')");

      // 3. empty data for string data type - take empty value
      // 4. empty data for non-string data type - Bad records/Null value based on configuration
      //table should have only two records.
      sql(
        """CREATE TABLE IF NOT EXISTS emptyColumnValues(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED BY 'carbondata'
        """)
      csvFilePath = currentDirectory +
                    "/src/test/resources/badrecords/emptyValues.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE emptyColumnValues OPTIONS"
          +
          "('bad_records_logger_enable'='true', 'bad_records_logger_action'='ignore', " +
          "'DELIMITER'= ',', 'QUOTECHAR'= '\"')");
      // load with bad_records_logger_enable to false
      sql(
        """CREATE TABLE IF NOT EXISTS emptyColumnValues_false(ID BigInt, date Timestamp, country
           String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED BY 'carbondata'
        """)
      csvFilePath = currentDirectory +
                    "/src/test/resources/badrecords/emptyValues.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE emptyColumnValues_false OPTIONS"
          + "('bad_records_logger_enable'='false', 'DELIMITER'= ',', 'QUOTECHAR'= '\"')");


      // 4.1 Time stamp empty data - Bad records/Null value based on configuration
      // 5. non-parsable data - Bad records/Null value based on configuration
      // 6. empty line(check current one) - Bad records/Null value based on configuration
      // only one value should be loadded.
      sql(
        """CREATE TABLE IF NOT EXISTS empty_timestamp(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED BY 'carbondata'
        """)
      csvFilePath = currentDirectory +
                    "/src/test/resources/badrecords/emptyTimeStampValue.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE empty_timestamp OPTIONS"
          +
          "('bad_records_logger_enable'='true', 'bad_records_logger_action'='ignore', " +
          "'DELIMITER'= ',', 'QUOTECHAR'= '\"')");
      // load with bad_records_logger_enable to false
      sql(
        """CREATE TABLE IF NOT EXISTS empty_timestamp_false(ID BigInt, date Timestamp, country
           String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED BY 'carbondata'
        """)
      csvFilePath = currentDirectory +
                    "/src/test/resources/badrecords/emptyTimeStampValue.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE empty_timestamp_false OPTIONS"
          + "('bad_records_logger_enable'='false', 'DELIMITER'= ',', 'QUOTECHAR'= '\"')");


    } catch {
      case x: Throwable => CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    }
  }

  test("select count(*) from sales") {
    sql("select count(*) from sales").show()
    checkAnswer(
      sql("select count(*) from sales"),
      Seq(Row(2)
      )
    )
  }

  test("select count(*) from serializable_values") {
    sql("select count(*) from serializable_values").show()
    checkAnswer(
      sql("select count(*) from serializable_values"),
      Seq(Row(2)
      )
    )
  }

  test("select count(*) from serializable_values_false") {
    sql("select count(*) from serializable_values_false").show()
    checkAnswer(
      sql("select count(*) from serializable_values_false"),
      Seq(Row(2)
      )
    )
  }

  test("select count(*) from empty_timestamp") {
    sql("select count(*) from empty_timestamp").show()
    checkAnswer(
      sql("select count(*) from empty_timestamp"),
      Seq(Row(1)
      )
    )
  }

  test("select count(*) from insufficientColumn") {
    sql("select count(*) from insufficientColumn").show()
    checkAnswer(
      sql("select count(*) from insufficientColumn"),
      Seq(Row(1)
      )
    )
  }

  test("select count(*) from insufficientColumn_false") {
    sql("select count(*) from insufficientColumn_false").show()
    checkAnswer(
      sql("select count(*) from insufficientColumn_false"),
      Seq(Row(3)
      )
    )
  }


  test("select count(*) from emptyColumnValues") {
    sql("select count(*) from emptyColumnValues").show()
    checkAnswer(
      sql("select count(*) from emptyColumnValues"),
      Seq(Row(2)
      )
    )
  }

  test("select count(*) from emptyColumnValues_false") {
    sql("select count(*) from emptyColumnValues_false").show()
    checkAnswer(
      sql("select count(*) from emptyColumnValues_false"),
      Seq(Row(7)
      )
    )
  }

  test("select count(*) from empty_timestamp_false") {
    sql("select count(*) from empty_timestamp_false").show()
    checkAnswer(
      sql("select count(*) from empty_timestamp_false"),
      Seq(Row(7)
      )
    )
  }


  override def afterAll {
    sql("drop table sales")
    sql("drop table serializable_values")
    sql("drop table serializable_values_false")
    sql("drop table insufficientColumn")
    sql("drop table insufficientColumn_false")
    sql("drop table emptyColumnValues")
    sql("drop table emptyColumnValues_false")
    sql("drop table empty_timestamp")
    sql("drop table empty_timestamp_false")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }
}