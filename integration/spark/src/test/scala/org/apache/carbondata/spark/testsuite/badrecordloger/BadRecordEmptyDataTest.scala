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

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test Class for detailed query on timestamp datatypes
 */
class BadRecordEmptyDataTest extends QueryTest with BeforeAndAfterAll {
  var hiveContext: HiveContext = _

  override def beforeAll {
    try {
      sql("drop table IF EXISTS emptyColumnValues")
      sql("drop table IF EXISTS emptyColumnValues_false")
      sql("drop table IF EXISTS empty_timestamp")
      sql("drop table IF EXISTS empty_timestamp_false")
      sql("drop table IF EXISTS dataloadOptionTests")
      sql("drop table IF EXISTS bigtab")
      CarbonProperties.getInstance().addProperty(
        CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, LoggerAction.FORCE.name())
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
      var csvFilePath = ""

      // 1. empty data for string data type - take empty value
      // 2. empty data for non-string data type - Bad records/Null value based on configuration
      // table should have only two records.
      sql(
        """CREATE TABLE IF NOT EXISTS emptyColumnValues(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata
        """)
      csvFilePath = s"$resourcesPath/badrecords/emptyValues.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE emptyColumnValues OPTIONS"
          +
          "('bad_records_logger_enable'='true','IS_EMPTY_DATA_BAD_RECORD'='false'," +
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
          "('bad_records_logger_enable'='true','IS_EMPTY_DATA_BAD_RECORD'='false' ," +
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
          + "('bad_records_logger_enable'='false','IS_EMPTY_DATA_BAD_RECORD'='false'," +
          " 'DELIMITER'= ',', 'QUOTECHAR'= '\"')");


    } catch {
      case x: Throwable => CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    }
  }

  def loadEmptyComplexData(isEmptyBadRecord: Boolean, badRecordsAction: String): Unit = {
    sql(s"LOAD DATA local inpath '" + resourcesPath +
        "/complextypeWithEmptyRecords.csv' INTO table complexcarbontable OPTIONS('DELIMITER'=','," +
        "'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId,ROMSize,ROMName," +
        "purchasedate,file,MAC,locationinfo,proddate,gamePointId,contractNumber,st,ar', " +
        "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':', " +
        s"'bad_records_logger_enable'='true','IS_EMPTY_DATA_BAD_RECORD'='$isEmptyBadRecord' ," +
        s"'bad_records_action'='$badRecordsAction')")
  }

  test("Test complex type with empty values and IS_EMPTY_DATA_BAD_RECORD property") {
    sql("DROP TABLE IF EXISTS complexcarbontable")
    sql("DROP TABLE IF EXISTS complexhivetable")
    sql(
      "create table complexcarbontable(deviceInformationId int, channelsId string, ROMSize " +
      "string, ROMName String, purchasedate string, file struct<school:array<string>, age:int>," +
      " MAC map<string, int>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, " +
      "ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>, " +
      "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId " +
      "double,contractNumber double, st struct<school:struct<a:string,b:int>, age:int>," +
      "ar array<array<string>>)  STORED AS carbondata")
    val exception = intercept[Exception] ( loadEmptyComplexData(true, "fail"))
    assert(exception.getMessage.contains(
        "The value with column name file.age and column data type INT is not a valid INT type."))
    loadEmptyComplexData(true, "ignore")
    checkAnswer(sql("select count(*) from complexcarbontable"), Seq(Row(0)))
    loadEmptyComplexData(false, "ignore")
    sql(
      "create table complexhivetable(deviceInformationId int, channelsId " +
      "string, ROMSize string, ROMName String, purchasedate string, file " +
      "struct<school:array<string>, age:int>, MAC map<string, int>, " +
      "locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, ActiveProvince:string, " +
      "Activecity:string, ActiveDistrict:string, " +
      "ActiveStreet:string>>, proddate struct<productionDate:string," +
      "activeDeactivedate:array<string>>, gamePointId double,contractNumber double," +
      "st struct<school:struct<a:string,b:int>, age:int>,ar array<array<string>>) row format " +
      "delimited fields terminated by ',' collection items terminated by '$' map keys terminated " +
      "by ':'")
    sql("LOAD DATA local inpath '" + resourcesPath +
        "/complextypeWithEmptyRecords.csv' INTO table complexhivetable")
    checkAnswer(sql("select count(*) from complexcarbontable"), Seq(Row(3)))
    checkAnswer(sql("select * from complexcarbontable"),
      sql("select * from complexhivetable"))
    sql("DROP TABLE IF EXISTS complexcarbontable")
  }

   test("select count(*) from empty_timestamp") {
    checkAnswer(
      sql("select count(*) from empty_timestamp"),
      Seq(Row(2)
      )
    )
  }

  test("select count(*) from emptyColumnValues") {
    checkAnswer(
      sql("select count(*) from emptyColumnValues"),
      Seq(Row(7)
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
    sql("select count(*) from empty_timestamp_false").collect()
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
          + "('IS_EMPTY_DATA_BAD_RECORD'='xyz', 'DELIMITER'= ',', 'QUOTECHAR'= '\"')");
    } catch {
      case ex: Exception =>
        assert("option IS_EMPTY_DATA_BAD_RECORD can have option either true or false"
          .equals(ex.getMessage))
    }
  }

  test("test load multiple loads- pne with valid record and one with invalid") {
    sql("create table bigtab (val string, bal int) STORED AS carbondata")
    intercept[Exception] {
      sql(s"load data  inpath '$resourcesPath/badrecords/bigtabbad.csv' into table bigtab " +
        "options('DELIMITER'=',','QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FAIL'," +
        "'FILEHEADER'='val,bal')")
    }
    sql(s"load data  inpath '$resourcesPath/badrecords/bigtab.csv' into table bigtab " +
        "options('DELIMITER'=',','QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FAIL'," +
        "'FILEHEADER'='val,bal')")
    checkAnswer(sql("select count(*) from bigtab"), Seq(Row(1)))
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table IF EXISTS emptyColumnValues")
    sql("drop table IF EXISTS emptyColumnValues_false")
    sql("drop table IF EXISTS empty_timestamp")
    sql("drop table IF EXISTS empty_timestamp_false")
    sql("drop table IF EXISTS dataloadOptionTests")
    sql("drop table IF EXISTS bigtab")
  }
}
