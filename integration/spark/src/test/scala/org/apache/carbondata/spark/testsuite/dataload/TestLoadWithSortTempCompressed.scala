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

package org.apache.carbondata.spark.testsuite.dataload

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestLoadWithSortTempCompressed extends QueryTest
  with BeforeAndAfterEach with BeforeAndAfterAll {
  val originOffHeapStatus: String = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT)
  val originSortTempCompressor: String = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.CARBON_SORT_TEMP_COMPRESSOR,
      CarbonCommonConstants.CARBON_SORT_TEMP_COMPRESSOR_DEFAULT)
  val simpleTable = "simpleTable"
  val complexCarbonTable = "complexCarbonTable"
  val complexHiveTable = "complexHiveTable"

  override def beforeEach(): Unit = {
    sql(s"drop table if exists $simpleTable")
    sql(s"drop table if exists $complexCarbonTable")
    sql(s"drop table if exists $complexHiveTable")
  }

  override def afterEach(): Unit = {
    sql(s"drop table if exists $simpleTable")
    sql(s"drop table if exists $complexCarbonTable")
    sql(s"drop table if exists $complexHiveTable")
  }


  override protected def beforeAll(): Unit = {
  }

  override def afterAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SORT_TEMP_COMPRESSOR,
        originSortTempCompressor)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, originOffHeapStatus)
  }

  private def testSimpleTable(): Unit = {
    val lineNum: Int = 10002
    val df = {
      import sqlContext.implicits._
      sqlContext.sparkContext.parallelize((1 to lineNum).reverse)
        .map(x => (s"a$x", s"b$x", s"c$x", 12.3 + x, x, System.currentTimeMillis(), s"d$x"))
        .toDF("c1", "c2", "c3", "c4", "c5", "c6", "c7")
    }

    df.write
      .format("carbondata")
      .option("tableName", simpleTable)
      .option("tempCSV", "true")
      .option("SORT_COLUMNS", "c1,c3")
      .save()

    checkAnswer(sql(s"select count(*) from $simpleTable"), Row(lineNum))
    checkAnswer(sql(s"select count(*) from $simpleTable where c5 > 5001"), Row(5001))
  }

  test("test data load for simple table without sort temp compressed and off-heap sort enabled") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SORT_TEMP_COMPRESSOR, "")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "true")
    testSimpleTable()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      originOffHeapStatus)
  }

  test("test data load for simple table without sort temp compressed and off-heap sort disabled") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SORT_TEMP_COMPRESSOR, "")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "false")
    testSimpleTable()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      originOffHeapStatus)
  }

  test("test data load for simple table with sort temp compressed with snappy" +
       " and off-heap sort enabled") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SORT_TEMP_COMPRESSOR,
      "SNAPPY")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "true")
    testSimpleTable()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      originOffHeapStatus)
  }

  test("test data load for simple table with sort temp compressed with snappy" +
       " and off-heap sort disabled") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SORT_TEMP_COMPRESSOR,
      "SNAPPY")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "false")
    testSimpleTable()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      originOffHeapStatus)
  }

  test("test data load for simple table with sort temp compressed with zstd" +
       " and off-heap sort enabled") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SORT_TEMP_COMPRESSOR,
      "ZSTD")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "true")
    testSimpleTable()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      originOffHeapStatus)
  }

  test("test data load for simple table with sort temp compressed with zstd" +
       " and off-heap sort disabled") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SORT_TEMP_COMPRESSOR,
      "ZSTD")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "false")
    testSimpleTable()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      originOffHeapStatus)
  }

  test("test data load for simple table with sort temp compressed with gzip" +
       " and off-heap sort enabled") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SORT_TEMP_COMPRESSOR,
      "gzip")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "true")
    testSimpleTable()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      originOffHeapStatus)
  }

  test("test data load for simple table with sort temp compressed with gzip" +
       " and off-heap sort disabled") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SORT_TEMP_COMPRESSOR,
      "gzip")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "false")
    testSimpleTable()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      originOffHeapStatus)
  }

  private def testComplexTable(): Unit = {
    // note: following tests are copied from `TestComplexTypeQuery`
    sql(
      s"create table $complexCarbonTable(deviceInformationId int, channelsId string, ROMSize " +
      "string, ROMName String, purchasedate string, mobile struct<imei:string, imsi:string>, MAC " +
      "array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, " +
      "ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>, " +
      "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId " +
      "double,contractNumber double)  STORED AS carbondata ")
    sql(s"LOAD DATA local inpath '$resourcesPath/complextypesample.csv' INTO table" +
        s" $complexCarbonTable  OPTIONS('DELIMITER'=',', " +
        "'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId,ROMSize,ROMName," +
        "purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber', " +
        "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")
    sql(
      s"create table $complexHiveTable(deviceInformationId int, channelsId string, ROMSize " +
      "string, ROMName String, purchasedate string,mobile struct<imei:string, imsi:string>,MAC " +
      "array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, " +
      "ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>, " +
      "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId " +
      "double,contractNumber double)row format delimited fields terminated by ',' collection " +
      "items terminated by '$' map keys terminated by ':'")
    sql(s"LOAD DATA local inpath '$resourcesPath/complextypesample.csv' INTO table" +
        s" $complexHiveTable")

    checkAnswer(sql(s"select * from $complexCarbonTable"), sql(s"select * from $complexHiveTable"))
    checkAnswer(sql(s"select MAC from $complexCarbonTable where MAC[0] = 'MAC1'"),
      sql(s"select MAC from $complexHiveTable where MAC[0] = 'MAC1'"))
    checkAnswer(sql(s"select mobile from $complexCarbonTable where mobile.imei like '1AA%'"),
      sql(s"select mobile from $complexHiveTable where mobile.imei like '1AA%'"))
    checkAnswer(sql(s"select locationinfo from $complexCarbonTable" +
                    " where locationinfo[0].ActiveAreaId > 2 AND locationinfo[0].ActiveAreaId < 7"),
      sql(s"select locationinfo from $complexHiveTable" +
          " where locationinfo[0].ActiveAreaId > 2 AND locationinfo[0].ActiveAreaId < 7"))
  }

  test("test data load for complex table with sort temp compressed with snappy" +
       " and off-heap sort enabled") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SORT_TEMP_COMPRESSOR,
      "SNAPPY")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "true")
    testComplexTable()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      originOffHeapStatus)
  }

  test("test data load for complex table with sort temp compressed with snappy" +
       " and off-heap sort disabled") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SORT_TEMP_COMPRESSOR,
      "SNAPPY")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "false")
    testComplexTable()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      originOffHeapStatus)
  }

  test("test data load for complex table with sort temp compressed with zstd" +
       " and off-heap sort enabled") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SORT_TEMP_COMPRESSOR,
      "ZSTD")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "true")
    testComplexTable()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      originOffHeapStatus)
  }

  test("test data load for complex table with sort temp compressed with zstd" +
       " and off-heap sort disabled") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SORT_TEMP_COMPRESSOR,
      "ZSTD")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "false")
    testComplexTable()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      originOffHeapStatus)
  }
}
