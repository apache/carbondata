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
package org.apache.carbondata.spark.testsuite.insertQuery

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties


class InsertIntoNonCarbonTableTestCase extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("drop table if exists TCarbonSource")
    sql(
      "create table TCarbonSource (imei string,deviceInformationId int,MAC string,deviceColor " +
      "string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize " +
      "string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode " +
      "string,internalModels string, deliveryTime string, channelsId string, channelsName string " +
      ", deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity " +
      "string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, " +
      "ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, " +
      "Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, " +
      "Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, " +
      "Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, " +
      "Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion" +
      " string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, " +
      "Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country " +
      "string, Latest_province string, Latest_city string, Latest_district string, Latest_street " +
      "string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string," +
      " Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, " +
      "Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
      "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
      "Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber " +
      "BigInt) STORED AS carbondata")
    sql(
      s"LOAD DATA INPATH '$resourcesPath/100_olap.csv' INTO table TCarbonSource options " +
      "('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor," +
      "device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series," +
      "productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId," +
      "deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet," +
      "oxSingleNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity," +
      "ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion," +
      "Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion," +
      "Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion," +
      "Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR," +
      "Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street," +
      "Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber," +
      "Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer," +
      "Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions," +
      "Latest_operatorId,gamePointDescription,gamePointId,contractNumber', " +
      "'bad_records_logger_enable'='false','bad_records_action'='FORCE')")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION, "true")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
  }

  test("insert into hive") {
    sql("drop table if exists thive2")
    sql(
      "create table thive2 row format delimited fields terminated by '\017' stored as textfile as" +
      " select imei,deviceInformationId,MAC from TCarbonSource")
    checkAnswer(
      sql(
        "select imei,deviceInformationId,MAC from TCarbonSource order by imei, " +
        "deviceInformationId,MAC"),
      sql("select imei,deviceInformationId,MAC from thive2 order by imei,deviceInformationId,MAC")
    )
    sql("drop table thive2")
  }
  test("insert into parquet") {
    sql("drop table if exists tparquet")
    sql("create table tparquet(imei string,deviceInformationId int) STORED AS PARQUET")
    sql("insert into tparquet select imei,deviceInformationId from TCarbonSource")
    checkAnswer(
      sql("select imei,deviceInformationId from TCarbonSource order by imei,deviceInformationId"),
      sql("select imei,deviceInformationId from tparquet order by imei,deviceInformationId")
    )
    sql("drop table tparquet")
  }
  test("insert into hive conditional") {
    sql("drop table if exists thive_cond")
    sql(
      "create table thive_cond row format delimited fields terminated by '\017' stored as " +
      "textfile as SELECT(CASE WHEN imei IS NOT NULL THEN imei ELSE MAC END) AS temp FROM " +
      "TCarbonSource limit 10")
    checkAnswer(
      sql("select count(*) from thive_cond"),
      Seq(Row(10))
    )
    sql("drop table thive_cond")
  }

  test("jvm crash when insert data from datasource table to session table") {
    val spark = sqlContext.sparkSession
    import spark.implicits._

    import scala.util.Random
    val r = new Random()
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => (r.nextInt(100000), "name" + x % 8, "city" + x % 50, BigDecimal.apply(x % 60)))
      .toDF("ID", "name", "city", "age")
    spark.sql("DROP TABLE IF EXISTS personTable")
    spark.sql("DROP TABLE IF EXISTS test_table")

    df.write.format("carbon").saveAsTable("personTable")
    spark.sql("create table test_table(ID int, name string, city string, age decimal) STORED AS carbondata tblproperties('sort_columns'='ID')")
    spark.sql("insert into test_table select * from personTable")
    spark.sql("insert into test_table select * from personTable limit 2")

    assert(spark.sql("select * from test_table").count() == 12)
    spark.sql("DROP TABLE IF EXISTS personTable")
    spark.sql("DROP TABLE IF EXISTS test_table")
  }

  test("jvm crash when insert data from datasource table to datasource table") {
    val spark = sqlContext.sparkSession
    import spark.implicits._

    import scala.util.Random
    val r = new Random()
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => (r.nextInt(100000), "name" + x % 8, "city" + x % 50, BigDecimal.apply(x % 60)))
      .toDF("ID", "name", "city", "age")
    spark.sql("DROP TABLE IF EXISTS personTable")
    spark.sql("DROP TABLE IF EXISTS test_table")

    df.write.format("carbon").saveAsTable("personTable")
    spark.sql("create table test_table(ID int, name string, city string, age decimal) using carbon")
    spark.sql("insert into test_table select * from personTable")
    spark.sql("insert into test_table select * from personTable limit 2")

    assert(spark.sql("select * from test_table").count() == 12)
    spark.sql("DROP TABLE IF EXISTS personTable")
    spark.sql("DROP TABLE IF EXISTS test_table")
  }

  test("jvm crash when insert data from session table to datasource table") {
    val spark = sqlContext.sparkSession
    import spark.implicits._

    import scala.util.Random
    val r = new Random()
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => (r.nextInt(100000), "name" + x % 8, "city" + x % 50, BigDecimal.apply(x % 60)))
      .toDF("ID", "name", "city", "age")
    spark.sql("DROP TABLE IF EXISTS personTable")
    spark.sql("DROP TABLE IF EXISTS test_table")

    df.write
      .format("carbondata")
      .option("tableName", "personTable")
      .mode(SaveMode.Overwrite)
      .save()
    spark.sql("create table test_table(ID int, name string, city string, age decimal) using carbon")
    spark.sql("insert into test_table select * from personTable")
    spark.sql("insert into test_table select * from personTable limit 2")

    assert(spark.sql("select * from test_table").count() == 12)
    spark.sql("DROP TABLE IF EXISTS personTable")
    spark.sql("DROP TABLE IF EXISTS test_table")
  }

  override def afterAll {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION, CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION_DEFAULTVALUE)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_DEFAULT)
    sql("DROP TABLE IF EXISTS TCarbonSource")
  }
}