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

package org.apache.carbondata.spark.testsuite.allqueries

import java.io.File

import antlr.NoViableAltException
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.{NonRunningTests, QueryTest}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.processing.etl.DataLoadingException
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException

/**
 * Test Class for all queries on multiple datatypes
 * Manohar
 */
class AllDataTypesTestCase4 extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {

    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    CarbonProperties.getInstance().addProperty("carbon.direct.surrogate", "false")
    try {
      sql("drop table if exists table_structure")
      sql("drop table if exists table_structure_hive")
      sql(
        "Create table table_structure (a0 STRING,a STRING,b0 INT)  stored by 'org.apache" +
        ".carbondata.format'")
      sql("LOAD DATA LOCAL INPATH '" + currentDirectory +
          "/src/test/resources/restructure_table.csv'" +
          " INTO TABLE table_structure OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '\"')")

      sql(
        "Create table table_structure_hive (a0 STRING,a STRING,b0 INT) row format delimited " +
        "fields terminated by ','")
      sql("LOAD DATA LOCAL INPATH '" + currentDirectory +
          "/src/test/resources/restructure_table.csv'" +
          " INTO TABLE table_structure_hive")

      sql("create schema myschema")
      sql("create schema myschema1")
      sql("create schema drug")
    } catch {
      case e: Exception => print("ERROR : " + e.getMessage)
    }


  }

  override def afterAll {
    try {
      sql("drop table table_structure")
      sql("drop table table_structure_hive")
      sql("drop schema myschema")
      sql("drop schema myschema1")
      sql("drop schema drug")
    } catch {
      case e: Exception => print("ERROR : " + e.getMessage)
    }

  }

  def dataFrameToSeq(dataframe: DataFrame): Seq[Row] = {
    dataframe.collect.toSeq
  }

  //TC_898
  test("select * from table_structure") {
    pending
    checkAnswer(sql("select * from table_structure"),
      dataFrameToSeq(sql("select * from table_structure_hive"))
    )

  }

  //TC_1059
  test("TC_1059") {
    sql("drop table if exists table1")

    sql(
      "create table  table1 (imei string,deviceInformationId INT,MAC string," +
      "deviceColor string,device_backColor string,modelId string,marketName string,AMSize " +
      "string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate " +
      "timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, " +
      "channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince " +
      "string, deliveryCity string,deliveryDistrict string, deliveryStreet string, " +
      "oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry " +
      "string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet " +
      "string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, " +
      "Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, " +
      "Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber " +
      "string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, " +
      "Latest_YEAR INT, Latest_MONTH INT, Latest_DAY INT, Latest_HOUR string, " +
      "Latest_areaId string, Latest_country string, Latest_province string, Latest_city string," +
      " Latest_district string, Latest_street string, Latest_releaseId string, " +
      "Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, " +
      "Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, " +
      "Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, " +
      "Latest_phonePADPartitionedVersions string, Latest_operatorId string, " +
      "gamePointDescription string,gamePointId decimal,contractNumber decimal) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table1")
  }

  //TC_1060
  test("TC_1060") {
    sql("drop table if exists myschema.table2")
    sql(
      "create table  myschema.table2 (imei string,deviceInformationId INT,MAC string," +
      "deviceColor string,device_backColor string,modelId string,marketName string,AMSize " +
      "string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate " +
      "timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, " +
      "channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince " +
      "string, deliveryCity string,deliveryDistrict string, deliveryStreet string, " +
      "oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry " +
      "string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet " +
      "string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, " +
      "Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, " +
      "Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber " +
      "string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, " +
      "Latest_YEAR INT, Latest_MONTH INT, Latest_DAY INT, Latest_HOUR string, " +
      "Latest_areaId string, Latest_country string, Latest_province string, Latest_city string," +
      " Latest_district string, Latest_street string, Latest_releaseId string, " +
      "Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, " +
      "Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, " +
      "Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, " +
      "Latest_phonePADPartitionedVersions string, Latest_operatorId string, " +
      "gamePointDescription string,gamePointId decimal,contractNumber decimal) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table2")
  }

  //TC_1061
  test("TC_1061") {
    sql("drop table if exists table3")
    sql(
      "CREATE table table3  (AMSize STRING,Latest_Day INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table3")
  }

  //TC_1062
  test("TC_1062") {
    sql("drop table if exists myschema.table4")
    sql(
      "CREATE table myschema.table4  (AMSize STRING,Latest_Day INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table4")
  }

  //TC_1065
  test("TC_1065") {
    sql("drop table if exists table7")
    sql(
      "CREATE table table7  (AMSize STRING,Latest_Day INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table7")
  }

  //TC_1066
  test("TC_1066") {
    sql("drop table if exists myschema.table8")
    sql(
      "CREATE table myschema.table8  (AMSize STRING,Latest_Day INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table8")
  }

  //TC_1069
  test("TC_1069") {
    sql("drop table if exists table27")
    sql(
      "CREATE table table27  (AMSize STRING) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table27")
  }

  //TC_1073
  test("TC_1073") {
    intercept[MalformedCarbonCommandException] {
      sql(
        "CREATE table table31 (Latest_Day INT) " +
        "stored by 'org.apache.carbondata.format'"
      )
    }
  }

  //TC_1074
  test("TC_1074") {
    intercept[AnalysisException] {
      sql(
        "CREATE table table32 (Latest_Day INT as col2)  " +
        "stored by 'org.apache.carbondata.format'"
      )
    }
  }

  //TC_1075
  test("TC_1075") {
    sql("drop table if exists table33")
    sql(
      "create table table33  (imei string,deviceInformationId INT,MAC string," +
      "deviceColor string, device_backColor string,modelId string, marketName string, AMSize " +
      "string, ROMSize string, CUPAudit string, CPIClocked string, series string, " +
      "productionDate string, bomCode string, internalModels string, deliveryTime string, " +
      "channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, " +
      "deliveryProvince  string, deliveryCity string,deliveryDistrict string, deliveryStreet " +
      "string, oxSingleNumber string, contractNumber decimal, ActiveCheckTime string, " +
      "ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, " +
      "ActiveDistrict  string, ActiveStreet string, ActiveOperatorId string, Active_releaseId " +
      "string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber " +
      "string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer " +
      "string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, " +
      "Active_phonePADPartitionedVersions string, Latest_YEAR  INT, Latest_MONTH INT, " +
      "Latest_DAY INT, Latest_HOUR string, Latest_areaId string, Latest_country string, " +
      "Latest_province string, Latest_city string, Latest_district string, Latest_street " +
      "string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion " +
      "string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion " +
      "string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
      "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
      "Latest_operatorId string, gamePointDescription string,gamePointId decimal) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table33")
  }

  //TC_1076
  test("TC_1076") {
    sql("drop table if exists table34")
    sql(
      "CREATE table table34  (AMSize STRING,deviceInformationId STRING, " +
      "Latest_Day INT) stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table34")
  }

  //TC_1077
  test("TC_1077") {
    sql("drop table if exists myschema.table35")
    sql(
      "CREATE table myschema.table35  (AMSize STRING,deviceInformationId STRING," +
      " Latest_Day INT) stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table35")
  }

  //TC_1082
  test("TC_1082") {
    sql("drop table if exists myschema.table40")
    intercept[MalformedCarbonCommandException] {
      sql(
        "CREATE table myschema.table40  (bomCode INT,Latest_Day INT) " +
        "stored by 'org.apache.carbondata.format'"
      )
    }
  }

  //TC_1083
  test("TC_1083") {
    sql(
      "CREATE table table41  (bomCode INT,Latest_Day INT)  " +
      "stored by 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='bomCode')"
    )
    sql("drop table table41")
  }

  //TC_1086
  test("TC_1086") {
      sql("drop table table44")
  }

  //TC_1087
  test("TC_1087") {
    sql("drop table if exists myschema.table45")
    sql(
      "CREATE table myschema.table45  (AMSize STRING,Latest_Day decimal) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table45")
  }

  //TC_1088
  test("TC_1088") {
    sql("drop table if exists table46")
    sql(
      "CREATE table table46  (AMSize STRING,Latest_Day decimal) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table46")
  }

  //TC_1091
  test("TC_1091") {
      sql("drop table table49")
  }

  //TC_1092
  test("TC_1092") {
    sql("drop table if exists myschema.table50")
    sql(
      "CREATE table myschema.table50  (AMSize decimal,Latest_Day decimal) " +
      "stored by 'org.apache.carbondata.format'  TBLPROPERTIES('DICTIONARY_INCLUDE'='AMSize')"
    )
    sql("drop table myschema.table50")
  }

  //TC_1093
  test("TC_1093") {
    sql("drop table if exists table51")
    intercept[MalformedCarbonCommandException] {
      sql(
        "CREATE table table51  (AMSize decimal,Latest_Day decimal) " +
        "stored by 'org.apache.carbondata.format'"
      )
    }
  }

  //TC_1098
  test("TC_1098") {
    sql("drop table if exists table106")
    sql(
      "CREATE table table106  (AMSize STRING,Latest_Day INT)  " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table106")
  }

  //TC_1111
  test("TC_1111") {
    intercept[AnalysisException] {
      sql(
        "create table table119  (,) stored by 'org.apache.carbondata.format'"
      )
    }

  }

  //DTS2015103007487
  test("DTS2015103007487") {
    sql("drop table if exists abc.babu8")
    sql("drop schema if exists abc")
    sql("create schema abc")
    sql("create table abc.babu8 (a string,b INT)")
    sql("drop table  abc.babu8")
    sql("drop schema abc")
  }

  //TC_1114
  test("TC_1114") {
    sql("drop table if exists table1_drop")
    sql(
      "create table  table1_drop (imei string,deviceInformationId INT,MAC string," +
      "deviceColor string,device_backColor string,modelId string,marketName string,AMSize " +
      "string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate " +
      "timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, " +
      "channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince " +
      "string, deliveryCity string,deliveryDistrict string, deliveryStreet string, " +
      "oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry " +
      "string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet " +
      "string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, " +
      "Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, " +
      "Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber " +
      "string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, " +
      "Latest_YEAR INT, Latest_MONTH INT, Latest_DAY INT, Latest_HOUR string, " +
      "Latest_areaId string, Latest_country string, Latest_province string, Latest_city string," +
      " Latest_district string, Latest_street string, Latest_releaseId string, " +
      "Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, " +
      "Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, " +
      "Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, " +
      "Latest_phonePADPartitionedVersions string, Latest_operatorId string, " +
      "gamePointDescription string,gamePointId decimal,contractNumber decimal) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table1_drop")
  }

  //TC_1115
  test("TC_1115") {
    sql("drop table if exists myschema.table2_drop")
    sql(
      "create table  myschema.table2_drop (imei string,deviceInformationId INT,MAC " +
      "string,deviceColor string,device_backColor string,modelId string,marketName string," +
      "AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string," +
      "productionDate timestamp,bomCode string,internalModels string, deliveryTime string, " +
      "channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, " +
      "deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet " +
      "string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, " +
      "ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, " +
      "ActiveStreet string, ActiveOperatorId string, Active_releaseId string, " +
      "Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, " +
      "Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string," +
      "Active_webTypeDataVerNumber string, Active_operatorsVersion string, " +
      "Active_phonePADPartitionedVersions string, Latest_YEAR INT, Latest_MONTH INT, " +
      "Latest_DAY INT, Latest_HOUR string, Latest_areaId string, Latest_country string, " +
      "Latest_province string, Latest_city string, Latest_district string, Latest_street " +
      "string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion " +
      "string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion " +
      "string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
      "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
      "Latest_operatorId string, gamePointDescription string,gamePointId decimal," +
      "contractNumber decimal) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table2_drop")
  }

  //TC_1117
  test("TC_1117") {
    sql("drop table if exists myschema.table4_drop")
    sql(
      "CREATE table myschema.table4_drop  (AMSize STRING,Latest_Day INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table4_drop")
  }


  //TC_1124
  test("TC_1124") {
    sql("drop table if exists table27_drop")
    sql(
      "CREATE table table27_drop  (AMSize STRING) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table27_drop")
  }

  //TC_1128
  test("TC_1128") {
    sql("drop table if exists table33_drop")
    sql(
      "create table table33_drop  (imei string,deviceInformationId INT,MAC string," +
      "deviceColor string, device_backColor string,modelId string, marketName string, AMSize " +
      "string, ROMSize string, CUPAudit string, CPIClocked string, series string, " +
      "productionDate string, bomCode string, internalModels string, deliveryTime string, " +
      "channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, " +
      "deliveryProvince  string, deliveryCity string,deliveryDistrict string, deliveryStreet " +
      "string, oxSingleNumber string, contractNumber decimal, ActiveCheckTime string, " +
      "ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, " +
      "ActiveDistrict  string, ActiveStreet string, ActiveOperatorId string, Active_releaseId " +
      "string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber " +
      "string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer " +
      "string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, " +
      "Active_phonePADPartitionedVersions string, Latest_YEAR  INT, Latest_MONTH INT, " +
      "Latest_DAY INT, Latest_HOUR string, Latest_areaId string, Latest_country string, " +
      "Latest_province string, Latest_city string, Latest_district string, Latest_street " +
      "string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion " +
      "string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion " +
      "string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
      "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
      "Latest_operatorId string, gamePointDescription string,gamePointId decimal) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table33_drop")
  }

  //TC_1129
  test("TC_1129") {
    sql("drop table if exists table34_drop")
    sql(
      "CREATE table table34_drop  (AMSize STRING,deviceInformationId STRING," +
      "Latest_Day INT) stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table34_drop")
  }

  //TC_1135
  test("TC_1135") {
    sql("drop table if exists myschema.table45_drop")
    sql(
      "CREATE table myschema.table45_drop  (AMSize STRING,Latest_Day decimal) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table45_drop")
  }

  //TC_1136
  test("TC_1136") {
    sql("drop table if exists table46_drop")
    sql(
      "CREATE table table46_drop  (AMSize STRING,Latest_Day decimal)  " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table46_drop")
  }

  //TC_1139
  test("TC_1139") {
    sql("drop table if exists myschema.table50_drop")
    sql(
      "CREATE table myschema.table50_drop  (AMSize decimal,Latest_Day decimal) " +
      "stored by 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='AMSize," +
      "Latest_Day')"
    )
    sql("drop table myschema.table50_drop")
  }

  //TC_1140
  test("TC_1140") {
    sql("drop table if exists table51_drop")
    intercept[MalformedCarbonCommandException] {
      sql(
        "CREATE table table51_drop  (AMSize decimal,Latest_Day decimal)  " +
        "stored by 'org.apache.carbondata.format'"
      )
    }
  }



  //TC_1157
  test("TC_1157") {
    sql("drop table if exists vardhan12")
    sql(
      "create table vardhan12 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,gamePointId decimal," +
      "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    intercept[Exception] {
      sql(
        "LOAD DATA LOCAL INPATH  '" + currentDirectory +
        "/src/test/resources/TestData1.csv' INTO table vardhan12 " +
        "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,AMSize,channelsId," +
        "ActiveCountry,Activecity')"
      )
      sql("drop table vardhan12")
    }
  }

  //TC_1158
  test("TC_1158") {
    sql("drop table if exists vardhan13")
    sql(
      "create table vardhan13 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,gamePointId decimal," +
      "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    intercept[Exception] {
      sql(
        "LOAD DATA LOCAL INPATH  '" + currentDirectory +
        "/src/test/resources/TestData1.csv' INTO table vardhan13 " +
        "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'deviceInformationId," +
        "gamePointId')"
      )
      sql("drop table vardhan13")
    }
  }

  //TC_1159
  test("TC_1159") {
    sql("drop table if exists vardhan3")
    sql(
      "create table vardhan3 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,gamePointId decimal," +
      "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    intercept[Exception] {
      sql(
        "LOAD DATA LOCAL INPATH  '" + currentDirectory +
        "/src/test/resources/TestData1.csv' INTO table vardhan3 " +
        "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'deviceInformationId,AMSize," +
        "ActiveCountry,Activecity,gamePointId')"
      )
      sql("drop table vardhan3")
    }
  }

  //TC_1169
  test("TC_1169") {
    sql("drop table if exists vardhan100")
    sql("drop table if exists vardhan100_hive")
    sql(
      "create table vardhan100 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,gamePointId decimal," +
      "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH  '" + currentDirectory +
      "/src/test/resources/TestData1.csv' INTO table vardhan100 " +
      "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"','FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize,channelsId,ActiveCountry,Activecity,gamePointId')"
    )

    sql(
      "create table vardhan100_hive (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,gamePointId decimal," +
      "deviceInformationId INT) row format delimited fields terminated by ','"
    )
    sql(
      "LOAD DATA LOCAL INPATH  '" + currentDirectory +
      "/src/test/resources/TestData1.csv' INTO table vardhan100_hive ")

    checkAnswer(sql("select count(*) from vardhan100 "),
      dataFrameToSeq(sql("select count(*) from vardhan100_hive")))

    sql("drop table vardhan100")
    sql("drop table vardhan100_hive")
  }

  //TC_1178
  test("TC_1178") {
    sql("drop table if exists vardhan13")
    sql(
      "create table vardhan13 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,gamePointId decimal," +
      "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    intercept[Exception] {
      sql(
        "LOAD DATA LOCAL INPATH  '" + currentDirectory +
        "/src/test/resources/TestData1.csv' INTO table vardhan13 " +
        "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'deviceInformationId," +
        "gamePointId')"
      )
    }
  }

  //TC_1179
  test("TC_1179") {
    sql("drop table if exists vardhan3")
    sql(
      "create table vardhan3 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,gamePointId decimal," +
      "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    intercept[Exception] {
      sql(
        "LOAD DATA LOCAL INPATH  '" + currentDirectory +
        "/src/test/resources/TestData1.csv' INTO table vardhan3 " +
        "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'deviceInformationId,AMSize," +
        "ActiveCountry,Activecity,gamePointId')"
      )
      checkAnswer(
        sql("select count(*) from vardhan3"),
        Seq()
      )
      sql("drop table vardhan3")
    }
  }

  //TC_1189
  test("TC_1189") {
    sql("drop table if exists vardhan100")
    sql("drop table if exists vardhan100_hive")
    sql(
      "create table vardhan100 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,gamePointId decimal," +
      "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH  '" + currentDirectory +
      "/src/test/resources/TestData1.csv' INTO table vardhan100 " +
      "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"','FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize,channelsId,ActiveCountry,Activecity,gamePointId')"
    )

    sql(
      "create table vardhan100_hive (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,gamePointId decimal," +
      "deviceInformationId INT) row format delimited fields terminated by ','"
    )
    sql(
      "LOAD DATA LOCAL INPATH  '" + currentDirectory +
      "/src/test/resources/TestData1.csv' INTO table vardhan100_hive ")

    checkAnswer(
      sql("select count(*) from vardhan100"),
      sql("select count(*) from vardhan100_hive")
    )
    sql("drop table vardhan100")
    sql("drop table vardhan100_hive")
  }

  //TC_1191
  test("TC_1191") {
    sql("drop table if exists vardhan500")
    sql("drop table if exists vardhan500_hive")

    sql(
      "create table vardhan500 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string, productionDate TIMESTAMP,gamePointId" +
      " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd/MM/yyyy HH:mm")
    sql(
      "LOAD DATA LOCAL INPATH  '" + currentDirectory +
      "/src/test/resources/TestData6.csv' INTO table vardhan500 " +
      "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
    )

    sql(
      "create table vardhan500_hive (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string, productionDate TIMESTAMP,gamePointId" +
      " decimal,deviceInformationId INT) row format delimited fields terminated by ','"
    )
    sql(
      "LOAD DATA LOCAL INPATH  '" + currentDirectory +
      "/src/test/resources/TestData6.csv' INTO table vardhan500_hive "
    )

    checkAnswer(
      sql("select count(*) from vardhan500"),
      sql("select count(*) from vardhan500_hive"))

    sql("drop table vardhan500")
    sql("drop table vardhan500_hive")

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
      )
  }

  //TC_1192
  test("TC_1192") {
    sql("drop table if exists vardhan1000")
    sql("drop table if exists testhive1000")
    sql(
      "create table vardhan1000 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,gamePointId decimal," +
      "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )

    sql(
      "create table testhive1000 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,gamePointId decimal," +
      "deviceInformationId INT) row format delimited fields terminated by ','"
    )

    sql(
      "LOAD DATA LOCAL INPATH  '" + currentDirectory +
      "/src/test/resources/TestData5.csv' INTO table testhive1000 "
    )
    sql(
      "LOAD DATA LOCAL INPATH  '" + currentDirectory +
      "/src/test/resources/TestData5.csv' INTO table vardhan1000 " +
      "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize,channelsId,ActiveCountry,Activecity,gamePointId')"
    )
    checkAnswer(
      sql("select count(*) from vardhan1000"),
      sql("select count(*) from testhive1000")
    )
    sql("drop table vardhan1000")
    sql("drop table testhive1000")
  }



  //TC_1194
  test("TC_1194") {
    sql("drop table if exists vardhan16")
    sql("drop table if exists vardhan16_hive")

    sql(
      "create table vardhan16 (AMSize STRING, deviceInformationId INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH  '" + currentDirectory +
      "/src/test/resources/TestData1.csv' INTO table vardhan16 " +
      "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize,channelsId,ActiveCountry,Activecity,gamePointId')"
    )

    sql(
      "create table vardhan16_hive (AMSize STRING, deviceInformationId INT) " +
      "row format delimited fields terminated by ','"
    )
    sql(
      "LOAD DATA LOCAL INPATH  '" + currentDirectory +
      "/src/test/resources/TestData1.csv' INTO table vardhan16_hive ")

    checkAnswer(
      sql("select count(*) from vardhan16"),
      sql("select count(*) from vardhan16_hive"))

    sql("drop table vardhan16")
    sql("drop table vardhan16_hive")
  }

  //TC_1195
  test("TC_1195") {
    sql("drop table if exists myschema.vardhan17")
    sql("drop table if exists myschema.vardhan17_hive")

    sql(
      "create table myschema.vardhan17  (AMSize STRING,deviceInformationId " +
      "INT) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH  '" + currentDirectory +
      "/src/test/resources/TestData1.csv' INTO table myschema.vardhan17" +
      " " +
      "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize,channelsId,ActiveCountry,Activecity,gamePointId')"
    )

    sql(
      "create table myschema.vardhan17_hive  (AMSize STRING,deviceInformationId " +
      "INT) row format delimited fields terminated by ','"
    )
    sql(
      "LOAD DATA LOCAL INPATH  '" + currentDirectory +
      "/src/test/resources/TestData1.csv' INTO table myschema.vardhan17_hive")

    checkAnswer(
      sql("select count(*)  from myschema.vardhan17"),
      sql("select count(*)  from myschema.vardhan17_hive")
    )
    sql("drop table myschema.vardhan17")
    sql("drop table myschema.vardhan17_hive")
  }

  //DTS2015111808892
  test("DTS2015111808892") {
    pending
    sql("drop table if exists table_restructure")
    sql("drop table if exists table_restructure_hive")

    sql(
      "CREATE table table_restructure  (a0 STRING,b0 INT)  " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/restructure_table.csv' INTO table " +
      "table_restructure" +
      " OPTIONS('DELIMITER'= ',', 'QUOTECHAR'=  '\"')"
    )


    sql(
      "CREATE table table_restructure_hive  (a0 STRING,b0 INT)  " +
      "row format delimited fields terminated by ','"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/restructure_table.csv' INTO table " +
      "table_restructure_hive"
    )

    checkAnswer(
      sql("select count(*)  from table_restructure"),
      sql("select count(*)  from table_restructure_hive"))

    sql("drop table table_restructure")
    sql("drop table table_restructure_hive")
  }

  //DTS2015112006803_01
  test("DTS2015112006803_01") {
    pending
    sql("drop table if exists incloading_DTS2015112006803_01")
    sql("drop table if exists incloading_DTS2015112006803_01_hive")

    sql(
      "CREATE table incloading_DTS2015112006803_01  (a0 STRING,b0 INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/restructure_table.csv' INTO table " +
      "incloading_DTS2015112006803_01 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'=  '\"')"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/restructure_table.csv' INTO table " +
      "incloading_DTS2015112006803_01 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'=  '\"')"
    )


    sql(
      "CREATE table incloading_DTS2015112006803_01_hive  (a0 STRING,b0 INT) " +
      "row format delimited fields terminated by ','"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/restructure_table.csv' INTO table " +
      "incloading_DTS2015112006803_01_hive "
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/restructure_table.csv' INTO table " +
      "incloading_DTS2015112006803_01_hive "
    )


    checkAnswer(
      sql("select count(*) from incloading_DTS2015112006803_01"),
      sql("select count(*) from incloading_DTS2015112006803_01_hive")
    )

    sql("drop table incloading_DTS2015112006803_01")
    sql("drop table incloading_DTS2015112006803_01_hive")
  }


  //DTS2015111810813
  test("DTS2015111810813", NonRunningTests) {
    pending
    sql("drop table if exists single")
    sql("drop table if exists single_hive")

    sql(
      "create table single (imei string,deviceInformationId INT,mac string," +
      "productdate timestamp,updatetime timestamp,gamePointId decimal,contractNumber " +
      "decimal) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/vmallFact_headr.csv' INTO table single " +
      "OPTIONS('DELIMITER'= '\001', 'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "mac,productdate,updatetime,gamePointId,contractNumber')"
    )

    sql(
      "create table single_hive (imei string,deviceInformationId INT,mac string," +
      "productdate timestamp,updatetime timestamp,gamePointId decimal,contractNumber " +
      "decimal) row format delimited fields terminated by ','"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/vmallFact_headr.csv' INTO table single_hive ")

    checkAnswer(
      sql("select count(*) from single"),
      sql("select count(*) from single_hive")
    )
    sql("drop table single")
    sql("drop table single_hive")
  }

  //DTS2015101504861
  test("DTS2015101504861", NonRunningTests) {
    sql("drop table if exists vard970")
    sql("drop table if exists vard970_hive")

    sql(
      "create table vard970 (imei string,productionDate timestamp,AMSize string," +
      "channelsId string,ActiveCountry string, Activecity string,gamePointId decimal," +
      "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/TestData2.csv' INTO table vard970 OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,productionDate," +
      "deviceInformationId," +
      "AMSize,channelsId,ActiveCountry,Activecity,gamePointId')"
    )

    sql(
      "create table vard970_hive (imei string,productionDate timestamp,AMSize string," +
      "channelsId string,ActiveCountry string, Activecity string,gamePointId decimal," +
      "deviceInformationId INT) row format delimited fields terminated by ','"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/TestData2.csv' INTO table vard970_hive")

    checkAnswer(
      sql("select imei from vard970 where productionDate='2015-07-06 12:07:00'"),
      sql("select imei from vard970_hive where productionDate='2015-07-06 12:07:00'"))

    sql("drop table vard970")
    sql("drop table vard970_hive")
  }

  //TC_1326
  test("TC_1326", NonRunningTests) {
    sql("drop table if exists vardhanincomp")
    sql("drop table if exists vardhanincomp_hive")

    sql(
      "create table vardhanincomp (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,gamePointId decimal," +
      "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/TestData4.csv' INTO table vardhanincomp " +
      "OPTIONS" +
      "('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize," +
      "channelsId," +
      "ActiveCountry,Activecity,gamePointId')"
    )

    sql(
      "create table vardhanincomp_hive (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,gamePointId decimal," +
      "deviceInformationId INT) row format delimited fields terminated by ','"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/TestData4.csv' INTO table vardhanincomp_hive ")

    checkAnswer(
      sql("select channelsId from vardhanincomp order by imei ASC limit 0"),
      sql("select channelsId from vardhanincomp_hive order by imei ASC limit 0")
    )
    sql("drop table vardhanincomp")
    sql("drop table vardhanincomp")
  }


  //DTS2015120304016
  test("DTS2015120304016") {
    pending
    sql("drop table if exists incloading1")
    sql("drop table if exists incloading1_hive")

    sql(
      "CREATE table incloading1  (a0 STRING,b0 INT) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/restructure_table.csv' INTO table incloading1" +
      " " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'=  '\"')"
    )

    sql(
      "CREATE table incloading1_hive  (a0 STRING,b0 INT) row format delimited fields terminated " +
      "by ','"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/restructure_table.csv' INTO table incloading1_hive")

    checkAnswer(
      sql("select count(*) from incloading1"),
      sql("select count(*) from incloading1_hive"))

    sql("drop table incloading1")
    sql("drop table incloading1_hive")
  }

  //TC_1160
  test("TC_1160") {
    sql("drop table if exists vardhan4")
    sql(
      "create table vardhan4 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,gamePointId decimal," +
      "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    intercept[AnalysisException] {
      sql(
        "LOAD DATA '" + currentDirectory +
        "/src/test/resources/TestData1.csv' INTO table vardhan4 OPTIONS" +
        "('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize," +
        "channelsId,ActiveCountry,Activecity,gamePointId')"
      )
      sql("drop table vardhan4")
    }
  }

  //TC_1161
  test("TC_1161") {
    sql("drop table if exists vardhan5")

    sql(
      "create table vardhan5 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,gamePointId decimal," +
      "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    intercept[AnalysisException] {
      sql(
        "LOAD DATA LOCAL INPATH  INTO table vardhan5 OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"'," +
        " " +
        "'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity," +
        "gamePointId')"
      )
      sql("drop table vardhan5")
    }
  }

  //TC_1181
  test("TC_1181") {
    sql("drop table if exists vardhan5")

    sql(
      "create table vardhan5 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,gamePointId decimal," +
      "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    intercept[AnalysisException] {
      sql(
        "LOAD DATA LOCAL INPATH  INTO table vardhan5 OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"'," +
        " " +
        "'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity," +
        "gamePointId')"
      )
      sql("drop table vardhan5")
    }

  }

  //DTS2015111900020
  test("DTS2015111900020") {
    sql("drop table if exists testwithout_measure")
    sql(
      "CREATE table testwithout_measure  (AMSize STRING) " +
      "stored by 'org.apache.carbondata.format'"
    )
    intercept[DataLoadingException] {
      sql(
        "LOAD DATA LOCAL INPATH '" + currentDirectory +
        "/src/test/resources/create_table.csv' INTO table " +
        "testwithout_measure" +
        " OPTIONS('DELIMITER'= ',', 'QUOTECHAR'=  '\"')"
      )
      sql("drop table testwithout_measure")
    }
  }


  //TC_1255
  test("TC_1255") {
    sql("drop table if exists table10")

    sql(
      "create table table10 (imei string,AMSize string,channelsId string,ActiveCountry " +
      "string, Activecity string,gamePointId decimal,deviceInformationId INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    intercept[RuntimeException] {
      sql("show segments for table tabledoesnotexist")
      sql("drop table table10")
    }
  }

  //TC_1256
  test("TC_1256") {
    sql("drop table if exists table11")
    sql(
      "create table table11 (imei string,AMSize string,channelsId string,ActiveCountry " +
      "string, Activecity string,gamePointId decimal,deviceInformationId INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    intercept[AnalysisException] {
      sql("show segments for table11")
      sql("drop table table11")
    }
  }


  //TC_1292
  test("TC_1292") {
    sql("drop table if exists myschema1.table2")
    sql(
      "create table myschema1.table2  (imei string,deviceInformationId INT,MAC " +
      "string,deviceColor string, device_backColor string,modelId string, marketName string, " +
      "AMSize string, ROMSize string, CUPAudit string, CPIClocked string, series string, " +
      "productionDate string, bomCode string, internalModels string, deliveryTime string, " +
      "channelsId string, channelsName string , deliveryAreaId string, deliveryCountry " +
      "string, deliveryProvince  string, deliveryCity string,deliveryDistrict string, " +
      "deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId " +
      "string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict" +
      "  string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, " +
      "Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, " +
      "Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string," +
      "Active_webTypeDataVerNumber string, Active_operatorsVersion string, " +
      "Active_phonePADPartitionedVersions string, Latest_YEAR  INT, Latest_MONTH INT," +
      " Latest_DAY INT, Latest_HOUR string, Latest_areaId string, Latest_country string, " +
      "Latest_province string, Latest_city string, Latest_district string, Latest_street " +
      "string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion " +
      "string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion " +
      "string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
      "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
      "Latest_operatorId string, gamePointDescription string,gamePointId decimal," +
      "contractNumber decimal) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/100.csv' INTO table myschema1.table2 " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'=  '\"', 'FILEHEADER' = 'imei," +
      "deviceInformationId," +
      "MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit," +
      "CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId," +
      "channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity," +
      "deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime," +
      "ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet," +
      "ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion," +
      "Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer," +
      "Active_webTypeDataVerNumber,Active_operatorsVersion," +
      "Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR," +
      "Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district," +
      "Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion," +
      "Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer," +
      "Latest_webTypeDataVerNumber,Latest_operatorsVersion," +
      "Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')"
    )
    intercept[AnalysisException] {
      sql("delete load 0 from  table myschema1.table2")

      sql("drop table myschema1.table2")
    }
  }

  //TC_1307
  test("TC_1307") {
    sql("drop table if exists table17")

    sql(
      "create table table17 (imei string,AMSize string,channelsId string,ActiveCountry " +
      "string, Activecity string,gamePointId decimal,deviceInformationId INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH  '" + currentDirectory +
      "/src/test/resources/TestData1.csv' INTO table table17 OPTIONS" +
      "('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize," +
      "channelsId,ActiveCountry,Activecity,gamePointId')"
    )
    intercept[AnalysisException] {
      sql("delete load 1 from  table table17")
      sql("drop table table17")
    }
  }

  //TC_1310
  test("TC_1310") {
    sql("drop table if exists table20")

    sql(
      "create table table20 (imei string,AMSize string,channelsId string,ActiveCountry " +
      "string, Activecity string,gamePointId decimal,deviceInformationId INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    intercept[AnalysisException] {
      sql("delete load 1 from table table20")

      sql("drop table table20")
    }
  }

  //TC_1313
  test("TC_1313") {
    sql("drop table if exists vardhanretention2")

    sql(
      "create table vardhanretention2 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,productionDate timestamp,gamePointId" +
      " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/TestData3.csv' INTO table vardhanretention2" +
      " " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize," +
      "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/TestData3.csv' INTO table vardhanretention2" +
      " " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize," +
      "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
    )
    intercept[AnalysisException] {
      sql("delete load 2 from table vardhanretention2")

      sql("drop table vardhanretention2")
    }
  }

  //TC_1315
  test("TC_1315") {
    sql("drop table if exists vardhanretention4")
    sql(
      "create table vardhanretention4 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,productionDate timestamp,gamePointId" +
      " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    intercept[AnalysisException] {
      sql("delete load 0 from table vardhanretention4")

      sql("drop table vardhanretention4")
    }
  }

  //TC_1317
  test("TC_1317") {
    sql("drop table if exists vardhanretention14")

    sql(
      "create table vardhanretention14 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,productionDate timestamp,gamePointId" +
      " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/TestData3.csv' INTO table " +
      "vardhanretention14 " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize," +
      "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
    )
    intercept[AnalysisException] {
      sql("delete load 0 from table vardhanretention14")
      sql("delete load 0 from table vardhanretention14")

      sql("drop table vardhanretention14")
    }
  }

  //TC_1319
  test("TC_1319") {
    sql("drop table if exists vardhanretention15")
    sql(
      "create table vardhanretention15 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,productionDate timestamp,gamePointId" +
      " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/TestData3.csv' INTO table " +
      "vardhanretention15 " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize," +
      "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
    )
    intercept[AnalysisException] {
      sql("delete load 0,1 from table vardhanretention15")
      sql("drop table vardhanretention15")

    }
  }

  //TC_1320
  test("TC_1320") {
    sql("drop table if exists vardhanretention7")
    sql(
      "create table vardhanretention7 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,productionDate timestamp,gamePointId" +
      " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/TestData3.csv' INTO table vardhanretention7" +
      " " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize," +
      "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
    )
    intercept[AnalysisException] {
      sql("delete from table vardhanretention7 where productionDate before '2015-07-05 '")
    }
    sql("drop table vardhanretention7")

  }

  //TC_1321
  test("TC_1321") {
    sql("drop table if exists vardhanretention8")
    sql(
      "create table vardhanretention8 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,productionDate timestamp,gamePointId" +
      " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    intercept[AnalysisException] {
      sql("delete from table vardhanretention8 where productionDate before '2015-07-05 '")

      sql("drop table vardhanretention8")
    }
  }

  //TC_1323
  test("TC_1323") {
    sql("drop table if exists vardhanretention10")

    sql(
      "create table vardhanretention10 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,productionDate timestamp,gamePointId" +
      " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/TestData3.csv' INTO table " +
      "vardhanretention10 " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize," +
      "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
    )
    intercept[AnalysisException] {
      sql("delete from table vardhanretention10 where productionDate before ''")
      sql("drop table vardhanretention10")

    }
  }

  //TC_1324
  test("TC_1324") {
    sql("drop table if exists vardhanretention12")
    sql(
      "create table vardhanretention12 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,productionDate timestamp,gamePointId" +
      " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/TestData3.csv' INTO table " +
      "vardhanretention12 " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize," +
      "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
    )
    intercept[AnalysisException] {
      sql("delete from table vardhanretention12 where productionDate before '10-06-2015 12:07:28'")
      sql("drop table vardhanretention12")
    }
  }

  //TC_1325
  test("TC_1325") {
    sql("drop table if exists vardhanretention13")
    sql(
      "create table vardhanretention13 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,productionDate timestamp,gamePointId" +
      " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    CarbonProperties.getInstance().
      addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
      )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/TestData3.csv' INTO table " +
      "vardhanretention13 " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize," +
      "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
    )
    intercept[AnalysisException] {
      sql("delete from table vardhanretention13 where productionDate before '2013-13-13 12:07:28 '")
      sql("drop table vardhanretention13")
    }
  }

  //DTS2015112608945
  test("DTS2015112608945") {
    sql("drop table if exists babu_67")
    sql(
      "create table babu_67 (imei string,deviceInformationId INT,mac string," +
      "productdate timestamp,updatetime timestamp,gamePointId decimal," +
      "contractNumber decimal) stored by 'org.apache.carbondata.format'"
    )
    intercept[AnalysisException] {
      sql("delete from table babu_67 where productdate before '2015-01-10 19:59:00'")

      sql("drop table babu_67")
    }
  }

  //DTS2015102804722
  test("DTS2015102804722") {
    sql("drop table if exists table_restructure62")

    sql(
      "create table table_restructure62  (a0 STRING,b0 INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/restructure_table.csv' INTO table " +
      "table_restructure62 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'=  '\"', 'FILEHEADER'= 'a0,b0')"
    )
    intercept[AnalysisException] {
      sql("delete load 0 from table table_restructure62")
      sql("delete load 0 from table table_restructure62")

      sql("drop table table_restructure62")
    }
  }

  //DTS2015111403600
  test("DTS2015111403600") {
    sql("drop table if exists table_restructure64")
    sql(
      "create table table_restructure64  (a0 STRING,b0 INT)  " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/restructure_table.csv' INTO table " +
      "table_restructure64 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'=  '\"', 'FILEHEADER'= 'a0,b0')"
    )
    intercept[AnalysisException] {
      sql("delete load 0 from table table_restructure64")
      sql("clean files for table table_restructure64")
      sql("delete load 0 from table table_restructure64")
      sql("delete load 1 from table table_restructure64")
      sql("drop table table_restructure64")
    }
  }

  //DTS2015112703910
  test("DTS2015112703910") {
    sql("drop table if exists table_restructure65")
    sql(
      "CREATE table table_restructure65  (a0 STRING,b0 INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    intercept[AnalysisException] {
      sql("delete load 1 from table table_restructure65")
      sql("drop table table_restructure65")
    }
  }

  //DTS2015110209665
  test("DTS2015110209665") {
    sql("drop table if exists vardhanretention13")

    sql(
      "create table vardhanretention13 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,productionDate timestamp,gamePointId" +
      " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    intercept[AnalysisException] {
      sql("delete from table vardhanretention13 where productionDate before '2013-13-13 12:07:28 '")

      sql("drop table vardhanretention13")
    }
  }

  //TC_1346
  test("TC_1346") {
    sql("drop table if exists vardhanretention")

    sql(
      "create table vardhanretention (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,productionDate timestamp,gamePointId" +
      " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    intercept[AnalysisException] {
      sql("delete load 0 from table vardhanretention")
      sql("drop table vardhanretention")
    }
  }


}