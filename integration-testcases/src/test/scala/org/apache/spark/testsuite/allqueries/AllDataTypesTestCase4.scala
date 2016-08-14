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

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.{NonRunningTests, QueryTest}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

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
      sql(
        "Create table table_structure (a0 STRING,a STRING,b0 INT)  stored by 'org.apache" +
        ".carbondata.format'")
      sql("LOAD DATA LOCAL INPATH '" + currentDirectory + "/src/test/resources/restructure_table.csv'" +
        " INTO TABLE table_structure OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '\"')")
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
      sql("drop schema myschema")
      sql("drop schema myschema1")
      sql("drop schema drug")
    } catch {
      case e: Exception => print("ERROR : " + e.getMessage)
    }

  }


  //TC_898
  test("select * from table_structure") {

    sql("select * from table_structure")

  }

  //TC_1059
  test("TC_1059") {
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
    sql(
      "CREATE table table3  (AMSize STRING,Latest_Day INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table3")
  }

  //TC_1062
  test("TC_1062") {
    sql(
      "CREATE table myschema.table4  (AMSize STRING,Latest_Day INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table4")
  }

  //TC_1065
  test("TC_1065") {
    sql(
      "CREATE table table7  (AMSize STRING,Latest_Day INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table7")
  }

  //TC_1066
  test("TC_1066") {
    sql(
      "CREATE table myschema.table8  (AMSize STRING,Latest_Day INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table8")
  }

  //TC_1069
  test("TC_1069") {
    sql(
      "CREATE table table27  (AMSize STRING) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table27")
  }

  //TC_1070
  test("TC_1070") {
    sql(
      "CREATE table myschema.table28  (AMSize STRING) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table28")
  }

  //TC_1073
  test("TC_1073") {
    try {
      sql(
        "CREATE table table31 (Latest_Day INT) " +
        "stored by 'org.apache.carbondata.format'"
      )
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1074
  test("TC_1074") {

    try {
      sql(
        "CREATE table table32 (Latest_Day INT as col2)  " +
        "stored by 'org.apache.carbondata.format'"
      )
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1075
  test("TC_1075") {
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
    sql(
      "CREATE table table34  (AMSize STRING,deviceInformationId STRING, " +
      "Latest_Day INT) stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table34")
  }

  //TC_1077
  test("TC_1077") {
    sql(
      "CREATE table myschema.table35  (AMSize STRING,deviceInformationId STRING," +
      " Latest_Day INT) stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table35")
  }

  //TC_1080
  test("TC_1080") {
    sql(
      "CREATE table myschema.table38  (AMSize STRING,Latest_Day INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table38")
  }

  //TC_1081
  test("TC_1081") {
    sql(
      "CREATE table table39  (AMSize STRING,Latest_Day INT)  " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table39")
  }

  //TC_1082
  test("TC_1082") {
    sql(
      "CREATE table myschema.table40  (bomCode INT,Latest_Day INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table40")
  }

  //TC_1083
  test("TC_1083") {
    sql(
      "CREATE table table41  (bomCode INT,Latest_Day INT)  " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table41")
  }

  //TC_1086
  test("TC_1086") {
    try {


      sql("drop table table44")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1087
  test("TC_1087") {
    sql(
      "CREATE table myschema.table45  (AMSize STRING,Latest_Day decimal) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table45")
  }

  //TC_1088
  test("TC_1088") {
    sql(
      "CREATE table table46  (AMSize STRING,Latest_Day decimal) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table46")
  }

  //TC_1091
  test("TC_1091") {
    try {

      sql("drop table table49")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1092
  test("TC_1092") {
    sql(
      "CREATE table myschema.table50  (AMSize decimal,Latest_Day decimal) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table50")
  }

  //TC_1093
  test("TC_1093") {
    sql(
      "CREATE table table51  (AMSize decimal,Latest_Day decimal) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table51")
  }

  //TC_1096
  test("TC_1096") {
    try {

      sql("drop table table54")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1097
  test("TC_1097") {
    try {

      sql("drop table table55")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1098
  test("TC_1098") {
    sql(
      "CREATE table table106  (AMSize STRING,Latest_Day INT)  " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table106")
  }

  //TC_1099
  test("TC_1099") {
    sql(
      "CREATE table table107  (AMSize STRING,Latest_Day INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table107")
  }

  //TC_1100
  test("TC_1100") {
    sql(
      "CREATE table table108  (AMSize STRING,Latest_Day INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table108")
  }

  //TC_1101
  test("TC_1101") {
    sql(
      "CREATE table table109  (AMSize STRING,Latest_Day INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table109")
  }

  //TC_1103
  test("TC_1103") {
    try {

      sql("drop table table111")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1104
  test("TC_1104") {
    try {


      sql("drop table table112")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }


  //TC_1107
  test("TC_1107") {

    try {

      sql("drop table table115")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }


  //TC_1110
  test("TC_1110") {
    sql(
      "create table table118  (AMSize STRING,Latest_Day INT)  " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table118")
  }

  //TC_1111
  test("TC_1111") {

    try {
      sql(
        "create table table119  (,) stored by 'org.apache.carbondata.format'"
      )
      sql("drop table table119")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }


  }

  //DTS2015103009506
  test("DTS2015103009506") {
    sql(
      "CREATE table table_restructure1  (a0 STRING,b0 INT)  " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table_restructure1")
  }

  //DTS2015103007487
  test("DTS2015103007487") {

    sql("create schema abc");
    sql("create table abc.babu8 (a string,b INT)")
    sql("drop table  abc.babu8")
    sql("drop schema abc");
  }

  //TC_1114
  test("TC_1114") {
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

  //TC_1116
  test("TC_1116") {
    sql(
      "CREATE table table3_drop  (AMSize STRING,Latest_Day INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table3_drop")
  }

  //TC_1117
  test("TC_1117") {
    sql(
      "CREATE table myschema.table4_drop  (AMSize STRING,Latest_Day INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table4_drop")
  }

  //TC_1120
  test("TC_1120") {
    sql(
      "CREATE table table7_drop  (AMSize STRING,Latest_Day INT)  " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table7_drop")
  }

  //TC_1121
  test("TC_1121") {
    sql(
      "CREATE table myschema.table8_drop  (AMSize STRING,Latest_Day INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table8_drop")
  }

  //TC_1124
  test("TC_1124") {
    sql(
      "CREATE table table27_drop  (AMSize STRING) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table27_drop")
  }

  //TC_1125
  test("TC_1125") {
    sql(
      "CREATE table myschema.table28_drop  (AMSize STRING) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table28_drop")
  }

  //TC_1128
  test("TC_1128") {
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
    sql(
      "CREATE table table34_drop  (AMSize STRING,deviceInformationId STRING," +
      "Latest_Day INT) stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table34_drop")
  }

  //TC_1130
  test("TC_1130") {
    sql(
      "CREATE table myschema.table35_drop  (AMSize STRING,deviceInformationId STRING, " +
      "Latest_Day INT) stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table35_drop")
  }

  //TC_1133
  test("TC_1133") {
    sql(
      "CREATE table myschema.table38_drop  (AMSize STRING,Latest_Day INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table38_drop")
  }

  //TC_1134
  test("TC_1134") {
    sql(
      "CREATE table table39_drop  (AMSize STRING,Latest_Day INT)  " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table39_drop")
  }

  //TC_1135
  test("TC_1135") {
    sql(
      "CREATE table myschema.table45_drop  (AMSize STRING,Latest_Day decimal) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table45_drop")
  }

  //TC_1136
  test("TC_1136") {
    sql(
      "CREATE table table46_drop  (AMSize STRING,Latest_Day decimal)  " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table46_drop")
  }

  //TC_1139
  test("TC_1139") {
    sql(
      "CREATE table myschema.table50_drop  (AMSize decimal,Latest_Day decimal) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table myschema.table50_drop")
  }

  //TC_1140
  test("TC_1140") {
    sql(
      "CREATE table table51_drop  (AMSize decimal,Latest_Day decimal)  " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table51_drop")
  }

  //TC_1143
  test("TC_1143") {
    sql(
      "CREATE table table106_drop  (AMSize STRING,Latest_Day INT)  " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table106_drop")
  }

  //TC_1144
  test("TC_1144") {
    sql(
      "CREATE table table107_drop  (AMSize STRING,Latest_Day INT)  " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table107_drop")
  }

  //TC_1145
  test("TC_1145") {
    sql(
      "CREATE table table108_drop  (AMSize STRING,Latest_Day INT)  " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table108_drop")
  }

  //TC_1146
  test("TC_1146") {
    sql(
      "CREATE table table109_drop  (AMSize STRING,Latest_Day INT)  " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table109_drop")
  }

  //TC_1147
  test("TC_1147") {
    sql(
      "CREATE table table110_drop  (AMSize STRING,Latest_Day INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table110_drop")
  }

  //TC_1148
  test("TC_1148") {
    sql(
      "CREATE table table111_drop  (AMSize STRING,Latest_Day INT)  " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table111_drop")
  }


  //TC_1151
  test("TC_1151") {
    sql(
      "create table table118_drop  (AMSize STRING,Latest_Day INT)  " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table118_drop")
  }


  //TC_1154
  test("TC_1154") {
    sql(
      "create table table119_drop  (AMSize STRING,Latest_Day INT)  " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql("drop table table119_drop")
  }


  //TC_1157
  test("TC_1157") {
    try {
      sql(
        "create table vardhan12 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,gamePointId decimal," +
        "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH  './src/test/resources/TestData1.csv' INTO table vardhan12 " +
        "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,AMSize,channelsId," +
        "ActiveCountry,Activecity')"
      )
      checkAnswer(
        sql("select count(*) from vardhan12"),
        Seq()
      )
      sql("drop table vardhan12")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1158
  test("TC_1158") {
    try {
      sql(
        "create table vardhan13 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,gamePointId decimal," +
        "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH  './src/test/resources/TestData1.csv' INTO table vardhan13 " +
        "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'deviceInformationId," +
        "gamePointId')"
      )
      checkAnswer(
        sql("select count(*) from vardhan13"),
        Seq()
      )
      sql("drop table vardhan13")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1159
  test("TC_1159") {
    try {
      sql(
        "create table vardhan3 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,gamePointId decimal," +
        "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH  './src/test/resources/TestData1.csv' INTO table vardhan3 " +
        "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'deviceInformationId,AMSize," +
        "ActiveCountry,Activecity,gamePointId')"
      )
      checkAnswer(
        sql("select count(*) from vardhan3"),
        Seq()
      )
      sql("drop table vardhan3")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1169
  test("TC_1169") {
    try {
      sql(
        "create table vardhan100 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,gamePointId decimal," +
        "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH  './src/test/resources/TestData1.csv' INTO table vardhan100 " +
        "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')"
      )
      checkAnswer(
        sql("select count(*) from vardhan100"),
        Seq()
      )
      sql("drop table vardhan100")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1178
  test("TC_1178") {
    try {
      sql(
        "create table vardhan13 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,gamePointId decimal," +
        "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH  './src/test/resources/TestData1.csv' INTO table vardhan13 " +
        "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'deviceInformationId," +
        "gamePointId')"
      )
      checkAnswer(
        sql("select count(*) from vardhan13"),
        Seq()
      )
      sql("drop table vardhan13")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1179
  test("TC_1179") {
    try {
      sql(
        "create table vardhan3 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,gamePointId decimal," +
        "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH  './src/test/resources/TestData1.csv' INTO table vardhan3 " +
        "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'deviceInformationId,AMSize," +
        "ActiveCountry,Activecity,gamePointId')"
      )
      checkAnswer(
        sql("select count(*) from vardhan3"),
        Seq()
      )
      sql("drop table vardhan3")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1189
  test("TC_1189") {
    try {
      sql(
        "create table vardhan100 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,gamePointId decimal," +
        "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH  './src/test/resources/TestData1.csv' INTO table vardhan100 " +
        "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')"
      )
      checkAnswer(
        sql("select count(*) from vardhan100"),
        Seq()
      )
      sql("drop table vardhan100")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1190
  test("TC_1190") {
    sql(
      "create table vardhan200 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,gamePointId decimal," +
      "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH  './src/test/resources/TestData1.csv' INTO table vardhan200 " +
      "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize,channelsId,ActiveCountry,Activecity,gamePointId')"
    )
    checkAnswer(
      sql("select count(*) from vardhan200"),
      Seq(Row(100))
    )
    sql("drop table vardhan200")
  }

  //TC_1191
  test("TC_1191") {
    sql(
      "create table vardhan500 (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string, productionDate TIMESTAMP,gamePointId" +
      " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd/MM/yyyy HH:mm")
    sql(
      "LOAD DATA LOCAL INPATH  './src/test/resources/TestData6.csv' INTO table vardhan500 " +
      "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
    )

    checkAnswer(
      sql("select count(*) from vardhan500"),
      Seq(Row(100))
    )
    sql("drop table vardhan500")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
      );
  }

  //TC_1192
  test("TC_1192") {
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
      "LOAD DATA LOCAL INPATH  './src/test/resources/TestData5.csv' INTO table testhive1000 "
    )
    sql(
      "LOAD DATA LOCAL INPATH  './src/test/resources/TestData5.csv' INTO table vardhan1000 " +
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

  //TC_1193
  test("TC_1193") {
    sql(
      "create table vardhan9 (imei string,AMSize string,channelsId string,ActiveCountry " +
      "string, Activecity string,gamePointId decimal,deviceInformationId INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH  './src/test/resources/TestData1.csv' INTO table vardhan9 OPTIONS" +
      "('DELIMITER'= ',' ,'QUOTECHAR'= '/', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId," +
      "ActiveCountry,Activecity,gamePointId')"
    )
    checkAnswer(
      sql("select count(*) from vardhan9"),
      Seq(Row(100))
    )
    sql("drop table vardhan9")
  }

  //TC_1194
  test("TC_1194") {
    sql(
      "create table vardhan16 (AMSize STRING, deviceInformationId INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH  './src/test/resources/TestData1.csv' INTO table vardhan16 " +
      "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize,channelsId,ActiveCountry,Activecity,gamePointId')"
    )
    checkAnswer(
      sql("select count(*) from vardhan16"),
      Seq(Row(100))
    )
    sql("drop table vardhan16")
  }

  //TC_1195
  test("TC_1195") {
    sql(
      "create table myschema.vardhan17  (AMSize STRING,deviceInformationId " +
      "INT) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH  './src/test/resources/TestData1.csv' INTO table myschema.vardhan17 " +
      "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize,channelsId,ActiveCountry,Activecity,gamePointId')"
    )
    checkAnswer(
      sql("select count(*)  from myschema.vardhan17"),
      Seq(Row(100))
    )
    sql("drop table myschema.vardhan17")
  }

  //DTS2015111808892
  test("DTS2015111808892") {
    sql(
      "CREATE table table_restructure  (a0 STRING,b0 INT)  " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH './src/test/resources/restructure_table.csv' INTO table table_restructure" +
      " OPTIONS('DELIMITER'= ',', 'QUOTECHAR'=  '\"')"
    )
    checkAnswer(
      sql("select count(*)  from table_restructure"),
      Seq(Row(100))
    )
    sql("drop table table_restructure")
  }

  //DTS2015112006803_01
  test("DTS2015112006803_01") {
    sql(
      "CREATE table incloading_DTS2015112006803_01  (a0 STRING,b0 INT) " +
      "stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH './src/test/resources/restructure_table.csv' INTO table " +
      "incloading_DTS2015112006803_01 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'=  '\"')"
    )
    sql(
      "LOAD DATA LOCAL INPATH './src/test/resources/restructure_table.csv' INTO table " +
      "incloading_DTS2015112006803_01 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'=  '\"')"
    )
    checkAnswer(
      sql("select count(*) from incloading_DTS2015112006803_01"),
      Seq(Row(200))
    )
    sql("drop table incloading_DTS2015112006803_01")
  }


  //DTS2015111810813
  test("DTS2015111810813", NonRunningTests) {
    sql(
      "create table single (imei string,deviceInformationId INT,mac string," +
      "productdate timestamp,updatetime timestamp,gamePointId decimal,contractNumber " +
      "decimal) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH './src/test/resources/vmallFact_headr.csv' INTO table single " +
      "OPTIONS('DELIMITER'= '\001', 'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "mac,productdate,updatetime,gamePointId,contractNumber')"
    )
    checkAnswer(
      sql("select count(*) from single"),
      Seq(Row(10))
    )
    sql("drop table single")
  }

  //DTS2015101504861
  test("DTS2015101504861", NonRunningTests) {
    sql(
      "create table vard970 (imei string,productionDate timestamp,AMSize string," +
      "channelsId string,ActiveCountry string, Activecity string) measures(gamePointId decimal," +
      "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH './src/test/resources/TestData2.csv' INTO table vard970 OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,productionDate,deviceInformationId," +
      "AMSize,channelsId,ActiveCountry,Activecity,gamePointId')"
    )
    checkAnswer(
      sql("select imei from vard970 where productionDate='2015-07-06 12:07:00'"),
      Seq()
    )
    sql("drop table vard970")
  }

  //TC_1326
  test("TC_1326", NonRunningTests) {
    sql(
      "create table vardhanincomp (imei string,AMSize string,channelsId string," +
      "ActiveCountry string, Activecity string,gamePointId decimal," +
      "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH './src/test/resources/TestData4.csv' INTO table vardhanincomp OPTIONS" +
      "('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId," +
      "ActiveCountry,Activecity,gamePointId')"
    )
    checkAnswer(
      sql("select channelsId from vardhanincomp order by imei ASC limit 0"),
      Seq()
    )
    sql("drop table vardhanincomp")
  }


  //DTS2015120304016
  test("DTS2015120304016") {
    sql(
      "CREATE table incloading1  (a0 STRING,b0 INT) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH './src/test/resources/restructure_table.csv' INTO table incloading1 " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'=  '\"\"')"
    )
    sql("drop table incloading1")
    sql(
      "CREATE table incloading1  (a0 STRING,b0 INT) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA LOCAL INPATH './src/test/resources/restructure_table.csv' INTO table incloading1 " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'=  '\"\"')"
    )
    checkAnswer(
      sql("select count(*) from incloading1"),
      Seq(Row(100))
    )
    sql("drop table incloading1")
  }

  //TC_1160
  test("TC_1160") {
    try {
      sql(
        "create table vardhan4 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,gamePointId decimal," +
        "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA './src/test/resources/TestData1.csv' INTO table vardhan4 OPTIONS" +
        "('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize," +
        "channelsId,ActiveCountry,Activecity,gamePointId')"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table vardhan4")

    }
  }

  //TC_1161
  test("TC_1161") {
    try {
      sql(
        "create table vardhan5 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,gamePointId decimal," +
        "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH  INTO table vardhan5 OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', " +
        "'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity," +
        "gamePointId')"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table vardhan5")

    }
  }

  //TC_1162
  test("TC_1162") {
    try {
      sql(
        "create table vardhan1 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,gamePointId decimal," +
        "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH  './src/test/resources/TestData1.csv' INTO table  OPTIONS" +
        "('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize," +
        "channelsId,ActiveCountry,Activecity,gamePointId')"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table vardhan1")

    }
  }

  //TC_1164
  test("TC_1164") {
    try {
      sql(
        "create table vardhan1 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,gamePointId decimal," +
        "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql(
        "\"LOAD DATA LOCAL INPATH  './src/test/resources/TestData1.csv' INTO table vardhan1 " +
        "('DELIMITER'= ',' ,'QUOTECHAR'= '\"\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize," +
        "channelsId,ActiveCountry,Activecity,gamePointId')"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table vardhan1")

    }
  }

  //TC_1165
  test("TC_1165") {
    try {
      sql(
        "create table vardhan10 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,gamePointId decimal," +
        "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH  './src/test/resources/TestData1.csv' INTO table vardhan10 " +
        "OPTIONS('QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId," +
        "ActiveCountry,Activecity,gamePointId')"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table vardhan10")

    }
  }

  //TC_1180
  test("TC_1180") {
    try {
      sql(
        "create table vardhan4 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,gamePointId decimal," +
        "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA './src/test/resources/TestData1.csv' INTO table vardhan4 OPTIONS" +
        "('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize," +
        "channelsId,ActiveCountry,Activecity,gamePointId')"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table vardhan4")

    }
  }

  //TC_1181
  test("TC_1181") {
    try {
      sql(
        "create table vardhan5 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,gamePointId decimal," +
        "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH  INTO table vardhan5 OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', " +
        "'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity," +
        "gamePointId')"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table vardhan5")

    }
  }

  //TC_1182
  test("TC_1182") {
    try {
      sql(
        "create table vardhan1 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,gamePointId decimal," +
        "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH  './src/test/resources/TestData1.csv' INTO table  OPTIONS" +
        "('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize," +
        "channelsId,ActiveCountry,Activecity,gamePointId')"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table vardhan1")

    }
  }

  //TC_1184
  test("TC_1184") {
    try {
      sql(
        "create table vardhan1 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,gamePointId decimal," +
        "deviceInformationId INT) stored by 'org.apache.carbondata.format'")
        sql(
          "LOAD DATA LOCAL INPATH  './src/test/resources/TestData1.csv' INTO table vardhan1 " +
          "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize," +
          "channelsId,ActiveCountry,Activecity,gamePointId')"
          )
        fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table vardhan1")

    }
  }

  //TC_1185
  test("TC_1185") {
    try {
      sql(
        "create table vardhan10 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,gamePointId decimal," +
        "deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH  './src/test/resources/TestData1.csv' INTO table vardhan10 " +
        "OPTIONS('QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId," +
        "ActiveCountry,Activecity,gamePointId')"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table vardhan10")

    }
  }

  //DTS2015111900020
  test("DTS2015111900020") {
    try {
      sql(
        "CREATE table testwithout_measure  (AMSize STRING) " +
        "stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH './src/test/resources/create_table.csv' INTO table testwithout_measure" +
        " OPTIONS('DELIMITER'= ',', 'QUOTECHAR'=  '\"')"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table testwithout_measure")

    }
  }


  //TC_1255
  test("TC_1255") {
    try {
      sql(
        "create table table10 (imei string,AMSize string,channelsId string,ActiveCountry " +
        "string, Activecity string,gamePointId decimal,deviceInformationId INT) " +
        "stored by 'org.apache.carbondata.format'"
      )
      checkAnswer(
        sql("show segments for table tabledoesnotexist"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table table10")

    }
  }

  //TC_1256
  test("TC_1256") {
    try {
      sql(
        "create table table11 (imei string,AMSize string,channelsId string,ActiveCountry " +
        "string, Activecity string,gamePointId decimal,deviceInformationId INT) " +
        "stored by 'org.apache.carbondata.format'"
      )
      checkAnswer(
        sql("show segments for table"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table table11")

    }
  }


  //TC_1292
  test("TC_1292") {
    try {
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
        "LOAD DATA LOCAL INPATH './src/test/resources/100.csv' INTO table myschema1.table2 " +
        "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'=  '\"\"', 'FILEHEADER' = 'imei,deviceInformationId," +
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
      sql("delete load 0 from  table myschema1.table2")
      checkAnswer(
        sql("select deviceInformationId from myschema1.table2"),
        Seq()
      )
    }
    catch {
      case ex: Throwable => sql("drop table myschema1.table2")

    }

  }

  //TC_1307
  test("TC_1307") {
    try {
      sql(
        "create table table17 (imei string,AMSize string,channelsId string,ActiveCountry " +
        "string, Activecity string,gamePointId decimal,deviceInformationId INT) " +
        "stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH  './src/test/resources/TestData1.csv' INTO table table17 OPTIONS" +
        "('DELIMITER'= ',' ,'QUOTECHAR'= '\"\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize," +
        "channelsId,ActiveCountry,Activecity,gamePointId')"
      )
      sql("delete load 1 from  table table17")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table table17")

    }
  }

  //TC_1310
  test("TC_1310") {
    try {
      sql(
        "create table table20 (imei string,AMSize string,channelsId string,ActiveCountry " +
        "string, Activecity string,gamePointId decimal,deviceInformationId INT) " +
        "stored by 'org.apache.carbondata.format'"
      )
      sql("delete load 1 from table table20")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table table20")

    }
  }

  //TC_1313
  test("TC_1313") {
    try {
      sql(
        "create table vardhanretention2 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId" +
        " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH './src/test/resources/TestData3.csv' INTO table vardhanretention2 " +
        "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize," +
        "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
      )
      sql(
        "LOAD DATA LOCAL INPATH './src/test/resources/TestData3.csv' INTO table vardhanretention2 " +
        "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize," +
        "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
      )
      sql("delete load 2 from table vardhanretention2")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table vardhanretention2")

    }
  }

  //TC_1315
  test("TC_1315") {
    try {
      sql(
        "create table vardhanretention4 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId" +
        " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql("delete load 0 from table vardhanretention4")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table vardhanretention4")

    }
  }

  //TC_1317
  test("TC_1317") {
    try {
      sql(
        "create table vardhanretention14 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId" +
        " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH './src/test/resources/TestData3.csv' INTO table vardhanretention14 " +
        "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize," +
        "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
      )
      sql("delete load 0 from table vardhanretention14")
      sql("delete load 0 from table vardhanretention14")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table vardhanretention14")

    }
  }

  //TC_1319
  test("TC_1319") {
    try {
      sql(
        "create table vardhanretention15 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId" +
        " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH './src/test/resources/TestData3.csv' INTO table vardhanretention15 " +
        "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize," +
        "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
      )
      sql("delete load 0,1 from table vardhanretention15")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table vardhanretention15")

    }
  }

  //TC_1320
  test("TC_1320") {
    try {
      sql(
        "create table vardhanretention7 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId" +
        " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH './src/test/resources/TestData3.csv' INTO table vardhanretention7 " +
        "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize," +
        "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
      )
      sql("delete from table vardhanretention7 where productionDate before '2015-07-05 '")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table vardhanretention7")

    }
  }

  //TC_1321
  test("TC_1321") {
    try {
      sql(
        "create table vardhanretention8 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId" +
        " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql("delete from table vardhanretention8 where productionDate before '2015-07-05 '")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table vardhanretention8")

    }
  }

  //TC_1323
  test("TC_1323") {
    try {
      sql(
        "create table vardhanretention10 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId" +
        " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH './src/test/resources/TestData3.csv' INTO table vardhanretention10 " +
        "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize," +
        "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
      )
      sql("delete from table vardhanretention10 where productionDate before ''")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table vardhanretention10")

    }
  }

  //TC_1324
  test("TC_1324") {
    try {
      sql(
        "create table vardhanretention12 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId" +
        " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH './src/test/resources/TestData3.csv' INTO table vardhanretention12 " +
        "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize," +
        "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
      )
      sql("delete from table vardhanretention12 where productionDate before '10-06-2015 12:07:28'")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table vardhanretention12")

    }
  }

  //TC_1325
  test("TC_1325") {
    try {
      sql(
        "create table vardhanretention13 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId" +
        " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      CarbonProperties.getInstance().
        addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
          CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
        )
      sql(
        "LOAD DATA LOCAL INPATH './src/test/resources/TestData3.csv' INTO table vardhanretention13 " +
        "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize," +
        "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
      )
      sql("delete from table vardhanretention13 where productionDate before '2013-13-13 12:07:28 '")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table vardhanretention13")

    }
  }

  //DTS2015112608945
  test("DTS2015112608945") {
    try {
      sql(
        "create table babu_67 (imei string,deviceInformationId INT,mac string," +
        "productdate timestamp,updatetime timestamp) measures(gamePointId decimal," +
        "contractNumber decimal) stored by 'org.apache.carbondata.format'"
      )
      sql("delete from table babu_67 where productdate before '2015-01-10 19:59:00'")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table babu_67")

    }
  }

  //DTS2015102804722
  test("DTS2015102804722") {
    try {
      sql(
        "create table table_restructure62  (a0 STRING,b0 INT) " +
        "stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH './src/test/resources/restructure_table.csv' INTO table " +
        "table_restructure62 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'=  '\"\"', 'FILEHEADER'= 'a0,b0')"
      )
      sql("delete load 0 from table table_restructure62\"")
      sql("delete load 0 from table table_restructure62")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table table_restructure62")

    }
  }

  //DTS2015111403600
  test("DTS2015111403600") {
    try {
      sql(
        "create table table_restructure64  (a0 STRING,b0 INT)  " +
        "stored by 'org.apache.carbondata.format'"
      )
      sql(
        "LOAD DATA LOCAL INPATH './src/test/resources/restructure_table.csv' INTO table " +
        "table_restructure64 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'=  '\"\"', 'FILEHEADER'= 'a0,b0')"
      )
      sql("delete load 0 from table table_restructure64")
      sql("clean files for table table_restructure64\"")
      sql("delete load 0 from table table_restructure64")
      sql("delete load 1 from table table_restructure64")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table table_restructure64")

    }
  }

  //DTS2015112703910
  test("DTS2015112703910") {
    try {
      sql(
        "CREATE table table_restructure65  (a0 STRING,b0 INT) " +
        "stored by 'org.apache.carbondata.format'"
      )
      sql("delete load 1 from table table_restructure65")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table table_restructure65")

    }
  }

  //DTS2015110209665
  test("DTS2015110209665") {
    try {
      sql(
        "create table vardhanretention13 (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,productionDate timestamp,gamePointId" +
        " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql("delete from table vardhanretention13 where productionDate before '2013-13-13 12:07:28 '")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table vardhanretention13")

    }
  }

  //TC_1346
  test("TC_1346") {
    try {
      sql(
        "create table vardhanretention (imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string,productionDate timestamp,gamePointId" +
        " decimal,deviceInformationId INT) stored by 'org.apache.carbondata.format'"
      )
      sql("delete load 0 from table vardhanretention")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop table vardhanretention")

    }
  }


}