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

package org.carbondata.spark.testsuite.allqueries

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.{NonRunningTests, QueryTest}
import org.scalatest.BeforeAndAfterAll

import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties

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
        "CREATE CUBE cube_restructure444 DIMENSIONS (a0 STRING,a STRING) MEASURES(b0 INTEGER) " +
          "OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ," +
          "PARTITION_COUNT=1] )"

      )
      sql("LOAD DATA FACT FROM '" + currentDirectory + "/src/test/resources/restructure_cube.csv'" +
        " INTO CUBE cube_restructure444 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"')")
      sql("create schema myschema")
      sql("create schema myschema1")
      sql("create schema drug")
    } catch {
      case e: Exception => print("ERROR: cube_restructure444")
    }


  }

  override def afterAll {
    try {
      sql("drop cube cube_restructure444")
      sql("drop schema myschema")
      sql("drop schema myschema1")
      sql("drop schema drug")
    } catch {
      case e: Exception => print("ERROR: DROP cube_restructure444 ")
    }

  }


  //TC_898
  test("select * from cube_restructure444") {

    sql("select * from cube_restructure444")

  }

  //TC_1059
  test("TC_1059") {
    sql(
      "create cube  cube1 dimensions(imei string,deviceInformationId integer,MAC string," +
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
        "Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, " +
        "Latest_areaId string, Latest_country string, Latest_province string, Latest_city string," +
        " Latest_district string, Latest_street string, Latest_releaseId string, " +
        "Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, " +
        "Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, " +
        "Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, " +
        "Latest_phonePADPartitionedVersions string, Latest_operatorId string, " +
        "gamePointDescription string)  measures(gamePointId numeric,contractNumber numeric) " +
        "OPTIONS (PARTITIONER [CLASS = 'org.carbondata.spark.partition.api.impl" +
        ".SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )"
    )
    sql("drop cube cube1")
  }

  //TC_1060
  test("TC_1060") {
    sql(
      "create cube  myschema.cube2 dimensions(imei string,deviceInformationId integer,MAC string," +
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
        "Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, " +
        "Latest_areaId string, Latest_country string, Latest_province string, Latest_city string," +
        " Latest_district string, Latest_street string, Latest_releaseId string, " +
        "Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, " +
        "Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, " +
        "Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, " +
        "Latest_phonePADPartitionedVersions string, Latest_operatorId string, " +
        "gamePointDescription string)  measures(gamePointId numeric,contractNumber numeric) " +
        "OPTIONS (PARTITIONER [CLASS = 'org.carbondata.spark.partition.api.impl" +
        ".SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )"
    )
    sql("drop cube myschema.cube2")
  }

  //TC_1061
  test("TC_1061") {
    sql(
      "CREATE CUBE cube3 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS " +
        "(AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )"
    )
    sql("drop cube cube3")
  }

  //TC_1062
  test("TC_1062") {
    sql(
      "CREATE CUBE myschema.cube4 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) " +
        "OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata" +
        ".spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ," +
        "PARTITION_COUNT=1] )"
    )
    sql("drop cube myschema.cube4")
  }

  //TC_1065
  test("TC_1065") {
    sql(
      "CREATE CUBE cube7 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS " +
        "(AGGREGATION [Latest_Day = count])"
    )
    sql("drop cube cube7")
  }

  //TC_1066
  test("TC_1066") {
    sql(
      "CREATE CUBE myschema.cube8 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) " +
        "OPTIONS (AGGREGATION [Latest_Day = count])"
    )
    sql("drop cube myschema.cube8")
  }

  //TC_1069
  test("TC_1069") {
    sql(
      "CREATE CUBE cube27 DIMENSIONS (AMSize STRING) OPTIONS (AGGREGATION [Latest_Day = count] " +
        "PARTITIONER [CLASS = 'org.carbondata.spark.partition.api.impl" +
        ".SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )"
    )
    sql("drop cube cube27")
  }

  //TC_1070
  test("TC_1070") {
    sql(
      "CREATE CUBE myschema.cube28 DIMENSIONS (AMSize STRING) OPTIONS (AGGREGATION [Latest_Day = " +
        "count] PARTITIONER [CLASS = 'org.carbondata.spark.partition.api.impl" +
        ".SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )"
    )
    sql("drop cube myschema.cube28")
  }

  //TC_1073
  test("TC_1073") {
    try {
      sql(
        "CREATE CUBE cube31 MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = " +
          "count] PARTITIONER [CLASS = 'org.carbondata.spark.partition.api.impl" +
          ".SampleDataPartitionerImpl', COLUMNS= (Latest_Day), PARTITION_COUNT=1] )"
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
        "CREATE CUBE cube32 MEASURES (Latest_Day INTEGER as col2) OPTIONS (AGGREGATION " +
          "[Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.spark.partition" +
          ".api.impl.SampleDataPartitionerImpl', COLUMNS= (col2), PARTITION_COUNT=1] )"
      )
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1075
  test("TC_1075") {
    sql(
      "create cube cube33 DIMENSIONS (imei string,deviceInformationId integer,MAC string," +
        "deviceColor string, device_backColor string,modelId string, marketName string, AMSize " +
        "string, ROMSize string, CUPAudit string, CPIClocked string, series string, " +
        "productionDate string, bomCode string, internalModels string, deliveryTime string, " +
        "channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, " +
        "deliveryProvince  string, deliveryCity string,deliveryDistrict string, deliveryStreet " +
        "string, oxSingleNumber string, contractNumber numeric, ActiveCheckTime string, " +
        "ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, " +
        "ActiveDistrict  string, ActiveStreet string, ActiveOperatorId string, Active_releaseId " +
        "string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber " +
        "string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer " +
        "string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, " +
        "Active_phonePADPartitionedVersions string, Latest_YEAR  integer, Latest_MONTH integer, " +
        "Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, " +
        "Latest_province string, Latest_city string, Latest_district string, Latest_street " +
        "string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion " +
        "string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion " +
        "string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
        "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
        "Latest_operatorId string, gamePointDescription string) MEASURES (gamePointId numeric) " +
        "OPTIONS (PARTITIONER [CLASS = 'org.carbondata.spark.partition.api.impl" +
        ".SampleDataPartitionerImpl', COLUMNS= (imei) ,PARTITION_COUNT=2] )"
    )
    sql("drop cube cube33")
  }

  //TC_1076
  test("TC_1076") {
    sql(
      "CREATE CUBE cube34 DIMENSIONS (AMSize STRING,deviceInformationId STRING) MEASURES " +
        "(Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org" +
        ".carbondata.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= " +
        "(AMSize,deviceInformationId) ,PARTITION_COUNT=1] )"
    )
    sql("drop cube cube34")
  }

  //TC_1077
  test("TC_1077") {
    sql(
      "CREATE CUBE myschema.cube35 DIMENSIONS (AMSize STRING,deviceInformationId STRING) MEASURES" +
        " (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = " +
        "'org.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl', " +
        "COLUMNS= (AMSize,deviceInformationId) ,PARTITION_COUNT=1] )"
    )
    sql("drop cube myschema.cube35")
  }

  //TC_1080
  test("TC_1080") {
    sql(
      "CREATE CUBE myschema.cube38 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) " +
        "OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata" +
        ".spark.partition.api.impl.SampleDataPartitionerImpl',COLUMNS= (AMSize) ," +
        "PARTITION_COUNT=10] )"
    )
    sql("drop cube myschema.cube38")
  }

  //TC_1081
  test("TC_1081") {
    sql(
      "CREATE CUBE cube39 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS " +
        "(AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl',COLUMNS= (AMSize) ,PARTITION_COUNT=10] )"
    )
    sql("drop cube cube39")
  }

  //TC_1082
  test("TC_1082") {
    sql(
      "CREATE CUBE myschema.cube40 DIMENSIONS (bomCode integer) MEASURES (Latest_Day INTEGER) " +
        "OPTIONS (AGGREGATION [Latest_Day = count])"
    )
    sql("drop cube myschema.cube40")
  }

  //TC_1083
  test("TC_1083") {
    sql(
      "CREATE CUBE cube41 DIMENSIONS (bomCode integer) MEASURES (Latest_Day INTEGER) OPTIONS " +
        "(AGGREGATION [Latest_Day = count])"
    )
    sql("drop cube cube41")
  }

  //TC_1086
  test("TC_1086") {
    try {


      sql("drop cube cube44")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1087
  test("TC_1087") {
    sql(
      "CREATE CUBE myschema.cube45 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day numeric) " +
        "OPTIONS (AGGREGATION [bomCode = count])"
    )
    sql("drop cube myschema.cube45")
  }

  //TC_1088
  test("TC_1088") {
    sql(
      "CREATE CUBE cube46 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day numeric) OPTIONS " +
        "(AGGREGATION [bomCode = count])"
    )
    sql("drop cube cube46")
  }

  //TC_1091
  test("TC_1091") {
    try {

      sql("drop cube cube49")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1092
  test("TC_1092") {
    sql(
      "CREATE CUBE myschema.cube50 DIMENSIONS (AMSize numeric) MEASURES (Latest_Day numeric) " +
        "OPTIONS (AGGREGATION [Latest_Day = count])"
    )
    sql("drop cube myschema.cube50")
  }

  //TC_1093
  test("TC_1093") {
    sql(
      "CREATE CUBE cube51 DIMENSIONS (AMSize numeric) MEASURES (Latest_Day numeric) OPTIONS " +
        "(AGGREGATION [Latest_Day = count])"
    )
    sql("drop cube cube51")
  }

  //TC_1096
  test("TC_1096") {
    try {

      sql("drop cube cube54")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1097
  test("TC_1097") {
    try {

      sql("drop cube cube55")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1098
  test("TC_1098") {
    sql(
      "CREATE CUBE cube106 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS " +
        "(AGGREGATION [Latest_Day = sum] PARTITIONER [CLASS = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )"
    )
    sql("drop cube cube106")
  }

  //TC_1099
  test("TC_1099") {
    sql(
      "CREATE CUBE cube107 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS " +
        "(AGGREGATION [Latest_Day = avg] PARTITIONER [CLASS = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )"
    )
    sql("drop cube cube107")
  }

  //TC_1100
  test("TC_1100") {
    sql(
      "CREATE CUBE cube108 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS " +
        "(AGGREGATION [Latest_Day = min] PARTITIONER [CLASS = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )"
    )
    sql("drop cube cube108")
  }

  //TC_1101
  test("TC_1101") {
    sql(
      "CREATE CUBE cube109 DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS " +
        "(AGGREGATION [Latest_Day = max] PARTITIONER [CLASS = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )"
    )
    sql("drop cube cube109")
  }

  //TC_1102
  test("TC_1102") {
    sql(
      "CREATE CUBE cube110 DIMENSIONS(imei String, test integer, key String, name String) " +
        "MEASURES(gamepointid Numeric, price integer) WITH dimFile2 RELATION (FACT.deviceid=key) " +
        "INCLUDE ( key, name)"
    )
    sql("drop cube cube110")
  }

  //TC_1103
  test("TC_1103") {
    try {

      sql("drop cube cube111")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1104
  test("TC_1104") {
    try {


      sql("drop cube cube112")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1105
  test("TC_1105") {
    sql(
      "CREATE CUBE cube113 dimensions(imei String, test integer, key String, name String) " +
        "measures(gamepointid Numeric, price integer) with dimFile1 relation (FACT.deviceid=key) " +
        "include ( key, name)"
    )
    sql("drop cube cube113")
  }

  //TC_1106
  test("TC_1106") {
    sql(
      "create cube myschema.cube114 dimensions(imei String, test integer,key String, name String)" +
        " measures(gamepointid Numeric, price integer) with dimFile relation (FACT.deviceid=key) " +
        "include ( key, name)"
    )
    sql("drop cube myschema.cube114")
  }

  //TC_1107
  test("TC_1107") {

    try {

      sql("drop cube cube115")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1108
  test("TC_1108") {

    try {
      sql(
        "CREATE CUBE cube116 DIMENSIONS(imei String, test integer) MEASURES(gamepointid Numeric, " +
          "price integer) WITH hivetest RELATION (FACT.deviceid=key1) INCLUDE ( key, name)"
      )
      sql("drop cube cube116")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }

  }

  //TC_1109
  test("TC_1109") {

    try {
      sql(
        "CREATE CUBE cube117 DIMENSIONS(imei String, test integer) MEASURES(gamepointid Numeric, " +
          "price integer) WITH dimFile:hivetest RELATION () INCLUDE ()"
      )
      sql("drop cube cube117")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }


  }

  //TC_1110
  test("TC_1110") {
    sql(
      "create cube cube118 dimensions (AMSize STRING) measures (Latest_Day INTEGER) options " +
        "(aggregation [Latest_Day = count] partitioner [class = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl', columns= (AMSize) ,partition_count=1] )"
    )
    sql("drop cube cube118")
  }

  //TC_1111
  test("TC_1111") {

    try {
      sql(
        "create cube cube119 dimensions () measures () options (aggregation [] partitioner [class" +
          " = 'org.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl', " +
          "columns= () ,partition_count=1] )"
      )
      sql("drop cube cube119")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }


  }

  //TC_1112
  test("TC_1112") {
    sql(
      "create cube cube120 dimensions(key string,name string) measures(gamepointid numeric,price " +
        "numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)"
    )
    sql("drop cube cube120")
  }

  //TC_1113
  test("TC_1113") {
    sql(
      "create cube myschema.cube121 dimensions(key string,name string) measures(gamepointid " +
        "numeric,price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)"
    )
    sql("drop cube myschema.cube121")
  }

  //DTS2015101203724
  test("DTS2015101203724") {
    try {
      sql(
        "CREATE CUBE myschema.include8 FROM './src/test/resources/100_hive.csv' INCLUDE (imei, " +
          "imei),'./src/test/resources/101_hive.csv' RELATION (fact.gamepointid = gamepointid) " +
          "EXCLUDE (MAC, bomcode)"
      )
      sql("drop cube myschema.include8")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }


  }

  //DTS2015103009506
  test("DTS2015103009506") {
    sql(
      "CREATE CUBE cube_restructure1 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS " +
        "(AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
    )
    sql("drop cube cube_restructure1")
  }

  //DTS2015103007487
  test("DTS2015103007487") {

    sql("create schema abc");
    sql("create cube abc.babu8 dimensions(a string) measures(b INTEGER)")
    sql("drop cube  abc.babu8")
    sql("drop schema abc");
  }

  //TC_1114
  test("TC_1114") {
    sql(
      "create cube  cube1_drop dimensions(imei string,deviceInformationId integer,MAC string," +
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
        "Latest_YEAR integer, Latest_MONTH integer, Latest_DAY integer, Latest_HOUR string, " +
        "Latest_areaId string, Latest_country string, Latest_province string, Latest_city string," +
        " Latest_district string, Latest_street string, Latest_releaseId string, " +
        "Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, " +
        "Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, " +
        "Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, " +
        "Latest_phonePADPartitionedVersions string, Latest_operatorId string, " +
        "gamePointDescription string)  measures(gamePointId numeric,contractNumber numeric) " +
        "OPTIONS (PARTITIONER [CLASS = 'org.carbondata.spark.partition.api.impl" +
        ".SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )"
    )
    sql("drop cube cube1_drop")
  }

  //TC_1115
  test("TC_1115") {
    sql(
      "create cube  myschema.cube2_drop dimensions(imei string,deviceInformationId integer,MAC " +
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
        "Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, " +
        "Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, " +
        "Latest_province string, Latest_city string, Latest_district string, Latest_street " +
        "string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion " +
        "string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion " +
        "string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
        "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
        "Latest_operatorId string, gamePointDescription string)  measures(gamePointId numeric," +
        "contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )"
    )
    sql("drop cube myschema.cube2_drop")
  }

  //TC_1116
  test("TC_1116") {
    sql(
      "CREATE CUBE cube3_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS " +
        "(AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )"
    )
    sql("drop cube cube3_drop")
  }

  //TC_1117
  test("TC_1117") {
    sql(
      "CREATE CUBE myschema.cube4_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) " +
        "OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata" +
        ".spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ," +
        "PARTITION_COUNT=1] )"
    )
    sql("drop cube myschema.cube4_drop")
  }

  //TC_1120
  test("TC_1120") {
    sql(
      "CREATE CUBE cube7_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS " +
        "(AGGREGATION [Latest_Day = count])"
    )
    sql("drop cube cube7_drop")
  }

  //TC_1121
  test("TC_1121") {
    sql(
      "CREATE CUBE myschema.cube8_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) " +
        "OPTIONS (AGGREGATION [Latest_Day = count])"
    )
    sql("drop cube myschema.cube8_drop")
  }

  //TC_1124
  test("TC_1124") {
    sql(
      "CREATE CUBE cube27_drop DIMENSIONS (AMSize STRING) OPTIONS (AGGREGATION [Latest_Day = " +
        "count] PARTITIONER [CLASS = 'org.carbondata.spark.partition.api.impl" +
        ".SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )"
    )
    sql("drop cube cube27_drop")
  }

  //TC_1125
  test("TC_1125") {
    sql(
      "CREATE CUBE myschema.cube28_drop DIMENSIONS (AMSize STRING) OPTIONS (AGGREGATION " +
        "[Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.spark.partition" +
        ".api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )"
    )
    sql("drop cube myschema.cube28_drop")
  }

  //TC_1128
  test("TC_1128") {
    sql(
      "create cube cube33_drop DIMENSIONS (imei string,deviceInformationId integer,MAC string," +
        "deviceColor string, device_backColor string,modelId string, marketName string, AMSize " +
        "string, ROMSize string, CUPAudit string, CPIClocked string, series string, " +
        "productionDate string, bomCode string, internalModels string, deliveryTime string, " +
        "channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, " +
        "deliveryProvince  string, deliveryCity string,deliveryDistrict string, deliveryStreet " +
        "string, oxSingleNumber string, contractNumber numeric, ActiveCheckTime string, " +
        "ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, " +
        "ActiveDistrict  string, ActiveStreet string, ActiveOperatorId string, Active_releaseId " +
        "string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber " +
        "string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer " +
        "string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, " +
        "Active_phonePADPartitionedVersions string, Latest_YEAR  integer, Latest_MONTH integer, " +
        "Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, " +
        "Latest_province string, Latest_city string, Latest_district string, Latest_street " +
        "string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion " +
        "string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion " +
        "string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
        "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
        "Latest_operatorId string, gamePointDescription string) MEASURES (gamePointId numeric) " +
        "OPTIONS (PARTITIONER [CLASS = 'org.carbondata.spark.partition.api.impl" +
        ".SampleDataPartitionerImpl', COLUMNS= (imei) ,PARTITION_COUNT=2] )"
    )
    sql("drop cube cube33_drop")
  }

  //TC_1129
  test("TC_1129") {
    sql(
      "CREATE CUBE cube34_drop DIMENSIONS (AMSize STRING,deviceInformationId STRING) MEASURES " +
        "(Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org" +
        ".carbondata.spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= " +
        "(AMSize,deviceInformationId) ,PARTITION_COUNT=1] )"
    )
    sql("drop cube cube34_drop")
  }

  //TC_1130
  test("TC_1130") {
    sql(
      "CREATE CUBE myschema.cube35_drop DIMENSIONS (AMSize STRING,deviceInformationId STRING) " +
        "MEASURES (Latest_Day INTEGER) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER " +
        "[CLASS = 'org.carbondata.spark.partition.api.impl" +
        ".SampleDataPartitionerImpl', COLUMNS= (AMSize,deviceInformationId) ,PARTITION_COUNT=1] )"
    )
    sql("drop cube myschema.cube35_drop")
  }

  //TC_1133
  test("TC_1133") {
    sql(
      "CREATE CUBE myschema.cube38_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) " +
        "OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata" +
        ".spark.partition.api.impl.SampleDataPartitionerImpl',COLUMNS= (AMSize) ," +
        "PARTITION_COUNT=10] )"
    )
    sql("drop cube myschema.cube38_drop")
  }

  //TC_1134
  test("TC_1134") {
    sql(
      "CREATE CUBE cube39_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS " +
        "(AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl',COLUMNS= (AMSize) ,PARTITION_COUNT=10] )"
    )
    sql("drop cube cube39_drop")
  }

  //TC_1135
  test("TC_1135") {
    sql(
      "CREATE CUBE myschema.cube45_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day numeric) " +
        "OPTIONS (AGGREGATION [bomCode = count])"
    )
    sql("drop cube myschema.cube45_drop")
  }

  //TC_1136
  test("TC_1136") {
    sql(
      "CREATE CUBE cube46_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day numeric) OPTIONS " +
        "(AGGREGATION [bomCode = count])"
    )
    sql("drop cube cube46_drop")
  }

  //TC_1139
  test("TC_1139") {
    sql(
      "CREATE CUBE myschema.cube50_drop DIMENSIONS (AMSize numeric) MEASURES (Latest_Day numeric)" +
        " OPTIONS (AGGREGATION [Latest_Day = count])"
    )
    sql("drop cube myschema.cube50_drop")
  }

  //TC_1140
  test("TC_1140") {
    sql(
      "CREATE CUBE cube51_drop DIMENSIONS (AMSize numeric) MEASURES (Latest_Day numeric) OPTIONS " +
        "(AGGREGATION [Latest_Day = count])"
    )
    sql("drop cube cube51_drop")
  }

  //TC_1143
  test("TC_1143") {
    sql(
      "CREATE CUBE cube106_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS " +
        "(AGGREGATION [Latest_Day = sum] PARTITIONER [CLASS = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )"
    )
    sql("drop cube cube106_drop")
  }

  //TC_1144
  test("TC_1144") {
    sql(
      "CREATE CUBE cube107_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS " +
        "(AGGREGATION [Latest_Day = avg] PARTITIONER [CLASS = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )"
    )
    sql("drop cube cube107_drop")
  }

  //TC_1145
  test("TC_1145") {
    sql(
      "CREATE CUBE cube108_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS " +
        "(AGGREGATION [Latest_Day = min] PARTITIONER [CLASS = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )"
    )
    sql("drop cube cube108_drop")
  }

  //TC_1146
  test("TC_1146") {
    sql(
      "CREATE CUBE cube109_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS " +
        "(AGGREGATION [Latest_Day = max] PARTITIONER [CLASS = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )"
    )
    sql("drop cube cube109_drop")
  }

  //TC_1147
  test("TC_1147") {
    sql(
      "CREATE CUBE cube110_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS " +
        "(AGGREGATION [Latest_Day = max] PARTITIONER [CLASS = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )"
    )
    sql("drop cube cube110_drop")
  }

  //TC_1148
  test("TC_1148") {
    sql(
      "CREATE CUBE cube111_drop DIMENSIONS (AMSize STRING) MEASURES (Latest_Day INTEGER) OPTIONS " +
        "(AGGREGATION [Latest_Day = max] PARTITIONER [CLASS = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )"
    )
    sql("drop cube cube111_drop")
  }

  //TC_1149
  test("TC_1149") {
    sql(
      "CREATE CUBE cube113_drop dimensions(imei String, test integer, key String, name String) " +
        "measures(gamepointid Numeric, price integer) with dimFile1 relation (FACT.deviceid=key) " +
        "include ( key, name)"
    )
    sql("drop cube cube113_drop")
  }

  //TC_1150
  test("TC_1150") {
    sql(
      "create cube myschema.cube114_drop dimensions(imei String, test integer,key String, name " +
        "String) measures(gamepointid Numeric, price integer) with dimFile relation (FACT" +
        ".deviceid=key) include ( key, name)"
    )
    sql("drop cube myschema.cube114_drop")
  }

  //TC_1151
  test("TC_1151") {
    sql(
      "create cube cube118_drop dimensions (AMSize STRING) measures (Latest_Day INTEGER) options " +
        "(aggregation [Latest_Day = count] partitioner [class = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl', columns= (AMSize) ,partition_count=1] )"
    )
    sql("drop cube cube118_drop")
  }

  //TC_1152
  test("TC_1152") {
    try {
      //sql("create cube cube118_drop dimensions (AMSize STRING) measures (Latest_Day INTEGER)
      // options (aggregation [Latest_Day = count] partitioner [class = 'org.carbondata
      // .spark.partition.api.impl.SampleDataPartitionerImpl', columns= (AMSize) ,
      // partition_count=1] )")

      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }


  //TC_1153
  test("TC_1153") {

    try {

      //sql("create cube cube118_drop dimensions (AMSize STRING) measures (Latest_Day INTEGER)
      // options (aggregation [Latest_Day = count] partitioner [class = 'org.carbondata
      // .spark.partition.api.impl.SampleDataPartitionerImpl', columns= (AMSize) ,
      // partition_count=1] )")

      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1154
  test("TC_1154") {
    sql(
      "create cube cube119_drop dimensions (AMSize STRING) measures (Latest_Day INTEGER) options " +
        "(aggregation [Latest_Day = count] partitioner [class = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl', columns= (AMSize) ,partition_count=1] )"
    )
    sql("drop cube cube119_drop")
  }

  //TC_1155
  test("TC_1155") {
    sql(
      "create cube cube120_drop dimensions(key string,name string) measures(gamepointid numeric," +
        "price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)"
    )
    sql("drop cube cube120_drop")
  }

  //DTS2015113006662
  test("DTS2015113006662") {
    sql(
      "CREATE CUBE default.t3 DIMENSIONS (imei String, productdate String, updatetime String, " +
        "gamePointId Integer, contractNumber Integer) MEASURES (deviceInformationId Integer) WITH" +
        " table4 RELATION (FACT.imei=imei) INCLUDE (imei, productdate, updatetime, gamePointId, " +
        "contractNumber)OPTIONS ( AGGREGATION[ deviceInformationId=count ] , PARTITIONER [ " +
        "CLASS='org.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl', " +
        "COLUMNS=(imei), PARTITION_COUNT=5 ] )"
    )
    sql("select * from t3")
    sql("drop cube default.t3")
  }

  //TC_1157
  test("TC_1157") {
    try {
      sql(
        "create cube vardhan12 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string)  measures(gamePointId numeric," +
          "deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) " +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan12 " +
          "partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,AMSize,channelsId," +
          "ActiveCountry,Activecity')"
      )
      checkAnswer(
        sql("select count(*) from vardhan12"),
        Seq()
      )
      sql("drop cube vardhan12")
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
        "create cube vardhan13 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string)  measures(gamePointId numeric," +
          "deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) " +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan13 " +
          "partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'deviceInformationId," +
          "gamePointId')"
      )
      checkAnswer(
        sql("select count(*) from vardhan13"),
        Seq()
      )
      sql("drop cube vardhan13")
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
        "create cube vardhan3 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string)  measures(gamePointId numeric," +
          "deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) " +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan3 " +
          "partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'deviceInformationId,AMSize," +
          "ActiveCountry,Activecity,gamePointId')"
      )
      checkAnswer(
        sql("select count(*) from vardhan3"),
        Seq()
      )
      sql("drop cube vardhan3")
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
        "create cube vardhan100 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string)  measures(gamePointId numeric," +
          "deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) " +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan100 " +
          "partitionData(DELIMITER ',' ,QUOTECHAR '\"',FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')"
      )
      checkAnswer(
        sql("select count(*) from vardhan100"),
        Seq()
      )
      sql("drop cube vardhan100")
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
        "create cube vardhan13 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string)  measures(gamePointId numeric," +
          "deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) " +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan13 " +
          "partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'deviceInformationId," +
          "gamePointId')"
      )
      checkAnswer(
        sql("select count(*) from vardhan13"),
        Seq()
      )
      sql("drop cube vardhan13")
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
        "create cube vardhan3 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string)  measures(gamePointId numeric," +
          "deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) " +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan3 " +
          "partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'deviceInformationId,AMSize," +
          "ActiveCountry,Activecity,gamePointId')"
      )
      checkAnswer(
        sql("select count(*) from vardhan3"),
        Seq()
      )
      sql("drop cube vardhan3")
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
        "create cube vardhan100 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string)  measures(gamePointId numeric," +
          "deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) " +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan100 " +
          "partitionData(DELIMITER ',' ,QUOTECHAR '\"',FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId')"
      )
      checkAnswer(
        sql("select count(*) from vardhan100"),
        Seq()
      )
      sql("drop cube vardhan100")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>
    }
  }

  //TC_1190
  test("TC_1190") {
    sql(
      "create cube vardhan200 dimensions(imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string)  measures(gamePointId numeric," +
        "deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
        ".spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei,AMSize) " +
        "PARTITION_COUNT=2] )"
    )
    sql(
      "LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan200 " +
        "partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId," +
        "AMSize,channelsId,ActiveCountry,Activecity,gamePointId')"
    )
    checkAnswer(
      sql("select count(*) from vardhan200"),
      Seq(Row(100))
    )
    sql("drop cube vardhan200")
  }

  //TC_1191
  test("TC_1191") {
    sql(
      "create cube vardhan500 dimensions(imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string, productionDate TIMESTAMP)  measures(gamePointId" +
        " numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
        ".spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) " +
        "PARTITION_COUNT=2] )"
    )
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd/MM/yyyy HH:mm")
    sql(
      "LOAD DATA FACT FROM  './src/test/resources/TestData6.csv' INTO Cube vardhan500 " +
        "partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId," +
        "AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
    )

    checkAnswer(
      sql("select count(*) from vardhan500"),
      Seq(Row(100))
    )
    sql("drop cube vardhan500")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
      );
  }

  //TC_1192
  test("TC_1192") {
    sql(
      "create cube vardhan1000 dimensions(imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string)  measures(gamePointId numeric," +
        "deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
        ".spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )"
    )
    sql(
      "LOAD DATA FACT FROM  './src/test/resources/TestData5.csv' INTO Cube vardhan1000 " +
        "partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId," +
        "AMSize,channelsId,ActiveCountry,Activecity,gamePointId')"
    )
    checkAnswer(
      sql("select count(*) from vardhan1000"),
      Seq(Row(97))
    )
    sql("drop cube vardhan1000")
  }

  //TC_1193
  test("TC_1193") {
    sql(
      "create cube vardhan9 dimensions(imei string,AMSize string,channelsId string,ActiveCountry " +
        "string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) " +
        "OPTIONS (PARTITIONER [CLASS = 'org.carbondata.spark.partition.api.impl" +
        ".SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )"
    )
    sql(
      "LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan9 partitionData" +
        "(DELIMITER ',' ,QUOTECHAR '/', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId," +
        "ActiveCountry,Activecity,gamePointId')"
    )
    checkAnswer(
      sql("select count(*) from vardhan9"),
      Seq(Row(100))
    )
    sql("drop cube vardhan9")
  }

  //TC_1194
  test("TC_1194") {
    sql(
      "create cube vardhan16 dimensions(AMSize STRING) measures(deviceInformationId integer) " +
        "OPTIONS (AGGREGATION [deviceInformationId=count])"
    )
    sql(
      "LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan16 " +
        "partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId," +
        "AMSize,channelsId,ActiveCountry,Activecity,gamePointId')"
    )
    checkAnswer(
      sql("select count(*) from vardhan16"),
      Seq(Row(100))
    )
    sql("drop cube vardhan16")
  }

  //TC_1195
  test("TC_1195") {
    sql(
      "create cube myschema.vardhan17 DIMENSIONS (AMSize STRING) MEASURES (deviceInformationId " +
        "integer) OPTIONS (AGGREGATION [deviceInformationId = count])"
    )
    sql(
      "LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube myschema.vardhan17 " +
        "partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId," +
        "AMSize,channelsId,ActiveCountry,Activecity,gamePointId')"
    )
    checkAnswer(
      sql("select count(*)  from myschema.vardhan17"),
      Seq(Row(100))
    )
    sql("drop cube myschema.vardhan17")
  }

  //DTS2015111808892
  test("DTS2015111808892") {
    sql(
      "CREATE CUBE cube_restructure DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS " +
        "(AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
    )
    sql(
      "LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE cube_restructure" +
        " PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"')"
    )
    checkAnswer(
      sql("select count(*)  from cube_restructure"),
      Seq(Row(100))
    )
    sql("drop cube cube_restructure")
  }

  //DTS2015111809054
  test("DTS2015111809054", NonRunningTests) {
    sql(
      "create cube cubeDTS2015111809054 dimensions(key string,name string) measures(gamepointid " +
        "numeric,price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)"
    )
    sql(
      "LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION " +
        "FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE cubeDTS2015111809054 " +
        "PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"', FILEHEADER '')"
    )
    checkAnswer(
      sql("select count(*)  from cubeDTS2015111809054"),
      Seq(Row(21))
    )
    sql("drop cube cubeDTS2015111809054")
  }

  //DTS2015112006803_01
  test("DTS2015112006803_01") {
    sql(
      "CREATE CUBE incloading_DTS2015112006803_01 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) " +
        "OPTIONS (AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
    )
    sql(
      "LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE " +
        "incloading_DTS2015112006803_01 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"')"
    )
    sql(
      "LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE " +
        "incloading_DTS2015112006803_01 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"')"
    )
    checkAnswer(
      sql("select count(*) from incloading_DTS2015112006803_01"),
      Seq(Row(200))
    )
    sql("drop cube incloading_DTS2015112006803_01")
  }


  //DTS2015112710336
  test("DTS2015112710336", NonRunningTests) {
    sql(
      "create cube rock dimensions(key string as col1,name string as col3) measures(gamepointid " +
        "numeric,price numeric) with dimFile RELATION (FACT.deviceid=col1) INCLUDE ( col1,col3)"
    )
    sql(
      "LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION " +
        "FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE rock OPTIONS(DELIMITER ',', " +
        "QUOTECHAR '\"\"', FILEHEADER '')"
    )
    checkAnswer(
      sql("select count(*)  from rock"),
      Seq(Row(21))
    )
    sql("drop cube rock")
  }

  //DTS2015111810813
  test("DTS2015111810813", NonRunningTests) {
    sql(
      "create cube single dimensions(imei string,deviceInformationId integer,mac string," +
        "productdate timestamp,updatetime timestamp) measures(gamePointId numeric,contractNumber " +
        "numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.spark.partition.api" +
        ".impl.SampleDataPartitionerImpl' ,columns= (imei) ,PARTITION_COUNT=2] )"
    )
    sql(
      "LOAD DATA fact from './src/test/resources/vmallFact_headr.csv' INTO CUBE single " +
        "PARTITIONDATA(DELIMITER '\001', QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId," +
        "mac,productdate,updatetime,gamePointId,contractNumber')"
    )
    checkAnswer(
      sql("select count(*) from single"),
      Seq(Row(10))
    )
    sql("drop cube single")
  }

  //DTS2015101504861
  test("DTS2015101504861", NonRunningTests) {
    sql(
      "create cube vard970 dimensions(imei string,productionDate timestamp,AMSize string," +
        "channelsId string,ActiveCountry string, Activecity string) measures(gamePointId numeric," +
        "deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
        ".spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ," +
        "PARTITION_COUNT=2] )"
    )
    sql(
      "LOAD DATA FACT FROM './src/test/resources/TestData2.csv' INTO CUBE vard970 OPTIONS" +
        "(DELIMITER ',', QUOTECHAR '\"', FILEHEADER 'imei,productionDate,deviceInformationId," +
        "AMSize,channelsId,ActiveCountry,Activecity,gamePointId')"
    )
    checkAnswer(
      sql("select imei from vard970 where productionDate='2015-07-06 12:07:00'"),
      Seq()
    )
    sql("drop cube vard970")
  }

  //TC_1326
  test("TC_1326", NonRunningTests) {
    sql(
      "create cube vardhanincomp dimensions(imei string,AMSize string,channelsId string," +
        "ActiveCountry string, Activecity string)  measures(gamePointId numeric," +
        "deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
        ".spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )"
    )
    sql(
      "LOAD DATA FACT FROM './src/test/resources/TestData4.csv' INTO Cube vardhanincomp OPTIONS" +
        "(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId," +
        "ActiveCountry,Activecity,gamePointId')"
    )
    checkAnswer(
      sql("select channelsId from vardhanincomp order by imei ASC limit 0"),
      Seq()
    )
    sql("drop cube vardhanincomp")
  }

  //TC_1327
  test("TC_1327", NonRunningTests) {
    sql(
      "create cube vardhan01 dimensions(key string,name string) measures(gamepointid numeric," +
        "price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)"
    )
    sql(
      "LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION " +
        "FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE vardhan01 PARTITIONDATA" +
        "(DELIMITER ',', QUOTECHAR '\"', FILEHEADER '')"
    )
    checkAnswer(
      sql("select count(*) from vardhan01"),
      Seq(Row(21))
    )
    sql("drop cube vardhan01")
  }

  //TC_1328
  test("TC_1328", NonRunningTests) {
    sql(
      "create cube vardhan01 dimensions(key string,name string as col1) measures(gamepointid " +
        "numeric,price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,col1)"
    )
    sql(
      "LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION " +
        "FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE vardhan01 PARTITIONDATA" +
        "(DELIMITER ',', QUOTECHAR '\"', FILEHEADER '')"
    )
    checkAnswer(
      sql("select count(*) from vardhan01"),
      Seq(Row(21))
    )
    sql("drop cube vardhan01")
  }


  //DTS2015112009008
  test("DTS2015112009008", NonRunningTests) {
    sql(
      "CREATE CUBE cube_restructure60 DIMENSIONS (AMSize STRING) MEASURES (Latest_DAY INTEGER) " +
        "OPTIONS (AGGREGATION [Latest_DAY = count] PARTITIONER [CLASS = 'org.carbondata" +
        ".spark.partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ," +
        "PARTITION_COUNT=1] )"
    )
    sql(
      "LOAD DATA FACT FROM './src/test/resources/create_cube.csv' INTO CUBE cube_restructure60 " +
        "PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"\"')"
    )
    sql("alter cube cube_restructure60 add dimensions(a1 string)")
    sql("select count(*) from cube_restructure60")
    sql("alter cube cube_restructure60 drop(a1)")
    checkAnswer(
      sql("select count(*) from cube_restructure60"),
      Seq(Row(200))
    )
    sql("drop cube cube_restructure60")
  }

  //DTS2015120304016
  test("DTS2015120304016") {
    sql(
      "CREATE CUBE incloading1 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION " +
        "[b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark.partition.api.impl" +
        ".SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
    )
    sql(
      "LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE incloading1 " +
        "PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')"
    )
    sql("drop cube incloading1")
    sql(
      "CREATE CUBE incloading1 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS (AGGREGATION " +
        "[b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark.partition.api.impl" +
        ".SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
    )
    sql(
      "LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE incloading1 " +
        "PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')"
    )
    checkAnswer(
      sql("select count(*) from incloading1"),
      Seq(Row(100))
    )
    sql("drop cube incloading1")
  }

  //DTS2015110311277
  test("DTS2015110311277", NonRunningTests) {
    sql(
      "create cube vardhan dimensions(imei string,AMSize string,channelsId string,ActiveCountry " +
        "string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) " +
        "OPTIONS (PARTITIONER [CLASS = 'org.carbondata.spark.partition.api.impl" +
        ".SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )"
    )
    sql(
      "LOAD DATA FACT FROM './src/test/resources/TestData1.csv' INTO Cube vardhan OPTIONS" +
        "(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize," +
        "channelsId,ActiveCountry,Activecity,gamePointId')"
    )
    sql("alter cube vardhan add dimensions(alreadID)")
    sql("alter cube vardhan drop (alreadID)")
    sql(
      "LOAD DATA FACT FROM './src/test/resources/TestData1.csv' INTO Cube vardhan OPTIONS" +
        "(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId," +
        "ActiveCountry,Activecity,gamePointId')"
    )
    checkAnswer(
      sql("select count(*) from vardhan"),
      Seq(Row(200))
    )
    sql("drop cube vardhan")
  }

  //DTS2015121511752
  test("DTS2015121511752") {
    sql(
      "CREATE CUBE cube_restructure68 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS " +
        "(AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
    )
    sql(
      "LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE " +
        "cube_restructure68 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')"
    )
    sql(
      "alter cube cube_restructure68 add dimensions(a10 string) measures(b9 integer) options " +
        "(AGGREGATION [b9 = MAX])"
    )
    sql(" drop cube cube_restructure68")
    sql(
      "CREATE CUBE cube_restructure68 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS " +
        "(AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
        ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
    )
    sql(
      "LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE " +
        "cube_restructure68 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"')"
    )
    checkAnswer(
      sql("select * from cube_restructure68 order by a0 ASC limit 3"),
      Seq(Row("1AA1", 2738.0), Row("1AA10", 1714.0), Row("1AA100", 1271.0))
    )
    sql("drop cube cube_restructure68")
  }

  //TC_1329
  test("TC_1329", NonRunningTests) {
    sql("drop cube IF EXISTS vardhan01")
    sql(
      "create cube vardhan01 dimensions(key string,name string) measures(gamepointid numeric," +
        "price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)"
    )
    sql(
      "alter cube vardhan01 add dimensions(productiondate timestamp) options (AGGREGATION [b = " +
        "SUM])"
    )
    sql(
      "LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION " +
        "FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE vardhan01 PARTITIONDATA" +
        "(DELIMITER ',', QUOTECHAR '\"', FILEHEADER '')"
    )
    checkAnswer(
      sql("select key from vardhan01 order by key ASC limit 1"),
      Seq(Row("1"))
    )
    sql("drop cube vardhan01")
  }

  //TC_1330
  test("TC_1330", NonRunningTests) {
    sql("drop cube IF EXISTS vardhan01")
    sql(
      "create cube vardhan01 dimensions(key string,name string) measures(gamepointid numeric," +
        "price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)"
    )
    sql(
      "LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION " +
        "FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE vardhan01 PARTITIONDATA" +
        "(DELIMITER ',', QUOTECHAR '\"', FILEHEADER '')"
    )
    sql(
      "alter cube vardhan01 add dimensions(productiondate timestamp) options (AGGREGATION [b = " +
        "SUM])"
    )
    sql(
      "LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION " +
        "FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE vardhan01 PARTITIONDATA" +
        "(DELIMITER ',', QUOTECHAR '\"', FILEHEADER '')"
    )
    checkAnswer(
      sql("select count(*) from vardhan01"),
      Seq(Row(42))
    )
    sql("drop cube vardhan01")
  }

  //TC_1331
  test("TC_1331", NonRunningTests) {
    sql("drop cube IF EXISTS vardhan01")
    sql(
      "create cube vardhan01 dimensions(key string,name string) measures(gamepointid numeric," +
        "price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)"
    )
    sql(
      "LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION " +
        "FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE vardhan01 PARTITIONDATA" +
        "(DELIMITER ',', QUOTECHAR '\"', FILEHEADER '')"
    )
    sql(
      "alter cube vardhan01 add dimensions(productiondate timestamp as col5) options (AGGREGATION" +
        " [b = SUM])"
    )
    sql(
      "LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION " +
        "FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE vardhan01 PARTITIONDATA" +
        "(DELIMITER ',', QUOTECHAR '\"', FILEHEADER '')"
    )
    checkAnswer(
      sql("select count(*) from vardhan01"),
      Seq(Row(42))
    )
    sql("drop cube vardhan01")
  }


  //TC_1332
  test("TC_1332") {

    try {
      sql(
        "create cube vardhan01 dimensions(key string,name string) measures(gamepointid numeric," +
          "price numeric) with dimFile RELATION (FACT.deviceid=key) INCLUDE ( key,name)"
      )
      sql("alter cube vardhan01 drop (name)")
      sql("alter cube vardhan01 add dimensions(name as name) options (AGGREGATION [b = SUM])")
      sql(
        "LOAD DATA FACT FROM './src/test/resources/100_default_date_11_Withheaders.csv' DIMENSION" +
          " FROM dimFile:'./src/test/resources/dimFile.csv' INTO CUBE vardhan01 PARTITIONDATA" +
          "(DELIMITER ',', QUOTECHAR '\"', FILEHEADER '')"
      )
      sql("drop cube vardhan01")
      fail("Unexpected behavior")
    }
    catch {

      case ex: Throwable => sql("drop cube vardhan01")
    }

  }


  //TC_1160
  test("TC_1160") {
    try {
      sql(
        "create cube vardhan4 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string)  measures(gamePointId numeric," +
          "deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) " +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA './src/test/resources/TestData1.csv' INTO Cube vardhan4 partitionData" +
          "(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize," +
          "channelsId,ActiveCountry,Activecity,gamePointId')"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube vardhan4")

    }
  }

  //TC_1161
  test("TC_1161") {
    try {
      sql(
        "create cube vardhan5 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string)  measures(gamePointId numeric," +
          "deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) " +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM  INTO Cube vardhan5 partitionData(DELIMITER ',' ,QUOTECHAR '\"', " +
          "FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity," +
          "gamePointId')"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube vardhan5")

    }
  }

  //TC_1162
  test("TC_1162") {
    try {
      sql(
        "create cube vardhan1 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string)  measures(gamePointId numeric," +
          "deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) " +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube  partitionData" +
          "(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize," +
          "channelsId,ActiveCountry,Activecity,gamePointId')"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube vardhan1")

    }
  }

  //TC_1164
  test("TC_1164") {
    try {
      sql(
        "create cube vardhan1 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string)  measures(gamePointId numeric," +
          "deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) " +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "\"LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan1 " +
          "(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize," +
          "channelsId,ActiveCountry,Activecity,gamePointId')"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube vardhan1")

    }
  }

  //TC_1165
  test("TC_1165") {
    try {
      sql(
        "create cube vardhan10 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string)  measures(gamePointId numeric," +
          "deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) " +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan10 " +
          "partitionData(QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId," +
          "ActiveCountry,Activecity,gamePointId')"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube vardhan10")

    }
  }

  //TC_1180
  test("TC_1180") {
    try {
      sql(
        "create cube vardhan4 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string)  measures(gamePointId numeric," +
          "deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) " +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA './src/test/resources/TestData1.csv' INTO Cube vardhan4 partitionData" +
          "(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize," +
          "channelsId,ActiveCountry,Activecity,gamePointId')"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube vardhan4")

    }
  }

  //TC_1181
  test("TC_1181") {
    try {
      sql(
        "create cube vardhan5 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string)  measures(gamePointId numeric," +
          "deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) " +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM  INTO Cube vardhan5 partitionData(DELIMITER ',' ,QUOTECHAR '\"', " +
          "FILEHEADER 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity," +
          "gamePointId')"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube vardhan5")

    }
  }

  //TC_1182
  test("TC_1182") {
    try {
      sql(
        "create cube vardhan1 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string)  measures(gamePointId numeric," +
          "deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) " +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube  partitionData" +
          "(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize," +
          "channelsId,ActiveCountry,Activecity,gamePointId')"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube vardhan1")

    }
  }

  //TC_1184
  test("TC_1184") {
    try {
      sql(
        "create cube vardhan1 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string)  measures(gamePointId numeric," +
          "deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) " +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "\"LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan1 " +
          "(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize," +
          "channelsId,ActiveCountry,Activecity,gamePointId')"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube vardhan1")

    }
  }

  //TC_1185
  test("TC_1185") {
    try {
      sql(
        "create cube vardhan10 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string)  measures(gamePointId numeric," +
          "deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) " +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube vardhan10 " +
          "partitionData(QUOTECHAR '\"', FILEHEADER 'imei,deviceInformationId,AMSize,channelsId," +
          "ActiveCountry,Activecity,gamePointId')"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube vardhan10")

    }
  }

  //DTS2015111900020
  test("DTS2015111900020") {
    try {
      sql(
        "CREATE CUBE testwithout_measure DIMENSIONS (AMSize STRING) OPTIONS (AGGREGATION " +
          "[Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.spark.partition" +
          ".api.impl.SampleDataPartitionerImpl', COLUMNS= (AMSize) ,PARTITION_COUNT=1] )"
      )
      sql(
        "LOAD DATA FACT FROM './src/test/resources/create_cube.csv' INTO CUBE testwithout_measure" +
          " PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"')"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube testwithout_measure")

    }
  }

  //TC_1199
  test("TC_1199") {
    try {
      sql(
        "CREATE CUBE cube_restructure4 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS " +
          "(AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
          ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
      )
      sql(
        "LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE " +
          "cube_restructure4 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')\""
      )
      sql("alter cube cube_restructure4 add dimensions(a2 string) options (AGGREGATION [a2 = SUM])")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube_restructure4")

    }
  }

  //TC_1201
  test("TC_1201") {
    try {
      sql(
        "CREATE CUBE cube_restructure6 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS " +
          "(AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
          ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
      )
      sql(
        "LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE " +
          "cube_restructure6 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')\""
      )
      sql(
        "alter cube cube_restructure6 add dimensions(a4 string, a5 integer, a3 timestamp) " +
          "measures(b3 integer, b4 double) options (AGGREGATION [b3 = SUM])"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube_restructure6")

    }
  }

  //TC_1202
  test("TC_1202") {
    try {
      sql(
        "CREATE CUBE cube_restructure7 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS " +
          "(AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
          ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
      )
      sql(
        "LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE " +
          "cube_restructure7 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')\""
      )
      sql("alter cube cube_restructure7 add dimensions(a6 double,a7 long)")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube_restructure7")

    }
  }

  //TC_1203
  test("TC_1203") {
    try {
      sql(
        "CREATE CUBE cube_restructure8 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS " +
          "(AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
          ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
      )
      sql(
        "LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE " +
          "cube_restructure8 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')\""
      )
      sql("alter cube cube_restructure add measures(b5 string, b6 timestamp)")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube_restructure8")

    }
  }

  //TC_1215
  test("TC_1215") {
    try {
      sql(
        "CREATE CUBE cube_restructure20 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS " +
          "(AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
          ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
      )
      sql(
        "LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE " +
          "cube_restructure20 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')\""
      )
      sql(
        "alter cube cube_restructure20 add dimensions(a22 string, a23 integer, a24 timestamp) " +
          "measures(b18 integer, b19 double) options (defaults [\"a22=test\",\"a23=test1\"," +
          "\"a24=27-04-1979\",\"b18=10\",\"b19=10\"])"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube_restructure20")

    }
  }

  //TC_1216
  test("TC_1216") {
    try {
      sql(
        "CREATE CUBE cube_restructure21 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS " +
          "(AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
          ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
      )
      sql(
        "LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE " +
          "cube_restructure21 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')\""
      )
      sql(
        "alter cube cube_restructure21 add dimensions(a25 string, a26 integer, a27 timestamp) " +
          "measures(b20 integer, b21 double) options (AGGREGATION [b20 = COUNT]) (defaults " +
          "[a25=10])"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube_restructure21")

    }
  }

  //TC_1221
  test("TC_1221") {
    try {
      sql(
        "CREATE CUBE cube_restructure26 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS " +
          "(AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
          ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
      )
      sql(
        "LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE " +
          "cube_restructure26 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"')\""
      )
      sql(
        "alter cube add dimensions(a36 string, a37 integer, a38 timestamp) measures(b37 integer, " +
          "b38 double) options (AGGREGATION [\"b37 = COUNT\"]) (defaults [\"a36=10\"])"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube_restructure26")

    }
  }

  //TC_1222
  test("TC_1222") {
    try {
      sql(
        "alter cube invalidcube add dimensions(a36 string, a37 integer, a38 timestamp) measures" +
          "(b37 integer, b38 double) options (AGGREGATION [\"b37 = COUNT\"]) (defaults " +
          "[\"a36=10\"])"
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable =>

    }
  }

  //TC_1242
  test("TC_1242") {
    try {
      sql(
        "CREATE CUBE cube_restructure47 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS " +
          "(AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
          ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
      )
      sql("alter cube cube_restructure47 add dimensions(a0 string) measures(b0 integer)")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube_restructure47")

    }
  }

  //TC_1244
  test("TC_1244") {
    try {
      sql(
        "CREATE CUBE cube_restructure49 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS " +
          "(AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
          ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
      )
      sql("alter cube cube_restructure49 drop(a1,b1)")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube_restructure49")

    }
  }

  //TC_1245
  test("TC_1245") {
    try {
      sql(
        "CREATE CUBE cube_restructure50 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS " +
          "(AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
          ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
      )
      sql("alter cube cube_restructure50 drop()")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube_restructure50")

    }
  }

  //DTS2015102211467_01
  test("DTS2015102211467_01") {
    try {
      sql(
        "CREATE CUBE cube_restructure51 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS " +
          "(AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
          ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
      )
      sql("alter cube cube_restructure51 DROP (b0)")
      sql("alter cube cube_restructure51 DROP (b0)")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube_restructure51")

    }
  }

  //DTS2015102211467_02
  test("DTS2015102211467_02") {
    try {
      sql(
        "CREATE CUBE cube_restructure52 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS " +
          "(AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
          ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
      )
      sql("alter cube cube_restructure52 DROP (b1)")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube_restructure52")

    }
  }

  //DTS2015102211467_03
  test("DTS2015102211467_03") {
    try {
      sql(
        "CREATE CUBE cube_restructure53 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS " +
          "(AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
          ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
      )
      sql("alter cube cube_restructure53 DROP ()")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube_restructure53")

    }
  }

  //DTS2015102211545
  test("DTS2015102211545") {
    try {
      sql(
        "CREATE CUBE cube_restructure54 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS " +
          "(AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
          ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
      )
      sql("alter cube cube_restructure54 DROP (a0)")
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube_restructure54")

    }
  }

  //TC_1247
  test("TC_1247") {

    try {
      sql("create schema IF NOT EXISTS myschema1")
      sql(
        "create cube myschema1.cube2 DIMENSIONS (imei string,deviceInformationId integer,MAC " +
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
          "Active_phonePADPartitionedVersions string, Latest_YEAR  integer, Latest_MONTH integer," +
          " Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, " +
          "Latest_province string, Latest_city string, Latest_district string, Latest_street " +
          "string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion " +
          "string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion " +
          "string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
          "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
          "Latest_operatorId string, gamePointDescription string) MEASURES (gamePointId numeric," +
          "contractNumber numeric) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS =" +
          " 'org.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl', " +
          "COLUMNS= (imei) ,PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM './src/test/resources/100.csv' INTO CUBE myschema1.cube2 " +
          "PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"', FILEHEADER 'imei,deviceInformationId," +
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
      checkAnswer(
        sql("SHOW LOADS for cube myschema1.cube2"),
        Seq(Row("0", "Success", "2015-11-05 15:01:23.0", " 2015-11-05 15:01:26.0"))
      )
    }
    catch {
      case ex: Throwable => sql("drop cube myschema1.cube2")

    }

  }

  //TC_1255
  test("TC_1255") {
    try {
      sql(
        "create cube cube10 dimensions(imei string,AMSize string,channelsId string,ActiveCountry " +
          "string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) " +
          "OPTIONS (PARTITIONER [CLASS = 'org.carbondata.spark.partition.api.impl" +
          ".SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )"
      )
      checkAnswer(
        sql("show loads for cube cubedoesnotexist"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube10")

    }
  }

  //TC_1256
  test("TC_1256") {
    try {
      sql(
        "create cube cube11 dimensions(imei string,AMSize string,channelsId string,ActiveCountry " +
          "string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) " +
          "OPTIONS (PARTITIONER [CLASS = 'org.carbondata.spark.partition.api.impl" +
          ".SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )"
      )
      checkAnswer(
        sql("show loads for cube"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube11")

    }
  }

  //TC_1272
  test("TC_1272") {
    try {
      sql("create schema IF NOT EXISTS  myschema1")
      sql(
        "create cube myschema1.cube2 DIMENSIONS (imei string,deviceInformationId integer,MAC " +
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
          "Active_phonePADPartitionedVersions string, Latest_YEAR  integer, Latest_MONTH integer," +
          " Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, " +
          "Latest_province string, Latest_city string, Latest_district string, Latest_street " +
          "string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion " +
          "string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion " +
          "string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
          "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
          "Latest_operatorId string, gamePointDescription string) MEASURES (gamePointId numeric," +
          "contractNumber numeric) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS =" +
          " 'org.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl', " +
          "COLUMNS= (imei) ,PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM './src/test/resources/100.csv' INTO CUBE myschema1.cube2 " +
          "PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"', FILEHEADER 'imei,deviceInformationId," +
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
      sql("create aggregatetable Latest_DAY,sum(contractNumber)from cube myschema1.cube2")
      checkAnswer(
        sql("select Latest_DAY from myschema1.cube2 limit 1"),
        Seq(Row(1))
      )
    }
    catch {
      case ex: Throwable => sql("drop cube myschema1.cube2")

    }

  }

  //TC_1273
  test("TC_1273") {
    try {
      sql(
        "create cube cube3 dimensions(imei string,AMSize string,channelsId string,ActiveCountry " +
          "string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) " +
          "OPTIONS (PARTITIONER [CLASS = 'org.carbondata.spark.partition.api.impl" +
          ".SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube3 partitionData" +
          "(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize," +
          "channelsId,ActiveCountry,Activecity,gamePointId')"
      )
      sql("create aggregatetable Latest_MONTH,sum(contractNumber)from cube cube3")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube3")

    }
  }

  //TC_1274
  test("TC_1274") {
    try {
      sql(
        "create cube myschema1.cube4 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string)  measures(gamePointId numeric," +
          "deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' columns= (imei) " +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube myschema1.cube4 " +
          "partitionData(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId," +
          "AMSize,channelsId,ActiveCountry,Activecity,gamePointId') \""
      )
      sql("create aggregatetable Latest_DAY,sum(contractNumber)from cube myschema1.cube4")

      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube myschema1.cube4")

    }
  }

  //TC_1287
  test("TC_1287") {
    try {
      sql(
        "create cube cube17 dimensions(imei string,AMSize string,channelsId string,ActiveCountry " +
          "string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) " +
          "OPTIONS (PARTITIONER [CLASS = 'org.carbondata.spark.partition.api.impl" +
          ".SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube17 partitionData" +
          "(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize," +
          "channelsId,ActiveCountry,Activecity,gamePointId')"
      )
      sql("create aggregatetable Latest_Day,max(gamePointId)from cube cube17")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube17")

    }
  }

  //TC_1290
  test("TC_1290") {
    try {
      sql(
        "create cube cube20 dimensions(imei string,AMSize string,channelsId string,ActiveCountry " +
          "string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) " +
          "OPTIONS (PARTITIONER [CLASS = 'org.carbondata.spark.partition.api.impl" +
          ".SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )"
      )
      sql("create aggregatetable ActiveCountry,max(gamePointId)from cube20")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube20")

    }
  }

  //TC_1292
  test("TC_1292") {
    try {
      sql(
        "create cube myschema1.cube2 DIMENSIONS (imei string,deviceInformationId integer,MAC " +
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
          "Active_phonePADPartitionedVersions string, Latest_YEAR  integer, Latest_MONTH integer," +
          " Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, " +
          "Latest_province string, Latest_city string, Latest_district string, Latest_street " +
          "string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion " +
          "string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion " +
          "string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
          "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
          "Latest_operatorId string, gamePointDescription string) MEASURES (gamePointId numeric," +
          "contractNumber numeric) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS =" +
          " 'org.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl', " +
          "COLUMNS= (imei) ,PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM './src/test/resources/100.csv' INTO CUBE myschema1.cube2 " +
          "PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"', FILEHEADER 'imei,deviceInformationId," +
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
      sql("delete load 0 from  cube myschema1.cube2")
      checkAnswer(
        sql("select deviceInformationId from myschema1.cube2"),
        Seq()
      )
    }
    catch {
      case ex: Throwable => sql("drop cube myschema1.cube2")

    }

  }

  //TC_1307
  test("TC_1307") {
    try {
      sql(
        "create cube cube17 dimensions(imei string,AMSize string,channelsId string,ActiveCountry " +
          "string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) " +
          "OPTIONS (PARTITIONER [CLASS = 'org.carbondata.spark.partition.api.impl" +
          ".SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM  './src/test/resources/TestData1.csv' INTO Cube cube17 partitionData" +
          "(DELIMITER ',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize," +
          "channelsId,ActiveCountry,Activecity,gamePointId')"
      )
      sql("delete load 1 from  cube cube17")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube17")

    }
  }

  //TC_1310
  test("TC_1310") {
    try {
      sql(
        "create cube cube20 dimensions(imei string,AMSize string,channelsId string,ActiveCountry " +
          "string, Activecity string)  measures(gamePointId numeric,deviceInformationId integer) " +
          "OPTIONS (PARTITIONER [CLASS = 'org.carbondata.spark.partition.api.impl" +
          ".SampleDataPartitionerImpl' columns= (imei) PARTITION_COUNT=2] )"
      )
      sql("delete load 1 from cube cube20")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube20")

    }
  }

  //TC_1313
  test("TC_1313") {
    try {
      sql(
        "create cube vardhanretention2 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId" +
          " numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ," +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention2 " +
          "OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize," +
          "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
      )
      sql(
        "LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention2 " +
          "OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize," +
          "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
      )
      sql("delete load 2 from cube vardhanretention2")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube vardhanretention2")

    }
  }

  //TC_1315
  test("TC_1315") {
    try {
      sql(
        "create cube vardhanretention4 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId" +
          " numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ," +
          "PARTITION_COUNT=2] )"
      )
      sql("delete load 0 from cube vardhanretention4")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube vardhanretention4")

    }
  }

  //TC_1317
  test("TC_1317") {
    try {
      sql(
        "create cube vardhanretention14 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId" +
          " numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ," +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention14 " +
          "OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize," +
          "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
      )
      sql("delete load 0 from cube vardhanretention14")
      sql("delete load 0 from cube vardhanretention14")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube vardhanretention14")

    }
  }

  //TC_1319
  test("TC_1319") {
    try {
      sql(
        "create cube vardhanretention15 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId" +
          " numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ," +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention15 " +
          "OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize," +
          "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
      )
      sql("delete load 0,1 from cube vardhanretention15")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube vardhanretention15")

    }
  }

  //TC_1320
  test("TC_1320") {
    try {
      sql(
        "create cube vardhanretention7 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId" +
          " numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ," +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention7 " +
          "OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize," +
          "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
      )
      sql("delete from cube vardhanretention7 where productionDate before '2015-07-05 '")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube vardhanretention7")

    }
  }

  //TC_1321
  test("TC_1321") {
    try {
      sql(
        "create cube vardhanretention8 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId" +
          " numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ," +
          "PARTITION_COUNT=2] )"
      )
      sql("delete from cube vardhanretention8 where productionDate before '2015-07-05 '")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube vardhanretention8")

    }
  }

  //TC_1323
  test("TC_1323") {
    try {
      sql(
        "create cube vardhanretention10 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId" +
          " numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ," +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention10 " +
          "OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize," +
          "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
      )
      sql("delete from cube vardhanretention10 where productionDate before ''")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube vardhanretention10")

    }
  }

  //TC_1324
  test("TC_1324") {
    try {
      sql(
        "create cube vardhanretention12 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId" +
          " numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ," +
          "PARTITION_COUNT=2] )"
      )
      sql(
        "LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention12 " +
          "OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize," +
          "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
      )
      sql("delete from cube vardhanretention12 where productionDate before '10-06-2015 12:07:28'")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube vardhanretention12")

    }
  }

  //TC_1325
  test("TC_1325") {
    try {
      sql(
        "create cube vardhanretention13 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId" +
          " numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ," +
          "PARTITION_COUNT=2] )"
      )
      CarbonProperties.getInstance().
        addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
          CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
        )
      sql(
        "LOAD DATA FACT FROM './src/test/resources/TestData3.csv' INTO CUBE vardhanretention13 " +
          "OPTIONS(DELIMITER ',', QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,AMSize," +
          "channelsId,ActiveCountry,Activecity,gamePointId,productionDate')"
      )
      sql("delete from cube vardhanretention13 where productionDate before '2013-13-13 12:07:28 '")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube vardhanretention13")

    }
  }

  //DTS2015112608945
  test("DTS2015112608945") {
    try {
      sql(
        "create cube babu_67 dimensions(imei string,deviceInformationId integer,mac string," +
          "productdate timestamp,updatetime timestamp) measures(gamePointId numeric," +
          "contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ," +
          "PARTITION_COUNT=2] )"
      )
      sql("delete from cube babu_67 where productdate before '2015-01-10 19:59:00'")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube babu_67")

    }
  }

  //DTS2015102804722
  test("DTS2015102804722") {
    try {
      sql(
        "create CUBE cube_restructure62 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS " +
          "(AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
          ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
      )
      sql(
        "LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE " +
          "cube_restructure62 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"', FILEHEADER 'a0,b0')"
      )
      sql("delete load 0 from cube cube_restructure62\"")
      sql("delete load 0 from cube cube_restructure62")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube_restructure62")

    }
  }

  //DTS2015111403600
  test("DTS2015111403600") {
    try {
      sql(
        "create CUBE cube_restructure64 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS " +
          "(AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
          ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
      )
      sql(
        "LOAD DATA FACT FROM './src/test/resources/restructure_cube.csv' INTO CUBE " +
          "cube_restructure64 PARTITIONDATA(DELIMITER ',', QUOTECHAR  '\"\"', FILEHEADER 'a0,b0')"
      )
      sql("delete load 0 from cube cube_restructure64")
      sql("clean files for cube cube_restructure64\"")
      sql("\"delete load 0 from cube cube_restructure64")
      sql("delete load 1 from cube cube_restructure64\"")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube_restructure64")

    }
  }

  //DTS2015112703910
  test("DTS2015112703910") {
    try {
      sql(
        "CREATE CUBE cube_restructure65 DIMENSIONS (a0 STRING) MEASURES (b0 INTEGER) OPTIONS " +
          "(AGGREGATION [b0 = count] PARTITIONER [CLASS = 'org.carbondata.spark" +
          ".partition.api.impl.SampleDataPartitionerImpl', COLUMNS= (a0) ,PARTITION_COUNT=1] )"
      )
      sql("delete load 1 from cube cube_restructure65")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube cube_restructure65")

    }
  }

  //DTS2015110209665
  test("DTS2015110209665") {
    try {
      sql(
        "create cube vardhanretention13 dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId" +
          " numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ," +
          "PARTITION_COUNT=2] )"
      )
      sql("delete from cube vardhanretention13 where productionDate before '2013-13-13 12:07:28 '")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube vardhanretention13")

    }
  }

  //TC_1346
  test("TC_1346") {
    try {
      sql(
        "create cube vardhanretention dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId" +
          " numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ," +
          "PARTITION_COUNT=2] )"
      )
      sql("delete load 0 from cube vardhanretention")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube vardhanretention")

    }
  }

  //TC_1348
  test("TC_1348") {
    try {
      sql(
        "create cube vardhanretention dimensions(imei string,AMSize string,channelsId string," +
          "ActiveCountry string, Activecity string,productionDate timestamp) measures(gamePointId" +
          " numeric,deviceInformationId integer) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' ,columns= (imei) ," +
          "PARTITION_COUNT=2] )"
      )
      sql("alter cube vardhanretention drop(AMSize) add dimensions(AMSize string)")
      sql("delete load 0 from cube vardhanretention")
      checkAnswer(
        sql("NA"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube vardhanretention")

    }
  }

  //TC_1267
  test("TC_1267") {
    try {
      sql("CREATE DATABASE IF NOT EXISTS my")
      sql(
        "create cube Carbon01 dimensions(imei string,deviceInformationId integer,MAC string," +
          "deviceColor string,device_backColor string,modelId string,marketName string,AMSize " +
          "string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate " +
          "timestamp,bomCode string,internalModels string, deliveryTime string, channelsId " +
          "string, channelsName string , deliveryAreaId string, deliveryCountry string, " +
          "deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet " +
          "string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, " +
          "ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string," +
          " ActiveStreet string, ActiveOperatorId string, Active_releaseId string, " +
          "Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, " +
          "Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string," +
          "Active_webTypeDataVerNumber string, Active_operatorsVersion string, " +
          "Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, " +
          "Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, " +
          "Latest_province string, Latest_city string, Latest_district string, Latest_street " +
          "string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion " +
          "string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion " +
          "string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
          "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
          "Latest_operatorId string, gamePointDescription string) measures(gamePointId numeric," +
          "contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (imei) , " +
          "PARTITION_COUNT=2] )"
      )
      checkAnswer(
        sql("SUGGEST AGGREGATE with scripts for cube my.Carbon01"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube Carbon01")

    }
  }

  //TC_1269
  test("TC_1269") {
    try {
      sql(
        "create cube Carbon01 dimensions(imei string,deviceInformationId integer,MAC string," +
          "deviceColor string,device_backColor string,modelId string,marketName string,AMSize " +
          "string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate " +
          "timestamp,bomCode string,internalModels string, deliveryTime string, channelsId " +
          "string, channelsName string , deliveryAreaId string, deliveryCountry string, " +
          "deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet " +
          "string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, " +
          "ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string," +
          " ActiveStreet string, ActiveOperatorId string, Active_releaseId string, " +
          "Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, " +
          "Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string," +
          "Active_webTypeDataVerNumber string, Active_operatorsVersion string, " +
          "Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, " +
          "Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, " +
          "Latest_province string, Latest_city string, Latest_district string, Latest_street " +
          "string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion " +
          "string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion " +
          "string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
          "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
          "Latest_operatorId string, gamePointDescription string) measures(gamePointId numeric," +
          "contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (imei) , " +
          "PARTITION_COUNT=2] )"
      )
      CarbonProperties.getInstance().
        addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
          CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
        )
      sql(
        "LOAD DATA FACT FROM './src/test/resources/100.csv' INTO Cube Carbon01 OPTIONS(DELIMITER " +
          "',' ,QUOTECHAR '\"\"', FILEHEADER 'imei,deviceInformationId,MAC,deviceColor," +
          "device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series," +
          "productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName," +
          "deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict," +
          "deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId," +
          "ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId," +
          "Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')"
      )
      checkAnswer(
        sql("SUGGEST AGGREGATE with scripts for cube Carbon01"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube Carbon01")

    }
  }

  //DTS2015112010240
  test("DTS2015112010240") {
    try {
      sql(
        "create cube twinkles DIMENSIONS (imei String,uuid String,MAC String,device_color String,device_shell_color String,device_name String,product_name String,ram String,rom String,cpu_clock String,series String,check_date String,check_year String,check_month String,check_day String,check_hour String,bom String,inside_name String,packing_date String,packing_year String,packing_month String,packing_day String,packing_hour String,customer_name String,deliveryAreaId String,deliveryCountry String,deliveryProvince String,deliveryCity String,deliveryDistrict String,packing_list_no String,order_no String,Active_check_time String,Active_check_year String,Active_check_month String,Active_check_day String,Active_check_hour String,ActiveAreaId String,ActiveCountry String,ActiveProvince String,Activecity String,ActiveDistrict String,Active_network String,Active_firmware_version String,Active_emui_version String,Active_os_version String,Latest_check_time String,Latest_check_year String,Latest_check_month String,Latest_check_day String,Latest_check_hour String,Latest_areaId String,Latest_country String,Latest_province String,Latest_city String,Latest_district String,Latest_firmware_version String,Latest_emui_version String,Latest_os_version String,Latest_network String,site String,site_desc String,product String,product_desc String) OPTIONS (AGGREGATION [Latest_Day = count] PARTITIONER [CLASS = 'org.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl' COLUMNS= (imei) PARTITION_COUNT=1] )"
      )
      checkAnswer(
        sql("SUGGEST AGGREGATE WITH SCRIPTS USING DATA_STATS FOR cube twinkles"),
        Seq()
      )
      fail("Unexpected behavior")
    }
    catch {
      case ex: Throwable => sql("drop cube twinkles")

    }
  }

}