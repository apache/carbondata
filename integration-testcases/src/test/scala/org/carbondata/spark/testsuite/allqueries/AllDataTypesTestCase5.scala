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
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties

/**
  * Test Class for all queries on multiple datatypes
  * Manohar
  */
class AllDataTypesTestCase5 extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {

    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
      )

    try {
      sql(
        "create cube  Carbon_automation_vmall_test1 dimensions(imei string,deviceInformationId " +
          "integer,MAC string,deviceColor string,device_backColor string,modelId string," +
          "marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string," +
          "series string,productionDate timestamp,bomCode string,internalModels string, " +
          "deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, " +
          "deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict " +
          "string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, " +
          "ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, " +
          "ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId " +
          "string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber " +
          "string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer" +
          " string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, " +
          "Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, " +
          "Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, " +
          "Latest_province string, Latest_city string, Latest_district string, Latest_street " +
          "string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion " +
          "string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion " +
          "string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
          "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
          "Latest_operatorId string, gamePointDescription string)  measures(gamePointId numeric," +
          "contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (imei) , " +
          "PARTITION_COUNT=2] )"

      )
      sql("LOAD DATA FACT FROM '" + currentDirectory + "/src/test/resources/Vmall_100_olap.csv' " +
        "INTO Cube Carbon_automation_vmall_test1 partitionData(DELIMITER ',' ,QUOTECHAR '\"', " +
        "FILEHEADER 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId," +
        "marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode," +
        "internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry," +
        "deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber," +
        "contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity," +
        "ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion," +
        "Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion," +
        "Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion," +
        "Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR," +
        "Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street," +
        "Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber," +
        "Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer," +
        "Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions," +
        "Latest_operatorId,gamePointId,gamePointDescription')");
      sql(
        "create cube Carbon_automation_test5 dimensions(imei string,deviceInformationId integer," +
          "MAC string,deviceColor string,device_backColor string,modelId string,marketName " +
          "string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string," +
          "productionDate timestamp,bomCode string,internalModels string, deliveryTime string, " +
          "channelsId string, channelsName string , deliveryAreaId string, deliveryCountry " +
          "string, deliveryProvince string, deliveryCity string,deliveryDistrict string, " +
          "deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId " +
          "string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict" +
          " string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, " +
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
          "contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (imei) , " +
          "PARTITION_COUNT=2] )"

      )
      sql("LOAD DATA FACT FROM '" + currentDirectory + "/src/test/resources/100_olap.csv' INTO " +
        "Cube Carbon_automation_test5 partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER " +
        "'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize," +
        "ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime," +
        "channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity," +
        "deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime," +
        "ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet," +
        "ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion," +
        "Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer," +
        "Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions," +
        "Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country," +
        "Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId," +
        "Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer," +
        "Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber," +
        "Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId," +
        "gamePointId,gamePointDescription')");
    } catch {
      case e: Exception => print("ERROR: DROP Carbon_automation_test5 ")
    }

  }

  override def afterAll {
    try {
      sql("drop cube Carbon_automation_vmall_test1")
      sql("drop cube Carbon_automation_test5")
    } catch {
      case e: Exception => print("ERROR: DROP Carbon_automation_test5 ")
    }

  }

  //TC_508
  test(
    "SELECT  Carbon_automation_test5.gamePointId AS gamePointId,Carbon_automation_test5.AMSize AS" +
      " AMSize, Carbon_automation_test5.ActiveCountry AS ActiveCountry, Carbon_automation_test5" +
      ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM" +
      " (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( " +
      "SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) " +
      "SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
      "Carbon_automation_vmall_test1.AMSize WHERE Carbon_automation_test5.AMSize LIKE '%1%' GROUP" +
      " BY Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, " +
      "Carbon_automation_test5.Activecity,Carbon_automation_test5. gamePointId ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT  Carbon_automation_test5.gamePointId AS gamePointId,Carbon_automation_test5" +
          ".AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS ActiveCountry, " +
          "Carbon_automation_test5.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, " +
          "ActiveCountry, Activecity FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
          "Carbon_automation_test5 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
          "(select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
          "Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize WHERE " +
          "Carbon_automation_test5.AMSize LIKE '%1%' GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity," +
          "Carbon_automation_test5. gamePointId ORDER BY Carbon_automation_test5.AMSize ASC, " +
          "Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row(1333.0, "1RAM size", "Chinese", "guangzhou"),
        Row(256.0, "1RAM size", "Chinese", "shenzhen"),
        Row(2175.0, "1RAM size", "Chinese", "xiangtan"),
        Row(202.0, "1RAM size", "Chinese", "xiangtan"),
        Row(2734.0, "1RAM size", "Chinese", "xiangtan"),
        Row(2399.0, "1RAM size", "Chinese", "xiangtan"),
        Row(2078.0, "1RAM size", "Chinese", "yichang"),
        Row(1864.0, "1RAM size", "Chinese", "yichang"),
        Row(2745.0, "1RAM size", "Chinese", "zhuzhou")
      )
    )
  }
  )

  //TC_518
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity , SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
      "ActiveCountry,gamePointId, Activecity FROM (select * from Carbon_automation_test5) SUB_QRY" +
      " ) Carbon_automation_test5 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
      "(select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize WHERE NOT" +
      "(Carbon_automation_test5.AMSize <= \"3RAM size\") GROUP BY Carbon_automation_test5.AMSize," +
      " Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity , SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
          "ActiveCountry,gamePointId, Activecity FROM (select * from Carbon_automation_test5) " +
          "SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT ActiveCountry, Activecity, " +
          "AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
          "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize WHERE NOT(Carbon_automation_test5.AMSize <= " +
          "\"3RAM size\") GROUP BY Carbon_automation_test5.AMSize, Carbon_automation_test5" +
          ".ActiveCountry, Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5" +
          ".AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5" +
          ".Activecity ASC"
      ),
      Seq(Row("4RAM size", "Chinese", "changsha", 200860.0),
        Row("4RAM size", "Chinese", "guangzhou", 38016.0),
        Row("4RAM size", "Chinese", "shenzhen", 49610.0),
        Row("4RAM size", "Chinese", "wuhan", 117581.96999999999),
        Row("4RAM size", "Chinese", "xiangtan", 254320.0),
        Row("4RAM size", "Chinese", "yichang", 29436.0),
        Row("5RAM size", "Chinese", "changsha", 13845.0),
        Row("5RAM size", "Chinese", "guangzhou", 23560.0),
        Row("5RAM size", "Chinese", "wuhan", 12390.0),
        Row("6RAM size", "Chinese", "changsha", 23697.0),
        Row("6RAM size", "Chinese", "guangzhou", 15912.0),
        Row("6RAM size", "Chinese", "shenzhen", 19278.0),
        Row("6RAM size", "Chinese", "wuhan", 29313.0),
        Row("6RAM size", "Chinese", "xiangtan", 7794.0),
        Row("6RAM size", "Chinese", "zhuzhou", 26568.0),
        Row("7RAM size", "Chinese", "changsha", 1057.0),
        Row("7RAM size", "Chinese", "wuhan", 27853.0),
        Row("7RAM size", "Chinese", "yichang", 14217.0),
        Row("7RAM size", "Chinese", "zhuzhou", 15673.0),
        Row("8RAM size", "Chinese", "guangzhou", 27385.619999999995),
        Row("8RAM size", "Chinese", "shenzhen", 3550.0),
        Row("8RAM size", "Chinese", "wuhan", 29700.0),
        Row("8RAM size", "Chinese", "xiangtan", 31020.0),
        Row("8RAM size", "Chinese", "yichang", 51660.0),
        Row("8RAM size", "Chinese", "zhuzhou", 26840.0),
        Row("9RAM size", "Chinese", "changsha", 32390.0),
        Row("9RAM size", "Chinese", "shenzhen", 30650.0),
        Row("9RAM size", "Chinese", "wuhan", 15670.0),
        Row("9RAM size", "Chinese", "xiangtan", 58210.0),
        Row("9RAM size", "Chinese", "yichang", 5710.0)
      )
    )
  }
  )

  //TC_519
  test(
    "SELECT Carbon_automation_test5.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry," +
      " Activecity FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5" +
      " INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
      "Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY " +
      "Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.Activecity ASC LIMIT " +
      "5000"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.Activecity AS Activecity FROM ( SELECT AMSize, " +
          "ActiveCountry, Activecity FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
          "Carbon_automation_test5 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
          "(select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
          "Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY " +
          "Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.Activecity ASC " +
          "LIMIT 5000"
      ),
      Seq(Row("changsha"),
        Row("guangzhou"),
        Row("shenzhen"),
        Row("wuhan"),
        Row("xiangtan"),
        Row("yichang"),
        Row("zhuzhou")
      )
    )
  }
  )

  //TC_521
  test("TC_521")({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize," +
          "gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test5) " +
          "SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT ActiveCountry, Activecity, " +
          "AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
          "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize WHERE NOT(Carbon_automation_test5.Activecity LIKE" +
          " 'xian%') GROUP BY Carbon_automation_test5.AMSize, Carbon_automation_test5" +
          ".ActiveCountry, Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5" +
          ".AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5" +
          ".Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 84293.0),
        Row("0RAM size", "Chinese", "guangzhou", 869.0),
        Row("0RAM size", "Chinese", "shenzhen", 31339.0),
        Row("0RAM size", "Chinese", "wuhan", 66902.0),
        Row("0RAM size", "Chinese", "zhuzhou", 14751.0),
        Row("1RAM size", "Chinese", "guangzhou", 11997.0),
        Row("1RAM size", "Chinese", "shenzhen", 2304.0),
        Row("1RAM size", "Chinese", "yichang", 35478.0),
        Row("1RAM size", "Chinese", "zhuzhou", 24705.0),
        Row("2RAM size", "Chinese", "changsha", 3946.0),
        Row("3RAM size", "Chinese", "changsha", 40082.0),
        Row("3RAM size", "Chinese", "guangzhou", 27986.0),
        Row("3RAM size", "Chinese", "shenzhen", 92960.0),
        Row("3RAM size", "Chinese", "wuhan", 36890.0),
        Row("3RAM size", "Chinese", "yichang", 20874.0),
        Row("3RAM size", "Chinese", "zhuzhou", 79786.0),
        Row("4RAM size", "Chinese", "changsha", 200860.0),
        Row("4RAM size", "Chinese", "guangzhou", 38016.0),
        Row("4RAM size", "Chinese", "shenzhen", 49610.0),
        Row("4RAM size", "Chinese", "wuhan", 117581.96999999999),
        Row("4RAM size", "Chinese", "yichang", 29436.0),
        Row("5RAM size", "Chinese", "changsha", 13845.0),
        Row("5RAM size", "Chinese", "guangzhou", 23560.0),
        Row("5RAM size", "Chinese", "wuhan", 12390.0),
        Row("6RAM size", "Chinese", "changsha", 23697.0),
        Row("6RAM size", "Chinese", "guangzhou", 15912.0),
        Row("6RAM size", "Chinese", "shenzhen", 19278.0),
        Row("6RAM size", "Chinese", "wuhan", 29313.0),
        Row("6RAM size", "Chinese", "zhuzhou", 26568.0),
        Row("7RAM size", "Chinese", "changsha", 1057.0),
        Row("7RAM size", "Chinese", "wuhan", 27853.0),
        Row("7RAM size", "Chinese", "yichang", 14217.0),
        Row("7RAM size", "Chinese", "zhuzhou", 15673.0),
        Row("8RAM size", "Chinese", "guangzhou", 27385.619999999995),
        Row("8RAM size", "Chinese", "shenzhen", 3550.0),
        Row("8RAM size", "Chinese", "wuhan", 29700.0),
        Row("8RAM size", "Chinese", "yichang", 51660.0),
        Row("8RAM size", "Chinese", "zhuzhou", 26840.0),
        Row("9RAM size", "Chinese", "changsha", 32390.0),
        Row("9RAM size", "Chinese", "shenzhen", 30650.0),
        Row("9RAM size", "Chinese", "wuhan", 15670.0),
        Row("9RAM size", "Chinese", "yichang", 5710.0)
      )
    )
  }
  )

  //TC_522
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
      "ActiveCountry, Activecity,gamePointId  FROM (select * from Carbon_automation_test5) " +
      "SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize " +
      "FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize WHERE NOT" +
      "(Carbon_automation_test5.AMSize BETWEEN \"4RAM size\" AND \"7RAM size\") GROUP BY " +
      "Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, " +
      "Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.AMSize ASC, " +
      "Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
          "ActiveCountry, Activecity,gamePointId  FROM (select * from Carbon_automation_test5) " +
          "SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT ActiveCountry, Activecity, " +
          "AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
          "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize WHERE NOT(Carbon_automation_test5.AMSize BETWEEN " +
          "\"4RAM size\" AND \"7RAM size\") GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 84293.0),
        Row("0RAM size", "Chinese", "guangzhou", 869.0),
        Row("0RAM size", "Chinese", "shenzhen", 31339.0),
        Row("0RAM size", "Chinese", "wuhan", 66902.0),
        Row("0RAM size", "Chinese", "zhuzhou", 14751.0),
        Row("1RAM size", "Chinese", "guangzhou", 11997.0),
        Row("1RAM size", "Chinese", "shenzhen", 2304.0),
        Row("1RAM size", "Chinese", "xiangtan", 67590.0),
        Row("1RAM size", "Chinese", "yichang", 35478.0),
        Row("1RAM size", "Chinese", "zhuzhou", 24705.0),
        Row("2RAM size", "Chinese", "changsha", 3946.0),
        Row("2RAM size", "Chinese", "xiangtan", 2700.0),
        Row("3RAM size", "Chinese", "changsha", 40082.0),
        Row("3RAM size", "Chinese", "guangzhou", 27986.0),
        Row("3RAM size", "Chinese", "shenzhen", 92960.0),
        Row("3RAM size", "Chinese", "wuhan", 36890.0),
        Row("3RAM size", "Chinese", "xiangtan", 53536.0),
        Row("3RAM size", "Chinese", "yichang", 20874.0),
        Row("3RAM size", "Chinese", "zhuzhou", 79786.0),
        Row("8RAM size", "Chinese", "guangzhou", 27385.619999999995),
        Row("8RAM size", "Chinese", "shenzhen", 3550.0),
        Row("8RAM size", "Chinese", "wuhan", 29700.0),
        Row("8RAM size", "Chinese", "xiangtan", 31020.0),
        Row("8RAM size", "Chinese", "yichang", 51660.0),
        Row("8RAM size", "Chinese", "zhuzhou", 26840.0),
        Row("9RAM size", "Chinese", "changsha", 32390.0),
        Row("9RAM size", "Chinese", "shenzhen", 30650.0),
        Row("9RAM size", "Chinese", "wuhan", 15670.0),
        Row("9RAM size", "Chinese", "xiangtan", 58210.0),
        Row("9RAM size", "Chinese", "yichang", 5710.0)
      )
    )
  }
  )

  //TC_523
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
      "ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation_test5) SUB_QRY" +
      " ) Carbon_automation_test5 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
      "(select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize WHERE NOT" +
      "(Carbon_automation_test5.AMSize IN (\"5RAM size\",\"8RAM size\")) GROUP BY " +
      "Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, " +
      "Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.AMSize ASC, " +
      "Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
          "ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation_test5) " +
          "SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT ActiveCountry, Activecity, " +
          "AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
          "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize WHERE NOT(Carbon_automation_test5.AMSize IN " +
          "(\"5RAM size\",\"8RAM size\")) GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 84293.0),
        Row("0RAM size", "Chinese", "guangzhou", 869.0),
        Row("0RAM size", "Chinese", "shenzhen", 31339.0),
        Row("0RAM size", "Chinese", "wuhan", 66902.0),
        Row("0RAM size", "Chinese", "zhuzhou", 14751.0),
        Row("1RAM size", "Chinese", "guangzhou", 11997.0),
        Row("1RAM size", "Chinese", "shenzhen", 2304.0),
        Row("1RAM size", "Chinese", "xiangtan", 67590.0),
        Row("1RAM size", "Chinese", "yichang", 35478.0),
        Row("1RAM size", "Chinese", "zhuzhou", 24705.0),
        Row("2RAM size", "Chinese", "changsha", 3946.0),
        Row("2RAM size", "Chinese", "xiangtan", 2700.0),
        Row("3RAM size", "Chinese", "changsha", 40082.0),
        Row("3RAM size", "Chinese", "guangzhou", 27986.0),
        Row("3RAM size", "Chinese", "shenzhen", 92960.0),
        Row("3RAM size", "Chinese", "wuhan", 36890.0),
        Row("3RAM size", "Chinese", "xiangtan", 53536.0),
        Row("3RAM size", "Chinese", "yichang", 20874.0),
        Row("3RAM size", "Chinese", "zhuzhou", 79786.0),
        Row("4RAM size", "Chinese", "changsha", 200860.0),
        Row("4RAM size", "Chinese", "guangzhou", 38016.0),
        Row("4RAM size", "Chinese", "shenzhen", 49610.0),
        Row("4RAM size", "Chinese", "wuhan", 117581.96999999999),
        Row("4RAM size", "Chinese", "xiangtan", 254320.0),
        Row("4RAM size", "Chinese", "yichang", 29436.0),
        Row("6RAM size", "Chinese", "changsha", 23697.0),
        Row("6RAM size", "Chinese", "guangzhou", 15912.0),
        Row("6RAM size", "Chinese", "shenzhen", 19278.0),
        Row("6RAM size", "Chinese", "wuhan", 29313.0),
        Row("6RAM size", "Chinese", "xiangtan", 7794.0),
        Row("6RAM size", "Chinese", "zhuzhou", 26568.0),
        Row("7RAM size", "Chinese", "changsha", 1057.0),
        Row("7RAM size", "Chinese", "wuhan", 27853.0),
        Row("7RAM size", "Chinese", "yichang", 14217.0),
        Row("7RAM size", "Chinese", "zhuzhou", 15673.0),
        Row("9RAM size", "Chinese", "changsha", 32390.0),
        Row("9RAM size", "Chinese", "shenzhen", 30650.0),
        Row("9RAM size", "Chinese", "wuhan", 15670.0),
        Row("9RAM size", "Chinese", "xiangtan", 58210.0),
        Row("9RAM size", "Chinese", "yichang", 5710.0)
      )
    )
  }
  )

  //TC_524
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
      "ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation_test5) SUB_QRY" +
      " ) Carbon_automation_test5 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
      "(select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize WHERE " +
      "Carbon_automation_test5.AMSize IS NOT NULL GROUP BY Carbon_automation_test5.AMSize, " +
      "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
          "ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation_test5) " +
          "SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT ActiveCountry, Activecity, " +
          "AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
          "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize WHERE Carbon_automation_test5.AMSize IS NOT NULL " +
          "GROUP BY Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, " +
          "Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.AMSize ASC, " +
          "Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 84293.0),
        Row("0RAM size", "Chinese", "guangzhou", 869.0),
        Row("0RAM size", "Chinese", "shenzhen", 31339.0),
        Row("0RAM size", "Chinese", "wuhan", 66902.0),
        Row("0RAM size", "Chinese", "zhuzhou", 14751.0),
        Row("1RAM size", "Chinese", "guangzhou", 11997.0),
        Row("1RAM size", "Chinese", "shenzhen", 2304.0),
        Row("1RAM size", "Chinese", "xiangtan", 67590.0),
        Row("1RAM size", "Chinese", "yichang", 35478.0),
        Row("1RAM size", "Chinese", "zhuzhou", 24705.0),
        Row("2RAM size", "Chinese", "changsha", 3946.0),
        Row("2RAM size", "Chinese", "xiangtan", 2700.0),
        Row("3RAM size", "Chinese", "changsha", 40082.0),
        Row("3RAM size", "Chinese", "guangzhou", 27986.0),
        Row("3RAM size", "Chinese", "shenzhen", 92960.0),
        Row("3RAM size", "Chinese", "wuhan", 36890.0),
        Row("3RAM size", "Chinese", "xiangtan", 53536.0),
        Row("3RAM size", "Chinese", "yichang", 20874.0),
        Row("3RAM size", "Chinese", "zhuzhou", 79786.0),
        Row("4RAM size", "Chinese", "changsha", 200860.0),
        Row("4RAM size", "Chinese", "guangzhou", 38016.0),
        Row("4RAM size", "Chinese", "shenzhen", 49610.0),
        Row("4RAM size", "Chinese", "wuhan", 117581.96999999999),
        Row("4RAM size", "Chinese", "xiangtan", 254320.0),
        Row("4RAM size", "Chinese", "yichang", 29436.0),
        Row("5RAM size", "Chinese", "changsha", 13845.0),
        Row("5RAM size", "Chinese", "guangzhou", 23560.0),
        Row("5RAM size", "Chinese", "wuhan", 12390.0),
        Row("6RAM size", "Chinese", "changsha", 23697.0),
        Row("6RAM size", "Chinese", "guangzhou", 15912.0),
        Row("6RAM size", "Chinese", "shenzhen", 19278.0),
        Row("6RAM size", "Chinese", "wuhan", 29313.0),
        Row("6RAM size", "Chinese", "xiangtan", 7794.0),
        Row("6RAM size", "Chinese", "zhuzhou", 26568.0),
        Row("7RAM size", "Chinese", "changsha", 1057.0),
        Row("7RAM size", "Chinese", "wuhan", 27853.0),
        Row("7RAM size", "Chinese", "yichang", 14217.0),
        Row("7RAM size", "Chinese", "zhuzhou", 15673.0),
        Row("8RAM size", "Chinese", "guangzhou", 27385.619999999995),
        Row("8RAM size", "Chinese", "shenzhen", 3550.0),
        Row("8RAM size", "Chinese", "wuhan", 29700.0),
        Row("8RAM size", "Chinese", "xiangtan", 31020.0),
        Row("8RAM size", "Chinese", "yichang", 51660.0),
        Row("8RAM size", "Chinese", "zhuzhou", 26840.0),
        Row("9RAM size", "Chinese", "changsha", 32390.0),
        Row("9RAM size", "Chinese", "shenzhen", 30650.0),
        Row("9RAM size", "Chinese", "wuhan", 15670.0),
        Row("9RAM size", "Chinese", "xiangtan", 58210.0),
        Row("9RAM size", "Chinese", "yichang", 5710.0)
      )
    )
  }
  )

  //TC_528
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId, SUM(Carbon_automation_test5" +
      ".deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, gamePointId," +
      "ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_test5)" +
      " SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize " +
      "FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY " +
      "Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, " +
      "Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.AMSize ASC, " +
      "Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId, SUM(Carbon_automation_test5" +
          ".deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, gamePointId," +
          "ActiveCountry,deviceInformationId, Activecity FROM (select * from " +
          "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 84293.0, 4401364),
        Row("0RAM size", "Chinese", "guangzhou", 869.0, 1100110),
        Row("0RAM size", "Chinese", "shenzhen", 31339.0, 1100308),
        Row("0RAM size", "Chinese", "wuhan", 66902.0, 4401639),
        Row("0RAM size", "Chinese", "zhuzhou", 14751.0, 1100022),
        Row("1RAM size", "Chinese", "guangzhou", 11997.0, 900270),
        Row("1RAM size", "Chinese", "shenzhen", 2304.0, 900180),
        Row("1RAM size", "Chinese", "xiangtan", 67590.0, 2790900),
        Row("1RAM size", "Chinese", "yichang", 35478.0, 1800954),
        Row("1RAM size", "Chinese", "zhuzhou", 24705.0, 900378),
        Row("2RAM size", "Chinese", "changsha", 3946.0, 200142),
        Row("2RAM size", "Chinese", "xiangtan", 2700.0, 20014),
        Row("3RAM size", "Chinese", "changsha", 40082.0, 1400210),
        Row("3RAM size", "Chinese", "guangzhou", 27986.0, 1400308),
        Row("3RAM size", "Chinese", "shenzhen", 92960.0, 5603668),
        Row("3RAM size", "Chinese", "wuhan", 36890.0, 1400812),
        Row("3RAM size", "Chinese", "xiangtan", 53536.0, 4201974),
        Row("3RAM size", "Chinese", "yichang", 20874.0, 1400966),
        Row("3RAM size", "Chinese", "zhuzhou", 79786.0, 2941190),
        Row("4RAM size", "Chinese", "changsha", 200860.0, 11225038),
        Row("4RAM size", "Chinese", "guangzhou", 38016.0, 2201210),
        Row("4RAM size", "Chinese", "shenzhen", 49610.0, 2421408),
        Row("4RAM size", "Chinese", "wuhan", 117581.96999999999, 4402222),
        Row("4RAM size", "Chinese", "xiangtan", 254320.0, 33004774),
        Row("4RAM size", "Chinese", "yichang", 29436.0, 8803168),
        Row("5RAM size", "Chinese", "changsha", 13845.0, 505385),
        Row("5RAM size", "Chinese", "guangzhou", 23560.0, 1000460),
        Row("5RAM size", "Chinese", "wuhan", 12390.0, 50030),
        Row("6RAM size", "Chinese", "changsha", 23697.0, 1800909),
        Row("6RAM size", "Chinese", "guangzhou", 15912.0, 900234),
        Row("6RAM size", "Chinese", "shenzhen", 19278.0, 900315),
        Row("6RAM size", "Chinese", "wuhan", 29313.0, 1801125),
        Row("6RAM size", "Chinese", "xiangtan", 7794.0, 990117),
        Row("6RAM size", "Chinese", "zhuzhou", 26568.0, 900558),
        Row("7RAM size", "Chinese", "changsha", 1057.0, 700098),
        Row("7RAM size", "Chinese", "wuhan", 27853.0, 2100455),
        Row("7RAM size", "Chinese", "yichang", 14217.0, 700931),
        Row("7RAM size", "Chinese", "zhuzhou", 15673.0, 700021),
        Row("8RAM size", "Chinese", "guangzhou", 27385.619999999995, 10),
        Row("8RAM size", "Chinese", "shenzhen", 3550.0, 1000130),
        Row("8RAM size", "Chinese", "wuhan", 29700.0, 1000040),
        Row("8RAM size", "Chinese", "xiangtan", 31020.0, 2000540),
        Row("8RAM size", "Chinese", "yichang", 51660.0, 1100250),
        Row("8RAM size", "Chinese", "zhuzhou", 26840.0, 3001960),
        Row("9RAM size", "Chinese", "changsha", 32390.0, 2000730),
        Row("9RAM size", "Chinese", "shenzhen", 30650.0, 2000980),
        Row("9RAM size", "Chinese", "wuhan", 15670.0, 1000700),
        Row("9RAM size", "Chinese", "xiangtan", 58210.0, 3102370),
        Row("9RAM size", "Chinese", "yichang", 5710.0, 1000430)
      )
    )
  }
  )

  //TC_529
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId, AVG(Carbon_automation_test5" +
      ".deviceInformationId) AS avg_deviceInformationId FROM ( SELECT AMSize,deviceInformationId," +
      " gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test5) " +
      "SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize " +
      "FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY " +
      "Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, " +
      "Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.AMSize ASC, " +
      "Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId, AVG(Carbon_automation_test5" +
          ".deviceInformationId) AS avg_deviceInformationId FROM ( SELECT AMSize," +
          "deviceInformationId, gamePointId, ActiveCountry, Activecity FROM (select * from " +
          "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 84293.0, 100031.0),
        Row("0RAM size", "Chinese", "guangzhou", 869.0, 100010.0),
        Row("0RAM size", "Chinese", "shenzhen", 31339.0, 100028.0),
        Row("0RAM size", "Chinese", "wuhan", 66902.0, 100037.25),
        Row("0RAM size", "Chinese", "zhuzhou", 14751.0, 100002.0),
        Row("1RAM size", "Chinese", "guangzhou", 11997.0, 100030.0),
        Row("1RAM size", "Chinese", "shenzhen", 2304.0, 100020.0),
        Row("1RAM size", "Chinese", "xiangtan", 67590.0, 77525.0),
        Row("1RAM size", "Chinese", "yichang", 35478.0, 100053.0),
        Row("1RAM size", "Chinese", "zhuzhou", 24705.0, 100042.0),
        Row("2RAM size", "Chinese", "changsha", 3946.0, 100071.0),
        Row("2RAM size", "Chinese", "xiangtan", 2700.0, 10007.0),
        Row("3RAM size", "Chinese", "changsha", 40082.0, 100015.0),
        Row("3RAM size", "Chinese", "guangzhou", 27986.0, 100022.0),
        Row("3RAM size", "Chinese", "shenzhen", 92960.0, 100065.5),
        Row("3RAM size", "Chinese", "wuhan", 36890.0, 100058.0),
        Row("3RAM size", "Chinese", "xiangtan", 53536.0, 100047.0),
        Row("3RAM size", "Chinese", "yichang", 20874.0, 100069.0),
        Row("3RAM size", "Chinese", "zhuzhou", 79786.0, 70028.33333333333),
        Row("4RAM size", "Chinese", "changsha", 200860.0, 85038.16666666667),
        Row("4RAM size", "Chinese", "guangzhou", 38016.0, 100055.0),
        Row("4RAM size", "Chinese", "shenzhen", 49610.0, 55032.0),
        Row("4RAM size", "Chinese", "wuhan", 117581.96999999999, 66700.33333333333),
        Row("4RAM size", "Chinese", "xiangtan", 254320.0, 250036.16666666666),
        Row("4RAM size", "Chinese", "yichang", 29436.0, 100036.0),
        Row("5RAM size", "Chinese", "changsha", 13845.0, 50538.5),
        Row("5RAM size", "Chinese", "guangzhou", 23560.0, 100046.0),
        Row("5RAM size", "Chinese", "wuhan", 12390.0, 10006.0),
        Row("6RAM size", "Chinese", "changsha", 23697.0, 100050.5),
        Row("6RAM size", "Chinese", "guangzhou", 15912.0, 100026.0),
        Row("6RAM size", "Chinese", "shenzhen", 19278.0, 100035.0),
        Row("6RAM size", "Chinese", "wuhan", 29313.0, 100062.5),
        Row("6RAM size", "Chinese", "xiangtan", 7794.0, 55006.5),
        Row("6RAM size", "Chinese", "zhuzhou", 26568.0, 100062.0),
        Row("7RAM size", "Chinese", "changsha", 1057.0, 100014.0),
        Row("7RAM size", "Chinese", "wuhan", 27853.0, 100021.66666666667),
        Row("7RAM size", "Chinese", "yichang", 14217.0, 50066.5),
        Row("7RAM size", "Chinese", "zhuzhou", 15673.0, 100003.0),
        Row("8RAM size", "Chinese", "guangzhou", 27385.619999999995, 1.0),
        Row("8RAM size", "Chinese", "shenzhen", 3550.0, 100013.0),
        Row("8RAM size", "Chinese", "wuhan", 29700.0, 100004.0),
        Row("8RAM size", "Chinese", "xiangtan", 31020.0, 100027.0),
        Row("8RAM size", "Chinese", "yichang", 51660.0, 55012.5),
        Row("8RAM size", "Chinese", "zhuzhou", 26840.0, 100065.33333333333),
        Row("9RAM size", "Chinese", "changsha", 32390.0, 100036.5),
        Row("9RAM size", "Chinese", "shenzhen", 30650.0, 100049.0),
        Row("9RAM size", "Chinese", "wuhan", 15670.0, 100070.0),
        Row("9RAM size", "Chinese", "xiangtan", 58210.0, 77559.25),
        Row("9RAM size", "Chinese", "yichang", 5710.0, 100043.0)
      )
    )
  }
  )

  //TC_530
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId, COUNT(Carbon_automation_test5" +
      ".deviceInformationId) AS Count_deviceInformationId FROM ( SELECT AMSize, ActiveCountry," +
      "gamePointId, Activecity,deviceInformationId FROM (select * from Carbon_automation_test5) " +
      "SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize " +
      "FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY " +
      "Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, " +
      "Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.AMSize ASC, " +
      "Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId, COUNT" +
          "(Carbon_automation_test5.deviceInformationId) AS Count_deviceInformationId FROM ( " +
          "SELECT AMSize, ActiveCountry,gamePointId, Activecity,deviceInformationId FROM (select " +
          "* from Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 84293.0, 44),
        Row("0RAM size", "Chinese", "guangzhou", 869.0, 11),
        Row("0RAM size", "Chinese", "shenzhen", 31339.0, 11),
        Row("0RAM size", "Chinese", "wuhan", 66902.0, 44),
        Row("0RAM size", "Chinese", "zhuzhou", 14751.0, 11),
        Row("1RAM size", "Chinese", "guangzhou", 11997.0, 9),
        Row("1RAM size", "Chinese", "shenzhen", 2304.0, 9),
        Row("1RAM size", "Chinese", "xiangtan", 67590.0, 36),
        Row("1RAM size", "Chinese", "yichang", 35478.0, 18),
        Row("1RAM size", "Chinese", "zhuzhou", 24705.0, 9),
        Row("2RAM size", "Chinese", "changsha", 3946.0, 2),
        Row("2RAM size", "Chinese", "xiangtan", 2700.0, 2),
        Row("3RAM size", "Chinese", "changsha", 40082.0, 14),
        Row("3RAM size", "Chinese", "guangzhou", 27986.0, 14),
        Row("3RAM size", "Chinese", "shenzhen", 92960.0, 56),
        Row("3RAM size", "Chinese", "wuhan", 36890.0, 14),
        Row("3RAM size", "Chinese", "xiangtan", 53536.0, 42),
        Row("3RAM size", "Chinese", "yichang", 20874.0, 14),
        Row("3RAM size", "Chinese", "zhuzhou", 79786.0, 42),
        Row("4RAM size", "Chinese", "changsha", 200860.0, 132),
        Row("4RAM size", "Chinese", "guangzhou", 38016.0, 22),
        Row("4RAM size", "Chinese", "shenzhen", 49610.0, 44),
        Row("4RAM size", "Chinese", "wuhan", 117581.96999999999, 66),
        Row("4RAM size", "Chinese", "xiangtan", 254320.0, 132),
        Row("4RAM size", "Chinese", "yichang", 29436.0, 88),
        Row("5RAM size", "Chinese", "changsha", 13845.0, 10),
        Row("5RAM size", "Chinese", "guangzhou", 23560.0, 10),
        Row("5RAM size", "Chinese", "wuhan", 12390.0, 5),
        Row("6RAM size", "Chinese", "changsha", 23697.0, 18),
        Row("6RAM size", "Chinese", "guangzhou", 15912.0, 9),
        Row("6RAM size", "Chinese", "shenzhen", 19278.0, 9),
        Row("6RAM size", "Chinese", "wuhan", 29313.0, 18),
        Row("6RAM size", "Chinese", "xiangtan", 7794.0, 18),
        Row("6RAM size", "Chinese", "zhuzhou", 26568.0, 9),
        Row("7RAM size", "Chinese", "changsha", 1057.0, 7),
        Row("7RAM size", "Chinese", "wuhan", 27853.0, 21),
        Row("7RAM size", "Chinese", "yichang", 14217.0, 14),
        Row("7RAM size", "Chinese", "zhuzhou", 15673.0, 7),
        Row("8RAM size", "Chinese", "guangzhou", 27385.619999999995, 10),
        Row("8RAM size", "Chinese", "shenzhen", 3550.0, 10),
        Row("8RAM size", "Chinese", "wuhan", 29700.0, 10),
        Row("8RAM size", "Chinese", "xiangtan", 31020.0, 20),
        Row("8RAM size", "Chinese", "yichang", 51660.0, 20),
        Row("8RAM size", "Chinese", "zhuzhou", 26840.0, 30),
        Row("9RAM size", "Chinese", "changsha", 32390.0, 20),
        Row("9RAM size", "Chinese", "shenzhen", 30650.0, 20),
        Row("9RAM size", "Chinese", "wuhan", 15670.0, 10),
        Row("9RAM size", "Chinese", "xiangtan", 58210.0, 40),
        Row("9RAM size", "Chinese", "yichang", 5710.0, 10)
      )
    )
  }
  )

  //TC_531
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId, COUNT(DISTINCT " +
      "Carbon_automation_test5.deviceInformationId) AS LONG_COL_0 FROM ( SELECT AMSize, " +
      "ActiveCountry,gamePointId,deviceInformationId, Activecity FROM (select * from " +
      "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
      "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
      "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
      "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
      "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId, COUNT(DISTINCT " +
          "Carbon_automation_test5.deviceInformationId) AS LONG_COL_0 FROM ( SELECT AMSize, " +
          "ActiveCountry,gamePointId,deviceInformationId, Activecity FROM (select * from " +
          "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 84293.0, 4),
        Row("0RAM size", "Chinese", "guangzhou", 869.0, 1),
        Row("0RAM size", "Chinese", "shenzhen", 31339.0, 1),
        Row("0RAM size", "Chinese", "wuhan", 66902.0, 4),
        Row("0RAM size", "Chinese", "zhuzhou", 14751.0, 1),
        Row("1RAM size", "Chinese", "guangzhou", 11997.0, 1),
        Row("1RAM size", "Chinese", "shenzhen", 2304.0, 1),
        Row("1RAM size", "Chinese", "xiangtan", 67590.0, 4),
        Row("1RAM size", "Chinese", "yichang", 35478.0, 2),
        Row("1RAM size", "Chinese", "zhuzhou", 24705.0, 1),
        Row("2RAM size", "Chinese", "changsha", 3946.0, 1),
        Row("2RAM size", "Chinese", "xiangtan", 2700.0, 1),
        Row("3RAM size", "Chinese", "changsha", 40082.0, 1),
        Row("3RAM size", "Chinese", "guangzhou", 27986.0, 1),
        Row("3RAM size", "Chinese", "shenzhen", 92960.0, 4),
        Row("3RAM size", "Chinese", "wuhan", 36890.0, 1),
        Row("3RAM size", "Chinese", "xiangtan", 53536.0, 3),
        Row("3RAM size", "Chinese", "yichang", 20874.0, 1),
        Row("3RAM size", "Chinese", "zhuzhou", 79786.0, 3),
        Row("4RAM size", "Chinese", "changsha", 200860.0, 6),
        Row("4RAM size", "Chinese", "guangzhou", 38016.0, 1),
        Row("4RAM size", "Chinese", "shenzhen", 49610.0, 2),
        Row("4RAM size", "Chinese", "wuhan", 117581.96999999999, 3),
        Row("4RAM size", "Chinese", "xiangtan", 254320.0, 6),
        Row("4RAM size", "Chinese", "yichang", 29436.0, 4),
        Row("5RAM size", "Chinese", "changsha", 13845.0, 2),
        Row("5RAM size", "Chinese", "guangzhou", 23560.0, 2),
        Row("5RAM size", "Chinese", "wuhan", 12390.0, 1),
        Row("6RAM size", "Chinese", "changsha", 23697.0, 2),
        Row("6RAM size", "Chinese", "guangzhou", 15912.0, 1),
        Row("6RAM size", "Chinese", "shenzhen", 19278.0, 1),
        Row("6RAM size", "Chinese", "wuhan", 29313.0, 2),
        Row("6RAM size", "Chinese", "xiangtan", 7794.0, 2),
        Row("6RAM size", "Chinese", "zhuzhou", 26568.0, 1),
        Row("7RAM size", "Chinese", "changsha", 1057.0, 1),
        Row("7RAM size", "Chinese", "wuhan", 27853.0, 3),
        Row("7RAM size", "Chinese", "yichang", 14217.0, 2),
        Row("7RAM size", "Chinese", "zhuzhou", 15673.0, 1),
        Row("8RAM size", "Chinese", "guangzhou", 27385.619999999995, 1),
        Row("8RAM size", "Chinese", "shenzhen", 3550.0, 1),
        Row("8RAM size", "Chinese", "wuhan", 29700.0, 1),
        Row("8RAM size", "Chinese", "xiangtan", 31020.0, 2),
        Row("8RAM size", "Chinese", "yichang", 51660.0, 2),
        Row("8RAM size", "Chinese", "zhuzhou", 26840.0, 3),
        Row("9RAM size", "Chinese", "changsha", 32390.0, 2),
        Row("9RAM size", "Chinese", "shenzhen", 30650.0, 2),
        Row("9RAM size", "Chinese", "wuhan", 15670.0, 1),
        Row("9RAM size", "Chinese", "xiangtan", 58210.0, 4),
        Row("9RAM size", "Chinese", "yichang", 5710.0, 1)
      )
    )
  }
  )

  //TC_533
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId, MAX(Carbon_automation_test5" +
      ".deviceInformationId) AS Max_deviceInformationId FROM ( SELECT AMSize,gamePointId," +
      "deviceInformationId, ActiveCountry, Activecity FROM (select * from " +
      "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
      "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
      "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
      "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
      "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId, MAX(Carbon_automation_test5" +
          ".deviceInformationId) AS Max_deviceInformationId FROM ( SELECT AMSize,gamePointId," +
          "deviceInformationId, ActiveCountry, Activecity FROM (select * from " +
          "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 84293.0, 100079),
        Row("0RAM size", "Chinese", "guangzhou", 869.0, 100010),
        Row("0RAM size", "Chinese", "shenzhen", 31339.0, 100028),
        Row("0RAM size", "Chinese", "wuhan", 66902.0, 100061),
        Row("0RAM size", "Chinese", "zhuzhou", 14751.0, 100002),
        Row("1RAM size", "Chinese", "guangzhou", 11997.0, 100030),
        Row("1RAM size", "Chinese", "shenzhen", 2304.0, 100020),
        Row("1RAM size", "Chinese", "xiangtan", 67590.0, 100048),
        Row("1RAM size", "Chinese", "yichang", 35478.0, 100066),
        Row("1RAM size", "Chinese", "zhuzhou", 24705.0, 100042),
        Row("2RAM size", "Chinese", "changsha", 3946.0, 100071),
        Row("2RAM size", "Chinese", "xiangtan", 2700.0, 10007),
        Row("3RAM size", "Chinese", "changsha", 40082.0, 100015),
        Row("3RAM size", "Chinese", "guangzhou", 27986.0, 100022),
        Row("3RAM size", "Chinese", "shenzhen", 92960.0, 100083),
        Row("3RAM size", "Chinese", "wuhan", 36890.0, 100058),
        Row("3RAM size", "Chinese", "xiangtan", 53536.0, 100059),
        Row("3RAM size", "Chinese", "yichang", 20874.0, 100069),
        Row("3RAM size", "Chinese", "zhuzhou", 79786.0, 100053),
        Row("4RAM size", "Chinese", "changsha", 200860.0, 100065),
        Row("4RAM size", "Chinese", "guangzhou", 38016.0, 100055),
        Row("4RAM size", "Chinese", "shenzhen", 49610.0, 100060),
        Row("4RAM size", "Chinese", "wuhan", 117581.96999999999, 100046),
        Row("4RAM size", "Chinese", "xiangtan", 254320.0, 1000000),
        Row("4RAM size", "Chinese", "yichang", 29436.0, 100076),
        Row("5RAM size", "Chinese", "changsha", 13845.0, 100077),
        Row("5RAM size", "Chinese", "guangzhou", 23560.0, 100075),
        Row("5RAM size", "Chinese", "wuhan", 12390.0, 10006),
        Row("6RAM size", "Chinese", "changsha", 23697.0, 100067),
        Row("6RAM size", "Chinese", "guangzhou", 15912.0, 100026),
        Row("6RAM size", "Chinese", "shenzhen", 19278.0, 100035),
        Row("6RAM size", "Chinese", "wuhan", 29313.0, 100078),
        Row("6RAM size", "Chinese", "xiangtan", 7794.0, 100012),
        Row("6RAM size", "Chinese", "zhuzhou", 26568.0, 100062),
        Row("7RAM size", "Chinese", "changsha", 1057.0, 100014),
        Row("7RAM size", "Chinese", "wuhan", 27853.0, 100039),
        Row("7RAM size", "Chinese", "yichang", 14217.0, 100033),
        Row("7RAM size", "Chinese", "zhuzhou", 15673.0, 100003),
        Row("8RAM size", "Chinese", "guangzhou", 27385.619999999995, 1),
        Row("8RAM size", "Chinese", "shenzhen", 3550.0, 100013),
        Row("8RAM size", "Chinese", "wuhan", 29700.0, 100004),
        Row("8RAM size", "Chinese", "xiangtan", 31020.0, 100038),
        Row("8RAM size", "Chinese", "yichang", 51660.0, 100023),
        Row("8RAM size", "Chinese", "zhuzhou", 26840.0, 100081),
        Row("9RAM size", "Chinese", "changsha", 32390.0, 100037),
        Row("9RAM size", "Chinese", "shenzhen", 30650.0, 100054),
        Row("9RAM size", "Chinese", "wuhan", 15670.0, 100070),
        Row("9RAM size", "Chinese", "xiangtan", 58210.0, 100082),
        Row("9RAM size", "Chinese", "yichang", 5710.0, 100043)
      )
    )
  }
  )

  //TC_534
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, MIN" +
      "(Carbon_automation_test5.deviceInformationId) AS Min_deviceInformationId, SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize," +
      "deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from " +
      "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
      "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
      "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
      "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
      "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, MIN" +
          "(Carbon_automation_test5.deviceInformationId) AS Min_deviceInformationId, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize," +
          "deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from " +
          "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 100005, 84293.0),
        Row("0RAM size", "Chinese", "guangzhou", 100010, 869.0),
        Row("0RAM size", "Chinese", "shenzhen", 100028, 31339.0),
        Row("0RAM size", "Chinese", "wuhan", 100008, 66902.0),
        Row("0RAM size", "Chinese", "zhuzhou", 100002, 14751.0),
        Row("1RAM size", "Chinese", "guangzhou", 100030, 11997.0),
        Row("1RAM size", "Chinese", "shenzhen", 100020, 2304.0),
        Row("1RAM size", "Chinese", "xiangtan", 10000, 67590.0),
        Row("1RAM size", "Chinese", "yichang", 100040, 35478.0),
        Row("1RAM size", "Chinese", "zhuzhou", 100042, 24705.0),
        Row("2RAM size", "Chinese", "changsha", 100071, 3946.0),
        Row("2RAM size", "Chinese", "xiangtan", 10007, 2700.0),
        Row("3RAM size", "Chinese", "changsha", 100015, 40082.0),
        Row("3RAM size", "Chinese", "guangzhou", 100022, 27986.0),
        Row("3RAM size", "Chinese", "shenzhen", 100032, 92960.0),
        Row("3RAM size", "Chinese", "wuhan", 100058, 36890.0),
        Row("3RAM size", "Chinese", "xiangtan", 100031, 53536.0),
        Row("3RAM size", "Chinese", "yichang", 100069, 20874.0),
        Row("3RAM size", "Chinese", "zhuzhou", 10005, 79786.0),
        Row("4RAM size", "Chinese", "changsha", 10008, 200860.0),
        Row("4RAM size", "Chinese", "guangzhou", 100055, 38016.0),
        Row("4RAM size", "Chinese", "shenzhen", 10004, 49610.0),
        Row("4RAM size", "Chinese", "wuhan", 10, 117581.96999999999),
        Row("4RAM size", "Chinese", "xiangtan", 100007, 254320.0),
        Row("4RAM size", "Chinese", "yichang", 100000, 29436.0),
        Row("5RAM size", "Chinese", "changsha", 1000, 13845.0),
        Row("5RAM size", "Chinese", "guangzhou", 100017, 23560.0),
        Row("5RAM size", "Chinese", "wuhan", 10006, 12390.0),
        Row("6RAM size", "Chinese", "changsha", 100034, 23697.0),
        Row("6RAM size", "Chinese", "guangzhou", 100026, 15912.0),
        Row("6RAM size", "Chinese", "shenzhen", 100035, 19278.0),
        Row("6RAM size", "Chinese", "wuhan", 100047, 29313.0),
        Row("6RAM size", "Chinese", "xiangtan", 10001, 7794.0),
        Row("6RAM size", "Chinese", "zhuzhou", 100062, 26568.0),
        Row("7RAM size", "Chinese", "changsha", 100014, 1057.0),
        Row("7RAM size", "Chinese", "wuhan", 100001, 27853.0),
        Row("7RAM size", "Chinese", "yichang", 100, 14217.0),
        Row("7RAM size", "Chinese", "zhuzhou", 100003, 15673.0),
        Row("8RAM size", "Chinese", "guangzhou", 1, 27385.619999999995),
        Row("8RAM size", "Chinese", "shenzhen", 100013, 3550.0),
        Row("8RAM size", "Chinese", "wuhan", 100004, 29700.0),
        Row("8RAM size", "Chinese", "xiangtan", 100016, 31020.0),
        Row("8RAM size", "Chinese", "yichang", 10002, 51660.0),
        Row("8RAM size", "Chinese", "zhuzhou", 100052, 26840.0),
        Row("9RAM size", "Chinese", "changsha", 100036, 32390.0),
        Row("9RAM size", "Chinese", "shenzhen", 100044, 30650.0),
        Row("9RAM size", "Chinese", "wuhan", 100070, 15670.0),
        Row("9RAM size", "Chinese", "xiangtan", 10003, 58210.0),
        Row("9RAM size", "Chinese", "yichang", 100043, 5710.0)
      )
    )
  }
  )

  //TC_537
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId," +
      " deviceInformationId,ActiveCountry, Activecity FROM (select * from " +
      "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
      "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
      "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
      "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
      "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize," +
          "gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from " +
          "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 4401364, 84293.0),
        Row("0RAM size", "Chinese", "guangzhou", 1100110, 869.0),
        Row("0RAM size", "Chinese", "shenzhen", 1100308, 31339.0),
        Row("0RAM size", "Chinese", "wuhan", 4401639, 66902.0),
        Row("0RAM size", "Chinese", "zhuzhou", 1100022, 14751.0),
        Row("1RAM size", "Chinese", "guangzhou", 900270, 11997.0),
        Row("1RAM size", "Chinese", "shenzhen", 900180, 2304.0),
        Row("1RAM size", "Chinese", "xiangtan", 2790900, 67590.0),
        Row("1RAM size", "Chinese", "yichang", 1800954, 35478.0),
        Row("1RAM size", "Chinese", "zhuzhou", 900378, 24705.0),
        Row("2RAM size", "Chinese", "changsha", 200142, 3946.0),
        Row("2RAM size", "Chinese", "xiangtan", 20014, 2700.0),
        Row("3RAM size", "Chinese", "changsha", 1400210, 40082.0),
        Row("3RAM size", "Chinese", "guangzhou", 1400308, 27986.0),
        Row("3RAM size", "Chinese", "shenzhen", 5603668, 92960.0),
        Row("3RAM size", "Chinese", "wuhan", 1400812, 36890.0),
        Row("3RAM size", "Chinese", "xiangtan", 4201974, 53536.0),
        Row("3RAM size", "Chinese", "yichang", 1400966, 20874.0),
        Row("3RAM size", "Chinese", "zhuzhou", 2941190, 79786.0),
        Row("4RAM size", "Chinese", "changsha", 11225038, 200860.0),
        Row("4RAM size", "Chinese", "guangzhou", 2201210, 38016.0),
        Row("4RAM size", "Chinese", "shenzhen", 2421408, 49610.0),
        Row("4RAM size", "Chinese", "wuhan", 4402222, 117581.96999999999),
        Row("4RAM size", "Chinese", "xiangtan", 33004774, 254320.0),
        Row("4RAM size", "Chinese", "yichang", 8803168, 29436.0),
        Row("5RAM size", "Chinese", "changsha", 505385, 13845.0),
        Row("5RAM size", "Chinese", "guangzhou", 1000460, 23560.0),
        Row("5RAM size", "Chinese", "wuhan", 50030, 12390.0),
        Row("6RAM size", "Chinese", "changsha", 1800909, 23697.0),
        Row("6RAM size", "Chinese", "guangzhou", 900234, 15912.0),
        Row("6RAM size", "Chinese", "shenzhen", 900315, 19278.0),
        Row("6RAM size", "Chinese", "wuhan", 1801125, 29313.0),
        Row("6RAM size", "Chinese", "xiangtan", 990117, 7794.0),
        Row("6RAM size", "Chinese", "zhuzhou", 900558, 26568.0),
        Row("7RAM size", "Chinese", "changsha", 700098, 1057.0),
        Row("7RAM size", "Chinese", "wuhan", 2100455, 27853.0),
        Row("7RAM size", "Chinese", "yichang", 700931, 14217.0),
        Row("7RAM size", "Chinese", "zhuzhou", 700021, 15673.0),
        Row("8RAM size", "Chinese", "guangzhou", 10, 27385.619999999995),
        Row("8RAM size", "Chinese", "shenzhen", 1000130, 3550.0),
        Row("8RAM size", "Chinese", "wuhan", 1000040, 29700.0),
        Row("8RAM size", "Chinese", "xiangtan", 2000540, 31020.0),
        Row("8RAM size", "Chinese", "yichang", 1100250, 51660.0),
        Row("8RAM size", "Chinese", "zhuzhou", 3001960, 26840.0),
        Row("9RAM size", "Chinese", "changsha", 2000730, 32390.0),
        Row("9RAM size", "Chinese", "shenzhen", 2000980, 30650.0),
        Row("9RAM size", "Chinese", "wuhan", 1000700, 15670.0),
        Row("9RAM size", "Chinese", "xiangtan", 3102370, 58210.0),
        Row("9RAM size", "Chinese", "yichang", 1000430, 5710.0)
      )
    )
  }
  )

  //TC_538
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, AVG" +
      "(Carbon_automation_test5.gamePointId) AS Avg_gamePointId FROM ( SELECT AMSize,gamePointId " +
      ",deviceInformationId, ActiveCountry, Activecity FROM (select * from " +
      "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
      "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
      "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
      "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
      "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, AVG" +
          "(Carbon_automation_test5.gamePointId) AS Avg_gamePointId FROM ( SELECT AMSize," +
          "gamePointId ,deviceInformationId, ActiveCountry, Activecity FROM (select * from " +
          "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 4401364, 1915.75),
        Row("0RAM size", "Chinese", "guangzhou", 1100110, 79.0),
        Row("0RAM size", "Chinese", "shenzhen", 1100308, 2849.0),
        Row("0RAM size", "Chinese", "wuhan", 4401639, 1520.5),
        Row("0RAM size", "Chinese", "zhuzhou", 1100022, 1341.0),
        Row("1RAM size", "Chinese", "guangzhou", 900270, 1333.0),
        Row("1RAM size", "Chinese", "shenzhen", 900180, 256.0),
        Row("1RAM size", "Chinese", "xiangtan", 2790900, 1877.5),
        Row("1RAM size", "Chinese", "yichang", 1800954, 1971.0),
        Row("1RAM size", "Chinese", "zhuzhou", 900378, 2745.0),
        Row("2RAM size", "Chinese", "changsha", 200142, 1973.0),
        Row("2RAM size", "Chinese", "xiangtan", 20014, 1350.0),
        Row("3RAM size", "Chinese", "changsha", 1400210, 2863.0),
        Row("3RAM size", "Chinese", "guangzhou", 1400308, 1999.0),
        Row("3RAM size", "Chinese", "shenzhen", 5603668, 1660.0),
        Row("3RAM size", "Chinese", "wuhan", 1400812, 2635.0),
        Row("3RAM size", "Chinese", "xiangtan", 4201974, 1274.6666666666667),
        Row("3RAM size", "Chinese", "yichang", 1400966, 1491.0),
        Row("3RAM size", "Chinese", "zhuzhou", 2941190, 1899.6666666666667),
        Row("4RAM size", "Chinese", "changsha", 11225038, 1521.6666666666667),
        Row("4RAM size", "Chinese", "guangzhou", 2201210, 1728.0),
        Row("4RAM size", "Chinese", "shenzhen", 2421408, 1127.5),
        Row("4RAM size", "Chinese", "wuhan", 4402222, 1781.5449999999998),
        Row("4RAM size", "Chinese", "xiangtan", 33004774, 1926.6666666666667),
        Row("4RAM size", "Chinese", "yichang", 8803168, 334.5),
        Row("5RAM size", "Chinese", "changsha", 505385, 1384.5),
        Row("5RAM size", "Chinese", "guangzhou", 1000460, 2356.0),
        Row("5RAM size", "Chinese", "wuhan", 50030, 2478.0),
        Row("6RAM size", "Chinese", "changsha", 1800909, 1316.5),
        Row("6RAM size", "Chinese", "guangzhou", 900234, 1768.0),
        Row("6RAM size", "Chinese", "shenzhen", 900315, 2142.0),
        Row("6RAM size", "Chinese", "wuhan", 1801125, 1628.5),
        Row("6RAM size", "Chinese", "xiangtan", 990117, 433.0),
        Row("6RAM size", "Chinese", "zhuzhou", 900558, 2952.0),
        Row("7RAM size", "Chinese", "changsha", 700098, 151.0),
        Row("7RAM size", "Chinese", "wuhan", 2100455, 1326.3333333333333),
        Row("7RAM size", "Chinese", "yichang", 700931, 1015.5),
        Row("7RAM size", "Chinese", "zhuzhou", 700021, 2239.0),
        Row("8RAM size", "Chinese", "guangzhou", 10, 2738.5619999999994),
        Row("8RAM size", "Chinese", "shenzhen", 1000130, 355.0),
        Row("8RAM size", "Chinese", "wuhan", 1000040, 2970.0),
        Row("8RAM size", "Chinese", "xiangtan", 2000540, 1551.0),
        Row("8RAM size", "Chinese", "yichang", 1100250, 2583.0),
        Row("8RAM size", "Chinese", "zhuzhou", 3001960, 894.6666666666666),
        Row("9RAM size", "Chinese", "changsha", 2000730, 1619.5),
        Row("9RAM size", "Chinese", "shenzhen", 2000980, 1532.5),
        Row("9RAM size", "Chinese", "wuhan", 1000700, 1567.0),
        Row("9RAM size", "Chinese", "xiangtan", 3102370, 1455.25),
        Row("9RAM size", "Chinese", "yichang", 1000430, 571.0)
      )
    )
  }
  )

  //TC_539
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, COUNT" +
      "(Carbon_automation_test5.gamePointId) AS Count_gamePointId FROM ( SELECT AMSize, " +
      "gamePointId,ActiveCountry, deviceInformationId,Activecity FROM (select * from " +
      "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
      "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
      "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
      "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
      "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, COUNT" +
          "(Carbon_automation_test5.gamePointId) AS Count_gamePointId FROM ( SELECT AMSize, " +
          "gamePointId,ActiveCountry, deviceInformationId,Activecity FROM (select * from " +
          "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 4401364, 44),
        Row("0RAM size", "Chinese", "guangzhou", 1100110, 11),
        Row("0RAM size", "Chinese", "shenzhen", 1100308, 11),
        Row("0RAM size", "Chinese", "wuhan", 4401639, 44),
        Row("0RAM size", "Chinese", "zhuzhou", 1100022, 11),
        Row("1RAM size", "Chinese", "guangzhou", 900270, 9),
        Row("1RAM size", "Chinese", "shenzhen", 900180, 9),
        Row("1RAM size", "Chinese", "xiangtan", 2790900, 36),
        Row("1RAM size", "Chinese", "yichang", 1800954, 18),
        Row("1RAM size", "Chinese", "zhuzhou", 900378, 9),
        Row("2RAM size", "Chinese", "changsha", 200142, 2),
        Row("2RAM size", "Chinese", "xiangtan", 20014, 2),
        Row("3RAM size", "Chinese", "changsha", 1400210, 14),
        Row("3RAM size", "Chinese", "guangzhou", 1400308, 14),
        Row("3RAM size", "Chinese", "shenzhen", 5603668, 56),
        Row("3RAM size", "Chinese", "wuhan", 1400812, 14),
        Row("3RAM size", "Chinese", "xiangtan", 4201974, 42),
        Row("3RAM size", "Chinese", "yichang", 1400966, 14),
        Row("3RAM size", "Chinese", "zhuzhou", 2941190, 42),
        Row("4RAM size", "Chinese", "changsha", 11225038, 132),
        Row("4RAM size", "Chinese", "guangzhou", 2201210, 22),
        Row("4RAM size", "Chinese", "shenzhen", 2421408, 44),
        Row("4RAM size", "Chinese", "wuhan", 4402222, 66),
        Row("4RAM size", "Chinese", "xiangtan", 33004774, 132),
        Row("4RAM size", "Chinese", "yichang", 8803168, 88),
        Row("5RAM size", "Chinese", "changsha", 505385, 10),
        Row("5RAM size", "Chinese", "guangzhou", 1000460, 10),
        Row("5RAM size", "Chinese", "wuhan", 50030, 5),
        Row("6RAM size", "Chinese", "changsha", 1800909, 18),
        Row("6RAM size", "Chinese", "guangzhou", 900234, 9),
        Row("6RAM size", "Chinese", "shenzhen", 900315, 9),
        Row("6RAM size", "Chinese", "wuhan", 1801125, 18),
        Row("6RAM size", "Chinese", "xiangtan", 990117, 18),
        Row("6RAM size", "Chinese", "zhuzhou", 900558, 9),
        Row("7RAM size", "Chinese", "changsha", 700098, 7),
        Row("7RAM size", "Chinese", "wuhan", 2100455, 21),
        Row("7RAM size", "Chinese", "yichang", 700931, 14),
        Row("7RAM size", "Chinese", "zhuzhou", 700021, 7),
        Row("8RAM size", "Chinese", "guangzhou", 10, 10),
        Row("8RAM size", "Chinese", "shenzhen", 1000130, 10),
        Row("8RAM size", "Chinese", "wuhan", 1000040, 10),
        Row("8RAM size", "Chinese", "xiangtan", 2000540, 20),
        Row("8RAM size", "Chinese", "yichang", 1100250, 20),
        Row("8RAM size", "Chinese", "zhuzhou", 3001960, 30),
        Row("9RAM size", "Chinese", "changsha", 2000730, 20),
        Row("9RAM size", "Chinese", "shenzhen", 2000980, 20),
        Row("9RAM size", "Chinese", "wuhan", 1000700, 10),
        Row("9RAM size", "Chinese", "xiangtan", 3102370, 40),
        Row("9RAM size", "Chinese", "yichang", 1000430, 10)
      )
    )
  }
  )

  //TC_540
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, COUNT(DISTINCT " +
      "Carbon_automation_test5.gamePointId) AS DistinctCount_gamePointId FROM ( SELECT AMSize," +
      "deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from " +
      "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
      "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
      "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
      "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
      "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, COUNT" +
          "(DISTINCT Carbon_automation_test5.gamePointId) AS DistinctCount_gamePointId FROM ( " +
          "SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select " +
          "* from Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 4401364, 4),
        Row("0RAM size", "Chinese", "guangzhou", 1100110, 1),
        Row("0RAM size", "Chinese", "shenzhen", 1100308, 1),
        Row("0RAM size", "Chinese", "wuhan", 4401639, 4),
        Row("0RAM size", "Chinese", "zhuzhou", 1100022, 1),
        Row("1RAM size", "Chinese", "guangzhou", 900270, 1),
        Row("1RAM size", "Chinese", "shenzhen", 900180, 1),
        Row("1RAM size", "Chinese", "xiangtan", 2790900, 4),
        Row("1RAM size", "Chinese", "yichang", 1800954, 2),
        Row("1RAM size", "Chinese", "zhuzhou", 900378, 1),
        Row("2RAM size", "Chinese", "changsha", 200142, 1),
        Row("2RAM size", "Chinese", "xiangtan", 20014, 1),
        Row("3RAM size", "Chinese", "changsha", 1400210, 1),
        Row("3RAM size", "Chinese", "guangzhou", 1400308, 1),
        Row("3RAM size", "Chinese", "shenzhen", 5603668, 4),
        Row("3RAM size", "Chinese", "wuhan", 1400812, 1),
        Row("3RAM size", "Chinese", "xiangtan", 4201974, 3),
        Row("3RAM size", "Chinese", "yichang", 1400966, 1),
        Row("3RAM size", "Chinese", "zhuzhou", 2941190, 3),
        Row("4RAM size", "Chinese", "changsha", 11225038, 6),
        Row("4RAM size", "Chinese", "guangzhou", 2201210, 1),
        Row("4RAM size", "Chinese", "shenzhen", 2421408, 2),
        Row("4RAM size", "Chinese", "wuhan", 4402222, 3),
        Row("4RAM size", "Chinese", "xiangtan", 33004774, 6),
        Row("4RAM size", "Chinese", "yichang", 8803168, 4),
        Row("5RAM size", "Chinese", "changsha", 505385, 2),
        Row("5RAM size", "Chinese", "guangzhou", 1000460, 2),
        Row("5RAM size", "Chinese", "wuhan", 50030, 1),
        Row("6RAM size", "Chinese", "changsha", 1800909, 2),
        Row("6RAM size", "Chinese", "guangzhou", 900234, 1),
        Row("6RAM size", "Chinese", "shenzhen", 900315, 1),
        Row("6RAM size", "Chinese", "wuhan", 1801125, 2),
        Row("6RAM size", "Chinese", "xiangtan", 990117, 2),
        Row("6RAM size", "Chinese", "zhuzhou", 900558, 1),
        Row("7RAM size", "Chinese", "changsha", 700098, 1),
        Row("7RAM size", "Chinese", "wuhan", 2100455, 3),
        Row("7RAM size", "Chinese", "yichang", 700931, 2),
        Row("7RAM size", "Chinese", "zhuzhou", 700021, 1),
        Row("8RAM size", "Chinese", "guangzhou", 10, 1),
        Row("8RAM size", "Chinese", "shenzhen", 1000130, 1),
        Row("8RAM size", "Chinese", "wuhan", 1000040, 1),
        Row("8RAM size", "Chinese", "xiangtan", 2000540, 2),
        Row("8RAM size", "Chinese", "yichang", 1100250, 2),
        Row("8RAM size", "Chinese", "zhuzhou", 3001960, 3),
        Row("9RAM size", "Chinese", "changsha", 2000730, 2),
        Row("9RAM size", "Chinese", "shenzhen", 2000980, 2),
        Row("9RAM size", "Chinese", "wuhan", 1000700, 1),
        Row("9RAM size", "Chinese", "xiangtan", 3102370, 4),
        Row("9RAM size", "Chinese", "yichang", 1000430, 1)
      )
    )
  }
  )

  //TC_542
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, MAX" +
      "(Carbon_automation_test5.gamePointId) AS Max_gamePointId FROM ( SELECT AMSize, " +
      "deviceInformationId,gamePointId,ActiveCountry, Activecity FROM (select * from " +
      "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
      "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
      "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
      "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
      "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, MAX" +
          "(Carbon_automation_test5.gamePointId) AS Max_gamePointId FROM ( SELECT AMSize, " +
          "deviceInformationId,gamePointId,ActiveCountry, Activecity FROM (select * from " +
          "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 4401364, 2593.0),
        Row("0RAM size", "Chinese", "guangzhou", 1100110, 79.0),
        Row("0RAM size", "Chinese", "shenzhen", 1100308, 2849.0),
        Row("0RAM size", "Chinese", "wuhan", 4401639, 2483.0),
        Row("0RAM size", "Chinese", "zhuzhou", 1100022, 1341.0),
        Row("1RAM size", "Chinese", "guangzhou", 900270, 1333.0),
        Row("1RAM size", "Chinese", "shenzhen", 900180, 256.0),
        Row("1RAM size", "Chinese", "xiangtan", 2790900, 2734.0),
        Row("1RAM size", "Chinese", "yichang", 1800954, 2078.0),
        Row("1RAM size", "Chinese", "zhuzhou", 900378, 2745.0),
        Row("2RAM size", "Chinese", "changsha", 200142, 1973.0),
        Row("2RAM size", "Chinese", "xiangtan", 20014, 1350.0),
        Row("3RAM size", "Chinese", "changsha", 1400210, 2863.0),
        Row("3RAM size", "Chinese", "guangzhou", 1400308, 1999.0),
        Row("3RAM size", "Chinese", "shenzhen", 5603668, 2488.0),
        Row("3RAM size", "Chinese", "wuhan", 1400812, 2635.0),
        Row("3RAM size", "Chinese", "xiangtan", 4201974, 1407.0),
        Row("3RAM size", "Chinese", "yichang", 1400966, 1491.0),
        Row("3RAM size", "Chinese", "zhuzhou", 2941190, 2436.0),
        Row("4RAM size", "Chinese", "changsha", 11225038, 2572.0),
        Row("4RAM size", "Chinese", "guangzhou", 2201210, 1728.0),
        Row("4RAM size", "Chinese", "shenzhen", 2421408, 1717.0),
        Row("4RAM size", "Chinese", "wuhan", 4402222, 2553.0),
        Row("4RAM size", "Chinese", "xiangtan", 33004774, 2890.0),
        Row("4RAM size", "Chinese", "yichang", 8803168, 732.0),
        Row("5RAM size", "Chinese", "changsha", 505385, 2077.0),
        Row("5RAM size", "Chinese", "guangzhou", 1000460, 2507.0),
        Row("5RAM size", "Chinese", "wuhan", 50030, 2478.0),
        Row("6RAM size", "Chinese", "changsha", 1800909, 2061.0),
        Row("6RAM size", "Chinese", "guangzhou", 900234, 1768.0),
        Row("6RAM size", "Chinese", "shenzhen", 900315, 2142.0),
        Row("6RAM size", "Chinese", "wuhan", 1801125, 1823.0),
        Row("6RAM size", "Chinese", "xiangtan", 990117, 568.0),
        Row("6RAM size", "Chinese", "zhuzhou", 900558, 2952.0),
        Row("7RAM size", "Chinese", "changsha", 700098, 151.0),
        Row("7RAM size", "Chinese", "wuhan", 2100455, 1750.0),
        Row("7RAM size", "Chinese", "yichang", 700931, 1271.0),
        Row("7RAM size", "Chinese", "zhuzhou", 700021, 2239.0),
        Row("8RAM size", "Chinese", "guangzhou", 10, 2738.562),
        Row("8RAM size", "Chinese", "shenzhen", 1000130, 355.0),
        Row("8RAM size", "Chinese", "wuhan", 1000040, 2970.0),
        Row("8RAM size", "Chinese", "xiangtan", 2000540, 1873.0),
        Row("8RAM size", "Chinese", "yichang", 1100250, 2972.0),
        Row("8RAM size", "Chinese", "zhuzhou", 3001960, 1226.0),
        Row("9RAM size", "Chinese", "changsha", 2000730, 2224.0),
        Row("9RAM size", "Chinese", "shenzhen", 2000980, 1697.0),
        Row("9RAM size", "Chinese", "wuhan", 1000700, 1567.0),
        Row("9RAM size", "Chinese", "xiangtan", 3102370, 2348.0),
        Row("9RAM size", "Chinese", "yichang", 1000430, 571.0)
      )
    )
  }
  )

  //TC_543
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, MIN" +
      "(Carbon_automation_test5.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize,gamePointId," +
      "deviceInformationId, ActiveCountry, Activecity FROM (select * from " +
      "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
      "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
      "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
      "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
      "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, MIN" +
          "(Carbon_automation_test5.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize," +
          "gamePointId,deviceInformationId, ActiveCountry, Activecity FROM (select * from " +
          "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 4401364, 1098.0),
        Row("0RAM size", "Chinese", "guangzhou", 1100110, 79.0),
        Row("0RAM size", "Chinese", "shenzhen", 1100308, 2849.0),
        Row("0RAM size", "Chinese", "wuhan", 4401639, 750.0),
        Row("0RAM size", "Chinese", "zhuzhou", 1100022, 1341.0),
        Row("1RAM size", "Chinese", "guangzhou", 900270, 1333.0),
        Row("1RAM size", "Chinese", "shenzhen", 900180, 256.0),
        Row("1RAM size", "Chinese", "xiangtan", 2790900, 202.0),
        Row("1RAM size", "Chinese", "yichang", 1800954, 1864.0),
        Row("1RAM size", "Chinese", "zhuzhou", 900378, 2745.0),
        Row("2RAM size", "Chinese", "changsha", 200142, 1973.0),
        Row("2RAM size", "Chinese", "xiangtan", 20014, 1350.0),
        Row("3RAM size", "Chinese", "changsha", 1400210, 2863.0),
        Row("3RAM size", "Chinese", "guangzhou", 1400308, 1999.0),
        Row("3RAM size", "Chinese", "shenzhen", 5603668, 907.0),
        Row("3RAM size", "Chinese", "wuhan", 1400812, 2635.0),
        Row("3RAM size", "Chinese", "xiangtan", 4201974, 1080.0),
        Row("3RAM size", "Chinese", "yichang", 1400966, 1491.0),
        Row("3RAM size", "Chinese", "zhuzhou", 2941190, 1608.0),
        Row("4RAM size", "Chinese", "changsha", 11225038, 813.0),
        Row("4RAM size", "Chinese", "guangzhou", 2201210, 1728.0),
        Row("4RAM size", "Chinese", "shenzhen", 2421408, 538.0),
        Row("4RAM size", "Chinese", "wuhan", 4402222, 1077.0),
        Row("4RAM size", "Chinese", "xiangtan", 33004774, 412.0),
        Row("4RAM size", "Chinese", "yichang", 8803168, 29.0),
        Row("5RAM size", "Chinese", "changsha", 505385, 692.0),
        Row("5RAM size", "Chinese", "guangzhou", 1000460, 2205.0),
        Row("5RAM size", "Chinese", "wuhan", 50030, 2478.0),
        Row("6RAM size", "Chinese", "changsha", 1800909, 572.0),
        Row("6RAM size", "Chinese", "guangzhou", 900234, 1768.0),
        Row("6RAM size", "Chinese", "shenzhen", 900315, 2142.0),
        Row("6RAM size", "Chinese", "wuhan", 1801125, 1434.0),
        Row("6RAM size", "Chinese", "xiangtan", 990117, 298.0),
        Row("6RAM size", "Chinese", "zhuzhou", 900558, 2952.0),
        Row("7RAM size", "Chinese", "changsha", 700098, 151.0),
        Row("7RAM size", "Chinese", "wuhan", 2100455, 505.0),
        Row("7RAM size", "Chinese", "yichang", 700931, 760.0),
        Row("7RAM size", "Chinese", "zhuzhou", 700021, 2239.0),
        Row("8RAM size", "Chinese", "guangzhou", 10, 2738.562),
        Row("8RAM size", "Chinese", "shenzhen", 1000130, 355.0),
        Row("8RAM size", "Chinese", "wuhan", 1000040, 2970.0),
        Row("8RAM size", "Chinese", "xiangtan", 2000540, 1229.0),
        Row("8RAM size", "Chinese", "yichang", 1100250, 2194.0),
        Row("8RAM size", "Chinese", "zhuzhou", 3001960, 613.0),
        Row("9RAM size", "Chinese", "changsha", 2000730, 1015.0),
        Row("9RAM size", "Chinese", "shenzhen", 2000980, 1368.0),
        Row("9RAM size", "Chinese", "wuhan", 1000700, 1567.0),
        Row("9RAM size", "Chinese", "xiangtan", 3102370, 448.0),
        Row("9RAM size", "Chinese", "yichang", 1000430, 571.0)
      )
    )
  }
  )

  //TC_547
  test(
    "SELECT Carbon_automation_test5.ActiveCountry AS ActiveCountry, Carbon_automation_test5" +
      ".Activecity AS Activecity, COUNT(DISTINCT Carbon_automation_test5.AMSize) AS " +
      "DistinctCount_AMSize, SUM(Carbon_automation_test5.deviceInformationId) AS " +
      "Sum_deviceInformationId, SUM(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM " +
      "( SELECT AMSize,deviceInformationId, gamePointId,ActiveCountry, Activecity FROM (select * " +
      "from Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
      "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
      "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
      "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.ActiveCountry, " +
      "Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.ActiveCountry AS ActiveCountry, Carbon_automation_test5" +
          ".Activecity AS Activecity, COUNT(DISTINCT Carbon_automation_test5.AMSize) AS " +
          "DistinctCount_AMSize, SUM(Carbon_automation_test5.deviceInformationId) AS " +
          "Sum_deviceInformationId, SUM(Carbon_automation_test5.gamePointId) AS Sum_gamePointId " +
          "FROM ( SELECT AMSize,deviceInformationId, gamePointId,ActiveCountry, Activecity FROM " +
          "(select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN (" +
          " SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5)" +
          " SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.ActiveCountry, " +
          "Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.ActiveCountry ASC," +
          " Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("Chinese", "changsha", 8, 22233876, 400170.0),
        Row("Chinese", "guangzhou", 7, 7502602, 145725.62),
        Row("Chinese", "shenzhen", 7, 13926989, 229691.0),
        Row("Chinese", "wuhan", 8, 16157023, 336299.97),
        Row("Chinese", "xiangtan", 7, 46110689, 475170.0),
        Row("Chinese", "yichang", 6, 14806699, 157375.0),
        Row("Chinese", "zhuzhou", 6, 9544129, 188323.0)
      )
    )
  }
  )

  //TC_549
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM(distinct " +
      "Carbon_automation_test5.gamePointId) AS Sum_distinct_gamePointId, SUM" +
      "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT " +
      "AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from " +
      "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
      "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
      "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
      "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
      "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM(distinct " +
          "Carbon_automation_test5.gamePointId) AS Sum_distinct_gamePointId, SUM" +
          "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT" +
          " AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from" +
          " Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 7663.0, 4401364),
        Row("0RAM size", "Chinese", "guangzhou", 79.0, 1100110),
        Row("0RAM size", "Chinese", "shenzhen", 2849.0, 1100308),
        Row("0RAM size", "Chinese", "wuhan", 6082.0, 4401639),
        Row("0RAM size", "Chinese", "zhuzhou", 1341.0, 1100022),
        Row("1RAM size", "Chinese", "guangzhou", 1333.0, 900270),
        Row("1RAM size", "Chinese", "shenzhen", 256.0, 900180),
        Row("1RAM size", "Chinese", "xiangtan", 7510.0, 2790900),
        Row("1RAM size", "Chinese", "yichang", 3942.0, 1800954),
        Row("1RAM size", "Chinese", "zhuzhou", 2745.0, 900378),
        Row("2RAM size", "Chinese", "changsha", 1973.0, 200142),
        Row("2RAM size", "Chinese", "xiangtan", 1350.0, 20014),
        Row("3RAM size", "Chinese", "changsha", 2863.0, 1400210),
        Row("3RAM size", "Chinese", "guangzhou", 1999.0, 1400308),
        Row("3RAM size", "Chinese", "shenzhen", 6640.0, 5603668),
        Row("3RAM size", "Chinese", "wuhan", 2635.0, 1400812),
        Row("3RAM size", "Chinese", "xiangtan", 3824.0, 4201974),
        Row("3RAM size", "Chinese", "yichang", 1491.0, 1400966),
        Row("3RAM size", "Chinese", "zhuzhou", 5699.0, 2941190),
        Row("4RAM size", "Chinese", "changsha", 9130.0, 11225038),
        Row("4RAM size", "Chinese", "guangzhou", 1728.0, 2201210),
        Row("4RAM size", "Chinese", "shenzhen", 2255.0, 2421408),
        Row("4RAM size", "Chinese", "wuhan", 5344.635, 4402222),
        Row("4RAM size", "Chinese", "xiangtan", 11560.0, 33004774),
        Row("4RAM size", "Chinese", "yichang", 1338.0, 8803168),
        Row("5RAM size", "Chinese", "changsha", 2769.0, 505385),
        Row("5RAM size", "Chinese", "guangzhou", 4712.0, 1000460),
        Row("5RAM size", "Chinese", "wuhan", 2478.0, 50030),
        Row("6RAM size", "Chinese", "changsha", 2633.0, 1800909),
        Row("6RAM size", "Chinese", "guangzhou", 1768.0, 900234),
        Row("6RAM size", "Chinese", "shenzhen", 2142.0, 900315),
        Row("6RAM size", "Chinese", "wuhan", 3257.0, 1801125),
        Row("6RAM size", "Chinese", "xiangtan", 866.0, 990117),
        Row("6RAM size", "Chinese", "zhuzhou", 2952.0, 900558),
        Row("7RAM size", "Chinese", "changsha", 151.0, 700098),
        Row("7RAM size", "Chinese", "wuhan", 3979.0, 2100455),
        Row("7RAM size", "Chinese", "yichang", 2031.0, 700931),
        Row("7RAM size", "Chinese", "zhuzhou", 2239.0, 700021),
        Row("8RAM size", "Chinese", "guangzhou", 2738.562, 10),
        Row("8RAM size", "Chinese", "shenzhen", 355.0, 1000130),
        Row("8RAM size", "Chinese", "wuhan", 2970.0, 1000040),
        Row("8RAM size", "Chinese", "xiangtan", 3102.0, 2000540),
        Row("8RAM size", "Chinese", "yichang", 5166.0, 1100250),
        Row("8RAM size", "Chinese", "zhuzhou", 2684.0, 3001960),
        Row("9RAM size", "Chinese", "changsha", 3239.0, 2000730),
        Row("9RAM size", "Chinese", "shenzhen", 3065.0, 2000980),
        Row("9RAM size", "Chinese", "wuhan", 1567.0, 1000700),
        Row("9RAM size", "Chinese", "xiangtan", 5821.0, 3102370),
        Row("9RAM size", "Chinese", "yichang", 571.0, 1000430)
      )
    )
  }
  )

  //TC_550
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM(distinct " +
      "Carbon_automation_test5.deviceInformationId) AS Sum_distinct_deviceInformationId, SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId," +
      " deviceInformationId,ActiveCountry, Activecity FROM (select * from " +
      "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
      "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
      "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
      "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
      "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM(distinct " +
          "Carbon_automation_test5.deviceInformationId) AS Sum_distinct_deviceInformationId, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize," +
          "gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from " +
          "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 INNER JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 400124, 84293.0),
        Row("0RAM size", "Chinese", "guangzhou", 100010, 869.0),
        Row("0RAM size", "Chinese", "shenzhen", 100028, 31339.0),
        Row("0RAM size", "Chinese", "wuhan", 400149, 66902.0),
        Row("0RAM size", "Chinese", "zhuzhou", 100002, 14751.0),
        Row("1RAM size", "Chinese", "guangzhou", 100030, 11997.0),
        Row("1RAM size", "Chinese", "shenzhen", 100020, 2304.0),
        Row("1RAM size", "Chinese", "xiangtan", 310100, 67590.0),
        Row("1RAM size", "Chinese", "yichang", 200106, 35478.0),
        Row("1RAM size", "Chinese", "zhuzhou", 100042, 24705.0),
        Row("2RAM size", "Chinese", "changsha", 100071, 3946.0),
        Row("2RAM size", "Chinese", "xiangtan", 10007, 2700.0),
        Row("3RAM size", "Chinese", "changsha", 100015, 40082.0),
        Row("3RAM size", "Chinese", "guangzhou", 100022, 27986.0),
        Row("3RAM size", "Chinese", "shenzhen", 400262, 92960.0),
        Row("3RAM size", "Chinese", "wuhan", 100058, 36890.0),
        Row("3RAM size", "Chinese", "xiangtan", 300141, 53536.0),
        Row("3RAM size", "Chinese", "yichang", 100069, 20874.0),
        Row("3RAM size", "Chinese", "zhuzhou", 210085, 79786.0),
        Row("4RAM size", "Chinese", "changsha", 510229, 200860.0),
        Row("4RAM size", "Chinese", "guangzhou", 100055, 38016.0),
        Row("4RAM size", "Chinese", "shenzhen", 110064, 49610.0),
        Row("4RAM size", "Chinese", "wuhan", 200101, 117581.96999999999),
        Row("4RAM size", "Chinese", "xiangtan", 1500217, 254320.0),
        Row("4RAM size", "Chinese", "yichang", 400144, 29436.0),
        Row("5RAM size", "Chinese", "changsha", 101077, 13845.0),
        Row("5RAM size", "Chinese", "guangzhou", 200092, 23560.0),
        Row("5RAM size", "Chinese", "wuhan", 10006, 12390.0),
        Row("6RAM size", "Chinese", "changsha", 200101, 23697.0),
        Row("6RAM size", "Chinese", "guangzhou", 100026, 15912.0),
        Row("6RAM size", "Chinese", "shenzhen", 100035, 19278.0),
        Row("6RAM size", "Chinese", "wuhan", 200125, 29313.0),
        Row("6RAM size", "Chinese", "xiangtan", 110013, 7794.0),
        Row("6RAM size", "Chinese", "zhuzhou", 100062, 26568.0),
        Row("7RAM size", "Chinese", "changsha", 100014, 1057.0),
        Row("7RAM size", "Chinese", "wuhan", 300065, 27853.0),
        Row("7RAM size", "Chinese", "yichang", 100133, 14217.0),
        Row("7RAM size", "Chinese", "zhuzhou", 100003, 15673.0),
        Row("8RAM size", "Chinese", "guangzhou", 1, 27385.619999999995),
        Row("8RAM size", "Chinese", "shenzhen", 100013, 3550.0),
        Row("8RAM size", "Chinese", "wuhan", 100004, 29700.0),
        Row("8RAM size", "Chinese", "xiangtan", 200054, 31020.0),
        Row("8RAM size", "Chinese", "yichang", 110025, 51660.0),
        Row("8RAM size", "Chinese", "zhuzhou", 300196, 26840.0),
        Row("9RAM size", "Chinese", "changsha", 200073, 32390.0),
        Row("9RAM size", "Chinese", "shenzhen", 200098, 30650.0),
        Row("9RAM size", "Chinese", "wuhan", 100070, 15670.0),
        Row("9RAM size", "Chinese", "xiangtan", 310237, 58210.0),
        Row("9RAM size", "Chinese", "yichang", 100043, 5710.0)
      )
    )
  }
  )

  //TC_568
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity , SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
      "ActiveCountry,gamePointId, Activecity FROM (select * from Carbon_automation_test5) SUB_QRY" +
      " ) Carbon_automation_test5 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
      "(select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize WHERE NOT" +
      "(Carbon_automation_test5.AMSize <= \"3RAM size\") GROUP BY Carbon_automation_test5.AMSize," +
      " Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity , SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
          "ActiveCountry,gamePointId, Activecity FROM (select * from Carbon_automation_test5) " +
          "SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize" +
          " FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 " +
          "ON Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize WHERE NOT" +
          "(Carbon_automation_test5.AMSize <= \"3RAM size\") GROUP BY Carbon_automation_test5" +
          ".AMSize, Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity " +
          "ORDER BY Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry " +
          "ASC, Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("4RAM size", "Chinese", "changsha", 200860.0),
        Row("4RAM size", "Chinese", "guangzhou", 38016.0),
        Row("4RAM size", "Chinese", "shenzhen", 49610.0),
        Row("4RAM size", "Chinese", "wuhan", 117581.96999999999),
        Row("4RAM size", "Chinese", "xiangtan", 254320.0),
        Row("4RAM size", "Chinese", "yichang", 29436.0),
        Row("5RAM size", "Chinese", "changsha", 13845.0),
        Row("5RAM size", "Chinese", "guangzhou", 23560.0),
        Row("5RAM size", "Chinese", "wuhan", 12390.0),
        Row("6RAM size", "Chinese", "changsha", 23697.0),
        Row("6RAM size", "Chinese", "guangzhou", 15912.0),
        Row("6RAM size", "Chinese", "shenzhen", 19278.0),
        Row("6RAM size", "Chinese", "wuhan", 29313.0),
        Row("6RAM size", "Chinese", "xiangtan", 7794.0),
        Row("6RAM size", "Chinese", "zhuzhou", 26568.0),
        Row("7RAM size", "Chinese", "changsha", 1057.0),
        Row("7RAM size", "Chinese", "wuhan", 27853.0),
        Row("7RAM size", "Chinese", "yichang", 14217.0),
        Row("7RAM size", "Chinese", "zhuzhou", 15673.0),
        Row("8RAM size", "Chinese", "guangzhou", 27385.619999999995),
        Row("8RAM size", "Chinese", "shenzhen", 3550.0),
        Row("8RAM size", "Chinese", "wuhan", 29700.0),
        Row("8RAM size", "Chinese", "xiangtan", 31020.0),
        Row("8RAM size", "Chinese", "yichang", 51660.0),
        Row("8RAM size", "Chinese", "zhuzhou", 26840.0),
        Row("9RAM size", "Chinese", "changsha", 32390.0),
        Row("9RAM size", "Chinese", "shenzhen", 30650.0),
        Row("9RAM size", "Chinese", "wuhan", 15670.0),
        Row("9RAM size", "Chinese", "xiangtan", 58210.0),
        Row("9RAM size", "Chinese", "yichang", 5710.0)
      )
    )
  }
  )

  //TC_569
  test(
    "SELECT Carbon_automation_test5.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry," +
      " Activecity FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5" +
      " LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
      "Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY " +
      "Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.Activecity ASC LIMIT " +
      "5000"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.Activecity AS Activecity FROM ( SELECT AMSize, " +
          "ActiveCountry, Activecity FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
          "Carbon_automation_test5 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
          "(select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
          "Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY " +
          "Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.Activecity ASC " +
          "LIMIT 5000"
      ),
      Seq(Row("changsha"),
        Row("guangzhou"),
        Row("shenzhen"),
        Row("wuhan"),
        Row("xiangtan"),
        Row("yichang"),
        Row("zhuzhou")
      )
    )
  }
  )

  //TC_571
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId," +
      " ActiveCountry, Activecity FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
      "Carbon_automation_test5 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select " +
      "* from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize WHERE NOT" +
      "(Carbon_automation_test5.Activecity LIKE 'xian%') GROUP BY Carbon_automation_test5.AMSize," +
      " Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize," +
          "gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test5) " +
          "SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize" +
          " FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 " +
          "ON Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize WHERE NOT" +
          "(Carbon_automation_test5.Activecity LIKE 'xian%') GROUP BY Carbon_automation_test5" +
          ".AMSize, Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity " +
          "ORDER BY Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry " +
          "ASC, Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 84293.0),
        Row("0RAM size", "Chinese", "guangzhou", 869.0),
        Row("0RAM size", "Chinese", "shenzhen", 31339.0),
        Row("0RAM size", "Chinese", "wuhan", 66902.0),
        Row("0RAM size", "Chinese", "zhuzhou", 14751.0),
        Row("1RAM size", "Chinese", "guangzhou", 11997.0),
        Row("1RAM size", "Chinese", "shenzhen", 2304.0),
        Row("1RAM size", "Chinese", "yichang", 35478.0),
        Row("1RAM size", "Chinese", "zhuzhou", 24705.0),
        Row("2RAM size", "Chinese", "changsha", 3946.0),
        Row("3RAM size", "Chinese", "changsha", 40082.0),
        Row("3RAM size", "Chinese", "guangzhou", 27986.0),
        Row("3RAM size", "Chinese", "shenzhen", 92960.0),
        Row("3RAM size", "Chinese", "wuhan", 36890.0),
        Row("3RAM size", "Chinese", "yichang", 20874.0),
        Row("3RAM size", "Chinese", "zhuzhou", 79786.0),
        Row("4RAM size", "Chinese", "changsha", 200860.0),
        Row("4RAM size", "Chinese", "guangzhou", 38016.0),
        Row("4RAM size", "Chinese", "shenzhen", 49610.0),
        Row("4RAM size", "Chinese", "wuhan", 117581.96999999999),
        Row("4RAM size", "Chinese", "yichang", 29436.0),
        Row("5RAM size", "Chinese", "changsha", 13845.0),
        Row("5RAM size", "Chinese", "guangzhou", 23560.0),
        Row("5RAM size", "Chinese", "wuhan", 12390.0),
        Row("6RAM size", "Chinese", "changsha", 23697.0),
        Row("6RAM size", "Chinese", "guangzhou", 15912.0),
        Row("6RAM size", "Chinese", "shenzhen", 19278.0),
        Row("6RAM size", "Chinese", "wuhan", 29313.0),
        Row("6RAM size", "Chinese", "zhuzhou", 26568.0),
        Row("7RAM size", "Chinese", "changsha", 1057.0),
        Row("7RAM size", "Chinese", "wuhan", 27853.0),
        Row("7RAM size", "Chinese", "yichang", 14217.0),
        Row("7RAM size", "Chinese", "zhuzhou", 15673.0),
        Row("8RAM size", "Chinese", "guangzhou", 27385.619999999995),
        Row("8RAM size", "Chinese", "shenzhen", 3550.0),
        Row("8RAM size", "Chinese", "wuhan", 29700.0),
        Row("8RAM size", "Chinese", "yichang", 51660.0),
        Row("8RAM size", "Chinese", "zhuzhou", 26840.0),
        Row("9RAM size", "Chinese", "changsha", 32390.0),
        Row("9RAM size", "Chinese", "shenzhen", 30650.0),
        Row("9RAM size", "Chinese", "wuhan", 15670.0),
        Row("9RAM size", "Chinese", "yichang", 5710.0)
      )
    )
  }
  )

  //TC_572
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
      "ActiveCountry, Activecity,gamePointId  FROM (select * from Carbon_automation_test5) " +
      "SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize " +
      "FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize WHERE NOT" +
      "(Carbon_automation_test5.AMSize BETWEEN \"4RAM size\" AND \"7RAM size\") GROUP BY " +
      "Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, " +
      "Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.AMSize ASC, " +
      "Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
          "ActiveCountry, Activecity,gamePointId  FROM (select * from Carbon_automation_test5) " +
          "SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize" +
          " FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 " +
          "ON Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize WHERE NOT" +
          "(Carbon_automation_test5.AMSize BETWEEN \"4RAM size\" AND \"7RAM size\") GROUP BY " +
          "Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, " +
          "Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.AMSize ASC, " +
          "Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 84293.0),
        Row("0RAM size", "Chinese", "guangzhou", 869.0),
        Row("0RAM size", "Chinese", "shenzhen", 31339.0),
        Row("0RAM size", "Chinese", "wuhan", 66902.0),
        Row("0RAM size", "Chinese", "zhuzhou", 14751.0),
        Row("1RAM size", "Chinese", "guangzhou", 11997.0),
        Row("1RAM size", "Chinese", "shenzhen", 2304.0),
        Row("1RAM size", "Chinese", "xiangtan", 67590.0),
        Row("1RAM size", "Chinese", "yichang", 35478.0),
        Row("1RAM size", "Chinese", "zhuzhou", 24705.0),
        Row("2RAM size", "Chinese", "changsha", 3946.0),
        Row("2RAM size", "Chinese", "xiangtan", 2700.0),
        Row("3RAM size", "Chinese", "changsha", 40082.0),
        Row("3RAM size", "Chinese", "guangzhou", 27986.0),
        Row("3RAM size", "Chinese", "shenzhen", 92960.0),
        Row("3RAM size", "Chinese", "wuhan", 36890.0),
        Row("3RAM size", "Chinese", "xiangtan", 53536.0),
        Row("3RAM size", "Chinese", "yichang", 20874.0),
        Row("3RAM size", "Chinese", "zhuzhou", 79786.0),
        Row("8RAM size", "Chinese", "guangzhou", 27385.619999999995),
        Row("8RAM size", "Chinese", "shenzhen", 3550.0),
        Row("8RAM size", "Chinese", "wuhan", 29700.0),
        Row("8RAM size", "Chinese", "xiangtan", 31020.0),
        Row("8RAM size", "Chinese", "yichang", 51660.0),
        Row("8RAM size", "Chinese", "zhuzhou", 26840.0),
        Row("9RAM size", "Chinese", "changsha", 32390.0),
        Row("9RAM size", "Chinese", "shenzhen", 30650.0),
        Row("9RAM size", "Chinese", "wuhan", 15670.0),
        Row("9RAM size", "Chinese", "xiangtan", 58210.0),
        Row("9RAM size", "Chinese", "yichang", 5710.0)
      )
    )
  }
  )

  //TC_573
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
      "ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation_test5) SUB_QRY" +
      " ) Carbon_automation_test5 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
      "(select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize WHERE NOT" +
      "(Carbon_automation_test5.AMSize IN (\"5RAM size\",\"8RAM size\")) GROUP BY " +
      "Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, " +
      "Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.AMSize ASC, " +
      "Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
          "ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation_test5) " +
          "SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize" +
          " FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 " +
          "ON Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize WHERE NOT" +
          "(Carbon_automation_test5.AMSize IN (\"5RAM size\",\"8RAM size\")) GROUP BY " +
          "Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, " +
          "Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.AMSize ASC, " +
          "Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 84293.0),
        Row("0RAM size", "Chinese", "guangzhou", 869.0),
        Row("0RAM size", "Chinese", "shenzhen", 31339.0),
        Row("0RAM size", "Chinese", "wuhan", 66902.0),
        Row("0RAM size", "Chinese", "zhuzhou", 14751.0),
        Row("1RAM size", "Chinese", "guangzhou", 11997.0),
        Row("1RAM size", "Chinese", "shenzhen", 2304.0),
        Row("1RAM size", "Chinese", "xiangtan", 67590.0),
        Row("1RAM size", "Chinese", "yichang", 35478.0),
        Row("1RAM size", "Chinese", "zhuzhou", 24705.0),
        Row("2RAM size", "Chinese", "changsha", 3946.0),
        Row("2RAM size", "Chinese", "xiangtan", 2700.0),
        Row("3RAM size", "Chinese", "changsha", 40082.0),
        Row("3RAM size", "Chinese", "guangzhou", 27986.0),
        Row("3RAM size", "Chinese", "shenzhen", 92960.0),
        Row("3RAM size", "Chinese", "wuhan", 36890.0),
        Row("3RAM size", "Chinese", "xiangtan", 53536.0),
        Row("3RAM size", "Chinese", "yichang", 20874.0),
        Row("3RAM size", "Chinese", "zhuzhou", 79786.0),
        Row("4RAM size", "Chinese", "changsha", 200860.0),
        Row("4RAM size", "Chinese", "guangzhou", 38016.0),
        Row("4RAM size", "Chinese", "shenzhen", 49610.0),
        Row("4RAM size", "Chinese", "wuhan", 117581.96999999999),
        Row("4RAM size", "Chinese", "xiangtan", 254320.0),
        Row("4RAM size", "Chinese", "yichang", 29436.0),
        Row("6RAM size", "Chinese", "changsha", 23697.0),
        Row("6RAM size", "Chinese", "guangzhou", 15912.0),
        Row("6RAM size", "Chinese", "shenzhen", 19278.0),
        Row("6RAM size", "Chinese", "wuhan", 29313.0),
        Row("6RAM size", "Chinese", "xiangtan", 7794.0),
        Row("6RAM size", "Chinese", "zhuzhou", 26568.0),
        Row("7RAM size", "Chinese", "changsha", 1057.0),
        Row("7RAM size", "Chinese", "wuhan", 27853.0),
        Row("7RAM size", "Chinese", "yichang", 14217.0),
        Row("7RAM size", "Chinese", "zhuzhou", 15673.0),
        Row("9RAM size", "Chinese", "changsha", 32390.0),
        Row("9RAM size", "Chinese", "shenzhen", 30650.0),
        Row("9RAM size", "Chinese", "wuhan", 15670.0),
        Row("9RAM size", "Chinese", "xiangtan", 58210.0),
        Row("9RAM size", "Chinese", "yichang", 5710.0)
      )
    )
  }
  )

  //TC_574
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
      "ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation_test5) SUB_QRY" +
      " ) Carbon_automation_test5 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
      "(select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize WHERE " +
      "Carbon_automation_test5.AMSize IS NOT NULL GROUP BY Carbon_automation_test5.AMSize, " +
      "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
          "ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation_test5) " +
          "SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize" +
          " FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 " +
          "ON Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize WHERE " +
          "Carbon_automation_test5.AMSize IS NOT NULL GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 84293.0),
        Row("0RAM size", "Chinese", "guangzhou", 869.0),
        Row("0RAM size", "Chinese", "shenzhen", 31339.0),
        Row("0RAM size", "Chinese", "wuhan", 66902.0),
        Row("0RAM size", "Chinese", "zhuzhou", 14751.0),
        Row("1RAM size", "Chinese", "guangzhou", 11997.0),
        Row("1RAM size", "Chinese", "shenzhen", 2304.0),
        Row("1RAM size", "Chinese", "xiangtan", 67590.0),
        Row("1RAM size", "Chinese", "yichang", 35478.0),
        Row("1RAM size", "Chinese", "zhuzhou", 24705.0),
        Row("2RAM size", "Chinese", "changsha", 3946.0),
        Row("2RAM size", "Chinese", "xiangtan", 2700.0),
        Row("3RAM size", "Chinese", "changsha", 40082.0),
        Row("3RAM size", "Chinese", "guangzhou", 27986.0),
        Row("3RAM size", "Chinese", "shenzhen", 92960.0),
        Row("3RAM size", "Chinese", "wuhan", 36890.0),
        Row("3RAM size", "Chinese", "xiangtan", 53536.0),
        Row("3RAM size", "Chinese", "yichang", 20874.0),
        Row("3RAM size", "Chinese", "zhuzhou", 79786.0),
        Row("4RAM size", "Chinese", "changsha", 200860.0),
        Row("4RAM size", "Chinese", "guangzhou", 38016.0),
        Row("4RAM size", "Chinese", "shenzhen", 49610.0),
        Row("4RAM size", "Chinese", "wuhan", 117581.96999999999),
        Row("4RAM size", "Chinese", "xiangtan", 254320.0),
        Row("4RAM size", "Chinese", "yichang", 29436.0),
        Row("5RAM size", "Chinese", "changsha", 13845.0),
        Row("5RAM size", "Chinese", "guangzhou", 23560.0),
        Row("5RAM size", "Chinese", "wuhan", 12390.0),
        Row("6RAM size", "Chinese", "changsha", 23697.0),
        Row("6RAM size", "Chinese", "guangzhou", 15912.0),
        Row("6RAM size", "Chinese", "shenzhen", 19278.0),
        Row("6RAM size", "Chinese", "wuhan", 29313.0),
        Row("6RAM size", "Chinese", "xiangtan", 7794.0),
        Row("6RAM size", "Chinese", "zhuzhou", 26568.0),
        Row("7RAM size", "Chinese", "changsha", 1057.0),
        Row("7RAM size", "Chinese", "wuhan", 27853.0),
        Row("7RAM size", "Chinese", "yichang", 14217.0),
        Row("7RAM size", "Chinese", "zhuzhou", 15673.0),
        Row("8RAM size", "Chinese", "guangzhou", 27385.619999999995),
        Row("8RAM size", "Chinese", "shenzhen", 3550.0),
        Row("8RAM size", "Chinese", "wuhan", 29700.0),
        Row("8RAM size", "Chinese", "xiangtan", 31020.0),
        Row("8RAM size", "Chinese", "yichang", 51660.0),
        Row("8RAM size", "Chinese", "zhuzhou", 26840.0),
        Row("9RAM size", "Chinese", "changsha", 32390.0),
        Row("9RAM size", "Chinese", "shenzhen", 30650.0),
        Row("9RAM size", "Chinese", "wuhan", 15670.0),
        Row("9RAM size", "Chinese", "xiangtan", 58210.0),
        Row("9RAM size", "Chinese", "yichang", 5710.0)
      )
    )
  }
  )

  //TC_578
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId, SUM(Carbon_automation_test5" +
      ".deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, gamePointId," +
      "ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_test5)" +
      " SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize " +
      "FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY " +
      "Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, " +
      "Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.AMSize ASC, " +
      "Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId, SUM(Carbon_automation_test5" +
          ".deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, gamePointId," +
          "ActiveCountry,deviceInformationId, Activecity FROM (select * from " +
          "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 84293.0, 4401364),
        Row("0RAM size", "Chinese", "guangzhou", 869.0, 1100110),
        Row("0RAM size", "Chinese", "shenzhen", 31339.0, 1100308),
        Row("0RAM size", "Chinese", "wuhan", 66902.0, 4401639),
        Row("0RAM size", "Chinese", "zhuzhou", 14751.0, 1100022),
        Row("1RAM size", "Chinese", "guangzhou", 11997.0, 900270),
        Row("1RAM size", "Chinese", "shenzhen", 2304.0, 900180),
        Row("1RAM size", "Chinese", "xiangtan", 67590.0, 2790900),
        Row("1RAM size", "Chinese", "yichang", 35478.0, 1800954),
        Row("1RAM size", "Chinese", "zhuzhou", 24705.0, 900378),
        Row("2RAM size", "Chinese", "changsha", 3946.0, 200142),
        Row("2RAM size", "Chinese", "xiangtan", 2700.0, 20014),
        Row("3RAM size", "Chinese", "changsha", 40082.0, 1400210),
        Row("3RAM size", "Chinese", "guangzhou", 27986.0, 1400308),
        Row("3RAM size", "Chinese", "shenzhen", 92960.0, 5603668),
        Row("3RAM size", "Chinese", "wuhan", 36890.0, 1400812),
        Row("3RAM size", "Chinese", "xiangtan", 53536.0, 4201974),
        Row("3RAM size", "Chinese", "yichang", 20874.0, 1400966),
        Row("3RAM size", "Chinese", "zhuzhou", 79786.0, 2941190),
        Row("4RAM size", "Chinese", "changsha", 200860.0, 11225038),
        Row("4RAM size", "Chinese", "guangzhou", 38016.0, 2201210),
        Row("4RAM size", "Chinese", "shenzhen", 49610.0, 2421408),
        Row("4RAM size", "Chinese", "wuhan", 117581.96999999999, 4402222),
        Row("4RAM size", "Chinese", "xiangtan", 254320.0, 33004774),
        Row("4RAM size", "Chinese", "yichang", 29436.0, 8803168),
        Row("5RAM size", "Chinese", "changsha", 13845.0, 505385),
        Row("5RAM size", "Chinese", "guangzhou", 23560.0, 1000460),
        Row("5RAM size", "Chinese", "wuhan", 12390.0, 50030),
        Row("6RAM size", "Chinese", "changsha", 23697.0, 1800909),
        Row("6RAM size", "Chinese", "guangzhou", 15912.0, 900234),
        Row("6RAM size", "Chinese", "shenzhen", 19278.0, 900315),
        Row("6RAM size", "Chinese", "wuhan", 29313.0, 1801125),
        Row("6RAM size", "Chinese", "xiangtan", 7794.0, 990117),
        Row("6RAM size", "Chinese", "zhuzhou", 26568.0, 900558),
        Row("7RAM size", "Chinese", "changsha", 1057.0, 700098),
        Row("7RAM size", "Chinese", "wuhan", 27853.0, 2100455),
        Row("7RAM size", "Chinese", "yichang", 14217.0, 700931),
        Row("7RAM size", "Chinese", "zhuzhou", 15673.0, 700021),
        Row("8RAM size", "Chinese", "guangzhou", 27385.619999999995, 10),
        Row("8RAM size", "Chinese", "shenzhen", 3550.0, 1000130),
        Row("8RAM size", "Chinese", "wuhan", 29700.0, 1000040),
        Row("8RAM size", "Chinese", "xiangtan", 31020.0, 2000540),
        Row("8RAM size", "Chinese", "yichang", 51660.0, 1100250),
        Row("8RAM size", "Chinese", "zhuzhou", 26840.0, 3001960),
        Row("9RAM size", "Chinese", "changsha", 32390.0, 2000730),
        Row("9RAM size", "Chinese", "shenzhen", 30650.0, 2000980),
        Row("9RAM size", "Chinese", "wuhan", 15670.0, 1000700),
        Row("9RAM size", "Chinese", "xiangtan", 58210.0, 3102370),
        Row("9RAM size", "Chinese", "yichang", 5710.0, 1000430)
      )
    )
  }
  )

  //TC_579
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId, SUM(Carbon_automation_test5" +
      ".deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize,deviceInformationId," +
      " gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test5) " +
      "SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize " +
      "FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY " +
      "Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, " +
      "Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.AMSize ASC, " +
      "Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId, SUM(Carbon_automation_test5" +
          ".deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize," +
          "deviceInformationId, gamePointId, ActiveCountry, Activecity FROM (select * from " +
          "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 84293.0, 4401364),
        Row("0RAM size", "Chinese", "guangzhou", 869.0, 1100110),
        Row("0RAM size", "Chinese", "shenzhen", 31339.0, 1100308),
        Row("0RAM size", "Chinese", "wuhan", 66902.0, 4401639),
        Row("0RAM size", "Chinese", "zhuzhou", 14751.0, 1100022),
        Row("1RAM size", "Chinese", "guangzhou", 11997.0, 900270),
        Row("1RAM size", "Chinese", "shenzhen", 2304.0, 900180),
        Row("1RAM size", "Chinese", "xiangtan", 67590.0, 2790900),
        Row("1RAM size", "Chinese", "yichang", 35478.0, 1800954),
        Row("1RAM size", "Chinese", "zhuzhou", 24705.0, 900378),
        Row("2RAM size", "Chinese", "changsha", 3946.0, 200142),
        Row("2RAM size", "Chinese", "xiangtan", 2700.0, 20014),
        Row("3RAM size", "Chinese", "changsha", 40082.0, 1400210),
        Row("3RAM size", "Chinese", "guangzhou", 27986.0, 1400308),
        Row("3RAM size", "Chinese", "shenzhen", 92960.0, 5603668),
        Row("3RAM size", "Chinese", "wuhan", 36890.0, 1400812),
        Row("3RAM size", "Chinese", "xiangtan", 53536.0, 4201974),
        Row("3RAM size", "Chinese", "yichang", 20874.0, 1400966),
        Row("3RAM size", "Chinese", "zhuzhou", 79786.0, 2941190),
        Row("4RAM size", "Chinese", "changsha", 200860.0, 11225038),
        Row("4RAM size", "Chinese", "guangzhou", 38016.0, 2201210),
        Row("4RAM size", "Chinese", "shenzhen", 49610.0, 2421408),
        Row("4RAM size", "Chinese", "wuhan", 117581.96999999999, 4402222),
        Row("4RAM size", "Chinese", "xiangtan", 254320.0, 33004774),
        Row("4RAM size", "Chinese", "yichang", 29436.0, 8803168),
        Row("5RAM size", "Chinese", "changsha", 13845.0, 505385),
        Row("5RAM size", "Chinese", "guangzhou", 23560.0, 1000460),
        Row("5RAM size", "Chinese", "wuhan", 12390.0, 50030),
        Row("6RAM size", "Chinese", "changsha", 23697.0, 1800909),
        Row("6RAM size", "Chinese", "guangzhou", 15912.0, 900234),
        Row("6RAM size", "Chinese", "shenzhen", 19278.0, 900315),
        Row("6RAM size", "Chinese", "wuhan", 29313.0, 1801125),
        Row("6RAM size", "Chinese", "xiangtan", 7794.0, 990117),
        Row("6RAM size", "Chinese", "zhuzhou", 26568.0, 900558),
        Row("7RAM size", "Chinese", "changsha", 1057.0, 700098),
        Row("7RAM size", "Chinese", "wuhan", 27853.0, 2100455),
        Row("7RAM size", "Chinese", "yichang", 14217.0, 700931),
        Row("7RAM size", "Chinese", "zhuzhou", 15673.0, 700021),
        Row("8RAM size", "Chinese", "guangzhou", 27385.619999999995, 10),
        Row("8RAM size", "Chinese", "shenzhen", 3550.0, 1000130),
        Row("8RAM size", "Chinese", "wuhan", 29700.0, 1000040),
        Row("8RAM size", "Chinese", "xiangtan", 31020.0, 2000540),
        Row("8RAM size", "Chinese", "yichang", 51660.0, 1100250),
        Row("8RAM size", "Chinese", "zhuzhou", 26840.0, 3001960),
        Row("9RAM size", "Chinese", "changsha", 32390.0, 2000730),
        Row("9RAM size", "Chinese", "shenzhen", 30650.0, 2000980),
        Row("9RAM size", "Chinese", "wuhan", 15670.0, 1000700),
        Row("9RAM size", "Chinese", "xiangtan", 58210.0, 3102370),
        Row("9RAM size", "Chinese", "yichang", 5710.0, 1000430)
      )
    )
  }
  )

  //TC_580
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId, COUNT(Carbon_automation_test5" +
      ".deviceInformationId) AS Count_deviceInformationId FROM ( SELECT AMSize, ActiveCountry," +
      "gamePointId, Activecity,deviceInformationId FROM (select * from Carbon_automation_test5) " +
      "SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize " +
      "FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY " +
      "Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, " +
      "Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.AMSize ASC, " +
      "Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId, COUNT" +
          "(Carbon_automation_test5.deviceInformationId) AS Count_deviceInformationId FROM ( " +
          "SELECT AMSize, ActiveCountry,gamePointId, Activecity,deviceInformationId FROM (select " +
          "* from Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 84293.0, 44),
        Row("0RAM size", "Chinese", "guangzhou", 869.0, 11),
        Row("0RAM size", "Chinese", "shenzhen", 31339.0, 11),
        Row("0RAM size", "Chinese", "wuhan", 66902.0, 44),
        Row("0RAM size", "Chinese", "zhuzhou", 14751.0, 11),
        Row("1RAM size", "Chinese", "guangzhou", 11997.0, 9),
        Row("1RAM size", "Chinese", "shenzhen", 2304.0, 9),
        Row("1RAM size", "Chinese", "xiangtan", 67590.0, 36),
        Row("1RAM size", "Chinese", "yichang", 35478.0, 18),
        Row("1RAM size", "Chinese", "zhuzhou", 24705.0, 9),
        Row("2RAM size", "Chinese", "changsha", 3946.0, 2),
        Row("2RAM size", "Chinese", "xiangtan", 2700.0, 2),
        Row("3RAM size", "Chinese", "changsha", 40082.0, 14),
        Row("3RAM size", "Chinese", "guangzhou", 27986.0, 14),
        Row("3RAM size", "Chinese", "shenzhen", 92960.0, 56),
        Row("3RAM size", "Chinese", "wuhan", 36890.0, 14),
        Row("3RAM size", "Chinese", "xiangtan", 53536.0, 42),
        Row("3RAM size", "Chinese", "yichang", 20874.0, 14),
        Row("3RAM size", "Chinese", "zhuzhou", 79786.0, 42),
        Row("4RAM size", "Chinese", "changsha", 200860.0, 132),
        Row("4RAM size", "Chinese", "guangzhou", 38016.0, 22),
        Row("4RAM size", "Chinese", "shenzhen", 49610.0, 44),
        Row("4RAM size", "Chinese", "wuhan", 117581.96999999999, 66),
        Row("4RAM size", "Chinese", "xiangtan", 254320.0, 132),
        Row("4RAM size", "Chinese", "yichang", 29436.0, 88),
        Row("5RAM size", "Chinese", "changsha", 13845.0, 10),
        Row("5RAM size", "Chinese", "guangzhou", 23560.0, 10),
        Row("5RAM size", "Chinese", "wuhan", 12390.0, 5),
        Row("6RAM size", "Chinese", "changsha", 23697.0, 18),
        Row("6RAM size", "Chinese", "guangzhou", 15912.0, 9),
        Row("6RAM size", "Chinese", "shenzhen", 19278.0, 9),
        Row("6RAM size", "Chinese", "wuhan", 29313.0, 18),
        Row("6RAM size", "Chinese", "xiangtan", 7794.0, 18),
        Row("6RAM size", "Chinese", "zhuzhou", 26568.0, 9),
        Row("7RAM size", "Chinese", "changsha", 1057.0, 7),
        Row("7RAM size", "Chinese", "wuhan", 27853.0, 21),
        Row("7RAM size", "Chinese", "yichang", 14217.0, 14),
        Row("7RAM size", "Chinese", "zhuzhou", 15673.0, 7),
        Row("8RAM size", "Chinese", "guangzhou", 27385.619999999995, 10),
        Row("8RAM size", "Chinese", "shenzhen", 3550.0, 10),
        Row("8RAM size", "Chinese", "wuhan", 29700.0, 10),
        Row("8RAM size", "Chinese", "xiangtan", 31020.0, 20),
        Row("8RAM size", "Chinese", "yichang", 51660.0, 20),
        Row("8RAM size", "Chinese", "zhuzhou", 26840.0, 30),
        Row("9RAM size", "Chinese", "changsha", 32390.0, 20),
        Row("9RAM size", "Chinese", "shenzhen", 30650.0, 20),
        Row("9RAM size", "Chinese", "wuhan", 15670.0, 10),
        Row("9RAM size", "Chinese", "xiangtan", 58210.0, 40),
        Row("9RAM size", "Chinese", "yichang", 5710.0, 10)
      )
    )
  }
  )

  //TC_581
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId, COUNT(DISTINCT " +
      "Carbon_automation_test5.deviceInformationId) AS LONG_COL_0 FROM ( SELECT AMSize, " +
      "ActiveCountry,gamePointId,deviceInformationId, Activecity FROM (select * from " +
      "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT " +
      "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
      "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
      "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
      "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId, COUNT(DISTINCT " +
          "Carbon_automation_test5.deviceInformationId) AS LONG_COL_0 FROM ( SELECT AMSize, " +
          "ActiveCountry,gamePointId,deviceInformationId, Activecity FROM (select * from " +
          "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 84293.0, 4),
        Row("0RAM size", "Chinese", "guangzhou", 869.0, 1),
        Row("0RAM size", "Chinese", "shenzhen", 31339.0, 1),
        Row("0RAM size", "Chinese", "wuhan", 66902.0, 4),
        Row("0RAM size", "Chinese", "zhuzhou", 14751.0, 1),
        Row("1RAM size", "Chinese", "guangzhou", 11997.0, 1),
        Row("1RAM size", "Chinese", "shenzhen", 2304.0, 1),
        Row("1RAM size", "Chinese", "xiangtan", 67590.0, 4),
        Row("1RAM size", "Chinese", "yichang", 35478.0, 2),
        Row("1RAM size", "Chinese", "zhuzhou", 24705.0, 1),
        Row("2RAM size", "Chinese", "changsha", 3946.0, 1),
        Row("2RAM size", "Chinese", "xiangtan", 2700.0, 1),
        Row("3RAM size", "Chinese", "changsha", 40082.0, 1),
        Row("3RAM size", "Chinese", "guangzhou", 27986.0, 1),
        Row("3RAM size", "Chinese", "shenzhen", 92960.0, 4),
        Row("3RAM size", "Chinese", "wuhan", 36890.0, 1),
        Row("3RAM size", "Chinese", "xiangtan", 53536.0, 3),
        Row("3RAM size", "Chinese", "yichang", 20874.0, 1),
        Row("3RAM size", "Chinese", "zhuzhou", 79786.0, 3),
        Row("4RAM size", "Chinese", "changsha", 200860.0, 6),
        Row("4RAM size", "Chinese", "guangzhou", 38016.0, 1),
        Row("4RAM size", "Chinese", "shenzhen", 49610.0, 2),
        Row("4RAM size", "Chinese", "wuhan", 117581.96999999999, 3),
        Row("4RAM size", "Chinese", "xiangtan", 254320.0, 6),
        Row("4RAM size", "Chinese", "yichang", 29436.0, 4),
        Row("5RAM size", "Chinese", "changsha", 13845.0, 2),
        Row("5RAM size", "Chinese", "guangzhou", 23560.0, 2),
        Row("5RAM size", "Chinese", "wuhan", 12390.0, 1),
        Row("6RAM size", "Chinese", "changsha", 23697.0, 2),
        Row("6RAM size", "Chinese", "guangzhou", 15912.0, 1),
        Row("6RAM size", "Chinese", "shenzhen", 19278.0, 1),
        Row("6RAM size", "Chinese", "wuhan", 29313.0, 2),
        Row("6RAM size", "Chinese", "xiangtan", 7794.0, 2),
        Row("6RAM size", "Chinese", "zhuzhou", 26568.0, 1),
        Row("7RAM size", "Chinese", "changsha", 1057.0, 1),
        Row("7RAM size", "Chinese", "wuhan", 27853.0, 3),
        Row("7RAM size", "Chinese", "yichang", 14217.0, 2),
        Row("7RAM size", "Chinese", "zhuzhou", 15673.0, 1),
        Row("8RAM size", "Chinese", "guangzhou", 27385.619999999995, 1),
        Row("8RAM size", "Chinese", "shenzhen", 3550.0, 1),
        Row("8RAM size", "Chinese", "wuhan", 29700.0, 1),
        Row("8RAM size", "Chinese", "xiangtan", 31020.0, 2),
        Row("8RAM size", "Chinese", "yichang", 51660.0, 2),
        Row("8RAM size", "Chinese", "zhuzhou", 26840.0, 3),
        Row("9RAM size", "Chinese", "changsha", 32390.0, 2),
        Row("9RAM size", "Chinese", "shenzhen", 30650.0, 2),
        Row("9RAM size", "Chinese", "wuhan", 15670.0, 1),
        Row("9RAM size", "Chinese", "xiangtan", 58210.0, 4),
        Row("9RAM size", "Chinese", "yichang", 5710.0, 1)
      )
    )
  }
  )

  //TC_583
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId, MAX(Carbon_automation_test5" +
      ".deviceInformationId) AS Max_deviceInformationId FROM ( SELECT AMSize,gamePointId," +
      "deviceInformationId, ActiveCountry, Activecity FROM (select * from " +
      "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT " +
      "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
      "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
      "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
      "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId, MAX(Carbon_automation_test5" +
          ".deviceInformationId) AS Max_deviceInformationId FROM ( SELECT AMSize,gamePointId," +
          "deviceInformationId, ActiveCountry, Activecity FROM (select * from " +
          "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 84293.0, 100079),
        Row("0RAM size", "Chinese", "guangzhou", 869.0, 100010),
        Row("0RAM size", "Chinese", "shenzhen", 31339.0, 100028),
        Row("0RAM size", "Chinese", "wuhan", 66902.0, 100061),
        Row("0RAM size", "Chinese", "zhuzhou", 14751.0, 100002),
        Row("1RAM size", "Chinese", "guangzhou", 11997.0, 100030),
        Row("1RAM size", "Chinese", "shenzhen", 2304.0, 100020),
        Row("1RAM size", "Chinese", "xiangtan", 67590.0, 100048),
        Row("1RAM size", "Chinese", "yichang", 35478.0, 100066),
        Row("1RAM size", "Chinese", "zhuzhou", 24705.0, 100042),
        Row("2RAM size", "Chinese", "changsha", 3946.0, 100071),
        Row("2RAM size", "Chinese", "xiangtan", 2700.0, 10007),
        Row("3RAM size", "Chinese", "changsha", 40082.0, 100015),
        Row("3RAM size", "Chinese", "guangzhou", 27986.0, 100022),
        Row("3RAM size", "Chinese", "shenzhen", 92960.0, 100083),
        Row("3RAM size", "Chinese", "wuhan", 36890.0, 100058),
        Row("3RAM size", "Chinese", "xiangtan", 53536.0, 100059),
        Row("3RAM size", "Chinese", "yichang", 20874.0, 100069),
        Row("3RAM size", "Chinese", "zhuzhou", 79786.0, 100053),
        Row("4RAM size", "Chinese", "changsha", 200860.0, 100065),
        Row("4RAM size", "Chinese", "guangzhou", 38016.0, 100055),
        Row("4RAM size", "Chinese", "shenzhen", 49610.0, 100060),
        Row("4RAM size", "Chinese", "wuhan", 117581.96999999999, 100046),
        Row("4RAM size", "Chinese", "xiangtan", 254320.0, 1000000),
        Row("4RAM size", "Chinese", "yichang", 29436.0, 100076),
        Row("5RAM size", "Chinese", "changsha", 13845.0, 100077),
        Row("5RAM size", "Chinese", "guangzhou", 23560.0, 100075),
        Row("5RAM size", "Chinese", "wuhan", 12390.0, 10006),
        Row("6RAM size", "Chinese", "changsha", 23697.0, 100067),
        Row("6RAM size", "Chinese", "guangzhou", 15912.0, 100026),
        Row("6RAM size", "Chinese", "shenzhen", 19278.0, 100035),
        Row("6RAM size", "Chinese", "wuhan", 29313.0, 100078),
        Row("6RAM size", "Chinese", "xiangtan", 7794.0, 100012),
        Row("6RAM size", "Chinese", "zhuzhou", 26568.0, 100062),
        Row("7RAM size", "Chinese", "changsha", 1057.0, 100014),
        Row("7RAM size", "Chinese", "wuhan", 27853.0, 100039),
        Row("7RAM size", "Chinese", "yichang", 14217.0, 100033),
        Row("7RAM size", "Chinese", "zhuzhou", 15673.0, 100003),
        Row("8RAM size", "Chinese", "guangzhou", 27385.619999999995, 1),
        Row("8RAM size", "Chinese", "shenzhen", 3550.0, 100013),
        Row("8RAM size", "Chinese", "wuhan", 29700.0, 100004),
        Row("8RAM size", "Chinese", "xiangtan", 31020.0, 100038),
        Row("8RAM size", "Chinese", "yichang", 51660.0, 100023),
        Row("8RAM size", "Chinese", "zhuzhou", 26840.0, 100081),
        Row("9RAM size", "Chinese", "changsha", 32390.0, 100037),
        Row("9RAM size", "Chinese", "shenzhen", 30650.0, 100054),
        Row("9RAM size", "Chinese", "wuhan", 15670.0, 100070),
        Row("9RAM size", "Chinese", "xiangtan", 58210.0, 100082),
        Row("9RAM size", "Chinese", "yichang", 5710.0, 100043)
      )
    )
  }
  )

  //TC_584
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, MIN" +
      "(Carbon_automation_test5.deviceInformationId) AS Min_deviceInformationId, SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize," +
      "deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from " +
      "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT " +
      "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
      "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
      "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
      "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, MIN" +
          "(Carbon_automation_test5.deviceInformationId) AS Min_deviceInformationId, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize," +
          "deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from " +
          "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 100005, 84293.0),
        Row("0RAM size", "Chinese", "guangzhou", 100010, 869.0),
        Row("0RAM size", "Chinese", "shenzhen", 100028, 31339.0),
        Row("0RAM size", "Chinese", "wuhan", 100008, 66902.0),
        Row("0RAM size", "Chinese", "zhuzhou", 100002, 14751.0),
        Row("1RAM size", "Chinese", "guangzhou", 100030, 11997.0),
        Row("1RAM size", "Chinese", "shenzhen", 100020, 2304.0),
        Row("1RAM size", "Chinese", "xiangtan", 10000, 67590.0),
        Row("1RAM size", "Chinese", "yichang", 100040, 35478.0),
        Row("1RAM size", "Chinese", "zhuzhou", 100042, 24705.0),
        Row("2RAM size", "Chinese", "changsha", 100071, 3946.0),
        Row("2RAM size", "Chinese", "xiangtan", 10007, 2700.0),
        Row("3RAM size", "Chinese", "changsha", 100015, 40082.0),
        Row("3RAM size", "Chinese", "guangzhou", 100022, 27986.0),
        Row("3RAM size", "Chinese", "shenzhen", 100032, 92960.0),
        Row("3RAM size", "Chinese", "wuhan", 100058, 36890.0),
        Row("3RAM size", "Chinese", "xiangtan", 100031, 53536.0),
        Row("3RAM size", "Chinese", "yichang", 100069, 20874.0),
        Row("3RAM size", "Chinese", "zhuzhou", 10005, 79786.0),
        Row("4RAM size", "Chinese", "changsha", 10008, 200860.0),
        Row("4RAM size", "Chinese", "guangzhou", 100055, 38016.0),
        Row("4RAM size", "Chinese", "shenzhen", 10004, 49610.0),
        Row("4RAM size", "Chinese", "wuhan", 10, 117581.96999999999),
        Row("4RAM size", "Chinese", "xiangtan", 100007, 254320.0),
        Row("4RAM size", "Chinese", "yichang", 100000, 29436.0),
        Row("5RAM size", "Chinese", "changsha", 1000, 13845.0),
        Row("5RAM size", "Chinese", "guangzhou", 100017, 23560.0),
        Row("5RAM size", "Chinese", "wuhan", 10006, 12390.0),
        Row("6RAM size", "Chinese", "changsha", 100034, 23697.0),
        Row("6RAM size", "Chinese", "guangzhou", 100026, 15912.0),
        Row("6RAM size", "Chinese", "shenzhen", 100035, 19278.0),
        Row("6RAM size", "Chinese", "wuhan", 100047, 29313.0),
        Row("6RAM size", "Chinese", "xiangtan", 10001, 7794.0),
        Row("6RAM size", "Chinese", "zhuzhou", 100062, 26568.0),
        Row("7RAM size", "Chinese", "changsha", 100014, 1057.0),
        Row("7RAM size", "Chinese", "wuhan", 100001, 27853.0),
        Row("7RAM size", "Chinese", "yichang", 100, 14217.0),
        Row("7RAM size", "Chinese", "zhuzhou", 100003, 15673.0),
        Row("8RAM size", "Chinese", "guangzhou", 1, 27385.619999999995),
        Row("8RAM size", "Chinese", "shenzhen", 100013, 3550.0),
        Row("8RAM size", "Chinese", "wuhan", 100004, 29700.0),
        Row("8RAM size", "Chinese", "xiangtan", 100016, 31020.0),
        Row("8RAM size", "Chinese", "yichang", 10002, 51660.0),
        Row("8RAM size", "Chinese", "zhuzhou", 100052, 26840.0),
        Row("9RAM size", "Chinese", "changsha", 100036, 32390.0),
        Row("9RAM size", "Chinese", "shenzhen", 100044, 30650.0),
        Row("9RAM size", "Chinese", "wuhan", 100070, 15670.0),
        Row("9RAM size", "Chinese", "xiangtan", 10003, 58210.0),
        Row("9RAM size", "Chinese", "yichang", 100043, 5710.0)
      )
    )
  }
  )

  //TC_587
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, SUM" +
      "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId," +
      " deviceInformationId,ActiveCountry, Activecity FROM (select * from " +
      "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT " +
      "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
      "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
      "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
      "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, SUM" +
          "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize," +
          "gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from " +
          "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 4401364, 84293.0),
        Row("0RAM size", "Chinese", "guangzhou", 1100110, 869.0),
        Row("0RAM size", "Chinese", "shenzhen", 1100308, 31339.0),
        Row("0RAM size", "Chinese", "wuhan", 4401639, 66902.0),
        Row("0RAM size", "Chinese", "zhuzhou", 1100022, 14751.0),
        Row("1RAM size", "Chinese", "guangzhou", 900270, 11997.0),
        Row("1RAM size", "Chinese", "shenzhen", 900180, 2304.0),
        Row("1RAM size", "Chinese", "xiangtan", 2790900, 67590.0),
        Row("1RAM size", "Chinese", "yichang", 1800954, 35478.0),
        Row("1RAM size", "Chinese", "zhuzhou", 900378, 24705.0),
        Row("2RAM size", "Chinese", "changsha", 200142, 3946.0),
        Row("2RAM size", "Chinese", "xiangtan", 20014, 2700.0),
        Row("3RAM size", "Chinese", "changsha", 1400210, 40082.0),
        Row("3RAM size", "Chinese", "guangzhou", 1400308, 27986.0),
        Row("3RAM size", "Chinese", "shenzhen", 5603668, 92960.0),
        Row("3RAM size", "Chinese", "wuhan", 1400812, 36890.0),
        Row("3RAM size", "Chinese", "xiangtan", 4201974, 53536.0),
        Row("3RAM size", "Chinese", "yichang", 1400966, 20874.0),
        Row("3RAM size", "Chinese", "zhuzhou", 2941190, 79786.0),
        Row("4RAM size", "Chinese", "changsha", 11225038, 200860.0),
        Row("4RAM size", "Chinese", "guangzhou", 2201210, 38016.0),
        Row("4RAM size", "Chinese", "shenzhen", 2421408, 49610.0),
        Row("4RAM size", "Chinese", "wuhan", 4402222, 117581.96999999999),
        Row("4RAM size", "Chinese", "xiangtan", 33004774, 254320.0),
        Row("4RAM size", "Chinese", "yichang", 8803168, 29436.0),
        Row("5RAM size", "Chinese", "changsha", 505385, 13845.0),
        Row("5RAM size", "Chinese", "guangzhou", 1000460, 23560.0),
        Row("5RAM size", "Chinese", "wuhan", 50030, 12390.0),
        Row("6RAM size", "Chinese", "changsha", 1800909, 23697.0),
        Row("6RAM size", "Chinese", "guangzhou", 900234, 15912.0),
        Row("6RAM size", "Chinese", "shenzhen", 900315, 19278.0),
        Row("6RAM size", "Chinese", "wuhan", 1801125, 29313.0),
        Row("6RAM size", "Chinese", "xiangtan", 990117, 7794.0),
        Row("6RAM size", "Chinese", "zhuzhou", 900558, 26568.0),
        Row("7RAM size", "Chinese", "changsha", 700098, 1057.0),
        Row("7RAM size", "Chinese", "wuhan", 2100455, 27853.0),
        Row("7RAM size", "Chinese", "yichang", 700931, 14217.0),
        Row("7RAM size", "Chinese", "zhuzhou", 700021, 15673.0),
        Row("8RAM size", "Chinese", "guangzhou", 10, 27385.619999999995),
        Row("8RAM size", "Chinese", "shenzhen", 1000130, 3550.0),
        Row("8RAM size", "Chinese", "wuhan", 1000040, 29700.0),
        Row("8RAM size", "Chinese", "xiangtan", 2000540, 31020.0),
        Row("8RAM size", "Chinese", "yichang", 1100250, 51660.0),
        Row("8RAM size", "Chinese", "zhuzhou", 3001960, 26840.0),
        Row("9RAM size", "Chinese", "changsha", 2000730, 32390.0),
        Row("9RAM size", "Chinese", "shenzhen", 2000980, 30650.0),
        Row("9RAM size", "Chinese", "wuhan", 1000700, 15670.0),
        Row("9RAM size", "Chinese", "xiangtan", 3102370, 58210.0),
        Row("9RAM size", "Chinese", "yichang", 1000430, 5710.0)
      )
    )
  }
  )

  //TC_588
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, AVG" +
      "(Carbon_automation_test5.gamePointId) AS Avg_gamePointId FROM ( SELECT AMSize,gamePointId " +
      ",deviceInformationId, ActiveCountry, Activecity FROM (select * from " +
      "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT " +
      "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
      "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
      "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
      "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, AVG" +
          "(Carbon_automation_test5.gamePointId) AS Avg_gamePointId FROM ( SELECT AMSize," +
          "gamePointId ,deviceInformationId, ActiveCountry, Activecity FROM (select * from " +
          "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 4401364, 1915.75),
        Row("0RAM size", "Chinese", "guangzhou", 1100110, 79.0),
        Row("0RAM size", "Chinese", "shenzhen", 1100308, 2849.0),
        Row("0RAM size", "Chinese", "wuhan", 4401639, 1520.5),
        Row("0RAM size", "Chinese", "zhuzhou", 1100022, 1341.0),
        Row("1RAM size", "Chinese", "guangzhou", 900270, 1333.0),
        Row("1RAM size", "Chinese", "shenzhen", 900180, 256.0),
        Row("1RAM size", "Chinese", "xiangtan", 2790900, 1877.5),
        Row("1RAM size", "Chinese", "yichang", 1800954, 1971.0),
        Row("1RAM size", "Chinese", "zhuzhou", 900378, 2745.0),
        Row("2RAM size", "Chinese", "changsha", 200142, 1973.0),
        Row("2RAM size", "Chinese", "xiangtan", 20014, 1350.0),
        Row("3RAM size", "Chinese", "changsha", 1400210, 2863.0),
        Row("3RAM size", "Chinese", "guangzhou", 1400308, 1999.0),
        Row("3RAM size", "Chinese", "shenzhen", 5603668, 1660.0),
        Row("3RAM size", "Chinese", "wuhan", 1400812, 2635.0),
        Row("3RAM size", "Chinese", "xiangtan", 4201974, 1274.6666666666667),
        Row("3RAM size", "Chinese", "yichang", 1400966, 1491.0),
        Row("3RAM size", "Chinese", "zhuzhou", 2941190, 1899.6666666666667),
        Row("4RAM size", "Chinese", "changsha", 11225038, 1521.6666666666667),
        Row("4RAM size", "Chinese", "guangzhou", 2201210, 1728.0),
        Row("4RAM size", "Chinese", "shenzhen", 2421408, 1127.5),
        Row("4RAM size", "Chinese", "wuhan", 4402222, 1781.5449999999998),
        Row("4RAM size", "Chinese", "xiangtan", 33004774, 1926.6666666666667),
        Row("4RAM size", "Chinese", "yichang", 8803168, 334.5),
        Row("5RAM size", "Chinese", "changsha", 505385, 1384.5),
        Row("5RAM size", "Chinese", "guangzhou", 1000460, 2356.0),
        Row("5RAM size", "Chinese", "wuhan", 50030, 2478.0),
        Row("6RAM size", "Chinese", "changsha", 1800909, 1316.5),
        Row("6RAM size", "Chinese", "guangzhou", 900234, 1768.0),
        Row("6RAM size", "Chinese", "shenzhen", 900315, 2142.0),
        Row("6RAM size", "Chinese", "wuhan", 1801125, 1628.5),
        Row("6RAM size", "Chinese", "xiangtan", 990117, 433.0),
        Row("6RAM size", "Chinese", "zhuzhou", 900558, 2952.0),
        Row("7RAM size", "Chinese", "changsha", 700098, 151.0),
        Row("7RAM size", "Chinese", "wuhan", 2100455, 1326.3333333333333),
        Row("7RAM size", "Chinese", "yichang", 700931, 1015.5),
        Row("7RAM size", "Chinese", "zhuzhou", 700021, 2239.0),
        Row("8RAM size", "Chinese", "guangzhou", 10, 2738.5619999999994),
        Row("8RAM size", "Chinese", "shenzhen", 1000130, 355.0),
        Row("8RAM size", "Chinese", "wuhan", 1000040, 2970.0),
        Row("8RAM size", "Chinese", "xiangtan", 2000540, 1551.0),
        Row("8RAM size", "Chinese", "yichang", 1100250, 2583.0),
        Row("8RAM size", "Chinese", "zhuzhou", 3001960, 894.6666666666666),
        Row("9RAM size", "Chinese", "changsha", 2000730, 1619.5),
        Row("9RAM size", "Chinese", "shenzhen", 2000980, 1532.5),
        Row("9RAM size", "Chinese", "wuhan", 1000700, 1567.0),
        Row("9RAM size", "Chinese", "xiangtan", 3102370, 1455.25),
        Row("9RAM size", "Chinese", "yichang", 1000430, 571.0)
      )
    )
  }
  )

  //TC_589
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, COUNT" +
      "(Carbon_automation_test5.gamePointId) AS Count_gamePointId FROM ( SELECT AMSize, " +
      "gamePointId,ActiveCountry, deviceInformationId,Activecity FROM (select * from " +
      "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT " +
      "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
      "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
      "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
      "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, COUNT" +
          "(Carbon_automation_test5.gamePointId) AS Count_gamePointId FROM ( SELECT AMSize, " +
          "gamePointId,ActiveCountry, deviceInformationId,Activecity FROM (select * from " +
          "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 4401364, 44),
        Row("0RAM size", "Chinese", "guangzhou", 1100110, 11),
        Row("0RAM size", "Chinese", "shenzhen", 1100308, 11),
        Row("0RAM size", "Chinese", "wuhan", 4401639, 44),
        Row("0RAM size", "Chinese", "zhuzhou", 1100022, 11),
        Row("1RAM size", "Chinese", "guangzhou", 900270, 9),
        Row("1RAM size", "Chinese", "shenzhen", 900180, 9),
        Row("1RAM size", "Chinese", "xiangtan", 2790900, 36),
        Row("1RAM size", "Chinese", "yichang", 1800954, 18),
        Row("1RAM size", "Chinese", "zhuzhou", 900378, 9),
        Row("2RAM size", "Chinese", "changsha", 200142, 2),
        Row("2RAM size", "Chinese", "xiangtan", 20014, 2),
        Row("3RAM size", "Chinese", "changsha", 1400210, 14),
        Row("3RAM size", "Chinese", "guangzhou", 1400308, 14),
        Row("3RAM size", "Chinese", "shenzhen", 5603668, 56),
        Row("3RAM size", "Chinese", "wuhan", 1400812, 14),
        Row("3RAM size", "Chinese", "xiangtan", 4201974, 42),
        Row("3RAM size", "Chinese", "yichang", 1400966, 14),
        Row("3RAM size", "Chinese", "zhuzhou", 2941190, 42),
        Row("4RAM size", "Chinese", "changsha", 11225038, 132),
        Row("4RAM size", "Chinese", "guangzhou", 2201210, 22),
        Row("4RAM size", "Chinese", "shenzhen", 2421408, 44),
        Row("4RAM size", "Chinese", "wuhan", 4402222, 66),
        Row("4RAM size", "Chinese", "xiangtan", 33004774, 132),
        Row("4RAM size", "Chinese", "yichang", 8803168, 88),
        Row("5RAM size", "Chinese", "changsha", 505385, 10),
        Row("5RAM size", "Chinese", "guangzhou", 1000460, 10),
        Row("5RAM size", "Chinese", "wuhan", 50030, 5),
        Row("6RAM size", "Chinese", "changsha", 1800909, 18),
        Row("6RAM size", "Chinese", "guangzhou", 900234, 9),
        Row("6RAM size", "Chinese", "shenzhen", 900315, 9),
        Row("6RAM size", "Chinese", "wuhan", 1801125, 18),
        Row("6RAM size", "Chinese", "xiangtan", 990117, 18),
        Row("6RAM size", "Chinese", "zhuzhou", 900558, 9),
        Row("7RAM size", "Chinese", "changsha", 700098, 7),
        Row("7RAM size", "Chinese", "wuhan", 2100455, 21),
        Row("7RAM size", "Chinese", "yichang", 700931, 14),
        Row("7RAM size", "Chinese", "zhuzhou", 700021, 7),
        Row("8RAM size", "Chinese", "guangzhou", 10, 10),
        Row("8RAM size", "Chinese", "shenzhen", 1000130, 10),
        Row("8RAM size", "Chinese", "wuhan", 1000040, 10),
        Row("8RAM size", "Chinese", "xiangtan", 2000540, 20),
        Row("8RAM size", "Chinese", "yichang", 1100250, 20),
        Row("8RAM size", "Chinese", "zhuzhou", 3001960, 30),
        Row("9RAM size", "Chinese", "changsha", 2000730, 20),
        Row("9RAM size", "Chinese", "shenzhen", 2000980, 20),
        Row("9RAM size", "Chinese", "wuhan", 1000700, 10),
        Row("9RAM size", "Chinese", "xiangtan", 3102370, 40),
        Row("9RAM size", "Chinese", "yichang", 1000430, 10)
      )
    )
  }
  )

  //TC_590
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, COUNT(DISTINCT " +
      "Carbon_automation_test5.gamePointId) AS DistinctCount_gamePointId FROM ( SELECT AMSize," +
      "deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from " +
      "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT " +
      "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
      "Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
      "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
      "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
      "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
      "Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, COUNT" +
          "(DISTINCT Carbon_automation_test5.gamePointId) AS DistinctCount_gamePointId FROM ( " +
          "SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select " +
          "* from Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT " +
          "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY" +
          " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
          "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
          "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
          "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
          "Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 4401364, 4),
        Row("0RAM size", "Chinese", "guangzhou", 1100110, 1),
        Row("0RAM size", "Chinese", "shenzhen", 1100308, 1),
        Row("0RAM size", "Chinese", "wuhan", 4401639, 4),
        Row("0RAM size", "Chinese", "zhuzhou", 1100022, 1),
        Row("1RAM size", "Chinese", "guangzhou", 900270, 1),
        Row("1RAM size", "Chinese", "shenzhen", 900180, 1),
        Row("1RAM size", "Chinese", "xiangtan", 2790900, 4),
        Row("1RAM size", "Chinese", "yichang", 1800954, 2),
        Row("1RAM size", "Chinese", "zhuzhou", 900378, 1),
        Row("2RAM size", "Chinese", "changsha", 200142, 1),
        Row("2RAM size", "Chinese", "xiangtan", 20014, 1),
        Row("3RAM size", "Chinese", "changsha", 1400210, 1),
        Row("3RAM size", "Chinese", "guangzhou", 1400308, 1),
        Row("3RAM size", "Chinese", "shenzhen", 5603668, 4),
        Row("3RAM size", "Chinese", "wuhan", 1400812, 1),
        Row("3RAM size", "Chinese", "xiangtan", 4201974, 3),
        Row("3RAM size", "Chinese", "yichang", 1400966, 1),
        Row("3RAM size", "Chinese", "zhuzhou", 2941190, 3),
        Row("4RAM size", "Chinese", "changsha", 11225038, 6),
        Row("4RAM size", "Chinese", "guangzhou", 2201210, 1),
        Row("4RAM size", "Chinese", "shenzhen", 2421408, 2),
        Row("4RAM size", "Chinese", "wuhan", 4402222, 3),
        Row("4RAM size", "Chinese", "xiangtan", 33004774, 6),
        Row("4RAM size", "Chinese", "yichang", 8803168, 4),
        Row("5RAM size", "Chinese", "changsha", 505385, 2),
        Row("5RAM size", "Chinese", "guangzhou", 1000460, 2),
        Row("5RAM size", "Chinese", "wuhan", 50030, 1),
        Row("6RAM size", "Chinese", "changsha", 1800909, 2),
        Row("6RAM size", "Chinese", "guangzhou", 900234, 1),
        Row("6RAM size", "Chinese", "shenzhen", 900315, 1),
        Row("6RAM size", "Chinese", "wuhan", 1801125, 2),
        Row("6RAM size", "Chinese", "xiangtan", 990117, 2),
        Row("6RAM size", "Chinese", "zhuzhou", 900558, 1),
        Row("7RAM size", "Chinese", "changsha", 700098, 1),
        Row("7RAM size", "Chinese", "wuhan", 2100455, 3),
        Row("7RAM size", "Chinese", "yichang", 700931, 2),
        Row("7RAM size", "Chinese", "zhuzhou", 700021, 1),
        Row("8RAM size", "Chinese", "guangzhou", 10, 1),
        Row("8RAM size", "Chinese", "shenzhen", 1000130, 1),
        Row("8RAM size", "Chinese", "wuhan", 1000040, 1),
        Row("8RAM size", "Chinese", "xiangtan", 2000540, 2),
        Row("8RAM size", "Chinese", "yichang", 1100250, 2),
        Row("8RAM size", "Chinese", "zhuzhou", 3001960, 3),
        Row("9RAM size", "Chinese", "changsha", 2000730, 2),
        Row("9RAM size", "Chinese", "shenzhen", 2000980, 2),
        Row("9RAM size", "Chinese", "wuhan", 1000700, 1),
        Row("9RAM size", "Chinese", "xiangtan", 3102370, 4),
        Row("9RAM size", "Chinese", "yichang", 1000430, 1)
      )
    )
  }
  )

  //TC_592
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, MAX(Carbon_automation_test5.gamePointId) AS Max_gamePointId FROM ( SELECT AMSize, deviceInformationId,gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, MAX(Carbon_automation_test5.gamePointId) AS Max_gamePointId FROM ( SELECT AMSize, deviceInformationId,gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 4401364, 2593.0),
        Row("0RAM size", "Chinese", "guangzhou", 1100110, 79.0),
        Row("0RAM size", "Chinese", "shenzhen", 1100308, 2849.0),
        Row("0RAM size", "Chinese", "wuhan", 4401639, 2483.0),
        Row("0RAM size", "Chinese", "zhuzhou", 1100022, 1341.0),
        Row("1RAM size", "Chinese", "guangzhou", 900270, 1333.0),
        Row("1RAM size", "Chinese", "shenzhen", 900180, 256.0),
        Row("1RAM size", "Chinese", "xiangtan", 2790900, 2734.0),
        Row("1RAM size", "Chinese", "yichang", 1800954, 2078.0),
        Row("1RAM size", "Chinese", "zhuzhou", 900378, 2745.0),
        Row("2RAM size", "Chinese", "changsha", 200142, 1973.0),
        Row("2RAM size", "Chinese", "xiangtan", 20014, 1350.0),
        Row("3RAM size", "Chinese", "changsha", 1400210, 2863.0),
        Row("3RAM size", "Chinese", "guangzhou", 1400308, 1999.0),
        Row("3RAM size", "Chinese", "shenzhen", 5603668, 2488.0),
        Row("3RAM size", "Chinese", "wuhan", 1400812, 2635.0),
        Row("3RAM size", "Chinese", "xiangtan", 4201974, 1407.0),
        Row("3RAM size", "Chinese", "yichang", 1400966, 1491.0),
        Row("3RAM size", "Chinese", "zhuzhou", 2941190, 2436.0),
        Row("4RAM size", "Chinese", "changsha", 11225038, 2572.0),
        Row("4RAM size", "Chinese", "guangzhou", 2201210, 1728.0),
        Row("4RAM size", "Chinese", "shenzhen", 2421408, 1717.0),
        Row("4RAM size", "Chinese", "wuhan", 4402222, 2553.0),
        Row("4RAM size", "Chinese", "xiangtan", 33004774, 2890.0),
        Row("4RAM size", "Chinese", "yichang", 8803168, 732.0),
        Row("5RAM size", "Chinese", "changsha", 505385, 2077.0),
        Row("5RAM size", "Chinese", "guangzhou", 1000460, 2507.0),
        Row("5RAM size", "Chinese", "wuhan", 50030, 2478.0),
        Row("6RAM size", "Chinese", "changsha", 1800909, 2061.0),
        Row("6RAM size", "Chinese", "guangzhou", 900234, 1768.0),
        Row("6RAM size", "Chinese", "shenzhen", 900315, 2142.0),
        Row("6RAM size", "Chinese", "wuhan", 1801125, 1823.0),
        Row("6RAM size", "Chinese", "xiangtan", 990117, 568.0),
        Row("6RAM size", "Chinese", "zhuzhou", 900558, 2952.0),
        Row("7RAM size", "Chinese", "changsha", 700098, 151.0),
        Row("7RAM size", "Chinese", "wuhan", 2100455, 1750.0),
        Row("7RAM size", "Chinese", "yichang", 700931, 1271.0),
        Row("7RAM size", "Chinese", "zhuzhou", 700021, 2239.0),
        Row("8RAM size", "Chinese", "guangzhou", 10, 2738.562),
        Row("8RAM size", "Chinese", "shenzhen", 1000130, 355.0),
        Row("8RAM size", "Chinese", "wuhan", 1000040, 2970.0),
        Row("8RAM size", "Chinese", "xiangtan", 2000540, 1873.0),
        Row("8RAM size", "Chinese", "yichang", 1100250, 2972.0),
        Row("8RAM size", "Chinese", "zhuzhou", 3001960, 1226.0),
        Row("9RAM size", "Chinese", "changsha", 2000730, 2224.0),
        Row("9RAM size", "Chinese", "shenzhen", 2000980, 1697.0),
        Row("9RAM size", "Chinese", "wuhan", 1000700, 1567.0),
        Row("9RAM size", "Chinese", "xiangtan", 3102370, 2348.0),
        Row("9RAM size", "Chinese", "yichang", 1000430, 571.0)
      )
    )
  }
  )

  //TC_593
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, MIN(Carbon_automation_test5.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize,gamePointId,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, MIN(Carbon_automation_test5.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize,gamePointId,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 4401364, 1098.0),
        Row("0RAM size", "Chinese", "guangzhou", 1100110, 79.0),
        Row("0RAM size", "Chinese", "shenzhen", 1100308, 2849.0),
        Row("0RAM size", "Chinese", "wuhan", 4401639, 750.0),
        Row("0RAM size", "Chinese", "zhuzhou", 1100022, 1341.0),
        Row("1RAM size", "Chinese", "guangzhou", 900270, 1333.0),
        Row("1RAM size", "Chinese", "shenzhen", 900180, 256.0),
        Row("1RAM size", "Chinese", "xiangtan", 2790900, 202.0),
        Row("1RAM size", "Chinese", "yichang", 1800954, 1864.0),
        Row("1RAM size", "Chinese", "zhuzhou", 900378, 2745.0),
        Row("2RAM size", "Chinese", "changsha", 200142, 1973.0),
        Row("2RAM size", "Chinese", "xiangtan", 20014, 1350.0),
        Row("3RAM size", "Chinese", "changsha", 1400210, 2863.0),
        Row("3RAM size", "Chinese", "guangzhou", 1400308, 1999.0),
        Row("3RAM size", "Chinese", "shenzhen", 5603668, 907.0),
        Row("3RAM size", "Chinese", "wuhan", 1400812, 2635.0),
        Row("3RAM size", "Chinese", "xiangtan", 4201974, 1080.0),
        Row("3RAM size", "Chinese", "yichang", 1400966, 1491.0),
        Row("3RAM size", "Chinese", "zhuzhou", 2941190, 1608.0),
        Row("4RAM size", "Chinese", "changsha", 11225038, 813.0),
        Row("4RAM size", "Chinese", "guangzhou", 2201210, 1728.0),
        Row("4RAM size", "Chinese", "shenzhen", 2421408, 538.0),
        Row("4RAM size", "Chinese", "wuhan", 4402222, 1077.0),
        Row("4RAM size", "Chinese", "xiangtan", 33004774, 412.0),
        Row("4RAM size", "Chinese", "yichang", 8803168, 29.0),
        Row("5RAM size", "Chinese", "changsha", 505385, 692.0),
        Row("5RAM size", "Chinese", "guangzhou", 1000460, 2205.0),
        Row("5RAM size", "Chinese", "wuhan", 50030, 2478.0),
        Row("6RAM size", "Chinese", "changsha", 1800909, 572.0),
        Row("6RAM size", "Chinese", "guangzhou", 900234, 1768.0),
        Row("6RAM size", "Chinese", "shenzhen", 900315, 2142.0),
        Row("6RAM size", "Chinese", "wuhan", 1801125, 1434.0),
        Row("6RAM size", "Chinese", "xiangtan", 990117, 298.0),
        Row("6RAM size", "Chinese", "zhuzhou", 900558, 2952.0),
        Row("7RAM size", "Chinese", "changsha", 700098, 151.0),
        Row("7RAM size", "Chinese", "wuhan", 2100455, 505.0),
        Row("7RAM size", "Chinese", "yichang", 700931, 760.0),
        Row("7RAM size", "Chinese", "zhuzhou", 700021, 2239.0),
        Row("8RAM size", "Chinese", "guangzhou", 10, 2738.562),
        Row("8RAM size", "Chinese", "shenzhen", 1000130, 355.0),
        Row("8RAM size", "Chinese", "wuhan", 1000040, 2970.0),
        Row("8RAM size", "Chinese", "xiangtan", 2000540, 1229.0),
        Row("8RAM size", "Chinese", "yichang", 1100250, 2194.0),
        Row("8RAM size", "Chinese", "zhuzhou", 3001960, 613.0),
        Row("9RAM size", "Chinese", "changsha", 2000730, 1015.0),
        Row("9RAM size", "Chinese", "shenzhen", 2000980, 1368.0),
        Row("9RAM size", "Chinese", "wuhan", 1000700, 1567.0),
        Row("9RAM size", "Chinese", "xiangtan", 3102370, 448.0),
        Row("9RAM size", "Chinese", "yichang", 1000430, 571.0)
      )
    )
  }
  )

  //TC_597
  test(
    "SELECT Carbon_automation_test5.ActiveCountry AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation_test5.AMSize) AS DistinctCount_AMSize, SUM(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, SUM(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,deviceInformationId, gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.ActiveCountry AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation_test5.AMSize) AS DistinctCount_AMSize, SUM(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId, SUM(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,deviceInformationId, gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("Chinese", "changsha", 8, 22233876, 400170.0),
        Row("Chinese", "guangzhou", 7, 7502602, 145725.62),
        Row("Chinese", "shenzhen", 7, 13926989, 229691.0),
        Row("Chinese", "wuhan", 8, 16157023, 336299.97),
        Row("Chinese", "xiangtan", 7, 46110689, 475170.0),
        Row("Chinese", "yichang", 6, 14806699, 157375.0),
        Row("Chinese", "zhuzhou", 6, 9544129, 188323.0)
      )
    )
  }
  )

  //TC_599
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM(distinct Carbon_automation_test5.gamePointId) AS Sum_distinct_gamePointId, SUM(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 Left join ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM(distinct Carbon_automation_test5.gamePointId) AS Sum_distinct_gamePointId, SUM(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 Left join ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 7663.0, 4401364),
        Row("0RAM size", "Chinese", "guangzhou", 79.0, 1100110),
        Row("0RAM size", "Chinese", "shenzhen", 2849.0, 1100308),
        Row("0RAM size", "Chinese", "wuhan", 6082.0, 4401639),
        Row("0RAM size", "Chinese", "zhuzhou", 1341.0, 1100022),
        Row("1RAM size", "Chinese", "guangzhou", 1333.0, 900270),
        Row("1RAM size", "Chinese", "shenzhen", 256.0, 900180),
        Row("1RAM size", "Chinese", "xiangtan", 7510.0, 2790900),
        Row("1RAM size", "Chinese", "yichang", 3942.0, 1800954),
        Row("1RAM size", "Chinese", "zhuzhou", 2745.0, 900378),
        Row("2RAM size", "Chinese", "changsha", 1973.0, 200142),
        Row("2RAM size", "Chinese", "xiangtan", 1350.0, 20014),
        Row("3RAM size", "Chinese", "changsha", 2863.0, 1400210),
        Row("3RAM size", "Chinese", "guangzhou", 1999.0, 1400308),
        Row("3RAM size", "Chinese", "shenzhen", 6640.0, 5603668),
        Row("3RAM size", "Chinese", "wuhan", 2635.0, 1400812),
        Row("3RAM size", "Chinese", "xiangtan", 3824.0, 4201974),
        Row("3RAM size", "Chinese", "yichang", 1491.0, 1400966),
        Row("3RAM size", "Chinese", "zhuzhou", 5699.0, 2941190),
        Row("4RAM size", "Chinese", "changsha", 9130.0, 11225038),
        Row("4RAM size", "Chinese", "guangzhou", 1728.0, 2201210),
        Row("4RAM size", "Chinese", "shenzhen", 2255.0, 2421408),
        Row("4RAM size", "Chinese", "wuhan", 5344.635, 4402222),
        Row("4RAM size", "Chinese", "xiangtan", 11560.0, 33004774),
        Row("4RAM size", "Chinese", "yichang", 1338.0, 8803168),
        Row("5RAM size", "Chinese", "changsha", 2769.0, 505385),
        Row("5RAM size", "Chinese", "guangzhou", 4712.0, 1000460),
        Row("5RAM size", "Chinese", "wuhan", 2478.0, 50030),
        Row("6RAM size", "Chinese", "changsha", 2633.0, 1800909),
        Row("6RAM size", "Chinese", "guangzhou", 1768.0, 900234),
        Row("6RAM size", "Chinese", "shenzhen", 2142.0, 900315),
        Row("6RAM size", "Chinese", "wuhan", 3257.0, 1801125),
        Row("6RAM size", "Chinese", "xiangtan", 866.0, 990117),
        Row("6RAM size", "Chinese", "zhuzhou", 2952.0, 900558),
        Row("7RAM size", "Chinese", "changsha", 151.0, 700098),
        Row("7RAM size", "Chinese", "wuhan", 3979.0, 2100455),
        Row("7RAM size", "Chinese", "yichang", 2031.0, 700931),
        Row("7RAM size", "Chinese", "zhuzhou", 2239.0, 700021),
        Row("8RAM size", "Chinese", "guangzhou", 2738.562, 10),
        Row("8RAM size", "Chinese", "shenzhen", 355.0, 1000130),
        Row("8RAM size", "Chinese", "wuhan", 2970.0, 1000040),
        Row("8RAM size", "Chinese", "xiangtan", 3102.0, 2000540),
        Row("8RAM size", "Chinese", "yichang", 5166.0, 1100250),
        Row("8RAM size", "Chinese", "zhuzhou", 2684.0, 3001960),
        Row("9RAM size", "Chinese", "changsha", 3239.0, 2000730),
        Row("9RAM size", "Chinese", "shenzhen", 3065.0, 2000980),
        Row("9RAM size", "Chinese", "wuhan", 1567.0, 1000700),
        Row("9RAM size", "Chinese", "xiangtan", 5821.0, 3102370),
        Row("9RAM size", "Chinese", "yichang", 571.0, 1000430)
      )
    )
  }
  )

  //TC_600
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM(distinct Carbon_automation_test5.deviceInformationId) AS Sum_distinct_deviceInformationId, SUM(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 Left join ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM(distinct Carbon_automation_test5.deviceInformationId) AS Sum_distinct_deviceInformationId, SUM(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 Left join ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
      ),
      Seq(Row("0RAM size", "Chinese", "changsha", 400124, 84293.0),
        Row("0RAM size", "Chinese", "guangzhou", 100010, 869.0),
        Row("0RAM size", "Chinese", "shenzhen", 100028, 31339.0),
        Row("0RAM size", "Chinese", "wuhan", 400149, 66902.0),
        Row("0RAM size", "Chinese", "zhuzhou", 100002, 14751.0),
        Row("1RAM size", "Chinese", "guangzhou", 100030, 11997.0),
        Row("1RAM size", "Chinese", "shenzhen", 100020, 2304.0),
        Row("1RAM size", "Chinese", "xiangtan", 310100, 67590.0),
        Row("1RAM size", "Chinese", "yichang", 200106, 35478.0),
        Row("1RAM size", "Chinese", "zhuzhou", 100042, 24705.0),
        Row("2RAM size", "Chinese", "changsha", 100071, 3946.0),
        Row("2RAM size", "Chinese", "xiangtan", 10007, 2700.0),
        Row("3RAM size", "Chinese", "changsha", 100015, 40082.0),
        Row("3RAM size", "Chinese", "guangzhou", 100022, 27986.0),
        Row("3RAM size", "Chinese", "shenzhen", 400262, 92960.0),
        Row("3RAM size", "Chinese", "wuhan", 100058, 36890.0),
        Row("3RAM size", "Chinese", "xiangtan", 300141, 53536.0),
        Row("3RAM size", "Chinese", "yichang", 100069, 20874.0),
        Row("3RAM size", "Chinese", "zhuzhou", 210085, 79786.0),
        Row("4RAM size", "Chinese", "changsha", 510229, 200860.0),
        Row("4RAM size", "Chinese", "guangzhou", 100055, 38016.0),
        Row("4RAM size", "Chinese", "shenzhen", 110064, 49610.0),
        Row("4RAM size", "Chinese", "wuhan", 200101, 117581.96999999999),
        Row("4RAM size", "Chinese", "xiangtan", 1500217, 254320.0),
        Row("4RAM size", "Chinese", "yichang", 400144, 29436.0),
        Row("5RAM size", "Chinese", "changsha", 101077, 13845.0),
        Row("5RAM size", "Chinese", "guangzhou", 200092, 23560.0),
        Row("5RAM size", "Chinese", "wuhan", 10006, 12390.0),
        Row("6RAM size", "Chinese", "changsha", 200101, 23697.0),
        Row("6RAM size", "Chinese", "guangzhou", 100026, 15912.0),
        Row("6RAM size", "Chinese", "shenzhen", 100035, 19278.0),
        Row("6RAM size", "Chinese", "wuhan", 200125, 29313.0),
        Row("6RAM size", "Chinese", "xiangtan", 110013, 7794.0),
        Row("6RAM size", "Chinese", "zhuzhou", 100062, 26568.0),
        Row("7RAM size", "Chinese", "changsha", 100014, 1057.0),
        Row("7RAM size", "Chinese", "wuhan", 300065, 27853.0),
        Row("7RAM size", "Chinese", "yichang", 100133, 14217.0),
        Row("7RAM size", "Chinese", "zhuzhou", 100003, 15673.0),
        Row("8RAM size", "Chinese", "guangzhou", 1, 27385.619999999995),
        Row("8RAM size", "Chinese", "shenzhen", 100013, 3550.0),
        Row("8RAM size", "Chinese", "wuhan", 100004, 29700.0),
        Row("8RAM size", "Chinese", "xiangtan", 200054, 31020.0),
        Row("8RAM size", "Chinese", "yichang", 110025, 51660.0),
        Row("8RAM size", "Chinese", "zhuzhou", 300196, 26840.0),
        Row("9RAM size", "Chinese", "changsha", 200073, 32390.0),
        Row("9RAM size", "Chinese", "shenzhen", 200098, 30650.0),
        Row("9RAM size", "Chinese", "wuhan", 100070, 15670.0),
        Row("9RAM size", "Chinese", "xiangtan", 310237, 58210.0),
        Row("9RAM size", "Chinese", "yichang", 100043, 5710.0)
      )
    )
  }
  )
}