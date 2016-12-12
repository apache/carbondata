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
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

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
        "create table  Carbon_automation_vmall_test1 (imei string,deviceInformationId int,MAC " +
        "string,deviceColor string,device_backColor string,modelId string, marketName string," +
        "AMSize string,ROMSize string,CUPAudit string,CPIClocked string, series string," +
        "productionDate timestamp,bomCode string,internalModels string, deliveryTime string, " +
        "channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, " +
        "deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet " +
        "string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, " +
        "ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, " +
        "ActiveStreet string, ActiveOperatorId string, Active_releaseId string, " +
        "Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, " +
        "Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string," +
        "Active_webTypeDataVerNumber string, Active_operatorsVersion string, " +
        "Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY" +
        " int, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province " +
        "string, Latest_city string, Latest_district string, Latest_street string, " +
        "Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, " +
        "Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, " +
        "Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
        "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
        "Latest_operatorId string, gamePointDescription string,gamePointId decimal,contractNumber" +
        " decimal) stored by 'org.apache.carbondata.format'")
      sql("LOAD DATA LOCAL INPATH '" + currentDirectory +
          "/src/test/resources/Vmall_100_olap.csv' INTO table " +
          "Carbon_automation_vmall_test1 OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', " +
          "'FILEHEADER'= 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId," +
          "marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode," +
          "internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry," +
          "deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber," +
          "contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity," +
          "ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion," +
          "Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion," +
          "Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion," +
          "Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR," +
          "Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district," +
          "Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion," +
          "Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer," +
          "Latest_webTypeDataVerNumber,Latest_operatorsVersion," +
          "Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")
      sql(
        "create table Carbon_automation_test5 (imei string,deviceInformationId int,MAC string," +
        "deviceColor string,device_backColor string,modelId string,marketName string,AMSize " +
        "string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate " +
        "string,bomCode string,internalModels string, deliveryTime string, channelsId string, " +
        "channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince " +
        "string, deliveryCity string,deliveryDistrict string, deliveryStreet string, " +
        "oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry " +
        "string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet " +
        "string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, " +
        "Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, " +
        "Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber " +
        "string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, " +
        "Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, Latest_areaId " +
        "string, Latest_country string, Latest_province string, Latest_city string, " +
        "Latest_district string, Latest_street string, Latest_releaseId string, " +
        "Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, " +
        "Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, " +
        "Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, " +
        "Latest_phonePADPartitionedVersions string, Latest_operatorId string, " +
        " gamePointId int,contractNumber int,gamePointDescription string) stored by 'org.apache" +
        ".carbondata.format'")
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
          CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
        )
      sql("LOAD DATA LOCAL INPATH '" + currentDirectory +
          "/src/test/resources/100_olap.csv' INTO table Carbon_automation_test5 OPTIONS" +
          "('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,MAC," +
          "deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked," +
          "series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName," +
          "deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict," +
          "deliveryStreet,oxSingleNumber,ActiveCheckTime,ActiveAreaId," +
          "ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId," +
          "Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber," +
          "Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer," +
          "Active_webTypeDataVerNumber,Active_operatorsVersion," +
          "Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR," +
          "Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district," +
          "Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion," +
          "Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer," +
          "Latest_webTypeDataVerNumber,Latest_operatorsVersion," +
          "Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId," +
          "contractNumber,gamePointDescription')")


      sql(
        "create table Carbon_automation_test5_hive (imei string,deviceInformationId int,MAC " +
        "string," +
        "deviceColor string,device_backColor string,modelId string,marketName string,AMSize " +
        "string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate " +
        "string,bomCode string,internalModels string, deliveryTime string, channelsId string, " +
        "channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince " +
        "string, deliveryCity string,deliveryDistrict string, deliveryStreet string, " +
        "oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry " +
        "string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet " +
        "string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, " +
        "Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, " +
        "Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber " +
        "string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, " +
        "Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, Latest_areaId " +
        "string, Latest_country string, Latest_province string, Latest_city string, " +
        "Latest_district string, Latest_street string, Latest_releaseId string, " +
        "Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, " +
        "Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, " +
        "Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, " +
        "Latest_phonePADPartitionedVersions string, Latest_operatorId string, " +
        " gamePointId int,contractNumber int,gamePointDescription string) row format delimited " +
        "fields terminated by ','")

      sql("LOAD DATA LOCAL INPATH '" + currentDirectory +
          "/src/test/resources/100_olap.csv' INTO table Carbon_automation_test5_hive")
    } catch {
      case e: Exception => print("ERROR : " + e.getMessage)
    }

  }

  override def afterAll {
    try {
      sql("drop table Carbon_automation_vmall_test1")
      sql("drop table Carbon_automation_test5")
      sql("drop table Carbon_automation_test5_hive")
    } catch {
      case e: Exception => print("ERROR : " + e.getMessage)
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
      Seq(Row(100020, "1RAM size", "1", "Guangdong Province"),
        Row(100030, "1RAM size", "2", "Guangdong Province"),
        Row(10000, "1RAM size", "4", "Hunan Province"),
        Row(100041, "1RAM size", "4", "Hunan Province"),
        Row(100048, "1RAM size", "4", "Hunan Province"),
        Row(100011, "1RAM size", "4", "Hunan Province"),
        Row(100042, "1RAM size", "5", "Hunan Province"),
        Row(100040, "1RAM size", "7", "Hubei Province"),
        Row(100066, "1RAM size", "7", "Hubei Province"))
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity , SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
        "ActiveCountry,gamePointId, Activecity FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY ) Carbon_automation_test5_hive INNER JOIN ( SELECT ActiveCountry, Activecity, " +
        "AMSize FROM (select * from Carbon_automation_test5_hive) SUB_QRY ) " +
        "Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize WHERE NOT(Carbon_automation_test5_hive.AMSize <= " +
        "\"3RAM size\") GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive" +
        ".ActiveCountry, Carbon_automation_test5_hive.Activecity ORDER BY " +
        "Carbon_automation_test5_hive" +
        ".AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC, " +
        "Carbon_automation_test5_hive" +
        ".Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.Activecity AS Activecity FROM ( SELECT AMSize, " +
        "ActiveCountry, Activecity FROM (select * from Carbon_automation_test5_hive) SUB_QRY ) " +
        "Carbon_automation_test5_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
        "(select * from Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test5_hive.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY " +
        "Carbon_automation_test5_hive.Activecity ORDER BY Carbon_automation_test5_hive.Activecity" +
        " ASC " +
        "LIMIT 5000"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize," +
        "gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test5_hive)" +
        " " +
        "SUB_QRY ) Carbon_automation_test5_hive INNER JOIN ( SELECT ActiveCountry, Activecity, " +
        "AMSize FROM (select * from Carbon_automation_test5_hive) SUB_QRY ) " +
        "Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize WHERE NOT(Carbon_automation_test5_hive.Activecity " +
        "LIKE" +
        " 'xian%') GROUP BY Carbon_automation_test5_hive.AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry, Carbon_automation_test5_hive.Activecity ORDER BY " +
        "Carbon_automation_test5_hive" +
        ".AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC, " +
        "Carbon_automation_test5_hive" +
        ".Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
        "ActiveCountry, Activecity,gamePointId  FROM (select * from Carbon_automation_test5_hive)" +
        " " +
        "SUB_QRY ) Carbon_automation_test5_hive INNER JOIN ( SELECT ActiveCountry, Activecity, " +
        "AMSize FROM (select * from Carbon_automation_test5_hive) SUB_QRY ) " +
        "Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize WHERE NOT(Carbon_automation_test5_hive.AMSize " +
        "BETWEEN " +
        "\"4RAM size\" AND \"7RAM size\") GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
        "ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY ) Carbon_automation_test5_hive INNER JOIN ( SELECT ActiveCountry, Activecity, " +
        "AMSize FROM (select * from Carbon_automation_test5_hive) SUB_QRY ) " +
        "Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize WHERE NOT(Carbon_automation_test5_hive.AMSize IN " +
        "(\"5RAM size\",\"8RAM size\")) GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
        "ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY ) Carbon_automation_test5_hive INNER JOIN ( SELECT ActiveCountry, Activecity, " +
        "AMSize FROM (select * from Carbon_automation_test5_hive) SUB_QRY ) " +
        "Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize WHERE Carbon_automation_test5_hive.AMSize IS NOT " +
        "NULL " +
        "GROUP BY Carbon_automation_test5_hive.AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry, " +
        "Carbon_automation_test5_hive.Activecity ORDER BY Carbon_automation_test5_hive.AMSize " +
        "ASC, " +
        "Carbon_automation_test5_hive.ActiveCountry ASC, Carbon_automation_test5_hive.Activecity " +
        "ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId, SUM" +
        "(Carbon_automation_test5_hive" +
        ".deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, gamePointId," +
        "ActiveCountry,deviceInformationId, Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive INNER JOIN ( SELECT" +
        " " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId, AVG" +
        "(Carbon_automation_test5_hive" +
        ".deviceInformationId) AS avg_deviceInformationId FROM ( SELECT AMSize," +
        "deviceInformationId, gamePointId, ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive INNER JOIN ( SELECT" +
        " " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId, COUNT" +
        "(Carbon_automation_test5_hive.deviceInformationId) AS Count_deviceInformationId FROM ( " +
        "SELECT AMSize, ActiveCountry,gamePointId, Activecity,deviceInformationId FROM (select " +
        "* from Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive INNER JOIN (" +
        " SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId, COUNT(DISTINCT " +
        "Carbon_automation_test5_hive.deviceInformationId) AS LONG_COL_0 FROM ( SELECT AMSize, " +
        "ActiveCountry,gamePointId,deviceInformationId, Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive INNER JOIN ( SELECT" +
        " " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId, MAX" +
        "(Carbon_automation_test5_hive" +
        ".deviceInformationId) AS Max_deviceInformationId FROM ( SELECT AMSize,gamePointId," +
        "deviceInformationId, ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive INNER JOIN ( SELECT" +
        " " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, MIN" +
        "(Carbon_automation_test5_hive.deviceInformationId) AS Min_deviceInformationId, SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize," +
        "deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive INNER JOIN ( SELECT" +
        " " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.deviceInformationId) AS Sum_deviceInformationId, SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize," +
        "gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive INNER JOIN ( SELECT" +
        " " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.deviceInformationId) AS Sum_deviceInformationId, AVG" +
        "(Carbon_automation_test5_hive.gamePointId) AS Avg_gamePointId FROM ( SELECT AMSize," +
        "gamePointId ,deviceInformationId, ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive INNER JOIN ( SELECT" +
        " " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.deviceInformationId) AS Sum_deviceInformationId, COUNT" +
        "(Carbon_automation_test5_hive.gamePointId) AS Count_gamePointId FROM ( SELECT AMSize, " +
        "gamePointId,ActiveCountry, deviceInformationId,Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive INNER JOIN ( SELECT" +
        " " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.deviceInformationId) AS Sum_deviceInformationId, COUNT" +
        "(DISTINCT Carbon_automation_test5_hive.gamePointId) AS DistinctCount_gamePointId FROM ( " +
        "SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select " +
        "* from Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive INNER JOIN (" +
        " SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.deviceInformationId) AS Sum_deviceInformationId, MAX" +
        "(Carbon_automation_test5_hive.gamePointId) AS Max_gamePointId FROM ( SELECT AMSize, " +
        "deviceInformationId,gamePointId,ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive INNER JOIN ( SELECT" +
        " " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.deviceInformationId) AS Sum_deviceInformationId, MIN" +
        "(Carbon_automation_test5_hive.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize," +
        "gamePointId,deviceInformationId, ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive INNER JOIN ( SELECT" +
        " " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.ActiveCountry AS ActiveCountry, " +
        "Carbon_automation_test5_hive" +
        ".Activecity AS Activecity, COUNT(DISTINCT Carbon_automation_test5_hive.AMSize) AS " +
        "DistinctCount_AMSize, SUM(Carbon_automation_test5_hive.deviceInformationId) AS " +
        "Sum_deviceInformationId, SUM(Carbon_automation_test5_hive.gamePointId) AS " +
        "Sum_gamePointId " +
        "FROM ( SELECT AMSize,deviceInformationId, gamePointId,ActiveCountry, Activecity FROM " +
        "(select * from Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive " +
        "INNER JOIN (" +
        " SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
        "Carbon_automation_test5_hive)" +
        " SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive" +
        ".ActiveCountry, " +
        "Carbon_automation_test5_hive.Activecity ORDER BY Carbon_automation_test5_hive" +
        ".ActiveCountry ASC," +
        " Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM(distinct " +
        "Carbon_automation_test5_hive.gamePointId) AS Sum_distinct_gamePointId, SUM" +
        "(Carbon_automation_test5_hive.deviceInformationId) AS Sum_deviceInformationId FROM ( " +
        "SELECT" +
        " AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from" +
        " Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive INNER JOIN ( " +
        "SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM(distinct " +
        "Carbon_automation_test5_hive.deviceInformationId) AS Sum_distinct_deviceInformationId, " +
        "SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize," +
        "gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive INNER JOIN ( SELECT" +
        " " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity , SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
        "ActiveCountry,gamePointId, Activecity FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY ) Carbon_automation_test5_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, " +
        "AMSize" +
        " FROM (select * from Carbon_automation_test5_hive) SUB_QRY ) " +
        "Carbon_automation_vmall_test1 " +
        "ON Carbon_automation_test5_hive.AMSize = Carbon_automation_vmall_test1.AMSize WHERE NOT" +
        "(Carbon_automation_test5_hive.AMSize <= \"3RAM size\") GROUP BY " +
        "Carbon_automation_test5_hive" +
        ".AMSize, Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive" +
        ".Activecity " +
        "ORDER BY Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "ASC, Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.Activecity AS Activecity FROM ( SELECT AMSize, " +
        "ActiveCountry, Activecity FROM (select * from Carbon_automation_test5_hive) SUB_QRY ) " +
        "Carbon_automation_test5_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
        "(select * from Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test5_hive.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY " +
        "Carbon_automation_test5_hive.Activecity ORDER BY Carbon_automation_test5_hive.Activecity" +
        " ASC " +
        "LIMIT 5000"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize," +
        "gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_test5_hive)" +
        " " +
        "SUB_QRY ) Carbon_automation_test5_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, " +
        "AMSize" +
        " FROM (select * from Carbon_automation_test5_hive) SUB_QRY ) " +
        "Carbon_automation_vmall_test1 " +
        "ON Carbon_automation_test5_hive.AMSize = Carbon_automation_vmall_test1.AMSize WHERE NOT" +
        "(Carbon_automation_test5_hive.Activecity LIKE 'xian%') GROUP BY " +
        "Carbon_automation_test5_hive" +
        ".AMSize, Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive" +
        ".Activecity " +
        "ORDER BY Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "ASC, Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
        "ActiveCountry, Activecity,gamePointId  FROM (select * from Carbon_automation_test5_hive)" +
        " " +
        "SUB_QRY ) Carbon_automation_test5_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, " +
        "AMSize" +
        " FROM (select * from Carbon_automation_test5_hive) SUB_QRY ) " +
        "Carbon_automation_vmall_test1 " +
        "ON Carbon_automation_test5_hive.AMSize = Carbon_automation_vmall_test1.AMSize WHERE NOT" +
        "(Carbon_automation_test5_hive.AMSize BETWEEN \"4RAM size\" AND \"7RAM size\") GROUP BY " +
        "Carbon_automation_test5_hive.AMSize, Carbon_automation_test5_hive.ActiveCountry, " +
        "Carbon_automation_test5_hive.Activecity ORDER BY Carbon_automation_test5_hive.AMSize " +
        "ASC, " +
        "Carbon_automation_test5_hive.ActiveCountry ASC, Carbon_automation_test5_hive.Activecity " +
        "ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
        "ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY ) Carbon_automation_test5_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, " +
        "AMSize" +
        " FROM (select * from Carbon_automation_test5_hive) SUB_QRY ) " +
        "Carbon_automation_vmall_test1 " +
        "ON Carbon_automation_test5_hive.AMSize = Carbon_automation_vmall_test1.AMSize WHERE NOT" +
        "(Carbon_automation_test5_hive.AMSize IN (\"5RAM size\",\"8RAM size\")) GROUP BY " +
        "Carbon_automation_test5_hive.AMSize, Carbon_automation_test5_hive.ActiveCountry, " +
        "Carbon_automation_test5_hive.Activecity ORDER BY Carbon_automation_test5_hive.AMSize " +
        "ASC, " +
        "Carbon_automation_test5_hive.ActiveCountry ASC, Carbon_automation_test5_hive.Activecity " +
        "ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
        "ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY ) Carbon_automation_test5_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, " +
        "AMSize" +
        " FROM (select * from Carbon_automation_test5_hive) SUB_QRY ) " +
        "Carbon_automation_vmall_test1 " +
        "ON Carbon_automation_test5_hive.AMSize = Carbon_automation_vmall_test1.AMSize WHERE " +
        "Carbon_automation_test5_hive.AMSize IS NOT NULL GROUP BY Carbon_automation_test5_hive" +
        ".AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId, SUM" +
        "(Carbon_automation_test5_hive" +
        ".deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, gamePointId," +
        "ActiveCountry,deviceInformationId, Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive LEFT JOIN ( SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId, SUM" +
        "(Carbon_automation_test5_hive" +
        ".deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize," +
        "deviceInformationId, gamePointId, ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive LEFT JOIN ( SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId, COUNT" +
        "(Carbon_automation_test5_hive.deviceInformationId) AS Count_deviceInformationId FROM ( " +
        "SELECT AMSize, ActiveCountry,gamePointId, Activecity,deviceInformationId FROM (select " +
        "* from Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive LEFT JOIN ( " +
        "SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId, COUNT(DISTINCT " +
        "Carbon_automation_test5_hive.deviceInformationId) AS LONG_COL_0 FROM ( SELECT AMSize, " +
        "ActiveCountry,gamePointId,deviceInformationId, Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive LEFT JOIN ( SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId, MAX" +
        "(Carbon_automation_test5_hive" +
        ".deviceInformationId) AS Max_deviceInformationId FROM ( SELECT AMSize,gamePointId," +
        "deviceInformationId, ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive LEFT JOIN ( SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, MIN" +
        "(Carbon_automation_test5_hive.deviceInformationId) AS Min_deviceInformationId, SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize," +
        "deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive LEFT JOIN ( SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.deviceInformationId) AS Sum_deviceInformationId, SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize," +
        "gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive LEFT JOIN ( SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.deviceInformationId) AS Sum_deviceInformationId, AVG" +
        "(Carbon_automation_test5_hive.gamePointId) AS Avg_gamePointId FROM ( SELECT AMSize," +
        "gamePointId ,deviceInformationId, ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive LEFT JOIN ( SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.deviceInformationId) AS Sum_deviceInformationId, COUNT" +
        "(Carbon_automation_test5_hive.gamePointId) AS Count_gamePointId FROM ( SELECT AMSize, " +
        "gamePointId,ActiveCountry, deviceInformationId,Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive LEFT JOIN ( SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
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
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.deviceInformationId) AS Sum_deviceInformationId, COUNT" +
        "(DISTINCT Carbon_automation_test5_hive.gamePointId) AS DistinctCount_gamePointId FROM ( " +
        "SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select " +
        "* from Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive LEFT JOIN ( " +
        "SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY" +
        " ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
      )
    )
  }
  )

  //TC_592
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
    "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM(Carbon_automation_test5" +
    ".deviceInformationId) AS Sum_deviceInformationId, MAX(Carbon_automation_test5.gamePointId) " +
    "AS Max_gamePointId FROM ( SELECT AMSize, deviceInformationId,gamePointId,ActiveCountry, " +
    "Activecity FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 " +
    "LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
    "Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test5" +
    ".AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
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
        "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY )" +
        " Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
        "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
        "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
        "Carbon_automation_test5.Activecity ASC"
      ),
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, " +
        "SUM(Carbon_automation_test5_hive.deviceInformationId) AS Sum_deviceInformationId, MAX" +
        "(Carbon_automation_test5_hive.gamePointId) AS Max_gamePointId FROM ( SELECT AMSize, " +
        "deviceInformationId,gamePointId,ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive LEFT JOIN ( SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive" +
        ".ActiveCountry ASC, Carbon_automation_test5_hive.Activecity ASC"
      )
    )
  }
  )

  //TC_593
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
    "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM(Carbon_automation_test5" +
    ".deviceInformationId) AS Sum_deviceInformationId, MIN(Carbon_automation_test5.gamePointId) " +
    "AS Min_gamePointId FROM ( SELECT AMSize,gamePointId,deviceInformationId, ActiveCountry, " +
    "Activecity FROM (select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 " +
    "LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
    "Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test5" +
    ".AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
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
        "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY )" +
        " Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
        "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
        "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
        "Carbon_automation_test5.Activecity ASC"
      ),
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test5_hive.deviceInformationId) AS Sum_deviceInformationId, MIN" +
        "(Carbon_automation_test5_hive.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize," +
        "gamePointId,deviceInformationId, ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive LEFT JOIN ( SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY )" +
        " Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
      )
    )
  }
  )

  //TC_597
  test(
    "SELECT Carbon_automation_test5.ActiveCountry AS ActiveCountry, Carbon_automation_test5" +
    ".Activecity AS Activecity, COUNT(DISTINCT Carbon_automation_test5.AMSize) AS " +
    "DistinctCount_AMSize, SUM(Carbon_automation_test5.deviceInformationId) AS " +
    "Sum_deviceInformationId, SUM(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( " +
    "SELECT AMSize,deviceInformationId, gamePointId,ActiveCountry, Activecity FROM (select * from" +
    " Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( SELECT " +
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
        "(select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 LEFT JOIN ( " +
        "SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) " +
        "SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.ActiveCountry, " +
        "Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.ActiveCountry ASC, " +
        "Carbon_automation_test5.Activecity ASC"
      ),
      sql(
        "SELECT Carbon_automation_test5_hive.ActiveCountry AS ActiveCountry, " +
        "Carbon_automation_test5_hive" +
        ".Activecity AS Activecity, COUNT(DISTINCT Carbon_automation_test5_hive.AMSize) AS " +
        "DistinctCount_AMSize, SUM(Carbon_automation_test5_hive.deviceInformationId) AS " +
        "Sum_deviceInformationId, SUM(Carbon_automation_test5_hive.gamePointId) AS " +
        "Sum_gamePointId " +
        "FROM ( SELECT AMSize,deviceInformationId, gamePointId,ActiveCountry, Activecity FROM " +
        "(select * from Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive LEFT" +
        " JOIN ( " +
        "SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
        "Carbon_automation_test5_hive) " +
        "SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive" +
        ".ActiveCountry, " +
        "Carbon_automation_test5_hive.Activecity ORDER BY Carbon_automation_test5_hive" +
        ".ActiveCountry ASC, " +
        "Carbon_automation_test5_hive.Activecity ASC"
      )
    )
  }
  )

  //TC_599
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
    "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM(distinct " +
    "Carbon_automation_test5.gamePointId) AS Sum_distinct_gamePointId, SUM" +
    "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT " +
    "AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from " +
    "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 Left join ( SELECT ActiveCountry," +
    " Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY ) " +
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
        "(Carbon_automation_test5.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT " +
        "AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from " +
        "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 Left join ( SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY )" +
        " Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
        "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
        "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
        "Carbon_automation_test5.Activecity ASC"
      ),
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM(distinct " +
        "Carbon_automation_test5_hive.gamePointId) AS Sum_distinct_gamePointId, SUM" +
        "(Carbon_automation_test5_hive.deviceInformationId) AS Sum_deviceInformationId FROM ( " +
        "SELECT " +
        "AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive Left join ( SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY )" +
        " Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
      )
    )
  }
  )

  //TC_600
  test(
    "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry AS " +
    "ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM(distinct " +
    "Carbon_automation_test5.deviceInformationId) AS Sum_distinct_deviceInformationId, SUM" +
    "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, " +
    "deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation_test5) " +
    "SUB_QRY ) Carbon_automation_test5 Left join ( SELECT ActiveCountry, Activecity, AMSize FROM " +
    "(select * from Carbon_automation_test5) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
    "Carbon_automation_test5.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY " +
    "Carbon_automation_test5.AMSize, Carbon_automation_test5.ActiveCountry, " +
    "Carbon_automation_test5.Activecity ORDER BY Carbon_automation_test5.AMSize ASC, " +
    "Carbon_automation_test5.ActiveCountry ASC, Carbon_automation_test5.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test5.AMSize AS AMSize, Carbon_automation_test5.ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5.Activecity AS Activecity, SUM(distinct " +
        "Carbon_automation_test5.deviceInformationId) AS Sum_distinct_deviceInformationId, SUM" +
        "(Carbon_automation_test5.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize," +
        "gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test5) SUB_QRY ) Carbon_automation_test5 Left join ( SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5) SUB_QRY )" +
        " Carbon_automation_vmall_test1 ON Carbon_automation_test5.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5.AMSize, " +
        "Carbon_automation_test5.ActiveCountry, Carbon_automation_test5.Activecity ORDER BY " +
        "Carbon_automation_test5.AMSize ASC, Carbon_automation_test5.ActiveCountry ASC, " +
        "Carbon_automation_test5.Activecity ASC"
      ),
      sql(
        "SELECT Carbon_automation_test5_hive.AMSize AS AMSize, Carbon_automation_test5_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test5_hive.Activecity AS Activecity, SUM(distinct " +
        "Carbon_automation_test5_hive.deviceInformationId) AS Sum_distinct_deviceInformationId, " +
        "SUM" +
        "(Carbon_automation_test5_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize," +
        "gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test5_hive) SUB_QRY ) Carbon_automation_test5_hive Left join ( SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test5_hive) " +
        "SUB_QRY )" +
        " Carbon_automation_vmall_test1 ON Carbon_automation_test5_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test5_hive.AMSize, " +
        "Carbon_automation_test5_hive.ActiveCountry, Carbon_automation_test5_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test5_hive.AMSize ASC, Carbon_automation_test5_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test5_hive.Activecity ASC"
      )
    )
  }
  )
}