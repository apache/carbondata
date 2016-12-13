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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
  * Test Class for all queries on multiple datatypes
  * Manohar
  */
class AllDataTypesTestCase2 extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {

    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath

    try {
      sql(
        "create table Carbon_automation_test2 (imei string,deviceInformationId int,MAC string," +
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
          "gamePointDescription string, gamePointId int,contractNumber int) stored by 'org.apache" +
          ".carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='Latest_webTypeDataVerNumber')")

      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
          CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
        )

      sql("LOAD DATA LOCAL INPATH '" + currentDirectory +
        "/src/test/resources/100_olap.csv' INTO table Carbon_automation_test2 OPTIONS" +
        "('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,MAC," +
        "deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked," +
        "series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName," +
        "deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict," +
        "deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId," +
        "ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId," +
        "Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber," +
        "Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer," +
        "Active_webTypeDataVerNumber,Active_operatorsVersion," +
        "Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR," +
        "Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district," +
        "Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion," +
        "Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer," +
        "Latest_webTypeDataVerNumber,Latest_operatorsVersion," +
        "Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")

      sql(
        "create table Carbon_automation_hive (imei string,deviceInformationId int,MAC string," +
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
          "gamePointDescription string, gamePointId int,contractNumber int)" +
          " row format delimited fields terminated by ','"
      )

      sql("LOAD DATA LOCAL INPATH '" + currentDirectory + "/src/test/resources/100_olap.csv' INTO " +
        "table Carbon_automation_hive ")

      sql(
        "create table if not exists Carbon_automation_hive2(imei string,deviceInformationId int," +
          "MAC string,deviceColor string,device_backColor string,modelId string,marketName " +
          "string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string," +
          "productionDate timestamp,bomCode string,internalModels string, deliveryTime string, " +
          "channelsId string, channelsName string , deliveryAreaId string, deliveryCountry " +
          "string, deliveryProvince string, deliveryCity string,deliveryDistrict string, " +
          "deliveryStreet string, oxSingleNumber string,contractNumber int, ActiveCheckTime string, ActiveAreaId " +
          "string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict" +
          " string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, " +
          "Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, " +
          "Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string," +
          "Active_webTypeDataVerNumber string, Active_operatorsVersion string, " +
          "Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, " +
          "Latest_DAY int, Latest_HOUR string, Latest_areaId string, Latest_country string, " +
          "Latest_province string, Latest_city string, Latest_district string, Latest_street " +
          "string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion " +
          "string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion " +
          "string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
          "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
          "Latest_operatorId string, gamePointId int,gamePointDescription string" +
          ") row format delimited fields terminated by ','"
      )
      sql("LOAD DATA LOCAL INPATH '" + currentDirectory + "/src/test/resources/100_olap.csv' INTO " +
        "table Carbon_automation_hive2 ")
    } catch {
      case e: Exception => print("ERROR : " + e.getMessage)
    }
  }

  override def afterAll {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    try {
      sql("drop table Carbon_automation_test2")
      sql("drop table Carbon_automation_hive")
      sql("drop table Carbon_automation_hive2")
    } catch {
      case e: Exception => print("ERROR : " + e.getMessage)
    }
  }


  //Test-23
  test(
    "select channelsId, sum(gamePointId+ 10) Total from Carbon_automation_test2 group by  " +
      "channelsId order by Total"
  ) {

    checkAnswer(
      sql(
        "select channelsId, sum(gamePointId+ 10) Total from Carbon_automation_test2 group by  " +
          "channelsId order by Total"
      ),
      sql("select channelsId, sum(gamePointId+ 10) Total from Carbon_automation_hive2 group by  " +
        "channelsId order by Total")
    )

  }

  //Test-28
  test(
    "select channelsId, avg(gamePointId+ 10) Total from Carbon_automation_test2 group by  " +
      "channelsId order by Total"
  ) {
    checkAnswer(
      sql(
        "select channelsId, avg(gamePointId+ 10) Total from Carbon_automation_test2 group by  " +
          "channelsId order by Total"
      ), sql(
        "select channelsId, avg(gamePointId+ 10) Total from Carbon_automation_hive2 group by  " +
          "channelsId order by Total")

    )

  }

  //Test-32
  test(
    "select channelsId, count(gamePointId+ 10) Total from Carbon_automation_test2 group by  " +
      "channelsId order by Total"
  ) {
    checkAnswer(
      sql(
        "select channelsId, count(gamePointId+ 10) Total from Carbon_automation_test2 group by  " +
          "channelsId order by Total"
      ), sql(
        "select channelsId, count(gamePointId+ 10) Total from Carbon_automation_hive2 group by  " +
          "channelsId order by Total"
      )

    )

  }


  //Test-36
  test(
    "select channelsId, min(gamePointId+ 10) Total from Carbon_automation_test2 group by  " +
      "channelsId order by Total"
  ) {
    checkAnswer(
      sql(
        "select channelsId, min(gamePointId+ 10) Total from Carbon_automation_test2 group by  " +
          "channelsId order by Total"
      ), sql(
        "select channelsId, min(gamePointId+ 10) Total from Carbon_automation_test2 group by  " +
          "channelsId order by Total"
      )

    )

  }

  //Test-40
  test(
    "select channelsId, max(gamePointId+ 10) Total from Carbon_automation_test2 group by  " +
      "channelsId order by Total"
  ) {
    checkAnswer(
      sql(
        "select channelsId, max(gamePointId+ 10) Total from Carbon_automation_test2 group by  " +
          "channelsId order by Total"
      ), sql(
        "select channelsId, max(gamePointId+ 10) Total from Carbon_automation_hive2 group by  " +
          "channelsId order by Total"
      )

    )

  }

  //Test-45
  test(
    "select Latest_YEAR ,count(distinct Latest_YEAR) from Carbon_automation_test2 group by " +
      "Latest_YEAR"
  )({

    checkAnswer(
      sql(
        "select Latest_YEAR ,count(distinct Latest_YEAR) from Carbon_automation_test2 group by " +
          "Latest_YEAR"
      ), sql(
        "select Latest_YEAR ,count(distinct Latest_YEAR) from Carbon_automation_hive2 group by " +
          "Latest_YEAR"
      )
    )
  }
  )

  //TC_068
  test("select count (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_test2")({
    checkAnswer(
      sql("select count (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_hive2"),
      sql("select count (if(gamePointId>100,NULL,gamePointId))  a from " +
        "Carbon_automation_test2")
    )
  }
  )

  //TC_070
  test("select count(DISTINCT gamePointId) as a from Carbon_automation_test2")({
    checkAnswer(
      sql("select count(DISTINCT gamePointId) as a from Carbon_automation_test2"),
      sql("select count(DISTINCT gamePointId) as a from Carbon_automation_hive2")

    )
  }
  )

  //TC_076
  test("select sum( DISTINCT gamePointId) a  from Carbon_automation_test2")({
    checkAnswer(
      sql("select sum( DISTINCT gamePointId) a  from Carbon_automation_test2"),
      sql("select sum( DISTINCT gamePointId) a  from Carbon_automation_hive2")
    )
  }
  )

  //TC_079
  test("select sum (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_test2")({
    checkAnswer(
      sql("select sum (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_hive2"),
      sql("select sum (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_test2")
    )
  }
  )

  //TC_082
  test("select avg(gamePointId) a  from Carbon_automation_test2")({
    checkAnswer(
      sql("select avg(gamePointId) a  from Carbon_automation_test2"),
      sql("select avg(gamePointId) a  from Carbon_automation_hive2")
    )
  }
  )

  //TC_087
  test("select avg (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_test2")({
    checkAnswer(
      sql("select avg (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_test2"),
      sql("select avg (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_hive2")

    )
  }
  )


  //TC_089
  test("select min(gamePointId) a  from Carbon_automation_test2")({
    checkAnswer(
      sql("select min(gamePointId) a  from Carbon_automation_test2"),
      sql("select min(gamePointId) a  from Carbon_automation_hive2")
    )
  }
  )


  //TC_094
  test("select min (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_test2")({
    checkAnswer(
      sql("select min (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_test2"),
      sql("select min (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_hive2")
    )
  }
  )

  //TC_096
  test("select max(gamePointId) a  from Carbon_automation_test2")({
    checkAnswer(
      sql("select max(gamePointId) a  from Carbon_automation_test2"),
      sql("select max(gamePointId) a  from Carbon_automation_hive2")
    )
  }
  )

  //TC_101
  test("select max (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_test2")({
    checkAnswer(
      sql("select max (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_test2"),
      sql("select max (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_hive2")
    )
  }
  )

  //TC_122
  test("select stddev_pop(gamePointId) as a  from Carbon_automation_test2")({
    checkAnswer(
      sql("select stddev_pop(gamePointId) as a  from Carbon_automation_test2"),
      sql("select stddev_pop(gamePointId) as a  from Carbon_automation_hive2")
    )
  }
  )

  //TC_124
  test("select covar_pop(gamePointId,gamePointId) as a  from Carbon_automation_test2")({
    checkAnswer(
      sql("select covar_pop(gamePointId,gamePointId) as a  from Carbon_automation_test2"),
      sql("select covar_pop(gamePointId,gamePointId) as a  from Carbon_automation_hive2")
    )
  }
  )

  //TC_125
  test("select covar_samp(gamePointId,gamePointId) as a  from Carbon_automation_test2")({
    checkAnswer(
      sql("select covar_samp(gamePointId,gamePointId) as a  from Carbon_automation_test2"),
      sql("select covar_samp(gamePointId,gamePointId) as a  from Carbon_automation_hive2")
    )
  }
  )

  //TC_129
  test("select percentile_approx(gamePointId,0.2) as a  from Carbon_automation_test2")({
    checkAnswer(
      sql("select percentile_approx(gamePointId,0.2) as a  from Carbon_automation_test2"),
      sql("select percentile_approx(gamePointId,0.2) as a  from Carbon_automation_hive2")
    )
  }
  )

  //TC_142
  test(
    "select deliveryCountry,deliveryProvince,series,sum(gamePointId) a from " +
      "Carbon_automation_test2 group by deliveryCountry,deliveryProvince,series order by " +
      "deliveryCountry,deliveryProvince,series"
  )({
    checkAnswer(
      sql(
        "select deliveryCountry,deliveryProvince,series,sum(gamePointId) a from " +
          "Carbon_automation_test2 group by deliveryCountry,deliveryProvince,series order by " +
          "deliveryCountry,deliveryProvince,series"
      ), sql(
        "select deliveryCountry,deliveryProvince,series,sum(gamePointId) a from " +
          "Carbon_automation_hive2 group by deliveryCountry,deliveryProvince,series order by " +
          "deliveryCountry,deliveryProvince,series"
      )

    )
  }
  )

  //TC_146
  test(
    "select series,avg(gamePointId) a from Carbon_automation_test2 group by series order by series"
  )({
    checkAnswer(
      sql(
        "select series,avg(gamePointId) a from Carbon_automation_test2 group by series order by " +
          "series"
      ), sql(
        "select series,avg(gamePointId) a from Carbon_automation_hive2 group by series order by " +
          "series"
      )

    )
  }
  )

  //TC_147
  test(
    "select deliveryCountry,deliveryProvince,series,avg(gamePointId) a from " +
      "Carbon_automation_test2 group by deliveryCountry,deliveryProvince,series order by " +
      "deliveryCountry,deliveryProvince,series"
  )({
    checkAnswer(
      sql(
        "select deliveryCountry,deliveryProvince,series,avg(gamePointId) a from " +
          "Carbon_automation_test2 group by deliveryCountry,deliveryProvince,series order by " +
          "deliveryCountry,deliveryProvince,series"
      ), sql(
        "select deliveryCountry,deliveryProvince,series,avg(gamePointId) a from " +
          "Carbon_automation_hive2 group by deliveryCountry,deliveryProvince,series order by " +
          "deliveryCountry,deliveryProvince,series"
      )

    )
  }
  )

  //TC_151
  test(
    "select series,min(gamePointId) a from Carbon_automation_test2 group by series order by series"
  )({
    checkAnswer(
      sql(
        "select series,min(gamePointId) a from Carbon_automation_test2 group by series order by " +
          "series"
      ), sql(
        "select series,min(gamePointId) a from Carbon_automation_hive2 group by series order by " +
          "series"
      )

    )
  }
  )



  //TC_156
  test(
    "select series,max(gamePointId) a from Carbon_automation_test2 group by series order by series"
  )({
    checkAnswer(
      sql(
        "select series,max(gamePointId) a from Carbon_automation_test2 group by series order by " +
          "series"
      ), sql(
        "select series,max(gamePointId) a from Carbon_automation_hive2 group by series order by " +
          "series"
      )

    )
  }
  )

  //TC_169
  test("select imei,series,gamePointId from Carbon_automation_test2 limit 10")({
    checkAnswer(
      sql("select imei,series,gamePointId from Carbon_automation_test2 limit 10"),
      sql("select imei,series,gamePointId from Carbon_automation_hive2 limit 10")

    )
  }
  )

  //TC_174
  test("select distinct gamePointId from Carbon_automation_test2")({
    checkAnswer(
      sql("select distinct gamePointId from Carbon_automation_test2"),
      sql("select distinct gamePointId from Carbon_automation_hive2")

    )
  }
  )

  //TC_188
  test("select series,gamePointId as a from Carbon_automation_test2  order by a asc limit 10")({
    checkAnswer(
      sql("select series,gamePointId as a from Carbon_automation_test2  order by a asc limit 10"),
      sql("select series,gamePointId as a from Carbon_automation_hive2  order by a asc limit 10")

    )
  }
  )

  //TC_189
  test("select series,gamePointId as a from Carbon_automation_test2  order by a desc limit 10")({
    checkAnswer(
      sql("select series,gamePointId as a from Carbon_automation_test2  order by a desc limit 10"),
      sql("select series,gamePointId as a from Carbon_automation_hive2  order by a desc limit 10")

    )
  }
  )

  //TC_192
  test(
    "select imei from Carbon_automation_test2 where  (contractNumber == 5281803) and " +
      "(gamePointId==2738.5621) "
  )({
    checkAnswer(
      sql(
        "select imei from Carbon_automation_test2 where  (contractNumber == 5281803) and " +
          "(gamePointId==2738.5621)"
      ), sql(
        "select imei from Carbon_automation_hive2 where  (contractNumber == 5281803) and " +
          "(gamePointId==2738.5621)"
      )
    )
  }
  )

  //TC_200
  test(
    "select contractNumber,gamePointId,series  from Carbon_automation_test2 where " +
      "(deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and " +
      "deviceColor='0Device Color')"
  )({
    checkAnswer(
      sql(
        "select contractNumber,gamePointId,series  from Carbon_automation_test2 where " +
          "(deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 " +
          "and deviceColor='0Device Color')"
      ), sql(
        "select contractNumber,gamePointId,series  from Carbon_automation_hive2 where " +
          "(deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 " +
          "and deviceColor='0Device Color')"
      ))
  }
  )

  //TC_201
  test(
    "select contractNumber,gamePointId,series from Carbon_automation_test2 where series !='8Series'"
  )({
    checkAnswer(
      sql(
        "select contractNumber,gamePointId,series from Carbon_automation_test2 where series " +
          "!='8Series'"
      ), sql(
        "select contractNumber,gamePointId,series from Carbon_automation_hive2 where series " +
          "!='8Series'"
      )

    )
  }
  )

  //TC_202
  test(
    "select contractNumber,gamePointId,series from Carbon_automation_test2 where series " +
      "!='8Series' and internalModels !='8Internal models'"
  )({
    checkAnswer(
      sql(
        "select contractNumber,gamePointId,series from Carbon_automation_test2 where series " +
          "!='8Series' and internalModels !='8Internal models'"
      ), sql(
        "select contractNumber,gamePointId,series from Carbon_automation_hive2 where series " +
          "!='8Series' and internalModels !='8Internal models'"
      )

    )
  }
  )

  //TC_207
  test(
    "select contractNumber,gamePointId,series from Carbon_automation_test2 where gamePointId " +
      ">2738.562"
  )({
    checkAnswer(
      sql(
        "select contractNumber,gamePointId,series from Carbon_automation_test2 where gamePointId " +
          ">2738.562"
      ), sql(
        "select contractNumber,gamePointId,series from Carbon_automation_hive2 where gamePointId " +
          ">2738.562"
      )

    )
  }
  )

  //TC_217
  test(
    "select imei, Latest_DAY from Carbon_automation_test2 where Latest_DAY BETWEEN Latest_areaId " +
      "AND  Latest_HOUR"
  )({
    checkAnswer(
      sql(
        "select imei, Latest_DAY from Carbon_automation_test2 where Latest_DAY BETWEEN " +
          "Latest_areaId AND  Latest_HOUR"
      ), sql(
        "select imei, Latest_DAY from Carbon_automation_hive2 where Latest_DAY BETWEEN " +
          "Latest_areaId AND  Latest_HOUR"
      )


    )
  }
  )

  //TC_221
  test(
    "select imei, Latest_DAY from Carbon_automation_test2 where Latest_DAY NOT LIKE Latest_areaId" +
      " AND Latest_DAY NOT LIKE  Latest_HOUR"
  )({
    checkAnswer(
      sql(
        "select imei, Latest_DAY from Carbon_automation_test2 where Latest_DAY NOT LIKE " +
          "Latest_areaId AND Latest_DAY NOT LIKE  Latest_HOUR"
      ), sql(
        "select imei, Latest_DAY from Carbon_automation_hive2 where Latest_DAY NOT LIKE " +
          "Latest_areaId AND Latest_DAY NOT LIKE  Latest_HOUR"
      )

    )
  }
  )

  //TC_225
  test("select imei,gamePointId from Carbon_automation_test2 where gamePointId >505")({
    checkAnswer(
      sql("select imei,gamePointId from Carbon_automation_test2 where gamePointId >505"),
      sql("select imei,gamePointId from Carbon_automation_hive2 where gamePointId >505")

    )
  }
  )

  //TC_226
  test("select imei,gamePointId from Carbon_automation_test2 where gamePointId <505")({
    checkAnswer(
      sql("select imei,gamePointId from Carbon_automation_test2 where gamePointId <505"),
      sql("select imei,gamePointId from Carbon_automation_hive2 where gamePointId <505")

    )
  }
  )

  //TC_229
  test("select imei,gamePointId from Carbon_automation_test2 where gamePointId >=505")({
    checkAnswer(
      sql("select imei,gamePointId from Carbon_automation_test2 where gamePointId >=505"),
      sql("select imei,gamePointId from Carbon_automation_hive2 where gamePointId >=505")

    )
  }
  )

  //TC_230
  test("select imei,gamePointId from Carbon_automation_test2 where gamePointId <=505")({
    checkAnswer(
      sql("select imei,gamePointId from Carbon_automation_test2 where gamePointId <=505"),
      sql("select imei,gamePointId from Carbon_automation_hive2 where gamePointId <=505")

    )
  }
  )

  //TC_241
  test(
    "select imei from Carbon_automation_test2 where  (contractNumber == 5281803) OR " +
      "(gamePointId==2738.562) order by contractNumber"
  )({
    checkAnswer(
      sql(
        "select imei from Carbon_automation_test2 where  (contractNumber == 5281803) OR " +
          "(gamePointId==2738.562) order by contractNumber"
      ), sql(
        "select imei from Carbon_automation_hive2 where  (contractNumber == 5281803) OR " +
          "(gamePointId==2738.562) order by contractNumber"
      )
    )
  }
  )

  //TC_242
  test(
    "select channelsId from Carbon_automation_test2 where  (channelsId == '4') OR " +
      "(gamePointId==2738.562) order by channelsId"
  )({
    checkAnswer(
      sql(
        "select channelsId from Carbon_automation_test2 where  (channelsId == '4') OR " +
          "(gamePointId==2738.562) order by channelsId"
      ), sql(
        "select channelsId from Carbon_automation_hive2 where  (channelsId == '4') OR " +
          "(gamePointId==2738.562) order by channelsId"
      )

    )
  }
  )



  //TC_244
  test(
    "select imei, gamePointId from Carbon_automation_test2 where contractNumber in (5281803) and " +
      "gamePointId IN (2738.562) ORDER BY gamePointId"
  )({
    checkAnswer(
      sql(
        "select imei, gamePointId from Carbon_automation_test2 where contractNumber in (5281803) " +
          "and gamePointId IN (2738.562) ORDER BY gamePointId"
      ), sql(
        "select imei, gamePointId from Carbon_automation_hive2 where contractNumber in (5281803) " +
          "and gamePointId IN (2738.562) ORDER BY gamePointId"
      )
    )
  }
  )

  //TC_245
  test(
    "select channelsId from Carbon_automation_test2 where  channelsId in ('4') or gamePointId IN " +
      "(2738.562) ORDER BY channelsId"
  )({
    checkAnswer(
      sql(
        "select channelsId from Carbon_automation_test2 where  channelsId in ('4') or gamePointId" +
          " IN (2738.562) ORDER BY channelsId"
      ), sql(
        "select channelsId from Carbon_automation_hive2 where  channelsId in ('4') or gamePointId" +
          " IN (2738.562) ORDER BY channelsId"
      )

    )
  }
  )

  //TC_246
  test(
    "select deliveryCity from Carbon_automation_test2 where  deliveryCity IN ('yichang') AND  " +
      "deliveryStreet IN ('yichang') ORDER BY deliveryCity"
  )({
    checkAnswer(
      sql(
        "select deliveryCity from Carbon_automation_test2 where  deliveryCity IN ('yichang') AND " +
          " deliveryStreet IN ('yichang') ORDER BY deliveryCity"
      ), sql(
        "select deliveryCity from Carbon_automation_hive2 where  deliveryCity IN ('yichang') AND " +
          " deliveryStreet IN ('yichang') ORDER BY deliveryCity"
      )

    )
  }
  )

  //TC_247
  test(
    "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  channelsId > " +
      "4 ORDER BY gamePointId limit 5"
  )({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  " +
          "channelsId > 4 ORDER BY gamePointId limit 5"
      ), sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_hive2 where  " +
          "channelsId > 4 ORDER BY gamePointId limit 5"
      )

    )
  }
  )

  //TC_248
  test(
    "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  channelsId < " +
      "4 ORDER BY gamePointId limit 5"
  )({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  " +
          "channelsId < 4 ORDER BY gamePointId limit 5"
      ), sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_hive2 where  " +
          "channelsId < 4 ORDER BY gamePointId limit 5"
      )

    )
  }
  )

  //TC_249
  test(
    "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  channelsId >=" +
      " 4 ORDER BY gamePointId limit 5"
  )({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  " +
          "channelsId >= 4 ORDER BY gamePointId limit 5"
      ), sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_hive2 where  " +
          "channelsId >= 4 ORDER BY gamePointId limit 5"
      )

    )
  }
  )

  //TC_250
  test(
    "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  channelsId <=" +
      " 4 ORDER BY gamePointId limit 5"
  )({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  " +
          "channelsId <= 4 ORDER BY gamePointId limit 5"
      ), sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_hive2 where  " +
          "channelsId <= 4 ORDER BY gamePointId limit 5"
      )

    )
  }
  )

  //TC_251
  test(
    "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  channelsId " +
      "BETWEEN 4 AND 5 ORDER BY gamePointId limit 5"
  )({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  " +
          "channelsId BETWEEN 4 AND 5 ORDER BY gamePointId limit 5"
      ), sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_hive2 where  " +
          "channelsId BETWEEN 4 AND 5 ORDER BY gamePointId limit 5"
      )

    )
  }
  )

  //TC_252
  test(
    "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  channelsId " +
      "NOT BETWEEN 4 AND 5 ORDER BY gamePointId limit 5"
  )({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  " +
          "channelsId NOT BETWEEN 4 AND 5 ORDER BY gamePointId limit 5"
      ), sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_hive2 where  " +
          "channelsId NOT BETWEEN 4 AND 5 ORDER BY gamePointId limit 5"
      )

    )
  }
  )

  //TC_253
  test(
    "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  channelsId " +
      "LIKE 4 ORDER BY gamePointId limit 5"
  )({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  " +
          "channelsId LIKE 4 ORDER BY gamePointId limit 5"
      ), sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_hive2 where  " +
          "channelsId LIKE 4 ORDER BY gamePointId limit 5"
      )

    )
  }
  )

  //TC_254
  test(
    "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  channelsId " +
      "NOT LIKE 4 AND channelsId NOT LIKE 5 ORDER BY gamePointId limit 5"
  )({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  " +
          "channelsId NOT LIKE 4 AND channelsId NOT LIKE 5 ORDER BY gamePointId limit 5"
      ), sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_hive2 where  " +
          "channelsId NOT LIKE 4 AND channelsId NOT LIKE 5 ORDER BY gamePointId limit 5"
      )

    )
  }
  )

  //TC_255
  test(
    "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  channelsId " +
      "RLIKE 4 ORDER BY gamePointId limit 5"
  )({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  " +
          "channelsId RLIKE 4 ORDER BY gamePointId limit 5"
      ), sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_hive2 where  " +
          "channelsId RLIKE 4 ORDER BY gamePointId limit 5"
      )

    )
  }
  )

  //TC_256
  test(
    "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  channelsId " +
      "NOT RLIKE 4 AND channelsId NOT RLIKE 5 ORDER BY gamePointId limit 5"
  )({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  " +
          "channelsId NOT RLIKE 4 AND channelsId NOT RLIKE 5 ORDER BY gamePointId limit 5"
      ), sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_hive2 where  " +
          "channelsId NOT RLIKE 4 AND channelsId NOT RLIKE 5 ORDER BY gamePointId limit 5"
      )

    )
  }
  )



  //TC_261
  test(
    "select  imei from Carbon_automation_test2 where UPPER(Latest_province) == 'GUANGDONG PROVINCE'"
  )({
    checkAnswer(
      sql(
        "select  imei from Carbon_automation_test2 where UPPER(Latest_province) == 'GUANGDONG " +
          "PROVINCE'"
      ), sql(
        "select  imei from Carbon_automation_hive2 where UPPER(Latest_province) == 'GUANGDONG " +
          "PROVINCE'"
      )

    )
  }
  )


  //TC_266
  test(
    "SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
      "Carbon_automation_test2) SUB_QRY WHERE AMSize > \"\" GROUP BY AMSize, ActiveAreaId ORDER " +
      "BY AMSize ASC, ActiveAreaId ASC"
  )({
    checkAnswer(
      sql(
        "SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
          "Carbon_automation_test2) SUB_QRY WHERE AMSize > \"\" GROUP BY AMSize, ActiveAreaId " +
          "ORDER BY AMSize ASC, ActiveAreaId ASC"
      ), sql(
        "SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
          "Carbon_automation_hive2) SUB_QRY WHERE AMSize > \"\" GROUP BY AMSize, ActiveAreaId " +
          "ORDER BY AMSize ASC, ActiveAreaId ASC"
      )

    )
  }
  )

  //TC_271
  test(
    "SELECT ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, ActiveStreet FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY GROUP BY ActiveCountry, ActiveDistrict, " +
      "ActiveOperatorId, ActiveProvince, ActiveStreet ORDER BY ActiveCountry ASC, ActiveDistrict " +
      "ASC, ActiveOperatorId ASC, ActiveProvince ASC, ActiveStreet ASC"
  )({
    checkAnswer(
      sql(
        "SELECT ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, ActiveStreet " +
          "FROM (select * from Carbon_automation_test2) SUB_QRY GROUP BY ActiveCountry, " +
          "ActiveDistrict, ActiveOperatorId, ActiveProvince, ActiveStreet ORDER BY ActiveCountry " +
          "ASC, ActiveDistrict ASC, ActiveOperatorId ASC, ActiveProvince ASC, ActiveStreet ASC"
      ), sql(
        "SELECT ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, ActiveStreet " +
          "FROM (select * from Carbon_automation_hive2) SUB_QRY GROUP BY ActiveCountry, " +
          "ActiveDistrict, ActiveOperatorId, ActiveProvince, ActiveStreet ORDER BY ActiveCountry " +
          "ASC, ActiveDistrict ASC, ActiveOperatorId ASC, ActiveProvince ASC, ActiveStreet ASC"
      )

    )
  }
  )

  //TC_272
  test(
    "SELECT ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, ActiveStreet, SUM" +
      "(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test2) SUB_QRY " +
      "GROUP BY ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, ActiveStreet " +
      "ORDER BY ActiveCountry ASC, ActiveDistrict ASC, ActiveOperatorId ASC, ActiveProvince ASC, " +
      "ActiveStreet ASC"
  )({
    checkAnswer(
      sql(
        "SELECT ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, ActiveStreet, " +
          "SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test2) " +
          "SUB_QRY GROUP BY ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, " +
          "ActiveStreet ORDER BY ActiveCountry ASC, ActiveDistrict ASC, ActiveOperatorId ASC, " +
          "ActiveProvince ASC, ActiveStreet ASC"
      ), sql(
        "SELECT ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, ActiveStreet, " +
          "SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_hive2) " +
          "SUB_QRY GROUP BY ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, " +
          "ActiveStreet ORDER BY ActiveCountry ASC, ActiveDistrict ASC, ActiveOperatorId ASC, " +
          "ActiveProvince ASC, ActiveStreet ASC"
      )

    )
  }
  )

  //TC_273
  test(
    "SELECT ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, ActiveStreet, AVG" +
      "(gamePointId) AS Avg_gamePointId FROM (select * from Carbon_automation_test2) SUB_QRY " +
      "GROUP BY ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, ActiveStreet " +
      "ORDER BY ActiveCountry ASC, ActiveDistrict ASC, ActiveOperatorId ASC, ActiveProvince ASC, " +
      "ActiveStreet ASC"
  )({
    checkAnswer(
      sql(
        "SELECT ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, ActiveStreet, " +
          "AVG(gamePointId) AS Avg_gamePointId FROM (select * from Carbon_automation_test2) " +
          "SUB_QRY GROUP BY ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, " +
          "ActiveStreet ORDER BY ActiveCountry ASC, ActiveDistrict ASC, ActiveOperatorId ASC, " +
          "ActiveProvince ASC, ActiveStreet ASC"
      ), sql(
        "SELECT ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, ActiveStreet, " +
          "AVG(gamePointId) AS Avg_gamePointId FROM (select * from Carbon_automation_hive2) " +
          "SUB_QRY GROUP BY ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, " +
          "ActiveStreet ORDER BY ActiveCountry ASC, ActiveDistrict ASC, ActiveOperatorId ASC, " +
          "ActiveProvince ASC, ActiveStreet ASC"
      )

    )
  }
  )
  //TC_283
  test(
    "select  AMSize,sum( gamePointId+ contractNumber) as total from Carbon_automation_test2 where" +
      " AMSize='0RAM size' and  ActiveProvince='Guangdong Province' group by AMSize"
  )({
    checkAnswer(
      sql(
        "select  AMSize,sum( gamePointId+ contractNumber) as total from Carbon_automation_test2 " +
          "where AMSize='0RAM size' and  ActiveProvince='Guangdong Province' group by AMSize"
      ), sql(
        "select  AMSize,sum( gamePointId+ contractNumber) as total from Carbon_automation_hive2 " +
          "where AMSize='0RAM size' and  ActiveProvince='Guangdong Province' group by AMSize"
      )
    )
  }
  )

  //TC_284
  test(
    "select AMSize,sum(gamePointId+contractNumber) as total from Carbon_automation_test2 where  " +
      "CUPAudit='0CPU Audit' group by AMSize having total > 10 order by total desc"
  )({
    checkAnswer(
      sql(
        "select AMSize,sum(gamePointId+contractNumber) as total from Carbon_automation_test2 " +
          "where  CUPAudit='0CPU Audit' group by AMSize having total > 10 order by total desc"
      ), sql(
        "select AMSize,sum(gamePointId+contractNumber) as total from Carbon_automation_hive2 " +
          "where  CUPAudit='0CPU Audit' group by AMSize having total > 10 order by total desc"
      )

    )
  }
  )

  //TC_286
  test(
    "select  ActiveAreaId,count(distinct AMSize) as AMSize_number, sum" +
      "(gamePointId+contractNumber) as total from Carbon_automation_test2 group by ActiveAreaId"
  )({
    checkAnswer(
      sql(
        "select  ActiveAreaId,count(distinct AMSize) as AMSize_number, sum" +
          "(gamePointId+contractNumber) as total from Carbon_automation_test2 group by ActiveAreaId"
      ), sql(
        "select  ActiveAreaId,count(distinct AMSize) as AMSize_number, sum" +
          "(gamePointId+contractNumber) as total from Carbon_automation_hive2 group by ActiveAreaId"
      )
    )
  }
  )

  //TC_308
  test(
    "select t2.AMSize,t1.Activecity,count(t1.AMSize) as AMSize_number,sum(t1.gamePointId+t1" +
      ".contractNumber) as total from Carbon_automation_test2 t1, Carbon_automation_test2 t2 " +
      "where t1.AMSize=t2.AMSize group by t1.Activecity,t2.AMSize"
  )({
    checkAnswer(
      sql(
        "select t2.AMSize,t1.Activecity,count(t1.AMSize) as AMSize_number,sum(t1.gamePointId+t1" +
          ".contractNumber) as total from Carbon_automation_test2 t1, Carbon_automation_test2 t2 " +
          "where t1.AMSize=t2.AMSize group by t1.Activecity,t2.AMSize"
      ), sql(
        "select t2.AMSize,t1.Activecity,count(t1.AMSize) as AMSize_number,sum(t1.gamePointId+t1" +
          ".contractNumber) as total from Carbon_automation_test2 t1, Carbon_automation_hive2 t2 " +
          "where t1.AMSize=t2.AMSize group by t1.Activecity,t2.AMSize"
      )

    )
  }
  )
  //TC_312
  test(
    "select ActiveOperatorId, sum(imeiupdown) as total, count(distinct AMSize) as AMSize_count " +
      "from (select AMSize, t1.gamePointId+t1.contractNumber as imeiupdown, if((t1.gamePointId+t1" +
      ".contractNumber)>100, '>50', if((t1.gamePointId+t1.contractNumber)>100,'50~10',if((t1" +
      ".gamePointId+t1.contractNumber)>100, '10~1','<1'))) as ActiveOperatorId from " +
      "Carbon_automation_test2 t1) t2 group by ActiveOperatorId"
  )({
    checkAnswer(
      sql(
        "select ActiveOperatorId, sum(imeiupdown) as total, count(distinct AMSize) as " +
          "AMSize_count from (select AMSize, t1.gamePointId+t1.contractNumber as imeiupdown, if(" +
          "(t1.gamePointId+t1.contractNumber)>100, '>50', if((t1.gamePointId+t1.contractNumber)" +
          ">100,'50~10',if((t1.gamePointId+t1.contractNumber)>100, '10~1','<1'))) as " +
          "ActiveOperatorId from Carbon_automation_test2 t1) t2 group by ActiveOperatorId"
      ),
      sql(
        "select ActiveOperatorId, sum(imeiupdown) as total, count(distinct AMSize) as " +
          "AMSize_count from (select AMSize, t1.gamePointId+t1.contractNumber as imeiupdown, if(" +
          "(t1.gamePointId+t1.contractNumber)>100, '>50', if((t1.gamePointId+t1.contractNumber)" +
          ">100,'50~10',if((t1.gamePointId+t1.contractNumber)>100, '10~1','<1'))) as " +
          "ActiveOperatorId from Carbon_automation_hive2 t1) t2 group by ActiveOperatorId"
      )
    )
  }
  )


  //TC_327
  test(
    "SELECT imei, deliveryCity, COUNT(Latest_YEAR) AS Count_Latest_YEAR, SUM(gamePointId) AS " +
      "Sum_gamePointId FROM (select * from Carbon_automation_test2) SUB_QRY WHERE imei >= " +
      "\"1AA1000000\" GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT imei, deliveryCity, COUNT(Latest_YEAR) AS Count_Latest_YEAR, SUM(gamePointId) AS " +
          "Sum_gamePointId FROM (select * from Carbon_automation_test2) SUB_QRY WHERE imei >= " +
          "\"1AA1000000\" GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC"
      ), sql(
        "SELECT imei, deliveryCity, COUNT(Latest_YEAR) AS Count_Latest_YEAR, SUM(gamePointId) AS " +
          "Sum_gamePointId FROM (select * from Carbon_automation_hive2) SUB_QRY WHERE imei >= " +
          "\"1AA1000000\" GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC"
      )

    )
  }
  )



  //TC_340
  test(
    "SELECT latest_year, latest_day, imei, gamepointid, deviceinformationid, series, imei, " +
      "deliverycity FROM (select * from Carbon_automation_test2) SUB_QRY WHERE NOT(gamepointid > " +
      "2000.0)"
  )({
    checkAnswer(
      sql(
        "SELECT latest_year, latest_day, imei, gamepointid, deviceinformationid, series, imei, " +
          "deliverycity FROM (select * from Carbon_automation_test2) SUB_QRY WHERE NOT" +
          "(gamepointid > 2000.0)"
      ), sql(
        "SELECT latest_year, latest_day, imei, gamepointid, deviceinformationid, series, imei, " +
          "deliverycity FROM (select * from Carbon_automation_hive2) SUB_QRY WHERE NOT" +
          "(gamepointid > 2000.0)"
      )

    )
  }
  )

  //TC_342
  test(
    "SELECT latest_year, latest_day, imei, gamepointid, deviceinformationid, series, imei, " +
      "deliverycity FROM (select * from Carbon_automation_test2) SUB_QRY WHERE NOT(gamepointid = " +
      "1600)"
  )({
    checkAnswer(
      sql(
        "SELECT latest_year, latest_day, imei, gamepointid, deviceinformationid, series, imei, " +
          "deliverycity FROM (select * from Carbon_automation_test2) SUB_QRY WHERE NOT" +
          "(gamepointid = 1600)"
      ), sql(
        "SELECT latest_year, latest_day, imei, gamepointid, deviceinformationid, series, imei, " +
          "deliverycity FROM (select * from Carbon_automation_hive2) SUB_QRY WHERE NOT" +
          "(gamepointid = 1600)"
      )

    )
  }
  )

  //TC_344
  test(
    "SELECT gamePointId, deviceInformationId, Latest_YEAR, series, imei, deliveryCity FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY WHERE deliveryCity LIKE 'wu%'"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, deviceInformationId, Latest_YEAR, series, imei, deliveryCity FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY WHERE deliveryCity LIKE 'wu%'"
      ), sql(
        "SELECT gamePointId, deviceInformationId, Latest_YEAR, series, imei, deliveryCity FROM " +
          "(select * from Carbon_automation_hive2) SUB_QRY WHERE deliveryCity LIKE 'wu%'"
      )

    )
  }
  )

  //TC_345
  test(
    "SELECT gamepointid, deviceinformationid, latest_year, series, imei, deliverycity FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(imei LIKE '%1AA10%')"
  )({
    checkAnswer(
      sql(
        "SELECT gamepointid, deviceinformationid, latest_year, series, imei, deliverycity FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(imei LIKE '%1AA10%')"
      ), sql(
        "SELECT gamepointid, deviceinformationid, latest_year, series, imei, deliverycity FROM " +
          "(select * from Carbon_automation_hive2) SUB_QRY WHERE NOT(imei LIKE '%1AA10%')"
      )
    )
  }
  )

  //TC_347
  test(
    "SELECT gamepointid, deviceinformationid, latest_year, series, imei, deliverycity FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY WHERE deviceinformationid = 100031"
  )({
    checkAnswer(
      sql(
        "SELECT gamepointid, deviceinformationid, latest_year, series, imei, deliverycity FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY WHERE deviceinformationid = 100031"
      ), sql(
        "SELECT gamepointid, deviceinformationid, latest_year, series, imei, deliverycity FROM " +
          "(select * from Carbon_automation_hive2) SUB_QRY WHERE deviceinformationid = 100031"
      )
    )
  }
  )

  //TC_348
  test("SELECT latest_year, gamepointid FROM (select * from Carbon_automation_test2) SUB_QRY")({
    checkAnswer(
      sql("SELECT latest_year, gamepointid FROM (select * from Carbon_automation_test2) SUB_QRY"),
      sql("SELECT latest_year, gamepointid FROM (select * from Carbon_automation_hive2) SUB_QRY")

    )
  }
  )

  //TC_349
  test(
    "SELECT latest_year, gamepointid FROM (select * from Carbon_automation_test2) SUB_QRY WHERE " +
      "gamepointid BETWEEN 200.0 AND 300.0"
  )({
    checkAnswer(
      sql(
        "SELECT latest_year, gamepointid FROM (select * from Carbon_automation_test2) SUB_QRY " +
          "WHERE gamepointid BETWEEN 200.0 AND 300.0"
      ), sql(
        "SELECT latest_year, gamepointid FROM (select * from Carbon_automation_hive2) SUB_QRY " +
          "WHERE gamepointid BETWEEN 200.0 AND 300.0"
      )
    )
  }
  )

  //TC_351
  test(
    "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * from" +
      " Carbon_automation_test2) SUB_QRY WHERE deliveryCity IN (\"changsha\")"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * " +
          "from Carbon_automation_test2) SUB_QRY WHERE deliveryCity IN (\"changsha\")"
      ), sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * " +
          "from Carbon_automation_hive2) SUB_QRY WHERE deliveryCity IN (\"changsha\")"
      )

    )
  }
  )

  //TC_352
  test(
    "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * from" +
      " Carbon_automation_test2) SUB_QRY WHERE deliveryCity IS NOT NULL"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * " +
          "from Carbon_automation_test2) SUB_QRY WHERE deliveryCity IS NOT NULL"
      ), sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * " +
          "from Carbon_automation_hive2) SUB_QRY WHERE deliveryCity IS NOT NULL"
      )

    )
  }
  )

  //TC_353
  test(
    "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * from" +
      " Carbon_automation_test2) SUB_QRY WHERE NOT(deliveryCity IN (\"guangzhou\",\"changsha\"))"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * " +
          "from Carbon_automation_test2) SUB_QRY WHERE NOT(deliveryCity IN (\"guangzhou\"," +
          "\"changsha\"))"
      ), sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * " +
          "from Carbon_automation_hive2) SUB_QRY WHERE NOT(deliveryCity IN (\"guangzhou\"," +
          "\"changsha\"))"
      )

    )
  }
  )

  //TC_354
  test(
    "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * from" +
      " Carbon_automation_test2) SUB_QRY WHERE NOT(imei BETWEEN \"1AA100\" AND \"1AA10000\")"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * " +
          "from Carbon_automation_test2) SUB_QRY WHERE NOT(imei BETWEEN \"1AA100\" AND " +
          "\"1AA10000\")"
      ), sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * " +
          "from Carbon_automation_hive2) SUB_QRY WHERE NOT(imei BETWEEN \"1AA100\" AND " +
          "\"1AA10000\")"
      )

    )
  }
  )

  //TC_356
  test(
    "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(gamePointId >= 1000.0)"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(gamePointId >= 1000.0)"
      ), sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
          "(select * from Carbon_automation_hive2) SUB_QRY WHERE NOT(gamePointId >= 1000.0)"
      )

    )
  }
  )

  //TC_357
  test(
    "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(gamePointId < 500.0)"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(gamePointId < 500.0)"
      ), sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
          "(select * from Carbon_automation_hive2) SUB_QRY WHERE NOT(gamePointId < 500.0)"
      )

    )
  }
  )

  //TC_358
  test(
    "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(gamePointId <= 600.0)"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(gamePointId <= 600.0)"
      ), sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
          "(select * from Carbon_automation_hive2) SUB_QRY WHERE NOT(gamePointId <= 600.0)"
      )

    )
  }
  )

  //TC_359
  test(
    "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(deliveryCity LIKE '%wuhan%')"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(deliveryCity LIKE '%wuhan%')"
      ), sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
          "(select * from Carbon_automation_hive2) SUB_QRY WHERE NOT(deliveryCity LIKE '%wuhan%')"
      )

    )
  }
  )

  //TC_360
  test(
    "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(deliveryCity LIKE 'wu%')"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(deliveryCity LIKE 'wu%')"
      ), sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
          "(select * from Carbon_automation_hive2) SUB_QRY WHERE NOT(deliveryCity LIKE 'wu%')"
      )

    )
  }
  )

  //TC_361
  test(
    "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY WHERE deliveryCity IN (\"changsha\")"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY WHERE deliveryCity IN (\"changsha\")"
      ), sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
          "(select * from Carbon_automation_hive2) SUB_QRY WHERE deliveryCity IN (\"changsha\")"
      )

    )
  }
  )

  //TC_362
  test(
    "SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY"
  )({
    checkAnswer(
      sql(
        "SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY"
      ), sql(
        "SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM " +
          "(select * from Carbon_automation_hive2) SUB_QRY"
      )

    )
  }
  )

  //TC_363
  test(
    "SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY ORDER BY deviceInformationId ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY ORDER BY deviceInformationId ASC"
      ), sql(
        "SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM " +
          "(select * from Carbon_automation_hive2) SUB_QRY ORDER BY deviceInformationId ASC"
      )

    )
  }
  )
  //TC_390
  test(
    "select Latest_DAY,imei,gamepointid  from Carbon_automation_test2 where ( Latest_DAY+1) == 2 order by imei limit 5"
  )({
    checkAnswer(
      sql(
        "select Latest_DAY,imei,gamepointid  from Carbon_automation_test2 where ( Latest_DAY+1) == 2 order by imei limit 5"
      ), sql(
        "select Latest_DAY,imei,gamepointid  from Carbon_automation_hive2 where ( Latest_DAY+1) == 2 order by imei limit 5"
      )

    )
  }
  )

  //TC_396
  test(
    "select Latest_province,imei from Carbon_automation_test2 where UPPER(Latest_province) == 'GUANGDONG PROVINCE' order by imei limit 5"
  )({
    checkAnswer(
      sql(
        "select Latest_province,imei from Carbon_automation_test2 where UPPER(Latest_province) == 'GUANGDONG PROVINCE' order by imei limit 5"
      ), sql(
        "select Latest_province,imei from Carbon_automation_hive2 where UPPER(Latest_province) == 'GUANGDONG PROVINCE' order by imei limit 5"
      )

    )
  }
  )

  //TC_398
  test(
    "select Latest_DAY,imei from Carbon_automation_test2 where LOWER(Latest_DAY) == '1' order by imei limit 5"
  )({
    checkAnswer(
      sql(
        "select Latest_DAY,imei from Carbon_automation_test2 where LOWER(Latest_DAY) == '1' order by imei limit 5"
      ), sql(
        "select Latest_DAY,imei from Carbon_automation_hive2 where LOWER(Latest_DAY) == '1' order by imei limit 5"
      )
    )
  }
  )

  //TC_399
  test(
    "select deviceInformationId,imei from Carbon_automation_test2 where UPPER(deviceInformationId) == '1' order by imei limit 5"
  )({
    checkAnswer(
      sql(
        "select deviceInformationId,imei from Carbon_automation_test2 where UPPER(deviceInformationId) == '1' order by imei limit 5"
      ), sql(
        "select deviceInformationId,imei from Carbon_automation_hive2 where UPPER(deviceInformationId) == '1' order by imei limit 5"
      )
    )
  }
  )

  //TC_400
  test(
    "select deviceInformationId,imei from Carbon_automation_test2 where LOWER(deviceInformationId) == '1' order by imei limit 5"
  )({
    checkAnswer(
      sql(
        "select deviceInformationId,imei from Carbon_automation_test2 where LOWER(deviceInformationId) == '1' order by imei limit 5"
      ), sql(
        "select deviceInformationId,imei from Carbon_automation_hive2 where LOWER(deviceInformationId) == '1' order by imei limit 5"
      )
    )
  }
  )

  //TC_426
  test("select  gamePointId from Carbon_automation_test2 where deviceInformationId is NOT null")({
    checkAnswer(
      sql("select  gamePointId from Carbon_automation_test2 where deviceInformationId is NOT null"),
      sql("select  gamePointId from Carbon_automation_hive2 where deviceInformationId is NOT null"
      )
    )
  }
  )


  //TC_436
  test("SELECT count(DISTINCT gamePointId) FROM  Carbon_automation_test2 where imei is NOT null")({
    checkAnswer(
      sql("SELECT count(DISTINCT gamePointId) FROM  Carbon_automation_test2 where imei is NOT null"),
      sql("SELECT count(DISTINCT gamePointId) FROM  Carbon_automation_hive2 where imei is NOT null")
    )
  }
  )

  //TC_438
  test("SELECT avg(contractNumber) FROM  Carbon_automation_test2  where imei is NOT null")({
    checkAnswer(
      sql("SELECT avg(contractNumber) FROM  Carbon_automation_test2  where imei is NOT null"),
      sql("SELECT avg(contractNumber) FROM  Carbon_automation_hive2  where imei is NOT null")
    )
  }
  )

  //TC_440
  test("SELECT max(gamePointId) FROM Carbon_automation_test2  where contractNumber is NOT null")({
    checkAnswer(
      sql("SELECT max(gamePointId) FROM Carbon_automation_test2  where contractNumber is NOT null"),
      sql("SELECT max(gamePointId) FROM Carbon_automation_hive2  where contractNumber is NOT null")
    )
  }
  )

  //TC_442
  test(
    "select variance(deviceInformationId), var_pop(imei)  from Carbon_automation_test2 where activeareaid>3"
  )({
    checkAnswer(
      sql(
        "select variance(deviceInformationId), var_pop(imei)  from Carbon_automation_test2 where activeareaid>3"
      ),
      sql(
        "select variance(deviceInformationId), var_pop(imei)  from Carbon_automation_hive2 where activeareaid>3"
      )
    )
  }
  )

  //TC_450
  test("select var_samp(gamepointId) from Carbon_automation_test2")({
    checkAnswer(
      sql("select var_samp(gamepointId) from Carbon_automation_test2"),
      sql("select var_samp(gamepointId) from Carbon_automation_hive2")
    )
  }
  )


  //TC_453
  test("select stddev_pop(gamePointId)from Carbon_automation_test2")({
    checkAnswer(
      sql("select stddev_pop(gamePointId)from Carbon_automation_test2"),
      sql("select stddev_pop(gamePointId)from Carbon_automation_hive2")
    )
  }
  )

  //TC_456
  test("select stddev_samp(contractNumber)from Carbon_automation_test2")({
    checkAnswer(
      sql("select stddev_samp(contractNumber)from Carbon_automation_test2"),
      sql("select stddev_samp(contractNumber)from Carbon_automation_hive2")
    )
  }
  )



  //TC_461
  test("select covar_pop(gamePointId, Latest_MONTH) from Carbon_automation_test2")({
    checkAnswer(
      sql("select covar_pop(gamePointId, Latest_MONTH) from Carbon_automation_test2"),
      sql("select covar_pop(gamePointId, Latest_MONTH) from Carbon_automation_hive2")
    )
  }
  )


  //TC_465
  test("select covar_samp(gamePointId, Latest_MONTH) from Carbon_automation_test2")({
    checkAnswer(
      sql("select covar_samp(gamePointId, Latest_MONTH) from Carbon_automation_test2"),
      sql("select covar_samp(gamePointId, Latest_MONTH) from Carbon_automation_hive2")
    )
  }
  )


  //TC_470
  test("select corr(gamePointId, deviceInformationId) from Carbon_automation_test2")({
    checkAnswer(
      sql("select corr(gamePointId, deviceInformationId) from Carbon_automation_test2"),
      sql("select corr(gamePointId, deviceInformationId) from Carbon_automation_hive2")
    )
  }
  )

  //TC_478
  test("select cast(gamepointid as int) as a from Carbon_automation_test2 limit 10")({
    checkAnswer(
      sql("select cast(gamepointid as int) as a from Carbon_automation_test2 limit 10"),
      sql("select cast(gamepointid as int) as a from Carbon_automation_hive2 limit 10")

    )
  }
  )

}

