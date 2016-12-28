
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

import java.io.{File, FileInputStream, InputStream}

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test Class for all queries on multiple datatypes
 * Manohar
 */
class AllDataTypesTestCase1 extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    val filesystem: InputStream = new FileInputStream(new File("target/classes/app.properties"))
    val properties = new java.util.Properties()
    properties.load(filesystem)
    val path: String = properties.getProperty("file-source")

    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath

    CarbonProperties.getInstance().addProperty("carbon.direct.surrogate", "false")
    try {
      sql(
        "create table Carbon_automation_test (imei string,deviceInformationId int,MAC string," +
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
          CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    }
    catch {
      case e: Exception => print("ERROR : " + e.getMessage)
    }
    try {
      sql("LOAD DATA LOCAL INPATH \"" + path +
          "100_olap.csv\" INTO table Carbon_automation_test OPTIONS" +
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
    }
    catch {
      case e: Exception => print("ERROR : " + e.getMessage)
    }
    try {
      sql(
        "create table if not exists Carbon_automation_hive(imei string,deviceInformationId int," +
        "MAC string,deviceColor string,device_backColor string,modelId string,marketName " +
        "string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string," +
        "productionDate timestamp,bomCode string,internalModels string, deliveryTime string, " +
        "channelsId string, channelsName string , deliveryAreaId string, deliveryCountry " +
        "string, deliveryProvince string, deliveryCity string,deliveryDistrict string, " +
        "deliveryStreet string, oxSingleNumber string,contractNumber int, ActiveCheckTime string," +
        " ActiveAreaId " +
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
    }
    catch {
      case e: Exception => print("ERROR : " + e.getMessage)
    }
    try {
      sql(
        "LOAD DATA LOCAL INPATH '" + currentDirectory + "/src/test/resources/100_olap.csv' INTO " +
        "table Carbon_automation_hive")

    }
    catch {
      case e: Exception => print("ERROR : " + e.getMessage)
    }

  }


  override def afterAll {
    sql("drop table if exists Carbon_automation_test")
    sql("drop table if exists Carbon_automation_hive")

  }


  //Test-22
  test(
    "select channelsId, sum(Latest_MONTH+ 10) as a from Carbon_automation_test group by  channelsId"
  ) {
    checkAnswer(
      sql(
        "select channelsId, sum(Latest_MONTH+ 10) as a from Carbon_automation_test group by  " +
        "channelsId"
      ),
      sql(
        "select channelsId, sum(Latest_MONTH+ 10) as a from Carbon_automation_hive group by  " +
        "channelsId"
      )

    )

  }


  //Test-24
  test(
    "select channelsId, sum(channelsId+ 10)  Total from Carbon_automation_test group by  " +
    "channelsId order by Total"
  ) {
    checkAnswer(
      sql(
        "select channelsId, sum(channelsId+ 10)  Total from Carbon_automation_test group by  " +
        "channelsId order by Total"
      ),
      sql(
        "select channelsId, sum(channelsId+ 10)  Total from Carbon_automation_hive group by  " +
        "channelsId order by Total"
      )

    )

  }
  test(
    "select channelsId, avg(Latest_MONTH+ 10) as a from Carbon_automation_test group by  channelsId"

  ) {
    checkAnswer(
      sql(
        "select channelsId, avg(Latest_MONTH+ 10) as a from Carbon_automation_test group by  " +
        "channelsId"
      ),
      sql(
        "select channelsId, avg(Latest_MONTH+ 10) as a from Carbon_automation_hive group by  " +
        "channelsId"
      )
    )

  }


  //Test-31
  test(

    "select channelsId, count(Latest_DAY+ 10) as a  from Carbon_automation_test group by  " +
    "channelsId"
  ) {
    checkAnswer(
      sql(
        "select channelsId, count(Latest_DAY+ 10) as a  from Carbon_automation_test group by  " +
        "channelsId"
      ),
      sql(
        "select channelsId, count(Latest_DAY+ 10) as a  from Carbon_automation_hive group by  " +
        "channelsId"
      )
    )
  }


  //Test-33
  test(
    "select channelsId, count(channelsId+ 10) Total from Carbon_automation_test group by  " +
    "channelsId order by channelsId"
  ) {
    checkAnswer(
      sql(
        "select channelsId, count(channelsId+ 10) Total from Carbon_automation_test group by  " +
        "channelsId order by channelsId"
      ),
      sql(
        "select channelsId, count(channelsId+ 10) Total from Carbon_automation_hive group by  " +
        "channelsId order by channelsId"
      )
    )

  }



  //Test-35
  test(
    "select channelsId, min(Latest_DAY+ 10) as a  from Carbon_automation_test group by  channelsId"
  ) {
    checkAnswer(
      sql(
        "select channelsId, min(Latest_DAY+ 10) as a  from Carbon_automation_test group by  " +
        "channelsId"
      ),
      sql(
        "select channelsId, min(Latest_DAY+ 10) as a  from Carbon_automation_hive group by  " +
        "channelsId"
      )
    )

  }


  //Test-37
  test(
    "select channelsId, min(channelsId+ 10) Total from Carbon_automation_test group by  " +
    "channelsId order by Total"
  ) {
    checkAnswer(
      sql(
        "select channelsId, min(channelsId+ 10) Total from Carbon_automation_test group by  " +
        "channelsId order by Total"
      ),
      sql(
        "select channelsId, min(channelsId+ 10) Total from Carbon_automation_hive group by  " +
        "channelsId order by Total"
      )
    )

  }

  //Test-39
  test(
    "select channelsId, max(Latest_DAY+ 10) as a  from Carbon_automation_test group by  channelsId"
  ) {
    checkAnswer(
      sql(
        "select channelsId, max(Latest_DAY+ 10) as a  from Carbon_automation_test group by  " +
        "channelsId"
      ),
      sql(
        "select channelsId, max(Latest_DAY+ 10) as a  from Carbon_automation_hive group by  " +
        "channelsId"
      )
    )

  }



  //Test-43
  test(
    "select Latest_YEAR ,sum(distinct Latest_YEAR)+10 from Carbon_automation_test group by " +
    "Latest_YEAR"
  )({

    checkAnswer(
      sql(
        "select Latest_YEAR ,sum(distinct Latest_YEAR)+10 from Carbon_automation_test group by " +
        "Latest_YEAR"
      ), sql(
        "select Latest_YEAR ,sum(distinct Latest_YEAR)+10 from Carbon_automation_hive group by " +
        "Latest_YEAR"
      )
    )
  }
  )


  //Test-47
  test("select sum(gamepointid) +10 as a ,series  from Carbon_automation_test group by series")({

    checkAnswer(
      sql("select sum(gamepointid) +10 as a ,series  from Carbon_automation_test group by series"),
      sql("select sum(gamepointid) +10 as a ,series  from Carbon_automation_hive group by series")

    )
  }
  )

  //Test-50
  test("select sum(gamepointid) +10.36 as a ,series  from Carbon_automation_test group by series")({

    checkAnswer(
      sql("select sum(gamepointid) +10.36 as a ,series  from Carbon_automation_test group by " +
          "series"),
      sql("select sum(gamepointid) +10.36 as a ,series  from Carbon_automation_hive group by " +
          "series")
    )
  }
  )

  //TC_057
  test("select count(latest_year)+10.364 as a,series  from Carbon_automation_test group by series")(
  {
    checkAnswer(
      sql(
        "select count(latest_year)+10.364 as a,series  from Carbon_automation_test group by series"
      ),
      sql(
        "select count(latest_year)+10.364 as a,series  from Carbon_automation_hive group by series"
      )
    )
  }
  )

  //TC_058
  test("select count(distinct series)+10 as a,series from Carbon_automation_test group by series")({
    checkAnswer(
      sql("select count(distinct series)+10 as a,series from Carbon_automation_test group by " +
          "series"),
      sql("select count(distinct series)+10 as a,series from Carbon_automation_hive group by " +
          "series")
    )
  }
  )
  //TC_060
  test("select count(*) as a  from Carbon_automation_test")({
    checkAnswer(
      sql("select count(*) as a  from Carbon_automation_test"),
      sql("select count(*) as a  from Carbon_automation_hive")
    )
  }
  )

  //TC_061
  test("Select count(1) as a  from Carbon_automation_test")({
    checkAnswer(
      sql("Select count(1) as a  from Carbon_automation_test"),
      sql("Select count(1) as a  from Carbon_automation_hive")
    )
  }
  )


  //TC_064
  test("select count(DISTINCT imei) as a  from Carbon_automation_test")({
    checkAnswer(
      sql("select count(DISTINCT imei) as a  from Carbon_automation_test"),
      sql("select count(DISTINCT imei) as a  from Carbon_automation_hive")
    )
  }
  )



  //TC_067
  test(
    "select count (if(deviceInformationId>100,NULL,deviceInformationId))  a from " +
    "Carbon_automation_test"
  )({
    checkAnswer(
      sql(
        "select count (if(deviceInformationId>100,NULL,deviceInformationId))  a from " +
        "Carbon_automation_test"
      ),
      sql(
        "select count (if(deviceInformationId>100,NULL,deviceInformationId))  a from " +
        "Carbon_automation_hive"
      )
    )
  }
  )
  //TC_069
  test("select count(gamePointId)  as a from Carbon_automation_test")({
    checkAnswer(
      sql("select count(gamePointId)  as a from Carbon_automation_test"),
      sql("select count(gamePointId)  as a from Carbon_automation_hive")
    )
  }
  )
  //TC_071
  test("select sum(gamePointId) a  from Carbon_automation_test")({
    checkAnswer(
      sql("select sum(gamePointId) a  from Carbon_automation_test"),
      sql("select sum(gamePointId) a  from Carbon_automation_hive")
    )
  }
  )

  //TC_080
  test(
    "select sum (if(deviceInformationId>100,NULL,deviceInformationId))  a from " +
    "Carbon_automation_test"
  )({
    checkAnswer(
      sql(
        "select sum (if(deviceInformationId>100,NULL,deviceInformationId))  a from " +
        "Carbon_automation_test"
      ),
      sql(
        "select sum (if(deviceInformationId>100,NULL,deviceInformationId))  a from " +
        "Carbon_automation_hive"
      ))
  }
  )

  //TC_081
  test("select sum( DISTINCT Latest_MONTH)  a from Carbon_automation_test")({
    checkAnswer(
      sql("select sum( DISTINCT Latest_MONTH)  a from Carbon_automation_test"),
      sql("select sum( DISTINCT Latest_MONTH)  a from Carbon_automation_hive")

    )
  }
  )

  //TC_088
  test(
    "select avg (if(deviceInformationId>100,NULL,deviceInformationId))  a from " +
    "Carbon_automation_test"
  )({
    checkAnswer(
      sql(
        "select avg (if(deviceInformationId>100,NULL,deviceInformationId))  a from " +
        "Carbon_automation_test"
      ),
      sql(
        "select avg (if(deviceInformationId>100,NULL,deviceInformationId))  a from " +
        "Carbon_automation_hive"
      )
    )
  }
  )
  //TC_090
  test("select min(deviceInformationId) a  from Carbon_automation_test")({
    checkAnswer(
      sql("select min(deviceInformationId) a  from Carbon_automation_test"),
      sql("select min(deviceInformationId) a  from Carbon_automation_hive")
    )
  }
  )



  //TC_095
  test(
    "select min (if(deviceInformationId>100,NULL,deviceInformationId))  a from " +
    "Carbon_automation_test"
  )({
    checkAnswer(
      sql(
        "select min (if(deviceInformationId>100,NULL,deviceInformationId))  a from " +
        "Carbon_automation_test"
      ),
      sql(
        "select min (if(deviceInformationId>100,NULL,deviceInformationId))  a from " +
        "Carbon_automation_hive"
      )
    )
  }
  )
  //TC_097
  test("select max(deviceInformationId) a  from Carbon_automation_test")({
    checkAnswer(
      sql("select max(deviceInformationId) a  from Carbon_automation_test"),
      sql("select max(deviceInformationId) a  from Carbon_automation_hive")
    )
  }
  )


  //TC_102
  test(
    "select max (if(deviceInformationId>100,NULL,deviceInformationId))  a from " +
    "Carbon_automation_test"
  )({
    checkAnswer(
      sql(
        "select max (if(deviceInformationId>100,NULL,deviceInformationId))  a from " +
        "Carbon_automation_test"
      ),
      sql(
        "select max (if(deviceInformationId>100,NULL,deviceInformationId))  a from " +
        "Carbon_automation_hive"
      )
    )
  }
  )

  //TC_103
  test("select variance(deviceInformationId) as a   from Carbon_automation_test")({
    checkAnswer(
      sql("select variance(deviceInformationId) as a   from Carbon_automation_test"),
      sql("select variance(deviceInformationId) as a   from Carbon_automation_hive")
    )
  }
  )
  //TC_105
  test("select var_samp(deviceInformationId) as a  from Carbon_automation_test")({
    checkAnswer(
      sql("select var_samp(deviceInformationId) as a  from Carbon_automation_test"),
      sql("select var_samp(deviceInformationId) as a  from Carbon_automation_hive")
    )
  }
  )

  //TC_106
  test("select stddev_pop(deviceInformationId) as a  from Carbon_automation_test")({
    checkAnswer(
      sql("select stddev_pop(deviceInformationId) as a  from Carbon_automation_test"),
      sql("select stddev_pop(deviceInformationId) as a  from Carbon_automation_hive")
    )
  }
  )

  //TC_107
  test("select stddev_samp(deviceInformationId)  as a from Carbon_automation_test")({
    checkAnswer(
      sql("select stddev_samp(deviceInformationId)  as a from Carbon_automation_test"),
      sql("select stddev_samp(deviceInformationId)  as a from Carbon_automation_hive")
    )
  }
  )

  //TC_108
  test("select covar_pop(deviceInformationId,deviceInformationId) as a  from " +
       "Carbon_automation_test")(
  {
    checkAnswer(
      sql(
        "select covar_pop(deviceInformationId,deviceInformationId) as a  from " +
        "Carbon_automation_test"
      ),
      sql(
        "select covar_pop(deviceInformationId,deviceInformationId) as a  from " +
        "Carbon_automation_hive"
      )
    )
  }
  )

  //TC_109
  test(
    "select covar_samp(deviceInformationId,deviceInformationId) as a  from Carbon_automation_test"
  )({
    checkAnswer(
      sql(
        "select covar_samp(deviceInformationId,deviceInformationId) as a  from " +
        "Carbon_automation_test"
      ),
      sql(
        "select covar_samp(deviceInformationId,deviceInformationId) as a  from " +
        "Carbon_automation_hive"
      )
    )
  }
  )

  //TC_110
  test("select corr(deviceInformationId,deviceInformationId)  as a from Carbon_automation_test")({
    checkAnswer(
      sql("select corr(deviceInformationId,deviceInformationId)  as a from Carbon_automation_test"),
      sql("select corr(deviceInformationId,deviceInformationId)  as a from Carbon_automation_hive")
    )
  }
  )

  //TC_111
  test("select percentile(deviceInformationId,0.2) as  a  from Carbon_automation_test")({
    checkAnswer(
      sql("select percentile(deviceInformationId,0.2) as  a  from Carbon_automation_test"),
      sql("select percentile(deviceInformationId,0.2) as  a  from Carbon_automation_hive")
    )
  }
  )

  //TC_113
  test("select percentile_approx(deviceInformationId,0.2) as a  from Carbon_automation_test")({
    checkAnswer(
      sql("select percentile_approx(deviceInformationId,0.2) as a  from Carbon_automation_test"),
      sql("select percentile_approx(deviceInformationId,0.2) as a  from Carbon_automation_hive")
    )
  }
  )

  //TC_134
  test("select last(imei) a from Carbon_automation_test")({
    checkAnswer(
      sql("select last(imei) a from Carbon_automation_test"),
      sql("select last(imei) a from Carbon_automation_hive")
    )
  }
  )
  //TC_136
  test("select series,count(imei) a from Carbon_automation_test group by series order by series")({
    checkAnswer(
      sql("select series,count(imei) a from Carbon_automation_test group by series order by " +
          "series"),
      sql("select series,count(imei) a from Carbon_automation_hive group by series order by " +
          "series")
    )
  }
  )

  //TC_138
  test(
    "select series,ActiveProvince,count(imei)  a from Carbon_automation_test group by " +
    "ActiveProvince,series order by series,ActiveProvince"
  )({
    checkAnswer(
      sql(
        "select series,ActiveProvince,count(imei)  a from Carbon_automation_test group by " +
        "ActiveProvince,series order by series,ActiveProvince"
      ),
      sql(
        "select series,ActiveProvince,count(imei)  a from Carbon_automation_hive group by " +
        "ActiveProvince,series order by series,ActiveProvince"
      )
    )
  }
  )

  //TC_162
  test(
    "select imei,series from Carbon_automation_test where Carbon_automation_test.series IN " +
    "('1Series','7Series')"
  )({
    checkAnswer(
      sql(
        "select imei,series from Carbon_automation_test where Carbon_automation_test.series IN " +
        "('1Series','7Series')"
      ),
      sql(
        "select imei,series from Carbon_automation_hive where Carbon_automation_hive.series IN " +
        "('1Series','7Series')"
      )
    )
  }
  )

  //TC_163
  test(
    "select imei,series from Carbon_automation_test where Carbon_automation_test.series  NOT IN " +
    "('1Series','7Series')"
  )({
    checkAnswer(
      sql(
        "select imei,series from Carbon_automation_test where Carbon_automation_test.series  NOT " +
        "IN ('1Series','7Series')"
      ),
      sql(
        "select imei,series from Carbon_automation_hive where Carbon_automation_hive.series  NOT " +
        "IN ('1Series','7Series')"
      )
    )
  }
  )

  //TC_166
  test("select Upper(series) a  from Carbon_automation_test")({
    checkAnswer(
      sql("select Upper(series) a  from Carbon_automation_test"),
      sql("select Upper(series) a  from Carbon_automation_hive")
    )
  }
  )

  //TC_168
  test("select imei,series from Carbon_automation_test limit 10")({
    checkAnswer(
      sql("select imei,series from Carbon_automation_test limit 10"),
      sql("select imei,series from Carbon_automation_hive limit 10")
    )
  }
  )

  //TC_171
  test("select Lower(series) a  from Carbon_automation_test")({
    checkAnswer(
      sql("select Lower(series) a  from Carbon_automation_test"),
      sql("select Lower(series) a  from Carbon_automation_hive")
    )
  }
  )

  //TC_173
  test("select distinct  Latest_DAY from Carbon_automation_test")({
    checkAnswer(
      sql("select distinct  Latest_DAY from Carbon_automation_test"),
      sql("select distinct  Latest_DAY from Carbon_automation_hive")
    )
  }
  )

  //TC_177
  test("select distinct count(series) as a  from Carbon_automation_test group by channelsName")({
    checkAnswer(
      sql("select distinct count(series) as a  from Carbon_automation_test group by channelsName"),
      sql("select distinct count(series) as a  from Carbon_automation_hive group by channelsName")
    )
  }
  )


  //TC_179
  test("select imei,series from Carbon_automation_test limit 101")({
    checkAnswer(
      sql("select imei,series from Carbon_automation_test limit 101"),
      sql("select imei,series from Carbon_automation_hive limit 101")
    )
  }
  )

  //TC_180
  test(
    "select series,sum(gamePointId) a from Carbon_automation_test group by series order by series" +
    " desc"
  )({
    checkAnswer(
      sql(
        "select series,sum(gamePointId) a from Carbon_automation_test group by series order by " +
        "series desc"
      ),
      sql(
        "select series,sum(gamePointId) a from Carbon_automation_hive group by series order by " +
        "series desc"
      )
    )
  }
  )


  //TC_182
  test(
    "select series,sum(gamePointId) a from Carbon_automation_test group by series order by series" +
    " desc ,a desc"
  )({
    checkAnswer(
      sql(
        "select series,sum(gamePointId) a from Carbon_automation_test group by series order by " +
        "series desc ,a desc"
      ),
      sql(
        "select series,sum(gamePointId) a from Carbon_automation_hive group by series order by " +
        "series desc ,a desc"
      ))
  }
  )

  //TC_183
  test(
    "select series,sum(gamePointId) a from Carbon_automation_test group by series order by series" +
    " asc"
  )({
    checkAnswer(
      sql(
        "select series,sum(gamePointId) a from Carbon_automation_test group by series order by " +
        "series asc"
      ),
      sql(
        "select series,sum(gamePointId) a from Carbon_automation_hive group by series order by " +
        "series asc"
      )
    )
  }
  )


  test(
    "select series,sum(gamePointId) a from Carbon_automation_test group by series order by series" +
    " asc ,a asc"
  )({
    checkAnswer(
      sql(
        "select series,sum(gamePointId) a from Carbon_automation_test group by series order by " +
        "series asc ,a asc"
      ),
      sql(
        "select series,sum(gamePointId) a from Carbon_automation_hive group by series order by " +
        "series asc ,a asc"
      )
    )
  }
  )

  //TC_208
  test("select Latest_DAY as a from Carbon_automation_test where Latest_DAY<=>Latest_areaId")({
    checkAnswer(
      sql("select Latest_DAY as a from Carbon_automation_test where Latest_DAY<=>Latest_areaId"),
      sql("select Latest_DAY as a from Carbon_automation_hive where Latest_DAY<=>Latest_areaId")
    )
  }
  )

  //TC_210
  test("select Latest_DAY  from Carbon_automation_test where Latest_DAY<>Latest_areaId")({
    checkAnswer(
      sql("select Latest_DAY  from Carbon_automation_test where Latest_DAY<>Latest_areaId"),
      sql("select Latest_DAY  from Carbon_automation_hive where Latest_DAY<>Latest_areaId")
    )
  }
  )

  //TC_211
  test("select Latest_DAY from Carbon_automation_test where Latest_DAY != Latest_areaId")({
    checkAnswer(
      sql("select Latest_DAY from Carbon_automation_test where Latest_DAY != Latest_areaId"),
      sql("select Latest_DAY from Carbon_automation_hive where Latest_DAY != Latest_areaId")
    )
  }
  )

  //TC_212
  test("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY<Latest_areaId")({
    checkAnswer(
      sql("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY<Latest_areaId"),
      sql("select imei, Latest_DAY from Carbon_automation_hive where Latest_DAY<Latest_areaId")
    )
  }
  )

  //TC_213
  test("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY<=Latest_areaId")({
    checkAnswer(
      sql("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY<=Latest_areaId"),
      sql("select imei, Latest_DAY from Carbon_automation_hive where Latest_DAY<=Latest_areaId")
    )
  }
  )

  //TC_215
  test("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY>=Latest_areaId")({
    checkAnswer(
      sql("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY>=Latest_areaId"),
      sql("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY>=Latest_areaId")
    )
  }
  )

  //TC_216
  test(
    "select imei, Latest_DAY from Carbon_automation_test where Latest_DAY NOT BETWEEN " +
    "Latest_areaId AND  Latest_HOUR"
  )({
    checkAnswer(
      sql(
        "select imei, Latest_DAY from Carbon_automation_test where Latest_DAY NOT BETWEEN " +
        "Latest_areaId AND  Latest_HOUR"
      ),
      sql(
        "select imei, Latest_DAY from Carbon_automation_hive where Latest_DAY NOT BETWEEN " +
        "Latest_areaId AND  Latest_HOUR"
      )
    )
  }
  )

  //TC_219
  test("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY IS NOT NULL")({
    checkAnswer(
      sql("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY IS NOT NULL"),
      sql("select imei, Latest_DAY from Carbon_automation_hive where Latest_DAY IS NOT NULL")
    )
  }
  )

  //TC_223
  test(
    "select * from (select if( Latest_areaId=7,7,NULL) as babu,Latest_MONTH from " +
    "Carbon_automation_test) qq where babu LIKE   Latest_MONTH"
  )({
    checkAnswer(
      sql(
        "select * from (select if( Latest_areaId=7,7,NULL) as babu,Latest_MONTH from " +
        "Carbon_automation_test) qq where babu LIKE   Latest_MONTH"
      ),
      sql(
        "select * from (select if( Latest_areaId=7,7,NULL) as babu,Latest_MONTH from " +
        "Carbon_automation_hive) qq where babu LIKE   Latest_MONTH"
      )
    )
  }
  )

  //TC_263
  test(
    "SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
    "Carbon_automation_test) SUB_QRY GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, " +
    "ActiveAreaId ASC"
  )({
    checkAnswer(
      sql(
        "SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
        "Carbon_automation_test) SUB_QRY GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, " +
        "ActiveAreaId ASC"
      ),
      sql(
        "SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
        "Carbon_automation_hive) SUB_QRY GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, " +
        "ActiveAreaId ASC"
      )
    )
  }
  )

  //TC_265
  test(
    "SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
    "Carbon_automation_test) SUB_QRY WHERE NOT(AMSize = \"\") GROUP BY AMSize, ActiveAreaId " +
    "ORDER BY AMSize ASC, ActiveAreaId ASC"
  )({
    checkAnswer(
      sql(
        "SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
        "Carbon_automation_test) SUB_QRY WHERE NOT(AMSize = \"\") GROUP BY AMSize, ActiveAreaId" +
        " ORDER BY AMSize ASC, ActiveAreaId ASC"
      ),
      sql(
        "SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
        "Carbon_automation_hive) SUB_QRY WHERE NOT(AMSize = \"\") GROUP BY AMSize, ActiveAreaId" +
        " ORDER BY AMSize ASC, ActiveAreaId ASC"
      )
    )
  }
  )

  //TC_274
  test(
    "SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamepointid) AS Sum_gamepointid FROM  " +
    "Carbon_automation_test group by ActiveCountry,ActiveDistrict,Activecity"
  )({
    checkAnswer(
      sql(
        "SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamepointid) AS Sum_gamepointid " +
        "FROM  Carbon_automation_test group by ActiveCountry,ActiveDistrict,Activecity"
      ),
      sql(
        "SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamepointid) AS Sum_gamepointid " +
        "FROM  Carbon_automation_hive group by ActiveCountry,ActiveDistrict,Activecity"
      )
    )
  }
  )

  //TC_275
  test(
    "SELECT Latest_country, Latest_city, Latest_district, SUM(gamepointid) AS Sum_gamepointid " +
    "FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY Latest_country, Latest_city, " +
    "Latest_district ORDER BY Latest_country ASC, Latest_city ASC, Latest_district ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Latest_country, Latest_city, Latest_district, SUM(gamepointid) AS Sum_gamepointid" +
        " FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY Latest_country, " +
        "Latest_city, Latest_district ORDER BY Latest_country ASC, Latest_city ASC, " +
        "Latest_district ASC"
      ),
      sql(
        "SELECT Latest_country, Latest_city, Latest_district, SUM(gamepointid) AS Sum_gamepointid" +
        " FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY Latest_country, " +
        "Latest_city, Latest_district ORDER BY Latest_country ASC, Latest_city ASC, " +
        "Latest_district ASC"
      )
    )
  }
  )

  //TC_276
  test(
    "SELECT Activecity, ActiveCountry, ActiveDistrict, COUNT(imei) AS Count_imei FROM (select * " +
    "from Carbon_automation_test) SUB_QRY GROUP BY Activecity, ActiveCountry, ActiveDistrict " +
    "ORDER BY Activecity ASC, ActiveCountry ASC, ActiveDistrict ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Activecity, ActiveCountry, ActiveDistrict, COUNT(imei) AS Count_imei FROM (select" +
        " * from Carbon_automation_test) SUB_QRY GROUP BY Activecity, ActiveCountry, " +
        "ActiveDistrict ORDER BY Activecity ASC, ActiveCountry ASC, ActiveDistrict ASC"
      ),
      sql(
        "SELECT Activecity, ActiveCountry, ActiveDistrict, COUNT(imei) AS Count_imei FROM (select" +
        " * from Carbon_automation_hive) SUB_QRY GROUP BY Activecity, ActiveCountry, " +
        "ActiveDistrict ORDER BY Activecity ASC, ActiveCountry ASC, ActiveDistrict ASC"
      )
    )
  }
  )

  //TC_279
  test(
    "SELECT ActiveCountry, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from " +
    "Carbon_automation_test) SUB_QRY GROUP BY ActiveCountry ORDER BY ActiveCountry ASC"
  )({
    checkAnswer(
      sql(
        "SELECT ActiveCountry, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from " +
        "Carbon_automation_test) SUB_QRY GROUP BY ActiveCountry ORDER BY ActiveCountry ASC"
      ),
      sql(
        "SELECT ActiveCountry, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from " +
        "Carbon_automation_hive) SUB_QRY GROUP BY ActiveCountry ORDER BY ActiveCountry ASC"
      )
    )
  }
  )

  //TC_282
  test(
    "SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamepointid) AS Sum_gamepointid FROM " +
    "(select * from Carbon_automation_test) SUB_QRY GROUP BY ActiveCountry, ActiveDistrict, " +
    "Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamepointid) AS Sum_gamepointid " +
        "FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY ActiveCountry, " +
        "ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity " +
        "ASC"
      ),
      sql(
        "SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamepointid) AS Sum_gamepointid " +
        "FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY ActiveCountry, " +
        "ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity " +
        "ASC"
      )
    )
  }
  )

  //TC_317
  test("select channelsId from Carbon_automation_test order by  channelsId")({
    checkAnswer(
      sql("select channelsId from Carbon_automation_test order by  channelsId"),
      sql("select channelsId from Carbon_automation_hive order by  channelsId")
    )
  }
  )

  //TC_318
  test(
    "select count(series),series from Carbon_automation_test group by series having " +
    "series='6Series'"
  )({
    checkAnswer(
      sql(
        "select count(series),series from Carbon_automation_test group by series having " +
        "series='6Series'"
      ),
      sql(
        "select count(series),series from Carbon_automation_hive group by series having " +
        "series='6Series'"
      )
    )
  }
  )

  //TC_319
  test(
    "SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId FROM " +
    "(select * from Carbon_automation_test) SUB_QRY GROUP BY ActiveCountry, ActiveDistrict, " +
    "Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId " +
        "FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY ActiveCountry, " +
        "ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity " +
        "ASC"
      ),
      sql(
        "SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId " +
        "FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY ActiveCountry, " +
        "ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity " +
        "ASC"
      )
    )
  }
  )

  //TC_321
  test(
    "SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId FROM " +
    "(select * from Carbon_automation_test) SUB_QRY WHERE imei = \"1AA100000\" GROUP BY " +
    "ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, " +
    "Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId " +
        "FROM (select * from Carbon_automation_test) SUB_QRY WHERE imei = \"1AA100000\" GROUP " +
        "BY ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, " +
        "ActiveDistrict ASC, Activecity ASC"
      ),
      sql(
        "SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId " +
        "FROM (select * from Carbon_automation_hive) SUB_QRY WHERE imei = \"1AA100000\" GROUP " +
        "BY ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, " +
        "ActiveDistrict ASC, Activecity ASC"
      )
    )
  }
  )

  //TC_384
  test(
    "SELECT series, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
    "Carbon_automation_test) SUB_QRY GROUP BY series ORDER BY series ASC"
  )({
    checkAnswer(
      sql(
        "SELECT series, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
        "Carbon_automation_test) SUB_QRY GROUP BY series ORDER BY series ASC"
      ),
      sql(
        "SELECT series, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
        "Carbon_automation_hive) SUB_QRY GROUP BY series ORDER BY series ASC"
      )
    )
  }
  )

  //TC_386
  test(
    "SELECT channelsId, deliveryCity FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY" +
    " channelsId, deliveryCity ORDER BY channelsId ASC, deliveryCity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT channelsId, deliveryCity FROM (select * from Carbon_automation_test) SUB_QRY " +
        "GROUP BY channelsId, deliveryCity ORDER BY channelsId ASC, deliveryCity ASC"
      ),
      sql(
        "SELECT channelsId, deliveryCity FROM (select * from Carbon_automation_hive) SUB_QRY " +
        "GROUP BY channelsId, deliveryCity ORDER BY channelsId ASC, deliveryCity ASC"
      )
    )
  }
  )

  //TC_419
  test("select  count(channelsId) from Carbon_automation_test where  modelId is  null")({
    checkAnswer(
      sql("select  count(channelsId) from Carbon_automation_test where  modelId is  null"),
      sql("select  count(channelsId) from Carbon_automation_hive where  modelId is  null")
    )
  }
  )

  //TC_421
  test("select  avg(channelsName) from Carbon_automation_test where  modelId is  null")({
    checkAnswer(
      sql("select  avg(channelsName) from Carbon_automation_test where  modelId is  null"),
      sql("select  avg(channelsName) from Carbon_automation_hive where  modelId is  null")
    )
  }
  )

  //TC_424
  test("SELECT count(DISTINCT gamePointId) FROM Carbon_automation_test where imei is null")({
    checkAnswer(
      sql("SELECT count(DISTINCT gamePointId) FROM Carbon_automation_test where imei is null"),
      sql("SELECT count(DISTINCT gamePointId) FROM Carbon_automation_hive where imei is null")
    )
  }
  )

  //TC_439
  test("SELECT min(AMSize) FROM Carbon_automation_test where imei is NOT null")({
    checkAnswer(
      sql("SELECT min(AMSize) FROM Carbon_automation_test where imei is NOT null"),
      sql("SELECT min(AMSize) FROM Carbon_automation_hive where imei is NOT null")
    )
  }
  )

  //TC_448
  test("select var_samp(Latest_YEAR) from Carbon_automation_test")({
    checkAnswer(
      sql("select var_samp(Latest_YEAR) from Carbon_automation_test"),
      sql("select var_samp(Latest_YEAR) from Carbon_automation_hive")
    )
  }
  )

  //TC_451
  test("select stddev_pop(bomcode)from Carbon_automation_test")({
    checkAnswer(
      sql("select stddev_pop(bomcode)from Carbon_automation_test"),
      sql("select stddev_pop(bomcode)from Carbon_automation_hive"
      ))
  }
  )

  //TC_457
  test("select stddev_samp(deviceInformationId)from Carbon_automation_test1")({
    checkAnswer(
      sql("select stddev_samp(deviceInformationId)from Carbon_automation_test"),
      sql("select stddev_samp(deviceInformationId)from Carbon_automation_hive")
    )
  }
  )


  //TC_472
  test("Select percentile(1,1.0) from Carbon_automation_test2")({
    checkAnswer(
      sql("Select percentile(1,1.0) from Carbon_automation_test"),
      sql("Select percentile(1,1.0) from Carbon_automation_hive")
    )
  }
  )
  //TC_474
  test("select cast(series as int) as a from Carbon_automation_test limit 10")({
    checkAnswer(
      sql("select cast(series as int) as a from Carbon_automation_test limit 10"),
      sql("select cast(series as int) as a from Carbon_automation_hive limit 10")
    )
  }
  )

  //TC_481
  test("select percentile_approx(1, 0.5 ,5000) from Carbon_automation_test")({
    checkAnswer(
      sql("select percentile_approx(1, 0.5 ,5000) from Carbon_automation_test"),
      sql("select percentile_approx(1, 0.5 ,5000) from Carbon_automation_hive")
    )
  }
  )
}