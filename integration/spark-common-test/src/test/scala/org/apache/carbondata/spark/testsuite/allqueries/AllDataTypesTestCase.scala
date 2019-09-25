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

package org.apache.carbondata.spark.testsuite.allqueries

import org.apache.spark.sql.{Row, SaveMode}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

/**
  * Test Class for all query on multiple datatypes
  *
  */
class AllDataTypesTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    clean

    sql("create table if not exists Carbon_automation_test (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string, gamePointId int,contractNumber int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='Latest_MONTH,Latest_DAY,deviceInformationId')")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/100_olap.csv' INTO table Carbon_automation_test options('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""")

    //hive table
    sql("create table if not exists Carbon_automation_test_hive (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string,contractNumber int, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointId int,gamePointDescription string)row format delimited fields terminated by ','")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/100_olap.csv' INTO table Carbon_automation_test_hive""")

  }

  def clean {
    sql("drop table if exists carbonunion")
    sql("drop table if exists Carbon_automation_test")
    sql("drop table if exists Carbon_automation_test_hive")
  }

  override def afterAll {
    clean
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }

  //Test-22
  test("select channelsId, sum(Latest_DAY+ 10) as a from Carbon_automation_test group by  channelsId") {
    checkAnswer(
      sql("select channelsId, sum(Latest_DAY+ 10) as a from Carbon_automation_test group by  channelsId"),
      Seq(Row("1", 132), Row("2", 110), Row("3", 176), Row("4", 132), Row("5", 132), Row("6", 209), Row("7", 198)))

  }

  test("select channelsId, Latest_DAY from Carbon_automation_test where count(channelsId) = 1") {
    try {
      sql("select channelsId, Latest_DAY from Carbon_automation_test where count(channelsId) = 1").collect
    } catch {
      case ce: UnsupportedOperationException => ce.getMessage
      case ce: Exception => ce.getMessage
    }
  }

  //Test-24
  test("select channelsId, sum(channelsId+ 10)  Total from Carbon_automation_test group by  channelsId order by Total") {
    checkAnswer(
      sql("select channelsId, sum(channelsId+ 10)  Total from Carbon_automation_test group by  channelsId order by Total"),
      Seq(Row("2", 120), Row("1", 132), Row("4", 168), Row("5", 180), Row("3", 208), Row("6", 304), Row("7", 306)))

  }

  //Test-25
  test("select channelsId, sum(channelsId+channelsId) Total from Carbon_automation_test group by  channelsId order by Total") {
    checkAnswer(
      sql("select channelsId, sum(channelsId+channelsId) Total from Carbon_automation_test group by  channelsId order by Total"),
      Seq(Row("1", 24), Row("2", 40), Row("3", 96), Row("4", 96), Row("5", 120), Row("6", 228), Row("7", 252)))

  }

  //Test-26
  test("select channelsId, sum(channelsId+channelsId) Total from Carbon_automation_test group by  channelsId") {
    checkAnswer(
      sql("select channelsId, sum(channelsId+channelsId) Total from Carbon_automation_test group by  channelsId"),
      Seq(Row("1", 24), Row("2", 40), Row("3", 96), Row("4", 96), Row("5", 120), Row("6", 228), Row("7", 252)))

  }

  //Test-27
  test("select channelsId, avg(Latest_DAY+ 10) as a from Carbon_automation_test group by  channelsId") {
    checkAnswer(
      sql("select channelsId, avg(Latest_DAY+ 10) as a from Carbon_automation_test group by  channelsId"),
      Seq(Row("1", 11), Row("2", 11), Row("3", 11), Row("4", 11), Row("5", 11), Row("6", 11), Row("7", 11)))

  }

  //Test-29
  test("select channelsId, avg(channelsId+ 10)  Total from Carbon_automation_test group by  channelsId order by Total") {
    checkAnswer(
      sql("select channelsId, avg(channelsId+ 10)  Total from Carbon_automation_test group by  channelsId order by Total"),
      Seq(Row("1", 11), Row("2", 12), Row("3", 13), Row("4", 14), Row("5", 15), Row("6", 16), Row("7", 17)))

  }


  //Test-30
  test("select channelsId, avg(channelsId+channelsId) Total from Carbon_automation_test group by  channelsId order by Total") {
    checkAnswer(
      sql("select channelsId, avg(channelsId+channelsId) Total from Carbon_automation_test group by  channelsId order by Total"),
      Seq(Row("1", 2), Row("2", 4), Row("3", 6), Row("4", 8), Row("5", 10), Row("6", 12), Row("7", 14)))

  }

  //Test-31
  test("select channelsId, count(Latest_DAY+ 10) as a  from Carbon_automation_test group by  channelsId") {
    checkAnswer(
      sql("select channelsId, count(Latest_DAY+ 10) as a  from Carbon_automation_test group by  channelsId"),
      Seq(Row("1", 12), Row("2", 10), Row("3", 16), Row("4", 12), Row("5", 12), Row("6", 19), Row("7", 18)))

  }


  //Test-33
  test("select channelsId, count(channelsId+ 10) Total from Carbon_automation_test group by  channelsId order by channelsId") {
    checkAnswer(
      sql("select channelsId, count(channelsId+ 10) Total from Carbon_automation_test group by  channelsId order by channelsId"),
      Seq(Row("1", 12), Row("2", 10), Row("3", 16), Row("4", 12), Row("5", 12), Row("6", 19), Row("7", 18)))

  }

  //Test-34
  test("select channelsId, count(channelsId+channelsId)  Total from Carbon_automation_test group by  channelsId order by channelsId") {
    checkAnswer(
      sql("select channelsId, count(channelsId+channelsId)  Total from Carbon_automation_test group by  channelsId order by channelsId"),
      Seq(Row("1", 12), Row("2", 10), Row("3", 16), Row("4", 12), Row("5", 12), Row("6", 19), Row("7", 18)))

  }

  //Test-35
  test("select channelsId, min(Latest_DAY+ 10) as a  from Carbon_automation_test group by  channelsId") {
    checkAnswer(
      sql("select channelsId, min(Latest_DAY+ 10) as a  from Carbon_automation_test group by  channelsId"),
      Seq(Row("1", 11), Row("2", 11), Row("3", 11), Row("4", 11), Row("5", 11), Row("6", 11), Row("7", 11)))

  }


  //Test-37
  test("select channelsId, min(channelsId+ 10) Total from Carbon_automation_test group by  channelsId order by Total") {
    checkAnswer(
      sql("select channelsId, min(channelsId+ 10) Total from Carbon_automation_test group by  channelsId order by Total"),
      Seq(Row("1", 11), Row("2", 12), Row("3", 13), Row("4", 14), Row("5", 15), Row("6", 16), Row("7", 17)))

  }

  //Test-38
  test("select channelsId, min(channelsId+channelsId)  Total from Carbon_automation_test group by  channelsId order by Total") {
    checkAnswer(
      sql("select channelsId, min(channelsId+channelsId)  Total from Carbon_automation_test group by  channelsId order by Total"),
      Seq(Row("1", 2), Row("2", 4), Row("3", 6), Row("4", 8), Row("5", 10), Row("6", 12), Row("7", 14)))

  }

  //Test-39
  test("select channelsId, max(Latest_DAY+ 10) as a  from Carbon_automation_test group by  channelsId") {
    checkAnswer(
      sql("select channelsId, max(Latest_DAY+ 10) as a  from Carbon_automation_test group by  channelsId"),
      Seq(Row("1", 11), Row("2", 11), Row("3", 11), Row("4", 11), Row("5", 11), Row("6", 11), Row("7", 11)))

  }


  //Test-41
  test("select channelsId, max(channelsId+ 10) Total from Carbon_automation_test group by  channelsId order by Total")({

    checkAnswer(
      sql("select channelsId, max(channelsId+ 10) Total from Carbon_automation_test group by  channelsId order by Total"),
      Seq(Row("1", 11), Row("2", 12), Row("3", 13), Row("4", 14), Row("5", 15), Row("6", 16), Row("7", 17)))
  })

  //Test-42
  test("select channelsId, max(channelsId+channelsId)  Total from Carbon_automation_test group by  channelsId order by Total")({

    checkAnswer(
      sql("select channelsId, max(channelsId+channelsId)  Total from Carbon_automation_test group by  channelsId order by Total"),
      Seq(Row("1", 2), Row("2", 4), Row("3", 6), Row("4", 8), Row("5", 10), Row("6", 12), Row("7", 14)))
  })

  //Test-43
  test("select Latest_YEAR ,sum(distinct Latest_YEAR)+10 from Carbon_automation_test group by Latest_YEAR")({

    checkAnswer(
      sql("select Latest_YEAR ,sum(distinct Latest_YEAR)+10 from Carbon_automation_test group by Latest_YEAR"),
      Seq(Row(2015, 2025)))
  })

  //Test-44

  test("select Latest_YEAR ,sum(distinct Latest_YEAR)+10 from Carbon_automation_test group by Latest_YEAR.")({

    checkAnswer(
      sql("select Latest_YEAR ,sum(distinct Latest_YEAR)+10 from Carbon_automation_test group by Latest_YEAR"),
      Seq(Row(2015, 2025)))
  })

    //Test-47
  test("select sum(gamepointid) +10 as a ,series  from Carbon_automation_test group by series")({

    checkAnswer(
      sql("select sum(gamepointid) +10 as a ,series  from Carbon_automation_test group by series"),
      sql("select sum(gamepointid) +10 as a ,series  from Carbon_automation_test_hive group by series"))
  })

  //Test-50
  test("select sum(gamepointid) +10.36 as a ,series  from Carbon_automation_test group by series")({

    checkAnswer(
      sql("select sum(gamepointid) +10.36 as a ,series  from Carbon_automation_test group by series"),
      sql("select sum(gamepointid) +10.36 as a ,series  from Carbon_automation_test_hive group by series"))
  })

  //TC_055
  test("select count(deviceinformationid)+10.32 as a ,series  from Carbon_automation_test group by series")({
    checkAnswer(
      sql("select count(deviceinformationid)+10.32 as a ,series  from Carbon_automation_test group by series"),
      Seq(Row(19.32, "6Series"), Row(25.32, "0Series"), Row(18.32, "4Series"), Row(21.32, "8Series"), Row(21.32, "7Series"), Row(13.32, "1Series"), Row(27.32, "5Series"), Row(18.32, "9Series"), Row(18.32, "3Series"), Row(19.32, "2Series")))
  })

  //TC_056
  test("select count(gamepointid) +10.36 as a ,series  from Carbon_automation_test group by series")({
    checkAnswer(
      sql("select count(gamepointid) +10.36 as a ,series  from Carbon_automation_test group by series"),
      Seq(Row(19.36, "6Series"), Row(25.36, "0Series"), Row(18.36, "4Series"), Row(21.36, "8Series"), Row(21.36, "7Series"), Row(13.36, "1Series"), Row(27.36, "5Series"), Row(18.36, "9Series"), Row(18.36, "3Series"), Row(19.36, "2Series")))
  })

  //TC_057
  test("select count(latest_year)+10.364 as a,series  from Carbon_automation_test group by series")({
    checkAnswer(
      sql("select count(latest_year)+10.364 as a,series  from Carbon_automation_test group by series"),
      Seq(Row(19.364, "6Series"), Row(25.364, "0Series"), Row(18.364, "4Series"), Row(21.364, "8Series"), Row(21.364, "7Series"), Row(13.364, "1Series"), Row(27.364, "5Series"), Row(18.364, "9Series"), Row(18.364, "3Series"), Row(19.364, "2Series")))
  })

  //TC_058
  test("select count(distinct series)+10 as a,series from Carbon_automation_test group by series")({
    checkAnswer(
      sql("select count(distinct series)+10 as a,series from Carbon_automation_test group by series"),
      Seq(Row(11, "6Series"), Row(11, "0Series"), Row(11, "4Series"), Row(11, "8Series"), Row(11, "7Series"), Row(11, "1Series"), Row(11, "5Series"), Row(11, "9Series"), Row(11, "3Series"), Row(11, "2Series")))
  })
    //TC_060
  test("select count(*) as a  from Carbon_automation_test")({
    checkAnswer(
      sql("select count(*) as a  from Carbon_automation_test"),
      Seq(Row(99)))
  })

  //TC_061
  test("Select count(1) as a  from Carbon_automation_test")({
    checkAnswer(
      sql("Select count(1) as a  from Carbon_automation_test"),
      Seq(Row(99)))
  })

  //TC_062
  test("select count(imei) as a   from Carbon_automation_test")({
    checkAnswer(
      sql("select count(imei) as a   from Carbon_automation_test"),
      Seq(Row(99)))
  })

  //TC_063
  test("select count(device_backColor)  as a from Carbon_automation_test")({
    checkAnswer(
      sql("select count(device_backColor)  as a from Carbon_automation_test"),
      Seq(Row(99)))
  })

  //TC_064
  test("select count(DISTINCT imei) as a  from Carbon_automation_test")({
    checkAnswer(
      sql("select count(DISTINCT imei) as a  from Carbon_automation_test"),
      Seq(Row(99)))
  })

  //TC_065
  test("select count(DISTINCT series) as a from Carbon_automation_test")({
    checkAnswer(
      sql("select count(DISTINCT series) as a from Carbon_automation_test"),
      Seq(Row(10)))
  })

  //TC_066
  test("select count(DISTINCT  device_backColor)  as a from Carbon_automation_test")({
    checkAnswer(
      sql("select count(DISTINCT  device_backColor)  as a from Carbon_automation_test"),
      Seq(Row(10)))
  })

  //TC_067
  test("select count (if(deviceInformationId>100,NULL,deviceInformationId))  a from Carbon_automation_test")({
    checkAnswer(
      sql("select count (if(deviceInformationId>100,NULL,deviceInformationId))  a from Carbon_automation_test"),
      Seq(Row(3)))
  })
    //TC_069
  test("select count(gamePointId)  as a from Carbon_automation_test")({
    checkAnswer(
      sql("select count(gamePointId)  as a from Carbon_automation_test"),
      Seq(Row(99)))
  })
   //TC_071
  test("select sum(gamePointId) a  from Carbon_automation_test")({
    checkAnswer(
      sql("select sum(gamePointId) a  from Carbon_automation_test"),
      sql("select sum(gamePointId) a  from Carbon_automation_test_hive"))
  })
    //TC_077
  test("select sum(DISTINCT  deviceInformationId) a  from Carbon_automation_test")({
    checkAnswer(
      sql("select sum(DISTINCT  deviceInformationId) a  from Carbon_automation_test"),
      Seq(Row(9594717)))
  })
    //TC_080
  test("select sum (if(deviceInformationId>100,NULL,deviceInformationId))  a from Carbon_automation_test")({
    checkAnswer(
      sql("select sum (if(deviceInformationId>100,NULL,deviceInformationId))  a from Carbon_automation_test"),
      Seq(Row(111)))
  })

  //TC_081
  test("select sum( DISTINCT Latest_MONTH)  a from Carbon_automation_test")({
    checkAnswer(
      sql("select sum( DISTINCT Latest_MONTH)  a from Carbon_automation_test"),
      Seq(Row(7)))
  })

    //TC_088
  test("select avg (if(deviceInformationId>100,NULL,deviceInformationId))  a from Carbon_automation_test")({
    checkAnswer(
      sql("select avg (if(deviceInformationId>100,NULL,deviceInformationId))  a from Carbon_automation_test"),
      Seq(Row(37.0)))
  })
  //TC_090
  test("select min(deviceInformationId) a  from Carbon_automation_test")({
    checkAnswer(
      sql("select min(deviceInformationId) a  from Carbon_automation_test"),
      Seq(Row(1)))
  })

  //TC_091
  test("select min(channelsId) a  from Carbon_automation_test")({
    checkAnswer(
      sql("select min(channelsId) a  from Carbon_automation_test"),
      Seq(Row("1")))
  })

  //TC_092
  test("select min(bomCode)  a  from Carbon_automation_test")({
    checkAnswer(
      sql("select min(bomCode)  a  from Carbon_automation_test"),
      Seq(Row("1")))
  })

  //TC_093
  test("select min(Latest_MONTH)  a  from Carbon_automation_test")({
    checkAnswer(
      sql("select min(Latest_MONTH)  a  from Carbon_automation_test"),
      Seq(Row(7)))
  })

   //TC_095
  test("select min (if(deviceInformationId>100,NULL,deviceInformationId))  a from Carbon_automation_test")({
    checkAnswer(
      sql("select min (if(deviceInformationId>100,NULL,deviceInformationId))  a from Carbon_automation_test"),
      Seq(Row(1)))
  })
   //TC_097
  test("select max(deviceInformationId) a  from Carbon_automation_test")({
    checkAnswer(
      sql("select max(deviceInformationId) a  from Carbon_automation_test"),
      Seq(Row(1000000)))
  })

  //TC_098
  test("select max(channelsId) a  from Carbon_automation_test")({
    checkAnswer(
      sql("select max(channelsId) a  from Carbon_automation_test"),
      Seq(Row("7")))
  })

  //TC_099
  test("select max(bomCode)  a  from Carbon_automation_test")({
    checkAnswer(
      sql("select max(bomCode)  a  from Carbon_automation_test"),
      Seq(Row("100084")))
  })

  //TC_100
  test("select max(Latest_MONTH)  a  from Carbon_automation_test")({
    checkAnswer(
      sql("select max(Latest_MONTH)  a  from Carbon_automation_test"),
      Seq(Row(7)))
  })

  //TC_102
  test("select max (if(deviceInformationId>100,NULL,deviceInformationId))  a from Carbon_automation_test")({
    checkAnswer(
      sql("select max (if(deviceInformationId>100,NULL,deviceInformationId))  a from Carbon_automation_test"),
      Seq(Row(100)))
  })

  //TC_103
  test("select variance(deviceInformationId) as a   from carbon_automation_test")({
    checkAnswer(
      sql("select variance(deviceInformationId) as a   from carbon_automation_test"),
      sql("select variance(deviceInformationId) as a   from Carbon_automation_test_hive"))
  })
   //TC_105
  test("select var_samp(deviceInformationId) as a  from Carbon_automation_test")({
    checkAnswer(
      sql("select var_samp(deviceInformationId) as a  from Carbon_automation_test"),
      Seq(Row(9.405419800040813E9)))
  })

  //TC_106
  test("select stddev_pop(deviceInformationId) as a  from Carbon_automation_test")({
    checkAnswer(
      sql("select stddev_pop(deviceInformationId) as a  from Carbon_automation_test"),
      Seq(Row(96490.49465950707)))
  })

  //TC_107
  test("select stddev_samp(deviceInformationId)  as a from Carbon_automation_test")({
    checkAnswer(
      sql("select stddev_samp(deviceInformationId)  as a from Carbon_automation_test"),
      Seq(Row(96981.54360516652)))
  })

  //TC_108
  test("select covar_pop(deviceInformationId,deviceInformationId) as a  from Carbon_automation_test")({
    checkAnswer(
      sql("select covar_pop(deviceInformationId,deviceInformationId) as a  from Carbon_automation_test"),
      Seq(Row(9310415559.636362)))
  })

  //TC_109
  test("select covar_samp(deviceInformationId,deviceInformationId) as a  from Carbon_automation_test")({
    checkAnswer(
      sql("select covar_samp(deviceInformationId,deviceInformationId) as a  from Carbon_automation_test"),
      Seq(Row(9.405419800040813E9)))
  })

  //TC_110
  test("select corr(deviceInformationId,deviceInformationId)  as a from Carbon_automation_test")({
    checkAnswer(
      sql("select corr(deviceInformationId,deviceInformationId)  as a from Carbon_automation_test"),
      sql("select corr(deviceInformationId,deviceInformationId)  as a from Carbon_automation_test_hive"))
  })

  //TC_111
  test("select percentile(deviceInformationId,0.2) as  a  from Carbon_automation_test")({
    checkAnswer(
      sql("select percentile(deviceInformationId,0.2) as  a  from Carbon_automation_test"),
      Seq(Row(100006.6)))
  })

  //TC_113
//  test("select percentile_approx(deviceInformationId,0.2) as a  from Carbon_automation_test")({
//    checkAnswer(
//      sql("select percentile_approx(deviceInformationId,0.2) as a  from Carbon_automation_test"),
//      Seq(Row(100005.8)))
//  })
   //TC_127
  test("select percentile(deviceInformationId,0.2) as  a  from Carbon_automation_test1")({
    checkAnswer(
      sql("select percentile(deviceInformationId,0.2) as  a  from Carbon_automation_test"),
      Seq(Row(100006.6)))
  })
    //TC_134
  test("select last(imei) a from Carbon_automation_test")({
    checkAnswer(
      sql("select last(imei) a from Carbon_automation_test"),
      Seq(Row("1AA100084")))
  })
    //TC_136
  test("select series,count(imei) a from Carbon_automation_test group by series order by series")({
    checkAnswer(
      sql("select series,count(imei) a from Carbon_automation_test group by series order by series"),
      Seq(Row("0Series", 15), Row("1Series", 3), Row("2Series", 9), Row("3Series", 8), Row("4Series", 8), Row("5Series", 17), Row("6Series", 9), Row("7Series", 11), Row("8Series", 11), Row("9Series", 8)))
  })

  //TC_138
  test("select series,ActiveProvince,count(imei)  a from Carbon_automation_test group by ActiveProvince,series order by series,ActiveProvince")({
    checkAnswer(
      sql("select series,ActiveProvince,count(imei)  a from Carbon_automation_test group by ActiveProvince,series order by series,ActiveProvince"),
      Seq(Row("0Series", "Guangdong Province", 1), Row("0Series", "Hubei Province", 5), Row("0Series", "Hunan Province", 9), Row("1Series", "Guangdong Province", 2), Row("1Series", "Hunan Province", 1), Row("2Series", "Hubei Province", 3), Row("2Series", "Hunan Province", 6), Row("3Series", "Guangdong Province", 2), Row("3Series", "Hubei Province", 2), Row("3Series", "Hunan Province", 4), Row("4Series", "Guangdong Province", 1), Row("4Series", "Hubei Province", 1), Row("4Series", "Hunan Province", 6), Row("5Series", "Guangdong Province", 5), Row("5Series", "Hubei Province", 3), Row("5Series", "Hunan Province", 9), Row("6Series", "Guangdong Province", 1), Row("6Series", "Hubei Province", 4), Row("6Series", "Hunan Province", 4), Row("7Series", "Guangdong Province", 5), Row("7Series", "Hubei Province", 1), Row("7Series", "Hunan Province", 5), Row("8Series", "Guangdong Province", 2), Row("8Series", "Hubei Province", 6), Row("8Series", "Hunan Province", 3), Row("9Series", "Guangdong Province", 1), Row("9Series", "Hubei Province", 3), Row("9Series", "Hunan Province", 4)))
  })

  //TC_139
  test("select count(distinct deviceColor) a,deliveryProvince from Carbon_automation_test group by deliveryProvince")({
    checkAnswer(
      sql("select count(distinct deviceColor) a,deliveryProvince from Carbon_automation_test group by deliveryProvince"),
      Seq(Row(10, "Hunan Province"), Row(10, "Guangdong Province"), Row(10, "Hubei Province")))
  })

    //TC_141
  test("select series,sum(gamePointId) a from Carbon_automation_test group by series order by series")({
    checkAnswer(
      sql("select series,sum(gamePointId) a from Carbon_automation_test group by series order by series"),
      sql("select series,sum(gamePointId) a from Carbon_automation_test_hive group by series order by series"))
  })

   //TC_162
  test("select imei,series from Carbon_automation_test where Carbon_automation_test.series IN ('1Series','7Series')")({
    checkAnswer(
      sql("select imei,series from Carbon_automation_test where Carbon_automation_test.series IN ('1Series','7Series')"),
      Seq(Row("1AA1", "7Series"), Row("1AA10", "7Series"), Row("1AA10000", "7Series"), Row("1AA1000000", "7Series"), Row("1AA100005", "1Series"), Row("1AA100013", "1Series"), Row("1AA100026", "7Series"), Row("1AA10003", "7Series"), Row("1AA100030", "7Series"), Row("1AA100031", "7Series"), Row("1AA100032", "1Series"), Row("1AA100037", "7Series"), Row("1AA100054", "7Series"), Row("1AA100055", "7Series")))
  })

  //TC_163
  test("select imei,series from Carbon_automation_test where Carbon_automation_test.series  NOT IN ('1Series','7Series')")({
    checkAnswer(
      sql("select imei,series from Carbon_automation_test where Carbon_automation_test.series  NOT IN ('1Series','7Series')"),
      Seq(Row("1AA100", "5Series"), Row("1AA1000", "5Series"), Row("1AA100000", "9Series"), Row("1AA100001", "0Series"), Row("1AA100002", "0Series"), Row("1AA100003", "5Series"), Row("1AA100004", "4Series"), Row("1AA100006", "6Series"), Row("1AA100007", "9Series"), Row("1AA100008", "8Series"), Row("1AA100009", "0Series"), Row("1AA10001", "2Series"), Row("1AA100010", "3Series"), Row("1AA100011", "0Series"), Row("1AA100012", "4Series"), Row("1AA100014", "5Series"), Row("1AA100015", "4Series"), Row("1AA100016", "3Series"), Row("1AA100017", "9Series"), Row("1AA100018", "8Series"), Row("1AA100019", "5Series"), Row("1AA10002", "0Series"), Row("1AA100020", "5Series"), Row("1AA100021", "0Series"), Row("1AA100022", "5Series"), Row("1AA100023", "5Series"), Row("1AA100024", "6Series"), Row("1AA100025", "0Series"), Row("1AA100027", "0Series"), Row("1AA100028", "5Series"), Row("1AA100029", "2Series"), Row("1AA100033", "8Series"), Row("1AA100034", "2Series"), Row("1AA100035", "5Series"), Row("1AA100036", "5Series"), Row("1AA100038", "6Series"), Row("1AA100039", "8Series"), Row("1AA10004", "5Series"), Row("1AA100040", "8Series"), Row("1AA100041", "5Series"), Row("1AA100042", "3Series"), Row("1AA100043", "9Series"), Row("1AA100044", "8Series"), Row("1AA100045", "2Series"), Row("1AA100046", "3Series"), Row("1AA100047", "9Series"), Row("1AA100048", "3Series"), Row("1AA100049", "0Series"), Row("1AA10005", "8Series"), Row("1AA100050", "2Series"), Row("1AA100051", "2Series"), Row("1AA100052", "6Series"), Row("1AA100053", "2Series"), Row("1AA100056", "6Series"), Row("1AA100057", "9Series"), Row("1AA100058", "5Series"), Row("1AA100059", "4Series"), Row("1AA10006", "3Series"), Row("1AA100060", "8Series"), Row("1AA100061", "6Series"), Row("1AA100062", "9Series"), Row("1AA100063", "2Series"), Row("1AA100064", "6Series"), Row("1AA100065", "0Series"), Row("1AA100066", "6Series"), Row("1AA100067", "4Series"), Row("1AA100068", "8Series"), Row("1AA100069", "8Series"), Row("1AA10007", "8Series"), Row("1AA100070", "0Series"), Row("1AA100071", "0Series"), Row("1AA100072", "4Series"), Row("1AA100073", "4Series"), Row("1AA100074", "6Series"), Row("1AA100075", "3Series"), Row("1AA100076", "0Series"), Row("1AA100077", "3Series"), Row("1AA100078", "2Series"), Row("1AA100079", "4Series"), Row("1AA10008", "5Series"), Row("1AA100080", "9Series"), Row("1AA100081", "5Series"), Row("1AA100082", "5Series"), Row("1AA100083", "0Series"), Row("1AA100084", "0Series")))
  })

   //TC_166
  test("select Upper(series) a  from Carbon_automation_test")({
    checkAnswer(
      sql("select Upper(series) a  from Carbon_automation_test"),
      Seq(Row("7SERIES"), Row("7SERIES"), Row("5SERIES"), Row("5SERIES"), Row("7SERIES"), Row("9SERIES"), Row("7SERIES"), Row("0SERIES"), Row("0SERIES"), Row("5SERIES"), Row("4SERIES"), Row("1SERIES"), Row("6SERIES"), Row("9SERIES"), Row("8SERIES"), Row("0SERIES"), Row("2SERIES"), Row("3SERIES"), Row("0SERIES"), Row("4SERIES"), Row("1SERIES"), Row("5SERIES"), Row("4SERIES"), Row("3SERIES"), Row("9SERIES"), Row("8SERIES"), Row("5SERIES"), Row("0SERIES"), Row("5SERIES"), Row("0SERIES"), Row("5SERIES"), Row("5SERIES"), Row("6SERIES"), Row("0SERIES"), Row("7SERIES"), Row("0SERIES"), Row("5SERIES"), Row("2SERIES"), Row("7SERIES"), Row("7SERIES"), Row("7SERIES"), Row("1SERIES"), Row("8SERIES"), Row("2SERIES"), Row("5SERIES"), Row("5SERIES"), Row("7SERIES"), Row("6SERIES"), Row("8SERIES"), Row("5SERIES"), Row("8SERIES"), Row("5SERIES"), Row("3SERIES"), Row("9SERIES"), Row("8SERIES"), Row("2SERIES"), Row("3SERIES"), Row("9SERIES"), Row("3SERIES"), Row("0SERIES"), Row("8SERIES"), Row("2SERIES"), Row("2SERIES"), Row("6SERIES"), Row("2SERIES"), Row("7SERIES"), Row("7SERIES"), Row("6SERIES"), Row("9SERIES"), Row("5SERIES"), Row("4SERIES"), Row("3SERIES"), Row("8SERIES"), Row("6SERIES"), Row("9SERIES"), Row("2SERIES"), Row("6SERIES"), Row("0SERIES"), Row("6SERIES"), Row("4SERIES"), Row("8SERIES"), Row("8SERIES"), Row("8SERIES"), Row("0SERIES"), Row("0SERIES"), Row("4SERIES"), Row("4SERIES"), Row("6SERIES"), Row("3SERIES"), Row("0SERIES"), Row("3SERIES"), Row("2SERIES"), Row("4SERIES"), Row("5SERIES"), Row("9SERIES"), Row("5SERIES"), Row("5SERIES"), Row("0SERIES"), Row("0SERIES")))
  })

  //TC_167
  test("select Upper(Latest_DAY) a  from Carbon_automation_test")({
    checkAnswer(
      sql("select Upper(Latest_DAY) a  from Carbon_automation_test"),
      Seq(Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1")))
  })

  //TC_168
  test("select imei,series from Carbon_automation_test limit 10")({
    checkAnswer(
      sql("select imei,series from Carbon_automation_test limit 10"),
      Seq(Row("1AA1", "7Series"), Row("1AA10", "7Series"), Row("1AA100", "5Series"), Row("1AA1000", "5Series"), Row("1AA10000", "7Series"), Row("1AA100000", "9Series"), Row("1AA1000000", "7Series"), Row("1AA100001", "0Series"), Row("1AA100002", "0Series"), Row("1AA100003", "5Series")))
  })

  //TC_171
  test("select Lower(series) a  from Carbon_automation_test")({
    checkAnswer(
      sql("select Lower(series) a  from Carbon_automation_test"),
      Seq(Row("7series"), Row("7series"), Row("5series"), Row("5series"), Row("7series"), Row("9series"), Row("7series"), Row("0series"), Row("0series"), Row("5series"), Row("4series"), Row("1series"), Row("6series"), Row("9series"), Row("8series"), Row("0series"), Row("2series"), Row("3series"), Row("0series"), Row("4series"), Row("1series"), Row("5series"), Row("4series"), Row("3series"), Row("9series"), Row("8series"), Row("5series"), Row("0series"), Row("5series"), Row("0series"), Row("5series"), Row("5series"), Row("6series"), Row("0series"), Row("7series"), Row("0series"), Row("5series"), Row("2series"), Row("7series"), Row("7series"), Row("7series"), Row("1series"), Row("8series"), Row("2series"), Row("5series"), Row("5series"), Row("7series"), Row("6series"), Row("8series"), Row("5series"), Row("8series"), Row("5series"), Row("3series"), Row("9series"), Row("8series"), Row("2series"), Row("3series"), Row("9series"), Row("3series"), Row("0series"), Row("8series"), Row("2series"), Row("2series"), Row("6series"), Row("2series"), Row("7series"), Row("7series"), Row("6series"), Row("9series"), Row("5series"), Row("4series"), Row("3series"), Row("8series"), Row("6series"), Row("9series"), Row("2series"), Row("6series"), Row("0series"), Row("6series"), Row("4series"), Row("8series"), Row("8series"), Row("8series"), Row("0series"), Row("0series"), Row("4series"), Row("4series"), Row("6series"), Row("3series"), Row("0series"), Row("3series"), Row("2series"), Row("4series"), Row("5series"), Row("9series"), Row("5series"), Row("5series"), Row("0series"), Row("0series")))
  })

  //TC_172
  test("select Lower(Latest_DAY) a  from Carbon_automation_test")({
    checkAnswer(
      sql("select Lower(Latest_DAY) a  from Carbon_automation_test"),
      Seq(Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1")))
  })

  //TC_173
  test("select distinct  Latest_DAY from Carbon_automation_test")({
    checkAnswer(
      sql("select distinct  Latest_DAY from Carbon_automation_test"),
      Seq(Row(1)))
  })

    //TC_175
  test("select distinct  channelsId from Carbon_automation_test")({
    checkAnswer(
      sql("select distinct  channelsId from Carbon_automation_test"),
      Seq(Row("1"), Row("2"), Row("3"), Row("4"), Row("5"), Row("6"), Row("7")))
  })

  //TC_176
  test("select distinct  series from Carbon_automation_test")({
    checkAnswer(
      sql("select distinct  series from Carbon_automation_test"),
      Seq(Row("6Series"), Row("0Series"), Row("4Series"), Row("8Series"), Row("7Series"), Row("1Series"), Row("5Series"), Row("9Series"), Row("3Series"), Row("2Series")))
  })

  //TC_177
  test("select distinct count(series) as a  from Carbon_automation_test group by channelsName")({
    checkAnswer(
      sql("select distinct count(series) as a  from Carbon_automation_test group by channelsName"),
      Seq(Row(10), Row(12), Row(16), Row(18), Row(19)))
  })

  //TC_178
  test("select distinct count(gamePointId) a from Carbon_automation_test group by channelsName")({
    checkAnswer(
      sql("select distinct count(gamePointId) a from Carbon_automation_test group by channelsName"),
      Seq(Row(10), Row(12), Row(16), Row(18), Row(19)))
  })

  //TC_179
  test("select imei,series from Carbon_automation_test limit 101")({
    checkAnswer(
      sql("select imei,series from Carbon_automation_test limit 101"),
      Seq(Row("1AA1", "7Series"), Row("1AA10", "7Series"), Row("1AA100", "5Series"), Row("1AA1000", "5Series"), Row("1AA10000", "7Series"), Row("1AA100000", "9Series"), Row("1AA1000000", "7Series"), Row("1AA100001", "0Series"), Row("1AA100002", "0Series"), Row("1AA100003", "5Series"), Row("1AA100004", "4Series"), Row("1AA100005", "1Series"), Row("1AA100006", "6Series"), Row("1AA100007", "9Series"), Row("1AA100008", "8Series"), Row("1AA100009", "0Series"), Row("1AA10001", "2Series"), Row("1AA100010", "3Series"), Row("1AA100011", "0Series"), Row("1AA100012", "4Series"), Row("1AA100013", "1Series"), Row("1AA100014", "5Series"), Row("1AA100015", "4Series"), Row("1AA100016", "3Series"), Row("1AA100017", "9Series"), Row("1AA100018", "8Series"), Row("1AA100019", "5Series"), Row("1AA10002", "0Series"), Row("1AA100020", "5Series"), Row("1AA100021", "0Series"), Row("1AA100022", "5Series"), Row("1AA100023", "5Series"), Row("1AA100024", "6Series"), Row("1AA100025", "0Series"), Row("1AA100026", "7Series"), Row("1AA100027", "0Series"), Row("1AA100028", "5Series"), Row("1AA100029", "2Series"), Row("1AA10003", "7Series"), Row("1AA100030", "7Series"), Row("1AA100031", "7Series"), Row("1AA100032", "1Series"), Row("1AA100033", "8Series"), Row("1AA100034", "2Series"), Row("1AA100035", "5Series"), Row("1AA100036", "5Series"), Row("1AA100037", "7Series"), Row("1AA100038", "6Series"), Row("1AA100039", "8Series"), Row("1AA10004", "5Series"), Row("1AA100040", "8Series"), Row("1AA100041", "5Series"), Row("1AA100042", "3Series"), Row("1AA100043", "9Series"), Row("1AA100044", "8Series"), Row("1AA100045", "2Series"), Row("1AA100046", "3Series"), Row("1AA100047", "9Series"), Row("1AA100048", "3Series"), Row("1AA100049", "0Series"), Row("1AA10005", "8Series"), Row("1AA100050", "2Series"), Row("1AA100051", "2Series"), Row("1AA100052", "6Series"), Row("1AA100053", "2Series"), Row("1AA100054", "7Series"), Row("1AA100055", "7Series"), Row("1AA100056", "6Series"), Row("1AA100057", "9Series"), Row("1AA100058", "5Series"), Row("1AA100059", "4Series"), Row("1AA10006", "3Series"), Row("1AA100060", "8Series"), Row("1AA100061", "6Series"), Row("1AA100062", "9Series"), Row("1AA100063", "2Series"), Row("1AA100064", "6Series"), Row("1AA100065", "0Series"), Row("1AA100066", "6Series"), Row("1AA100067", "4Series"), Row("1AA100068", "8Series"), Row("1AA100069", "8Series"), Row("1AA10007", "8Series"), Row("1AA100070", "0Series"), Row("1AA100071", "0Series"), Row("1AA100072", "4Series"), Row("1AA100073", "4Series"), Row("1AA100074", "6Series"), Row("1AA100075", "3Series"), Row("1AA100076", "0Series"), Row("1AA100077", "3Series"), Row("1AA100078", "2Series"), Row("1AA100079", "4Series"), Row("1AA10008", "5Series"), Row("1AA100080", "9Series"), Row("1AA100081", "5Series"), Row("1AA100082", "5Series"), Row("1AA100083", "0Series"), Row("1AA100084", "0Series")))
  })

  //TC_180
  test("select series,sum(gamePointId) a from Carbon_automation_test group by series order by series desc")({
    checkAnswer(
      sql("select series,sum(gamePointId) a from Carbon_automation_test group by series order by series desc"),
      sql("select series,sum(gamePointId) a from Carbon_automation_test_hive group by series order by series desc"))
  })

  //TC_181
  test("select series,sum(gamePointId) a from Carbon_automation_test group by series order by a desc")({
    checkAnswer(
      sql("select series,sum(gamePointId) a from Carbon_automation_test group by series order by a desc"),
      sql("select series,sum(gamePointId) a from Carbon_automation_test_hive group by series order by a desc"))
  })

  //TC_182
  test("select series,sum(gamePointId) a from Carbon_automation_test group by series order by series desc ,a desc")({
    checkAnswer(
      sql("select series,sum(gamePointId) a from Carbon_automation_test group by series order by series desc ,a desc"),
      sql("select series,sum(gamePointId) a from Carbon_automation_test_hive group by series order by series desc ,a desc"))
  })

  //TC_183
  test("select series,sum(gamePointId) a from Carbon_automation_test group by series order by series asc")({
    checkAnswer(
      sql("select series,sum(gamePointId) a from Carbon_automation_test group by series order by series asc"),
      sql("select series,sum(gamePointId) a from Carbon_automation_test_hive group by series order by series asc"))
  })

  //TC_184
  test("select series,sum(gamePointId) a from Carbon_automation_test group by series order by a asc")({
    checkAnswer(
      sql("select series,sum(gamePointId) a from Carbon_automation_test group by series order by a asc"),
      sql("select series,sum(gamePointId) a from Carbon_automation_test_hive group by series order by a asc"))
  })

  //TC_185
  test("select series,sum(gamePointId) a from Carbon_automation_test group by series order by series asc ,a asc")({
    checkAnswer(
      sql("select series,sum(gamePointId) a from Carbon_automation_test group by series order by series asc ,a asc"),
      sql("select series,sum(gamePointId) a from Carbon_automation_test_hive group by series order by series asc ,a asc"))
  })

  //TC_186
  test("select series,sum(gamePointId) a from Carbon_automation_test group by series order by series desc ,a asc")({
    checkAnswer(
      sql("select series,sum(gamePointId) a from Carbon_automation_test group by series order by series desc ,a asc"),
      sql("select series,sum(gamePointId) a from Carbon_automation_test_hive group by series order by series desc ,a asc"))
  })

  //TC_187
  test("select series,ActiveProvince,sum(gamePointId) a from Carbon_automation_test group by series,ActiveProvince order by series desc,ActiveProvince asc")({
    checkAnswer(
      sql("select series,ActiveProvince,sum(gamePointId) a from Carbon_automation_test group by series,ActiveProvince order by series desc,ActiveProvince asc"),
      sql("select series,ActiveProvince,sum(gamePointId) a from Carbon_automation_test_hive group by series,ActiveProvince order by series desc,ActiveProvince asc"))
  })

   //TC_208
  test("select Latest_DAY as a from Carbon_automation_test where Latest_DAY<=>Latest_areaId")({
    checkAnswer(
      sql("select Latest_DAY as a from Carbon_automation_test where Latest_DAY<=>Latest_areaId"),
      Seq(Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1)))
  })

    //TC_210
  test("select Latest_DAY  from Carbon_automation_test where Latest_DAY<>Latest_areaId")({
    checkAnswer(
      sql("select Latest_DAY  from Carbon_automation_test where Latest_DAY<>Latest_areaId"),
      Seq(Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1)))
  })

  //TC_211
  test("select Latest_DAY from Carbon_automation_test where Latest_DAY != Latest_areaId")({
    checkAnswer(
      sql("select Latest_DAY from Carbon_automation_test where Latest_DAY != Latest_areaId"),
      Seq(Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1), Row(1)))
  })

  //TC_212
  test("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY<Latest_areaId")({
    checkAnswer(
      sql("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY<Latest_areaId"),
      Seq(Row("1AA1", 1), Row("1AA10", 1), Row("1AA100", 1), Row("1AA1000", 1), Row("1AA10000", 1), Row("1AA100000", 1), Row("1AA100001", 1), Row("1AA100002", 1), Row("1AA100003", 1), Row("1AA100004", 1), Row("1AA100006", 1), Row("1AA100007", 1), Row("1AA100008", 1), Row("1AA100009", 1), Row("1AA10001", 1), Row("1AA100010", 1), Row("1AA100011", 1), Row("1AA100012", 1), Row("1AA100013", 1), Row("1AA100014", 1), Row("1AA100015", 1), Row("1AA100016", 1), Row("1AA100017", 1), Row("1AA100018", 1), Row("1AA100019", 1), Row("1AA10002", 1), Row("1AA100020", 1), Row("1AA100021", 1), Row("1AA100022", 1), Row("1AA100023", 1), Row("1AA100024", 1), Row("1AA100029", 1), Row("1AA10003", 1), Row("1AA100030", 1), Row("1AA100031", 1), Row("1AA100032", 1), Row("1AA100033", 1), Row("1AA100035", 1), Row("1AA100036", 1), Row("1AA100037", 1), Row("1AA100038", 1), Row("1AA10004", 1), Row("1AA100040", 1), Row("1AA100041", 1), Row("1AA100042", 1), Row("1AA100043", 1), Row("1AA100044", 1), Row("1AA100045", 1), Row("1AA100046", 1), Row("1AA100047", 1), Row("1AA100048", 1), Row("1AA100049", 1), Row("1AA10005", 1), Row("1AA100051", 1), Row("1AA100053", 1), Row("1AA100054", 1), Row("1AA100055", 1), Row("1AA100056", 1), Row("1AA100057", 1), Row("1AA100058", 1), Row("1AA100059", 1), Row("1AA10006", 1), Row("1AA100060", 1), Row("1AA100062", 1), Row("1AA100063", 1), Row("1AA100064", 1), Row("1AA100065", 1), Row("1AA100066", 1), Row("1AA100067", 1), Row("1AA100068", 1), Row("1AA100069", 1), Row("1AA10007", 1), Row("1AA100070", 1), Row("1AA100071", 1), Row("1AA100072", 1), Row("1AA100073", 1), Row("1AA100075", 1), Row("1AA100076", 1), Row("1AA100077", 1), Row("1AA100078", 1), Row("1AA100079", 1), Row("1AA10008", 1), Row("1AA100080", 1), Row("1AA100081", 1), Row("1AA100082", 1), Row("1AA100083", 1), Row("1AA100084", 1)))
  })

  //TC_213
  test("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY<=Latest_areaId")({
    checkAnswer(
      sql("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY<=Latest_areaId"),
      Seq(Row("1AA1", 1), Row("1AA10", 1), Row("1AA100", 1), Row("1AA1000", 1), Row("1AA10000", 1), Row("1AA100000", 1), Row("1AA1000000", 1), Row("1AA100001", 1), Row("1AA100002", 1), Row("1AA100003", 1), Row("1AA100004", 1), Row("1AA100005", 1), Row("1AA100006", 1), Row("1AA100007", 1), Row("1AA100008", 1), Row("1AA100009", 1), Row("1AA10001", 1), Row("1AA100010", 1), Row("1AA100011", 1), Row("1AA100012", 1), Row("1AA100013", 1), Row("1AA100014", 1), Row("1AA100015", 1), Row("1AA100016", 1), Row("1AA100017", 1), Row("1AA100018", 1), Row("1AA100019", 1), Row("1AA10002", 1), Row("1AA100020", 1), Row("1AA100021", 1), Row("1AA100022", 1), Row("1AA100023", 1), Row("1AA100024", 1), Row("1AA100025", 1), Row("1AA100026", 1), Row("1AA100027", 1), Row("1AA100028", 1), Row("1AA100029", 1), Row("1AA10003", 1), Row("1AA100030", 1), Row("1AA100031", 1), Row("1AA100032", 1), Row("1AA100033", 1), Row("1AA100034", 1), Row("1AA100035", 1), Row("1AA100036", 1), Row("1AA100037", 1), Row("1AA100038", 1), Row("1AA100039", 1), Row("1AA10004", 1), Row("1AA100040", 1), Row("1AA100041", 1), Row("1AA100042", 1), Row("1AA100043", 1), Row("1AA100044", 1), Row("1AA100045", 1), Row("1AA100046", 1), Row("1AA100047", 1), Row("1AA100048", 1), Row("1AA100049", 1), Row("1AA10005", 1), Row("1AA100050", 1), Row("1AA100051", 1), Row("1AA100052", 1), Row("1AA100053", 1), Row("1AA100054", 1), Row("1AA100055", 1), Row("1AA100056", 1), Row("1AA100057", 1), Row("1AA100058", 1), Row("1AA100059", 1), Row("1AA10006", 1), Row("1AA100060", 1), Row("1AA100061", 1), Row("1AA100062", 1), Row("1AA100063", 1), Row("1AA100064", 1), Row("1AA100065", 1), Row("1AA100066", 1), Row("1AA100067", 1), Row("1AA100068", 1), Row("1AA100069", 1), Row("1AA10007", 1), Row("1AA100070", 1), Row("1AA100071", 1), Row("1AA100072", 1), Row("1AA100073", 1), Row("1AA100074", 1), Row("1AA100075", 1), Row("1AA100076", 1), Row("1AA100077", 1), Row("1AA100078", 1), Row("1AA100079", 1), Row("1AA10008", 1), Row("1AA100080", 1), Row("1AA100081", 1), Row("1AA100082", 1), Row("1AA100083", 1), Row("1AA100084", 1)))
  })

  //TC_215
  test("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY>=Latest_areaId")({
    checkAnswer(
      sql("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY>=Latest_areaId"),
      Seq(Row("1AA1000000", 1), Row("1AA100005", 1), Row("1AA100025", 1), Row("1AA100026", 1), Row("1AA100027", 1), Row("1AA100028", 1), Row("1AA100034", 1), Row("1AA100039", 1), Row("1AA100050", 1), Row("1AA100052", 1), Row("1AA100061", 1), Row("1AA100074", 1)))
  })

  //TC_216
  test("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY NOT BETWEEN Latest_areaId AND  Latest_HOUR")({
    checkAnswer(
      sql("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY NOT BETWEEN Latest_areaId AND  Latest_HOUR"),
      Seq(Row("1AA1", 1), Row("1AA10", 1), Row("1AA100", 1), Row("1AA1000", 1), Row("1AA10000", 1), Row("1AA100000", 1), Row("1AA100001", 1), Row("1AA100002", 1), Row("1AA100003", 1), Row("1AA100004", 1), Row("1AA100006", 1), Row("1AA100007", 1), Row("1AA100008", 1), Row("1AA100009", 1), Row("1AA10001", 1), Row("1AA100010", 1), Row("1AA100011", 1), Row("1AA100012", 1), Row("1AA100013", 1), Row("1AA100014", 1), Row("1AA100015", 1), Row("1AA100016", 1), Row("1AA100017", 1), Row("1AA100018", 1), Row("1AA100019", 1), Row("1AA10002", 1), Row("1AA100020", 1), Row("1AA100021", 1), Row("1AA100022", 1), Row("1AA100023", 1), Row("1AA100024", 1), Row("1AA100029", 1), Row("1AA10003", 1), Row("1AA100030", 1), Row("1AA100031", 1), Row("1AA100032", 1), Row("1AA100033", 1), Row("1AA100035", 1), Row("1AA100036", 1), Row("1AA100037", 1), Row("1AA100038", 1), Row("1AA10004", 1), Row("1AA100040", 1), Row("1AA100041", 1), Row("1AA100042", 1), Row("1AA100043", 1), Row("1AA100044", 1), Row("1AA100045", 1), Row("1AA100046", 1), Row("1AA100047", 1), Row("1AA100048", 1), Row("1AA100049", 1), Row("1AA10005", 1), Row("1AA100051", 1), Row("1AA100053", 1), Row("1AA100054", 1), Row("1AA100055", 1), Row("1AA100056", 1), Row("1AA100057", 1), Row("1AA100058", 1), Row("1AA100059", 1), Row("1AA10006", 1), Row("1AA100060", 1), Row("1AA100062", 1), Row("1AA100063", 1), Row("1AA100064", 1), Row("1AA100065", 1), Row("1AA100066", 1), Row("1AA100067", 1), Row("1AA100068", 1), Row("1AA100069", 1), Row("1AA10007", 1), Row("1AA100070", 1), Row("1AA100071", 1), Row("1AA100072", 1), Row("1AA100073", 1), Row("1AA100075", 1), Row("1AA100076", 1), Row("1AA100077", 1), Row("1AA100078", 1), Row("1AA100079", 1), Row("1AA10008", 1), Row("1AA100080", 1), Row("1AA100081", 1), Row("1AA100082", 1), Row("1AA100083", 1), Row("1AA100084", 1)))
  })

    //TC_219
  test("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY IS NOT NULL")({
    checkAnswer(
      sql("select imei, Latest_DAY from Carbon_automation_test where Latest_DAY IS NOT NULL"),
      Seq(Row("1AA1", 1), Row("1AA10", 1), Row("1AA100", 1), Row("1AA1000", 1), Row("1AA10000", 1), Row("1AA100000", 1), Row("1AA1000000", 1), Row("1AA100001", 1), Row("1AA100002", 1), Row("1AA100003", 1), Row("1AA100004", 1), Row("1AA100005", 1), Row("1AA100006", 1), Row("1AA100007", 1), Row("1AA100008", 1), Row("1AA100009", 1), Row("1AA10001", 1), Row("1AA100010", 1), Row("1AA100011", 1), Row("1AA100012", 1), Row("1AA100013", 1), Row("1AA100014", 1), Row("1AA100015", 1), Row("1AA100016", 1), Row("1AA100017", 1), Row("1AA100018", 1), Row("1AA100019", 1), Row("1AA10002", 1), Row("1AA100020", 1), Row("1AA100021", 1), Row("1AA100022", 1), Row("1AA100023", 1), Row("1AA100024", 1), Row("1AA100025", 1), Row("1AA100026", 1), Row("1AA100027", 1), Row("1AA100028", 1), Row("1AA100029", 1), Row("1AA10003", 1), Row("1AA100030", 1), Row("1AA100031", 1), Row("1AA100032", 1), Row("1AA100033", 1), Row("1AA100034", 1), Row("1AA100035", 1), Row("1AA100036", 1), Row("1AA100037", 1), Row("1AA100038", 1), Row("1AA100039", 1), Row("1AA10004", 1), Row("1AA100040", 1), Row("1AA100041", 1), Row("1AA100042", 1), Row("1AA100043", 1), Row("1AA100044", 1), Row("1AA100045", 1), Row("1AA100046", 1), Row("1AA100047", 1), Row("1AA100048", 1), Row("1AA100049", 1), Row("1AA10005", 1), Row("1AA100050", 1), Row("1AA100051", 1), Row("1AA100052", 1), Row("1AA100053", 1), Row("1AA100054", 1), Row("1AA100055", 1), Row("1AA100056", 1), Row("1AA100057", 1), Row("1AA100058", 1), Row("1AA100059", 1), Row("1AA10006", 1), Row("1AA100060", 1), Row("1AA100061", 1), Row("1AA100062", 1), Row("1AA100063", 1), Row("1AA100064", 1), Row("1AA100065", 1), Row("1AA100066", 1), Row("1AA100067", 1), Row("1AA100068", 1), Row("1AA100069", 1), Row("1AA10007", 1), Row("1AA100070", 1), Row("1AA100071", 1), Row("1AA100072", 1), Row("1AA100073", 1), Row("1AA100074", 1), Row("1AA100075", 1), Row("1AA100076", 1), Row("1AA100077", 1), Row("1AA100078", 1), Row("1AA100079", 1), Row("1AA10008", 1), Row("1AA100080", 1), Row("1AA100081", 1), Row("1AA100082", 1), Row("1AA100083", 1), Row("1AA100084", 1)))
  })

  //TC_220
  test("select imei, Latest_DAY from Carbon_automation_test where imei IS NOT NULL")({
    checkAnswer(
      sql("select imei, Latest_DAY from Carbon_automation_test where imei IS NOT NULL"),
      Seq(Row("1AA1", 1), Row("1AA10", 1), Row("1AA100", 1), Row("1AA1000", 1), Row("1AA10000", 1), Row("1AA100000", 1), Row("1AA1000000", 1), Row("1AA100001", 1), Row("1AA100002", 1), Row("1AA100003", 1), Row("1AA100004", 1), Row("1AA100005", 1), Row("1AA100006", 1), Row("1AA100007", 1), Row("1AA100008", 1), Row("1AA100009", 1), Row("1AA10001", 1), Row("1AA100010", 1), Row("1AA100011", 1), Row("1AA100012", 1), Row("1AA100013", 1), Row("1AA100014", 1), Row("1AA100015", 1), Row("1AA100016", 1), Row("1AA100017", 1), Row("1AA100018", 1), Row("1AA100019", 1), Row("1AA10002", 1), Row("1AA100020", 1), Row("1AA100021", 1), Row("1AA100022", 1), Row("1AA100023", 1), Row("1AA100024", 1), Row("1AA100025", 1), Row("1AA100026", 1), Row("1AA100027", 1), Row("1AA100028", 1), Row("1AA100029", 1), Row("1AA10003", 1), Row("1AA100030", 1), Row("1AA100031", 1), Row("1AA100032", 1), Row("1AA100033", 1), Row("1AA100034", 1), Row("1AA100035", 1), Row("1AA100036", 1), Row("1AA100037", 1), Row("1AA100038", 1), Row("1AA100039", 1), Row("1AA10004", 1), Row("1AA100040", 1), Row("1AA100041", 1), Row("1AA100042", 1), Row("1AA100043", 1), Row("1AA100044", 1), Row("1AA100045", 1), Row("1AA100046", 1), Row("1AA100047", 1), Row("1AA100048", 1), Row("1AA100049", 1), Row("1AA10005", 1), Row("1AA100050", 1), Row("1AA100051", 1), Row("1AA100052", 1), Row("1AA100053", 1), Row("1AA100054", 1), Row("1AA100055", 1), Row("1AA100056", 1), Row("1AA100057", 1), Row("1AA100058", 1), Row("1AA100059", 1), Row("1AA10006", 1), Row("1AA100060", 1), Row("1AA100061", 1), Row("1AA100062", 1), Row("1AA100063", 1), Row("1AA100064", 1), Row("1AA100065", 1), Row("1AA100066", 1), Row("1AA100067", 1), Row("1AA100068", 1), Row("1AA100069", 1), Row("1AA10007", 1), Row("1AA100070", 1), Row("1AA100071", 1), Row("1AA100072", 1), Row("1AA100073", 1), Row("1AA100074", 1), Row("1AA100075", 1), Row("1AA100076", 1), Row("1AA100077", 1), Row("1AA100078", 1), Row("1AA100079", 1), Row("1AA10008", 1), Row("1AA100080", 1), Row("1AA100081", 1), Row("1AA100082", 1), Row("1AA100083", 1), Row("1AA100084", 1)))
  })

   //TC_223
  test("select * from (select if( Latest_areaId=7,7,NULL) as babu,Latest_MONTH from Carbon_automation_test) qq where babu LIKE   Latest_MONTH")({
    checkAnswer(
      sql("select * from (select if( Latest_areaId=7,7,NULL) as babu,Latest_MONTH from Carbon_automation_test) qq where babu LIKE   Latest_MONTH"),
      Seq(Row(7, 7), Row(7, 7), Row(7, 7), Row(7, 7), Row(7, 7), Row(7, 7), Row(7, 7), Row(7, 7), Row(7, 7), Row(7, 7), Row(7, 7), Row(7, 7), Row(7, 7), Row(7, 7), Row(7, 7), Row(7, 7), Row(7, 7)))
  })

    //TC_263
  test("SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC")({
    checkAnswer(
      sql("SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC"),
      sql("SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test_hive) SUB_QRY GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC"))
  })

  //TC_265
  test("SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test) SUB_QRY WHERE NOT(AMSize = \"\") GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC")({
    checkAnswer(
      sql("SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test) SUB_QRY WHERE NOT(AMSize = \"\") GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC"),
      sql("SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test_hive) SUB_QRY WHERE NOT(AMSize = \"\") GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC"))
  })

   //TC_274
  test("SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamepointid) AS Sum_gamepointid FROM  Carbon_automation_test group by ActiveCountry,ActiveDistrict,Activecity")({
    checkAnswer(
      sql("SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamepointid) AS Sum_gamepointid FROM  Carbon_automation_test group by ActiveCountry,ActiveDistrict,Activecity"),
      sql("SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamepointid) AS Sum_gamepointid FROM  Carbon_automation_test_hive group by ActiveCountry,ActiveDistrict,Activecity"))
  })

  //TC_275
  test("SELECT Latest_country, Latest_city, Latest_district, SUM(gamepointid) AS Sum_gamepointid FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY Latest_country, Latest_city, Latest_district ORDER BY Latest_country ASC, Latest_city ASC, Latest_district ASC")({
    checkAnswer(
      sql("SELECT Latest_country, Latest_city, Latest_district, SUM(gamepointid) AS Sum_gamepointid FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY Latest_country, Latest_city, Latest_district ORDER BY Latest_country ASC, Latest_city ASC, Latest_district ASC"),
      sql("SELECT Latest_country, Latest_city, Latest_district, SUM(gamepointid) AS Sum_gamepointid FROM (select * from Carbon_automation_test_hive) SUB_QRY GROUP BY Latest_country, Latest_city, Latest_district ORDER BY Latest_country ASC, Latest_city ASC, Latest_district ASC"))
  })

  //TC_276
  test("SELECT Activecity, ActiveCountry, ActiveDistrict, COUNT(imei) AS Count_imei FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY Activecity, ActiveCountry, ActiveDistrict ORDER BY Activecity ASC, ActiveCountry ASC, ActiveDistrict ASC")({
    checkAnswer(
      sql("SELECT Activecity, ActiveCountry, ActiveDistrict, COUNT(imei) AS Count_imei FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY Activecity, ActiveCountry, ActiveDistrict ORDER BY Activecity ASC, ActiveCountry ASC, ActiveDistrict ASC"),
      Seq(Row("changsha", "Chinese", "yuhua", 19), Row("guangzhou", "Chinese", "longhua", 8), Row("shenzhen", "Chinese", "longgang", 12), Row("wuhan", "Chinese", "hongshan", 16), Row("xiangtan", "Chinese", "xiangtan", 22), Row("yichang", "Chinese", "yichang", 12), Row("zhuzhou", "Chinese", "tianyuan", 10)))
  })

   //TC_279
  test("SELECT ActiveCountry, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY ActiveCountry ORDER BY ActiveCountry ASC")({
    checkAnswer(
      sql("SELECT ActiveCountry, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY ActiveCountry ORDER BY ActiveCountry ASC"),
      Seq(Row("Chinese", 99)))
  })

    //TC_282
  test("SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamepointid) AS Sum_gamepointid FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity ASC")({
    checkAnswer(
      sql("SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamepointid) AS Sum_gamepointid FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity ASC"),
      sql("SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamepointid) AS Sum_gamepointid FROM (select * from Carbon_automation_test_hive) SUB_QRY GROUP BY ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity ASC"))
  })

    //TC_317
  test("select channelsId from Carbon_automation_test order by  channelsId")({
    checkAnswer(
      sql("select channelsId from Carbon_automation_test order by  channelsId"),
      Seq(Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("1"), Row("2"), Row("2"), Row("2"), Row("2"), Row("2"), Row("2"), Row("2"), Row("2"), Row("2"), Row("2"), Row("3"), Row("3"), Row("3"), Row("3"), Row("3"), Row("3"), Row("3"), Row("3"), Row("3"), Row("3"), Row("3"), Row("3"), Row("3"), Row("3"), Row("3"), Row("3"), Row("4"), Row("4"), Row("4"), Row("4"), Row("4"), Row("4"), Row("4"), Row("4"), Row("4"), Row("4"), Row("4"), Row("4"), Row("5"), Row("5"), Row("5"), Row("5"), Row("5"), Row("5"), Row("5"), Row("5"), Row("5"), Row("5"), Row("5"), Row("5"), Row("6"), Row("6"), Row("6"), Row("6"), Row("6"), Row("6"), Row("6"), Row("6"), Row("6"), Row("6"), Row("6"), Row("6"), Row("6"), Row("6"), Row("6"), Row("6"), Row("6"), Row("6"), Row("6"), Row("7"), Row("7"), Row("7"), Row("7"), Row("7"), Row("7"), Row("7"), Row("7"), Row("7"), Row("7"), Row("7"), Row("7"), Row("7"), Row("7"), Row("7"), Row("7"), Row("7"), Row("7")))
  })

  //TC_318
  test("select count(series),series from Carbon_automation_test group by series having series='6Series'")({
    checkAnswer(
      sql("select count(series),series from Carbon_automation_test group by series having series='6Series'"),
      Seq(Row(9, "6Series")))
  })

  //TC_319
  test("SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity ASC")({
    checkAnswer(
      sql("SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity ASC"),
      sql("SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test_hive) SUB_QRY GROUP BY ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity ASC"))
  })

  //TC_321
  test("SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test) SUB_QRY WHERE imei = \"1AA100000\" GROUP BY ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity ASC")({
    checkAnswer(
      sql("SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test) SUB_QRY WHERE imei = \"1AA100000\" GROUP BY ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity ASC"),
      Seq(Row("Chinese", "yichang", "yichang", 136.0)))
  })

    //TC_384
  test("SELECT series, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY series ORDER BY series ASC")({
    checkAnswer(
      sql("SELECT series, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY series ORDER BY series ASC"),
      sql("SELECT series, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test_hive) SUB_QRY GROUP BY series ORDER BY series ASC"))
  })

  //TC_386
  test("SELECT channelsId, deliveryCity FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY channelsId, deliveryCity ORDER BY channelsId ASC, deliveryCity ASC")({
    checkAnswer(
      sql("SELECT channelsId, deliveryCity FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY channelsId, deliveryCity ORDER BY channelsId ASC, deliveryCity ASC"),
      Seq(Row("1", "changsha"), Row("1", "guangzhou"), Row("1", "shenzhen"), Row("1", "xiangtan"), Row("1", "yichang"), Row("1", "zhuzhou"), Row("2", "changsha"), Row("2", "guangzhou"), Row("2", "shenzhen"), Row("2", "xiangtan"), Row("2", "yichang"), Row("2", "zhuzhou"), Row("3", "changsha"), Row("3", "guangzhou"), Row("3", "shenzhen"), Row("3", "wuhan"), Row("3", "xiangtan"), Row("3", "yichang"), Row("3", "zhuzhou"), Row("4", "guangzhou"), Row("4", "shenzhen"), Row("4", "xiangtan"), Row("4", "yichang"), Row("4", "zhuzhou"), Row("5", "changsha"), Row("5", "guangzhou"), Row("5", "shenzhen"), Row("5", "wuhan"), Row("5", "xiangtan"), Row("5", "yichang"), Row("5", "zhuzhou"), Row("6", "changsha"), Row("6", "guangzhou"), Row("6", "shenzhen"), Row("6", "wuhan"), Row("6", "xiangtan"), Row("6", "yichang"), Row("6", "zhuzhou"), Row("7", "changsha"), Row("7", "guangzhou"), Row("7", "shenzhen"), Row("7", "wuhan"), Row("7", "xiangtan"), Row("7", "yichang"), Row("7", "zhuzhou")))
  })

  //TC_387
  test("SELECT modelId, SUM(gamepointid) AS Sum_gamepointid FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY modelId ORDER BY modelId ASC")({
    checkAnswer(
      sql("SELECT modelId, SUM(gamepointid) AS Sum_gamepointid FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY modelId ORDER BY modelId ASC"),
      sql("SELECT modelId, SUM(gamepointid) AS Sum_gamepointid FROM (select * from Carbon_automation_test_hive) SUB_QRY GROUP BY modelId ORDER BY modelId ASC"))
  })

  //TC_388
  test("SELECT imei, channelsId, COUNT(deliveryTime) AS Count_deliveryTime FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY imei, channelsId ORDER BY imei ASC, channelsId ASC")({
    checkAnswer(
      sql("SELECT imei, channelsId, COUNT(deliveryTime) AS Count_deliveryTime FROM (select * from Carbon_automation_test) SUB_QRY GROUP BY imei, channelsId ORDER BY imei ASC, channelsId ASC"),
      Seq(Row("1AA1", "4", 1), Row("1AA10", "4", 1), Row("1AA100", "6", 1), Row("1AA1000", "3", 1), Row("1AA10000", "1", 1), Row("1AA100000", "6", 1), Row("1AA1000000", "6", 1), Row("1AA100001", "7", 1), Row("1AA100002", "3", 1), Row("1AA100003", "3", 1), Row("1AA100004", "2", 1), Row("1AA100005", "1", 1), Row("1AA100006", "2", 1), Row("1AA100007", "3", 1), Row("1AA100008", "1", 1), Row("1AA100009", "3", 1), Row("1AA10001", "5", 1), Row("1AA100010", "6", 1), Row("1AA100011", "1", 1), Row("1AA100012", "2", 1), Row("1AA100013", "6", 1), Row("1AA100014", "3", 1), Row("1AA100015", "1", 1), Row("1AA100016", "3", 1), Row("1AA100017", "3", 1), Row("1AA100018", "4", 1), Row("1AA100019", "3", 1), Row("1AA10002", "5", 1), Row("1AA100020", "7", 1), Row("1AA100021", "6", 1), Row("1AA100022", "5", 1), Row("1AA100023", "3", 1), Row("1AA100024", "2", 1), Row("1AA100025", "1", 1), Row("1AA100026", "7", 1), Row("1AA100027", "4", 1), Row("1AA100028", "5", 1), Row("1AA100029", "5", 1), Row("1AA10003", "4", 1), Row("1AA100030", "3", 1), Row("1AA100031", "1", 1), Row("1AA100032", "7", 1), Row("1AA100033", "6", 1), Row("1AA100034", "6", 1), Row("1AA100035", "7", 1), Row("1AA100036", "5", 1), Row("1AA100037", "6", 1), Row("1AA100038", "3", 1), Row("1AA100039", "1", 1), Row("1AA10004", "4", 1), Row("1AA100040", "7", 1), Row("1AA100041", "1", 1), Row("1AA100042", "5", 1), Row("1AA100043", "6", 1), Row("1AA100044", "2", 1), Row("1AA100045", "6", 1), Row("1AA100046", "4", 1), Row("1AA100047", "1", 1), Row("1AA100048", "1", 1), Row("1AA100049", "6", 1), Row("1AA10005", "2", 1), Row("1AA100050", "1", 1), Row("1AA100051", "7", 1), Row("1AA100052", "7", 1), Row("1AA100053", "3", 1), Row("1AA100054", "2", 1), Row("1AA100055", "7", 1), Row("1AA100056", "5", 1), Row("1AA100057", "6", 1), Row("1AA100058", "4", 1), Row("1AA100059", "7", 1), Row("1AA10006", "5", 1), Row("1AA100060", "4", 1), Row("1AA100061", "6", 1), Row("1AA100062", "6", 1), Row("1AA100063", "3", 1), Row("1AA100064", "7", 1), Row("1AA100065", "7", 1), Row("1AA100066", "4", 1), Row("1AA100067", "7", 1), Row("1AA100068", "7", 1), Row("1AA100069", "5", 1), Row("1AA10007", "3", 1), Row("1AA100070", "3", 1), Row("1AA100071", "5", 1), Row("1AA100072", "7", 1), Row("1AA100073", "2", 1), Row("1AA100074", "7", 1), Row("1AA100075", "6", 1), Row("1AA100076", "7", 1), Row("1AA100077", "6", 1), Row("1AA100078", "5", 1), Row("1AA100079", "6", 1), Row("1AA10008", "4", 1), Row("1AA100080", "6", 1), Row("1AA100081", "2", 1), Row("1AA100082", "7", 1), Row("1AA100083", "2", 1), Row("1AA100084", "4", 1)))
  })

    //TC_408
  test("select imei,series from Carbon_automation_test where series='7Series' order by imei limit 10")({
    checkAnswer(
      sql("select imei,series from Carbon_automation_test where series='7Series' order by imei limit 10"),
      Seq(Row("1AA1", "7Series"), Row("1AA10", "7Series"), Row("1AA10000", "7Series"), Row("1AA1000000", "7Series"), Row("1AA100026", "7Series"), Row("1AA10003", "7Series"), Row("1AA100030", "7Series"), Row("1AA100031", "7Series"), Row("1AA100037", "7Series"), Row("1AA100054", "7Series")))
  })

  //TC_419
  test("select  count(channelsId) from Carbon_automation_test where  modelId is  null")({
    checkAnswer(
      sql("select  count(channelsId) from Carbon_automation_test where  modelId is  null"),
      Seq(Row(0)))
  })

  //TC_420
  test("select  sum(channelsId) from Carbon_automation_test where  deviceinformationid is  null")({
    checkAnswer(
      sql("select  sum(channelsId) from Carbon_automation_test where  deviceinformationid is  null"),
      Seq(Row(null)))
  })

  //TC_421
  test("select  avg(channelsName) from Carbon_automation_test where  modelId is  null")({
    checkAnswer(
      sql("select  avg(channelsName) from Carbon_automation_test where  modelId is  null"),
      Seq(Row(null)))
  })

   //TC_424
  test("SELECT count(DISTINCT gamePointId) FROM Carbon_automation_test where imei is null")({
    checkAnswer(
      sql("SELECT count(DISTINCT gamePointId) FROM Carbon_automation_test where imei is null"),
      Seq(Row(0)))
  })

  //TC_425
  test("select  imei from Carbon_automation_test where contractNumber is NOT null")({
    checkAnswer(
      sql("select  imei from Carbon_automation_test where contractNumber is NOT null"),
      Seq(Row("1AA1"), Row("1AA10"), Row("1AA100"), Row("1AA1000"), Row("1AA10000"), Row("1AA100000"), Row("1AA1000000"), Row("1AA100001"), Row("1AA100002"), Row("1AA100003"), Row("1AA100004"), Row("1AA100005"), Row("1AA100006"), Row("1AA100007"), Row("1AA100008"), Row("1AA100009"), Row("1AA10001"), Row("1AA100010"), Row("1AA100011"), Row("1AA100012"), Row("1AA100013"), Row("1AA100014"), Row("1AA100015"), Row("1AA100016"), Row("1AA100017"), Row("1AA100018"), Row("1AA100019"), Row("1AA10002"), Row("1AA100020"), Row("1AA100021"), Row("1AA100022"), Row("1AA100023"), Row("1AA100024"), Row("1AA100025"), Row("1AA100026"), Row("1AA100027"), Row("1AA100028"), Row("1AA100029"), Row("1AA10003"), Row("1AA100030"), Row("1AA100031"), Row("1AA100032"), Row("1AA100033"), Row("1AA100034"), Row("1AA100035"), Row("1AA100036"), Row("1AA100037"), Row("1AA100038"), Row("1AA100039"), Row("1AA10004"), Row("1AA100040"), Row("1AA100041"), Row("1AA100042"), Row("1AA100043"), Row("1AA100044"), Row("1AA100045"), Row("1AA100046"), Row("1AA100047"), Row("1AA100048"), Row("1AA100049"), Row("1AA10005"), Row("1AA100050"), Row("1AA100051"), Row("1AA100052"), Row("1AA100053"), Row("1AA100054"), Row("1AA100055"), Row("1AA100056"), Row("1AA100057"), Row("1AA100058"), Row("1AA100059"), Row("1AA10006"), Row("1AA100060"), Row("1AA100061"), Row("1AA100062"), Row("1AA100063"), Row("1AA100064"), Row("1AA100065"), Row("1AA100066"), Row("1AA100067"), Row("1AA100068"), Row("1AA100069"), Row("1AA10007"), Row("1AA100070"), Row("1AA100071"), Row("1AA100072"), Row("1AA100073"), Row("1AA100074"), Row("1AA100075"), Row("1AA100076"), Row("1AA100077"), Row("1AA100078"), Row("1AA100079"), Row("1AA10008"), Row("1AA100080"), Row("1AA100081"), Row("1AA100082"), Row("1AA100083"), Row("1AA100084")))
  })

    //TC_429
  test("select  count(gamePointId) from Carbon_automation_test where imei is NOT null")({
    checkAnswer(
      sql("select  count(gamePointId) from Carbon_automation_test where imei is NOT null"),
      Seq(Row(99)))
  })

  //TC_430
  test("select  count(bomCode) from Carbon_automation_test where contractNumber is NOT null")({
    checkAnswer(
      sql("select  count(bomCode) from Carbon_automation_test where contractNumber is NOT null"),
      Seq(Row(99)))
  })

  //TC_431
  test("select  channelsName from Carbon_automation_test where contractNumber is NOT null")({
    checkAnswer(
      sql("select  channelsName from Carbon_automation_test where contractNumber is NOT null"),
      Seq(Row("guomei"), Row("guomei"), Row("yidong"), Row("shuling"), Row("taobao"), Row("yidong"), Row("yidong"), Row("liantong"), Row("shuling"), Row("shuling"), Row("jingdong"), Row("taobao"), Row("jingdong"), Row("shuling"), Row("taobao"), Row("shuling"), Row("shishang"), Row("yidong"), Row("taobao"), Row("jingdong"), Row("yidong"), Row("shuling"), Row("taobao"), Row("shuling"), Row("shuling"), Row("guomei"), Row("shuling"), Row("shishang"), Row("liantong"), Row("yidong"), Row("shishang"), Row("shuling"), Row("jingdong"), Row("taobao"), Row("liantong"), Row("guomei"), Row("shishang"), Row("shishang"), Row("guomei"), Row("shuling"), Row("taobao"), Row("liantong"), Row("yidong"), Row("yidong"), Row("liantong"), Row("shishang"), Row("yidong"), Row("shuling"), Row("taobao"), Row("guomei"), Row("liantong"), Row("taobao"), Row("shishang"), Row("yidong"), Row("jingdong"), Row("yidong"), Row("guomei"), Row("taobao"), Row("taobao"), Row("yidong"), Row("jingdong"), Row("taobao"), Row("liantong"), Row("liantong"), Row("shuling"), Row("jingdong"), Row("liantong"), Row("shishang"), Row("yidong"), Row("guomei"), Row("liantong"), Row("shishang"), Row("guomei"), Row("yidong"), Row("yidong"), Row("shuling"), Row("liantong"), Row("liantong"), Row("guomei"), Row("liantong"), Row("liantong"), Row("shishang"), Row("shuling"), Row("shuling"), Row("shishang"), Row("liantong"), Row("jingdong"), Row("liantong"), Row("yidong"), Row("liantong"), Row("yidong"), Row("shishang"), Row("yidong"), Row("guomei"), Row("yidong"), Row("jingdong"), Row("liantong"), Row("jingdong"), Row("guomei")))
  })

  //TC_432
  test("select  channelsId from Carbon_automation_test where gamePointId is NOT null")({
    checkAnswer(
      sql("select  channelsId from Carbon_automation_test where gamePointId is NOT null"),
      Seq(Row("4"), Row("4"), Row("6"), Row("3"), Row("1"), Row("6"), Row("6"), Row("7"), Row("3"), Row("3"), Row("2"), Row("1"), Row("2"), Row("3"), Row("1"), Row("3"), Row("5"), Row("6"), Row("1"), Row("2"), Row("6"), Row("3"), Row("1"), Row("3"), Row("3"), Row("4"), Row("3"), Row("5"), Row("7"), Row("6"), Row("5"), Row("3"), Row("2"), Row("1"), Row("7"), Row("4"), Row("5"), Row("5"), Row("4"), Row("3"), Row("1"), Row("7"), Row("6"), Row("6"), Row("7"), Row("5"), Row("6"), Row("3"), Row("1"), Row("4"), Row("7"), Row("1"), Row("5"), Row("6"), Row("2"), Row("6"), Row("4"), Row("1"), Row("1"), Row("6"), Row("2"), Row("1"), Row("7"), Row("7"), Row("3"), Row("2"), Row("7"), Row("5"), Row("6"), Row("4"), Row("7"), Row("5"), Row("4"), Row("6"), Row("6"), Row("3"), Row("7"), Row("7"), Row("4"), Row("7"), Row("7"), Row("5"), Row("3"), Row("3"), Row("5"), Row("7"), Row("2"), Row("7"), Row("6"), Row("7"), Row("6"), Row("5"), Row("6"), Row("4"), Row("6"), Row("2"), Row("7"), Row("2"), Row("4")))
  })

  //TC_433
  test("select  channelsName from Carbon_automation_test where gamePointId is NOT null")({
    checkAnswer(
      sql("select  channelsName from Carbon_automation_test where gamePointId is NOT null"),
      Seq(Row("guomei"), Row("guomei"), Row("yidong"), Row("shuling"), Row("taobao"), Row("yidong"), Row("yidong"), Row("liantong"), Row("shuling"), Row("shuling"), Row("jingdong"), Row("taobao"), Row("jingdong"), Row("shuling"), Row("taobao"), Row("shuling"), Row("shishang"), Row("yidong"), Row("taobao"), Row("jingdong"), Row("yidong"), Row("shuling"), Row("taobao"), Row("shuling"), Row("shuling"), Row("guomei"), Row("shuling"), Row("shishang"), Row("liantong"), Row("yidong"), Row("shishang"), Row("shuling"), Row("jingdong"), Row("taobao"), Row("liantong"), Row("guomei"), Row("shishang"), Row("shishang"), Row("guomei"), Row("shuling"), Row("taobao"), Row("liantong"), Row("yidong"), Row("yidong"), Row("liantong"), Row("shishang"), Row("yidong"), Row("shuling"), Row("taobao"), Row("guomei"), Row("liantong"), Row("taobao"), Row("shishang"), Row("yidong"), Row("jingdong"), Row("yidong"), Row("guomei"), Row("taobao"), Row("taobao"), Row("yidong"), Row("jingdong"), Row("taobao"), Row("liantong"), Row("liantong"), Row("shuling"), Row("jingdong"), Row("liantong"), Row("shishang"), Row("yidong"), Row("guomei"), Row("liantong"), Row("shishang"), Row("guomei"), Row("yidong"), Row("yidong"), Row("shuling"), Row("liantong"), Row("liantong"), Row("guomei"), Row("liantong"), Row("liantong"), Row("shishang"), Row("shuling"), Row("shuling"), Row("shishang"), Row("liantong"), Row("jingdong"), Row("liantong"), Row("yidong"), Row("liantong"), Row("yidong"), Row("shishang"), Row("yidong"), Row("guomei"), Row("yidong"), Row("jingdong"), Row("liantong"), Row("jingdong"), Row("guomei")))
  })

  //TC_434
  test("select  channelsId from Carbon_automation_test where latest_day is NOT null")({
    checkAnswer(
      sql("select  channelsId from Carbon_automation_test where latest_day is NOT null"),
      Seq(Row("4"), Row("4"), Row("6"), Row("3"), Row("1"), Row("6"), Row("6"), Row("7"), Row("3"), Row("3"), Row("2"), Row("1"), Row("2"), Row("3"), Row("1"), Row("3"), Row("5"), Row("6"), Row("1"), Row("2"), Row("6"), Row("3"), Row("1"), Row("3"), Row("3"), Row("4"), Row("3"), Row("5"), Row("7"), Row("6"), Row("5"), Row("3"), Row("2"), Row("1"), Row("7"), Row("4"), Row("5"), Row("5"), Row("4"), Row("3"), Row("1"), Row("7"), Row("6"), Row("6"), Row("7"), Row("5"), Row("6"), Row("3"), Row("1"), Row("4"), Row("7"), Row("1"), Row("5"), Row("6"), Row("2"), Row("6"), Row("4"), Row("1"), Row("1"), Row("6"), Row("2"), Row("1"), Row("7"), Row("7"), Row("3"), Row("2"), Row("7"), Row("5"), Row("6"), Row("4"), Row("7"), Row("5"), Row("4"), Row("6"), Row("6"), Row("3"), Row("7"), Row("7"), Row("4"), Row("7"), Row("7"), Row("5"), Row("3"), Row("3"), Row("5"), Row("7"), Row("2"), Row("7"), Row("6"), Row("7"), Row("6"), Row("5"), Row("6"), Row("4"), Row("6"), Row("2"), Row("7"), Row("2"), Row("4")))
  })

  //TC_435
  test("select  channelsName from Carbon_automation_test where latest_day is NOT null")({
    checkAnswer(
      sql("select  channelsName from Carbon_automation_test where latest_day is NOT null"),
      Seq(Row("guomei"), Row("guomei"), Row("yidong"), Row("shuling"), Row("taobao"), Row("yidong"), Row("yidong"), Row("liantong"), Row("shuling"), Row("shuling"), Row("jingdong"), Row("taobao"), Row("jingdong"), Row("shuling"), Row("taobao"), Row("shuling"), Row("shishang"), Row("yidong"), Row("taobao"), Row("jingdong"), Row("yidong"), Row("shuling"), Row("taobao"), Row("shuling"), Row("shuling"), Row("guomei"), Row("shuling"), Row("shishang"), Row("liantong"), Row("yidong"), Row("shishang"), Row("shuling"), Row("jingdong"), Row("taobao"), Row("liantong"), Row("guomei"), Row("shishang"), Row("shishang"), Row("guomei"), Row("shuling"), Row("taobao"), Row("liantong"), Row("yidong"), Row("yidong"), Row("liantong"), Row("shishang"), Row("yidong"), Row("shuling"), Row("taobao"), Row("guomei"), Row("liantong"), Row("taobao"), Row("shishang"), Row("yidong"), Row("jingdong"), Row("yidong"), Row("guomei"), Row("taobao"), Row("taobao"), Row("yidong"), Row("jingdong"), Row("taobao"), Row("liantong"), Row("liantong"), Row("shuling"), Row("jingdong"), Row("liantong"), Row("shishang"), Row("yidong"), Row("guomei"), Row("liantong"), Row("shishang"), Row("guomei"), Row("yidong"), Row("yidong"), Row("shuling"), Row("liantong"), Row("liantong"), Row("guomei"), Row("liantong"), Row("liantong"), Row("shishang"), Row("shuling"), Row("shuling"), Row("shishang"), Row("liantong"), Row("jingdong"), Row("liantong"), Row("yidong"), Row("liantong"), Row("yidong"), Row("shishang"), Row("yidong"), Row("guomei"), Row("yidong"), Row("jingdong"), Row("liantong"), Row("jingdong"), Row("guomei")))
  })

   //TC_439
  test("SELECT min(AMSize) FROM Carbon_automation_test where imei is NOT null")({
    checkAnswer(
      sql("SELECT min(AMSize) FROM Carbon_automation_test where imei is NOT null"),
      Seq(Row("0RAM size")))
  })

    //TC_448
  test("select var_samp(Latest_YEAR) from Carbon_automation_test")({
    checkAnswer(
      sql("select var_samp(Latest_YEAR) from Carbon_automation_test"),
      Seq(Row(0.0)))
  })

  //TC_449
  test("select var_samp(AMSize) from Carbon_automation_test")({
    checkAnswer(
      sql("select var_samp(AMSize) from Carbon_automation_test"),
      Seq(Row(null)))
  })

   //TC_451
  test("select stddev_pop(bomcode)from Carbon_automation_test")({
    checkAnswer(
      sql("select stddev_pop(bomcode)from Carbon_automation_test"),
      Seq(Row(96490.49465950707)))
  })

  //TC_452
  test("select stddev_pop(deviceInformationId)from Carbon_automation_test1")({
    checkAnswer(
      sql("select stddev_pop(deviceInformationId)from Carbon_automation_test"),
      Seq(Row(96490.49465950707)))
  })

    //TC_454
  test("select stddev_pop(AMSIZE)from Carbon_automation_test")({
    checkAnswer(
      sql("select stddev_pop(AMSIZE)from Carbon_automation_test"),
      Seq(Row(null)))
  })

  //TC_457
  test("select stddev_samp(deviceInformationId)from Carbon_automation_test1")({
    checkAnswer(
      sql("select stddev_samp(deviceInformationId)from Carbon_automation_test"),
      Seq(Row(96981.54360516652)))
  })

  //TC_458
  test("select stddev_samp(AMSIZE)from Carbon_automation_test")({
    checkAnswer(
      sql("select stddev_samp(AMSIZE)from Carbon_automation_test"),
      Seq(Row(null)))
  })

  //TC_459
  test("select stddev_samp(Latest_MONTH)from Carbon_automation_test")({
    checkAnswer(
      sql("select stddev_samp(Latest_MONTH)from Carbon_automation_test"),
      Seq(Row(0.0)))
  })

    //TC_472
  test("Select percentile(1,1.0) from Carbon_automation_test2")({
    checkAnswer(
      sql("Select percentile(1,1.0) from Carbon_automation_test"),
      Seq(Row(1.0)))
  })

  //TC_473
  test("Select percentile(1,1.0) from Carbon_automation_test")({
    checkAnswer(
      sql("Select percentile(1,1.0) from Carbon_automation_test"),
      Seq(Row(1.0)))
  })

  //TC_474
  test("select cast(series as int) as a from Carbon_automation_test limit 10")({
    checkAnswer(
      sql("select cast(series as int) as a from Carbon_automation_test limit 10"),
      Seq(Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null)))
  })

  //TC_475
  test("select cast(modelid as int) as a from Carbon_automation_test limit 10")({
    checkAnswer(
      sql("select cast(modelid as int) as a from Carbon_automation_test limit 10"),
      Seq(Row(109), Row(93), Row(2591), Row(2531), Row(2408), Row(1815), Row(2479), Row(1845), Row(2008), Row(1121)))
  })

  //TC_476
  test("Select percentile(1,1.0) from Carbon_automation_test1")({
    checkAnswer(
      sql("Select percentile(1,1.0) from Carbon_automation_test"),
      Seq(Row(1.0)))
  })

  //TC_481
  test("select percentile_approx(1, 0.5 ,5000) from Carbon_automation_test")({
    checkAnswer(
      sql("select percentile_approx(1, 0.5 ,5000) from Carbon_automation_test"),
      Seq(Row(1.0)))
  })

  //TC_482
  test("select percentile_approx(1, 0.5 ,1000) from Carbon_automation_test")({
    checkAnswer(
      sql("select percentile_approx(1, 0.5 ,1000) from Carbon_automation_test"),
      Seq(Row(1.0)))
  })

  //TC_483
  test("select percentile_approx(1, 0.5 ,700) from Carbon_automation_test")({
    checkAnswer(
      sql("select percentile_approx(1, 0.5 ,700) from Carbon_automation_test"),
      Seq(Row(1.0)))
  })

  //TC_484
  test("select percentile_approx(1, 0.5 ,500) from Carbon_automation_test")({
    checkAnswer(
      sql("select percentile_approx(1, 0.5 ,500) from Carbon_automation_test"),
      Seq(Row(1.0)))
  })



  //TC_495
  test("select var_samp(Latest_YEAR) from Carbon_automation_test1")({
    checkAnswer(
      sql("select var_samp(Latest_YEAR) from Carbon_automation_test"),
      Seq(Row(0.0)))
  })

  //TC_496
  test("select stddev_pop(deviceInformationId)from Carbon_automation_test")({
    checkAnswer(
      sql("select stddev_pop(deviceInformationId)from Carbon_automation_test"),
      Seq(Row(96490.49465950707)))
  })

  //TC_497
  test("select stddev_samp(deviceInformationId)from Carbon_automation_test")({
    checkAnswer(
      sql("select stddev_samp(deviceInformationId)from Carbon_automation_test"),
      Seq(Row(96981.54360516652)))
  })

  test("CARBONDATA-60-union-defect")({
    sql("drop table if exists carbonunion")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 1000).map(x => (x+"", (x+100)+"")).toDF("c1", "c2")
    df.createOrReplaceTempView("sparkunion")
    df.write
      .format("carbondata")
      .mode(SaveMode.Overwrite)
      .option("tableName", "carbonunion")
      .save()
    checkAnswer(
      sql("select c1,count(c1) from (select c1 as c1,c2 as c2 from carbonunion union all select c2 as c1,c1 as c2 from carbonunion)t where c1='200' group by c1"),
      sql("select c1,count(c1) from (select c1 as c1,c2 as c2 from sparkunion union all select c2 as c1,c1 as c2 from sparkunion)t where c1='200' group by c1"))
    sql("drop table if exists carbonunion")
  })

  test("select Min(imei) from (select imei from Carbon_automation_test order by imei) t")({
    checkAnswer(
      sql("select Min(imei) from (select imei from Carbon_automation_test order by imei) t"),
      sql("select Min(imei) from (select imei from Carbon_automation_test_hive order by imei) t"))
  })

  test("select b.IMEI from Carbon_automation_test a join Carbon_automation_test b on a.imei=b.imei")({
    checkAnswer(
      sql("select b.IMEI from Carbon_automation_test a join Carbon_automation_test b on a.imei=b.imei"),
      sql("select b.IMEI from Carbon_automation_test_hive a join Carbon_automation_test_hive b on a.imei=b.imei"))
  })

  test("Right join with filter issue1") {

    checkAnswer(
      sql("""SELECT Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.ActiveCountry AS
       ActiveCountry, Carbon_automation_test.Activecity AS Activecity, SUM(Carbon_automation_test
       .gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,
       gamePointId FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test RIGHT JOIN
       ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from
       Carbon_automation_test) SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.gamePointId =
       Carbon_automation_test1.gamePointId WHERE Carbon_automation_test.AMSize IN ("6RAM size","8RAM size")
        GROUP BY Carbon_automation_test.AMSize, Carbon_automation_test.ActiveCountry, Carbon_automation_test
        .Activecity ORDER BY Carbon_automation_test.AMSize ASC, Carbon_automation_test.ActiveCountry ASC,
        Carbon_automation_test.Activecity ASC"""),
      sql("""SELECT Carbon_automation_test_hive.AMSize AS AMSize, Carbon_automation_test_hive.ActiveCountry AS
       ActiveCountry, Carbon_automation_test_hive.Activecity AS Activecity, SUM(Carbon_automation_test_hive
       .gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,
       gamePointId FROM (select * from Carbon_automation_test_hive) SUB_QRY ) Carbon_automation_test_hive RIGHT JOIN
       ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from
       Carbon_automation_test_hive) SUB_QRY ) Carbon_automation_test_hive1 ON Carbon_automation_test_hive.gamePointId =
       Carbon_automation_test_hive1.gamePointId WHERE Carbon_automation_test_hive.AMSize IN ("6RAM size","8RAM size")
        GROUP BY Carbon_automation_test_hive.AMSize, Carbon_automation_test_hive.ActiveCountry, Carbon_automation_test_hive
        .Activecity ORDER BY Carbon_automation_test_hive.AMSize ASC, Carbon_automation_test_hive.ActiveCountry ASC,
        Carbon_automation_test_hive.Activecity ASC """))
  }

  test("Right join with filter issue2") {

    checkAnswer(
      sql("""SELECT Carbon_automation_test.AMSize AS AMSize, Carbon_automation_test.gamePointId AS
       gamePointId, Carbon_automation_test.ActiveCountry AS ActiveCountry, Carbon_automation_test
       .Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId
       FROM (select * from Carbon_automation_test) SUB_QRY ) Carbon_automation_test RIGHT JOIN ( SELECT
       AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_test)
       SUB_QRY ) Carbon_automation_test1 ON Carbon_automation_test.gamePointId = Carbon_automation_test1
       .gamePointId WHERE Carbon_automation_test.AMSize > "5RAM size" """),
      sql("""SELECT Carbon_automation_test_hive.AMSize AS AMSize, Carbon_automation_test_hive.gamePointId AS
       gamePointId, Carbon_automation_test_hive.ActiveCountry AS ActiveCountry, Carbon_automation_test_hive
       .Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId
       FROM (select * from Carbon_automation_test_hive) SUB_QRY ) Carbon_automation_test_hive RIGHT JOIN ( SELECT
       AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_test_hive)
       SUB_QRY ) Carbon_automation_test_hive1 ON Carbon_automation_test_hive.gamePointId = Carbon_automation_test_hive1
       .gamePointId WHERE Carbon_automation_test_hive.AMSize > "5RAM size" """))

  }

  test("test self join query fail") {
    sql("DROP TABLE IF EXISTS uniqdata_INCLUDEDICTIONARY")

    sql("CREATE TABLE uniqdata_INCLUDEDICTIONARY (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
    sql(s"LOAD DATA INPATH '${resourcesPath + "/data_with_all_types.csv"}' into table" +
              " uniqdata_INCLUDEDICTIONARY OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\"'," +
              "'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION," +
              "DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2," +
              "Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")

    val count = sql("select b.BIGINT_COLUMN1,b.DECIMAL_COLUMN1,b.Double_COLUMN1,b.DOB,b.CUST_ID,b.CUST_NAME from uniqdata_INCLUDEDICTIONARY a join uniqdata_INCLUDEDICTIONARY b on a.cust_name=b.cust_name and a.cust_name RLIKE '10'").count()
    assert(count > 0)
    sql("DROP TABLE IF EXISTS uniqdata_INCLUDEDICTIONARY")
    }
}