
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

package org.apache.carbondata.cluster.sdv.generated

import org.apache.spark.sql.common.util._
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for Carbonautomation to verify all scenerios
 */

class CARBONAUTOMATIONTestCase extends QueryTest with BeforeAndAfterAll {
         

//TC_000_Drop
test("TC_000_Drop", Include) {
  sql(s"""drop table if exists  Carbon_automation""").collect

  sql(s"""drop table if exists  Carbon_automation_hive""").collect

  sql(s"""drop table if exists  Carbon_automation1""").collect

}
       

//TC_000_
test("TC_000_", Include) {
  sql(s"""create table Carbon_automation (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber double,imei_count int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES ('DICTIONARY_INCLUDE'='deviceInformationId,Latest_YEAR,Latest_MONTH,Latest_DAY')""").collect

  sql(s"""create table Carbon_automation_hive (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, contractNumber double,ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointId double,gamePointDescription string,imei_count int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

  sql(s"""create table Carbon_automation1 (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES ('DICTIONARY_INCLUDE'='deviceInformationId,Latest_YEAR,Latest_MONTH,Latest_DAY')""")

}
       

//TC_000_1
test("TC_000_1", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/HiveData' INTO TABLE Carbon_automation OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription,imei_count')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/HiveData' INTO TABLE Carbon_automation_hive """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/VmaLL100/100olap.csv' INTO TABLE Carbon_automation1 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""")
}
       

//TC_016
test("TC_016", Include) {
  checkAnswer(s"""select imei, Latest_DAY+ 10 as a  from Carbon_automation order by imei""",
    s"""select imei, Latest_DAY+ 10 as a  from Carbon_automation_hive order by imei""")
}
       

//TC_017
test("TC_017", Include) {
  checkAnswer(s"""select imei, gamePointId+ 10 Total from Carbon_automation order by imei""",
    s"""select imei, gamePointId+ 10 Total from Carbon_automation_hive order by imei""")
}
       

//TC_018
test("TC_018", Include) {
  checkAnswer(s"""select imei, modelId+ 10 Total from Carbon_automation order by imei""",
    s"""select imei, modelId+ 10 Total from Carbon_automation_hive order by imei""")
}
       

//TC_019
test("TC_019", Include) {
  checkAnswer(s"""select imei, gamePointId+contractNumber as a  from Carbon_automation order by imei""",
    s"""select imei, gamePointId+contractNumber as a  from Carbon_automation_hive order by imei""")
}
       

//TC_020
test("TC_020", Include) {
  checkAnswer(s"""select imei, deviceInformationId+gamePointId as Total from Carbon_automation""",
    s"""select imei, deviceInformationId+gamePointId as Total from Carbon_automation_hive""")
}
       

//TC_021
test("TC_021", Include) {
  checkAnswer(s"""select imei, deviceInformationId+deviceInformationId Total from Carbon_automation""",
    s"""select imei, deviceInformationId+deviceInformationId Total from Carbon_automation_hive""")
}
       

//TC_022
test("TC_022", Include) {
  checkAnswer(s"""select channelsId, sum(Latest_DAY+ 10) as a from Carbon_automation group by  channelsId order by channelsId""",
    s"""select channelsId, sum(Latest_DAY+ 10) as a from Carbon_automation_hive group by  channelsId order by channelsId""")
}
       

//TC_023
test("TC_023", Include) {
  checkAnswer(s"""select channelsId, sum(gamePointId+ 10) Total from Carbon_automation group by  channelsId order by Total""",
    s"""select channelsId, sum(gamePointId+ 10) Total from Carbon_automation_hive group by  channelsId order by Total""")
}
       

//TC_024
test("TC_024", Include) {
  checkAnswer(s"""select channelsId, sum(channelsId+ 10)  Total from Carbon_automation group by  channelsId order by Total""",
    s"""select channelsId, sum(channelsId+ 10)  Total from Carbon_automation_hive group by  channelsId order by Total""")
}
       

//TC_025
test("TC_025", Include) {
  checkAnswer(s"""select channelsId, sum(channelsId+channelsId) Total from Carbon_automation group by  channelsId order by Total""",
    s"""select channelsId, sum(channelsId+channelsId) Total from Carbon_automation_hive group by  channelsId order by Total""")
}
       

//TC_026
test("TC_026", Include) {
  checkAnswer(s"""select channelsId, sum(channelsId+channelsId) Total from Carbon_automation group by  channelsId""",
    s"""select channelsId, sum(channelsId+channelsId) Total from Carbon_automation_hive group by  channelsId""")
}
       

//TC_027
test("TC_027", Include) {
  checkAnswer(s"""select channelsId, avg(Latest_DAY+ 10) as a from Carbon_automation group by  channelsId order by channelsId""",
    s"""select channelsId, avg(Latest_DAY+ 10) as a from Carbon_automation_hive group by  channelsId order by channelsId""")
}
       

//TC_028
test("TC_028", Include) {
  checkAnswer(s"""select channelsId, avg(gamePointId+ 10) Total from Carbon_automation group by  channelsId order by Total""",
    s"""select channelsId, avg(gamePointId+ 10) Total from Carbon_automation_hive group by  channelsId order by Total""")
}
       

//TC_029
test("TC_029", Include) {
  checkAnswer(s"""select channelsId, avg(channelsId+ 10)  Total from Carbon_automation group by  channelsId order by Total""",
    s"""select channelsId, avg(channelsId+ 10)  Total from Carbon_automation_hive group by  channelsId order by Total""")
}
       

//TC_030
test("TC_030", Include) {
  checkAnswer(s"""select channelsId, avg(channelsId+channelsId) Total from Carbon_automation group by  channelsId order by Total""",
    s"""select channelsId, avg(channelsId+channelsId) Total from Carbon_automation_hive group by  channelsId order by Total""")
}
       

//TC_031
test("TC_031", Include) {
  checkAnswer(s"""select channelsId, count(Latest_DAY+ 10) as a  from Carbon_automation group by  channelsId""",
    s"""select channelsId, count(Latest_DAY+ 10) as a  from Carbon_automation_hive group by  channelsId""")
}
       

//TC_032
test("TC_032", Include) {
  checkAnswer(s"""select channelsId, count(gamePointId+ 10) Total from Carbon_automation group by  channelsId order by channelsId, Total""",
    s"""select channelsId, count(gamePointId+ 10) Total from Carbon_automation_hive group by  channelsId order by channelsId, Total""")
}
       

//TC_033
test("TC_033", Include) {
  checkAnswer(s"""select channelsId, count(channelsId+ 10) Total from Carbon_automation group by  channelsId order by Total""",
    s"""select channelsId, count(channelsId+ 10) Total from Carbon_automation_hive group by  channelsId order by Total""")
}
       

//TC_034
test("TC_034", Include) {
  checkAnswer(s"""select channelsId, count(channelsId+channelsId)  Total from Carbon_automation group by  channelsId order by Total""",
    s"""select channelsId, count(channelsId+channelsId)  Total from Carbon_automation_hive group by  channelsId order by Total""")
}
       

//TC_035
test("TC_035", Include) {
  checkAnswer(s"""select channelsId, min(Latest_DAY+ 10) as a  from Carbon_automation group by  channelsId order by channelsId""",
    s"""select channelsId, min(Latest_DAY+ 10) as a  from Carbon_automation_hive group by  channelsId order by channelsId""")
}
       

//TC_036
test("TC_036", Include) {
  checkAnswer(s"""select channelsId, min(gamePointId+ 10) Total from Carbon_automation group by  channelsId order by Total""",
    s"""select channelsId, min(gamePointId+ 10) Total from Carbon_automation_hive group by  channelsId order by Total""")
}
       

//TC_037
test("TC_037", Include) {
  checkAnswer(s"""select channelsId, min(channelsId+ 10) Total from Carbon_automation group by  channelsId order by Total""",
    s"""select channelsId, min(channelsId+ 10) Total from Carbon_automation_hive group by  channelsId order by Total""")
}
       

//TC_038
test("TC_038", Include) {
  checkAnswer(s"""select channelsId, min(channelsId+channelsId)  Total from Carbon_automation group by  channelsId order by Total""",
    s"""select channelsId, min(channelsId+channelsId)  Total from Carbon_automation_hive group by  channelsId order by Total""")
}
       

//TC_039
test("TC_039", Include) {
  checkAnswer(s"""select channelsId, max(Latest_DAY+ 10) as a  from Carbon_automation group by  channelsId order by channelsId""",
    s"""select channelsId, max(Latest_DAY+ 10) as a  from Carbon_automation_hive group by  channelsId order by channelsId""")
}
       

//TC_040
test("TC_040", Include) {
  checkAnswer(s"""select channelsId, max(gamePointId+ 10) Total from Carbon_automation group by  channelsId order by channelsId,Total""",
    s"""select channelsId, max(gamePointId+ 10) Total from Carbon_automation_hive group by  channelsId order by channelsId,Total""")
}
       

//TC_041
test("TC_041", Include) {
  checkAnswer(s"""select channelsId, max(channelsId+ 10) Total from Carbon_automation group by  channelsId order by Total""",
    s"""select channelsId, max(channelsId+ 10) Total from Carbon_automation_hive group by  channelsId order by Total""")
}
       

//TC_042
test("TC_042", Include) {
  checkAnswer(s"""select channelsId, max(channelsId+channelsId)  Total from Carbon_automation group by  channelsId order by Total""",
    s"""select channelsId, max(channelsId+channelsId)  Total from Carbon_automation_hive group by  channelsId order by Total""")
}
       

//TC_043
test("TC_043", Include) {
  checkAnswer(s"""select Latest_YEAR ,sum(distinct Latest_YEAR)+10 from Carbon_automation group by Latest_YEAR order by Latest_YEAR""",
    s"""select Latest_YEAR ,sum(distinct Latest_YEAR)+10 from Carbon_automation_hive group by Latest_YEAR order by Latest_YEAR""")
}
       

//TC_044
test("TC_044", Include) {
  checkAnswer(s"""select Latest_YEAR ,sum(distinct Latest_YEAR)+10 from Carbon_automation group by Latest_YEAR order by Latest_YEAR""",
    s"""select Latest_YEAR ,sum(distinct Latest_YEAR)+10 from Carbon_automation_hive group by Latest_YEAR order by Latest_YEAR""")
}
       

//TC_045
test("TC_045", Include) {
  checkAnswer(s"""select Latest_YEAR ,count(distinct Latest_YEAR) from Carbon_automation group by Latest_YEAR order by Latest_YEAR""",
    s"""select Latest_YEAR ,count(distinct Latest_YEAR) from Carbon_automation_hive group by Latest_YEAR order by Latest_YEAR""")
}
       

//TC_046
test("TC_046", Include) {
  checkAnswer(s"""select sum(deviceinformationid)+10 as a ,series  from Carbon_automation group by series""",
    s"""select sum(deviceinformationid)+10 as a ,series  from Carbon_automation_hive group by series""")
}
       

//TC_047
test("TC_047", Include) {
  checkAnswer(s"""select sum(gamepointid) +10 as a ,series  from Carbon_automation group by series""",
    s"""select sum(gamepointid) +10 as a ,series  from Carbon_automation_hive group by series""")
}
       

//TC_048
test("TC_048", Include) {
  checkAnswer(s"""select sum(latest_year)+10 as a ,series  from Carbon_automation group by series order by series""",
    s"""select sum(latest_year)+10 as a ,series  from Carbon_automation_hive group by series order by series""")
}
       

//TC_049
test("TC_049", Include) {
  checkAnswer(s"""select sum(deviceinformationid)+10.32 as a ,series  from Carbon_automation group by series order by series""",
    s"""select sum(deviceinformationid)+10.32 as a ,series  from Carbon_automation_hive group by series order by series""")
}
       

//TC_050
test("TC_050", Include) {
  checkAnswer(s"""select sum(gamepointid) +10.36 as a ,series  from Carbon_automation group by series""",
    s"""select sum(gamepointid) +10.36 as a ,series  from Carbon_automation_hive group by series""")
}
       

//TC_051
test("TC_051", Include) {
  checkAnswer(s"""select sum(latest_year)+10.364 as a,series  from Carbon_automation group by series order by series""",
    s"""select sum(latest_year)+10.364 as a,series  from Carbon_automation_hive group by series order by series""")
}
       

//TC_052
test("TC_052", Include) {
  checkAnswer(s"""select avg(deviceinformationid)+10 as a ,series  from Carbon_automation group by series""",
    s"""select avg(deviceinformationid)+10 as a ,series  from Carbon_automation_hive group by series""")
}
       

//TC_053
test("TC_053", Include) {
  checkAnswer(s"""select avg(gamepointid) +10 as a ,series  from Carbon_automation group by series""",
    s"""select avg(gamepointid) +10 as a ,series  from Carbon_automation_hive group by series""")
}
       

//TC_054
test("TC_054", Include) {
  checkAnswer(s"""select avg(latest_year)+10 as a ,series  from Carbon_automation group by series order by series""",
    s"""select avg(latest_year)+10 as a ,series  from Carbon_automation_hive group by series order by series""")
}
       

//TC_055
test("TC_055", Include) {
  checkAnswer(s"""select count(deviceinformationid)+10.32 as a ,series  from Carbon_automation group by series""",
    s"""select count(deviceinformationid)+10.32 as a ,series  from Carbon_automation_hive group by series""")
}
       

//TC_056
test("TC_056", Include) {
  checkAnswer(s"""select count(gamepointid) +10.36 as a ,series  from Carbon_automation group by series""",
    s"""select count(gamepointid) +10.36 as a ,series  from Carbon_automation_hive group by series""")
}
       

//TC_057
test("TC_057", Include) {
  checkAnswer(s"""select count(latest_year)+10.364 as a,series  from Carbon_automation group by series order by series""",
    s"""select count(latest_year)+10.364 as a,series  from Carbon_automation_hive group by series order by series""")
}
       

//TC_058
test("TC_058", Include) {
  checkAnswer(s"""select count(distinct series)+10 as a,series from Carbon_automation group by series""",
    s"""select count(distinct series)+10 as a,series from Carbon_automation_hive group by series""")
}
       

//TC_059
test("TC_059", Include) {
  checkAnswer(s"""select count(distinct gamepointid)+10 as a,series from Carbon_automation group by series""",
    s"""select count(distinct gamepointid)+10 as a,series from Carbon_automation_hive group by series""")
}
       

//TC_060
test("TC_060", Include) {
  checkAnswer(s"""select count(*) as a  from Carbon_automation""",
    s"""select count(*) as a  from Carbon_automation_hive""")
}
       

//TC_061
test("TC_061", Include) {
  checkAnswer(s"""Select count(1) as a  from Carbon_automation""",
    s"""Select count(1) as a  from Carbon_automation_hive""")
}
       

//TC_062
test("TC_062", Include) {
  checkAnswer(s"""select count(imei) as a   from Carbon_automation""",
    s"""select count(imei) as a   from Carbon_automation_hive""")
}
       

//TC_063
test("TC_063", Include) {
  checkAnswer(s"""select count(device_backColor)  as a from Carbon_automation""",
    s"""select count(device_backColor)  as a from Carbon_automation_hive""")
}
       

//TC_064
test("TC_064", Include) {
  checkAnswer(s"""select count(DISTINCT imei) as a  from Carbon_automation""",
    s"""select count(DISTINCT imei) as a  from Carbon_automation_hive""")
}
       

//TC_065
test("TC_065", Include) {
  checkAnswer(s"""select count(DISTINCT series) as a from Carbon_automation""",
    s"""select count(DISTINCT series) as a from Carbon_automation_hive""")
}
       

//TC_066
test("TC_066", Include) {
  checkAnswer(s"""select count(DISTINCT  device_backColor)  as a from Carbon_automation""",
    s"""select count(DISTINCT  device_backColor)  as a from Carbon_automation_hive""")
}
       

//TC_067
test("TC_067", Include) {
  checkAnswer(s"""select count (if(deviceInformationId>100,NULL,deviceInformationId))  a from Carbon_automation""",
    s"""select count (if(deviceInformationId>100,NULL,deviceInformationId))  a from Carbon_automation_hive""")
}
       

//TC_068
test("TC_068", Include) {
  checkAnswer(s"""select count (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation""",
    s"""select count (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_hive""")
}
       

//TC_069
test("TC_069", Include) {
  checkAnswer(s"""select count(gamePointId)  as a from Carbon_automation""",
    s"""select count(gamePointId)  as a from Carbon_automation_hive""")
}
       

//TC_070
test("TC_070", Include) {
  checkAnswer(s"""select count(DISTINCT gamePointId) as a from Carbon_automation""",
    s"""select count(DISTINCT gamePointId) as a from Carbon_automation_hive""")
}
       

//TC_071
test("TC_071", Include) {
  checkAnswer(s"""select sum(gamePointId) a  from Carbon_automation""",
    s"""select sum(gamePointId) a  from Carbon_automation_hive""")
}
       

//TC_072
test("TC_072", Include) {
  checkAnswer(s"""select sum(deviceInformationId) a  from Carbon_automation""",
    s"""select sum(deviceInformationId) a  from Carbon_automation_hive""")
}
       

//TC_073
test("TC_073", Include) {
  checkAnswer(s"""select sum(channelsId) a  from Carbon_automation""",
    s"""select sum(channelsId) a  from Carbon_automation_hive""")
}
       

//TC_074
test("TC_074", Include) {
  checkAnswer(s"""select sum(bomCode)  a  from Carbon_automation""",
    s"""select sum(bomCode)  a  from Carbon_automation_hive""")
}
       

//TC_075
test("TC_075", Include) {
  checkAnswer(s"""select sum(Latest_MONTH)  a  from Carbon_automation""",
    s"""select sum(Latest_MONTH)  a  from Carbon_automation_hive""")
}
       

//TC_076
test("TC_076", Include) {
  checkAnswer(s"""select sum( DISTINCT gamePointId) a  from Carbon_automation""",
    s"""select sum( DISTINCT gamePointId) a  from Carbon_automation_hive""")
}
       

//TC_077
test("TC_077", Include) {
  checkAnswer(s"""select sum(DISTINCT  deviceInformationId) a  from Carbon_automation""",
    s"""select sum(DISTINCT  deviceInformationId) a  from Carbon_automation_hive""")
}
       

//TC_078
test("TC_078", Include) {
  checkAnswer(s"""select sum( DISTINCT channelsId) a  from Carbon_automation""",
    s"""select sum( DISTINCT channelsId) a  from Carbon_automation_hive""")
}
       

//TC_079
test("TC_079", Include) {
  checkAnswer(s"""select sum (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation""",
    s"""select sum (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_hive""")
}
       

//TC_080
test("TC_080", Include) {
  checkAnswer(s"""select sum (if(deviceInformationId>100,NULL,deviceInformationId))  a from Carbon_automation""",
    s"""select sum (if(deviceInformationId>100,NULL,deviceInformationId))  a from Carbon_automation_hive""")
}
       

//TC_081
test("TC_081", Include) {
  checkAnswer(s"""select sum( DISTINCT Latest_MONTH)  a from Carbon_automation""",
    s"""select sum( DISTINCT Latest_MONTH)  a from Carbon_automation_hive""")
}
       

//TC_082
test("TC_082", Include) {
  checkAnswer(s"""select avg(gamePointId) a  from Carbon_automation""",
    s"""select avg(gamePointId) a  from Carbon_automation_hive""")
}
       

//TC_083
test("TC_083", Include) {
  checkAnswer(s"""select avg(deviceInformationId) a  from Carbon_automation""",
    s"""select avg(deviceInformationId) a  from Carbon_automation_hive""")
}
       

//TC_084
test("TC_084", Include) {
  checkAnswer(s"""select avg(channelsId) a  from Carbon_automation""",
    s"""select avg(channelsId) a  from Carbon_automation_hive""")
}
       

//TC_085
test("TC_085", Include) {
  checkAnswer(s"""select avg(bomCode)  a  from Carbon_automation""",
    s"""select avg(bomCode)  a  from Carbon_automation_hive""")
}
       

//TC_086
test("TC_086", Include) {
  checkAnswer(s"""select avg(Latest_MONTH)  a  from Carbon_automation""",
    s"""select avg(Latest_MONTH)  a  from Carbon_automation_hive""")
}
       

//TC_087
test("TC_087", Include) {
  checkAnswer(s"""select avg (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation""",
    s"""select avg (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_hive""")
}
       

//TC_088
test("TC_088", Include) {
  checkAnswer(s"""select avg (if(deviceInformationId>100,NULL,deviceInformationId))  a from Carbon_automation""",
    s"""select avg (if(deviceInformationId>100,NULL,deviceInformationId))  a from Carbon_automation_hive""")
}
       

//TC_089
test("TC_089", Include) {
  checkAnswer(s"""select min(gamePointId) a  from Carbon_automation""",
    s"""select min(gamePointId) a  from Carbon_automation_hive""")
}
       

//TC_090
test("TC_090", Include) {
  checkAnswer(s"""select min(deviceInformationId) a  from Carbon_automation""",
    s"""select min(deviceInformationId) a  from Carbon_automation_hive""")
}
       

//TC_091
test("TC_091", Include) {
  checkAnswer(s"""select min(channelsId) a  from Carbon_automation""",
    s"""select min(channelsId) a  from Carbon_automation_hive""")
}
       

//TC_092
test("TC_092", Include) {
  checkAnswer(s"""select min(bomCode)  a  from Carbon_automation""",
    s"""select min(bomCode)  a  from Carbon_automation_hive""")
}
       

//TC_093
test("TC_093", Include) {
  checkAnswer(s"""select min(Latest_MONTH)  a  from Carbon_automation""",
    s"""select min(Latest_MONTH)  a  from Carbon_automation_hive""")
}
       

//TC_094
test("TC_094", Include) {
  checkAnswer(s"""select min (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation""",
    s"""select min (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_hive""")
}
       

//TC_095
test("TC_095", Include) {
  checkAnswer(s"""select min (if(deviceInformationId>100,NULL,deviceInformationId))  a from Carbon_automation""",
    s"""select min (if(deviceInformationId>100,NULL,deviceInformationId))  a from Carbon_automation_hive""")
}
       

//TC_096
test("TC_096", Include) {
  checkAnswer(s"""select max(gamePointId) a  from Carbon_automation""",
    s"""select max(gamePointId) a  from Carbon_automation_hive""")
}
       

//TC_097
test("TC_097", Include) {
  checkAnswer(s"""select max(deviceInformationId) a  from Carbon_automation""",
    s"""select max(deviceInformationId) a  from Carbon_automation_hive""")
}
       

//TC_098
test("TC_098", Include) {
  checkAnswer(s"""select max(channelsId) a  from Carbon_automation""",
    s"""select max(channelsId) a  from Carbon_automation_hive""")
}
       

//TC_099
test("TC_099", Include) {
  checkAnswer(s"""select max(bomCode)  a  from Carbon_automation""",
    s"""select max(bomCode)  a  from Carbon_automation_hive""")
}
       

//TC_100
test("TC_100", Include) {
  checkAnswer(s"""select max(Latest_MONTH)  a  from Carbon_automation""",
    s"""select max(Latest_MONTH)  a  from Carbon_automation_hive""")
}
       

//TC_101
test("TC_101", Include) {
  checkAnswer(s"""select max (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation""",
    s"""select max (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_hive""")
}
       

//TC_102
test("TC_102", Include) {
  checkAnswer(s"""select max (if(deviceInformationId>100,NULL,deviceInformationId))  a from Carbon_automation""",
    s"""select max (if(deviceInformationId>100,NULL,deviceInformationId))  a from Carbon_automation_hive""")
}
       

//TC_103
ignore("TC_103", Include) {
  sql(s"""select variance(deviceInformationId) as a   from Carbon_automation""").collect
}
       

//TC_104
  ignore("TC_104", Include) {
  sql(s"""select var_pop(deviceInformationId)  as a from Carbon_automation""").collect
}
       

//TC_105
  ignore("TC_105", Include) {
  sql(s"""select var_samp(deviceInformationId) as a  from Carbon_automation""").collect
}
       

//TC_106
  ignore("TC_106", Include) {
  sql(s"""select stddev_pop(deviceInformationId) as a  from Carbon_automation""").collect
}
       

//TC_107
  ignore("TC_107", Include) {
  sql(s"""select stddev_samp(deviceInformationId)  as a from Carbon_automation""").collect
}
       

//TC_108
  ignore("TC_108", Include) {
  sql(s"""select covar_pop(deviceInformationId,deviceInformationId) as a  from Carbon_automation""").collect
}
       

//TC_109
  ignore("TC_109", Include) {
  sql(s"""select covar_samp(deviceInformationId,deviceInformationId) as a  from Carbon_automation""").collect
}
       

//TC_110
test("TC_110", Include) {
  checkAnswer(s"""select corr(deviceInformationId,deviceInformationId)  as a from Carbon_automation""",
    s"""select corr(deviceInformationId,deviceInformationId)  as a from Carbon_automation_hive""")
}
       

//TC_111
test("TC_111", Include) {
  checkAnswer(s"""select percentile(deviceInformationId,0.2) as  a  from Carbon_automation""",
    s"""select percentile(deviceInformationId,0.2) as  a  from Carbon_automation_hive""")
}
       

//TC_112
test("TC_112", Include) {
  checkAnswer(s"""select percentile(deviceInformationId,array(0,0.2,0.3,1))  as  a from Carbon_automation""",
    s"""select percentile(deviceInformationId,array(0,0.2,0.3,1))  as  a from Carbon_automation_hive""")
}
       

//TC_113
test("TC_113", Include) {
  checkAnswer(s"""select percentile_approx(deviceInformationId,0.2) as a  from Carbon_automation""",
    s"""select percentile_approx(deviceInformationId,0.2) as a  from Carbon_automation_hive""")
}
       

//TC_114
test("TC_114", Include) {
  checkAnswer(s"""select percentile_approx(deviceInformationId,0.2,5) as a  from Carbon_automation""",
    s"""select percentile_approx(deviceInformationId,0.2,5) as a  from Carbon_automation_hive""")
}
       

//TC_115
test("TC_115", Include) {
  checkAnswer(s"""select percentile_approx(deviceInformationId,array(0.2,0.3,0.99))  as a from Carbon_automation""",
    s"""select percentile_approx(deviceInformationId,array(0.2,0.3,0.99))  as a from Carbon_automation_hive""")
}
       

//TC_116
test("TC_116", Include) {
  sql(s"""select percentile_approx(deviceInformationId,array(0.2,0.3,0.99),5) as a from Carbon_automation""").collect
}
       

//TC_117
test("TC_117", Include) {
  sql(s"""select histogram_numeric(deviceInformationId,2)  as a from Carbon_automation""").collect
}
       

//TC_118
test("TC_118", Include) {
  sql(s"""select * from (select collect_set(series) as myseries from Carbon_automation) aa sort by myseries""").collect
}
       

//TC_119
test("TC_119", Include) {
  sql(s"""select variance(gamePointId) as a   from Carbon_automation""").collect
}
       

//TC_120
test("TC_120", Include) {
  sql(s"""select var_pop(gamePointId)  as a from Carbon_automation""").collect
}
       

//TC_121
test("TC_121", Include) {
  sql(s"""select var_samp(gamePointId) as a  from Carbon_automation""").collect
}
       

//TC_122
test("TC_122", Include) {
  sql(s"""select stddev_pop(gamePointId) as a  from Carbon_automation""").collect
}
       

//TC_123
test("TC_123", Include) {
  sql(s"""select stddev_samp(gamePointId)  as a from Carbon_automation""").collect
}
       

//TC_124
test("TC_124", Include) {
  sql(s"""select covar_pop(gamePointId,gamePointId) as a  from Carbon_automation""").collect
}
       

//TC_125
test("TC_125", Include) {
  sql(s"""select covar_samp(gamePointId,gamePointId) as a  from Carbon_automation""").collect
}
       

//TC_126
test("TC_126", Include) {
  checkAnswer(s"""select corr(gamePointId,gamePointId)  as a from Carbon_automation""",
    s"""select corr(gamePointId,gamePointId)  as a from Carbon_automation_hive""")
}
       

//TC_127
test("TC_127", Include) {
  checkAnswer(s"""select percentile(deviceInformationId,0.2) as  a  from Carbon_automation""",
    s"""select percentile(deviceInformationId,0.2) as  a  from Carbon_automation_hive""")
}
       

//TC_128
test("TC_128", Include) {
  checkAnswer(s"""select percentile(deviceInformationId,array(0,0.2,0.3,1))  as  a from Carbon_automation""",
    s"""select percentile(deviceInformationId,array(0,0.2,0.3,1))  as  a from Carbon_automation_hive""")
}
       

//TC_129
test("TC_129", Include) {
  checkAnswer(s"""select percentile_approx(gamePointId,0.2) as a  from Carbon_automation""",
    s"""select percentile_approx(gamePointId,0.2) as a  from Carbon_automation_hive""")
}
       

//TC_130
test("TC_130", Include) {
  checkAnswer(s"""select percentile_approx(gamePointId,0.2,5) as a  from Carbon_automation""",
    s"""select percentile_approx(gamePointId,0.2,5) as a  from Carbon_automation_hive""")
}
       

//TC_131
test("TC_131", Include) {
  checkAnswer(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99))  as a from Carbon_automation""",
    s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99))  as a from Carbon_automation_hive""")
}
       

//TC_132
test("TC_132", Include) {
  sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99),5) as a from Carbon_automation""").collect
}
       

//TC_133
test("TC_133", Include) {
  sql(s"""select histogram_numeric(gamePointId,2)  as a from Carbon_automation""").collect
}
       

//TC_134
test("TC_134", Include) {
  sql(s"""select last(imei) a from Carbon_automation""").collect
}
       

//TC_135
test("TC_135", Include) {
  sql(s"""select FIRST(imei) a from Carbon_automation""").collect
}
       

//TC_136
test("TC_136", Include) {
  checkAnswer(s"""select series,count(imei) a from Carbon_automation group by series order by series""",
    s"""select series,count(imei) a from Carbon_automation_hive group by series order by series""")
}
       

//TC_137
test("TC_137", Include) {
  checkAnswer(s"""select series,count(imei) a from Carbon_automation group by series order by series,a""",
    s"""select series,count(imei) a from Carbon_automation_hive group by series order by series,a""")
}
       

//TC_138
test("TC_138", Include) {
  checkAnswer(s"""select series,ActiveProvince,count(imei)  a from Carbon_automation group by ActiveProvince,series order by series,ActiveProvince""",
    s"""select series,ActiveProvince,count(imei)  a from Carbon_automation_hive group by ActiveProvince,series order by series,ActiveProvince""")
}
       

//TC_139
test("TC_139", Include) {
  checkAnswer(s"""select count(distinct deviceColor) a,deliveryProvince from Carbon_automation group by deliveryProvince""",
    s"""select count(distinct deviceColor) a,deliveryProvince from Carbon_automation_hive group by deliveryProvince""")
}
       

//TC_140
test("TC_140", Include) {
  checkAnswer(s"""select count(distinct deviceColor) a,deliveryProvince,series from Carbon_automation group by deliveryProvince,series order by deliveryProvince,series""",
    s"""select count(distinct deviceColor) a,deliveryProvince,series from Carbon_automation_hive group by deliveryProvince,series order by deliveryProvince,series""")
}
       

//TC_141
test("TC_141", Include) {
  checkAnswer(s"""select series,sum(gamePointId) a from Carbon_automation group by series order by series""",
    s"""select series,sum(gamePointId) a from Carbon_automation_hive group by series order by series""")
}
       

//TC_142
test("TC_142", Include) {
  checkAnswer(s"""select deliveryCountry,deliveryProvince,series,sum(gamePointId) a from Carbon_automation group by deliveryCountry,deliveryProvince,series order by deliveryCountry,deliveryProvince,series""",
    s"""select deliveryCountry,deliveryProvince,series,sum(gamePointId) a from Carbon_automation_hive group by deliveryCountry,deliveryProvince,series order by deliveryCountry,deliveryProvince,series""")
}
       

//TC_146
test("TC_146", Include) {
  checkAnswer(s"""select series,avg(gamePointId) a from Carbon_automation group by series order by series""",
    s"""select series,avg(gamePointId) a from Carbon_automation_hive group by series order by series""")
}
       

//TC_147
test("TC_147", Include) {
  checkAnswer(s"""select deliveryCountry,deliveryProvince,series,avg(gamePointId) a from Carbon_automation group by deliveryCountry,deliveryProvince,series order by deliveryCountry,deliveryProvince,series""",
    s"""select deliveryCountry,deliveryProvince,series,avg(gamePointId) a from Carbon_automation_hive group by deliveryCountry,deliveryProvince,series order by deliveryCountry,deliveryProvince,series""")
}
       

//TC_151
test("TC_151", Include) {
  checkAnswer(s"""select series,min(gamePointId) a from Carbon_automation group by series order by series""",
    s"""select series,min(gamePointId) a from Carbon_automation_hive group by series order by series""")
}
       

//TC_152
test("TC_152", Include) {
  checkAnswer(s"""select deliveryCountry,deliveryProvince,series,min(gamePointId) a from Carbon_automation group by deliveryCountry,deliveryProvince,series order by deliveryCountry,deliveryProvince,series""",
    s"""select deliveryCountry,deliveryProvince,series,min(gamePointId) a from Carbon_automation_hive group by deliveryCountry,deliveryProvince,series order by deliveryCountry,deliveryProvince,series""")
}
       

//TC_156
test("TC_156", Include) {
  checkAnswer(s"""select series,max(gamePointId) a from Carbon_automation group by series order by series""",
    s"""select series,max(gamePointId) a from Carbon_automation_hive group by series order by series""")
}
       

//TC_157
test("TC_157", Include) {
  checkAnswer(s"""select deliveryCountry,deliveryProvince,series,max(gamePointId) a from Carbon_automation group by deliveryCountry,deliveryProvince,series order by deliveryCountry,deliveryProvince,series""",
    s"""select deliveryCountry,deliveryProvince,series,max(gamePointId) a from Carbon_automation_hive group by deliveryCountry,deliveryProvince,series order by deliveryCountry,deliveryProvince,series""")
}
       

//TC_162
test("TC_162", Include) {
  checkAnswer(s"""select imei,series from Carbon_automation where Carbon_automation.series IN ('1Series','7Series')""",
    s"""select imei,series from Carbon_automation_hive where Carbon_automation_hive.series IN ('1Series','7Series')""")
}
       

//TC_163
test("TC_163", Include) {
  checkAnswer(s"""select imei,series from Carbon_automation where Carbon_automation.series  NOT IN ('1Series','7Series')""",
    s"""select imei,series from Carbon_automation_hive where Carbon_automation_hive.series  NOT IN ('1Series','7Series')""")
}
       

//TC_166
test("TC_166", Include) {
  checkAnswer(s"""select Upper(series) a  from Carbon_automation""",
    s"""select Upper(series) a  from Carbon_automation_hive""")
}
       

//TC_167
test("TC_167", Include) {
  checkAnswer(s"""select Upper(Latest_DAY) a  from Carbon_automation""",
    s"""select Upper(Latest_DAY) a  from Carbon_automation_hive""")
}
       

//TC_168
test("TC_168", Include) {
  checkAnswer(s"""select imei,series from Carbon_automation limit 10""",
    s"""select imei,series from Carbon_automation_hive limit 10""")
}
       

//TC_169
test("TC_169", Include) {
  checkAnswer(s"""select imei,series,gamePointId from Carbon_automation limit 10""",
    s"""select imei,series,gamePointId from Carbon_automation_hive limit 10""")
}
       

//TC_170
test("TC_170", Include) {
  checkAnswer(s"""select Upper(series) a ,channelsId from Carbon_automation group by series,channelsId order by a,channelsId""",
    s"""select Upper(series) a ,channelsId from Carbon_automation_hive group by series,channelsId order by a,channelsId""")
}
       

//TC_171
test("TC_171", Include) {
  checkAnswer(s"""select Lower(series) a  from Carbon_automation""",
    s"""select Lower(series) a  from Carbon_automation_hive""")
}
       

//TC_172
test("TC_172", Include) {
  checkAnswer(s"""select Lower(Latest_DAY) a  from Carbon_automation""",
    s"""select Lower(Latest_DAY) a  from Carbon_automation_hive""")
}
       

//TC_173
test("TC_173", Include) {
  checkAnswer(s"""select distinct  Latest_DAY from Carbon_automation""",
    s"""select distinct  Latest_DAY from Carbon_automation_hive""")
}
       

//TC_174
test("TC_174", Include) {
  checkAnswer(s"""select distinct gamePointId from Carbon_automation""",
    s"""select distinct gamePointId from Carbon_automation_hive""")
}
       

//TC_175
test("TC_175", Include) {
  checkAnswer(s"""select distinct  channelsId from Carbon_automation""",
    s"""select distinct  channelsId from Carbon_automation_hive""")
}
       

//TC_176
test("TC_176", Include) {
  checkAnswer(s"""select distinct  series from Carbon_automation""",
    s"""select distinct  series from Carbon_automation_hive""")
}
       

//TC_179
test("TC_179", Include) {
  checkAnswer(s"""select imei,series from Carbon_automation limit 101""",
    s"""select imei,series from Carbon_automation_hive limit 101""")
}
       

//TC_180
test("TC_180", Include) {
  checkAnswer(s"""select series,sum(gamePointId) a from Carbon_automation group by series order by series desc""",
    s"""select series,sum(gamePointId) a from Carbon_automation_hive group by series order by series desc""")
}
       

//TC_181
test("TC_181", Include) {
  checkAnswer(s"""select series,sum(gamePointId) a from Carbon_automation group by series order by a desc""",
    s"""select series,sum(gamePointId) a from Carbon_automation_hive group by series order by a desc""")
}
       

//TC_182
test("TC_182", Include) {
  checkAnswer(s"""select series,sum(gamePointId) a from Carbon_automation group by series order by series desc ,a desc""",
    s"""select series,sum(gamePointId) a from Carbon_automation_hive group by series order by series desc ,a desc""")
}
       

//TC_183
test("TC_183", Include) {
  checkAnswer(s"""select series,sum(gamePointId) a from Carbon_automation group by series order by series asc""",
    s"""select series,sum(gamePointId) a from Carbon_automation_hive group by series order by series asc""")
}
       

//TC_184
test("TC_184", Include) {
  checkAnswer(s"""select series,sum(gamePointId) a from Carbon_automation group by series order by a asc""",
    s"""select series,sum(gamePointId) a from Carbon_automation_hive group by series order by a asc""")
}
       

//TC_185
test("TC_185", Include) {
  checkAnswer(s"""select series,sum(gamePointId) a from Carbon_automation group by series order by series asc ,a asc""",
    s"""select series,sum(gamePointId) a from Carbon_automation_hive group by series order by series asc ,a asc""")
}
       

//TC_186
test("TC_186", Include) {
  checkAnswer(s"""select series,sum(gamePointId) a from Carbon_automation group by series order by series desc ,a asc""",
    s"""select series,sum(gamePointId) a from Carbon_automation_hive group by series order by series desc ,a asc""")
}
       

//TC_187
test("TC_187", Include) {
  checkAnswer(s"""select series,ActiveProvince,sum(gamePointId) a from Carbon_automation group by series,ActiveProvince order by series desc,ActiveProvince asc""",
    s"""select series,ActiveProvince,sum(gamePointId) a from Carbon_automation_hive group by series,ActiveProvince order by series desc,ActiveProvince asc""")
}
       

//TC_188
test("TC_188", Include) {
  checkAnswer(s"""select series,gamePointId as a from Carbon_automation  order by a asc limit 10""",
    s"""select series,gamePointId as a from Carbon_automation_hive  order by a asc limit 10""")
}
       

//TC_189
test("TC_189", Include) {
  checkAnswer(s"""select series,gamePointId as a from Carbon_automation  order by a desc limit 10""",
    s"""select series,gamePointId as a from Carbon_automation_hive  order by a desc limit 10""")
}
       

//TC_190
test("TC_190", Include) {
  sql(s"""select series,gamePointId as a from Carbon_automation  order by series asc limit 10""").collect
}
       

//TC_191
test("TC_191", Include) {
  sql(s"""select series,gamePointId as a from Carbon_automation  order by series desc limit 10""").collect
}
       

//TC_193
test("TC_193", Include) {
  checkAnswer(s"""select contractNumber,gamePointId,series from Carbon_automation where  (contractNumber == 5281803) and (gamePointId==2738.562)""",
    s"""select contractNumber,gamePointId,series from Carbon_automation_hive where  (contractNumber == 5281803) and (gamePointId==2738.562)""")
}
       

//TC_194
test("TC_194", Include) {
  checkAnswer(s"""select contractNumber,gamePointId,series from Carbon_automation where series='8Series'""",
    s"""select contractNumber,gamePointId,series from Carbon_automation_hive where series='8Series'""")
}
       

//TC_195
test("TC_195", Include) {
  checkAnswer(s"""select contractNumber,gamePointId,series from Carbon_automation where series='8Series' and internalModels='8Internal models'""",
    s"""select contractNumber,gamePointId,series from Carbon_automation_hive where series='8Series' and internalModels='8Internal models'""")
}
       

//TC_196
test("TC_196", Include) {
  checkAnswer(s"""select contractNumber,gamePointId,series from Carbon_automation where series='8Series' or  internalModels='8Internal models'""",
    s"""select contractNumber,gamePointId,series from Carbon_automation_hive where series='8Series' or  internalModels='8Internal models'""")
}
       

//TC_197
test("TC_197", Include) {
  checkAnswer(s"""select contractNumber,gamePointId,series from Carbon_automation where series='8Series' or series='7Series'""",
    s"""select contractNumber,gamePointId,series from Carbon_automation_hive where series='8Series' or series='7Series'""")
}
       

//TC_198
test("TC_198", Include) {
  checkAnswer(s"""select contractNumber,gamePointId,series from Carbon_automation where gamePointId=2738.562""",
    s"""select contractNumber,gamePointId,series from Carbon_automation_hive where gamePointId=2738.562""")
}
       

//TC_199
test("TC_199", Include) {
  checkAnswer(s"""select contractNumber,gamePointId,series from Carbon_automation where deviceInformationId=10""",
    s"""select contractNumber,gamePointId,series from Carbon_automation_hive where deviceInformationId=10""")
}
       

//TC_200
test("TC_200", Include) {
  checkAnswer(s"""select contractNumber,gamePointId,series  from Carbon_automation where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
    s"""select contractNumber,gamePointId,series  from Carbon_automation_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""")
}
       

//TC_201
test("TC_201", Include) {
  checkAnswer(s"""select contractNumber,gamePointId,series from Carbon_automation where series !='8Series'""",
    s"""select contractNumber,gamePointId,series from Carbon_automation_hive where series !='8Series'""")
}
       

//TC_202
test("TC_202", Include) {
  checkAnswer(s"""select contractNumber,gamePointId,series from Carbon_automation where series !='8Series' and internalModels !='8Internal models'""",
    s"""select contractNumber,gamePointId,series from Carbon_automation_hive where series !='8Series' and internalModels !='8Internal models'""")
}
       

//TC_203
test("TC_203", Include) {
  checkAnswer(s"""select contractNumber,gamePointId,series from Carbon_automation where series !='8Series' or  internalModels !='8Internal models'""",
    s"""select contractNumber,gamePointId,series from Carbon_automation_hive where series !='8Series' or  internalModels !='8Internal models'""")
}
       

//TC_204
test("TC_204", Include) {
  checkAnswer(s"""select contractNumber,gamePointId,series from Carbon_automation where series !='8Series' or series !='7Series'""",
    s"""select contractNumber,gamePointId,series from Carbon_automation_hive where series !='8Series' or series !='7Series'""")
}
       

//TC_205
test("TC_205", Include) {
  checkAnswer(s"""select contractNumber,gamePointId,series from Carbon_automation where gamePointId !=2738.562""",
    s"""select contractNumber,gamePointId,series from Carbon_automation_hive where gamePointId !=2738.562""")
}
       

//TC_206
test("TC_206", Include) {
  checkAnswer(s"""select contractNumber,gamePointId,series from Carbon_automation where deviceInformationId !=10""",
    s"""select contractNumber,gamePointId,series from Carbon_automation_hive where deviceInformationId !=10""")
}
       

//TC_207
test("TC_207", Include) {
  checkAnswer(s"""select contractNumber,gamePointId,series from Carbon_automation where gamePointId >2738.562""",
    s"""select contractNumber,gamePointId,series from Carbon_automation_hive where gamePointId >2738.562""")
}
       

//TC_208
test("TC_208", Include) {
  checkAnswer(s"""select Latest_DAY as a from Carbon_automation where Latest_DAY<=>Latest_areaId""",
    s"""select Latest_DAY as a from Carbon_automation_hive where Latest_DAY<=>Latest_areaId""")
}
       

//TC_210
test("TC_210", Include) {
  checkAnswer(s"""select Latest_DAY  from Carbon_automation where Latest_DAY<>Latest_areaId""",
    s"""select Latest_DAY  from Carbon_automation_hive where Latest_DAY<>Latest_areaId""")
}
       

//TC_211
test("TC_211", Include) {
  checkAnswer(s"""select Latest_DAY from Carbon_automation where Latest_DAY != Latest_areaId""",
    s"""select Latest_DAY from Carbon_automation_hive where Latest_DAY != Latest_areaId""")
}
       

//TC_212
test("TC_212", Include) {
  checkAnswer(s"""select imei, Latest_DAY from Carbon_automation where Latest_DAY<Latest_areaId""",
    s"""select imei, Latest_DAY from Carbon_automation_hive where Latest_DAY<Latest_areaId""")
}
       

//TC_213
test("TC_213", Include) {
  checkAnswer(s"""select imei, Latest_DAY from Carbon_automation where Latest_DAY<=Latest_areaId""",
    s"""select imei, Latest_DAY from Carbon_automation_hive where Latest_DAY<=Latest_areaId""")
}
       

//TC_214
test("TC_214", Include) {
  checkAnswer(s"""select imei, Latest_DAY from Carbon_automation where Latest_DAY>Latest_areaId""",
    s"""select imei, Latest_DAY from Carbon_automation_hive where Latest_DAY>Latest_areaId""")
}
       

//TC_215
test("TC_215", Include) {
  checkAnswer(s"""select imei, Latest_DAY from Carbon_automation where Latest_DAY>=Latest_areaId""",
    s"""select imei, Latest_DAY from Carbon_automation_hive where Latest_DAY>=Latest_areaId""")
}
       

//TC_216
test("TC_216", Include) {
  checkAnswer(s"""select imei, Latest_DAY from Carbon_automation where Latest_DAY NOT BETWEEN Latest_areaId AND  Latest_HOUR""",
    s"""select imei, Latest_DAY from Carbon_automation_hive where Latest_DAY NOT BETWEEN Latest_areaId AND  Latest_HOUR""")
}
       

//TC_217
test("TC_217", Include) {
  checkAnswer(s"""select imei, Latest_DAY from Carbon_automation where Latest_DAY BETWEEN Latest_areaId AND  Latest_HOUR""",
    s"""select imei, Latest_DAY from Carbon_automation_hive where Latest_DAY BETWEEN Latest_areaId AND  Latest_HOUR""")
}
       

//TC_218
test("TC_218", Include) {
  checkAnswer(s"""select imei, Latest_DAY from Carbon_automation where Latest_DAY IS NULL""",
    s"""select imei, Latest_DAY from Carbon_automation_hive where Latest_DAY IS NULL""")
}
       

//TC_219
test("TC_219", Include) {
  checkAnswer(s"""select imei, Latest_DAY from Carbon_automation where Latest_DAY IS NOT NULL""",
    s"""select imei, Latest_DAY from Carbon_automation_hive where Latest_DAY IS NOT NULL""")
}
       

//TC_220
test("TC_220", Include) {
  checkAnswer(s"""select imei, Latest_DAY from Carbon_automation where imei IS NOT NULL""",
    s"""select imei, Latest_DAY from Carbon_automation_hive where imei IS NOT NULL""")
}
       

//TC_221
test("TC_221", Include) {
  checkAnswer(s"""select imei, Latest_DAY from Carbon_automation where Latest_DAY NOT LIKE Latest_areaId AND Latest_DAY NOT LIKE  Latest_HOUR""",
    s"""select imei, Latest_DAY from Carbon_automation_hive where Latest_DAY NOT LIKE Latest_areaId AND Latest_DAY NOT LIKE  Latest_HOUR""")
}
       

//TC_222
test("TC_222", Include) {
  checkAnswer(s"""select imei, Latest_DAY from Carbon_automation where Latest_DAY  LIKE Latest_areaId AND  Latest_DAY  LIKE Latest_HOUR""",
    s"""select imei, Latest_DAY from Carbon_automation_hive where Latest_DAY  LIKE Latest_areaId AND  Latest_DAY  LIKE Latest_HOUR""")
}
       

//TC_225
test("TC_225", Include) {
  checkAnswer(s"""select imei,gamePointId from Carbon_automation where gamePointId >505""",
    s"""select imei,gamePointId from Carbon_automation_hive where gamePointId >505""")
}
       

//TC_226
test("TC_226", Include) {
  checkAnswer(s"""select imei,gamePointId from Carbon_automation where gamePointId <505""",
    s"""select imei,gamePointId from Carbon_automation_hive where gamePointId <505""")
}
       

//TC_227
test("TC_227", Include) {
  sql(s"""select imei,gamePointId from Carbon_automation where channelsId <2""").collect
}
       

//TC_228
test("TC_228", Include) {
  checkAnswer(s"""select imei,gamePointId from Carbon_automation where channelsId >2""",
    s"""select imei,gamePointId from Carbon_automation_hive where channelsId >2""")
}
       

//TC_229
test("TC_229", Include) {
  checkAnswer(s"""select imei,gamePointId from Carbon_automation where gamePointId >=505""",
    s"""select imei,gamePointId from Carbon_automation_hive where gamePointId >=505""")
}
       

//TC_230
test("TC_230", Include) {
  checkAnswer(s"""select imei,gamePointId from Carbon_automation where gamePointId <=505""",
    s"""select imei,gamePointId from Carbon_automation_hive where gamePointId <=505""")
}
       

//TC_231
test("TC_231", Include) {
  checkAnswer(s"""select imei,gamePointId from Carbon_automation where channelsId <=2""",
    s"""select imei,gamePointId from Carbon_automation_hive where channelsId <=2""")
}
       

//TC_232
test("TC_232", Include) {
  sql(s"""select imei,gamePointId from Carbon_automation where channelsId >=2""").collect
}
       

//TC_233
test("TC_233", Include) {
  sql(s"""select imei,gamePointId, channelsId,series  from Carbon_automation where channelsId >=10 OR channelsId <=1 and series='7Series'""").collect
}
       

//TC_234
test("TC_234", Include) {
  sql(s"""select imei,gamePointId, channelsId,series  from Carbon_automation where channelsId >=10 OR channelsId <=1 or series='7Series'""").collect
}
       

//TC_236
test("TC_236", Include) {
  sql(s"""select sum(gamePointId) a from Carbon_automation where channelsId >=10 OR (channelsId <=1 and series='1Series')""").collect
}
       

//TC_237
test("TC_237", Include) {
  checkAnswer(s"""select * from (select imei,if(imei='1AA100060',NULL,imei) a from Carbon_automation) aa  where a IS NULL""",
    s"""select * from (select imei,if(imei='1AA100060',NULL,imei) a from Carbon_automation_hive) aa  where a IS NULL""")
}
       

//TC_257
test("TC_257", Include) {
  checkAnswer(s"""select imei,internalModels,series from Carbon_automation where (series='8Series' and internalModels='8Internal models') OR (series='7Series' and internalModels='7Internal models')""",
    s"""select imei,internalModels,series from Carbon_automation_hive where (series='8Series' and internalModels='8Internal models') OR (series='7Series' and internalModels='7Internal models')""")
}
       

//TC_259
test("TC_259", Include) {
  checkAnswer(s"""select imei,internalModels,series from Carbon_automation where (series='8Series' or internalModels='8Internal models') and (series='7Series' or internalModels='7Internal models')
""",
    s"""select imei,internalModels,series from Carbon_automation_hive where (series='8Series' or internalModels='8Internal models') and (series='7Series' or internalModels='7Internal models')
""")
}
       

//TC_260
test("TC_260", Include) {
  checkAnswer(s"""select imei,internalModels,series from Carbon_automation where (series='8Series' and internalModels='8Internal models') or (deviceInformationId is not NULL)""",
    s"""select imei,internalModels,series from Carbon_automation_hive where (series='8Series' and internalModels='8Internal models') or (deviceInformationId is not NULL)""")
}
       

//TC_261
test("TC_261", Include) {
  checkAnswer(s"""select  imei from Carbon_automation where UPPER(Latest_province) == 'GUANGDONG PROVINCE'""",
    s"""select  imei from Carbon_automation_hive where UPPER(Latest_province) == 'GUANGDONG PROVINCE'""")
}
       

//TC_262
test("TC_262", Include) {
  checkAnswer(s"""select count(imei) ,series from Carbon_automation group by series having sum (Latest_DAY) == 99""",
    s"""select count(imei) ,series from Carbon_automation_hive group by series having sum (Latest_DAY) == 99""")
}
       

//TC_274
test("TC_274", Include) {
  checkAnswer(s"""SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamepointid) AS Sum_gamepointid FROM  Carbon_automation group by ActiveCountry,ActiveDistrict,Activecity""",
    s"""SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamepointid) AS Sum_gamepointid FROM  Carbon_automation_hive group by ActiveCountry,ActiveDistrict,Activecity""")
}
       

//TC_283
test("TC_283", Include) {
  checkAnswer(s"""select  AMSize,sum( gamePointId+ contractNumber) as total from Carbon_automation where AMSize='0RAM size' and  ActiveProvince='Guangdong Province' group by AMSize""",
    s"""select  AMSize,sum( gamePointId+ contractNumber) as total from Carbon_automation_hive where AMSize='0RAM size' and  ActiveProvince='Guangdong Province' group by AMSize""")
}
       

//TC_284
test("TC_284", Include) {
  checkAnswer(s"""select AMSize,sum(gamePointId+contractNumber) as total from Carbon_automation where  CUPAudit='0CPU Audit' group by AMSize having total > 10 order by total desc
""",
    s"""select AMSize,sum(gamePointId+contractNumber) as total from Carbon_automation_hive where  CUPAudit='0CPU Audit' group by AMSize having total > 10 order by total desc
""")
}
       

//TC_285
test("TC_285", Include) {
  checkAnswer(s"""select AMSize,sum(gamePointId+contractNumber) as total from Carbon_automation where CUPAudit='0CPU Audit' group by AMSize having total > 10 order by total desc""",
    s"""select AMSize,sum(gamePointId+contractNumber) as total from Carbon_automation_hive where CUPAudit='0CPU Audit' group by AMSize having total > 10 order by total desc""")
}
       

//TC_286
test("TC_286", Include) {
  checkAnswer(s"""select  ActiveAreaId,count(distinct AMSize) as AMSize_number, sum(gamePointId+contractNumber) as total from Carbon_automation group by ActiveAreaId""",
    s"""select  ActiveAreaId,count(distinct AMSize) as AMSize_number, sum(gamePointId+contractNumber) as total from Carbon_automation_hive group by ActiveAreaId""")
}
       

//TC_287
test("TC_287", Include) {
  checkAnswer(s"""select ActiveAreaId,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation where ActiveAreaId='6'group by ActiveAreaId order by AMSize_number desc""",
    s"""select ActiveAreaId,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation_hive where ActiveAreaId='6'group by ActiveAreaId order by AMSize_number desc""")
}
       

//TC_288
test("TC_288", Include) {
  checkAnswer(s"""select ActiveAreaId,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation group by ActiveAreaId""",
    s"""select ActiveAreaId,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation_hive group by ActiveAreaId""")
}
       

//TC_289
test("TC_289", Include) {
  checkAnswer(s"""select ActiveAreaId,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation where ActiveAreaId='6' and ActiveProvince='Hubei Province' group by ActiveAreaId order by AMSize_number desc""",
    s"""select ActiveAreaId,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation_hive where ActiveAreaId='6' and ActiveProvince='Hubei Province' group by ActiveAreaId order by AMSize_number desc""")
}
       

//TC_290
test("TC_290", Include) {
  checkAnswer(s"""select ActiveProvince,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation group by ActiveProvince""",
    s"""select ActiveProvince,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation_hive group by ActiveProvince""")
}
       

//TC_291
test("TC_291", Include) {
  checkAnswer(s"""select AMSize,count(distinct imei) as imei_number,sum(gamePointId+contractNumber) as total from Carbon_automation where AMSize='0RAM size' group by AMSize order by imei_number desc""",
    s"""select AMSize,count(distinct imei) as imei_number,sum(gamePointId+contractNumber) as total from Carbon_automation_hive where AMSize='0RAM size' group by AMSize order by imei_number desc""")
}
       

//TC_292
test("TC_292", Include) {
  checkAnswer(s"""select ActiveProvince,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation group by ActiveProvince""",
    s"""select ActiveProvince,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation_hive group by ActiveProvince""")
}
       

//TC_293
test("TC_293", Include) {
  checkAnswer(s"""select ActiveProvince,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation where CUPAudit='0CPU Audit' group by ActiveProvince order by total desc""",
    s"""select ActiveProvince,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation_hive where CUPAudit='0CPU Audit' group by ActiveProvince order by total desc""")
}
       

//TC_294
test("TC_294", Include) {
  checkAnswer(s"""select  ActiveOperatorId,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation group by  ActiveOperatorId""",
    s"""select  ActiveOperatorId,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation_hive group by  ActiveOperatorId""")
}
       

//TC_295
test("TC_295", Include) {
  checkAnswer(s"""select ActiveOperatorId,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation where ActiveOperatorId='100000' group by ActiveOperatorId order by total desc""",
    s"""select ActiveOperatorId,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation_hive where ActiveOperatorId='100000' group by ActiveOperatorId order by total desc""")
}
       

//TC_296
test("TC_296", Include) {
  checkAnswer(s"""select ActiveAreaId,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation group by ActiveAreaId""",
    s"""select ActiveAreaId,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation_hive group by ActiveAreaId""")
}
       

//TC_297
test("TC_297", Include) {
  sql(s"""select  Latest_DAY,Latest_HOUR,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation group by Latest_DAY,Latest_HOUR""").collect
}
       

//TC_298
test("TC_298", Include) {
  sql(s"""select Latest_DAY,Latest_HOUR,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation where Latest_HOUR between 12 and 15 group by Latest_DAY,Latest_HOUR order by total desc""").collect
}
       

//TC_299
test("TC_299", Include) {
  checkAnswer(s"""select  Activecity,ActiveProvince,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation group by Activecity,ActiveProvince""",
    s"""select  Activecity,ActiveProvince,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation_hive group by Activecity,ActiveProvince""")
}
       

//TC_300
test("TC_300", Include) {
  checkAnswer(s"""select Activecity,ActiveProvince,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation where  ActiveProvince='Guangdong Province' group by Activecity,ActiveProvince order by total desc""",
    s"""select Activecity,ActiveProvince,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation_hive where  ActiveProvince='Guangdong Province' group by Activecity,ActiveProvince order by total desc""")
}
       

//TC_301
test("TC_301", Include) {
  checkAnswer(s"""select ActiveOperatorId,Activecity,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation group by Activecity,ActiveOperatorId""",
    s"""select ActiveOperatorId,Activecity,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation_hive group by Activecity,ActiveOperatorId""")
}
       

//TC_302
test("TC_302", Include) {
  checkAnswer(s"""select ActiveOperatorId,Activecity,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation where  ActiveAreaId='6' and ActiveOperatorId='100004' group by ActiveOperatorId,Activecity order by total desc""",
    s"""select ActiveOperatorId,Activecity,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation_hive where  ActiveAreaId='6' and ActiveOperatorId='100004' group by ActiveOperatorId,Activecity order by total desc""")
}
       

//TC_303
test("TC_303", Include) {
  checkAnswer(s"""select ActiveAreaId,Activecity,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation  group by Activecity,ActiveAreaId""",
    s"""select ActiveAreaId,Activecity,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation_hive  group by Activecity,ActiveAreaId""")
}
       

//TC_304
test("TC_304", Include) {
  checkAnswer(s"""select ActiveAreaId,Activecity,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation where  ActiveAreaId='6' group by Activecity,ActiveAreaId order by total desc""",
    s"""select ActiveAreaId,Activecity,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation_hive where  ActiveAreaId='6' group by Activecity,ActiveAreaId order by total desc""")
}
       

//TC_305
test("TC_305", Include) {
  checkAnswer(s"""select ActiveAreaId,Activecity,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation  group by Activecity,ActiveAreaId""",
    s"""select ActiveAreaId,Activecity,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation_hive  group by Activecity,ActiveAreaId""")
}
       

//TC_306
test("TC_306", Include) {
  checkAnswer(s"""select ActiveAreaId,Activecity,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation where  ActiveOperatorId in('100000','100004') group by Activecity,ActiveAreaId order by total desc""",
    s"""select ActiveAreaId,Activecity,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation_hive where  ActiveOperatorId in('100000','100004') group by Activecity,ActiveAreaId order by total desc""")
}
       

//TC_307
test("TC_307", Include) {
  checkAnswer(s"""select ActiveOperatorId,ActiveAreaId,Activecity,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation group by ActiveOperatorId,Activecity,ActiveAreaId""",
    s"""select ActiveOperatorId,ActiveAreaId,Activecity,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) as total from Carbon_automation_hive group by ActiveOperatorId,Activecity,ActiveAreaId""")
}
       

//TC_308
test("TC_308", Include) {
  checkAnswer(s"""select t2.AMSize,t1.Activecity,count(t1.AMSize) as AMSize_number,sum(t1.gamePointId+t1.contractNumber) as total from Carbon_automation t1, Carbon_automation t2 where t1.AMSize=t2.AMSize group by t1.Activecity,t2.AMSize""",
    s"""select t2.AMSize,t1.Activecity,count(t1.AMSize) as AMSize_number,sum(t1.gamePointId+t1.contractNumber) as total from Carbon_automation_hive t1, Carbon_automation_hive t2 where t1.AMSize=t2.AMSize group by t1.Activecity,t2.AMSize""")
}
       

//TC_309
test("TC_309", Include) {
  checkAnswer(s"""select t2.AMSize,t1.Activecity,count(t1.AMSize) as AMSize_number,sum(t1.gamePointId+t1.contractNumber) as total from Carbon_automation t1, Carbon_automation t2 where t1.ActiveOperatorId='100004' and t1.AMSize=t2.AMSize group by t1.Activecity,t2.AMSize""",
    s"""select t2.AMSize,t1.Activecity,count(t1.AMSize) as AMSize_number,sum(t1.gamePointId+t1.contractNumber) as total from Carbon_automation_hive t1, Carbon_automation_hive t2 where t1.ActiveOperatorId='100004' and t1.AMSize=t2.AMSize group by t1.Activecity,t2.AMSize""")
}
       

//TC_310
test("TC_310", Include) {
  checkAnswer(s"""select t2.AMSize,t1.Activecity,count(t1.AMSize) as AMSize_number,sum(t1.gamePointId+t1.contractNumber) as total from Carbon_automation t1, Carbon_automation t2 where t1.ActiveOperatorId='100004' and t1.AMSize=t2.AMSize group by t1.Activecity,t2.AMSize order by total desc""",
    s"""select t2.AMSize,t1.Activecity,count(t1.AMSize) as AMSize_number,sum(t1.gamePointId+t1.contractNumber) as total from Carbon_automation_hive t1, Carbon_automation_hive t2 where t1.ActiveOperatorId='100004' and t1.AMSize=t2.AMSize group by t1.Activecity,t2.AMSize order by total desc""")
}
       

//TC_311
test("TC_311", Include) {
  checkAnswer(s"""select t2.AMSize,t1.ActiveAreaId,count(t1.AMSize) as AMSize_number,sum(t1.gamePointId+t1.contractNumber) as total from Carbon_automation t1, Carbon_automation t2 where t1.AMSize=t2.AMSize group by t1.ActiveAreaId,t2.AMSize""",
    s"""select t2.AMSize,t1.ActiveAreaId,count(t1.AMSize) as AMSize_number,sum(t1.gamePointId+t1.contractNumber) as total from Carbon_automation_hive t1, Carbon_automation_hive t2 where t1.AMSize=t2.AMSize group by t1.ActiveAreaId,t2.AMSize""")
}
       

//TC_312
test("TC_312", Include) {
  sql(s"""select ActiveOperatorId, sum(imeiupdown) as total, count(distinct AMSize) as AMSize_count from (select AMSize, t1.gamePointId+t1.contractNumber as imeiupdown, if((t1.gamePointId+t1.contractNumber)>100, '>50', if((t1.gamePointId+t1.contractNumber)>100,'50~10',if((t1.gamePointId+t1.contractNumber)>100, '10~1','<1'))) as ActiveOperatorId from Carbon_automation t1) t2 group by ActiveOperatorId""").collect
}
       

//TC_313
test("TC_313", Include) {
  sql(s"""select ActiveOperatorId, sum(imeiupdown) as total, count(distinct AMSize) as AMSize_count from (select AMSize, t1.gamePointId+ t1.contractNumber as imeiupdown, if((t1.gamePointId+ t1.contractNumber)>100, '>50', if((t1.gamePointId+t1.contractNumber)>100,'50~10',if((t1.gamePointId+t1.contractNumber)>100, '10~1','<1'))) as ActiveOperatorId from Carbon_automation t1) t2 group by ActiveOperatorId""").collect
}
       

//TC_314
test("TC_314", Include) {
  checkAnswer(s"""SELECT AMSize, ActiveAreaId, SUM(gamepointid) AS Sum_gamepointid FROM (select * from Carbon_automation) SUB_QRY WHERE AMSize BETWEEN "0RAM size" AND "1RAM size" GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC
""",
    s"""SELECT AMSize, ActiveAreaId, SUM(gamepointid) AS Sum_gamepointid FROM (select * from Carbon_automation_hive) SUB_QRY WHERE AMSize BETWEEN "0RAM size" AND "1RAM size" GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC
""")
}
       

//TC_315
test("TC_315", Include) {
  checkAnswer(s"""SELECT AMSize, ActiveAreaId, imei FROM (select * from Carbon_automation) SUB_QRY WHERE  Latest_DAY BETWEEN 1 AND 1""",
    s"""SELECT AMSize, ActiveAreaId, imei FROM (select * from Carbon_automation_hive) SUB_QRY WHERE  Latest_DAY BETWEEN 1 AND 1""")
}
       

//TC_316
test("TC_316", Include) {
  checkAnswer(s"""select series,gamepointid from Carbon_automation where gamepointid between 1407 and 1407""",
    s"""select series,gamepointid from Carbon_automation_hive where gamepointid between 1407 and 1407""")
}
       

//TC_317
test("TC_317", Include) {
  checkAnswer(s"""select channelsId from Carbon_automation order by  channelsId""",
    s"""select channelsId from Carbon_automation_hive order by  channelsId""")
}
       

//TC_318
test("TC_318", Include) {
  checkAnswer(s"""select count(series),series from Carbon_automation group by series having series='6Series'""",
    s"""select count(series),series from Carbon_automation_hive group by series having series='6Series'""")
}
       

//TC_319
test("TC_319", Include) {
  checkAnswer(s"""SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation) SUB_QRY GROUP BY ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity ASC""",
    s"""SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity ASC""")
}
       

//TC_320
test("TC_320", Include) {
  checkAnswer(s"""SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation) SUB_QRY WHERE gamePointId > 1.0E9 GROUP BY ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity ASC""",
    s"""SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY WHERE gamePointId > 1.0E9 GROUP BY ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity ASC""")
}
       

//TC_321
test("TC_321", Include) {
  checkAnswer(s"""SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation) SUB_QRY WHERE imei = "1AA100000" GROUP BY ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity ASC""",
    s"""SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY WHERE imei = "1AA100000" GROUP BY ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, Activecity ASC""")
}
       

//TC_322
test("TC_322", Include) {
  checkAnswer(s"""SELECT ActiveCountry, ActiveDistrict, deliveryCity, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * from Carbon_automation) SUB_QRY WHERE series = "5Series" GROUP BY ActiveCountry, ActiveDistrict, deliveryCity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, deliveryCity ASC""",
    s"""SELECT ActiveCountry, ActiveDistrict, deliveryCity, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * from Carbon_automation_hive) SUB_QRY WHERE series = "5Series" GROUP BY ActiveCountry, ActiveDistrict, deliveryCity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, deliveryCity ASC""")
}
       

//TC_323
test("TC_323", Include) {
  checkAnswer(s"""SELECT ActiveCountry, ActiveDistrict, deliveryCity, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * from Carbon_automation) SUB_QRY WHERE deliveryCity = "zhuzhou" GROUP BY ActiveCountry, ActiveDistrict, deliveryCity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, deliveryCity ASC""",
    s"""SELECT ActiveCountry, ActiveDistrict, deliveryCity, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * from Carbon_automation_hive) SUB_QRY WHERE deliveryCity = "zhuzhou" GROUP BY ActiveCountry, ActiveDistrict, deliveryCity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, deliveryCity ASC""")
}
       

//TC_324
test("TC_324", Include) {
  checkAnswer(s"""SELECT modelId, ActiveCountry, ActiveDistrict, deliveryCity, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * from Carbon_automation) SUB_QRY WHERE modelId > "2000" GROUP BY modelId, ActiveCountry, ActiveDistrict, deliveryCity ORDER BY modelId ASC, ActiveCountry ASC, ActiveDistrict ASC, deliveryCity ASC""",
    s"""SELECT modelId, ActiveCountry, ActiveDistrict, deliveryCity, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * from Carbon_automation_hive) SUB_QRY WHERE modelId > "2000" GROUP BY modelId, ActiveCountry, ActiveDistrict, deliveryCity ORDER BY modelId ASC, ActiveCountry ASC, ActiveDistrict ASC, deliveryCity ASC""")
}
       

//TC_325
test("TC_325", Include) {
  checkAnswer(s"""SELECT imei, deliveryCity, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation) SUB_QRY WHERE modelId > "2000" GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC""",
    s"""SELECT imei, deliveryCity, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY WHERE modelId > "2000" GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC""")
}
       

//TC_326
test("TC_326", Include) {
  checkAnswer(s"""SELECT imei, deliveryCity, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation) SUB_QRY WHERE imei >= "1AA1000000" GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC""",
    s"""SELECT imei, deliveryCity, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY WHERE imei >= "1AA1000000" GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC""")
}
       

//TC_327
test("TC_327", Include) {
  checkAnswer(s"""SELECT imei, deliveryCity, COUNT(Latest_YEAR) AS Count_Latest_YEAR, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation) SUB_QRY WHERE imei >= "1AA1000000" GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC""",
    s"""SELECT imei, deliveryCity, COUNT(Latest_YEAR) AS Count_Latest_YEAR, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY WHERE imei >= "1AA1000000" GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC""")
}
       

//TC_328
test("TC_328", Include) {
  checkAnswer(s"""SELECT imei, deliveryCity, series, Latest_YEAR, gamePointId FROM (select * from Carbon_automation) SUB_QRY WHERE imei >= "1AA1000000" ORDER BY series ASC""",
    s"""SELECT imei, deliveryCity, series, Latest_YEAR, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY WHERE imei >= "1AA1000000" ORDER BY series ASC""")
}
       

//TC_329
test("TC_329", Include) {
  checkAnswer(s"""SELECT imei, deliveryCity, series, SUM(gamePointId) AS Sum_gamePointId, COUNT(Latest_DAY) AS Count_Latest_DAY FROM (select * from Carbon_automation) SUB_QRY WHERE imei >= "1AA1000000" GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""",
    s"""SELECT imei, deliveryCity, series, SUM(gamePointId) AS Sum_gamePointId, COUNT(Latest_DAY) AS Count_Latest_DAY FROM (select * from Carbon_automation_hive) SUB_QRY WHERE imei >= "1AA1000000" GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""")
}
       

//TC_330
test("TC_330", Include) {
  checkAnswer(s"""SELECT deliveryCity, channelsId, SUM(deviceInformationId) AS Sum_deviceInformationId FROM (select * from Carbon_automation) SUB_QRY GROUP BY deliveryCity, channelsId ORDER BY deliveryCity ASC, channelsId ASC""",
    s"""SELECT deliveryCity, channelsId, SUM(deviceInformationId) AS Sum_deviceInformationId FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY deliveryCity, channelsId ORDER BY deliveryCity ASC, channelsId ASC""")
}
       

//TC_331
test("TC_331", Include) {
  checkAnswer(s"""SELECT series, imei, deliveryCity, SUM(Latest_DAY) AS Sum_Latest_DAY, SUM(Latest_MONTH) AS Sum_Latest_MONTH, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(contractNumber) AS Sum_contractNumber, SUM(deviceInformationId) AS Sum_deviceInformationId, AVG(gamePointId) AS Avg_gamePointId, SUM(gamepointid) AS Sum_gamepointid FROM (select * from Carbon_automation) SUB_QRY GROUP BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC""",
    s"""SELECT series, imei, deliveryCity, SUM(Latest_DAY) AS Sum_Latest_DAY, SUM(Latest_MONTH) AS Sum_Latest_MONTH, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(contractNumber) AS Sum_contractNumber, SUM(deviceInformationId) AS Sum_deviceInformationId, AVG(gamePointId) AS Avg_gamePointId, SUM(gamepointid) AS Sum_gamepointid FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC""")
}
       

//TC_332
test("TC_332", Include) {
  checkAnswer(s"""SELECT imei, deliveryCity, SUM(Latest_DAY) AS Sum_Latest_DAY, SUM(Latest_MONTH) AS Sum_Latest_MONTH, SUM(contractNumber) AS Sum_contractNumber, SUM(deviceInformationId) AS Sum_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId, SUM(gamepointid) AS Sum_gamepointid, COUNT(series) AS Count_series FROM (select * from Carbon_automation) SUB_QRY GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC""",
    s"""SELECT imei, deliveryCity, SUM(Latest_DAY) AS Sum_Latest_DAY, SUM(Latest_MONTH) AS Sum_Latest_MONTH, SUM(contractNumber) AS Sum_contractNumber, SUM(deviceInformationId) AS Sum_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId, SUM(gamepointid) AS Sum_gamepointid, COUNT(series) AS Count_series FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC""")
}
       

//TC_333
test("TC_333", Include) {
  checkAnswer(s"""SELECT imei, deliveryCity, SUM(Latest_DAY) AS Sum_Latest_DAY, SUM(Latest_MONTH) AS Sum_Latest_MONTH, SUM(contractNumber) AS Sum_contractNumber, SUM(deviceInformationId) AS Sum_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId, SUM(gamepointid) AS Sum_gamepointid, COUNT(DISTINCT series) AS DistinctCount_series FROM (select * from Carbon_automation) SUB_QRY GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC""",
    s"""SELECT imei, deliveryCity, SUM(Latest_DAY) AS Sum_Latest_DAY, SUM(Latest_MONTH) AS Sum_Latest_MONTH, SUM(contractNumber) AS Sum_contractNumber, SUM(deviceInformationId) AS Sum_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId, SUM(gamepointid) AS Sum_gamepointid, COUNT(DISTINCT series) AS DistinctCount_series FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC""")
}
       

//TC_334
test("TC_334", Include) {
  checkAnswer(s"""SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * from Carbon_automation) SUB_QRY WHERE series = "7Series" GROUP BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC""",
    s"""SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * from Carbon_automation_hive) SUB_QRY WHERE series = "7Series" GROUP BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC""")
}
       

//TC_335
test("TC_335", Include) {
  checkAnswer(s"""SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * from Carbon_automation) SUB_QRY WHERE series > "5Series" GROUP BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC""",
    s"""SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * from Carbon_automation_hive) SUB_QRY WHERE series > "5Series" GROUP BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC""")
}
       

//TC_336
test("TC_336", Include) {
  checkAnswer(s"""SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * from Carbon_automation) SUB_QRY WHERE series >= "4Series" GROUP BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC""",
    s"""SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * from Carbon_automation_hive) SUB_QRY WHERE series >= "4Series" GROUP BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC""")
}
       

//TC_337
test("TC_337", Include) {
  checkAnswer(s"""SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * from Carbon_automation) SUB_QRY WHERE series < "3Series" GROUP BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC""",
    s"""SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * from Carbon_automation_hive) SUB_QRY WHERE series < "3Series" GROUP BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC""")
}
       

//TC_338
test("TC_338", Include) {
  checkAnswer(s"""SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * from Carbon_automation) SUB_QRY WHERE series <= "5Series" GROUP BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC""",
    s"""SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * from Carbon_automation_hive) SUB_QRY WHERE series <= "5Series" GROUP BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC""")
}
       

//TC_339
test("TC_339", Include) {
  checkAnswer(s"""SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * from Carbon_automation) SUB_QRY WHERE deliveryCity LIKE '%wuhan%' GROUP BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC""",
    s"""SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * from Carbon_automation_hive) SUB_QRY WHERE deliveryCity LIKE '%wuhan%' GROUP BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC""")
}
       

//TC_340
test("TC_340", Include) {
  checkAnswer(s"""SELECT latest_year, latest_day, imei, gamepointid, deviceinformationid, series, imei, deliverycity FROM (select * from Carbon_automation) SUB_QRY WHERE NOT(gamepointid > 2000.0)""",
    s"""SELECT latest_year, latest_day, imei, gamepointid, deviceinformationid, series, imei, deliverycity FROM (select * from Carbon_automation_hive) SUB_QRY WHERE NOT(gamepointid > 2000.0)""")
}
       

//TC_341
test("TC_341", Include) {
  checkAnswer(s"""SELECT gamePointId, deviceInformationId, Latest_YEAR, series, imei, deliveryCity FROM (select * from Carbon_automation) SUB_QRY WHERE imei = "1AA10000"""",
    s"""SELECT gamePointId, deviceInformationId, Latest_YEAR, series, imei, deliveryCity FROM (select * from Carbon_automation_hive) SUB_QRY WHERE imei = "1AA10000"""")
}
       

//TC_342
test("TC_342", Include) {
  checkAnswer(s"""SELECT latest_year, latest_day, imei, gamepointid, deviceinformationid, series, imei, deliverycity FROM (select * from Carbon_automation) SUB_QRY WHERE NOT(gamepointid = 1600)""",
    s"""SELECT latest_year, latest_day, imei, gamepointid, deviceinformationid, series, imei, deliverycity FROM (select * from Carbon_automation_hive) SUB_QRY WHERE NOT(gamepointid = 1600)""")
}
       

//TC_343
test("TC_343", Include) {
  checkAnswer(s"""SELECT latest_year, latest_day, imei, gamepointid, deviceinformationid, series, imei, deliverycity FROM (select * from Carbon_automation) SUB_QRY WHERE deliverycity IS NULL""",
    s"""SELECT latest_year, latest_day, imei, gamepointid, deviceinformationid, series, imei, deliverycity FROM (select * from Carbon_automation_hive) SUB_QRY WHERE deliverycity IS NULL""")
}
       

//TC_344
test("TC_344", Include) {
  checkAnswer(s"""SELECT gamePointId, deviceInformationId, Latest_YEAR, series, imei, deliveryCity FROM (select * from Carbon_automation) SUB_QRY WHERE deliveryCity LIKE 'wu%'""",
    s"""SELECT gamePointId, deviceInformationId, Latest_YEAR, series, imei, deliveryCity FROM (select * from Carbon_automation_hive) SUB_QRY WHERE deliveryCity LIKE 'wu%'""")
}
       

//TC_345
test("TC_345", Include) {
  checkAnswer(s"""SELECT gamepointid, deviceinformationid, latest_year, series, imei, deliverycity FROM (select * from Carbon_automation) SUB_QRY WHERE NOT(imei LIKE '%1AA10%')""",
    s"""SELECT gamepointid, deviceinformationid, latest_year, series, imei, deliverycity FROM (select * from Carbon_automation_hive) SUB_QRY WHERE NOT(imei LIKE '%1AA10%')""")
}
       

//TC_346
test("TC_346", Include) {
  checkAnswer(s"""SELECT gamepointid, deviceinformationid, latest_year, series, imei, deliverycity FROM (select * from Carbon_automation) SUB_QRY WHERE gamepointid BETWEEN 1015 AND 1080""",
    s"""SELECT gamepointid, deviceinformationid, latest_year, series, imei, deliverycity FROM (select * from Carbon_automation_hive) SUB_QRY WHERE gamepointid BETWEEN 1015 AND 1080""")
}
       

//TC_347
test("TC_347", Include) {
  checkAnswer(s"""SELECT gamepointid, deviceinformationid, latest_year, series, imei, deliverycity FROM (select * from Carbon_automation) SUB_QRY WHERE deviceinformationid = 100031""",
    s"""SELECT gamepointid, deviceinformationid, latest_year, series, imei, deliverycity FROM (select * from Carbon_automation_hive) SUB_QRY WHERE deviceinformationid = 100031""")
}
       

//TC_348
test("TC_348", Include) {
  checkAnswer(s"""SELECT latest_year, gamepointid FROM (select * from Carbon_automation) SUB_QRY""",
    s"""SELECT latest_year, gamepointid FROM (select * from Carbon_automation_hive) SUB_QRY""")
}
       

//TC_349
test("TC_349", Include) {
  checkAnswer(s"""SELECT latest_year, gamepointid FROM (select * from Carbon_automation) SUB_QRY WHERE gamepointid BETWEEN 200.0 AND 300.0""",
    s"""SELECT latest_year, gamepointid FROM (select * from Carbon_automation_hive) SUB_QRY WHERE gamepointid BETWEEN 200.0 AND 300.0""")
}
       

//TC_350
test("TC_350", Include) {
  checkAnswer(s"""SELECT imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId FROM (select * from Carbon_automation) SUB_QRY WHERE NOT(deliveryCity = "wuhan") GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC""",
    s"""SELECT imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId FROM (select * from Carbon_automation_hive) SUB_QRY WHERE NOT(deliveryCity = "wuhan") GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC""")
}
       

//TC_351
test("TC_351", Include) {
  checkAnswer(s"""SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * from Carbon_automation) SUB_QRY WHERE deliveryCity IN ("changsha")""",
    s"""SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * from Carbon_automation_hive) SUB_QRY WHERE deliveryCity IN ("changsha")""")
}
       

//TC_352
test("TC_352", Include) {
  checkAnswer(s"""SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * from Carbon_automation) SUB_QRY WHERE deliveryCity IS NOT NULL""",
    s"""SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * from Carbon_automation_hive) SUB_QRY WHERE deliveryCity IS NOT NULL""")
}
       

//TC_353
test("TC_353", Include) {
  checkAnswer(s"""SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * from Carbon_automation) SUB_QRY WHERE NOT(deliveryCity IN ("guangzhou","changsha"))""",
    s"""SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * from Carbon_automation_hive) SUB_QRY WHERE NOT(deliveryCity IN ("guangzhou","changsha"))""")
}
       

//TC_354
test("TC_354", Include) {
  checkAnswer(s"""SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * from Carbon_automation) SUB_QRY WHERE NOT(imei BETWEEN "1AA100" AND "1AA10000")""",
    s"""SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * from Carbon_automation_hive) SUB_QRY WHERE NOT(imei BETWEEN "1AA100" AND "1AA10000")""")
}
       

//TC_355
test("TC_355", Include) {
  checkAnswer(s"""SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM (select * from Carbon_automation) SUB_QRY WHERE NOT(series BETWEEN "2Series" AND "5Series")""",
    s"""SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM (select * from Carbon_automation_hive) SUB_QRY WHERE NOT(series BETWEEN "2Series" AND "5Series")""")
}
       

//TC_356
test("TC_356", Include) {
  checkAnswer(s"""SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM (select * from Carbon_automation) SUB_QRY WHERE NOT(gamePointId >= 1000.0)""",
    s"""SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM (select * from Carbon_automation_hive) SUB_QRY WHERE NOT(gamePointId >= 1000.0)""")
}
       

//TC_357
test("TC_357", Include) {
  checkAnswer(s"""SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM (select * from Carbon_automation) SUB_QRY WHERE NOT(gamePointId < 500.0)""",
    s"""SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM (select * from Carbon_automation_hive) SUB_QRY WHERE NOT(gamePointId < 500.0)""")
}
       

//TC_358
test("TC_358", Include) {
  checkAnswer(s"""SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM (select * from Carbon_automation) SUB_QRY WHERE NOT(gamePointId <= 600.0)""",
    s"""SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM (select * from Carbon_automation_hive) SUB_QRY WHERE NOT(gamePointId <= 600.0)""")
}
       

//TC_359
test("TC_359", Include) {
  checkAnswer(s"""SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM (select * from Carbon_automation) SUB_QRY WHERE NOT(deliveryCity LIKE '%wuhan%')""",
    s"""SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM (select * from Carbon_automation_hive) SUB_QRY WHERE NOT(deliveryCity LIKE '%wuhan%')""")
}
       

//TC_360
test("TC_360", Include) {
  checkAnswer(s"""SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM (select * from Carbon_automation) SUB_QRY WHERE NOT(deliveryCity LIKE 'wu%')""",
    s"""SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM (select * from Carbon_automation_hive) SUB_QRY WHERE NOT(deliveryCity LIKE 'wu%')""")
}
       

//TC_361
test("TC_361", Include) {
  checkAnswer(s"""SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM (select * from Carbon_automation) SUB_QRY WHERE deliveryCity IN ("changsha")""",
    s"""SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM (select * from Carbon_automation_hive) SUB_QRY WHERE deliveryCity IN ("changsha")""")
}
       

//TC_362
test("TC_362", Include) {
  checkAnswer(s"""SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM (select * from Carbon_automation) SUB_QRY""",
    s"""SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY""")
}
       

//TC_363
test("TC_363", Include) {
  checkAnswer(s"""SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM (select * from Carbon_automation) SUB_QRY ORDER BY deviceInformationId ASC""",
    s"""SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ORDER BY deviceInformationId ASC""")
}
       

//TC_364
test("TC_364", Include) {
  checkAnswer(s"""SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM (select * from Carbon_automation) SUB_QRY ORDER BY deviceInformationId DESC""",
    s"""SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ORDER BY deviceInformationId DESC""")
}
       

//TC_365
test("TC_365", Include) {
  checkAnswer(s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""",
    s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""")
}
       

//TC_366
test("TC_366", Include) {
  checkAnswer(s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, AVG(deviceInformationId) AS Avg_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""",
    s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, AVG(deviceInformationId) AS Avg_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""")
}
       

//TC_367
test("TC_367", Include) {
  checkAnswer(s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, COUNT(deviceInformationId) AS Count_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""",
    s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, COUNT(deviceInformationId) AS Count_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""")
}
       

//TC_368
test("TC_368", Include) {
  checkAnswer(s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, COUNT(DISTINCT deviceInformationId) AS LONG_COL_0, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""",
    s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, COUNT(DISTINCT deviceInformationId) AS LONG_COL_0, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""")
}
       

//TC_369
test("TC_369", Include) {
  checkAnswer(s"""SELECT series, gamePointId, deviceInformationId, Latest_YEAR, imei, deliveryCity FROM (select * from Carbon_automation) SUB_QRY ORDER BY imei ASC, deliveryCity ASC, series ASC""",
    s"""SELECT series, gamePointId, deviceInformationId, Latest_YEAR, imei, deliveryCity FROM (select * from Carbon_automation_hive) SUB_QRY ORDER BY imei ASC, deliveryCity ASC, series ASC""")
}
       

//TC_370
test("TC_370", Include) {
  checkAnswer(s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, MAX(deviceInformationId) AS Max_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""",
    s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, MAX(deviceInformationId) AS Max_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""")
}
       

//TC_371
test("TC_371", Include) {
  checkAnswer(s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, MIN(deviceInformationId) AS Min_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""",
    s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, MIN(deviceInformationId) AS Min_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""")
}
       

//TC_372
test("TC_372", Include) {
  checkAnswer(s"""SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM (select * from Carbon_automation) SUB_QRY ORDER BY gamePointId ASC""",
    s"""SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ORDER BY gamePointId ASC""")
}
       

//TC_373
test("TC_373", Include) {
  checkAnswer(s"""SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM (select * from Carbon_automation) SUB_QRY ORDER BY gamePointId DESC""",
    s"""SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ORDER BY gamePointId DESC""")
}
       

//TC_374
test("TC_374", Include) {
  checkAnswer(s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId, AVG(gamePointId) AS Avg_gamePointId FROM (select * from Carbon_automation) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""",
    s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId, AVG(gamePointId) AS Avg_gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""")
}
       

//TC_375
test("TC_375", Include) {
  checkAnswer(s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId, COUNT(gamePointId) AS Count_gamePointId FROM (select * from Carbon_automation) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""",
    s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId, COUNT(gamePointId) AS Count_gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""")
}
       

//TC_376
test("TC_376", Include) {
  checkAnswer(s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId, COUNT(DISTINCT gamePointId) AS DistinctCount_gamePointId FROM (select * from Carbon_automation) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""",
    s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId, COUNT(DISTINCT gamePointId) AS DistinctCount_gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""")
}
       

//TC_377
test("TC_377", Include) {
  checkAnswer(s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId, MAX(gamePointId) AS Max_gamePointId FROM (select * from Carbon_automation) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""",
    s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId, MAX(gamePointId) AS Max_gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""")
}
       

//TC_378
test("TC_378", Include) {
  checkAnswer(s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId, MIN(gamePointId) AS Min_gamePointId FROM (select * from Carbon_automation) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""",
    s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId, MIN(gamePointId) AS Min_gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC""")
}
       

//TC_379
test("TC_379", Include) {
  checkAnswer(s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId, MIN(gamePointId) AS Min_gamePointId FROM (select * from Carbon_automation) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei DESC, deliveryCity ASC, series ASC""",
    s"""SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId, MIN(gamePointId) AS Min_gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei DESC, deliveryCity ASC, series ASC""")
}
       

//TC_380
test("TC_380", Include) {
  checkAnswer(s"""SELECT deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId, COUNT(imei) AS Count_imei, MIN(gamePointId) AS Min_gamePointId FROM (select * from Carbon_automation) SUB_QRY GROUP BY deliveryCity, series ORDER BY deliveryCity ASC, series ASC""",
    s"""SELECT deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId, COUNT(imei) AS Count_imei, MIN(gamePointId) AS Min_gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY deliveryCity, series ORDER BY deliveryCity ASC, series ASC""")
}
       

//TC_381
test("TC_381", Include) {
  checkAnswer(s"""SELECT deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId, COUNT(DISTINCT imei) AS DistinctCount_imei, MIN(gamePointId) AS Min_gamePointId FROM (select * from Carbon_automation) SUB_QRY GROUP BY deliveryCity, series ORDER BY deliveryCity ASC, series ASC""",
    s"""SELECT deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId, COUNT(DISTINCT imei) AS DistinctCount_imei, MIN(gamePointId) AS Min_gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY deliveryCity, series ORDER BY deliveryCity ASC, series ASC""")
}
       

//TC_382
test("TC_382", Include) {
  checkAnswer(s"""SELECT deliveryCity, Latest_YEAR, imei, SUM(gamePointId) AS Sum_gamePointId, SUM(deviceInformationId) AS Sum_deviceInformationId FROM (select * from Carbon_automation) SUB_QRY GROUP BY deliveryCity, Latest_YEAR, imei ORDER BY deliveryCity ASC, Latest_YEAR ASC, imei ASC""",
    s"""SELECT deliveryCity, Latest_YEAR, imei, SUM(gamePointId) AS Sum_gamePointId, SUM(deviceInformationId) AS Sum_deviceInformationId FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY deliveryCity, Latest_YEAR, imei ORDER BY deliveryCity ASC, Latest_YEAR ASC, imei ASC""")
}
       

//TC_383
test("TC_383", Include) {
  checkAnswer(s"""SELECT deliveryCity, imei, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * from Carbon_automation) SUB_QRY GROUP BY deliveryCity, imei, series ORDER BY deliveryCity ASC, imei ASC, series ASC""",
    s"""SELECT deliveryCity, imei, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY deliveryCity, imei, series ORDER BY deliveryCity ASC, imei ASC, series ASC""")
}
       

//TC_384
test("TC_384", Include) {
  checkAnswer(s"""SELECT series, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation) SUB_QRY GROUP BY series ORDER BY series ASC""",
    s"""SELECT series, SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY series ORDER BY series ASC""")
}
       

//TC_386
test("TC_386", Include) {
  checkAnswer(s"""SELECT channelsId, deliveryCity FROM (select * from Carbon_automation) SUB_QRY GROUP BY channelsId, deliveryCity ORDER BY channelsId ASC, deliveryCity ASC""",
    s"""SELECT channelsId, deliveryCity FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY channelsId, deliveryCity ORDER BY channelsId ASC, deliveryCity ASC""")
}
       

//TC_387
test("TC_387", Include) {
  checkAnswer(s"""SELECT modelId, SUM(gamepointid) AS Sum_gamepointid FROM (select * from Carbon_automation) SUB_QRY GROUP BY modelId ORDER BY modelId ASC""",
    s"""SELECT modelId, SUM(gamepointid) AS Sum_gamepointid FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY modelId ORDER BY modelId ASC""")
}
       

//TC_388
test("TC_388", Include) {
  checkAnswer(s"""SELECT imei, channelsId, COUNT(deliveryTime) AS Count_deliveryTime FROM (select * from Carbon_automation) SUB_QRY GROUP BY imei, channelsId ORDER BY imei ASC, channelsId ASC""",
    s"""SELECT imei, channelsId, COUNT(deliveryTime) AS Count_deliveryTime FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY imei, channelsId ORDER BY imei ASC, channelsId ASC""")
}
       

//TC_389
test("TC_389", Include) {
  checkAnswer(s"""SELECT imei, SUM(deviceInformationId) AS Sum_deviceInformationId FROM (select * from Carbon_automation) SUB_QRY GROUP BY imei ORDER BY imei ASC""",
    s"""SELECT imei, SUM(deviceInformationId) AS Sum_deviceInformationId FROM (select * from Carbon_automation_hive) SUB_QRY GROUP BY imei ORDER BY imei ASC""")
}
       

//TC_390
test("TC_390", Include) {
  checkAnswer(s"""select Latest_DAY,imei,gamepointid  from carbon_automation where ( Latest_DAY+1) == 2 order by imei limit 5""",
    s"""select Latest_DAY,imei,gamepointid  from carbon_automation_hive where ( Latest_DAY+1) == 2 order by imei limit 5""")
}
       

//TC_391
test("TC_391", Include) {
  checkAnswer(s"""select gamepointid,imei from carbon_automation where ( gamePointId+1) == 80 order by imei limit 5""",
    s"""select gamepointid,imei from carbon_automation_hive where ( gamePointId+1) == 80 order by imei limit 5""")
}
       

//TC_392
test("TC_392", Include) {
  checkAnswer(s"""select deviceInformationId,imei from carbon_automation where ( deviceInformationId+1) == 100084 order by imei limit 5""",
    s"""select deviceInformationId,imei from carbon_automation_hive where ( deviceInformationId+1) == 100084 order by imei limit 5""")
}
       

//TC_393
test("TC_393", Include) {
  checkAnswer(s"""select channelsId,imei from carbon_automation where ( channelsId+1) == 5 order by imei limit 5""",
    s"""select channelsId,imei from carbon_automation_hive where ( channelsId+1) == 5 order by imei limit 5""")
}
       

//TC_394
test("TC_394", Include) {
  checkAnswer(s"""select contractNumber,imei from carbon_automation where (contractNumber+1) == 507230.0 order by imei limit 5""",
    s"""select contractNumber,imei from carbon_automation_hive where (contractNumber+1) == 507230.0 order by imei limit 5""")
}
       

//TC_395
test("TC_395", Include) {
  checkAnswer(s"""select  Latest_YEAR,imei from carbon_automation where ( Latest_YEAR+1) == 2016 order by imei limit 5""",
    s"""select  Latest_YEAR,imei from carbon_automation_hive where ( Latest_YEAR+1) == 2016 order by imei limit 5""")
}
       

//TC_396
test("TC_396", Include) {
  checkAnswer(s"""select Latest_province,imei from carbon_automation where UPPER(Latest_province) == 'GUANGDONG PROVINCE' order by imei limit 5""",
    s"""select Latest_province,imei from carbon_automation_hive where UPPER(Latest_province) == 'GUANGDONG PROVINCE' order by imei limit 5""")
}
       

//TC_397
test("TC_397", Include) {
  checkAnswer(s"""select Latest_DAY,imei from carbon_automation where UPPER(Latest_DAY) == '1' order by imei limit 5""",
    s"""select Latest_DAY,imei from carbon_automation_hive where UPPER(Latest_DAY) == '1' order by imei limit 5""")
}
       

//TC_398
test("TC_398", Include) {
  checkAnswer(s"""select Latest_DAY,imei from carbon_automation where LOWER(Latest_DAY) == '1' order by imei limit 5""",
    s"""select Latest_DAY,imei from carbon_automation_hive where LOWER(Latest_DAY) == '1' order by imei limit 5""")
}
       

//TC_399
test("TC_399", Include) {
  checkAnswer(s"""select deviceInformationId,imei from carbon_automation where UPPER(deviceInformationId) == '1' order by imei limit 5""",
    s"""select deviceInformationId,imei from carbon_automation_hive where UPPER(deviceInformationId) == '1' order by imei limit 5""")
}
       

//TC_400
test("TC_400", Include) {
  checkAnswer(s"""select deviceInformationId,imei from carbon_automation where LOWER(deviceInformationId) == '1' order by imei limit 5""",
    s"""select deviceInformationId,imei from carbon_automation_hive where LOWER(deviceInformationId) == '1' order by imei limit 5""")
}
       

//TC_401
test("TC_401", Include) {
  checkAnswer(s"""select channelsId,imei from carbon_automation where UPPER(channelsId) == '4' order by imei limit 5""",
    s"""select channelsId,imei from carbon_automation_hive where UPPER(channelsId) == '4' order by imei limit 5""")
}
       

//TC_402
test("TC_402", Include) {
  checkAnswer(s"""select channelsId,imei from carbon_automation where LOWER(channelsId) == '4' order by imei limit 5""",
    s"""select channelsId,imei from carbon_automation_hive where LOWER(channelsId) == '4' order by imei limit 5""")
}
       

//TC_403
test("TC_403", Include) {
  checkAnswer(s"""select gamePointId,imei from carbon_automation where UPPER(gamePointId) == '136.0' order by imei limit 5""",
    s"""select gamePointId,imei from carbon_automation_hive where UPPER(gamePointId) == '136.0' order by imei limit 5""")
}
       

//TC_404
test("TC_404", Include) {
  checkAnswer(s"""select gamePointId,imei from carbon_automation where LOWER(gamePointId) == '136.0' order by imei limit 5""",
    s"""select gamePointId,imei from carbon_automation_hive where LOWER(gamePointId) == '136.0' order by imei limit 5""")
}
       

//TC_405
test("TC_405", Include) {
  checkAnswer(s"""select imei from carbon_automation where UPPER(imei) == '1AA100083' order by imei limit 5""",
    s"""select imei from carbon_automation_hive where UPPER(imei) == '1AA100083' order by imei limit 5""")
}
       

//TC_406
test("TC_406", Include) {
  checkAnswer(s"""select imei from carbon_automation where LOWER(imei) == '1aa100083' order by imei limit 5""",
    s"""select imei from carbon_automation_hive where LOWER(imei) == '1aa100083' order by imei limit 5""")
}
       

//TC_407
test("TC_407", Include) {
  checkAnswer(s"""select MAC,imei from carbon_automation where UPPER(MAC)='MAC' order by imei limit 10""",
    s"""select MAC,imei from carbon_automation_hive where UPPER(MAC)='MAC' order by imei limit 10""")
}
       

//TC_408
test("TC_408", Include) {
  checkAnswer(s"""select imei,series from carbon_automation where series='7Series' order by imei limit 10""",
    s"""select imei,series from carbon_automation_hive where series='7Series' order by imei limit 10""")
}
       

//TC_409
test("TC_409", Include) {
  checkAnswer(s"""select  gamePointId from carbon_automation where  modelId is  null                                 """,
    s"""select  gamePointId from carbon_automation_hive where  modelId is  null                                 """)
}
       

//TC_410
test("TC_410", Include) {
  checkAnswer(s"""select  contractNumber from carbon_automation where bomCode is  null                """,
    s"""select  contractNumber from carbon_automation_hive where bomCode is  null                """)
}
       

//TC_411
test("TC_411", Include) {
  checkAnswer(s"""select  imei from carbon_automation where AMSIZE is  null""",
    s"""select  imei from carbon_automation_hive where AMSIZE is  null""")
}
       

//TC_412
test("TC_412", Include) {
  checkAnswer(s"""select  bomCode from carbon_automation where contractnumber is  null """,
    s"""select  bomCode from carbon_automation_hive where contractnumber is  null """)
}
       

//TC_413
test("TC_413", Include) {
  checkAnswer(s"""select  latest_day from carbon_automation where  modelId is  null""",
    s"""select  latest_day from carbon_automation_hive where  modelId is  null""")
}
       

//TC_414
test("TC_414", Include) {
  checkAnswer(s"""select  latest_day from carbon_automation where  deviceinformationid is  null""",
    s"""select  latest_day from carbon_automation_hive where  deviceinformationid is  null""")
}
       

//TC_415
test("TC_415", Include) {
  checkAnswer(s"""select  deviceinformationid from carbon_automation where  modelId is  null""",
    s"""select  deviceinformationid from carbon_automation_hive where  modelId is  null""")
}
       

//TC_416
test("TC_416", Include) {
  checkAnswer(s"""select  deviceinformationid from carbon_automation where  deviceinformationid is  null""",
    s"""select  deviceinformationid from carbon_automation_hive where  deviceinformationid is  null""")
}
       

//TC_417
test("TC_417", Include) {
  checkAnswer(s"""select  imei from carbon_automation where  modelId is  null""",
    s"""select  imei from carbon_automation_hive where  modelId is  null""")
}
       

//TC_418
test("TC_418", Include) {
  checkAnswer(s"""select  imei from carbon_automation where  deviceinformationid is  null""",
    s"""select  imei from carbon_automation_hive where  deviceinformationid is  null""")
}
       

//TC_419
test("TC_419", Include) {
  checkAnswer(s"""select  count(channelsId) from carbon_automation where  modelId is  null""",
    s"""select  count(channelsId) from carbon_automation_hive where  modelId is  null""")
}
       

//TC_420
test("TC_420", Include) {
  checkAnswer(s"""select  sum(channelsId) from carbon_automation where  deviceinformationid is  null""",
    s"""select  sum(channelsId) from carbon_automation_hive where  deviceinformationid is  null""")
}
       

//TC_421
test("TC_421", Include) {
  checkAnswer(s"""select  avg(channelsName) from carbon_automation where  modelId is  null""",
    s"""select  avg(channelsName) from carbon_automation_hive where  modelId is  null""")
}
       

//TC_422
test("TC_422", Include) {
  checkAnswer(s"""select  min(channelsName) from carbon_automation where  deviceinformationid is  null""",
    s"""select  min(channelsName) from carbon_automation_hive where  deviceinformationid is  null""")
}
       

//TC_423
test("TC_423", Include) {
  checkAnswer(s"""select  max(channelsName) from  carbon_automation where  deviceinformationid is  null""",
    s"""select  max(channelsName) from  carbon_automation_hive where  deviceinformationid is  null""")
}
       

//TC_424
test("TC_424", Include) {
  checkAnswer(s"""SELECT count(DISTINCT gamePointId) FROM carbon_automation where imei is null """,
    s"""SELECT count(DISTINCT gamePointId) FROM carbon_automation_hive where imei is null """)
}
       

//TC_425
test("TC_425", Include) {
  checkAnswer(s"""select  imei from carbon_automation where contractNumber is NOT null""",
    s"""select  imei from carbon_automation_hive where contractNumber is NOT null""")
}
       

//TC_426
test("TC_426", Include) {
  checkAnswer(s"""select  gamePointId from carbon_automation where deviceInformationId is NOT null""",
    s"""select  gamePointId from carbon_automation_hive where deviceInformationId is NOT null""")
}
       

//TC_427
test("TC_427", Include) {
  checkAnswer(s"""select contractnumber from carbon_automation where AMSIZE is NOT null""",
    s"""select contractnumber from carbon_automation_hive where AMSIZE is NOT null""")
}
       

//TC_428
test("TC_428", Include) {
  checkAnswer(s"""select gamePointId from carbon_automation where LATEST_YEAR is NOT null""",
    s"""select gamePointId from carbon_automation_hive where LATEST_YEAR is NOT null""")
}
       

//TC_429
test("TC_429", Include) {
  checkAnswer(s"""select  count(gamePointId) from carbon_automation where imei is NOT null""",
    s"""select  count(gamePointId) from carbon_automation_hive where imei is NOT null""")
}
       

//TC_430
test("TC_430", Include) {
  checkAnswer(s"""select  count(bomCode) from carbon_automation where contractNumber is NOT null""",
    s"""select  count(bomCode) from carbon_automation_hive where contractNumber is NOT null""")
}
       

//TC_431
test("TC_431", Include) {
  checkAnswer(s"""select  channelsName from carbon_automation where contractNumber is NOT null""",
    s"""select  channelsName from carbon_automation_hive where contractNumber is NOT null""")
}
       

//TC_432
test("TC_432", Include) {
  checkAnswer(s"""select  channelsId from carbon_automation where gamePointId is NOT null""",
    s"""select  channelsId from carbon_automation_hive where gamePointId is NOT null""")
}
       

//TC_433
test("TC_433", Include) {
  checkAnswer(s"""select  channelsName from carbon_automation where gamePointId is NOT null""",
    s"""select  channelsName from carbon_automation_hive where gamePointId is NOT null""")
}
       

//TC_434
test("TC_434", Include) {
  checkAnswer(s"""select  channelsId from carbon_automation where latest_day is NOT null""",
    s"""select  channelsId from carbon_automation_hive where latest_day is NOT null""")
}
       

//TC_435
test("TC_435", Include) {
  checkAnswer(s"""select  channelsName from carbon_automation where latest_day is NOT null""",
    s"""select  channelsName from carbon_automation_hive where latest_day is NOT null""")
}
       

//TC_436
test("TC_436", Include) {
  checkAnswer(s"""SELECT count(DISTINCT gamePointId) FROM  carbon_automation where imei is NOT null  """,
    s"""SELECT count(DISTINCT gamePointId) FROM  carbon_automation_hive where imei is NOT null  """)
}
       

//TC_437
test("TC_437", Include) {
  checkAnswer(s"""SELECT sum(deviceInformationId) FROM carbon_automation where imei is NOT null""",
    s"""SELECT sum(deviceInformationId) FROM carbon_automation_hive where imei is NOT null""")
}
       

//TC_438
test("TC_438", Include) {
  checkAnswer(s"""SELECT avg(contractNumber) FROM  carbon_automation  where imei is NOT null    """,
    s"""SELECT avg(contractNumber) FROM  carbon_automation_hive  where imei is NOT null    """)
}
       

//TC_439
test("TC_439", Include) {
  checkAnswer(s"""SELECT min(AMSize) FROM carbon_automation where imei is NOT null""",
    s"""SELECT min(AMSize) FROM carbon_automation_hive where imei is NOT null""")
}
       

//TC_440
test("TC_440", Include) {
  checkAnswer(s"""SELECT max(gamePointId) FROM carbon_automation  where contractNumber is NOT null  """,
    s"""SELECT max(gamePointId) FROM carbon_automation_hive  where contractNumber is NOT null  """)
}
       

//TC_441
test("TC_441", Include) {
  sql(s"""select variance(gamepointid), var_pop(gamepointid)  from carbon_automation where channelsid>2""").collect
}
       

//TC_442
test("TC_442", Include) {
  sql(s"""select variance(deviceInformationId), var_pop(imei)  from carbon_automation where activeareaid>3""").collect
}
       

//TC_443
test("TC_443", Include) {
  sql(s"""select variance(contractNumber), var_pop(contractNumber)  from carbon_automation where deliveryareaid>5""").collect
}
       

//TC_444
test("TC_444", Include) {
  sql(s"""select variance(AMSize), var_pop(channelsid)  from carbon_automation where channelsid>2""").collect
}
       

//TC_445
test("TC_445", Include) {
  sql(s"""select variance(bomcode), var_pop(gamepointid)  from carbon_automation where activeareaid>3""").collect
}
       

//TC_446
test("TC_446", Include) {
  checkAnswer(s"""select variance(deviceInformationId), var_pop(deviceInformationId)  from carbon_automation where activeareaid>3""",
    s"""select variance(deviceInformationId), var_pop(deviceInformationId)  from carbon_automation_hive where activeareaid>3""")
}
       

//TC_447
test("TC_447", Include) {
  sql(s"""select var_samp(contractNumber) from carbon_automation""").collect
}
       

//TC_448
test("TC_448", Include) {
  sql(s"""select var_samp(Latest_YEAR) from carbon_automation""").collect
}
       

//TC_449
test("TC_449", Include) {
  checkAnswer(s"""select var_samp(AMSize) from carbon_automation""",
    s"""select var_samp(AMSize) from carbon_automation_hive""")
}
       

//TC_450
test("TC_450", Include) {
  sql(s"""select var_samp(gamepointId) from carbon_automation""").collect
}
       

//TC_451
test("TC_451", Include) {
  sql(s"""select stddev_pop(bomcode)from carbon_automation""").collect
}
       

//TC_452
test("TC_452", Include) {
  sql(s"""select stddev_pop(deviceInformationId)from carbon_automation""").collect
}
       

//TC_453
test("TC_453", Include) {
  sql(s"""select stddev_pop(gamePointId)from carbon_automation""").collect
}
       

//TC_454
test("TC_454", Include) {
  checkAnswer(s"""select stddev_pop(AMSIZE)from carbon_automation""",
    s"""select stddev_pop(AMSIZE)from carbon_automation_hive""")
}
       

//TC_455
test("TC_455", Include) {
  sql(s"""select stddev_pop(contractNumber)from carbon_automation""").collect
}
       

//TC_456
test("TC_456", Include) {
  sql(s"""select stddev_samp(contractNumber)from carbon_automation""").collect
}
       

//TC_457
test("TC_457", Include) {
  sql(s"""select stddev_samp(deviceInformationId)from carbon_automation""").collect
}
       

//TC_459
test("TC_459", Include) {
  sql(s"""select stddev_samp(Latest_MONTH)from carbon_automation""").collect
}
       

//TC_460
test("TC_460", Include) {
  sql(s"""select stddev_samp(contractnumber)from carbon_automation""").collect
}
       

//TC_461
test("TC_461", Include) {
  sql(s"""select covar_pop(gamePointId, Latest_MONTH) from carbon_automation""").collect
}
       

//TC_462
test("TC_462", Include) {
  sql(s"""select covar_pop(gamePointId, contractNumber) from carbon_automation""").collect
}
       

//TC_463
test("TC_463", Include) {
  sql(s"""select covar_pop(gamePointId, Latest_DAY) from carbon_automation""").collect
}
       

//TC_464
test("TC_464", Include) {
  sql(s"""select covar_pop(gamePointId,deviceInformationId ) from carbon_automation""").collect
}
       

//TC_465
test("TC_465", Include) {
  sql(s"""select covar_samp(gamePointId, Latest_MONTH) from carbon_automation""").collect
}
       

//TC_466
test("TC_466", Include) {
  sql(s"""select covar_samp(gamePointId, contractNumber) from carbon_automation""").collect
}
       

//TC_467
test("TC_467", Include) {
  sql(s"""select covar_samp(gamePointId, Latest_DAY) from carbon_automation""").collect
}
       

//TC_468
test("TC_468", Include) {
  sql(s"""select covar_samp(gamePointId, Latest_YEAR) from carbon_automation""").collect
}
       

//TC_469
test("TC_469", Include) {
  sql(s"""select covar_samp(gamePointId, deviceInformationId) from carbon_automation""").collect
}
       

//TC_470
test("TC_470", Include) {
  sql(s"""select corr(gamePointId, deviceInformationId) from carbon_automation""").collect
}
       

//TC_471
test("TC_471", Include) {
  sql(s"""select corr(Latest_MONTH, gamePointId) from carbon_automation""").collect
}
       

//TC_472
test("TC_472", Include) {
  checkAnswer(s"""Select percentile(1,1.0) from carbon_automation""",
    s"""Select percentile(1,1.0) from carbon_automation_hive""")
}
       

//TC_473
test("TC_473", Include) {
  checkAnswer(s"""Select percentile(1,1.0) from carbon_automation""",
    s"""Select percentile(1,1.0) from carbon_automation_hive""")
}
       

//TC_474
test("TC_474", Include) {
  checkAnswer(s"""select cast(series as int) as a from carbon_automation limit 10""",
    s"""select cast(series as int) as a from carbon_automation_hive limit 10""")
}
       

//TC_475
test("TC_475", Include) {
  checkAnswer(s"""select cast(modelid as int) as a from carbon_automation limit 10""",
    s"""select cast(modelid as int) as a from carbon_automation_hive limit 10""")
}
       

//TC_476
test("TC_476", Include) {
  checkAnswer(s"""Select percentile(1,1.0) from carbon_automation""",
    s"""Select percentile(1,1.0) from carbon_automation_hive""")
}
       

//TC_477
test("TC_477", Include) {
  checkAnswer(s"""select percentile(1,array(1)) from carbon_automation""",
    s"""select percentile(1,array(1)) from carbon_automation_hive""")
}
       

//TC_479
test("TC_479", Include) {
  checkAnswer(s"""select percentile(1,array(0.5)) from carbon_automation""",
    s"""select percentile(1,array(0.5)) from carbon_automation_hive""")
}
       

//TC_480
test("TC_480", Include) {
  checkAnswer(s"""select percentile(1,array(1)) from carbon_automation""",
    s"""select percentile(1,array(1)) from carbon_automation_hive""")
}
       

//TC_481
test("TC_481", Include) {
  checkAnswer(s"""select percentile_approx(1, 0.5 ,5000) from carbon_automation""",
    s"""select percentile_approx(1, 0.5 ,5000) from carbon_automation_hive""")
}
       

//TC_482
test("TC_482", Include) {
  checkAnswer(s"""select percentile_approx(1, 0.5 ,1000) from carbon_automation""",
    s"""select percentile_approx(1, 0.5 ,1000) from carbon_automation_hive""")
}
       

//TC_483
test("TC_483", Include) {
  checkAnswer(s"""select percentile_approx(1, 0.5 ,700) from carbon_automation""",
    s"""select percentile_approx(1, 0.5 ,700) from carbon_automation_hive""")
}
       

//TC_484
test("TC_484", Include) {
  checkAnswer(s"""select percentile_approx(1, 0.5 ,500) from carbon_automation""",
    s"""select percentile_approx(1, 0.5 ,500) from carbon_automation_hive""")
}
       

//TC_485
test("TC_485", Include) {
  checkAnswer(s"""select percentile_approx(1,array(0.5),5000) from carbon_automation""",
    s"""select percentile_approx(1,array(0.5),5000) from carbon_automation_hive""")
}
       

//TC_486
test("TC_486", Include) {
  checkAnswer(s"""select percentile_approx(1,array(0.5),5000) from carbon_automation""",
    s"""select percentile_approx(1,array(0.5),5000) from carbon_automation_hive""")
}
       

//TC_487
test("TC_487", Include) {
  checkAnswer(s"""select histogram_numeric(1, 5000)from carbon_automation""",
    s"""select histogram_numeric(1, 5000)from carbon_automation_hive""")
}
       

//TC_488
test("TC_488", Include) {
  checkAnswer(s"""select histogram_numeric(1, 1000)from carbon_automation""",
    s"""select histogram_numeric(1, 1000)from carbon_automation_hive""")
}
       

//TC_489
test("TC_489", Include) {
  checkAnswer(s"""select histogram_numeric(1, 500)from carbon_automation""",
    s"""select histogram_numeric(1, 500)from carbon_automation_hive""")
}
       

//TC_490
test("TC_490", Include) {
  checkAnswer(s"""select histogram_numeric(1, 500)from carbon_automation""",
    s"""select histogram_numeric(1, 500)from carbon_automation_hive""")
}
       

//TC_491
test("TC_491", Include) {
  sql(s"""select collect_set(gamePointId) from carbon_automation""").collect
}
       

//TC_492
test("TC_492", Include) {
  sql(s"""select collect_set(AMSIZE) from carbon_automation""").collect
}
       

//TC_493
test("TC_493", Include) {
  sql(s"""select collect_set(bomcode) from carbon_automation""").collect
}
       

//TC_494
test("TC_494", Include) {
  sql(s"""select collect_set(imei) from carbon_automation""").collect
}
       

//TC_495
test("TC_495", Include) {
  sql(s"""select var_samp(Latest_YEAR) from carbon_automation""").collect
}
       

//TC_496
test("TC_496", Include) {
  sql(s"""select stddev_pop(deviceInformationId)from carbon_automation""").collect
}
       

//TC_497
test("TC_497", Include) {
  sql(s"""select stddev_samp(deviceInformationId)from carbon_automation""").collect
}
       

//TC_498
test("TC_498", Include) {
  sql(s"""select covar_pop(gamePointId, contractNumber) from carbon_automation""").collect
}
       

//TC_499
test("TC_499", Include) {
  sql(s"""select covar_samp(gamePointId, contractNumber) from carbon_automation""").collect
}
       

//TC_500
test("TC_500", Include) {
  checkAnswer(s"""select percentile(1,array(0.5)) from carbon_automation""",
    s"""select percentile(1,array(0.5)) from carbon_automation_hive""")
}
       

//TC_501
test("TC_501", Include) {
  sql(s"""select percentile_approx(1,array(0.5),5000) from carbon_automation""").collect
}
       

//TC_502
test("TC_502", Include) {
  sql(s"""select collect_set(AMSIZE) from carbon_automation""").collect
}
       

//TC_503
test("TC_503", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, ActiveDistrict,  AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE Carbon_automation.AMSize = "1RAM size" GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity,Carbon_automation.gamePointId  ORDER BY Carbon_automation.gamePointId ,Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, ActiveDistrict,  AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE Carbon_automation_hive.AMSize = "1RAM size" GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity,Carbon_automation_hive.gamePointId  ORDER BY Carbon_automation_hive.gamePointId ,Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_504
test("TC_504", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE Carbon_automation.AMSize > "1RAM size" GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity,Carbon_automation.gamePointId ORDER BY Carbon_automation.gamePointId ,Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE Carbon_automation_hive.AMSize > "1RAM size" GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity,Carbon_automation_hive.gamePointId ORDER BY Carbon_automation_hive.gamePointId ,Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_505
test("TC_505", Include) {
  checkAnswer(s"""SELECT  Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE Carbon_automation.AMSize >= "2RAM size" GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry,Carbon_automation.gamePointId, Carbon_automation.Activecity ORDER BY Carbon_automation.gamePointId ,Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT  Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE Carbon_automation_hive.AMSize >= "2RAM size" GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry,Carbon_automation_hive.gamePointId, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.gamePointId ,Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_506
test("TC_506", Include) {
  checkAnswer(s"""SELECT  Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE Carbon_automation.AMSize < "3RAM size" GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity, Carbon_automation.gamePointId ORDER BY Carbon_automation.gamePointId ,Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT  Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE Carbon_automation_hive.AMSize < "3RAM size" GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity, Carbon_automation_hive.gamePointId ORDER BY Carbon_automation_hive.gamePointId ,Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_507
test("TC_507", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE Carbon_automation.AMSize <= "5RAM size" GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity,Carbon_automation.gamePointId ORDER BY Carbon_automation.gamePointId ,Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE Carbon_automation_hive.AMSize <= "5RAM size" GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity,Carbon_automation_hive.gamePointId ORDER BY Carbon_automation_hive.gamePointId , Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_508
test("TC_508", Include) {
  checkAnswer(s"""SELECT  Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE Carbon_automation.AMSize LIKE '%1%' GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity,Carbon_automation. gamePointId ORDER BY Carbon_automation.gamePointId ,Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT  Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE Carbon_automation_hive.AMSize LIKE '%1%' GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity,Carbon_automation_hive. gamePointId ORDER BY Carbon_automation_hive.gamePointId , Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_509
test("TC_509", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.Activecity AS Activecity, Carbon_automation.AMSize AS AMSize FROM ( SELECT AMSize, gamePointId ,deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.AMSize AS AMSize FROM ( SELECT AMSize, gamePointId ,deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_510
test("TC_510", Include) {
  checkAnswer(s"""SELECT  Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE Carbon_automation.AMSize LIKE '5RAM %' GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity,Carbon_automation.gamePointId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT  Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE Carbon_automation_hive.AMSize LIKE '5RAM %' GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity,Carbon_automation_hive.gamePointId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_511
test("TC_511", Include) {
  checkAnswer(s"""SELECT  Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE Carbon_automation.AMSize BETWEEN "2RAM size" AND "6RAM size" GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity,Carbon_automation.gamePointId ORDER BY Carbon_automation.gamePointId ,Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT  Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE Carbon_automation_hive.AMSize BETWEEN "2RAM size" AND "6RAM size" GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity,Carbon_automation_hive.gamePointId ORDER BY Carbon_automation_hive.gamePointId ,Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_512
test("TC_512", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE Carbon_automation.AMSize IN ("4RAM size","8RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ,Carbon_automation.gamePointId ORDER BY Carbon_automation.gamePointId , Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE Carbon_automation_hive.AMSize IN ("4RAM size","8RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ,Carbon_automation_hive.gamePointId ORDER BY Carbon_automation_hive.gamePointId ,Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_513
test("TC_513", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE Carbon_automation.AMSize IS NULL GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ,Carbon_automation.gamePointId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE Carbon_automation_hive.AMSize IS NULL GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ,Carbon_automation_hive.gamePointId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_514
test("TC_514", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE NOT(Carbon_automation.AMSize = "8RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity,Carbon_automation.gamePointId  ORDER BY Carbon_automation.gamePointId ,Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE NOT(Carbon_automation_hive.AMSize = "8RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity,Carbon_automation_hive.gamePointId  ORDER BY Carbon_automation_hive.gamePointId ,Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_515
test("TC_515", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE NOT(Carbon_automation.AMSize > "6RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ,Carbon_automation.gamePointId ORDER BY Carbon_automation.gamePointId ,Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE NOT(Carbon_automation_hive.AMSize > "6RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ,Carbon_automation_hive.gamePointId ORDER BY Carbon_automation_hive.gamePointId ,Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       


//TC_518
test("TC_518", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity , SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry,gamePointId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE NOT(Carbon_automation.AMSize <= "3RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity , SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry,gamePointId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE NOT(Carbon_automation_hive.AMSize <= "3RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_519
test("TC_519", Include) {
  checkAnswer(s"""SELECT Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.Activecity ORDER BY Carbon_automation.Activecity ASC LIMIT 5000""",
    s"""SELECT Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.Activecity ASC LIMIT 5000""")
}
       

//TC_520
test("TC_520", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.AMSize AS AMSize, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, gamePointId,Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, gamePointId,Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_521
test("TC_521", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE NOT(Carbon_automation.Activecity LIKE 'xian%') GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE NOT(Carbon_automation_hive.Activecity LIKE 'xian%') GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_522
test("TC_522", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE NOT(Carbon_automation.AMSize BETWEEN "4RAM size" AND "7RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE NOT(Carbon_automation_hive.AMSize BETWEEN "4RAM size" AND "7RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_523
test("TC_523", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE NOT(Carbon_automation.AMSize IN ("5RAM size","8RAM size")) GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE NOT(Carbon_automation_hive.AMSize IN ("5RAM size","8RAM size")) GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_524
test("TC_524", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE Carbon_automation.AMSize IS NOT NULL GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE Carbon_automation_hive.AMSize IS NOT NULL GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_525
test("TC_525", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity,gamePointId, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity,gamePointId, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize""")
}
       

//TC_526
test("TC_526", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.deviceInformationId ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.deviceInformationId ASC""")
}
       

//TC_527
test("TC_527", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.deviceInformationId AS deviceInformationId FROM ( SELECT  AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.deviceInformationId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.deviceInformationId AS deviceInformationId FROM ( SELECT  AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.deviceInformationId DESC""")
}
       

//TC_528
test("TC_528", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_529
test("TC_529", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, AVG(Carbon_automation.deviceInformationId) AS avg_deviceInformationId FROM ( SELECT AMSize,deviceInformationId, gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId, AVG(Carbon_automation_hive.deviceInformationId) AS avg_deviceInformationId FROM ( SELECT AMSize,deviceInformationId, gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_530
test("TC_530", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, COUNT(Carbon_automation.deviceInformationId) AS Count_deviceInformationId FROM ( SELECT AMSize, ActiveCountry,gamePointId, Activecity,deviceInformationId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId, COUNT(Carbon_automation_hive.deviceInformationId) AS Count_deviceInformationId FROM ( SELECT AMSize, ActiveCountry,gamePointId, Activecity,deviceInformationId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_531
test("TC_531", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, COUNT(DISTINCT Carbon_automation.deviceInformationId) AS LONG_COL_0 FROM ( SELECT AMSize, ActiveCountry,gamePointId,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId, COUNT(DISTINCT Carbon_automation_hive.deviceInformationId) AS LONG_COL_0 FROM ( SELECT AMSize, ActiveCountry,gamePointId,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_532
test("TC_532", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.AMSize AS AMSize, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_533
test("TC_533", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, MAX(Carbon_automation.deviceInformationId) AS Max_deviceInformationId FROM ( SELECT AMSize,gamePointId,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId, MAX(Carbon_automation_hive.deviceInformationId) AS Max_deviceInformationId FROM ( SELECT AMSize,gamePointId,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_534
test("TC_534", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MIN(Carbon_automation.deviceInformationId) AS Min_deviceInformationId, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, MIN(Carbon_automation_hive.deviceInformationId) AS Min_deviceInformationId, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_535
test("TC_535", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.gamePointId AS gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.gamePointId ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.gamePointId AS gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.gamePointId ASC""")
}
       

//TC_536
test("TC_536", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry, deviceInformationId,gamePointId,Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.gamePointId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry, deviceInformationId,gamePointId,Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.gamePointId DESC""")
}
       

//TC_537
test("TC_537", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_538
test("TC_538", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, AVG(Carbon_automation.gamePointId) AS Avg_gamePointId FROM ( SELECT AMSize,gamePointId ,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, AVG(Carbon_automation_hive.gamePointId) AS Avg_gamePointId FROM ( SELECT AMSize,gamePointId ,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_539
test("TC_539", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, COUNT(Carbon_automation.gamePointId) AS Count_gamePointId FROM ( SELECT AMSize, gamePointId,ActiveCountry, deviceInformationId,Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, COUNT(Carbon_automation_hive.gamePointId) AS Count_gamePointId FROM ( SELECT AMSize, gamePointId,ActiveCountry, deviceInformationId,Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_540
test("TC_540", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, COUNT(DISTINCT Carbon_automation.gamePointId) AS DistinctCount_gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, COUNT(DISTINCT Carbon_automation_hive.gamePointId) AS DistinctCount_gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_541
test("TC_541", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.AMSize AS AMSize, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_542
test("TC_542", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, MAX(Carbon_automation.gamePointId) AS Max_gamePointId FROM ( SELECT AMSize, deviceInformationId,gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, MAX(Carbon_automation_hive.gamePointId) AS Max_gamePointId FROM ( SELECT AMSize, deviceInformationId,gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_543
test("TC_543", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, MIN(Carbon_automation.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize,gamePointId,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, MIN(Carbon_automation_hive.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize,gamePointId,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_544
test("TC_544", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry,deviceInformationId, gamePointId,Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry,deviceInformationId, gamePointId,Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId DESC""")
}
       

//TC_545
test("TC_545", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.gamePointId AS gamePointId FROM ( SELECT AMSize, deviceInformationId,gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize DESC, Carbon_automation.gamePointId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.gamePointId AS gamePointId FROM ( SELECT AMSize, deviceInformationId,gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize DESC, Carbon_automation_hive.gamePointId DESC""")
}
       

//TC_546
test("TC_546", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry,deviceInformationId, gamePointId,Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize DESC, Carbon_automation.gamePointId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry,deviceInformationId, gamePointId,Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize DESC, Carbon_automation_hive.gamePointId DESC""")
}
       

//TC_547
test("TC_547", Include) {
  checkAnswer(s"""SELECT Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation.AMSize) AS DistinctCount_AMSize, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,deviceInformationId, gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation_hive.AMSize) AS DistinctCount_AMSize, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,deviceInformationId, gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_548
test("TC_548", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.Activecity AS Activecity, Carbon_automation.AMSize AS AMSize FROM ( SELECT AMSize,gamePointId, ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.AMSize AS AMSize FROM ( SELECT AMSize,gamePointId, ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_549
test("TC_549", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(distinct Carbon_automation.gamePointId) AS Sum_distinct_gamePointId, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(distinct Carbon_automation_hive.gamePointId) AS Sum_distinct_gamePointId, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_550
test("TC_550", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(distinct Carbon_automation.deviceInformationId) AS Sum_distinct_deviceInformationId, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(distinct Carbon_automation_hive.deviceInformationId) AS Sum_distinct_deviceInformationId, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_551
test("TC_551", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, First(Carbon_automation.deviceInformationId) AS First_deviceInformationId FROM ( SELECT AMSize,deviceInformationId, gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC
""").collect
}
       

//TC_552
test("TC_552", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, First(Carbon_automation.gamePointId) AS First_gamePointId FROM ( SELECT AMSize,gamePointId ,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC
""").collect
}
       


//TC_559
test("TC_559", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.Activecity AS Activecity, Carbon_automation.AMSize AS AMSize FROM ( SELECT AMSize, gamePointId ,deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.AMSize AS AMSize FROM ( SELECT AMSize, gamePointId ,deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_560
test("TC_560", Include) {
  checkAnswer(s"""SELECT  Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE Carbon_automation.AMSize LIKE '5RAM %' GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity,Carbon_automation.gamePointId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT  Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE Carbon_automation_hive.AMSize LIKE '5RAM %' GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity,Carbon_automation_hive.gamePointId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       


//TC_563
test("TC_563", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE Carbon_automation.AMSize IS NULL GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ,Carbon_automation.gamePointId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE Carbon_automation_hive.AMSize IS NULL GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ,Carbon_automation_hive.gamePointId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       



//TC_568
test("TC_568", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity , SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry,gamePointId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE NOT(Carbon_automation.AMSize <= "3RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity , SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry,gamePointId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE NOT(Carbon_automation_hive.AMSize <= "3RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_569
test("TC_569", Include) {
  checkAnswer(s"""SELECT Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.Activecity ORDER BY Carbon_automation.Activecity ASC LIMIT 5000""",
    s"""SELECT Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.Activecity ASC LIMIT 5000""")
}
       

//TC_570
test("TC_570", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.AMSize AS AMSize, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, gamePointId,Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, gamePointId,Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_571
test("TC_571", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE NOT(Carbon_automation.Activecity LIKE 'xian%') GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE NOT(Carbon_automation_hive.Activecity LIKE 'xian%') GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_572
test("TC_572", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE NOT(Carbon_automation.AMSize BETWEEN "4RAM size" AND "7RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE NOT(Carbon_automation_hive.AMSize BETWEEN "4RAM size" AND "7RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_573
test("TC_573", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE NOT(Carbon_automation.AMSize IN ("5RAM size","8RAM size")) GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE NOT(Carbon_automation_hive.AMSize IN ("5RAM size","8RAM size")) GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_574
test("TC_574", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE Carbon_automation.AMSize IS NOT NULL GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE Carbon_automation_hive.AMSize IS NOT NULL GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_575
test("TC_575", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity,gamePointId, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity,gamePointId, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize""")
}
       

//TC_576
test("TC_576", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.deviceInformationId ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.deviceInformationId ASC""")
}
       

//TC_577
test("TC_577", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.deviceInformationId AS deviceInformationId FROM ( SELECT  AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.deviceInformationId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.deviceInformationId AS deviceInformationId FROM ( SELECT  AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.deviceInformationId DESC""")
}
       

//TC_578
test("TC_578", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_579
test("TC_579", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize,deviceInformationId, gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize,deviceInformationId, gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_580
test("TC_580", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, COUNT(Carbon_automation.deviceInformationId) AS Count_deviceInformationId FROM ( SELECT AMSize, ActiveCountry,gamePointId, Activecity,deviceInformationId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId, COUNT(Carbon_automation_hive.deviceInformationId) AS Count_deviceInformationId FROM ( SELECT AMSize, ActiveCountry,gamePointId, Activecity,deviceInformationId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_581
test("TC_581", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, COUNT(DISTINCT Carbon_automation.deviceInformationId) AS LONG_COL_0 FROM ( SELECT AMSize, ActiveCountry,gamePointId,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId, COUNT(DISTINCT Carbon_automation_hive.deviceInformationId) AS LONG_COL_0 FROM ( SELECT AMSize, ActiveCountry,gamePointId,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_582
test("TC_582", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.AMSize AS AMSize, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_583
test("TC_583", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, MAX(Carbon_automation.deviceInformationId) AS Max_deviceInformationId FROM ( SELECT AMSize,gamePointId,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId, MAX(Carbon_automation_hive.deviceInformationId) AS Max_deviceInformationId FROM ( SELECT AMSize,gamePointId,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_584
test("TC_584", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MIN(Carbon_automation.deviceInformationId) AS Min_deviceInformationId, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, MIN(Carbon_automation_hive.deviceInformationId) AS Min_deviceInformationId, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_585
test("TC_585", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.gamePointId AS gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.gamePointId ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.gamePointId AS gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.gamePointId ASC""")
}
       

//TC_586
test("TC_586", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry, deviceInformationId,gamePointId,Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.gamePointId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry, deviceInformationId,gamePointId,Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.gamePointId DESC""")
}
       

//TC_587
test("TC_587", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_588
test("TC_588", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, AVG(Carbon_automation.gamePointId) AS Avg_gamePointId FROM ( SELECT AMSize,gamePointId ,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, AVG(Carbon_automation_hive.gamePointId) AS Avg_gamePointId FROM ( SELECT AMSize,gamePointId ,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_589
test("TC_589", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, COUNT(Carbon_automation.gamePointId) AS Count_gamePointId FROM ( SELECT AMSize, gamePointId,ActiveCountry, deviceInformationId,Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, COUNT(Carbon_automation_hive.gamePointId) AS Count_gamePointId FROM ( SELECT AMSize, gamePointId,ActiveCountry, deviceInformationId,Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_590
test("TC_590", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, COUNT(DISTINCT Carbon_automation.gamePointId) AS DistinctCount_gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, COUNT(DISTINCT Carbon_automation_hive.gamePointId) AS DistinctCount_gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_591
test("TC_591", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.AMSize AS AMSize, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_592
test("TC_592", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, MAX(Carbon_automation.gamePointId) AS Max_gamePointId FROM ( SELECT AMSize, deviceInformationId,gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, MAX(Carbon_automation_hive.gamePointId) AS Max_gamePointId FROM ( SELECT AMSize, deviceInformationId,gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_593
test("TC_593", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, MIN(Carbon_automation.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize,gamePointId,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, MIN(Carbon_automation_hive.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize,gamePointId,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_594
test("TC_594", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry,deviceInformationId, gamePointId,Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry,deviceInformationId, gamePointId,Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId DESC""")
}
       

//TC_595
test("TC_595", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.gamePointId AS gamePointId FROM ( SELECT AMSize, deviceInformationId,gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize DESC, Carbon_automation.gamePointId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.gamePointId AS gamePointId FROM ( SELECT AMSize, deviceInformationId,gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize DESC, Carbon_automation_hive.gamePointId DESC""")
}
       

//TC_596
test("TC_596", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry,deviceInformationId, gamePointId,Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize DESC, Carbon_automation.gamePointId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry,deviceInformationId, gamePointId,Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize DESC, Carbon_automation_hive.gamePointId DESC""")
}
       

//TC_597
test("TC_597", Include) {
  checkAnswer(s"""SELECT Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation.AMSize) AS DistinctCount_AMSize, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,deviceInformationId, gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation_hive.AMSize) AS DistinctCount_AMSize, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,deviceInformationId, gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_598
test("TC_598", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.Activecity AS Activecity, Carbon_automation.AMSize AS AMSize FROM ( SELECT AMSize,gamePointId, ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.AMSize AS AMSize FROM ( SELECT AMSize,gamePointId, ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_599
test("TC_599", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(distinct Carbon_automation.gamePointId) AS Sum_distinct_gamePointId, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation Left join ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(distinct Carbon_automation_hive.gamePointId) AS Sum_distinct_gamePointId, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive Left join ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_600
test("TC_600", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(distinct Carbon_automation.deviceInformationId) AS Sum_distinct_deviceInformationId, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation Left join ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(distinct Carbon_automation_hive.deviceInformationId) AS Sum_distinct_deviceInformationId, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive Left join ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_601
test("TC_601", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, First(Carbon_automation.deviceInformationId) AS First_deviceInformationId FROM ( SELECT AMSize,deviceInformationId, gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation Left join ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC
""").collect
}
       

//TC_602
test("TC_602", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, First(Carbon_automation.gamePointId) AS First_gamePointId FROM ( SELECT AMSize,gamePointId ,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation Left join ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC
""").collect
}
       

//TC_609
test("TC_609", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.Activecity AS Activecity, Carbon_automation.AMSize AS AMSize FROM ( SELECT AMSize, gamePointId ,deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.AMSize AS AMSize FROM ( SELECT AMSize, gamePointId ,deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_610
test("TC_610", Include) {
  checkAnswer(s"""SELECT  Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE Carbon_automation.AMSize LIKE '5RAM %' GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity,Carbon_automation.gamePointId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT  Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE Carbon_automation_hive.AMSize LIKE '5RAM %' GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity,Carbon_automation_hive.gamePointId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_613
test("TC_613", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE Carbon_automation.AMSize IS NULL GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ,Carbon_automation.gamePointId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE Carbon_automation_hive.AMSize IS NULL GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ,Carbon_automation_hive.gamePointId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       



//TC_618
test("TC_618", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity , SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry,gamePointId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE NOT(Carbon_automation.AMSize <= "3RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity , SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry,gamePointId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE NOT(Carbon_automation_hive.AMSize <= "3RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_619
test("TC_619", Include) {
  checkAnswer(s"""SELECT Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.Activecity ORDER BY Carbon_automation.Activecity ASC LIMIT 5000""",
    s"""SELECT Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.Activecity ASC LIMIT 5000""")
}
       

//TC_620
test("TC_620", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.AMSize AS AMSize, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, gamePointId,Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, gamePointId,Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_621
test("TC_621", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE NOT(Carbon_automation.Activecity LIKE 'xian%') GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE NOT(Carbon_automation_hive.Activecity LIKE 'xian%') GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_622
test("TC_622", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE NOT(Carbon_automation.AMSize BETWEEN "4RAM size" AND "7RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE NOT(Carbon_automation_hive.AMSize BETWEEN "4RAM size" AND "7RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_623
test("TC_623", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE NOT(Carbon_automation.AMSize IN ("5RAM size","8RAM size")) GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE NOT(Carbon_automation_hive.AMSize IN ("5RAM size","8RAM size")) GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_624
test("TC_624", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE Carbon_automation.AMSize IS NOT NULL GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE Carbon_automation_hive.AMSize IS NOT NULL GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_625
test("TC_625", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity,gamePointId, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity,gamePointId, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize""")
}
       

//TC_626
test("TC_626", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.deviceInformationId ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.deviceInformationId ASC""")
}
       

//TC_627
test("TC_627", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.deviceInformationId AS deviceInformationId FROM ( SELECT  AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.deviceInformationId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.deviceInformationId AS deviceInformationId FROM ( SELECT  AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.deviceInformationId DESC""")
}
       

//TC_628
test("TC_628", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""").collect
}
       

//TC_629
test("TC_629", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize,deviceInformationId, gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""").collect
}
       

//TC_630
test("TC_630", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, COUNT(Carbon_automation.deviceInformationId) AS Count_deviceInformationId FROM ( SELECT AMSize, ActiveCountry,gamePointId, Activecity,deviceInformationId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""").collect
}
       

//TC_631
test("TC_631", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, COUNT(DISTINCT Carbon_automation.deviceInformationId) AS LONG_COL_0 FROM ( SELECT AMSize, ActiveCountry,gamePointId,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""").collect
}
       

//TC_632
test("TC_632", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.AMSize AS AMSize, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_633
test("TC_633", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, MAX(Carbon_automation.deviceInformationId) AS Max_deviceInformationId FROM ( SELECT AMSize,gamePointId,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""").collect
}
       

//TC_634
test("TC_634", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MIN(Carbon_automation.deviceInformationId) AS Min_deviceInformationId, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""").collect
}
       

//TC_635
test("TC_635", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.gamePointId AS gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.gamePointId ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.gamePointId AS gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.gamePointId ASC""")
}
       

//TC_636
test("TC_636", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry, deviceInformationId,gamePointId,Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.gamePointId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry, deviceInformationId,gamePointId,Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.gamePointId DESC""")
}
       

//TC_637
test("TC_637", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""").collect
}
       

//TC_638
test("TC_638", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, AVG(Carbon_automation.gamePointId) AS Avg_gamePointId FROM ( SELECT AMSize,gamePointId ,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""").collect
}
       

//TC_639
test("TC_639", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, COUNT(Carbon_automation.gamePointId) AS Count_gamePointId FROM ( SELECT AMSize, gamePointId,ActiveCountry, deviceInformationId,Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, COUNT(Carbon_automation_hive.gamePointId) AS Count_gamePointId FROM ( SELECT AMSize, gamePointId,ActiveCountry, deviceInformationId,Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_640
test("TC_640", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, COUNT(DISTINCT Carbon_automation.gamePointId) AS DistinctCount_gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, COUNT(DISTINCT Carbon_automation_hive.gamePointId) AS DistinctCount_gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_641
test("TC_641", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.AMSize AS AMSize, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_642
test("TC_642", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, MAX(Carbon_automation.gamePointId) AS Max_gamePointId FROM ( SELECT AMSize, deviceInformationId,gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, MAX(Carbon_automation_hive.gamePointId) AS Max_gamePointId FROM ( SELECT AMSize, deviceInformationId,gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_643
test("TC_643", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, MIN(Carbon_automation.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize,gamePointId,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, MIN(Carbon_automation_hive.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize,gamePointId,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_644
test("TC_644", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry,deviceInformationId, gamePointId,Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry,deviceInformationId, gamePointId,Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId DESC""")
}
       

//TC_645
test("TC_645", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.gamePointId AS gamePointId FROM ( SELECT AMSize, deviceInformationId,gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize DESC, Carbon_automation.gamePointId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.gamePointId AS gamePointId FROM ( SELECT AMSize, deviceInformationId,gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize DESC, Carbon_automation_hive.gamePointId DESC""")
}
       

//TC_646
test("TC_646", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry,deviceInformationId, gamePointId,Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize DESC, Carbon_automation.gamePointId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry,deviceInformationId, gamePointId,Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize DESC, Carbon_automation_hive.gamePointId DESC""")
}
       

//TC_647
test("TC_647", Include) {
  checkAnswer(s"""SELECT Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation.AMSize) AS DistinctCount_AMSize, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,deviceInformationId, gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation_hive.AMSize) AS DistinctCount_AMSize, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,deviceInformationId, gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_648
test("TC_648", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.Activecity AS Activecity, Carbon_automation.AMSize AS AMSize FROM ( SELECT AMSize,gamePointId, ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.AMSize AS AMSize FROM ( SELECT AMSize,gamePointId, ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_649
test("TC_649", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(distinct Carbon_automation.gamePointId) AS Sum_distinct_gamePointId, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation Right Join ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(distinct Carbon_automation_hive.gamePointId) AS Sum_distinct_gamePointId, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive Right Join ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_650
test("TC_650", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(distinct Carbon_automation.deviceInformationId) AS Sum_distinct_deviceInformationId, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation Right Join ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""").collect
}
       

//TC_651
test("TC_651", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, First(Carbon_automation.deviceInformationId) AS First_deviceInformationId FROM ( SELECT AMSize,deviceInformationId, gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation Right Join ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC
""").collect
}
       

//TC_652
test("TC_652", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, First(Carbon_automation.gamePointId) AS First_gamePointId FROM ( SELECT AMSize,gamePointId ,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation Right Join ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC
""").collect
}
       



//TC_659
test("TC_659", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.Activecity AS Activecity, Carbon_automation.AMSize AS AMSize FROM ( SELECT AMSize, gamePointId ,deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.AMSize AS AMSize FROM ( SELECT AMSize, gamePointId ,deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_660
test("TC_660", Include) {
  checkAnswer(s"""SELECT  Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE Carbon_automation.AMSize LIKE '5RAM %' GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity,Carbon_automation.gamePointId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT  Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE Carbon_automation_hive.AMSize LIKE '5RAM %' GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity,Carbon_automation_hive.gamePointId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_661
test("TC_661", Include) {
  checkAnswer(s"""SELECT  Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE Carbon_automation.AMSize BETWEEN "2RAM size" AND "6RAM size" GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity,Carbon_automation.gamePointId ORDER BY Carbon_automation.gamePointId , Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT  Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE Carbon_automation_hive.AMSize BETWEEN "2RAM size" AND "6RAM size" GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity,Carbon_automation_hive.gamePointId ORDER BY Carbon_automation_hive.gamePointId ,Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_662
test("TC_662", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE Carbon_automation.AMSize IN ("4RAM size","8RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ,Carbon_automation.gamePointId ORDER BY Carbon_automation.gamePointId,Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE Carbon_automation_hive.AMSize IN ("4RAM size","8RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ,Carbon_automation_hive.gamePointId ORDER BY Carbon_automation_hive.gamePointId,Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_663
test("TC_663", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE Carbon_automation.AMSize IS NULL GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ,Carbon_automation.gamePointId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE Carbon_automation_hive.AMSize IS NULL GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ,Carbon_automation_hive.gamePointId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_664
test("TC_664", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE NOT(Carbon_automation.AMSize = "8RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity,Carbon_automation.gamePointId  ORDER BY Carbon_automation.gamePointId ,Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE NOT(Carbon_automation_hive.AMSize = "8RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity,Carbon_automation_hive.gamePointId  ORDER BY Carbon_automation_hive.gamePointId ,Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_665
test("TC_665", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE NOT(Carbon_automation.AMSize > "6RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ,Carbon_automation.gamePointId ORDER BY Carbon_automation.gamePointId ,Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE NOT(Carbon_automation_hive.AMSize > "6RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ,Carbon_automation_hive.gamePointId ORDER BY Carbon_automation_hive.gamePointId ,Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_666
test("TC_666", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity , SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE NOT(Carbon_automation.AMSize >= "5RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity,Carbon_automation.gamePointId ORDER BY Carbon_automation.gamePointId ,Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity , SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE NOT(Carbon_automation_hive.AMSize >= "5RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity,Carbon_automation_hive.gamePointId ORDER BY Carbon_automation_hive.gamePointId ,Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_667
test("TC_667", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId,Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity , SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE NOT(Carbon_automation.AMSize < "4RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity,Carbon_automation.gamePointId  ORDER BY Carbon_automation.gamePointId ,Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId,Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity , SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE NOT(Carbon_automation_hive.AMSize < "4RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity,Carbon_automation_hive.gamePointId  ORDER BY Carbon_automation_hive.gamePointId ,Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_668
test("TC_668", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity , SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry,gamePointId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE NOT(Carbon_automation.AMSize <= "3RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity , SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry,gamePointId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE NOT(Carbon_automation_hive.AMSize <= "3RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_669
test("TC_669", Include) {
  checkAnswer(s"""SELECT Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.Activecity ORDER BY Carbon_automation.Activecity ASC LIMIT 5000""",
    s"""SELECT Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.Activecity ASC LIMIT 5000""")
}
       

//TC_670
test("TC_670", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.AMSize AS AMSize, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, gamePointId,Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, gamePointId,Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_671
test("TC_671", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE NOT(Carbon_automation.Activecity LIKE 'xian%') GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE NOT(Carbon_automation_hive.Activecity LIKE 'xian%') GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_672
test("TC_672", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE NOT(Carbon_automation.AMSize BETWEEN "4RAM size" AND "7RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE NOT(Carbon_automation_hive.AMSize BETWEEN "4RAM size" AND "7RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_673
test("TC_673", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE NOT(Carbon_automation.AMSize IN ("5RAM size","8RAM size")) GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE NOT(Carbon_automation_hive.AMSize IN ("5RAM size","8RAM size")) GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_674
test("TC_674", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize WHERE Carbon_automation.AMSize IS NOT NULL GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity,gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize WHERE Carbon_automation_hive.AMSize IS NOT NULL GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_675
test("TC_675", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity,gamePointId, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity,gamePointId, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize""")
}
       

//TC_676
test("TC_676", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.deviceInformationId ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.deviceInformationId ASC""")
}
       

//TC_677
test("TC_677", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.deviceInformationId AS deviceInformationId FROM ( SELECT  AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.deviceInformationId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.deviceInformationId AS deviceInformationId FROM ( SELECT  AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.deviceInformationId DESC""")
}
       

//TC_678
test("TC_678", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""").collect
}
       

//TC_679
test("TC_679", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize,deviceInformationId, gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""").collect
}
       

//TC_680
test("TC_680", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, COUNT(Carbon_automation.deviceInformationId) AS Count_deviceInformationId FROM ( SELECT AMSize, ActiveCountry,gamePointId, Activecity,deviceInformationId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""").collect
}
       

//TC_681
test("TC_681", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, COUNT(DISTINCT Carbon_automation.deviceInformationId) AS LONG_COL_0 FROM ( SELECT AMSize, ActiveCountry,gamePointId,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId, COUNT(DISTINCT Carbon_automation_hive.deviceInformationId) AS LONG_COL_0 FROM ( SELECT AMSize, ActiveCountry,gamePointId,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_682
test("TC_682", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.AMSize AS AMSize, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_683
test("TC_683", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, MAX(Carbon_automation.deviceInformationId) AS Max_deviceInformationId FROM ( SELECT AMSize,gamePointId,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""").collect
}
       

//TC_684
test("TC_684", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MIN(Carbon_automation.deviceInformationId) AS Min_deviceInformationId, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""").collect
}
       

//TC_685
test("TC_685", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.gamePointId AS gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.gamePointId ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.gamePointId AS gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.gamePointId ASC""")
}
       

//TC_686
test("TC_686", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry, deviceInformationId,gamePointId,Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.gamePointId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry, deviceInformationId,gamePointId,Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.gamePointId DESC""")
}
       

//TC_687
test("TC_687", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""").collect
}
       

//TC_688
test("TC_688", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, AVG(Carbon_automation.gamePointId) AS Avg_gamePointId FROM ( SELECT AMSize,gamePointId ,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""").collect
}
       

//TC_689
test("TC_689", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, COUNT(Carbon_automation.gamePointId) AS Count_gamePointId FROM ( SELECT AMSize, gamePointId,ActiveCountry, deviceInformationId,Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, COUNT(Carbon_automation_hive.gamePointId) AS Count_gamePointId FROM ( SELECT AMSize, gamePointId,ActiveCountry, deviceInformationId,Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_690
test("TC_690", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, COUNT(DISTINCT Carbon_automation.gamePointId) AS DistinctCount_gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, COUNT(DISTINCT Carbon_automation_hive.gamePointId) AS DistinctCount_gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_691
test("TC_691", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.AMSize AS AMSize, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_692
test("TC_692", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, MAX(Carbon_automation.gamePointId) AS Max_gamePointId FROM ( SELECT AMSize, deviceInformationId,gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, MAX(Carbon_automation_hive.gamePointId) AS Max_gamePointId FROM ( SELECT AMSize, deviceInformationId,gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_693
test("TC_693", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, MIN(Carbon_automation.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize,gamePointId,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId, MIN(Carbon_automation_hive.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize,gamePointId,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_694
test("TC_694", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry,deviceInformationId, gamePointId,Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry,deviceInformationId, gamePointId,Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId DESC""")
}
       

//TC_695
test("TC_695", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.gamePointId AS gamePointId FROM ( SELECT AMSize, deviceInformationId,gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize DESC, Carbon_automation.gamePointId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.gamePointId AS gamePointId FROM ( SELECT AMSize, deviceInformationId,gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize DESC, Carbon_automation_hive.gamePointId DESC""")
}
       

//TC_696
test("TC_696", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry,deviceInformationId, gamePointId,Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.AMSize DESC, Carbon_automation.gamePointId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.gamePointId AS gamePointId FROM ( SELECT AMSize, ActiveCountry,deviceInformationId, gamePointId,Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.AMSize DESC, Carbon_automation_hive.gamePointId DESC""")
}
       

//TC_697
test("TC_697", Include) {
  sql(s"""SELECT Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation.AMSize) AS DistinctCount_AMSize, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,deviceInformationId, gamePointId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""").collect
}
       

//TC_698
test("TC_698", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.Activecity AS Activecity, Carbon_automation.AMSize AS AMSize FROM ( SELECT AMSize,gamePointId, ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize ORDER BY Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.AMSize AS AMSize FROM ( SELECT AMSize,gamePointId, ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize ORDER BY Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_699
test("TC_699", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(distinct Carbon_automation.gamePointId) AS Sum_distinct_gamePointId, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation Full outer Join ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(distinct Carbon_automation_hive.gamePointId) AS Sum_distinct_gamePointId, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive Full outer Join ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_700
test("TC_700", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(distinct Carbon_automation.deviceInformationId) AS Sum_distinct_deviceInformationId, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation Full outer Join ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(distinct Carbon_automation_hive.deviceInformationId) AS Sum_distinct_deviceInformationId, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize,gamePointId, deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive Full outer Join ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.AMSize = Carbon_automation_hive1.AMSize GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_701
test("TC_701", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId, First(Carbon_automation.deviceInformationId) AS First_deviceInformationId FROM ( SELECT AMSize,deviceInformationId, gamePointId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation Full outer Join ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC
""").collect
}
       

//TC_702
test("TC_702", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId, First(Carbon_automation.gamePointId) AS First_gamePointId FROM ( SELECT AMSize,gamePointId ,deviceInformationId, ActiveCountry, Activecity FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation Full outer Join ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.AMSize = Carbon_automation1.AMSize GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC
""").collect
}
       

//TC_703
test("TC_703", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize = "5RAM size"""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize = "5RAM size"""")
}
       

//TC_704
test("TC_704", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize > "5RAM size"""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize > "5RAM size"""")
}
       

//TC_705
test("TC_705", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize >= "5RAM size"""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize >= "5RAM size"""")
}
       

//TC_706
test("TC_706", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize < "5RAM size"""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize < "5RAM size"""")
}
       

//TC_707
test("TC_707", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize <= "5RAM size"""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize <= "5RAM size"""")
}
       

//TC_708
test("TC_708", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize LIKE '%7RAM size%' GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize LIKE '%7RAM size%' GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_709
test("TC_709", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize LIKE '6RAM %' GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize LIKE '6RAM %' GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_710
test("TC_710", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize BETWEEN "6RAM size" AND "9RAM size" GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize BETWEEN "6RAM size" AND "9RAM size" GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_711
test("TC_711", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize IN ("6RAM size","8RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize IN ("6RAM size","8RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_712
test("TC_712", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize IS NULL GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize IS NULL GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_713
test("TC_713", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE  NOT(Carbon_automation.AMSize = "5RAM size")""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE  NOT(Carbon_automation_hive.AMSize = "5RAM size")""")
}
       

//TC_714
test("TC_714", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE  NOT(Carbon_automation.AMSize > "5RAM size")""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE  NOT(Carbon_automation_hive.AMSize > "5RAM size")""")
}
       

//TC_715
test("TC_715", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT( Carbon_automation.AMSize >= "5RAM size")""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT( Carbon_automation_hive.AMSize >= "5RAM size")""")
}
       

//TC_716
test("TC_716", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT( Carbon_automation.AMSize < "5RAM size")""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT( Carbon_automation_hive.AMSize < "5RAM size")""")
}
       

//TC_717
test("TC_717", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize <= "5RAM size")""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize <= "5RAM size")""")
}
       

//TC_718
test("TC_718", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize LIKE '%6RAM size%') GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize LIKE '%6RAM size%') GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_719
test("TC_719", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize LIKE '7RAM%') GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize LIKE '7RAM%') GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_720
test("TC_720", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize BETWEEN "7RAM size" AND "9RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize BETWEEN "7RAM size" AND "9RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_721
test("TC_721", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_722
test("TC_722", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize IS NOT NULL GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize IS NOT NULL GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_723
test("TC_723", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize,deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity, Carbon_automation.deviceInformationId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC, Carbon_automation.deviceInformationId ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize,deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity, Carbon_automation_hive.deviceInformationId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC, Carbon_automation_hive.deviceInformationId ASC""")
}
       

//TC_724
test("TC_724", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity, Carbon_automation.deviceInformationId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC, Carbon_automation.deviceInformationId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity, Carbon_automation_hive.deviceInformationId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC, Carbon_automation_hive.deviceInformationId DESC""")
}
       

//TC_725
test("TC_725", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_726
test("TC_726", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, AVG(Carbon_automation.deviceInformationId) AS Avg_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, AVG(Carbon_automation_hive.deviceInformationId) AS Avg_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_727
test("TC_727", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(Carbon_automation.deviceInformationId) AS Count_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(Carbon_automation_hive.deviceInformationId) AS Count_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_728
test("TC_728", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation.deviceInformationId) AS LONG_COL_0 FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation_hive.deviceInformationId) AS LONG_COL_0 FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_729
test("TC_729", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_730
test("TC_730", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MAX(Carbon_automation.deviceInformationId) AS Max_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, MAX(Carbon_automation_hive.deviceInformationId) AS Max_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_731
test("TC_731", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MIN(Carbon_automation.deviceInformationId) AS Min_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, MIN(Carbon_automation_hive.deviceInformationId) AS Min_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_732
test("TC_732", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_733
test("TC_733", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_734
test("TC_734", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId DESC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId DESC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_735
test("TC_735", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_736
test("TC_736", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, AVG(Carbon_automation.gamePointId) AS Avg_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, AVG(Carbon_automation_hive.gamePointId) AS Avg_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_737
test("TC_737", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(Carbon_automation.gamePointId) AS Count_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(Carbon_automation_hive.gamePointId) AS Count_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_738
test("TC_738", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation.gamePointId) AS DistinctCount_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation_hive.gamePointId) AS DistinctCount_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_739
test("TC_739", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_740
test("TC_740", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MAX(Carbon_automation.gamePointId) AS Max_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, MAX(Carbon_automation_hive.gamePointId) AS Max_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_741
test("TC_741", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MIN(Carbon_automation.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, MIN(Carbon_automation_hive.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_742
test("TC_742", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MIN(Carbon_automation.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, MIN(Carbon_automation_hive.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_743
test("TC_743", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize DESC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize DESC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_744
test("TC_744", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(Carbon_automation.AMSize) AS Count_AMSize FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(Carbon_automation_hive.AMSize) AS Count_AMSize FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_745
test("TC_745", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation.AMSize) AS DistinctCount_AMSize FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation_hive.AMSize) AS DistinctCount_AMSize FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_746
test("TC_746", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId ORDER BY Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId ORDER BY Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_747
test("TC_747", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(distinct Carbon_automation.deviceInformationId) AS Sum_distinct_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(distinct Carbon_automation_hive.deviceInformationId) AS Sum_distinct_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_748
test("TC_748", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(distinct Carbon_automation.gamePointId) AS Sum_distinct_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(distinct Carbon_automation_hive.gamePointId) AS Sum_distinct_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_749
test("TC_749", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, First( Carbon_automation.deviceInformationId) AS First_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC
""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, First( Carbon_automation_hive.deviceInformationId) AS First_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC
""")
}
       

//TC_750
test("TC_750", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, First(Carbon_automation.gamePointId) AS First_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""").collect
}
       

//TC_751
test("TC_751", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize = "5RAM size"""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize = "5RAM size"""")
}
       

//TC_752
test("TC_752", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize > "5RAM size"""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize > "5RAM size"""")
}
       

//TC_753
test("TC_753", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize >= "5RAM size"""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize >= "5RAM size"""")
}
       

//TC_754
test("TC_754", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize < "5RAM size"""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize < "5RAM size"""")
}
       

//TC_755
test("TC_755", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize <= "5RAM size"""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize <= "5RAM size"""")
}
       

//TC_756
test("TC_756", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize LIKE '%7RAM size%' GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize LIKE '%7RAM size%' GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_757
test("TC_757", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize LIKE '6RAM %' GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize LIKE '6RAM %' GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_758
test("TC_758", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize BETWEEN "6RAM size" AND "9RAM size" GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize BETWEEN "6RAM size" AND "9RAM size" GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_759
test("TC_759", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize IN ("6RAM size","8RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize IN ("6RAM size","8RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_760
test("TC_760", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize IS NULL GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize IS NULL GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_761
test("TC_761", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE  NOT(Carbon_automation.AMSize = "5RAM size")""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE  NOT(Carbon_automation_hive.AMSize = "5RAM size")""")
}
       

//TC_762
test("TC_762", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE  NOT(Carbon_automation.AMSize > "5RAM size")""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE  NOT(Carbon_automation_hive.AMSize > "5RAM size")""")
}
       

//TC_763
test("TC_763", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT( Carbon_automation.AMSize >= "5RAM size")""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT( Carbon_automation_hive.AMSize >= "5RAM size")""")
}
       

//TC_764
test("TC_764", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT( Carbon_automation.AMSize < "5RAM size")""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT( Carbon_automation_hive.AMSize < "5RAM size")""")
}
       

//TC_765
test("TC_765", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize <= "5RAM size")""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize <= "5RAM size")""")
}
       

//TC_766
test("TC_766", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize LIKE '%6RAM size%') GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize LIKE '%6RAM size%') GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_767
test("TC_767", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize LIKE '7RAM%') GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize LIKE '7RAM%') GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_768
test("TC_768", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize BETWEEN "7RAM size" AND "9RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize BETWEEN "7RAM size" AND "9RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_769
test("TC_769", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_770
test("TC_770", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize IS NOT NULL GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize IS NOT NULL GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_771
test("TC_771", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize,deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity, Carbon_automation.deviceInformationId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC, Carbon_automation.deviceInformationId ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize,deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity, Carbon_automation_hive.deviceInformationId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC, Carbon_automation_hive.deviceInformationId ASC""")
}
       

//TC_772
test("TC_772", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity, Carbon_automation.deviceInformationId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC, Carbon_automation.deviceInformationId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity, Carbon_automation_hive.deviceInformationId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC, Carbon_automation_hive.deviceInformationId DESC""")
}
       

//TC_773
test("TC_773", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_774
test("TC_774", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, AVG(Carbon_automation.deviceInformationId) AS Avg_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, AVG(Carbon_automation_hive.deviceInformationId) AS Avg_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_775
test("TC_775", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(Carbon_automation.deviceInformationId) AS Count_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(Carbon_automation_hive.deviceInformationId) AS Count_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_776
test("TC_776", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation.deviceInformationId) AS LONG_COL_0 FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation_hive.deviceInformationId) AS LONG_COL_0 FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_777
test("TC_777", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_778
test("TC_778", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MAX(Carbon_automation.deviceInformationId) AS Max_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, MAX(Carbon_automation_hive.deviceInformationId) AS Max_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_779
test("TC_779", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MIN(Carbon_automation.deviceInformationId) AS Min_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, MIN(Carbon_automation_hive.deviceInformationId) AS Min_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_780
test("TC_780", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_781
test("TC_781", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_782
test("TC_782", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId DESC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId DESC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_783
test("TC_783", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_784
test("TC_784", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, AVG(Carbon_automation.gamePointId) AS Avg_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, AVG(Carbon_automation_hive.gamePointId) AS Avg_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_785
test("TC_785", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(Carbon_automation.gamePointId) AS Count_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(Carbon_automation_hive.gamePointId) AS Count_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_786
test("TC_786", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation.gamePointId) AS DistinctCount_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation_hive.gamePointId) AS DistinctCount_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_787
test("TC_787", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_788
test("TC_788", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MAX(Carbon_automation.gamePointId) AS Max_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, MAX(Carbon_automation_hive.gamePointId) AS Max_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_789
test("TC_789", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MIN(Carbon_automation.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, MIN(Carbon_automation_hive.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_790
test("TC_790", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MIN(Carbon_automation.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, MIN(Carbon_automation_hive.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_791
test("TC_791", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize DESC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize DESC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_792
test("TC_792", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(Carbon_automation.AMSize) AS Count_AMSize FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(Carbon_automation_hive.AMSize) AS Count_AMSize FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_793
test("TC_793", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation.AMSize) AS DistinctCount_AMSize FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation_hive.AMSize) AS DistinctCount_AMSize FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_794
test("TC_794", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId ORDER BY Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId ORDER BY Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_795
test("TC_795", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(distinct Carbon_automation.deviceInformationId) AS Sum_distinct_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation Left Join ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(distinct Carbon_automation_hive.deviceInformationId) AS Sum_distinct_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive Left Join ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_796
test("TC_796", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(distinct Carbon_automation.gamePointId) AS Sum_distinct_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation Left Join ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(distinct Carbon_automation_hive.gamePointId) AS Sum_distinct_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive Left Join ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_797
test("TC_797", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, First( Carbon_automation.deviceInformationId) AS First_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation Left Join ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC
""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, First( Carbon_automation_hive.deviceInformationId) AS First_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive Left Join ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC
""")
}
       

//TC_798
test("TC_798", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, First(Carbon_automation.gamePointId) AS First_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation Left Join ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""").collect
}
       

//TC_799
test("TC_799", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize = "5RAM size"""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize = "5RAM size"""")
}
       

//TC_800
test("TC_800", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize > "5RAM size"""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize > "5RAM size"""")
}
       

//TC_801
test("TC_801", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize >= "5RAM size"""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize >= "5RAM size"""")
}
       

//TC_802
test("TC_802", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize < "5RAM size"""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize < "5RAM size"""")
}
       

//TC_803
test("TC_803", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize <= "5RAM size"""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize <= "5RAM size"""")
}
       

//TC_804
test("TC_804", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize LIKE '%7RAM size%' GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize LIKE '%7RAM size%' GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_805
test("TC_805", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize LIKE '6RAM %' GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize LIKE '6RAM %' GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_806
test("TC_806", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize BETWEEN "6RAM size" AND "9RAM size" GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize BETWEEN "6RAM size" AND "9RAM size" GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_807
test("TC_807", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize IN ("6RAM size","8RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize IN ("6RAM size","8RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_808
test("TC_808", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize IS NULL GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize IS NULL GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_809
test("TC_809", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE  NOT(Carbon_automation.AMSize = "5RAM size")""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE  NOT(Carbon_automation_hive.AMSize = "5RAM size")""")
}
       

//TC_810
test("TC_810", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE  NOT(Carbon_automation.AMSize > "5RAM size")""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE  NOT(Carbon_automation_hive.AMSize > "5RAM size")""")
}
       

//TC_811
test("TC_811", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT( Carbon_automation.AMSize >= "5RAM size")""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT( Carbon_automation_hive.AMSize >= "5RAM size")""")
}
       

//TC_812
test("TC_812", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT( Carbon_automation.AMSize < "5RAM size")""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT( Carbon_automation_hive.AMSize < "5RAM size")""")
}
       

//TC_813
test("TC_813", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize <= "5RAM size")""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize <= "5RAM size")""")
}
       

//TC_814
test("TC_814", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize LIKE '%6RAM size%') GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize LIKE '%6RAM size%') GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_815
test("TC_815", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize LIKE '7RAM%') GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize LIKE '7RAM%') GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_816
test("TC_816", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize BETWEEN "7RAM size" AND "9RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize BETWEEN "7RAM size" AND "9RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_817
test("TC_817", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_818
test("TC_818", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize IS NOT NULL GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize IS NOT NULL GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_819
test("TC_819", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize,deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity, Carbon_automation.deviceInformationId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC, Carbon_automation.deviceInformationId ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize,deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity, Carbon_automation_hive.deviceInformationId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC, Carbon_automation_hive.deviceInformationId ASC""")
}
       

//TC_820
test("TC_820", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity, Carbon_automation.deviceInformationId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC, Carbon_automation.deviceInformationId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity, Carbon_automation_hive.deviceInformationId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC, Carbon_automation_hive.deviceInformationId DESC""")
}
       

//TC_821
test("TC_821", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_822
test("TC_822", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, AVG(Carbon_automation.deviceInformationId) AS Avg_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, AVG(Carbon_automation_hive.deviceInformationId) AS Avg_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_823
test("TC_823", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(Carbon_automation.deviceInformationId) AS Count_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(Carbon_automation_hive.deviceInformationId) AS Count_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_824
test("TC_824", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation.deviceInformationId) AS LONG_COL_0 FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation_hive.deviceInformationId) AS LONG_COL_0 FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_825
test("TC_825", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_826
test("TC_826", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MAX(Carbon_automation.deviceInformationId) AS Max_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, MAX(Carbon_automation_hive.deviceInformationId) AS Max_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_827
test("TC_827", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MIN(Carbon_automation.deviceInformationId) AS Min_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, MIN(Carbon_automation_hive.deviceInformationId) AS Min_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_828
test("TC_828", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_829
test("TC_829", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_830
test("TC_830", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId DESC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId DESC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_831
test("TC_831", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_832
test("TC_832", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, AVG(Carbon_automation.gamePointId) AS Avg_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, AVG(Carbon_automation_hive.gamePointId) AS Avg_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_833
test("TC_833", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(Carbon_automation.gamePointId) AS Count_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(Carbon_automation_hive.gamePointId) AS Count_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_834
test("TC_834", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation.gamePointId) AS DistinctCount_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation_hive.gamePointId) AS DistinctCount_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_835
test("TC_835", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_836
test("TC_836", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MAX(Carbon_automation.gamePointId) AS Max_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, MAX(Carbon_automation_hive.gamePointId) AS Max_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_837
test("TC_837", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MIN(Carbon_automation.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, MIN(Carbon_automation_hive.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_838
test("TC_838", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MIN(Carbon_automation.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, MIN(Carbon_automation_hive.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_839
test("TC_839", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize DESC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize DESC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_840
test("TC_840", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(Carbon_automation.AMSize) AS Count_AMSize FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(Carbon_automation_hive.AMSize) AS Count_AMSize FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_841
test("TC_841", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation.AMSize) AS DistinctCount_AMSize FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation_hive.AMSize) AS DistinctCount_AMSize FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_842
test("TC_842", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId ORDER BY Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId ORDER BY Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_843
test("TC_843", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(distinct Carbon_automation.deviceInformationId) AS Sum_distinct_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(distinct Carbon_automation_hive.deviceInformationId) AS Sum_distinct_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_844
test("TC_844", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(distinct Carbon_automation.gamePointId) AS Sum_distinct_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(distinct Carbon_automation_hive.gamePointId) AS Sum_distinct_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_845
test("TC_845", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, First( Carbon_automation.deviceInformationId) AS First_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, First( Carbon_automation_hive.deviceInformationId) AS First_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_846
test("TC_846", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, First(Carbon_automation.gamePointId) AS First_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation RIGHT JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""").collect
}
       

//TC_847
test("TC_847", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize = "5RAM size"""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize = "5RAM size"""")
}
       

//TC_848
test("TC_848", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize > "5RAM size"""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize > "5RAM size"""")
}
       

//TC_849
test("TC_849", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize >= "5RAM size"""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize >= "5RAM size"""")
}
       

//TC_850
test("TC_850", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize < "5RAM size"""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize < "5RAM size"""")
}
       

//TC_851
test("TC_851", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize <= "5RAM size"""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize <= "5RAM size"""")
}
       

//TC_852
test("TC_852", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize LIKE '%7RAM size%' GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize LIKE '%7RAM size%' GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_853
test("TC_853", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize LIKE '6RAM %' GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize LIKE '6RAM %' GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_854
test("TC_854", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize BETWEEN "6RAM size" AND "9RAM size" GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize BETWEEN "6RAM size" AND "9RAM size" GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_855
test("TC_855", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize IN ("6RAM size","8RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize IN ("6RAM size","8RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_856
test("TC_856", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize IS NULL GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize IS NULL GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_857
test("TC_857", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE  NOT(Carbon_automation.AMSize = "5RAM size")""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE  NOT(Carbon_automation_hive.AMSize = "5RAM size")""")
}
       

//TC_858
test("TC_858", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE  NOT(Carbon_automation.AMSize > "5RAM size")""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE  NOT(Carbon_automation_hive.AMSize > "5RAM size")""")
}
       

//TC_859
test("TC_859", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT( Carbon_automation.AMSize >= "5RAM size")""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT( Carbon_automation_hive.AMSize >= "5RAM size")""")
}
       

//TC_860
test("TC_860", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT( Carbon_automation.AMSize < "5RAM size")""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT( Carbon_automation_hive.AMSize < "5RAM size")""")
}
       

//TC_861
test("TC_861", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize <= "5RAM size")""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,  ActiveCountry,  Activecity, gamePointId  FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize,  ActiveCountry,  Activecity,  gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize <= "5RAM size")""")
}
       

//TC_862
test("TC_862", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize LIKE '%6RAM size%') GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize LIKE '%6RAM size%') GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_863
test("TC_863", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize LIKE '7RAM%') GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize LIKE '7RAM%') GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_864
test("TC_864", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize BETWEEN "7RAM size" AND "9RAM size") GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize BETWEEN "7RAM size" AND "9RAM size") GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_865
test("TC_865", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_866
test("TC_866", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE Carbon_automation.AMSize IS NOT NULL GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE Carbon_automation_hive.AMSize IS NOT NULL GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_867
test("TC_867", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize,deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity, Carbon_automation.deviceInformationId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC, Carbon_automation.deviceInformationId ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize,deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity, Carbon_automation_hive.deviceInformationId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC, Carbon_automation_hive.deviceInformationId ASC""")
}
       

//TC_868
test("TC_868", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, Carbon_automation.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId WHERE NOT(Carbon_automation.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity, Carbon_automation.deviceInformationId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC, Carbon_automation.deviceInformationId DESC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, Carbon_automation_hive.deviceInformationId AS deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId WHERE NOT(Carbon_automation_hive.AMSize IN ("5RAM size","8RAM size","7RAM size")) GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity, Carbon_automation_hive.deviceInformationId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC, Carbon_automation_hive.deviceInformationId DESC""")
}
       

//TC_869
test("TC_869", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.deviceInformationId) AS Sum_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_870
test("TC_870", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, AVG(Carbon_automation.deviceInformationId) AS Avg_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, AVG(Carbon_automation_hive.deviceInformationId) AS Avg_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_871
test("TC_871", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(Carbon_automation.deviceInformationId) AS Count_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(Carbon_automation_hive.deviceInformationId) AS Count_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_872
test("TC_872", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation.deviceInformationId) AS LONG_COL_0 FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation_hive.deviceInformationId) AS LONG_COL_0 FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_873
test("TC_873", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.deviceInformationId AS deviceInformationId, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.deviceInformationId AS deviceInformationId, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_874
test("TC_874", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MAX(Carbon_automation.deviceInformationId) AS Max_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, MAX(Carbon_automation_hive.deviceInformationId) AS Max_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_875
test("TC_875", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MIN(Carbon_automation.deviceInformationId) AS Min_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, MIN(Carbon_automation_hive.deviceInformationId) AS Min_deviceInformationId FROM ( SELECT AMSize,deviceInformationId,  ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_876
test("TC_876", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_877
test("TC_877", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_878
test("TC_878", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId DESC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId DESC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_879
test("TC_879", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(Carbon_automation.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(Carbon_automation_hive.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_880
test("TC_880", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, AVG(Carbon_automation.gamePointId) AS Avg_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, AVG(Carbon_automation_hive.gamePointId) AS Avg_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_881
test("TC_881", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(Carbon_automation.gamePointId) AS Count_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(Carbon_automation_hive.gamePointId) AS Count_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_882
test("TC_882", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation.gamePointId) AS DistinctCount_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation_hive.gamePointId) AS DistinctCount_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_883
test("TC_883", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_884
test("TC_884", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MAX(Carbon_automation.gamePointId) AS Max_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, MAX(Carbon_automation_hive.gamePointId) AS Max_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_885
test("TC_885", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MIN(Carbon_automation.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, MIN(Carbon_automation_hive.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_886
test("TC_886", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, MIN(Carbon_automation.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, MIN(Carbon_automation_hive.gamePointId) AS Min_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_887
test("TC_887", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize DESC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize DESC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_888
test("TC_888", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(Carbon_automation.AMSize) AS Count_AMSize FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(Carbon_automation_hive.AMSize) AS Count_AMSize FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_889
test("TC_889", Include) {
  checkAnswer(s"""SELECT Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation.AMSize) AS DistinctCount_AMSize FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, COUNT(DISTINCT Carbon_automation_hive.AMSize) AS DistinctCount_AMSize FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_890
test("TC_890", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId ORDER BY Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId ORDER BY Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_891
test("TC_891", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(distinct Carbon_automation.deviceInformationId) AS Sum_distinct_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(distinct Carbon_automation_hive.deviceInformationId) AS Sum_distinct_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_892
test("TC_892", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, SUM(distinct Carbon_automation.gamePointId) AS Sum_distinct_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, SUM(distinct Carbon_automation_hive.gamePointId) AS Sum_distinct_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC""")
}
       

//TC_893
test("TC_893", Include) {
  checkAnswer(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.gamePointId AS gamePointId, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, First( Carbon_automation.deviceInformationId) AS First_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.gamePointId, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.gamePointId ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC
""",
    s"""SELECT Carbon_automation_hive.AMSize AS AMSize, Carbon_automation_hive.gamePointId AS gamePointId, Carbon_automation_hive.ActiveCountry AS ActiveCountry, Carbon_automation_hive.Activecity AS Activecity, First( Carbon_automation_hive.deviceInformationId) AS First_deviceInformationId FROM ( SELECT AMSize, deviceInformationId, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_hive) SUB_QRY ) Carbon_automation_hive1 ON Carbon_automation_hive.gamePointId = Carbon_automation_hive1.gamePointId GROUP BY Carbon_automation_hive.AMSize, Carbon_automation_hive.gamePointId, Carbon_automation_hive.ActiveCountry, Carbon_automation_hive.Activecity ORDER BY Carbon_automation_hive.AMSize ASC, Carbon_automation_hive.gamePointId ASC, Carbon_automation_hive.ActiveCountry ASC, Carbon_automation_hive.Activecity ASC
""")
}
       

//TC_894
test("TC_894", Include) {
  sql(s"""SELECT Carbon_automation.AMSize AS AMSize, Carbon_automation.ActiveCountry AS ActiveCountry, Carbon_automation.Activecity AS Activecity, First(Carbon_automation.gamePointId) AS First_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation) SUB_QRY ) Carbon_automation1 ON Carbon_automation.gamePointId = Carbon_automation1.gamePointId GROUP BY Carbon_automation.AMSize, Carbon_automation.ActiveCountry, Carbon_automation.Activecity ORDER BY Carbon_automation.AMSize ASC, Carbon_automation.ActiveCountry ASC, Carbon_automation.Activecity ASC""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC001
test("PushUP_FILTER_Carbon_automation_TC001", Include) {
  sql(s"""select contractNumber,gamePointId,series from Carbon_automation where series='8Series' or  internalModels='8Internal models' or deviceinformationid='10000' or Latest_YEAR='2015'""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC002
test("PushUP_FILTER_Carbon_automation_TC002", Include) {
  sql(s"""select contractNumber,gamePointId,series  from Carbon_automation where (deviceInformationId=100 and deviceColor='1Device Color' and Latest_MONTH!='2016') OR (deviceInformationId=10 and deviceColor='0Device Color' and Latest_MONTH='2015')""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC003
test("PushUP_FILTER_Carbon_automation_TC003", Include) {
  sql(s"""select imei from Carbon_automation where  (contractNumber == 9.2233720368547696E16) and (gamePointId== 4.29496729780698E23) and (deviceInformationId==100) and (Latest_MONTH==7)""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC004
test("PushUP_FILTER_Carbon_automation_TC004", Include) {
  sql(s"""select imei from Carbon_automation where  (contractNumber == 9.2233720368547696E16) or (gamePointId== 4.29496729780698E23) or (deviceInformationId==100) or (Latest_MONTH==7)""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC005
test("PushUP_FILTER_Carbon_automation_TC005", Include) {
  sql(s"""select imei from Carbon_automation where  (contractNumber == 9.2233720368547696E16) and (gamePointId== 4.29496729780698E23) or (deviceInformationId==100) and (Latest_MONTH==7)""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC006
test("PushUP_FILTER_Carbon_automation_TC006", Include) {
  sql(s"""select contractNumber,gamePointId,series from Carbon_automation where series !='8Series' and internalModels !='8Internal models' and deviceInformationId!=101 and Latest_MONTH!=1""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC007
test("PushUP_FILTER_Carbon_automation_TC007", Include) {
  sql(s"""select contractNumber,gamePointId,series from Carbon_automation where series !='8Series' or internalModels !='8Internal models' or deviceInformationId!=101 or Latest_MONTH!=1""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC008
test("PushUP_FILTER_Carbon_automation_TC008", Include) {
  sql(s"""select contractNumber,gamePointId,series from Carbon_automation where series !='8Series' or internalModels !='8Internal models' and deviceInformationId!=101 or Latest_MONTH!=1""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC009
test("PushUP_FILTER_Carbon_automation_TC009", Include) {
  sql(s"""select imei, Latest_DAY,deviceInformationId from Carbon_automation where deviceInformationId IS NOT NULL and Latest_HOUR IS NOT NULL and productiondate IS NOT NULL""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC010
test("PushUP_FILTER_Carbon_automation_TC010", Include) {
  sql(s"""select imei, Latest_DAY,deviceInformationId from Carbon_automation where deviceInformationId IS NOT NULL or Latest_HOUR IS NOT NULL or productiondate IS NOT NULL""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC011
test("PushUP_FILTER_Carbon_automation_TC011", Include) {
  sql(s"""select imei, Latest_DAY,deviceInformationId from Carbon_automation where deviceInformationId IS NOT NULL and Latest_HOUR IS NOT NULL or productiondate IS NOT NULL""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC012
test("PushUP_FILTER_Carbon_automation_TC012", Include) {
  sql(s"""select imei, Latest_DAY,deviceInformationId from Carbon_automation where Latest_DAY is null and Latest_DAY is null""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC013
test("PushUP_FILTER_Carbon_automation_TC013", Include) {
  sql(s"""select imei, Latest_DAY,deviceInformationId from Carbon_automation where Latest_DAY is null or Latest_DAY is null""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC014
test("PushUP_FILTER_Carbon_automation_TC014", Include) {
  sql(s"""select imei, Latest_DAY,deviceInformationId from Carbon_automation where deviceInformationId IS NOT NULL or Latest_HOUR IS NOT NULL and Latest_DAY is NULL""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC015
test("PushUP_FILTER_Carbon_automation_TC015", Include) {
  sql(s"""select imei,series from Carbon_automation where Carbon_automation.series  NOT IN ('1Series','7Series') and Carbon_automation.deviceInformationId NOT IN (1,10) and Carbon_automation.latest_year NOT IN (2014,2016)""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC016
test("PushUP_FILTER_Carbon_automation_TC016", Include) {
  sql(s"""select imei,series from Carbon_automation where Carbon_automation.series IN   ('1Series','7Series') and Carbon_automation.deviceInformationId IN (1,10) and Carbon_automation.latest_year IN (2015)""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC017
test("PushUP_FILTER_Carbon_automation_TC017", Include) {
  sql(s"""select imei,series from Carbon_automation where Carbon_automation.series IN   ('1Series','7Series') or Carbon_automation.deviceInformationId IN (1,10) and Carbon_automation.latest_year NOT IN (2014,2016)""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC018
test("PushUP_FILTER_Carbon_automation_TC018", Include) {
  sql(s"""select imei, Latest_DAY+ 10 as a,deviceInformationId+0.99,latest_year+10.1,gamePointId+contractNumber as b from Carbon_automation""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC019
test("PushUP_FILTER_Carbon_automation_TC019", Include) {
  sql(s"""select imei, Latest_DAY- 10 as a,deviceInformationId-0.85,latest_year-100.1,gamePointId-contractNumber as b from Carbon_automation""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC020
test("PushUP_FILTER_Carbon_automation_TC020", Include) {
  sql(s"""select imei, Latest_DAY*10 as a,deviceInformationId*0.94,latest_year*109.1,gamePointId*contractNumber as b from Carbon_automation""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC021
test("PushUP_FILTER_Carbon_automation_TC021", Include) {
  sql(s"""select imei, Latest_DAY/10 as a,deviceInformationId/0.74,latest_year/10000.1,gamePointId/contractNumber as b from Carbon_automation""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC022
test("PushUP_FILTER_Carbon_automation_TC022", Include) {
  sql(s"""select contractNumber,gamePointId,series,deviceInformationId from Carbon_automation where deviceInformationId >10 and latest_year>2014 and gamePointId >2738.562""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC023
test("PushUP_FILTER_Carbon_automation_TC023", Include) {
  sql(s"""select contractNumber,gamePointId,series,deviceInformationId from Carbon_automation where deviceInformationId <1000 and latest_year<2016 or gamePointId <2739.562""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC024
test("PushUP_FILTER_Carbon_automation_TC024", Include) {
  sql(s"""select contractNumber,gamePointId,series,deviceInformationId from Carbon_automation where deviceInformationId >=10 and latest_year>=2014 and gamePointId >=2738.562""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC025
test("PushUP_FILTER_Carbon_automation_TC025", Include) {
  sql(s"""select contractNumber,gamePointId,series,deviceInformationId from Carbon_automation where deviceInformationId <=1000 and latest_year<=2016 or gamePointId <=2738.562""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC026
test("PushUP_FILTER_Carbon_automation_TC026", Include) {
  sql(s"""select imei,gamePointId, channelsId,series from Carbon_automation where  channelsId LIKE 4 and deviceInformationId like 10 and latest_year like 2015 ORDER BY gamePointId limit 5""").collect
}
       

//PushUP_FILTER_Carbon_automation_TC027
test("PushUP_FILTER_Carbon_automation_TC027", Include) {
  sql(s"""select imei, Latest_DAY from Carbon_automation where channelsId not LIKE 4 and deviceInformationId not like 10 and latest_year not like 2014""").collect
}
       

//TC_000_drop
test("TC_000_drop", Include) {
  sql(s"""drop table if exists Carbon_automation""").collect

  sql(s"""drop table if exists Carbon_automation_hive""").collect

}
       

//TC_0000_Drop
test("TC_0000_Drop", Include) {
  sql(s"""drop table if exists  Carbon_automation1""").collect

  sql(s"""drop table if exists  Carbon_automation1_hive""").collect

}
       

//TC_0000
test("TC_0000", Include) {
  sql(s"""create table Carbon_automation1 (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES ('DICTIONARY_INCLUDE'='deviceInformationId,Latest_YEAR,Latest_MONTH,Latest_DAY')""").collect

  sql(s"""create table Carbon_automation1_hive (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//TC_00001
test("TC_00001", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/VmaLL100/100olap.csv' INTO TABLE Carbon_automation1 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/VmaLL100/100olap.csv' INTO TABLE Carbon_automation1_hive """).collect

}

//POC_TableJoin_AllRecords_01
test("POC_TableJoin_AllRecords_01", Include) {
  sql(s"""select a.* from Carbon_automation a inner join Carbon_automation1 b on a.imei = b.imei and b.mac = 'MAC'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_02
test("POC_TableJoin_AllRecords_02", Include) {
  sql(s"""select a.devicecolor from Carbon_automation a inner join Carbon_automation1 b on a.imei = b.imei and b.mac = 'MAC'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_03
test("POC_TableJoin_AllRecords_03", Include) {
  sql(s"""select a.*,a.deviceinformationid from Carbon_automation a inner join Carbon_automation1 b on a.imei = b.imei and b.mac = 'MAC'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_04
test("POC_TableJoin_AllRecords_04", Include) {
  sql(s"""select a.*,b.device_backcolor from Carbon_automation a inner join Carbon_automation1 b on a.mac = b.mac and b.imei = '1AA100'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_05
test("POC_TableJoin_AllRecords_05", Include) {
  sql(s"""select b.*,a.contractnumber from Carbon_automation a inner join Carbon_automation1 b on a.mac = b.mac and b.imei = '1AA100'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_06
test("POC_TableJoin_AllRecords_06", Include) {
  sql(s"""select b.*,a.marketname,b.* from Carbon_automation a inner join Carbon_automation1 b on a.mac = b.mac and b.imei = '1AA100'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_07
test("POC_TableJoin_AllRecords_07", Include) {
  sql(s"""select b.*,a.* from Carbon_automation a inner join Carbon_automation1 b on a.mac = b.mac and b.imei = '1AA100'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_08
test("POC_TableJoin_AllRecords_08", Include) {
  sql(s"""select a.* from Carbon_automation a left outer join Carbon_automation1 b on a.imei = b.imei and b.mac = 'MAC'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_09
test("POC_TableJoin_AllRecords_09", Include) {
  sql(s"""select a.devicecolor from Carbon_automation a left outer join Carbon_automation1 b on a.imei = b.imei and b.mac = 'MAC'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_10
test("POC_TableJoin_AllRecords_10", Include) {
  sql(s"""select a.*,a.deviceinformationid from Carbon_automation a left outer join Carbon_automation1 b on a.imei = b.imei and b.mac = 'MAC'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_11
test("POC_TableJoin_AllRecords_11", Include) {
  sql(s"""select a.*,b.device_backcolor from Carbon_automation a left outer join Carbon_automation1 b on a.mac = b.mac and b.imei = '1AA100'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_12
test("POC_TableJoin_AllRecords_12", Include) {
  sql(s"""select b.*,a.contractnumber from Carbon_automation a left outer join Carbon_automation1 b on a.mac = b.mac and b.imei = '1AA100'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_13
test("POC_TableJoin_AllRecords_13", Include) {
  sql(s"""select b.*,a.marketname,b.* from Carbon_automation a left outer join Carbon_automation1 b on a.mac = b.mac and b.imei = '1AA100'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_14
test("POC_TableJoin_AllRecords_14", Include) {
  sql(s"""select b.*,a.* from Carbon_automation a left outer join Carbon_automation1 b on a.mac = b.mac and b.imei = '1AA100'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_15
test("POC_TableJoin_AllRecords_15", Include) {
  sql(s"""select a.* from Carbon_automation a right outer join Carbon_automation1 b on a.imei = b.imei and b.mac = 'MAC'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_16
test("POC_TableJoin_AllRecords_16", Include) {
  sql(s"""select a.devicecolor from Carbon_automation a right outer join Carbon_automation1 b on a.imei = b.imei and b.mac = 'MAC'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_17
test("POC_TableJoin_AllRecords_17", Include) {
  sql(s"""select a.*,a.deviceinformationid from Carbon_automation a right outer join Carbon_automation1 b on a.imei = b.imei and b.mac = 'MAC'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_18
test("POC_TableJoin_AllRecords_18", Include) {
  sql(s"""select a.*,b.device_backcolor from Carbon_automation a right outer join Carbon_automation1 b on a.mac = b.mac and b.imei = '1AA100'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_19
test("POC_TableJoin_AllRecords_19", Include) {
  sql(s"""select b.*,a.contractnumber from Carbon_automation a right outer join Carbon_automation1 b on a.mac = b.mac and b.imei = '1AA100'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_20
test("POC_TableJoin_AllRecords_20", Include) {
  sql(s"""select b.*,a.contractnumber from Carbon_automation a right outer join Carbon_automation1 b on a.mac = b.mac and b.imei = '1AA100'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_21
test("POC_TableJoin_AllRecords_21", Include) {
  sql(s"""select b.*,a.* from Carbon_automation a right outer join Carbon_automation1 b on a.mac = b.mac and b.imei = '1AA100'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_22
test("POC_TableJoin_AllRecords_22", Include) {
  sql(s"""select a.* from Carbon_automation a full outer join Carbon_automation1 b on a.imei = b.imei and b.mac = 'MAC'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_23
test("POC_TableJoin_AllRecords_23", Include) {
  sql(s"""select a.devicecolor from Carbon_automation a full outer join Carbon_automation1 b on a.imei = b.imei and b.mac = 'MAC'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_24
test("POC_TableJoin_AllRecords_24", Include) {
  sql(s"""select a.*,a.deviceinformationid from Carbon_automation a full outer join Carbon_automation1 b on a.imei = b.imei and b.mac = 'MAC'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_25
test("POC_TableJoin_AllRecords_25", Include) {
  sql(s"""select a.*,b.device_backcolor from Carbon_automation a full outer join Carbon_automation1 b on a.mac = b.mac and b.imei = '1AA100'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_26
test("POC_TableJoin_AllRecords_26", Include) {
  sql(s"""select b.*,a.contractnumber from Carbon_automation a full outer join Carbon_automation1 b on a.mac = b.mac and b.imei = '1AA100'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_27
test("POC_TableJoin_AllRecords_27", Include) {
  sql(s"""select b.*,a.contractnumber from Carbon_automation a full outer join Carbon_automation1 b on a.mac = b.mac and b.imei = '1AA100'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_28
test("POC_TableJoin_AllRecords_28", Include) {
  sql(s"""select b.*,a.* from Carbon_automation a full outer join Carbon_automation1 b on a.mac = b.mac and b.imei = '1AA100'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_29
test("POC_TableJoin_AllRecords_29", Include) {
  sql(s"""select a.* from Carbon_automation a left semi join Carbon_automation1 b on a.imei = b.imei and b.mac = 'MAC'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_30
test("POC_TableJoin_AllRecords_30", Include) {
  sql(s"""select a.devicecolor from Carbon_automation a left semi join Carbon_automation1 b on a.imei = b.imei and b.mac = 'MAC'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//POC_TableJoin_AllRecords_31
test("POC_TableJoin_AllRecords_31", Include) {
  sql(s"""select a.*,a.deviceinformationid from Carbon_automation a left semi join Carbon_automation1 b on a.imei = b.imei and b.mac = 'MAC'  where a.productionDate between '2015-07-01 12:07:28.0' and '2015-07-20 12:07:28.0' and a.amsize in ('8RAM size','7RAM size')""").collect
}
       

//SparkSQL_Compatibility_01
test("SparkSQL_Compatibility_01", Include) {
  sql(s"""select Carbon_automation.activeareaid,Carbon_automation1.activeareaid from Carbon_automation JOIN Carbon_automation1 ON (Carbon_automation.activeareaid<>Carbon_automation1.activeareaid)""").collect
}
       

//SparkSQL_Compatibility_02
test("SparkSQL_Compatibility_02", Include) {
  sql(s"""select Carbon_automation.latest_year,Carbon_automation1.latest_year from Carbon_automation JOIN Carbon_automation1 ON (Carbon_automation.latest_year<>Carbon_automation1.latest_year)""").collect
}
       

//SparkSQL_Compatibility_04
test("SparkSQL_Compatibility_04", Include) {
  sql(s"""SELECT Carbon_automation.* FROM Carbon_automation UNION SELECT * FROM Carbon_automation1""").collect
}
       

//SparkSQL_Compatibility_05
test("SparkSQL_Compatibility_05", Include) {
  sql(s"""SELECT Carbon_automation.imei FROM Carbon_automation UNION SELECT Carbon_automation1.imei FROM Carbon_automation1""").collect
}
       

//SparkSQL_Compatibility_06
test("SparkSQL_Compatibility_06", Include) {
  sql(s"""SELECT Carbon_automation.imei,latest_year FROM Carbon_automation UNION SELECT imei,latest_year FROM Carbon_automation1""").collect
}
       

//SparkSQL_Compatibility_10
test("SparkSQL_Compatibility_10", Include) {
  sql(s"""SELECT Carbon_automation.* FROM Carbon_automation EXCEPT SELECT * FROM Carbon_automation1""").collect
}
       

//SparkSQL_Compatibility_11
test("SparkSQL_Compatibility_11", Include) {
  sql(s"""SELECT Carbon_automation.imei FROM Carbon_automation EXCEPT select Carbon_automation1.imei FROM Carbon_automation1""").collect
}
       

//SparkSQL_Compatibility_12
test("SparkSQL_Compatibility_12", Include) {
  sql(s"""SELECT Carbon_automation.imei,latest_year FROM Carbon_automation EXCEPT SELECT imei,latest_year FROM Carbon_automation1""").collect
}
       

//SparkSQL_Compatibility_15
test("SparkSQL_Compatibility_15", Include) {
  sql(s"""SELECT Carbon_automation.*, count(imei) OVER (ORDER BY imei ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM Carbon_automation""").collect
}
       

//SparkSQL_Compatibility_16
test("SparkSQL_Compatibility_16", Include) {
  sql(s"""SELECT imei, count(imei), avg(deviceinformationid),min(deviceinformationid),max(deviceinformationid),sum(deviceinformationid) OVER (ORDER BY imei,deviceinformationid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM Carbon_automation group by imei,deviceinformationid order by imei,deviceinformationid""").collect
}
       

//SparkSQL_Compatibility_17
test("SparkSQL_Compatibility_17", Include) {
  sql(s"""SELECT activeareaid,imei, SUM(latest_year) FROM Carbon_automation GROUP BY ROLLUP(activeareaid, imei)""").collect
}
       

//SparkSQL_Compatibility_18
test("SparkSQL_Compatibility_18", Include) {
  sql(s"""SELECT activeareaid,imei, SUM(latest_year) FROM Carbon_automation GROUP BY activeareaid, imei with rollup""").collect
}
       

//SparkSQL_Compatibility_19
test("SparkSQL_Compatibility_19", Include) {
  sql(s"""SELECT activeareaid,imei, SUM(latest_year) FROM Carbon_automation GROUP BY CUBE(activeareaid, imei)""").collect
}
       

//SparkSQL_Compatibility_21
test("SparkSQL_Compatibility_21", Include) {
  sql(s"""select imei from (select imei,deviceInformationId from (select imei,deviceInformationId,MAC from Carbon_automation a)b)c""").collect
}
       

//Select_coalesce_01
test("Select_coalesce_01", Include) {
  sql(s"""select imei,deviceInformationId,MAC,deviceColor from Carbon_automation where coalesce( imei ,1) != '1AA1'""").collect
}
       

//Select_coalesce_02
test("Select_coalesce_02", Include) {
  sql(s"""select imei,deviceInformationId,productionDate from Carbon_automation where coalesce(deviceInformationId ,1) = 1000""").collect
}
       
  override def afterAll {
    sql("drop table if exists Carbon_automation")
    sql("drop table if exists Carbon_automation_hive")
    sql("drop table if exists Carbon_automation1")
    sql(s"""drop table if exists  Carbon_automation1""").collect
  }
}