
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
 * Test Class for QueriesExcludeDictionaryTestCase to verify all scenerios
 */

class QueriesExcludeDictionaryTestCase extends QueryTest with BeforeAndAfterAll {


  //DICTIONARY_EXCLUDE_CreateCube
  test("Queries_DICTIONARY_EXCLUDE_CreateCube", Include) {
    sql(s"""drop table if exists TABLE_DICTIONARY_EXCLUDE""").collect
    sql(s"""drop table if exists TABLE_DICTIONARY_EXCLUDE1_hive""").collect

    sql(s"""create table  TABLE_DICTIONARY_EXCLUDE (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt)  STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='imei')""").collect

    sql(s"""create table  TABLE_DICTIONARY_EXCLUDE1_hive (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string,deliveryTime string,channelsId string,channelsName string,deliveryAreaId string,deliveryCountry string,deliveryProvince string,deliveryCity string,deliveryDistrict string,deliveryStreet string,oxSingleNumber string,contractNumber BigInt,ActiveCheckTime string,ActiveAreaId string,ActiveCountry string,ActiveProvince string,Activecity string,ActiveDistrict string,ActiveStreet string,ActiveOperatorId string,Active_releaseId string,Active_EMUIVersion string,Active_operaSysVersion string,Active_BacVerNumber string,Active_BacFlashVer string,Active_webUIVersion string,Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string,Active_operatorsVersion string,Active_phonePADPartitionedVersions string,Latest_YEAR int,Latest_MONTH int,Latest_DAY Decimal(30,10),Latest_HOUR string,Latest_areaId string,Latest_country string,Latest_province string,Latest_city string,Latest_district string,Latest_street string,Latest_releaseId string,Latest_EMUIVersion string,Latest_operaSysVersion string,Latest_BacVerNumber string,Latest_BacFlashVer string,Latest_webUIVersion string,Latest_webUITypeCarrVer string,Latest_webTypeDataVerNumber string,Latest_operatorsVersion string,Latest_phonePADPartitionedVersions string,Latest_operatorId string,gamePointId double,gamePointDescription string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect


  }


  //DICTIONARY_EXCLUDE_CreateCube_count
  test("Queries_DICTIONARY_EXCLUDE_CreateCube_count", Include) {

    sql(s"""select count(*) from TABLE_DICTIONARY_EXCLUDE""").collect

  }


  //DICTIONARY_EXCLUDE_DataLoad
  test("Queries_DICTIONARY_EXCLUDE_DataLoad", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table TABLE_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table TABLE_DICTIONARY_EXCLUDE1_hive """).collect


  }


  //DICTIONARY_EXCLUDE_CreateCube1
  test("Queries_DICTIONARY_EXCLUDE_CreateCube1", Include) {
    sql(s"""drop table if exists TABLE_DICTIONARY_EXCLUDE1""").collect

    sql(s"""create table  TABLE_DICTIONARY_EXCLUDE1 (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt)  STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='imei')""").collect

  }


  //DICTIONARY_EXCLUDE_DataLoad1
  test("Queries_DICTIONARY_EXCLUDE_DataLoad1", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table TABLE_DICTIONARY_EXCLUDE1 options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  }


  //DICTIONARY_EXCLUDE_001
  test("Queries_DICTIONARY_EXCLUDE_001", Include) {

    checkAnswer(s"""Select count(imei) from TABLE_DICTIONARY_EXCLUDE""",
      s"""Select count(imei) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_001")

  }


  //DICTIONARY_EXCLUDE_002
  test("Queries_DICTIONARY_EXCLUDE_002", Include) {

    checkAnswer(s"""select count(DISTINCT imei) as a from TABLE_DICTIONARY_EXCLUDE""",
      s"""select count(DISTINCT imei) as a from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_002")

  }


  //DICTIONARY_EXCLUDE_003
  test("Queries_DICTIONARY_EXCLUDE_003", Include) {

    checkAnswer(s"""select sum(Latest_month)+10 as a ,imei  from TABLE_DICTIONARY_EXCLUDE group by imei order by imei""",
      s"""select sum(Latest_month)+10 as a ,imei  from TABLE_DICTIONARY_EXCLUDE1_hive group by imei order by imei""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_003")

  }


  //DICTIONARY_EXCLUDE_004
  test("Queries_DICTIONARY_EXCLUDE_004", Include) {

    checkAnswer(s"""select max(imei),min(imei) from TABLE_DICTIONARY_EXCLUDE""",
      s"""select max(imei),min(imei) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_004")

  }


  //DICTIONARY_EXCLUDE_005
  test("Queries_DICTIONARY_EXCLUDE_005", Include) {

    checkAnswer(s"""select min(imei), max(imei) Total from TABLE_DICTIONARY_EXCLUDE group by  channelsId order by Total""",
      s"""select min(imei), max(imei) Total from TABLE_DICTIONARY_EXCLUDE1_hive group by  channelsId order by Total""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_005")

  }


  //DICTIONARY_EXCLUDE_006
  test("Queries_DICTIONARY_EXCLUDE_006", Include) {

    sql(s"""select last(imei) a from TABLE_DICTIONARY_EXCLUDE  group by imei order by imei limit 1""").collect

  }


  //DICTIONARY_EXCLUDE_007
  test("Queries_DICTIONARY_EXCLUDE_007", Include) {

    sql(s"""select FIRST(imei) a from TABLE_DICTIONARY_EXCLUDE group by imei order by imei limit 1""").collect

  }


  //DICTIONARY_EXCLUDE_008
  test("Queries_DICTIONARY_EXCLUDE_008", Include) {

    checkAnswer(s"""select imei,count(imei) a from TABLE_DICTIONARY_EXCLUDE group by imei order by imei""",
      s"""select imei,count(imei) a from TABLE_DICTIONARY_EXCLUDE1_hive group by imei order by imei""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_008")

  }


  //DICTIONARY_EXCLUDE_009
  test("Queries_DICTIONARY_EXCLUDE_009", Include) {

    checkAnswer(s"""select Lower(imei) a  from TABLE_DICTIONARY_EXCLUDE order by imei""",
      s"""select Lower(imei) a  from TABLE_DICTIONARY_EXCLUDE1_hive order by imei""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_009")

  }


  //DICTIONARY_EXCLUDE_010
  test("Queries_DICTIONARY_EXCLUDE_010", Include) {

    checkAnswer(s"""select distinct imei from TABLE_DICTIONARY_EXCLUDE order by imei""",
      s"""select distinct imei from TABLE_DICTIONARY_EXCLUDE1_hive order by imei""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_010")

  }


  //DICTIONARY_EXCLUDE_011
  test("Queries_DICTIONARY_EXCLUDE_011", Include) {

    checkAnswer(s"""select imei from TABLE_DICTIONARY_EXCLUDE order by imei limit 101 """,
      s"""select imei from TABLE_DICTIONARY_EXCLUDE1_hive order by imei limit 101 """, "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_011")

  }


  //DICTIONARY_EXCLUDE_012
  test("Queries_DICTIONARY_EXCLUDE_012", Include) {

    sql(s"""select imei as a from TABLE_DICTIONARY_EXCLUDE  order by a asc limit 10""").collect

  }


  //DICTIONARY_EXCLUDE_013
  test("Queries_DICTIONARY_EXCLUDE_013", Include) {

    checkAnswer(s"""select imei from TABLE_DICTIONARY_EXCLUDE where  (contractNumber == 9223372047700) and (imei=='1AA100004')""",
      s"""select imei from TABLE_DICTIONARY_EXCLUDE1_hive where  (contractNumber == 9223372047700) and (imei=='1AA100004')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_013")

  }


  //DICTIONARY_EXCLUDE_014
  test("Queries_DICTIONARY_EXCLUDE_014", Include) {

    checkAnswer(s"""select imei from TABLE_DICTIONARY_EXCLUDE where imei !='1AA100064' order by imei""",
      s"""select imei from TABLE_DICTIONARY_EXCLUDE1_hive where imei !='1AA100064' order by imei""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_014")

  }


  //DICTIONARY_EXCLUDE_015
  test("Queries_DICTIONARY_EXCLUDE_015", Include) {

    checkAnswer(s"""select imei  from TABLE_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select imei  from TABLE_DICTIONARY_EXCLUDE1_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_015")

  }


  //DICTIONARY_EXCLUDE_016
  test("Queries_DICTIONARY_EXCLUDE_016", Include) {

    checkAnswer(s"""select imei from TABLE_DICTIONARY_EXCLUDE where imei !='1AA100012' order by imei""",
      s"""select imei from TABLE_DICTIONARY_EXCLUDE1_hive where imei !='1AA100012' order by imei""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_016")

  }


  //DICTIONARY_EXCLUDE_017
  test("Queries_DICTIONARY_EXCLUDE_017", Include) {

    checkAnswer(s"""select imei from TABLE_DICTIONARY_EXCLUDE where imei >'1AA100012' order by imei""",
      s"""select imei from TABLE_DICTIONARY_EXCLUDE1_hive where imei >'1AA100012' order by imei""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_017")

  }


  //DICTIONARY_EXCLUDE_018
  test("Queries_DICTIONARY_EXCLUDE_018", Include) {

    checkAnswer(s"""select imei  from TABLE_DICTIONARY_EXCLUDE where imei<>imei""",
      s"""select imei  from TABLE_DICTIONARY_EXCLUDE1_hive where imei<>imei""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_018")

  }


  //DICTIONARY_EXCLUDE_019
  test("Queries_DICTIONARY_EXCLUDE_019", Include) {

    checkAnswer(s"""select imei from TABLE_DICTIONARY_EXCLUDE where imei != Latest_areaId order by imei""",
      s"""select imei from TABLE_DICTIONARY_EXCLUDE1_hive where imei != Latest_areaId order by imei""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_019")

  }


  //DICTIONARY_EXCLUDE_020
  test("Queries_DICTIONARY_EXCLUDE_020", Include) {

    checkAnswer(s"""select imei from TABLE_DICTIONARY_EXCLUDE where Latest_areaId<imei order by imei""",
      s"""select imei from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_areaId<imei order by imei""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_020")

  }


  //DICTIONARY_EXCLUDE_021
  test("Queries_DICTIONARY_EXCLUDE_021", Include) {

    checkAnswer(s"""select imei from TABLE_DICTIONARY_EXCLUDE where Latest_DAY<=imei order by imei""",
      s"""select imei from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_DAY<=imei order by imei""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_021")

  }


  //DICTIONARY_EXCLUDE_022
  test("Queries_DICTIONARY_EXCLUDE_022", Include) {

    checkAnswer(s"""select imei from TABLE_DICTIONARY_EXCLUDE where imei <'1AA10002' order by imei""",
      s"""select imei from TABLE_DICTIONARY_EXCLUDE1_hive where imei <'1AA10002' order by imei""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_022")

  }


  //DICTIONARY_EXCLUDE_023
  test("Queries_DICTIONARY_EXCLUDE_023", Include) {

    checkAnswer(s"""select Latest_day  from TABLE_DICTIONARY_EXCLUDE where imei IS NULL""",
      s"""select Latest_day  from TABLE_DICTIONARY_EXCLUDE1_hive where imei IS NULL""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_023")

  }


  //DICTIONARY_EXCLUDE_024
  test("Queries_DICTIONARY_EXCLUDE_024", Include) {

    checkAnswer(s"""select Latest_day  from TABLE_DICTIONARY_EXCLUDE where imei IS NOT NULL order by Latest_day""",
      s"""select Latest_day  from TABLE_DICTIONARY_EXCLUDE1_hive where imei IS NOT NULL order by Latest_day""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_024")

  }


  //DICTIONARY_EXCLUDE_025
  test("Queries_DICTIONARY_EXCLUDE_025", Include) {

    checkAnswer(s"""Select count(imei),min(imei) from TABLE_DICTIONARY_EXCLUDE """,
      s"""Select count(imei),min(imei) from TABLE_DICTIONARY_EXCLUDE1_hive """, "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_025")

  }


  //DICTIONARY_EXCLUDE_026
  test("Queries_DICTIONARY_EXCLUDE_026", Include) {

    checkAnswer(s"""select count(DISTINCT imei,latest_day) as a from TABLE_DICTIONARY_EXCLUDE""",
      s"""select count(DISTINCT imei,latest_day) as a from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_026")

  }


  //DICTIONARY_EXCLUDE_027
  test("Queries_DICTIONARY_EXCLUDE_027", Include) {

    checkAnswer(s"""select max(imei),min(imei),count(imei) from TABLE_DICTIONARY_EXCLUDE""",
      s"""select max(imei),min(imei),count(imei) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_027")

  }


  //DICTIONARY_EXCLUDE_028
  test("Queries_DICTIONARY_EXCLUDE_028", Include) {

    checkAnswer(s"""select sum(imei),avg(imei),count(imei) a  from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(imei),avg(imei),count(imei) a  from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_028")

  }


  //DICTIONARY_EXCLUDE_029
  test("Queries_DICTIONARY_EXCLUDE_029", Include) {

    sql(s"""select last(imei),Min(imei),max(imei)  a from TABLE_DICTIONARY_EXCLUDE  order by a""").collect

  }


  //DICTIONARY_EXCLUDE_030
  test("Queries_DICTIONARY_EXCLUDE_030", Include) {

    sql(s"""select FIRST(imei),Last(imei) a from TABLE_DICTIONARY_EXCLUDE group by imei order by imei limit 1""").collect

  }


  //DICTIONARY_EXCLUDE_031
  test("Queries_DICTIONARY_EXCLUDE_031", Include) {

    checkAnswer(s"""select imei,count(imei) a from TABLE_DICTIONARY_EXCLUDE group by imei order by imei""",
      s"""select imei,count(imei) a from TABLE_DICTIONARY_EXCLUDE1_hive group by imei order by imei""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_031")

  }


  //DICTIONARY_EXCLUDE_032
  test("Queries_DICTIONARY_EXCLUDE_032", Include) {

    checkAnswer(s"""select Lower(imei),upper(imei)  a  from TABLE_DICTIONARY_EXCLUDE order by imei""",
      s"""select Lower(imei),upper(imei)  a  from TABLE_DICTIONARY_EXCLUDE1_hive order by imei""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_032")

  }


  //DICTIONARY_EXCLUDE_033
  test("Queries_DICTIONARY_EXCLUDE_033", Include) {

    checkAnswer(s"""select imei as a from TABLE_DICTIONARY_EXCLUDE  order by a asc limit 10""",
      s"""select imei as a from TABLE_DICTIONARY_EXCLUDE1_hive  order by a asc limit 10""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_033")

  }


  //DICTIONARY_EXCLUDE_034
  test("Queries_DICTIONARY_EXCLUDE_034", Include) {

    checkAnswer(s"""select imei from TABLE_DICTIONARY_EXCLUDE where  (contractNumber == 9223372047700) and (imei=='1AA100012')""",
      s"""select imei from TABLE_DICTIONARY_EXCLUDE1_hive where  (contractNumber == 9223372047700) and (imei=='1AA100012')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_034")

  }


  //DICTIONARY_EXCLUDE_035
  test("Queries_DICTIONARY_EXCLUDE_035", Include) {

    checkAnswer(s"""select imei from TABLE_DICTIONARY_EXCLUDE where imei !='8imei' order by imei""",
      s"""select imei from TABLE_DICTIONARY_EXCLUDE1_hive where imei !='8imei' order by imei""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_035")

  }


  //DICTIONARY_EXCLUDE_036
  test("Queries_DICTIONARY_EXCLUDE_036", Include) {

    checkAnswer(s"""select imei  from TABLE_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select imei  from TABLE_DICTIONARY_EXCLUDE1_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_036")

  }


  //DICTIONARY_EXCLUDE_037
  test("Queries_DICTIONARY_EXCLUDE_037", Include) {

    checkAnswer(s"""Select count(contractNumber) from TABLE_DICTIONARY_EXCLUDE""",
      s"""Select count(contractNumber) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_037")

  }


  //DICTIONARY_EXCLUDE_038
  test("Queries_DICTIONARY_EXCLUDE_038", Include) {

    checkAnswer(s"""select count(DISTINCT contractNumber) as a from TABLE_DICTIONARY_EXCLUDE""",
      s"""select count(DISTINCT contractNumber) as a from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_038")

  }


  //DICTIONARY_EXCLUDE_039
  test("Queries_DICTIONARY_EXCLUDE_039", Include) {

    checkAnswer(s"""select sum(contractNumber)+10 as a ,contractNumber  from TABLE_DICTIONARY_EXCLUDE group by contractNumber""",
      s"""select sum(contractNumber)+10 as a ,contractNumber  from TABLE_DICTIONARY_EXCLUDE1_hive group by contractNumber""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_039")

  }


  //DICTIONARY_EXCLUDE_040
  test("Queries_DICTIONARY_EXCLUDE_040", Include) {

    checkAnswer(s"""select max(contractNumber),min(contractNumber) from TABLE_DICTIONARY_EXCLUDE""",
      s"""select max(contractNumber),min(contractNumber) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_040")

  }


  //DICTIONARY_EXCLUDE_041
  test("Queries_DICTIONARY_EXCLUDE_041", Include) {

    checkAnswer(s"""select sum(contractNumber) a  from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(contractNumber) a  from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_041")

  }


  //DICTIONARY_EXCLUDE_042
  test("Queries_DICTIONARY_EXCLUDE_042", Include) {

    checkAnswer(s"""select avg(contractNumber) a  from TABLE_DICTIONARY_EXCLUDE""",
      s"""select avg(contractNumber) a  from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_042")

  }


  //DICTIONARY_EXCLUDE_043
  test("Queries_DICTIONARY_EXCLUDE_043", Include) {

    checkAnswer(s"""select min(contractNumber) a  from TABLE_DICTIONARY_EXCLUDE""",
      s"""select min(contractNumber) a  from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_043")

  }


  //DICTIONARY_EXCLUDE_044
  test("Queries_DICTIONARY_EXCLUDE_044", Include) {

    sql(s"""select variance(contractNumber) as a   from (select contractNumber from TABLE_DICTIONARY_EXCLUDE order by contractNumber) t""").collect

  }


  //DICTIONARY_EXCLUDE_045
  ignore("Queries_DICTIONARY_EXCLUDE_045", Include) {

    checkAnswer(s"""select var_pop(contractNumber) as a from (select * from TABLE_DICTIONARY_EXCLUDE order by contractNumber) t""",
      s"""select var_pop(contractNumber) as a from (select * from TABLE_DICTIONARY_EXCLUDE1_hive order by contractNumber) t""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_045")

  }


  //DICTIONARY_EXCLUDE_046
  ignore("Queries_DICTIONARY_EXCLUDE_046", Include) {

    checkAnswer(s"""select var_samp(contractNumber) as a from (select * from TABLE_DICTIONARY_EXCLUDE order by contractNumber) t""",
      s"""select var_samp(contractNumber) as a from (select * from TABLE_DICTIONARY_EXCLUDE1_hive order by contractNumber) t""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_046")

  }


  //DICTIONARY_EXCLUDE_047
  test("Queries_DICTIONARY_EXCLUDE_047", Include) {

    sql(s"""select stddev_pop(contractNumber) as a  from (select contractNumber from TABLE_DICTIONARY_EXCLUDE order by contractNumber) t""").collect

  }


  //DICTIONARY_EXCLUDE_048
  test("Queries_DICTIONARY_EXCLUDE_048", Include) {

    sql(s"""select stddev_samp(contractNumber)  as a from (select contractNumber from TABLE_DICTIONARY_EXCLUDE order by contractNumber) t""").collect

  }


  //DICTIONARY_EXCLUDE_049
  test("Queries_DICTIONARY_EXCLUDE_049", Include) {

    sql(s"""select covar_pop(contractNumber,contractNumber) as a  from (select contractNumber from TABLE_DICTIONARY_EXCLUDE order by contractNumber) t""").collect

  }


  //DICTIONARY_EXCLUDE_050
  test("Queries_DICTIONARY_EXCLUDE_050", Include) {

    sql(s"""select covar_samp(contractNumber,contractNumber) as a  from (select contractNumber from TABLE_DICTIONARY_EXCLUDE order by contractNumber) t""").collect

  }


  //DICTIONARY_EXCLUDE_051
  test("Queries_DICTIONARY_EXCLUDE_051", Include) {

    checkAnswer(s"""select corr(contractNumber,contractNumber)  as a from TABLE_DICTIONARY_EXCLUDE""",
      s"""select corr(contractNumber,contractNumber)  as a from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_051")

  }


  //DICTIONARY_EXCLUDE_052
  test("Queries_DICTIONARY_EXCLUDE_052", Include) {

    sql(s"""select percentile_approx(contractNumber,0.2) as a  from (select contractNumber from TABLE_DICTIONARY_EXCLUDE order by contractNumber) t""").collect

  }


  //DICTIONARY_EXCLUDE_053
  test("Queries_DICTIONARY_EXCLUDE_053", Include) {

    sql(s"""select percentile_approx(contractNumber,0.2,5) as a  from (select contractNumber from TABLE_DICTIONARY_EXCLUDE order by contractNumber) t""").collect

  }


  //DICTIONARY_EXCLUDE_054
  test("Queries_DICTIONARY_EXCLUDE_054", Include) {

    sql(s"""select percentile_approx(contractNumber,array(0.2,0.3,0.99))  as a from (select contractNumber from TABLE_DICTIONARY_EXCLUDE order by contractNumber) t""").collect

  }


  //DICTIONARY_EXCLUDE_055
  test("Queries_DICTIONARY_EXCLUDE_055", Include) {

    sql(s"""select percentile_approx(contractNumber,array(0.2,0.3,0.99),5) as a from (select contractNumber from TABLE_DICTIONARY_EXCLUDE order by contractNumber) t""").collect

  }


  //DICTIONARY_EXCLUDE_056
  test("Queries_DICTIONARY_EXCLUDE_056", Include) {

    sql(s"""select histogram_numeric(contractNumber,2)  as a from (select contractNumber from TABLE_DICTIONARY_EXCLUDE order by contractNumber) t""").collect

  }


  //DICTIONARY_EXCLUDE_057
  test("Queries_DICTIONARY_EXCLUDE_057", Include) {

    checkAnswer(s"""select contractNumber+ 10 as a  from TABLE_DICTIONARY_EXCLUDE order by a""",
      s"""select contractNumber+ 10 as a  from TABLE_DICTIONARY_EXCLUDE1_hive order by a""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_057")

  }


  //DICTIONARY_EXCLUDE_058
  test("Queries_DICTIONARY_EXCLUDE_058", Include) {

    checkAnswer(s"""select min(contractNumber), max(contractNumber+ 10) Total from TABLE_DICTIONARY_EXCLUDE group by  channelsId order by Total""",
      s"""select min(contractNumber), max(contractNumber+ 10) Total from TABLE_DICTIONARY_EXCLUDE1_hive group by  channelsId order by Total""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_058")

  }


  //DICTIONARY_EXCLUDE_059
  test("Queries_DICTIONARY_EXCLUDE_059", Include) {

    sql(s"""select last(contractNumber) a from TABLE_DICTIONARY_EXCLUDE  order by a""").collect

  }


  //DICTIONARY_EXCLUDE_060
  test("Queries_DICTIONARY_EXCLUDE_060", Include) {

    sql(s"""select FIRST(contractNumber) a from TABLE_DICTIONARY_EXCLUDE order by a""").collect

  }


  //DICTIONARY_EXCLUDE_061
  test("Queries_DICTIONARY_EXCLUDE_061", Include) {

    checkAnswer(s"""select contractNumber,count(contractNumber) a from TABLE_DICTIONARY_EXCLUDE group by contractNumber order by contractNumber""",
      s"""select contractNumber,count(contractNumber) a from TABLE_DICTIONARY_EXCLUDE1_hive group by contractNumber order by contractNumber""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_061")

  }


  //DICTIONARY_EXCLUDE_062
  test("Queries_DICTIONARY_EXCLUDE_062", Include) {

    checkAnswer(s"""select Lower(contractNumber) a  from TABLE_DICTIONARY_EXCLUDE order by contractNumber""",
      s"""select Lower(contractNumber) a  from TABLE_DICTIONARY_EXCLUDE1_hive order by contractNumber""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_062")

  }


  //DICTIONARY_EXCLUDE_063
  test("Queries_DICTIONARY_EXCLUDE_063", Include) {

    checkAnswer(s"""select distinct contractNumber from TABLE_DICTIONARY_EXCLUDE order by contractNumber""",
      s"""select distinct contractNumber from TABLE_DICTIONARY_EXCLUDE1_hive order by contractNumber""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_063")

  }


  //DICTIONARY_EXCLUDE_064
  test("Queries_DICTIONARY_EXCLUDE_064", Include) {

    checkAnswer(s"""select contractNumber from TABLE_DICTIONARY_EXCLUDE order by contractNumber limit 101""",
      s"""select contractNumber from TABLE_DICTIONARY_EXCLUDE1_hive order by contractNumber limit 101""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_064")

  }


  //DICTIONARY_EXCLUDE_065
  test("Queries_DICTIONARY_EXCLUDE_065", Include) {

    checkAnswer(s"""select contractNumber as a from TABLE_DICTIONARY_EXCLUDE  order by a asc limit 10""",
      s"""select contractNumber as a from TABLE_DICTIONARY_EXCLUDE1_hive  order by a asc limit 10""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_065")

  }


  //DICTIONARY_EXCLUDE_066
  test("Queries_DICTIONARY_EXCLUDE_066", Include) {

    checkAnswer(s"""select contractNumber from TABLE_DICTIONARY_EXCLUDE where  (contractNumber == 9223372047700) and (imei=='1AA100012')""",
      s"""select contractNumber from TABLE_DICTIONARY_EXCLUDE1_hive where  (contractNumber == 9223372047700) and (imei=='1AA100012')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_066")

  }


  //DICTIONARY_EXCLUDE_067
  test("Queries_DICTIONARY_EXCLUDE_067", Include) {

    checkAnswer(s"""select contractNumber from TABLE_DICTIONARY_EXCLUDE where contractNumber !=9223372047700 order by contractNumber""",
      s"""select contractNumber from TABLE_DICTIONARY_EXCLUDE1_hive where contractNumber !=9223372047700 order by contractNumber""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_067")

  }


  //DICTIONARY_EXCLUDE_068
  test("Queries_DICTIONARY_EXCLUDE_068", Include) {

    checkAnswer(s"""select contractNumber  from TABLE_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color') order by contractNumber""",
      s"""select contractNumber  from TABLE_DICTIONARY_EXCLUDE1_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color') order by contractNumber""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_068")

  }


  //DICTIONARY_EXCLUDE_069
  test("Queries_DICTIONARY_EXCLUDE_069", Include) {

    checkAnswer(s"""select contractNumber from TABLE_DICTIONARY_EXCLUDE where contractNumber !=9223372047700 order by contractNumber""",
      s"""select contractNumber from TABLE_DICTIONARY_EXCLUDE1_hive where contractNumber !=9223372047700 order by contractNumber""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_069")

  }


  //DICTIONARY_EXCLUDE_070
  test("Queries_DICTIONARY_EXCLUDE_070", Include) {

    checkAnswer(s"""select contractNumber from TABLE_DICTIONARY_EXCLUDE where contractNumber >9223372047700 order by contractNumber""",
      s"""select contractNumber from TABLE_DICTIONARY_EXCLUDE1_hive where contractNumber >9223372047700 order by contractNumber""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_070")

  }


  //DICTIONARY_EXCLUDE_071
  test("Queries_DICTIONARY_EXCLUDE_071", Include) {

    checkAnswer(s"""select contractNumber  from TABLE_DICTIONARY_EXCLUDE where contractNumber<>contractNumber""",
      s"""select contractNumber  from TABLE_DICTIONARY_EXCLUDE1_hive where contractNumber<>contractNumber""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_071")

  }


  //DICTIONARY_EXCLUDE_072
  test("Queries_DICTIONARY_EXCLUDE_072", Include) {

    checkAnswer(s"""select contractNumber from TABLE_DICTIONARY_EXCLUDE where contractNumber != Latest_areaId order by contractNumber""",
      s"""select contractNumber from TABLE_DICTIONARY_EXCLUDE1_hive where contractNumber != Latest_areaId order by contractNumber""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_072")

  }


  //DICTIONARY_EXCLUDE_073
  test("Queries_DICTIONARY_EXCLUDE_073", Include) {

    checkAnswer(s"""select contractNumber, contractNumber from TABLE_DICTIONARY_EXCLUDE where Latest_areaId<contractNumber order by contractNumber""",
      s"""select contractNumber, contractNumber from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_areaId<contractNumber order by contractNumber""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_073")

  }


  //DICTIONARY_EXCLUDE_074
  test("Queries_DICTIONARY_EXCLUDE_074", Include) {

    checkAnswer(s"""select contractNumber, contractNumber from TABLE_DICTIONARY_EXCLUDE where Latest_DAY<=contractNumber order by contractNumber""",
      s"""select contractNumber, contractNumber from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_DAY<=contractNumber order by contractNumber""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_074")

  }


  //DICTIONARY_EXCLUDE_075
  test("Queries_DICTIONARY_EXCLUDE_075", Include) {

    checkAnswer(s"""select contractNumber from TABLE_DICTIONARY_EXCLUDE where contractNumber <1000 order by contractNumber""",
      s"""select contractNumber from TABLE_DICTIONARY_EXCLUDE1_hive where contractNumber <1000 order by contractNumber""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_075")

  }


  //DICTIONARY_EXCLUDE_076
  test("Queries_DICTIONARY_EXCLUDE_076", Include) {

    checkAnswer(s"""select contractNumber from TABLE_DICTIONARY_EXCLUDE where contractNumber >1000 order by contractNumber""",
      s"""select contractNumber from TABLE_DICTIONARY_EXCLUDE1_hive where contractNumber >1000 order by contractNumber""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_076")

  }


  //DICTIONARY_EXCLUDE_077
  test("Queries_DICTIONARY_EXCLUDE_077", Include) {

    checkAnswer(s"""select contractNumber  from TABLE_DICTIONARY_EXCLUDE where contractNumber IS NULL order by contractNumber""",
      s"""select contractNumber  from TABLE_DICTIONARY_EXCLUDE1_hive where contractNumber IS NULL order by contractNumber""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_077")

  }


  //DICTIONARY_EXCLUDE_078
  test("Queries_DICTIONARY_EXCLUDE_078", Include) {

    checkAnswer(s"""select contractNumber  from TABLE_DICTIONARY_EXCLUDE where Latest_DAY IS NOT NULL order by contractNumber""",
      s"""select contractNumber  from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_DAY IS NOT NULL order by contractNumber""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_078")

  }


  //DICTIONARY_EXCLUDE_079
  test("Queries_DICTIONARY_EXCLUDE_079", Include) {

    checkAnswer(s"""Select count(Latest_DAY) from TABLE_DICTIONARY_EXCLUDE""",
      s"""Select count(Latest_DAY) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_079")

  }


  //DICTIONARY_EXCLUDE_080
  test("Queries_DICTIONARY_EXCLUDE_080", Include) {

    checkAnswer(s"""select count(DISTINCT Latest_DAY) as a from TABLE_DICTIONARY_EXCLUDE""",
      s"""select count(DISTINCT Latest_DAY) as a from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_080")

  }


  //DICTIONARY_EXCLUDE_081
  test("Queries_DICTIONARY_EXCLUDE_081", Include) {

    checkAnswer(s"""select sum(Latest_DAY)+10 as a ,Latest_DAY  from TABLE_DICTIONARY_EXCLUDE group by Latest_DAY order by a""",
      s"""select sum(Latest_DAY)+10 as a ,Latest_DAY  from TABLE_DICTIONARY_EXCLUDE1_hive group by Latest_DAY order by a""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_081")

  }


  //DICTIONARY_EXCLUDE_082
  test("Queries_DICTIONARY_EXCLUDE_082", Include) {

    checkAnswer(s"""select max(Latest_DAY),min(Latest_DAY) from TABLE_DICTIONARY_EXCLUDE""",
      s"""select max(Latest_DAY),min(Latest_DAY) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_082")

  }


  //DICTIONARY_EXCLUDE_083
  test("Queries_DICTIONARY_EXCLUDE_083", Include) {

    checkAnswer(s"""select sum(Latest_DAY) a  from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(Latest_DAY) a  from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_083")

  }


  //DICTIONARY_EXCLUDE_084
  test("Queries_DICTIONARY_EXCLUDE_084", Include) {

    checkAnswer(s"""select avg(Latest_DAY) a  from TABLE_DICTIONARY_EXCLUDE""",
      s"""select avg(Latest_DAY) a  from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_084")

  }


  //DICTIONARY_EXCLUDE_085
  test("Queries_DICTIONARY_EXCLUDE_085", Include) {

    checkAnswer(s"""select min(Latest_DAY) a  from TABLE_DICTIONARY_EXCLUDE""",
      s"""select min(Latest_DAY) a  from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_085")

  }


  //DICTIONARY_EXCLUDE_086
  test("Queries_DICTIONARY_EXCLUDE_086", Include) {

    sql(s"""select variance(Latest_DAY) as a   from (select Latest_DAY from TABLE_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //DICTIONARY_EXCLUDE_087
  test("Queries_DICTIONARY_EXCLUDE_087", Include) {

    sql(s"""select var_pop(Latest_DAY)  as a from (select Latest_DAY from TABLE_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //DICTIONARY_EXCLUDE_088
  test("Queries_DICTIONARY_EXCLUDE_088", Include) {

    sql(s"""select var_samp(Latest_DAY) as a  from (select Latest_DAY from TABLE_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //DICTIONARY_EXCLUDE_089
  test("Queries_DICTIONARY_EXCLUDE_089", Include) {

    sql(s"""select stddev_pop(Latest_DAY) as a  from (select Latest_DAY from TABLE_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //DICTIONARY_EXCLUDE_090
  test("Queries_DICTIONARY_EXCLUDE_090", Include) {

    sql(s"""select stddev_samp(Latest_DAY)  as a from (select Latest_DAY from TABLE_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //DICTIONARY_EXCLUDE_091
  test("Queries_DICTIONARY_EXCLUDE_091", Include) {

    sql(s"""select covar_pop(Latest_DAY,Latest_DAY) as a  from (select Latest_DAY from TABLE_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //DICTIONARY_EXCLUDE_092
  test("Queries_DICTIONARY_EXCLUDE_092", Include) {

    sql(s"""select covar_samp(Latest_DAY,Latest_DAY) as a  from (select Latest_DAY from TABLE_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //DICTIONARY_EXCLUDE_093
  test("Queries_DICTIONARY_EXCLUDE_093", Include) {

    checkAnswer(s"""select corr(Latest_DAY,Latest_DAY)  as a from TABLE_DICTIONARY_EXCLUDE""",
      s"""select corr(Latest_DAY,Latest_DAY)  as a from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_093")

  }


  //DICTIONARY_EXCLUDE_094
  test("Queries_DICTIONARY_EXCLUDE_094", Include) {

    sql(s"""select percentile_approx(Latest_DAY,0.2) as a  from (select Latest_DAY from TABLE_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //DICTIONARY_EXCLUDE_095
  test("Queries_DICTIONARY_EXCLUDE_095", Include) {

    sql(s"""select percentile_approx(Latest_DAY,0.2,5) as a  from (select Latest_DAY from TABLE_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //DICTIONARY_EXCLUDE_096
  test("Queries_DICTIONARY_EXCLUDE_096", Include) {

    sql(s"""select percentile_approx(Latest_DAY,array(0.2,0.3,0.99))  as a from (select Latest_DAY from TABLE_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //DICTIONARY_EXCLUDE_097
  test("Queries_DICTIONARY_EXCLUDE_097", Include) {

    sql(s"""select percentile_approx(Latest_DAY,array(0.2,0.3,0.99),5) as a from (select Latest_DAY from TABLE_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //DICTIONARY_EXCLUDE_098
  test("Queries_DICTIONARY_EXCLUDE_098", Include) {

    sql(s"""select histogram_numeric(Latest_DAY,2)  as a from (select Latest_DAY from TABLE_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //DICTIONARY_EXCLUDE_099
  test("Queries_DICTIONARY_EXCLUDE_099", Include) {

    checkAnswer(s"""select Latest_DAY, Latest_DAY+ 10 as a  from TABLE_DICTIONARY_EXCLUDE order by a""",
      s"""select Latest_DAY, Latest_DAY+ 10 as a  from TABLE_DICTIONARY_EXCLUDE1_hive order by a""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_099")

  }


  //DICTIONARY_EXCLUDE_100
  test("Queries_DICTIONARY_EXCLUDE_100", Include) {

    checkAnswer(s"""select min(Latest_DAY) d, max(Latest_DAY+ 10) Total from TABLE_DICTIONARY_EXCLUDE group by  channelsId order by d, Total""",
      s"""select min(Latest_DAY) d, max(Latest_DAY+ 10) Total from TABLE_DICTIONARY_EXCLUDE1_hive group by  channelsId order by d,Total""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_100")

  }


  //DICTIONARY_EXCLUDE_101
  test("Queries_DICTIONARY_EXCLUDE_101", Include) {

    sql(s"""select last(Latest_DAY) a from TABLE_DICTIONARY_EXCLUDE order by a""").collect

  }


  //DICTIONARY_EXCLUDE_102
  test("Queries_DICTIONARY_EXCLUDE_102", Include) {

    sql(s"""select FIRST(Latest_DAY) a from (select Latest_DAY from TABLE_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //DICTIONARY_EXCLUDE_103
  test("Queries_DICTIONARY_EXCLUDE_103", Include) {

    checkAnswer(s"""select Latest_DAY,count(Latest_DAY) a from TABLE_DICTIONARY_EXCLUDE group by Latest_DAY order by Latest_DAY""",
      s"""select Latest_DAY,count(Latest_DAY) a from TABLE_DICTIONARY_EXCLUDE1_hive group by Latest_DAY order by Latest_DAY""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_103")

  }


  //DICTIONARY_EXCLUDE_104
  test("Queries_DICTIONARY_EXCLUDE_104", Include) {

    checkAnswer(s"""select Lower(Latest_DAY) a  from TABLE_DICTIONARY_EXCLUDE order by a""",
      s"""select Lower(Latest_DAY) a  from TABLE_DICTIONARY_EXCLUDE1_hive order by a""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_104")

  }


  //DICTIONARY_EXCLUDE_105
  test("Queries_DICTIONARY_EXCLUDE_105", Include) {

    checkAnswer(s"""select distinct Latest_DAY from TABLE_DICTIONARY_EXCLUDE order by Latest_DAY""",
      s"""select distinct Latest_DAY from TABLE_DICTIONARY_EXCLUDE1_hive order by Latest_DAY""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_105")

  }


  //DICTIONARY_EXCLUDE_106
  test("Queries_DICTIONARY_EXCLUDE_106", Include) {

    checkAnswer(s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE order by Latest_DAY limit 101""",
      s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE1_hive order by Latest_DAY limit 101""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_106")

  }


  //DICTIONARY_EXCLUDE_107
  test("Queries_DICTIONARY_EXCLUDE_107", Include) {

    checkAnswer(s"""select Latest_DAY as a from TABLE_DICTIONARY_EXCLUDE  order by a asc limit 10""",
      s"""select Latest_DAY as a from TABLE_DICTIONARY_EXCLUDE1_hive  order by a asc limit 10""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_107")

  }


  //DICTIONARY_EXCLUDE_108
  test("Queries_DICTIONARY_EXCLUDE_108", Include) {

    checkAnswer(s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE where  (Latest_DAY == 1234567890123450.0000000000)  and (imei=='1AA1')""",
      s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE1_hive where  (Latest_DAY == 1234567890123450.0000000000)  and (imei=='1AA1')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_108")

  }


  //DICTIONARY_EXCLUDE_109
  test("Queries_DICTIONARY_EXCLUDE_109", Include) {

    checkAnswer(s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""",
      s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_109")

  }


  //DICTIONARY_EXCLUDE_110
  test("Queries_DICTIONARY_EXCLUDE_110", Include) {

    checkAnswer(s"""select Latest_DAY  from TABLE_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select Latest_DAY  from TABLE_DICTIONARY_EXCLUDE1_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_110")

  }


  //DICTIONARY_EXCLUDE_111
  test("Queries_DICTIONARY_EXCLUDE_111", Include) {

    checkAnswer(s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""",
      s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_111")

  }


  //DICTIONARY_EXCLUDE_112
  test("Queries_DICTIONARY_EXCLUDE_112", Include) {

    checkAnswer(s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE where Latest_DAY >1234567890123450.0000000000  order by Latest_DAY""",
      s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_DAY >1234567890123450.0000000000  order by Latest_DAY""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_112")

  }


  //DICTIONARY_EXCLUDE_113
  test("Queries_DICTIONARY_EXCLUDE_113", Include) {

    checkAnswer(s"""select Latest_DAY  from TABLE_DICTIONARY_EXCLUDE where Latest_DAY<>Latest_DAY""",
      s"""select Latest_DAY  from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_DAY<>Latest_DAY""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_113")

  }


  //DICTIONARY_EXCLUDE_114
  test("Queries_DICTIONARY_EXCLUDE_114", Include) {

    checkAnswer(s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE where Latest_DAY != Latest_areaId order by Latest_DAY""",
      s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_DAY != Latest_areaId order by Latest_DAY""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_114")

  }


  //DICTIONARY_EXCLUDE_115
  test("Queries_DICTIONARY_EXCLUDE_115", Include) {

    checkAnswer(s"""select Latest_DAY, Latest_DAY from TABLE_DICTIONARY_EXCLUDE where Latest_areaId<Latest_DAY order by Latest_DAY""",
      s"""select Latest_DAY, Latest_DAY from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_areaId<Latest_DAY order by Latest_DAY""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_115")

  }


  //DICTIONARY_EXCLUDE_116
  test("Queries_DICTIONARY_EXCLUDE_116", Include) {

    checkAnswer(s"""select Latest_DAY, Latest_DAY from TABLE_DICTIONARY_EXCLUDE where Latest_DAY<=Latest_DAY  order by Latest_DAY""",
      s"""select Latest_DAY, Latest_DAY from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_DAY<=Latest_DAY  order by Latest_DAY""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_116")

  }


  //DICTIONARY_EXCLUDE_117
  test("Queries_DICTIONARY_EXCLUDE_117", Include) {

    checkAnswer(s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE where Latest_DAY <1000  order by Latest_DAY""",
      s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_DAY <1000  order by Latest_DAY""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_117")

  }


  //DICTIONARY_EXCLUDE_118
  test("Queries_DICTIONARY_EXCLUDE_118", Include) {

    checkAnswer(s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE where Latest_DAY >1000  order by Latest_DAY""",
      s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_DAY >1000  order by Latest_DAY""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_118")

  }


  //DICTIONARY_EXCLUDE_119
  test("Queries_DICTIONARY_EXCLUDE_119", Include) {

    checkAnswer(s"""select Latest_DAY  from TABLE_DICTIONARY_EXCLUDE where Latest_DAY IS NULL  order by Latest_DAY""",
      s"""select Latest_DAY  from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_DAY IS NULL  order by Latest_DAY""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_119")

  }


  //DICTIONARY_EXCLUDE_120
  test("Queries_DICTIONARY_EXCLUDE_120", Include) {

    checkAnswer(s"""select Latest_DAY  from TABLE_DICTIONARY_EXCLUDE where Latest_DAY IS NOT NULL  order by Latest_DAY""",
      s"""select Latest_DAY  from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_DAY IS NOT NULL  order by Latest_DAY""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_120")

  }


  //DICTIONARY_EXCLUDE_121
  test("Queries_DICTIONARY_EXCLUDE_121", Include) {

    checkAnswer(s"""Select count(gamePointId) from TABLE_DICTIONARY_EXCLUDE""",
      s"""Select count(gamePointId) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_121")

  }


  //DICTIONARY_EXCLUDE_122
  test("Queries_DICTIONARY_EXCLUDE_122", Include) {

    checkAnswer(s"""select count(DISTINCT gamePointId) as a from TABLE_DICTIONARY_EXCLUDE""",
      s"""select count(DISTINCT gamePointId) as a from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_122")

  }


  //DICTIONARY_EXCLUDE_123
  test("Queries_DICTIONARY_EXCLUDE_123", Include) {

    checkAnswer(s"""select sum(gamePointId)+10 as a ,gamePointId  from TABLE_DICTIONARY_EXCLUDE group by gamePointId order by a""",
      s"""select sum(gamePointId)+10 as a ,gamePointId  from TABLE_DICTIONARY_EXCLUDE1_hive group by gamePointId order by a""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_123")

  }


  //DICTIONARY_EXCLUDE_124
  test("Queries_DICTIONARY_EXCLUDE_124", Include) {

    checkAnswer(s"""select max(gamePointId),min(gamePointId) from TABLE_DICTIONARY_EXCLUDE""",
      s"""select max(gamePointId),min(gamePointId) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_124")

  }


  //DICTIONARY_EXCLUDE_125
  ignore("Queries_DICTIONARY_EXCLUDE_125", Include) {

    checkAnswer(s"""select sum(gamePointId) a from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId)""",
      s"""select sum(gamePointId) a from (select gamePointId from TABLE_DICTIONARY_EXCLUDE1_hive order by gamePointId)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_125")

  }


  //DICTIONARY_EXCLUDE_126
  ignore("Queries_DICTIONARY_EXCLUDE_126", Include) {

    checkAnswer(s"""select avg(gamePointId) a from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId)""",
      s"""select avg(gamePointId) a from (select gamePointId from TABLE_DICTIONARY_EXCLUDE1_hive order by gamePointId)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_126")

  }


  //DICTIONARY_EXCLUDE_127
  test("Queries_DICTIONARY_EXCLUDE_127", Include) {

    checkAnswer(s"""select min(gamePointId) a  from TABLE_DICTIONARY_EXCLUDE""",
      s"""select min(gamePointId) a  from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_127")

  }


  //DICTIONARY_EXCLUDE_128
  test("Queries_DICTIONARY_EXCLUDE_128", Include) {

    sql(s"""select variance(gamePointId) as a   from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //DICTIONARY_EXCLUDE_129
  test("Queries_DICTIONARY_EXCLUDE_129", Include) {

    sql(s"""select var_pop(gamePointId)  as a from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //DICTIONARY_EXCLUDE_130
  test("Queries_DICTIONARY_EXCLUDE_130", Include) {

    sql(s"""select var_samp(gamePointId) as a  from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //DICTIONARY_EXCLUDE_131
  test("Queries_DICTIONARY_EXCLUDE_131", Include) {

    sql(s"""select stddev_pop(gamePointId) as a  from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //DICTIONARY_EXCLUDE_132
  test("Queries_DICTIONARY_EXCLUDE_132", Include) {

    sql(s"""select stddev_samp(gamePointId)  as a from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //DICTIONARY_EXCLUDE_133
  test("Queries_DICTIONARY_EXCLUDE_133", Include) {

    sql(s"""select covar_pop(gamePointId,gamePointId) as a  from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //DICTIONARY_EXCLUDE_134
  test("Queries_DICTIONARY_EXCLUDE_134", Include) {

    sql(s"""select covar_samp(gamePointId,gamePointId) as a  from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //DICTIONARY_EXCLUDE_135
  test("Queries_DICTIONARY_EXCLUDE_135", Include) {

    checkAnswer(s"""select corr(gamePointId,gamePointId)  as a from TABLE_DICTIONARY_EXCLUDE""",
      s"""select corr(gamePointId,gamePointId)  as a from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_135")

  }


  //DICTIONARY_EXCLUDE_136
  test("Queries_DICTIONARY_EXCLUDE_136", Include) {

    sql(s"""select percentile_approx(gamePointId,0.2) as a  from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //DICTIONARY_EXCLUDE_137
  test("Queries_DICTIONARY_EXCLUDE_137", Include) {

    sql(s"""select percentile_approx(gamePointId,0.2,5) as a  from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //DICTIONARY_EXCLUDE_138
  test("Queries_DICTIONARY_EXCLUDE_138", Include) {

    sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99))  as a from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //DICTIONARY_EXCLUDE_139
  test("Queries_DICTIONARY_EXCLUDE_139", Include) {

    sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99),5) as a from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //DICTIONARY_EXCLUDE_140
  test("Queries_DICTIONARY_EXCLUDE_140", Include) {

    sql(s"""select histogram_numeric(gamePointId,2)  as a from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //DICTIONARY_EXCLUDE_141
  test("Queries_DICTIONARY_EXCLUDE_141", Include) {

    checkAnswer(s"""select gamePointId, gamePointId+ 10 as a  from TABLE_DICTIONARY_EXCLUDE order by a""",
      s"""select gamePointId, gamePointId+ 10 as a  from TABLE_DICTIONARY_EXCLUDE1_hive order by a""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_141")

  }


  //DICTIONARY_EXCLUDE_142
  test("Queries_DICTIONARY_EXCLUDE_142", Include) {

    checkAnswer(s"""select min(gamePointId), max(gamePointId+ 10) Total from TABLE_DICTIONARY_EXCLUDE group by  channelsId order by Total""",
      s"""select min(gamePointId), max(gamePointId+ 10) Total from TABLE_DICTIONARY_EXCLUDE1_hive group by  channelsId order by Total""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_142")

  }


  //DICTIONARY_EXCLUDE_143
  test("Queries_DICTIONARY_EXCLUDE_143", Include) {

    sql(s"""select last(gamePointId) a from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //DICTIONARY_EXCLUDE_144
  ignore("Queries_DICTIONARY_EXCLUDE_144", Include) {

    checkAnswer(s"""select FIRST(gamePointId) a from TABLE_DICTIONARY_EXCLUDE order by a""",
      s"""select FIRST(gamePointId) a from TABLE_DICTIONARY_EXCLUDE1_hive order by a""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_144")

  }


  //DICTIONARY_EXCLUDE_145
  test("Queries_DICTIONARY_EXCLUDE_145", Include) {

    checkAnswer(s"""select gamePointId,count(gamePointId) a from TABLE_DICTIONARY_EXCLUDE group by gamePointId order by gamePointId""",
      s"""select gamePointId,count(gamePointId) a from TABLE_DICTIONARY_EXCLUDE1_hive group by gamePointId order by gamePointId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_145")

  }


  //DICTIONARY_EXCLUDE_146
  test("Queries_DICTIONARY_EXCLUDE_146", Include) {

    checkAnswer(s"""select Lower(gamePointId) a  from TABLE_DICTIONARY_EXCLUDE order by gamePointId""",
      s"""select Lower(gamePointId) a  from TABLE_DICTIONARY_EXCLUDE1_hive order by gamePointId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_146")

  }


  //DICTIONARY_EXCLUDE_147
  test("Queries_DICTIONARY_EXCLUDE_147", Include) {

    checkAnswer(s"""select distinct gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId""",
      s"""select distinct gamePointId from TABLE_DICTIONARY_EXCLUDE1_hive order by gamePointId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_147")

  }


  //DICTIONARY_EXCLUDE_148
  test("Queries_DICTIONARY_EXCLUDE_148", Include) {

    checkAnswer(s"""select gamePointId from TABLE_DICTIONARY_EXCLUDE  order by gamePointId limit 101""",
      s"""select gamePointId from TABLE_DICTIONARY_EXCLUDE1_hive  order by gamePointId limit 101""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_148")

  }


  //DICTIONARY_EXCLUDE_149
  test("Queries_DICTIONARY_EXCLUDE_149", Include) {

    checkAnswer(s"""select gamePointId as a from TABLE_DICTIONARY_EXCLUDE  order by a asc limit 10""",
      s"""select gamePointId as a from TABLE_DICTIONARY_EXCLUDE1_hive  order by a asc limit 10""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_149")

  }


  //DICTIONARY_EXCLUDE_150
  test("Queries_DICTIONARY_EXCLUDE_150", Include) {

    checkAnswer(s"""select gamePointId from TABLE_DICTIONARY_EXCLUDE where  (gamePointId == 4.70133553923674E43) and (imei=='1AA100084')""",
      s"""select gamePointId from TABLE_DICTIONARY_EXCLUDE1_hive where  (gamePointId == 4.70133553923674E43) and (imei=='1AA100084')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_150")

  }


  //DICTIONARY_EXCLUDE_151
  test("Queries_DICTIONARY_EXCLUDE_151", Include) {

    checkAnswer(s"""select gamePointId from TABLE_DICTIONARY_EXCLUDE where gamePointId !=4.70133553923674E43  order by gamePointId""",
      s"""select gamePointId from TABLE_DICTIONARY_EXCLUDE1_hive where gamePointId !=4.70133553923674E43  order by gamePointId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_151")

  }


  //DICTIONARY_EXCLUDE_152
  test("Queries_DICTIONARY_EXCLUDE_152", Include) {

    checkAnswer(s"""select gamePointId  from TABLE_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select gamePointId  from TABLE_DICTIONARY_EXCLUDE1_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_152")

  }


  //DICTIONARY_EXCLUDE_153
  test("Queries_DICTIONARY_EXCLUDE_153", Include) {

    checkAnswer(s"""select gamePointId from TABLE_DICTIONARY_EXCLUDE where gamePointId !=4.70133553923674E43""",
      s"""select gamePointId from TABLE_DICTIONARY_EXCLUDE1_hive where gamePointId !=4.70133553923674E43""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_153")

  }


  //DICTIONARY_EXCLUDE_154
  test("Queries_DICTIONARY_EXCLUDE_154", Include) {

    checkAnswer(s"""select gamePointId from TABLE_DICTIONARY_EXCLUDE where gamePointId >4.70133553923674E43""",
      s"""select gamePointId from TABLE_DICTIONARY_EXCLUDE1_hive where gamePointId >4.70133553923674E43""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_154")

  }


  //DICTIONARY_EXCLUDE_155
  test("Queries_DICTIONARY_EXCLUDE_155", Include) {

    checkAnswer(s"""select gamePointId  from TABLE_DICTIONARY_EXCLUDE where gamePointId<>gamePointId""",
      s"""select gamePointId  from TABLE_DICTIONARY_EXCLUDE1_hive where gamePointId<>gamePointId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_155")

  }


  //DICTIONARY_EXCLUDE_156
  test("Queries_DICTIONARY_EXCLUDE_156", Include) {

    checkAnswer(s"""select gamePointId from TABLE_DICTIONARY_EXCLUDE where gamePointId != Latest_areaId  order by gamePointId""",
      s"""select gamePointId from TABLE_DICTIONARY_EXCLUDE1_hive where gamePointId != Latest_areaId  order by gamePointId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_156")

  }


  //DICTIONARY_EXCLUDE_157
  test("Queries_DICTIONARY_EXCLUDE_157", Include) {

    checkAnswer(s"""select gamePointId, gamePointId from TABLE_DICTIONARY_EXCLUDE where Latest_areaId<gamePointId  order by gamePointId""",
      s"""select gamePointId, gamePointId from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_areaId<gamePointId  order by gamePointId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_157")

  }


  //DICTIONARY_EXCLUDE_158
  test("Queries_DICTIONARY_EXCLUDE_158", Include) {

    checkAnswer(s"""select gamePointId, gamePointId from TABLE_DICTIONARY_EXCLUDE where gamePointId<=gamePointId  order by gamePointId""",
      s"""select gamePointId, gamePointId from TABLE_DICTIONARY_EXCLUDE1_hive where gamePointId<=gamePointId  order by gamePointId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_158")

  }


  //DICTIONARY_EXCLUDE_159
  test("Queries_DICTIONARY_EXCLUDE_159", Include) {

    checkAnswer(s"""select gamePointId from TABLE_DICTIONARY_EXCLUDE where gamePointId <1000 order by gamePointId""",
      s"""select gamePointId from TABLE_DICTIONARY_EXCLUDE1_hive where gamePointId <1000 order by gamePointId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_159")

  }


  //DICTIONARY_EXCLUDE_160
  test("Queries_DICTIONARY_EXCLUDE_160", Include) {

    checkAnswer(s"""select gamePointId from TABLE_DICTIONARY_EXCLUDE where gamePointId >1000 order by gamePointId""",
      s"""select gamePointId from TABLE_DICTIONARY_EXCLUDE1_hive where gamePointId >1000 order by gamePointId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_160")

  }


  //DICTIONARY_EXCLUDE_161
  test("Queries_DICTIONARY_EXCLUDE_161", Include) {

    checkAnswer(s"""select gamePointId  from TABLE_DICTIONARY_EXCLUDE where gamePointId IS NULL order by gamePointId""",
      s"""select gamePointId  from TABLE_DICTIONARY_EXCLUDE1_hive where gamePointId IS NULL order by gamePointId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_161")

  }


  //DICTIONARY_EXCLUDE_162
  test("Queries_DICTIONARY_EXCLUDE_162", Include) {

    checkAnswer(s"""select gamePointId  from TABLE_DICTIONARY_EXCLUDE where gamePointId IS NOT NULL order by gamePointId""",
      s"""select gamePointId  from TABLE_DICTIONARY_EXCLUDE1_hive where gamePointId IS NOT NULL order by gamePointId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_162")

  }


  //DICTIONARY_EXCLUDE_163
  test("Queries_DICTIONARY_EXCLUDE_163", Include) {

    checkAnswer(s"""Select count(productionDate) from TABLE_DICTIONARY_EXCLUDE""",
      s"""Select count(productionDate) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_163")

  }


  //DICTIONARY_EXCLUDE_164
  test("Queries_DICTIONARY_EXCLUDE_164", Include) {

    checkAnswer(s"""select count(DISTINCT productionDate) as a from TABLE_DICTIONARY_EXCLUDE""",
      s"""select count(DISTINCT productionDate) as a from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_164")

  }


  //DICTIONARY_EXCLUDE_165
  test("Queries_DICTIONARY_EXCLUDE_165", Include) {

    checkAnswer(s"""select sum(productionDate)+10 as a ,productionDate  from TABLE_DICTIONARY_EXCLUDE group by productionDate order by productionDate""",
      s"""select sum(productionDate)+10 as a ,productionDate  from TABLE_DICTIONARY_EXCLUDE1_hive group by productionDate order by productionDate""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_165")

  }


  //DICTIONARY_EXCLUDE_166
  test("Queries_DICTIONARY_EXCLUDE_166", Include) {

    checkAnswer(s"""select max(productionDate),min(productionDate) from TABLE_DICTIONARY_EXCLUDE""",
      s"""select max(productionDate),min(productionDate) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_166")

  }


  //DICTIONARY_EXCLUDE_167
  test("Queries_DICTIONARY_EXCLUDE_167", Include) {

    checkAnswer(s"""select sum(productionDate) a  from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(productionDate) a  from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_167")

  }


  //DICTIONARY_EXCLUDE_168
  test("Queries_DICTIONARY_EXCLUDE_168", Include) {

    checkAnswer(s"""select avg(productionDate) a  from TABLE_DICTIONARY_EXCLUDE""",
      s"""select avg(productionDate) a  from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_168")

  }


  //DICTIONARY_EXCLUDE_169
  test("Queries_DICTIONARY_EXCLUDE_169", Include) {

    checkAnswer(s"""select min(productionDate) a  from TABLE_DICTIONARY_EXCLUDE""",
      s"""select min(productionDate) a  from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_169")

  }


  //DICTIONARY_EXCLUDE_170
  test("Queries_DICTIONARY_EXCLUDE_170", Include) {

    sql(s"""select variance(gamePointId) as a   from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //DICTIONARY_EXCLUDE_171
  ignore("Queries_DICTIONARY_EXCLUDE_171", Include) {

    checkAnswer(s"""select var_pop(gamePointId) as a from (select * from TABLE_DICTIONARY_EXCLUDE order by gamePointId)""",
      s"""select var_pop(gamePointId) as a from (select * from TABLE_DICTIONARY_EXCLUDE1_hive order by gamePointId)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_171")

  }


  //DICTIONARY_EXCLUDE_172
  ignore("Queries_DICTIONARY_EXCLUDE_172", Include) {

    checkAnswer(s"""select var_samp(gamePointId) as a from (select * from TABLE_DICTIONARY_EXCLUDE order by gamePointId)""",
      s"""select var_samp(gamePointId) as a from (select * from TABLE_DICTIONARY_EXCLUDE1_hive order by gamePointId)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_172")

  }


  //DICTIONARY_EXCLUDE_173
  test("Queries_DICTIONARY_EXCLUDE_173", Include) {

    sql(s"""select stddev_pop(gamePointId) as a  from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //DICTIONARY_EXCLUDE_174
  test("Queries_DICTIONARY_EXCLUDE_174", Include) {

    sql(s"""select stddev_samp(gamePointId)  as a from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //DICTIONARY_EXCLUDE_175
  test("Queries_DICTIONARY_EXCLUDE_175", Include) {

    sql(s"""select covar_pop(gamePointId,gamePointId) as a  from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //DICTIONARY_EXCLUDE_176
  test("Queries_DICTIONARY_EXCLUDE_176", Include) {

    sql(s"""select covar_samp(gamePointId,gamePointId) as a  from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //DICTIONARY_EXCLUDE_177
  test("Queries_DICTIONARY_EXCLUDE_177", Include) {

    checkAnswer(s"""select corr(gamePointId,gamePointId)  as a from TABLE_DICTIONARY_EXCLUDE""",
      s"""select corr(gamePointId,gamePointId)  as a from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_177")

  }


  //DICTIONARY_EXCLUDE_178
  test("Queries_DICTIONARY_EXCLUDE_178", Include) {

    sql(s"""select percentile_approx(gamePointId,0.2) as a  from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //DICTIONARY_EXCLUDE_179
  test("Queries_DICTIONARY_EXCLUDE_179", Include) {

    sql(s"""select percentile_approx(gamePointId,0.2,5) as a  from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //DICTIONARY_EXCLUDE_180
  test("Queries_DICTIONARY_EXCLUDE_180", Include) {

    sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99))  as a from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //DICTIONARY_EXCLUDE_181
  test("Queries_DICTIONARY_EXCLUDE_181", Include) {

    sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99),5) as a from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //DICTIONARY_EXCLUDE_182
  test("Queries_DICTIONARY_EXCLUDE_182", Include) {

    sql(s"""select histogram_numeric(productionDate,2)  as a from (select productionDate from TABLE_DICTIONARY_EXCLUDE order by productionDate) t""").collect

  }


  //DICTIONARY_EXCLUDE_183
  test("Queries_DICTIONARY_EXCLUDE_183", Include) {

    sql(s"""select last(productionDate) a from TABLE_DICTIONARY_EXCLUDE order by a""").collect

  }


  //DICTIONARY_EXCLUDE_184
  test("Queries_DICTIONARY_EXCLUDE_184", Include) {

    sql(s"""select FIRST(productionDate) a from (select productionDate from TABLE_DICTIONARY_EXCLUDE order by productionDate) t""").collect

  }


  //DICTIONARY_EXCLUDE_185
  test("Queries_DICTIONARY_EXCLUDE_185", Include) {

    checkAnswer(s"""select productionDate,count(productionDate) a from TABLE_DICTIONARY_EXCLUDE group by productionDate order by productionDate""",
      s"""select productionDate,count(productionDate) a from TABLE_DICTIONARY_EXCLUDE1_hive group by productionDate order by productionDate""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_185")

  }


  //DICTIONARY_EXCLUDE_186
  test("Queries_DICTIONARY_EXCLUDE_186", Include) {

    checkAnswer(s"""select Lower(productionDate) a  from TABLE_DICTIONARY_EXCLUDE order by productionDate""",
      s"""select Lower(productionDate) a  from TABLE_DICTIONARY_EXCLUDE1_hive order by productionDate""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_186")

  }


  //DICTIONARY_EXCLUDE_187
  test("Queries_DICTIONARY_EXCLUDE_187", Include) {

    checkAnswer(s"""select distinct productionDate from TABLE_DICTIONARY_EXCLUDE order by productionDate""",
      s"""select distinct productionDate from TABLE_DICTIONARY_EXCLUDE1_hive order by productionDate""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_187")

  }


  //DICTIONARY_EXCLUDE_188
  test("Queries_DICTIONARY_EXCLUDE_188", Include) {

    checkAnswer(s"""select productionDate from TABLE_DICTIONARY_EXCLUDE order by productionDate limit 101""",
      s"""select productionDate from TABLE_DICTIONARY_EXCLUDE1_hive order by productionDate limit 101""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_188")

  }


  //DICTIONARY_EXCLUDE_189
  test("Queries_DICTIONARY_EXCLUDE_189", Include) {

    checkAnswer(s"""select productionDate as a from TABLE_DICTIONARY_EXCLUDE  order by a asc limit 10""",
      s"""select productionDate as a from TABLE_DICTIONARY_EXCLUDE1_hive  order by a asc limit 10""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_189")

  }


  //DICTIONARY_EXCLUDE_190
  test("Queries_DICTIONARY_EXCLUDE_190", Include) {

    checkAnswer(s"""select productionDate from TABLE_DICTIONARY_EXCLUDE where  (productionDate == '2015-07-01 12:07:28.0') and (productionDate=='2015-07-01 12:07:28.0')""",
      s"""select productionDate from TABLE_DICTIONARY_EXCLUDE1_hive where  (productionDate == '2015-07-01 12:07:28.0') and (productionDate=='2015-07-01 12:07:28.0')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_190")

  }


  //DICTIONARY_EXCLUDE_191
  test("Queries_DICTIONARY_EXCLUDE_191", Include) {

    checkAnswer(s"""select productionDate from TABLE_DICTIONARY_EXCLUDE where productionDate !='2015-07-01 12:07:28.0' order by productionDate""",
      s"""select productionDate from TABLE_DICTIONARY_EXCLUDE1_hive where productionDate !='2015-07-01 12:07:28.0' order by productionDate""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_191")

  }


  //DICTIONARY_EXCLUDE_192
  test("Queries_DICTIONARY_EXCLUDE_192", Include) {

    checkAnswer(s"""select productionDate  from TABLE_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select productionDate  from TABLE_DICTIONARY_EXCLUDE1_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_192")

  }


  //DICTIONARY_EXCLUDE_193
  test("Queries_DICTIONARY_EXCLUDE_193", Include) {

    checkAnswer(s"""select productionDate from TABLE_DICTIONARY_EXCLUDE where productionDate !='2015-07-01 12:07:28.0' order by productionDate""",
      s"""select productionDate from TABLE_DICTIONARY_EXCLUDE1_hive where productionDate !='2015-07-01 12:07:28.0' order by productionDate""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_193")

  }


  //DICTIONARY_EXCLUDE_194
  test("Queries_DICTIONARY_EXCLUDE_194", Include) {

    checkAnswer(s"""select productionDate from TABLE_DICTIONARY_EXCLUDE where productionDate >'2015-07-01 12:07:28.0' order by productionDate""",
      s"""select productionDate from TABLE_DICTIONARY_EXCLUDE1_hive where productionDate >'2015-07-01 12:07:28.0' order by productionDate""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_194")

  }


  //DICTIONARY_EXCLUDE_195
  test("Queries_DICTIONARY_EXCLUDE_195", Include) {

    checkAnswer(s"""select productionDate  from TABLE_DICTIONARY_EXCLUDE where productionDate<>productionDate order by productionDate""",
      s"""select productionDate  from TABLE_DICTIONARY_EXCLUDE1_hive where productionDate<>productionDate order by productionDate""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_195")

  }


  //DICTIONARY_EXCLUDE_196
  test("Queries_DICTIONARY_EXCLUDE_196", Include) {

    checkAnswer(s"""select productionDate from TABLE_DICTIONARY_EXCLUDE where productionDate != Latest_areaId order by productionDate""",
      s"""select productionDate from TABLE_DICTIONARY_EXCLUDE1_hive where productionDate != Latest_areaId order by productionDate""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_196")

  }


  //DICTIONARY_EXCLUDE_197
  test("Queries_DICTIONARY_EXCLUDE_197", Include) {

    checkAnswer(s"""select productionDate from TABLE_DICTIONARY_EXCLUDE where Latest_areaId<productionDate order by productionDate""",
      s"""select productionDate from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_areaId<productionDate order by productionDate""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_197")

  }


  //DICTIONARY_EXCLUDE_198
  test("Queries_DICTIONARY_EXCLUDE_198", Include) {

    checkAnswer(s"""select productionDate from TABLE_DICTIONARY_EXCLUDE where productionDate<=productionDate order by productionDate""",
      s"""select productionDate from TABLE_DICTIONARY_EXCLUDE1_hive where productionDate<=productionDate order by productionDate""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_198")

  }


  //DICTIONARY_EXCLUDE_199
  test("Queries_DICTIONARY_EXCLUDE_199", Include) {

    checkAnswer(s"""select productionDate from TABLE_DICTIONARY_EXCLUDE where productionDate <cast('2015-07-01 12:07:28.0' as timestamp) order by productionDate""",
      s"""select productionDate from TABLE_DICTIONARY_EXCLUDE1_hive where productionDate <cast('2015-07-01 12:07:28.0' as timestamp) order by productionDate""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_199")

  }


  //DICTIONARY_EXCLUDE_200
  test("Queries_DICTIONARY_EXCLUDE_200", Include) {

    checkAnswer(s"""select productionDate  from TABLE_DICTIONARY_EXCLUDE where productionDate IS NULL""",
      s"""select productionDate  from TABLE_DICTIONARY_EXCLUDE1_hive where productionDate IS NULL""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_200")

  }


  //DICTIONARY_EXCLUDE_201
  test("Queries_DICTIONARY_EXCLUDE_201", Include) {

    checkAnswer(s"""select productionDate  from TABLE_DICTIONARY_EXCLUDE where productionDate IS NOT NULL order by productionDate""",
      s"""select productionDate  from TABLE_DICTIONARY_EXCLUDE1_hive where productionDate IS NOT NULL order by productionDate""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_201")

  }


  //DICTIONARY_EXCLUDE_202
  test("Queries_DICTIONARY_EXCLUDE_202", Include) {

    checkAnswer(s"""Select count(deviceInformationId) from TABLE_DICTIONARY_EXCLUDE""",
      s"""Select count(deviceInformationId) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_202")

  }


  //DICTIONARY_EXCLUDE_203
  test("Queries_DICTIONARY_EXCLUDE_203", Include) {

    checkAnswer(s"""select count(DISTINCT deviceInformationId) as a from TABLE_DICTIONARY_EXCLUDE""",
      s"""select count(DISTINCT deviceInformationId) as a from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_203")

  }


  //DICTIONARY_EXCLUDE_204
  test("Queries_DICTIONARY_EXCLUDE_204", Include) {

    checkAnswer(s"""select sum(deviceInformationId)+10 as a ,deviceInformationId  from TABLE_DICTIONARY_EXCLUDE group by deviceInformationId order by deviceInformationId""",
      s"""select sum(deviceInformationId)+10 as a ,deviceInformationId  from TABLE_DICTIONARY_EXCLUDE1_hive group by deviceInformationId order by deviceInformationId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_204")

  }


  //DICTIONARY_EXCLUDE_205
  test("Queries_DICTIONARY_EXCLUDE_205", Include) {

    checkAnswer(s"""select max(deviceInformationId),min(deviceInformationId) from TABLE_DICTIONARY_EXCLUDE""",
      s"""select max(deviceInformationId),min(deviceInformationId) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_205")

  }


  //DICTIONARY_EXCLUDE_206
  test("Queries_DICTIONARY_EXCLUDE_206", Include) {

    checkAnswer(s"""select sum(deviceInformationId) a  from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(deviceInformationId) a  from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_206")

  }


  //DICTIONARY_EXCLUDE_207
  test("Queries_DICTIONARY_EXCLUDE_207", Include) {

    checkAnswer(s"""select avg(deviceInformationId) a  from TABLE_DICTIONARY_EXCLUDE""",
      s"""select avg(deviceInformationId) a  from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_207")

  }


  //DICTIONARY_EXCLUDE_208
  test("Queries_DICTIONARY_EXCLUDE_208", Include) {

    checkAnswer(s"""select min(deviceInformationId) a  from TABLE_DICTIONARY_EXCLUDE""",
      s"""select min(deviceInformationId) a  from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_208")

  }


  //DICTIONARY_EXCLUDE_209
  test("Queries_DICTIONARY_EXCLUDE_209", Include) {

    sql(s"""select variance(deviceInformationId) as a   from (select deviceInformationId from TABLE_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect

  }


  //DICTIONARY_EXCLUDE_210
  ignore("Queries_DICTIONARY_EXCLUDE_210", Include) {

    checkAnswer(s"""select var_pop(deviceInformationId)  as a from TABLE_DICTIONARY_EXCLUDE""",
      s"""select var_pop(deviceInformationId)  as a from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_210")

  }


  //DICTIONARY_EXCLUDE_211
  ignore("Queries_DICTIONARY_EXCLUDE_211", Include) {

    checkAnswer(s"""select var_samp(deviceInformationId) as a  from TABLE_DICTIONARY_EXCLUDE""",
      s"""select var_samp(deviceInformationId) as a  from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_211")

  }


  //DICTIONARY_EXCLUDE_212
  test("Queries_DICTIONARY_EXCLUDE_212", Include) {

    sql(s"""select stddev_pop(deviceInformationId) as a  from (select deviceInformationId from TABLE_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect

  }


  //DICTIONARY_EXCLUDE_213
  test("Queries_DICTIONARY_EXCLUDE_213", Include) {

    sql(s"""select stddev_samp(deviceInformationId)  as a from (select deviceInformationId from TABLE_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect

  }


  //DICTIONARY_EXCLUDE_214
  test("Queries_DICTIONARY_EXCLUDE_214", Include) {

    sql(s"""select covar_pop(deviceInformationId,deviceInformationId) as a  from (select deviceInformationId from TABLE_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect

  }


  //DICTIONARY_EXCLUDE_215
  test("Queries_DICTIONARY_EXCLUDE_215", Include) {

    sql(s"""select covar_samp(deviceInformationId,deviceInformationId) as a  from (select deviceInformationId from TABLE_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect

  }


  //DICTIONARY_EXCLUDE_216
  test("Queries_DICTIONARY_EXCLUDE_216", Include) {

    checkAnswer(s"""select corr(deviceInformationId,deviceInformationId)  as a from TABLE_DICTIONARY_EXCLUDE""",
      s"""select corr(deviceInformationId,deviceInformationId)  as a from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_216")

  }


  //DICTIONARY_EXCLUDE_217
  test("Queries_DICTIONARY_EXCLUDE_217", Include) {

    sql(s"""select percentile_approx(deviceInformationId,0.2) as a  from (select deviceInformationId from TABLE_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect

  }


  //DICTIONARY_EXCLUDE_218
  test("Queries_DICTIONARY_EXCLUDE_218", Include) {

    sql(s"""select percentile_approx(deviceInformationId,0.2,5) as a  from (select deviceInformationId from TABLE_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect

  }


  //DICTIONARY_EXCLUDE_219
  test("Queries_DICTIONARY_EXCLUDE_219", Include) {

    sql(s"""select percentile_approx(deviceInformationId,array(0.2,0.3,0.99))  as a from (select deviceInformationId from TABLE_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect

  }


  //DICTIONARY_EXCLUDE_220
  test("Queries_DICTIONARY_EXCLUDE_220", Include) {

    sql(s"""select percentile_approx(deviceInformationId,array(0.2,0.3,0.99),5) as a from (select deviceInformationId from TABLE_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect

  }


  //DICTIONARY_EXCLUDE_221
  test("Queries_DICTIONARY_EXCLUDE_221", Include) {

    sql(s"""select histogram_numeric(deviceInformationId,2)  as a from (select deviceInformationId from TABLE_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect

  }


  //DICTIONARY_EXCLUDE_222
  test("Queries_DICTIONARY_EXCLUDE_222", Include) {

    checkAnswer(s"""select deviceInformationId, deviceInformationId+ 10 as a  from TABLE_DICTIONARY_EXCLUDE order by deviceInformationId""",
      s"""select deviceInformationId, deviceInformationId+ 10 as a  from TABLE_DICTIONARY_EXCLUDE1_hive order by deviceInformationId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_222")

  }


  //DICTIONARY_EXCLUDE_223
  test("Queries_DICTIONARY_EXCLUDE_223", Include) {

    checkAnswer(s"""select min(deviceInformationId), max(deviceInformationId+ 10) Total from TABLE_DICTIONARY_EXCLUDE group by  channelsId order by Total""",
      s"""select min(deviceInformationId), max(deviceInformationId+ 10) Total from TABLE_DICTIONARY_EXCLUDE1_hive group by  channelsId order by Total""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_223")

  }


  //DICTIONARY_EXCLUDE_224
  test("Queries_DICTIONARY_EXCLUDE_224", Include) {

    sql(s"""select last(deviceInformationId) a from TABLE_DICTIONARY_EXCLUDE order by a""").collect

  }


  //DICTIONARY_EXCLUDE_225
  test("Queries_DICTIONARY_EXCLUDE_225", Include) {

    sql(s"""select FIRST(deviceInformationId) a from (select deviceInformationId from TABLE_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect

  }


  //DICTIONARY_EXCLUDE_226
  test("Queries_DICTIONARY_EXCLUDE_226", Include) {

    checkAnswer(s"""select deviceInformationId,count(deviceInformationId) a from TABLE_DICTIONARY_EXCLUDE group by deviceInformationId order by deviceInformationId""",
      s"""select deviceInformationId,count(deviceInformationId) a from TABLE_DICTIONARY_EXCLUDE1_hive group by deviceInformationId order by deviceInformationId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_226")

  }


  //DICTIONARY_EXCLUDE_227
  test("Queries_DICTIONARY_EXCLUDE_227", Include) {

    checkAnswer(s"""select Lower(deviceInformationId) a  from TABLE_DICTIONARY_EXCLUDE order by deviceInformationId""",
      s"""select Lower(deviceInformationId) a  from TABLE_DICTIONARY_EXCLUDE1_hive order by deviceInformationId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_227")

  }


  //DICTIONARY_EXCLUDE_228
  test("Queries_DICTIONARY_EXCLUDE_228", Include) {

    checkAnswer(s"""select distinct deviceInformationId from TABLE_DICTIONARY_EXCLUDE order by deviceInformationId""",
      s"""select distinct deviceInformationId from TABLE_DICTIONARY_EXCLUDE1_hive order by deviceInformationId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_228")

  }


  //DICTIONARY_EXCLUDE_229
  test("Queries_DICTIONARY_EXCLUDE_229", Include) {

    checkAnswer(s"""select deviceInformationId from TABLE_DICTIONARY_EXCLUDE order by deviceInformationId limit 101""",
      s"""select deviceInformationId from TABLE_DICTIONARY_EXCLUDE1_hive order by deviceInformationId limit 101""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_229")

  }


  //DICTIONARY_EXCLUDE_230
  test("Queries_DICTIONARY_EXCLUDE_230", Include) {

    checkAnswer(s"""select deviceInformationId as a from TABLE_DICTIONARY_EXCLUDE  order by a asc limit 10""",
      s"""select deviceInformationId as a from TABLE_DICTIONARY_EXCLUDE1_hive  order by a asc limit 10""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_230")

  }


  //DICTIONARY_EXCLUDE_231
  test("Queries_DICTIONARY_EXCLUDE_231", Include) {

    checkAnswer(s"""select deviceInformationId from TABLE_DICTIONARY_EXCLUDE where  (deviceInformationId == 100084) and (deviceInformationId==100084)""",
      s"""select deviceInformationId from TABLE_DICTIONARY_EXCLUDE1_hive where  (deviceInformationId == 100084) and (deviceInformationId==100084)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_231")

  }


  //DICTIONARY_EXCLUDE_232
  test("Queries_DICTIONARY_EXCLUDE_232", Include) {

    checkAnswer(s"""select deviceInformationId from TABLE_DICTIONARY_EXCLUDE where deviceInformationId !='100084' order by deviceInformationId""",
      s"""select deviceInformationId from TABLE_DICTIONARY_EXCLUDE1_hive where deviceInformationId !='100084' order by deviceInformationId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_232")

  }


  //DICTIONARY_EXCLUDE_233
  test("Queries_DICTIONARY_EXCLUDE_233", Include) {

    checkAnswer(s"""select deviceInformationId  from TABLE_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select deviceInformationId  from TABLE_DICTIONARY_EXCLUDE1_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_233")

  }


  //DICTIONARY_EXCLUDE_234
  test("Queries_DICTIONARY_EXCLUDE_234", Include) {

    checkAnswer(s"""select deviceInformationId from TABLE_DICTIONARY_EXCLUDE where deviceInformationId !=100084 order by deviceInformationId""",
      s"""select deviceInformationId from TABLE_DICTIONARY_EXCLUDE1_hive where deviceInformationId !=100084 order by deviceInformationId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_234")

  }


  //DICTIONARY_EXCLUDE_235
  test("Queries_DICTIONARY_EXCLUDE_235", Include) {

    checkAnswer(s"""select deviceInformationId from TABLE_DICTIONARY_EXCLUDE where deviceInformationId >100084 order by deviceInformationId""",
      s"""select deviceInformationId from TABLE_DICTIONARY_EXCLUDE1_hive where deviceInformationId >100084 order by deviceInformationId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_235")

  }


  //DICTIONARY_EXCLUDE_236
  test("Queries_DICTIONARY_EXCLUDE_236", Include) {

    checkAnswer(s"""select deviceInformationId  from TABLE_DICTIONARY_EXCLUDE where deviceInformationId<>deviceInformationId order by deviceInformationId""",
      s"""select deviceInformationId  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceInformationId<>deviceInformationId order by deviceInformationId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_236")

  }


  //DICTIONARY_EXCLUDE_237
  test("Queries_DICTIONARY_EXCLUDE_237", Include) {

    checkAnswer(s"""select deviceInformationId from TABLE_DICTIONARY_EXCLUDE where deviceInformationId != Latest_areaId order by deviceInformationId""",
      s"""select deviceInformationId from TABLE_DICTIONARY_EXCLUDE1_hive where deviceInformationId != Latest_areaId order by deviceInformationId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_237")

  }


  //DICTIONARY_EXCLUDE_238
  test("Queries_DICTIONARY_EXCLUDE_238", Include) {

    checkAnswer(s"""select deviceInformationId, deviceInformationId from TABLE_DICTIONARY_EXCLUDE where Latest_areaId<deviceInformationId order by deviceInformationId""",
      s"""select deviceInformationId, deviceInformationId from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_areaId<deviceInformationId order by deviceInformationId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_238")

  }


  //DICTIONARY_EXCLUDE_239
  test("Queries_DICTIONARY_EXCLUDE_239", Include) {

    checkAnswer(s"""select deviceInformationId, deviceInformationId from TABLE_DICTIONARY_EXCLUDE where deviceInformationId<=deviceInformationId order by deviceInformationId""",
      s"""select deviceInformationId, deviceInformationId from TABLE_DICTIONARY_EXCLUDE1_hive where deviceInformationId<=deviceInformationId order by deviceInformationId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_239")

  }


  //DICTIONARY_EXCLUDE_240
  test("Queries_DICTIONARY_EXCLUDE_240", Include) {

    checkAnswer(s"""select deviceInformationId from TABLE_DICTIONARY_EXCLUDE where deviceInformationId <1000 order by deviceInformationId""",
      s"""select deviceInformationId from TABLE_DICTIONARY_EXCLUDE1_hive where deviceInformationId <1000 order by deviceInformationId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_240")

  }


  //DICTIONARY_EXCLUDE_241
  test("Queries_DICTIONARY_EXCLUDE_241", Include) {

    checkAnswer(s"""select deviceInformationId from TABLE_DICTIONARY_EXCLUDE where deviceInformationId >1000 order by deviceInformationId""",
      s"""select deviceInformationId from TABLE_DICTIONARY_EXCLUDE1_hive where deviceInformationId >1000 order by deviceInformationId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_241")

  }


  //DICTIONARY_EXCLUDE_242
  test("Queries_DICTIONARY_EXCLUDE_242", Include) {

    checkAnswer(s"""select deviceInformationId  from TABLE_DICTIONARY_EXCLUDE where deviceInformationId IS NULL order by deviceInformationId""",
      s"""select deviceInformationId  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceInformationId IS NULL order by deviceInformationId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_242")

  }


  //DICTIONARY_EXCLUDE_243
  test("Queries_DICTIONARY_EXCLUDE_243", Include) {

    checkAnswer(s"""select deviceInformationId  from TABLE_DICTIONARY_EXCLUDE where deviceInformationId IS NOT NULL order by deviceInformationId""",
      s"""select deviceInformationId  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceInformationId IS NOT NULL order by deviceInformationId""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_243")

  }


  //DICTIONARY_EXCLUDE_244
  test("Queries_DICTIONARY_EXCLUDE_244", Include) {

    checkAnswer(s"""select sum(imei)+10 as a   from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(imei)+10 as a   from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_244")

  }


  //DICTIONARY_EXCLUDE_245
  test("Queries_DICTIONARY_EXCLUDE_245", Include) {

    checkAnswer(s"""select sum(imei)*10 as a   from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(imei)*10 as a   from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_245")

  }


  //DICTIONARY_EXCLUDE_246
  test("Queries_DICTIONARY_EXCLUDE_246", Include) {

    checkAnswer(s"""select sum(imei)/10 as a   from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(imei)/10 as a   from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_246")

  }


  //DICTIONARY_EXCLUDE_247
  test("Queries_DICTIONARY_EXCLUDE_247", Include) {

    checkAnswer(s"""select sum(imei)-10 as a   from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(imei)-10 as a   from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_247")

  }


  //DICTIONARY_EXCLUDE_248
  test("Queries_DICTIONARY_EXCLUDE_248", Include) {

    checkAnswer(s"""select sum(contractNumber)+10 as a   from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(contractNumber)+10 as a   from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_248")

  }


  //DICTIONARY_EXCLUDE_249
  test("Queries_DICTIONARY_EXCLUDE_249", Include) {

    checkAnswer(s"""select sum(contractNumber)*10 as a   from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(contractNumber)*10 as a   from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_249")

  }


  //DICTIONARY_EXCLUDE_250
  test("Queries_DICTIONARY_EXCLUDE_250", Include) {

    checkAnswer(s"""select sum(contractNumber)/10 as a   from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(contractNumber)/10 as a   from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_250")

  }


  //DICTIONARY_EXCLUDE_251
  test("Queries_DICTIONARY_EXCLUDE_251", Include) {

    checkAnswer(s"""select sum(contractNumber)-10 as a   from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(contractNumber)-10 as a   from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_251")

  }


  //DICTIONARY_EXCLUDE_252
  test("Queries_DICTIONARY_EXCLUDE_252", Include) {

    checkAnswer(s"""select sum(Latest_DAY)+10 as a   from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(Latest_DAY)+10 as a   from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_252")

  }


  //DICTIONARY_EXCLUDE_253
  test("Queries_DICTIONARY_EXCLUDE_253", Include) {

    checkAnswer(s"""select sum(Latest_DAY)*10 as a   from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(Latest_DAY)*10 as a   from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_253")

  }


  //DICTIONARY_EXCLUDE_254
  test("Queries_DICTIONARY_EXCLUDE_254", Include) {

    checkAnswer(s"""select sum(Latest_DAY)/10 as a   from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(Latest_DAY)/10 as a   from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_254")

  }


  //DICTIONARY_EXCLUDE_255
  test("Queries_DICTIONARY_EXCLUDE_255", Include) {

    checkAnswer(s"""select sum(Latest_DAY)-10 as a   from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(Latest_DAY)-10 as a   from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_255")

  }


  //DICTIONARY_EXCLUDE_256
  ignore("Queries_DICTIONARY_EXCLUDE_256", Include) {

    checkAnswer(s"""select sum(gamePointId)+10 as a from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId)""",
      s"""select sum(gamePointId)+10 as a from (select gamePointId from TABLE_DICTIONARY_EXCLUDE1_hive order by gamePointId)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_256")

  }


  //DICTIONARY_EXCLUDE_257
  ignore("Queries_DICTIONARY_EXCLUDE_257", Include) {

    checkAnswer(s"""select sum(gamePointId)*10 as a from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId)""",
      s"""select sum(gamePointId)*10 as a from (select gamePointId from TABLE_DICTIONARY_EXCLUDE1_hive order by gamePointId)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_257")

  }


  //DICTIONARY_EXCLUDE_258
  test("Queries_DICTIONARY_EXCLUDE_258", Include) {

    checkAnswer(s"""select sum(gamePointId)/10 as a   from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(gamePointId)/10 as a   from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_258")

  }


  //DICTIONARY_EXCLUDE_259
  ignore("Queries_DICTIONARY_EXCLUDE_259", Include) {

    checkAnswer(s"""select sum(gamePointId)-10 as a   from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId)""",
      s"""select sum(gamePointId)-10 as a   from (select gamePointId from TABLE_DICTIONARY_EXCLUDE1_hive order by gamePointId)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_259")

  }


  //DICTIONARY_EXCLUDE_260
  test("Queries_DICTIONARY_EXCLUDE_260", Include) {

    checkAnswer(s"""select sum(productionDate)+10 as a   from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(productionDate)+10 as a   from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_260")

  }


  //DICTIONARY_EXCLUDE_261
  test("Queries_DICTIONARY_EXCLUDE_261", Include) {

    checkAnswer(s"""select sum(productionDate)*10 as a   from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(productionDate)*10 as a   from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_261")

  }


  //DICTIONARY_EXCLUDE_262
  test("Queries_DICTIONARY_EXCLUDE_262", Include) {

    checkAnswer(s"""select sum(productionDate)/10 as a   from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(productionDate)/10 as a   from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_262")

  }


  //DICTIONARY_EXCLUDE_263
  test("Queries_DICTIONARY_EXCLUDE_263", Include) {

    checkAnswer(s"""select sum(productionDate)-10 as a   from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(productionDate)-10 as a   from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_263")

  }


  //DICTIONARY_EXCLUDE_264
  test("Queries_DICTIONARY_EXCLUDE_264", Include) {

    checkAnswer(s"""select sum(deviceInformationId)+10 as a   from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(deviceInformationId)+10 as a   from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_264")

  }


  //DICTIONARY_EXCLUDE_265
  test("Queries_DICTIONARY_EXCLUDE_265", Include) {

    checkAnswer(s"""select sum(deviceInformationId)*10 as a   from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(deviceInformationId)*10 as a   from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_265")

  }


  //DICTIONARY_EXCLUDE_266
  test("Queries_DICTIONARY_EXCLUDE_266", Include) {

    checkAnswer(s"""select sum(deviceInformationId)/10 as a   from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(deviceInformationId)/10 as a   from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_266")

  }


  //DICTIONARY_EXCLUDE_267
  test("Queries_DICTIONARY_EXCLUDE_267", Include) {

    checkAnswer(s"""select sum(deviceInformationId)-10 as a   from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(deviceInformationId)-10 as a   from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_267")

  }


  //DICTIONARY_EXCLUDE_292
  test("Queries_DICTIONARY_EXCLUDE_292", Include) {

    checkAnswer(s"""SELECT productiondate from TABLE_DICTIONARY_EXCLUDE where productiondate LIKE '2015-09-30%'""",
      s"""SELECT productiondate from TABLE_DICTIONARY_EXCLUDE1_hive where productiondate LIKE '2015-09-30%'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_292")

  }


  //DICTIONARY_EXCLUDE_293
  test("Queries_DICTIONARY_EXCLUDE_293", Include) {

    checkAnswer(s"""SELECT productiondate from TABLE_DICTIONARY_EXCLUDE where productiondate LIKE '% %'""",
      s"""SELECT productiondate from TABLE_DICTIONARY_EXCLUDE1_hive where productiondate LIKE '% %'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_293")

  }


  //DICTIONARY_EXCLUDE_294
  test("Queries_DICTIONARY_EXCLUDE_294", Include) {

    checkAnswer(s"""SELECT productiondate from TABLE_DICTIONARY_EXCLUDE where productiondate LIKE '%12:07:28'""",
      s"""SELECT productiondate from TABLE_DICTIONARY_EXCLUDE1_hive where productiondate LIKE '%12:07:28'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_294")

  }


  //DICTIONARY_EXCLUDE_295
  test("Queries_DICTIONARY_EXCLUDE_295", Include) {

    checkAnswer(s"""select contractnumber from TABLE_DICTIONARY_EXCLUDE where contractnumber like '922337204%' """,
      s"""select contractnumber from TABLE_DICTIONARY_EXCLUDE1_hive where contractnumber like '922337204%' """, "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_295")

  }


  //DICTIONARY_EXCLUDE_296
  test("Queries_DICTIONARY_EXCLUDE_296", Include) {

    checkAnswer(s"""select contractnumber from TABLE_DICTIONARY_EXCLUDE where contractnumber like '%047800'""",
      s"""select contractnumber from TABLE_DICTIONARY_EXCLUDE1_hive where contractnumber like '%047800'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_296")

  }


  //DICTIONARY_EXCLUDE_297
  test("Queries_DICTIONARY_EXCLUDE_297", Include) {

    checkAnswer(s"""select contractnumber from TABLE_DICTIONARY_EXCLUDE where contractnumber like '%720%'""",
      s"""select contractnumber from TABLE_DICTIONARY_EXCLUDE1_hive where contractnumber like '%720%'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_297")

  }


  //DICTIONARY_EXCLUDE_298
  test("Queries_DICTIONARY_EXCLUDE_298", Include) {

    checkAnswer(s"""SELECT Latest_DAY from TABLE_DICTIONARY_EXCLUDE where Latest_DAY like '12345678%'""",
      s"""SELECT Latest_DAY from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_DAY like '12345678%'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_298")

  }


  //DICTIONARY_EXCLUDE_299
  test("Queries_DICTIONARY_EXCLUDE_299", Include) {

    checkAnswer(s"""SELECT Latest_DAY from TABLE_DICTIONARY_EXCLUDE where Latest_DAY like '%5678%'""",
      s"""SELECT Latest_DAY from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_DAY like '%5678%'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_299")

  }


  //DICTIONARY_EXCLUDE_300
  test("Queries_DICTIONARY_EXCLUDE_300", Include) {

    checkAnswer(s"""SELECT Latest_DAY from TABLE_DICTIONARY_EXCLUDE where Latest_DAY like '1234567%'""",
      s"""SELECT Latest_DAY from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_DAY like '1234567%'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_300")

  }


  //DICTIONARY_EXCLUDE_301
  test("Queries_DICTIONARY_EXCLUDE_301", Include) {

    checkAnswer(s"""SELECT gamepointID from TABLE_DICTIONARY_EXCLUDE where gamepointID like '1.1098347722%'""",
      s"""SELECT gamepointID from TABLE_DICTIONARY_EXCLUDE1_hive where gamepointID like '1.1098347722%'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_301")

  }


  //DICTIONARY_EXCLUDE_302
  test("Queries_DICTIONARY_EXCLUDE_302", Include) {

    checkAnswer(s"""SELECT gamepointID from TABLE_DICTIONARY_EXCLUDE where gamepointID like '%8347722%'""",
      s"""SELECT gamepointID from TABLE_DICTIONARY_EXCLUDE1_hive where gamepointID like '%8347722%'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_302")

  }


  //DICTIONARY_EXCLUDE_303
  test("Queries_DICTIONARY_EXCLUDE_303", Include) {

    checkAnswer(s"""SELECT gamepointID from TABLE_DICTIONARY_EXCLUDE where gamepointID like '%7512E42'""",
      s"""SELECT gamepointID from TABLE_DICTIONARY_EXCLUDE1_hive where gamepointID like '%7512E42'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_303")

  }


  //DICTIONARY_EXCLUDE_304
  test("Queries_DICTIONARY_EXCLUDE_304", Include) {

    checkAnswer(s"""SELECT deviceinformationid from TABLE_DICTIONARY_EXCLUDE where deviceinformationid like '1000%'""",
      s"""SELECT deviceinformationid from TABLE_DICTIONARY_EXCLUDE1_hive where deviceinformationid like '1000%'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_304")

  }


  //DICTIONARY_EXCLUDE_305
  test("Queries_DICTIONARY_EXCLUDE_305", Include) {

    checkAnswer(s"""SELECT deviceinformationid from TABLE_DICTIONARY_EXCLUDE where deviceinformationid like '%00%'""",
      s"""SELECT deviceinformationid from TABLE_DICTIONARY_EXCLUDE1_hive where deviceinformationid like '%00%'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_305")

  }


  //DICTIONARY_EXCLUDE_306
  test("Queries_DICTIONARY_EXCLUDE_306", Include) {

    checkAnswer(s"""SELECT deviceinformationid from TABLE_DICTIONARY_EXCLUDE where deviceinformationid like '%0084'""",
      s"""SELECT deviceinformationid from TABLE_DICTIONARY_EXCLUDE1_hive where deviceinformationid like '%0084'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_306")

  }


  //DICTIONARY_EXCLUDE_307
  test("Queries_DICTIONARY_EXCLUDE_307", Include) {

    checkAnswer(s"""select imei from TABLE_DICTIONARY_EXCLUDE where imei like '1AA10%'""",
      s"""select imei from TABLE_DICTIONARY_EXCLUDE1_hive where imei like '1AA10%'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_307")

  }


  //DICTIONARY_EXCLUDE_308
  test("Queries_DICTIONARY_EXCLUDE_308", Include) {

    checkAnswer(s"""select imei from TABLE_DICTIONARY_EXCLUDE where imei like '%A10%'""",
      s"""select imei from TABLE_DICTIONARY_EXCLUDE1_hive where imei like '%A10%'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_308")

  }


  //DICTIONARY_EXCLUDE_309
  test("Queries_DICTIONARY_EXCLUDE_309", Include) {

    checkAnswer(s"""select imei from TABLE_DICTIONARY_EXCLUDE where imei like '%00084'""",
      s"""select imei from TABLE_DICTIONARY_EXCLUDE1_hive where imei like '%00084'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_309")

  }


  //DICTIONARY_EXCLUDE_310
  test("Queries_DICTIONARY_EXCLUDE_310", Include) {

    checkAnswer(s"""select imei from TABLE_DICTIONARY_EXCLUDE where imei in ('1AA100074','1AA100075','1AA100077')""",
      s"""select imei from TABLE_DICTIONARY_EXCLUDE1_hive where imei in ('1AA100074','1AA100075','1AA100077')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_310")

  }


  //DICTIONARY_EXCLUDE_311
  test("Queries_DICTIONARY_EXCLUDE_311", Include) {

    checkAnswer(s"""select imei from TABLE_DICTIONARY_EXCLUDE where imei not in ('1AA100074','1AA100075','1AA100077')""",
      s"""select imei from TABLE_DICTIONARY_EXCLUDE1_hive where imei not in ('1AA100074','1AA100075','1AA100077')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_311")

  }


  //DICTIONARY_EXCLUDE_312
  test("Queries_DICTIONARY_EXCLUDE_312", Include) {

    checkAnswer(s"""select deviceinformationid from TABLE_DICTIONARY_EXCLUDE where deviceinformationid in (100081,100078,10008)""",
      s"""select deviceinformationid from TABLE_DICTIONARY_EXCLUDE1_hive where deviceinformationid in (100081,100078,10008)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_312")

  }


  //DICTIONARY_EXCLUDE_313
  test("Queries_DICTIONARY_EXCLUDE_313", Include) {

    checkAnswer(s"""select deviceinformationid from TABLE_DICTIONARY_EXCLUDE where deviceinformationid not in (100081,100078,10008)""",
      s"""select deviceinformationid from TABLE_DICTIONARY_EXCLUDE1_hive where deviceinformationid not in (100081,100078,10008)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_313")

  }


  //DICTIONARY_EXCLUDE_314
  test("Queries_DICTIONARY_EXCLUDE_314", Include) {

    checkAnswer(s"""select productiondate from TABLE_DICTIONARY_EXCLUDE where productiondate in ('2015-10-04 12:07:28','2015-10-07%','2015-10-07 12:07:28')""",
      s"""select productiondate from TABLE_DICTIONARY_EXCLUDE1_hive where productiondate in ('2015-10-04 12:07:28','2015-10-07%','2015-10-07 12:07:28')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_314")

  }


  //DICTIONARY_EXCLUDE_315
  test("Queries_DICTIONARY_EXCLUDE_315", Include) {

    checkAnswer(s"""select productiondate from TABLE_DICTIONARY_EXCLUDE where productiondate not in (cast('2015-10-04 12:07:28' as timestamp),cast('2015-10-07 12:07:28' as timestamp))""",
      s"""select productiondate from TABLE_DICTIONARY_EXCLUDE1_hive where productiondate not in (cast('2015-10-04 12:07:28' as timestamp),cast('2015-10-07 12:07:28' as timestamp))""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_315")

  }


  //DICTIONARY_EXCLUDE_316
  test("Queries_DICTIONARY_EXCLUDE_316", Include) {

    checkAnswer(s"""select gamepointid from TABLE_DICTIONARY_EXCLUDE where gamepointid in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""",
      s"""select gamepointid from TABLE_DICTIONARY_EXCLUDE1_hive where gamepointid in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_316")

  }


  //DICTIONARY_EXCLUDE_317
  test("Queries_DICTIONARY_EXCLUDE_317", Include) {

    checkAnswer(s"""select gamepointid from TABLE_DICTIONARY_EXCLUDE where gamepointid not in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""",
      s"""select gamepointid from TABLE_DICTIONARY_EXCLUDE1_hive where gamepointid not in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_317")

  }


  //DICTIONARY_EXCLUDE_318
  test("Queries_DICTIONARY_EXCLUDE_318", Include) {

    checkAnswer(s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE where Latest_DAY in (1234567890123530.0000000000,1234567890123520.0000000000)""",
      s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_DAY in (1234567890123530.0000000000,1234567890123520.0000000000)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_318")

  }


  //DICTIONARY_EXCLUDE_319
  test("Queries_DICTIONARY_EXCLUDE_319", Include) {

    checkAnswer(s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE where Latest_DAY not in (1234567890123530.0000000000,1234567890123520.0000000000)""",
      s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_DAY not in (1234567890123530.0000000000,1234567890123520.0000000000)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_319")

  }


  //DICTIONARY_EXCLUDE_322
  test("Queries_DICTIONARY_EXCLUDE_322", Include) {

    checkAnswer(s"""select imei from TABLE_DICTIONARY_EXCLUDE where imei !='1AA100077'""",
      s"""select imei from TABLE_DICTIONARY_EXCLUDE1_hive where imei !='1AA100077'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_322")

  }


  //DICTIONARY_EXCLUDE_323
  test("Queries_DICTIONARY_EXCLUDE_323", Include) {

    checkAnswer(s"""select imei from TABLE_DICTIONARY_EXCLUDE where imei NOT LIKE '1AA100077'""",
      s"""select imei from TABLE_DICTIONARY_EXCLUDE1_hive where imei NOT LIKE '1AA100077'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_323")

  }


  //DICTIONARY_EXCLUDE_324
  test("Queries_DICTIONARY_EXCLUDE_324", Include) {

    checkAnswer(s"""select deviceinformationid from TABLE_DICTIONARY_EXCLUDE where deviceinformationid !=100078""",
      s"""select deviceinformationid from TABLE_DICTIONARY_EXCLUDE1_hive where deviceinformationid !=100078""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_324")

  }


  //DICTIONARY_EXCLUDE_325
  test("Queries_DICTIONARY_EXCLUDE_325", Include) {

    checkAnswer(s"""select deviceinformationid from TABLE_DICTIONARY_EXCLUDE where deviceinformationid NOT LIKE 100079""",
      s"""select deviceinformationid from TABLE_DICTIONARY_EXCLUDE1_hive where deviceinformationid NOT LIKE 100079""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_325")

  }


  //DICTIONARY_EXCLUDE_326
  test("Queries_DICTIONARY_EXCLUDE_326", Include) {

    checkAnswer(s"""select productiondate from TABLE_DICTIONARY_EXCLUDE where productiondate !='2015-10-07 12:07:28'""",
      s"""select productiondate from TABLE_DICTIONARY_EXCLUDE1_hive where productiondate !='2015-10-07 12:07:28'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_326")

  }


  //DICTIONARY_EXCLUDE_327
  ignore("Queries_DICTIONARY_EXCLUDE_327", Include) {

    checkAnswer(s"""select productiondate from TABLE_DICTIONARY_EXCLUDE where productiondate NOT LIKE '2015-10-07 12:07:28'""",
      s"""select productiondate from TABLE_DICTIONARY_EXCLUDE1_hive where productiondate NOT LIKE '2015-10-07 12:07:28'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_327")

  }


  //DICTIONARY_EXCLUDE_328
  test("Queries_DICTIONARY_EXCLUDE_328", Include) {

    checkAnswer(s"""select gamepointid from TABLE_DICTIONARY_EXCLUDE where gamepointid !=6.8591561117512E42""",
      s"""select gamepointid from TABLE_DICTIONARY_EXCLUDE1_hive where gamepointid !=6.8591561117512E42""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_328")

  }


  //DICTIONARY_EXCLUDE_329
  test("Queries_DICTIONARY_EXCLUDE_329", Include) {

    checkAnswer(s"""select gamepointid from TABLE_DICTIONARY_EXCLUDE where gamepointid NOT LIKE 6.8591561117512E43""",
      s"""select gamepointid from TABLE_DICTIONARY_EXCLUDE1_hive where gamepointid NOT LIKE 6.8591561117512E43""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_329")

  }


  //DICTIONARY_EXCLUDE_330
  test("Queries_DICTIONARY_EXCLUDE_330", Include) {

    checkAnswer(s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE where Latest_DAY != 1234567890123520.0000000000""",
      s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_DAY != 1234567890123520.0000000000""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_330")

  }


  //DICTIONARY_EXCLUDE_331
  test("Queries_DICTIONARY_EXCLUDE_331", Include) {

    checkAnswer(s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE where Latest_DAY NOT LIKE 1234567890123520.0000000000""",
      s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_DAY NOT LIKE 1234567890123520.0000000000""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_331")

  }


  //DICTIONARY_EXCLUDE_335
  test("Queries_DICTIONARY_EXCLUDE_335", Include) {

    checkAnswer(s"""SELECT productiondate,IMEI from TABLE_DICTIONARY_EXCLUDE where IMEI RLIKE '1AA100077'""",
      s"""SELECT productiondate,IMEI from TABLE_DICTIONARY_EXCLUDE1_hive where IMEI RLIKE '1AA100077'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_335")

  }


  //DICTIONARY_EXCLUDE_336
  test("Queries_DICTIONARY_EXCLUDE_336", Include) {

    checkAnswer(s"""SELECT deviceinformationId from TABLE_DICTIONARY_EXCLUDE where deviceinformationId RLIKE '100079'""",
      s"""SELECT deviceinformationId from TABLE_DICTIONARY_EXCLUDE1_hive where deviceinformationId RLIKE '100079'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_336")

  }


  //DICTIONARY_EXCLUDE_337
  test("Queries_DICTIONARY_EXCLUDE_337", Include) {

    checkAnswer(s"""SELECT gamepointid from TABLE_DICTIONARY_EXCLUDE where gamepointid RLIKE '1.61922711065643E42'""",
      s"""SELECT gamepointid from TABLE_DICTIONARY_EXCLUDE1_hive where gamepointid RLIKE '1.61922711065643E42'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_337")

  }


  //DICTIONARY_EXCLUDE_338
  test("Queries_DICTIONARY_EXCLUDE_338", Include) {

    checkAnswer(s"""SELECT Latest_Day from TABLE_DICTIONARY_EXCLUDE where Latest_Day RLIKE '1234567890123550.0000000000'""",
      s"""SELECT Latest_Day from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_Day RLIKE '1234567890123550.0000000000'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_338")

  }


  //DICTIONARY_EXCLUDE_339
  test("Queries_DICTIONARY_EXCLUDE_339", Include) {

    checkAnswer(s"""SELECT contractnumber from TABLE_DICTIONARY_EXCLUDE where contractnumber RLIKE '9223372047800'""",
      s"""SELECT contractnumber from TABLE_DICTIONARY_EXCLUDE1_hive where contractnumber RLIKE '9223372047800'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_339")

  }


  //DICTIONARY_EXCLUDE_340
  test("Queries_DICTIONARY_EXCLUDE_340", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from TABLE_DICTIONARY_EXCLUDE a join TABLE_DICTIONARY_EXCLUDE b on a.productiondate=b.productiondate""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from TABLE_DICTIONARY_EXCLUDE1_hive a join TABLE_DICTIONARY_EXCLUDE1_hive b on a.productiondate=b.productiondate""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_340")

  }


  //DICTIONARY_EXCLUDE_341
  test("Queries_DICTIONARY_EXCLUDE_341", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from TABLE_DICTIONARY_EXCLUDE a join TABLE_DICTIONARY_EXCLUDE b on a.deviceinformationid=b.deviceinformationid""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from TABLE_DICTIONARY_EXCLUDE1_hive a join TABLE_DICTIONARY_EXCLUDE1_hive b on a.deviceinformationid=b.deviceinformationid""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_341")

  }


  //DICTIONARY_EXCLUDE_342
  test("Queries_DICTIONARY_EXCLUDE_342", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from TABLE_DICTIONARY_EXCLUDE a join TABLE_DICTIONARY_EXCLUDE b on a.imei=b.imei""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from TABLE_DICTIONARY_EXCLUDE1_hive a join TABLE_DICTIONARY_EXCLUDE1_hive b on a.imei=b.imei""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_342")

  }


  //DICTIONARY_EXCLUDE_343
  test("Queries_DICTIONARY_EXCLUDE_343", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from TABLE_DICTIONARY_EXCLUDE a join TABLE_DICTIONARY_EXCLUDE b on a.gamepointid=b.gamepointid""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from TABLE_DICTIONARY_EXCLUDE1_hive a join TABLE_DICTIONARY_EXCLUDE1_hive b on a.gamepointid=b.gamepointid""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_343")

  }


  //DICTIONARY_EXCLUDE_344
  test("Queries_DICTIONARY_EXCLUDE_344", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from TABLE_DICTIONARY_EXCLUDE a join TABLE_DICTIONARY_EXCLUDE b on a.Latest_Day=b.Latest_Day""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from TABLE_DICTIONARY_EXCLUDE1_hive a join TABLE_DICTIONARY_EXCLUDE1_hive b on a.Latest_Day=b.Latest_Day""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_344")

  }


  //DICTIONARY_EXCLUDE_345
  test("Queries_DICTIONARY_EXCLUDE_345", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from TABLE_DICTIONARY_EXCLUDE a join TABLE_DICTIONARY_EXCLUDE b on a.contractnumber=b.contractnumber""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from TABLE_DICTIONARY_EXCLUDE1_hive a join TABLE_DICTIONARY_EXCLUDE1_hive b on a.contractnumber=b.contractnumber""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_345")

  }


  //DICTIONARY_EXCLUDE_346
  test("Queries_DICTIONARY_EXCLUDE_346", Include) {

    checkAnswer(s"""select count( contractNumber ),sum( contractNumber ),count(distinct contractNumber ),avg( contractNumber ),max( contractNumber ),min( contractNumber ),1 from TABLE_DICTIONARY_EXCLUDE""",
      s"""select count( contractNumber ),sum( contractNumber ),count(distinct contractNumber ),avg( contractNumber ),max( contractNumber ),min( contractNumber ),1 from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_346")

  }


  //DICTIONARY_EXCLUDE_347
  test("Queries_DICTIONARY_EXCLUDE_347", Include) {

    checkAnswer(s"""select count( Latest_Day ),sum( Latest_Day ),count(distinct Latest_Day ),avg( Latest_Day ),max( Latest_Day ),min( Latest_Day ),1 from TABLE_DICTIONARY_EXCLUDE""",
      s"""select count( Latest_Day ),sum( Latest_Day ),count(distinct Latest_Day ),avg( Latest_Day ),max( Latest_Day ),min( Latest_Day ),1 from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_347")

  }


  //DICTIONARY_EXCLUDE_348
  ignore("Queries_DICTIONARY_EXCLUDE_348", Include) {

    checkAnswer(s"""select count( gamePointId),sum( gamePointId ),count(distinct gamePointId ),avg(gamePointId),max(gamePointId),min(gamePointId),1 from TABLE_DICTIONARY_EXCLUDE""",
      s"""select count( gamePointId),sum( gamePointId ),count(distinct gamePointId ),avg(gamePointId),max(gamePointId),min(gamePointId),1 from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_348")

  }


  //DICTIONARY_EXCLUDE_349
  test("Queries_DICTIONARY_EXCLUDE_349", Include) {

    checkAnswer(s"""select count(deviceInformationId),sum(deviceInformationId),count(deviceInformationId),avg(deviceInformationId),max(deviceInformationId),min(deviceInformationId),1 from TABLE_DICTIONARY_EXCLUDE""",
      s"""select count(deviceInformationId),sum(deviceInformationId),count(deviceInformationId),avg(deviceInformationId),max(deviceInformationId),min(deviceInformationId),1 from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_349")

  }


  //DICTIONARY_EXCLUDE_350
  test("Queries_DICTIONARY_EXCLUDE_350", Include) {

    checkAnswer(s"""select count( productionDate),sum(  productionDate ),count(distinct  productionDate ),avg(  productionDate ),max(  productionDate),min(  productionDate ),1 from TABLE_DICTIONARY_EXCLUDE""",
      s"""select count( productionDate),sum(  productionDate ),count(distinct  productionDate ),avg(  productionDate ),max(  productionDate),min(  productionDate ),1 from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_350")

  }


  //DICTIONARY_EXCLUDE_351
  test("Queries_DICTIONARY_EXCLUDE_351", Include) {

    checkAnswer(s"""select count(IMEI ),sum(IMEI ),count(distinct IMEI ),avg(IMEI ),max(IMEI ),min(IMEI ),1 from TABLE_DICTIONARY_EXCLUDE""",
      s"""select count(IMEI ),sum(IMEI ),count(distinct IMEI ),avg(IMEI ),max(IMEI ),min(IMEI ),1 from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_351")

  }


  //DICTIONARY_EXCLUDE_352
  test("Queries_DICTIONARY_EXCLUDE_352", Include) {

    checkAnswer(s"""select sum(contractNumber),count(contractNumber),avg(contractNumber),sum(contractNumber)/count(contractNumber) from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(contractNumber),count(contractNumber),avg(contractNumber),sum(contractNumber)/count(contractNumber) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_352")

  }


  //DICTIONARY_EXCLUDE_353
  test("Queries_DICTIONARY_EXCLUDE_353", Include) {

    checkAnswer(s"""select sum(Latest_Day),count(Latest_Day),avg(Latest_Day),sum(Latest_Day)/count(Latest_Day) from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(Latest_Day),count(Latest_Day),avg(Latest_Day),sum(Latest_Day)/count(Latest_Day) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_353")

  }


  //DICTIONARY_EXCLUDE_354
  ignore("Queries_DICTIONARY_EXCLUDE_354", Include) {

    checkAnswer(s"""select sum(gamepointId),count(gamepointId),avg(gamepointID),sum(gamepointID)/count(gamepointID) from (select gamePointId from TABLE_DICTIONARY_EXCLUDE order by gamePointId)""",
      s"""select sum(gamepointId),count(gamepointId),avg(gamepointID),sum(gamepointID)/count(gamepointID) from (select gamePointId from TABLE_DICTIONARY_EXCLUDE1_hive order by gamePointId)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_354")

  }


  //DICTIONARY_EXCLUDE_355
  test("Queries_DICTIONARY_EXCLUDE_355", Include) {

    checkAnswer(s"""select sum(deviceinformationId),count(deviceinformationId),avg(deviceinformationId),sum(deviceinformationId)/count(deviceinformationId) from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(deviceinformationId),count(deviceinformationId),avg(deviceinformationId),sum(deviceinformationId)/count(deviceinformationId) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_355")

  }


  //DICTIONARY_EXCLUDE_356
  test("Queries_DICTIONARY_EXCLUDE_356", Include) {

    checkAnswer(s"""select sum(IMEI),count(IMEI),avg(IMEI),sum(IMEI)/count(IMEI) from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(IMEI),count(IMEI),avg(IMEI),sum(IMEI)/count(IMEI) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_356")

  }


  //DICTIONARY_EXCLUDE_357
  test("Queries_DICTIONARY_EXCLUDE_357", Include) {

    checkAnswer(s"""select sum(productionDate),count(productionDate),avg(productionDate),sum(productionDate)/count(productionDate) from TABLE_DICTIONARY_EXCLUDE""",
      s"""select sum(productionDate),count(productionDate),avg(productionDate),sum(productionDate)/count(productionDate) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_357")

  }


  //DICTIONARY_EXCLUDE_358
  test("Queries_DICTIONARY_EXCLUDE_358", Include) {

    checkAnswer(s"""select contractNumber,Latest_DAY,gamePointId,productionDate,deviceInformationId,IMEI  from TABLE_DICTIONARY_EXCLUDE""",
      s"""select contractNumber,Latest_DAY,gamePointId,productionDate,deviceInformationId,IMEI  from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_358")

  }


  //DICTIONARY_EXCLUDE_359
  test("Queries_DICTIONARY_EXCLUDE_359", Include) {

    checkAnswer(s"""select count(MAC) from TABLE_DICTIONARY_EXCLUDE""",
      s"""select count(MAC) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_359")

  }


  //DICTIONARY_EXCLUDE_360
  test("Queries_DICTIONARY_EXCLUDE_360", Include) {

    checkAnswer(s"""select count(gamePointId) from TABLE_DICTIONARY_EXCLUDE""",
      s"""select count(gamePointId) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_360")

  }


  //DICTIONARY_EXCLUDE_361
  test("Queries_DICTIONARY_EXCLUDE_361", Include) {

    checkAnswer(s"""select count(contractNumber) from TABLE_DICTIONARY_EXCLUDE""",
      s"""select count(contractNumber) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_361")

  }


  //DICTIONARY_EXCLUDE_362
  test("Queries_DICTIONARY_EXCLUDE_362", Include) {

    checkAnswer(s"""select count(Latest_DAY) from TABLE_DICTIONARY_EXCLUDE""",
      s"""select count(Latest_DAY) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_362")

  }


  //DICTIONARY_EXCLUDE_363
  test("Queries_DICTIONARY_EXCLUDE_363", Include) {

    checkAnswer(s"""select count(productionDate) from TABLE_DICTIONARY_EXCLUDE""",
      s"""select count(productionDate) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_363")

  }


  //DICTIONARY_EXCLUDE_364
  test("Queries_DICTIONARY_EXCLUDE_364", Include) {

    checkAnswer(s"""select count(deviceInformationId) from TABLE_DICTIONARY_EXCLUDE""",
      s"""select count(deviceInformationId) from TABLE_DICTIONARY_EXCLUDE1_hive""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_364")

  }


  //DICTIONARY_EXCLUDE_365
  test("Queries_DICTIONARY_EXCLUDE_365", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from TABLE_DICTIONARY_EXCLUDE where  contractNumber  != '9223372047700'""",
      s"""select imei,deviceInformationId,MAC,deviceColor from TABLE_DICTIONARY_EXCLUDE1_hive where  contractNumber  != '9223372047700'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_365")

  }


  //DICTIONARY_EXCLUDE_366
  test("Queries_DICTIONARY_EXCLUDE_366", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from TABLE_DICTIONARY_EXCLUDE where  Latest_DAY  != '1234567890123480.0000000000' order by imei,deviceInformationId,MAC,deviceColor limit 5""",
      s"""select imei,deviceInformationId,MAC,deviceColor from TABLE_DICTIONARY_EXCLUDE1_hive where  Latest_DAY  != '1234567890123480.0000000000' order by imei,deviceInformationId,MAC,deviceColor limit 5""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_366")

  }


  //DICTIONARY_EXCLUDE_367
  test("Queries_DICTIONARY_EXCLUDE_367", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from TABLE_DICTIONARY_EXCLUDE where  gamePointId  != '2.27852521808948E36' order by imei,deviceInformationId,MAC,deviceColor limit 5""",
      s"""select imei,deviceInformationId,MAC,deviceColor from TABLE_DICTIONARY_EXCLUDE1_hive where  gamePointId  != '2.27852521808948E36' order by imei,deviceInformationId,MAC,deviceColor limit 5""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_367")

  }


  //DICTIONARY_EXCLUDE_368
  test("Queries_DICTIONARY_EXCLUDE_368", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from TABLE_DICTIONARY_EXCLUDE where  productionDate  != '2015-09-18 12:07:28.0' order by imei,deviceInformationId,MAC,deviceColor limit 5""",
      s"""select imei,deviceInformationId,MAC,deviceColor from TABLE_DICTIONARY_EXCLUDE1_hive where  productionDate  != '2015-09-18 12:07:28.0' order by imei,deviceInformationId,MAC,deviceColor limit 5""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_368")

  }


  //DICTIONARY_EXCLUDE_369
  test("Queries_DICTIONARY_EXCLUDE_369", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from TABLE_DICTIONARY_EXCLUDE where  deviceInformationId  != 100075 order by imei,deviceInformationId,MAC,deviceColor limit 5""",
      s"""select imei,deviceInformationId,MAC,deviceColor from TABLE_DICTIONARY_EXCLUDE1_hive where  deviceInformationId  != 100075 order by imei,deviceInformationId,MAC,deviceColor limit 5""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_369")

  }


  //DICTIONARY_EXCLUDE_370
  test("Queries_DICTIONARY_EXCLUDE_370", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from TABLE_DICTIONARY_EXCLUDE where  contractNumber  not like '9223372047700' order by  deviceInformationId limit 5""",
      s"""select imei,deviceInformationId,MAC,deviceColor from TABLE_DICTIONARY_EXCLUDE1_hive where  contractNumber  not like '9223372047700' order by  deviceInformationId limit 5""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_370")

  }


  //DICTIONARY_EXCLUDE_371
  test("Queries_DICTIONARY_EXCLUDE_371", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from TABLE_DICTIONARY_EXCLUDE where  Latest_DAY  not like '1234567890123480.0000000000' order by imei, deviceInformationId, deviceColor limit 5""",
      s"""select imei,deviceInformationId,MAC,deviceColor from TABLE_DICTIONARY_EXCLUDE1_hive where  Latest_DAY  not like '1234567890123480.0000000000' order by imei, deviceInformationId, deviceColor limit 5""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_371")

  }


  //DICTIONARY_EXCLUDE_372
  test("Queries_DICTIONARY_EXCLUDE_372", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from TABLE_DICTIONARY_EXCLUDE where  gamePointId  not like '2.27852521808948E36' order by imei,deviceInformationId,MAC limit 5""",
      s"""select imei,deviceInformationId,MAC,deviceColor from TABLE_DICTIONARY_EXCLUDE1_hive where  gamePointId  not like '2.27852521808948E36' order by imei,deviceInformationId,MAC limit 5""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_372")

  }


  //DICTIONARY_EXCLUDE_373
  test("Queries_DICTIONARY_EXCLUDE_373", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from TABLE_DICTIONARY_EXCLUDE where  productionDate  not like '2015-09-18 12:07:28.0' order by imei,deviceInformationId,MAC limit 5""",
      s"""select imei,deviceInformationId,MAC,deviceColor from TABLE_DICTIONARY_EXCLUDE1_hive where  productionDate  not like '2015-09-18 12:07:28.0' order by imei,deviceInformationId,MAC limit 5""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_373")

  }


  //DICTIONARY_EXCLUDE_374
  test("Queries_DICTIONARY_EXCLUDE_374", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from TABLE_DICTIONARY_EXCLUDE where  deviceInformationId  not like '100075' order by imei,deviceInformationId,MAC limit 5""",
      s"""select imei,deviceInformationId,MAC,deviceColor from TABLE_DICTIONARY_EXCLUDE1_hive where  deviceInformationId  not like '100075' order by imei,deviceInformationId,MAC limit 5""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_374")

  }


  //DICTIONARY_EXCLUDE_375
  test("Queries_DICTIONARY_EXCLUDE_375", Include) {

    checkAnswer(s"""select imei from TABLE_DICTIONARY_EXCLUDE where imei is not null""",
      s"""select imei from TABLE_DICTIONARY_EXCLUDE1_hive where imei is not null""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_375")

  }


  //DICTIONARY_EXCLUDE_376
  test("Queries_DICTIONARY_EXCLUDE_376", Include) {

    checkAnswer(s"""select gamePointId from TABLE_DICTIONARY_EXCLUDE where gamePointId is not null""",
      s"""select gamePointId from TABLE_DICTIONARY_EXCLUDE1_hive where gamePointId is not null""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_376")

  }


  //DICTIONARY_EXCLUDE_377
  test("Queries_DICTIONARY_EXCLUDE_377", Include) {

    checkAnswer(s"""select contractNumber from TABLE_DICTIONARY_EXCLUDE where contractNumber is not null""",
      s"""select contractNumber from TABLE_DICTIONARY_EXCLUDE1_hive where contractNumber is not null""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_377")

  }


  //DICTIONARY_EXCLUDE_378
  test("Queries_DICTIONARY_EXCLUDE_378", Include) {

    checkAnswer(s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE where Latest_DAY is not null""",
      s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_DAY is not null""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_378")

  }


  //DICTIONARY_EXCLUDE_379
  test("Queries_DICTIONARY_EXCLUDE_379", Include) {

    checkAnswer(s"""select productionDate from TABLE_DICTIONARY_EXCLUDE where productionDate is not null""",
      s"""select productionDate from TABLE_DICTIONARY_EXCLUDE1_hive where productionDate is not null""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_379")

  }


  //DICTIONARY_EXCLUDE_380
  test("Queries_DICTIONARY_EXCLUDE_380", Include) {

    checkAnswer(s"""select deviceInformationId from TABLE_DICTIONARY_EXCLUDE where deviceInformationId is not null""",
      s"""select deviceInformationId from TABLE_DICTIONARY_EXCLUDE1_hive where deviceInformationId is not null""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_380")

  }


  //DICTIONARY_EXCLUDE_381
  test("Queries_DICTIONARY_EXCLUDE_381", Include) {

    checkAnswer(s"""select imei from TABLE_DICTIONARY_EXCLUDE where imei is  null""",
      s"""select imei from TABLE_DICTIONARY_EXCLUDE1_hive where imei is  null""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_381")

  }


  //DICTIONARY_EXCLUDE_382
  test("Queries_DICTIONARY_EXCLUDE_382", Include) {

    checkAnswer(s"""select gamePointId from TABLE_DICTIONARY_EXCLUDE where gamePointId is  null""",
      s"""select gamePointId from TABLE_DICTIONARY_EXCLUDE1_hive where gamePointId is  null""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_382")

  }


  //DICTIONARY_EXCLUDE_383
  test("Queries_DICTIONARY_EXCLUDE_383", Include) {

    checkAnswer(s"""select contractNumber from TABLE_DICTIONARY_EXCLUDE where contractNumber is  null""",
      s"""select contractNumber from TABLE_DICTIONARY_EXCLUDE1_hive where contractNumber is  null""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_383")

  }


  //DICTIONARY_EXCLUDE_384
  test("Queries_DICTIONARY_EXCLUDE_384", Include) {

    checkAnswer(s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE where Latest_DAY is  null""",
      s"""select Latest_DAY from TABLE_DICTIONARY_EXCLUDE1_hive where Latest_DAY is  null""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_384")

  }


  //DICTIONARY_EXCLUDE_385
  test("Queries_DICTIONARY_EXCLUDE_385", Include) {

    checkAnswer(s"""select productionDate from TABLE_DICTIONARY_EXCLUDE where productionDate is  null""",
      s"""select productionDate from TABLE_DICTIONARY_EXCLUDE1_hive where productionDate is  null""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_385")

  }


  //DICTIONARY_EXCLUDE_386
  test("Queries_DICTIONARY_EXCLUDE_386", Include) {

    checkAnswer(s"""select deviceInformationId from TABLE_DICTIONARY_EXCLUDE where deviceInformationId is  null""",
      s"""select deviceInformationId from TABLE_DICTIONARY_EXCLUDE1_hive where deviceInformationId is  null""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_386")

  }


  //DICTIONARY_EXCLUDE_387
  test("Queries_DICTIONARY_EXCLUDE_387", Include) {

    checkAnswer(s"""select count(*) from TABLE_DICTIONARY_EXCLUDE where imei = '1AA1'""",
      s"""select count(*) from TABLE_DICTIONARY_EXCLUDE1_hive where imei = '1AA1'""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_387")

  }


  //DICTIONARY_EXCLUDE_PushUP_001
  test("Queries_DICTIONARY_EXCLUDE_PushUP_001", Include) {

    checkAnswer(s"""select count(imei)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000'  and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(imei)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000'  and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_001")

  }


  //DICTIONARY_EXCLUDE_PushUP_002
  test("Queries_DICTIONARY_EXCLUDE_PushUP_002", Include) {

    checkAnswer(s"""select count(deviceinformationId)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(deviceinformationId)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_002")

  }


  //DICTIONARY_EXCLUDE_PushUP_003
  test("Queries_DICTIONARY_EXCLUDE_PushUP_003", Include) {

    checkAnswer(s"""select count(productionDate)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(productionDate)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_003")

  }


  //DICTIONARY_EXCLUDE_PushUP_004
  test("Queries_DICTIONARY_EXCLUDE_PushUP_004", Include) {

    checkAnswer(s"""select count(gamePointId)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(gamePointId)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_004")

  }


  //DICTIONARY_EXCLUDE_PushUP_005
  test("Queries_DICTIONARY_EXCLUDE_PushUP_005", Include) {

    checkAnswer(s"""select count(Latest_DAY)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(Latest_DAY)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_005")

  }


  //DICTIONARY_EXCLUDE_PushUP_006
  test("Queries_DICTIONARY_EXCLUDE_PushUP_006", Include) {

    checkAnswer(s"""select count(contractNumber)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(contractNumber)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_006")

  }


  //DICTIONARY_EXCLUDE_PushUP_007
  test("Queries_DICTIONARY_EXCLUDE_PushUP_007", Include) {

    checkAnswer(s"""select count(distinct imei)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(distinct imei)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_007")

  }


  //DICTIONARY_EXCLUDE_PushUP_008
  test("Queries_DICTIONARY_EXCLUDE_PushUP_008", Include) {

    checkAnswer(s"""select count(distinct deviceinformationId)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(distinct deviceinformationId)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_008")

  }


  //DICTIONARY_EXCLUDE_PushUP_009
  test("Queries_DICTIONARY_EXCLUDE_PushUP_009", Include) {

    checkAnswer(s"""select count(distinct productionDate)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(distinct productionDate)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_009")

  }


  //DICTIONARY_EXCLUDE_PushUP_010
  test("Queries_DICTIONARY_EXCLUDE_PushUP_010", Include) {

    checkAnswer(s"""select count(distinct gamePointId)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(distinct gamePointId)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_010")

  }


  //DICTIONARY_EXCLUDE_PushUP_011
  test("Queries_DICTIONARY_EXCLUDE_PushUP_011", Include) {

    checkAnswer(s"""select count(distinct Latest_DAY)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(distinct Latest_DAY)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_011")

  }


  //DICTIONARY_EXCLUDE_PushUP_012
  test("Queries_DICTIONARY_EXCLUDE_PushUP_012", Include) {

    checkAnswer(s"""select count(distinct contractNumber)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(distinct contractNumber)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_012")

  }


  //DICTIONARY_EXCLUDE_PushUP_013
  test("Queries_DICTIONARY_EXCLUDE_PushUP_013", Include) {

    checkAnswer(s"""select sum(deviceinformationId)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select sum(deviceinformationId)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_013")

  }


  //DICTIONARY_EXCLUDE_PushUP_014
  test("Queries_DICTIONARY_EXCLUDE_PushUP_014", Include) {

    checkAnswer(s"""select sum(productionDate)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select sum(productionDate)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_014")

  }


  //DICTIONARY_EXCLUDE_PushUP_015
  test("Queries_DICTIONARY_EXCLUDE_PushUP_015", Include) {

    sql(s"""select sum(gamePointId)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect

  }


  //DICTIONARY_EXCLUDE_PushUP_016
  test("Queries_DICTIONARY_EXCLUDE_PushUP_016", Include) {

    checkAnswer(s"""select sum(Latest_DAY)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select sum(Latest_DAY)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_016")

  }


  //DICTIONARY_EXCLUDE_PushUP_017
  test("Queries_DICTIONARY_EXCLUDE_PushUP_017", Include) {

    checkAnswer(s"""select sum(contractNumber)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select sum(contractNumber)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_017")

  }


  //DICTIONARY_EXCLUDE_PushUP_018
  test("Queries_DICTIONARY_EXCLUDE_PushUP_018", Include) {

    checkAnswer(s"""select sum(distinct deviceinformationId)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select sum(distinct deviceinformationId)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_018")

  }


  //DICTIONARY_EXCLUDE_PushUP_019
  test("Queries_DICTIONARY_EXCLUDE_PushUP_019", Include) {

    checkAnswer(s"""select sum(distinct gamePointId)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select sum(distinct gamePointId)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_019")

  }


  //DICTIONARY_EXCLUDE_PushUP_020
  test("Queries_DICTIONARY_EXCLUDE_PushUP_020", Include) {

    checkAnswer(s"""select sum(distinct Latest_DAY)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select sum(distinct Latest_DAY)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_020")

  }


  //DICTIONARY_EXCLUDE_PushUP_021
  test("Queries_DICTIONARY_EXCLUDE_PushUP_021", Include) {

    checkAnswer(s"""select sum(distinct contractNumber)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select sum(distinct contractNumber)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_021")

  }


  //DICTIONARY_EXCLUDE_PushUP_022
  test("Queries_DICTIONARY_EXCLUDE_PushUP_022", Include) {

    checkAnswer(s"""select min(deviceinformationId)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select min(deviceinformationId)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_022")

  }


  //DICTIONARY_EXCLUDE_PushUP_023
  test("Queries_DICTIONARY_EXCLUDE_PushUP_023", Include) {

    checkAnswer(s"""select min(productionDate)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select min(productionDate)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_023")

  }


  //DICTIONARY_EXCLUDE_PushUP_024
  test("Queries_DICTIONARY_EXCLUDE_PushUP_024", Include) {

    checkAnswer(s"""select min(gamePointId)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select min(gamePointId)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_024")

  }


  //DICTIONARY_EXCLUDE_PushUP_025
  test("Queries_DICTIONARY_EXCLUDE_PushUP_025", Include) {

    checkAnswer(s"""select min(Latest_DAY)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select min(Latest_DAY)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_025")

  }


  //DICTIONARY_EXCLUDE_PushUP_026
  test("Queries_DICTIONARY_EXCLUDE_PushUP_026", Include) {

    checkAnswer(s"""select min(contractNumber)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select min(contractNumber)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_026")

  }


  //DICTIONARY_EXCLUDE_PushUP_027
  test("Queries_DICTIONARY_EXCLUDE_PushUP_027", Include) {

    checkAnswer(s"""select max(imei)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select max(imei)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_027")

  }


  //DICTIONARY_EXCLUDE_PushUP_028
  test("Queries_DICTIONARY_EXCLUDE_PushUP_028", Include) {

    checkAnswer(s"""select max(deviceinformationId)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select max(deviceinformationId)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_028")

  }


  //DICTIONARY_EXCLUDE_PushUP_029
  test("Queries_DICTIONARY_EXCLUDE_PushUP_029", Include) {

    checkAnswer(s"""select max(productionDate)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select max(productionDate)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_029")

  }


  //DICTIONARY_EXCLUDE_PushUP_030
  test("Queries_DICTIONARY_EXCLUDE_PushUP_030", Include) {

    checkAnswer(s"""select max(gamePointId)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select max(gamePointId)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_030")

  }


  //DICTIONARY_EXCLUDE_PushUP_031
  test("Queries_DICTIONARY_EXCLUDE_PushUP_031", Include) {

    checkAnswer(s"""select max(Latest_DAY)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select max(Latest_DAY)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_031")

  }


  //DICTIONARY_EXCLUDE_PushUP_032
  test("Queries_DICTIONARY_EXCLUDE_PushUP_032", Include) {

    checkAnswer(s"""select max(contractNumber)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select max(contractNumber)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_032")

  }


  //DICTIONARY_EXCLUDE_PushUP_033
  test("Queries_DICTIONARY_EXCLUDE_PushUP_033", Include) {

    checkAnswer(s"""select variance(deviceInformationId)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select variance(deviceInformationId)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_033")

  }


  //DICTIONARY_EXCLUDE_PushUP_034
  ignore("Queries_DICTIONARY_EXCLUDE_PushUP_034", Include) {

    checkAnswer(s"""select variance(gamePointId) from (select * from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by gamePointId)""",
      s"""select variance(gamePointId) from (select * from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by gamePointId)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_034")

  }


  //DICTIONARY_EXCLUDE_PushUP_035
  test("Queries_DICTIONARY_EXCLUDE_PushUP_035", Include) {

    sql(s"""select variance(gamePointId)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect

  }


  //DICTIONARY_EXCLUDE_PushUP_036
  ignore("Queries_DICTIONARY_EXCLUDE_PushUP_036", Include) {

    checkAnswer(s"""select variance(Latest_DAY) from (select * from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by Latest_DAY)""",
      s"""select variance(Latest_DAY) from (select * from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by Latest_DAY)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_036")

  }


  //DICTIONARY_EXCLUDE_PushUP_037
  test("Queries_DICTIONARY_EXCLUDE_PushUP_037", Include) {

    checkAnswer(s"""select variance(contractNumber) from (select * from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by contractNumber)""",
      s"""select variance(contractNumber) from (select * from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by contractNumber)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_037")

  }


  //DICTIONARY_EXCLUDE_PushUP_038
  test("Queries_DICTIONARY_EXCLUDE_PushUP_038", Include) {

    checkAnswer(s"""select var_samp(deviceInformationId)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select var_samp(deviceInformationId)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_038")

  }


  //DICTIONARY_EXCLUDE_PushUP_039
  ignore("Queries_DICTIONARY_EXCLUDE_PushUP_039", Include) {

    checkAnswer(s"""select var_samp(gamePointId) from (select * from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by gamePointId)""",
      s"""select var_samp(gamePointId) from (select * from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by gamePointId)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_039")

  }


  //DICTIONARY_EXCLUDE_PushUP_040
  test("Queries_DICTIONARY_EXCLUDE_PushUP_040", Include) {

    sql(s"""select var_samp(gamePointId)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect

  }


  //DICTIONARY_EXCLUDE_PushUP_041
  ignore("Queries_DICTIONARY_EXCLUDE_PushUP_041", Include) {

    checkAnswer(s"""select var_samp(Latest_DAY) from (select * from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by Latest_DAY)""",
      s"""select var_samp(Latest_DAY) from (select * from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by Latest_DAY)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_041")

  }


  //DICTIONARY_EXCLUDE_PushUP_042
  test("Queries_DICTIONARY_EXCLUDE_PushUP_042", Include) {

    sql(s"""select var_samp(contractNumber) from (select contractNumber from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')order by contractNumber)""").collect

  }


  //DICTIONARY_EXCLUDE_PushUP_043
  ignore("Queries_DICTIONARY_EXCLUDE_PushUP_043", Include) {

    checkAnswer(s"""select stddev_pop(deviceInformationId)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select stddev_pop(deviceInformationId)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_043")

  }


  //DICTIONARY_EXCLUDE_PushUP_044
  test("Queries_DICTIONARY_EXCLUDE_PushUP_044", Include) {

    checkAnswer(s"""select stddev_pop(gamePointId) from (select * from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by gamePointId)""",
      s"""select stddev_pop(gamePointId) from (select * from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by gamePointId)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_044")

  }


  //DICTIONARY_EXCLUDE_PushUP_045
  test("Queries_DICTIONARY_EXCLUDE_PushUP_045", Include) {

    sql(s"""select stddev_pop(gamePointId)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect

  }


  //DICTIONARY_EXCLUDE_PushUP_046
  ignore("Queries_DICTIONARY_EXCLUDE_PushUP_046", Include) {

    checkAnswer(s"""select stddev_pop(Latest_DAY) from (select * from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by Latest_DAY)""",
      s"""select stddev_pop(Latest_DAY) from (select * from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by Latest_DAY)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_046")

  }


  //DICTIONARY_EXCLUDE_PushUP_047
  ignore("Queries_DICTIONARY_EXCLUDE_PushUP_047", Include) {

    checkAnswer(s"""select stddev_pop(contractNumber)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select stddev_pop(contractNumber)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_047")

  }


  //DICTIONARY_EXCLUDE_PushUP_048
  test("Queries_DICTIONARY_EXCLUDE_PushUP_048", Include) {

    checkAnswer(s"""select stddev_samp(deviceInformationId)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select stddev_samp(deviceInformationId)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_048")

  }


  //DICTIONARY_EXCLUDE_PushUP_049
  ignore("Queries_DICTIONARY_EXCLUDE_PushUP_049", Include) {

    checkAnswer(s"""select stddev_samp(gamePointId) from (select * from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by gamePointId)""",
      s"""select stddev_samp(gamePointId) from (select * from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by gamePointId)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_049")

  }


  //DICTIONARY_EXCLUDE_PushUP_050
  ignore("Queries_DICTIONARY_EXCLUDE_PushUP_050", Include) {

    checkAnswer(s"""select stddev_samp(gamePointId) from (select * from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by gamePointId)""",
      s"""select stddev_samp(gamePointId) from (select * from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by gamePointId)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_050")

  }


  //DICTIONARY_EXCLUDE_PushUP_051
  ignore("Queries_DICTIONARY_EXCLUDE_PushUP_051", Include) {

    checkAnswer(s"""select stddev_samp(Latest_DAY) from (select * from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by Latest_DAY)""",
      s"""select stddev_samp(Latest_DAY) from (select * from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by Latest_DAY)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_051")

  }


  //DICTIONARY_EXCLUDE_PushUP_053
  ignore("Queries_DICTIONARY_EXCLUDE_PushUP_053", Include) {

    checkAnswer(s"""select count(imei),count(distinct deviceinformationId),sum(deviceinformationId),sum(distinct deviceinformationId),min(deviceinformationId),max(imei),variance(deviceInformationId),var_samp(deviceInformationId),stddev_pop(gamePointId),stddev_samp(Latest_DAY) from (select * from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by deviceinformationId)""",
      s"""select count(imei),count(distinct deviceinformationId),sum(deviceinformationId),sum(distinct deviceinformationId),min(deviceinformationId),max(imei),variance(deviceInformationId),var_samp(deviceInformationId),stddev_pop(gamePointId),stddev_samp(Latest_DAY) from (select * from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by deviceinformationId)""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_053")

  }


  //DICTIONARY_EXCLUDE_PushUP_054
  test("Queries_DICTIONARY_EXCLUDE_PushUP_054", Include) {

    checkAnswer(s"""select AVG(deviceinformationId)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select AVG(deviceinformationId)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_054")

  }


  //DICTIONARY_EXCLUDE_PushUP_055
  test("Queries_DICTIONARY_EXCLUDE_PushUP_055", Include) {

    checkAnswer(s"""select AVG(productionDate)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select AVG(productionDate)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_055")

  }


  //DICTIONARY_EXCLUDE_PushUP_056
  test("Queries_DICTIONARY_EXCLUDE_PushUP_056", Include) {

    checkAnswer(s"""select AVG(gamePointId)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select AVG(gamePointId)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_056")

  }


  //DICTIONARY_EXCLUDE_PushUP_057
  test("Queries_DICTIONARY_EXCLUDE_PushUP_057", Include) {

    checkAnswer(s"""select AVG(Latest_DAY)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select AVG(Latest_DAY)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_057")

  }


  //DICTIONARY_EXCLUDE_PushUP_058
  test("Queries_DICTIONARY_EXCLUDE_PushUP_058", Include) {

    checkAnswer(s"""select AVG(contractNumber)  from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select AVG(contractNumber)  from TABLE_DICTIONARY_EXCLUDE1_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_058")

  }


  //DICTIONARY_EXCLUDE_PushUP_059
  test("Queries_DICTIONARY_EXCLUDE_PushUP_059", Include) {

    sql(s"""select count(imei),sum(distinct deviceinformationId),min(productionDate),max(imei),variance(Latest_DAY) from TABLE_DICTIONARY_EXCLUDE group by deviceInformationId limit 5""").collect

  }


  //DICTIONARY_EXCLUDE_PushUP_060
  test("Queries_DICTIONARY_EXCLUDE_PushUP_060", Include) {

    sql(s"""select sum(deviceinformationId),sum(distinct deviceinformationId),min(deviceinformationId),max(imei),variance(deviceInformationId) from TABLE_DICTIONARY_EXCLUDE group by deviceInformationId order by deviceinformationId limit 5""").collect

  }


  //DICTIONARY_EXCLUDE_PushUP_061
  test("Queries_DICTIONARY_EXCLUDE_PushUP_061", Include) {

    sql(s"""select count(imei),sum(distinct deviceinformationId),min(productionDate),max(imei),variance(Latest_DAY) from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')group by deviceInformationId limit 5""").collect

  }


  //DICTIONARY_EXCLUDE_PushUP_062
  test("Queries_DICTIONARY_EXCLUDE_PushUP_062", Include) {

    sql(s"""select count(imei),sum(distinct deviceinformationId),min(productionDate),max(imei),variance(Latest_DAY) from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') group by deviceInformationId order by deviceinformationId limit 5""").collect

  }


  //DICTIONARY_EXCLUDE_PushUP_063
  test("Queries_DICTIONARY_EXCLUDE_PushUP_063", Include) {

    sql(s"""select count(imei),sum(distinct deviceinformationId),min(productionDate),max(imei),variance(Latest_DAY) from TABLE_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') group by deviceInformationId,productionDate   sort by productionDate limit 5
  """).collect

  }


  //DICTIONARY_EXCLUDE_PushUP_064
  test("Queries_DICTIONARY_EXCLUDE_PushUP_064", Include) {

    checkAnswer(s"""select sum(deviceinformationId+10)t  from  TABLE_DICTIONARY_EXCLUDE having t >1234567""",
      s"""select sum(deviceinformationId+10)t  from  TABLE_DICTIONARY_EXCLUDE1_hive having t >1234567""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_064")

  }


  //DICTIONARY_EXCLUDE_PushUP_065
  ignore("Queries_DICTIONARY_EXCLUDE_PushUP_065", Include) {

    checkAnswer(s"""select sum(deviceinformationId+gamePointId)t  from  (select gamePointId,deviceinformationId from TABLE_DICTIONARY_EXCLUDE order by gamePointId,deviceinformationId) having t >1234567""",
      s"""select sum(deviceinformationId+gamePointId)t  from  (select gamePointId,deviceinformationId from TABLE_DICTIONARY_EXCLUDE1_hive order by gamePointId,deviceinformationId) having t >1234567""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_065")

  }


  //DICTIONARY_EXCLUDE_PushUP_066
  ignore("Queries_DICTIONARY_EXCLUDE_PushUP_066", Include) {

    checkAnswer(s"""select sum(deviceinformationId)t,Sum(gamePointId)   from  (select gamePointId,deviceinformationId from TABLE_DICTIONARY_EXCLUDE order by gamePointId,deviceinformationId) having t >1234567""",
      s"""select sum(deviceinformationId)t,Sum(gamePointId)   from  (select gamePointId,deviceinformationId from TABLE_DICTIONARY_EXCLUDE1_hive order by gamePointId,deviceinformationId) having t >1234567""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_066")

  }


  //DICTIONARY_EXCLUDE_PushUP_067
  test("Queries_DICTIONARY_EXCLUDE_PushUP_067", Include) {

    checkAnswer(s"""select count(imei) a,sum(distinct deviceinformationId) b,min(productionDate) c  from TABLE_DICTIONARY_EXCLUDE group by imei,deviceinformationId,productionDate  order by  a,b,c""",
      s"""select count(imei) a,sum(distinct deviceinformationId) b,min(productionDate) c  from TABLE_DICTIONARY_EXCLUDE1_hive group by imei,deviceinformationId,productionDate  order by  a,b,c""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_067")

  }


  //DICTIONARY_EXCLUDE_PushUP_069
  test("Queries_DICTIONARY_EXCLUDE_PushUP_069", Include) {

    sql(s"""SELECT  min(Latest_DAY),max(imei),variance(contractNumber), SUM(gamePointId),count(imei),sum(distinct deviceinformationId) FROM (select * from TABLE_DICTIONARY_EXCLUDE) SUB_QRY GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC limit 10""").collect

  }


  //DICTIONARY_EXCLUDE_PushUP_070
  test("Queries_DICTIONARY_EXCLUDE_PushUP_070", Include) {

    checkAnswer(s"""SELECT  TABLE_DICTIONARY_EXCLUDE.gamePointId AS gamePointId,TABLE_DICTIONARY_EXCLUDE.AMSize AS AMSize, TABLE_DICTIONARY_EXCLUDE.ActiveCountry AS ActiveCountry, TABLE_DICTIONARY_EXCLUDE.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from TABLE_DICTIONARY_EXCLUDE) SUB_QRY ) TABLE_DICTIONARY_EXCLUDE FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from TABLE_DICTIONARY_EXCLUDE) SUB_QRY ) TABLE_DICTIONARY_EXCLUDE1 ON TABLE_DICTIONARY_EXCLUDE.AMSize = TABLE_DICTIONARY_EXCLUDE1.AMSize WHERE TABLE_DICTIONARY_EXCLUDE.AMSize LIKE '5RAM %' GROUP BY TABLE_DICTIONARY_EXCLUDE.AMSize, TABLE_DICTIONARY_EXCLUDE.ActiveCountry, TABLE_DICTIONARY_EXCLUDE.Activecity,TABLE_DICTIONARY_EXCLUDE.gamePointId ORDER BY TABLE_DICTIONARY_EXCLUDE.gamePointId ASC, TABLE_DICTIONARY_EXCLUDE.AMSize ASC, TABLE_DICTIONARY_EXCLUDE.ActiveCountry ASC, TABLE_DICTIONARY_EXCLUDE.Activecity ASC""",
      s"""SELECT  TABLE_DICTIONARY_EXCLUDE1_hive.gamePointId AS gamePointId,TABLE_DICTIONARY_EXCLUDE1_hive.AMSize AS AMSize, TABLE_DICTIONARY_EXCLUDE1_hive.ActiveCountry AS ActiveCountry, TABLE_DICTIONARY_EXCLUDE1_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from TABLE_DICTIONARY_EXCLUDE1_hive) SUB_QRY ) TABLE_DICTIONARY_EXCLUDE1_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from TABLE_DICTIONARY_EXCLUDE1_hive) SUB_QRY ) TABLE_DICTIONARY_EXCLUDE1_hive1 ON TABLE_DICTIONARY_EXCLUDE1_hive.AMSize = TABLE_DICTIONARY_EXCLUDE1_hive1.AMSize WHERE TABLE_DICTIONARY_EXCLUDE1_hive.AMSize LIKE '5RAM %' GROUP BY TABLE_DICTIONARY_EXCLUDE1_hive.AMSize, TABLE_DICTIONARY_EXCLUDE1_hive.ActiveCountry, TABLE_DICTIONARY_EXCLUDE1_hive.Activecity,TABLE_DICTIONARY_EXCLUDE1_hive.gamePointId ORDER BY TABLE_DICTIONARY_EXCLUDE1_hive.gamePointId ASC, TABLE_DICTIONARY_EXCLUDE1_hive.AMSize ASC, TABLE_DICTIONARY_EXCLUDE1_hive.ActiveCountry ASC, TABLE_DICTIONARY_EXCLUDE1_hive.Activecity ASC""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_070")

  }


  //DICTIONARY_EXCLUDE_PushUP_071
  test("Queries_DICTIONARY_EXCLUDE_PushUP_071", Include) {

    sql(s"""SELECT TABLE_DICTIONARY_EXCLUDE.gamePointId AS gamePointId,TABLE_DICTIONARY_EXCLUDE.AMSize AS AMSize, TABLE_DICTIONARY_EXCLUDE.ActiveCountry AS ActiveCountry, TABLE_DICTIONARY_EXCLUDE.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from TABLE_DICTIONARY_EXCLUDE) SUB_QRY ) TABLE_DICTIONARY_EXCLUDE RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from TABLE_DICTIONARY_EXCLUDE) SUB_QRY ) TABLE_DICTIONARY_EXCLUDE1 ON TABLE_DICTIONARY_EXCLUDE.AMSize = TABLE_DICTIONARY_EXCLUDE1.AMSize WHERE NOT(TABLE_DICTIONARY_EXCLUDE.AMSize = "8RAM size") GROUP BY TABLE_DICTIONARY_EXCLUDE.AMSize, TABLE_DICTIONARY_EXCLUDE.ActiveCountry, TABLE_DICTIONARY_EXCLUDE.Activecity,TABLE_DICTIONARY_EXCLUDE.gamePointId  ORDER BY TABLE_DICTIONARY_EXCLUDE.AMSize ASC, TABLE_DICTIONARY_EXCLUDE.ActiveCountry ASC, TABLE_DICTIONARY_EXCLUDE.Activecity ASC limit 10""").collect

  }


  //DICTIONARY_EXCLUDE_PushUP_072
  test("Queries_DICTIONARY_EXCLUDE_PushUP_072", Include) {

    sql(s"""SELECT TABLE_DICTIONARY_EXCLUDE.gamePointId AS gamePointId,TABLE_DICTIONARY_EXCLUDE.AMSize AS AMSize, TABLE_DICTIONARY_EXCLUDE.ActiveCountry AS ActiveCountry, TABLE_DICTIONARY_EXCLUDE.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from TABLE_DICTIONARY_EXCLUDE) SUB_QRY ) TABLE_DICTIONARY_EXCLUDE LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from TABLE_DICTIONARY_EXCLUDE) SUB_QRY ) TABLE_DICTIONARY_EXCLUDE1 ON TABLE_DICTIONARY_EXCLUDE.AMSize = TABLE_DICTIONARY_EXCLUDE1.AMSize WHERE NOT(TABLE_DICTIONARY_EXCLUDE.AMSize = "8RAM size") GROUP BY TABLE_DICTIONARY_EXCLUDE.AMSize, TABLE_DICTIONARY_EXCLUDE.ActiveCountry, TABLE_DICTIONARY_EXCLUDE.Activecity,TABLE_DICTIONARY_EXCLUDE.gamePointId  ORDER BY TABLE_DICTIONARY_EXCLUDE.AMSize ASC, TABLE_DICTIONARY_EXCLUDE.ActiveCountry ASC, TABLE_DICTIONARY_EXCLUDE.Activecity ASC limit 10
  """).collect

  }


  //DICTIONARY_EXCLUDE_PushUP_073
  test("Queries_DICTIONARY_EXCLUDE_PushUP_073", Include) {

    sql(s"""SELECT TABLE_DICTIONARY_EXCLUDE.gamePointId AS gamePointId,TABLE_DICTIONARY_EXCLUDE.AMSize AS AMSize, TABLE_DICTIONARY_EXCLUDE.ActiveCountry AS ActiveCountry, TABLE_DICTIONARY_EXCLUDE.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from TABLE_DICTIONARY_EXCLUDE) SUB_QRY ) TABLE_DICTIONARY_EXCLUDE INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from TABLE_DICTIONARY_EXCLUDE) SUB_QRY ) TABLE_DICTIONARY_EXCLUDE1 ON TABLE_DICTIONARY_EXCLUDE.AMSize = TABLE_DICTIONARY_EXCLUDE1.AMSize WHERE NOT(TABLE_DICTIONARY_EXCLUDE.AMSize = "8RAM size") GROUP BY TABLE_DICTIONARY_EXCLUDE.AMSize, TABLE_DICTIONARY_EXCLUDE.ActiveCountry, TABLE_DICTIONARY_EXCLUDE.Activecity,TABLE_DICTIONARY_EXCLUDE.gamePointId  ORDER BY TABLE_DICTIONARY_EXCLUDE.AMSize ASC, TABLE_DICTIONARY_EXCLUDE.ActiveCountry ASC, TABLE_DICTIONARY_EXCLUDE.Activecity ASC limit 10
  """).collect

  }


  //DICTIONARY_EXCLUDE_PushUP_074
  test("Queries_DICTIONARY_EXCLUDE_PushUP_074", Include) {

    sql(s"""select count(gamepointid),series  from TABLE_DICTIONARY_EXCLUDE group by series order by series limit 5""").collect

  }


  //DICTIONARY_EXCLUDE_PushUP_075
  test("Queries_DICTIONARY_EXCLUDE_PushUP_075", Include) {

    checkAnswer(s"""select count(gamepointid),series  from TABLE_DICTIONARY_EXCLUDE group by series order by series""",
      s"""select count(gamepointid),series  from TABLE_DICTIONARY_EXCLUDE1_hive group by series order by series""", "QueriesExcludeDictionaryTestCase_DICTIONARY_EXCLUDE_PushUP_075")

  }


  //C20_SEQ_CreateTable-DICTIONARY_EXCLUDE-01
  test("C20_SEQ_CreateTable-DICTIONARY_EXCLUDE-01", Include) {

    sql(s"""create table smart_500_DE (MSISDN string,IMSI string,IMEI string,INTERFACEID int,GROUPID int,GGSN_ID double,SGSN_ID double,SESSION_INDICATOR int,BEGIN_TIME double,BEGIN_TIME_MSEL int,END_TIME double,END_TIME_MSEL int,PROT_CATEGORY int,PROT_TYPE int,L7_CARRIER_PROT int,SUB_PROT_TYPE int,SID double,PROBEID double,ENCRYPT_VERSION int,ROAMING_TYPE int,ROAM_DIRECTION int,MS_IP string,SERVER_IP string,MS_PORT int,SERVER_DECIMAL Decimal,APN string,SGSN_SIG_IP string,GGSN_SIG_IP_BigInt_NEGATIVE bigint,SGSN_USER_IP string,GGSN_USER_IP string,MCC string,MNC string,RAT int,LAC string,RAC string,SAC string,CI string,BROWSER_TIMESTAMP timestamp,TCP_CONN_STATES int,TCP_STATES_BIGINTPOSITIVE int,TCP_WIN_SIZE int,TCP_MSS int,TCP_CONN_TIMES int,TCP_CONN_2_FAILED_TIMES int,TCP_CONN_3_FAILED_TIMES int,HOST string,STREAMING_URL string,GET_STREAMING_FAILED_CODE int,GET_STREAMING_FLAG int,GET_NUM int,GET_SUCCEED_NUM int,GET_RETRANS_NUM int,GET_TIMEOUT_NUM int,INTBUFFER_FST_FLAG int,INTBUFFER_FULL_FLAG int,STALL_NUM int,VIDEO_FRAME_RATE int,VIDEO_CODEC_ID string,VIDEO_WIDTH int,VIDEO_HEIGHT int,AUDIO_CODEC_ID string,MEDIA_FILE_TYPE int,PLAY_STATE int,STREAMING_FLAG int,TCP_STATUS_INDICATOR int,DISCONNECTION_FLAG int,FAILURE_CODE int,FLAG int,TAC string,ECI string,TCP_SYN_TIME_MSEL int,TCP_FST_SYN_DIRECTION int,RAN_NE_USER_IP string,HOMEMCC string,HOMEMNC string,CHARGE_FLAG int,PREPAID_FLAG int,USER_AGENT string,MS_WIN_STAT_TOTAL_NUM int,MS_WIN_STAT_SMALL_NUM int,MS_ACK_TO_1STGET_DELAY int,SERVER_ACK_TO_1STDATA_DELAY int,STREAMING_TYPE int,SOURCE_VIDEO_QUALITY int,TETHERING_FLAG int,CARRIER_ID double,LAYER1ID int,LAYER2ID int,LAYER3ID int,LAYER4ID int,LAYER5ID int,LAYER6ID int,CHARGING_RULE_BASE_NAME string,SP string,EXTENDED_URL string,SV string,FIRST_SAI_CGI_ECGI string,EXTENDED_URL_OTHER string,SIGNALING_USE_FLAG int,DNS_RETRANS_NUM int,DNS_FAIL_CODE int,FIRST_RAT int,MS_INDICATOR string,LAST_SAI_CGI_ECGI string,LAST_RAT int,FIRST_LONGITUDE double,FIRST_LATITUDE double,FIRST_ALTITUDE int,FIRST_RASTERLONGITUDE double,FIRST_RASTERLATITUDE double,FIRST_RASTERALTITUDE int,FIRST_FREQUENCYSPOT int,FIRST_CLUTTER int,FIRST_USERBEHAVIOR int,FIRST_SPEED int,FIRST_CREDIBILITY int,LAST_LONGITUDE double,LAST_LATITUDE double,LAST_ALTITUDE int,LAST_RASTERLONGITUDE double,LAST_RASTERLATITUDE double,LAST_RASTERALTITUDE int,LAST_FREQUENCYSPOT int,LAST_CLUTTER int,LAST_USERBEHAVIOR int,LAST_SPEED int,LAST_CREDIBILITY int,IMEI_CIPHERTEXT string,APP_ID int,DOMAIN_NAME string,STREAMING_CACHE_IP string,STOP_LONGER_THAN_MIN_THRESHOLD int,STOP_LONGER_THAN_MAX_THRESHOLD int,PLAY_END_STAT int,STOP_START_TIME1 double,STOP_END_TIME1 double,STOP_START_TIME2 double,STOP_END_TIME2 double,STOP_START_TIME3 double,STOP_END_TIME3 double,STOP_START_TIME4 double,STOP_END_TIME4 double,STOP_START_TIME5 double,STOP_END_TIME5 double,STOP_START_TIME6 double,STOP_END_TIME6 double,STOP_START_TIME7 double,STOP_END_TIME7 double,STOP_START_TIME8 double,STOP_END_TIME8 double,STOP_START_TIME9 double,STOP_END_TIME9 double,STOP_START_TIME10 double,STOP_END_TIME10 double,FAIL_CLASS double,RECORD_TYPE double,NODATA_COUNT double,VIDEO_NODATA_DURATION double,VIDEO_SMOOTH_DURATION double,VIDEO_SD_DURATION double,VIDEO_HD_DURATION double,VIDEO_UHD_DURATION double,VIDEO_FHD_DURATION double,FLUCTUATION double,START_DOWNLOAD_THROUGHPUT double,L7_UL_GOODPUT_FULL_MSS double,SESSIONKEY string,FIRST_UCELLID double,LAST_UCELLID double,UCELLID1 double,LONGITUDE1 double,LATITUDE1 double,UCELLID2 double,LONGITUDE2 double,LATITUDE2 double,UCELLID3 double,LONGITUDE3 double,LATITUDE3 double,UCELLID4 double,LONGITUDE4 double,LATITUDE4 double,UCELLID5 double,LONGITUDE5 double,LATITUDE5 double,UCELLID6 double,LONGITUDE6 double,LATITUDE6 double,UCELLID7 double,LONGITUDE7 double,LATITUDE7 double,UCELLID8 double,LONGITUDE8 double,LATITUDE8 double,UCELLID9 double,LONGITUDE9 double,LATITUDE9 double,UCELLID10 double,LONGITUDE10 double,LATITUDE10 double,INTBUFFER_FULL_DELAY double,STALL_DURATION double,STREAMING_DW_PACKETS double,STREAMING_DOWNLOAD_DELAY double,PLAY_DURATION double,STREAMING_QUALITY int,VIDEO_DATA_RATE double,AUDIO_DATA_RATE double,STREAMING_FILESIZE double,STREAMING_DURATIOIN double,TCP_SYN_TIME double,TCP_RTT_STEP1 double,CHARGE_ID double,UL_REVERSE_TO_DL_DELAY double,DL_REVERSE_TO_UL_DELAY double,DATATRANS_DW_GOODPUT double,DATATRANS_DW_TOTAL_DURATION double,SUM_FRAGMENT_INTERVAL double,TCP_FIN_TIMES double,TCP_RESET_TIMES double,URL_CLASSIFICATION double,STREAMING_LQ_DURATIOIN double,MAX_DNS_DELAY double,MAX_DNS2SYN double,MAX_LATANCY_OF_LINK_SETUP double,MAX_SYNACK2FIRSTACK double,MAX_SYNACK2LASTACK double,MAX_ACK2GET_DELAY double,MAX_FRAG_INTERVAL_PREDELAY double,SUM_FRAG_INTERVAL_PREDELAY double,SERVICE_DELAY_MSEC double,HOMEPROVINCE double,HOMECITY double,SERVICE_ID double,CHARGING_CLASS double,DATATRANS_UL_DURATION double,ASSOCIATED_ID double,PACKET_LOSS_NUM double,JITTER double,MS_DNS_DELAY_MSEL double,GET_STREAMING_DELAY double,TCP_UL_RETRANS_WITHOUTPL double,TCP_DW_RETRANS_WITHOUTPL double,GET_MAX_UL_SIZE double,GET_MIN_UL_SIZE double,GET_MAX_DL_SIZE double,GET_MIN_DL_SIZE double,L4_UL_THROUGHPUT double,L4_DW_THROUGHPUT double,L4_UL_GOODPUT double,L4_DW_GOODPUT double,NETWORK_UL_TRAFFIC double,NETWORK_DL_TRAFFIC double,L4_UL_PACKETS double,L4_DW_PACKETS double,TCP_RTT double,TCP_UL_OUTOFSEQU double,TCP_DW_OUTOFSEQU double,TCP_UL_RETRANS double,TCP_DW_RETRANS double,TCP_UL_RETRANS_WITHPL double,TCP_DW_RETRANS_WITHPL double,TCP_UL_PACKAGES_WITHPL double,TCP_DW_PACKAGES_WITHPL double,TCP_UL_PACKAGES_WITHOUTPL double,TCP_DW_PACKAGES_WITHOUTPL double,UPPERLAYER_IP_UL_PACKETS double,UPPERLAYER_IP_DL_PACKETS double,DOWNLAYER_IP_UL_PACKETS double,DOWNLAYER_IP_DL_PACKETS double,UPPERLAYER_IP_UL_FRAGMENTS double,UPPERLAYER_IP_DL_FRAGMENTS double,DOWNLAYER_IP_UL_FRAGMENTS double,DOWNLAYER_IP_DL_FRAGMENTS double,VALID_TRANS_DURATION double,AIR_PORT_DURATION double,RADIO_CONN_TIMES double,RAN_NE_ID double,AVG_UL_RTT double,AVG_DW_RTT double,UL_RTT_LONG_NUM int,DW_RTT_LONG_NUM int,UL_RTT_STAT_NUM int,DW_RTT_STAT_NUM int,USER_PROBE_UL_LOST_PKT int,SERVER_PROBE_UL_LOST_PKT int,SERVER_PROBE_DW_LOST_PKT int,USER_PROBE_DW_LOST_PKT int,CHARGING_CHARACTERISTICS double,DL_SERIOUS_OUT_OF_ORDER_NUM double,DL_SLIGHT_OUT_OF_ORDER_NUM double,DL_FLIGHT_TOTAL_SIZE double,DL_FLIGHT_TOTAL_NUM double,DL_MAX_FLIGHT_SIZE double,UL_SERIOUS_OUT_OF_ORDER_NUM double,UL_SLIGHT_OUT_OF_ORDER_NUM double,UL_FLIGHT_TOTAL_SIZE double,UL_FLIGHT_TOTAL_NUM double,UL_MAX_FLIGHT_SIZE double,USER_DL_SLIGHT_OUT_OF_ORDER_PACKETS double,SERVER_UL_SLIGHT_OUT_OF_ORDER_PACKETS double,DL_CONTINUOUS_RETRANSMISSION_DELAY double,USER_HUNGRY_DELAY double,SERVER_HUNGRY_DELAY double,AVG_DW_RTT_MICRO_SEC int,AVG_UL_RTT_MICRO_SEC int,FLOW_SAMPLE_RATIO int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES ( 'DICTIONARY_EXCLUDE'='MSISDN,IMSI,IMEI,MS_IP,SERVER_IP,HOST,SP,MS_INDICATOR,streaming_url','DICTIONARY_INCLUDE'='SESSION_INDICATOR,SERVER_DECIMAL,TCP_STATES_BIGINTPOSITIVE')""").collect

    sql(s"""create table smart_500_DE_hive (SID double,PROBEID double,INTERFACEID int,GROUPID int,GGSN_ID double,SGSN_ID double,dummy_6 string,SESSION_INDICATOR int,BEGIN_TIME double,BEGIN_TIME_MSEL int,END_TIME double,END_TIME_MSEL int,PROT_CATEGORY int,PROT_TYPE int,L7_CARRIER_PROT int,SUB_PROT_TYPE int,MSISDN string,IMSI string,IMEI string,ENCRYPT_VERSION int,ROAMING_TYPE int,ROAM_DIRECTION int,MS_IP string,SERVER_IP string,MS_PORT int,APN string,SGSN_SIG_IP string,GGSN_USER_IP string,SGSN_USER_IP string,MCC string,MNC string,RAT int,LAC string,RAC string,SAC string,CI string,SERVER_DECIMAL decimal,BROWSER_TIMESTAMP timestamp,TCP_CONN_STATES int,GGSN_SIG_IP_BigInt_NEGATIVE bigint,TCP_STATES_BIGINTPOSITIVE bigint,dummy_41 string,TCP_WIN_SIZE int,dummy_43 string,TCP_MSS int,dummy_45 string,TCP_CONN_TIMES int,dummy_47 string,TCP_CONN_2_FAILED_TIMES int,dummy_49 string,TCP_CONN_3_FAILED_TIMES int,HOST string,STREAMING_URL string,dummy_53 string,GET_STREAMING_FAILED_CODE int,dummy_55 string,GET_STREAMING_FLAG int,dummy_57 string,GET_NUM int,dummy_59 string,GET_SUCCEED_NUM int,dummy_61 string,GET_RETRANS_NUM int,dummy_63 string,GET_TIMEOUT_NUM int,INTBUFFER_FST_FLAG int,INTBUFFER_FULL_FLAG int,STALL_NUM int,dummy_68 string,VIDEO_FRAME_RATE int,dummy_70 string,VIDEO_CODEC_ID string,dummy_72 string,VIDEO_WIDTH int,dummy_74 string,VIDEO_HEIGHT int,dummy_76 string,AUDIO_CODEC_ID string,dummy_78 string,MEDIA_FILE_TYPE int,dummy_80 string,PLAY_STATE int,dummy_82 string,PLAY_STATE_1 int,dummy_84 string,STREAMING_FLAG int,dummy_86 string,TCP_STATUS_INDICATOR int,dummy_88 string,DISCONNECTION_FLAG int,dummy_90 string,FAILURE_CODE int,FLAG int,TAC string,ECI string,dummy_95 string,TCP_SYN_TIME_MSEL int,dummy_97 string,TCP_FST_SYN_DIRECTION int,RAN_NE_USER_IP string,HOMEMCC string,HOMEMNC string,dummy_102 string,CHARGE_FLAG int,dummy_104 string,PREPAID_FLAG int,dummy_106 string,USER_AGENT string,dummy_108 string,MS_WIN_STAT_TOTAL_NUM int,dummy_110 string,MS_WIN_STAT_SMALL_NUM int,dummy_112 string,MS_ACK_TO_1STGET_DELAY int,dummy_114 string,SERVER_ACK_TO_1STDATA_DELAY int,dummy_116 string,STREAMING_TYPE int,dummy_118 string,SOURCE_VIDEO_QUALITY int,TETHERING_FLAG int,CARRIER_ID double,LAYER1ID int,LAYER2ID int,dummy_124 string,LAYER3ID int,dummy_126 string,LAYER4ID int,dummy_128 string,LAYER5ID int,dummy_130 string,LAYER6ID int,CHARGING_RULE_BASE_NAME string,SP string,dummy_134 string,EXTENDED_URL string,SV string,FIRST_SAI_CGI_ECGI string,dummy_138 string,EXTENDED_URL_OTHER string,SIGNALING_USE_FLAG int,dummy_141 string,DNS_RETRANS_NUM int,dummy_143 string,DNS_FAIL_CODE int,FIRST_RAT int,FIRST_RAT_1 int,MS_INDICATOR string,LAST_SAI_CGI_ECGI string,LAST_RAT int,dummy_150 string,FIRST_LONGITUDE double,dummy_152 string,FIRST_LATITUDE double,dummy_154 string,FIRST_ALTITUDE int,dummy_156 string,FIRST_RASTERLONGITUDE double,dummy_158 string,FIRST_RASTERLATITUDE double,dummy_160 string,FIRST_RASTERALTITUDE int,dummy_162 string,FIRST_FREQUENCYSPOT int,dummy_164 string,FIRST_CLUTTER int,dummy_166 string,FIRST_USERBEHAVIOR int,dummy_168 string,FIRST_SPEED int,dummy_170 string,FIRST_CREDIBILITY int,dummy_172 string,LAST_LONGITUDE double,dummy_174 string,LAST_LATITUDE double,dummy_176 string,LAST_ALTITUDE int,dummy_178 string,LAST_RASTERLONGITUDE double,dummy_180 string,LAST_RASTERLATITUDE double,dummy_182 string,LAST_RASTERALTITUDE int,dummy_184 string,LAST_FREQUENCYSPOT int,dummy_186 string,LAST_CLUTTER int,dummy_188 string,LAST_USERBEHAVIOR int,dummy_190 string,LAST_SPEED int,dummy_192 string,LAST_CREDIBILITY int,dummy_194 string,IMEI_CIPHERTEXT string,APP_ID int,dummy_197 string,DOMAIN_NAME string,dummy_199 string,STREAMING_CACHE_IP string,dummy_201 string,STOP_LONGER_THAN_MIN_THRESHOLD int,dummy_203 string,STOP_LONGER_THAN_MAX_THRESHOLD int,dummy_205 string,PLAY_END_STAT int,dummy_207 string,STOP_START_TIME1 double,dummy_209 string,STOP_END_TIME1 double,dummy_211 string,STOP_START_TIME2 double,dummy_213 string,STOP_END_TIME2 double,dummy_215 string,STOP_START_TIME3 double,dummy_217 string,STOP_END_TIME3 double,dummy_219 string,STOP_START_TIME4 double,dummy_221 string,STOP_END_TIME4 double,dummy_223 string,STOP_START_TIME5 double,dummy_225 string,STOP_END_TIME5 double,dummy_227 string,STOP_START_TIME6 double,dummy_229 string,STOP_END_TIME6 double,dummy_231 string,STOP_START_TIME7 double,dummy_233 string,STOP_END_TIME7 double,dummy_235 string,STOP_START_TIME8 double,dummy_237 string,STOP_END_TIME8 double,dummy_239 string,STOP_START_TIME9 double,dummy_241 string,STOP_END_TIME9 double,dummy_243 string,STOP_START_TIME10 double,dummy_245 string,STOP_END_TIME10 double,dummy_247 string,FAIL_CLASS double,RECORD_TYPE double,dummy_250 string,NODATA_COUNT double,dummy_252 string,VIDEO_NODATA_DURATION double,dummy_254 string,VIDEO_SMOOTH_DURATION double,dummy_256 string,VIDEO_SD_DURATION double,dummy_258 string,VIDEO_HD_DURATION double,dummy_260 string,VIDEO_UHD_DURATION double,dummy_262 string,VIDEO_FHD_DURATION double,dummy_264 string,FLUCTUATION double,dummy_266 string,START_DOWNLOAD_THROUGHPUT double,dummy_268 string,L7_UL_GOODPUT_FULL_MSS double,dummy_270 string,SESSIONKEY string,dummy_272 string,FIRST_UCELLID double,dummy_274 string,LAST_UCELLID double,dummy_276 string,UCELLID1 double,dummy_278 string,LONGITUDE1 double,dummy_280 string,LATITUDE1 double,dummy_282 string,UCELLID2 double,dummy_284 string,LONGITUDE2 double,dummy_286 string,LATITUDE2 double,dummy_288 string,UCELLID3 double,dummy_290 string,LONGITUDE3 double,dummy_292 string,LATITUDE3 double,dummy_294 string,UCELLID4 double,dummy_296 string,LONGITUDE4 double,dummy_2101 string,LATITUDE4 double,dummy_300 string,UCELLID5 double,dummy_302 string,LONGITUDE5 double,dummy_304 string,LATITUDE5 double,dummy_306 string,UCELLID6 double,dummy_308 string,LONGITUDE6 double,dummy_310 string,LATITUDE6 double,dummy_312 string,UCELLID7 double,dummy_314 string,LONGITUDE7 double,dummy_316 string,LATITUDE7 double,dummy_318 string,UCELLID8 double,dummy_320 string,LONGITUDE8 double,dummy_322 string,LATITUDE8 double,dummy_324 string,UCELLID9 double,dummy_326 string,LONGITUDE9 double,dummy_328 string,LATITUDE9 double,dummy_330 string,UCELLID10 double,dummy_332 string,LONGITUDE10 double,dummy_334 string,LATITUDE10 double,dummy_336 string,INTBUFFER_FULL_DELAY double,dummy_338 string,STALL_DURATION double,dummy_340 string,STREAMING_DW_PACKETS double,dummy_342 string,STREAMING_DOWNLOAD_DELAY double,dummy_344 string,PLAY_DURATION double,dummy_346 string,STREAMING_QUALITY int,dummy_348 string,VIDEO_DATA_RATE double,dummy_350 string,AUDIO_DATA_RATE double,dummy_352 string,STREAMING_FILESIZE double,dummy_354 string,STREAMING_DURATIOIN double,dummy_356 string,TCP_SYN_TIME double,dummy_358 string,TCP_RTT_STEP1 double,CHARGE_ID double,dummy_361 string,UL_REVERSE_TO_DL_DELAY double,dummy_363 string,DL_REVERSE_TO_UL_DELAY double,dummy_365 string,DATATRANS_DW_GOODPUT double,dummy_367 string,DATATRANS_DW_TOTAL_DURATION double,dummy_369 string,SUM_FRAGMENT_INTERVAL double,dummy_371 string,TCP_FIN_TIMES double,dummy_373 string,TCP_RESET_TIMES double,dummy_375 string,URL_CLASSIFICATION double,dummy_377 string,STREAMING_LQ_DURATIOIN double,dummy_379 string,MAX_DNS_DELAY double,dummy_381 string,MAX_DNS2SYN double,dummy_383 string,MAX_LATANCY_OF_LINK_SETUP double,dummy_385 string,MAX_SYNACK2FIRSTACK double,dummy_387 string,MAX_SYNACK2LASTACK double,dummy_389 string,MAX_ACK2GET_DELAY double,dummy_391 string,MAX_FRAG_INTERVAL_PREDELAY double,dummy_393 string,SUM_FRAG_INTERVAL_PREDELAY double,dummy_395 string,SERVICE_DELAY_MSEC double,dummy_397 string,HOMEPROVINCE double,dummy_399 string,HOMECITY double,dummy_401 string,SERVICE_ID double,dummy_403 string,CHARGING_CLASS double,dummy_405 string,DATATRANS_UL_DURATION double,dummy_407 string,ASSOCIATED_ID double,dummy_409 string,PACKET_LOSS_NUM double,dummy_411 string,JITTER double,dummy_413 string,MS_DNS_DELAY_MSEL double,dummy_415 string,GET_STREAMING_DELAY double,dummy_417 string,TCP_UL_RETRANS_WITHOUTPL double,dummy_419 string,TCP_DW_RETRANS_WITHOUTPL double,dummy_421 string,GET_MAX_UL_SIZE double,dummy_423 string,GET_MIN_UL_SIZE double,dummy_425 string,GET_MAX_DL_SIZE double,dummy_427 string,GET_MIN_DL_SIZE double,dummy_429 string,FLOW_SAMPLE_RATIO int,dummy_431 string,UL_RTT_LONG_NUM int,dummy_433 string,DW_RTT_LONG_NUM int,dummy_435 string,UL_RTT_STAT_NUM int,dummy_437 string,DW_RTT_STAT_NUM int,dummy_439 string,USER_PROBE_UL_LOST_PKT int,dummy_441 string,SERVER_PROBE_UL_LOST_PKT int,dummy_443 string,SERVER_PROBE_DW_LOST_PKT int,dummy_445 string,USER_PROBE_DW_LOST_PKT int,dummy_447 string,AVG_DW_RTT_MICRO_SEC int,dummy_449 string,AVG_UL_RTT_MICRO_SEC int,dummy_451 string,RAN_NE_ID double,dummy_453 string,AVG_UL_RTT double,dummy_455 string,AVG_DW_RTT double,dummy_457 string,CHARGING_CHARACTERISTICS double,dummy_459 string,DL_SERIOUS_OUT_OF_ORDER_NUM double,dummy_461 string,DL_SLIGHT_OUT_OF_ORDER_NUM double,dummy_463 string,DL_FLIGHT_TOTAL_SIZE double,dummy_465 string,DL_FLIGHT_TOTAL_NUM double,dummy_467 string,DL_MAX_FLIGHT_SIZE double,dummy_469 string,VALID_TRANS_DURATION double,dummy_471 string,AIR_PORT_DURATION double,dummy_473 string,RADIO_CONN_TIMES double,dummy_475 string,UL_SERIOUS_OUT_OF_ORDER_NUM double,dummy_477 string,UL_SLIGHT_OUT_OF_ORDER_NUM double,dummy_479 string,UL_FLIGHT_TOTAL_SIZE double,dummy_481 string,UL_FLIGHT_TOTAL_NUM double,dummy_483 string,UL_MAX_FLIGHT_SIZE double,dummy_485 string,USER_DL_SLIGHT_OUT_OF_ORDER_PACKETS double,dummy_487 string,SERVER_UL_SLIGHT_OUT_OF_ORDER_PACKETS double,dummy_489 string,DL_CONTINUOUS_RETRANSMISSION_DELAY double,dummy_491 string,USER_HUNGRY_DELAY double,dummy_493 string,SERVER_HUNGRY_DELAY double,dummy_495 string,UPPERLAYER_IP_UL_FRAGMENTS double,dummy_497 string,UPPERLAYER_IP_DL_FRAGMENTS double,dummy_499 string,DOWNLAYER_IP_UL_FRAGMENTS double,dummy_501 string,DOWNLAYER_IP_DL_FRAGMENTS double,dummy_503 string,UPPERLAYER_IP_UL_PACKETS double,dummy_505 string,UPPERLAYER_IP_DL_PACKETS double,dummy_507 string,DOWNLAYER_IP_UL_PACKETS double,dummy_509 string,DOWNLAYER_IP_DL_PACKETS double,dummy_511 string,TCP_UL_PACKAGES_WITHPL double,dummy_513 string,TCP_DW_PACKAGES_WITHPL double,dummy_515 string,TCP_UL_PACKAGES_WITHOUTPL double,dummy_517 string,TCP_DW_PACKAGES_WITHOUTPL double,dummy_519 string,TCP_UL_RETRANS_WITHPL double,dummy_521 string,TCP_DW_RETRANS_WITHPL double,L4_UL_THROUGHPUT double,L4_DW_THROUGHPUT double,L4_UL_GOODPUT double,L4_DW_GOODPUT double,NETWORK_UL_TRAFFIC double,NETWORK_DL_TRAFFIC double,L4_UL_PACKETS double,L4_DW_PACKETS double,TCP_RTT double,TCP_UL_OUTOFSEQU double,TCP_DW_OUTOFSEQU double,TCP_UL_RETRANS double,TCP_DW_RETRANS double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect


  }


  //C20_SEQ_Dataload-01
  test("C20_SEQ_Dataload-01", Include) {

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/SEQ500/seq_500Records.csv' into table smart_500_DE options('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='SID,PROBEID,INTERFACEID,GROUPID,GGSN_ID,SGSN_ID,dummy,SESSION_INDICATOR,BEGIN_TIME,BEGIN_TIME_MSEL,END_TIME,END_TIME_MSEL,PROT_CATEGORY,PROT_TYPE,L7_CARRIER_PROT,SUB_PROT_TYPE,MSISDN,IMSI,IMEI,ENCRYPT_VERSION,ROAMING_TYPE,ROAM_DIRECTION,MS_IP,SERVER_IP,MS_PORT,APN,SGSN_SIG_IP,GGSN_USER_IP,SGSN_USER_IP,MCC,MNC,RAT,LAC,RAC,SAC,CI,SERVER_DECIMAL,BROWSER_TIMESTAMP,TCP_CONN_STATES,GGSN_SIG_IP_BigInt_NEGATIVE,TCP_STATES_BIGINTPOSITIVE,dummy,TCP_WIN_SIZE,dummy,TCP_MSS,dummy,TCP_CONN_TIMES,dummy,TCP_CONN_2_FAILED_TIMES,dummy,TCP_CONN_3_FAILED_TIMES,HOST,STREAMING_URL,dummy,GET_STREAMING_FAILED_CODE,dummy,GET_STREAMING_FLAG,dummy,GET_NUM,dummy,GET_SUCCEED_NUM,dummy,GET_RETRANS_NUM,dummy,GET_TIMEOUT_NUM,INTBUFFER_FST_FLAG,INTBUFFER_FULL_FLAG,STALL_NUM,dummy,VIDEO_FRAME_RATE,dummy,VIDEO_CODEC_ID,dummy,VIDEO_WIDTH,dummy,VIDEO_HEIGHT,dummy,AUDIO_CODEC_ID,dummy,MEDIA_FILE_TYPE,dummy,PLAY_STATE,dummy,PLAY_STATE,dummy,STREAMING_FLAG,dummy,TCP_STATUS_INDICATOR,dummy,DISCONNECTION_FLAG,dummy,FAILURE_CODE,FLAG,TAC,ECI,dummy,TCP_SYN_TIME_MSEL,dummy,TCP_FST_SYN_DIRECTION,RAN_NE_USER_IP,HOMEMCC,HOMEMNC,dummy,CHARGE_FLAG,dummy,PREPAID_FLAG,dummy,USER_AGENT,dummy,MS_WIN_STAT_TOTAL_NUM,dummy,MS_WIN_STAT_SMALL_NUM,dummy,MS_ACK_TO_1STGET_DELAY,dummy,SERVER_ACK_TO_1STDATA_DELAY,dummy,STREAMING_TYPE,dummy,SOURCE_VIDEO_QUALITY,TETHERING_FLAG,CARRIER_ID,LAYER1ID,LAYER2ID,dummy,LAYER3ID,dummy,LAYER4ID,dummy,LAYER5ID,dummy,LAYER6ID,CHARGING_RULE_BASE_NAME,SP,dummy,EXTENDED_URL,SV,FIRST_SAI_CGI_ECGI,dummy,EXTENDED_URL_OTHER,SIGNALING_USE_FLAG,dummy,DNS_RETRANS_NUM,dummy,DNS_FAIL_CODE,FIRST_RAT,FIRST_RAT,MS_INDICATOR,LAST_SAI_CGI_ECGI,LAST_RAT,dummy,FIRST_LONGITUDE,dummy,FIRST_LATITUDE,dummy,FIRST_ALTITUDE,dummy,FIRST_RASTERLONGITUDE,dummy,FIRST_RASTERLATITUDE,dummy,FIRST_RASTERALTITUDE,dummy,FIRST_FREQUENCYSPOT,dummy,FIRST_CLUTTER,dummy,FIRST_USERBEHAVIOR,dummy,FIRST_SPEED,dummy,FIRST_CREDIBILITY,dummy,LAST_LONGITUDE,dummy,LAST_LATITUDE,dummy,LAST_ALTITUDE,dummy,LAST_RASTERLONGITUDE,dummy,LAST_RASTERLATITUDE,dummy,LAST_RASTERALTITUDE,dummy,LAST_FREQUENCYSPOT,dummy,LAST_CLUTTER,dummy,LAST_USERBEHAVIOR,dummy,LAST_SPEED,dummy,LAST_CREDIBILITY,dummy,IMEI_CIPHERTEXT,APP_ID,dummy,DOMAIN_NAME,dummy,STREAMING_CACHE_IP,dummy,STOP_LONGER_THAN_MIN_THRESHOLD,dummy,STOP_LONGER_THAN_MAX_THRESHOLD,dummy,PLAY_END_STAT,dummy,STOP_START_TIME1,dummy,STOP_END_TIME1,dummy,STOP_START_TIME2,dummy,STOP_END_TIME2,dummy,STOP_START_TIME3,dummy,STOP_END_TIME3,dummy,STOP_START_TIME4,dummy,STOP_END_TIME4,dummy,STOP_START_TIME5,dummy,STOP_END_TIME5,dummy,STOP_START_TIME6,dummy,STOP_END_TIME6,dummy,STOP_START_TIME7,dummy,STOP_END_TIME7,dummy,STOP_START_TIME8,dummy,STOP_END_TIME8,dummy,STOP_START_TIME9,dummy,STOP_END_TIME9,dummy,STOP_START_TIME10,dummy,STOP_END_TIME10,dummy,FAIL_CLASS,RECORD_TYPE,dummy,NODATA_COUNT,dummy,VIDEO_NODATA_DURATION,dummy,VIDEO_SMOOTH_DURATION,dummy,VIDEO_SD_DURATION,dummy,VIDEO_HD_DURATION,dummy,VIDEO_UHD_DURATION,dummy,VIDEO_FHD_DURATION,dummy,FLUCTUATION,dummy,START_DOWNLOAD_THROUGHPUT,dummy,L7_UL_GOODPUT_FULL_MSS,dummy,SESSIONKEY,dummy,FIRST_UCELLID,dummy,LAST_UCELLID,dummy,UCELLID1,dummy,LONGITUDE1,dummy,LATITUDE1,dummy,UCELLID2,dummy,LONGITUDE2,dummy,LATITUDE2,dummy,UCELLID3,dummy,LONGITUDE3,dummy,LATITUDE3,dummy,UCELLID4,dummy,LONGITUDE4,dummy,LATITUDE4,dummy,UCELLID5,dummy,LONGITUDE5,dummy,LATITUDE5,dummy,UCELLID6,dummy,LONGITUDE6,dummy,LATITUDE6,dummy,UCELLID7,dummy,LONGITUDE7,dummy,LATITUDE7,dummy,UCELLID8,dummy,LONGITUDE8,dummy,LATITUDE8,dummy,UCELLID9,dummy,LONGITUDE9,dummy,LATITUDE9,dummy,UCELLID10,dummy,LONGITUDE10,dummy,LATITUDE10,dummy,INTBUFFER_FULL_DELAY,dummy,STALL_DURATION,dummy,STREAMING_DW_PACKETS,dummy,STREAMING_DOWNLOAD_DELAY,dummy,PLAY_DURATION,dummy,STREAMING_QUALITY,dummy,VIDEO_DATA_RATE,dummy,AUDIO_DATA_RATE,dummy,STREAMING_FILESIZE,dummy,STREAMING_DURATIOIN,dummy,TCP_SYN_TIME,dummy,TCP_RTT_STEP1,CHARGE_ID,dummy,UL_REVERSE_TO_DL_DELAY,dummy,DL_REVERSE_TO_UL_DELAY,dummy,DATATRANS_DW_GOODPUT,dummy,DATATRANS_DW_TOTAL_DURATION,dummy,SUM_FRAGMENT_INTERVAL,dummy,TCP_FIN_TIMES,dummy,TCP_RESET_TIMES,dummy,URL_CLASSIFICATION,dummy,STREAMING_LQ_DURATIOIN,dummy,MAX_DNS_DELAY,dummy,MAX_DNS2SYN,dummy,MAX_LATANCY_OF_LINK_SETUP,dummy,MAX_SYNACK2FIRSTACK,dummy,MAX_SYNACK2LASTACK,dummy,MAX_ACK2GET_DELAY,dummy,MAX_FRAG_INTERVAL_PREDELAY,dummy,SUM_FRAG_INTERVAL_PREDELAY,dummy,SERVICE_DELAY_MSEC,dummy,HOMEPROVINCE,dummy,HOMECITY,dummy,SERVICE_ID,dummy,CHARGING_CLASS,dummy,DATATRANS_UL_DURATION,dummy,ASSOCIATED_ID,dummy,PACKET_LOSS_NUM,dummy,JITTER,dummy,MS_DNS_DELAY_MSEL,dummy,GET_STREAMING_DELAY,dummy,TCP_UL_RETRANS_WITHOUTPL,dummy,TCP_DW_RETRANS_WITHOUTPL,dummy,GET_MAX_UL_SIZE,dummy,GET_MIN_UL_SIZE,dummy,GET_MAX_DL_SIZE,dummy,GET_MIN_DL_SIZE,dummy,FLOW_SAMPLE_RATIO,dummy,UL_RTT_LONG_NUM,dummy,DW_RTT_LONG_NUM,dummy,UL_RTT_STAT_NUM,dummy,DW_RTT_STAT_NUM,dummy,USER_PROBE_UL_LOST_PKT,dummy,SERVER_PROBE_UL_LOST_PKT,dummy,SERVER_PROBE_DW_LOST_PKT,dummy,USER_PROBE_DW_LOST_PKT,dummy,AVG_DW_RTT_MICRO_SEC,dummy,AVG_UL_RTT_MICRO_SEC,dummy,RAN_NE_ID,dummy,AVG_UL_RTT,dummy,AVG_DW_RTT,dummy,CHARGING_CHARACTERISTICS,dummy,DL_SERIOUS_OUT_OF_ORDER_NUM,dummy,DL_SLIGHT_OUT_OF_ORDER_NUM,dummy,DL_FLIGHT_TOTAL_SIZE,dummy,DL_FLIGHT_TOTAL_NUM,dummy,DL_MAX_FLIGHT_SIZE,dummy,VALID_TRANS_DURATION,dummy,AIR_PORT_DURATION,dummy,RADIO_CONN_TIMES,dummy,UL_SERIOUS_OUT_OF_ORDER_NUM,dummy,UL_SLIGHT_OUT_OF_ORDER_NUM,dummy,UL_FLIGHT_TOTAL_SIZE,dummy,UL_FLIGHT_TOTAL_NUM,dummy,UL_MAX_FLIGHT_SIZE,dummy,USER_DL_SLIGHT_OUT_OF_ORDER_PACKETS,dummy,SERVER_UL_SLIGHT_OUT_OF_ORDER_PACKETS,dummy,DL_CONTINUOUS_RETRANSMISSION_DELAY,dummy,USER_HUNGRY_DELAY,dummy,SERVER_HUNGRY_DELAY,dummy,UPPERLAYER_IP_UL_FRAGMENTS,dummy,UPPERLAYER_IP_DL_FRAGMENTS,dummy,DOWNLAYER_IP_UL_FRAGMENTS,dummy,DOWNLAYER_IP_DL_FRAGMENTS,dummy,UPPERLAYER_IP_UL_PACKETS,dummy,UPPERLAYER_IP_DL_PACKETS,dummy,DOWNLAYER_IP_UL_PACKETS,dummy,DOWNLAYER_IP_DL_PACKETS,dummy,TCP_UL_PACKAGES_WITHPL,dummy,TCP_DW_PACKAGES_WITHPL,dummy,TCP_UL_PACKAGES_WITHOUTPL,dummy,TCP_DW_PACKAGES_WITHOUTPL,dummy,TCP_UL_RETRANS_WITHPL,dummy,TCP_DW_RETRANS_WITHPL,L4_UL_THROUGHPUT,L4_DW_THROUGHPUT,L4_UL_GOODPUT,L4_DW_GOODPUT,NETWORK_UL_TRAFFIC,NETWORK_DL_TRAFFIC,L4_UL_PACKETS,L4_DW_PACKETS,TCP_RTT,TCP_UL_OUTOFSEQU,TCP_DW_OUTOFSEQU,TCP_UL_RETRANS,TCP_DW_RETRANS')""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/SEQ500/seq_500Records.csv' into table smart_500_DE_hive """).collect


  }


  //C20_DICTIONARY_EXCLUDE_TC001
  test("C20_DICTIONARY_EXCLUDE_TC001", Include) {

    checkAnswer(s"""select SID, IMEI from smart_500_DE where HOST not in ('www.hua735435.com')""",
      s"""select SID, IMEI from smart_500_DE_hive where HOST not in ('www.hua735435.com')""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC001")

  }


  //C20_DICTIONARY_EXCLUDE_TC002
  test("C20_DICTIONARY_EXCLUDE_TC002", Include) {

    checkAnswer(s"""select SID, IMEI from smart_500_DE where HOST in  ('www.hua735435.com')""",
      s"""select SID, IMEI from smart_500_DE_hive where HOST in  ('www.hua735435.com')""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC002")

  }


  //C20_DICTIONARY_EXCLUDE_TC003
  test("C20_DICTIONARY_EXCLUDE_TC003", Include) {

    checkAnswer(s"""select SID, IMEI from smart_500_DE where HOST LIKE  'www.hua735435.com'""",
      s"""select SID, IMEI from smart_500_DE_hive where HOST LIKE  'www.hua735435.com'""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC003")

  }


  //C20_DICTIONARY_EXCLUDE_TC004
  test("C20_DICTIONARY_EXCLUDE_TC004", Include) {

    checkAnswer(s"""select SID, IMEI from smart_500_DE where HOST Not LIKE  'www.hua735435.com'""",
      s"""select SID, IMEI from smart_500_DE_hive where HOST Not LIKE  'www.hua735435.com'""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC004")

  }


  //C20_DICTIONARY_EXCLUDE_TC005
  test("C20_DICTIONARY_EXCLUDE_TC005", Include) {

    checkAnswer(s"""select length(HOST) from smart_500_DE where HOST in ('www.hua735435.com')""",
      s"""select length(HOST) from smart_500_DE_hive where HOST in ('www.hua735435.com')""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC005")

  }


  //C20_DICTIONARY_EXCLUDE_TC006
  test("C20_DICTIONARY_EXCLUDE_TC006", Include) {

    checkAnswer(s"""select avg(HOST),avg(LAYER1ID) from smart_500_DE where HOST in ('www.hua735435.com')""",
      s"""select avg(HOST),avg(LAYER1ID) from smart_500_DE_hive where HOST in ('www.hua735435.com')""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC006")

  }


  //C20_DICTIONARY_EXCLUDE_TC007
  test("C20_DICTIONARY_EXCLUDE_TC007", Include) {

    checkAnswer(s"""select avg(HOST),avg(LAYER1ID) from smart_500_DE where HOST not in ('www.hua735435.com')""",
      s"""select avg(HOST),avg(LAYER1ID) from smart_500_DE_hive where HOST not in ('www.hua735435.com')""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC007")

  }


  //C20_DICTIONARY_EXCLUDE_TC008
  test("C20_DICTIONARY_EXCLUDE_TC008", Include) {

    checkAnswer(s"""select substring(IMEI,1,4) from smart_500_DE where HOST in ('www.hua735435.com')""",
      s"""select substring(IMEI,1,4) from smart_500_DE_hive where HOST in ('www.hua735435.com')""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC008")

  }


  //C20_DICTIONARY_EXCLUDE_TC009
  test("C20_DICTIONARY_EXCLUDE_TC009", Include) {

    checkAnswer(s"""select length(HOST)+10 from smart_500_DE where HOST in ('www.hua735435.com')""",
      s"""select length(HOST)+10 from smart_500_DE_hive where HOST in ('www.hua735435.com')""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC009")

  }


  //C20_DICTIONARY_EXCLUDE_TC010
  test("C20_DICTIONARY_EXCLUDE_TC010", Include) {

    checkAnswer(s"""select length(HOST)-10 from smart_500_DE where HOST in ('www.hua735435.com')""",
      s"""select length(HOST)-10 from smart_500_DE_hive where HOST in ('www.hua735435.com')""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC010")

  }


  //C20_DICTIONARY_EXCLUDE_TC011
  test("C20_DICTIONARY_EXCLUDE_TC011", Include) {

    checkAnswer(s"""select length(HOST)/10 from smart_500_DE where HOST in ('www.hua735435.com')""",
      s"""select length(HOST)/10 from smart_500_DE_hive where HOST in ('www.hua735435.com')""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC011")

  }


  //C20_DICTIONARY_EXCLUDE_TC012
  test("C20_DICTIONARY_EXCLUDE_TC012", Include) {

    checkAnswer(s"""select length(HOST)*10 from smart_500_DE where HOST in ('www.hua735435.com')""",
      s"""select length(HOST)*10 from smart_500_DE_hive where HOST in ('www.hua735435.com')""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC012")

  }


  //C20_DICTIONARY_EXCLUDE_TC013
  test("C20_DICTIONARY_EXCLUDE_TC013", Include) {

    checkAnswer(s"""select lower(MS_IP),sum(LAYER1ID) from smart_500_DE  group by lower(MS_IP)""",
      s"""select lower(MS_IP),sum(LAYER1ID) from smart_500_DE_hive  group by lower(MS_IP)""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC013")

  }


  //C20_DICTIONARY_EXCLUDE_TC014
  test("C20_DICTIONARY_EXCLUDE_TC014", Include) {

    checkAnswer(s"""select * from smart_500_DE  where unix_timestamp(MS_IP)=1420268400""",
      s"""select * from smart_500_DE_hive  where unix_timestamp(MS_IP)=1420268400""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC014")

  }


  //C20_DICTIONARY_EXCLUDE_TC015
  test("C20_DICTIONARY_EXCLUDE_TC015", Include) {

    checkAnswer(s"""select * from smart_500_DE  where to_date(MS_IP)='2015-01-07'""",
      s"""select * from smart_500_DE_hive  where to_date(MS_IP)='2015-01-07'""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC015")

  }


  //C20_DICTIONARY_EXCLUDE_TC016
  test("C20_DICTIONARY_EXCLUDE_TC016", Include) {

    checkAnswer(s"""select * from smart_500_DE  where datediff(MS_IP,'2014-12-01')>=35""",
      s"""select * from smart_500_DE_hive  where datediff(MS_IP,'2014-12-01')>=35""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC016")

  }


  //C20_DICTIONARY_EXCLUDE_TC017
  test("C20_DICTIONARY_EXCLUDE_TC017", Include) {

    checkAnswer(s"""select MS_IP,count(*) from smart_500_DE  group by MS_IP""",
      s"""select MS_IP,count(*) from smart_500_DE_hive  group by MS_IP""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC017")

  }


  //C20_DICTIONARY_EXCLUDE_TC018
  test("C20_DICTIONARY_EXCLUDE_TC018", Include) {

    sql(s"""select MS_IP,SID,count(*) from smart_500_DE  group by MS_IP,SID order by MS_IP limit 10""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC019
  test("C20_DICTIONARY_EXCLUDE_TC019", Include) {

    sql(s"""select SID,length( MSISDN),avg(LAYER1ID),avg(TCP_DW_RETRANS) from smart_500_DE  group by SID,length( MSISDN) order by SID limit 10""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC020
  test("C20_DICTIONARY_EXCLUDE_TC020", Include) {

    sql(s"""select SID,length( MSISDN),max(LAYER1ID),min(LAYER1ID) from smart_500_DE  group by SID,length( MSISDN) order by SID limit 10""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC021
  test("C20_DICTIONARY_EXCLUDE_TC021", Include) {

    sql(s"""select SID,length( MSISDN),max(LAYER1ID),max(LAYER1ID) from smart_500_DE  group by SID,length( MSISDN) order by SID limit 10""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC022
  test("C20_DICTIONARY_EXCLUDE_TC022", Include) {

    sql(s"""select SID,length( MSISDN),min(LAYER1ID),min(LAYER1ID) from smart_500_DE  group by SID,length( MSISDN) order by SID limit 10""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC023
  test("C20_DICTIONARY_EXCLUDE_TC023", Include) {

    sql(s"""select SID,length( MSISDN),max(LAYER1ID),min(LAYER1ID),avg(LAYER1ID) from smart_500_DE  group by SID,length( MSISDN) order by SID limit 10""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC024
  test("C20_DICTIONARY_EXCLUDE_TC024", Include) {

    checkAnswer(s"""select concat(upper(MSISDN),1),sum(LAYER1ID) from smart_500_DE  group by concat(upper(MSISDN),1)""",
      s"""select concat(upper(MSISDN),1),sum(LAYER1ID) from smart_500_DE_hive  group by concat(upper(MSISDN),1)""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC024")

  }


  //C20_DICTIONARY_EXCLUDE_TC025
  test("C20_DICTIONARY_EXCLUDE_TC025", Include) {

    checkAnswer(s"""select upper(substring(MSISDN,1,4)),sum(LAYER1ID) from smart_500_DE group by upper(substring(MSISDN,1,4)) """,
      s"""select upper(substring(MSISDN,1,4)),sum(LAYER1ID) from smart_500_DE_hive group by upper(substring(MSISDN,1,4)) """, "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC025")

  }


  //C20_DICTIONARY_EXCLUDE_TC026
  test("C20_DICTIONARY_EXCLUDE_TC026", Include) {

    checkAnswer(s"""select max(SERVER_IP) from smart_500_DE""",
      s"""select max(SERVER_IP) from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC026")

  }


  //C20_DICTIONARY_EXCLUDE_TC027
  test("C20_DICTIONARY_EXCLUDE_TC027", Include) {

    checkAnswer(s"""select max(SERVER_IP+10) from smart_500_DE""",
      s"""select max(SERVER_IP+10) from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC027")

  }


  //C20_DICTIONARY_EXCLUDE_TC028
  test("C20_DICTIONARY_EXCLUDE_TC028", Include) {

    checkAnswer(s"""select max(MSISDN) from smart_500_DE""",
      s"""select max(MSISDN) from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC028")

  }


  //C20_DICTIONARY_EXCLUDE_TC029
  test("C20_DICTIONARY_EXCLUDE_TC029", Include) {

    checkAnswer(s"""select max(MSISDN+10) from smart_500_DE""",
      s"""select max(MSISDN+10) from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC029")

  }


  //C20_DICTIONARY_EXCLUDE_TC030
  test("C20_DICTIONARY_EXCLUDE_TC030", Include) {

    checkAnswer(s"""select avg(TCP_DW_RETRANS) from smart_500_DE""",
      s"""select avg(TCP_DW_RETRANS) from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC030")

  }


  //C20_DICTIONARY_EXCLUDE_TC031
  test("C20_DICTIONARY_EXCLUDE_TC031", Include) {

    checkAnswer(s"""select avg(TCP_DW_RETRANS+10) from smart_500_DE""",
      s"""select avg(TCP_DW_RETRANS+10) from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC031")

  }


  //C20_DICTIONARY_EXCLUDE_TC032
  test("C20_DICTIONARY_EXCLUDE_TC032", Include) {

    checkAnswer(s"""select avg(TCP_DW_RETRANS-10) from smart_500_DE""",
      s"""select avg(TCP_DW_RETRANS-10) from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC032")

  }


  //C20_DICTIONARY_EXCLUDE_TC033
  test("C20_DICTIONARY_EXCLUDE_TC033", Include) {

    checkAnswer(s"""select count(TCP_DW_RETRANS)-10 from smart_500_DE""",
      s"""select count(TCP_DW_RETRANS)-10 from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC033")

  }


  //C20_DICTIONARY_EXCLUDE_TC034
  test("C20_DICTIONARY_EXCLUDE_TC034", Include) {

    checkAnswer(s"""select count(TCP_DW_RETRANS)-10 from smart_500_DE""",
      s"""select count(TCP_DW_RETRANS)-10 from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC034")

  }


  //C20_DICTIONARY_EXCLUDE_TC035
  test("C20_DICTIONARY_EXCLUDE_TC035", Include) {

    checkAnswer(s"""select count(TCP_DW_RETRANS)-10 from smart_500_DE""",
      s"""select count(TCP_DW_RETRANS)-10 from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC035")

  }


  //C20_DICTIONARY_EXCLUDE_TC036
  test("C20_DICTIONARY_EXCLUDE_TC036", Include) {

    checkAnswer(s"""select sum(MSISDN), sum(DISTINCT MSISDN) from smart_500_DE""",
      s"""select sum(MSISDN), sum(DISTINCT MSISDN) from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC036")

  }


  //C20_DICTIONARY_EXCLUDE_TC037
  test("C20_DICTIONARY_EXCLUDE_TC037", Include) {

    checkAnswer(s"""select count (if(TCP_DW_RETRANS>100,NULL,TCP_DW_RETRANS))  a from smart_500_DE""",
      s"""select count (if(TCP_DW_RETRANS>100,NULL,TCP_DW_RETRANS))  a from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC037")

  }


  //C20_DICTIONARY_EXCLUDE_TC038
  test("C20_DICTIONARY_EXCLUDE_TC038", Include) {

    checkAnswer(s"""select count (if(TCP_DW_RETRANS<100,NULL,TCP_DW_RETRANS))  a from smart_500_DE""",
      s"""select count (if(TCP_DW_RETRANS<100,NULL,TCP_DW_RETRANS))  a from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC038")

  }


  //C20_DICTIONARY_EXCLUDE_TC039
  test("C20_DICTIONARY_EXCLUDE_TC039", Include) {

    checkAnswer(s"""select count (if(TCP_DW_RETRANS=100,NULL,TCP_DW_RETRANS))  a from smart_500_DE""",
      s"""select count (if(TCP_DW_RETRANS=100,NULL,TCP_DW_RETRANS))  a from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC039")

  }


  //C20_DICTIONARY_EXCLUDE_TC040
  test("C20_DICTIONARY_EXCLUDE_TC040", Include) {

    checkAnswer(s"""select count(TCP_DW_RETRANS) from smart_500_DE where TCP_DW_RETRANS=100""",
      s"""select count(TCP_DW_RETRANS) from smart_500_DE_hive where TCP_DW_RETRANS=100""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC040")

  }


  //C20_DICTIONARY_EXCLUDE_TC041
  test("C20_DICTIONARY_EXCLUDE_TC041", Include) {

    checkAnswer(s"""select count(TCP_DW_RETRANS) from smart_500_DE where TCP_DW_RETRANS<100""",
      s"""select count(TCP_DW_RETRANS) from smart_500_DE_hive where TCP_DW_RETRANS<100""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC041")

  }


  //C20_DICTIONARY_EXCLUDE_TC042
  test("C20_DICTIONARY_EXCLUDE_TC042", Include) {

    checkAnswer(s"""select count(TCP_DW_RETRANS) from smart_500_DE where TCP_DW_RETRANS>100""",
      s"""select count(TCP_DW_RETRANS) from smart_500_DE_hive where TCP_DW_RETRANS>100""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC042")

  }


  //C20_DICTIONARY_EXCLUDE_TC043
  test("C20_DICTIONARY_EXCLUDE_TC043", Include) {

    sql(s"""select MSISDN, TCP_DW_RETRANS + LAYER1ID as a  from smart_500_DE order by MSISDN limit 10""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC044
  test("C20_DICTIONARY_EXCLUDE_TC044", Include) {

    sql(s"""select MSISDN, sum(TCP_DW_RETRANS + 10) Total from smart_500_DE group by  MSISDN order by Total limit 10""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC045
  test("C20_DICTIONARY_EXCLUDE_TC045", Include) {

    checkAnswer(s"""select MSISDN, min(LAYER1ID + 10)  Total from smart_500_DE group by  MSISDN order by MSISDN""",
      s"""select MSISDN, min(LAYER1ID + 10)  Total from smart_500_DE_hive group by  MSISDN order by MSISDN""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC045")

  }


  //C20_DICTIONARY_EXCLUDE_TC046
  test("C20_DICTIONARY_EXCLUDE_TC046", Include) {

    checkAnswer(s"""select avg (if(LAYER1ID>100,NULL,LAYER1ID))  a from smart_500_DE""",
      s"""select avg (if(LAYER1ID>100,NULL,LAYER1ID))  a from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC046")

  }


  //C20_DICTIONARY_EXCLUDE_TC047
  test("C20_DICTIONARY_EXCLUDE_TC047", Include) {

    checkAnswer(s"""select avg (if(TCP_DW_RETRANS>100,NULL,TCP_DW_RETRANS))  a from smart_500_DE""",
      s"""select avg (if(TCP_DW_RETRANS>100,NULL,TCP_DW_RETRANS))  a from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC047")

  }


  //C20_DICTIONARY_EXCLUDE_TC048
  ignore("C20_DICTIONARY_EXCLUDE_TC048", Include) {

    checkAnswer(s"""select variance(LAYER1ID) as a   from smart_500_DE""",
      s"""select variance(LAYER1ID) as a   from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC048")

  }


  //C20_DICTIONARY_EXCLUDE_TC049
  ignore("C20_DICTIONARY_EXCLUDE_TC049", Include) {

    checkAnswer(s"""select var_pop(LAYER1ID)  as a from smart_500_DE""",
      s"""select var_pop(LAYER1ID)  as a from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC049")

  }


  //C20_DICTIONARY_EXCLUDE_TC050
  ignore("C20_DICTIONARY_EXCLUDE_TC050", Include) {

    checkAnswer(s"""select var_samp(LAYER1ID) as a  from smart_500_DE""",
      s"""select var_samp(LAYER1ID) as a  from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC050")

  }


  //C20_DICTIONARY_EXCLUDE_TC051
  ignore("C20_DICTIONARY_EXCLUDE_TC051", Include) {

    checkAnswer(s"""select stddev_pop(LAYER1ID) as a  from smart_500_DE""",
      s"""select stddev_pop(LAYER1ID) as a  from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC051")

  }


  //C20_DICTIONARY_EXCLUDE_TC052
  ignore("C20_DICTIONARY_EXCLUDE_TC052", Include) {

    checkAnswer(s"""select stddev_samp(LAYER1ID)  as a from smart_500_DE""",
      s"""select stddev_samp(LAYER1ID)  as a from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC052")

  }


  //C20_DICTIONARY_EXCLUDE_TC053
  ignore("C20_DICTIONARY_EXCLUDE_TC053", Include) {

    checkAnswer(s"""select covar_pop(LAYER1ID,LAYER1ID) as a  from smart_500_DE""",
      s"""select covar_pop(LAYER1ID,LAYER1ID) as a  from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC053")

  }


  //C20_DICTIONARY_EXCLUDE_TC054
  ignore("C20_DICTIONARY_EXCLUDE_TC054", Include) {

    checkAnswer(s"""select covar_samp(LAYER1ID,LAYER1ID) as a  from smart_500_DE""",
      s"""select covar_samp(LAYER1ID,LAYER1ID) as a  from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC054")

  }


  //C20_DICTIONARY_EXCLUDE_TC055
  test("C20_DICTIONARY_EXCLUDE_TC055", Include) {

    checkAnswer(s"""select corr(LAYER1ID,LAYER1ID)  as a from smart_500_DE""",
      s"""select corr(LAYER1ID,LAYER1ID)  as a from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC055")

  }


  //C20_DICTIONARY_EXCLUDE_TC056
  test("C20_DICTIONARY_EXCLUDE_TC056", Include) {

    checkAnswer(s"""select percentile(LAYER1ID,0.2) as  a  from smart_500_DE""",
      s"""select percentile(LAYER1ID,0.2) as  a  from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC056")

  }


  //C20_DICTIONARY_EXCLUDE_TC057
  test("C20_DICTIONARY_EXCLUDE_TC057", Include) {

    checkAnswer(s"""select percentile(LAYER1ID,array(0,0.2,0.3,1))  as  a from smart_500_DE""",
      s"""select percentile(LAYER1ID,array(0,0.2,0.3,1))  as  a from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC057")

  }


  //C20_DICTIONARY_EXCLUDE_TC058
  test("C20_DICTIONARY_EXCLUDE_TC058", Include) {

    sql(s"""select percentile_approx(LAYER1ID,0.2) as a  from (select LAYER1ID from smart_500_DE order by LAYER1ID) t""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC059
  test("C20_DICTIONARY_EXCLUDE_TC059", Include) {

    sql(s"""select percentile_approx(LAYER1ID,0.2,5) as a  from (select LAYER1ID from smart_500_DE order by LAYER1ID) t""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC060
  test("C20_DICTIONARY_EXCLUDE_TC060", Include) {

    sql(s"""select percentile_approx(LAYER1ID,array(0.2,0.3,0.99))  as a from (select LAYER1ID from smart_500_DE order by LAYER1ID) t""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC061
  test("C20_DICTIONARY_EXCLUDE_TC061", Include) {

    sql(s"""select percentile_approx(LAYER1ID,array(0.2,0.3,0.99),5) as a from (select LAYER1ID from smart_500_DE order by LAYER1ID) t""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC062
  test("C20_DICTIONARY_EXCLUDE_TC062", Include) {

    sql(s"""select histogram_numeric(LAYER1ID,2)  as a from (select LAYER1ID from smart_500_DE order by LAYER1ID) t""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC063
  ignore("C20_DICTIONARY_EXCLUDE_TC063", Include) {

    checkAnswer(s"""select variance(TCP_DW_RETRANS) as a from (select * from smart_500_DE order by TCP_DW_RETRANS) t""",
      s"""select variance(TCP_DW_RETRANS) as a from (select * from smart_500_DE_hive order by TCP_DW_RETRANS) t""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC063")

  }


  //C20_DICTIONARY_EXCLUDE_TC064
  ignore("C20_DICTIONARY_EXCLUDE_TC064", Include) {

    checkAnswer(s"""select var_pop(TCP_DW_RETRANS)  as a from smart_500_DE""",
      s"""select var_pop(TCP_DW_RETRANS)  as a from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC064")

  }


  //C20_DICTIONARY_EXCLUDE_TC065
  ignore("C20_DICTIONARY_EXCLUDE_TC065", Include) {

    checkAnswer(s"""select var_samp(TCP_DW_RETRANS) as a  from smart_500_DE""",
      s"""select var_samp(TCP_DW_RETRANS) as a  from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC065")

  }


  //C20_DICTIONARY_EXCLUDE_TC066
  ignore("C20_DICTIONARY_EXCLUDE_TC066", Include) {

    checkAnswer(s"""select stddev_pop(TCP_DW_RETRANS) as a  from smart_500_DE""",
      s"""select stddev_pop(TCP_DW_RETRANS) as a  from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC066")

  }


  //C20_DICTIONARY_EXCLUDE_TC067
  ignore("C20_DICTIONARY_EXCLUDE_TC067", Include) {

    checkAnswer(s"""select stddev_samp(TCP_DW_RETRANS)  as a from smart_500_DE""",
      s"""select stddev_samp(TCP_DW_RETRANS)  as a from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC067")

  }


  //C20_DICTIONARY_EXCLUDE_TC068
  ignore("C20_DICTIONARY_EXCLUDE_TC068", Include) {

    checkAnswer(s"""select covar_pop(TCP_DW_RETRANS,TCP_DW_RETRANS) as a  from smart_500_DE""",
      s"""select covar_pop(TCP_DW_RETRANS,TCP_DW_RETRANS) as a  from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC068")

  }


  //C20_DICTIONARY_EXCLUDE_TC069
  ignore("C20_DICTIONARY_EXCLUDE_TC069", Include) {

    checkAnswer(s"""select covar_samp(TCP_DW_RETRANS,TCP_DW_RETRANS) as a  from smart_500_DE""",
      s"""select covar_samp(TCP_DW_RETRANS,TCP_DW_RETRANS) as a  from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC069")

  }


  //C20_DICTIONARY_EXCLUDE_TC070
  test("C20_DICTIONARY_EXCLUDE_TC070", Include) {

    checkAnswer(s"""select corr(TCP_DW_RETRANS,TCP_DW_RETRANS)  as a from smart_500_DE""",
      s"""select corr(TCP_DW_RETRANS,TCP_DW_RETRANS)  as a from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC070")

  }


  //C20_DICTIONARY_EXCLUDE_TC073
  test("C20_DICTIONARY_EXCLUDE_TC073", Include) {

    sql(s"""select percentile_approx(TCP_DW_RETRANS,0.2) as a  from (select TCP_DW_RETRANS from smart_500_DE order by TCP_DW_RETRANS) t""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC074
  test("C20_DICTIONARY_EXCLUDE_TC074", Include) {

    sql(s"""select percentile_approx(TCP_DW_RETRANS,0.2,5) as a  from (select TCP_DW_RETRANS from smart_500_DE order by TCP_DW_RETRANS) t""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC075
  test("C20_DICTIONARY_EXCLUDE_TC075", Include) {

    sql(s"""select percentile_approx(TCP_DW_RETRANS,array(0.2,0.3,0.99))  as a from (select TCP_DW_RETRANS from smart_500_DE order by TCP_DW_RETRANS) t""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC076
  test("C20_DICTIONARY_EXCLUDE_TC076", Include) {

    sql(s"""select percentile_approx(TCP_DW_RETRANS,array(0.2,0.3,0.99),5) as a from (select TCP_DW_RETRANS from smart_500_DE order by TCP_DW_RETRANS) t""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC077
  test("C20_DICTIONARY_EXCLUDE_TC077", Include) {

    sql(s"""select histogram_numeric(TCP_DW_RETRANS,2)  as a from (select TCP_DW_RETRANS from smart_500_DE order by TCP_DW_RETRANS) t""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC094
  test("C20_DICTIONARY_EXCLUDE_TC094", Include) {

    sql(s"""select Upper(streaming_url) a ,host from smart_500_DE order by host limit 10""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC095
  test("C20_DICTIONARY_EXCLUDE_TC095", Include) {

    sql(s"""select Lower(streaming_url) a  from smart_500_DE order by host limit 10""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC096
  test("C20_DICTIONARY_EXCLUDE_TC096", Include) {

    sql(s"""select streaming_url as b,LAYER1ID as a from smart_500_DE  order by a,b asc limit 10""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC097
  test("C20_DICTIONARY_EXCLUDE_TC097", Include) {

    sql(s"""select streaming_url as b,TCP_DW_RETRANS as a from smart_500_DE  order by a,b desc limit 10""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC098
  test("C20_DICTIONARY_EXCLUDE_TC098", Include) {

    checkAnswer(s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE where streaming_url ='www.hua1/xyz'""",
      s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE_hive where streaming_url ='www.hua1/xyz'""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC098")

  }


  //C20_DICTIONARY_EXCLUDE_TC099
  test("C20_DICTIONARY_EXCLUDE_TC099", Include) {

    checkAnswer(s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE where streaming_url ='www.hua90/xyz' and TCP_DW_RETRANS ='82.0' limit 10""",
      s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE_hive where streaming_url ='www.hua90/xyz' and TCP_DW_RETRANS ='82.0' limit 10""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC099")

  }


  //C20_DICTIONARY_EXCLUDE_TC100
  test("C20_DICTIONARY_EXCLUDE_TC100", Include) {

    checkAnswer(s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE where streaming_url ='www.hua1/xyz' or  TCP_DW_RETRANS ='82.0'""",
      s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE_hive where streaming_url ='www.hua1/xyz' or  TCP_DW_RETRANS ='82.0'""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC100")

  }


  //C20_DICTIONARY_EXCLUDE_TC101
  test("C20_DICTIONARY_EXCLUDE_TC101", Include) {

    checkAnswer(s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE where streaming_url !='www.hua1/xyz'  order by LAYER1ID,TCP_DW_RETRANS,streaming_url""",
      s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE_hive where streaming_url !='www.hua1/xyz'  order by LAYER1ID,TCP_DW_RETRANS,streaming_url""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC101")

  }


  //C20_DICTIONARY_EXCLUDE_TC102
  test("C20_DICTIONARY_EXCLUDE_TC102", Include) {

    checkAnswer(s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE where streaming_url !='www.hua1/xyz' and TCP_DW_RETRANS !='152.0'  order by LAYER1ID,TCP_DW_RETRANS,streaming_url limit 10""",
      s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE_hive where streaming_url !='www.hua1/xyz' and TCP_DW_RETRANS !='152.0'  order by LAYER1ID,TCP_DW_RETRANS,streaming_url limit 10""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC102")

  }


  //C20_DICTIONARY_EXCLUDE_TC103
  test("C20_DICTIONARY_EXCLUDE_TC103", Include) {

    checkAnswer(s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE where TCP_DW_RETRANS >2.0 order by LAYER1ID,TCP_DW_RETRANS,streaming_url limit 10""",
      s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE_hive where TCP_DW_RETRANS >2.0 order by LAYER1ID,TCP_DW_RETRANS,streaming_url limit 10""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC103")

  }


  //C20_DICTIONARY_EXCLUDE_TC104
  test("C20_DICTIONARY_EXCLUDE_TC104", Include) {

    sql(s"""select LAYER1ID as a from smart_500_DE where LAYER1ID<=>LAYER1ID order by LAYER1ID desc limit 10""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC105
  test("C20_DICTIONARY_EXCLUDE_TC105", Include) {

    sql(s"""SELECT LAYER1ID,TCP_DW_RETRANS,streaming_url FROM (select * from smart_500_DE) SUB_QRY ORDER BY LAYER1ID,TCP_DW_RETRANS,streaming_url ASC limit 10""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC106
  test("C20_DICTIONARY_EXCLUDE_TC106", Include) {

    sql(s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE where ( LAYER1ID+1) == 101 order by TCP_DW_RETRANS,LAYER1ID limit 5""").collect

  }


  //C20_DICTIONARY_EXCLUDE_TC107
  test("C20_DICTIONARY_EXCLUDE_TC107", Include) {

    checkAnswer(s"""select  LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE where  streaming_url is  null order by LAYER1ID,TCP_DW_RETRANS,streaming_url                                """,
      s"""select  LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE_hive where  streaming_url is  null order by LAYER1ID,TCP_DW_RETRANS,streaming_url                                """, "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC107")

  }


  //C20_DICTIONARY_EXCLUDE_TC108
  test("C20_DICTIONARY_EXCLUDE_TC108", Include) {

    checkAnswer(s"""select  LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE where  streaming_url is  not null order by LAYER1ID,TCP_DW_RETRANS,streaming_url""",
      s"""select  LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE_hive where  streaming_url is  not null order by LAYER1ID,TCP_DW_RETRANS,streaming_url""", "QueriesExcludeDictionaryTestCase_C20_DICTIONARY_EXCLUDE_TC108")

  }


  //PushUP_FILTER_smart_500_DE_TC001
  test("PushUP_FILTER_smart_500_DE_TC001", Include) {

    checkAnswer(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN=17846415579 or  IMSI=460075195040377 or BEGIN_TIME=1.463483694712E12""",
      s"""select BEGIN_TIME,END_TIME from smart_500_DE_hive where MSISDN=17846415579 or  IMSI=460075195040377 or BEGIN_TIME=1.463483694712E12""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC001")

  }


  //PushUP_FILTER_smart_500_DE_TC002
  test("PushUP_FILTER_smart_500_DE_TC002", Include) {

    checkAnswer(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN=17846602163 and  BEGIN_TIME=1.463483694712E12 and SERVER_IP='192.26.210.204'""",
      s"""select BEGIN_TIME,END_TIME from smart_500_DE_hive where MSISDN=17846602163 and  BEGIN_TIME=1.463483694712E12 and SERVER_IP='192.26.210.204'""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC002")

  }


  //PushUP_FILTER_smart_500_DE_TC003
  test("PushUP_FILTER_smart_500_DE_TC003", Include) {

    checkAnswer(s"""select BEGIN_TIME,END_TIME from smart_500_DE where (MSISDN==17846602163) and  (BEGIN_TIME=1.463483694712E12) and (SERVER_IP=='192.26.210.204')""",
      s"""select BEGIN_TIME,END_TIME from smart_500_DE_hive where (MSISDN==17846602163) and  (BEGIN_TIME=1.463483694712E12) and (SERVER_IP=='192.26.210.204')""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC003")

  }


  //PushUP_FILTER_smart_500_DE_TC004
  test("PushUP_FILTER_smart_500_DE_TC004", Include) {

    checkAnswer(s"""select BEGIN_TIME,END_TIME from smart_500_DE where (MSISDN==17846415579) or  (IMSI==460075195040377) or (BEGIN_TIME==1.463483694712E12)""",
      s"""select BEGIN_TIME,END_TIME from smart_500_DE_hive where (MSISDN==17846415579) or  (IMSI==460075195040377) or (BEGIN_TIME==1.463483694712E12)""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC004")

  }


  //PushUP_FILTER_smart_500_DE_TC005
  test("PushUP_FILTER_smart_500_DE_TC005", Include) {

    checkAnswer(s"""select BEGIN_TIME,END_TIME from smart_500_DE where (MSISDN==17846602163) and  (BEGIN_TIME==1.463483694712E12) or (SERVER_IP=='192.26.210.204')""",
      s"""select BEGIN_TIME,END_TIME from smart_500_DE_hive where (MSISDN==17846602163) and  (BEGIN_TIME==1.463483694712E12) or (SERVER_IP=='192.26.210.204')""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC005")

  }


  //PushUP_FILTER_smart_500_DE_TC006
  test("PushUP_FILTER_smart_500_DE_TC006", Include) {

    checkAnswer(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN!=17846602163 and  BEGIN_TIME!=1.463483694712E12 or SERVER_IP!='192.26.210.204'""",
      s"""select BEGIN_TIME,END_TIME from smart_500_DE_hive where MSISDN!=17846602163 and  BEGIN_TIME!=1.463483694712E12 or SERVER_IP!='192.26.210.204'""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC006")

  }


  //PushUP_FILTER_smart_500_DE_TC007
  test("PushUP_FILTER_smart_500_DE_TC007", Include) {

    checkAnswer(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN!=17846602163 or  BEGIN_TIME!=1.463483694712E12 or SERVER_IP!='192.26.210.204'""",
      s"""select BEGIN_TIME,END_TIME from smart_500_DE_hive where MSISDN!=17846602163 or  BEGIN_TIME!=1.463483694712E12 or SERVER_IP!='192.26.210.204'""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC007")

  }


  //PushUP_FILTER_smart_500_DE_TC008
  test("PushUP_FILTER_smart_500_DE_TC008", Include) {

    checkAnswer(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN!=17846602163 and  BEGIN_TIME!=1.463483694712E12 or SERVER_IP!='192.26.210.204'""",
      s"""select BEGIN_TIME,END_TIME from smart_500_DE_hive where MSISDN!=17846602163 and  BEGIN_TIME!=1.463483694712E12 or SERVER_IP!='192.26.210.204'""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC008")

  }


  //PushUP_FILTER_smart_500_DE_TC009
  test("PushUP_FILTER_smart_500_DE_TC009", Include) {

    checkAnswer(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN IS NOT NULL and  BEGIN_TIME IS NOT NULL and SERVER_IP IS NOT NULL""",
      s"""select BEGIN_TIME,END_TIME from smart_500_DE_hive where MSISDN IS NOT NULL and  BEGIN_TIME IS NOT NULL and SERVER_IP IS NOT NULL""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC009")

  }


  //PushUP_FILTER_smart_500_DE_TC010
  test("PushUP_FILTER_smart_500_DE_TC010", Include) {

    checkAnswer(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN IS NOT NULL or  BEGIN_TIME IS NOT NULL or SERVER_IP IS NOT NULL""",
      s"""select BEGIN_TIME,END_TIME from smart_500_DE_hive where MSISDN IS NOT NULL or  BEGIN_TIME IS NOT NULL or SERVER_IP IS NOT NULL""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC010")

  }


  //PushUP_FILTER_smart_500_DE_TC011
  test("PushUP_FILTER_smart_500_DE_TC011", Include) {

    checkAnswer(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN NOT IN (17846415579,17846415580) and  IMSI NOT IN (460075195040377,460075195040378) or BEGIN_TIME NOT IN (1.463483694712E12,1.463483694712E13)""",
      s"""select BEGIN_TIME,END_TIME from smart_500_DE_hive where MSISDN NOT IN (17846415579,17846415580) and  IMSI NOT IN (460075195040377,460075195040378) or BEGIN_TIME NOT IN (1.463483694712E12,1.463483694712E13)""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC011")

  }


  //PushUP_FILTER_smart_500_DE_TC012
  test("PushUP_FILTER_smart_500_DE_TC012", Include) {

    checkAnswer(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN IN (17846415579,17846415580) and  IMSI IN (460075195040377,460075195040378) or BEGIN_TIME IN (1.463483694712E12,1.463483694712E13)""",
      s"""select BEGIN_TIME,END_TIME from smart_500_DE_hive where MSISDN IN (17846415579,17846415580) and  IMSI IN (460075195040377,460075195040378) or BEGIN_TIME IN (1.463483694712E12,1.463483694712E13)""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC012")

  }


  //PushUP_FILTER_smart_500_DE_TC013
  test("PushUP_FILTER_smart_500_DE_TC013", Include) {

    checkAnswer(s"""select MSISDN+0.1000001,IMSI+9999999,BEGIN_TIME+9.999999 from smart_500_DE""",
      s"""select MSISDN+0.1000001,IMSI+9999999,BEGIN_TIME+9.999999 from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC013")

  }


  //PushUP_FILTER_smart_500_DE_TC014
  test("PushUP_FILTER_smart_500_DE_TC014", Include) {

    checkAnswer(s"""select MSISDN-0.1000001,IMSI-9999999,BEGIN_TIME-9.999999 from smart_500_DE""",
      s"""select MSISDN-0.1000001,IMSI-9999999,BEGIN_TIME-9.999999 from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC014")

  }


  //PushUP_FILTER_smart_500_DE_TC015
  test("PushUP_FILTER_smart_500_DE_TC015", Include) {

    checkAnswer(s"""select MSISDN*0.1000001,IMSI*9999999,BEGIN_TIME*9.999999 from smart_500_DE""",
      s"""select MSISDN*0.1000001,IMSI*9999999,BEGIN_TIME*9.999999 from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC015")

  }


  //PushUP_FILTER_smart_500_DE_TC016
  test("PushUP_FILTER_smart_500_DE_TC016", Include) {

    checkAnswer(s"""select MSISDN/0.1000001,IMSI/9999999,BEGIN_TIME/9.999999 from smart_500_DE""",
      s"""select MSISDN/0.1000001,IMSI/9999999,BEGIN_TIME/9.999999 from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC016")

  }


  //PushUP_FILTER_smart_500_DE_TC017
  test("PushUP_FILTER_smart_500_DE_TC017", Include) {

    checkAnswer(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN>17846602163 and  BEGIN_TIME>1.463483694712E12 or SERVER_IP!='192.26.210.204'""",
      s"""select BEGIN_TIME,END_TIME from smart_500_DE_hive where MSISDN>17846602163 and  BEGIN_TIME>1.463483694712E12 or SERVER_IP!='192.26.210.204'""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC017")

  }


  //PushUP_FILTER_smart_500_DE_TC018
  test("PushUP_FILTER_smart_500_DE_TC018", Include) {

    checkAnswer(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN<17846602163 and  BEGIN_TIME<1.463483694712E12 or SERVER_IP!='192.26.210.204'""",
      s"""select BEGIN_TIME,END_TIME from smart_500_DE_hive where MSISDN<17846602163 and  BEGIN_TIME<1.463483694712E12 or SERVER_IP!='192.26.210.204'""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC018")

  }


  //PushUP_FILTER_smart_500_DE_TC019
  test("PushUP_FILTER_smart_500_DE_TC019", Include) {

    checkAnswer(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN>=17846602163 and  BEGIN_TIME>=1.463483694712E12 or SERVER_IP!='192.26.210.204'""",
      s"""select BEGIN_TIME,END_TIME from smart_500_DE_hive where MSISDN>=17846602163 and  BEGIN_TIME>=1.463483694712E12 or SERVER_IP!='192.26.210.204'""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC019")

  }


  //PushUP_FILTER_smart_500_DE_TC020
  test("PushUP_FILTER_smart_500_DE_TC020", Include) {

    checkAnswer(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN<=17846602163 and  BEGIN_TIME<=1.463483694712E12 or SERVER_IP!='192.26.210.204'""",
      s"""select BEGIN_TIME,END_TIME from smart_500_DE_hive where MSISDN<=17846602163 and  BEGIN_TIME<=1.463483694712E12 or SERVER_IP!='192.26.210.204'""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC020")

  }


  //PushUP_FILTER_smart_500_DE_TC021
  test("PushUP_FILTER_smart_500_DE_TC021", Include) {

    checkAnswer(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN like 17846415579 and  IMSI like 460075195040377 or BEGIN_TIME like 1.463483694712E12""",
      s"""select BEGIN_TIME,END_TIME from smart_500_DE_hive where MSISDN like 17846415579 and  IMSI like 460075195040377 or BEGIN_TIME like 1.463483694712E12""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC021")

  }


  //PushUP_FILTER_smart_500_DE_TC022
  test("PushUP_FILTER_smart_500_DE_TC022", Include) {

    checkAnswer(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN not like 17846415579 and  IMSI not like 460075195040377 or BEGIN_TIME not like 1.463483694712E12""",
      s"""select BEGIN_TIME,END_TIME from smart_500_DE_hive where MSISDN not like 17846415579 and  IMSI not like 460075195040377 or BEGIN_TIME not like 1.463483694712E12""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC022")

  }


  //PushUP_FILTER_smart_500_DE_TC023
  test("PushUP_FILTER_smart_500_DE_TC023", Include) {

    checkAnswer(s"""select BEGIN_TIME,SID from smart_500_DE where SID=4.61168620184322E14 or  IMSI=460075195040377 or BEGIN_TIME=1.463483694712E12""",
      s"""select BEGIN_TIME,SID from smart_500_DE_hive where SID=4.61168620184322E14 or  IMSI=460075195040377 or BEGIN_TIME=1.463483694712E12""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC023")

  }


  //PushUP_FILTER_smart_500_DE_TC024
  test("PushUP_FILTER_smart_500_DE_TC024", Include) {

    checkAnswer(s"""select BEGIN_TIME,SID from smart_500_DE where SID=4.61168620184322E14 and  IMSI=460075195040377 and BEGIN_TIME=1.463483694712E12""",
      s"""select BEGIN_TIME,SID from smart_500_DE_hive where SID=4.61168620184322E14 and  IMSI=460075195040377 and BEGIN_TIME=1.463483694712E12""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC024")

  }


  //PushUP_FILTER_smart_500_DE_TC025
  test("PushUP_FILTER_smart_500_DE_TC025", Include) {

    checkAnswer(s"""select BEGIN_TIME,SID from smart_500_DE where (SID==4.61168620184322E14) and  (IMSI==460075171072129) and (BEGIN_TIME==1.463483694712E12)""",
      s"""select BEGIN_TIME,SID from smart_500_DE_hive where (SID==4.61168620184322E14) and  (IMSI==460075171072129) and (BEGIN_TIME==1.463483694712E12)""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC025")

  }


  //PushUP_FILTER_smart_500_DE_TC026
  test("PushUP_FILTER_smart_500_DE_TC026", Include) {

    checkAnswer(s"""select BEGIN_TIME,SID from smart_500_DE where (SID==4.61168620184322E14) or  (IMSI==460075171072129) or (BEGIN_TIME==1.463483694712E12)""",
      s"""select BEGIN_TIME,SID from smart_500_DE_hive where (SID==4.61168620184322E14) or  (IMSI==460075171072129) or (BEGIN_TIME==1.463483694712E12)""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC026")

  }


  //PushUP_FILTER_smart_500_DE_TC027
  test("PushUP_FILTER_smart_500_DE_TC027", Include) {

    checkAnswer(s"""select BEGIN_TIME,SID from smart_500_DE where (SID==4.61168620184322E14) and  (IMSI==460075171072129) or (BEGIN_TIME==1.463483694712E12)""",
      s"""select BEGIN_TIME,SID from smart_500_DE_hive where (SID==4.61168620184322E14) and  (IMSI==460075171072129) or (BEGIN_TIME==1.463483694712E12)""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC027")

  }


  //PushUP_FILTER_smart_500_DE_TC028
  test("PushUP_FILTER_smart_500_DE_TC028", Include) {

    checkAnswer(s"""select BEGIN_TIME,SID from smart_500_DE where SID!=4.61168620184322E14 or  IMSI!=460075171072129 or BEGIN_TIME!=1.463483694712E12""",
      s"""select BEGIN_TIME,SID from smart_500_DE_hive where SID!=4.61168620184322E14 or  IMSI!=460075171072129 or BEGIN_TIME!=1.463483694712E12""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC028")

  }


  //PushUP_FILTER_smart_500_DE_TC029
  test("PushUP_FILTER_smart_500_DE_TC029", Include) {

    checkAnswer(s"""select BEGIN_TIME,SID from smart_500_DE where SID!=4.61168620184322E14 and  IMSI!=460075171072129 or BEGIN_TIME!=1.463483694712E12""",
      s"""select BEGIN_TIME,SID from smart_500_DE_hive where SID!=4.61168620184322E14 and  IMSI!=460075171072129 or BEGIN_TIME!=1.463483694712E12""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC029")

  }


  //PushUP_FILTER_smart_500_DE_TC030
  test("PushUP_FILTER_smart_500_DE_TC030", Include) {

    checkAnswer(s"""select BEGIN_TIME,SID from smart_500_DE where SID is NOT NULL and  IMSI is not null and BEGIN_TIME is not null""",
      s"""select BEGIN_TIME,SID from smart_500_DE_hive where SID is NOT NULL and  IMSI is not null and BEGIN_TIME is not null""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC030")

  }


  //PushUP_FILTER_smart_500_DE_TC031
  test("PushUP_FILTER_smart_500_DE_TC031", Include) {

    checkAnswer(s"""select BEGIN_TIME,SID from smart_500_DE where SID is NOT NULL or  IMSI is not null or BEGIN_TIME is not null""",
      s"""select BEGIN_TIME,SID from smart_500_DE_hive where SID is NOT NULL or  IMSI is not null or BEGIN_TIME is not null""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC031")

  }


  //PushUP_FILTER_smart_500_DE_TC032
  test("PushUP_FILTER_smart_500_DE_TC032", Include) {

    checkAnswer(s"""select BEGIN_TIME,SID from smart_500_DE where SID NOT IN (4.61168620184322E14,4.61168620184322E16) or  IMSI NOT IN (460075171072129,460075171072130) or BEGIN_TIME NOT IN (1.463483694712E12,1.463483694712E14)""",
      s"""select BEGIN_TIME,SID from smart_500_DE_hive where SID NOT IN (4.61168620184322E14,4.61168620184322E16) or  IMSI NOT IN (460075171072129,460075171072130) or BEGIN_TIME NOT IN (1.463483694712E12,1.463483694712E14)""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC032")

  }


  //PushUP_FILTER_smart_500_DE_TC033
  test("PushUP_FILTER_smart_500_DE_TC033", Include) {

    checkAnswer(s"""select BEGIN_TIME,SID from smart_500_DE where SID IN (4.61168620184322E14,4.61168620184322E16) or  IMSI IN (460075171072129,460075171072130) or BEGIN_TIME IN (1.463483694712E12,1.463483694712E14)""",
      s"""select BEGIN_TIME,SID from smart_500_DE_hive where SID IN (4.61168620184322E14,4.61168620184322E16) or  IMSI IN (460075171072129,460075171072130) or BEGIN_TIME IN (1.463483694712E12,1.463483694712E14)""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC033")

  }


  //PushUP_FILTER_smart_500_DE_TC034
  test("PushUP_FILTER_smart_500_DE_TC034", Include) {

    checkAnswer(s"""select BEGIN_TIME+2,SID+0.234,IMSI+99.99999999 from smart_500_DE""",
      s"""select BEGIN_TIME+2,SID+0.234,IMSI+99.99999999 from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC034")

  }


  //PushUP_FILTER_smart_500_DE_TC035
  test("PushUP_FILTER_smart_500_DE_TC035", Include) {

    checkAnswer(s"""select BEGIN_TIME-2,SID-0.234,IMSI-99.99999999 from smart_500_DE""",
      s"""select BEGIN_TIME-2,SID-0.234,IMSI-99.99999999 from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC035")

  }


  //PushUP_FILTER_smart_500_DE_TC036
  test("PushUP_FILTER_smart_500_DE_TC036", Include) {

    checkAnswer(s"""select BEGIN_TIME*2,SID*0.234,IMSI*99.99999999 from smart_500_DE""",
      s"""select BEGIN_TIME*2,SID*0.234,IMSI*99.99999999 from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC036")

  }


  //PushUP_FILTER_smart_500_DE_TC037
  test("PushUP_FILTER_smart_500_DE_TC037", Include) {

    checkAnswer(s"""select BEGIN_TIME/2,SID/0.234,IMSI/99.99999999 from smart_500_DE""",
      s"""select BEGIN_TIME/2,SID/0.234,IMSI/99.99999999 from smart_500_DE_hive""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC037")

  }


  //PushUP_FILTER_smart_500_DE_TC038
  test("PushUP_FILTER_smart_500_DE_TC038", Include) {

    checkAnswer(s"""select BEGIN_TIME,SID from smart_500_DE where SID like 4.61168620184322E14 and  IMSI like 460075171072129  or BEGIN_TIME like 1.463483694712E12""",
      s"""select BEGIN_TIME,SID from smart_500_DE_hive where SID like 4.61168620184322E14 and  IMSI like 460075171072129  or BEGIN_TIME like 1.463483694712E12""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC038")

  }


  //PushUP_FILTER_smart_500_DE_TC039
  test("PushUP_FILTER_smart_500_DE_TC039", Include) {

    checkAnswer(s"""select BEGIN_TIME,SID from smart_500_DE where SID not like 4.61168620184322E14 and  IMSI not like 460075171072129  or BEGIN_TIME not like 1.463483694712E12""",
      s"""select BEGIN_TIME,SID from smart_500_DE_hive where SID not like 4.61168620184322E14 and  IMSI not like 460075171072129  or BEGIN_TIME not like 1.463483694712E12""", "QueriesExcludeDictionaryTestCase_PushUP_FILTER_smart_500_DE_TC039")

  }

  override def afterAll {
  sql("drop table if exists TABLE_DICTIONARY_EXCLUDE")
  sql("drop table if exists TABLE_DICTIONARY_EXCLUDE1_hive")
  sql("drop table if exists TABLE_DICTIONARY_EXCLUDE1")
  sql("drop table if exists TABLE_DICTIONARY_EXCLUDE1_hive")
  sql("drop table if exists smart_500_DE")
  sql("drop table if exists smart_500_DE_hive")
  }
}