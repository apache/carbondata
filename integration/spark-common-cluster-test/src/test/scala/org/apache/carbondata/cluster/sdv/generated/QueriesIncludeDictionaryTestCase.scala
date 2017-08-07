
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
 * Test Class for QueriesIncludeDictionaryTestCase to verify all scenerios
 */

class QueriesIncludeDictionaryTestCase extends QueryTest with BeforeAndAfterAll {
         

  //VMALL_DICTIONARY_INCLUDE_CreateCube
  test("VMALL_DICTIONARY_INCLUDE_CreateCube", Include) {
    sql(s"""drop table if exists VMALL_DICTIONARY_INCLUDE""").collect
    sql(s"""drop table if exists VMALL_DICTIONARY_INCLUDE_hive""").collect

    sql(s"""create table  VMALL_DICTIONARY_INCLUDE (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt)  STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='imei,deviceInformationId,productionDate,gamePointId,Latest_DAY,contractNumber')
  """).collect

    sql(s"""create table  VMALL_DICTIONARY_INCLUDE_hive (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string,deliveryTime string,channelsId string,channelsName string,deliveryAreaId string,deliveryCountry string,deliveryProvince string,deliveryCity string,deliveryDistrict string,deliveryStreet string,oxSingleNumber string,contractNumber BigInt,ActiveCheckTime string,ActiveAreaId string,ActiveCountry string,ActiveProvince string,Activecity string,ActiveDistrict string,ActiveStreet string,ActiveOperatorId string,Active_releaseId string,Active_EMUIVersion string,Active_operaSysVersion string,Active_BacVerNumber string,Active_BacFlashVer string,Active_webUIVersion string,Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string,Active_operatorsVersion string,Active_phonePADPartitionedVersions string,Latest_YEAR int,Latest_MONTH int,Latest_DAY Decimal(30,10),Latest_HOUR string,Latest_areaId string,Latest_country string,Latest_province string,Latest_city string,Latest_district string,Latest_street string,Latest_releaseId string,Latest_EMUIVersion string,Latest_operaSysVersion string,Latest_BacVerNumber string,Latest_BacFlashVer string,Latest_webUIVersion string,Latest_webUITypeCarrVer string,Latest_webTypeDataVerNumber string,Latest_operatorsVersion string,Latest_phonePADPartitionedVersions string,Latest_operatorId string,gamePointId double,gamePointDescription string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect


  }


  //VMALL_DICTIONARY_INCLUDE_CreateCube_count
  test("VMALL_DICTIONARY_INCLUDE_CreateCube_count", Include) {

    sql(s"""select count(*) from VMALL_DICTIONARY_INCLUDE""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_DataLoad
  test("VMALL_DICTIONARY_INCLUDE_DataLoad", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table VMALL_DICTIONARY_INCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table VMALL_DICTIONARY_INCLUDE_hive """).collect


  }


  //VMALL_DICTIONARY_INCLUDE_001
  test("VMALL_DICTIONARY_INCLUDE_001", Include) {

    checkAnswer(s"""Select count(imei) from VMALL_DICTIONARY_INCLUDE""",
      s"""Select count(imei) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_001")

  }


  //VMALL_DICTIONARY_INCLUDE_002
  test("VMALL_DICTIONARY_INCLUDE_002", Include) {

    checkAnswer(s"""select count(DISTINCT imei) as a from VMALL_DICTIONARY_INCLUDE""",
      s"""select count(DISTINCT imei) as a from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_002")

  }


  //VMALL_DICTIONARY_INCLUDE_003
  test("VMALL_DICTIONARY_INCLUDE_003", Include) {

    checkAnswer(s"""select sum(Latest_month)+10 as a ,imei  from VMALL_DICTIONARY_INCLUDE group by imei order by imei""",
      s"""select sum(Latest_month)+10 as a ,imei  from VMALL_DICTIONARY_INCLUDE_hive group by imei order by imei""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_003")

  }


  //VMALL_DICTIONARY_INCLUDE_004
  test("VMALL_DICTIONARY_INCLUDE_004", Include) {

    checkAnswer(s"""select max(imei),min(imei) from VMALL_DICTIONARY_INCLUDE""",
      s"""select max(imei),min(imei) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_004")

  }


  //VMALL_DICTIONARY_INCLUDE_005
  test("VMALL_DICTIONARY_INCLUDE_005", Include) {

    checkAnswer(s"""select min(imei), max(imei) Total from VMALL_DICTIONARY_INCLUDE group by  channelsId order by Total""",
      s"""select min(imei), max(imei) Total from VMALL_DICTIONARY_INCLUDE_hive group by  channelsId order by Total""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_005")

  }


  //VMALL_DICTIONARY_INCLUDE_006
  test("VMALL_DICTIONARY_INCLUDE_006", Include) {

    checkAnswer(s"""select last(imei) a from VMALL_DICTIONARY_INCLUDE  group by imei order by imei limit 1""",
      s"""select last(imei) a from VMALL_DICTIONARY_INCLUDE_hive  group by imei order by imei limit 1""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_006")

  }


  //VMALL_DICTIONARY_INCLUDE_007
  test("VMALL_DICTIONARY_INCLUDE_007", Include) {

    sql(s"""select FIRST(imei) a from VMALL_DICTIONARY_INCLUDE group by imei order by imei limit 1""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_008
  test("VMALL_DICTIONARY_INCLUDE_008", Include) {

    checkAnswer(s"""select imei,count(imei) a from VMALL_DICTIONARY_INCLUDE group by imei order by imei""",
      s"""select imei,count(imei) a from VMALL_DICTIONARY_INCLUDE_hive group by imei order by imei""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_008")

  }


  //VMALL_DICTIONARY_INCLUDE_009
  test("VMALL_DICTIONARY_INCLUDE_009", Include) {

    checkAnswer(s"""select Lower(imei) a  from VMALL_DICTIONARY_INCLUDE order by imei""",
      s"""select Lower(imei) a  from VMALL_DICTIONARY_INCLUDE_hive order by imei""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_009")

  }


  //VMALL_DICTIONARY_INCLUDE_010
  test("VMALL_DICTIONARY_INCLUDE_010", Include) {

    checkAnswer(s"""select distinct imei from VMALL_DICTIONARY_INCLUDE order by imei""",
      s"""select distinct imei from VMALL_DICTIONARY_INCLUDE_hive order by imei""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_010")

  }


  //VMALL_DICTIONARY_INCLUDE_011
  test("VMALL_DICTIONARY_INCLUDE_011", Include) {

    checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE order by imei limit 101 """,
      s"""select imei from VMALL_DICTIONARY_INCLUDE_hive order by imei limit 101 """, "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_011")

  }


  //VMALL_DICTIONARY_INCLUDE_012
  test("VMALL_DICTIONARY_INCLUDE_012", Include) {

    checkAnswer(s"""select imei as a from VMALL_DICTIONARY_INCLUDE  order by a asc limit 10""",
      s"""select imei as a from VMALL_DICTIONARY_INCLUDE_hive  order by a asc limit 10""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_012")

  }


  //VMALL_DICTIONARY_INCLUDE_013
  test("VMALL_DICTIONARY_INCLUDE_013", Include) {

    checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where  (contractNumber == 9223372047700) and (imei=='1AA100004')""",
      s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where  (contractNumber == 9223372047700) and (imei=='1AA100004')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_013")

  }


  //VMALL_DICTIONARY_INCLUDE_014
  test("VMALL_DICTIONARY_INCLUDE_014", Include) {

    checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei !='1AA100064' order by imei""",
      s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei !='1AA100064' order by imei""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_014")

  }


  //VMALL_DICTIONARY_INCLUDE_015
  test("VMALL_DICTIONARY_INCLUDE_015", Include) {

    checkAnswer(s"""select imei  from VMALL_DICTIONARY_INCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select imei  from VMALL_DICTIONARY_INCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_015")

  }


  //VMALL_DICTIONARY_INCLUDE_016
  test("VMALL_DICTIONARY_INCLUDE_016", Include) {

    checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei !='1AA100012' order by imei""",
      s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei !='1AA100012' order by imei""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_016")

  }


  //VMALL_DICTIONARY_INCLUDE_017
  test("VMALL_DICTIONARY_INCLUDE_017", Include) {

    checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei >'1AA100012' order by imei""",
      s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei >'1AA100012' order by imei""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_017")

  }


  //VMALL_DICTIONARY_INCLUDE_018
  test("VMALL_DICTIONARY_INCLUDE_018", Include) {

    checkAnswer(s"""select imei  from VMALL_DICTIONARY_INCLUDE where imei<>imei""",
      s"""select imei  from VMALL_DICTIONARY_INCLUDE_hive where imei<>imei""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_018")

  }


  //VMALL_DICTIONARY_INCLUDE_019
  test("VMALL_DICTIONARY_INCLUDE_019", Include) {

    checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei != Latest_areaId order by imei""",
      s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei != Latest_areaId order by imei""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_019")

  }


  //VMALL_DICTIONARY_INCLUDE_020
  test("VMALL_DICTIONARY_INCLUDE_020", Include) {

    checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where Latest_areaId<imei order by imei""",
      s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where Latest_areaId<imei order by imei""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_020")

  }


  //VMALL_DICTIONARY_INCLUDE_021
  test("VMALL_DICTIONARY_INCLUDE_021", Include) {

    checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where Latest_DAY<=imei order by imei""",
      s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY<=imei order by imei""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_021")

  }


  //VMALL_DICTIONARY_INCLUDE_022
  test("VMALL_DICTIONARY_INCLUDE_022", Include) {

    checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei <'1AA10002' order by imei""",
      s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei <'1AA10002' order by imei""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_022")

  }


  //VMALL_DICTIONARY_INCLUDE_023
  test("VMALL_DICTIONARY_INCLUDE_023", Include) {

    checkAnswer(s"""select Latest_day  from VMALL_DICTIONARY_INCLUDE where imei IS NULL""",
      s"""select Latest_day  from VMALL_DICTIONARY_INCLUDE_hive where imei IS NULL""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_023")

  }


  //VMALL_DICTIONARY_INCLUDE_024
  test("VMALL_DICTIONARY_INCLUDE_024", Include) {

    checkAnswer(s"""select Latest_day  from VMALL_DICTIONARY_INCLUDE where imei IS NOT NULL order by Latest_day""",
      s"""select Latest_day  from VMALL_DICTIONARY_INCLUDE_hive where imei IS NOT NULL order by Latest_day""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_024")

  }


  //VMALL_DICTIONARY_INCLUDE_025
  test("VMALL_DICTIONARY_INCLUDE_025", Include) {

    checkAnswer(s"""Select count(imei),min(imei) from VMALL_DICTIONARY_INCLUDE """,
      s"""Select count(imei),min(imei) from VMALL_DICTIONARY_INCLUDE_hive """, "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_025")

  }


  //VMALL_DICTIONARY_INCLUDE_026
  test("VMALL_DICTIONARY_INCLUDE_026", Include) {

    checkAnswer(s"""select count(DISTINCT imei,latest_day) as a from VMALL_DICTIONARY_INCLUDE""",
      s"""select count(DISTINCT imei,latest_day) as a from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_026")

  }


  //VMALL_DICTIONARY_INCLUDE_027
  test("VMALL_DICTIONARY_INCLUDE_027", Include) {

    checkAnswer(s"""select max(imei),min(imei),count(imei) from VMALL_DICTIONARY_INCLUDE""",
      s"""select max(imei),min(imei),count(imei) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_027")

  }


  //VMALL_DICTIONARY_INCLUDE_028
  test("VMALL_DICTIONARY_INCLUDE_028", Include) {

    checkAnswer(s"""select sum(imei),avg(imei),count(imei) a  from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(imei),avg(imei),count(imei) a  from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_028")

  }


  //VMALL_DICTIONARY_INCLUDE_029
  test("VMALL_DICTIONARY_INCLUDE_029", Include) {

    sql(s"""select last(imei),Min(imei),max(imei)  a from (select imei from VMALL_DICTIONARY_INCLUDE order by imei) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_030
  test("VMALL_DICTIONARY_INCLUDE_030", Include) {

    sql(s"""select FIRST(imei),Last(imei) a from VMALL_DICTIONARY_INCLUDE group by imei order by imei limit 1""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_031
  test("VMALL_DICTIONARY_INCLUDE_031", Include) {

    checkAnswer(s"""select imei,count(imei) a from VMALL_DICTIONARY_INCLUDE group by imei order by imei""",
      s"""select imei,count(imei) a from VMALL_DICTIONARY_INCLUDE_hive group by imei order by imei""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_031")

  }


  //VMALL_DICTIONARY_INCLUDE_032
  test("VMALL_DICTIONARY_INCLUDE_032", Include) {

    checkAnswer(s"""select Lower(imei),upper(imei)  a  from VMALL_DICTIONARY_INCLUDE order by imei""",
      s"""select Lower(imei),upper(imei)  a  from VMALL_DICTIONARY_INCLUDE_hive order by imei""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_032")

  }


  //VMALL_DICTIONARY_INCLUDE_033
  test("VMALL_DICTIONARY_INCLUDE_033", Include) {

    checkAnswer(s"""select imei as a from VMALL_DICTIONARY_INCLUDE  order by a asc limit 10""",
      s"""select imei as a from VMALL_DICTIONARY_INCLUDE_hive  order by a asc limit 10""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_033")

  }


  //VMALL_DICTIONARY_INCLUDE_034
  test("VMALL_DICTIONARY_INCLUDE_034", Include) {

    checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where  (contractNumber == 9223372047700) and (imei=='1AA100012')""",
      s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where  (contractNumber == 9223372047700) and (imei=='1AA100012')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_034")

  }


  //VMALL_DICTIONARY_INCLUDE_035
  test("VMALL_DICTIONARY_INCLUDE_035", Include) {

    checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei !='8imei' order by imei""",
      s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei !='8imei' order by imei""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_035")

  }


  //VMALL_DICTIONARY_INCLUDE_036
  test("VMALL_DICTIONARY_INCLUDE_036", Include) {

    checkAnswer(s"""select imei  from VMALL_DICTIONARY_INCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select imei  from VMALL_DICTIONARY_INCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_036")

  }


  //VMALL_DICTIONARY_INCLUDE_037
  test("VMALL_DICTIONARY_INCLUDE_037", Include) {

    checkAnswer(s"""Select count(contractNumber) from VMALL_DICTIONARY_INCLUDE""",
      s"""Select count(contractNumber) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_037")

  }


  //VMALL_DICTIONARY_INCLUDE_038
  test("VMALL_DICTIONARY_INCLUDE_038", Include) {

    checkAnswer(s"""select count(DISTINCT contractNumber) as a from VMALL_DICTIONARY_INCLUDE""",
      s"""select count(DISTINCT contractNumber) as a from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_038")

  }


  //VMALL_DICTIONARY_INCLUDE_039
  test("VMALL_DICTIONARY_INCLUDE_039", Include) {

    checkAnswer(s"""select sum(contractNumber)+10 as a ,contractNumber  from VMALL_DICTIONARY_INCLUDE group by contractNumber""",
      s"""select sum(contractNumber)+10 as a ,contractNumber  from VMALL_DICTIONARY_INCLUDE_hive group by contractNumber""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_039")

  }


  //VMALL_DICTIONARY_INCLUDE_040
  test("VMALL_DICTIONARY_INCLUDE_040", Include) {

    checkAnswer(s"""select max(contractNumber),min(contractNumber) from VMALL_DICTIONARY_INCLUDE""",
      s"""select max(contractNumber),min(contractNumber) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_040")

  }


  //VMALL_DICTIONARY_INCLUDE_041
  test("VMALL_DICTIONARY_INCLUDE_041", Include) {

    checkAnswer(s"""select sum(contractNumber) a  from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(contractNumber) a  from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_041")

  }


  //VMALL_DICTIONARY_INCLUDE_042
  test("VMALL_DICTIONARY_INCLUDE_042", Include) {

    checkAnswer(s"""select avg(contractNumber) a  from VMALL_DICTIONARY_INCLUDE""",
      s"""select avg(contractNumber) a  from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_042")

  }


  //VMALL_DICTIONARY_INCLUDE_043
  test("VMALL_DICTIONARY_INCLUDE_043", Include) {

    checkAnswer(s"""select min(contractNumber) a  from VMALL_DICTIONARY_INCLUDE""",
      s"""select min(contractNumber) a  from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_043")

  }


  //VMALL_DICTIONARY_INCLUDE_044
  test("VMALL_DICTIONARY_INCLUDE_044", Include) {

    sql(s"""select variance(contractNumber) as a   from (select contractNumber from VMALL_DICTIONARY_INCLUDE order by contractNumber) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_045
  ignore("VMALL_DICTIONARY_INCLUDE_045", Include) {

    checkAnswer(s"""select var_pop(contractNumber) as a from (select * from VMALL_DICTIONARY_INCLUDE order by contractNumber) t""",
      s"""select var_pop(contractNumber) as a from (select * from VMALL_DICTIONARY_INCLUDE_hive order by contractNumber) t""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_045")

  }


  //VMALL_DICTIONARY_INCLUDE_046
  test("VMALL_DICTIONARY_INCLUDE_046", Include) {

    checkAnswer(s"""select var_samp(contractNumber) as a from  (select * from VMALL_DICTIONARY_INCLUDE order by contractNumber) t""",
      s"""select var_samp(contractNumber) as a from  (select * from VMALL_DICTIONARY_INCLUDE_hive order by contractNumber) t""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_046")

  }


  //VMALL_DICTIONARY_INCLUDE_047
  test("VMALL_DICTIONARY_INCLUDE_047", Include) {

    sql(s"""select stddev_pop(contractNumber) as a  from (select contractNumber from VMALL_DICTIONARY_INCLUDE order by contractNumber) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_048
  test("VMALL_DICTIONARY_INCLUDE_048", Include) {

    sql(s"""select stddev_samp(contractNumber)  as a from (select contractNumber from VMALL_DICTIONARY_INCLUDE order by contractNumber) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_049
  test("VMALL_DICTIONARY_INCLUDE_049", Include) {

    sql(s"""select covar_pop(contractNumber,contractNumber) as a  from (select contractNumber from VMALL_DICTIONARY_INCLUDE order by contractNumber) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_050
  test("VMALL_DICTIONARY_INCLUDE_050", Include) {

    sql(s"""select covar_samp(contractNumber,contractNumber) as a  from (select contractNumber from VMALL_DICTIONARY_INCLUDE order by contractNumber) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_051
  test("VMALL_DICTIONARY_INCLUDE_051", Include) {

    checkAnswer(s"""select corr(contractNumber,contractNumber)  as a from VMALL_DICTIONARY_INCLUDE""",
      s"""select corr(contractNumber,contractNumber)  as a from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_051")

  }


  //VMALL_DICTIONARY_INCLUDE_052
  test("VMALL_DICTIONARY_INCLUDE_052", Include) {

    sql(s"""select percentile_approx(contractNumber,0.2) as a  from (select contractNumber from VMALL_DICTIONARY_INCLUDE order by contractNumber) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_053
  test("VMALL_DICTIONARY_INCLUDE_053", Include) {

    sql(s"""select percentile_approx(contractNumber,0.2,5) as a  from (select contractNumber from VMALL_DICTIONARY_INCLUDE order by contractNumber) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_054
  test("VMALL_DICTIONARY_INCLUDE_054", Include) {

    sql(s"""select percentile_approx(contractNumber,array(0.2,0.3,0.99))  as a from (select contractNumber from VMALL_DICTIONARY_INCLUDE order by contractNumber) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_055
  test("VMALL_DICTIONARY_INCLUDE_055", Include) {

    sql(s"""select percentile_approx(contractNumber,array(0.2,0.3,0.99),5) as a from (select contractNumber from VMALL_DICTIONARY_INCLUDE order by contractNumber) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_056
  test("VMALL_DICTIONARY_INCLUDE_056", Include) {

    sql(s"""select histogram_numeric(contractNumber,2)  as a from (select contractNumber from VMALL_DICTIONARY_INCLUDE order by contractNumber) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_057
  test("VMALL_DICTIONARY_INCLUDE_057", Include) {

    checkAnswer(s"""select contractNumber+ 10 as a  from VMALL_DICTIONARY_INCLUDE order by a""",
      s"""select contractNumber+ 10 as a  from VMALL_DICTIONARY_INCLUDE_hive order by a""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_057")

  }


  //VMALL_DICTIONARY_INCLUDE_058
  test("VMALL_DICTIONARY_INCLUDE_058", Include) {

    checkAnswer(s"""select min(contractNumber), max(contractNumber+ 10) Total from VMALL_DICTIONARY_INCLUDE group by  channelsId order by Total""",
      s"""select min(contractNumber), max(contractNumber+ 10) Total from VMALL_DICTIONARY_INCLUDE_hive group by  channelsId order by Total""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_058")

  }


  //VMALL_DICTIONARY_INCLUDE_059
  test("VMALL_DICTIONARY_INCLUDE_059", Include) {

    sql(s"""select last(contractNumber) a from VMALL_DICTIONARY_INCLUDE  order by a""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_060
  test("VMALL_DICTIONARY_INCLUDE_060", Include) {

    checkAnswer(s"""select FIRST(contractNumber) a from VMALL_DICTIONARY_INCLUDE order by a""",
      s"""select FIRST(contractNumber) a from VMALL_DICTIONARY_INCLUDE_hive order by a""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_060")

  }


  //VMALL_DICTIONARY_INCLUDE_061
  test("VMALL_DICTIONARY_INCLUDE_061", Include) {

    checkAnswer(s"""select contractNumber,count(contractNumber) a from VMALL_DICTIONARY_INCLUDE group by contractNumber order by contractNumber""",
      s"""select contractNumber,count(contractNumber) a from VMALL_DICTIONARY_INCLUDE_hive group by contractNumber order by contractNumber""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_061")

  }


  //VMALL_DICTIONARY_INCLUDE_062
  test("VMALL_DICTIONARY_INCLUDE_062", Include) {

    checkAnswer(s"""select Lower(contractNumber) a  from VMALL_DICTIONARY_INCLUDE order by contractNumber""",
      s"""select Lower(contractNumber) a  from VMALL_DICTIONARY_INCLUDE_hive order by contractNumber""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_062")

  }


  //VMALL_DICTIONARY_INCLUDE_063
  test("VMALL_DICTIONARY_INCLUDE_063", Include) {

    checkAnswer(s"""select distinct contractNumber from VMALL_DICTIONARY_INCLUDE order by contractNumber""",
      s"""select distinct contractNumber from VMALL_DICTIONARY_INCLUDE_hive order by contractNumber""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_063")

  }


  //VMALL_DICTIONARY_INCLUDE_064
  test("VMALL_DICTIONARY_INCLUDE_064", Include) {

    checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_INCLUDE order by contractNumber limit 101""",
      s"""select contractNumber from VMALL_DICTIONARY_INCLUDE_hive order by contractNumber limit 101""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_064")

  }


  //VMALL_DICTIONARY_INCLUDE_065
  test("VMALL_DICTIONARY_INCLUDE_065", Include) {

    checkAnswer(s"""select contractNumber as a from VMALL_DICTIONARY_INCLUDE  order by a asc limit 10""",
      s"""select contractNumber as a from VMALL_DICTIONARY_INCLUDE_hive  order by a asc limit 10""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_065")

  }


  //VMALL_DICTIONARY_INCLUDE_066
  test("VMALL_DICTIONARY_INCLUDE_066", Include) {

    checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_INCLUDE where  (contractNumber == 9223372047700) and (imei=='1AA100012')""",
      s"""select contractNumber from VMALL_DICTIONARY_INCLUDE_hive where  (contractNumber == 9223372047700) and (imei=='1AA100012')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_066")

  }


  //VMALL_DICTIONARY_INCLUDE_067
  test("VMALL_DICTIONARY_INCLUDE_067", Include) {

    checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_INCLUDE where contractNumber !=9223372047700 order by contractNumber""",
      s"""select contractNumber from VMALL_DICTIONARY_INCLUDE_hive where contractNumber !=9223372047700 order by contractNumber""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_067")

  }


  //VMALL_DICTIONARY_INCLUDE_068
  test("VMALL_DICTIONARY_INCLUDE_068", Include) {

    checkAnswer(s"""select contractNumber  from VMALL_DICTIONARY_INCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color') order by contractNumber""",
      s"""select contractNumber  from VMALL_DICTIONARY_INCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color') order by contractNumber""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_068")

  }


  //VMALL_DICTIONARY_INCLUDE_069
  test("VMALL_DICTIONARY_INCLUDE_069", Include) {

    checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_INCLUDE where contractNumber !=9223372047700 order by contractNumber""",
      s"""select contractNumber from VMALL_DICTIONARY_INCLUDE_hive where contractNumber !=9223372047700 order by contractNumber""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_069")

  }


  //VMALL_DICTIONARY_INCLUDE_070
  test("VMALL_DICTIONARY_INCLUDE_070", Include) {

    checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_INCLUDE where contractNumber >9223372047700 order by contractNumber""",
      s"""select contractNumber from VMALL_DICTIONARY_INCLUDE_hive where contractNumber >9223372047700 order by contractNumber""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_070")

  }


  //VMALL_DICTIONARY_INCLUDE_071
  test("VMALL_DICTIONARY_INCLUDE_071", Include) {

    checkAnswer(s"""select contractNumber  from VMALL_DICTIONARY_INCLUDE where contractNumber<>contractNumber""",
      s"""select contractNumber  from VMALL_DICTIONARY_INCLUDE_hive where contractNumber<>contractNumber""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_071")

  }


  //VMALL_DICTIONARY_INCLUDE_072
  test("VMALL_DICTIONARY_INCLUDE_072", Include) {

    checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_INCLUDE where contractNumber != Latest_areaId order by contractNumber""",
      s"""select contractNumber from VMALL_DICTIONARY_INCLUDE_hive where contractNumber != Latest_areaId order by contractNumber""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_072")

  }


  //VMALL_DICTIONARY_INCLUDE_073
  test("VMALL_DICTIONARY_INCLUDE_073", Include) {

    checkAnswer(s"""select contractNumber, contractNumber from VMALL_DICTIONARY_INCLUDE where Latest_areaId<contractNumber order by contractNumber""",
      s"""select contractNumber, contractNumber from VMALL_DICTIONARY_INCLUDE_hive where Latest_areaId<contractNumber order by contractNumber""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_073")

  }


  //VMALL_DICTIONARY_INCLUDE_074
  test("VMALL_DICTIONARY_INCLUDE_074", Include) {

    checkAnswer(s"""select contractNumber, contractNumber from VMALL_DICTIONARY_INCLUDE where Latest_DAY<=contractNumber order by contractNumber""",
      s"""select contractNumber, contractNumber from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY<=contractNumber order by contractNumber""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_074")

  }


  //VMALL_DICTIONARY_INCLUDE_075
  test("VMALL_DICTIONARY_INCLUDE_075", Include) {

    checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_INCLUDE where contractNumber <1000 order by contractNumber""",
      s"""select contractNumber from VMALL_DICTIONARY_INCLUDE_hive where contractNumber <1000 order by contractNumber""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_075")

  }


  //VMALL_DICTIONARY_INCLUDE_076
  test("VMALL_DICTIONARY_INCLUDE_076", Include) {

    checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_INCLUDE where contractNumber >1000 order by contractNumber""",
      s"""select contractNumber from VMALL_DICTIONARY_INCLUDE_hive where contractNumber >1000 order by contractNumber""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_076")

  }


  //VMALL_DICTIONARY_INCLUDE_077
  test("VMALL_DICTIONARY_INCLUDE_077", Include) {

    checkAnswer(s"""select contractNumber  from VMALL_DICTIONARY_INCLUDE where contractNumber IS NULL order by contractNumber""",
      s"""select contractNumber  from VMALL_DICTIONARY_INCLUDE_hive where contractNumber IS NULL order by contractNumber""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_077")

  }


  //VMALL_DICTIONARY_INCLUDE_078
  test("VMALL_DICTIONARY_INCLUDE_078", Include) {

    checkAnswer(s"""select contractNumber  from VMALL_DICTIONARY_INCLUDE where Latest_DAY IS NOT NULL order by contractNumber""",
      s"""select contractNumber  from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY IS NOT NULL order by contractNumber""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_078")

  }


  //VMALL_DICTIONARY_INCLUDE_079
  test("VMALL_DICTIONARY_INCLUDE_079", Include) {

    checkAnswer(s"""Select count(Latest_DAY) from VMALL_DICTIONARY_INCLUDE""",
      s"""Select count(Latest_DAY) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_079")

  }


  //VMALL_DICTIONARY_INCLUDE_080
  test("VMALL_DICTIONARY_INCLUDE_080", Include) {

    checkAnswer(s"""select count(DISTINCT Latest_DAY) as a from VMALL_DICTIONARY_INCLUDE""",
      s"""select count(DISTINCT Latest_DAY) as a from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_080")

  }


  //VMALL_DICTIONARY_INCLUDE_081
  test("VMALL_DICTIONARY_INCLUDE_081", Include) {

    checkAnswer(s"""select sum(Latest_DAY)+10 as a ,Latest_DAY  from VMALL_DICTIONARY_INCLUDE group by Latest_DAY order by a""",
      s"""select sum(Latest_DAY)+10 as a ,Latest_DAY  from VMALL_DICTIONARY_INCLUDE_hive group by Latest_DAY order by a""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_081")

  }


  //VMALL_DICTIONARY_INCLUDE_082
  test("VMALL_DICTIONARY_INCLUDE_082", Include) {

    checkAnswer(s"""select max(Latest_DAY),min(Latest_DAY) from VMALL_DICTIONARY_INCLUDE""",
      s"""select max(Latest_DAY),min(Latest_DAY) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_082")

  }


  //VMALL_DICTIONARY_INCLUDE_083
  test("VMALL_DICTIONARY_INCLUDE_083", Include) {

    checkAnswer(s"""select sum(Latest_DAY) a  from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(Latest_DAY) a  from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_083")

  }


  //VMALL_DICTIONARY_INCLUDE_084
  test("VMALL_DICTIONARY_INCLUDE_084", Include) {

    checkAnswer(s"""select avg(Latest_DAY) a  from VMALL_DICTIONARY_INCLUDE""",
      s"""select avg(Latest_DAY) a  from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_084")

  }


  //VMALL_DICTIONARY_INCLUDE_085
  test("VMALL_DICTIONARY_INCLUDE_085", Include) {

    checkAnswer(s"""select min(Latest_DAY) a  from VMALL_DICTIONARY_INCLUDE""",
      s"""select min(Latest_DAY) a  from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_085")

  }


  //VMALL_DICTIONARY_INCLUDE_086
  test("VMALL_DICTIONARY_INCLUDE_086", Include) {

    sql(s"""select variance(Latest_DAY) as a   from (select Latest_DAY from VMALL_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_087
  test("VMALL_DICTIONARY_INCLUDE_087", Include) {

    sql(s"""select var_pop(Latest_DAY)  as a from (select Latest_DAY from VMALL_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_088
  test("VMALL_DICTIONARY_INCLUDE_088", Include) {

    sql(s"""select var_samp(Latest_DAY) as a  from (select Latest_DAY from VMALL_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_089
  test("VMALL_DICTIONARY_INCLUDE_089", Include) {

    sql(s"""select stddev_pop(Latest_DAY) as a  from (select Latest_DAY from VMALL_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_090
  test("VMALL_DICTIONARY_INCLUDE_090", Include) {

    sql(s"""select stddev_samp(Latest_DAY)  as a from (select Latest_DAY from VMALL_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_091
  test("VMALL_DICTIONARY_INCLUDE_091", Include) {

    sql(s"""select covar_pop(Latest_DAY,Latest_DAY) as a  from (select Latest_DAY from VMALL_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_092
  test("VMALL_DICTIONARY_INCLUDE_092", Include) {

    sql(s"""select covar_samp(Latest_DAY,Latest_DAY) as a  from (select Latest_DAY from VMALL_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_093
  test("VMALL_DICTIONARY_INCLUDE_093", Include) {

    checkAnswer(s"""select corr(Latest_DAY,Latest_DAY)  as a from VMALL_DICTIONARY_INCLUDE""",
      s"""select corr(Latest_DAY,Latest_DAY)  as a from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_093")

  }


  //VMALL_DICTIONARY_INCLUDE_094
  test("VMALL_DICTIONARY_INCLUDE_094", Include) {

    sql(s"""select percentile_approx(Latest_DAY,0.2) as a  from (select Latest_DAY from VMALL_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_095
  test("VMALL_DICTIONARY_INCLUDE_095", Include) {

    sql(s"""select percentile_approx(Latest_DAY,0.2,5) as a  from (select Latest_DAY from VMALL_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_096
  test("VMALL_DICTIONARY_INCLUDE_096", Include) {

    sql(s"""select percentile_approx(Latest_DAY,array(0.2,0.3,0.99))  as a from (select Latest_DAY from VMALL_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_097
  test("VMALL_DICTIONARY_INCLUDE_097", Include) {

    sql(s"""select percentile_approx(Latest_DAY,array(0.2,0.3,0.99),5) as a from (select Latest_DAY from VMALL_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_098
  test("VMALL_DICTIONARY_INCLUDE_098", Include) {

    sql(s"""select histogram_numeric(Latest_DAY,2)  as a from (select Latest_DAY from VMALL_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_099
  test("VMALL_DICTIONARY_INCLUDE_099", Include) {

    checkAnswer(s"""select Latest_DAY, Latest_DAY+ 10 as a  from VMALL_DICTIONARY_INCLUDE order by a""",
      s"""select Latest_DAY, Latest_DAY+ 10 as a  from VMALL_DICTIONARY_INCLUDE_hive order by a""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_099")

  }


  //VMALL_DICTIONARY_INCLUDE_100
  test("VMALL_DICTIONARY_INCLUDE_100", Include) {

    checkAnswer(s"""select min(Latest_DAY) a, max(Latest_DAY+ 10) Total from VMALL_DICTIONARY_INCLUDE group by  channelsId order by a,Total""",
      s"""select min(Latest_DAY) a, max(Latest_DAY+ 10) Total from VMALL_DICTIONARY_INCLUDE_hive group by  channelsId order by a,Total""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_100")

  }


  //VMALL_DICTIONARY_INCLUDE_101
  test("VMALL_DICTIONARY_INCLUDE_101", Include) {

    sql(s"""select last(Latest_DAY) a from VMALL_DICTIONARY_INCLUDE order by a""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_102
  test("VMALL_DICTIONARY_INCLUDE_102", Include) {

    sql(s"""select FIRST(Latest_DAY) a from (select Latest_DAY from VMALL_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_103
  test("VMALL_DICTIONARY_INCLUDE_103", Include) {

    checkAnswer(s"""select Latest_DAY,count(Latest_DAY) a from VMALL_DICTIONARY_INCLUDE group by Latest_DAY order by Latest_DAY""",
      s"""select Latest_DAY,count(Latest_DAY) a from VMALL_DICTIONARY_INCLUDE_hive group by Latest_DAY order by Latest_DAY""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_103")

  }


  //VMALL_DICTIONARY_INCLUDE_104
  test("VMALL_DICTIONARY_INCLUDE_104", Include) {

    checkAnswer(s"""select Lower(Latest_DAY) a  from VMALL_DICTIONARY_INCLUDE order by a""",
      s"""select Lower(Latest_DAY) a  from VMALL_DICTIONARY_INCLUDE_hive order by a""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_104")

  }


  //VMALL_DICTIONARY_INCLUDE_105
  test("VMALL_DICTIONARY_INCLUDE_105", Include) {

    checkAnswer(s"""select distinct Latest_DAY from VMALL_DICTIONARY_INCLUDE order by Latest_DAY""",
      s"""select distinct Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive order by Latest_DAY""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_105")

  }


  //VMALL_DICTIONARY_INCLUDE_106
  test("VMALL_DICTIONARY_INCLUDE_106", Include) {

    checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE order by Latest_DAY limit 101""",
      s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive order by Latest_DAY limit 101""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_106")

  }


  //VMALL_DICTIONARY_INCLUDE_107
  test("VMALL_DICTIONARY_INCLUDE_107", Include) {

    checkAnswer(s"""select Latest_DAY as a from VMALL_DICTIONARY_INCLUDE  order by a asc limit 10""",
      s"""select Latest_DAY as a from VMALL_DICTIONARY_INCLUDE_hive  order by a asc limit 10""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_107")

  }


  //VMALL_DICTIONARY_INCLUDE_108
  test("VMALL_DICTIONARY_INCLUDE_108", Include) {

    checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where  (Latest_DAY == 1234567890123450.0000000000)  and (imei=='1AA1')""",
      s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where  (Latest_DAY == 1234567890123450.0000000000)  and (imei=='1AA1')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_108")

  }


  //VMALL_DICTIONARY_INCLUDE_109
  test("VMALL_DICTIONARY_INCLUDE_109", Include) {

    checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""",
      s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_109")

  }


  //VMALL_DICTIONARY_INCLUDE_110
  test("VMALL_DICTIONARY_INCLUDE_110", Include) {

    checkAnswer(s"""select Latest_DAY  from VMALL_DICTIONARY_INCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select Latest_DAY  from VMALL_DICTIONARY_INCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_110")

  }


  //VMALL_DICTIONARY_INCLUDE_111
  test("VMALL_DICTIONARY_INCLUDE_111", Include) {

    checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""",
      s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_111")

  }


  //VMALL_DICTIONARY_INCLUDE_112
  test("VMALL_DICTIONARY_INCLUDE_112", Include) {

    checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY >1234567890123450.0000000000  order by Latest_DAY""",
      s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY >1234567890123450.0000000000  order by Latest_DAY""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_112")

  }


  //VMALL_DICTIONARY_INCLUDE_113
  test("VMALL_DICTIONARY_INCLUDE_113", Include) {

    checkAnswer(s"""select Latest_DAY  from VMALL_DICTIONARY_INCLUDE where Latest_DAY<>Latest_DAY""",
      s"""select Latest_DAY  from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY<>Latest_DAY""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_113")

  }


  //VMALL_DICTIONARY_INCLUDE_114
  test("VMALL_DICTIONARY_INCLUDE_114", Include) {

    checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY != Latest_areaId order by Latest_DAY""",
      s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY != Latest_areaId order by Latest_DAY""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_114")

  }


  //VMALL_DICTIONARY_INCLUDE_115
  test("VMALL_DICTIONARY_INCLUDE_115", Include) {

    checkAnswer(s"""select Latest_DAY, Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_areaId<Latest_DAY order by Latest_DAY""",
      s"""select Latest_DAY, Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_areaId<Latest_DAY order by Latest_DAY""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_115")

  }


  //VMALL_DICTIONARY_INCLUDE_116
  test("VMALL_DICTIONARY_INCLUDE_116", Include) {

    checkAnswer(s"""select Latest_DAY, Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY<=Latest_DAY  order by Latest_DAY""",
      s"""select Latest_DAY, Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY<=Latest_DAY  order by Latest_DAY""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_116")

  }


  //VMALL_DICTIONARY_INCLUDE_117
  test("VMALL_DICTIONARY_INCLUDE_117", Include) {

    checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY <1000  order by Latest_DAY""",
      s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY <1000  order by Latest_DAY""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_117")

  }


  //VMALL_DICTIONARY_INCLUDE_118
  test("VMALL_DICTIONARY_INCLUDE_118", Include) {

    checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY >1000  order by Latest_DAY""",
      s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY >1000  order by Latest_DAY""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_118")

  }


  //VMALL_DICTIONARY_INCLUDE_119
  test("VMALL_DICTIONARY_INCLUDE_119", Include) {

    checkAnswer(s"""select Latest_DAY  from VMALL_DICTIONARY_INCLUDE where Latest_DAY IS NULL  order by Latest_DAY""",
      s"""select Latest_DAY  from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY IS NULL  order by Latest_DAY""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_119")

  }


  //VMALL_DICTIONARY_INCLUDE_120
  test("VMALL_DICTIONARY_INCLUDE_120", Include) {

    checkAnswer(s"""select Latest_DAY  from VMALL_DICTIONARY_INCLUDE where Latest_DAY IS NOT NULL  order by Latest_DAY""",
      s"""select Latest_DAY  from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY IS NOT NULL  order by Latest_DAY""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_120")

  }


  //VMALL_DICTIONARY_INCLUDE_121
  test("VMALL_DICTIONARY_INCLUDE_121", Include) {

    checkAnswer(s"""Select count(gamePointId) from VMALL_DICTIONARY_INCLUDE""",
      s"""Select count(gamePointId) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_121")

  }


  //VMALL_DICTIONARY_INCLUDE_122
  test("VMALL_DICTIONARY_INCLUDE_122", Include) {

    checkAnswer(s"""select count(DISTINCT gamePointId) as a from VMALL_DICTIONARY_INCLUDE""",
      s"""select count(DISTINCT gamePointId) as a from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_122")

  }


  //VMALL_DICTIONARY_INCLUDE_123
  test("VMALL_DICTIONARY_INCLUDE_123", Include) {

    checkAnswer(s"""select sum(gamePointId)+10 as a ,gamePointId  from VMALL_DICTIONARY_INCLUDE group by gamePointId order by a""",
      s"""select sum(gamePointId)+10 as a ,gamePointId  from VMALL_DICTIONARY_INCLUDE_hive group by gamePointId order by a""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_123")

  }


  //VMALL_DICTIONARY_INCLUDE_124
  test("VMALL_DICTIONARY_INCLUDE_124", Include) {

    checkAnswer(s"""select max(gamePointId),min(gamePointId) from VMALL_DICTIONARY_INCLUDE""",
      s"""select max(gamePointId),min(gamePointId) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_124")

  }


  //VMALL_DICTIONARY_INCLUDE_125
  test("VMALL_DICTIONARY_INCLUDE_125", Include) {

    checkAnswer(s"""select sum(gamePointId) a  from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(gamePointId) a  from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_125")

  }


  //VMALL_DICTIONARY_INCLUDE_126
  test("VMALL_DICTIONARY_INCLUDE_126", Include) {

    checkAnswer(s"""select avg(gamePointId) a  from VMALL_DICTIONARY_INCLUDE""",
      s"""select avg(gamePointId) a  from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_126")

  }


  //VMALL_DICTIONARY_INCLUDE_127
  test("VMALL_DICTIONARY_INCLUDE_127", Include) {

    checkAnswer(s"""select min(gamePointId) a  from VMALL_DICTIONARY_INCLUDE""",
      s"""select min(gamePointId) a  from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_127")

  }


  //VMALL_DICTIONARY_INCLUDE_128
  test("VMALL_DICTIONARY_INCLUDE_128", Include) {

    sql(s"""select variance(gamePointId) as a   from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_129
  test("VMALL_DICTIONARY_INCLUDE_129", Include) {

    sql(s"""select var_pop(gamePointId)  as a from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_130
  test("VMALL_DICTIONARY_INCLUDE_130", Include) {

    sql(s"""select var_samp(gamePointId) as a  from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_131
  test("VMALL_DICTIONARY_INCLUDE_131", Include) {

    sql(s"""select stddev_pop(gamePointId) as a  from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_132
  test("VMALL_DICTIONARY_INCLUDE_132", Include) {

    sql(s"""select stddev_samp(gamePointId)  as a from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_133
  test("VMALL_DICTIONARY_INCLUDE_133", Include) {

    sql(s"""select covar_pop(gamePointId,gamePointId) as a  from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_134
  test("VMALL_DICTIONARY_INCLUDE_134", Include) {

    sql(s"""select covar_samp(gamePointId,gamePointId) as a  from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_135
  test("VMALL_DICTIONARY_INCLUDE_135", Include) {

    checkAnswer(s"""select corr(gamePointId,gamePointId)  as a from VMALL_DICTIONARY_INCLUDE""",
      s"""select corr(gamePointId,gamePointId)  as a from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_135")

  }


  //VMALL_DICTIONARY_INCLUDE_136
  test("VMALL_DICTIONARY_INCLUDE_136", Include) {

    sql(s"""select percentile_approx(gamePointId,0.2) as a  from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_137
  test("VMALL_DICTIONARY_INCLUDE_137", Include) {

    sql(s"""select percentile_approx(gamePointId,0.2,5) as a  from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_138
  test("VMALL_DICTIONARY_INCLUDE_138", Include) {

    sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99))  as a from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_139
  test("VMALL_DICTIONARY_INCLUDE_139", Include) {

    sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99),5) as a from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_140
  test("VMALL_DICTIONARY_INCLUDE_140", Include) {

    sql(s"""select histogram_numeric(gamePointId,2)  as a from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_141
  test("VMALL_DICTIONARY_INCLUDE_141", Include) {

    checkAnswer(s"""select gamePointId, gamePointId+ 10 as a  from VMALL_DICTIONARY_INCLUDE order by a""",
      s"""select gamePointId, gamePointId+ 10 as a  from VMALL_DICTIONARY_INCLUDE_hive order by a""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_141")

  }


  //VMALL_DICTIONARY_INCLUDE_142
  test("VMALL_DICTIONARY_INCLUDE_142", Include) {

    checkAnswer(s"""select min(gamePointId), max(gamePointId+ 10) Total from VMALL_DICTIONARY_INCLUDE group by  channelsId order by Total""",
      s"""select min(gamePointId), max(gamePointId+ 10) Total from VMALL_DICTIONARY_INCLUDE_hive group by  channelsId order by Total""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_142")

  }


  //VMALL_DICTIONARY_INCLUDE_143
  test("VMALL_DICTIONARY_INCLUDE_143", Include) {

    sql(s"""select last(gamePointId) a from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_144
  test("VMALL_DICTIONARY_INCLUDE_144", Include) {

    sql(s"""select FIRST(gamePointId) a from VMALL_DICTIONARY_INCLUDE order by a""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_145
  test("VMALL_DICTIONARY_INCLUDE_145", Include) {

    checkAnswer(s"""select gamePointId,count(gamePointId) a from VMALL_DICTIONARY_INCLUDE group by gamePointId order by gamePointId""",
      s"""select gamePointId,count(gamePointId) a from VMALL_DICTIONARY_INCLUDE_hive group by gamePointId order by gamePointId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_145")

  }


  //VMALL_DICTIONARY_INCLUDE_146
  test("VMALL_DICTIONARY_INCLUDE_146", Include) {

    checkAnswer(s"""select Lower(gamePointId) a  from VMALL_DICTIONARY_INCLUDE order by gamePointId""",
      s"""select Lower(gamePointId) a  from VMALL_DICTIONARY_INCLUDE_hive order by gamePointId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_146")

  }


  //VMALL_DICTIONARY_INCLUDE_147
  test("VMALL_DICTIONARY_INCLUDE_147", Include) {

    checkAnswer(s"""select distinct gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId""",
      s"""select distinct gamePointId from VMALL_DICTIONARY_INCLUDE_hive order by gamePointId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_147")

  }


  //VMALL_DICTIONARY_INCLUDE_148
  test("VMALL_DICTIONARY_INCLUDE_148", Include) {

    checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_INCLUDE  order by gamePointId limit 101""",
      s"""select gamePointId from VMALL_DICTIONARY_INCLUDE_hive  order by gamePointId limit 101""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_148")

  }


  //VMALL_DICTIONARY_INCLUDE_149
  test("VMALL_DICTIONARY_INCLUDE_149", Include) {

    checkAnswer(s"""select gamePointId as a from VMALL_DICTIONARY_INCLUDE  order by a asc limit 10""",
      s"""select gamePointId as a from VMALL_DICTIONARY_INCLUDE_hive  order by a asc limit 10""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_149")

  }


  //VMALL_DICTIONARY_INCLUDE_150
  test("VMALL_DICTIONARY_INCLUDE_150", Include) {

    checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_INCLUDE where  (gamePointId == 4.70133553923674E43) and (imei=='1AA100084')""",
      s"""select gamePointId from VMALL_DICTIONARY_INCLUDE_hive where  (gamePointId == 4.70133553923674E43) and (imei=='1AA100084')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_150")

  }


  //VMALL_DICTIONARY_INCLUDE_151
  test("VMALL_DICTIONARY_INCLUDE_151", Include) {

    checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_INCLUDE where gamePointId !=4.70133553923674E43  order by gamePointId""",
      s"""select gamePointId from VMALL_DICTIONARY_INCLUDE_hive where gamePointId !=4.70133553923674E43  order by gamePointId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_151")

  }


  //VMALL_DICTIONARY_INCLUDE_152
  test("VMALL_DICTIONARY_INCLUDE_152", Include) {

    checkAnswer(s"""select gamePointId  from VMALL_DICTIONARY_INCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select gamePointId  from VMALL_DICTIONARY_INCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_152")

  }


  //VMALL_DICTIONARY_INCLUDE_153
  test("VMALL_DICTIONARY_INCLUDE_153", Include) {

    checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_INCLUDE where gamePointId !=4.70133553923674E43""",
      s"""select gamePointId from VMALL_DICTIONARY_INCLUDE_hive where gamePointId !=4.70133553923674E43""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_153")

  }


  //VMALL_DICTIONARY_INCLUDE_154
  test("VMALL_DICTIONARY_INCLUDE_154", Include) {

    checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_INCLUDE where gamePointId >4.70133553923674E43""",
      s"""select gamePointId from VMALL_DICTIONARY_INCLUDE_hive where gamePointId >4.70133553923674E43""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_154")

  }


  //VMALL_DICTIONARY_INCLUDE_155
  test("VMALL_DICTIONARY_INCLUDE_155", Include) {

    checkAnswer(s"""select gamePointId  from VMALL_DICTIONARY_INCLUDE where gamePointId<>gamePointId""",
      s"""select gamePointId  from VMALL_DICTIONARY_INCLUDE_hive where gamePointId<>gamePointId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_155")

  }


  //VMALL_DICTIONARY_INCLUDE_156
  test("VMALL_DICTIONARY_INCLUDE_156", Include) {

    checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_INCLUDE where gamePointId != Latest_areaId  order by gamePointId""",
      s"""select gamePointId from VMALL_DICTIONARY_INCLUDE_hive where gamePointId != Latest_areaId  order by gamePointId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_156")

  }


  //VMALL_DICTIONARY_INCLUDE_157
  test("VMALL_DICTIONARY_INCLUDE_157", Include) {

    checkAnswer(s"""select gamePointId, gamePointId from VMALL_DICTIONARY_INCLUDE where Latest_areaId<gamePointId  order by gamePointId""",
      s"""select gamePointId, gamePointId from VMALL_DICTIONARY_INCLUDE_hive where Latest_areaId<gamePointId  order by gamePointId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_157")

  }


  //VMALL_DICTIONARY_INCLUDE_158
  test("VMALL_DICTIONARY_INCLUDE_158", Include) {

    checkAnswer(s"""select gamePointId, gamePointId from VMALL_DICTIONARY_INCLUDE where gamePointId<=gamePointId  order by gamePointId""",
      s"""select gamePointId, gamePointId from VMALL_DICTIONARY_INCLUDE_hive where gamePointId<=gamePointId  order by gamePointId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_158")

  }


  //VMALL_DICTIONARY_INCLUDE_159
  test("VMALL_DICTIONARY_INCLUDE_159", Include) {

    checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_INCLUDE where gamePointId <1000 order by gamePointId""",
      s"""select gamePointId from VMALL_DICTIONARY_INCLUDE_hive where gamePointId <1000 order by gamePointId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_159")

  }


  //VMALL_DICTIONARY_INCLUDE_160
  test("VMALL_DICTIONARY_INCLUDE_160", Include) {

    checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_INCLUDE where gamePointId >1000 order by gamePointId""",
      s"""select gamePointId from VMALL_DICTIONARY_INCLUDE_hive where gamePointId >1000 order by gamePointId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_160")

  }


  //VMALL_DICTIONARY_INCLUDE_161
  test("VMALL_DICTIONARY_INCLUDE_161", Include) {

    checkAnswer(s"""select gamePointId  from VMALL_DICTIONARY_INCLUDE where gamePointId IS NULL order by gamePointId""",
      s"""select gamePointId  from VMALL_DICTIONARY_INCLUDE_hive where gamePointId IS NULL order by gamePointId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_161")

  }


  //VMALL_DICTIONARY_INCLUDE_162
  test("VMALL_DICTIONARY_INCLUDE_162", Include) {

    checkAnswer(s"""select gamePointId  from VMALL_DICTIONARY_INCLUDE where gamePointId IS NOT NULL order by gamePointId""",
      s"""select gamePointId  from VMALL_DICTIONARY_INCLUDE_hive where gamePointId IS NOT NULL order by gamePointId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_162")

  }


  //VMALL_DICTIONARY_INCLUDE_163
  test("VMALL_DICTIONARY_INCLUDE_163", Include) {

    checkAnswer(s"""Select count(productionDate) from VMALL_DICTIONARY_INCLUDE""",
      s"""Select count(productionDate) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_163")

  }


  //VMALL_DICTIONARY_INCLUDE_164
  test("VMALL_DICTIONARY_INCLUDE_164", Include) {

    checkAnswer(s"""select count(DISTINCT productionDate) as a from VMALL_DICTIONARY_INCLUDE""",
      s"""select count(DISTINCT productionDate) as a from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_164")

  }


  //VMALL_DICTIONARY_INCLUDE_165
  test("VMALL_DICTIONARY_INCLUDE_165", Include) {

    checkAnswer(s"""select sum(productionDate)+10 as a ,productionDate  from VMALL_DICTIONARY_INCLUDE group by productionDate order by productionDate""",
      s"""select sum(productionDate)+10 as a ,productionDate  from VMALL_DICTIONARY_INCLUDE_hive group by productionDate order by productionDate""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_165")

  }


  //VMALL_DICTIONARY_INCLUDE_166
  test("VMALL_DICTIONARY_INCLUDE_166", Include) {

    checkAnswer(s"""select max(productionDate),min(productionDate) from VMALL_DICTIONARY_INCLUDE""",
      s"""select max(productionDate),min(productionDate) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_166")

  }


  //VMALL_DICTIONARY_INCLUDE_167
  test("VMALL_DICTIONARY_INCLUDE_167", Include) {

    checkAnswer(s"""select sum(productionDate) a  from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(productionDate) a  from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_167")

  }


  //VMALL_DICTIONARY_INCLUDE_168
  test("VMALL_DICTIONARY_INCLUDE_168", Include) {

    checkAnswer(s"""select avg(productionDate) a  from VMALL_DICTIONARY_INCLUDE""",
      s"""select avg(productionDate) a  from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_168")

  }


  //VMALL_DICTIONARY_INCLUDE_169
  test("VMALL_DICTIONARY_INCLUDE_169", Include) {

    checkAnswer(s"""select min(productionDate) a  from VMALL_DICTIONARY_INCLUDE""",
      s"""select min(productionDate) a  from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_169")

  }


  //VMALL_DICTIONARY_INCLUDE_170
  test("VMALL_DICTIONARY_INCLUDE_170", Include) {

    sql(s"""select variance(gamePointId) as a   from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_171
  ignore("VMALL_DICTIONARY_INCLUDE_171", Include) {

    checkAnswer(s"""select var_pop(gamePointId) as a from (select * from VMALL_DICTIONARY_INCLUDE order by gamePointId)""",
      s"""select var_pop(gamePointId) as a from (select * from VMALL_DICTIONARY_INCLUDE_hive order by gamePointId)""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_171")

  }


  //VMALL_DICTIONARY_INCLUDE_172
  ignore("VMALL_DICTIONARY_INCLUDE_172", Include) {

    checkAnswer(s"""select var_samp(gamePointId) as a from (select * from VMALL_DICTIONARY_INCLUDE order by gamePointId)""",
      s"""select var_samp(gamePointId) as a from (select * from VMALL_DICTIONARY_INCLUDE_hive order by gamePointId)""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_172")

  }


  //VMALL_DICTIONARY_INCLUDE_173
  test("VMALL_DICTIONARY_INCLUDE_173", Include) {

    sql(s"""select stddev_pop(gamePointId) as a  from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_174
  test("VMALL_DICTIONARY_INCLUDE_174", Include) {

    sql(s"""select stddev_samp(gamePointId)  as a from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_175
  test("VMALL_DICTIONARY_INCLUDE_175", Include) {

    sql(s"""select covar_pop(gamePointId,gamePointId) as a  from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_176
  test("VMALL_DICTIONARY_INCLUDE_176", Include) {

    sql(s"""select covar_samp(gamePointId,gamePointId) as a  from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_177
  test("VMALL_DICTIONARY_INCLUDE_177", Include) {

    checkAnswer(s"""select corr(gamePointId,gamePointId)  as a from VMALL_DICTIONARY_INCLUDE""",
      s"""select corr(gamePointId,gamePointId)  as a from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_177")

  }


  //VMALL_DICTIONARY_INCLUDE_178
  test("VMALL_DICTIONARY_INCLUDE_178", Include) {

    sql(s"""select percentile_approx(gamePointId,0.2) as a  from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_179
  test("VMALL_DICTIONARY_INCLUDE_179", Include) {

    sql(s"""select percentile_approx(gamePointId,0.2,5) as a  from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_180
  test("VMALL_DICTIONARY_INCLUDE_180", Include) {

    sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99))  as a from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_181
  test("VMALL_DICTIONARY_INCLUDE_181", Include) {

    sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99),5) as a from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_182
  test("VMALL_DICTIONARY_INCLUDE_182", Include) {

    sql(s"""select histogram_numeric(productionDate,2)  as a from (select productionDate from VMALL_DICTIONARY_INCLUDE order by productionDate) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_183
  ignore("VMALL_DICTIONARY_INCLUDE_183", Include) {

    checkAnswer(s"""select last(productionDate) a from VMALL_DICTIONARY_INCLUDE order by a""",
      s"""select last(productionDate) a from VMALL_DICTIONARY_INCLUDE_hive order by a""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_183")

  }


  //VMALL_DICTIONARY_INCLUDE_184
  test("VMALL_DICTIONARY_INCLUDE_184", Include) {

    sql(s"""select FIRST(productionDate) a from VMALL_DICTIONARY_INCLUDE  order by a""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_185
  test("VMALL_DICTIONARY_INCLUDE_185", Include) {

    checkAnswer(s"""select productionDate,count(productionDate) a from VMALL_DICTIONARY_INCLUDE group by productionDate order by productionDate""",
      s"""select productionDate,count(productionDate) a from VMALL_DICTIONARY_INCLUDE_hive group by productionDate order by productionDate""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_185")

  }


  //VMALL_DICTIONARY_INCLUDE_186
  test("VMALL_DICTIONARY_INCLUDE_186", Include) {

    checkAnswer(s"""select Lower(productionDate) a  from VMALL_DICTIONARY_INCLUDE order by productionDate""",
      s"""select Lower(productionDate) a  from VMALL_DICTIONARY_INCLUDE_hive order by productionDate""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_186")

  }


  //VMALL_DICTIONARY_INCLUDE_187
  test("VMALL_DICTIONARY_INCLUDE_187", Include) {

    checkAnswer(s"""select distinct productionDate from VMALL_DICTIONARY_INCLUDE order by productionDate""",
      s"""select distinct productionDate from VMALL_DICTIONARY_INCLUDE_hive order by productionDate""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_187")

  }


  //VMALL_DICTIONARY_INCLUDE_188
  test("VMALL_DICTIONARY_INCLUDE_188", Include) {

    checkAnswer(s"""select productionDate from VMALL_DICTIONARY_INCLUDE order by productionDate limit 101""",
      s"""select productionDate from VMALL_DICTIONARY_INCLUDE_hive order by productionDate limit 101""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_188")

  }


  //VMALL_DICTIONARY_INCLUDE_189
  test("VMALL_DICTIONARY_INCLUDE_189", Include) {

    checkAnswer(s"""select productionDate as a from VMALL_DICTIONARY_INCLUDE  order by a asc limit 10""",
      s"""select productionDate as a from VMALL_DICTIONARY_INCLUDE_hive  order by a asc limit 10""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_189")

  }


  //VMALL_DICTIONARY_INCLUDE_190
  test("VMALL_DICTIONARY_INCLUDE_190", Include) {

    checkAnswer(s"""select productionDate from VMALL_DICTIONARY_INCLUDE where  (productionDate == '2015-07-01 12:07:28.0') and (productionDate=='2015-07-01 12:07:28.0')""",
      s"""select productionDate from VMALL_DICTIONARY_INCLUDE_hive where  (productionDate == '2015-07-01 12:07:28.0') and (productionDate=='2015-07-01 12:07:28.0')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_190")

  }


  //VMALL_DICTIONARY_INCLUDE_191
  test("VMALL_DICTIONARY_INCLUDE_191", Include) {

    checkAnswer(s"""select productionDate from VMALL_DICTIONARY_INCLUDE where productionDate !='2015-07-01 12:07:28.0' order by productionDate""",
      s"""select productionDate from VMALL_DICTIONARY_INCLUDE_hive where productionDate !='2015-07-01 12:07:28.0' order by productionDate""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_191")

  }


  //VMALL_DICTIONARY_INCLUDE_192
  test("VMALL_DICTIONARY_INCLUDE_192", Include) {

    checkAnswer(s"""select productionDate  from VMALL_DICTIONARY_INCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select productionDate  from VMALL_DICTIONARY_INCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_192")

  }


  //VMALL_DICTIONARY_INCLUDE_193
  test("VMALL_DICTIONARY_INCLUDE_193", Include) {

    checkAnswer(s"""select productionDate from VMALL_DICTIONARY_INCLUDE where productionDate !='2015-07-01 12:07:28.0' order by productionDate""",
      s"""select productionDate from VMALL_DICTIONARY_INCLUDE_hive where productionDate !='2015-07-01 12:07:28.0' order by productionDate""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_193")

  }


  //VMALL_DICTIONARY_INCLUDE_194
  test("VMALL_DICTIONARY_INCLUDE_194", Include) {

    checkAnswer(s"""select productionDate from VMALL_DICTIONARY_INCLUDE where productionDate >'2015-07-01 12:07:28.0' order by productionDate""",
      s"""select productionDate from VMALL_DICTIONARY_INCLUDE_hive where productionDate >'2015-07-01 12:07:28.0' order by productionDate""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_194")

  }


  //VMALL_DICTIONARY_INCLUDE_195
  test("VMALL_DICTIONARY_INCLUDE_195", Include) {

    checkAnswer(s"""select productionDate  from VMALL_DICTIONARY_INCLUDE where productionDate<>productionDate order by productionDate""",
      s"""select productionDate  from VMALL_DICTIONARY_INCLUDE_hive where productionDate<>productionDate order by productionDate""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_195")

  }


  //VMALL_DICTIONARY_INCLUDE_196
  test("VMALL_DICTIONARY_INCLUDE_196", Include) {

    checkAnswer(s"""select productionDate from VMALL_DICTIONARY_INCLUDE where productionDate != Latest_areaId order by productionDate""",
      s"""select productionDate from VMALL_DICTIONARY_INCLUDE_hive where productionDate != Latest_areaId order by productionDate""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_196")

  }


  //VMALL_DICTIONARY_INCLUDE_197
  test("VMALL_DICTIONARY_INCLUDE_197", Include) {

    checkAnswer(s"""select productionDate from VMALL_DICTIONARY_INCLUDE where Latest_areaId<productionDate order by productionDate""",
      s"""select productionDate from VMALL_DICTIONARY_INCLUDE_hive where Latest_areaId<productionDate order by productionDate""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_197")

  }


  //VMALL_DICTIONARY_INCLUDE_198
  test("VMALL_DICTIONARY_INCLUDE_198", Include) {

    checkAnswer(s"""select productionDate from VMALL_DICTIONARY_INCLUDE where productionDate<=productionDate order by productionDate""",
      s"""select productionDate from VMALL_DICTIONARY_INCLUDE_hive where productionDate<=productionDate order by productionDate""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_198")

  }


  //VMALL_DICTIONARY_INCLUDE_199
  test("VMALL_DICTIONARY_INCLUDE_199", Include) {

    checkAnswer(s"""select productionDate from VMALL_DICTIONARY_INCLUDE where productionDate <cast('2015-07-01 12:07:28.0' as timestamp) order by productionDate""",
      s"""select productionDate from VMALL_DICTIONARY_INCLUDE_hive where productionDate <cast('2015-07-01 12:07:28.0' as timestamp) order by productionDate""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_199")

  }


  //VMALL_DICTIONARY_INCLUDE_200
  test("VMALL_DICTIONARY_INCLUDE_200", Include) {

    checkAnswer(s"""select productionDate  from VMALL_DICTIONARY_INCLUDE where productionDate IS NULL""",
      s"""select productionDate  from VMALL_DICTIONARY_INCLUDE_hive where productionDate IS NULL""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_200")

  }


  //VMALL_DICTIONARY_INCLUDE_201
  test("VMALL_DICTIONARY_INCLUDE_201", Include) {

    checkAnswer(s"""select productionDate  from VMALL_DICTIONARY_INCLUDE where productionDate IS NOT NULL order by productionDate""",
      s"""select productionDate  from VMALL_DICTIONARY_INCLUDE_hive where productionDate IS NOT NULL order by productionDate""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_201")

  }


  //VMALL_DICTIONARY_INCLUDE_202
  test("VMALL_DICTIONARY_INCLUDE_202", Include) {

    checkAnswer(s"""Select count(deviceInformationId) from VMALL_DICTIONARY_INCLUDE""",
      s"""Select count(deviceInformationId) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_202")

  }


  //VMALL_DICTIONARY_INCLUDE_203
  test("VMALL_DICTIONARY_INCLUDE_203", Include) {

    checkAnswer(s"""select count(DISTINCT deviceInformationId) as a from VMALL_DICTIONARY_INCLUDE""",
      s"""select count(DISTINCT deviceInformationId) as a from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_203")

  }


  //VMALL_DICTIONARY_INCLUDE_204
  test("VMALL_DICTIONARY_INCLUDE_204", Include) {

    checkAnswer(s"""select sum(deviceInformationId)+10 as a ,deviceInformationId  from VMALL_DICTIONARY_INCLUDE group by deviceInformationId order by deviceInformationId""",
      s"""select sum(deviceInformationId)+10 as a ,deviceInformationId  from VMALL_DICTIONARY_INCLUDE_hive group by deviceInformationId order by deviceInformationId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_204")

  }


  //VMALL_DICTIONARY_INCLUDE_205
  test("VMALL_DICTIONARY_INCLUDE_205", Include) {

    checkAnswer(s"""select max(deviceInformationId),min(deviceInformationId) from VMALL_DICTIONARY_INCLUDE""",
      s"""select max(deviceInformationId),min(deviceInformationId) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_205")

  }


  //VMALL_DICTIONARY_INCLUDE_206
  test("VMALL_DICTIONARY_INCLUDE_206", Include) {

    checkAnswer(s"""select sum(deviceInformationId) a  from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(deviceInformationId) a  from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_206")

  }


  //VMALL_DICTIONARY_INCLUDE_207
  test("VMALL_DICTIONARY_INCLUDE_207", Include) {

    checkAnswer(s"""select avg(deviceInformationId) a  from VMALL_DICTIONARY_INCLUDE""",
      s"""select avg(deviceInformationId) a  from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_207")

  }


  //VMALL_DICTIONARY_INCLUDE_208
  test("VMALL_DICTIONARY_INCLUDE_208", Include) {

    checkAnswer(s"""select min(deviceInformationId) a  from VMALL_DICTIONARY_INCLUDE""",
      s"""select min(deviceInformationId) a  from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_208")

  }


  //VMALL_DICTIONARY_INCLUDE_209
  test("VMALL_DICTIONARY_INCLUDE_209", Include) {

    sql(s"""select variance(deviceInformationId) as a   from (select deviceInformationId from VMALL_DICTIONARY_INCLUDE order by deviceInformationId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_210
  ignore("VMALL_DICTIONARY_INCLUDE_210", Include) {

    checkAnswer(s"""select var_pop(deviceInformationId)  as a from VMALL_DICTIONARY_INCLUDE""",
      s"""select var_pop(deviceInformationId)  as a from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_210")

  }


  //VMALL_DICTIONARY_INCLUDE_211
  ignore("VMALL_DICTIONARY_INCLUDE_211", Include) {

    checkAnswer(s"""select var_samp(deviceInformationId) as a  from VMALL_DICTIONARY_INCLUDE""",
      s"""select var_samp(deviceInformationId) as a  from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_211")

  }


  //VMALL_DICTIONARY_INCLUDE_212
  test("VMALL_DICTIONARY_INCLUDE_212", Include) {

    sql(s"""select stddev_pop(deviceInformationId) as a  from (select deviceInformationId from VMALL_DICTIONARY_INCLUDE order by deviceInformationId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_213
  test("VMALL_DICTIONARY_INCLUDE_213", Include) {

    sql(s"""select stddev_samp(deviceInformationId)  as a from (select deviceInformationId from VMALL_DICTIONARY_INCLUDE order by deviceInformationId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_214
  test("VMALL_DICTIONARY_INCLUDE_214", Include) {

    sql(s"""select covar_pop(deviceInformationId,deviceInformationId) as a  from (select deviceInformationId from VMALL_DICTIONARY_INCLUDE order by deviceInformationId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_215
  test("VMALL_DICTIONARY_INCLUDE_215", Include) {

    sql(s"""select covar_samp(deviceInformationId,deviceInformationId) as a  from (select deviceInformationId from VMALL_DICTIONARY_INCLUDE order by deviceInformationId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_216
  test("VMALL_DICTIONARY_INCLUDE_216", Include) {

    checkAnswer(s"""select corr(deviceInformationId,deviceInformationId)  as a from VMALL_DICTIONARY_INCLUDE""",
      s"""select corr(deviceInformationId,deviceInformationId)  as a from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_216")

  }


  //VMALL_DICTIONARY_INCLUDE_217
  test("VMALL_DICTIONARY_INCLUDE_217", Include) {

    sql(s"""select percentile_approx(deviceInformationId,0.2) as a  from (select deviceInformationId from VMALL_DICTIONARY_INCLUDE order by deviceInformationId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_218
  test("VMALL_DICTIONARY_INCLUDE_218", Include) {

    sql(s"""select percentile_approx(deviceInformationId,0.2,5) as a  from (select deviceInformationId from VMALL_DICTIONARY_INCLUDE order by deviceInformationId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_219
  test("VMALL_DICTIONARY_INCLUDE_219", Include) {

    sql(s"""select percentile_approx(deviceInformationId,array(0.2,0.3,0.99))  as a from (select deviceInformationId from VMALL_DICTIONARY_INCLUDE order by deviceInformationId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_220
  test("VMALL_DICTIONARY_INCLUDE_220", Include) {

    sql(s"""select percentile_approx(deviceInformationId,array(0.2,0.3,0.99),5) as a from (select deviceInformationId from VMALL_DICTIONARY_INCLUDE order by deviceInformationId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_221
  test("VMALL_DICTIONARY_INCLUDE_221", Include) {

    sql(s"""select histogram_numeric(deviceInformationId,2)  as a from (select deviceInformationId from VMALL_DICTIONARY_INCLUDE order by deviceInformationId) t""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_222
  test("VMALL_DICTIONARY_INCLUDE_222", Include) {

    checkAnswer(s"""select deviceInformationId, deviceInformationId+ 10 as a  from VMALL_DICTIONARY_INCLUDE order by deviceInformationId""",
      s"""select deviceInformationId, deviceInformationId+ 10 as a  from VMALL_DICTIONARY_INCLUDE_hive order by deviceInformationId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_222")

  }


  //VMALL_DICTIONARY_INCLUDE_223
  test("VMALL_DICTIONARY_INCLUDE_223", Include) {

    checkAnswer(s"""select min(deviceInformationId), max(deviceInformationId+ 10) Total from VMALL_DICTIONARY_INCLUDE group by  channelsId order by Total""",
      s"""select min(deviceInformationId), max(deviceInformationId+ 10) Total from VMALL_DICTIONARY_INCLUDE_hive group by  channelsId order by Total""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_223")

  }


  //VMALL_DICTIONARY_INCLUDE_224
  test("VMALL_DICTIONARY_INCLUDE_224", Include) {

    sql(s"""select last(deviceInformationId) a from VMALL_DICTIONARY_INCLUDE order by a""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_225
  test("VMALL_DICTIONARY_INCLUDE_225", Include) {

    sql(s"""select FIRST(deviceInformationId) a from VMALL_DICTIONARY_INCLUDE order by a""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_226
  test("VMALL_DICTIONARY_INCLUDE_226", Include) {

    checkAnswer(s"""select deviceInformationId,count(deviceInformationId) a from VMALL_DICTIONARY_INCLUDE group by deviceInformationId order by deviceInformationId""",
      s"""select deviceInformationId,count(deviceInformationId) a from VMALL_DICTIONARY_INCLUDE_hive group by deviceInformationId order by deviceInformationId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_226")

  }


  //VMALL_DICTIONARY_INCLUDE_227
  test("VMALL_DICTIONARY_INCLUDE_227", Include) {

    checkAnswer(s"""select Lower(deviceInformationId) a  from VMALL_DICTIONARY_INCLUDE order by deviceInformationId""",
      s"""select Lower(deviceInformationId) a  from VMALL_DICTIONARY_INCLUDE_hive order by deviceInformationId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_227")

  }


  //VMALL_DICTIONARY_INCLUDE_228
  test("VMALL_DICTIONARY_INCLUDE_228", Include) {

    checkAnswer(s"""select distinct deviceInformationId from VMALL_DICTIONARY_INCLUDE order by deviceInformationId""",
      s"""select distinct deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive order by deviceInformationId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_228")

  }


  //VMALL_DICTIONARY_INCLUDE_229
  test("VMALL_DICTIONARY_INCLUDE_229", Include) {

    checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE order by deviceInformationId limit 101""",
      s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive order by deviceInformationId limit 101""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_229")

  }


  //VMALL_DICTIONARY_INCLUDE_230
  test("VMALL_DICTIONARY_INCLUDE_230", Include) {

    checkAnswer(s"""select deviceInformationId as a from VMALL_DICTIONARY_INCLUDE  order by a asc limit 10""",
      s"""select deviceInformationId as a from VMALL_DICTIONARY_INCLUDE_hive  order by a asc limit 10""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_230")

  }


  //VMALL_DICTIONARY_INCLUDE_231
  test("VMALL_DICTIONARY_INCLUDE_231", Include) {

    checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE where  (deviceInformationId == 100084) and (deviceInformationId==100084)""",
      s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive where  (deviceInformationId == 100084) and (deviceInformationId==100084)""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_231")

  }


  //VMALL_DICTIONARY_INCLUDE_232
  test("VMALL_DICTIONARY_INCLUDE_232", Include) {

    checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE where deviceInformationId !='100084' order by deviceInformationId""",
      s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId !='100084' order by deviceInformationId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_232")

  }


  //VMALL_DICTIONARY_INCLUDE_233
  test("VMALL_DICTIONARY_INCLUDE_233", Include) {

    checkAnswer(s"""select deviceInformationId  from VMALL_DICTIONARY_INCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select deviceInformationId  from VMALL_DICTIONARY_INCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_233")

  }


  //VMALL_DICTIONARY_INCLUDE_234
  test("VMALL_DICTIONARY_INCLUDE_234", Include) {

    checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE where deviceInformationId !=100084 order by deviceInformationId""",
      s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId !=100084 order by deviceInformationId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_234")

  }


  //VMALL_DICTIONARY_INCLUDE_235
  test("VMALL_DICTIONARY_INCLUDE_235", Include) {

    checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE where deviceInformationId >100084 order by deviceInformationId""",
      s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId >100084 order by deviceInformationId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_235")

  }


  //VMALL_DICTIONARY_INCLUDE_236
  test("VMALL_DICTIONARY_INCLUDE_236", Include) {

    checkAnswer(s"""select deviceInformationId  from VMALL_DICTIONARY_INCLUDE where deviceInformationId<>deviceInformationId order by deviceInformationId""",
      s"""select deviceInformationId  from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId<>deviceInformationId order by deviceInformationId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_236")

  }


  //VMALL_DICTIONARY_INCLUDE_237
  test("VMALL_DICTIONARY_INCLUDE_237", Include) {

    checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE where deviceInformationId != Latest_areaId order by deviceInformationId""",
      s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId != Latest_areaId order by deviceInformationId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_237")

  }


  //VMALL_DICTIONARY_INCLUDE_238
  test("VMALL_DICTIONARY_INCLUDE_238", Include) {

    checkAnswer(s"""select deviceInformationId, deviceInformationId from VMALL_DICTIONARY_INCLUDE where Latest_areaId<deviceInformationId order by deviceInformationId""",
      s"""select deviceInformationId, deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive where Latest_areaId<deviceInformationId order by deviceInformationId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_238")

  }


  //VMALL_DICTIONARY_INCLUDE_239
  test("VMALL_DICTIONARY_INCLUDE_239", Include) {

    checkAnswer(s"""select deviceInformationId, deviceInformationId from VMALL_DICTIONARY_INCLUDE where deviceInformationId<=deviceInformationId order by deviceInformationId""",
      s"""select deviceInformationId, deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId<=deviceInformationId order by deviceInformationId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_239")

  }


  //VMALL_DICTIONARY_INCLUDE_240
  test("VMALL_DICTIONARY_INCLUDE_240", Include) {

    checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE where deviceInformationId <1000 order by deviceInformationId""",
      s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId <1000 order by deviceInformationId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_240")

  }


  //VMALL_DICTIONARY_INCLUDE_241
  test("VMALL_DICTIONARY_INCLUDE_241", Include) {

    checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE where deviceInformationId >1000 order by deviceInformationId""",
      s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId >1000 order by deviceInformationId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_241")

  }


  //VMALL_DICTIONARY_INCLUDE_242
  test("VMALL_DICTIONARY_INCLUDE_242", Include) {

    checkAnswer(s"""select deviceInformationId  from VMALL_DICTIONARY_INCLUDE where deviceInformationId IS NULL order by deviceInformationId""",
      s"""select deviceInformationId  from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId IS NULL order by deviceInformationId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_242")

  }


  //VMALL_DICTIONARY_INCLUDE_243
  test("VMALL_DICTIONARY_INCLUDE_243", Include) {

    checkAnswer(s"""select deviceInformationId  from VMALL_DICTIONARY_INCLUDE where deviceInformationId IS NOT NULL order by deviceInformationId""",
      s"""select deviceInformationId  from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId IS NOT NULL order by deviceInformationId""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_243")

  }


  //VMALL_DICTIONARY_INCLUDE_244
  test("VMALL_DICTIONARY_INCLUDE_244", Include) {

    checkAnswer(s"""select sum(imei)+10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(imei)+10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_244")

  }


  //VMALL_DICTIONARY_INCLUDE_245
  test("VMALL_DICTIONARY_INCLUDE_245", Include) {

    checkAnswer(s"""select sum(imei)*10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(imei)*10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_245")

  }


  //VMALL_DICTIONARY_INCLUDE_246
  test("VMALL_DICTIONARY_INCLUDE_246", Include) {

    checkAnswer(s"""select sum(imei)/10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(imei)/10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_246")

  }


  //VMALL_DICTIONARY_INCLUDE_247
  test("VMALL_DICTIONARY_INCLUDE_247", Include) {

    checkAnswer(s"""select sum(imei)-10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(imei)-10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_247")

  }


  //VMALL_DICTIONARY_INCLUDE_248
  test("VMALL_DICTIONARY_INCLUDE_248", Include) {

    checkAnswer(s"""select sum(contractNumber)+10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(contractNumber)+10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_248")

  }


  //VMALL_DICTIONARY_INCLUDE_249
  test("VMALL_DICTIONARY_INCLUDE_249", Include) {

    checkAnswer(s"""select sum(contractNumber)*10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(contractNumber)*10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_249")

  }


  //VMALL_DICTIONARY_INCLUDE_250
  test("VMALL_DICTIONARY_INCLUDE_250", Include) {

    checkAnswer(s"""select sum(contractNumber)/10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(contractNumber)/10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_250")

  }


  //VMALL_DICTIONARY_INCLUDE_251
  test("VMALL_DICTIONARY_INCLUDE_251", Include) {

    checkAnswer(s"""select sum(contractNumber)-10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(contractNumber)-10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_251")

  }


  //VMALL_DICTIONARY_INCLUDE_252
  test("VMALL_DICTIONARY_INCLUDE_252", Include) {

    checkAnswer(s"""select sum(Latest_DAY)+10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(Latest_DAY)+10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_252")

  }


  //VMALL_DICTIONARY_INCLUDE_253
  test("VMALL_DICTIONARY_INCLUDE_253", Include) {

    checkAnswer(s"""select sum(Latest_DAY)*10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(Latest_DAY)*10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_253")

  }


  //VMALL_DICTIONARY_INCLUDE_254
  test("VMALL_DICTIONARY_INCLUDE_254", Include) {

    checkAnswer(s"""select sum(Latest_DAY)/10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(Latest_DAY)/10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_254")

  }


  //VMALL_DICTIONARY_INCLUDE_255
  test("VMALL_DICTIONARY_INCLUDE_255", Include) {

    checkAnswer(s"""select sum(Latest_DAY)-10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(Latest_DAY)-10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_255")

  }


  //VMALL_DICTIONARY_INCLUDE_256
  test("VMALL_DICTIONARY_INCLUDE_256", Include) {

    checkAnswer(s"""select sum(gamePointId)+10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(gamePointId)+10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_256")

  }


  //VMALL_DICTIONARY_INCLUDE_257
  test("VMALL_DICTIONARY_INCLUDE_257", Include) {

    checkAnswer(s"""select sum(gamePointId)*10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(gamePointId)*10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_257")

  }


  //VMALL_DICTIONARY_INCLUDE_258
  test("VMALL_DICTIONARY_INCLUDE_258", Include) {

    checkAnswer(s"""select sum(gamePointId)/10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(gamePointId)/10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_258")

  }


  //VMALL_DICTIONARY_INCLUDE_259
  test("VMALL_DICTIONARY_INCLUDE_259", Include) {

    checkAnswer(s"""select sum(gamePointId)-10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(gamePointId)-10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_259")

  }


  //VMALL_DICTIONARY_INCLUDE_260
  test("VMALL_DICTIONARY_INCLUDE_260", Include) {

    checkAnswer(s"""select sum(productionDate)+10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(productionDate)+10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_260")

  }


  //VMALL_DICTIONARY_INCLUDE_261
  test("VMALL_DICTIONARY_INCLUDE_261", Include) {

    checkAnswer(s"""select sum(productionDate)*10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(productionDate)*10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_261")

  }


  //VMALL_DICTIONARY_INCLUDE_262
  test("VMALL_DICTIONARY_INCLUDE_262", Include) {

    checkAnswer(s"""select sum(productionDate)/10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(productionDate)/10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_262")

  }


  //VMALL_DICTIONARY_INCLUDE_263
  test("VMALL_DICTIONARY_INCLUDE_263", Include) {

    checkAnswer(s"""select sum(productionDate)-10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(productionDate)-10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_263")

  }


  //VMALL_DICTIONARY_INCLUDE_264
  test("VMALL_DICTIONARY_INCLUDE_264", Include) {

    checkAnswer(s"""select sum(deviceInformationId)+10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(deviceInformationId)+10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_264")

  }


  //VMALL_DICTIONARY_INCLUDE_265
  test("VMALL_DICTIONARY_INCLUDE_265", Include) {

    checkAnswer(s"""select sum(deviceInformationId)*10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(deviceInformationId)*10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_265")

  }


  //VMALL_DICTIONARY_INCLUDE_266
  test("VMALL_DICTIONARY_INCLUDE_266", Include) {

    checkAnswer(s"""select sum(deviceInformationId)/10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(deviceInformationId)/10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_266")

  }


  //VMALL_DICTIONARY_INCLUDE_267
  test("VMALL_DICTIONARY_INCLUDE_267", Include) {

    checkAnswer(s"""select sum(deviceInformationId)-10 as a   from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(deviceInformationId)-10 as a   from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_267")

  }


  //VMALL_DICTIONARY_INCLUDE_292
  test("VMALL_DICTIONARY_INCLUDE_292", Include) {

    checkAnswer(s"""SELECT productiondate from VMALL_DICTIONARY_INCLUDE where productiondate LIKE '2015-09-30%'""",
      s"""SELECT productiondate from VMALL_DICTIONARY_INCLUDE_hive where productiondate LIKE '2015-09-30%'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_292")

  }


  //VMALL_DICTIONARY_INCLUDE_293
  test("VMALL_DICTIONARY_INCLUDE_293", Include) {

    checkAnswer(s"""SELECT productiondate from VMALL_DICTIONARY_INCLUDE where productiondate LIKE '% %'""",
      s"""SELECT productiondate from VMALL_DICTIONARY_INCLUDE_hive where productiondate LIKE '% %'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_293")

  }


  //VMALL_DICTIONARY_INCLUDE_294
  test("VMALL_DICTIONARY_INCLUDE_294", Include) {

    checkAnswer(s"""SELECT productiondate from VMALL_DICTIONARY_INCLUDE where productiondate LIKE '%12:07:28'""",
      s"""SELECT productiondate from VMALL_DICTIONARY_INCLUDE_hive where productiondate LIKE '%12:07:28'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_294")

  }


  //VMALL_DICTIONARY_INCLUDE_295
  test("VMALL_DICTIONARY_INCLUDE_295", Include) {

    checkAnswer(s"""select contractnumber from VMALL_DICTIONARY_INCLUDE where contractnumber like '922337204%' """,
      s"""select contractnumber from VMALL_DICTIONARY_INCLUDE_hive where contractnumber like '922337204%' """, "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_295")

  }


  //VMALL_DICTIONARY_INCLUDE_296
  test("VMALL_DICTIONARY_INCLUDE_296", Include) {

    checkAnswer(s"""select contractnumber from VMALL_DICTIONARY_INCLUDE where contractnumber like '%047800'""",
      s"""select contractnumber from VMALL_DICTIONARY_INCLUDE_hive where contractnumber like '%047800'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_296")

  }


  //VMALL_DICTIONARY_INCLUDE_297
  test("VMALL_DICTIONARY_INCLUDE_297", Include) {

    checkAnswer(s"""select contractnumber from VMALL_DICTIONARY_INCLUDE where contractnumber like '%720%'""",
      s"""select contractnumber from VMALL_DICTIONARY_INCLUDE_hive where contractnumber like '%720%'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_297")

  }


  //VMALL_DICTIONARY_INCLUDE_298
  test("VMALL_DICTIONARY_INCLUDE_298", Include) {

    checkAnswer(s"""SELECT Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY like '12345678%'""",
      s"""SELECT Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY like '12345678%'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_298")

  }


  //VMALL_DICTIONARY_INCLUDE_299
  test("VMALL_DICTIONARY_INCLUDE_299", Include) {

    checkAnswer(s"""SELECT Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY like '%5678%'""",
      s"""SELECT Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY like '%5678%'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_299")

  }


  //VMALL_DICTIONARY_INCLUDE_300
  test("VMALL_DICTIONARY_INCLUDE_300", Include) {

    checkAnswer(s"""SELECT Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY like '1234567%'""",
      s"""SELECT Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY like '1234567%'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_300")

  }


  //VMALL_DICTIONARY_INCLUDE_301
  test("VMALL_DICTIONARY_INCLUDE_301", Include) {

    checkAnswer(s"""SELECT gamepointID from VMALL_DICTIONARY_INCLUDE where gamepointID like '1.1098347722%'""",
      s"""SELECT gamepointID from VMALL_DICTIONARY_INCLUDE_hive where gamepointID like '1.1098347722%'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_301")

  }


  //VMALL_DICTIONARY_INCLUDE_302
  test("VMALL_DICTIONARY_INCLUDE_302", Include) {

    checkAnswer(s"""SELECT gamepointID from VMALL_DICTIONARY_INCLUDE where gamepointID like '%8347722%'""",
      s"""SELECT gamepointID from VMALL_DICTIONARY_INCLUDE_hive where gamepointID like '%8347722%'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_302")

  }


  //VMALL_DICTIONARY_INCLUDE_303
  test("VMALL_DICTIONARY_INCLUDE_303", Include) {

    checkAnswer(s"""SELECT gamepointID from VMALL_DICTIONARY_INCLUDE where gamepointID like '%7512E42'""",
      s"""SELECT gamepointID from VMALL_DICTIONARY_INCLUDE_hive where gamepointID like '%7512E42'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_303")

  }


  //VMALL_DICTIONARY_INCLUDE_304
  test("VMALL_DICTIONARY_INCLUDE_304", Include) {

    checkAnswer(s"""SELECT deviceinformationid from VMALL_DICTIONARY_INCLUDE where deviceinformationid like '1000%'""",
      s"""SELECT deviceinformationid from VMALL_DICTIONARY_INCLUDE_hive where deviceinformationid like '1000%'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_304")

  }


  //VMALL_DICTIONARY_INCLUDE_305
  test("VMALL_DICTIONARY_INCLUDE_305", Include) {

    checkAnswer(s"""SELECT deviceinformationid from VMALL_DICTIONARY_INCLUDE where deviceinformationid like '%00%'""",
      s"""SELECT deviceinformationid from VMALL_DICTIONARY_INCLUDE_hive where deviceinformationid like '%00%'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_305")

  }


  //VMALL_DICTIONARY_INCLUDE_306
  test("VMALL_DICTIONARY_INCLUDE_306", Include) {

    checkAnswer(s"""SELECT deviceinformationid from VMALL_DICTIONARY_INCLUDE where deviceinformationid like '%0084'""",
      s"""SELECT deviceinformationid from VMALL_DICTIONARY_INCLUDE_hive where deviceinformationid like '%0084'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_306")

  }


  //VMALL_DICTIONARY_INCLUDE_307
  test("VMALL_DICTIONARY_INCLUDE_307", Include) {

    checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei like '1AA10%'""",
      s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei like '1AA10%'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_307")

  }


  //VMALL_DICTIONARY_INCLUDE_308
  test("VMALL_DICTIONARY_INCLUDE_308", Include) {

    checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei like '%A10%'""",
      s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei like '%A10%'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_308")

  }


  //VMALL_DICTIONARY_INCLUDE_309
  test("VMALL_DICTIONARY_INCLUDE_309", Include) {

    checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei like '%00084'""",
      s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei like '%00084'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_309")

  }


  //VMALL_DICTIONARY_INCLUDE_310
  test("VMALL_DICTIONARY_INCLUDE_310", Include) {

    checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei in ('1AA100074','1AA100075','1AA100077')""",
      s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei in ('1AA100074','1AA100075','1AA100077')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_310")

  }


  //VMALL_DICTIONARY_INCLUDE_311
  test("VMALL_DICTIONARY_INCLUDE_311", Include) {

    checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei not in ('1AA100074','1AA100075','1AA100077')""",
      s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei not in ('1AA100074','1AA100075','1AA100077')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_311")

  }


  //VMALL_DICTIONARY_INCLUDE_312
  test("VMALL_DICTIONARY_INCLUDE_312", Include) {

    checkAnswer(s"""select deviceinformationid from VMALL_DICTIONARY_INCLUDE where deviceinformationid in (100081,100078,10008)""",
      s"""select deviceinformationid from VMALL_DICTIONARY_INCLUDE_hive where deviceinformationid in (100081,100078,10008)""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_312")

  }


  //VMALL_DICTIONARY_INCLUDE_313
  test("VMALL_DICTIONARY_INCLUDE_313", Include) {

    checkAnswer(s"""select deviceinformationid from VMALL_DICTIONARY_INCLUDE where deviceinformationid not in (100081,100078,10008)""",
      s"""select deviceinformationid from VMALL_DICTIONARY_INCLUDE_hive where deviceinformationid not in (100081,100078,10008)""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_313")

  }


  //VMALL_DICTIONARY_INCLUDE_314
  test("VMALL_DICTIONARY_INCLUDE_314", Include) {

    checkAnswer(s"""select productiondate from VMALL_DICTIONARY_INCLUDE where productiondate in ('2015-10-04 12:07:28','2015-10-07%','2015-10-07 12:07:28')""",
      s"""select productiondate from VMALL_DICTIONARY_INCLUDE_hive where productiondate in ('2015-10-04 12:07:28','2015-10-07%','2015-10-07 12:07:28')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_314")

  }


  //VMALL_DICTIONARY_INCLUDE_315
  test("VMALL_DICTIONARY_INCLUDE_315", Include) {

    checkAnswer(s"""select productiondate from VMALL_DICTIONARY_INCLUDE where productiondate not in (cast('2015-10-04 12:07:28' as timestamp),cast('2015-10-07 12:07:28' as timestamp))""",
      s"""select productiondate from VMALL_DICTIONARY_INCLUDE_hive where productiondate not in (cast('2015-10-04 12:07:28' as timestamp),cast('2015-10-07 12:07:28' as timestamp))""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_315")

  }


  //VMALL_DICTIONARY_INCLUDE_316
  test("VMALL_DICTIONARY_INCLUDE_316", Include) {

    checkAnswer(s"""select gamepointid from VMALL_DICTIONARY_INCLUDE where gamepointid in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""",
      s"""select gamepointid from VMALL_DICTIONARY_INCLUDE_hive where gamepointid in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_316")

  }


  //VMALL_DICTIONARY_INCLUDE_317
  test("VMALL_DICTIONARY_INCLUDE_317", Include) {

    checkAnswer(s"""select gamepointid from VMALL_DICTIONARY_INCLUDE where gamepointid not in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""",
      s"""select gamepointid from VMALL_DICTIONARY_INCLUDE_hive where gamepointid not in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_317")

  }


  //VMALL_DICTIONARY_INCLUDE_318
  test("VMALL_DICTIONARY_INCLUDE_318", Include) {

    checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY in (1234567890123530.0000000000,1234567890123520.0000000000)""",
      s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY in (1234567890123530.0000000000,1234567890123520.0000000000)""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_318")

  }


  //VMALL_DICTIONARY_INCLUDE_319
  test("VMALL_DICTIONARY_INCLUDE_319", Include) {

    checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY not in (1234567890123530.0000000000,1234567890123520.0000000000)""",
      s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY not in (1234567890123530.0000000000,1234567890123520.0000000000)""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_319")

  }


  //VMALL_DICTIONARY_INCLUDE_322
  test("VMALL_DICTIONARY_INCLUDE_322", Include) {

    checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei !='1AA100077'""",
      s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei !='1AA100077'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_322")

  }


  //VMALL_DICTIONARY_INCLUDE_323
  test("VMALL_DICTIONARY_INCLUDE_323", Include) {

    checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei NOT LIKE '1AA100077'""",
      s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei NOT LIKE '1AA100077'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_323")

  }


  //VMALL_DICTIONARY_INCLUDE_324
  test("VMALL_DICTIONARY_INCLUDE_324", Include) {

    checkAnswer(s"""select deviceinformationid from VMALL_DICTIONARY_INCLUDE where deviceinformationid !=100078""",
      s"""select deviceinformationid from VMALL_DICTIONARY_INCLUDE_hive where deviceinformationid !=100078""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_324")

  }


  //VMALL_DICTIONARY_INCLUDE_325
  test("VMALL_DICTIONARY_INCLUDE_325", Include) {

    checkAnswer(s"""select deviceinformationid from VMALL_DICTIONARY_INCLUDE where deviceinformationid NOT LIKE 100079""",
      s"""select deviceinformationid from VMALL_DICTIONARY_INCLUDE_hive where deviceinformationid NOT LIKE 100079""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_325")

  }


  //VMALL_DICTIONARY_INCLUDE_326
  test("VMALL_DICTIONARY_INCLUDE_326", Include) {

    checkAnswer(s"""select productiondate from VMALL_DICTIONARY_INCLUDE where productiondate !='2015-10-07 12:07:28'""",
      s"""select productiondate from VMALL_DICTIONARY_INCLUDE_hive where productiondate !='2015-10-07 12:07:28'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_326")

  }


  //VMALL_DICTIONARY_INCLUDE_327
  ignore("VMALL_DICTIONARY_INCLUDE_327", Include) {

    checkAnswer(s"""select productiondate from VMALL_DICTIONARY_INCLUDE where productiondate NOT LIKE '2015-10-07 12:07:28'""",
      s"""select productiondate from VMALL_DICTIONARY_INCLUDE_hive where productiondate NOT LIKE '2015-10-07 12:07:28'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_327")

  }


  //VMALL_DICTIONARY_INCLUDE_328
  test("VMALL_DICTIONARY_INCLUDE_328", Include) {

    checkAnswer(s"""select gamepointid from VMALL_DICTIONARY_INCLUDE where gamepointid !=6.8591561117512E42""",
      s"""select gamepointid from VMALL_DICTIONARY_INCLUDE_hive where gamepointid !=6.8591561117512E42""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_328")

  }


  //VMALL_DICTIONARY_INCLUDE_329
  test("VMALL_DICTIONARY_INCLUDE_329", Include) {

    checkAnswer(s"""select gamepointid from VMALL_DICTIONARY_INCLUDE where gamepointid NOT LIKE 6.8591561117512E43""",
      s"""select gamepointid from VMALL_DICTIONARY_INCLUDE_hive where gamepointid NOT LIKE 6.8591561117512E43""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_329")

  }


  //VMALL_DICTIONARY_INCLUDE_330
  test("VMALL_DICTIONARY_INCLUDE_330", Include) {

    checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY != 1234567890123520.0000000000""",
      s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY != 1234567890123520.0000000000""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_330")

  }


  //VMALL_DICTIONARY_INCLUDE_331
  test("VMALL_DICTIONARY_INCLUDE_331", Include) {

    checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY NOT LIKE 1234567890123520.0000000000""",
      s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY NOT LIKE 1234567890123520.0000000000""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_331")

  }


  //VMALL_DICTIONARY_INCLUDE_335
  test("VMALL_DICTIONARY_INCLUDE_335", Include) {

    checkAnswer(s"""SELECT productiondate,IMEI from VMALL_DICTIONARY_INCLUDE where IMEI RLIKE '1AA100077'""",
      s"""SELECT productiondate,IMEI from VMALL_DICTIONARY_INCLUDE_hive where IMEI RLIKE '1AA100077'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_335")

  }


  //VMALL_DICTIONARY_INCLUDE_336
  test("VMALL_DICTIONARY_INCLUDE_336", Include) {

    checkAnswer(s"""SELECT deviceinformationId from VMALL_DICTIONARY_INCLUDE where deviceinformationId RLIKE '100079'""",
      s"""SELECT deviceinformationId from VMALL_DICTIONARY_INCLUDE_hive where deviceinformationId RLIKE '100079'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_336")

  }


  //VMALL_DICTIONARY_INCLUDE_337
  test("VMALL_DICTIONARY_INCLUDE_337", Include) {

    checkAnswer(s"""SELECT gamepointid from VMALL_DICTIONARY_INCLUDE where gamepointid RLIKE '1.61922711065643E42'""",
      s"""SELECT gamepointid from VMALL_DICTIONARY_INCLUDE_hive where gamepointid RLIKE '1.61922711065643E42'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_337")

  }


  //VMALL_DICTIONARY_INCLUDE_338
  test("VMALL_DICTIONARY_INCLUDE_338", Include) {

    checkAnswer(s"""SELECT Latest_Day from VMALL_DICTIONARY_INCLUDE where Latest_Day RLIKE '1234567890123550.0000000000'""",
      s"""SELECT Latest_Day from VMALL_DICTIONARY_INCLUDE_hive where Latest_Day RLIKE '1234567890123550.0000000000'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_338")

  }


  //VMALL_DICTIONARY_INCLUDE_339
  test("VMALL_DICTIONARY_INCLUDE_339", Include) {

    checkAnswer(s"""SELECT contractnumber from VMALL_DICTIONARY_INCLUDE where contractnumber RLIKE '9223372047800'""",
      s"""SELECT contractnumber from VMALL_DICTIONARY_INCLUDE_hive where contractnumber RLIKE '9223372047800'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_339")

  }


  //VMALL_DICTIONARY_INCLUDE_340
  test("VMALL_DICTIONARY_INCLUDE_340", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE a join vmall_dictionary_include b on a.productiondate=b.productiondate""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE_hive a join vmall_dictionary_include_hive b on a.productiondate=b.productiondate""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_340")

  }


  //VMALL_DICTIONARY_INCLUDE_341
  test("VMALL_DICTIONARY_INCLUDE_341", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE a join vmall_dictionary_include b on a.deviceinformationid=b.deviceinformationid""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE_hive a join vmall_dictionary_include_hive b on a.deviceinformationid=b.deviceinformationid""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_341")

  }


  //VMALL_DICTIONARY_INCLUDE_342
  test("VMALL_DICTIONARY_INCLUDE_342", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE a join vmall_dictionary_include b on a.imei=b.imei""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE_hive a join vmall_dictionary_include_hive b on a.imei=b.imei""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_342")

  }


  //VMALL_DICTIONARY_INCLUDE_343
  test("VMALL_DICTIONARY_INCLUDE_343", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE a join vmall_dictionary_include b on a.gamepointid=b.gamepointid""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE_hive a join vmall_dictionary_include_hive b on a.gamepointid=b.gamepointid""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_343")

  }


  //VMALL_DICTIONARY_INCLUDE_344
  test("VMALL_DICTIONARY_INCLUDE_344", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE a join vmall_dictionary_include b on a.Latest_Day=b.Latest_Day""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE_hive a join vmall_dictionary_include_hive b on a.Latest_Day=b.Latest_Day""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_344")

  }


  //VMALL_DICTIONARY_INCLUDE_345
  test("VMALL_DICTIONARY_INCLUDE_345", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE a join vmall_dictionary_include b on a.contractnumber=b.contractnumber""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE_hive a join vmall_dictionary_include_hive b on a.contractnumber=b.contractnumber""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_345")

  }


  //VMALL_DICTIONARY_INCLUDE_346
  test("VMALL_DICTIONARY_INCLUDE_346", Include) {

    checkAnswer(s"""select count( contractNumber ),sum( contractNumber ),count(distinct contractNumber ),avg( contractNumber ),max( contractNumber ),min( contractNumber ),1 from VMALL_DICTIONARY_INCLUDE""",
      s"""select count( contractNumber ),sum( contractNumber ),count(distinct contractNumber ),avg( contractNumber ),max( contractNumber ),min( contractNumber ),1 from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_346")

  }


  //VMALL_DICTIONARY_INCLUDE_347
  test("VMALL_DICTIONARY_INCLUDE_347", Include) {

    checkAnswer(s"""select count( Latest_Day ),sum( Latest_Day ),count(distinct Latest_Day ),avg( Latest_Day ),max( Latest_Day ),min( Latest_Day ),1 from VMALL_DICTIONARY_INCLUDE""",
      s"""select count( Latest_Day ),sum( Latest_Day ),count(distinct Latest_Day ),avg( Latest_Day ),max( Latest_Day ),min( Latest_Day ),1 from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_347")

  }


  //VMALL_DICTIONARY_INCLUDE_348
  ignore("VMALL_DICTIONARY_INCLUDE_348", Include) {

    checkAnswer(s"""select count( gamePointId),sum( gamePointId ),count(distinct gamePointId ),avg(gamePointId),max(gamePointId),min(gamePointId),1 from VMALL_DICTIONARY_INCLUDE""",
      s"""select count( gamePointId),sum( gamePointId ),count(distinct gamePointId ),avg(gamePointId),max(gamePointId),min(gamePointId),1 from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_348")

  }


  //VMALL_DICTIONARY_INCLUDE_349
  test("VMALL_DICTIONARY_INCLUDE_349", Include) {

    checkAnswer(s"""select count(deviceInformationId),sum(deviceInformationId),count(deviceInformationId),avg(deviceInformationId),max(deviceInformationId),min(deviceInformationId),1 from VMALL_DICTIONARY_INCLUDE""",
      s"""select count(deviceInformationId),sum(deviceInformationId),count(deviceInformationId),avg(deviceInformationId),max(deviceInformationId),min(deviceInformationId),1 from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_349")

  }


  //VMALL_DICTIONARY_INCLUDE_350
  test("VMALL_DICTIONARY_INCLUDE_350", Include) {

    checkAnswer(s"""select count( productionDate),sum(  productionDate ),count(distinct  productionDate ),avg(  productionDate ),max(  productionDate),min(  productionDate ),1 from VMALL_DICTIONARY_INCLUDE""",
      s"""select count( productionDate),sum(  productionDate ),count(distinct  productionDate ),avg(  productionDate ),max(  productionDate),min(  productionDate ),1 from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_350")

  }


  //VMALL_DICTIONARY_INCLUDE_351
  test("VMALL_DICTIONARY_INCLUDE_351", Include) {

    checkAnswer(s"""select count(IMEI ),sum(IMEI ),count(distinct IMEI ),avg(IMEI ),max(IMEI ),min(IMEI ),1 from VMALL_DICTIONARY_INCLUDE""",
      s"""select count(IMEI ),sum(IMEI ),count(distinct IMEI ),avg(IMEI ),max(IMEI ),min(IMEI ),1 from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_351")

  }


  //VMALL_DICTIONARY_INCLUDE_352
  test("VMALL_DICTIONARY_INCLUDE_352", Include) {

    checkAnswer(s"""select sum(contractNumber),count(contractNumber),avg(contractNumber),sum(contractNumber)/count(contractNumber) from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(contractNumber),count(contractNumber),avg(contractNumber),sum(contractNumber)/count(contractNumber) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_352")

  }


  //VMALL_DICTIONARY_INCLUDE_353
  test("VMALL_DICTIONARY_INCLUDE_353", Include) {

    checkAnswer(s"""select sum(Latest_Day),count(Latest_Day),avg(Latest_Day),sum(Latest_Day)/count(Latest_Day) from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(Latest_Day),count(Latest_Day),avg(Latest_Day),sum(Latest_Day)/count(Latest_Day) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_353")

  }


  //VMALL_DICTIONARY_INCLUDE_354
  test("VMALL_DICTIONARY_INCLUDE_354", Include) {

    checkAnswer(s"""select sum(gamepointId),count(gamepointId),avg(gamepointID),sum(gamepointID)/count(gamepointID) from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(gamepointId),count(gamepointId),avg(gamepointID),sum(gamepointID)/count(gamepointID) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_354")

  }


  //VMALL_DICTIONARY_INCLUDE_355
  test("VMALL_DICTIONARY_INCLUDE_355", Include) {

    checkAnswer(s"""select sum(deviceinformationId),count(deviceinformationId),avg(deviceinformationId),sum(deviceinformationId)/count(deviceinformationId) from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(deviceinformationId),count(deviceinformationId),avg(deviceinformationId),sum(deviceinformationId)/count(deviceinformationId) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_355")

  }


  //VMALL_DICTIONARY_INCLUDE_356
  test("VMALL_DICTIONARY_INCLUDE_356", Include) {

    checkAnswer(s"""select sum(IMEI),count(IMEI),avg(IMEI),sum(IMEI)/count(IMEI) from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(IMEI),count(IMEI),avg(IMEI),sum(IMEI)/count(IMEI) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_356")

  }


  //VMALL_DICTIONARY_INCLUDE_357
  test("VMALL_DICTIONARY_INCLUDE_357", Include) {

    checkAnswer(s"""select sum(productionDate),count(productionDate),avg(productionDate),sum(productionDate)/count(productionDate) from VMALL_DICTIONARY_INCLUDE""",
      s"""select sum(productionDate),count(productionDate),avg(productionDate),sum(productionDate)/count(productionDate) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_357")

  }


  //VMALL_DICTIONARY_INCLUDE_358
  test("VMALL_DICTIONARY_INCLUDE_358", Include) {

    checkAnswer(s"""select contractNumber,Latest_DAY,gamePointId,productionDate,deviceInformationId,IMEI  from VMALL_DICTIONARY_INCLUDE""",
      s"""select contractNumber,Latest_DAY,gamePointId,productionDate,deviceInformationId,IMEI  from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_358")

  }


  //VMALL_DICTIONARY_INCLUDE_359
  test("VMALL_DICTIONARY_INCLUDE_359", Include) {

    checkAnswer(s"""select count(MAC) from VMALL_DICTIONARY_INCLUDE""",
      s"""select count(MAC) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_359")

  }


  //VMALL_DICTIONARY_INCLUDE_360
  test("VMALL_DICTIONARY_INCLUDE_360", Include) {

    checkAnswer(s"""select count(gamePointId) from VMALL_DICTIONARY_INCLUDE""",
      s"""select count(gamePointId) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_360")

  }


  //VMALL_DICTIONARY_INCLUDE_361
  test("VMALL_DICTIONARY_INCLUDE_361", Include) {

    checkAnswer(s"""select count(contractNumber) from VMALL_DICTIONARY_INCLUDE""",
      s"""select count(contractNumber) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_361")

  }


  //VMALL_DICTIONARY_INCLUDE_362
  test("VMALL_DICTIONARY_INCLUDE_362", Include) {

    checkAnswer(s"""select count(Latest_DAY) from VMALL_DICTIONARY_INCLUDE""",
      s"""select count(Latest_DAY) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_362")

  }


  //VMALL_DICTIONARY_INCLUDE_363
  test("VMALL_DICTIONARY_INCLUDE_363", Include) {

    checkAnswer(s"""select count(productionDate) from VMALL_DICTIONARY_INCLUDE""",
      s"""select count(productionDate) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_363")

  }


  //VMALL_DICTIONARY_INCLUDE_364
  test("VMALL_DICTIONARY_INCLUDE_364", Include) {

    checkAnswer(s"""select count(deviceInformationId) from VMALL_DICTIONARY_INCLUDE""",
      s"""select count(deviceInformationId) from VMALL_DICTIONARY_INCLUDE_hive""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_364")

  }


  //VMALL_DICTIONARY_INCLUDE_365
  test("VMALL_DICTIONARY_INCLUDE_365", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE where  contractNumber  != '9223372047700'""",
      s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE_hive where  contractNumber  != '9223372047700'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_365")

  }


  //VMALL_DICTIONARY_INCLUDE_366
  test("VMALL_DICTIONARY_INCLUDE_366", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE where  Latest_DAY  != '1234567890123480.0000000000' order by imei,deviceInformationId,MAC,deviceColor limit 5""",
      s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE_hive where  Latest_DAY  != '1234567890123480.0000000000' order by imei,deviceInformationId,MAC,deviceColor limit 5""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_366")

  }


  //VMALL_DICTIONARY_INCLUDE_367
  test("VMALL_DICTIONARY_INCLUDE_367", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE where  gamePointId  != '2.27852521808948E36' order by imei,deviceInformationId,MAC limit 5""",
      s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE_hive where  gamePointId  != '2.27852521808948E36' order by imei,deviceInformationId,MAC limit 5""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_367")

  }


  //VMALL_DICTIONARY_INCLUDE_368
  test("VMALL_DICTIONARY_INCLUDE_368", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE where  productionDate  != '2015-09-18 12:07:28.0' order by imei,deviceInformationId,MAC limit 5""",
      s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE_hive where  productionDate  != '2015-09-18 12:07:28.0' order by imei,deviceInformationId,MAC limit 5""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_368")

  }


  //VMALL_DICTIONARY_INCLUDE_369
  test("VMALL_DICTIONARY_INCLUDE_369", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE where  deviceInformationId  != '100075' order by imei,deviceInformationId,MAC limit 5""",
      s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE_hive where  deviceInformationId  != '100075' order by imei,deviceInformationId,MAC limit 5""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_369")

  }


  //VMALL_DICTIONARY_INCLUDE_370
  test("VMALL_DICTIONARY_INCLUDE_370", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE where  contractNumber  not like '9223372047700' order by  imei,deviceInformationId,MAC limit 5""",
      s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE_hive where  contractNumber  not like '9223372047700' order by  imei,deviceInformationId,MAC limit 5""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_370")

  }


  //VMALL_DICTIONARY_INCLUDE_371
  test("VMALL_DICTIONARY_INCLUDE_371", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE where  Latest_DAY  not like '1234567890123480.0000000000' order by deviceInformationId limit 5""",
      s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE_hive where  Latest_DAY  not like '1234567890123480.0000000000' order by deviceInformationId limit 5""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_371")

  }


  //VMALL_DICTIONARY_INCLUDE_372
  test("VMALL_DICTIONARY_INCLUDE_372", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE where  gamePointId  not like '2.27852521808948E36' order by imei,deviceInformationId,MAC limit 5""",
      s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE_hive where  gamePointId  not like '2.27852521808948E36' order by imei,deviceInformationId,MAC limit 5""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_372")

  }


  //VMALL_DICTIONARY_INCLUDE_373
  test("VMALL_DICTIONARY_INCLUDE_373", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE where  productionDate  not like '2015-09-18 12:07:28.0' order by imei,deviceInformationId,MAC limit 5""",
      s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE_hive where  productionDate  not like '2015-09-18 12:07:28.0' order by imei,deviceInformationId,MAC limit 5""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_373")

  }


  //VMALL_DICTIONARY_INCLUDE_374
  test("VMALL_DICTIONARY_INCLUDE_374", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE where  deviceInformationId  not like '100075' order by imei,deviceInformationId,MAC limit 5""",
      s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE_hive where  deviceInformationId  not like '100075' order by imei,deviceInformationId,MAC limit 5""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_374")

  }


  //VMALL_DICTIONARY_INCLUDE_375
  test("VMALL_DICTIONARY_INCLUDE_375", Include) {

    checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei is not null""",
      s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei is not null""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_375")

  }


  //VMALL_DICTIONARY_INCLUDE_376
  test("VMALL_DICTIONARY_INCLUDE_376", Include) {

    checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_INCLUDE where gamePointId is not null""",
      s"""select gamePointId from VMALL_DICTIONARY_INCLUDE_hive where gamePointId is not null""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_376")

  }


  //VMALL_DICTIONARY_INCLUDE_377
  test("VMALL_DICTIONARY_INCLUDE_377", Include) {

    checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_INCLUDE where contractNumber is not null""",
      s"""select contractNumber from VMALL_DICTIONARY_INCLUDE_hive where contractNumber is not null""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_377")

  }


  //VMALL_DICTIONARY_INCLUDE_378
  test("VMALL_DICTIONARY_INCLUDE_378", Include) {

    checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY is not null""",
      s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY is not null""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_378")

  }


  //VMALL_DICTIONARY_INCLUDE_379
  test("VMALL_DICTIONARY_INCLUDE_379", Include) {

    checkAnswer(s"""select productionDate from VMALL_DICTIONARY_INCLUDE where productionDate is not null""",
      s"""select productionDate from VMALL_DICTIONARY_INCLUDE_hive where productionDate is not null""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_379")

  }


  //VMALL_DICTIONARY_INCLUDE_380
  test("VMALL_DICTIONARY_INCLUDE_380", Include) {

    checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE where deviceInformationId is not null""",
      s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId is not null""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_380")

  }


  //VMALL_DICTIONARY_INCLUDE_381
  test("VMALL_DICTIONARY_INCLUDE_381", Include) {

    checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei is  null""",
      s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei is  null""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_381")

  }


  //VMALL_DICTIONARY_INCLUDE_382
  test("VMALL_DICTIONARY_INCLUDE_382", Include) {

    checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_INCLUDE where gamePointId is  null""",
      s"""select gamePointId from VMALL_DICTIONARY_INCLUDE_hive where gamePointId is  null""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_382")

  }


  //VMALL_DICTIONARY_INCLUDE_383
  test("VMALL_DICTIONARY_INCLUDE_383", Include) {

    checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_INCLUDE where contractNumber is  null""",
      s"""select contractNumber from VMALL_DICTIONARY_INCLUDE_hive where contractNumber is  null""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_383")

  }


  //VMALL_DICTIONARY_INCLUDE_384
  test("VMALL_DICTIONARY_INCLUDE_384", Include) {

    checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY is  null""",
      s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY is  null""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_384")

  }


  //VMALL_DICTIONARY_INCLUDE_385
  test("VMALL_DICTIONARY_INCLUDE_385", Include) {

    checkAnswer(s"""select productionDate from VMALL_DICTIONARY_INCLUDE where productionDate is  null""",
      s"""select productionDate from VMALL_DICTIONARY_INCLUDE_hive where productionDate is  null""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_385")

  }


  //VMALL_DICTIONARY_INCLUDE_386
  test("VMALL_DICTIONARY_INCLUDE_386", Include) {

    checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE where deviceInformationId is  null""",
      s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId is  null""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_386")

  }


  //VMALL_DICTIONARY_INCLUDE_387
  test("VMALL_DICTIONARY_INCLUDE_387", Include) {

    checkAnswer(s"""select count(*) from VMALL_DICTIONARY_INCLUDE where imei = '1AA1'""",
      s"""select count(*) from VMALL_DICTIONARY_INCLUDE_hive where imei = '1AA1'""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_387")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_001
  test("VMALL_DICTIONARY_INCLUDE_PushUP_001", Include) {

    checkAnswer(s"""select count(imei)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000'  and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site'  and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(imei)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000'  and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site'  and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_001")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_002
  test("VMALL_DICTIONARY_INCLUDE_PushUP_002", Include) {

    checkAnswer(s"""select count(deviceinformationId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site'  and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(deviceinformationId)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site'  and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_002")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_003
  test("VMALL_DICTIONARY_INCLUDE_PushUP_003", Include) {

    checkAnswer(s"""select count(productionDate)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site'  and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(productionDate)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site'  and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_003")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_004
  test("VMALL_DICTIONARY_INCLUDE_PushUP_004", Include) {

    checkAnswer(s"""select count(gamePointId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site'  and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(gamePointId)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site'  and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_004")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_005
  test("VMALL_DICTIONARY_INCLUDE_PushUP_005", Include) {

    checkAnswer(s"""select count(Latest_DAY)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site'  and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(Latest_DAY)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site'  and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_005")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_006
  test("VMALL_DICTIONARY_INCLUDE_PushUP_006", Include) {

    checkAnswer(s"""select count(contractNumber)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site'  and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(contractNumber)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site'  and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_006")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_007
  test("VMALL_DICTIONARY_INCLUDE_PushUP_007", Include) {

    checkAnswer(s"""select count(distinct imei)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site'  and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(distinct imei)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site'  and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_007")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_008
  test("VMALL_DICTIONARY_INCLUDE_PushUP_008", Include) {

    checkAnswer(s"""select count(distinct deviceinformationId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(distinct deviceinformationId)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_008")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_009
  test("VMALL_DICTIONARY_INCLUDE_PushUP_009", Include) {

    checkAnswer(s"""select count(distinct productionDate)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(distinct productionDate)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_009")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_010
  test("VMALL_DICTIONARY_INCLUDE_PushUP_010", Include) {

    checkAnswer(s"""select count(distinct gamePointId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(distinct gamePointId)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_010")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_011
  test("VMALL_DICTIONARY_INCLUDE_PushUP_011", Include) {

    checkAnswer(s"""select count(distinct Latest_DAY)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(distinct Latest_DAY)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_011")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_012
  test("VMALL_DICTIONARY_INCLUDE_PushUP_012", Include) {

    checkAnswer(s"""select count(distinct contractNumber)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select count(distinct contractNumber)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_012")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_013
  test("VMALL_DICTIONARY_INCLUDE_PushUP_013", Include) {

    checkAnswer(s"""select sum(deviceinformationId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select sum(deviceinformationId)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_013")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_014
  test("VMALL_DICTIONARY_INCLUDE_PushUP_014", Include) {

    checkAnswer(s"""select sum(productionDate)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select sum(productionDate)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_014")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_015
  test("VMALL_DICTIONARY_INCLUDE_PushUP_015", Include) {

    sql(s"""select sum(gamePointId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_016
  test("VMALL_DICTIONARY_INCLUDE_PushUP_016", Include) {

    checkAnswer(s"""select sum(Latest_DAY)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select sum(Latest_DAY)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_016")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_017
  test("VMALL_DICTIONARY_INCLUDE_PushUP_017", Include) {

    checkAnswer(s"""select sum(contractNumber)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select sum(contractNumber)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_017")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_018
  test("VMALL_DICTIONARY_INCLUDE_PushUP_018", Include) {

    checkAnswer(s"""select sum(distinct deviceinformationId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select sum(distinct deviceinformationId)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_018")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_019
  test("VMALL_DICTIONARY_INCLUDE_PushUP_019", Include) {

    sql(s"""select sum(distinct gamePointId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_020
  test("VMALL_DICTIONARY_INCLUDE_PushUP_020", Include) {

    checkAnswer(s"""select sum(distinct Latest_DAY)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select sum(distinct Latest_DAY)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_020")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_021
  test("VMALL_DICTIONARY_INCLUDE_PushUP_021", Include) {

    checkAnswer(s"""select sum(distinct contractNumber)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select sum(distinct contractNumber)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_021")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_022
  test("VMALL_DICTIONARY_INCLUDE_PushUP_022", Include) {

    checkAnswer(s"""select min(deviceinformationId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select min(deviceinformationId)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_022")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_023
  test("VMALL_DICTIONARY_INCLUDE_PushUP_023", Include) {

    checkAnswer(s"""select min(productionDate)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select min(productionDate)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_023")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_024
  test("VMALL_DICTIONARY_INCLUDE_PushUP_024", Include) {

    checkAnswer(s"""select min(gamePointId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select min(gamePointId)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_024")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_025
  test("VMALL_DICTIONARY_INCLUDE_PushUP_025", Include) {

    checkAnswer(s"""select min(Latest_DAY)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select min(Latest_DAY)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_025")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_026
  test("VMALL_DICTIONARY_INCLUDE_PushUP_026", Include) {

    checkAnswer(s"""select min(contractNumber)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select min(contractNumber)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_026")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_027
  test("VMALL_DICTIONARY_INCLUDE_PushUP_027", Include) {

    checkAnswer(s"""select max(imei)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select max(imei)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_027")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_028
  test("VMALL_DICTIONARY_INCLUDE_PushUP_028", Include) {

    checkAnswer(s"""select max(deviceinformationId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select max(deviceinformationId)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_028")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_029
  test("VMALL_DICTIONARY_INCLUDE_PushUP_029", Include) {

    checkAnswer(s"""select max(productionDate)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select max(productionDate)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_029")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_030
  test("VMALL_DICTIONARY_INCLUDE_PushUP_030", Include) {

    checkAnswer(s"""select max(gamePointId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select max(gamePointId)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_030")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_031
  test("VMALL_DICTIONARY_INCLUDE_PushUP_031", Include) {

    checkAnswer(s"""select max(Latest_DAY)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select max(Latest_DAY)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_031")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_032
  test("VMALL_DICTIONARY_INCLUDE_PushUP_032", Include) {

    checkAnswer(s"""select max(contractNumber)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select max(contractNumber)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_032")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_033
  test("VMALL_DICTIONARY_INCLUDE_PushUP_033", Include) {

    checkAnswer(s"""select variance(deviceInformationId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select variance(deviceInformationId)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_033")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_034
  ignore("VMALL_DICTIONARY_INCLUDE_PushUP_034", Include) {

    checkAnswer(s"""select variance(gamepointid) from (select * from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by gamePointId)""",
      s"""select variance(gamepointid) from (select * from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by gamePointId)""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_034")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_035
  ignore("VMALL_DICTIONARY_INCLUDE_PushUP_035", Include) {

    checkAnswer(s"""select variance(gamePointId) from (select * from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by gamePointId)""",
      s"""select variance(gamePointId) from (select * from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by gamePointId)""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_035")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_036
  ignore("VMALL_DICTIONARY_INCLUDE_PushUP_036", Include) {

    checkAnswer(s"""select variance(Latest_DAY) from (select * from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by Latest_DAY)""",
      s"""select variance(Latest_DAY) from (select * from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by Latest_DAY)""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_036")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_037
  test("VMALL_DICTIONARY_INCLUDE_PushUP_037", Include) {

    checkAnswer(s"""select variance(contractNumber) from (select * from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by contractNumber)""",
      s"""select variance(contractNumber) from (select * from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by contractNumber)""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_037")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_038
  test("VMALL_DICTIONARY_INCLUDE_PushUP_038", Include) {

    checkAnswer(s"""select var_samp(deviceInformationId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select var_samp(deviceInformationId)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_038")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_039
  test("VMALL_DICTIONARY_INCLUDE_PushUP_039", Include) {

    checkAnswer(s"""select var_samp(gamepointid) from (select * from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by gamePointId)""",
      s"""select var_samp(gamepointid) from (select * from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by gamePointId)""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_039")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_040
  test("VMALL_DICTIONARY_INCLUDE_PushUP_040", Include) {

    sql(s"""select var_samp(gamePointId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_041
  ignore("VMALL_DICTIONARY_INCLUDE_PushUP_041", Include) {

    checkAnswer(s"""select var_samp(Latest_DAY) from (select * from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by Latest_DAY)""",
      s"""select var_samp(Latest_DAY) from (select * from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by Latest_DAY)""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_041")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_042
  ignore("VMALL_DICTIONARY_INCLUDE_PushUP_042", Include) {

    checkAnswer(s"""select var_samp(contractNumber)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select var_samp(contractNumber)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_042")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_043
  ignore("VMALL_DICTIONARY_INCLUDE_PushUP_043", Include) {

    checkAnswer(s"""select stddev_pop(deviceInformationId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select stddev_pop(deviceInformationId)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_043")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_044
  test("VMALL_DICTIONARY_INCLUDE_PushUP_044", Include) {

    checkAnswer(s"""select stddev_pop(gamepointid) from (select * from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by gamePointId)""",
      s"""select stddev_pop(gamepointid) from (select * from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by gamePointId)""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_044")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_045
  test("VMALL_DICTIONARY_INCLUDE_PushUP_045", Include) {

    sql(s"""select stddev_pop(gamePointId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_046
  ignore("VMALL_DICTIONARY_INCLUDE_PushUP_046", Include) {

    checkAnswer(s"""select stddev_pop(Latest_DAY) from (select * from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by Latest_DAY)""",
      s"""select stddev_pop(Latest_DAY) from (select * from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by Latest_DAY)""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_046")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_047
  ignore("VMALL_DICTIONARY_INCLUDE_PushUP_047", Include) {

    checkAnswer(s"""select stddev_pop(contractNumber)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select stddev_pop(contractNumber)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_047")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_048
  test("VMALL_DICTIONARY_INCLUDE_PushUP_048", Include) {

    checkAnswer(s"""select stddev_samp(deviceInformationId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select stddev_samp(deviceInformationId)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_048")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_049
  ignore("VMALL_DICTIONARY_INCLUDE_PushUP_049", Include) {

    checkAnswer(s"""select stddev_samp(gamepointid) from (select * from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by gamepointid)""",
      s"""select stddev_samp(gamepointid) from (select * from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by gamepointid)""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_049")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_050
  test("VMALL_DICTIONARY_INCLUDE_PushUP_050", Include) {

    sql(s"""select stddev_samp(gamePointId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_051
  ignore("VMALL_DICTIONARY_INCLUDE_PushUP_051", Include) {

    checkAnswer(s"""select stddev_samp(Latest_DAY) from (select * from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by Latest_DAY)""",
      s"""select stddev_samp(Latest_DAY) from (select * from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by Latest_DAY)""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_051")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_053
  ignore("VMALL_DICTIONARY_INCLUDE_PushUP_053", Include) {

    checkAnswer(s"""select count(imei),count(distinct deviceinformationId),sum(deviceinformationId),sum(distinct deviceinformationId),min(deviceinformationId),max(imei),variance(gamePointId),var_samp(gamePointId),stddev_pop(gamePointId),stddev_samp(Latest_DAY) from (select * from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by gamePointId)""",
      s"""select count(imei),count(distinct deviceinformationId),sum(deviceinformationId),sum(distinct deviceinformationId),min(deviceinformationId),max(imei),variance(gamePointId),var_samp(gamePointId),stddev_pop(gamePointId),stddev_samp(Latest_DAY) from (select * from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') order by gamePointId)""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_053")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_054
  test("VMALL_DICTIONARY_INCLUDE_PushUP_054", Include) {

    checkAnswer(s"""select AVG(deviceinformationId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select AVG(deviceinformationId)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_054")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_055
  test("VMALL_DICTIONARY_INCLUDE_PushUP_055", Include) {

    checkAnswer(s"""select AVG(productionDate)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select AVG(productionDate)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_055")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_056
  test("VMALL_DICTIONARY_INCLUDE_PushUP_056", Include) {

    checkAnswer(s"""select AVG(gamePointId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select AVG(gamePointId)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_056")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_057
  test("VMALL_DICTIONARY_INCLUDE_PushUP_057", Include) {

    checkAnswer(s"""select AVG(Latest_DAY)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select AVG(Latest_DAY)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_057")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_058
  test("VMALL_DICTIONARY_INCLUDE_PushUP_058", Include) {

    checkAnswer(s"""select AVG(contractNumber)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""",
      s"""select AVG(contractNumber)  from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_058")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_059
  test("VMALL_DICTIONARY_INCLUDE_PushUP_059", Include) {

    sql(s"""select count(imei),sum(distinct deviceinformationId),min(productionDate),max(imei),variance(Latest_DAY) from VMALL_DICTIONARY_INCLUDE group by deviceInformationId limit 5""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_060
  test("VMALL_DICTIONARY_INCLUDE_PushUP_060", Include) {

    sql(s"""select sum(deviceinformationId),sum(distinct deviceinformationId),min(deviceinformationId),max(imei),variance(deviceInformationId) from VMALL_DICTIONARY_INCLUDE group by deviceInformationId order by deviceinformationId limit 5""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_061
  test("VMALL_DICTIONARY_INCLUDE_PushUP_061", Include) {

    sql(s"""select count(imei),sum(distinct deviceinformationId),min(productionDate),max(imei),variance(Latest_DAY) from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')group by deviceInformationId limit 5""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_062
  ignore("VMALL_DICTIONARY_INCLUDE_PushUP_062", Include) {

    checkAnswer(s"""select count(imei),sum(distinct deviceinformationId),min(productionDate),max(imei),variance(Latest_DAY) from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') group by deviceInformationId order by deviceinformationId limit 5""",
      s"""select count(imei),sum(distinct deviceinformationId),min(productionDate),max(imei),variance(Latest_DAY) from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') group by deviceInformationId order by deviceinformationId limit 5""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_062")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_063
  test("VMALL_DICTIONARY_INCLUDE_PushUP_063", Include) {

    sql(s"""select count(imei),sum(distinct deviceinformationId),min(productionDate),max(imei),variance(Latest_DAY) from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') group by deviceInformationId,productionDate   sort by productionDate limit 5
  """).collect

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_064
  ignore("VMALL_DICTIONARY_INCLUDE_PushUP_064", Include) {

    checkAnswer(s"""select sum(deviceinformationId+10)t  from  VMALL_DICTIONARY_INCLUDE having t >1234567""",
      s"""select sum(deviceinformationId+10)t  from  VMALL_DICTIONARY_INCLUDE_hive having t >1234567""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_064")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_065
  ignore("VMALL_DICTIONARY_INCLUDE_PushUP_065", Include) {

    checkAnswer(s"""select sum(deviceinformationId+gamePointId)t  from  VMALL_DICTIONARY_INCLUDE having t >1234567""",
      s"""select sum(deviceinformationId+gamePointId)t  from  VMALL_DICTIONARY_INCLUDE_hive having t >1234567""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_065")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_066
  ignore("VMALL_DICTIONARY_INCLUDE_PushUP_066", Include) {

    checkAnswer(s"""select sum(deviceinformationId)t,Sum(gamePointId)   from  VMALL_DICTIONARY_INCLUDE having t >1234567""",
      s"""select sum(deviceinformationId)t,Sum(gamePointId)   from  VMALL_DICTIONARY_INCLUDE_hive having t >1234567""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_066")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_067
  ignore("VMALL_DICTIONARY_INCLUDE_PushUP_067", Include) {

    checkAnswer(s"""select count(imei),sum(distinct deviceinformationId),min(productionDate)  from VMALL_DICTIONARY_INCLUDE group by imei,deviceinformationId,productionDate  order by  imei""",
      s"""select count(imei),sum(distinct deviceinformationId),min(productionDate)  from VMALL_DICTIONARY_INCLUDE_hive group by imei,deviceinformationId,productionDate  order by  imei""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_067")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_069
  test("VMALL_DICTIONARY_INCLUDE_PushUP_069", Include) {

    sql(s"""SELECT  min(Latest_DAY),max(imei),variance(contractNumber), SUM(gamePointId),count(imei),sum(distinct deviceinformationId) FROM (select * from VMALL_DICTIONARY_INCLUDE) SUB_QRY GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC limit 10""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_070
  test("VMALL_DICTIONARY_INCLUDE_PushUP_070", Include) {

    checkAnswer(s"""SELECT  VMALL_DICTIONARY_INCLUDE.gamePointId AS gamePointId,VMALL_DICTIONARY_INCLUDE.AMSize AS AMSize, VMALL_DICTIONARY_INCLUDE.ActiveCountry AS ActiveCountry, VMALL_DICTIONARY_INCLUDE.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from VMALL_DICTIONARY_INCLUDE) SUB_QRY ) VMALL_DICTIONARY_INCLUDE FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from VMALL_DICTIONARY_INCLUDE) SUB_QRY ) VMALL_DICTIONARY_INCLUDE1 ON VMALL_DICTIONARY_INCLUDE.AMSize = VMALL_DICTIONARY_INCLUDE1.AMSize WHERE VMALL_DICTIONARY_INCLUDE.AMSize LIKE '5RAM %' GROUP BY VMALL_DICTIONARY_INCLUDE.AMSize, VMALL_DICTIONARY_INCLUDE.ActiveCountry, VMALL_DICTIONARY_INCLUDE.Activecity,VMALL_DICTIONARY_INCLUDE.gamePointId ORDER BY VMALL_DICTIONARY_INCLUDE.gamePointId, VMALL_DICTIONARY_INCLUDE.AMSize ASC, VMALL_DICTIONARY_INCLUDE.ActiveCountry ASC, VMALL_DICTIONARY_INCLUDE.Activecity ASC""",
      s"""SELECT  VMALL_DICTIONARY_INCLUDE_hive.gamePointId AS gamePointId,VMALL_DICTIONARY_INCLUDE_hive.AMSize AS AMSize, VMALL_DICTIONARY_INCLUDE_hive.ActiveCountry AS ActiveCountry, VMALL_DICTIONARY_INCLUDE_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from VMALL_DICTIONARY_INCLUDE_hive) SUB_QRY ) VMALL_DICTIONARY_INCLUDE_hive FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from VMALL_DICTIONARY_INCLUDE_hive) SUB_QRY ) VMALL_DICTIONARY_INCLUDE_hive1 ON VMALL_DICTIONARY_INCLUDE_hive.AMSize = VMALL_DICTIONARY_INCLUDE_hive1.AMSize WHERE VMALL_DICTIONARY_INCLUDE_hive.AMSize LIKE '5RAM %' GROUP BY VMALL_DICTIONARY_INCLUDE_hive.AMSize, VMALL_DICTIONARY_INCLUDE_hive.ActiveCountry, VMALL_DICTIONARY_INCLUDE_hive.Activecity,VMALL_DICTIONARY_INCLUDE_hive.gamePointId ORDER BY VMALL_DICTIONARY_INCLUDE_hive.gamePointId, VMALL_DICTIONARY_INCLUDE_hive.AMSize ASC, VMALL_DICTIONARY_INCLUDE_hive.ActiveCountry ASC, VMALL_DICTIONARY_INCLUDE_hive.Activecity ASC""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_070")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_071
  test("VMALL_DICTIONARY_INCLUDE_PushUP_071", Include) {

    sql(s"""SELECT VMALL_DICTIONARY_INCLUDE.gamePointId AS gamePointId,VMALL_DICTIONARY_INCLUDE.AMSize AS AMSize, VMALL_DICTIONARY_INCLUDE.ActiveCountry AS ActiveCountry, VMALL_DICTIONARY_INCLUDE.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from VMALL_DICTIONARY_INCLUDE) SUB_QRY ) VMALL_DICTIONARY_INCLUDE RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from VMALL_DICTIONARY_INCLUDE) SUB_QRY ) Carbon_automation1 ON VMALL_DICTIONARY_INCLUDE.AMSize = Carbon_automation1.AMSize WHERE NOT(VMALL_DICTIONARY_INCLUDE.AMSize = "8RAM size") GROUP BY VMALL_DICTIONARY_INCLUDE.AMSize, VMALL_DICTIONARY_INCLUDE.ActiveCountry, VMALL_DICTIONARY_INCLUDE.Activecity,VMALL_DICTIONARY_INCLUDE.gamePointId  ORDER BY VMALL_DICTIONARY_INCLUDE.AMSize ASC, VMALL_DICTIONARY_INCLUDE.ActiveCountry ASC, VMALL_DICTIONARY_INCLUDE.Activecity ASC limit 10""").collect

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_072
  test("VMALL_DICTIONARY_INCLUDE_PushUP_072", Include) {

    sql(s"""SELECT VMALL_DICTIONARY_INCLUDE.gamePointId AS gamePointId,VMALL_DICTIONARY_INCLUDE.AMSize AS AMSize, VMALL_DICTIONARY_INCLUDE.ActiveCountry AS ActiveCountry, VMALL_DICTIONARY_INCLUDE.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from VMALL_DICTIONARY_INCLUDE) SUB_QRY ) VMALL_DICTIONARY_INCLUDE LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from VMALL_DICTIONARY_INCLUDE) SUB_QRY ) Carbon_automation1 ON VMALL_DICTIONARY_INCLUDE.AMSize = Carbon_automation1.AMSize WHERE NOT(VMALL_DICTIONARY_INCLUDE.AMSize = "8RAM size") GROUP BY VMALL_DICTIONARY_INCLUDE.AMSize, VMALL_DICTIONARY_INCLUDE.ActiveCountry, VMALL_DICTIONARY_INCLUDE.Activecity,VMALL_DICTIONARY_INCLUDE.gamePointId  ORDER BY VMALL_DICTIONARY_INCLUDE.AMSize ASC, VMALL_DICTIONARY_INCLUDE.ActiveCountry ASC, VMALL_DICTIONARY_INCLUDE.Activecity ASC limit 10
  """).collect

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_073
  test("VMALL_DICTIONARY_INCLUDE_PushUP_073", Include) {

    sql(s"""SELECT VMALL_DICTIONARY_INCLUDE.gamePointId AS gamePointId,VMALL_DICTIONARY_INCLUDE.AMSize AS AMSize, VMALL_DICTIONARY_INCLUDE.ActiveCountry AS ActiveCountry, VMALL_DICTIONARY_INCLUDE.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from VMALL_DICTIONARY_INCLUDE) SUB_QRY ) VMALL_DICTIONARY_INCLUDE INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from VMALL_DICTIONARY_INCLUDE) SUB_QRY ) Carbon_automation1 ON VMALL_DICTIONARY_INCLUDE.AMSize = Carbon_automation1.AMSize WHERE NOT(VMALL_DICTIONARY_INCLUDE.AMSize = "8RAM size") GROUP BY VMALL_DICTIONARY_INCLUDE.AMSize, VMALL_DICTIONARY_INCLUDE.ActiveCountry, VMALL_DICTIONARY_INCLUDE.Activecity,VMALL_DICTIONARY_INCLUDE.gamePointId  ORDER BY VMALL_DICTIONARY_INCLUDE.AMSize ASC, VMALL_DICTIONARY_INCLUDE.ActiveCountry ASC, VMALL_DICTIONARY_INCLUDE.Activecity ASC limit 10
  """).collect

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_074
  test("VMALL_DICTIONARY_INCLUDE_PushUP_074", Include) {

    checkAnswer(s"""select count(gamepointid),series  from VMALL_DICTIONARY_INCLUDE group by series order by series limit 5""",
      s"""select count(gamepointid),series  from VMALL_DICTIONARY_INCLUDE_hive group by series order by series limit 5""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_074")

  }


  //VMALL_DICTIONARY_INCLUDE_PushUP_075
  test("VMALL_DICTIONARY_INCLUDE_PushUP_075", Include) {

    checkAnswer(s"""select count(gamepointid),series  from VMALL_DICTIONARY_INCLUDE group by series order by series""",
      s"""select count(gamepointid),series  from VMALL_DICTIONARY_INCLUDE_hive group by series order by series""", "QueriesIncludeDictionaryTestCase_VMALL_DICTIONARY_INCLUDE_PushUP_075")

  }


  //C20_SEQ_CreateTable-DICTIONARY_INCLUDE-01
  test("C20_SEQ_CreateTable-DICTIONARY_INCLUDE-01", Include) {
    sql(s"""drop table if exists smart_500_DINC""").collect
    sql(s"""drop table if exists smart_500_DINC_hive""").collect

    sql(s"""create table smart_500_DINC (MSISDN string,IMSI string,IMEI string,INTERFACEID int,GROUPID int,GGSN_ID double,SGSN_ID double,SESSION_INDICATOR int,BEGIN_TIME double,BEGIN_TIME_MSEL int,END_TIME double,END_TIME_MSEL int,PROT_CATEGORY int,PROT_TYPE int,L7_CARRIER_PROT int,SUB_PROT_TYPE int,SID double,PROBEID double,ENCRYPT_VERSION int,ROAMING_TYPE int,ROAM_DIRECTION int,MS_IP string,SERVER_IP string,MS_PORT int,SERVER_DECIMAL Decimal,APN string,SGSN_SIG_IP string,GGSN_SIG_IP_BigInt_NEGATIVE bigint,SGSN_USER_IP string,GGSN_USER_IP string,MCC string,MNC string,RAT int,LAC string,RAC string,SAC string,CI string,BROWSER_TIMESTAMP timestamp,TCP_CONN_STATES int,TCP_STATES_BIGINTPOSITIVE int,TCP_WIN_SIZE int,TCP_MSS int,TCP_CONN_TIMES int,TCP_CONN_2_FAILED_TIMES int,TCP_CONN_3_FAILED_TIMES int,HOST string,STREAMING_URL string,GET_STREAMING_FAILED_CODE int,GET_STREAMING_FLAG int,GET_NUM int,GET_SUCCEED_NUM int,GET_RETRANS_NUM int,GET_TIMEOUT_NUM int,INTBUFFER_FST_FLAG int,INTBUFFER_FULL_FLAG int,STALL_NUM int,VIDEO_FRAME_RATE int,VIDEO_CODEC_ID string,VIDEO_WIDTH int,VIDEO_HEIGHT int,AUDIO_CODEC_ID string,MEDIA_FILE_TYPE int,PLAY_STATE int,STREAMING_FLAG int,TCP_STATUS_INDICATOR int,DISCONNECTION_FLAG int,FAILURE_CODE int,FLAG int,TAC string,ECI string,TCP_SYN_TIME_MSEL int,TCP_FST_SYN_DIRECTION int,RAN_NE_USER_IP string,HOMEMCC string,HOMEMNC string,CHARGE_FLAG int,PREPAID_FLAG int,USER_AGENT string,MS_WIN_STAT_TOTAL_NUM int,MS_WIN_STAT_SMALL_NUM int,MS_ACK_TO_1STGET_DELAY int,SERVER_ACK_TO_1STDATA_DELAY int,STREAMING_TYPE int,SOURCE_VIDEO_QUALITY int,TETHERING_FLAG int,CARRIER_ID double,LAYER1ID int,LAYER2ID int,LAYER3ID int,LAYER4ID int,LAYER5ID int,LAYER6ID int,CHARGING_RULE_BASE_NAME string,SP string,EXTENDED_URL string,SV string,FIRST_SAI_CGI_ECGI string,EXTENDED_URL_OTHER string,SIGNALING_USE_FLAG int,DNS_RETRANS_NUM int,DNS_FAIL_CODE int,FIRST_RAT int,MS_INDICATOR string,LAST_SAI_CGI_ECGI string,LAST_RAT int,FIRST_LONGITUDE double,FIRST_LATITUDE double,FIRST_ALTITUDE int,FIRST_RASTERLONGITUDE double,FIRST_RASTERLATITUDE double,FIRST_RASTERALTITUDE int,FIRST_FREQUENCYSPOT int,FIRST_CLUTTER int,FIRST_USERBEHAVIOR int,FIRST_SPEED int,FIRST_CREDIBILITY int,LAST_LONGITUDE double,LAST_LATITUDE double,LAST_ALTITUDE int,LAST_RASTERLONGITUDE double,LAST_RASTERLATITUDE double,LAST_RASTERALTITUDE int,LAST_FREQUENCYSPOT int,LAST_CLUTTER int,LAST_USERBEHAVIOR int,LAST_SPEED int,LAST_CREDIBILITY int,IMEI_CIPHERTEXT string,APP_ID int,DOMAIN_NAME string,STREAMING_CACHE_IP string,STOP_LONGER_THAN_MIN_THRESHOLD int,STOP_LONGER_THAN_MAX_THRESHOLD int,PLAY_END_STAT int,STOP_START_TIME1 double,STOP_END_TIME1 double,STOP_START_TIME2 double,STOP_END_TIME2 double,STOP_START_TIME3 double,STOP_END_TIME3 double,STOP_START_TIME4 double,STOP_END_TIME4 double,STOP_START_TIME5 double,STOP_END_TIME5 double,STOP_START_TIME6 double,STOP_END_TIME6 double,STOP_START_TIME7 double,STOP_END_TIME7 double,STOP_START_TIME8 double,STOP_END_TIME8 double,STOP_START_TIME9 double,STOP_END_TIME9 double,STOP_START_TIME10 double,STOP_END_TIME10 double,FAIL_CLASS double,RECORD_TYPE double,NODATA_COUNT double,VIDEO_NODATA_DURATION double,VIDEO_SMOOTH_DURATION double,VIDEO_SD_DURATION double,VIDEO_HD_DURATION double,VIDEO_UHD_DURATION double,VIDEO_FHD_DURATION double,FLUCTUATION double,START_DOWNLOAD_THROUGHPUT double,L7_UL_GOODPUT_FULL_MSS double,SESSIONKEY string,FIRST_UCELLID double,LAST_UCELLID double,UCELLID1 double,LONGITUDE1 double,LATITUDE1 double,UCELLID2 double,LONGITUDE2 double,LATITUDE2 double,UCELLID3 double,LONGITUDE3 double,LATITUDE3 double,UCELLID4 double,LONGITUDE4 double,LATITUDE4 double,UCELLID5 double,LONGITUDE5 double,LATITUDE5 double,UCELLID6 double,LONGITUDE6 double,LATITUDE6 double,UCELLID7 double,LONGITUDE7 double,LATITUDE7 double,UCELLID8 double,LONGITUDE8 double,LATITUDE8 double,UCELLID9 double,LONGITUDE9 double,LATITUDE9 double,UCELLID10 double,LONGITUDE10 double,LATITUDE10 double,INTBUFFER_FULL_DELAY double,STALL_DURATION double,STREAMING_DW_PACKETS double,STREAMING_DOWNLOAD_DELAY double,PLAY_DURATION double,STREAMING_QUALITY int,VIDEO_DATA_RATE double,AUDIO_DATA_RATE double,STREAMING_FILESIZE double,STREAMING_DURATIOIN double,TCP_SYN_TIME double,TCP_RTT_STEP1 double,CHARGE_ID double,UL_REVERSE_TO_DL_DELAY double,DL_REVERSE_TO_UL_DELAY double,DATATRANS_DW_GOODPUT double,DATATRANS_DW_TOTAL_DURATION double,SUM_FRAGMENT_INTERVAL double,TCP_FIN_TIMES double,TCP_RESET_TIMES double,URL_CLASSIFICATION double,STREAMING_LQ_DURATIOIN double,MAX_DNS_DELAY double,MAX_DNS2SYN double,MAX_LATANCY_OF_LINK_SETUP double,MAX_SYNACK2FIRSTACK double,MAX_SYNACK2LASTACK double,MAX_ACK2GET_DELAY double,MAX_FRAG_INTERVAL_PREDELAY double,SUM_FRAG_INTERVAL_PREDELAY double,SERVICE_DELAY_MSEC double,HOMEPROVINCE double,HOMECITY double,SERVICE_ID double,CHARGING_CLASS double,DATATRANS_UL_DURATION double,ASSOCIATED_ID double,PACKET_LOSS_NUM double,JITTER double,MS_DNS_DELAY_MSEL double,GET_STREAMING_DELAY double,TCP_UL_RETRANS_WITHOUTPL double,TCP_DW_RETRANS_WITHOUTPL double,GET_MAX_UL_SIZE double,GET_MIN_UL_SIZE double,GET_MAX_DL_SIZE double,GET_MIN_DL_SIZE double,L4_UL_THROUGHPUT double,L4_DW_THROUGHPUT double,L4_UL_GOODPUT double,L4_DW_GOODPUT double,NETWORK_UL_TRAFFIC double,NETWORK_DL_TRAFFIC double,L4_UL_PACKETS double,L4_DW_PACKETS double,TCP_RTT double,TCP_UL_OUTOFSEQU double,TCP_DW_OUTOFSEQU double,TCP_UL_RETRANS double,TCP_DW_RETRANS double,TCP_UL_RETRANS_WITHPL double,TCP_DW_RETRANS_WITHPL double,TCP_UL_PACKAGES_WITHPL double,TCP_DW_PACKAGES_WITHPL double,TCP_UL_PACKAGES_WITHOUTPL double,TCP_DW_PACKAGES_WITHOUTPL double,UPPERLAYER_IP_UL_PACKETS double,UPPERLAYER_IP_DL_PACKETS double,DOWNLAYER_IP_UL_PACKETS double,DOWNLAYER_IP_DL_PACKETS double,UPPERLAYER_IP_UL_FRAGMENTS double,UPPERLAYER_IP_DL_FRAGMENTS double,DOWNLAYER_IP_UL_FRAGMENTS double,DOWNLAYER_IP_DL_FRAGMENTS double,VALID_TRANS_DURATION double,AIR_PORT_DURATION double,RADIO_CONN_TIMES double,RAN_NE_ID double,AVG_UL_RTT double,AVG_DW_RTT double,UL_RTT_LONG_NUM int,DW_RTT_LONG_NUM int,UL_RTT_STAT_NUM int,DW_RTT_STAT_NUM int,USER_PROBE_UL_LOST_PKT int,SERVER_PROBE_UL_LOST_PKT int,SERVER_PROBE_DW_LOST_PKT int,USER_PROBE_DW_LOST_PKT int,CHARGING_CHARACTERISTICS double,DL_SERIOUS_OUT_OF_ORDER_NUM double,DL_SLIGHT_OUT_OF_ORDER_NUM double,DL_FLIGHT_TOTAL_SIZE double,DL_FLIGHT_TOTAL_NUM double,DL_MAX_FLIGHT_SIZE double,UL_SERIOUS_OUT_OF_ORDER_NUM double,UL_SLIGHT_OUT_OF_ORDER_NUM double,UL_FLIGHT_TOTAL_SIZE double,UL_FLIGHT_TOTAL_NUM double,UL_MAX_FLIGHT_SIZE double,USER_DL_SLIGHT_OUT_OF_ORDER_PACKETS double,SERVER_UL_SLIGHT_OUT_OF_ORDER_PACKETS double,DL_CONTINUOUS_RETRANSMISSION_DELAY double,USER_HUNGRY_DELAY double,SERVER_HUNGRY_DELAY double,AVG_DW_RTT_MICRO_SEC int,AVG_UL_RTT_MICRO_SEC int,FLOW_SAMPLE_RATIO int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES ( 'DICTIONARY_INCLUDE'='BEGIN_TIME,END_TIME,SID,MSISDN,IMSI,IMEI,MS_IP,SERVER_IP,HOST,SP,MS_INDICATOR,streaming_url,LAYER1ID,TCP_DW_RETRANS')""").collect

    sql(s"""create table smart_500_DINC_hive (SID double,PROBEID double,INTERFACEID int,GROUPID int,GGSN_ID double,SGSN_ID double,dummy_6 string,SESSION_INDICATOR int,BEGIN_TIME double,BEGIN_TIME_MSEL int,END_TIME double,END_TIME_MSEL int,PROT_CATEGORY int,PROT_TYPE int,L7_CARRIER_PROT int,SUB_PROT_TYPE int,MSISDN string,IMSI string,IMEI string,ENCRYPT_VERSION int,ROAMING_TYPE int,ROAM_DIRECTION int,MS_IP string,SERVER_IP string,MS_PORT int,APN string,SGSN_SIG_IP string,GGSN_USER_IP string,SGSN_USER_IP string,MCC string,MNC string,RAT int,LAC string,RAC string,SAC string,CI string,SERVER_DECIMAL decimal,BROWSER_TIMESTAMP timestamp,TCP_CONN_STATES int,GGSN_SIG_IP_BigInt_NEGATIVE bigint,TCP_STATES_BIGINTPOSITIVE bigint,dummy_41 string,TCP_WIN_SIZE int,dummy_43 string,TCP_MSS int,dummy_45 string,TCP_CONN_TIMES int,dummy_47 string,TCP_CONN_2_FAILED_TIMES int,dummy_49 string,TCP_CONN_3_FAILED_TIMES int,HOST string,STREAMING_URL string,dummy_53 string,GET_STREAMING_FAILED_CODE int,dummy_55 string,GET_STREAMING_FLAG int,dummy_57 string,GET_NUM int,dummy_59 string,GET_SUCCEED_NUM int,dummy_61 string,GET_RETRANS_NUM int,dummy_63 string,GET_TIMEOUT_NUM int,INTBUFFER_FST_FLAG int,INTBUFFER_FULL_FLAG int,STALL_NUM int,dummy_68 string,VIDEO_FRAME_RATE int,dummy_70 string,VIDEO_CODEC_ID string,dummy_72 string,VIDEO_WIDTH int,dummy_74 string,VIDEO_HEIGHT int,dummy_76 string,AUDIO_CODEC_ID string,dummy_78 string,MEDIA_FILE_TYPE int,dummy_80 string,PLAY_STATE int,dummy_82 string,PLAY_STATE_1 int,dummy_84 string,STREAMING_FLAG int,dummy_86 string,TCP_STATUS_INDICATOR int,dummy_88 string,DISCONNECTION_FLAG int,dummy_90 string,FAILURE_CODE int,FLAG int,TAC string,ECI string,dummy_95 string,TCP_SYN_TIME_MSEL int,dummy_97 string,TCP_FST_SYN_DIRECTION int,RAN_NE_USER_IP string,HOMEMCC string,HOMEMNC string,dummy_102 string,CHARGE_FLAG int,dummy_104 string,PREPAID_FLAG int,dummy_106 string,USER_AGENT string,dummy_108 string,MS_WIN_STAT_TOTAL_NUM int,dummy_110 string,MS_WIN_STAT_SMALL_NUM int,dummy_112 string,MS_ACK_TO_1STGET_DELAY int,dummy_114 string,SERVER_ACK_TO_1STDATA_DELAY int,dummy_116 string,STREAMING_TYPE int,dummy_118 string,SOURCE_VIDEO_QUALITY int,TETHERING_FLAG int,CARRIER_ID double,LAYER1ID int,LAYER2ID int,dummy_124 string,LAYER3ID int,dummy_126 string,LAYER4ID int,dummy_128 string,LAYER5ID int,dummy_130 string,LAYER6ID int,CHARGING_RULE_BASE_NAME string,SP string,dummy_134 string,EXTENDED_URL string,SV string,FIRST_SAI_CGI_ECGI string,dummy_138 string,EXTENDED_URL_OTHER string,SIGNALING_USE_FLAG int,dummy_141 string,DNS_RETRANS_NUM int,dummy_143 string,DNS_FAIL_CODE int,FIRST_RAT int,FIRST_RAT_1 int,MS_INDICATOR string,LAST_SAI_CGI_ECGI string,LAST_RAT int,dummy_150 string,FIRST_LONGITUDE double,dummy_152 string,FIRST_LATITUDE double,dummy_154 string,FIRST_ALTITUDE int,dummy_156 string,FIRST_RASTERLONGITUDE double,dummy_158 string,FIRST_RASTERLATITUDE double,dummy_160 string,FIRST_RASTERALTITUDE int,dummy_162 string,FIRST_FREQUENCYSPOT int,dummy_164 string,FIRST_CLUTTER int,dummy_166 string,FIRST_USERBEHAVIOR int,dummy_168 string,FIRST_SPEED int,dummy_170 string,FIRST_CREDIBILITY int,dummy_172 string,LAST_LONGITUDE double,dummy_174 string,LAST_LATITUDE double,dummy_176 string,LAST_ALTITUDE int,dummy_178 string,LAST_RASTERLONGITUDE double,dummy_180 string,LAST_RASTERLATITUDE double,dummy_182 string,LAST_RASTERALTITUDE int,dummy_184 string,LAST_FREQUENCYSPOT int,dummy_186 string,LAST_CLUTTER int,dummy_188 string,LAST_USERBEHAVIOR int,dummy_190 string,LAST_SPEED int,dummy_192 string,LAST_CREDIBILITY int,dummy_194 string,IMEI_CIPHERTEXT string,APP_ID int,dummy_197 string,DOMAIN_NAME string,dummy_199 string,STREAMING_CACHE_IP string,dummy_201 string,STOP_LONGER_THAN_MIN_THRESHOLD int,dummy_203 string,STOP_LONGER_THAN_MAX_THRESHOLD int,dummy_205 string,PLAY_END_STAT int,dummy_207 string,STOP_START_TIME1 double,dummy_209 string,STOP_END_TIME1 double,dummy_211 string,STOP_START_TIME2 double,dummy_213 string,STOP_END_TIME2 double,dummy_215 string,STOP_START_TIME3 double,dummy_217 string,STOP_END_TIME3 double,dummy_219 string,STOP_START_TIME4 double,dummy_221 string,STOP_END_TIME4 double,dummy_223 string,STOP_START_TIME5 double,dummy_225 string,STOP_END_TIME5 double,dummy_227 string,STOP_START_TIME6 double,dummy_229 string,STOP_END_TIME6 double,dummy_231 string,STOP_START_TIME7 double,dummy_233 string,STOP_END_TIME7 double,dummy_235 string,STOP_START_TIME8 double,dummy_237 string,STOP_END_TIME8 double,dummy_239 string,STOP_START_TIME9 double,dummy_241 string,STOP_END_TIME9 double,dummy_243 string,STOP_START_TIME10 double,dummy_245 string,STOP_END_TIME10 double,dummy_247 string,FAIL_CLASS double,RECORD_TYPE double,dummy_250 string,NODATA_COUNT double,dummy_252 string,VIDEO_NODATA_DURATION double,dummy_254 string,VIDEO_SMOOTH_DURATION double,dummy_256 string,VIDEO_SD_DURATION double,dummy_258 string,VIDEO_HD_DURATION double,dummy_260 string,VIDEO_UHD_DURATION double,dummy_262 string,VIDEO_FHD_DURATION double,dummy_264 string,FLUCTUATION double,dummy_266 string,START_DOWNLOAD_THROUGHPUT double,dummy_268 string,L7_UL_GOODPUT_FULL_MSS double,dummy_270 string,SESSIONKEY string,dummy_272 string,FIRST_UCELLID double,dummy_274 string,LAST_UCELLID double,dummy_276 string,UCELLID1 double,dummy_278 string,LONGITUDE1 double,dummy_280 string,LATITUDE1 double,dummy_282 string,UCELLID2 double,dummy_284 string,LONGITUDE2 double,dummy_286 string,LATITUDE2 double,dummy_288 string,UCELLID3 double,dummy_290 string,LONGITUDE3 double,dummy_292 string,LATITUDE3 double,dummy_294 string,UCELLID4 double,dummy_296 string,LONGITUDE4 double,dummy_2101 string,LATITUDE4 double,dummy_300 string,UCELLID5 double,dummy_302 string,LONGITUDE5 double,dummy_304 string,LATITUDE5 double,dummy_306 string,UCELLID6 double,dummy_308 string,LONGITUDE6 double,dummy_310 string,LATITUDE6 double,dummy_312 string,UCELLID7 double,dummy_314 string,LONGITUDE7 double,dummy_316 string,LATITUDE7 double,dummy_318 string,UCELLID8 double,dummy_320 string,LONGITUDE8 double,dummy_322 string,LATITUDE8 double,dummy_324 string,UCELLID9 double,dummy_326 string,LONGITUDE9 double,dummy_328 string,LATITUDE9 double,dummy_330 string,UCELLID10 double,dummy_332 string,LONGITUDE10 double,dummy_334 string,LATITUDE10 double,dummy_336 string,INTBUFFER_FULL_DELAY double,dummy_338 string,STALL_DURATION double,dummy_340 string,STREAMING_DW_PACKETS double,dummy_342 string,STREAMING_DOWNLOAD_DELAY double,dummy_344 string,PLAY_DURATION double,dummy_346 string,STREAMING_QUALITY int,dummy_348 string,VIDEO_DATA_RATE double,dummy_350 string,AUDIO_DATA_RATE double,dummy_352 string,STREAMING_FILESIZE double,dummy_354 string,STREAMING_DURATIOIN double,dummy_356 string,TCP_SYN_TIME double,dummy_358 string,TCP_RTT_STEP1 double,CHARGE_ID double,dummy_361 string,UL_REVERSE_TO_DL_DELAY double,dummy_363 string,DL_REVERSE_TO_UL_DELAY double,dummy_365 string,DATATRANS_DW_GOODPUT double,dummy_367 string,DATATRANS_DW_TOTAL_DURATION double,dummy_369 string,SUM_FRAGMENT_INTERVAL double,dummy_371 string,TCP_FIN_TIMES double,dummy_373 string,TCP_RESET_TIMES double,dummy_375 string,URL_CLASSIFICATION double,dummy_377 string,STREAMING_LQ_DURATIOIN double,dummy_379 string,MAX_DNS_DELAY double,dummy_381 string,MAX_DNS2SYN double,dummy_383 string,MAX_LATANCY_OF_LINK_SETUP double,dummy_385 string,MAX_SYNACK2FIRSTACK double,dummy_387 string,MAX_SYNACK2LASTACK double,dummy_389 string,MAX_ACK2GET_DELAY double,dummy_391 string,MAX_FRAG_INTERVAL_PREDELAY double,dummy_393 string,SUM_FRAG_INTERVAL_PREDELAY double,dummy_395 string,SERVICE_DELAY_MSEC double,dummy_397 string,HOMEPROVINCE double,dummy_399 string,HOMECITY double,dummy_401 string,SERVICE_ID double,dummy_403 string,CHARGING_CLASS double,dummy_405 string,DATATRANS_UL_DURATION double,dummy_407 string,ASSOCIATED_ID double,dummy_409 string,PACKET_LOSS_NUM double,dummy_411 string,JITTER double,dummy_413 string,MS_DNS_DELAY_MSEL double,dummy_415 string,GET_STREAMING_DELAY double,dummy_417 string,TCP_UL_RETRANS_WITHOUTPL double,dummy_419 string,TCP_DW_RETRANS_WITHOUTPL double,dummy_421 string,GET_MAX_UL_SIZE double,dummy_423 string,GET_MIN_UL_SIZE double,dummy_425 string,GET_MAX_DL_SIZE double,dummy_427 string,GET_MIN_DL_SIZE double,dummy_429 string,FLOW_SAMPLE_RATIO int,dummy_431 string,UL_RTT_LONG_NUM int,dummy_433 string,DW_RTT_LONG_NUM int,dummy_435 string,UL_RTT_STAT_NUM int,dummy_437 string,DW_RTT_STAT_NUM int,dummy_439 string,USER_PROBE_UL_LOST_PKT int,dummy_441 string,SERVER_PROBE_UL_LOST_PKT int,dummy_443 string,SERVER_PROBE_DW_LOST_PKT int,dummy_445 string,USER_PROBE_DW_LOST_PKT int,dummy_447 string,AVG_DW_RTT_MICRO_SEC int,dummy_449 string,AVG_UL_RTT_MICRO_SEC int,dummy_451 string,RAN_NE_ID double,dummy_453 string,AVG_UL_RTT double,dummy_455 string,AVG_DW_RTT double,dummy_457 string,CHARGING_CHARACTERISTICS double,dummy_459 string,DL_SERIOUS_OUT_OF_ORDER_NUM double,dummy_461 string,DL_SLIGHT_OUT_OF_ORDER_NUM double,dummy_463 string,DL_FLIGHT_TOTAL_SIZE double,dummy_465 string,DL_FLIGHT_TOTAL_NUM double,dummy_467 string,DL_MAX_FLIGHT_SIZE double,dummy_469 string,VALID_TRANS_DURATION double,dummy_471 string,AIR_PORT_DURATION double,dummy_473 string,RADIO_CONN_TIMES double,dummy_475 string,UL_SERIOUS_OUT_OF_ORDER_NUM double,dummy_477 string,UL_SLIGHT_OUT_OF_ORDER_NUM double,dummy_479 string,UL_FLIGHT_TOTAL_SIZE double,dummy_481 string,UL_FLIGHT_TOTAL_NUM double,dummy_483 string,UL_MAX_FLIGHT_SIZE double,dummy_485 string,USER_DL_SLIGHT_OUT_OF_ORDER_PACKETS double,dummy_487 string,SERVER_UL_SLIGHT_OUT_OF_ORDER_PACKETS double,dummy_489 string,DL_CONTINUOUS_RETRANSMISSION_DELAY double,dummy_491 string,USER_HUNGRY_DELAY double,dummy_493 string,SERVER_HUNGRY_DELAY double,dummy_495 string,UPPERLAYER_IP_UL_FRAGMENTS double,dummy_497 string,UPPERLAYER_IP_DL_FRAGMENTS double,dummy_499 string,DOWNLAYER_IP_UL_FRAGMENTS double,dummy_501 string,DOWNLAYER_IP_DL_FRAGMENTS double,dummy_503 string,UPPERLAYER_IP_UL_PACKETS double,dummy_505 string,UPPERLAYER_IP_DL_PACKETS double,dummy_507 string,DOWNLAYER_IP_UL_PACKETS double,dummy_509 string,DOWNLAYER_IP_DL_PACKETS double,dummy_511 string,TCP_UL_PACKAGES_WITHPL double,dummy_513 string,TCP_DW_PACKAGES_WITHPL double,dummy_515 string,TCP_UL_PACKAGES_WITHOUTPL double,dummy_517 string,TCP_DW_PACKAGES_WITHOUTPL double,dummy_519 string,TCP_UL_RETRANS_WITHPL double,dummy_521 string,TCP_DW_RETRANS_WITHPL double,L4_UL_THROUGHPUT double,L4_DW_THROUGHPUT double,L4_UL_GOODPUT double,L4_DW_GOODPUT double,NETWORK_UL_TRAFFIC double,NETWORK_DL_TRAFFIC double,L4_UL_PACKETS double,L4_DW_PACKETS double,TCP_RTT double,TCP_UL_OUTOFSEQU double,TCP_DW_OUTOFSEQU double,TCP_UL_RETRANS double,TCP_DW_RETRANS double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect


  }


  //C20_SEQ_Dataload-DICTIONARY_INCLUDE-01
  test("C20_SEQ_Dataload-DICTIONARY_INCLUDE-01", Include) {

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/SEQ500/seq_500Records.csv' into table smart_500_DINC options('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='SID,PROBEID,INTERFACEID,GROUPID,GGSN_ID,SGSN_ID,dummy,SESSION_INDICATOR,BEGIN_TIME,BEGIN_TIME_MSEL,END_TIME,END_TIME_MSEL,PROT_CATEGORY,PROT_TYPE,L7_CARRIER_PROT,SUB_PROT_TYPE,MSISDN,IMSI,IMEI,ENCRYPT_VERSION,ROAMING_TYPE,ROAM_DIRECTION,MS_IP,SERVER_IP,MS_PORT,APN,SGSN_SIG_IP,GGSN_USER_IP,SGSN_USER_IP,MCC,MNC,RAT,LAC,RAC,SAC,CI,SERVER_DECIMAL,BROWSER_TIMESTAMP,TCP_CONN_STATES,GGSN_SIG_IP_BigInt_NEGATIVE,TCP_STATES_BIGINTPOSITIVE,dummy,TCP_WIN_SIZE,dummy,TCP_MSS,dummy,TCP_CONN_TIMES,dummy,TCP_CONN_2_FAILED_TIMES,dummy,TCP_CONN_3_FAILED_TIMES,HOST,STREAMING_URL,dummy,GET_STREAMING_FAILED_CODE,dummy,GET_STREAMING_FLAG,dummy,GET_NUM,dummy,GET_SUCCEED_NUM,dummy,GET_RETRANS_NUM,dummy,GET_TIMEOUT_NUM,INTBUFFER_FST_FLAG,INTBUFFER_FULL_FLAG,STALL_NUM,dummy,VIDEO_FRAME_RATE,dummy,VIDEO_CODEC_ID,dummy,VIDEO_WIDTH,dummy,VIDEO_HEIGHT,dummy,AUDIO_CODEC_ID,dummy,MEDIA_FILE_TYPE,dummy,PLAY_STATE,dummy,PLAY_STATE,dummy,STREAMING_FLAG,dummy,TCP_STATUS_INDICATOR,dummy,DISCONNECTION_FLAG,dummy,FAILURE_CODE,FLAG,TAC,ECI,dummy,TCP_SYN_TIME_MSEL,dummy,TCP_FST_SYN_DIRECTION,RAN_NE_USER_IP,HOMEMCC,HOMEMNC,dummy,CHARGE_FLAG,dummy,PREPAID_FLAG,dummy,USER_AGENT,dummy,MS_WIN_STAT_TOTAL_NUM,dummy,MS_WIN_STAT_SMALL_NUM,dummy,MS_ACK_TO_1STGET_DELAY,dummy,SERVER_ACK_TO_1STDATA_DELAY,dummy,STREAMING_TYPE,dummy,SOURCE_VIDEO_QUALITY,TETHERING_FLAG,CARRIER_ID,LAYER1ID,LAYER2ID,dummy,LAYER3ID,dummy,LAYER4ID,dummy,LAYER5ID,dummy,LAYER6ID,CHARGING_RULE_BASE_NAME,SP,dummy,EXTENDED_URL,SV,FIRST_SAI_CGI_ECGI,dummy,EXTENDED_URL_OTHER,SIGNALING_USE_FLAG,dummy,DNS_RETRANS_NUM,dummy,DNS_FAIL_CODE,FIRST_RAT,FIRST_RAT,MS_INDICATOR,LAST_SAI_CGI_ECGI,LAST_RAT,dummy,FIRST_LONGITUDE,dummy,FIRST_LATITUDE,dummy,FIRST_ALTITUDE,dummy,FIRST_RASTERLONGITUDE,dummy,FIRST_RASTERLATITUDE,dummy,FIRST_RASTERALTITUDE,dummy,FIRST_FREQUENCYSPOT,dummy,FIRST_CLUTTER,dummy,FIRST_USERBEHAVIOR,dummy,FIRST_SPEED,dummy,FIRST_CREDIBILITY,dummy,LAST_LONGITUDE,dummy,LAST_LATITUDE,dummy,LAST_ALTITUDE,dummy,LAST_RASTERLONGITUDE,dummy,LAST_RASTERLATITUDE,dummy,LAST_RASTERALTITUDE,dummy,LAST_FREQUENCYSPOT,dummy,LAST_CLUTTER,dummy,LAST_USERBEHAVIOR,dummy,LAST_SPEED,dummy,LAST_CREDIBILITY,dummy,IMEI_CIPHERTEXT,APP_ID,dummy,DOMAIN_NAME,dummy,STREAMING_CACHE_IP,dummy,STOP_LONGER_THAN_MIN_THRESHOLD,dummy,STOP_LONGER_THAN_MAX_THRESHOLD,dummy,PLAY_END_STAT,dummy,STOP_START_TIME1,dummy,STOP_END_TIME1,dummy,STOP_START_TIME2,dummy,STOP_END_TIME2,dummy,STOP_START_TIME3,dummy,STOP_END_TIME3,dummy,STOP_START_TIME4,dummy,STOP_END_TIME4,dummy,STOP_START_TIME5,dummy,STOP_END_TIME5,dummy,STOP_START_TIME6,dummy,STOP_END_TIME6,dummy,STOP_START_TIME7,dummy,STOP_END_TIME7,dummy,STOP_START_TIME8,dummy,STOP_END_TIME8,dummy,STOP_START_TIME9,dummy,STOP_END_TIME9,dummy,STOP_START_TIME10,dummy,STOP_END_TIME10,dummy,FAIL_CLASS,RECORD_TYPE,dummy,NODATA_COUNT,dummy,VIDEO_NODATA_DURATION,dummy,VIDEO_SMOOTH_DURATION,dummy,VIDEO_SD_DURATION,dummy,VIDEO_HD_DURATION,dummy,VIDEO_UHD_DURATION,dummy,VIDEO_FHD_DURATION,dummy,FLUCTUATION,dummy,START_DOWNLOAD_THROUGHPUT,dummy,L7_UL_GOODPUT_FULL_MSS,dummy,SESSIONKEY,dummy,FIRST_UCELLID,dummy,LAST_UCELLID,dummy,UCELLID1,dummy,LONGITUDE1,dummy,LATITUDE1,dummy,UCELLID2,dummy,LONGITUDE2,dummy,LATITUDE2,dummy,UCELLID3,dummy,LONGITUDE3,dummy,LATITUDE3,dummy,UCELLID4,dummy,LONGITUDE4,dummy,LATITUDE4,dummy,UCELLID5,dummy,LONGITUDE5,dummy,LATITUDE5,dummy,UCELLID6,dummy,LONGITUDE6,dummy,LATITUDE6,dummy,UCELLID7,dummy,LONGITUDE7,dummy,LATITUDE7,dummy,UCELLID8,dummy,LONGITUDE8,dummy,LATITUDE8,dummy,UCELLID9,dummy,LONGITUDE9,dummy,LATITUDE9,dummy,UCELLID10,dummy,LONGITUDE10,dummy,LATITUDE10,dummy,INTBUFFER_FULL_DELAY,dummy,STALL_DURATION,dummy,STREAMING_DW_PACKETS,dummy,STREAMING_DOWNLOAD_DELAY,dummy,PLAY_DURATION,dummy,STREAMING_QUALITY,dummy,VIDEO_DATA_RATE,dummy,AUDIO_DATA_RATE,dummy,STREAMING_FILESIZE,dummy,STREAMING_DURATIOIN,dummy,TCP_SYN_TIME,dummy,TCP_RTT_STEP1,CHARGE_ID,dummy,UL_REVERSE_TO_DL_DELAY,dummy,DL_REVERSE_TO_UL_DELAY,dummy,DATATRANS_DW_GOODPUT,dummy,DATATRANS_DW_TOTAL_DURATION,dummy,SUM_FRAGMENT_INTERVAL,dummy,TCP_FIN_TIMES,dummy,TCP_RESET_TIMES,dummy,URL_CLASSIFICATION,dummy,STREAMING_LQ_DURATIOIN,dummy,MAX_DNS_DELAY,dummy,MAX_DNS2SYN,dummy,MAX_LATANCY_OF_LINK_SETUP,dummy,MAX_SYNACK2FIRSTACK,dummy,MAX_SYNACK2LASTACK,dummy,MAX_ACK2GET_DELAY,dummy,MAX_FRAG_INTERVAL_PREDELAY,dummy,SUM_FRAG_INTERVAL_PREDELAY,dummy,SERVICE_DELAY_MSEC,dummy,HOMEPROVINCE,dummy,HOMECITY,dummy,SERVICE_ID,dummy,CHARGING_CLASS,dummy,DATATRANS_UL_DURATION,dummy,ASSOCIATED_ID,dummy,PACKET_LOSS_NUM,dummy,JITTER,dummy,MS_DNS_DELAY_MSEL,dummy,GET_STREAMING_DELAY,dummy,TCP_UL_RETRANS_WITHOUTPL,dummy,TCP_DW_RETRANS_WITHOUTPL,dummy,GET_MAX_UL_SIZE,dummy,GET_MIN_UL_SIZE,dummy,GET_MAX_DL_SIZE,dummy,GET_MIN_DL_SIZE,dummy,FLOW_SAMPLE_RATIO,dummy,UL_RTT_LONG_NUM,dummy,DW_RTT_LONG_NUM,dummy,UL_RTT_STAT_NUM,dummy,DW_RTT_STAT_NUM,dummy,USER_PROBE_UL_LOST_PKT,dummy,SERVER_PROBE_UL_LOST_PKT,dummy,SERVER_PROBE_DW_LOST_PKT,dummy,USER_PROBE_DW_LOST_PKT,dummy,AVG_DW_RTT_MICRO_SEC,dummy,AVG_UL_RTT_MICRO_SEC,dummy,RAN_NE_ID,dummy,AVG_UL_RTT,dummy,AVG_DW_RTT,dummy,CHARGING_CHARACTERISTICS,dummy,DL_SERIOUS_OUT_OF_ORDER_NUM,dummy,DL_SLIGHT_OUT_OF_ORDER_NUM,dummy,DL_FLIGHT_TOTAL_SIZE,dummy,DL_FLIGHT_TOTAL_NUM,dummy,DL_MAX_FLIGHT_SIZE,dummy,VALID_TRANS_DURATION,dummy,AIR_PORT_DURATION,dummy,RADIO_CONN_TIMES,dummy,UL_SERIOUS_OUT_OF_ORDER_NUM,dummy,UL_SLIGHT_OUT_OF_ORDER_NUM,dummy,UL_FLIGHT_TOTAL_SIZE,dummy,UL_FLIGHT_TOTAL_NUM,dummy,UL_MAX_FLIGHT_SIZE,dummy,USER_DL_SLIGHT_OUT_OF_ORDER_PACKETS,dummy,SERVER_UL_SLIGHT_OUT_OF_ORDER_PACKETS,dummy,DL_CONTINUOUS_RETRANSMISSION_DELAY,dummy,USER_HUNGRY_DELAY,dummy,SERVER_HUNGRY_DELAY,dummy,UPPERLAYER_IP_UL_FRAGMENTS,dummy,UPPERLAYER_IP_DL_FRAGMENTS,dummy,DOWNLAYER_IP_UL_FRAGMENTS,dummy,DOWNLAYER_IP_DL_FRAGMENTS,dummy,UPPERLAYER_IP_UL_PACKETS,dummy,UPPERLAYER_IP_DL_PACKETS,dummy,DOWNLAYER_IP_UL_PACKETS,dummy,DOWNLAYER_IP_DL_PACKETS,dummy,TCP_UL_PACKAGES_WITHPL,dummy,TCP_DW_PACKAGES_WITHPL,dummy,TCP_UL_PACKAGES_WITHOUTPL,dummy,TCP_DW_PACKAGES_WITHOUTPL,dummy,TCP_UL_RETRANS_WITHPL,dummy,TCP_DW_RETRANS_WITHPL,L4_UL_THROUGHPUT,L4_DW_THROUGHPUT,L4_UL_GOODPUT,L4_DW_GOODPUT,NETWORK_UL_TRAFFIC,NETWORK_DL_TRAFFIC,L4_UL_PACKETS,L4_DW_PACKETS,TCP_RTT,TCP_UL_OUTOFSEQU,TCP_DW_OUTOFSEQU,TCP_UL_RETRANS,TCP_DW_RETRANS')""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/SEQ500/seq_500Records.csv' into table smart_500_DINC_hive """).collect


  }


  //C20_DICTIONARY_INCLUDE_TC001
  test("C20_DICTIONARY_INCLUDE_TC001", Include) {

    checkAnswer(s"""select SID, IMEI from smart_500_DINC where HOST not in ('www.hua735435.com')""",
      s"""select SID, IMEI from smart_500_DINC_hive where HOST not in ('www.hua735435.com')""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC001")

  }


  //C20_DICTIONARY_INCLUDE_TC002
  test("C20_DICTIONARY_INCLUDE_TC002", Include) {

    checkAnswer(s"""select SID, IMEI from smart_500_DINC where HOST in  ('www.hua735435.com')""",
      s"""select SID, IMEI from smart_500_DINC_hive where HOST in  ('www.hua735435.com')""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC002")

  }


  //C20_DICTIONARY_INCLUDE_TC003
  test("C20_DICTIONARY_INCLUDE_TC003", Include) {

    checkAnswer(s"""select SID, IMEI from smart_500_DINC where HOST LIKE  'www.hua735435.com'""",
      s"""select SID, IMEI from smart_500_DINC_hive where HOST LIKE  'www.hua735435.com'""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC003")

  }


  //C20_DICTIONARY_INCLUDE_TC004
  test("C20_DICTIONARY_INCLUDE_TC004", Include) {

    checkAnswer(s"""select SID, IMEI from smart_500_DINC where HOST Not LIKE  'www.hua735435.com'""",
      s"""select SID, IMEI from smart_500_DINC_hive where HOST Not LIKE  'www.hua735435.com'""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC004")

  }


  //C20_DICTIONARY_INCLUDE_TC005
  test("C20_DICTIONARY_INCLUDE_TC005", Include) {

    checkAnswer(s"""select length(HOST) from smart_500_DINC where HOST in ('www.hua735435.com')""",
      s"""select length(HOST) from smart_500_DINC_hive where HOST in ('www.hua735435.com')""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC005")

  }


  //C20_DICTIONARY_INCLUDE_TC006
  test("C20_DICTIONARY_INCLUDE_TC006", Include) {

    checkAnswer(s"""select avg(HOST),avg(LAYER1ID) from smart_500_DINC where HOST in ('www.hua735435.com')""",
      s"""select avg(HOST),avg(LAYER1ID) from smart_500_DINC_hive where HOST in ('www.hua735435.com')""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC006")

  }


  //C20_DICTIONARY_INCLUDE_TC007
  test("C20_DICTIONARY_INCLUDE_TC007", Include) {

    checkAnswer(s"""select avg(HOST),avg(LAYER1ID) from smart_500_DINC where HOST not in ('www.hua735435.com')""",
      s"""select avg(HOST),avg(LAYER1ID) from smart_500_DINC_hive where HOST not in ('www.hua735435.com')""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC007")

  }


  //C20_DICTIONARY_INCLUDE_TC008
  test("C20_DICTIONARY_INCLUDE_TC008", Include) {

    checkAnswer(s"""select substring(IMEI,1,4) from smart_500_DINC where HOST in ('www.hua735435.com')""",
      s"""select substring(IMEI,1,4) from smart_500_DINC_hive where HOST in ('www.hua735435.com')""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC008")

  }


  //C20_DICTIONARY_INCLUDE_TC009
  test("C20_DICTIONARY_INCLUDE_TC009", Include) {

    checkAnswer(s"""select length(HOST)+10 from smart_500_DINC where HOST in ('www.hua735435.com')""",
      s"""select length(HOST)+10 from smart_500_DINC_hive where HOST in ('www.hua735435.com')""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC009")

  }


  //C20_DICTIONARY_INCLUDE_TC010
  test("C20_DICTIONARY_INCLUDE_TC010", Include) {

    checkAnswer(s"""select length(HOST)-10 from smart_500_DINC where HOST in ('www.hua735435.com')""",
      s"""select length(HOST)-10 from smart_500_DINC_hive where HOST in ('www.hua735435.com')""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC010")

  }


  //C20_DICTIONARY_INCLUDE_TC011
  test("C20_DICTIONARY_INCLUDE_TC011", Include) {

    checkAnswer(s"""select length(HOST)/10 from smart_500_DINC where HOST in ('www.hua735435.com')""",
      s"""select length(HOST)/10 from smart_500_DINC_hive where HOST in ('www.hua735435.com')""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC011")

  }


  //C20_DICTIONARY_INCLUDE_TC012
  test("C20_DICTIONARY_INCLUDE_TC012", Include) {

    checkAnswer(s"""select length(HOST)*10 from smart_500_DINC where HOST in ('www.hua735435.com')""",
      s"""select length(HOST)*10 from smart_500_DINC_hive where HOST in ('www.hua735435.com')""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC012")

  }


  //C20_DICTIONARY_INCLUDE_TC013
  test("C20_DICTIONARY_INCLUDE_TC013", Include) {

    checkAnswer(s"""select lower(MS_IP),sum(LAYER1ID) from smart_500_DINC  group by lower(MS_IP)""",
      s"""select lower(MS_IP),sum(LAYER1ID) from smart_500_DINC_hive  group by lower(MS_IP)""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC013")

  }


  //C20_DICTIONARY_INCLUDE_TC014
  test("C20_DICTIONARY_INCLUDE_TC014", Include) {

    checkAnswer(s"""select * from smart_500_DINC  where unix_timestamp(MS_IP)=1420268400""",
      s"""select * from smart_500_DINC_hive  where unix_timestamp(MS_IP)=1420268400""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC014")

  }


  //C20_DICTIONARY_INCLUDE_TC015
  test("C20_DICTIONARY_INCLUDE_TC015", Include) {

    checkAnswer(s"""select * from smart_500_DINC  where to_date(MS_IP)='2015-01-07'""",
      s"""select * from smart_500_DINC_hive  where to_date(MS_IP)='2015-01-07'""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC015")

  }


  //C20_DICTIONARY_INCLUDE_TC016
  test("C20_DICTIONARY_INCLUDE_TC016", Include) {

    checkAnswer(s"""select * from smart_500_DINC  where datediff(MS_IP,'2014-12-01')>=35""",
      s"""select * from smart_500_DINC_hive  where datediff(MS_IP,'2014-12-01')>=35""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC016")

  }


  //C20_DICTIONARY_INCLUDE_TC017
  test("C20_DICTIONARY_INCLUDE_TC017", Include) {

    checkAnswer(s"""select MS_IP,count(*) from smart_500_DINC  group by MS_IP""",
      s"""select MS_IP,count(*) from smart_500_DINC_hive  group by MS_IP""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC017")

  }


  //C20_DICTIONARY_INCLUDE_TC018
  test("C20_DICTIONARY_INCLUDE_TC018", Include) {

    sql(s"""select MS_IP,SID,count(*) from smart_500_DINC  group by MS_IP,SID order by MS_IP limit 10""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC019
  test("C20_DICTIONARY_INCLUDE_TC019", Include) {

    sql(s"""select SID,length( MSISDN),avg(LAYER1ID),avg(TCP_DW_RETRANS) from smart_500_DINC  group by SID,length( MSISDN) order by SID limit 10""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC020
  test("C20_DICTIONARY_INCLUDE_TC020", Include) {

    sql(s"""select SID,length( MSISDN),max(LAYER1ID),min(LAYER1ID) from smart_500_DINC  group by SID,length( MSISDN) order by SID limit 10""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC021
  test("C20_DICTIONARY_INCLUDE_TC021", Include) {

    sql(s"""select SID,length( MSISDN),max(LAYER1ID),max(LAYER1ID) from smart_500_DINC  group by SID,length( MSISDN) order by SID limit 10""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC022
  test("C20_DICTIONARY_INCLUDE_TC022", Include) {

    sql(s"""select SID,length( MSISDN),min(LAYER1ID),min(LAYER1ID) from smart_500_DINC  group by SID,length( MSISDN) order by SID limit 10""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC023
  test("C20_DICTIONARY_INCLUDE_TC023", Include) {

    sql(s"""select SID,length( MSISDN),max(LAYER1ID),min(LAYER1ID),avg(LAYER1ID) from smart_500_DINC  group by SID,length( MSISDN) order by SID limit 10""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC024
  test("C20_DICTIONARY_INCLUDE_TC024", Include) {

    checkAnswer(s"""select concat(upper(MSISDN),1),sum(LAYER1ID) from smart_500_DINC  group by concat(upper(MSISDN),1)""",
      s"""select concat(upper(MSISDN),1),sum(LAYER1ID) from smart_500_DINC_hive  group by concat(upper(MSISDN),1)""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC024")

  }


  //C20_DICTIONARY_INCLUDE_TC025
  test("C20_DICTIONARY_INCLUDE_TC025", Include) {

    checkAnswer(s"""select upper(substring(MSISDN,1,4)),sum(LAYER1ID) from smart_500_DINC group by upper(substring(MSISDN,1,4)) """,
      s"""select upper(substring(MSISDN,1,4)),sum(LAYER1ID) from smart_500_DINC_hive group by upper(substring(MSISDN,1,4)) """, "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC025")

  }


  //C20_DICTIONARY_INCLUDE_TC026
  test("C20_DICTIONARY_INCLUDE_TC026", Include) {

    checkAnswer(s"""select max(SERVER_IP) from smart_500_DINC""",
      s"""select max(SERVER_IP) from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC026")

  }


  //C20_DICTIONARY_INCLUDE_TC027
  test("C20_DICTIONARY_INCLUDE_TC027", Include) {

    checkAnswer(s"""select max(SERVER_IP+10) from smart_500_DINC""",
      s"""select max(SERVER_IP+10) from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC027")

  }


  //C20_DICTIONARY_INCLUDE_TC028
  test("C20_DICTIONARY_INCLUDE_TC028", Include) {

    checkAnswer(s"""select max(MSISDN) from smart_500_DINC""",
      s"""select max(MSISDN) from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC028")

  }


  //C20_DICTIONARY_INCLUDE_TC029
  test("C20_DICTIONARY_INCLUDE_TC029", Include) {

    checkAnswer(s"""select max(MSISDN+10) from smart_500_DINC""",
      s"""select max(MSISDN+10) from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC029")

  }


  //C20_DICTIONARY_INCLUDE_TC030
  test("C20_DICTIONARY_INCLUDE_TC030", Include) {

    checkAnswer(s"""select avg(TCP_DW_RETRANS) from smart_500_DINC""",
      s"""select avg(TCP_DW_RETRANS) from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC030")

  }


  //C20_DICTIONARY_INCLUDE_TC031
  test("C20_DICTIONARY_INCLUDE_TC031", Include) {

    checkAnswer(s"""select avg(TCP_DW_RETRANS+10) from smart_500_DINC""",
      s"""select avg(TCP_DW_RETRANS+10) from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC031")

  }


  //C20_DICTIONARY_INCLUDE_TC032
  test("C20_DICTIONARY_INCLUDE_TC032", Include) {

    checkAnswer(s"""select avg(TCP_DW_RETRANS-10) from smart_500_DINC""",
      s"""select avg(TCP_DW_RETRANS-10) from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC032")

  }


  //C20_DICTIONARY_INCLUDE_TC033
  test("C20_DICTIONARY_INCLUDE_TC033", Include) {

    checkAnswer(s"""select count(TCP_DW_RETRANS)-10 from smart_500_DINC""",
      s"""select count(TCP_DW_RETRANS)-10 from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC033")

  }


  //C20_DICTIONARY_INCLUDE_TC034
  test("C20_DICTIONARY_INCLUDE_TC034", Include) {

    checkAnswer(s"""select count(TCP_DW_RETRANS)-10 from smart_500_DINC""",
      s"""select count(TCP_DW_RETRANS)-10 from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC034")

  }


  //C20_DICTIONARY_INCLUDE_TC035
  test("C20_DICTIONARY_INCLUDE_TC035", Include) {

    checkAnswer(s"""select count(TCP_DW_RETRANS)-10 from smart_500_DINC""",
      s"""select count(TCP_DW_RETRANS)-10 from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC035")

  }


  //C20_DICTIONARY_INCLUDE_TC036
  test("C20_DICTIONARY_INCLUDE_TC036", Include) {

    checkAnswer(s"""select sum(MSISDN), sum(DISTINCT MSISDN) from smart_500_DINC""",
      s"""select sum(MSISDN), sum(DISTINCT MSISDN) from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC036")

  }


  //C20_DICTIONARY_INCLUDE_TC037
  test("C20_DICTIONARY_INCLUDE_TC037", Include) {

    checkAnswer(s"""select count (if(TCP_DW_RETRANS>100,NULL,TCP_DW_RETRANS))  a from smart_500_DINC""",
      s"""select count (if(TCP_DW_RETRANS>100,NULL,TCP_DW_RETRANS))  a from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC037")

  }


  //C20_DICTIONARY_INCLUDE_TC038
  test("C20_DICTIONARY_INCLUDE_TC038", Include) {

    checkAnswer(s"""select count (if(TCP_DW_RETRANS<100,NULL,TCP_DW_RETRANS))  a from smart_500_DINC""",
      s"""select count (if(TCP_DW_RETRANS<100,NULL,TCP_DW_RETRANS))  a from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC038")

  }


  //C20_DICTIONARY_INCLUDE_TC039
  test("C20_DICTIONARY_INCLUDE_TC039", Include) {

    checkAnswer(s"""select count (if(TCP_DW_RETRANS=100,NULL,TCP_DW_RETRANS))  a from smart_500_DINC""",
      s"""select count (if(TCP_DW_RETRANS=100,NULL,TCP_DW_RETRANS))  a from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC039")

  }


  //C20_DICTIONARY_INCLUDE_TC040
  test("C20_DICTIONARY_INCLUDE_TC040", Include) {

    checkAnswer(s"""select count(TCP_DW_RETRANS) from smart_500_DINC where TCP_DW_RETRANS=100""",
      s"""select count(TCP_DW_RETRANS) from smart_500_DINC_hive where TCP_DW_RETRANS=100""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC040")

  }


  //C20_DICTIONARY_INCLUDE_TC041
  test("C20_DICTIONARY_INCLUDE_TC041", Include) {

    checkAnswer(s"""select count(TCP_DW_RETRANS) from smart_500_DINC where TCP_DW_RETRANS<100""",
      s"""select count(TCP_DW_RETRANS) from smart_500_DINC_hive where TCP_DW_RETRANS<100""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC041")

  }


  //C20_DICTIONARY_INCLUDE_TC042
  test("C20_DICTIONARY_INCLUDE_TC042", Include) {

    checkAnswer(s"""select count(TCP_DW_RETRANS) from smart_500_DINC where TCP_DW_RETRANS>100""",
      s"""select count(TCP_DW_RETRANS) from smart_500_DINC_hive where TCP_DW_RETRANS>100""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC042")

  }


  //C20_DICTIONARY_INCLUDE_TC043
  test("C20_DICTIONARY_INCLUDE_TC043", Include) {

    sql(s"""select MSISDN, TCP_DW_RETRANS + LAYER1ID as a  from smart_500_DINC order by MSISDN limit 10""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC044
  test("C20_DICTIONARY_INCLUDE_TC044", Include) {

    sql(s"""select MSISDN, sum(TCP_DW_RETRANS + 10) Total from smart_500_DINC group by  MSISDN order by Total limit 10""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC045
  test("C20_DICTIONARY_INCLUDE_TC045", Include) {

    checkAnswer(s"""select MSISDN, min(LAYER1ID + 10)  Total from smart_500_DINC group by  MSISDN order by MSISDN""",
      s"""select MSISDN, min(LAYER1ID + 10)  Total from smart_500_DINC_hive group by  MSISDN order by MSISDN""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC045")

  }


  //C20_DICTIONARY_INCLUDE_TC046
  test("C20_DICTIONARY_INCLUDE_TC046", Include) {

    checkAnswer(s"""select avg (if(LAYER1ID>100,NULL,LAYER1ID))  a from smart_500_DINC""",
      s"""select avg (if(LAYER1ID>100,NULL,LAYER1ID))  a from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC046")

  }


  //C20_DICTIONARY_INCLUDE_TC047
  test("C20_DICTIONARY_INCLUDE_TC047", Include) {

    checkAnswer(s"""select avg (if(TCP_DW_RETRANS>100,NULL,TCP_DW_RETRANS))  a from smart_500_DINC""",
      s"""select avg (if(TCP_DW_RETRANS>100,NULL,TCP_DW_RETRANS))  a from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC047")

  }


  //C20_DICTIONARY_INCLUDE_TC048
  ignore("C20_DICTIONARY_INCLUDE_TC048", Include) {

    checkAnswer(s"""select variance(LAYER1ID) as a   from smart_500_DINC""",
      s"""select variance(LAYER1ID) as a   from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC048")

  }


  //C20_DICTIONARY_INCLUDE_TC049
  ignore("C20_DICTIONARY_INCLUDE_TC049", Include) {

    checkAnswer(s"""select var_pop(LAYER1ID)  as a from smart_500_DINC""",
      s"""select var_pop(LAYER1ID)  as a from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC049")

  }


  //C20_DICTIONARY_INCLUDE_TC050
  ignore("C20_DICTIONARY_INCLUDE_TC050", Include) {

    checkAnswer(s"""select var_samp(LAYER1ID) as a  from smart_500_DINC""",
      s"""select var_samp(LAYER1ID) as a  from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC050")

  }


  //C20_DICTIONARY_INCLUDE_TC051
  ignore("C20_DICTIONARY_INCLUDE_TC051", Include) {

    checkAnswer(s"""select stddev_pop(LAYER1ID) as a  from smart_500_DINC""",
      s"""select stddev_pop(LAYER1ID) as a  from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC051")

  }


  //C20_DICTIONARY_INCLUDE_TC052
  ignore("C20_DICTIONARY_INCLUDE_TC052", Include) {

    checkAnswer(s"""select stddev_samp(LAYER1ID)  as a from smart_500_DINC""",
      s"""select stddev_samp(LAYER1ID)  as a from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC052")

  }


  //C20_DICTIONARY_INCLUDE_TC053
  ignore("C20_DICTIONARY_INCLUDE_TC053", Include) {

    checkAnswer(s"""select covar_pop(LAYER1ID,LAYER1ID) as a  from smart_500_DINC""",
      s"""select covar_pop(LAYER1ID,LAYER1ID) as a  from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC053")

  }


  //C20_DICTIONARY_INCLUDE_TC054
  ignore("C20_DICTIONARY_INCLUDE_TC054", Include) {

    checkAnswer(s"""select covar_samp(LAYER1ID,LAYER1ID) as a  from smart_500_DINC""",
      s"""select covar_samp(LAYER1ID,LAYER1ID) as a  from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC054")

  }


  //C20_DICTIONARY_INCLUDE_TC055
  test("C20_DICTIONARY_INCLUDE_TC055", Include) {

    checkAnswer(s"""select corr(LAYER1ID,LAYER1ID)  as a from smart_500_DINC""",
      s"""select corr(LAYER1ID,LAYER1ID)  as a from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC055")

  }


  //C20_DICTIONARY_INCLUDE_TC056
  test("C20_DICTIONARY_INCLUDE_TC056", Include) {

    checkAnswer(s"""select percentile(LAYER1ID,0.2) as  a  from smart_500_DINC""",
      s"""select percentile(LAYER1ID,0.2) as  a  from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC056")

  }


  //C20_DICTIONARY_INCLUDE_TC057
  test("C20_DICTIONARY_INCLUDE_TC057", Include) {

    checkAnswer(s"""select percentile(LAYER1ID,array(0,0.2,0.3,1))  as  a from smart_500_DINC""",
      s"""select percentile(LAYER1ID,array(0,0.2,0.3,1))  as  a from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC057")

  }


  //C20_DICTIONARY_INCLUDE_TC058
  test("C20_DICTIONARY_INCLUDE_TC058", Include) {

    sql(s"""select percentile_approx(LAYER1ID,0.2) as a  from (select LAYER1ID from smart_500_DINC order by LAYER1ID) t""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC059
  test("C20_DICTIONARY_INCLUDE_TC059", Include) {

    sql(s"""select percentile_approx(LAYER1ID,0.2,5) as a  from (select LAYER1ID from smart_500_DINC order by LAYER1ID) t""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC060
  test("C20_DICTIONARY_INCLUDE_TC060", Include) {

    sql(s"""select percentile_approx(LAYER1ID,array(0.2,0.3,0.99))  as a from (select LAYER1ID from smart_500_DINC order by LAYER1ID) t""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC061
  test("C20_DICTIONARY_INCLUDE_TC061", Include) {

    sql(s"""select percentile_approx(LAYER1ID,array(0.2,0.3,0.99),5) as a from (select LAYER1ID from smart_500_DINC order by LAYER1ID) t""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC062
  test("C20_DICTIONARY_INCLUDE_TC062", Include) {

    sql(s"""select histogram_numeric(LAYER1ID,2)  as a from (select LAYER1ID from smart_500_DINC order by LAYER1ID) t""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC063
  ignore("C20_DICTIONARY_INCLUDE_TC063", Include) {

    checkAnswer(s"""select variance(TCP_DW_RETRANS) as a   from smart_500_DINC""",
      s"""select variance(TCP_DW_RETRANS) as a   from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC063")

  }


  //C20_DICTIONARY_INCLUDE_TC064
  ignore("C20_DICTIONARY_INCLUDE_TC064", Include) {

    checkAnswer(s"""select var_pop(TCP_DW_RETRANS)  as a from smart_500_DINC""",
      s"""select var_pop(TCP_DW_RETRANS)  as a from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC064")

  }


  //C20_DICTIONARY_INCLUDE_TC065
  ignore("C20_DICTIONARY_INCLUDE_TC065", Include) {

    checkAnswer(s"""select var_samp(TCP_DW_RETRANS) as a  from smart_500_DINC""",
      s"""select var_samp(TCP_DW_RETRANS) as a  from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC065")

  }


  //C20_DICTIONARY_INCLUDE_TC066
  ignore("C20_DICTIONARY_INCLUDE_TC066", Include) {

    checkAnswer(s"""select stddev_pop(TCP_DW_RETRANS) as a  from smart_500_DINC""",
      s"""select stddev_pop(TCP_DW_RETRANS) as a  from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC066")

  }


  //C20_DICTIONARY_INCLUDE_TC067
  ignore("C20_DICTIONARY_INCLUDE_TC067", Include) {

    checkAnswer(s"""select stddev_samp(TCP_DW_RETRANS)  as a from smart_500_DINC""",
      s"""select stddev_samp(TCP_DW_RETRANS)  as a from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC067")

  }


  //C20_DICTIONARY_INCLUDE_TC068
  ignore("C20_DICTIONARY_INCLUDE_TC068", Include) {

    checkAnswer(s"""select covar_pop(TCP_DW_RETRANS,TCP_DW_RETRANS) as a  from smart_500_DINC""",
      s"""select covar_pop(TCP_DW_RETRANS,TCP_DW_RETRANS) as a  from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC068")

  }


  //C20_DICTIONARY_INCLUDE_TC069
  ignore("C20_DICTIONARY_INCLUDE_TC069", Include) {

    checkAnswer(s"""select covar_samp(TCP_DW_RETRANS,TCP_DW_RETRANS) as a  from smart_500_DINC""",
      s"""select covar_samp(TCP_DW_RETRANS,TCP_DW_RETRANS) as a  from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC069")

  }


  //C20_DICTIONARY_INCLUDE_TC070
  test("C20_DICTIONARY_INCLUDE_TC070", Include) {

    checkAnswer(s"""select corr(TCP_DW_RETRANS,TCP_DW_RETRANS)  as a from smart_500_DINC""",
      s"""select corr(TCP_DW_RETRANS,TCP_DW_RETRANS)  as a from smart_500_DINC_hive""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC070")

  }


  //C20_DICTIONARY_INCLUDE_TC073
  test("C20_DICTIONARY_INCLUDE_TC073", Include) {

    sql(s"""select percentile_approx(TCP_DW_RETRANS,0.2) as a  from (select TCP_DW_RETRANS from smart_500_DINC order by TCP_DW_RETRANS) t""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC074
  test("C20_DICTIONARY_INCLUDE_TC074", Include) {

    sql(s"""select percentile_approx(TCP_DW_RETRANS,0.2,5) as a  from (select TCP_DW_RETRANS from smart_500_DINC order by TCP_DW_RETRANS) t""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC075
  test("C20_DICTIONARY_INCLUDE_TC075", Include) {

    sql(s"""select percentile_approx(TCP_DW_RETRANS,array(0.2,0.3,0.99))  as a from (select TCP_DW_RETRANS from smart_500_DINC order by TCP_DW_RETRANS) t""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC076
  test("C20_DICTIONARY_INCLUDE_TC076", Include) {

    sql(s"""select percentile_approx(TCP_DW_RETRANS,array(0.2,0.3,0.99),5) as a from (select TCP_DW_RETRANS from smart_500_DINC order by TCP_DW_RETRANS) t""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC077
  test("C20_DICTIONARY_INCLUDE_TC077", Include) {

    sql(s"""select histogram_numeric(TCP_DW_RETRANS,2)  as a from (select TCP_DW_RETRANS from smart_500_DINC order by TCP_DW_RETRANS) t""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC094
  test("C20_DICTIONARY_INCLUDE_TC094", Include) {

    sql(s"""select Upper(streaming_url) a ,host from smart_500_DINC order by host limit 10""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC095
  test("C20_DICTIONARY_INCLUDE_TC095", Include) {

    sql(s"""select Lower(streaming_url) a  from smart_500_DINC order by host limit 10""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC096
  test("C20_DICTIONARY_INCLUDE_TC096", Include) {

    sql(s"""select streaming_url as b,LAYER1ID as a from smart_500_DINC  order by a,b asc limit 10""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC097
  test("C20_DICTIONARY_INCLUDE_TC097", Include) {

    sql(s"""select streaming_url as b,TCP_DW_RETRANS as a from smart_500_DINC  order by a,b desc limit 10""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC098
  test("C20_DICTIONARY_INCLUDE_TC098", Include) {

    checkAnswer(s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DINC where streaming_url ='www.hua1/xyz'""",
      s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DINC_hive where streaming_url ='www.hua1/xyz'""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC098")

  }


  //C20_DICTIONARY_INCLUDE_TC099
  test("C20_DICTIONARY_INCLUDE_TC099", Include) {

    sql(s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DINC where streaming_url ='www.hua90/xyz' and TCP_DW_RETRANS ='82.0' limit 10""").collect

  }


  //C20_DICTIONARY_INCLUDE_TC100
  test("C20_DICTIONARY_INCLUDE_TC100", Include) {

    checkAnswer(s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DINC where streaming_url ='www.hua1/xyz' or  TCP_DW_RETRANS ='82.0'""",
      s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DINC_hive where streaming_url ='www.hua1/xyz' or  TCP_DW_RETRANS ='82.0'""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC100")

  }


  //C20_DICTIONARY_INCLUDE_TC101
  test("C20_DICTIONARY_INCLUDE_TC101", Include) {

    checkAnswer(s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DINC where streaming_url !='www.hua1/xyz'  order by LAYER1ID,TCP_DW_RETRANS,streaming_url""",
      s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DINC_hive where streaming_url !='www.hua1/xyz'  order by LAYER1ID,TCP_DW_RETRANS,streaming_url""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC101")

  }


  //C20_DICTIONARY_INCLUDE_TC102
  test("C20_DICTIONARY_INCLUDE_TC102", Include) {

    checkAnswer(s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DINC where streaming_url !='www.hua1/xyz' and TCP_DW_RETRANS !='152.0'  order by  LAYER1ID,TCP_DW_RETRANS,streaming_url limit 10""",
      s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DINC_hive where streaming_url !='www.hua1/xyz' and TCP_DW_RETRANS !='152.0'  order by  LAYER1ID,TCP_DW_RETRANS,streaming_url limit 10""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC102")

  }


  //C20_DICTIONARY_INCLUDE_TC103
  test("C20_DICTIONARY_INCLUDE_TC103", Include) {

    checkAnswer(s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DINC where TCP_DW_RETRANS >2.0 order by LAYER1ID,TCP_DW_RETRANS,streaming_url limit 10""",
      s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DINC_hive where TCP_DW_RETRANS >2.0 order by LAYER1ID,TCP_DW_RETRANS,streaming_url limit 10""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC103")

  }


  //C20_DICTIONARY_INCLUDE_TC104
  test("C20_DICTIONARY_INCLUDE_TC104", Include) {

    checkAnswer(s"""select LAYER1ID as a from smart_500_DINC where LAYER1ID<=>LAYER1ID order by LAYER1ID desc limit 10""",
      s"""select LAYER1ID as a from smart_500_DINC_hive where LAYER1ID<=>LAYER1ID order by LAYER1ID desc limit 10""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC104")

  }


  //C20_DICTIONARY_INCLUDE_TC105
  test("C20_DICTIONARY_INCLUDE_TC105", Include) {

    checkAnswer(s"""SELECT LAYER1ID,TCP_DW_RETRANS,streaming_url FROM (select * from smart_500_DINC) SUB_QRY ORDER BY LAYER1ID,TCP_DW_RETRANS,streaming_url ASC limit 10""",
      s"""SELECT LAYER1ID,TCP_DW_RETRANS,streaming_url FROM (select * from smart_500_DINC_hive) SUB_QRY ORDER BY LAYER1ID,TCP_DW_RETRANS,streaming_url ASC limit 10""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC105")

  }


  //C20_DICTIONARY_INCLUDE_TC106
  test("C20_DICTIONARY_INCLUDE_TC106", Include) {

    checkAnswer(s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DINC where ( LAYER1ID+1) == 101 order by TCP_DW_RETRANS,LAYER1ID limit 5""",
      s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DINC_hive where ( LAYER1ID+1) == 101 order by TCP_DW_RETRANS,LAYER1ID limit 5""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC106")

  }


  //C20_DICTIONARY_INCLUDE_TC107
  test("C20_DICTIONARY_INCLUDE_TC107", Include) {

    checkAnswer(s"""select  LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DINC where  streaming_url is  null order by LAYER1ID,TCP_DW_RETRANS,streaming_url                                """,
      s"""select  LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DINC_hive where  streaming_url is  null order by LAYER1ID,TCP_DW_RETRANS,streaming_url                                """, "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC107")

  }


  //C20_DICTIONARY_INCLUDE_TC108
  test("C20_DICTIONARY_INCLUDE_TC108", Include) {

    checkAnswer(s"""select  LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DINC where  streaming_url is  not null order by LAYER1ID,TCP_DW_RETRANS,streaming_url""",
      s"""select  LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DINC_hive where  streaming_url is  not null order by LAYER1ID,TCP_DW_RETRANS,streaming_url""", "QueriesIncludeDictionaryTestCase_C20_DICTIONARY_INCLUDE_TC108")

  }

  override def afterAll {
    sql("drop table if exists VMALL_DICTIONARY_INCLUDE")
    sql("drop table if exists VMALL_DICTIONARY_INCLUDE_hive")
    sql("drop table if exists VMALL_DICTIONARY_INCLUDE1")
    sql("drop table if exists VMALL_DICTIONARY_INCLUDE1_hive")
    sql("drop table if exists smart_500_DINC")
    sql("drop table if exists smart_500_DINC_hive")
  }
}