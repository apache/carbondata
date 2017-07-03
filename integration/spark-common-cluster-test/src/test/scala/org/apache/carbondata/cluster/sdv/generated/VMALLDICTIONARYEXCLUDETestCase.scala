
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
 * Test Class for vmalldictionaryexclude to verify all scenerios
 */

class VMALLDICTIONARYEXCLUDETestCase extends QueryTest with BeforeAndAfterAll {
         

//VMALL_DICTIONARY_EXCLUDE_CreateCube_Drop
test("VMALL_DICTIONARY_EXCLUDE_CreateCube_Drop", Include) {
  sql(s"""drop table if exists  vmall_dictionary_exclude""").collect

  sql(s"""drop table if exists  vmall_dictionary_exclude_hive""").collect

}
       

//VMALL_DICTIONARY_EXCLUDE_CreateCube
test("VMALL_DICTIONARY_EXCLUDE_CreateCube", Include) {
  sql(s"""create table  VMALL_DICTIONARY_EXCLUDE (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt)  STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='imei')""").collect

  sql(s"""create table  VMALL_DICTIONARY_EXCLUDE_hive (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt)   ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//VMALL_DICTIONARY_EXCLUDE_CreateCube_count
test("VMALL_DICTIONARY_EXCLUDE_CreateCube_count", Include) {
  sql(s"""select count(*) from VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_DataLoad
test("VMALL_DICTIONARY_EXCLUDE_DataLoad", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table VMALL_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table VMALL_DICTIONARY_EXCLUDE_hive """).collect

}
       

//VMALL_DICTIONARY_EXCLUDE_001
test("VMALL_DICTIONARY_EXCLUDE_001", Include) {
  checkAnswer(s"""Select count(imei) from VMALL_DICTIONARY_EXCLUDE""",
    s"""Select count(imei) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_002
test("VMALL_DICTIONARY_EXCLUDE_002", Include) {
  checkAnswer(s"""select count(DISTINCT imei) as a from VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(DISTINCT imei) as a from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_003
test("VMALL_DICTIONARY_EXCLUDE_003", Include) {
  checkAnswer(s"""select sum(Latest_month)+10 as a ,imei  from VMALL_DICTIONARY_EXCLUDE group by imei order by imei""",
    s"""select sum(Latest_month)+10 as a ,imei  from VMALL_DICTIONARY_EXCLUDE_hive group by imei order by imei""")
}
       

//VMALL_DICTIONARY_EXCLUDE_004
test("VMALL_DICTIONARY_EXCLUDE_004", Include) {
  checkAnswer(s"""select max(imei),min(imei) from VMALL_DICTIONARY_EXCLUDE""",
    s"""select max(imei),min(imei) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_005
test("VMALL_DICTIONARY_EXCLUDE_005", Include) {
  checkAnswer(s"""select min(imei), max(imei) Total from VMALL_DICTIONARY_EXCLUDE group by  channelsId order by Total""",
    s"""select min(imei), max(imei) Total from VMALL_DICTIONARY_EXCLUDE_hive group by  channelsId order by Total""")
}
       

//VMALL_DICTIONARY_EXCLUDE_006
test("VMALL_DICTIONARY_EXCLUDE_006", Include) {
  checkAnswer(s"""select last(imei) a from VMALL_DICTIONARY_EXCLUDE  group by imei order by imei limit 1""",
    s"""select last(imei) a from VMALL_DICTIONARY_EXCLUDE_hive  group by imei order by imei limit 1""")
}
       

//VMALL_DICTIONARY_EXCLUDE_007
test("VMALL_DICTIONARY_EXCLUDE_007", Include) {
  sql(s"""select FIRST(imei) a from VMALL_DICTIONARY_EXCLUDE group by imei order by imei limit 1""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_008
test("VMALL_DICTIONARY_EXCLUDE_008", Include) {
  checkAnswer(s"""select imei,count(imei) a from VMALL_DICTIONARY_EXCLUDE group by imei order by imei""",
    s"""select imei,count(imei) a from VMALL_DICTIONARY_EXCLUDE_hive group by imei order by imei""")
}
       

//VMALL_DICTIONARY_EXCLUDE_009
test("VMALL_DICTIONARY_EXCLUDE_009", Include) {
  checkAnswer(s"""select Lower(imei) a  from VMALL_DICTIONARY_EXCLUDE order by imei""",
    s"""select Lower(imei) a  from VMALL_DICTIONARY_EXCLUDE_hive order by imei""")
}
       

//VMALL_DICTIONARY_EXCLUDE_010
test("VMALL_DICTIONARY_EXCLUDE_010", Include) {
  checkAnswer(s"""select distinct imei from VMALL_DICTIONARY_EXCLUDE order by imei""",
    s"""select distinct imei from VMALL_DICTIONARY_EXCLUDE_hive order by imei""")
}
       

//VMALL_DICTIONARY_EXCLUDE_011
test("VMALL_DICTIONARY_EXCLUDE_011", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_EXCLUDE order by imei limit 101 """,
    s"""select imei from VMALL_DICTIONARY_EXCLUDE_hive order by imei limit 101 """)
}
       

//VMALL_DICTIONARY_EXCLUDE_012
test("VMALL_DICTIONARY_EXCLUDE_012", Include) {
  checkAnswer(s"""select imei as a from VMALL_DICTIONARY_EXCLUDE  order by a asc limit 10""",
    s"""select imei as a from VMALL_DICTIONARY_EXCLUDE_hive  order by a asc limit 10""")
}
       

//VMALL_DICTIONARY_EXCLUDE_013
test("VMALL_DICTIONARY_EXCLUDE_013", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_EXCLUDE where  (contractNumber == 9223372047700) and (imei=='1AA100004')""",
    s"""select imei from VMALL_DICTIONARY_EXCLUDE_hive where  (contractNumber == 9223372047700) and (imei=='1AA100004')""")
}
       

//VMALL_DICTIONARY_EXCLUDE_014
test("VMALL_DICTIONARY_EXCLUDE_014", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_EXCLUDE where imei !='1AA100064' order by imei""",
    s"""select imei from VMALL_DICTIONARY_EXCLUDE_hive where imei !='1AA100064' order by imei""")
}
       

//VMALL_DICTIONARY_EXCLUDE_015
test("VMALL_DICTIONARY_EXCLUDE_015", Include) {
  checkAnswer(s"""select imei  from VMALL_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
    s"""select imei  from VMALL_DICTIONARY_EXCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""")
}
       

//VMALL_DICTIONARY_EXCLUDE_016
test("VMALL_DICTIONARY_EXCLUDE_016", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_EXCLUDE where imei !='1AA100012' order by imei""",
    s"""select imei from VMALL_DICTIONARY_EXCLUDE_hive where imei !='1AA100012' order by imei""")
}
       

//VMALL_DICTIONARY_EXCLUDE_017
test("VMALL_DICTIONARY_EXCLUDE_017", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_EXCLUDE where imei >'1AA100012' order by imei""",
    s"""select imei from VMALL_DICTIONARY_EXCLUDE_hive where imei >'1AA100012' order by imei""")
}
       

//VMALL_DICTIONARY_EXCLUDE_018
test("VMALL_DICTIONARY_EXCLUDE_018", Include) {
  checkAnswer(s"""select imei  from VMALL_DICTIONARY_EXCLUDE where imei<>imei""",
    s"""select imei  from VMALL_DICTIONARY_EXCLUDE_hive where imei<>imei""")
}
       

//VMALL_DICTIONARY_EXCLUDE_019
test("VMALL_DICTIONARY_EXCLUDE_019", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_EXCLUDE where imei != Latest_areaId order by imei""",
    s"""select imei from VMALL_DICTIONARY_EXCLUDE_hive where imei != Latest_areaId order by imei""")
}
       

//VMALL_DICTIONARY_EXCLUDE_020
test("VMALL_DICTIONARY_EXCLUDE_020", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_EXCLUDE where Latest_areaId<imei order by imei""",
    s"""select imei from VMALL_DICTIONARY_EXCLUDE_hive where Latest_areaId<imei order by imei""")
}
       

//VMALL_DICTIONARY_EXCLUDE_021
test("VMALL_DICTIONARY_EXCLUDE_021", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_EXCLUDE where Latest_DAY<=imei order by imei""",
    s"""select imei from VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY<=imei order by imei""")
}
       

//VMALL_DICTIONARY_EXCLUDE_022
test("VMALL_DICTIONARY_EXCLUDE_022", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_EXCLUDE where imei <'1AA10002' order by imei""",
    s"""select imei from VMALL_DICTIONARY_EXCLUDE_hive where imei <'1AA10002' order by imei""")
}
       

//VMALL_DICTIONARY_EXCLUDE_023
test("VMALL_DICTIONARY_EXCLUDE_023", Include) {
  checkAnswer(s"""select Latest_day  from VMALL_DICTIONARY_EXCLUDE where imei IS NULL""",
    s"""select Latest_day  from VMALL_DICTIONARY_EXCLUDE_hive where imei IS NULL""")
}
       

//VMALL_DICTIONARY_EXCLUDE_024
test("VMALL_DICTIONARY_EXCLUDE_024", Include) {
  checkAnswer(s"""select Latest_day  from VMALL_DICTIONARY_EXCLUDE where imei IS NOT NULL order by imei""",
    s"""select Latest_day  from VMALL_DICTIONARY_EXCLUDE_hive where imei IS NOT NULL order by imei""")
}
       

//VMALL_DICTIONARY_EXCLUDE_025
test("VMALL_DICTIONARY_EXCLUDE_025", Include) {
  checkAnswer(s"""Select count(imei),min(imei) from VMALL_DICTIONARY_EXCLUDE """,
    s"""Select count(imei),min(imei) from VMALL_DICTIONARY_EXCLUDE_hive """)
}
       

//VMALL_DICTIONARY_EXCLUDE_026
test("VMALL_DICTIONARY_EXCLUDE_026", Include) {
  checkAnswer(s"""select count(DISTINCT imei,latest_day) as a from VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(DISTINCT imei,latest_day) as a from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_027
test("VMALL_DICTIONARY_EXCLUDE_027", Include) {
  checkAnswer(s"""select max(imei),min(imei),count(imei) from VMALL_DICTIONARY_EXCLUDE""",
    s"""select max(imei),min(imei),count(imei) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_028
test("VMALL_DICTIONARY_EXCLUDE_028", Include) {
  checkAnswer(s"""select sum(imei),avg(imei),count(imei) a  from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(imei),avg(imei),count(imei) a  from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_029
test("VMALL_DICTIONARY_EXCLUDE_029", Include) {
  sql(s"""select last(imei),Min(imei),max(imei)  a from VMALL_DICTIONARY_EXCLUDE  order by a""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_030
test("VMALL_DICTIONARY_EXCLUDE_030", Include) {
  sql(s"""select FIRST(imei),Last(imei) a from VMALL_DICTIONARY_EXCLUDE group by imei order by imei limit 1""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_031
test("VMALL_DICTIONARY_EXCLUDE_031", Include) {
  checkAnswer(s"""select imei,count(imei) a from VMALL_DICTIONARY_EXCLUDE group by imei order by imei""",
    s"""select imei,count(imei) a from VMALL_DICTIONARY_EXCLUDE_hive group by imei order by imei""")
}
       

//VMALL_DICTIONARY_EXCLUDE_032
test("VMALL_DICTIONARY_EXCLUDE_032", Include) {
  checkAnswer(s"""select Lower(imei),upper(imei)  a  from VMALL_DICTIONARY_EXCLUDE order by imei""",
    s"""select Lower(imei),upper(imei)  a  from VMALL_DICTIONARY_EXCLUDE_hive order by imei""")
}
       

//VMALL_DICTIONARY_EXCLUDE_033
test("VMALL_DICTIONARY_EXCLUDE_033", Include) {
  checkAnswer(s"""select imei as a from VMALL_DICTIONARY_EXCLUDE  order by a asc limit 10""",
    s"""select imei as a from VMALL_DICTIONARY_EXCLUDE_hive  order by a asc limit 10""")
}
       

//VMALL_DICTIONARY_EXCLUDE_034
test("VMALL_DICTIONARY_EXCLUDE_034", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_EXCLUDE where  (contractNumber == 9223372047700) and (imei=='1AA100012')""",
    s"""select imei from VMALL_DICTIONARY_EXCLUDE_hive where  (contractNumber == 9223372047700) and (imei=='1AA100012')""")
}
       

//VMALL_DICTIONARY_EXCLUDE_035
test("VMALL_DICTIONARY_EXCLUDE_035", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_EXCLUDE where imei !='8imei' order by imei""",
    s"""select imei from VMALL_DICTIONARY_EXCLUDE_hive where imei !='8imei' order by imei""")
}
       

//VMALL_DICTIONARY_EXCLUDE_036
test("VMALL_DICTIONARY_EXCLUDE_036", Include) {
  checkAnswer(s"""select imei  from VMALL_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
    s"""select imei  from VMALL_DICTIONARY_EXCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""")
}
       

//VMALL_DICTIONARY_EXCLUDE_037
test("VMALL_DICTIONARY_EXCLUDE_037", Include) {
  checkAnswer(s"""Select count(contractNumber) from VMALL_DICTIONARY_EXCLUDE""",
    s"""Select count(contractNumber) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_038
test("VMALL_DICTIONARY_EXCLUDE_038", Include) {
  checkAnswer(s"""select count(DISTINCT contractNumber) as a from VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(DISTINCT contractNumber) as a from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_039
test("VMALL_DICTIONARY_EXCLUDE_039", Include) {
  checkAnswer(s"""select sum(contractNumber)+10 as a ,contractNumber  from VMALL_DICTIONARY_EXCLUDE group by contractNumber""",
    s"""select sum(contractNumber)+10 as a ,contractNumber  from VMALL_DICTIONARY_EXCLUDE_hive group by contractNumber""")
}
       

//VMALL_DICTIONARY_EXCLUDE_040
test("VMALL_DICTIONARY_EXCLUDE_040", Include) {
  checkAnswer(s"""select max(contractNumber),min(contractNumber) from VMALL_DICTIONARY_EXCLUDE""",
    s"""select max(contractNumber),min(contractNumber) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_041
test("VMALL_DICTIONARY_EXCLUDE_041", Include) {
  checkAnswer(s"""select sum(contractNumber) a  from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(contractNumber) a  from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_042
test("VMALL_DICTIONARY_EXCLUDE_042", Include) {
  checkAnswer(s"""select avg(contractNumber) a  from VMALL_DICTIONARY_EXCLUDE""",
    s"""select avg(contractNumber) a  from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_043
test("VMALL_DICTIONARY_EXCLUDE_043", Include) {
  checkAnswer(s"""select min(contractNumber) a  from VMALL_DICTIONARY_EXCLUDE""",
    s"""select min(contractNumber) a  from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_044
test("VMALL_DICTIONARY_EXCLUDE_044", Include) {
  sql(s"""select variance(contractNumber) as a   from (select contractNumber from VMALL_DICTIONARY_EXCLUDE order by contractNumber) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_045
test("VMALL_DICTIONARY_EXCLUDE_045", Include) {
  sql(s"""select var_pop(contractNumber)  as a from VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_046
test("VMALL_DICTIONARY_EXCLUDE_046", Include) {
  sql(s"""select var_samp(contractNumber) as a  from VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_047
test("VMALL_DICTIONARY_EXCLUDE_047", Include) {
  sql(s"""select stddev_pop(contractNumber) as a  from (select contractNumber from VMALL_DICTIONARY_EXCLUDE order by contractNumber) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_048
test("VMALL_DICTIONARY_EXCLUDE_048", Include) {
  sql(s"""select stddev_samp(contractNumber)  as a from (select contractNumber from VMALL_DICTIONARY_EXCLUDE order by contractNumber) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_049
test("VMALL_DICTIONARY_EXCLUDE_049", Include) {
  sql(s"""select covar_pop(contractNumber,contractNumber) as a  from (select contractNumber from VMALL_DICTIONARY_EXCLUDE order by contractNumber) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_050
test("VMALL_DICTIONARY_EXCLUDE_050", Include) {
  sql(s"""select covar_samp(contractNumber,contractNumber) as a  from (select contractNumber from VMALL_DICTIONARY_EXCLUDE order by contractNumber) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_051
test("VMALL_DICTIONARY_EXCLUDE_051", Include) {
  checkAnswer(s"""select corr(contractNumber,contractNumber)  as a from VMALL_DICTIONARY_EXCLUDE""",
    s"""select corr(contractNumber,contractNumber)  as a from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_052
test("VMALL_DICTIONARY_EXCLUDE_052", Include) {
  checkAnswer(s"""select percentile_approx(contractNumber,0.2) as a  from (select contractNumber from VMALL_DICTIONARY_EXCLUDE order by contractNumber) t""",
    s"""select percentile_approx(contractNumber,0.2) as a  from (select contractNumber from VMALL_DICTIONARY_EXCLUDE_hive order by contractNumber) t""")
}
       

//VMALL_DICTIONARY_EXCLUDE_053
test("VMALL_DICTIONARY_EXCLUDE_053", Include) {
  checkAnswer(s"""select percentile_approx(contractNumber,0.2,5) as a  from (select contractNumber from VMALL_DICTIONARY_EXCLUDE order by contractNumber) t""",
    s"""select percentile_approx(contractNumber,0.2,5) as a  from (select contractNumber from VMALL_DICTIONARY_EXCLUDE_hive order by contractNumber) t""")
}
       

//VMALL_DICTIONARY_EXCLUDE_054
test("VMALL_DICTIONARY_EXCLUDE_054", Include) {
  checkAnswer(s"""select percentile_approx(contractNumber,array(0.2,0.3,0.99))  as a from (select contractNumber from VMALL_DICTIONARY_EXCLUDE order by contractNumber) t""",
    s"""select percentile_approx(contractNumber,array(0.2,0.3,0.99))  as a from (select contractNumber from VMALL_DICTIONARY_EXCLUDE_hive order by contractNumber) t""")
}
       

//VMALL_DICTIONARY_EXCLUDE_055
test("VMALL_DICTIONARY_EXCLUDE_055", Include) {
  checkAnswer(s"""select percentile_approx(contractNumber,array(0.2,0.3,0.99),5) as a from (select contractNumber from VMALL_DICTIONARY_EXCLUDE order by contractNumber) t""",
    s"""select percentile_approx(contractNumber,array(0.2,0.3,0.99),5) as a from (select contractNumber from VMALL_DICTIONARY_EXCLUDE_hive order by contractNumber) t""")
}
       

//VMALL_DICTIONARY_EXCLUDE_056
test("VMALL_DICTIONARY_EXCLUDE_056", Include) {
  checkAnswer(s"""select histogram_numeric(contractNumber,2)  as a from (select contractNumber from VMALL_DICTIONARY_EXCLUDE order by contractNumber) t""",
    s"""select histogram_numeric(contractNumber,2)  as a from (select contractNumber from VMALL_DICTIONARY_EXCLUDE_hive order by contractNumber) t""")
}
       

//VMALL_DICTIONARY_EXCLUDE_057
test("VMALL_DICTIONARY_EXCLUDE_057", Include) {
  checkAnswer(s"""select contractNumber+ 10 as a  from VMALL_DICTIONARY_EXCLUDE order by a""",
    s"""select contractNumber+ 10 as a  from VMALL_DICTIONARY_EXCLUDE_hive order by a""")
}
       

//VMALL_DICTIONARY_EXCLUDE_058
test("VMALL_DICTIONARY_EXCLUDE_058", Include) {
  checkAnswer(s"""select min(contractNumber), max(contractNumber+ 10) Total from VMALL_DICTIONARY_EXCLUDE group by  channelsId order by Total""",
    s"""select min(contractNumber), max(contractNumber+ 10) Total from VMALL_DICTIONARY_EXCLUDE_hive group by  channelsId order by Total""")
}
       

//VMALL_DICTIONARY_EXCLUDE_059
test("VMALL_DICTIONARY_EXCLUDE_059", Include) {
  checkAnswer(s"""select last(contractNumber) a from VMALL_DICTIONARY_EXCLUDE  order by a""",
    s"""select last(contractNumber) a from VMALL_DICTIONARY_EXCLUDE_hive  order by a""")
}
       

//VMALL_DICTIONARY_EXCLUDE_060
test("VMALL_DICTIONARY_EXCLUDE_060", Include) {
  sql(s"""select FIRST(contractNumber) a from VMALL_DICTIONARY_EXCLUDE order by a""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_061
test("VMALL_DICTIONARY_EXCLUDE_061", Include) {
  checkAnswer(s"""select contractNumber,count(contractNumber) a from VMALL_DICTIONARY_EXCLUDE group by contractNumber order by contractNumber""",
    s"""select contractNumber,count(contractNumber) a from VMALL_DICTIONARY_EXCLUDE_hive group by contractNumber order by contractNumber""")
}
       

//VMALL_DICTIONARY_EXCLUDE_062
test("VMALL_DICTIONARY_EXCLUDE_062", Include) {
  checkAnswer(s"""select Lower(contractNumber) a  from VMALL_DICTIONARY_EXCLUDE order by contractNumber""",
    s"""select Lower(contractNumber) a  from VMALL_DICTIONARY_EXCLUDE_hive order by contractNumber""")
}
       

//VMALL_DICTIONARY_EXCLUDE_063
test("VMALL_DICTIONARY_EXCLUDE_063", Include) {
  checkAnswer(s"""select distinct contractNumber from VMALL_DICTIONARY_EXCLUDE order by contractNumber""",
    s"""select distinct contractNumber from VMALL_DICTIONARY_EXCLUDE_hive order by contractNumber""")
}
       

//VMALL_DICTIONARY_EXCLUDE_064
test("VMALL_DICTIONARY_EXCLUDE_064", Include) {
  checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_EXCLUDE order by contractNumber limit 101""",
    s"""select contractNumber from VMALL_DICTIONARY_EXCLUDE_hive order by contractNumber limit 101""")
}
       

//VMALL_DICTIONARY_EXCLUDE_065
test("VMALL_DICTIONARY_EXCLUDE_065", Include) {
  checkAnswer(s"""select contractNumber as a from VMALL_DICTIONARY_EXCLUDE  order by a asc limit 10""",
    s"""select contractNumber as a from VMALL_DICTIONARY_EXCLUDE_hive  order by a asc limit 10""")
}
       

//VMALL_DICTIONARY_EXCLUDE_066
test("VMALL_DICTIONARY_EXCLUDE_066", Include) {
  checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_EXCLUDE where  (contractNumber == 9223372047700) and (imei=='1AA100012')""",
    s"""select contractNumber from VMALL_DICTIONARY_EXCLUDE_hive where  (contractNumber == 9223372047700) and (imei=='1AA100012')""")
}
       

//VMALL_DICTIONARY_EXCLUDE_067
test("VMALL_DICTIONARY_EXCLUDE_067", Include) {
  checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_EXCLUDE where contractNumber !=9223372047700 order by contractNumber""",
    s"""select contractNumber from VMALL_DICTIONARY_EXCLUDE_hive where contractNumber !=9223372047700 order by contractNumber""")
}
       

//VMALL_DICTIONARY_EXCLUDE_068
test("VMALL_DICTIONARY_EXCLUDE_068", Include) {
  checkAnswer(s"""select contractNumber  from VMALL_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color') order by contractNumber""",
    s"""select contractNumber  from VMALL_DICTIONARY_EXCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color') order by contractNumber""")
}
       

//VMALL_DICTIONARY_EXCLUDE_069
test("VMALL_DICTIONARY_EXCLUDE_069", Include) {
  checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_EXCLUDE where contractNumber !=9223372047700 order by contractNumber""",
    s"""select contractNumber from VMALL_DICTIONARY_EXCLUDE_hive where contractNumber !=9223372047700 order by contractNumber""")
}
       

//VMALL_DICTIONARY_EXCLUDE_070
test("VMALL_DICTIONARY_EXCLUDE_070", Include) {
  checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_EXCLUDE where contractNumber >9223372047700 order by contractNumber""",
    s"""select contractNumber from VMALL_DICTIONARY_EXCLUDE_hive where contractNumber >9223372047700 order by contractNumber""")
}
       

//VMALL_DICTIONARY_EXCLUDE_071
test("VMALL_DICTIONARY_EXCLUDE_071", Include) {
  checkAnswer(s"""select contractNumber  from VMALL_DICTIONARY_EXCLUDE where contractNumber<>contractNumber""",
    s"""select contractNumber  from VMALL_DICTIONARY_EXCLUDE_hive where contractNumber<>contractNumber""")
}
       

//VMALL_DICTIONARY_EXCLUDE_072
test("VMALL_DICTIONARY_EXCLUDE_072", Include) {
  checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_EXCLUDE where contractNumber != Latest_areaId order by contractNumber""",
    s"""select contractNumber from VMALL_DICTIONARY_EXCLUDE_hive where contractNumber != Latest_areaId order by contractNumber""")
}
       

//VMALL_DICTIONARY_EXCLUDE_073
test("VMALL_DICTIONARY_EXCLUDE_073", Include) {
  checkAnswer(s"""select contractNumber, contractNumber from VMALL_DICTIONARY_EXCLUDE where Latest_areaId<contractNumber order by contractNumber""",
    s"""select contractNumber, contractNumber from VMALL_DICTIONARY_EXCLUDE_hive where Latest_areaId<contractNumber order by contractNumber""")
}
       

//VMALL_DICTIONARY_EXCLUDE_074
test("VMALL_DICTIONARY_EXCLUDE_074", Include) {
  checkAnswer(s"""select contractNumber, contractNumber from VMALL_DICTIONARY_EXCLUDE where Latest_DAY<=contractNumber order by contractNumber""",
    s"""select contractNumber, contractNumber from VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY<=contractNumber order by contractNumber""")
}
       

//VMALL_DICTIONARY_EXCLUDE_075
test("VMALL_DICTIONARY_EXCLUDE_075", Include) {
  checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_EXCLUDE where contractNumber <1000 order by contractNumber""",
    s"""select contractNumber from VMALL_DICTIONARY_EXCLUDE_hive where contractNumber <1000 order by contractNumber""")
}
       

//VMALL_DICTIONARY_EXCLUDE_076
test("VMALL_DICTIONARY_EXCLUDE_076", Include) {
  checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_EXCLUDE where contractNumber >1000 order by contractNumber""",
    s"""select contractNumber from VMALL_DICTIONARY_EXCLUDE_hive where contractNumber >1000 order by contractNumber""")
}
       

//VMALL_DICTIONARY_EXCLUDE_077
test("VMALL_DICTIONARY_EXCLUDE_077", Include) {
  checkAnswer(s"""select contractNumber  from VMALL_DICTIONARY_EXCLUDE where contractNumber IS NULL order by contractNumber""",
    s"""select contractNumber  from VMALL_DICTIONARY_EXCLUDE_hive where contractNumber IS NULL order by contractNumber""")
}
       

//VMALL_DICTIONARY_EXCLUDE_078
test("VMALL_DICTIONARY_EXCLUDE_078", Include) {
  checkAnswer(s"""select contractNumber  from VMALL_DICTIONARY_EXCLUDE where Latest_DAY IS NOT NULL order by contractNumber""",
    s"""select contractNumber  from VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY IS NOT NULL order by contractNumber""")
}
       

//VMALL_DICTIONARY_EXCLUDE_079
test("VMALL_DICTIONARY_EXCLUDE_079", Include) {
  checkAnswer(s"""Select count(Latest_DAY) from VMALL_DICTIONARY_EXCLUDE""",
    s"""Select count(Latest_DAY) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_080
test("VMALL_DICTIONARY_EXCLUDE_080", Include) {
  checkAnswer(s"""select count(DISTINCT Latest_DAY) as a from VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(DISTINCT Latest_DAY) as a from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_081
test("VMALL_DICTIONARY_EXCLUDE_081", Include) {
  checkAnswer(s"""select sum(Latest_DAY)+10 as a ,Latest_DAY  from VMALL_DICTIONARY_EXCLUDE group by Latest_DAY order by a""",
    s"""select sum(Latest_DAY)+10 as a ,Latest_DAY  from VMALL_DICTIONARY_EXCLUDE_hive group by Latest_DAY order by a""")
}
       

//VMALL_DICTIONARY_EXCLUDE_082
test("VMALL_DICTIONARY_EXCLUDE_082", Include) {
  checkAnswer(s"""select max(Latest_DAY),min(Latest_DAY) from VMALL_DICTIONARY_EXCLUDE""",
    s"""select max(Latest_DAY),min(Latest_DAY) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_083
test("VMALL_DICTIONARY_EXCLUDE_083", Include) {
  checkAnswer(s"""select sum(Latest_DAY) a  from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(Latest_DAY) a  from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_084
test("VMALL_DICTIONARY_EXCLUDE_084", Include) {
  checkAnswer(s"""select avg(Latest_DAY) a  from VMALL_DICTIONARY_EXCLUDE""",
    s"""select avg(Latest_DAY) a  from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_085
test("VMALL_DICTIONARY_EXCLUDE_085", Include) {
  checkAnswer(s"""select min(Latest_DAY) a  from VMALL_DICTIONARY_EXCLUDE""",
    s"""select min(Latest_DAY) a  from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_086
test("VMALL_DICTIONARY_EXCLUDE_086", Include) {
  sql(s"""select variance(Latest_DAY) as a   from (select Latest_DAY from VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_087
test("VMALL_DICTIONARY_EXCLUDE_087", Include) {
  sql(s"""select var_pop(Latest_DAY)  as a from (select Latest_DAY from VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_088
test("VMALL_DICTIONARY_EXCLUDE_088", Include) {
  sql(s"""select var_samp(Latest_DAY) as a  from (select Latest_DAY from VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_089
test("VMALL_DICTIONARY_EXCLUDE_089", Include) {
  sql(s"""select stddev_pop(Latest_DAY) as a  from (select Latest_DAY from VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_090
test("VMALL_DICTIONARY_EXCLUDE_090", Include) {
  sql(s"""select stddev_samp(Latest_DAY)  as a from (select Latest_DAY from VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_091
test("VMALL_DICTIONARY_EXCLUDE_091", Include) {
  sql(s"""select covar_pop(Latest_DAY,Latest_DAY) as a  from (select Latest_DAY from VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_092
test("VMALL_DICTIONARY_EXCLUDE_092", Include) {
  sql(s"""select covar_samp(Latest_DAY,Latest_DAY) as a  from (select Latest_DAY from VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_093
test("VMALL_DICTIONARY_EXCLUDE_093", Include) {
  checkAnswer(s"""select corr(Latest_DAY,Latest_DAY)  as a from VMALL_DICTIONARY_EXCLUDE""",
    s"""select corr(Latest_DAY,Latest_DAY)  as a from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_094
test("VMALL_DICTIONARY_EXCLUDE_094", Include) {
  checkAnswer(s"""select percentile_approx(Latest_DAY,0.2) as a  from (select Latest_DAY from VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""",
    s"""select percentile_approx(Latest_DAY,0.2) as a  from (select Latest_DAY from VMALL_DICTIONARY_EXCLUDE_hive order by Latest_DAY) t""")
}
       

//VMALL_DICTIONARY_EXCLUDE_095
test("VMALL_DICTIONARY_EXCLUDE_095", Include) {
  checkAnswer(s"""select percentile_approx(Latest_DAY,0.2,5) as a  from (select Latest_DAY from VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""",
    s"""select percentile_approx(Latest_DAY,0.2,5) as a  from (select Latest_DAY from VMALL_DICTIONARY_EXCLUDE_hive order by Latest_DAY) t""")
}
       

//VMALL_DICTIONARY_EXCLUDE_096
test("VMALL_DICTIONARY_EXCLUDE_096", Include) {
  checkAnswer(s"""select percentile_approx(Latest_DAY,array(0.2,0.3,0.99))  as a from (select Latest_DAY from VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""",
    s"""select percentile_approx(Latest_DAY,array(0.2,0.3,0.99))  as a from (select Latest_DAY from VMALL_DICTIONARY_EXCLUDE_hive order by Latest_DAY) t""")
}
       

//VMALL_DICTIONARY_EXCLUDE_097
test("VMALL_DICTIONARY_EXCLUDE_097", Include) {
  sql(s"""select percentile_approx(Latest_DAY,array(0.2,0.3,0.99),5) as a from (select Latest_DAY from VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_098
test("VMALL_DICTIONARY_EXCLUDE_098", Include) {
  sql(s"""select histogram_numeric(Latest_DAY,2)  as a from (select Latest_DAY from VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_099
test("VMALL_DICTIONARY_EXCLUDE_099", Include) {
  checkAnswer(s"""select Latest_DAY, Latest_DAY+ 10 as a  from VMALL_DICTIONARY_EXCLUDE order by a""",
    s"""select Latest_DAY, Latest_DAY+ 10 as a  from VMALL_DICTIONARY_EXCLUDE_hive order by a""")
}
       

//VMALL_DICTIONARY_EXCLUDE_100
test("VMALL_DICTIONARY_EXCLUDE_100", Include) {
  checkAnswer(s"""select min(Latest_DAY), max(Latest_DAY+ 10) Total from VMALL_DICTIONARY_EXCLUDE group by  channelsId order by Total""",
    s"""select min(Latest_DAY), max(Latest_DAY+ 10) Total from VMALL_DICTIONARY_EXCLUDE_hive group by  channelsId order by Total""")
}
       

//VMALL_DICTIONARY_EXCLUDE_101
test("VMALL_DICTIONARY_EXCLUDE_101", Include) {
  sql(s"""select last(Latest_DAY) a from VMALL_DICTIONARY_EXCLUDE order by a""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_102
test("VMALL_DICTIONARY_EXCLUDE_102", Include) {
  sql(s"""select FIRST(Latest_DAY) a from (select Latest_DAY from VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_103
test("VMALL_DICTIONARY_EXCLUDE_103", Include) {
  checkAnswer(s"""select Latest_DAY,count(Latest_DAY) a from VMALL_DICTIONARY_EXCLUDE group by Latest_DAY order by Latest_DAY""",
    s"""select Latest_DAY,count(Latest_DAY) a from VMALL_DICTIONARY_EXCLUDE_hive group by Latest_DAY order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_EXCLUDE_104
test("VMALL_DICTIONARY_EXCLUDE_104", Include) {
  checkAnswer(s"""select Lower(Latest_DAY) a  from VMALL_DICTIONARY_EXCLUDE order by a""",
    s"""select Lower(Latest_DAY) a  from VMALL_DICTIONARY_EXCLUDE_hive order by a""")
}
       

//VMALL_DICTIONARY_EXCLUDE_105
test("VMALL_DICTIONARY_EXCLUDE_105", Include) {
  checkAnswer(s"""select distinct Latest_DAY from VMALL_DICTIONARY_EXCLUDE order by Latest_DAY""",
    s"""select distinct Latest_DAY from VMALL_DICTIONARY_EXCLUDE_hive order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_EXCLUDE_106
test("VMALL_DICTIONARY_EXCLUDE_106", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE order by Latest_DAY limit 101""",
    s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE_hive order by Latest_DAY limit 101""")
}
       

//VMALL_DICTIONARY_EXCLUDE_107
test("VMALL_DICTIONARY_EXCLUDE_107", Include) {
  checkAnswer(s"""select Latest_DAY as a from VMALL_DICTIONARY_EXCLUDE  order by a asc limit 10""",
    s"""select Latest_DAY as a from VMALL_DICTIONARY_EXCLUDE_hive  order by a asc limit 10""")
}
       

//VMALL_DICTIONARY_EXCLUDE_108
test("VMALL_DICTIONARY_EXCLUDE_108", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE where  (Latest_DAY == 1234567890123450.0000000000)  and (imei=='1AA1')""",
    s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE_hive where  (Latest_DAY == 1234567890123450.0000000000)  and (imei=='1AA1')""")
}
       

//VMALL_DICTIONARY_EXCLUDE_109
test("VMALL_DICTIONARY_EXCLUDE_109", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""",
    s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_EXCLUDE_110
test("VMALL_DICTIONARY_EXCLUDE_110", Include) {
  checkAnswer(s"""select Latest_DAY  from VMALL_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
    s"""select Latest_DAY  from VMALL_DICTIONARY_EXCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""")
}
       

//VMALL_DICTIONARY_EXCLUDE_111
test("VMALL_DICTIONARY_EXCLUDE_111", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""",
    s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_EXCLUDE_112
test("VMALL_DICTIONARY_EXCLUDE_112", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE where Latest_DAY >1234567890123450.0000000000  order by Latest_DAY""",
    s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY >1234567890123450.0000000000  order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_EXCLUDE_113
test("VMALL_DICTIONARY_EXCLUDE_113", Include) {
  checkAnswer(s"""select Latest_DAY  from VMALL_DICTIONARY_EXCLUDE where Latest_DAY<>Latest_DAY""",
    s"""select Latest_DAY  from VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY<>Latest_DAY""")
}
       

//VMALL_DICTIONARY_EXCLUDE_114
test("VMALL_DICTIONARY_EXCLUDE_114", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE where Latest_DAY != Latest_areaId order by Latest_DAY""",
    s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY != Latest_areaId order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_EXCLUDE_115
test("VMALL_DICTIONARY_EXCLUDE_115", Include) {
  checkAnswer(s"""select Latest_DAY, Latest_DAY from VMALL_DICTIONARY_EXCLUDE where Latest_areaId<Latest_DAY order by Latest_DAY""",
    s"""select Latest_DAY, Latest_DAY from VMALL_DICTIONARY_EXCLUDE_hive where Latest_areaId<Latest_DAY order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_EXCLUDE_116
test("VMALL_DICTIONARY_EXCLUDE_116", Include) {
  checkAnswer(s"""select Latest_DAY, Latest_DAY from VMALL_DICTIONARY_EXCLUDE where Latest_DAY<=Latest_DAY  order by Latest_DAY""",
    s"""select Latest_DAY, Latest_DAY from VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY<=Latest_DAY  order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_EXCLUDE_117
test("VMALL_DICTIONARY_EXCLUDE_117", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE where Latest_DAY <1000  order by Latest_DAY""",
    s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY <1000  order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_EXCLUDE_118
test("VMALL_DICTIONARY_EXCLUDE_118", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE where Latest_DAY >1000  order by Latest_DAY""",
    s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY >1000  order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_EXCLUDE_119
test("VMALL_DICTIONARY_EXCLUDE_119", Include) {
  checkAnswer(s"""select Latest_DAY  from VMALL_DICTIONARY_EXCLUDE where Latest_DAY IS NULL  order by Latest_DAY""",
    s"""select Latest_DAY  from VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY IS NULL  order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_EXCLUDE_120
test("VMALL_DICTIONARY_EXCLUDE_120", Include) {
  checkAnswer(s"""select Latest_DAY  from VMALL_DICTIONARY_EXCLUDE where Latest_DAY IS NOT NULL  order by Latest_DAY""",
    s"""select Latest_DAY  from VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY IS NOT NULL  order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_EXCLUDE_121
test("VMALL_DICTIONARY_EXCLUDE_121", Include) {
  checkAnswer(s"""Select count(gamePointId) from VMALL_DICTIONARY_EXCLUDE""",
    s"""Select count(gamePointId) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_122
test("VMALL_DICTIONARY_EXCLUDE_122", Include) {
  sql(s"""select count(DISTINCT gamePointId) as a from VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_123
test("VMALL_DICTIONARY_EXCLUDE_123", Include) {
  sql(s"""select sum(gamePointId)+10 as a ,gamePointId  from VMALL_DICTIONARY_EXCLUDE group by gamePointId order by a""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_124
test("VMALL_DICTIONARY_EXCLUDE_124", Include) {
  sql(s"""select max(gamePointId),min(gamePointId) from VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_125
test("VMALL_DICTIONARY_EXCLUDE_125", Include) {
  sql(s"""select sum(gamePointId) a  from VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_126
test("VMALL_DICTIONARY_EXCLUDE_126", Include) {
  sql(s"""select avg(gamePointId) a  from VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_127
test("VMALL_DICTIONARY_EXCLUDE_127", Include) {
  sql(s"""select min(gamePointId) a  from VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_128
test("VMALL_DICTIONARY_EXCLUDE_128", Include) {
  sql(s"""select variance(gamePointId) as a   from (select gamePointId from VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_129
test("VMALL_DICTIONARY_EXCLUDE_129", Include) {
  sql(s"""select var_pop(gamePointId)  as a from (select gamePointId from VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_130
test("VMALL_DICTIONARY_EXCLUDE_130", Include) {
  sql(s"""select var_samp(gamePointId) as a  from (select gamePointId from VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_131
test("VMALL_DICTIONARY_EXCLUDE_131", Include) {
  sql(s"""select stddev_pop(gamePointId) as a  from (select gamePointId from VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_132
test("VMALL_DICTIONARY_EXCLUDE_132", Include) {
  sql(s"""select stddev_samp(gamePointId)  as a from (select gamePointId from VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_133
test("VMALL_DICTIONARY_EXCLUDE_133", Include) {
  sql(s"""select covar_pop(gamePointId,gamePointId) as a  from (select gamePointId from VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_134
test("VMALL_DICTIONARY_EXCLUDE_134", Include) {
  sql(s"""select covar_samp(gamePointId,gamePointId) as a  from (select gamePointId from VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_135
test("VMALL_DICTIONARY_EXCLUDE_135", Include) {
  sql(s"""select corr(gamePointId,gamePointId)  as a from VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_136
test("VMALL_DICTIONARY_EXCLUDE_136", Include) {
  sql(s"""select percentile_approx(gamePointId,0.2) as a  from (select gamePointId from VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_137
test("VMALL_DICTIONARY_EXCLUDE_137", Include) {
  sql(s"""select percentile_approx(gamePointId,0.2,5) as a  from (select gamePointId from VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_138
test("VMALL_DICTIONARY_EXCLUDE_138", Include) {
  sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99))  as a from (select gamePointId from VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_139
test("VMALL_DICTIONARY_EXCLUDE_139", Include) {
  sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99),5) as a from (select gamePointId from VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_140
test("VMALL_DICTIONARY_EXCLUDE_140", Include) {
  sql(s"""select histogram_numeric(gamePointId,2)  as a from (select gamePointId from VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_141
test("VMALL_DICTIONARY_EXCLUDE_141", Include) {
  sql(s"""select gamePointId, gamePointId+ 10 as a  from VMALL_DICTIONARY_EXCLUDE order by a""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_142
test("VMALL_DICTIONARY_EXCLUDE_142", Include) {
  sql(s"""select min(gamePointId), max(gamePointId+ 10) Total from VMALL_DICTIONARY_EXCLUDE group by  channelsId order by Total""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_143
test("VMALL_DICTIONARY_EXCLUDE_143", Include) {
  sql(s"""select last(gamePointId) a from (select gamePointId from VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_144
test("VMALL_DICTIONARY_EXCLUDE_144", Include) {
  sql(s"""select FIRST(gamePointId) a from VMALL_DICTIONARY_EXCLUDE order by a""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_145
test("VMALL_DICTIONARY_EXCLUDE_145", Include) {
  sql(s"""select gamePointId,count(gamePointId) a from VMALL_DICTIONARY_EXCLUDE group by gamePointId order by gamePointId""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_146
test("VMALL_DICTIONARY_EXCLUDE_146", Include) {
  sql(s"""select Lower(gamePointId) a  from VMALL_DICTIONARY_EXCLUDE order by gamePointId""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_147
test("VMALL_DICTIONARY_EXCLUDE_147", Include) {
  sql(s"""select distinct gamePointId from VMALL_DICTIONARY_EXCLUDE order by gamePointId""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_148
test("VMALL_DICTIONARY_EXCLUDE_148", Include) {
  sql(s"""select gamePointId from VMALL_DICTIONARY_EXCLUDE  order by gamePointId limit 101""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_149
test("VMALL_DICTIONARY_EXCLUDE_149", Include) {
  sql(s"""select gamePointId as a from VMALL_DICTIONARY_EXCLUDE  order by a asc limit 10""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_150
test("VMALL_DICTIONARY_EXCLUDE_150", Include) {
  sql(s"""select gamePointId from VMALL_DICTIONARY_EXCLUDE where  (gamePointId == 4.70133553923674E43) and (imei=='1AA100084')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_151
test("VMALL_DICTIONARY_EXCLUDE_151", Include) {
  sql(s"""select gamePointId from VMALL_DICTIONARY_EXCLUDE where gamePointId !=4.70133553923674E43  order by gamePointId""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_152
test("VMALL_DICTIONARY_EXCLUDE_152", Include) {
  sql(s"""select gamePointId  from VMALL_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_153
test("VMALL_DICTIONARY_EXCLUDE_153", Include) {
  sql(s"""select gamePointId from VMALL_DICTIONARY_EXCLUDE where gamePointId !=4.70133553923674E43""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_154
test("VMALL_DICTIONARY_EXCLUDE_154", Include) {
  checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_EXCLUDE where gamePointId >4.70133553923674E43""",
    s"""select gamePointId from VMALL_DICTIONARY_EXCLUDE_hive where gamePointId >4.70133553923674E43""")
}
       

//VMALL_DICTIONARY_EXCLUDE_155
test("VMALL_DICTIONARY_EXCLUDE_155", Include) {
  checkAnswer(s"""select gamePointId  from VMALL_DICTIONARY_EXCLUDE where gamePointId<>gamePointId""",
    s"""select gamePointId  from VMALL_DICTIONARY_EXCLUDE_hive where gamePointId<>gamePointId""")
}
       

//VMALL_DICTIONARY_EXCLUDE_156
test("VMALL_DICTIONARY_EXCLUDE_156", Include) {
  sql(s"""select gamePointId from VMALL_DICTIONARY_EXCLUDE where gamePointId != Latest_areaId  order by gamePointId""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_157
test("VMALL_DICTIONARY_EXCLUDE_157", Include) {
  sql(s"""select gamePointId, gamePointId from VMALL_DICTIONARY_EXCLUDE where Latest_areaId<gamePointId  order by gamePointId""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_158
test("VMALL_DICTIONARY_EXCLUDE_158", Include) {
  sql(s"""select gamePointId, gamePointId from VMALL_DICTIONARY_EXCLUDE where gamePointId<=gamePointId  order by gamePointId""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_159
test("VMALL_DICTIONARY_EXCLUDE_159", Include) {
  checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_EXCLUDE where gamePointId <1000 order by gamePointId""",
    s"""select gamePointId from VMALL_DICTIONARY_EXCLUDE_hive where gamePointId <1000 order by gamePointId""")
}
       

//VMALL_DICTIONARY_EXCLUDE_160
test("VMALL_DICTIONARY_EXCLUDE_160", Include) {
  sql(s"""select gamePointId from VMALL_DICTIONARY_EXCLUDE where gamePointId >1000 order by gamePointId""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_161
test("VMALL_DICTIONARY_EXCLUDE_161", Include) {
  checkAnswer(s"""select gamePointId  from VMALL_DICTIONARY_EXCLUDE where gamePointId IS NULL order by gamePointId""",
    s"""select gamePointId  from VMALL_DICTIONARY_EXCLUDE_hive where gamePointId IS NULL order by gamePointId""")
}
       

//VMALL_DICTIONARY_EXCLUDE_162
test("VMALL_DICTIONARY_EXCLUDE_162", Include) {
  sql(s"""select gamePointId  from VMALL_DICTIONARY_EXCLUDE where gamePointId IS NOT NULL order by gamePointId""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_163
test("VMALL_DICTIONARY_EXCLUDE_163", Include) {
  checkAnswer(s"""Select count(productionDate) from VMALL_DICTIONARY_EXCLUDE""",
    s"""Select count(productionDate) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_164
test("VMALL_DICTIONARY_EXCLUDE_164", Include) {
  checkAnswer(s"""select count(DISTINCT productionDate) as a from VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(DISTINCT productionDate) as a from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_165
test("VMALL_DICTIONARY_EXCLUDE_165", Include) {
  checkAnswer(s"""select sum(productionDate)+10 as a ,productionDate  from VMALL_DICTIONARY_EXCLUDE group by productionDate order by productionDate""",
    s"""select sum(productionDate)+10 as a ,productionDate  from VMALL_DICTIONARY_EXCLUDE_hive group by productionDate order by productionDate""")
}
       

//VMALL_DICTIONARY_EXCLUDE_166
test("VMALL_DICTIONARY_EXCLUDE_166", Include) {
  checkAnswer(s"""select max(productionDate),min(productionDate) from VMALL_DICTIONARY_EXCLUDE""",
    s"""select max(productionDate),min(productionDate) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_167
test("VMALL_DICTIONARY_EXCLUDE_167", Include) {
  checkAnswer(s"""select sum(productionDate) a  from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(productionDate) a  from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_168
test("VMALL_DICTIONARY_EXCLUDE_168", Include) {
  checkAnswer(s"""select avg(productionDate) a  from VMALL_DICTIONARY_EXCLUDE""",
    s"""select avg(productionDate) a  from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_169
test("VMALL_DICTIONARY_EXCLUDE_169", Include) {
  checkAnswer(s"""select min(productionDate) a  from VMALL_DICTIONARY_EXCLUDE""",
    s"""select min(productionDate) a  from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_183
test("VMALL_DICTIONARY_EXCLUDE_183", Include) {
  sql(s"""select last(productionDate) a from VMALL_DICTIONARY_EXCLUDE order by a""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_184
test("VMALL_DICTIONARY_EXCLUDE_184", Include) {
  sql(s"""select FIRST(productionDate) a from (select productionDate from VMALL_DICTIONARY_EXCLUDE order by productionDate) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_185
test("VMALL_DICTIONARY_EXCLUDE_185", Include) {
  checkAnswer(s"""select productionDate,count(productionDate) a from VMALL_DICTIONARY_EXCLUDE group by productionDate order by productionDate""",
    s"""select productionDate,count(productionDate) a from VMALL_DICTIONARY_EXCLUDE_hive group by productionDate order by productionDate""")
}
       

//VMALL_DICTIONARY_EXCLUDE_186
test("VMALL_DICTIONARY_EXCLUDE_186", Include) {
  checkAnswer(s"""select Lower(productionDate) a  from VMALL_DICTIONARY_EXCLUDE order by productionDate""",
    s"""select Lower(productionDate) a  from VMALL_DICTIONARY_EXCLUDE_hive order by productionDate""")
}
       

//VMALL_DICTIONARY_EXCLUDE_187
test("VMALL_DICTIONARY_EXCLUDE_187", Include) {
  checkAnswer(s"""select distinct productionDate from VMALL_DICTIONARY_EXCLUDE order by productionDate""",
    s"""select distinct productionDate from VMALL_DICTIONARY_EXCLUDE_hive order by productionDate""")
}
       

//VMALL_DICTIONARY_EXCLUDE_188
test("VMALL_DICTIONARY_EXCLUDE_188", Include) {
  checkAnswer(s"""select productionDate from VMALL_DICTIONARY_EXCLUDE order by productionDate limit 101""",
    s"""select productionDate from VMALL_DICTIONARY_EXCLUDE_hive order by productionDate limit 101""")
}
       

//VMALL_DICTIONARY_EXCLUDE_189
test("VMALL_DICTIONARY_EXCLUDE_189", Include) {
  checkAnswer(s"""select productionDate as a from VMALL_DICTIONARY_EXCLUDE  order by a asc limit 10""",
    s"""select productionDate as a from VMALL_DICTIONARY_EXCLUDE_hive  order by a asc limit 10""")
}
       

//VMALL_DICTIONARY_EXCLUDE_190
test("VMALL_DICTIONARY_EXCLUDE_190", Include) {
  checkAnswer(s"""select productionDate from VMALL_DICTIONARY_EXCLUDE where  (productionDate == '2015-07-01 12:07:28.0') and (productionDate=='2015-07-01 12:07:28.0')""",
    s"""select productionDate from VMALL_DICTIONARY_EXCLUDE_hive where  (productionDate == '2015-07-01 12:07:28.0') and (productionDate=='2015-07-01 12:07:28.0')""")
}
       

//VMALL_DICTIONARY_EXCLUDE_191
test("VMALL_DICTIONARY_EXCLUDE_191", Include) {
  checkAnswer(s"""select productionDate from VMALL_DICTIONARY_EXCLUDE where productionDate !='2015-07-01 12:07:28.0' order by productionDate""",
    s"""select productionDate from VMALL_DICTIONARY_EXCLUDE_hive where productionDate !='2015-07-01 12:07:28.0' order by productionDate""")
}
       

//VMALL_DICTIONARY_EXCLUDE_192
test("VMALL_DICTIONARY_EXCLUDE_192", Include) {
  checkAnswer(s"""select productionDate  from VMALL_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
    s"""select productionDate  from VMALL_DICTIONARY_EXCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""")
}
       

//VMALL_DICTIONARY_EXCLUDE_193
test("VMALL_DICTIONARY_EXCLUDE_193", Include) {
  checkAnswer(s"""select productionDate from VMALL_DICTIONARY_EXCLUDE where productionDate !='2015-07-01 12:07:28.0' order by productionDate""",
    s"""select productionDate from VMALL_DICTIONARY_EXCLUDE_hive where productionDate !='2015-07-01 12:07:28.0' order by productionDate""")
}
       

//VMALL_DICTIONARY_EXCLUDE_194
test("VMALL_DICTIONARY_EXCLUDE_194", Include) {
  checkAnswer(s"""select productionDate from VMALL_DICTIONARY_EXCLUDE where productionDate >'2015-07-01 12:07:28.0' order by productionDate""",
    s"""select productionDate from VMALL_DICTIONARY_EXCLUDE_hive where productionDate >'2015-07-01 12:07:28.0' order by productionDate""")
}
       

//VMALL_DICTIONARY_EXCLUDE_195
test("VMALL_DICTIONARY_EXCLUDE_195", Include) {
  checkAnswer(s"""select productionDate  from VMALL_DICTIONARY_EXCLUDE where productionDate<>productionDate order by productionDate""",
    s"""select productionDate  from VMALL_DICTIONARY_EXCLUDE_hive where productionDate<>productionDate order by productionDate""")
}
       

//VMALL_DICTIONARY_EXCLUDE_196
test("VMALL_DICTIONARY_EXCLUDE_196", Include) {
  checkAnswer(s"""select productionDate from VMALL_DICTIONARY_EXCLUDE where productionDate != Latest_areaId order by productionDate""",
    s"""select productionDate from VMALL_DICTIONARY_EXCLUDE_hive where productionDate != Latest_areaId order by productionDate""")
}
       

//VMALL_DICTIONARY_EXCLUDE_197
test("VMALL_DICTIONARY_EXCLUDE_197", Include) {
  checkAnswer(s"""select productionDate from VMALL_DICTIONARY_EXCLUDE where Latest_areaId<productionDate order by productionDate""",
    s"""select productionDate from VMALL_DICTIONARY_EXCLUDE_hive where Latest_areaId<productionDate order by productionDate""")
}
       

//VMALL_DICTIONARY_EXCLUDE_198
test("VMALL_DICTIONARY_EXCLUDE_198", Include) {
  checkAnswer(s"""select productionDate from VMALL_DICTIONARY_EXCLUDE where productionDate<=productionDate order by productionDate""",
    s"""select productionDate from VMALL_DICTIONARY_EXCLUDE_hive where productionDate<=productionDate order by productionDate""")
}
       

//VMALL_DICTIONARY_EXCLUDE_199
test("VMALL_DICTIONARY_EXCLUDE_199", Include) {
  sql(s"""select productionDate from VMALL_DICTIONARY_EXCLUDE where productionDate <'2015-07-01 12:07:28.0' order by productionDate""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_200
test("VMALL_DICTIONARY_EXCLUDE_200", Include) {
  checkAnswer(s"""select productionDate  from VMALL_DICTIONARY_EXCLUDE where productionDate IS NULL""",
    s"""select productionDate  from VMALL_DICTIONARY_EXCLUDE_hive where productionDate IS NULL""")
}
       

//VMALL_DICTIONARY_EXCLUDE_201
test("VMALL_DICTIONARY_EXCLUDE_201", Include) {
  checkAnswer(s"""select productionDate  from VMALL_DICTIONARY_EXCLUDE where productionDate IS NOT NULL order by productionDate""",
    s"""select productionDate  from VMALL_DICTIONARY_EXCLUDE_hive where productionDate IS NOT NULL order by productionDate""")
}
       

//VMALL_DICTIONARY_EXCLUDE_202
test("VMALL_DICTIONARY_EXCLUDE_202", Include) {
  checkAnswer(s"""Select count(deviceInformationId) from VMALL_DICTIONARY_EXCLUDE""",
    s"""Select count(deviceInformationId) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_203
test("VMALL_DICTIONARY_EXCLUDE_203", Include) {
  checkAnswer(s"""select count(DISTINCT deviceInformationId) as a from VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(DISTINCT deviceInformationId) as a from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_204
test("VMALL_DICTIONARY_EXCLUDE_204", Include) {
  checkAnswer(s"""select sum(deviceInformationId)+10 as a ,deviceInformationId  from VMALL_DICTIONARY_EXCLUDE group by deviceInformationId order by deviceInformationId""",
    s"""select sum(deviceInformationId)+10 as a ,deviceInformationId  from VMALL_DICTIONARY_EXCLUDE_hive group by deviceInformationId order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_EXCLUDE_205
test("VMALL_DICTIONARY_EXCLUDE_205", Include) {
  checkAnswer(s"""select max(deviceInformationId),min(deviceInformationId) from VMALL_DICTIONARY_EXCLUDE""",
    s"""select max(deviceInformationId),min(deviceInformationId) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_206
test("VMALL_DICTIONARY_EXCLUDE_206", Include) {
  checkAnswer(s"""select sum(deviceInformationId) a  from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(deviceInformationId) a  from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_207
test("VMALL_DICTIONARY_EXCLUDE_207", Include) {
  checkAnswer(s"""select avg(deviceInformationId) a  from VMALL_DICTIONARY_EXCLUDE""",
    s"""select avg(deviceInformationId) a  from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_208
test("VMALL_DICTIONARY_EXCLUDE_208", Include) {
  checkAnswer(s"""select min(deviceInformationId) a  from VMALL_DICTIONARY_EXCLUDE""",
    s"""select min(deviceInformationId) a  from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_209
test("VMALL_DICTIONARY_EXCLUDE_209", Include) {
  sql(s"""select variance(deviceInformationId) as a   from (select deviceInformationId from VMALL_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_210
test("VMALL_DICTIONARY_EXCLUDE_210", Include) {
  sql(s"""select var_pop(deviceInformationId)  as a from VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_211
test("VMALL_DICTIONARY_EXCLUDE_211", Include) {
  sql(s"""select var_samp(deviceInformationId) as a  from VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_212
test("VMALL_DICTIONARY_EXCLUDE_212", Include) {
  sql(s"""select stddev_pop(deviceInformationId) as a  from (select deviceInformationId from VMALL_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_213
test("VMALL_DICTIONARY_EXCLUDE_213", Include) {
  checkAnswer(s"""select stddev_samp(deviceInformationId)  as a from (select deviceInformationId from VMALL_DICTIONARY_EXCLUDE order by deviceInformationId) t""",
    s"""select stddev_samp(deviceInformationId)  as a from (select deviceInformationId from VMALL_DICTIONARY_EXCLUDE_hive order by deviceInformationId) t""")
}
       

//VMALL_DICTIONARY_EXCLUDE_214
test("VMALL_DICTIONARY_EXCLUDE_214", Include) {
  sql(s"""select covar_pop(deviceInformationId,deviceInformationId) as a  from (select deviceInformationId from VMALL_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_215
test("VMALL_DICTIONARY_EXCLUDE_215", Include) {
  sql(s"""select covar_samp(deviceInformationId,deviceInformationId) as a  from (select deviceInformationId from VMALL_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_216
test("VMALL_DICTIONARY_EXCLUDE_216", Include) {
  checkAnswer(s"""select corr(deviceInformationId,deviceInformationId)  as a from VMALL_DICTIONARY_EXCLUDE""",
    s"""select corr(deviceInformationId,deviceInformationId)  as a from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_217
test("VMALL_DICTIONARY_EXCLUDE_217", Include) {
  checkAnswer(s"""select percentile_approx(deviceInformationId,0.2) as a  from (select deviceInformationId from VMALL_DICTIONARY_EXCLUDE order by deviceInformationId) t""",
    s"""select percentile_approx(deviceInformationId,0.2) as a  from (select deviceInformationId from VMALL_DICTIONARY_EXCLUDE_hive order by deviceInformationId) t""")
}
       

//VMALL_DICTIONARY_EXCLUDE_218
test("VMALL_DICTIONARY_EXCLUDE_218", Include) {
  checkAnswer(s"""select percentile_approx(deviceInformationId,0.2,5) as a  from (select deviceInformationId from VMALL_DICTIONARY_EXCLUDE order by deviceInformationId) t""",
    s"""select percentile_approx(deviceInformationId,0.2,5) as a  from (select deviceInformationId from VMALL_DICTIONARY_EXCLUDE_hive order by deviceInformationId) t""")
}
       

//VMALL_DICTIONARY_EXCLUDE_219
test("VMALL_DICTIONARY_EXCLUDE_219", Include) {
  checkAnswer(s"""select percentile_approx(deviceInformationId,array(0.2,0.3,0.99))  as a from (select deviceInformationId from VMALL_DICTIONARY_EXCLUDE order by deviceInformationId) t""",
    s"""select percentile_approx(deviceInformationId,array(0.2,0.3,0.99))  as a from (select deviceInformationId from VMALL_DICTIONARY_EXCLUDE_hive order by deviceInformationId) t""")
}
       

//VMALL_DICTIONARY_EXCLUDE_220
test("VMALL_DICTIONARY_EXCLUDE_220", Include) {
  sql(s"""select percentile_approx(deviceInformationId,array(0.2,0.3,0.99),5) as a from (select deviceInformationId from VMALL_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_221
test("VMALL_DICTIONARY_EXCLUDE_221", Include) {
  sql(s"""select histogram_numeric(deviceInformationId,2)  as a from (select deviceInformationId from VMALL_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_222
test("VMALL_DICTIONARY_EXCLUDE_222", Include) {
  checkAnswer(s"""select deviceInformationId, deviceInformationId+ 10 as a  from VMALL_DICTIONARY_EXCLUDE order by deviceInformationId""",
    s"""select deviceInformationId, deviceInformationId+ 10 as a  from VMALL_DICTIONARY_EXCLUDE_hive order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_EXCLUDE_223
test("VMALL_DICTIONARY_EXCLUDE_223", Include) {
  checkAnswer(s"""select min(deviceInformationId), max(deviceInformationId+ 10) Total from VMALL_DICTIONARY_EXCLUDE group by  channelsId order by Total""",
    s"""select min(deviceInformationId), max(deviceInformationId+ 10) Total from VMALL_DICTIONARY_EXCLUDE_hive group by  channelsId order by Total""")
}
       

//VMALL_DICTIONARY_EXCLUDE_224
test("VMALL_DICTIONARY_EXCLUDE_224", Include) {
  sql(s"""select last(deviceInformationId) a from VMALL_DICTIONARY_EXCLUDE order by a""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_225
test("VMALL_DICTIONARY_EXCLUDE_225", Include) {
  sql(s"""select FIRST(deviceInformationId) a from (select deviceInformationId from VMALL_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_226
test("VMALL_DICTIONARY_EXCLUDE_226", Include) {
  checkAnswer(s"""select deviceInformationId,count(deviceInformationId) a from VMALL_DICTIONARY_EXCLUDE group by deviceInformationId order by deviceInformationId""",
    s"""select deviceInformationId,count(deviceInformationId) a from VMALL_DICTIONARY_EXCLUDE_hive group by deviceInformationId order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_EXCLUDE_227
test("VMALL_DICTIONARY_EXCLUDE_227", Include) {
  checkAnswer(s"""select Lower(deviceInformationId) a  from VMALL_DICTIONARY_EXCLUDE order by deviceInformationId""",
    s"""select Lower(deviceInformationId) a  from VMALL_DICTIONARY_EXCLUDE_hive order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_EXCLUDE_228
test("VMALL_DICTIONARY_EXCLUDE_228", Include) {
  checkAnswer(s"""select distinct deviceInformationId from VMALL_DICTIONARY_EXCLUDE order by deviceInformationId""",
    s"""select distinct deviceInformationId from VMALL_DICTIONARY_EXCLUDE_hive order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_EXCLUDE_229
test("VMALL_DICTIONARY_EXCLUDE_229", Include) {
  checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_EXCLUDE order by deviceInformationId limit 101""",
    s"""select deviceInformationId from VMALL_DICTIONARY_EXCLUDE_hive order by deviceInformationId limit 101""")
}
       

//VMALL_DICTIONARY_EXCLUDE_230
test("VMALL_DICTIONARY_EXCLUDE_230", Include) {
  checkAnswer(s"""select deviceInformationId as a from VMALL_DICTIONARY_EXCLUDE  order by a asc limit 10""",
    s"""select deviceInformationId as a from VMALL_DICTIONARY_EXCLUDE_hive  order by a asc limit 10""")
}
       

//VMALL_DICTIONARY_EXCLUDE_231
test("VMALL_DICTIONARY_EXCLUDE_231", Include) {
  checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_EXCLUDE where  (deviceInformationId == 100084) and (deviceInformationId==100084)""",
    s"""select deviceInformationId from VMALL_DICTIONARY_EXCLUDE_hive where  (deviceInformationId == 100084) and (deviceInformationId==100084)""")
}
       

//VMALL_DICTIONARY_EXCLUDE_232
test("VMALL_DICTIONARY_EXCLUDE_232", Include) {
  checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_EXCLUDE where deviceInformationId !='100084' order by deviceInformationId""",
    s"""select deviceInformationId from VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId !='100084' order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_EXCLUDE_233
test("VMALL_DICTIONARY_EXCLUDE_233", Include) {
  checkAnswer(s"""select deviceInformationId  from VMALL_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
    s"""select deviceInformationId  from VMALL_DICTIONARY_EXCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""")
}
       

//VMALL_DICTIONARY_EXCLUDE_234
test("VMALL_DICTIONARY_EXCLUDE_234", Include) {
  checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_EXCLUDE where deviceInformationId !=100084 order by deviceInformationId""",
    s"""select deviceInformationId from VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId !=100084 order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_EXCLUDE_235
test("VMALL_DICTIONARY_EXCLUDE_235", Include) {
  checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_EXCLUDE where deviceInformationId >100084 order by deviceInformationId""",
    s"""select deviceInformationId from VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId >100084 order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_EXCLUDE_236
test("VMALL_DICTIONARY_EXCLUDE_236", Include) {
  checkAnswer(s"""select deviceInformationId  from VMALL_DICTIONARY_EXCLUDE where deviceInformationId<>deviceInformationId order by deviceInformationId""",
    s"""select deviceInformationId  from VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId<>deviceInformationId order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_EXCLUDE_237
test("VMALL_DICTIONARY_EXCLUDE_237", Include) {
  checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_EXCLUDE where deviceInformationId != Latest_areaId order by deviceInformationId""",
    s"""select deviceInformationId from VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId != Latest_areaId order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_EXCLUDE_238
test("VMALL_DICTIONARY_EXCLUDE_238", Include) {
  checkAnswer(s"""select deviceInformationId, deviceInformationId from VMALL_DICTIONARY_EXCLUDE where Latest_areaId<deviceInformationId order by deviceInformationId""",
    s"""select deviceInformationId, deviceInformationId from VMALL_DICTIONARY_EXCLUDE_hive where Latest_areaId<deviceInformationId order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_EXCLUDE_239
test("VMALL_DICTIONARY_EXCLUDE_239", Include) {
  checkAnswer(s"""select deviceInformationId, deviceInformationId from VMALL_DICTIONARY_EXCLUDE where deviceInformationId<=deviceInformationId order by deviceInformationId""",
    s"""select deviceInformationId, deviceInformationId from VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId<=deviceInformationId order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_EXCLUDE_240
test("VMALL_DICTIONARY_EXCLUDE_240", Include) {
  checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_EXCLUDE where deviceInformationId <1000 order by deviceInformationId""",
    s"""select deviceInformationId from VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId <1000 order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_EXCLUDE_241
test("VMALL_DICTIONARY_EXCLUDE_241", Include) {
  checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_EXCLUDE where deviceInformationId >1000 order by deviceInformationId""",
    s"""select deviceInformationId from VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId >1000 order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_EXCLUDE_242
test("VMALL_DICTIONARY_EXCLUDE_242", Include) {
  checkAnswer(s"""select deviceInformationId  from VMALL_DICTIONARY_EXCLUDE where deviceInformationId IS NULL order by deviceInformationId""",
    s"""select deviceInformationId  from VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId IS NULL order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_EXCLUDE_243
test("VMALL_DICTIONARY_EXCLUDE_243", Include) {
  checkAnswer(s"""select deviceInformationId  from VMALL_DICTIONARY_EXCLUDE where deviceInformationId IS NOT NULL order by deviceInformationId""",
    s"""select deviceInformationId  from VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId IS NOT NULL order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_EXCLUDE_244
test("VMALL_DICTIONARY_EXCLUDE_244", Include) {
  checkAnswer(s"""select sum(imei)+10 as a   from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(imei)+10 as a   from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_245
test("VMALL_DICTIONARY_EXCLUDE_245", Include) {
  checkAnswer(s"""select sum(imei)*10 as a   from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(imei)*10 as a   from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_246
test("VMALL_DICTIONARY_EXCLUDE_246", Include) {
  checkAnswer(s"""select sum(imei)/10 as a   from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(imei)/10 as a   from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_247
test("VMALL_DICTIONARY_EXCLUDE_247", Include) {
  checkAnswer(s"""select sum(imei)-10 as a   from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(imei)-10 as a   from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_248
test("VMALL_DICTIONARY_EXCLUDE_248", Include) {
  checkAnswer(s"""select sum(contractNumber)+10 as a   from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(contractNumber)+10 as a   from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_249
test("VMALL_DICTIONARY_EXCLUDE_249", Include) {
  checkAnswer(s"""select sum(contractNumber)*10 as a   from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(contractNumber)*10 as a   from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_250
test("VMALL_DICTIONARY_EXCLUDE_250", Include) {
  checkAnswer(s"""select sum(contractNumber)/10 as a   from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(contractNumber)/10 as a   from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_251
test("VMALL_DICTIONARY_EXCLUDE_251", Include) {
  checkAnswer(s"""select sum(contractNumber)-10 as a   from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(contractNumber)-10 as a   from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_252
test("VMALL_DICTIONARY_EXCLUDE_252", Include) {
  checkAnswer(s"""select sum(Latest_DAY)+10 as a   from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(Latest_DAY)+10 as a   from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_253
test("VMALL_DICTIONARY_EXCLUDE_253", Include) {
  checkAnswer(s"""select sum(Latest_DAY)*10 as a   from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(Latest_DAY)*10 as a   from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_254
test("VMALL_DICTIONARY_EXCLUDE_254", Include) {
  checkAnswer(s"""select sum(Latest_DAY)/10 as a   from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(Latest_DAY)/10 as a   from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_255
test("VMALL_DICTIONARY_EXCLUDE_255", Include) {
  checkAnswer(s"""select sum(Latest_DAY)-10 as a   from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(Latest_DAY)-10 as a   from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_256
test("VMALL_DICTIONARY_EXCLUDE_256", Include) {
  sql(s"""select sum(gamePointId)+10 as a   from VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_257
test("VMALL_DICTIONARY_EXCLUDE_257", Include) {
  sql(s"""select sum(gamePointId)*10 as a   from VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_258
test("VMALL_DICTIONARY_EXCLUDE_258", Include) {
  sql(s"""select sum(gamePointId)/10 as a   from VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_259
test("VMALL_DICTIONARY_EXCLUDE_259", Include) {
  sql(s"""select sum(gamePointId)-10 as a   from VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_260
test("VMALL_DICTIONARY_EXCLUDE_260", Include) {
  checkAnswer(s"""select sum(productionDate)+10 as a   from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(productionDate)+10 as a   from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_261
test("VMALL_DICTIONARY_EXCLUDE_261", Include) {
  checkAnswer(s"""select sum(productionDate)*10 as a   from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(productionDate)*10 as a   from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_262
test("VMALL_DICTIONARY_EXCLUDE_262", Include) {
  checkAnswer(s"""select sum(productionDate)/10 as a   from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(productionDate)/10 as a   from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_263
test("VMALL_DICTIONARY_EXCLUDE_263", Include) {
  checkAnswer(s"""select sum(productionDate)-10 as a   from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(productionDate)-10 as a   from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_264
test("VMALL_DICTIONARY_EXCLUDE_264", Include) {
  checkAnswer(s"""select sum(deviceInformationId)+10 as a   from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(deviceInformationId)+10 as a   from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_265
test("VMALL_DICTIONARY_EXCLUDE_265", Include) {
  checkAnswer(s"""select sum(deviceInformationId)*10 as a   from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(deviceInformationId)*10 as a   from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_266
test("VMALL_DICTIONARY_EXCLUDE_266", Include) {
  checkAnswer(s"""select sum(deviceInformationId)/10 as a   from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(deviceInformationId)/10 as a   from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_267
test("VMALL_DICTIONARY_EXCLUDE_267", Include) {
  checkAnswer(s"""select sum(deviceInformationId)-10 as a   from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(deviceInformationId)-10 as a   from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_292
test("VMALL_DICTIONARY_EXCLUDE_292", Include) {
  checkAnswer(s"""SELECT productiondate from VMALL_DICTIONARY_EXCLUDE where productiondate LIKE '2015-09-30%'""",
    s"""SELECT productiondate from VMALL_DICTIONARY_EXCLUDE_hive where productiondate LIKE '2015-09-30%'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_293
test("VMALL_DICTIONARY_EXCLUDE_293", Include) {
  checkAnswer(s"""SELECT productiondate from VMALL_DICTIONARY_EXCLUDE where productiondate LIKE '% %'""",
    s"""SELECT productiondate from VMALL_DICTIONARY_EXCLUDE_hive where productiondate LIKE '% %'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_294
test("VMALL_DICTIONARY_EXCLUDE_294", Include) {
  checkAnswer(s"""SELECT productiondate from VMALL_DICTIONARY_EXCLUDE where productiondate LIKE '%12:07:28'""",
    s"""SELECT productiondate from VMALL_DICTIONARY_EXCLUDE_hive where productiondate LIKE '%12:07:28'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_295
test("VMALL_DICTIONARY_EXCLUDE_295", Include) {
  checkAnswer(s"""select contractnumber from VMALL_DICTIONARY_EXCLUDE where contractnumber like '922337204%' """,
    s"""select contractnumber from VMALL_DICTIONARY_EXCLUDE_hive where contractnumber like '922337204%' """)
}
       

//VMALL_DICTIONARY_EXCLUDE_296
test("VMALL_DICTIONARY_EXCLUDE_296", Include) {
  checkAnswer(s"""select contractnumber from VMALL_DICTIONARY_EXCLUDE where contractnumber like '%047800'""",
    s"""select contractnumber from VMALL_DICTIONARY_EXCLUDE_hive where contractnumber like '%047800'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_297
test("VMALL_DICTIONARY_EXCLUDE_297", Include) {
  checkAnswer(s"""select contractnumber from VMALL_DICTIONARY_EXCLUDE where contractnumber like '%720%'""",
    s"""select contractnumber from VMALL_DICTIONARY_EXCLUDE_hive where contractnumber like '%720%'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_298
test("VMALL_DICTIONARY_EXCLUDE_298", Include) {
  checkAnswer(s"""SELECT Latest_DAY from VMALL_DICTIONARY_EXCLUDE where Latest_DAY like '12345678%'""",
    s"""SELECT Latest_DAY from VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY like '12345678%'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_299
test("VMALL_DICTIONARY_EXCLUDE_299", Include) {
  checkAnswer(s"""SELECT Latest_DAY from VMALL_DICTIONARY_EXCLUDE where Latest_DAY like '%5678%'""",
    s"""SELECT Latest_DAY from VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY like '%5678%'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_300
test("VMALL_DICTIONARY_EXCLUDE_300", Include) {
  checkAnswer(s"""SELECT Latest_DAY from VMALL_DICTIONARY_EXCLUDE where Latest_DAY like '1234567%'""",
    s"""SELECT Latest_DAY from VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY like '1234567%'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_301
test("VMALL_DICTIONARY_EXCLUDE_301", Include) {
  sql(s"""SELECT gamepointID from VMALL_DICTIONARY_EXCLUDE where gamepointID like '1.1098347722%'""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_302
test("VMALL_DICTIONARY_EXCLUDE_302", Include) {
  sql(s"""SELECT gamepointID from VMALL_DICTIONARY_EXCLUDE where gamepointID like '%8347722%'""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_303
test("VMALL_DICTIONARY_EXCLUDE_303", Include) {
  sql(s"""SELECT gamepointID from VMALL_DICTIONARY_EXCLUDE where gamepointID like '%7512E42'""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_304
test("VMALL_DICTIONARY_EXCLUDE_304", Include) {
  checkAnswer(s"""SELECT deviceinformationid from VMALL_DICTIONARY_EXCLUDE where deviceinformationid like '1000%'""",
    s"""SELECT deviceinformationid from VMALL_DICTIONARY_EXCLUDE_hive where deviceinformationid like '1000%'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_305
test("VMALL_DICTIONARY_EXCLUDE_305", Include) {
  checkAnswer(s"""SELECT deviceinformationid from VMALL_DICTIONARY_EXCLUDE where deviceinformationid like '%00%'""",
    s"""SELECT deviceinformationid from VMALL_DICTIONARY_EXCLUDE_hive where deviceinformationid like '%00%'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_306
test("VMALL_DICTIONARY_EXCLUDE_306", Include) {
  checkAnswer(s"""SELECT deviceinformationid from VMALL_DICTIONARY_EXCLUDE where deviceinformationid like '%0084'""",
    s"""SELECT deviceinformationid from VMALL_DICTIONARY_EXCLUDE_hive where deviceinformationid like '%0084'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_307
test("VMALL_DICTIONARY_EXCLUDE_307", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_EXCLUDE where imei like '1AA10%'""",
    s"""select imei from VMALL_DICTIONARY_EXCLUDE_hive where imei like '1AA10%'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_308
test("VMALL_DICTIONARY_EXCLUDE_308", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_EXCLUDE where imei like '%A10%'""",
    s"""select imei from VMALL_DICTIONARY_EXCLUDE_hive where imei like '%A10%'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_309
test("VMALL_DICTIONARY_EXCLUDE_309", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_EXCLUDE where imei like '%00084'""",
    s"""select imei from VMALL_DICTIONARY_EXCLUDE_hive where imei like '%00084'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_310
test("VMALL_DICTIONARY_EXCLUDE_310", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_EXCLUDE where imei in ('1AA100074','1AA100075','1AA100077')""",
    s"""select imei from VMALL_DICTIONARY_EXCLUDE_hive where imei in ('1AA100074','1AA100075','1AA100077')""")
}
       

//VMALL_DICTIONARY_EXCLUDE_311
test("VMALL_DICTIONARY_EXCLUDE_311", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_EXCLUDE where imei not in ('1AA100074','1AA100075','1AA100077')""",
    s"""select imei from VMALL_DICTIONARY_EXCLUDE_hive where imei not in ('1AA100074','1AA100075','1AA100077')""")
}
       

//VMALL_DICTIONARY_EXCLUDE_312
test("VMALL_DICTIONARY_EXCLUDE_312", Include) {
  checkAnswer(s"""select deviceinformationid from VMALL_DICTIONARY_EXCLUDE where deviceinformationid in (100081,100078,10008)""",
    s"""select deviceinformationid from VMALL_DICTIONARY_EXCLUDE_hive where deviceinformationid in (100081,100078,10008)""")
}
       

//VMALL_DICTIONARY_EXCLUDE_313
test("VMALL_DICTIONARY_EXCLUDE_313", Include) {
  checkAnswer(s"""select deviceinformationid from VMALL_DICTIONARY_EXCLUDE where deviceinformationid not in (100081,100078,10008)""",
    s"""select deviceinformationid from VMALL_DICTIONARY_EXCLUDE_hive where deviceinformationid not in (100081,100078,10008)""")
}
       

//VMALL_DICTIONARY_EXCLUDE_314
test("VMALL_DICTIONARY_EXCLUDE_314", Include) {
  sql(s"""select productiondate from VMALL_DICTIONARY_EXCLUDE where productiondate in ('2015-10-04 12:07:28','2015-10-07%','2015-10-07 12:07:28')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_315
test("VMALL_DICTIONARY_EXCLUDE_315", Include) {
  sql(s"""select productiondate from VMALL_DICTIONARY_EXCLUDE where productiondate not in ('2015-10-04 12:07:28','2015-10-07 12:07:28')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_316
test("VMALL_DICTIONARY_EXCLUDE_316", Include) {
  sql(s"""select gamepointid from VMALL_DICTIONARY_EXCLUDE where gamepointid in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_317
test("VMALL_DICTIONARY_EXCLUDE_317", Include) {
  sql(s"""select gamepointid from VMALL_DICTIONARY_EXCLUDE where gamepointid not in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_318
test("VMALL_DICTIONARY_EXCLUDE_318", Include) {
  sql(s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE where Latest_DAY in (1234567890123530.0000000000,1234567890123520.0000000000)""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_319
test("VMALL_DICTIONARY_EXCLUDE_319", Include) {
  sql(s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE where Latest_DAY not in (1234567890123530.0000000000,1234567890123520.0000000000)""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_322
test("VMALL_DICTIONARY_EXCLUDE_322", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_EXCLUDE where imei !='1AA100077'""",
    s"""select imei from VMALL_DICTIONARY_EXCLUDE_hive where imei !='1AA100077'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_323
test("VMALL_DICTIONARY_EXCLUDE_323", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_EXCLUDE where imei NOT LIKE '1AA100077'""",
    s"""select imei from VMALL_DICTIONARY_EXCLUDE_hive where imei NOT LIKE '1AA100077'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_324
test("VMALL_DICTIONARY_EXCLUDE_324", Include) {
  checkAnswer(s"""select deviceinformationid from VMALL_DICTIONARY_EXCLUDE where deviceinformationid !=100078""",
    s"""select deviceinformationid from VMALL_DICTIONARY_EXCLUDE_hive where deviceinformationid !=100078""")
}
       

//VMALL_DICTIONARY_EXCLUDE_325
test("VMALL_DICTIONARY_EXCLUDE_325", Include) {
  checkAnswer(s"""select deviceinformationid from VMALL_DICTIONARY_EXCLUDE where deviceinformationid NOT LIKE 100079""",
    s"""select deviceinformationid from VMALL_DICTIONARY_EXCLUDE_hive where deviceinformationid NOT LIKE 100079""")
}
       

//VMALL_DICTIONARY_EXCLUDE_326
test("VMALL_DICTIONARY_EXCLUDE_326", Include) {
  checkAnswer(s"""select productiondate from VMALL_DICTIONARY_EXCLUDE where productiondate !='2015-10-07 12:07:28'""",
    s"""select productiondate from VMALL_DICTIONARY_EXCLUDE_hive where productiondate !='2015-10-07 12:07:28'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_327
test("VMALL_DICTIONARY_EXCLUDE_327", Include) {
  sql(s"""select productiondate from VMALL_DICTIONARY_EXCLUDE where productiondate NOT LIKE '2015-10-07 12:07:28'""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_328
test("VMALL_DICTIONARY_EXCLUDE_328", Include) {
  sql(s"""select gamepointid from VMALL_DICTIONARY_EXCLUDE where gamepointid !=6.8591561117512E42""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_329
test("VMALL_DICTIONARY_EXCLUDE_329", Include) {
  sql(s"""select gamepointid from VMALL_DICTIONARY_EXCLUDE where gamepointid NOT LIKE 6.8591561117512E43""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_330
test("VMALL_DICTIONARY_EXCLUDE_330", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE where Latest_DAY != 1234567890123520.0000000000""",
    s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY != 1234567890123520.0000000000""")
}
       

//VMALL_DICTIONARY_EXCLUDE_331
test("VMALL_DICTIONARY_EXCLUDE_331", Include) {
  sql(s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE where Latest_DAY NOT LIKE 1234567890123520.0000000000""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_335
test("VMALL_DICTIONARY_EXCLUDE_335", Include) {
  checkAnswer(s"""SELECT productiondate,IMEI from VMALL_DICTIONARY_EXCLUDE where IMEI RLIKE '1AA100077'""",
    s"""SELECT productiondate,IMEI from VMALL_DICTIONARY_EXCLUDE_hive where IMEI RLIKE '1AA100077'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_336
test("VMALL_DICTIONARY_EXCLUDE_336", Include) {
  checkAnswer(s"""SELECT deviceinformationId from VMALL_DICTIONARY_EXCLUDE where deviceinformationId RLIKE '100079'""",
    s"""SELECT deviceinformationId from VMALL_DICTIONARY_EXCLUDE_hive where deviceinformationId RLIKE '100079'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_337
test("VMALL_DICTIONARY_EXCLUDE_337", Include) {
  sql(s"""SELECT gamepointid from VMALL_DICTIONARY_EXCLUDE where gamepointid RLIKE '1.61922711065643E42'""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_338
test("VMALL_DICTIONARY_EXCLUDE_338", Include) {
  checkAnswer(s"""SELECT Latest_Day from VMALL_DICTIONARY_EXCLUDE where Latest_Day RLIKE '1234567890123550.0000000000'""",
    s"""SELECT Latest_Day from VMALL_DICTIONARY_EXCLUDE_hive where Latest_Day RLIKE '1234567890123550.0000000000'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_339
test("VMALL_DICTIONARY_EXCLUDE_339", Include) {
  checkAnswer(s"""SELECT contractnumber from VMALL_DICTIONARY_EXCLUDE where contractnumber RLIKE '9223372047800'""",
    s"""SELECT contractnumber from VMALL_DICTIONARY_EXCLUDE_hive where contractnumber RLIKE '9223372047800'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_340
test("VMALL_DICTIONARY_EXCLUDE_340", Include) {
  sql(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_EXCLUDE a join VMALL_DICTIONARY_EXCLUDE b on a.productiondate=b.productiondate""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_341
test("VMALL_DICTIONARY_EXCLUDE_341", Include) {
  sql(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_EXCLUDE a join VMALL_DICTIONARY_EXCLUDE b on a.deviceinformationid=b.deviceinformationid""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_342
test("VMALL_DICTIONARY_EXCLUDE_342", Include) {
  sql(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_EXCLUDE a join VMALL_DICTIONARY_EXCLUDE b on a.imei=b.imei""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_343
test("VMALL_DICTIONARY_EXCLUDE_343", Include) {
  sql(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_EXCLUDE a join VMALL_DICTIONARY_EXCLUDE b on a.gamepointid=b.gamepointid""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_344
test("VMALL_DICTIONARY_EXCLUDE_344", Include) {
  sql(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_EXCLUDE a join VMALL_DICTIONARY_EXCLUDE b on a.Latest_Day=b.Latest_Day""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_345
test("VMALL_DICTIONARY_EXCLUDE_345", Include) {
  sql(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_EXCLUDE a join VMALL_DICTIONARY_EXCLUDE b on a.contractnumber=b.contractnumber""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_346
test("VMALL_DICTIONARY_EXCLUDE_346", Include) {
  checkAnswer(s"""select count( contractNumber ),sum( contractNumber ),count(distinct contractNumber ),avg( contractNumber ),max( contractNumber ),min( contractNumber ),1 from VMALL_DICTIONARY_EXCLUDE""",
    s"""select count( contractNumber ),sum( contractNumber ),count(distinct contractNumber ),avg( contractNumber ),max( contractNumber ),min( contractNumber ),1 from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_347
test("VMALL_DICTIONARY_EXCLUDE_347", Include) {
  checkAnswer(s"""select count( Latest_Day ),sum( Latest_Day ),count(distinct Latest_Day ),avg( Latest_Day ),max( Latest_Day ),min( Latest_Day ),1 from VMALL_DICTIONARY_EXCLUDE""",
    s"""select count( Latest_Day ),sum( Latest_Day ),count(distinct Latest_Day ),avg( Latest_Day ),max( Latest_Day ),min( Latest_Day ),1 from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_348
test("VMALL_DICTIONARY_EXCLUDE_348", Include) {
  sql(s"""select count( gamePointId),sum( gamePointId ),count(distinct gamePointId ),avg(gamePointId),max(gamePointId),min(gamePointId),1 from VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_349
test("VMALL_DICTIONARY_EXCLUDE_349", Include) {
  checkAnswer(s"""select count(deviceInformationId),sum(deviceInformationId),count(deviceInformationId),avg(deviceInformationId),max(deviceInformationId),min(deviceInformationId),1 from VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(deviceInformationId),sum(deviceInformationId),count(deviceInformationId),avg(deviceInformationId),max(deviceInformationId),min(deviceInformationId),1 from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_350
test("VMALL_DICTIONARY_EXCLUDE_350", Include) {
  checkAnswer(s"""select count( productionDate),sum(  productionDate ),count(distinct  productionDate ),avg(  productionDate ),max(  productionDate),min(  productionDate ),1 from VMALL_DICTIONARY_EXCLUDE""",
    s"""select count( productionDate),sum(  productionDate ),count(distinct  productionDate ),avg(  productionDate ),max(  productionDate),min(  productionDate ),1 from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_351
test("VMALL_DICTIONARY_EXCLUDE_351", Include) {
  checkAnswer(s"""select count(IMEI ),sum(IMEI ),count(distinct IMEI ),avg(IMEI ),max(IMEI ),min(IMEI ),1 from VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(IMEI ),sum(IMEI ),count(distinct IMEI ),avg(IMEI ),max(IMEI ),min(IMEI ),1 from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_352
test("VMALL_DICTIONARY_EXCLUDE_352", Include) {
  checkAnswer(s"""select sum(contractNumber),count(contractNumber),avg(contractNumber),sum(contractNumber)/count(contractNumber) from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(contractNumber),count(contractNumber),avg(contractNumber),sum(contractNumber)/count(contractNumber) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_353
test("VMALL_DICTIONARY_EXCLUDE_353", Include) {
  checkAnswer(s"""select sum(Latest_Day),count(Latest_Day),avg(Latest_Day),sum(Latest_Day)/count(Latest_Day) from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(Latest_Day),count(Latest_Day),avg(Latest_Day),sum(Latest_Day)/count(Latest_Day) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_354
test("VMALL_DICTIONARY_EXCLUDE_354", Include) {
  sql(s"""select sum(gamepointId),count(gamepointId),avg(gamepointID),sum(gamepointID)/count(gamepointID) from VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_355
test("VMALL_DICTIONARY_EXCLUDE_355", Include) {
  checkAnswer(s"""select sum(deviceinformationId),count(deviceinformationId),avg(deviceinformationId),sum(deviceinformationId)/count(deviceinformationId) from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(deviceinformationId),count(deviceinformationId),avg(deviceinformationId),sum(deviceinformationId)/count(deviceinformationId) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_356
test("VMALL_DICTIONARY_EXCLUDE_356", Include) {
  checkAnswer(s"""select sum(IMEI),count(IMEI),avg(IMEI),sum(IMEI)/count(IMEI) from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(IMEI),count(IMEI),avg(IMEI),sum(IMEI)/count(IMEI) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_357
test("VMALL_DICTIONARY_EXCLUDE_357", Include) {
  checkAnswer(s"""select sum(productionDate),count(productionDate),avg(productionDate),sum(productionDate)/count(productionDate) from VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(productionDate),count(productionDate),avg(productionDate),sum(productionDate)/count(productionDate) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_358
test("VMALL_DICTIONARY_EXCLUDE_358", Include) {
  sql(s"""select contractNumber,Latest_DAY,gamePointId,productionDate,deviceInformationId,IMEI  from VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_359
test("VMALL_DICTIONARY_EXCLUDE_359", Include) {
  checkAnswer(s"""select count(MAC) from VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(MAC) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_360
test("VMALL_DICTIONARY_EXCLUDE_360", Include) {
  checkAnswer(s"""select count(gamePointId) from VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(gamePointId) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_361
test("VMALL_DICTIONARY_EXCLUDE_361", Include) {
  checkAnswer(s"""select count(contractNumber) from VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(contractNumber) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_362
test("VMALL_DICTIONARY_EXCLUDE_362", Include) {
  checkAnswer(s"""select count(Latest_DAY) from VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(Latest_DAY) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_363
test("VMALL_DICTIONARY_EXCLUDE_363", Include) {
  checkAnswer(s"""select count(productionDate) from VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(productionDate) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_364
test("VMALL_DICTIONARY_EXCLUDE_364", Include) {
  checkAnswer(s"""select count(deviceInformationId) from VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(deviceInformationId) from VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//VMALL_DICTIONARY_EXCLUDE_365
test("VMALL_DICTIONARY_EXCLUDE_365", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_EXCLUDE where  contractNumber  != '9223372047700'""",
    s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_EXCLUDE_hive where  contractNumber  != '9223372047700'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_366
test("VMALL_DICTIONARY_EXCLUDE_366", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_EXCLUDE where  Latest_DAY  != '1234567890123480.0000000000' order by deviceInformationId limit 5""",
    s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_EXCLUDE_hive where  Latest_DAY  != '1234567890123480.0000000000' order by deviceInformationId limit 5""")
}
       

//VMALL_DICTIONARY_EXCLUDE_367
test("VMALL_DICTIONARY_EXCLUDE_367", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_EXCLUDE where  gamePointId  != '2.27852521808948E36' order by imei limit 5""",
    s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_EXCLUDE_hive where  gamePointId  != '2.27852521808948E36' order by imei limit 5""")
}
       

//VMALL_DICTIONARY_EXCLUDE_368
test("VMALL_DICTIONARY_EXCLUDE_368", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_EXCLUDE where  productionDate  != '2015-09-18 12:07:28.0' order by imei limit 5""",
    s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_EXCLUDE_hive where  productionDate  != '2015-09-18 12:07:28.0' order by imei limit 5""")
}
       

//VMALL_DICTIONARY_EXCLUDE_369
test("VMALL_DICTIONARY_EXCLUDE_369", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_EXCLUDE where  deviceInformationId  != '100075' order by imei limit 5""",
    s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_EXCLUDE_hive where  deviceInformationId  != '100075' order by imei limit 5""")
}
       

//VMALL_DICTIONARY_EXCLUDE_370
test("VMALL_DICTIONARY_EXCLUDE_370", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_EXCLUDE where  contractNumber  not like '9223372047700' order by  deviceInformationId limit 5""",
    s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_EXCLUDE_hive where  contractNumber  not like '9223372047700' order by  deviceInformationId limit 5""")
}
       

//VMALL_DICTIONARY_EXCLUDE_371
test("VMALL_DICTIONARY_EXCLUDE_371", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_EXCLUDE where  Latest_DAY  not like '1234567890123480.0000000000' order by deviceInformationId limit 5""",
    s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_EXCLUDE_hive where  Latest_DAY  not like '1234567890123480.0000000000' order by deviceInformationId limit 5""")
}
       

//VMALL_DICTIONARY_EXCLUDE_372
test("VMALL_DICTIONARY_EXCLUDE_372", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_EXCLUDE where  gamePointId  not like '2.27852521808948E36' order by imei limit 5""",
    s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_EXCLUDE_hive where  gamePointId  not like '2.27852521808948E36' order by imei limit 5""")
}
       

//VMALL_DICTIONARY_EXCLUDE_373
test("VMALL_DICTIONARY_EXCLUDE_373", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_EXCLUDE where  productionDate  not like '2015-09-18 12:07:28.0' order by imei limit 5""",
    s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_EXCLUDE_hive where  productionDate  not like '2015-09-18 12:07:28.0' order by imei limit 5""")
}
       

//VMALL_DICTIONARY_EXCLUDE_374
test("VMALL_DICTIONARY_EXCLUDE_374", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_EXCLUDE where  deviceInformationId  not like '100075' order by imei limit 5""",
    s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_EXCLUDE_hive where  deviceInformationId  not like '100075' order by imei limit 5""")
}
       

//VMALL_DICTIONARY_EXCLUDE_375
test("VMALL_DICTIONARY_EXCLUDE_375", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_EXCLUDE where imei is not null""",
    s"""select imei from VMALL_DICTIONARY_EXCLUDE_hive where imei is not null""")
}
       

//VMALL_DICTIONARY_EXCLUDE_376
test("VMALL_DICTIONARY_EXCLUDE_376", Include) {
  sql(s"""select gamePointId from VMALL_DICTIONARY_EXCLUDE where gamePointId is not null""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_377
test("VMALL_DICTIONARY_EXCLUDE_377", Include) {
  checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_EXCLUDE where contractNumber is not null""",
    s"""select contractNumber from VMALL_DICTIONARY_EXCLUDE_hive where contractNumber is not null""")
}
       

//VMALL_DICTIONARY_EXCLUDE_378
test("VMALL_DICTIONARY_EXCLUDE_378", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE where Latest_DAY is not null""",
    s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY is not null""")
}
       

//VMALL_DICTIONARY_EXCLUDE_379
test("VMALL_DICTIONARY_EXCLUDE_379", Include) {
  checkAnswer(s"""select productionDate from VMALL_DICTIONARY_EXCLUDE where productionDate is not null""",
    s"""select productionDate from VMALL_DICTIONARY_EXCLUDE_hive where productionDate is not null""")
}
       

//VMALL_DICTIONARY_EXCLUDE_380
test("VMALL_DICTIONARY_EXCLUDE_380", Include) {
  checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_EXCLUDE where deviceInformationId is not null""",
    s"""select deviceInformationId from VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId is not null""")
}
       

//VMALL_DICTIONARY_EXCLUDE_381
test("VMALL_DICTIONARY_EXCLUDE_381", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_EXCLUDE where imei is  null""",
    s"""select imei from VMALL_DICTIONARY_EXCLUDE_hive where imei is  null""")
}
       

//VMALL_DICTIONARY_EXCLUDE_382
test("VMALL_DICTIONARY_EXCLUDE_382", Include) {
  checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_EXCLUDE where gamePointId is  null""",
    s"""select gamePointId from VMALL_DICTIONARY_EXCLUDE_hive where gamePointId is  null""")
}
       

//VMALL_DICTIONARY_EXCLUDE_383
test("VMALL_DICTIONARY_EXCLUDE_383", Include) {
  checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_EXCLUDE where contractNumber is  null""",
    s"""select contractNumber from VMALL_DICTIONARY_EXCLUDE_hive where contractNumber is  null""")
}
       

//VMALL_DICTIONARY_EXCLUDE_384
test("VMALL_DICTIONARY_EXCLUDE_384", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE where Latest_DAY is  null""",
    s"""select Latest_DAY from VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY is  null""")
}
       

//VMALL_DICTIONARY_EXCLUDE_385
test("VMALL_DICTIONARY_EXCLUDE_385", Include) {
  checkAnswer(s"""select productionDate from VMALL_DICTIONARY_EXCLUDE where productionDate is  null""",
    s"""select productionDate from VMALL_DICTIONARY_EXCLUDE_hive where productionDate is  null""")
}
       

//VMALL_DICTIONARY_EXCLUDE_386
test("VMALL_DICTIONARY_EXCLUDE_386", Include) {
  checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_EXCLUDE where deviceInformationId is  null""",
    s"""select deviceInformationId from VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId is  null""")
}
       

//VMALL_DICTIONARY_EXCLUDE_387
test("VMALL_DICTIONARY_EXCLUDE_387", Include) {
  checkAnswer(s"""select count(*) from VMALL_DICTIONARY_EXCLUDE where imei = '1AA1'""",
    s"""select count(*) from VMALL_DICTIONARY_EXCLUDE_hive where imei = '1AA1'""")
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_001
test("VMALL_DICTIONARY_EXCLUDE_PushUP_001", Include) {
  sql(s"""select count(imei)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000'  and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_002
test("VMALL_DICTIONARY_EXCLUDE_PushUP_002", Include) {
  sql(s"""select count(deviceinformationId)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_003
test("VMALL_DICTIONARY_EXCLUDE_PushUP_003", Include) {
  sql(s"""select count(productionDate)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_004
test("VMALL_DICTIONARY_EXCLUDE_PushUP_004", Include) {
  sql(s"""select count(gamePointId)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_005
test("VMALL_DICTIONARY_EXCLUDE_PushUP_005", Include) {
  sql(s"""select count(Latest_DAY)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_006
test("VMALL_DICTIONARY_EXCLUDE_PushUP_006", Include) {
  sql(s"""select count(contractNumber)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_007
test("VMALL_DICTIONARY_EXCLUDE_PushUP_007", Include) {
  sql(s"""select count(distinct imei)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_008
test("VMALL_DICTIONARY_EXCLUDE_PushUP_008", Include) {
  sql(s"""select count(distinct deviceinformationId)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_009
test("VMALL_DICTIONARY_EXCLUDE_PushUP_009", Include) {
  sql(s"""select count(distinct productionDate)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_010
test("VMALL_DICTIONARY_EXCLUDE_PushUP_010", Include) {
  sql(s"""select count(distinct gamePointId)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_011
test("VMALL_DICTIONARY_EXCLUDE_PushUP_011", Include) {
  sql(s"""select count(distinct Latest_DAY)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_012
test("VMALL_DICTIONARY_EXCLUDE_PushUP_012", Include) {
  sql(s"""select count(distinct contractNumber)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_013
test("VMALL_DICTIONARY_EXCLUDE_PushUP_013", Include) {
  sql(s"""select sum(deviceinformationId)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_014
test("VMALL_DICTIONARY_EXCLUDE_PushUP_014", Include) {
  sql(s"""select sum(productionDate)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_015
test("VMALL_DICTIONARY_EXCLUDE_PushUP_015", Include) {
  sql(s"""select sum(gamePointId)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_016
test("VMALL_DICTIONARY_EXCLUDE_PushUP_016", Include) {
  sql(s"""select sum(Latest_DAY)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_017
test("VMALL_DICTIONARY_EXCLUDE_PushUP_017", Include) {
  sql(s"""select sum(contractNumber)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_018
test("VMALL_DICTIONARY_EXCLUDE_PushUP_018", Include) {
  sql(s"""select sum(distinct deviceinformationId)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_019
test("VMALL_DICTIONARY_EXCLUDE_PushUP_019", Include) {
  sql(s"""select sum(distinct gamePointId)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_020
test("VMALL_DICTIONARY_EXCLUDE_PushUP_020", Include) {
  sql(s"""select sum(distinct Latest_DAY)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_021
test("VMALL_DICTIONARY_EXCLUDE_PushUP_021", Include) {
  sql(s"""select sum(distinct contractNumber)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_022
test("VMALL_DICTIONARY_EXCLUDE_PushUP_022", Include) {
  sql(s"""select min(deviceinformationId)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_023
test("VMALL_DICTIONARY_EXCLUDE_PushUP_023", Include) {
  sql(s"""select min(productionDate)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_024
test("VMALL_DICTIONARY_EXCLUDE_PushUP_024", Include) {
  sql(s"""select min(gamePointId)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_025
test("VMALL_DICTIONARY_EXCLUDE_PushUP_025", Include) {
  sql(s"""select min(Latest_DAY)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_026
test("VMALL_DICTIONARY_EXCLUDE_PushUP_026", Include) {
  sql(s"""select min(contractNumber)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_027
test("VMALL_DICTIONARY_EXCLUDE_PushUP_027", Include) {
  sql(s"""select max(imei)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_028
test("VMALL_DICTIONARY_EXCLUDE_PushUP_028", Include) {
  sql(s"""select max(deviceinformationId)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_029
test("VMALL_DICTIONARY_EXCLUDE_PushUP_029", Include) {
  sql(s"""select max(productionDate)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_030
test("VMALL_DICTIONARY_EXCLUDE_PushUP_030", Include) {
  sql(s"""select max(gamePointId)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_031
test("VMALL_DICTIONARY_EXCLUDE_PushUP_031", Include) {
  sql(s"""select max(Latest_DAY)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_032
test("VMALL_DICTIONARY_EXCLUDE_PushUP_032", Include) {
  sql(s"""select max(contractNumber)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_033
test("VMALL_DICTIONARY_EXCLUDE_PushUP_033", Include) {
  sql(s"""select variance(deviceInformationId)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_035
test("VMALL_DICTIONARY_EXCLUDE_PushUP_035", Include) {
  sql(s"""select variance(gamePointId)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_036
test("VMALL_DICTIONARY_EXCLUDE_PushUP_036", Include) {
  sql(s"""select variance(Latest_DAY)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_037
test("VMALL_DICTIONARY_EXCLUDE_PushUP_037", Include) {
  sql(s"""select variance(contractNumber)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_038
test("VMALL_DICTIONARY_EXCLUDE_PushUP_038", Include) {
  sql(s"""select var_samp(deviceInformationId)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_040
test("VMALL_DICTIONARY_EXCLUDE_PushUP_040", Include) {
  sql(s"""select var_samp(gamePointId)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_041
test("VMALL_DICTIONARY_EXCLUDE_PushUP_041", Include) {
  sql(s"""select var_samp(Latest_DAY)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_042
test("VMALL_DICTIONARY_EXCLUDE_PushUP_042", Include) {
  sql(s"""select var_samp(contractNumber)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_043
test("VMALL_DICTIONARY_EXCLUDE_PushUP_043", Include) {
  sql(s"""select stddev_pop(deviceInformationId)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_045
test("VMALL_DICTIONARY_EXCLUDE_PushUP_045", Include) {
  sql(s"""select stddev_pop(gamePointId)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_046
test("VMALL_DICTIONARY_EXCLUDE_PushUP_046", Include) {
  sql(s"""select stddev_pop(Latest_DAY)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_047
test("VMALL_DICTIONARY_EXCLUDE_PushUP_047", Include) {
  sql(s"""select stddev_pop(contractNumber)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_048
test("VMALL_DICTIONARY_EXCLUDE_PushUP_048", Include) {
  sql(s"""select stddev_samp(deviceInformationId)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_050
test("VMALL_DICTIONARY_EXCLUDE_PushUP_050", Include) {
  sql(s"""select stddev_samp(gamePointId)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_051
test("VMALL_DICTIONARY_EXCLUDE_PushUP_051", Include) {
  sql(s"""select stddev_samp(Latest_DAY)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_054
test("VMALL_DICTIONARY_EXCLUDE_PushUP_054", Include) {
  sql(s"""select AVG(deviceinformationId)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_055
test("VMALL_DICTIONARY_EXCLUDE_PushUP_055", Include) {
  sql(s"""select AVG(productionDate)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_056
test("VMALL_DICTIONARY_EXCLUDE_PushUP_056", Include) {
  sql(s"""select AVG(gamePointId)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_057
test("VMALL_DICTIONARY_EXCLUDE_PushUP_057", Include) {
  sql(s"""select AVG(Latest_DAY)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_058
test("VMALL_DICTIONARY_EXCLUDE_PushUP_058", Include) {
  sql(s"""select AVG(contractNumber)  from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_059
test("VMALL_DICTIONARY_EXCLUDE_PushUP_059", Include) {
  checkAnswer(s"""select count(imei),sum(distinct deviceinformationId),min(productionDate),max(imei),variance(Latest_DAY) from VMALL_DICTIONARY_EXCLUDE group by deviceInformationId limit 5""",
    s"""select count(imei),sum(distinct deviceinformationId),min(productionDate),max(imei),variance(Latest_DAY) from VMALL_DICTIONARY_EXCLUDE_hive group by deviceInformationId limit 5""")
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_060
test("VMALL_DICTIONARY_EXCLUDE_PushUP_060", Include) {
  checkAnswer(s"""select sum(deviceinformationId),sum(distinct deviceinformationId),min(deviceinformationId),max(imei),variance(deviceInformationId) from VMALL_DICTIONARY_EXCLUDE group by deviceInformationId order by deviceinformationId limit 5""",
    s"""select sum(deviceinformationId),sum(distinct deviceinformationId),min(deviceinformationId),max(imei),variance(deviceInformationId) from VMALL_DICTIONARY_EXCLUDE_hive group by deviceInformationId order by deviceinformationId limit 5""")
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_061
test("VMALL_DICTIONARY_EXCLUDE_PushUP_061", Include) {
  sql(s"""select count(imei),sum(distinct deviceinformationId),min(productionDate),max(imei),variance(Latest_DAY) from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')group by deviceInformationId limit 5""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_062
test("VMALL_DICTIONARY_EXCLUDE_PushUP_062", Include) {
  sql(s"""select count(imei),sum(distinct deviceinformationId),min(productionDate),max(imei),variance(Latest_DAY) from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') group by deviceInformationId order by deviceinformationId limit 5""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_063
test("VMALL_DICTIONARY_EXCLUDE_PushUP_063", Include) {
  sql(s"""select count(imei),sum(distinct deviceinformationId),min(productionDate),max(imei),variance(Latest_DAY) from VMALL_DICTIONARY_EXCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') group by deviceInformationId,productionDate   sort by productionDate limit 5
""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_064
test("VMALL_DICTIONARY_EXCLUDE_PushUP_064", Include) {
  checkAnswer(s"""select sum(deviceinformationId+10)t  from  VMALL_DICTIONARY_EXCLUDE having t >1234567""",
    s"""select sum(deviceinformationId+10)t  from  VMALL_DICTIONARY_EXCLUDE_hive having t >1234567""")
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_065
test("VMALL_DICTIONARY_EXCLUDE_PushUP_065", Include) {
  sql(s"""select sum(deviceinformationId+gamePointId)t  from  VMALL_DICTIONARY_EXCLUDE having t >1234567""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_066
test("VMALL_DICTIONARY_EXCLUDE_PushUP_066", Include) {
  sql(s"""select sum(deviceinformationId)t,Sum(gamePointId)   from  VMALL_DICTIONARY_EXCLUDE having t >1234567""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_067
test("VMALL_DICTIONARY_EXCLUDE_PushUP_067", Include) {
  checkAnswer(s"""select count(imei),sum(distinct deviceinformationId),min(productionDate)  from VMALL_DICTIONARY_EXCLUDE group by imei,deviceinformationId,productionDate  order by  imei""",
    s"""select count(imei),sum(distinct deviceinformationId),min(productionDate)  from VMALL_DICTIONARY_EXCLUDE_hive group by imei,deviceinformationId,productionDate  order by  imei""")
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_069
test("VMALL_DICTIONARY_EXCLUDE_PushUP_069", Include) {
  sql(s"""SELECT  min(Latest_DAY),max(imei),variance(contractNumber), SUM(gamePointId),count(imei),sum(distinct deviceinformationId) FROM (select * from VMALL_DICTIONARY_EXCLUDE) SUB_QRY GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC limit 10""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_070
test("VMALL_DICTIONARY_EXCLUDE_PushUP_070", Include) {
  sql(s"""SELECT  VMALL_DICTIONARY_EXCLUDE.gamePointId AS gamePointId,VMALL_DICTIONARY_EXCLUDE.AMSize AS AMSize, VMALL_DICTIONARY_EXCLUDE.ActiveCountry AS ActiveCountry, VMALL_DICTIONARY_EXCLUDE.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from VMALL_DICTIONARY_EXCLUDE) SUB_QRY ) VMALL_DICTIONARY_EXCLUDE FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from VMALL_DICTIONARY_EXCLUDE) SUB_QRY ) VMALL_DICTIONARY_EXCLUDE1 ON VMALL_DICTIONARY_EXCLUDE.AMSize = VMALL_DICTIONARY_EXCLUDE1.AMSize WHERE VMALL_DICTIONARY_EXCLUDE.AMSize LIKE '5RAM %' GROUP BY VMALL_DICTIONARY_EXCLUDE.AMSize, VMALL_DICTIONARY_EXCLUDE.ActiveCountry, VMALL_DICTIONARY_EXCLUDE.Activecity,VMALL_DICTIONARY_EXCLUDE.gamePointId ORDER BY VMALL_DICTIONARY_EXCLUDE.AMSize ASC, VMALL_DICTIONARY_EXCLUDE.ActiveCountry ASC, VMALL_DICTIONARY_EXCLUDE.Activecity ASC""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_071
test("VMALL_DICTIONARY_EXCLUDE_PushUP_071", Include) {
  sql(s"""SELECT VMALL_DICTIONARY_EXCLUDE.gamePointId AS gamePointId,VMALL_DICTIONARY_EXCLUDE.AMSize AS AMSize, VMALL_DICTIONARY_EXCLUDE.ActiveCountry AS ActiveCountry, VMALL_DICTIONARY_EXCLUDE.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from VMALL_DICTIONARY_EXCLUDE) SUB_QRY ) VMALL_DICTIONARY_EXCLUDE RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from VMALL_DICTIONARY_EXCLUDE) SUB_QRY ) VMALL_DICTIONARY_EXCLUDE1 ON VMALL_DICTIONARY_EXCLUDE.AMSize = VMALL_DICTIONARY_EXCLUDE1.AMSize WHERE NOT(VMALL_DICTIONARY_EXCLUDE.AMSize = "8RAM size") GROUP BY VMALL_DICTIONARY_EXCLUDE.AMSize, VMALL_DICTIONARY_EXCLUDE.ActiveCountry, VMALL_DICTIONARY_EXCLUDE.Activecity,VMALL_DICTIONARY_EXCLUDE.gamePointId  ORDER BY VMALL_DICTIONARY_EXCLUDE.AMSize ASC, VMALL_DICTIONARY_EXCLUDE.ActiveCountry ASC, VMALL_DICTIONARY_EXCLUDE.Activecity ASC limit 10""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_072
test("VMALL_DICTIONARY_EXCLUDE_PushUP_072", Include) {
  sql(s"""SELECT VMALL_DICTIONARY_EXCLUDE.gamePointId AS gamePointId,VMALL_DICTIONARY_EXCLUDE.AMSize AS AMSize, VMALL_DICTIONARY_EXCLUDE.ActiveCountry AS ActiveCountry, VMALL_DICTIONARY_EXCLUDE.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from VMALL_DICTIONARY_EXCLUDE) SUB_QRY ) VMALL_DICTIONARY_EXCLUDE LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from VMALL_DICTIONARY_EXCLUDE) SUB_QRY ) VMALL_DICTIONARY_EXCLUDE1 ON VMALL_DICTIONARY_EXCLUDE.AMSize = VMALL_DICTIONARY_EXCLUDE1.AMSize WHERE NOT(VMALL_DICTIONARY_EXCLUDE.AMSize = "8RAM size") GROUP BY VMALL_DICTIONARY_EXCLUDE.AMSize, VMALL_DICTIONARY_EXCLUDE.ActiveCountry, VMALL_DICTIONARY_EXCLUDE.Activecity,VMALL_DICTIONARY_EXCLUDE.gamePointId  ORDER BY VMALL_DICTIONARY_EXCLUDE.AMSize ASC, VMALL_DICTIONARY_EXCLUDE.ActiveCountry ASC, VMALL_DICTIONARY_EXCLUDE.Activecity ASC limit 10
""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_073
test("VMALL_DICTIONARY_EXCLUDE_PushUP_073", Include) {
  sql(s"""SELECT VMALL_DICTIONARY_EXCLUDE.gamePointId AS gamePointId,VMALL_DICTIONARY_EXCLUDE.AMSize AS AMSize, VMALL_DICTIONARY_EXCLUDE.ActiveCountry AS ActiveCountry, VMALL_DICTIONARY_EXCLUDE.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from VMALL_DICTIONARY_EXCLUDE) SUB_QRY ) VMALL_DICTIONARY_EXCLUDE INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from VMALL_DICTIONARY_EXCLUDE) SUB_QRY ) VMALL_DICTIONARY_EXCLUDE1 ON VMALL_DICTIONARY_EXCLUDE.AMSize = VMALL_DICTIONARY_EXCLUDE1.AMSize WHERE NOT(VMALL_DICTIONARY_EXCLUDE.AMSize = "8RAM size") GROUP BY VMALL_DICTIONARY_EXCLUDE.AMSize, VMALL_DICTIONARY_EXCLUDE.ActiveCountry, VMALL_DICTIONARY_EXCLUDE.Activecity,VMALL_DICTIONARY_EXCLUDE.gamePointId  ORDER BY VMALL_DICTIONARY_EXCLUDE.AMSize ASC, VMALL_DICTIONARY_EXCLUDE.ActiveCountry ASC, VMALL_DICTIONARY_EXCLUDE.Activecity ASC limit 10
""").collect
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_074
test("VMALL_DICTIONARY_EXCLUDE_PushUP_074", Include) {
  checkAnswer(s"""select count(gamepointid),series  from VMALL_DICTIONARY_EXCLUDE group by series order by series limit 5""",
    s"""select count(gamepointid),series  from VMALL_DICTIONARY_EXCLUDE_hive group by series order by series limit 5""")
}
       

//VMALL_DICTIONARY_EXCLUDE_PushUP_075
test("VMALL_DICTIONARY_EXCLUDE_PushUP_075", Include) {
  checkAnswer(s"""select count(gamepointid),series  from VMALL_DICTIONARY_EXCLUDE group by series order by series""",
    s"""select count(gamepointid),series  from VMALL_DICTIONARY_EXCLUDE_hive group by series order by series""")
}
       
override def afterAll {
sql("drop table if exists vmall_dictionary_exclude")
sql("drop table if exists vmall_dictionary_exclude_hive")
sql("drop table if exists VMALL_DICTIONARY_EXCLUDE")
sql("drop table if exists VMALL_DICTIONARY_EXCLUDE_hive")
}
}