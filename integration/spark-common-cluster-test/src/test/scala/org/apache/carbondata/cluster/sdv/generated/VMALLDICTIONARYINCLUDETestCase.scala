
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
 * Test Class for vmalldictionaryinclude to verify all scenerios
 */

class VMALLDICTIONARYINCLUDETestCase extends QueryTest with BeforeAndAfterAll {
         

//VMALL_DICTIONARY_INCLUDE_CreateCube_Drop
test("VMALL_DICTIONARY_INCLUDE_CreateCube_Drop", Include) {
  sql(s"""drop table if exists  vmall_dictionary_include""").collect

  sql(s"""drop table if exists  vmall_dictionary_include_hive""").collect

}
       

//VMALL_DICTIONARY_INCLUDE_CreateCube
test("VMALL_DICTIONARY_INCLUDE_CreateCube", Include) {
  sql(s"""create table  VMALL_DICTIONARY_INCLUDE (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt)  STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='imei,deviceInformationId,productionDate,gamePointId,Latest_DAY,contractNumber')
""").collect

  sql(s"""create table  VMALL_DICTIONARY_INCLUDE_hive (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt)   ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//VMALL_DICTIONARY_INCLUDE_CreateCube_count
test("VMALL_DICTIONARY_INCLUDE_CreateCube_count", Include) {
  sql(s"""select count(*) from VMALL_DICTIONARY_INCLUDE""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_DataLoad
test("VMALL_DICTIONARY_INCLUDE_DataLoad", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table VMALL_DICTIONARY_INCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table VMALL_DICTIONARY_INCLUDE_hive """).collect

}
       

//VMALL_DICTIONARY_INCLUDE_001
test("VMALL_DICTIONARY_INCLUDE_001", Include) {
  sql(s"""Select count(imei) from VMALL_DICTIONARY_INCLUDE""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_002
test("VMALL_DICTIONARY_INCLUDE_002", Include) {
  sql(s"""select count(DISTINCT imei) as a from VMALL_DICTIONARY_INCLUDE""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_003
test("VMALL_DICTIONARY_INCLUDE_003", Include) {
  sql(s"""select sum(Latest_month)+10 as a ,imei  from VMALL_DICTIONARY_INCLUDE group by imei order by imei""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_004
test("VMALL_DICTIONARY_INCLUDE_004", Include) {
  sql(s"""select max(imei),min(imei) from VMALL_DICTIONARY_INCLUDE""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_005
test("VMALL_DICTIONARY_INCLUDE_005", Include) {
  sql(s"""select min(imei), max(imei) Total from VMALL_DICTIONARY_INCLUDE group by  channelsId order by Total""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_006
test("VMALL_DICTIONARY_INCLUDE_006", Include) {
  checkAnswer(s"""select last(imei) a from VMALL_DICTIONARY_INCLUDE  group by imei order by imei limit 1""",
    s"""select last(imei) a from VMALL_DICTIONARY_INCLUDE_hive  group by imei order by imei limit 1""")
}
       

//VMALL_DICTIONARY_INCLUDE_007
test("VMALL_DICTIONARY_INCLUDE_007", Include) {
  sql(s"""select FIRST(imei) a from VMALL_DICTIONARY_INCLUDE group by imei order by imei limit 1""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_008
test("VMALL_DICTIONARY_INCLUDE_008", Include) {
  sql(s"""select imei,count(imei) a from VMALL_DICTIONARY_INCLUDE group by imei order by imei""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_009
test("VMALL_DICTIONARY_INCLUDE_009", Include) {
  sql(s"""select Lower(imei) a  from VMALL_DICTIONARY_INCLUDE order by imei""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_010
test("VMALL_DICTIONARY_INCLUDE_010", Include) {
  sql(s"""select distinct imei from VMALL_DICTIONARY_INCLUDE order by imei""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_011
test("VMALL_DICTIONARY_INCLUDE_011", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_INCLUDE order by imei limit 101 """).collect
}
       

//VMALL_DICTIONARY_INCLUDE_012
test("VMALL_DICTIONARY_INCLUDE_012", Include) {
  sql(s"""select imei as a from VMALL_DICTIONARY_INCLUDE  order by a asc limit 10""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_013
test("VMALL_DICTIONARY_INCLUDE_013", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_INCLUDE where  (contractNumber == 9223372047700) and (imei=='1AA100004')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_014
test("VMALL_DICTIONARY_INCLUDE_014", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei !='1AA100064' order by imei""",
    s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei !='1AA100064' order by imei""")
}
       

//VMALL_DICTIONARY_INCLUDE_015
test("VMALL_DICTIONARY_INCLUDE_015", Include) {
  checkAnswer(s"""select imei  from VMALL_DICTIONARY_INCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
    s"""select imei  from VMALL_DICTIONARY_INCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""")
}
       

//VMALL_DICTIONARY_INCLUDE_016
test("VMALL_DICTIONARY_INCLUDE_016", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei !='1AA100012' order by imei""",
    s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei !='1AA100012' order by imei""")
}
       

//VMALL_DICTIONARY_INCLUDE_017
test("VMALL_DICTIONARY_INCLUDE_017", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei >'1AA100012' order by imei""",
    s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei >'1AA100012' order by imei""")
}
       

//VMALL_DICTIONARY_INCLUDE_018
test("VMALL_DICTIONARY_INCLUDE_018", Include) {
  checkAnswer(s"""select imei  from VMALL_DICTIONARY_INCLUDE where imei<>imei""",
    s"""select imei  from VMALL_DICTIONARY_INCLUDE_hive where imei<>imei""")
}
       

//VMALL_DICTIONARY_INCLUDE_026
test("VMALL_DICTIONARY_INCLUDE_026", Include) {
  checkAnswer(s"""select count(DISTINCT imei,latest_day) as a from VMALL_DICTIONARY_INCLUDE""",
    s"""select count(DISTINCT imei,latest_day) as a from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_027
test("VMALL_DICTIONARY_INCLUDE_027", Include) {
  checkAnswer(s"""select max(imei),min(imei),count(imei) from VMALL_DICTIONARY_INCLUDE""",
    s"""select max(imei),min(imei),count(imei) from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_028
test("VMALL_DICTIONARY_INCLUDE_028", Include) {
  checkAnswer(s"""select sum(imei),avg(imei),count(imei) a  from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(imei),avg(imei),count(imei) a  from VMALL_DICTIONARY_INCLUDE_hive""")
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
    s"""select imei,count(imei) a from VMALL_DICTIONARY_INCLUDE_hive group by imei order by imei""")
}
       

//VMALL_DICTIONARY_INCLUDE_032
test("VMALL_DICTIONARY_INCLUDE_032", Include) {
  checkAnswer(s"""select Lower(imei),upper(imei)  a  from VMALL_DICTIONARY_INCLUDE order by imei""",
    s"""select Lower(imei),upper(imei)  a  from VMALL_DICTIONARY_INCLUDE_hive order by imei""")
}
       

//VMALL_DICTIONARY_INCLUDE_033
test("VMALL_DICTIONARY_INCLUDE_033", Include) {
  sql(s"""select imei as a from VMALL_DICTIONARY_INCLUDE  order by a asc limit 10""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_034
test("VMALL_DICTIONARY_INCLUDE_034", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_INCLUDE where  (contractNumber == 9223372047700) and (imei=='1AA100012')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_035
test("VMALL_DICTIONARY_INCLUDE_035", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei !='8imei' order by imei""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_036
test("VMALL_DICTIONARY_INCLUDE_036", Include) {
  sql(s"""select imei  from VMALL_DICTIONARY_INCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_037
test("VMALL_DICTIONARY_INCLUDE_037", Include) {
  sql(s"""Select count(contractNumber) from VMALL_DICTIONARY_INCLUDE""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_038
test("VMALL_DICTIONARY_INCLUDE_038", Include) {
  sql(s"""select count(DISTINCT contractNumber) as a from VMALL_DICTIONARY_INCLUDE""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_039
test("VMALL_DICTIONARY_INCLUDE_039", Include) {
  sql(s"""select sum(contractNumber)+10 as a ,contractNumber  from VMALL_DICTIONARY_INCLUDE group by contractNumber""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_040
test("VMALL_DICTIONARY_INCLUDE_040", Include) {
  sql(s"""select max(contractNumber),min(contractNumber) from VMALL_DICTIONARY_INCLUDE""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_041
test("VMALL_DICTIONARY_INCLUDE_041", Include) {
  sql(s"""select sum(contractNumber) a  from VMALL_DICTIONARY_INCLUDE""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_042
test("VMALL_DICTIONARY_INCLUDE_042", Include) {
  sql(s"""select avg(contractNumber) a  from VMALL_DICTIONARY_INCLUDE""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_043
test("VMALL_DICTIONARY_INCLUDE_043", Include) {
  sql(s"""select min(contractNumber) a  from VMALL_DICTIONARY_INCLUDE""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_044
test("VMALL_DICTIONARY_INCLUDE_044", Include) {
  sql(s"""select variance(contractNumber) as a   from (select contractNumber from VMALL_DICTIONARY_INCLUDE order by contractNumber) t""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_045
test("VMALL_DICTIONARY_INCLUDE_045", Include) {
  sql(s"""select var_pop(contractNumber)  as a from VMALL_DICTIONARY_INCLUDE""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_046
test("VMALL_DICTIONARY_INCLUDE_046", Include) {
  sql(s"""select var_samp(contractNumber) as a  from VMALL_DICTIONARY_INCLUDE""").collect
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
  sql(s"""select corr(contractNumber,contractNumber)  as a from VMALL_DICTIONARY_INCLUDE""").collect
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
  checkAnswer(s"""select percentile_approx(contractNumber,array(0.2,0.3,0.99),5) as a from (select contractNumber from VMALL_DICTIONARY_INCLUDE order by contractNumber) t""",
    s"""select percentile_approx(contractNumber,array(0.2,0.3,0.99),5) as a from (select contractNumber from VMALL_DICTIONARY_INCLUDE_hive order by contractNumber) t""")
}
       

//VMALL_DICTIONARY_INCLUDE_056
test("VMALL_DICTIONARY_INCLUDE_056", Include) {
  checkAnswer(s"""select histogram_numeric(contractNumber,2)  as a from (select contractNumber from VMALL_DICTIONARY_INCLUDE order by contractNumber) t""",
    s"""select histogram_numeric(contractNumber,2)  as a from (select contractNumber from VMALL_DICTIONARY_INCLUDE_hive order by contractNumber) t""")
}
       

//VMALL_DICTIONARY_INCLUDE_057
test("VMALL_DICTIONARY_INCLUDE_057", Include) {
  checkAnswer(s"""select contractNumber+ 10 as a  from VMALL_DICTIONARY_INCLUDE order by a""",
    s"""select contractNumber+ 10 as a  from VMALL_DICTIONARY_INCLUDE_hive order by a""")
}
       

//VMALL_DICTIONARY_INCLUDE_058
test("VMALL_DICTIONARY_INCLUDE_058", Include) {
  checkAnswer(s"""select min(contractNumber), max(contractNumber+ 10) Total from VMALL_DICTIONARY_INCLUDE group by  channelsId order by Total""",
    s"""select min(contractNumber), max(contractNumber+ 10) Total from VMALL_DICTIONARY_INCLUDE_hive group by  channelsId order by Total""")
}
       

//VMALL_DICTIONARY_INCLUDE_059
test("VMALL_DICTIONARY_INCLUDE_059", Include) {
  sql(s"""select last(contractNumber) a from VMALL_DICTIONARY_INCLUDE  order by a""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_060
test("VMALL_DICTIONARY_INCLUDE_060", Include) {
  sql(s"""select FIRST(contractNumber) a from VMALL_DICTIONARY_INCLUDE order by a""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_061
test("VMALL_DICTIONARY_INCLUDE_061", Include) {
  checkAnswer(s"""select contractNumber,count(contractNumber) a from VMALL_DICTIONARY_INCLUDE group by contractNumber order by contractNumber""",
    s"""select contractNumber,count(contractNumber) a from VMALL_DICTIONARY_INCLUDE_hive group by contractNumber order by contractNumber""")
}
       

//VMALL_DICTIONARY_INCLUDE_062
test("VMALL_DICTIONARY_INCLUDE_062", Include) {
  checkAnswer(s"""select Lower(contractNumber) a  from VMALL_DICTIONARY_INCLUDE order by contractNumber""",
    s"""select Lower(contractNumber) a  from VMALL_DICTIONARY_INCLUDE_hive order by contractNumber""")
}
       

//VMALL_DICTIONARY_INCLUDE_063
test("VMALL_DICTIONARY_INCLUDE_063", Include) {
  sql(s"""select distinct contractNumber from VMALL_DICTIONARY_INCLUDE order by contractNumber""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_064
test("VMALL_DICTIONARY_INCLUDE_064", Include) {
  sql(s"""select contractNumber from VMALL_DICTIONARY_INCLUDE order by contractNumber limit 101""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_065
test("VMALL_DICTIONARY_INCLUDE_065", Include) {
  sql(s"""select contractNumber as a from VMALL_DICTIONARY_INCLUDE  order by a asc limit 10""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_066
test("VMALL_DICTIONARY_INCLUDE_066", Include) {
  sql(s"""select contractNumber from VMALL_DICTIONARY_INCLUDE where  (contractNumber == 9223372047700) and (imei=='1AA100012')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_067
test("VMALL_DICTIONARY_INCLUDE_067", Include) {
  sql(s"""select contractNumber from VMALL_DICTIONARY_INCLUDE where contractNumber !=9223372047700 order by contractNumber""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_068
test("VMALL_DICTIONARY_INCLUDE_068", Include) {
  sql(s"""select contractNumber  from VMALL_DICTIONARY_INCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color') order by contractNumber""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_069
test("VMALL_DICTIONARY_INCLUDE_069", Include) {
  sql(s"""select contractNumber from VMALL_DICTIONARY_INCLUDE where contractNumber !=9223372047700 order by contractNumber""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_070
test("VMALL_DICTIONARY_INCLUDE_070", Include) {
  checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_INCLUDE where contractNumber >9223372047700 order by contractNumber""",
    s"""select contractNumber from VMALL_DICTIONARY_INCLUDE_hive where contractNumber >9223372047700 order by contractNumber""")
}
       

//VMALL_DICTIONARY_INCLUDE_071
test("VMALL_DICTIONARY_INCLUDE_071", Include) {
  checkAnswer(s"""select contractNumber  from VMALL_DICTIONARY_INCLUDE where contractNumber<>contractNumber""",
    s"""select contractNumber  from VMALL_DICTIONARY_INCLUDE_hive where contractNumber<>contractNumber""")
}
       

//VMALL_DICTIONARY_INCLUDE_072
test("VMALL_DICTIONARY_INCLUDE_072", Include) {
  checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_INCLUDE where contractNumber != Latest_areaId order by contractNumber""",
    s"""select contractNumber from VMALL_DICTIONARY_INCLUDE_hive where contractNumber != Latest_areaId order by contractNumber""")
}
       

//VMALL_DICTIONARY_INCLUDE_073
test("VMALL_DICTIONARY_INCLUDE_073", Include) {
  checkAnswer(s"""select contractNumber, contractNumber from VMALL_DICTIONARY_INCLUDE where Latest_areaId<contractNumber order by contractNumber""",
    s"""select contractNumber, contractNumber from VMALL_DICTIONARY_INCLUDE_hive where Latest_areaId<contractNumber order by contractNumber""")
}
       

//VMALL_DICTIONARY_INCLUDE_074
test("VMALL_DICTIONARY_INCLUDE_074", Include) {
  checkAnswer(s"""select contractNumber, contractNumber from VMALL_DICTIONARY_INCLUDE where Latest_DAY<=contractNumber order by contractNumber""",
    s"""select contractNumber, contractNumber from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY<=contractNumber order by contractNumber""")
}
       

//VMALL_DICTIONARY_INCLUDE_075
test("VMALL_DICTIONARY_INCLUDE_075", Include) {
  checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_INCLUDE where contractNumber <1000 order by contractNumber""",
    s"""select contractNumber from VMALL_DICTIONARY_INCLUDE_hive where contractNumber <1000 order by contractNumber""")
}
       

//VMALL_DICTIONARY_INCLUDE_076
test("VMALL_DICTIONARY_INCLUDE_076", Include) {
  checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_INCLUDE where contractNumber >1000 order by contractNumber""",
    s"""select contractNumber from VMALL_DICTIONARY_INCLUDE_hive where contractNumber >1000 order by contractNumber""")
}
       

//VMALL_DICTIONARY_INCLUDE_077
test("VMALL_DICTIONARY_INCLUDE_077", Include) {
  checkAnswer(s"""select contractNumber  from VMALL_DICTIONARY_INCLUDE where contractNumber IS NULL order by contractNumber""",
    s"""select contractNumber  from VMALL_DICTIONARY_INCLUDE_hive where contractNumber IS NULL order by contractNumber""")
}
       

//VMALL_DICTIONARY_INCLUDE_078
test("VMALL_DICTIONARY_INCLUDE_078", Include) {
  checkAnswer(s"""select contractNumber  from VMALL_DICTIONARY_INCLUDE where Latest_DAY IS NOT NULL order by contractNumber""",
    s"""select contractNumber  from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY IS NOT NULL order by contractNumber""")
}
       

//VMALL_DICTIONARY_INCLUDE_079
test("VMALL_DICTIONARY_INCLUDE_079", Include) {
  checkAnswer(s"""Select count(Latest_DAY) from VMALL_DICTIONARY_INCLUDE""",
    s"""Select count(Latest_DAY) from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_080
test("VMALL_DICTIONARY_INCLUDE_080", Include) {
  checkAnswer(s"""select count(DISTINCT Latest_DAY) as a from VMALL_DICTIONARY_INCLUDE""",
    s"""select count(DISTINCT Latest_DAY) as a from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_081
test("VMALL_DICTIONARY_INCLUDE_081", Include) {
  checkAnswer(s"""select sum(Latest_DAY)+10 as a ,Latest_DAY  from VMALL_DICTIONARY_INCLUDE group by Latest_DAY order by a""",
    s"""select sum(Latest_DAY)+10 as a ,Latest_DAY  from VMALL_DICTIONARY_INCLUDE_hive group by Latest_DAY order by a""")
}
       

//VMALL_DICTIONARY_INCLUDE_082
test("VMALL_DICTIONARY_INCLUDE_082", Include) {
  checkAnswer(s"""select max(Latest_DAY),min(Latest_DAY) from VMALL_DICTIONARY_INCLUDE""",
    s"""select max(Latest_DAY),min(Latest_DAY) from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_083
test("VMALL_DICTIONARY_INCLUDE_083", Include) {
  checkAnswer(s"""select sum(Latest_DAY) a  from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(Latest_DAY) a  from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_084
test("VMALL_DICTIONARY_INCLUDE_084", Include) {
  sql(s"""select avg(Latest_DAY) a  from VMALL_DICTIONARY_INCLUDE""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_085
test("VMALL_DICTIONARY_INCLUDE_085", Include) {
  checkAnswer(s"""select min(Latest_DAY) a  from VMALL_DICTIONARY_INCLUDE""",
    s"""select min(Latest_DAY) a  from VMALL_DICTIONARY_INCLUDE_hive""")
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
    s"""select corr(Latest_DAY,Latest_DAY)  as a from VMALL_DICTIONARY_INCLUDE_hive""")
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
    s"""select Latest_DAY, Latest_DAY+ 10 as a  from VMALL_DICTIONARY_INCLUDE_hive order by a""")
}
       

//VMALL_DICTIONARY_INCLUDE_100
test("VMALL_DICTIONARY_INCLUDE_100", Include) {
  checkAnswer(s"""select min(Latest_DAY), max(Latest_DAY+ 10) Total from VMALL_DICTIONARY_INCLUDE group by  channelsId order by Total""",
    s"""select min(Latest_DAY), max(Latest_DAY+ 10) Total from VMALL_DICTIONARY_INCLUDE_hive group by  channelsId order by Total""")
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
    s"""select Latest_DAY,count(Latest_DAY) a from VMALL_DICTIONARY_INCLUDE_hive group by Latest_DAY order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_INCLUDE_104
test("VMALL_DICTIONARY_INCLUDE_104", Include) {
  checkAnswer(s"""select Lower(Latest_DAY) a  from VMALL_DICTIONARY_INCLUDE order by a""",
    s"""select Lower(Latest_DAY) a  from VMALL_DICTIONARY_INCLUDE_hive order by a""")
}
       

//VMALL_DICTIONARY_INCLUDE_105
test("VMALL_DICTIONARY_INCLUDE_105", Include) {
  checkAnswer(s"""select distinct Latest_DAY from VMALL_DICTIONARY_INCLUDE order by Latest_DAY""",
    s"""select distinct Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_INCLUDE_106
test("VMALL_DICTIONARY_INCLUDE_106", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE order by Latest_DAY limit 101""",
    s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive order by Latest_DAY limit 101""")
}
       

//VMALL_DICTIONARY_INCLUDE_107
test("VMALL_DICTIONARY_INCLUDE_107", Include) {
  checkAnswer(s"""select Latest_DAY as a from VMALL_DICTIONARY_INCLUDE  order by a asc limit 10""",
    s"""select Latest_DAY as a from VMALL_DICTIONARY_INCLUDE_hive  order by a asc limit 10""")
}
       

//VMALL_DICTIONARY_INCLUDE_108
test("VMALL_DICTIONARY_INCLUDE_108", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where  (Latest_DAY == 1234567890123450.0000000000)  and (imei=='1AA1')""",
    s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where  (Latest_DAY == 1234567890123450.0000000000)  and (imei=='1AA1')""")
}
       

//VMALL_DICTIONARY_INCLUDE_109
test("VMALL_DICTIONARY_INCLUDE_109", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""",
    s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_INCLUDE_110
test("VMALL_DICTIONARY_INCLUDE_110", Include) {
  checkAnswer(s"""select Latest_DAY  from VMALL_DICTIONARY_INCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
    s"""select Latest_DAY  from VMALL_DICTIONARY_INCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""")
}
       

//VMALL_DICTIONARY_INCLUDE_111
test("VMALL_DICTIONARY_INCLUDE_111", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""",
    s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_INCLUDE_112
test("VMALL_DICTIONARY_INCLUDE_112", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY >1234567890123450.0000000000  order by Latest_DAY""",
    s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY >1234567890123450.0000000000  order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_INCLUDE_113
test("VMALL_DICTIONARY_INCLUDE_113", Include) {
  checkAnswer(s"""select Latest_DAY  from VMALL_DICTIONARY_INCLUDE where Latest_DAY<>Latest_DAY""",
    s"""select Latest_DAY  from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY<>Latest_DAY""")
}
       

//VMALL_DICTIONARY_INCLUDE_114
test("VMALL_DICTIONARY_INCLUDE_114", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY != Latest_areaId order by Latest_DAY""",
    s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY != Latest_areaId order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_INCLUDE_115
test("VMALL_DICTIONARY_INCLUDE_115", Include) {
  checkAnswer(s"""select Latest_DAY, Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_areaId<Latest_DAY order by Latest_DAY""",
    s"""select Latest_DAY, Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_areaId<Latest_DAY order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_INCLUDE_116
test("VMALL_DICTIONARY_INCLUDE_116", Include) {
  checkAnswer(s"""select Latest_DAY, Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY<=Latest_DAY  order by Latest_DAY""",
    s"""select Latest_DAY, Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY<=Latest_DAY  order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_INCLUDE_117
test("VMALL_DICTIONARY_INCLUDE_117", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY <1000  order by Latest_DAY""",
    s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY <1000  order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_INCLUDE_118
test("VMALL_DICTIONARY_INCLUDE_118", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY >1000  order by Latest_DAY""",
    s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY >1000  order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_INCLUDE_119
test("VMALL_DICTIONARY_INCLUDE_119", Include) {
  checkAnswer(s"""select Latest_DAY  from VMALL_DICTIONARY_INCLUDE where Latest_DAY IS NULL  order by Latest_DAY""",
    s"""select Latest_DAY  from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY IS NULL  order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_INCLUDE_120
test("VMALL_DICTIONARY_INCLUDE_120", Include) {
  checkAnswer(s"""select Latest_DAY  from VMALL_DICTIONARY_INCLUDE where Latest_DAY IS NOT NULL  order by Latest_DAY""",
    s"""select Latest_DAY  from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY IS NOT NULL  order by Latest_DAY""")
}
       

//VMALL_DICTIONARY_INCLUDE_121
test("VMALL_DICTIONARY_INCLUDE_121", Include) {
  checkAnswer(s"""Select count(gamePointId) from VMALL_DICTIONARY_INCLUDE""",
    s"""Select count(gamePointId) from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_122
test("VMALL_DICTIONARY_INCLUDE_122", Include) {
  checkAnswer(s"""select count(DISTINCT gamePointId) as a from VMALL_DICTIONARY_INCLUDE""",
    s"""select count(DISTINCT gamePointId) as a from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_123
test("VMALL_DICTIONARY_INCLUDE_123", Include) {
  checkAnswer(s"""select sum(gamePointId)+10 as a ,gamePointId  from VMALL_DICTIONARY_INCLUDE group by gamePointId order by a""",
    s"""select sum(gamePointId)+10 as a ,gamePointId  from VMALL_DICTIONARY_INCLUDE_hive group by gamePointId order by a""")
}
       

//VMALL_DICTIONARY_INCLUDE_124
test("VMALL_DICTIONARY_INCLUDE_124", Include) {
  checkAnswer(s"""select max(gamePointId),min(gamePointId) from VMALL_DICTIONARY_INCLUDE""",
    s"""select max(gamePointId),min(gamePointId) from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_125
test("VMALL_DICTIONARY_INCLUDE_125", Include) {
  checkAnswer(s"""select sum(gamePointId) a  from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(gamePointId) a  from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_126
test("VMALL_DICTIONARY_INCLUDE_126", Include) {
  checkAnswer(s"""select avg(gamePointId) a  from VMALL_DICTIONARY_INCLUDE""",
    s"""select avg(gamePointId) a  from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_127
test("VMALL_DICTIONARY_INCLUDE_127", Include) {
  checkAnswer(s"""select min(gamePointId) a  from VMALL_DICTIONARY_INCLUDE""",
    s"""select min(gamePointId) a  from VMALL_DICTIONARY_INCLUDE_hive""")
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
    s"""select corr(gamePointId,gamePointId)  as a from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_136
test("VMALL_DICTIONARY_INCLUDE_136", Include) {
  checkAnswer(s"""select percentile_approx(gamePointId,0.2) as a  from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""",
    s"""select percentile_approx(gamePointId,0.2) as a  from (select gamePointId from VMALL_DICTIONARY_INCLUDE_hive order by gamePointId) t""")
}
       

//VMALL_DICTIONARY_INCLUDE_137
test("VMALL_DICTIONARY_INCLUDE_137", Include) {
  checkAnswer(s"""select percentile_approx(gamePointId,0.2,5) as a  from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""",
    s"""select percentile_approx(gamePointId,0.2,5) as a  from (select gamePointId from VMALL_DICTIONARY_INCLUDE_hive order by gamePointId) t""")
}
       

//VMALL_DICTIONARY_INCLUDE_138
test("VMALL_DICTIONARY_INCLUDE_138", Include) {
  checkAnswer(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99))  as a from (select gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId) t""",
    s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99))  as a from (select gamePointId from VMALL_DICTIONARY_INCLUDE_hive order by gamePointId) t""")
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
    s"""select gamePointId, gamePointId+ 10 as a  from VMALL_DICTIONARY_INCLUDE_hive order by a""")
}
       

//VMALL_DICTIONARY_INCLUDE_142
test("VMALL_DICTIONARY_INCLUDE_142", Include) {
  checkAnswer(s"""select min(gamePointId), max(gamePointId+ 10) Total from VMALL_DICTIONARY_INCLUDE group by  channelsId order by Total""",
    s"""select min(gamePointId), max(gamePointId+ 10) Total from VMALL_DICTIONARY_INCLUDE_hive group by  channelsId order by Total""")
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
    s"""select gamePointId,count(gamePointId) a from VMALL_DICTIONARY_INCLUDE_hive group by gamePointId order by gamePointId""")
}
       

//VMALL_DICTIONARY_INCLUDE_146
test("VMALL_DICTIONARY_INCLUDE_146", Include) {
  checkAnswer(s"""select Lower(gamePointId) a  from VMALL_DICTIONARY_INCLUDE order by gamePointId""",
    s"""select Lower(gamePointId) a  from VMALL_DICTIONARY_INCLUDE_hive order by gamePointId""")
}
       

//VMALL_DICTIONARY_INCLUDE_147
test("VMALL_DICTIONARY_INCLUDE_147", Include) {
  checkAnswer(s"""select distinct gamePointId from VMALL_DICTIONARY_INCLUDE order by gamePointId""",
    s"""select distinct gamePointId from VMALL_DICTIONARY_INCLUDE_hive order by gamePointId""")
}
       

//VMALL_DICTIONARY_INCLUDE_148
test("VMALL_DICTIONARY_INCLUDE_148", Include) {
  checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_INCLUDE  order by gamePointId limit 101""",
    s"""select gamePointId from VMALL_DICTIONARY_INCLUDE_hive  order by gamePointId limit 101""")
}
       

//VMALL_DICTIONARY_INCLUDE_149
test("VMALL_DICTIONARY_INCLUDE_149", Include) {
  checkAnswer(s"""select gamePointId as a from VMALL_DICTIONARY_INCLUDE  order by a asc limit 10""",
    s"""select gamePointId as a from VMALL_DICTIONARY_INCLUDE_hive  order by a asc limit 10""")
}
       

//VMALL_DICTIONARY_INCLUDE_150
test("VMALL_DICTIONARY_INCLUDE_150", Include) {
  checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_INCLUDE where  (gamePointId == 4.70133553923674E43) and (imei=='1AA100084')""",
    s"""select gamePointId from VMALL_DICTIONARY_INCLUDE_hive where  (gamePointId == 4.70133553923674E43) and (imei=='1AA100084')""")
}
       

//VMALL_DICTIONARY_INCLUDE_151
test("VMALL_DICTIONARY_INCLUDE_151", Include) {
  checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_INCLUDE where gamePointId !=4.70133553923674E43  order by gamePointId""",
    s"""select gamePointId from VMALL_DICTIONARY_INCLUDE_hive where gamePointId !=4.70133553923674E43  order by gamePointId""")
}
       

//VMALL_DICTIONARY_INCLUDE_152
test("VMALL_DICTIONARY_INCLUDE_152", Include) {
  checkAnswer(s"""select gamePointId  from VMALL_DICTIONARY_INCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
    s"""select gamePointId  from VMALL_DICTIONARY_INCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""")
}
       

//VMALL_DICTIONARY_INCLUDE_153
test("VMALL_DICTIONARY_INCLUDE_153", Include) {
  checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_INCLUDE where gamePointId !=4.70133553923674E43""",
    s"""select gamePointId from VMALL_DICTIONARY_INCLUDE_hive where gamePointId !=4.70133553923674E43""")
}
       

//VMALL_DICTIONARY_INCLUDE_154
test("VMALL_DICTIONARY_INCLUDE_154", Include) {
  checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_INCLUDE where gamePointId >4.70133553923674E43""",
    s"""select gamePointId from VMALL_DICTIONARY_INCLUDE_hive where gamePointId >4.70133553923674E43""")
}
       

//VMALL_DICTIONARY_INCLUDE_155
test("VMALL_DICTIONARY_INCLUDE_155", Include) {
  checkAnswer(s"""select gamePointId  from VMALL_DICTIONARY_INCLUDE where gamePointId<>gamePointId""",
    s"""select gamePointId  from VMALL_DICTIONARY_INCLUDE_hive where gamePointId<>gamePointId""")
}
       

//VMALL_DICTIONARY_INCLUDE_156
test("VMALL_DICTIONARY_INCLUDE_156", Include) {
  checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_INCLUDE where gamePointId != Latest_areaId  order by gamePointId""",
    s"""select gamePointId from VMALL_DICTIONARY_INCLUDE_hive where gamePointId != Latest_areaId  order by gamePointId""")
}
       

//VMALL_DICTIONARY_INCLUDE_157
test("VMALL_DICTIONARY_INCLUDE_157", Include) {
  checkAnswer(s"""select gamePointId, gamePointId from VMALL_DICTIONARY_INCLUDE where Latest_areaId<gamePointId  order by gamePointId""",
    s"""select gamePointId, gamePointId from VMALL_DICTIONARY_INCLUDE_hive where Latest_areaId<gamePointId  order by gamePointId""")
}
       

//VMALL_DICTIONARY_INCLUDE_158
test("VMALL_DICTIONARY_INCLUDE_158", Include) {
  checkAnswer(s"""select gamePointId, gamePointId from VMALL_DICTIONARY_INCLUDE where gamePointId<=gamePointId  order by gamePointId""",
    s"""select gamePointId, gamePointId from VMALL_DICTIONARY_INCLUDE_hive where gamePointId<=gamePointId  order by gamePointId""")
}
       

//VMALL_DICTIONARY_INCLUDE_159
test("VMALL_DICTIONARY_INCLUDE_159", Include) {
  checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_INCLUDE where gamePointId <1000 order by gamePointId""",
    s"""select gamePointId from VMALL_DICTIONARY_INCLUDE_hive where gamePointId <1000 order by gamePointId""")
}
       

//VMALL_DICTIONARY_INCLUDE_160
test("VMALL_DICTIONARY_INCLUDE_160", Include) {
  checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_INCLUDE where gamePointId >1000 order by gamePointId""",
    s"""select gamePointId from VMALL_DICTIONARY_INCLUDE_hive where gamePointId >1000 order by gamePointId""")
}
       

//VMALL_DICTIONARY_INCLUDE_161
test("VMALL_DICTIONARY_INCLUDE_161", Include) {
  checkAnswer(s"""select gamePointId  from VMALL_DICTIONARY_INCLUDE where gamePointId IS NULL order by gamePointId""",
    s"""select gamePointId  from VMALL_DICTIONARY_INCLUDE_hive where gamePointId IS NULL order by gamePointId""")
}
       

//VMALL_DICTIONARY_INCLUDE_162
test("VMALL_DICTIONARY_INCLUDE_162", Include) {
  checkAnswer(s"""select gamePointId  from VMALL_DICTIONARY_INCLUDE where gamePointId IS NOT NULL order by gamePointId""",
    s"""select gamePointId  from VMALL_DICTIONARY_INCLUDE_hive where gamePointId IS NOT NULL order by gamePointId""")
}
       

//VMALL_DICTIONARY_INCLUDE_163
test("VMALL_DICTIONARY_INCLUDE_163", Include) {
  checkAnswer(s"""Select count(productionDate) from VMALL_DICTIONARY_INCLUDE""",
    s"""Select count(productionDate) from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_164
test("VMALL_DICTIONARY_INCLUDE_164", Include) {
  checkAnswer(s"""select count(DISTINCT productionDate) as a from VMALL_DICTIONARY_INCLUDE""",
    s"""select count(DISTINCT productionDate) as a from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_165
test("VMALL_DICTIONARY_INCLUDE_165", Include) {
  checkAnswer(s"""select sum(productionDate)+10 as a ,productionDate  from VMALL_DICTIONARY_INCLUDE group by productionDate order by productionDate""",
    s"""select sum(productionDate)+10 as a ,productionDate  from VMALL_DICTIONARY_INCLUDE_hive group by productionDate order by productionDate""")
}
       

//VMALL_DICTIONARY_INCLUDE_166
test("VMALL_DICTIONARY_INCLUDE_166", Include) {
  checkAnswer(s"""select max(productionDate),min(productionDate) from VMALL_DICTIONARY_INCLUDE""",
    s"""select max(productionDate),min(productionDate) from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_167
test("VMALL_DICTIONARY_INCLUDE_167", Include) {
  checkAnswer(s"""select sum(productionDate) a  from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(productionDate) a  from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_168
test("VMALL_DICTIONARY_INCLUDE_168", Include) {
  checkAnswer(s"""select avg(productionDate) a  from VMALL_DICTIONARY_INCLUDE""",
    s"""select avg(productionDate) a  from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_169
test("VMALL_DICTIONARY_INCLUDE_169", Include) {
  checkAnswer(s"""select min(productionDate) a  from VMALL_DICTIONARY_INCLUDE""",
    s"""select min(productionDate) a  from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_183
test("VMALL_DICTIONARY_INCLUDE_183", Include) {
  sql(s"""select last(productionDate) a from VMALL_DICTIONARY_INCLUDE order by a""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_184
test("VMALL_DICTIONARY_INCLUDE_184", Include) {
  sql(s"""select FIRST(productionDate) a from VMALL_DICTIONARY_INCLUDE  order by a""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_185
test("VMALL_DICTIONARY_INCLUDE_185", Include) {
  checkAnswer(s"""select productionDate,count(productionDate) a from VMALL_DICTIONARY_INCLUDE group by productionDate order by productionDate""",
    s"""select productionDate,count(productionDate) a from VMALL_DICTIONARY_INCLUDE_hive group by productionDate order by productionDate""")
}
       

//VMALL_DICTIONARY_INCLUDE_186
test("VMALL_DICTIONARY_INCLUDE_186", Include) {
  checkAnswer(s"""select Lower(productionDate) a  from VMALL_DICTIONARY_INCLUDE order by productionDate""",
    s"""select Lower(productionDate) a  from VMALL_DICTIONARY_INCLUDE_hive order by productionDate""")
}
       

//VMALL_DICTIONARY_INCLUDE_187
test("VMALL_DICTIONARY_INCLUDE_187", Include) {
  checkAnswer(s"""select distinct productionDate from VMALL_DICTIONARY_INCLUDE order by productionDate""",
    s"""select distinct productionDate from VMALL_DICTIONARY_INCLUDE_hive order by productionDate""")
}
       

//VMALL_DICTIONARY_INCLUDE_188
test("VMALL_DICTIONARY_INCLUDE_188", Include) {
  checkAnswer(s"""select productionDate from VMALL_DICTIONARY_INCLUDE order by productionDate limit 101""",
    s"""select productionDate from VMALL_DICTIONARY_INCLUDE_hive order by productionDate limit 101""")
}
       

//VMALL_DICTIONARY_INCLUDE_189
test("VMALL_DICTIONARY_INCLUDE_189", Include) {
  checkAnswer(s"""select productionDate as a from VMALL_DICTIONARY_INCLUDE  order by a asc limit 10""",
    s"""select productionDate as a from VMALL_DICTIONARY_INCLUDE_hive  order by a asc limit 10""")
}
       

//VMALL_DICTIONARY_INCLUDE_190
test("VMALL_DICTIONARY_INCLUDE_190", Include) {
  checkAnswer(s"""select productionDate from VMALL_DICTIONARY_INCLUDE where  (productionDate == '2015-07-01 12:07:28.0') and (productionDate=='2015-07-01 12:07:28.0')""",
    s"""select productionDate from VMALL_DICTIONARY_INCLUDE_hive where  (productionDate == '2015-07-01 12:07:28.0') and (productionDate=='2015-07-01 12:07:28.0')""")
}
       

//VMALL_DICTIONARY_INCLUDE_191
test("VMALL_DICTIONARY_INCLUDE_191", Include) {
  checkAnswer(s"""select productionDate from VMALL_DICTIONARY_INCLUDE where productionDate !='2015-07-01 12:07:28.0' order by productionDate""",
    s"""select productionDate from VMALL_DICTIONARY_INCLUDE_hive where productionDate !='2015-07-01 12:07:28.0' order by productionDate""")
}
       

//VMALL_DICTIONARY_INCLUDE_192
test("VMALL_DICTIONARY_INCLUDE_192", Include) {
  checkAnswer(s"""select productionDate  from VMALL_DICTIONARY_INCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
    s"""select productionDate  from VMALL_DICTIONARY_INCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""")
}
       

//VMALL_DICTIONARY_INCLUDE_193
test("VMALL_DICTIONARY_INCLUDE_193", Include) {
  checkAnswer(s"""select productionDate from VMALL_DICTIONARY_INCLUDE where productionDate !='2015-07-01 12:07:28.0' order by productionDate""",
    s"""select productionDate from VMALL_DICTIONARY_INCLUDE_hive where productionDate !='2015-07-01 12:07:28.0' order by productionDate""")
}
       

//VMALL_DICTIONARY_INCLUDE_194
test("VMALL_DICTIONARY_INCLUDE_194", Include) {
  checkAnswer(s"""select productionDate from VMALL_DICTIONARY_INCLUDE where productionDate >'2015-07-01 12:07:28.0' order by productionDate""",
    s"""select productionDate from VMALL_DICTIONARY_INCLUDE_hive where productionDate >'2015-07-01 12:07:28.0' order by productionDate""")
}
       

//VMALL_DICTIONARY_INCLUDE_195
test("VMALL_DICTIONARY_INCLUDE_195", Include) {
  checkAnswer(s"""select productionDate  from VMALL_DICTIONARY_INCLUDE where productionDate<>productionDate order by productionDate""",
    s"""select productionDate  from VMALL_DICTIONARY_INCLUDE_hive where productionDate<>productionDate order by productionDate""")
}
       

//VMALL_DICTIONARY_INCLUDE_196
test("VMALL_DICTIONARY_INCLUDE_196", Include) {
  checkAnswer(s"""select productionDate from VMALL_DICTIONARY_INCLUDE where productionDate != Latest_areaId order by productionDate""",
    s"""select productionDate from VMALL_DICTIONARY_INCLUDE_hive where productionDate != Latest_areaId order by productionDate""")
}
       

//VMALL_DICTIONARY_INCLUDE_197
test("VMALL_DICTIONARY_INCLUDE_197", Include) {
  checkAnswer(s"""select productionDate from VMALL_DICTIONARY_INCLUDE where Latest_areaId<productionDate order by productionDate""",
    s"""select productionDate from VMALL_DICTIONARY_INCLUDE_hive where Latest_areaId<productionDate order by productionDate""")
}
       

//VMALL_DICTIONARY_INCLUDE_198
test("VMALL_DICTIONARY_INCLUDE_198", Include) {
  checkAnswer(s"""select productionDate from VMALL_DICTIONARY_INCLUDE where productionDate<=productionDate order by productionDate""",
    s"""select productionDate from VMALL_DICTIONARY_INCLUDE_hive where productionDate<=productionDate order by productionDate""")
}
       

//VMALL_DICTIONARY_INCLUDE_199
test("VMALL_DICTIONARY_INCLUDE_199", Include) {
  sql(s"""select productionDate from VMALL_DICTIONARY_INCLUDE where productionDate <'2015-07-01 12:07:28.0' order by productionDate""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_200
test("VMALL_DICTIONARY_INCLUDE_200", Include) {
  checkAnswer(s"""select productionDate  from VMALL_DICTIONARY_INCLUDE where productionDate IS NULL""",
    s"""select productionDate  from VMALL_DICTIONARY_INCLUDE_hive where productionDate IS NULL""")
}
       

//VMALL_DICTIONARY_INCLUDE_201
test("VMALL_DICTIONARY_INCLUDE_201", Include) {
  checkAnswer(s"""select productionDate  from VMALL_DICTIONARY_INCLUDE where productionDate IS NOT NULL order by productionDate""",
    s"""select productionDate  from VMALL_DICTIONARY_INCLUDE_hive where productionDate IS NOT NULL order by productionDate""")
}
       

//VMALL_DICTIONARY_INCLUDE_202
test("VMALL_DICTIONARY_INCLUDE_202", Include) {
  checkAnswer(s"""Select count(deviceInformationId) from VMALL_DICTIONARY_INCLUDE""",
    s"""Select count(deviceInformationId) from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_203
test("VMALL_DICTIONARY_INCLUDE_203", Include) {
  checkAnswer(s"""select count(DISTINCT deviceInformationId) as a from VMALL_DICTIONARY_INCLUDE""",
    s"""select count(DISTINCT deviceInformationId) as a from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_204
test("VMALL_DICTIONARY_INCLUDE_204", Include) {
  checkAnswer(s"""select sum(deviceInformationId)+10 as a ,deviceInformationId  from VMALL_DICTIONARY_INCLUDE group by deviceInformationId order by deviceInformationId""",
    s"""select sum(deviceInformationId)+10 as a ,deviceInformationId  from VMALL_DICTIONARY_INCLUDE_hive group by deviceInformationId order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_INCLUDE_205
test("VMALL_DICTIONARY_INCLUDE_205", Include) {
  checkAnswer(s"""select max(deviceInformationId),min(deviceInformationId) from VMALL_DICTIONARY_INCLUDE""",
    s"""select max(deviceInformationId),min(deviceInformationId) from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_206
test("VMALL_DICTIONARY_INCLUDE_206", Include) {
  checkAnswer(s"""select sum(deviceInformationId) a  from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(deviceInformationId) a  from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_207
test("VMALL_DICTIONARY_INCLUDE_207", Include) {
  checkAnswer(s"""select avg(deviceInformationId) a  from VMALL_DICTIONARY_INCLUDE""",
    s"""select avg(deviceInformationId) a  from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_208
test("VMALL_DICTIONARY_INCLUDE_208", Include) {
  checkAnswer(s"""select min(deviceInformationId) a  from VMALL_DICTIONARY_INCLUDE""",
    s"""select min(deviceInformationId) a  from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_209
test("VMALL_DICTIONARY_INCLUDE_209", Include) {
  sql(s"""select variance(deviceInformationId) as a   from (select deviceInformationId from VMALL_DICTIONARY_INCLUDE order by deviceInformationId) t""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_210
test("VMALL_DICTIONARY_INCLUDE_210", Include) {
  sql(s"""select var_pop(deviceInformationId)  as a from VMALL_DICTIONARY_INCLUDE""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_211
test("VMALL_DICTIONARY_INCLUDE_211", Include) {
  sql(s"""select var_samp(deviceInformationId) as a  from VMALL_DICTIONARY_INCLUDE""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_212
test("VMALL_DICTIONARY_INCLUDE_212", Include) {
  sql(s"""select stddev_pop(deviceInformationId) as a  from (select deviceInformationId from VMALL_DICTIONARY_INCLUDE order by deviceInformationId) t""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_213
test("VMALL_DICTIONARY_INCLUDE_213", Include) {
  checkAnswer(s"""select stddev_samp(deviceInformationId)  as a from (select deviceInformationId from VMALL_DICTIONARY_INCLUDE order by deviceInformationId) t""",
    s"""select stddev_samp(deviceInformationId)  as a from (select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive order by deviceInformationId) t""")
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
    s"""select corr(deviceInformationId,deviceInformationId)  as a from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_217
test("VMALL_DICTIONARY_INCLUDE_217", Include) {
  checkAnswer(s"""select percentile_approx(deviceInformationId,0.2) as a  from (select deviceInformationId from VMALL_DICTIONARY_INCLUDE order by deviceInformationId) t""",
    s"""select percentile_approx(deviceInformationId,0.2) as a  from (select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive order by deviceInformationId) t""")
}
       

//VMALL_DICTIONARY_INCLUDE_218
test("VMALL_DICTIONARY_INCLUDE_218", Include) {
  checkAnswer(s"""select percentile_approx(deviceInformationId,0.2,5) as a  from (select deviceInformationId from VMALL_DICTIONARY_INCLUDE order by deviceInformationId) t""",
    s"""select percentile_approx(deviceInformationId,0.2,5) as a  from (select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive order by deviceInformationId) t""")
}
       

//VMALL_DICTIONARY_INCLUDE_219
test("VMALL_DICTIONARY_INCLUDE_219", Include) {
  checkAnswer(s"""select percentile_approx(deviceInformationId,array(0.2,0.3,0.99))  as a from (select deviceInformationId from VMALL_DICTIONARY_INCLUDE order by deviceInformationId) t""",
    s"""select percentile_approx(deviceInformationId,array(0.2,0.3,0.99))  as a from (select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive order by deviceInformationId) t""")
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
    s"""select deviceInformationId, deviceInformationId+ 10 as a  from VMALL_DICTIONARY_INCLUDE_hive order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_INCLUDE_223
test("VMALL_DICTIONARY_INCLUDE_223", Include) {
  checkAnswer(s"""select min(deviceInformationId), max(deviceInformationId+ 10) Total from VMALL_DICTIONARY_INCLUDE group by  channelsId order by Total""",
    s"""select min(deviceInformationId), max(deviceInformationId+ 10) Total from VMALL_DICTIONARY_INCLUDE_hive group by  channelsId order by Total""")
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
    s"""select deviceInformationId,count(deviceInformationId) a from VMALL_DICTIONARY_INCLUDE_hive group by deviceInformationId order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_INCLUDE_227
test("VMALL_DICTIONARY_INCLUDE_227", Include) {
  checkAnswer(s"""select Lower(deviceInformationId) a  from VMALL_DICTIONARY_INCLUDE order by deviceInformationId""",
    s"""select Lower(deviceInformationId) a  from VMALL_DICTIONARY_INCLUDE_hive order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_INCLUDE_228
test("VMALL_DICTIONARY_INCLUDE_228", Include) {
  checkAnswer(s"""select distinct deviceInformationId from VMALL_DICTIONARY_INCLUDE order by deviceInformationId""",
    s"""select distinct deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_INCLUDE_229
test("VMALL_DICTIONARY_INCLUDE_229", Include) {
  checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE order by deviceInformationId limit 101""",
    s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive order by deviceInformationId limit 101""")
}
       

//VMALL_DICTIONARY_INCLUDE_230
test("VMALL_DICTIONARY_INCLUDE_230", Include) {
  checkAnswer(s"""select deviceInformationId as a from VMALL_DICTIONARY_INCLUDE  order by a asc limit 10""",
    s"""select deviceInformationId as a from VMALL_DICTIONARY_INCLUDE_hive  order by a asc limit 10""")
}
       

//VMALL_DICTIONARY_INCLUDE_231
test("VMALL_DICTIONARY_INCLUDE_231", Include) {
  checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE where  (deviceInformationId == 100084) and (deviceInformationId==100084)""",
    s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive where  (deviceInformationId == 100084) and (deviceInformationId==100084)""")
}
       

//VMALL_DICTIONARY_INCLUDE_232
test("VMALL_DICTIONARY_INCLUDE_232", Include) {
  sql(s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE where deviceInformationId !='100084' order by deviceInformationId""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_233
test("VMALL_DICTIONARY_INCLUDE_233", Include) {
  checkAnswer(s"""select deviceInformationId  from VMALL_DICTIONARY_INCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
    s"""select deviceInformationId  from VMALL_DICTIONARY_INCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""")
}
       

//VMALL_DICTIONARY_INCLUDE_234
test("VMALL_DICTIONARY_INCLUDE_234", Include) {
  checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE where deviceInformationId !=100084 order by deviceInformationId""",
    s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId !=100084 order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_INCLUDE_235
test("VMALL_DICTIONARY_INCLUDE_235", Include) {
  checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE where deviceInformationId >100084 order by deviceInformationId""",
    s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId >100084 order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_INCLUDE_236
test("VMALL_DICTIONARY_INCLUDE_236", Include) {
  checkAnswer(s"""select deviceInformationId  from VMALL_DICTIONARY_INCLUDE where deviceInformationId<>deviceInformationId order by deviceInformationId""",
    s"""select deviceInformationId  from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId<>deviceInformationId order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_INCLUDE_237
test("VMALL_DICTIONARY_INCLUDE_237", Include) {
  checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE where deviceInformationId != Latest_areaId order by deviceInformationId""",
    s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId != Latest_areaId order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_INCLUDE_238
test("VMALL_DICTIONARY_INCLUDE_238", Include) {
  checkAnswer(s"""select deviceInformationId, deviceInformationId from VMALL_DICTIONARY_INCLUDE where Latest_areaId<deviceInformationId order by deviceInformationId""",
    s"""select deviceInformationId, deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive where Latest_areaId<deviceInformationId order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_INCLUDE_239
test("VMALL_DICTIONARY_INCLUDE_239", Include) {
  checkAnswer(s"""select deviceInformationId, deviceInformationId from VMALL_DICTIONARY_INCLUDE where deviceInformationId<=deviceInformationId order by deviceInformationId""",
    s"""select deviceInformationId, deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId<=deviceInformationId order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_INCLUDE_240
test("VMALL_DICTIONARY_INCLUDE_240", Include) {
  checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE where deviceInformationId <1000 order by deviceInformationId""",
    s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId <1000 order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_INCLUDE_241
test("VMALL_DICTIONARY_INCLUDE_241", Include) {
  checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE where deviceInformationId >1000 order by deviceInformationId""",
    s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId >1000 order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_INCLUDE_242
test("VMALL_DICTIONARY_INCLUDE_242", Include) {
  checkAnswer(s"""select deviceInformationId  from VMALL_DICTIONARY_INCLUDE where deviceInformationId IS NULL order by deviceInformationId""",
    s"""select deviceInformationId  from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId IS NULL order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_INCLUDE_243
test("VMALL_DICTIONARY_INCLUDE_243", Include) {
  checkAnswer(s"""select deviceInformationId  from VMALL_DICTIONARY_INCLUDE where deviceInformationId IS NOT NULL order by deviceInformationId""",
    s"""select deviceInformationId  from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId IS NOT NULL order by deviceInformationId""")
}
       

//VMALL_DICTIONARY_INCLUDE_244
test("VMALL_DICTIONARY_INCLUDE_244", Include) {
  checkAnswer(s"""select sum(imei)+10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(imei)+10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_245
test("VMALL_DICTIONARY_INCLUDE_245", Include) {
  checkAnswer(s"""select sum(imei)*10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(imei)*10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_246
test("VMALL_DICTIONARY_INCLUDE_246", Include) {
  checkAnswer(s"""select sum(imei)/10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(imei)/10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_247
test("VMALL_DICTIONARY_INCLUDE_247", Include) {
  checkAnswer(s"""select sum(imei)-10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(imei)-10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_248
test("VMALL_DICTIONARY_INCLUDE_248", Include) {
  checkAnswer(s"""select sum(contractNumber)+10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(contractNumber)+10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_249
test("VMALL_DICTIONARY_INCLUDE_249", Include) {
  checkAnswer(s"""select sum(contractNumber)*10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(contractNumber)*10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_250
test("VMALL_DICTIONARY_INCLUDE_250", Include) {
  checkAnswer(s"""select sum(contractNumber)/10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(contractNumber)/10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_251
test("VMALL_DICTIONARY_INCLUDE_251", Include) {
  checkAnswer(s"""select sum(contractNumber)-10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(contractNumber)-10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_252
test("VMALL_DICTIONARY_INCLUDE_252", Include) {
  checkAnswer(s"""select sum(Latest_DAY)+10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(Latest_DAY)+10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_253
test("VMALL_DICTIONARY_INCLUDE_253", Include) {
  checkAnswer(s"""select sum(Latest_DAY)*10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(Latest_DAY)*10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_254
test("VMALL_DICTIONARY_INCLUDE_254", Include) {
  checkAnswer(s"""select sum(Latest_DAY)/10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(Latest_DAY)/10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_255
test("VMALL_DICTIONARY_INCLUDE_255", Include) {
  checkAnswer(s"""select sum(Latest_DAY)-10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(Latest_DAY)-10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_256
test("VMALL_DICTIONARY_INCLUDE_256", Include) {
  checkAnswer(s"""select sum(gamePointId)+10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(gamePointId)+10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_257
test("VMALL_DICTIONARY_INCLUDE_257", Include) {
  checkAnswer(s"""select sum(gamePointId)*10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(gamePointId)*10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_258
test("VMALL_DICTIONARY_INCLUDE_258", Include) {
  checkAnswer(s"""select sum(gamePointId)/10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(gamePointId)/10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_259
test("VMALL_DICTIONARY_INCLUDE_259", Include) {
  checkAnswer(s"""select sum(gamePointId)-10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(gamePointId)-10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_260
test("VMALL_DICTIONARY_INCLUDE_260", Include) {
  checkAnswer(s"""select sum(productionDate)+10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(productionDate)+10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_261
test("VMALL_DICTIONARY_INCLUDE_261", Include) {
  checkAnswer(s"""select sum(productionDate)*10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(productionDate)*10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_262
test("VMALL_DICTIONARY_INCLUDE_262", Include) {
  checkAnswer(s"""select sum(productionDate)/10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(productionDate)/10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_263
test("VMALL_DICTIONARY_INCLUDE_263", Include) {
  checkAnswer(s"""select sum(productionDate)-10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(productionDate)-10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_264
test("VMALL_DICTIONARY_INCLUDE_264", Include) {
  checkAnswer(s"""select sum(deviceInformationId)+10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(deviceInformationId)+10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_265
test("VMALL_DICTIONARY_INCLUDE_265", Include) {
  checkAnswer(s"""select sum(deviceInformationId)*10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(deviceInformationId)*10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_266
test("VMALL_DICTIONARY_INCLUDE_266", Include) {
  checkAnswer(s"""select sum(deviceInformationId)/10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(deviceInformationId)/10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_267
test("VMALL_DICTIONARY_INCLUDE_267", Include) {
  checkAnswer(s"""select sum(deviceInformationId)-10 as a   from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(deviceInformationId)-10 as a   from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_292
test("VMALL_DICTIONARY_INCLUDE_292", Include) {
  checkAnswer(s"""SELECT productiondate from VMALL_DICTIONARY_INCLUDE where productiondate LIKE '2015-09-30%'""",
    s"""SELECT productiondate from VMALL_DICTIONARY_INCLUDE_hive where productiondate LIKE '2015-09-30%'""")
}
       

//VMALL_DICTIONARY_INCLUDE_293
test("VMALL_DICTIONARY_INCLUDE_293", Include) {
  checkAnswer(s"""SELECT productiondate from VMALL_DICTIONARY_INCLUDE where productiondate LIKE '% %'""",
    s"""SELECT productiondate from VMALL_DICTIONARY_INCLUDE_hive where productiondate LIKE '% %'""")
}
       

//VMALL_DICTIONARY_INCLUDE_294
test("VMALL_DICTIONARY_INCLUDE_294", Include) {
  checkAnswer(s"""SELECT productiondate from VMALL_DICTIONARY_INCLUDE where productiondate LIKE '%12:07:28'""",
    s"""SELECT productiondate from VMALL_DICTIONARY_INCLUDE_hive where productiondate LIKE '%12:07:28'""")
}
       

//VMALL_DICTIONARY_INCLUDE_295
test("VMALL_DICTIONARY_INCLUDE_295", Include) {
  checkAnswer(s"""select contractnumber from VMALL_DICTIONARY_INCLUDE where contractnumber like '922337204%' """,
    s"""select contractnumber from VMALL_DICTIONARY_INCLUDE_hive where contractnumber like '922337204%' """)
}
       

//VMALL_DICTIONARY_INCLUDE_296
test("VMALL_DICTIONARY_INCLUDE_296", Include) {
  checkAnswer(s"""select contractnumber from VMALL_DICTIONARY_INCLUDE where contractnumber like '%047800'""",
    s"""select contractnumber from VMALL_DICTIONARY_INCLUDE_hive where contractnumber like '%047800'""")
}
       

//VMALL_DICTIONARY_INCLUDE_297
test("VMALL_DICTIONARY_INCLUDE_297", Include) {
  checkAnswer(s"""select contractnumber from VMALL_DICTIONARY_INCLUDE where contractnumber like '%720%'""",
    s"""select contractnumber from VMALL_DICTIONARY_INCLUDE_hive where contractnumber like '%720%'""")
}
       

//VMALL_DICTIONARY_INCLUDE_298
test("VMALL_DICTIONARY_INCLUDE_298", Include) {
  checkAnswer(s"""SELECT Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY like '12345678%'""",
    s"""SELECT Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY like '12345678%'""")
}
       

//VMALL_DICTIONARY_INCLUDE_299
test("VMALL_DICTIONARY_INCLUDE_299", Include) {
  checkAnswer(s"""SELECT Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY like '%5678%'""",
    s"""SELECT Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY like '%5678%'""")
}
       

//VMALL_DICTIONARY_INCLUDE_300
test("VMALL_DICTIONARY_INCLUDE_300", Include) {
  checkAnswer(s"""SELECT Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY like '1234567%'""",
    s"""SELECT Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY like '1234567%'""")
}
       

//VMALL_DICTIONARY_INCLUDE_301
test("VMALL_DICTIONARY_INCLUDE_301", Include) {
  checkAnswer(s"""SELECT gamepointID from VMALL_DICTIONARY_INCLUDE where gamepointID like '1.1098347722%'""",
    s"""SELECT gamepointID from VMALL_DICTIONARY_INCLUDE_hive where gamepointID like '1.1098347722%'""")
}
       

//VMALL_DICTIONARY_INCLUDE_302
test("VMALL_DICTIONARY_INCLUDE_302", Include) {
  checkAnswer(s"""SELECT gamepointID from VMALL_DICTIONARY_INCLUDE where gamepointID like '%8347722%'""",
    s"""SELECT gamepointID from VMALL_DICTIONARY_INCLUDE_hive where gamepointID like '%8347722%'""")
}
       

//VMALL_DICTIONARY_INCLUDE_303
test("VMALL_DICTIONARY_INCLUDE_303", Include) {
  checkAnswer(s"""SELECT gamepointID from VMALL_DICTIONARY_INCLUDE where gamepointID like '%7512E42'""",
    s"""SELECT gamepointID from VMALL_DICTIONARY_INCLUDE_hive where gamepointID like '%7512E42'""")
}
       

//VMALL_DICTIONARY_INCLUDE_304
test("VMALL_DICTIONARY_INCLUDE_304", Include) {
  checkAnswer(s"""SELECT deviceinformationid from VMALL_DICTIONARY_INCLUDE where deviceinformationid like '1000%'""",
    s"""SELECT deviceinformationid from VMALL_DICTIONARY_INCLUDE_hive where deviceinformationid like '1000%'""")
}
       

//VMALL_DICTIONARY_INCLUDE_305
test("VMALL_DICTIONARY_INCLUDE_305", Include) {
  checkAnswer(s"""SELECT deviceinformationid from VMALL_DICTIONARY_INCLUDE where deviceinformationid like '%00%'""",
    s"""SELECT deviceinformationid from VMALL_DICTIONARY_INCLUDE_hive where deviceinformationid like '%00%'""")
}
       

//VMALL_DICTIONARY_INCLUDE_306
test("VMALL_DICTIONARY_INCLUDE_306", Include) {
  checkAnswer(s"""SELECT deviceinformationid from VMALL_DICTIONARY_INCLUDE where deviceinformationid like '%0084'""",
    s"""SELECT deviceinformationid from VMALL_DICTIONARY_INCLUDE_hive where deviceinformationid like '%0084'""")
}
       

//VMALL_DICTIONARY_INCLUDE_307
test("VMALL_DICTIONARY_INCLUDE_307", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei like '1AA10%'""",
    s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei like '1AA10%'""")
}
       

//VMALL_DICTIONARY_INCLUDE_308
test("VMALL_DICTIONARY_INCLUDE_308", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei like '%A10%'""",
    s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei like '%A10%'""")
}
       

//VMALL_DICTIONARY_INCLUDE_309
test("VMALL_DICTIONARY_INCLUDE_309", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei like '%00084'""",
    s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei like '%00084'""")
}
       

//VMALL_DICTIONARY_INCLUDE_310
test("VMALL_DICTIONARY_INCLUDE_310", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei in ('1AA100074','1AA100075','1AA100077')""",
    s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei in ('1AA100074','1AA100075','1AA100077')""")
}
       

//VMALL_DICTIONARY_INCLUDE_311
test("VMALL_DICTIONARY_INCLUDE_311", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei not in ('1AA100074','1AA100075','1AA100077')""",
    s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei not in ('1AA100074','1AA100075','1AA100077')""")
}
       

//VMALL_DICTIONARY_INCLUDE_312
test("VMALL_DICTIONARY_INCLUDE_312", Include) {
  checkAnswer(s"""select deviceinformationid from VMALL_DICTIONARY_INCLUDE where deviceinformationid in (100081,100078,10008)""",
    s"""select deviceinformationid from VMALL_DICTIONARY_INCLUDE_hive where deviceinformationid in (100081,100078,10008)""")
}
       

//VMALL_DICTIONARY_INCLUDE_313
test("VMALL_DICTIONARY_INCLUDE_313", Include) {
  checkAnswer(s"""select deviceinformationid from VMALL_DICTIONARY_INCLUDE where deviceinformationid not in (100081,100078,10008)""",
    s"""select deviceinformationid from VMALL_DICTIONARY_INCLUDE_hive where deviceinformationid not in (100081,100078,10008)""")
}
       

//VMALL_DICTIONARY_INCLUDE_314
test("VMALL_DICTIONARY_INCLUDE_314", Include) {
  sql(s"""select productiondate from VMALL_DICTIONARY_INCLUDE where productiondate in ('2015-10-04 12:07:28','2015-10-07%','2015-10-07 12:07:28')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_315
test("VMALL_DICTIONARY_INCLUDE_315", Include) {
  sql(s"""select productiondate from VMALL_DICTIONARY_INCLUDE where productiondate not in ('2015-10-04 12:07:28','2015-10-07 12:07:28')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_316
test("VMALL_DICTIONARY_INCLUDE_316", Include) {
  checkAnswer(s"""select gamepointid from VMALL_DICTIONARY_INCLUDE where gamepointid in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""",
    s"""select gamepointid from VMALL_DICTIONARY_INCLUDE_hive where gamepointid in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""")
}
       

//VMALL_DICTIONARY_INCLUDE_317
test("VMALL_DICTIONARY_INCLUDE_317", Include) {
  checkAnswer(s"""select gamepointid from VMALL_DICTIONARY_INCLUDE where gamepointid not in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""",
    s"""select gamepointid from VMALL_DICTIONARY_INCLUDE_hive where gamepointid not in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""")
}
       

//VMALL_DICTIONARY_INCLUDE_318
test("VMALL_DICTIONARY_INCLUDE_318", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY in (1234567890123530.0000000000,1234567890123520.0000000000)""",
    s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY in (1234567890123530.0000000000,1234567890123520.0000000000)""")
}
       

//VMALL_DICTIONARY_INCLUDE_319
test("VMALL_DICTIONARY_INCLUDE_319", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY not in (1234567890123530.0000000000,1234567890123520.0000000000)""",
    s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY not in (1234567890123530.0000000000,1234567890123520.0000000000)""")
}
       

//VMALL_DICTIONARY_INCLUDE_322
test("VMALL_DICTIONARY_INCLUDE_322", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei !='1AA100077'""",
    s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei !='1AA100077'""")
}
       

//VMALL_DICTIONARY_INCLUDE_323
test("VMALL_DICTIONARY_INCLUDE_323", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei NOT LIKE '1AA100077'""",
    s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei NOT LIKE '1AA100077'""")
}
       

//VMALL_DICTIONARY_INCLUDE_324
test("VMALL_DICTIONARY_INCLUDE_324", Include) {
  checkAnswer(s"""select deviceinformationid from VMALL_DICTIONARY_INCLUDE where deviceinformationid !=100078""",
    s"""select deviceinformationid from VMALL_DICTIONARY_INCLUDE_hive where deviceinformationid !=100078""")
}
       

//VMALL_DICTIONARY_INCLUDE_325
test("VMALL_DICTIONARY_INCLUDE_325", Include) {
  checkAnswer(s"""select deviceinformationid from VMALL_DICTIONARY_INCLUDE where deviceinformationid NOT LIKE 100079""",
    s"""select deviceinformationid from VMALL_DICTIONARY_INCLUDE_hive where deviceinformationid NOT LIKE 100079""")
}
       

//VMALL_DICTIONARY_INCLUDE_326
test("VMALL_DICTIONARY_INCLUDE_326", Include) {
  checkAnswer(s"""select productiondate from VMALL_DICTIONARY_INCLUDE where productiondate !='2015-10-07 12:07:28'""",
    s"""select productiondate from VMALL_DICTIONARY_INCLUDE_hive where productiondate !='2015-10-07 12:07:28'""")
}
       

//VMALL_DICTIONARY_INCLUDE_327
test("VMALL_DICTIONARY_INCLUDE_327", Include) {
  sql(s"""select productiondate from VMALL_DICTIONARY_INCLUDE where productiondate NOT LIKE '2015-10-07 12:07:28'""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_328
test("VMALL_DICTIONARY_INCLUDE_328", Include) {
  checkAnswer(s"""select gamepointid from VMALL_DICTIONARY_INCLUDE where gamepointid !=6.8591561117512E42""",
    s"""select gamepointid from VMALL_DICTIONARY_INCLUDE_hive where gamepointid !=6.8591561117512E42""")
}
       

//VMALL_DICTIONARY_INCLUDE_329
test("VMALL_DICTIONARY_INCLUDE_329", Include) {
  checkAnswer(s"""select gamepointid from VMALL_DICTIONARY_INCLUDE where gamepointid NOT LIKE 6.8591561117512E43""",
    s"""select gamepointid from VMALL_DICTIONARY_INCLUDE_hive where gamepointid NOT LIKE 6.8591561117512E43""")
}
       

//VMALL_DICTIONARY_INCLUDE_330
test("VMALL_DICTIONARY_INCLUDE_330", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY != 1234567890123520.0000000000""",
    s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY != 1234567890123520.0000000000""")
}
       

//VMALL_DICTIONARY_INCLUDE_331
test("VMALL_DICTIONARY_INCLUDE_331", Include) {
  sql(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY NOT LIKE 1234567890123520.0000000000""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_335
test("VMALL_DICTIONARY_INCLUDE_335", Include) {
  checkAnswer(s"""SELECT productiondate,IMEI from VMALL_DICTIONARY_INCLUDE where IMEI RLIKE '1AA100077'""",
    s"""SELECT productiondate,IMEI from VMALL_DICTIONARY_INCLUDE_hive where IMEI RLIKE '1AA100077'""")
}
       

//VMALL_DICTIONARY_INCLUDE_336
test("VMALL_DICTIONARY_INCLUDE_336", Include) {
  checkAnswer(s"""SELECT deviceinformationId from VMALL_DICTIONARY_INCLUDE where deviceinformationId RLIKE '100079'""",
    s"""SELECT deviceinformationId from VMALL_DICTIONARY_INCLUDE_hive where deviceinformationId RLIKE '100079'""")
}
       

//VMALL_DICTIONARY_INCLUDE_337
test("VMALL_DICTIONARY_INCLUDE_337", Include) {
  checkAnswer(s"""SELECT gamepointid from VMALL_DICTIONARY_INCLUDE where gamepointid RLIKE '1.61922711065643E42'""",
    s"""SELECT gamepointid from VMALL_DICTIONARY_INCLUDE_hive where gamepointid RLIKE '1.61922711065643E42'""")
}
       

//VMALL_DICTIONARY_INCLUDE_338
test("VMALL_DICTIONARY_INCLUDE_338", Include) {
  checkAnswer(s"""SELECT Latest_Day from VMALL_DICTIONARY_INCLUDE where Latest_Day RLIKE '1234567890123550.0000000000'""",
    s"""SELECT Latest_Day from VMALL_DICTIONARY_INCLUDE_hive where Latest_Day RLIKE '1234567890123550.0000000000'""")
}
       

//VMALL_DICTIONARY_INCLUDE_339
test("VMALL_DICTIONARY_INCLUDE_339", Include) {
  checkAnswer(s"""SELECT contractnumber from VMALL_DICTIONARY_INCLUDE where contractnumber RLIKE '9223372047800'""",
    s"""SELECT contractnumber from VMALL_DICTIONARY_INCLUDE_hive where contractnumber RLIKE '9223372047800'""")
}
       

//VMALL_DICTIONARY_INCLUDE_340
test("VMALL_DICTIONARY_INCLUDE_340", Include) {
  checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE a join vmall_dictionary_include b on a.productiondate=b.productiondate""",
    s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE_hive a join vmall_dictionary_include_hive b on a.productiondate=b.productiondate""")
}
       

//VMALL_DICTIONARY_INCLUDE_341
test("VMALL_DICTIONARY_INCLUDE_341", Include) {
  checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE a join vmall_dictionary_include b on a.deviceinformationid=b.deviceinformationid""",
    s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE_hive a join vmall_dictionary_include_hive b on a.deviceinformationid=b.deviceinformationid""")
}
       

//VMALL_DICTIONARY_INCLUDE_342
test("VMALL_DICTIONARY_INCLUDE_342", Include) {
  checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE a join vmall_dictionary_include b on a.imei=b.imei""",
    s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE_hive a join vmall_dictionary_include_hive b on a.imei=b.imei""")
}
       

//VMALL_DICTIONARY_INCLUDE_343
test("VMALL_DICTIONARY_INCLUDE_343", Include) {
  checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE a join vmall_dictionary_include b on a.gamepointid=b.gamepointid""",
    s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE_hive a join vmall_dictionary_include_hive b on a.gamepointid=b.gamepointid""")
}
       

//VMALL_DICTIONARY_INCLUDE_344
test("VMALL_DICTIONARY_INCLUDE_344", Include) {
  checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE a join vmall_dictionary_include b on a.Latest_Day=b.Latest_Day""",
    s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE_hive a join vmall_dictionary_include_hive b on a.Latest_Day=b.Latest_Day""")
}
       

//VMALL_DICTIONARY_INCLUDE_345
test("VMALL_DICTIONARY_INCLUDE_345", Include) {
  checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE a join vmall_dictionary_include b on a.contractnumber=b.contractnumber""",
    s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_INCLUDE_hive a join vmall_dictionary_include_hive b on a.contractnumber=b.contractnumber""")
}
       

//VMALL_DICTIONARY_INCLUDE_346
test("VMALL_DICTIONARY_INCLUDE_346", Include) {
  checkAnswer(s"""select count( contractNumber ),sum( contractNumber ),count(distinct contractNumber ),avg( contractNumber ),max( contractNumber ),min( contractNumber ),1 from VMALL_DICTIONARY_INCLUDE""",
    s"""select count( contractNumber ),sum( contractNumber ),count(distinct contractNumber ),avg( contractNumber ),max( contractNumber ),min( contractNumber ),1 from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_347
test("VMALL_DICTIONARY_INCLUDE_347", Include) {
  checkAnswer(s"""select count( Latest_Day ),sum( Latest_Day ),count(distinct Latest_Day ),avg( Latest_Day ),max( Latest_Day ),min( Latest_Day ),1 from VMALL_DICTIONARY_INCLUDE""",
    s"""select count( Latest_Day ),sum( Latest_Day ),count(distinct Latest_Day ),avg( Latest_Day ),max( Latest_Day ),min( Latest_Day ),1 from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_348
test("VMALL_DICTIONARY_INCLUDE_348", Include) {
  sql(s"""select count( gamePointId),sum( gamePointId ),count(distinct gamePointId ),avg(gamePointId),max(gamePointId),min(gamePointId),1 from VMALL_DICTIONARY_INCLUDE""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_349
test("VMALL_DICTIONARY_INCLUDE_349", Include) {
  checkAnswer(s"""select count(deviceInformationId),sum(deviceInformationId),count(deviceInformationId),avg(deviceInformationId),max(deviceInformationId),min(deviceInformationId),1 from VMALL_DICTIONARY_INCLUDE""",
    s"""select count(deviceInformationId),sum(deviceInformationId),count(deviceInformationId),avg(deviceInformationId),max(deviceInformationId),min(deviceInformationId),1 from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_350
test("VMALL_DICTIONARY_INCLUDE_350", Include) {
  checkAnswer(s"""select count( productionDate),sum(  productionDate ),count(distinct  productionDate ),avg(  productionDate ),max(  productionDate),min(  productionDate ),1 from VMALL_DICTIONARY_INCLUDE""",
    s"""select count( productionDate),sum(  productionDate ),count(distinct  productionDate ),avg(  productionDate ),max(  productionDate),min(  productionDate ),1 from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_351
test("VMALL_DICTIONARY_INCLUDE_351", Include) {
  checkAnswer(s"""select count(IMEI ),sum(IMEI ),count(distinct IMEI ),avg(IMEI ),max(IMEI ),min(IMEI ),1 from VMALL_DICTIONARY_INCLUDE""",
    s"""select count(IMEI ),sum(IMEI ),count(distinct IMEI ),avg(IMEI ),max(IMEI ),min(IMEI ),1 from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_352
test("VMALL_DICTIONARY_INCLUDE_352", Include) {
  checkAnswer(s"""select sum(contractNumber),count(contractNumber),avg(contractNumber),sum(contractNumber)/count(contractNumber) from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(contractNumber),count(contractNumber),avg(contractNumber),sum(contractNumber)/count(contractNumber) from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_353
test("VMALL_DICTIONARY_INCLUDE_353", Include) {
  checkAnswer(s"""select sum(Latest_Day),count(Latest_Day),avg(Latest_Day),sum(Latest_Day)/count(Latest_Day) from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(Latest_Day),count(Latest_Day),avg(Latest_Day),sum(Latest_Day)/count(Latest_Day) from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_354
test("VMALL_DICTIONARY_INCLUDE_354", Include) {
  checkAnswer(s"""select sum(gamepointId),count(gamepointId),avg(gamepointID),sum(gamepointID)/count(gamepointID) from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(gamepointId),count(gamepointId),avg(gamepointID),sum(gamepointID)/count(gamepointID) from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_355
test("VMALL_DICTIONARY_INCLUDE_355", Include) {
  checkAnswer(s"""select sum(deviceinformationId),count(deviceinformationId),avg(deviceinformationId),sum(deviceinformationId)/count(deviceinformationId) from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(deviceinformationId),count(deviceinformationId),avg(deviceinformationId),sum(deviceinformationId)/count(deviceinformationId) from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_356
test("VMALL_DICTIONARY_INCLUDE_356", Include) {
  checkAnswer(s"""select sum(IMEI),count(IMEI),avg(IMEI),sum(IMEI)/count(IMEI) from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(IMEI),count(IMEI),avg(IMEI),sum(IMEI)/count(IMEI) from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_357
test("VMALL_DICTIONARY_INCLUDE_357", Include) {
  checkAnswer(s"""select sum(productionDate),count(productionDate),avg(productionDate),sum(productionDate)/count(productionDate) from VMALL_DICTIONARY_INCLUDE""",
    s"""select sum(productionDate),count(productionDate),avg(productionDate),sum(productionDate)/count(productionDate) from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_358
test("VMALL_DICTIONARY_INCLUDE_358", Include) {
  checkAnswer(s"""select contractNumber,Latest_DAY,gamePointId,productionDate,deviceInformationId,IMEI  from VMALL_DICTIONARY_INCLUDE""",
    s"""select contractNumber,Latest_DAY,gamePointId,productionDate,deviceInformationId,IMEI  from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_359
test("VMALL_DICTIONARY_INCLUDE_359", Include) {
  checkAnswer(s"""select count(MAC) from VMALL_DICTIONARY_INCLUDE""",
    s"""select count(MAC) from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_360
test("VMALL_DICTIONARY_INCLUDE_360", Include) {
  checkAnswer(s"""select count(gamePointId) from VMALL_DICTIONARY_INCLUDE""",
    s"""select count(gamePointId) from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_361
test("VMALL_DICTIONARY_INCLUDE_361", Include) {
  checkAnswer(s"""select count(contractNumber) from VMALL_DICTIONARY_INCLUDE""",
    s"""select count(contractNumber) from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_362
test("VMALL_DICTIONARY_INCLUDE_362", Include) {
  checkAnswer(s"""select count(Latest_DAY) from VMALL_DICTIONARY_INCLUDE""",
    s"""select count(Latest_DAY) from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_363
test("VMALL_DICTIONARY_INCLUDE_363", Include) {
  checkAnswer(s"""select count(productionDate) from VMALL_DICTIONARY_INCLUDE""",
    s"""select count(productionDate) from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_364
test("VMALL_DICTIONARY_INCLUDE_364", Include) {
  checkAnswer(s"""select count(deviceInformationId) from VMALL_DICTIONARY_INCLUDE""",
    s"""select count(deviceInformationId) from VMALL_DICTIONARY_INCLUDE_hive""")
}
       

//VMALL_DICTIONARY_INCLUDE_365
test("VMALL_DICTIONARY_INCLUDE_365", Include) {
  sql(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE where  contractNumber  != '9223372047700'""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_366
test("VMALL_DICTIONARY_INCLUDE_366", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE where  Latest_DAY  != '1234567890123480.0000000000' order by deviceInformationId limit 5""",
    s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE_hive where  Latest_DAY  != '1234567890123480.0000000000' order by deviceInformationId limit 5""")
}
       

//VMALL_DICTIONARY_INCLUDE_367
test("VMALL_DICTIONARY_INCLUDE_367", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE where  gamePointId  != '2.27852521808948E36' order by imei limit 5""",
    s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE_hive where  gamePointId  != '2.27852521808948E36' order by imei limit 5""")
}
       

//VMALL_DICTIONARY_INCLUDE_368
test("VMALL_DICTIONARY_INCLUDE_368", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE where  productionDate  != '2015-09-18 12:07:28.0' order by imei limit 5""",
    s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE_hive where  productionDate  != '2015-09-18 12:07:28.0' order by imei limit 5""")
}
       

//VMALL_DICTIONARY_INCLUDE_369
test("VMALL_DICTIONARY_INCLUDE_369", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE where  deviceInformationId  != '100075' order by imei limit 5""",
    s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE_hive where  deviceInformationId  != '100075' order by imei limit 5""")
}
       

//VMALL_DICTIONARY_INCLUDE_370
test("VMALL_DICTIONARY_INCLUDE_370", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE where  contractNumber  not like '9223372047700' order by  deviceInformationId limit 5""",
    s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE_hive where  contractNumber  not like '9223372047700' order by  deviceInformationId limit 5""")
}
       

//VMALL_DICTIONARY_INCLUDE_371
test("VMALL_DICTIONARY_INCLUDE_371", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE where  Latest_DAY  not like '1234567890123480.0000000000' order by deviceInformationId limit 5""",
    s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE_hive where  Latest_DAY  not like '1234567890123480.0000000000' order by deviceInformationId limit 5""")
}
       

//VMALL_DICTIONARY_INCLUDE_372
test("VMALL_DICTIONARY_INCLUDE_372", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE where  gamePointId  not like '2.27852521808948E36' order by imei limit 5""",
    s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE_hive where  gamePointId  not like '2.27852521808948E36' order by imei limit 5""")
}
       

//VMALL_DICTIONARY_INCLUDE_373
test("VMALL_DICTIONARY_INCLUDE_373", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE where  productionDate  not like '2015-09-18 12:07:28.0' order by imei limit 5""",
    s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE_hive where  productionDate  not like '2015-09-18 12:07:28.0' order by imei limit 5""")
}
       

//VMALL_DICTIONARY_INCLUDE_374
test("VMALL_DICTIONARY_INCLUDE_374", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE where  deviceInformationId  not like '100075' order by imei limit 5""",
    s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_INCLUDE_hive where  deviceInformationId  not like '100075' order by imei limit 5""")
}
       

//VMALL_DICTIONARY_INCLUDE_375
test("VMALL_DICTIONARY_INCLUDE_375", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei is not null""",
    s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei is not null""")
}
       

//VMALL_DICTIONARY_INCLUDE_376
test("VMALL_DICTIONARY_INCLUDE_376", Include) {
  checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_INCLUDE where gamePointId is not null""",
    s"""select gamePointId from VMALL_DICTIONARY_INCLUDE_hive where gamePointId is not null""")
}
       

//VMALL_DICTIONARY_INCLUDE_377
test("VMALL_DICTIONARY_INCLUDE_377", Include) {
  checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_INCLUDE where contractNumber is not null""",
    s"""select contractNumber from VMALL_DICTIONARY_INCLUDE_hive where contractNumber is not null""")
}
       

//VMALL_DICTIONARY_INCLUDE_378
test("VMALL_DICTIONARY_INCLUDE_378", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY is not null""",
    s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY is not null""")
}
       

//VMALL_DICTIONARY_INCLUDE_379
test("VMALL_DICTIONARY_INCLUDE_379", Include) {
  checkAnswer(s"""select productionDate from VMALL_DICTIONARY_INCLUDE where productionDate is not null""",
    s"""select productionDate from VMALL_DICTIONARY_INCLUDE_hive where productionDate is not null""")
}
       

//VMALL_DICTIONARY_INCLUDE_380
test("VMALL_DICTIONARY_INCLUDE_380", Include) {
  checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE where deviceInformationId is not null""",
    s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId is not null""")
}
       

//VMALL_DICTIONARY_INCLUDE_381
test("VMALL_DICTIONARY_INCLUDE_381", Include) {
  checkAnswer(s"""select imei from VMALL_DICTIONARY_INCLUDE where imei is  null""",
    s"""select imei from VMALL_DICTIONARY_INCLUDE_hive where imei is  null""")
}
       

//VMALL_DICTIONARY_INCLUDE_382
test("VMALL_DICTIONARY_INCLUDE_382", Include) {
  checkAnswer(s"""select gamePointId from VMALL_DICTIONARY_INCLUDE where gamePointId is  null""",
    s"""select gamePointId from VMALL_DICTIONARY_INCLUDE_hive where gamePointId is  null""")
}
       

//VMALL_DICTIONARY_INCLUDE_383
test("VMALL_DICTIONARY_INCLUDE_383", Include) {
  checkAnswer(s"""select contractNumber from VMALL_DICTIONARY_INCLUDE where contractNumber is  null""",
    s"""select contractNumber from VMALL_DICTIONARY_INCLUDE_hive where contractNumber is  null""")
}
       

//VMALL_DICTIONARY_INCLUDE_384
test("VMALL_DICTIONARY_INCLUDE_384", Include) {
  checkAnswer(s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE where Latest_DAY is  null""",
    s"""select Latest_DAY from VMALL_DICTIONARY_INCLUDE_hive where Latest_DAY is  null""")
}
       

//VMALL_DICTIONARY_INCLUDE_385
test("VMALL_DICTIONARY_INCLUDE_385", Include) {
  checkAnswer(s"""select productionDate from VMALL_DICTIONARY_INCLUDE where productionDate is  null""",
    s"""select productionDate from VMALL_DICTIONARY_INCLUDE_hive where productionDate is  null""")
}
       

//VMALL_DICTIONARY_INCLUDE_386
test("VMALL_DICTIONARY_INCLUDE_386", Include) {
  checkAnswer(s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE where deviceInformationId is  null""",
    s"""select deviceInformationId from VMALL_DICTIONARY_INCLUDE_hive where deviceInformationId is  null""")
}
       

//VMALL_DICTIONARY_INCLUDE_387
test("VMALL_DICTIONARY_INCLUDE_387", Include) {
  checkAnswer(s"""select count(*) from VMALL_DICTIONARY_INCLUDE where imei = '1AA1'""",
    s"""select count(*) from VMALL_DICTIONARY_INCLUDE_hive where imei = '1AA1'""")
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_001
test("VMALL_DICTIONARY_INCLUDE_PushUP_001", Include) {
  sql(s"""select count(imei)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000'  and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site'  and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_002
test("VMALL_DICTIONARY_INCLUDE_PushUP_002", Include) {
  sql(s"""select count(deviceinformationId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site'  and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_003
test("VMALL_DICTIONARY_INCLUDE_PushUP_003", Include) {
  sql(s"""select count(productionDate)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site'  and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_004
test("VMALL_DICTIONARY_INCLUDE_PushUP_004", Include) {
  sql(s"""select count(gamePointId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site'  and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_005
test("VMALL_DICTIONARY_INCLUDE_PushUP_005", Include) {
  sql(s"""select count(Latest_DAY)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site'  and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_006
test("VMALL_DICTIONARY_INCLUDE_PushUP_006", Include) {
  sql(s"""select count(contractNumber)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site'  and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_007
test("VMALL_DICTIONARY_INCLUDE_PushUP_007", Include) {
  sql(s"""select count(distinct imei)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site'  and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_008
test("VMALL_DICTIONARY_INCLUDE_PushUP_008", Include) {
  sql(s"""select count(distinct deviceinformationId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_009
test("VMALL_DICTIONARY_INCLUDE_PushUP_009", Include) {
  sql(s"""select count(distinct productionDate)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_010
test("VMALL_DICTIONARY_INCLUDE_PushUP_010", Include) {
  sql(s"""select count(distinct gamePointId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_011
test("VMALL_DICTIONARY_INCLUDE_PushUP_011", Include) {
  sql(s"""select count(distinct Latest_DAY)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_012
test("VMALL_DICTIONARY_INCLUDE_PushUP_012", Include) {
  sql(s"""select count(distinct contractNumber)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_013
test("VMALL_DICTIONARY_INCLUDE_PushUP_013", Include) {
  sql(s"""select sum(deviceinformationId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_014
test("VMALL_DICTIONARY_INCLUDE_PushUP_014", Include) {
  sql(s"""select sum(productionDate)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_015
test("VMALL_DICTIONARY_INCLUDE_PushUP_015", Include) {
  sql(s"""select sum(gamePointId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_016
test("VMALL_DICTIONARY_INCLUDE_PushUP_016", Include) {
  sql(s"""select sum(Latest_DAY)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_017
test("VMALL_DICTIONARY_INCLUDE_PushUP_017", Include) {
  sql(s"""select sum(contractNumber)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_018
test("VMALL_DICTIONARY_INCLUDE_PushUP_018", Include) {
  sql(s"""select sum(distinct deviceinformationId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_019
test("VMALL_DICTIONARY_INCLUDE_PushUP_019", Include) {
  sql(s"""select sum(distinct gamePointId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_020
test("VMALL_DICTIONARY_INCLUDE_PushUP_020", Include) {
  sql(s"""select sum(distinct Latest_DAY)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_021
test("VMALL_DICTIONARY_INCLUDE_PushUP_021", Include) {
  sql(s"""select sum(distinct contractNumber)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_022
test("VMALL_DICTIONARY_INCLUDE_PushUP_022", Include) {
  sql(s"""select min(deviceinformationId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_023
test("VMALL_DICTIONARY_INCLUDE_PushUP_023", Include) {
  sql(s"""select min(productionDate)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_024
test("VMALL_DICTIONARY_INCLUDE_PushUP_024", Include) {
  sql(s"""select min(gamePointId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_025
test("VMALL_DICTIONARY_INCLUDE_PushUP_025", Include) {
  sql(s"""select min(Latest_DAY)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_026
test("VMALL_DICTIONARY_INCLUDE_PushUP_026", Include) {
  sql(s"""select min(contractNumber)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_027
test("VMALL_DICTIONARY_INCLUDE_PushUP_027", Include) {
  sql(s"""select max(imei)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_028
test("VMALL_DICTIONARY_INCLUDE_PushUP_028", Include) {
  sql(s"""select max(deviceinformationId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_029
test("VMALL_DICTIONARY_INCLUDE_PushUP_029", Include) {
  sql(s"""select max(productionDate)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_030
test("VMALL_DICTIONARY_INCLUDE_PushUP_030", Include) {
  sql(s"""select max(gamePointId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_031
test("VMALL_DICTIONARY_INCLUDE_PushUP_031", Include) {
  sql(s"""select max(Latest_DAY)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_032
test("VMALL_DICTIONARY_INCLUDE_PushUP_032", Include) {
  sql(s"""select max(contractNumber)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_033
test("VMALL_DICTIONARY_INCLUDE_PushUP_033", Include) {
  sql(s"""select variance(deviceInformationId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_035
test("VMALL_DICTIONARY_INCLUDE_PushUP_035", Include) {
  sql(s"""select variance(gamePointId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_036
test("VMALL_DICTIONARY_INCLUDE_PushUP_036", Include) {
  sql(s"""select variance(Latest_DAY)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_037
test("VMALL_DICTIONARY_INCLUDE_PushUP_037", Include) {
  sql(s"""select variance(contractNumber)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_038
test("VMALL_DICTIONARY_INCLUDE_PushUP_038", Include) {
  sql(s"""select var_samp(deviceInformationId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_040
test("VMALL_DICTIONARY_INCLUDE_PushUP_040", Include) {
  sql(s"""select var_samp(gamePointId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_041
test("VMALL_DICTIONARY_INCLUDE_PushUP_041", Include) {
  sql(s"""select var_samp(Latest_DAY)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_042
test("VMALL_DICTIONARY_INCLUDE_PushUP_042", Include) {
  sql(s"""select var_samp(contractNumber)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_043
test("VMALL_DICTIONARY_INCLUDE_PushUP_043", Include) {
  sql(s"""select stddev_pop(deviceInformationId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_045
test("VMALL_DICTIONARY_INCLUDE_PushUP_045", Include) {
  sql(s"""select stddev_pop(gamePointId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_046
test("VMALL_DICTIONARY_INCLUDE_PushUP_046", Include) {
  sql(s"""select stddev_pop(Latest_DAY)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_047
test("VMALL_DICTIONARY_INCLUDE_PushUP_047", Include) {
  sql(s"""select stddev_pop(contractNumber)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_048
test("VMALL_DICTIONARY_INCLUDE_PushUP_048", Include) {
  sql(s"""select stddev_samp(deviceInformationId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_050
test("VMALL_DICTIONARY_INCLUDE_PushUP_050", Include) {
  sql(s"""select stddev_samp(gamePointId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_051
test("VMALL_DICTIONARY_INCLUDE_PushUP_051", Include) {
  sql(s"""select stddev_samp(Latest_DAY)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_054
test("VMALL_DICTIONARY_INCLUDE_PushUP_054", Include) {
  sql(s"""select AVG(deviceinformationId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_055
test("VMALL_DICTIONARY_INCLUDE_PushUP_055", Include) {
  sql(s"""select AVG(productionDate)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_056
test("VMALL_DICTIONARY_INCLUDE_PushUP_056", Include) {
  sql(s"""select AVG(gamePointId)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_057
test("VMALL_DICTIONARY_INCLUDE_PushUP_057", Include) {
  sql(s"""select AVG(Latest_DAY)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_058
test("VMALL_DICTIONARY_INCLUDE_PushUP_058", Include) {
  sql(s"""select AVG(contractNumber)  from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_059
test("VMALL_DICTIONARY_INCLUDE_PushUP_059", Include) {
  checkAnswer(s"""select count(imei),sum(distinct deviceinformationId),min(productionDate),max(imei),variance(Latest_DAY) from VMALL_DICTIONARY_INCLUDE group by deviceInformationId limit 5""",
    s"""select count(imei),sum(distinct deviceinformationId),min(productionDate),max(imei),variance(Latest_DAY) from VMALL_DICTIONARY_INCLUDE_hive group by deviceInformationId limit 5""")
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_060
test("VMALL_DICTIONARY_INCLUDE_PushUP_060", Include) {
  checkAnswer(s"""select sum(deviceinformationId),sum(distinct deviceinformationId),min(deviceinformationId),max(imei),variance(deviceInformationId) from VMALL_DICTIONARY_INCLUDE group by deviceInformationId order by deviceinformationId limit 5""",
    s"""select sum(deviceinformationId),sum(distinct deviceinformationId),min(deviceinformationId),max(imei),variance(deviceInformationId) from VMALL_DICTIONARY_INCLUDE_hive group by deviceInformationId order by deviceinformationId limit 5""")
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_061
test("VMALL_DICTIONARY_INCLUDE_PushUP_061", Include) {
  checkAnswer(s"""select count(imei),sum(distinct deviceinformationId),min(productionDate),max(imei),variance(Latest_DAY) from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')group by deviceInformationId limit 5""",
    s"""select count(imei),sum(distinct deviceinformationId),min(productionDate),max(imei),variance(Latest_DAY) from VMALL_DICTIONARY_INCLUDE_hive where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null')group by deviceInformationId limit 5""")
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_062
test("VMALL_DICTIONARY_INCLUDE_PushUP_062", Include) {
  sql(s"""select count(imei),sum(distinct deviceinformationId),min(productionDate),max(imei),variance(Latest_DAY) from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') group by deviceInformationId order by deviceinformationId limit 5""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_063
test("VMALL_DICTIONARY_INCLUDE_PushUP_063", Include) {
  sql(s"""select count(imei),sum(distinct deviceinformationId),min(productionDate),max(imei),variance(Latest_DAY) from VMALL_DICTIONARY_INCLUDE where deviceColor ='5Device Color' and modelId != '109' or Latest_DAY > '1234567890123540.0000000000' and contractNumber == '92233720368547800' or  Active_operaSysVersion like 'Operating System Version' and gamePointId <=> '8.1366141918611E39' and deviceInformationId < '1000000' and productionDate not like '2016-07-01' and imei is null and Latest_HOUR is not null and channelsId <= '7' and Latest_releaseId >= '1' and Latest_MONTH between 6 and 8 and Latest_YEAR not between 2016 and 2017 and Latest_HOUR RLIKE '12' and gamePointDescription REGEXP 'Site' and imei in ('1AA1','1AA100','1AA10','1AA1000','1AA10000','1AA100000','1AA1000000','1AA100001','1AA100002','1AA100004','','NULL') and Active_BacVerNumber not in ('Background version number1','','null') group by deviceInformationId,productionDate   sort by productionDate limit 5
""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_064
test("VMALL_DICTIONARY_INCLUDE_PushUP_064", Include) {
  sql(s"""select sum(deviceinformationId+10)t  from  VMALL_DICTIONARY_INCLUDE having t >1234567""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_065
test("VMALL_DICTIONARY_INCLUDE_PushUP_065", Include) {
  checkAnswer(s"""select sum(deviceinformationId+gamePointId)t  from  VMALL_DICTIONARY_INCLUDE having t >1234567""",
    s"""select sum(deviceinformationId+gamePointId)t  from  VMALL_DICTIONARY_INCLUDE_hive having t >1234567""")
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_066
test("VMALL_DICTIONARY_INCLUDE_PushUP_066", Include) {
  checkAnswer(s"""select sum(deviceinformationId)t,Sum(gamePointId)   from  VMALL_DICTIONARY_INCLUDE having t >1234567""",
    s"""select sum(deviceinformationId)t,Sum(gamePointId)   from  VMALL_DICTIONARY_INCLUDE_hive having t >1234567""")
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_067
test("VMALL_DICTIONARY_INCLUDE_PushUP_067", Include) {
  checkAnswer(s"""select count(imei),sum(distinct deviceinformationId),min(productionDate)  from VMALL_DICTIONARY_INCLUDE group by imei,deviceinformationId,productionDate  order by  imei""",
    s"""select count(imei),sum(distinct deviceinformationId),min(productionDate)  from VMALL_DICTIONARY_INCLUDE_hive group by imei,deviceinformationId,productionDate  order by  imei""")
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_069
test("VMALL_DICTIONARY_INCLUDE_PushUP_069", Include) {
  sql(s"""SELECT  min(Latest_DAY),max(imei),variance(contractNumber), SUM(gamePointId),count(imei),sum(distinct deviceinformationId) FROM (select * from VMALL_DICTIONARY_INCLUDE) SUB_QRY GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC limit 10""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_070
test("VMALL_DICTIONARY_INCLUDE_PushUP_070", Include) {
  sql(s"""SELECT  VMALL_DICTIONARY_INCLUDE.gamePointId AS gamePointId,VMALL_DICTIONARY_INCLUDE.AMSize AS AMSize, VMALL_DICTIONARY_INCLUDE.ActiveCountry AS ActiveCountry, VMALL_DICTIONARY_INCLUDE.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from VMALL_DICTIONARY_INCLUDE) SUB_QRY ) VMALL_DICTIONARY_INCLUDE FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from VMALL_DICTIONARY_INCLUDE) SUB_QRY ) VMALL_DICTIONARY_INCLUDE1 ON VMALL_DICTIONARY_INCLUDE.AMSize = VMALL_DICTIONARY_INCLUDE1.AMSize WHERE VMALL_DICTIONARY_INCLUDE.AMSize LIKE '5RAM %' GROUP BY VMALL_DICTIONARY_INCLUDE.AMSize, VMALL_DICTIONARY_INCLUDE.ActiveCountry, VMALL_DICTIONARY_INCLUDE.Activecity,VMALL_DICTIONARY_INCLUDE.gamePointId ORDER BY VMALL_DICTIONARY_INCLUDE.AMSize ASC, VMALL_DICTIONARY_INCLUDE.ActiveCountry ASC, VMALL_DICTIONARY_INCLUDE.Activecity ASC""").collect
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_071
test("VMALL_DICTIONARY_INCLUDE_PushUP_071", Include) {
  checkAnswer(s"""SELECT VMALL_DICTIONARY_INCLUDE.gamePointId AS gamePointId,VMALL_DICTIONARY_INCLUDE.AMSize AS AMSize, VMALL_DICTIONARY_INCLUDE.ActiveCountry AS ActiveCountry, VMALL_DICTIONARY_INCLUDE.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from VMALL_DICTIONARY_INCLUDE) SUB_QRY ) VMALL_DICTIONARY_INCLUDE RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from VMALL_DICTIONARY_INCLUDE) SUB_QRY ) Carbon_automation1 ON VMALL_DICTIONARY_INCLUDE.AMSize = Carbon_automation1.AMSize WHERE NOT(VMALL_DICTIONARY_INCLUDE.AMSize = "8RAM size") GROUP BY VMALL_DICTIONARY_INCLUDE.AMSize, VMALL_DICTIONARY_INCLUDE.ActiveCountry, VMALL_DICTIONARY_INCLUDE.Activecity,VMALL_DICTIONARY_INCLUDE.gamePointId  ORDER BY VMALL_DICTIONARY_INCLUDE.AMSize ASC, VMALL_DICTIONARY_INCLUDE.ActiveCountry ASC, VMALL_DICTIONARY_INCLUDE.Activecity ASC limit 10""",
    s"""SELECT VMALL_DICTIONARY_INCLUDE_hive.gamePointId AS gamePointId,VMALL_DICTIONARY_INCLUDE_hive.AMSize AS AMSize, VMALL_DICTIONARY_INCLUDE_hive.ActiveCountry AS ActiveCountry, VMALL_DICTIONARY_INCLUDE_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from VMALL_DICTIONARY_INCLUDE_hive) SUB_QRY ) VMALL_DICTIONARY_INCLUDE_hive RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from VMALL_DICTIONARY_INCLUDE_hive) SUB_QRY ) Carbon_automation1 ON VMALL_DICTIONARY_INCLUDE_hive.AMSize = Carbon_automation1.AMSize WHERE NOT(VMALL_DICTIONARY_INCLUDE_hive.AMSize = "8RAM size") GROUP BY VMALL_DICTIONARY_INCLUDE_hive.AMSize, VMALL_DICTIONARY_INCLUDE_hive.ActiveCountry, VMALL_DICTIONARY_INCLUDE_hive.Activecity,VMALL_DICTIONARY_INCLUDE_hive.gamePointId  ORDER BY VMALL_DICTIONARY_INCLUDE_hive.AMSize ASC, VMALL_DICTIONARY_INCLUDE_hive.ActiveCountry ASC, VMALL_DICTIONARY_INCLUDE_hive.Activecity ASC limit 10""")
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_072
test("VMALL_DICTIONARY_INCLUDE_PushUP_072", Include) {
  checkAnswer(s"""SELECT VMALL_DICTIONARY_INCLUDE.gamePointId AS gamePointId,VMALL_DICTIONARY_INCLUDE.AMSize AS AMSize, VMALL_DICTIONARY_INCLUDE.ActiveCountry AS ActiveCountry, VMALL_DICTIONARY_INCLUDE.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from VMALL_DICTIONARY_INCLUDE) SUB_QRY ) VMALL_DICTIONARY_INCLUDE LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from VMALL_DICTIONARY_INCLUDE) SUB_QRY ) Carbon_automation1 ON VMALL_DICTIONARY_INCLUDE.AMSize = Carbon_automation1.AMSize WHERE NOT(VMALL_DICTIONARY_INCLUDE.AMSize = "8RAM size") GROUP BY VMALL_DICTIONARY_INCLUDE.AMSize, VMALL_DICTIONARY_INCLUDE.ActiveCountry, VMALL_DICTIONARY_INCLUDE.Activecity,VMALL_DICTIONARY_INCLUDE.gamePointId  ORDER BY VMALL_DICTIONARY_INCLUDE.AMSize ASC, VMALL_DICTIONARY_INCLUDE.ActiveCountry ASC, VMALL_DICTIONARY_INCLUDE.Activecity ASC limit 10
""",
    s"""SELECT VMALL_DICTIONARY_INCLUDE_hive.gamePointId AS gamePointId,VMALL_DICTIONARY_INCLUDE_hive.AMSize AS AMSize, VMALL_DICTIONARY_INCLUDE_hive.ActiveCountry AS ActiveCountry, VMALL_DICTIONARY_INCLUDE_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from VMALL_DICTIONARY_INCLUDE_hive) SUB_QRY ) VMALL_DICTIONARY_INCLUDE_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from VMALL_DICTIONARY_INCLUDE_hive) SUB_QRY ) Carbon_automation1 ON VMALL_DICTIONARY_INCLUDE_hive.AMSize = Carbon_automation1.AMSize WHERE NOT(VMALL_DICTIONARY_INCLUDE_hive.AMSize = "8RAM size") GROUP BY VMALL_DICTIONARY_INCLUDE_hive.AMSize, VMALL_DICTIONARY_INCLUDE_hive.ActiveCountry, VMALL_DICTIONARY_INCLUDE_hive.Activecity,VMALL_DICTIONARY_INCLUDE_hive.gamePointId  ORDER BY VMALL_DICTIONARY_INCLUDE_hive.AMSize ASC, VMALL_DICTIONARY_INCLUDE_hive.ActiveCountry ASC, VMALL_DICTIONARY_INCLUDE_hive.Activecity ASC limit 10
""")
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_073
test("VMALL_DICTIONARY_INCLUDE_PushUP_073", Include) {
  checkAnswer(s"""SELECT VMALL_DICTIONARY_INCLUDE.gamePointId AS gamePointId,VMALL_DICTIONARY_INCLUDE.AMSize AS AMSize, VMALL_DICTIONARY_INCLUDE.ActiveCountry AS ActiveCountry, VMALL_DICTIONARY_INCLUDE.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from VMALL_DICTIONARY_INCLUDE) SUB_QRY ) VMALL_DICTIONARY_INCLUDE INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from VMALL_DICTIONARY_INCLUDE) SUB_QRY ) Carbon_automation1 ON VMALL_DICTIONARY_INCLUDE.AMSize = Carbon_automation1.AMSize WHERE NOT(VMALL_DICTIONARY_INCLUDE.AMSize = "8RAM size") GROUP BY VMALL_DICTIONARY_INCLUDE.AMSize, VMALL_DICTIONARY_INCLUDE.ActiveCountry, VMALL_DICTIONARY_INCLUDE.Activecity,VMALL_DICTIONARY_INCLUDE.gamePointId  ORDER BY VMALL_DICTIONARY_INCLUDE.AMSize ASC, VMALL_DICTIONARY_INCLUDE.ActiveCountry ASC, VMALL_DICTIONARY_INCLUDE.Activecity ASC limit 10
""",
    s"""SELECT VMALL_DICTIONARY_INCLUDE_hive.gamePointId AS gamePointId,VMALL_DICTIONARY_INCLUDE_hive.AMSize AS AMSize, VMALL_DICTIONARY_INCLUDE_hive.ActiveCountry AS ActiveCountry, VMALL_DICTIONARY_INCLUDE_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select * from VMALL_DICTIONARY_INCLUDE_hive) SUB_QRY ) VMALL_DICTIONARY_INCLUDE_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from VMALL_DICTIONARY_INCLUDE_hive) SUB_QRY ) Carbon_automation1 ON VMALL_DICTIONARY_INCLUDE_hive.AMSize = Carbon_automation1.AMSize WHERE NOT(VMALL_DICTIONARY_INCLUDE_hive.AMSize = "8RAM size") GROUP BY VMALL_DICTIONARY_INCLUDE_hive.AMSize, VMALL_DICTIONARY_INCLUDE_hive.ActiveCountry, VMALL_DICTIONARY_INCLUDE_hive.Activecity,VMALL_DICTIONARY_INCLUDE_hive.gamePointId  ORDER BY VMALL_DICTIONARY_INCLUDE_hive.AMSize ASC, VMALL_DICTIONARY_INCLUDE_hive.ActiveCountry ASC, VMALL_DICTIONARY_INCLUDE_hive.Activecity ASC limit 10
""")
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_074
test("VMALL_DICTIONARY_INCLUDE_PushUP_074", Include) {
  checkAnswer(s"""select count(gamepointid),series  from VMALL_DICTIONARY_INCLUDE group by series order by series limit 5""",
    s"""select count(gamepointid),series  from VMALL_DICTIONARY_INCLUDE_hive group by series order by series limit 5""")
}
       

//VMALL_DICTIONARY_INCLUDE_PushUP_075
test("VMALL_DICTIONARY_INCLUDE_PushUP_075", Include) {
  checkAnswer(s"""select count(gamepointid),series  from VMALL_DICTIONARY_INCLUDE group by series order by series""",
    s"""select count(gamepointid),series  from VMALL_DICTIONARY_INCLUDE_hive group by series order by series""")
}
       
override def afterAll {
sql("drop table if exists vmall_dictionary_include")
sql("drop table if exists vmall_dictionary_include_hive")
sql("drop table if exists VMALL_DICTIONARY_INCLUDE")
sql("drop table if exists VMALL_DICTIONARY_INCLUDE_hive")
}
}