
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
 * Test Class for VMALLDICTIONARYCOLUMNGRP to verify all scenerios
 */

class VMALLDICTIONARYCOLUMNGRPTestCase extends QueryTest with BeforeAndAfterAll {
         

//drop_columgroupdata
test("drop_columgroupdata", Include) {
  sql(s"""drop table if exists  VMALL_DICTIONARY_COLUMNGRP""").collect

  sql(s"""drop table if exists  VMALL_DICTIONARY_COLUMNGRP_hive""").collect

}
       

//VMALL_DICTIONARY_COLUMNGRP_CreteCube
test("VMALL_DICTIONARY_COLUMNGRP_CreteCube", Include) {
  sql(s"""create table  VMALL_DICTIONARY_COLUMNGRP (imei string,MAC string,deviceColor string,deviceInformationId int,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt)  STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='imei,deviceInformationId,productionDate,gamePointId,Latest_DAY,contractNumber','columnproperties.deviceInformationId.shared_column'='shared.deviceInformationId','columnproperties.imei.shared_column'='shared.imei','columnproperties.contractNumber.shared_column'='shared.contractNumber','columnproperties.Latest_DAY.shared_column'='shared.Latest_DAY','columnproperties.gamePointId.shared_column'='shared.gamePointId')""").collect

  sql(s"""create table  VMALL_DICTIONARY_COLUMNGRP_hive (imei string,MAC string,deviceColor string,deviceInformationId int,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt)   ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//VMALL_DICTIONARY_COLUMNGRP_CreteCube_count
test("VMALL_DICTIONARY_COLUMNGRP_CreteCube_count", Include) {
  sql(s"""select count(*) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_Dataload
test("VMALL_DICTIONARY_COLUMNGRP_Dataload", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table VMALL_DICTIONARY_COLUMNGRP options ('DELIMITER'=',', 'QUOTECHAR'='"', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table VMALL_DICTIONARY_COLUMNGRP_hive """).collect

}
       

//VMALL_DICTIONARY_COLUMNGRP_001
test("VMALL_DICTIONARY_COLUMNGRP_001", Include) {
  sql(s"""Select count(imei) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_002
test("VMALL_DICTIONARY_COLUMNGRP_002", Include) {
  sql(s"""select count(DISTINCT imei) as a from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_003
test("VMALL_DICTIONARY_COLUMNGRP_003", Include) {
  sql(s"""select sum(Latest_month)+10 as a ,imei  from VMALL_DICTIONARY_COLUMNGRP group by imei order by imei""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_004
test("VMALL_DICTIONARY_COLUMNGRP_004", Include) {
  sql(s"""select max(imei),min(imei) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_005
test("VMALL_DICTIONARY_COLUMNGRP_005", Include) {
  sql(s"""select min(imei), max(imei) Total from VMALL_DICTIONARY_COLUMNGRP group by  channelsId order by Total""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_006
test("VMALL_DICTIONARY_COLUMNGRP_006", Include) {
  sql(s"""select last(imei) a from VMALL_DICTIONARY_COLUMNGRP  group by imei order by imei limit 1""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_007
test("VMALL_DICTIONARY_COLUMNGRP_007", Include) {
  sql(s"""select FIRST(imei) a from VMALL_DICTIONARY_COLUMNGRP group by imei order by imei limit 1""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_008
test("VMALL_DICTIONARY_COLUMNGRP_008", Include) {
  sql(s"""select imei,count(imei) a from VMALL_DICTIONARY_COLUMNGRP group by imei order by imei""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_009
test("VMALL_DICTIONARY_COLUMNGRP_009", Include) {
  sql(s"""select Lower(imei) a  from VMALL_DICTIONARY_COLUMNGRP order by imei""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_010
test("VMALL_DICTIONARY_COLUMNGRP_010", Include) {
  sql(s"""select distinct imei from VMALL_DICTIONARY_COLUMNGRP order by imei""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_011
test("VMALL_DICTIONARY_COLUMNGRP_011", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_COLUMNGRP order by imei limit 101 """).collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_012
test("VMALL_DICTIONARY_COLUMNGRP_012", Include) {
  sql(s"""select imei as a from VMALL_DICTIONARY_COLUMNGRP  order by a asc limit 10""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_013
test("VMALL_DICTIONARY_COLUMNGRP_013", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_COLUMNGRP where  (contractNumber == 9223372047700) and (imei=='1AA100004')""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_014
test("VMALL_DICTIONARY_COLUMNGRP_014", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_COLUMNGRP where imei !='1AA100064' order by imei""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_015
test("VMALL_DICTIONARY_COLUMNGRP_015", Include) {
  sql(s"""select imei  from VMALL_DICTIONARY_COLUMNGRP where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_016
test("VMALL_DICTIONARY_COLUMNGRP_016", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_COLUMNGRP where imei !='1AA100012' order by imei""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_017
test("VMALL_DICTIONARY_COLUMNGRP_017", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_COLUMNGRP where imei >'1AA100012' order by imei""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_018
test("VMALL_DICTIONARY_COLUMNGRP_018", Include) {
  sql(s"""select imei  from VMALL_DICTIONARY_COLUMNGRP where imei<>imei""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_019
test("VMALL_DICTIONARY_COLUMNGRP_019", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_COLUMNGRP where imei != Latest_areaId order by imei""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_020
test("VMALL_DICTIONARY_COLUMNGRP_020", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_COLUMNGRP where Latest_areaId<imei order by imei""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_021
test("VMALL_DICTIONARY_COLUMNGRP_021", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_COLUMNGRP where Latest_DAY<=imei order by imei""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_022
test("VMALL_DICTIONARY_COLUMNGRP_022", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_COLUMNGRP where imei <'1AA10002' order by imei""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_023
test("VMALL_DICTIONARY_COLUMNGRP_023", Include) {
  sql(s"""select Latest_day  from VMALL_DICTIONARY_COLUMNGRP where imei IS NULL""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_024
test("VMALL_DICTIONARY_COLUMNGRP_024", Include) {
  sql(s"""select Latest_day  from VMALL_DICTIONARY_COLUMNGRP where imei IS NOT NULL order by imei""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_025
test("VMALL_DICTIONARY_COLUMNGRP_025", Include) {
  sql(s"""Select count(imei),min(imei) from VMALL_DICTIONARY_COLUMNGRP """).collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_026
test("VMALL_DICTIONARY_COLUMNGRP_026", Include) {
  sql(s"""select count(DISTINCT imei,latest_day) as a from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_027
test("VMALL_DICTIONARY_COLUMNGRP_027", Include) {
  sql(s"""select max(imei),min(imei),count(imei) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_028
test("VMALL_DICTIONARY_COLUMNGRP_028", Include) {
  sql(s"""select sum(imei),avg(imei),count(imei) a  from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_029
test("VMALL_DICTIONARY_COLUMNGRP_029", Include) {
  sql(s"""select last(imei),Min(imei),max(imei)  a from (select imei from VMALL_DICTIONARY_COLUMNGRP order by imei) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_030
test("VMALL_DICTIONARY_COLUMNGRP_030", Include) {
  sql(s"""select FIRST(imei),Last(imei) a from VMALL_DICTIONARY_COLUMNGRP group by imei order by imei limit 1""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_031
test("VMALL_DICTIONARY_COLUMNGRP_031", Include) {
  sql(s"""select imei,count(imei) a from VMALL_DICTIONARY_COLUMNGRP group by imei order by imei""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_032
test("VMALL_DICTIONARY_COLUMNGRP_032", Include) {
  sql(s"""select Lower(imei),upper(imei)  a  from VMALL_DICTIONARY_COLUMNGRP order by imei""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_033
test("VMALL_DICTIONARY_COLUMNGRP_033", Include) {
  sql(s"""select imei as a from VMALL_DICTIONARY_COLUMNGRP  order by a asc limit 10""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_034
test("VMALL_DICTIONARY_COLUMNGRP_034", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_COLUMNGRP where  (contractNumber == 9223372047700) and (imei=='1AA100012')""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_035
test("VMALL_DICTIONARY_COLUMNGRP_035", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_COLUMNGRP where imei !='8imei' order by imei""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_036
test("VMALL_DICTIONARY_COLUMNGRP_036", Include) {
  sql(s"""select imei  from VMALL_DICTIONARY_COLUMNGRP where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_037
test("VMALL_DICTIONARY_COLUMNGRP_037", Include) {
  sql(s"""Select count(contractNumber) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_038
test("VMALL_DICTIONARY_COLUMNGRP_038", Include) {
  sql(s"""select count(DISTINCT contractNumber) as a from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_039
test("VMALL_DICTIONARY_COLUMNGRP_039", Include) {
  sql(s"""select sum(contractNumber)+10 as a ,contractNumber  from VMALL_DICTIONARY_COLUMNGRP group by contractNumber""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_040
test("VMALL_DICTIONARY_COLUMNGRP_040", Include) {
  sql(s"""select max(contractNumber),min(contractNumber) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_041
test("VMALL_DICTIONARY_COLUMNGRP_041", Include) {
  sql(s"""select sum(contractNumber) a  from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_042
test("VMALL_DICTIONARY_COLUMNGRP_042", Include) {
  sql(s"""select avg(contractNumber) a  from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_043
test("VMALL_DICTIONARY_COLUMNGRP_043", Include) {
  sql(s"""select min(contractNumber) a  from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_044
test("VMALL_DICTIONARY_COLUMNGRP_044", Include) {
  sql(s"""select variance(contractNumber) as a   from (select contractNumber from VMALL_DICTIONARY_COLUMNGRP order by contractNumber) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_045
test("VMALL_DICTIONARY_COLUMNGRP_045", Include) {
  sql(s"""select var_pop(contractNumber)  as a from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_046
test("VMALL_DICTIONARY_COLUMNGRP_046", Include) {
  sql(s"""select var_samp(contractNumber) as a  from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_047
test("VMALL_DICTIONARY_COLUMNGRP_047", Include) {
  sql(s"""select stddev_pop(contractNumber) as a  from (select contractNumber from VMALL_DICTIONARY_COLUMNGRP order by contractNumber) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_048
test("VMALL_DICTIONARY_COLUMNGRP_048", Include) {
  sql(s"""select stddev_samp(contractNumber)  as a from (select contractNumber from VMALL_DICTIONARY_COLUMNGRP order by contractNumber) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_049
test("VMALL_DICTIONARY_COLUMNGRP_049", Include) {
  sql(s"""select covar_pop(contractNumber,contractNumber) as a  from (select contractNumber from VMALL_DICTIONARY_COLUMNGRP order by contractNumber) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_050
test("VMALL_DICTIONARY_COLUMNGRP_050", Include) {
  sql(s"""select covar_samp(contractNumber,contractNumber) as a  from (select contractNumber from VMALL_DICTIONARY_COLUMNGRP order by contractNumber) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_051
test("VMALL_DICTIONARY_COLUMNGRP_051", Include) {
  sql(s"""select corr(contractNumber,contractNumber)  as a from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_052
test("VMALL_DICTIONARY_COLUMNGRP_052", Include) {
  sql(s"""select percentile_approx(contractNumber,0.2) as a  from (select contractNumber from VMALL_DICTIONARY_COLUMNGRP order by contractNumber) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_053
test("VMALL_DICTIONARY_COLUMNGRP_053", Include) {
  sql(s"""select percentile_approx(contractNumber,0.2,5) as a  from (select contractNumber from VMALL_DICTIONARY_COLUMNGRP order by contractNumber) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_054
test("VMALL_DICTIONARY_COLUMNGRP_054", Include) {
  sql(s"""select percentile_approx(contractNumber,array(0.2,0.3,0.99))  as a from (select contractNumber from VMALL_DICTIONARY_COLUMNGRP order by contractNumber) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_055
test("VMALL_DICTIONARY_COLUMNGRP_055", Include) {
  sql(s"""select percentile_approx(contractNumber,array(0.2,0.3,0.99),5) as a from (select contractNumber from VMALL_DICTIONARY_COLUMNGRP order by contractNumber) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_056
test("VMALL_DICTIONARY_COLUMNGRP_056", Include) {
  sql(s"""select histogram_numeric(contractNumber,2)  as a from (select contractNumber from VMALL_DICTIONARY_COLUMNGRP order by contractNumber) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_057
test("VMALL_DICTIONARY_COLUMNGRP_057", Include) {
  sql(s"""select contractNumber+ 10 as a  from VMALL_DICTIONARY_COLUMNGRP order by a""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_058
test("VMALL_DICTIONARY_COLUMNGRP_058", Include) {
  sql(s"""select min(contractNumber), max(contractNumber+ 10) Total from VMALL_DICTIONARY_COLUMNGRP group by  channelsId order by Total""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_059
test("VMALL_DICTIONARY_COLUMNGRP_059", Include) {
  sql(s"""select last(contractNumber) a from VMALL_DICTIONARY_COLUMNGRP  order by a""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_060
test("VMALL_DICTIONARY_COLUMNGRP_060", Include) {
  sql(s"""select FIRST(contractNumber) a from VMALL_DICTIONARY_COLUMNGRP order by a""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_061
test("VMALL_DICTIONARY_COLUMNGRP_061", Include) {
  sql(s"""select contractNumber,count(contractNumber) a from VMALL_DICTIONARY_COLUMNGRP group by contractNumber order by contractNumber""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_062
test("VMALL_DICTIONARY_COLUMNGRP_062", Include) {
  sql(s"""select Lower(contractNumber) a  from VMALL_DICTIONARY_COLUMNGRP order by contractNumber""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_063
test("VMALL_DICTIONARY_COLUMNGRP_063", Include) {
  sql(s"""select distinct contractNumber from VMALL_DICTIONARY_COLUMNGRP order by contractNumber""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_064
test("VMALL_DICTIONARY_COLUMNGRP_064", Include) {
  sql(s"""select contractNumber from VMALL_DICTIONARY_COLUMNGRP order by contractNumber limit 101""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_065
test("VMALL_DICTIONARY_COLUMNGRP_065", Include) {
  sql(s"""select contractNumber as a from VMALL_DICTIONARY_COLUMNGRP  order by a asc limit 10""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_066
test("VMALL_DICTIONARY_COLUMNGRP_066", Include) {
  sql(s"""select contractNumber from VMALL_DICTIONARY_COLUMNGRP where  (contractNumber == 9223372047700) and (imei=='1AA100012')""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_067
test("VMALL_DICTIONARY_COLUMNGRP_067", Include) {
  sql(s"""select contractNumber from VMALL_DICTIONARY_COLUMNGRP where contractNumber !=9223372047700 order by contractNumber""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_068
test("VMALL_DICTIONARY_COLUMNGRP_068", Include) {
  sql(s"""select contractNumber  from VMALL_DICTIONARY_COLUMNGRP where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color') order by contractNumber""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_069
test("VMALL_DICTIONARY_COLUMNGRP_069", Include) {
  sql(s"""select contractNumber from VMALL_DICTIONARY_COLUMNGRP where contractNumber !=9223372047700 order by contractNumber""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_070
test("VMALL_DICTIONARY_COLUMNGRP_070", Include) {
  sql(s"""select contractNumber from VMALL_DICTIONARY_COLUMNGRP where contractNumber >9223372047700 order by contractNumber""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_071
test("VMALL_DICTIONARY_COLUMNGRP_071", Include) {
  sql(s"""select contractNumber  from VMALL_DICTIONARY_COLUMNGRP where contractNumber<>contractNumber""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_072
test("VMALL_DICTIONARY_COLUMNGRP_072", Include) {
  sql(s"""select contractNumber from VMALL_DICTIONARY_COLUMNGRP where contractNumber != Latest_areaId order by contractNumber""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_073
test("VMALL_DICTIONARY_COLUMNGRP_073", Include) {
  sql(s"""select contractNumber, contractNumber from VMALL_DICTIONARY_COLUMNGRP where Latest_areaId<contractNumber order by contractNumber""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_074
test("VMALL_DICTIONARY_COLUMNGRP_074", Include) {
  sql(s"""select contractNumber, contractNumber from VMALL_DICTIONARY_COLUMNGRP where Latest_DAY<=contractNumber order by contractNumber""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_075
test("VMALL_DICTIONARY_COLUMNGRP_075", Include) {
  sql(s"""select contractNumber from VMALL_DICTIONARY_COLUMNGRP where contractNumber <1000 order by contractNumber""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_076
test("VMALL_DICTIONARY_COLUMNGRP_076", Include) {
  sql(s"""select contractNumber from VMALL_DICTIONARY_COLUMNGRP where contractNumber >1000 order by contractNumber""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_077
test("VMALL_DICTIONARY_COLUMNGRP_077", Include) {
  sql(s"""select contractNumber  from VMALL_DICTIONARY_COLUMNGRP where contractNumber IS NULL order by contractNumber""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_078
test("VMALL_DICTIONARY_COLUMNGRP_078", Include) {
  sql(s"""select contractNumber  from VMALL_DICTIONARY_COLUMNGRP where Latest_DAY IS NOT NULL order by contractNumber""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_079
test("VMALL_DICTIONARY_COLUMNGRP_079", Include) {
  sql(s"""Select count(Latest_DAY) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_080
test("VMALL_DICTIONARY_COLUMNGRP_080", Include) {
  sql(s"""select count(DISTINCT Latest_DAY) as a from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_081
test("VMALL_DICTIONARY_COLUMNGRP_081", Include) {
  sql(s"""select sum(Latest_DAY)+10 as a ,Latest_DAY  from VMALL_DICTIONARY_COLUMNGRP group by Latest_DAY order by a""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_082
test("VMALL_DICTIONARY_COLUMNGRP_082", Include) {
  sql(s"""select max(Latest_DAY),min(Latest_DAY) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_083
test("VMALL_DICTIONARY_COLUMNGRP_083", Include) {
  sql(s"""select sum(Latest_DAY) a  from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_084
test("VMALL_DICTIONARY_COLUMNGRP_084", Include) {
  sql(s"""select avg(Latest_DAY) a  from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_085
test("VMALL_DICTIONARY_COLUMNGRP_085", Include) {
  sql(s"""select min(Latest_DAY) a  from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_086
test("VMALL_DICTIONARY_COLUMNGRP_086", Include) {
  sql(s"""select variance(Latest_DAY) as a   from (select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_087
test("VMALL_DICTIONARY_COLUMNGRP_087", Include) {
  sql(s"""select var_pop(Latest_DAY)  as a from (select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_088
test("VMALL_DICTIONARY_COLUMNGRP_088", Include) {
  sql(s"""select var_samp(Latest_DAY) as a  from (select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_089
test("VMALL_DICTIONARY_COLUMNGRP_089", Include) {
  sql(s"""select stddev_pop(Latest_DAY) as a  from (select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_090
test("VMALL_DICTIONARY_COLUMNGRP_090", Include) {
  sql(s"""select stddev_samp(Latest_DAY)  as a from (select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_091
test("VMALL_DICTIONARY_COLUMNGRP_091", Include) {
  sql(s"""select covar_pop(Latest_DAY,Latest_DAY) as a  from (select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_092
test("VMALL_DICTIONARY_COLUMNGRP_092", Include) {
  sql(s"""select covar_samp(Latest_DAY,Latest_DAY) as a  from (select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_093
test("VMALL_DICTIONARY_COLUMNGRP_093", Include) {
  sql(s"""select corr(Latest_DAY,Latest_DAY)  as a from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_094
test("VMALL_DICTIONARY_COLUMNGRP_094", Include) {
  sql(s"""select percentile_approx(Latest_DAY,0.2) as a  from (select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_095
test("VMALL_DICTIONARY_COLUMNGRP_095", Include) {
  sql(s"""select percentile_approx(Latest_DAY,0.2,5) as a  from (select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_096
test("VMALL_DICTIONARY_COLUMNGRP_096", Include) {
  sql(s"""select percentile_approx(Latest_DAY,array(0.2,0.3,0.99))  as a from (select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_097
test("VMALL_DICTIONARY_COLUMNGRP_097", Include) {
  sql(s"""select percentile_approx(Latest_DAY,array(0.2,0.3,0.99),5) as a from (select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_098
test("VMALL_DICTIONARY_COLUMNGRP_098", Include) {
  sql(s"""select histogram_numeric(Latest_DAY,2)  as a from (select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_099
test("VMALL_DICTIONARY_COLUMNGRP_099", Include) {
  sql(s"""select Latest_DAY, Latest_DAY+ 10 as a  from VMALL_DICTIONARY_COLUMNGRP order by a""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_100
test("VMALL_DICTIONARY_COLUMNGRP_100", Include) {
  sql(s"""select min(Latest_DAY), max(Latest_DAY+ 10) Total from VMALL_DICTIONARY_COLUMNGRP group by  channelsId order by Total""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_101
test("VMALL_DICTIONARY_COLUMNGRP_101", Include) {
  sql(s"""select last(Latest_DAY) a from VMALL_DICTIONARY_COLUMNGRP order by a""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_102
test("VMALL_DICTIONARY_COLUMNGRP_102", Include) {
  sql(s"""select FIRST(Latest_DAY) a from (select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP order by Latest_DAY) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_103
test("VMALL_DICTIONARY_COLUMNGRP_103", Include) {
  sql(s"""select Latest_DAY,count(Latest_DAY) a from VMALL_DICTIONARY_COLUMNGRP group by Latest_DAY order by Latest_DAY""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_104
test("VMALL_DICTIONARY_COLUMNGRP_104", Include) {
  sql(s"""select Lower(Latest_DAY) a  from VMALL_DICTIONARY_COLUMNGRP order by a""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_105
test("VMALL_DICTIONARY_COLUMNGRP_105", Include) {
  sql(s"""select distinct Latest_DAY from VMALL_DICTIONARY_COLUMNGRP order by Latest_DAY""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_106
test("VMALL_DICTIONARY_COLUMNGRP_106", Include) {
  sql(s"""select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP order by Latest_DAY limit 101""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_107
test("VMALL_DICTIONARY_COLUMNGRP_107", Include) {
  sql(s"""select Latest_DAY as a from VMALL_DICTIONARY_COLUMNGRP  order by a asc limit 10""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_108
test("VMALL_DICTIONARY_COLUMNGRP_108", Include) {
  sql(s"""select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP where  (Latest_DAY == 1234567890123450.0000000000)  and (imei=='1AA1')""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_109
test("VMALL_DICTIONARY_COLUMNGRP_109", Include) {
  sql(s"""select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_110
test("VMALL_DICTIONARY_COLUMNGRP_110", Include) {
  sql(s"""select Latest_DAY  from VMALL_DICTIONARY_COLUMNGRP where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_111
test("VMALL_DICTIONARY_COLUMNGRP_111", Include) {
  sql(s"""select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_112
test("VMALL_DICTIONARY_COLUMNGRP_112", Include) {
  sql(s"""select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP where Latest_DAY >1234567890123450.0000000000  order by Latest_DAY""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_113
test("VMALL_DICTIONARY_COLUMNGRP_113", Include) {
  sql(s"""select Latest_DAY  from VMALL_DICTIONARY_COLUMNGRP where Latest_DAY<>Latest_DAY""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_114
test("VMALL_DICTIONARY_COLUMNGRP_114", Include) {
  sql(s"""select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP where Latest_DAY != Latest_areaId order by Latest_DAY""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_115
test("VMALL_DICTIONARY_COLUMNGRP_115", Include) {
  sql(s"""select Latest_DAY, Latest_DAY from VMALL_DICTIONARY_COLUMNGRP where Latest_areaId<Latest_DAY order by Latest_DAY""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_116
test("VMALL_DICTIONARY_COLUMNGRP_116", Include) {
  sql(s"""select Latest_DAY, Latest_DAY from VMALL_DICTIONARY_COLUMNGRP where Latest_DAY<=Latest_DAY  order by Latest_DAY""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_117
test("VMALL_DICTIONARY_COLUMNGRP_117", Include) {
  sql(s"""select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP where Latest_DAY <1000  order by Latest_DAY""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_118
test("VMALL_DICTIONARY_COLUMNGRP_118", Include) {
  sql(s"""select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP where Latest_DAY >1000  order by Latest_DAY""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_119
test("VMALL_DICTIONARY_COLUMNGRP_119", Include) {
  sql(s"""select Latest_DAY  from VMALL_DICTIONARY_COLUMNGRP where Latest_DAY IS NULL  order by Latest_DAY""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_120
test("VMALL_DICTIONARY_COLUMNGRP_120", Include) {
  sql(s"""select Latest_DAY  from VMALL_DICTIONARY_COLUMNGRP where Latest_DAY IS NOT NULL  order by Latest_DAY""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_121
test("VMALL_DICTIONARY_COLUMNGRP_121", Include) {
  sql(s"""Select count(gamePointId) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_122
test("VMALL_DICTIONARY_COLUMNGRP_122", Include) {
  sql(s"""select count(DISTINCT gamePointId) as a from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_123
test("VMALL_DICTIONARY_COLUMNGRP_123", Include) {
  sql(s"""select sum(gamePointId)+10 as a ,gamePointId  from VMALL_DICTIONARY_COLUMNGRP group by gamePointId order by a""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_124
test("VMALL_DICTIONARY_COLUMNGRP_124", Include) {
  sql(s"""select max(gamePointId),min(gamePointId) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_125
test("VMALL_DICTIONARY_COLUMNGRP_125", Include) {
  sql(s"""select sum(gamePointId) a  from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_126
test("VMALL_DICTIONARY_COLUMNGRP_126", Include) {
  sql(s"""select avg(gamePointId) a  from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_127
test("VMALL_DICTIONARY_COLUMNGRP_127", Include) {
  sql(s"""select min(gamePointId) a  from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_128
test("VMALL_DICTIONARY_COLUMNGRP_128", Include) {
  sql(s"""select variance(gamePointId) as a   from (select gamePointId from VMALL_DICTIONARY_COLUMNGRP order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_129
test("VMALL_DICTIONARY_COLUMNGRP_129", Include) {
  sql(s"""select var_pop(gamePointId)  as a from (select gamePointId from VMALL_DICTIONARY_COLUMNGRP order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_130
test("VMALL_DICTIONARY_COLUMNGRP_130", Include) {
  sql(s"""select var_samp(gamePointId) as a  from (select gamePointId from VMALL_DICTIONARY_COLUMNGRP order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_131
test("VMALL_DICTIONARY_COLUMNGRP_131", Include) {
  sql(s"""select stddev_pop(gamePointId) as a  from (select gamePointId from VMALL_DICTIONARY_COLUMNGRP order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_132
test("VMALL_DICTIONARY_COLUMNGRP_132", Include) {
  sql(s"""select stddev_samp(gamePointId)  as a from (select gamePointId from VMALL_DICTIONARY_COLUMNGRP order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_133
test("VMALL_DICTIONARY_COLUMNGRP_133", Include) {
  sql(s"""select covar_pop(gamePointId,gamePointId) as a  from (select gamePointId from VMALL_DICTIONARY_COLUMNGRP order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_134
test("VMALL_DICTIONARY_COLUMNGRP_134", Include) {
  sql(s"""select covar_samp(gamePointId,gamePointId) as a  from (select gamePointId from VMALL_DICTIONARY_COLUMNGRP order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_135
test("VMALL_DICTIONARY_COLUMNGRP_135", Include) {
  sql(s"""select corr(gamePointId,gamePointId)  as a from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_136
test("VMALL_DICTIONARY_COLUMNGRP_136", Include) {
  sql(s"""select percentile_approx(gamePointId,0.2) as a  from (select gamePointId from VMALL_DICTIONARY_COLUMNGRP order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_137
test("VMALL_DICTIONARY_COLUMNGRP_137", Include) {
  sql(s"""select percentile_approx(gamePointId,0.2,5) as a  from (select gamePointId from VMALL_DICTIONARY_COLUMNGRP order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_138
test("VMALL_DICTIONARY_COLUMNGRP_138", Include) {
  sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99))  as a from (select gamePointId from VMALL_DICTIONARY_COLUMNGRP order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_139
test("VMALL_DICTIONARY_COLUMNGRP_139", Include) {
  sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99),5) as a from (select gamePointId from VMALL_DICTIONARY_COLUMNGRP order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_140
test("VMALL_DICTIONARY_COLUMNGRP_140", Include) {
  sql(s"""select histogram_numeric(gamePointId,2)  as a from (select gamePointId from VMALL_DICTIONARY_COLUMNGRP order by gamePointId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_141
test("VMALL_DICTIONARY_COLUMNGRP_141", Include) {
  sql(s"""select gamePointId, gamePointId+ 10 as a  from VMALL_DICTIONARY_COLUMNGRP order by a""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_142
test("VMALL_DICTIONARY_COLUMNGRP_142", Include) {
  sql(s"""select min(gamePointId), max(gamePointId+ 10) Total from VMALL_DICTIONARY_COLUMNGRP group by  channelsId order by Total""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_143
test("VMALL_DICTIONARY_COLUMNGRP_143", Include) {
  sql(s"""select last(gamePointId) a from VMALL_DICTIONARY_COLUMNGRP order by a""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_144
test("VMALL_DICTIONARY_COLUMNGRP_144", Include) {
  sql(s"""select FIRST(gamePointId) a from VMALL_DICTIONARY_COLUMNGRP order by a""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_145
test("VMALL_DICTIONARY_COLUMNGRP_145", Include) {
  sql(s"""select gamePointId,count(gamePointId) a from VMALL_DICTIONARY_COLUMNGRP group by gamePointId order by gamePointId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_146
test("VMALL_DICTIONARY_COLUMNGRP_146", Include) {
  sql(s"""select Lower(gamePointId) a  from VMALL_DICTIONARY_COLUMNGRP order by gamePointId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_147
test("VMALL_DICTIONARY_COLUMNGRP_147", Include) {
  sql(s"""select distinct gamePointId from VMALL_DICTIONARY_COLUMNGRP order by gamePointId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_148
test("VMALL_DICTIONARY_COLUMNGRP_148", Include) {
  sql(s"""select gamePointId from VMALL_DICTIONARY_COLUMNGRP  order by gamePointId limit 101""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_149
test("VMALL_DICTIONARY_COLUMNGRP_149", Include) {
  sql(s"""select gamePointId as a from VMALL_DICTIONARY_COLUMNGRP  order by a asc limit 10""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_150
test("VMALL_DICTIONARY_COLUMNGRP_150", Include) {
  sql(s"""select gamePointId from VMALL_DICTIONARY_COLUMNGRP where  (gamePointId == 4.70133553923674E43) and (imei=='1AA100084')""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_151
test("VMALL_DICTIONARY_COLUMNGRP_151", Include) {
  sql(s"""select gamePointId from VMALL_DICTIONARY_COLUMNGRP where gamePointId !=4.70133553923674E43  order by gamePointId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_152
test("VMALL_DICTIONARY_COLUMNGRP_152", Include) {
  sql(s"""select gamePointId  from VMALL_DICTIONARY_COLUMNGRP where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_153
test("VMALL_DICTIONARY_COLUMNGRP_153", Include) {
  sql(s"""select gamePointId from VMALL_DICTIONARY_COLUMNGRP where gamePointId !=4.70133553923674E43""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_154
test("VMALL_DICTIONARY_COLUMNGRP_154", Include) {
  sql(s"""select gamePointId from VMALL_DICTIONARY_COLUMNGRP where gamePointId >4.70133553923674E43""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_155
test("VMALL_DICTIONARY_COLUMNGRP_155", Include) {
  sql(s"""select gamePointId  from VMALL_DICTIONARY_COLUMNGRP where gamePointId<>gamePointId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_156
test("VMALL_DICTIONARY_COLUMNGRP_156", Include) {
  sql(s"""select gamePointId from VMALL_DICTIONARY_COLUMNGRP where gamePointId != Latest_areaId  order by gamePointId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_157
test("VMALL_DICTIONARY_COLUMNGRP_157", Include) {
  sql(s"""select gamePointId, gamePointId from VMALL_DICTIONARY_COLUMNGRP where Latest_areaId<gamePointId  order by gamePointId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_158
test("VMALL_DICTIONARY_COLUMNGRP_158", Include) {
  sql(s"""select gamePointId, gamePointId from VMALL_DICTIONARY_COLUMNGRP where gamePointId<=gamePointId  order by gamePointId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_159
test("VMALL_DICTIONARY_COLUMNGRP_159", Include) {
  sql(s"""select gamePointId from VMALL_DICTIONARY_COLUMNGRP where gamePointId <1000 order by gamePointId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_160
test("VMALL_DICTIONARY_COLUMNGRP_160", Include) {
  sql(s"""select gamePointId from VMALL_DICTIONARY_COLUMNGRP where gamePointId >1000 order by gamePointId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_161
test("VMALL_DICTIONARY_COLUMNGRP_161", Include) {
  sql(s"""select gamePointId  from VMALL_DICTIONARY_COLUMNGRP where gamePointId IS NULL order by gamePointId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_162
test("VMALL_DICTIONARY_COLUMNGRP_162", Include) {
  sql(s"""select gamePointId  from VMALL_DICTIONARY_COLUMNGRP where gamePointId IS NOT NULL order by gamePointId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_163
test("VMALL_DICTIONARY_COLUMNGRP_163", Include) {
  sql(s"""Select count(productionDate) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_164
test("VMALL_DICTIONARY_COLUMNGRP_164", Include) {
  sql(s"""select count(DISTINCT productionDate) as a from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_165
test("VMALL_DICTIONARY_COLUMNGRP_165", Include) {
  sql(s"""select sum(productionDate)+10 as a ,productionDate  from VMALL_DICTIONARY_COLUMNGRP group by productionDate order by productionDate""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_166
test("VMALL_DICTIONARY_COLUMNGRP_166", Include) {
  sql(s"""select max(productionDate),min(productionDate) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_167
test("VMALL_DICTIONARY_COLUMNGRP_167", Include) {
  sql(s"""select sum(productionDate) a  from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_168
test("VMALL_DICTIONARY_COLUMNGRP_168", Include) {
  sql(s"""select avg(productionDate) a  from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_169
test("VMALL_DICTIONARY_COLUMNGRP_169", Include) {
  sql(s"""select min(productionDate) a  from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_183
test("VMALL_DICTIONARY_COLUMNGRP_183", Include) {
  sql(s"""select last(productionDate) a from VMALL_DICTIONARY_COLUMNGRP order by a""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_184
test("VMALL_DICTIONARY_COLUMNGRP_184", Include) {
  sql(s"""select FIRST(productionDate) a from (select productionDate from VMALL_DICTIONARY_COLUMNGRP order by productionDate) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_185
test("VMALL_DICTIONARY_COLUMNGRP_185", Include) {
  sql(s"""select productionDate,count(productionDate) a from VMALL_DICTIONARY_COLUMNGRP group by productionDate order by productionDate""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_186
test("VMALL_DICTIONARY_COLUMNGRP_186", Include) {
  sql(s"""select Lower(productionDate) a  from VMALL_DICTIONARY_COLUMNGRP order by productionDate""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_187
test("VMALL_DICTIONARY_COLUMNGRP_187", Include) {
  sql(s"""select distinct productionDate from VMALL_DICTIONARY_COLUMNGRP order by productionDate""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_188
test("VMALL_DICTIONARY_COLUMNGRP_188", Include) {
  sql(s"""select productionDate from VMALL_DICTIONARY_COLUMNGRP order by productionDate limit 101""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_189
test("VMALL_DICTIONARY_COLUMNGRP_189", Include) {
  sql(s"""select productionDate as a from VMALL_DICTIONARY_COLUMNGRP  order by a asc limit 10""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_190
test("VMALL_DICTIONARY_COLUMNGRP_190", Include) {
  sql(s"""select productionDate from VMALL_DICTIONARY_COLUMNGRP where  (productionDate == '2015-07-01 12:07:28.0') and (productionDate=='2015-07-01 12:07:28.0')""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_191
test("VMALL_DICTIONARY_COLUMNGRP_191", Include) {
  sql(s"""select productionDate from VMALL_DICTIONARY_COLUMNGRP where productionDate !='2015-07-01 12:07:28.0' order by productionDate""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_192
test("VMALL_DICTIONARY_COLUMNGRP_192", Include) {
  sql(s"""select productionDate  from VMALL_DICTIONARY_COLUMNGRP where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_193
test("VMALL_DICTIONARY_COLUMNGRP_193", Include) {
  sql(s"""select productionDate from VMALL_DICTIONARY_COLUMNGRP where productionDate !='2015-07-01 12:07:28.0' order by productionDate""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_194
test("VMALL_DICTIONARY_COLUMNGRP_194", Include) {
  sql(s"""select productionDate from VMALL_DICTIONARY_COLUMNGRP where productionDate >'2015-07-01 12:07:28.0' order by productionDate""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_195
test("VMALL_DICTIONARY_COLUMNGRP_195", Include) {
  sql(s"""select productionDate  from VMALL_DICTIONARY_COLUMNGRP where productionDate<>productionDate order by productionDate""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_196
test("VMALL_DICTIONARY_COLUMNGRP_196", Include) {
  sql(s"""select productionDate from VMALL_DICTIONARY_COLUMNGRP where productionDate != Latest_areaId order by productionDate""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_197
test("VMALL_DICTIONARY_COLUMNGRP_197", Include) {
  sql(s"""select productionDate from VMALL_DICTIONARY_COLUMNGRP where Latest_areaId<productionDate order by productionDate""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_198
test("VMALL_DICTIONARY_COLUMNGRP_198", Include) {
  sql(s"""select productionDate from VMALL_DICTIONARY_COLUMNGRP where productionDate<=productionDate order by productionDate""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_200
test("VMALL_DICTIONARY_COLUMNGRP_200", Include) {
  sql(s"""select productionDate  from VMALL_DICTIONARY_COLUMNGRP where productionDate IS NULL""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_201
test("VMALL_DICTIONARY_COLUMNGRP_201", Include) {
  sql(s"""select productionDate  from VMALL_DICTIONARY_COLUMNGRP where productionDate IS NOT NULL order by productionDate""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_202
test("VMALL_DICTIONARY_COLUMNGRP_202", Include) {
  sql(s"""Select count(deviceInformationId) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_203
test("VMALL_DICTIONARY_COLUMNGRP_203", Include) {
  sql(s"""select count(DISTINCT deviceInformationId) as a from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_204
test("VMALL_DICTIONARY_COLUMNGRP_204", Include) {
  sql(s"""select sum(deviceInformationId)+10 as a ,deviceInformationId  from VMALL_DICTIONARY_COLUMNGRP group by deviceInformationId order by deviceInformationId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_205
test("VMALL_DICTIONARY_COLUMNGRP_205", Include) {
  sql(s"""select max(deviceInformationId),min(deviceInformationId) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_206
test("VMALL_DICTIONARY_COLUMNGRP_206", Include) {
  sql(s"""select sum(deviceInformationId) a  from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_207
test("VMALL_DICTIONARY_COLUMNGRP_207", Include) {
  sql(s"""select avg(deviceInformationId) a  from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_208
test("VMALL_DICTIONARY_COLUMNGRP_208", Include) {
  sql(s"""select min(deviceInformationId) a  from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_209
test("VMALL_DICTIONARY_COLUMNGRP_209", Include) {
  sql(s"""select variance(deviceInformationId) as a   from (select deviceInformationId from VMALL_DICTIONARY_COLUMNGRP order by deviceInformationId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_210
test("VMALL_DICTIONARY_COLUMNGRP_210", Include) {
  sql(s"""select var_pop(deviceInformationId)  as a from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_211
test("VMALL_DICTIONARY_COLUMNGRP_211", Include) {
  sql(s"""select var_samp(deviceInformationId) as a  from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_212
test("VMALL_DICTIONARY_COLUMNGRP_212", Include) {
  sql(s"""select stddev_pop(deviceInformationId) as a  from (select deviceInformationId from VMALL_DICTIONARY_COLUMNGRP order by deviceInformationId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_213
test("VMALL_DICTIONARY_COLUMNGRP_213", Include) {
  sql(s"""select stddev_samp(deviceInformationId)  as a from (select deviceInformationId from VMALL_DICTIONARY_COLUMNGRP order by deviceInformationId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_214
test("VMALL_DICTIONARY_COLUMNGRP_214", Include) {
  sql(s"""select covar_pop(deviceInformationId,deviceInformationId) as a  from (select deviceInformationId from VMALL_DICTIONARY_COLUMNGRP order by deviceInformationId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_215
test("VMALL_DICTIONARY_COLUMNGRP_215", Include) {
  sql(s"""select covar_samp(deviceInformationId,deviceInformationId) as a  from (select deviceInformationId from VMALL_DICTIONARY_COLUMNGRP order by deviceInformationId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_216
test("VMALL_DICTIONARY_COLUMNGRP_216", Include) {
  sql(s"""select corr(deviceInformationId,deviceInformationId)  as a from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_217
test("VMALL_DICTIONARY_COLUMNGRP_217", Include) {
  sql(s"""select percentile_approx(deviceInformationId,0.2) as a  from (select deviceInformationId from VMALL_DICTIONARY_COLUMNGRP order by deviceInformationId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_218
test("VMALL_DICTIONARY_COLUMNGRP_218", Include) {
  sql(s"""select percentile_approx(deviceInformationId,0.2,5) as a  from (select deviceInformationId from VMALL_DICTIONARY_COLUMNGRP order by deviceInformationId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_219
test("VMALL_DICTIONARY_COLUMNGRP_219", Include) {
  sql(s"""select percentile_approx(deviceInformationId,array(0.2,0.3,0.99))  as a from (select deviceInformationId from VMALL_DICTIONARY_COLUMNGRP order by deviceInformationId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_220
test("VMALL_DICTIONARY_COLUMNGRP_220", Include) {
  sql(s"""select percentile_approx(deviceInformationId,array(0.2,0.3,0.99),5) as a from (select deviceInformationId from VMALL_DICTIONARY_COLUMNGRP order by deviceInformationId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_221
test("VMALL_DICTIONARY_COLUMNGRP_221", Include) {
  sql(s"""select histogram_numeric(deviceInformationId,2)  as a from (select deviceInformationId from VMALL_DICTIONARY_COLUMNGRP order by deviceInformationId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_222
test("VMALL_DICTIONARY_COLUMNGRP_222", Include) {
  sql(s"""select deviceInformationId, deviceInformationId+ 10 as a  from VMALL_DICTIONARY_COLUMNGRP order by deviceInformationId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_223
test("VMALL_DICTIONARY_COLUMNGRP_223", Include) {
  sql(s"""select min(deviceInformationId), max(deviceInformationId+ 10) Total from VMALL_DICTIONARY_COLUMNGRP group by  channelsId order by Total""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_224
test("VMALL_DICTIONARY_COLUMNGRP_224", Include) {
  sql(s"""select last(deviceInformationId) a from (select deviceInformationId from VMALL_DICTIONARY_COLUMNGRP order by deviceInformationId) t""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_225
test("VMALL_DICTIONARY_COLUMNGRP_225", Include) {
  sql(s"""select FIRST(deviceInformationId) a from VMALL_DICTIONARY_COLUMNGRP order by a""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_226
test("VMALL_DICTIONARY_COLUMNGRP_226", Include) {
  sql(s"""select deviceInformationId,count(deviceInformationId) a from VMALL_DICTIONARY_COLUMNGRP group by deviceInformationId order by deviceInformationId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_227
test("VMALL_DICTIONARY_COLUMNGRP_227", Include) {
  sql(s"""select Lower(deviceInformationId) a  from VMALL_DICTIONARY_COLUMNGRP order by deviceInformationId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_228
test("VMALL_DICTIONARY_COLUMNGRP_228", Include) {
  sql(s"""select distinct deviceInformationId from VMALL_DICTIONARY_COLUMNGRP order by deviceInformationId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_229
test("VMALL_DICTIONARY_COLUMNGRP_229", Include) {
  sql(s"""select deviceInformationId from VMALL_DICTIONARY_COLUMNGRP order by deviceInformationId limit 101""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_230
test("VMALL_DICTIONARY_COLUMNGRP_230", Include) {
  sql(s"""select deviceInformationId as a from VMALL_DICTIONARY_COLUMNGRP  order by a asc limit 10""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_231
test("VMALL_DICTIONARY_COLUMNGRP_231", Include) {
  sql(s"""select deviceInformationId from VMALL_DICTIONARY_COLUMNGRP where  (deviceInformationId == 100084) and (deviceInformationId==100084)""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_232
test("VMALL_DICTIONARY_COLUMNGRP_232", Include) {
  sql(s"""select deviceInformationId from VMALL_DICTIONARY_COLUMNGRP where deviceInformationId !='100084' order by deviceInformationId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_233
test("VMALL_DICTIONARY_COLUMNGRP_233", Include) {
  sql(s"""select deviceInformationId  from VMALL_DICTIONARY_COLUMNGRP where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_234
test("VMALL_DICTIONARY_COLUMNGRP_234", Include) {
  sql(s"""select deviceInformationId from VMALL_DICTIONARY_COLUMNGRP where deviceInformationId !=100084 order by deviceInformationId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_235
test("VMALL_DICTIONARY_COLUMNGRP_235", Include) {
  sql(s"""select deviceInformationId from VMALL_DICTIONARY_COLUMNGRP where deviceInformationId >100084 order by deviceInformationId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_236
test("VMALL_DICTIONARY_COLUMNGRP_236", Include) {
  sql(s"""select deviceInformationId  from VMALL_DICTIONARY_COLUMNGRP where deviceInformationId<>deviceInformationId order by deviceInformationId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_237
test("VMALL_DICTIONARY_COLUMNGRP_237", Include) {
  sql(s"""select deviceInformationId from VMALL_DICTIONARY_COLUMNGRP where deviceInformationId != Latest_areaId order by deviceInformationId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_238
test("VMALL_DICTIONARY_COLUMNGRP_238", Include) {
  sql(s"""select deviceInformationId, deviceInformationId from VMALL_DICTIONARY_COLUMNGRP where Latest_areaId<deviceInformationId order by deviceInformationId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_239
test("VMALL_DICTIONARY_COLUMNGRP_239", Include) {
  sql(s"""select deviceInformationId, deviceInformationId from VMALL_DICTIONARY_COLUMNGRP where deviceInformationId<=deviceInformationId order by deviceInformationId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_240
test("VMALL_DICTIONARY_COLUMNGRP_240", Include) {
  sql(s"""select deviceInformationId from VMALL_DICTIONARY_COLUMNGRP where deviceInformationId <1000 order by deviceInformationId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_241
test("VMALL_DICTIONARY_COLUMNGRP_241", Include) {
  sql(s"""select deviceInformationId from VMALL_DICTIONARY_COLUMNGRP where deviceInformationId >1000 order by deviceInformationId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_242
test("VMALL_DICTIONARY_COLUMNGRP_242", Include) {
  sql(s"""select deviceInformationId  from VMALL_DICTIONARY_COLUMNGRP where deviceInformationId IS NULL order by deviceInformationId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_243
test("VMALL_DICTIONARY_COLUMNGRP_243", Include) {
  sql(s"""select deviceInformationId  from VMALL_DICTIONARY_COLUMNGRP where deviceInformationId IS NOT NULL order by deviceInformationId""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_244
test("VMALL_DICTIONARY_COLUMNGRP_244", Include) {
  sql(s"""select sum(imei)+10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_245
test("VMALL_DICTIONARY_COLUMNGRP_245", Include) {
  sql(s"""select sum(imei)*10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_246
test("VMALL_DICTIONARY_COLUMNGRP_246", Include) {
  sql(s"""select sum(imei)/10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_247
test("VMALL_DICTIONARY_COLUMNGRP_247", Include) {
  sql(s"""select sum(imei)-10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_248
test("VMALL_DICTIONARY_COLUMNGRP_248", Include) {
  sql(s"""select sum(contractNumber)+10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_249
test("VMALL_DICTIONARY_COLUMNGRP_249", Include) {
  sql(s"""select sum(contractNumber)*10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_250
test("VMALL_DICTIONARY_COLUMNGRP_250", Include) {
  sql(s"""select sum(contractNumber)/10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_251
test("VMALL_DICTIONARY_COLUMNGRP_251", Include) {
  sql(s"""select sum(contractNumber)-10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_252
test("VMALL_DICTIONARY_COLUMNGRP_252", Include) {
  sql(s"""select sum(Latest_DAY)+10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_253
test("VMALL_DICTIONARY_COLUMNGRP_253", Include) {
  sql(s"""select sum(Latest_DAY)*10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_254
test("VMALL_DICTIONARY_COLUMNGRP_254", Include) {
  sql(s"""select sum(Latest_DAY)/10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_255
test("VMALL_DICTIONARY_COLUMNGRP_255", Include) {
  sql(s"""select sum(Latest_DAY)-10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_256
test("VMALL_DICTIONARY_COLUMNGRP_256", Include) {
  sql(s"""select sum(gamePointId)+10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_257
test("VMALL_DICTIONARY_COLUMNGRP_257", Include) {
  sql(s"""select sum(gamePointId)*10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_258
test("VMALL_DICTIONARY_COLUMNGRP_258", Include) {
  sql(s"""select sum(gamePointId)/10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_259
test("VMALL_DICTIONARY_COLUMNGRP_259", Include) {
  sql(s"""select sum(gamePointId)-10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_260
test("VMALL_DICTIONARY_COLUMNGRP_260", Include) {
  sql(s"""select sum(productionDate)+10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_261
test("VMALL_DICTIONARY_COLUMNGRP_261", Include) {
  sql(s"""select sum(productionDate)*10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_262
test("VMALL_DICTIONARY_COLUMNGRP_262", Include) {
  sql(s"""select sum(productionDate)/10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_263
test("VMALL_DICTIONARY_COLUMNGRP_263", Include) {
  sql(s"""select sum(productionDate)-10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_264
test("VMALL_DICTIONARY_COLUMNGRP_264", Include) {
  sql(s"""select sum(deviceInformationId)+10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_265
test("VMALL_DICTIONARY_COLUMNGRP_265", Include) {
  sql(s"""select sum(deviceInformationId)*10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_266
test("VMALL_DICTIONARY_COLUMNGRP_266", Include) {
  sql(s"""select sum(deviceInformationId)/10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_267
test("VMALL_DICTIONARY_COLUMNGRP_267", Include) {
  sql(s"""select sum(deviceInformationId)-10 as a   from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_292
test("VMALL_DICTIONARY_COLUMNGRP_292", Include) {
  sql(s"""SELECT productiondate from VMALL_DICTIONARY_COLUMNGRP where productiondate LIKE '2015-09-30%'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_293
test("VMALL_DICTIONARY_COLUMNGRP_293", Include) {
  sql(s"""SELECT productiondate from VMALL_DICTIONARY_COLUMNGRP where productiondate LIKE '% %'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_294
test("VMALL_DICTIONARY_COLUMNGRP_294", Include) {
  sql(s"""SELECT productiondate from VMALL_DICTIONARY_COLUMNGRP where productiondate LIKE '%12:07:28'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_295
test("VMALL_DICTIONARY_COLUMNGRP_295", Include) {
  sql(s"""select contractnumber from VMALL_DICTIONARY_COLUMNGRP where contractnumber like '922337204%' """).collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_296
test("VMALL_DICTIONARY_COLUMNGRP_296", Include) {
  sql(s"""select contractnumber from VMALL_DICTIONARY_COLUMNGRP where contractnumber like '%047800'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_297
test("VMALL_DICTIONARY_COLUMNGRP_297", Include) {
  sql(s"""select contractnumber from VMALL_DICTIONARY_COLUMNGRP where contractnumber like '%720%'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_298
test("VMALL_DICTIONARY_COLUMNGRP_298", Include) {
  sql(s"""SELECT Latest_DAY from VMALL_DICTIONARY_COLUMNGRP where Latest_DAY like '12345678%'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_299
test("VMALL_DICTIONARY_COLUMNGRP_299", Include) {
  sql(s"""SELECT Latest_DAY from VMALL_DICTIONARY_COLUMNGRP where Latest_DAY like '%5678%'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_300
test("VMALL_DICTIONARY_COLUMNGRP_300", Include) {
  sql(s"""SELECT Latest_DAY from VMALL_DICTIONARY_COLUMNGRP where Latest_DAY like '1234567%'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_301
test("VMALL_DICTIONARY_COLUMNGRP_301", Include) {
  sql(s"""SELECT gamepointID from VMALL_DICTIONARY_COLUMNGRP where gamepointID like '1.1098347722%'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_302
test("VMALL_DICTIONARY_COLUMNGRP_302", Include) {
  sql(s"""SELECT gamepointID from VMALL_DICTIONARY_COLUMNGRP where gamepointID like '%8347722%'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_303
test("VMALL_DICTIONARY_COLUMNGRP_303", Include) {
  sql(s"""SELECT gamepointID from VMALL_DICTIONARY_COLUMNGRP where gamepointID like '%7512E42'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_304
test("VMALL_DICTIONARY_COLUMNGRP_304", Include) {
  sql(s"""SELECT deviceinformationid from VMALL_DICTIONARY_COLUMNGRP where deviceinformationid like '1000%'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_305
test("VMALL_DICTIONARY_COLUMNGRP_305", Include) {
  sql(s"""SELECT deviceinformationid from VMALL_DICTIONARY_COLUMNGRP where deviceinformationid like '%00%'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_306
test("VMALL_DICTIONARY_COLUMNGRP_306", Include) {
  sql(s"""SELECT deviceinformationid from VMALL_DICTIONARY_COLUMNGRP where deviceinformationid like '%0084'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_307
test("VMALL_DICTIONARY_COLUMNGRP_307", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_COLUMNGRP where imei like '1AA10%'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_308
test("VMALL_DICTIONARY_COLUMNGRP_308", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_COLUMNGRP where imei like '%A10%'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_309
test("VMALL_DICTIONARY_COLUMNGRP_309", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_COLUMNGRP where imei like '%00084'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_310
test("VMALL_DICTIONARY_COLUMNGRP_310", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_COLUMNGRP where imei in ('1AA100074','1AA100075','1AA100077')""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_311
test("VMALL_DICTIONARY_COLUMNGRP_311", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_COLUMNGRP where imei not in ('1AA100074','1AA100075','1AA100077')""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_312
test("VMALL_DICTIONARY_COLUMNGRP_312", Include) {
  sql(s"""select deviceinformationid from VMALL_DICTIONARY_COLUMNGRP where deviceinformationid in (100081,100078,10008)""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_313
test("VMALL_DICTIONARY_COLUMNGRP_313", Include) {
  sql(s"""select deviceinformationid from VMALL_DICTIONARY_COLUMNGRP where deviceinformationid not in (100081,100078,10008)""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_314
test("VMALL_DICTIONARY_COLUMNGRP_314", Include) {
  sql(s"""select productiondate from VMALL_DICTIONARY_COLUMNGRP where productiondate in ('2015-10-04 12:07:28','2015-10-07%','2015-10-07 12:07:28')""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_315
test("VMALL_DICTIONARY_COLUMNGRP_315", Include) {
  sql(s"""select productiondate from VMALL_DICTIONARY_COLUMNGRP where productiondate not in ('2015-10-04 12:07:28','2015-10-07 12:07:28')""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_316
test("VMALL_DICTIONARY_COLUMNGRP_316", Include) {
  sql(s"""select gamepointid from VMALL_DICTIONARY_COLUMNGRP where gamepointid in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_317
test("VMALL_DICTIONARY_COLUMNGRP_317", Include) {
  sql(s"""select gamepointid from VMALL_DICTIONARY_COLUMNGRP where gamepointid not in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_318
test("VMALL_DICTIONARY_COLUMNGRP_318", Include) {
  sql(s"""select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP where Latest_DAY in (1234567890123530.0000000000,1234567890123520.0000000000)""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_319
test("VMALL_DICTIONARY_COLUMNGRP_319", Include) {
  sql(s"""select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP where Latest_DAY not in (1234567890123530.0000000000,1234567890123520.0000000000)""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_322
test("VMALL_DICTIONARY_COLUMNGRP_322", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_COLUMNGRP where imei !='1AA100077'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_323
test("VMALL_DICTIONARY_COLUMNGRP_323", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_COLUMNGRP where imei NOT LIKE '1AA100077'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_324
test("VMALL_DICTIONARY_COLUMNGRP_324", Include) {
  sql(s"""select deviceinformationid from VMALL_DICTIONARY_COLUMNGRP where deviceinformationid !=100078""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_325
test("VMALL_DICTIONARY_COLUMNGRP_325", Include) {
  sql(s"""select deviceinformationid from VMALL_DICTIONARY_COLUMNGRP where deviceinformationid NOT LIKE 100079""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_326
test("VMALL_DICTIONARY_COLUMNGRP_326", Include) {
  sql(s"""select productiondate from VMALL_DICTIONARY_COLUMNGRP where productiondate !='2015-10-07 12:07:28'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_327
test("VMALL_DICTIONARY_COLUMNGRP_327", Include) {
  sql(s"""select productiondate from VMALL_DICTIONARY_COLUMNGRP where productiondate NOT LIKE '2015-10-07 12:07:28'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_328
test("VMALL_DICTIONARY_COLUMNGRP_328", Include) {
  sql(s"""select gamepointid from VMALL_DICTIONARY_COLUMNGRP where gamepointid !=6.8591561117512E42""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_329
test("VMALL_DICTIONARY_COLUMNGRP_329", Include) {
  sql(s"""select gamepointid from VMALL_DICTIONARY_COLUMNGRP where gamepointid NOT LIKE 6.8591561117512E43""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_330
test("VMALL_DICTIONARY_COLUMNGRP_330", Include) {
  sql(s"""select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP where Latest_DAY != 1234567890123520.0000000000""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_331
test("VMALL_DICTIONARY_COLUMNGRP_331", Include) {
  sql(s"""select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP where Latest_DAY NOT LIKE 1234567890123520.0000000000""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_335
test("VMALL_DICTIONARY_COLUMNGRP_335", Include) {
  sql(s"""SELECT productiondate,IMEI from VMALL_DICTIONARY_COLUMNGRP where IMEI RLIKE '1AA100077'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_336
test("VMALL_DICTIONARY_COLUMNGRP_336", Include) {
  sql(s"""SELECT deviceinformationId from VMALL_DICTIONARY_COLUMNGRP where deviceinformationId RLIKE '100079'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_337
test("VMALL_DICTIONARY_COLUMNGRP_337", Include) {
  sql(s"""SELECT gamepointid from VMALL_DICTIONARY_COLUMNGRP where gamepointid RLIKE '1.61922711065643E42'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_338
test("VMALL_DICTIONARY_COLUMNGRP_338", Include) {
  sql(s"""SELECT Latest_Day from VMALL_DICTIONARY_COLUMNGRP where Latest_Day RLIKE '1234567890123550.0000000000'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_339
test("VMALL_DICTIONARY_COLUMNGRP_339", Include) {
  sql(s"""SELECT contractnumber from VMALL_DICTIONARY_COLUMNGRP where contractnumber RLIKE '9223372047800'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_340
test("VMALL_DICTIONARY_COLUMNGRP_340", Include) {
  sql(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_COLUMNGRP a join VMALL_DICTIONARY_COLUMNGRP b on a.productiondate=b.productiondate""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_341
test("VMALL_DICTIONARY_COLUMNGRP_341", Include) {
  sql(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_COLUMNGRP a join VMALL_DICTIONARY_COLUMNGRP b on a.deviceinformationid=b.deviceinformationid""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_342
test("VMALL_DICTIONARY_COLUMNGRP_342", Include) {
  sql(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_COLUMNGRP a join VMALL_DICTIONARY_COLUMNGRP b on a.imei=b.imei""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_343
test("VMALL_DICTIONARY_COLUMNGRP_343", Include) {
  sql(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_COLUMNGRP a join VMALL_DICTIONARY_COLUMNGRP b on a.gamepointid=b.gamepointid""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_344
test("VMALL_DICTIONARY_COLUMNGRP_344", Include) {
  sql(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_COLUMNGRP a join VMALL_DICTIONARY_COLUMNGRP b on a.Latest_Day=b.Latest_Day""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_345
test("VMALL_DICTIONARY_COLUMNGRP_345", Include) {
  sql(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from VMALL_DICTIONARY_COLUMNGRP a join VMALL_DICTIONARY_COLUMNGRP b on a.contractnumber=b.contractnumber""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_346
test("VMALL_DICTIONARY_COLUMNGRP_346", Include) {
  sql(s"""select count( contractNumber ),sum( contractNumber ),count(distinct contractNumber ),avg( contractNumber ),max( contractNumber ),min( contractNumber ),1 from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_347
test("VMALL_DICTIONARY_COLUMNGRP_347", Include) {
  sql(s"""select count( Latest_Day ),sum( Latest_Day ),count(distinct Latest_Day ),avg( Latest_Day ),max( Latest_Day ),min( Latest_Day ),1 from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_348
test("VMALL_DICTIONARY_COLUMNGRP_348", Include) {
  sql(s"""select count( gamePointId),sum( gamePointId ),count(distinct gamePointId ),avg(gamePointId),max(gamePointId),min(gamePointId),1 from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_349
test("VMALL_DICTIONARY_COLUMNGRP_349", Include) {
  sql(s"""select count(deviceInformationId),sum(deviceInformationId),count(deviceInformationId),avg(deviceInformationId),max(deviceInformationId),min(deviceInformationId),1 from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_350
test("VMALL_DICTIONARY_COLUMNGRP_350", Include) {
  sql(s"""select count(productionDate ),sum(productionDate),count(distinct productionDate),avg(productionDate),max(productionDate ),min(productionDate),1 from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_351
test("VMALL_DICTIONARY_COLUMNGRP_351", Include) {
  sql(s"""select count(IMEI ),sum(IMEI ),count(distinct IMEI ),avg(IMEI ),max(IMEI ),min(IMEI ),1 from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_352
test("VMALL_DICTIONARY_COLUMNGRP_352", Include) {
  sql(s"""select sum(contractNumber),count(contractNumber),avg(contractNumber),sum(contractNumber)/count(contractNumber) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_353
test("VMALL_DICTIONARY_COLUMNGRP_353", Include) {
  sql(s"""select sum(Latest_Day),count(Latest_Day),avg(Latest_Day),sum(Latest_Day)/count(Latest_Day) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_354
test("VMALL_DICTIONARY_COLUMNGRP_354", Include) {
  sql(s"""select sum(gamepointId),count(gamepointId),avg(gamepointID),sum(gamepointID)/count(gamepointID) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_355
test("VMALL_DICTIONARY_COLUMNGRP_355", Include) {
  sql(s"""select sum(deviceinformationId),count(deviceinformationId),avg(deviceinformationId),sum(deviceinformationId)/count(deviceinformationId) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_356
test("VMALL_DICTIONARY_COLUMNGRP_356", Include) {
  sql(s"""select sum(IMEI),count(IMEI),avg(IMEI),sum(IMEI)/count(IMEI) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_357
test("VMALL_DICTIONARY_COLUMNGRP_357", Include) {
  sql(s"""select sum(productionDate),count(productionDate),avg(productionDate),sum(productionDate)/count(productionDate) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_358
test("VMALL_DICTIONARY_COLUMNGRP_358", Include) {
  sql(s"""select contractNumber,Latest_DAY,gamePointId,productionDate,deviceInformationId,IMEI  from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_359
test("VMALL_DICTIONARY_COLUMNGRP_359", Include) {
  sql(s"""select count(MAC) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_360
test("VMALL_DICTIONARY_COLUMNGRP_360", Include) {
  sql(s"""select count(gamePointId) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_361
test("VMALL_DICTIONARY_COLUMNGRP_361", Include) {
  sql(s"""select count(contractNumber) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_362
test("VMALL_DICTIONARY_COLUMNGRP_362", Include) {
  sql(s"""select count(Latest_DAY) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_363
test("VMALL_DICTIONARY_COLUMNGRP_363", Include) {
  sql(s"""select count(productionDate) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_364
test("VMALL_DICTIONARY_COLUMNGRP_364", Include) {
  sql(s"""select count(deviceInformationId) from VMALL_DICTIONARY_COLUMNGRP""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_365
test("VMALL_DICTIONARY_COLUMNGRP_365", Include) {
  sql(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_COLUMNGRP where  contractNumber  != '9223372047700'""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_366
test("VMALL_DICTIONARY_COLUMNGRP_366", Include) {
  sql(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_COLUMNGRP where  Latest_DAY  != '1234567890123480.0000000000' order by deviceInformationId limit 5""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_367
test("VMALL_DICTIONARY_COLUMNGRP_367", Include) {
  sql(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_COLUMNGRP where  gamePointId  != '2.27852521808948E36' order by imei limit 5""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_368
test("VMALL_DICTIONARY_COLUMNGRP_368", Include) {
  sql(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_COLUMNGRP where  productionDate  != '2015-09-18 12:07:28.0' order by imei limit 5""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_369
test("VMALL_DICTIONARY_COLUMNGRP_369", Include) {
  sql(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_COLUMNGRP where  deviceInformationId  != '100075' order by imei limit 5""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_370
test("VMALL_DICTIONARY_COLUMNGRP_370", Include) {
  sql(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_COLUMNGRP where  contractNumber  not like '9223372047700' order by  deviceInformationId limit 5""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_371
test("VMALL_DICTIONARY_COLUMNGRP_371", Include) {
  sql(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_COLUMNGRP where  Latest_DAY  not like '1234567890123480.0000000000' order by deviceInformationId limit 5""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_372
test("VMALL_DICTIONARY_COLUMNGRP_372", Include) {
  sql(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_COLUMNGRP where  gamePointId  not like '2.27852521808948E36' order by imei limit 5""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_373
test("VMALL_DICTIONARY_COLUMNGRP_373", Include) {
  sql(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_COLUMNGRP where  productionDate  not like '2015-09-18 12:07:28.0' order by imei limit 5""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_374
test("VMALL_DICTIONARY_COLUMNGRP_374", Include) {
  sql(s"""select imei,deviceInformationId,MAC,deviceColor from VMALL_DICTIONARY_COLUMNGRP where  deviceInformationId  not like '100075' order by imei limit 5""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_375
test("VMALL_DICTIONARY_COLUMNGRP_375", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_COLUMNGRP where imei is not null""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_376
test("VMALL_DICTIONARY_COLUMNGRP_376", Include) {
  sql(s"""select gamePointId from VMALL_DICTIONARY_COLUMNGRP where gamePointId is not null""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_377
test("VMALL_DICTIONARY_COLUMNGRP_377", Include) {
  sql(s"""select contractNumber from VMALL_DICTIONARY_COLUMNGRP where contractNumber is not null""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_378
test("VMALL_DICTIONARY_COLUMNGRP_378", Include) {
  sql(s"""select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP where Latest_DAY is not null""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_379
test("VMALL_DICTIONARY_COLUMNGRP_379", Include) {
  sql(s"""select productionDate from VMALL_DICTIONARY_COLUMNGRP where productionDate is not null""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_380
test("VMALL_DICTIONARY_COLUMNGRP_380", Include) {
  sql(s"""select deviceInformationId from VMALL_DICTIONARY_COLUMNGRP where deviceInformationId is not null""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_381
test("VMALL_DICTIONARY_COLUMNGRP_381", Include) {
  sql(s"""select imei from VMALL_DICTIONARY_COLUMNGRP where imei is  null""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_382
test("VMALL_DICTIONARY_COLUMNGRP_382", Include) {
  sql(s"""select gamePointId from VMALL_DICTIONARY_COLUMNGRP where gamePointId is  null""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_383
test("VMALL_DICTIONARY_COLUMNGRP_383", Include) {
  sql(s"""select contractNumber from VMALL_DICTIONARY_COLUMNGRP where contractNumber is  null""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_384
test("VMALL_DICTIONARY_COLUMNGRP_384", Include) {
  sql(s"""select Latest_DAY from VMALL_DICTIONARY_COLUMNGRP where Latest_DAY is  null""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_385
test("VMALL_DICTIONARY_COLUMNGRP_385", Include) {
  sql(s"""select productionDate from VMALL_DICTIONARY_COLUMNGRP where productionDate is  null""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_386
test("VMALL_DICTIONARY_COLUMNGRP_386", Include) {
  sql(s"""select deviceInformationId from VMALL_DICTIONARY_COLUMNGRP where deviceInformationId is  null""").collect
}
       

//VMALL_DICTIONARY_COLUMNGRP_387
test("VMALL_DICTIONARY_COLUMNGRP_387", Include) {
  sql(s"""select count(*) from VMALL_DICTIONARY_COLUMNGRP where imei = '1AA1'""").collect
}
       
override def afterAll {
sql("drop table if exists VMALL_DICTIONARY_COLUMNGRP")
sql("drop table if exists VMALL_DICTIONARY_COLUMNGRP_hive")
}
}