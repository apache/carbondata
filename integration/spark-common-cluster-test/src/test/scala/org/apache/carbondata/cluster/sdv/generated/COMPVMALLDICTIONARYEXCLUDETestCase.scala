
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
 * Test Class for compvmalldictionaryexclude to verify all scenerios
 */

class COMPVMALLDICTIONARYEXCLUDETestCase extends QueryTest with BeforeAndAfterAll {
         

//Comp_VMALL_DICTIONARY_EXCLUDE_CreateCube_Drop
test("Comp_VMALL_DICTIONARY_EXCLUDE_CreateCube_Drop", Include) {
  sql(s"""drop table if exists  comp_vmall_dictionary_exclude""").collect

  sql(s"""drop table if exists  comp_vmall_dictionary_exclude_hive""").collect

}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_CreateCube
test("Comp_VMALL_DICTIONARY_EXCLUDE_CreateCube", Include) {
  sql(s"""create table  Comp_VMALL_DICTIONARY_EXCLUDE (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt)  STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='imei')""").collect

  sql(s"""create table  Comp_VMALL_DICTIONARY_EXCLUDE_hive (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, contractNumber BigInt,ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointId double,gamePointDescription string)   ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad1
test("Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad1", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE_hive """).collect

}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad2
test("Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad2", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE_hive """).collect

}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad3
test("Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad3", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE_hive """).collect

}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad4
test("Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad4", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE_hive """).collect

}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad5
test("Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad5", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE_hive """).collect

}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad6
test("Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad6", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE_hive """).collect

}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad7
test("Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad7", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE_hive """).collect

}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad8
test("Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad8", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE_hive """).collect

}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad9
test("Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad9", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE_hive """).collect

}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad10
test("Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad10", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE_hive """).collect

}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad11
test("Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad11", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE_hive """).collect

}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad12
test("Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad12", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE_hive """).collect

}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad13
test("Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad13", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE_hive """).collect

}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad14
test("Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad14", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE_hive """).collect

}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad15
test("Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad15", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE_hive """).collect

}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad16
test("Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad16", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE_hive """).collect

}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad17
test("Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad17", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE_hive """).collect

}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad18
test("Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad18", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE_hive """).collect

}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad19
test("Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad19", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE_hive """).collect

}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad20
test("Comp_VMALL_DICTIONARY_EXCLUDE_DataLoad20", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_VMALL_DICTIONARY_EXCLUDE_hive """).collect

}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_001
test("Comp_VMALL_DICTIONARY_EXCLUDE_001", Include) {
  checkAnswer(s"""Select count(imei) from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""Select count(imei) from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_002
test("Comp_VMALL_DICTIONARY_EXCLUDE_002", Include) {
  checkAnswer(s"""select count(DISTINCT imei) as a from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(DISTINCT imei) as a from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_003
test("Comp_VMALL_DICTIONARY_EXCLUDE_003", Include) {
  checkAnswer(s"""select sum(Latest_month)+10 as a ,imei  from Comp_VMALL_DICTIONARY_EXCLUDE group by imei order by imei""",
    s"""select sum(Latest_month)+10 as a ,imei  from Comp_VMALL_DICTIONARY_EXCLUDE_hive group by imei order by imei""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_004
test("Comp_VMALL_DICTIONARY_EXCLUDE_004", Include) {
  checkAnswer(s"""select max(imei),min(imei) from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select max(imei),min(imei) from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_005
test("Comp_VMALL_DICTIONARY_EXCLUDE_005", Include) {
  checkAnswer(s"""select min(imei), max(imei) Total from Comp_VMALL_DICTIONARY_EXCLUDE group by  channelsId order by Total""",
    s"""select min(imei), max(imei) Total from Comp_VMALL_DICTIONARY_EXCLUDE_hive group by  channelsId order by Total""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_006
test("Comp_VMALL_DICTIONARY_EXCLUDE_006", Include) {
  checkAnswer(s"""select last(imei) a from Comp_VMALL_DICTIONARY_EXCLUDE  group by imei order by imei limit 1""",
    s"""select last(imei) a from Comp_VMALL_DICTIONARY_EXCLUDE_hive  group by imei order by imei limit 1""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_007
test("Comp_VMALL_DICTIONARY_EXCLUDE_007", Include) {
  sql(s"""select FIRST(imei) a from Comp_VMALL_DICTIONARY_EXCLUDE group by imei order by imei limit 1""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_008
test("Comp_VMALL_DICTIONARY_EXCLUDE_008", Include) {
  checkAnswer(s"""select imei,count(imei) a from Comp_VMALL_DICTIONARY_EXCLUDE group by imei order by imei""",
    s"""select imei,count(imei) a from Comp_VMALL_DICTIONARY_EXCLUDE_hive group by imei order by imei""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_009
test("Comp_VMALL_DICTIONARY_EXCLUDE_009", Include) {
  checkAnswer(s"""select Lower(imei) a  from Comp_VMALL_DICTIONARY_EXCLUDE order by imei""",
    s"""select Lower(imei) a  from Comp_VMALL_DICTIONARY_EXCLUDE_hive order by imei""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_010
test("Comp_VMALL_DICTIONARY_EXCLUDE_010", Include) {
  checkAnswer(s"""select distinct imei from Comp_VMALL_DICTIONARY_EXCLUDE order by imei""",
    s"""select distinct imei from Comp_VMALL_DICTIONARY_EXCLUDE_hive order by imei""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_011
test("Comp_VMALL_DICTIONARY_EXCLUDE_011", Include) {
  checkAnswer(s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE order by imei limit 101 """,
    s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE_hive order by imei limit 101 """)
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_012
test("Comp_VMALL_DICTIONARY_EXCLUDE_012", Include) {
  checkAnswer(s"""select imei as a from Comp_VMALL_DICTIONARY_EXCLUDE  order by a asc limit 10""",
    s"""select imei as a from Comp_VMALL_DICTIONARY_EXCLUDE_hive  order by a asc limit 10""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_013
test("Comp_VMALL_DICTIONARY_EXCLUDE_013", Include) {
  checkAnswer(s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE where  (contractNumber == 9223372047700) and (imei=='1AA100004')""",
    s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE_hive where  (contractNumber == 9223372047700) and (imei=='1AA100004')""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_014
test("Comp_VMALL_DICTIONARY_EXCLUDE_014", Include) {
  checkAnswer(s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE where imei !='1AA100064' order by imei""",
    s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE_hive where imei !='1AA100064' order by imei""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_015
test("Comp_VMALL_DICTIONARY_EXCLUDE_015", Include) {
  checkAnswer(s"""select imei  from Comp_VMALL_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
    s"""select imei  from Comp_VMALL_DICTIONARY_EXCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_016
test("Comp_VMALL_DICTIONARY_EXCLUDE_016", Include) {
  checkAnswer(s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE where imei !='1AA100012' order by imei""",
    s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE_hive where imei !='1AA100012' order by imei""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_017
test("Comp_VMALL_DICTIONARY_EXCLUDE_017", Include) {
  checkAnswer(s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE where imei >'1AA100012' order by imei""",
    s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE_hive where imei >'1AA100012' order by imei""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_018
test("Comp_VMALL_DICTIONARY_EXCLUDE_018", Include) {
  checkAnswer(s"""select imei  from Comp_VMALL_DICTIONARY_EXCLUDE where imei<>imei""",
    s"""select imei  from Comp_VMALL_DICTIONARY_EXCLUDE_hive where imei<>imei""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_019
test("Comp_VMALL_DICTIONARY_EXCLUDE_019", Include) {
  checkAnswer(s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE where imei != Latest_areaId order by imei""",
    s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE_hive where imei != Latest_areaId order by imei""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_020
test("Comp_VMALL_DICTIONARY_EXCLUDE_020", Include) {
  checkAnswer(s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_areaId<imei order by imei""",
    s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE_hive where Latest_areaId<imei order by imei""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_021
test("Comp_VMALL_DICTIONARY_EXCLUDE_021", Include) {
  checkAnswer(s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_DAY<=imei order by imei""",
    s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY<=imei order by imei""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_022
test("Comp_VMALL_DICTIONARY_EXCLUDE_022", Include) {
  checkAnswer(s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE where imei <'1AA10002' order by imei""",
    s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE_hive where imei <'1AA10002' order by imei""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_023
test("Comp_VMALL_DICTIONARY_EXCLUDE_023", Include) {
  checkAnswer(s"""select Latest_day  from Comp_VMALL_DICTIONARY_EXCLUDE where imei IS NULL""",
    s"""select Latest_day  from Comp_VMALL_DICTIONARY_EXCLUDE_hive where imei IS NULL""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_024
test("Comp_VMALL_DICTIONARY_EXCLUDE_024", Include) {
  checkAnswer(s"""select Latest_day  from Comp_VMALL_DICTIONARY_EXCLUDE where imei IS NOT NULL order by Latest_day""",
    s"""select Latest_day  from Comp_VMALL_DICTIONARY_EXCLUDE_hive where imei IS NOT NULL order by Latest_day""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_025
test("Comp_VMALL_DICTIONARY_EXCLUDE_025", Include) {
  checkAnswer(s"""Select count(imei),min(imei) from Comp_VMALL_DICTIONARY_EXCLUDE """,
    s"""Select count(imei),min(imei) from Comp_VMALL_DICTIONARY_EXCLUDE_hive """)
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_026
test("Comp_VMALL_DICTIONARY_EXCLUDE_026", Include) {
  checkAnswer(s"""select count(DISTINCT imei,latest_day) as a from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(DISTINCT imei,latest_day) as a from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_027
test("Comp_VMALL_DICTIONARY_EXCLUDE_027", Include) {
  checkAnswer(s"""select max(imei),min(imei),count(imei) from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select max(imei),min(imei),count(imei) from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_028
test("Comp_VMALL_DICTIONARY_EXCLUDE_028", Include) {
  checkAnswer(s"""select sum(imei),avg(imei),count(imei) a  from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(imei),avg(imei),count(imei) a  from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_029
test("Comp_VMALL_DICTIONARY_EXCLUDE_029", Include) {
  sql(s"""select last(imei),Min(imei),max(imei)  a from Comp_VMALL_DICTIONARY_EXCLUDE  order by a""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_030
test("Comp_VMALL_DICTIONARY_EXCLUDE_030", Include) {
  sql(s"""select FIRST(imei),Last(imei) a from Comp_VMALL_DICTIONARY_EXCLUDE group by imei order by imei limit 1""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_031
test("Comp_VMALL_DICTIONARY_EXCLUDE_031", Include) {
  checkAnswer(s"""select imei,count(imei) a from Comp_VMALL_DICTIONARY_EXCLUDE group by imei order by imei""",
    s"""select imei,count(imei) a from Comp_VMALL_DICTIONARY_EXCLUDE_hive group by imei order by imei""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_032
test("Comp_VMALL_DICTIONARY_EXCLUDE_032", Include) {
  checkAnswer(s"""select Lower(imei),upper(imei)  a  from Comp_VMALL_DICTIONARY_EXCLUDE order by imei""",
    s"""select Lower(imei),upper(imei)  a  from Comp_VMALL_DICTIONARY_EXCLUDE_hive order by imei""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_033
test("Comp_VMALL_DICTIONARY_EXCLUDE_033", Include) {
  checkAnswer(s"""select imei as a from Comp_VMALL_DICTIONARY_EXCLUDE  order by a asc limit 10""",
    s"""select imei as a from Comp_VMALL_DICTIONARY_EXCLUDE_hive  order by a asc limit 10""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_034
test("Comp_VMALL_DICTIONARY_EXCLUDE_034", Include) {
  checkAnswer(s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE where  (contractNumber == 9223372047700) and (imei=='1AA100012')""",
    s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE_hive where  (contractNumber == 9223372047700) and (imei=='1AA100012')""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_035
test("Comp_VMALL_DICTIONARY_EXCLUDE_035", Include) {
  checkAnswer(s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE where imei !='8imei' order by imei""",
    s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE_hive where imei !='8imei' order by imei""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_036
test("Comp_VMALL_DICTIONARY_EXCLUDE_036", Include) {
  checkAnswer(s"""select imei  from Comp_VMALL_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
    s"""select imei  from Comp_VMALL_DICTIONARY_EXCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_037
test("Comp_VMALL_DICTIONARY_EXCLUDE_037", Include) {
  checkAnswer(s"""Select count(contractNumber) from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""Select count(contractNumber) from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_038
test("Comp_VMALL_DICTIONARY_EXCLUDE_038", Include) {
  checkAnswer(s"""select count(DISTINCT contractNumber) as a from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(DISTINCT contractNumber) as a from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_039
test("Comp_VMALL_DICTIONARY_EXCLUDE_039", Include) {
  checkAnswer(s"""select sum(contractNumber)+10 as a ,contractNumber  from Comp_VMALL_DICTIONARY_EXCLUDE group by contractNumber""",
    s"""select sum(contractNumber)+10 as a ,contractNumber  from Comp_VMALL_DICTIONARY_EXCLUDE_hive group by contractNumber""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_040
test("Comp_VMALL_DICTIONARY_EXCLUDE_040", Include) {
  checkAnswer(s"""select max(contractNumber),min(contractNumber) from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select max(contractNumber),min(contractNumber) from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_041
test("Comp_VMALL_DICTIONARY_EXCLUDE_041", Include) {
  checkAnswer(s"""select sum(contractNumber) a  from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(contractNumber) a  from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_042
test("Comp_VMALL_DICTIONARY_EXCLUDE_042", Include) {
  checkAnswer(s"""select avg(contractNumber) a  from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select avg(contractNumber) a  from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_043
test("Comp_VMALL_DICTIONARY_EXCLUDE_043", Include) {
  checkAnswer(s"""select min(contractNumber) a  from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select min(contractNumber) a  from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_044
test("Comp_VMALL_DICTIONARY_EXCLUDE_044", Include) {
  sql(s"""select variance(contractNumber) as a   from (select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE order by contractNumber) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_045
test("Comp_VMALL_DICTIONARY_EXCLUDE_045", Include) {
  sql(s"""select var_pop(contractNumber)  as a from Comp_VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_046
test("Comp_VMALL_DICTIONARY_EXCLUDE_046", Include) {
  sql(s"""select var_samp(contractNumber) as a  from Comp_VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_047
test("Comp_VMALL_DICTIONARY_EXCLUDE_047", Include) {
  sql(s"""select stddev_pop(contractNumber) as a  from (select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE order by contractNumber) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_048
test("Comp_VMALL_DICTIONARY_EXCLUDE_048", Include) {
  sql(s"""select stddev_samp(contractNumber)  as a from (select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE order by contractNumber) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_049
test("Comp_VMALL_DICTIONARY_EXCLUDE_049", Include) {
  sql(s"""select covar_pop(contractNumber,contractNumber) as a  from (select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE order by contractNumber) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_050
test("Comp_VMALL_DICTIONARY_EXCLUDE_050", Include) {
  sql(s"""select covar_samp(contractNumber,contractNumber) as a  from (select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE order by contractNumber) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_051
test("Comp_VMALL_DICTIONARY_EXCLUDE_051", Include) {
  checkAnswer(s"""select corr(contractNumber,contractNumber)  as a from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select corr(contractNumber,contractNumber)  as a from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_052
test("Comp_VMALL_DICTIONARY_EXCLUDE_052", Include) {
  sql(s"""select percentile_approx(contractNumber,0.2) as a  from (select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE order by contractNumber) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_053
test("Comp_VMALL_DICTIONARY_EXCLUDE_053", Include) {
  sql(s"""select percentile_approx(contractNumber,0.2,5) as a  from (select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE order by contractNumber) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_054
test("Comp_VMALL_DICTIONARY_EXCLUDE_054", Include) {
  sql(s"""select percentile_approx(contractNumber,array(0.2,0.3,0.99))  as a from (select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE order by contractNumber) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_055
test("Comp_VMALL_DICTIONARY_EXCLUDE_055", Include) {
  sql(s"""select percentile_approx(contractNumber,array(0.2,0.3,0.99),5) as a from (select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE order by contractNumber) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_056
test("Comp_VMALL_DICTIONARY_EXCLUDE_056", Include) {
  sql(s"""select histogram_numeric(contractNumber,2)  as a from (select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE order by contractNumber) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_057
test("Comp_VMALL_DICTIONARY_EXCLUDE_057", Include) {
  checkAnswer(s"""select contractNumber+ 10 as a  from Comp_VMALL_DICTIONARY_EXCLUDE order by a""",
    s"""select contractNumber+ 10 as a  from Comp_VMALL_DICTIONARY_EXCLUDE_hive order by a""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_058
test("Comp_VMALL_DICTIONARY_EXCLUDE_058", Include) {
  checkAnswer(s"""select min(contractNumber), max(contractNumber+ 10) Total from Comp_VMALL_DICTIONARY_EXCLUDE group by  channelsId order by Total""",
    s"""select min(contractNumber), max(contractNumber+ 10) Total from Comp_VMALL_DICTIONARY_EXCLUDE_hive group by  channelsId order by Total""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_059
test("Comp_VMALL_DICTIONARY_EXCLUDE_059", Include) {
  sql(s"""select last(contractNumber) a from Comp_VMALL_DICTIONARY_EXCLUDE  order by a""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_060
test("Comp_VMALL_DICTIONARY_EXCLUDE_060", Include) {
  sql(s"""select FIRST(contractNumber) a from Comp_VMALL_DICTIONARY_EXCLUDE order by a""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_061
test("Comp_VMALL_DICTIONARY_EXCLUDE_061", Include) {
  checkAnswer(s"""select contractNumber,count(contractNumber) a from Comp_VMALL_DICTIONARY_EXCLUDE group by contractNumber order by contractNumber""",
    s"""select contractNumber,count(contractNumber) a from Comp_VMALL_DICTIONARY_EXCLUDE_hive group by contractNumber order by contractNumber""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_062
test("Comp_VMALL_DICTIONARY_EXCLUDE_062", Include) {
  checkAnswer(s"""select Lower(contractNumber) a  from Comp_VMALL_DICTIONARY_EXCLUDE order by contractNumber""",
    s"""select Lower(contractNumber) a  from Comp_VMALL_DICTIONARY_EXCLUDE_hive order by contractNumber""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_063
test("Comp_VMALL_DICTIONARY_EXCLUDE_063", Include) {
  checkAnswer(s"""select distinct contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE order by contractNumber""",
    s"""select distinct contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE_hive order by contractNumber""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_064
test("Comp_VMALL_DICTIONARY_EXCLUDE_064", Include) {
  checkAnswer(s"""select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE order by contractNumber limit 101""",
    s"""select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE_hive order by contractNumber limit 101""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_065
test("Comp_VMALL_DICTIONARY_EXCLUDE_065", Include) {
  checkAnswer(s"""select contractNumber as a from Comp_VMALL_DICTIONARY_EXCLUDE  order by a asc limit 10""",
    s"""select contractNumber as a from Comp_VMALL_DICTIONARY_EXCLUDE_hive  order by a asc limit 10""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_066
test("Comp_VMALL_DICTIONARY_EXCLUDE_066", Include) {
  checkAnswer(s"""select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE where  (contractNumber == 9223372047700) and (imei=='1AA100012')""",
    s"""select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE_hive where  (contractNumber == 9223372047700) and (imei=='1AA100012')""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_067
test("Comp_VMALL_DICTIONARY_EXCLUDE_067", Include) {
  checkAnswer(s"""select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE where contractNumber !=9223372047700 order by contractNumber""",
    s"""select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE_hive where contractNumber !=9223372047700 order by contractNumber""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_068
test("Comp_VMALL_DICTIONARY_EXCLUDE_068", Include) {
  checkAnswer(s"""select contractNumber  from Comp_VMALL_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color') order by contractNumber""",
    s"""select contractNumber  from Comp_VMALL_DICTIONARY_EXCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color') order by contractNumber""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_069
test("Comp_VMALL_DICTIONARY_EXCLUDE_069", Include) {
  checkAnswer(s"""select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE where contractNumber !=9223372047700 order by contractNumber""",
    s"""select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE_hive where contractNumber !=9223372047700 order by contractNumber""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_070
test("Comp_VMALL_DICTIONARY_EXCLUDE_070", Include) {
  checkAnswer(s"""select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE where contractNumber >9223372047700 order by contractNumber""",
    s"""select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE_hive where contractNumber >9223372047700 order by contractNumber""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_071
test("Comp_VMALL_DICTIONARY_EXCLUDE_071", Include) {
  checkAnswer(s"""select contractNumber  from Comp_VMALL_DICTIONARY_EXCLUDE where contractNumber<>contractNumber""",
    s"""select contractNumber  from Comp_VMALL_DICTIONARY_EXCLUDE_hive where contractNumber<>contractNumber""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_072
test("Comp_VMALL_DICTIONARY_EXCLUDE_072", Include) {
  checkAnswer(s"""select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE where contractNumber != Latest_areaId order by contractNumber""",
    s"""select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE_hive where contractNumber != Latest_areaId order by contractNumber""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_073
test("Comp_VMALL_DICTIONARY_EXCLUDE_073", Include) {
  checkAnswer(s"""select contractNumber, contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_areaId<contractNumber order by contractNumber""",
    s"""select contractNumber, contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE_hive where Latest_areaId<contractNumber order by contractNumber""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_074
test("Comp_VMALL_DICTIONARY_EXCLUDE_074", Include) {
  checkAnswer(s"""select contractNumber, contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_DAY<=contractNumber order by contractNumber""",
    s"""select contractNumber, contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY<=contractNumber order by contractNumber""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_075
test("Comp_VMALL_DICTIONARY_EXCLUDE_075", Include) {
  checkAnswer(s"""select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE where contractNumber <1000 order by contractNumber""",
    s"""select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE_hive where contractNumber <1000 order by contractNumber""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_076
test("Comp_VMALL_DICTIONARY_EXCLUDE_076", Include) {
  checkAnswer(s"""select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE where contractNumber >1000 order by contractNumber""",
    s"""select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE_hive where contractNumber >1000 order by contractNumber""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_077
test("Comp_VMALL_DICTIONARY_EXCLUDE_077", Include) {
  checkAnswer(s"""select contractNumber  from Comp_VMALL_DICTIONARY_EXCLUDE where contractNumber IS NULL order by contractNumber""",
    s"""select contractNumber  from Comp_VMALL_DICTIONARY_EXCLUDE_hive where contractNumber IS NULL order by contractNumber""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_078
test("Comp_VMALL_DICTIONARY_EXCLUDE_078", Include) {
  checkAnswer(s"""select contractNumber  from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_DAY IS NOT NULL order by contractNumber""",
    s"""select contractNumber  from Comp_VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY IS NOT NULL order by contractNumber""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_079
test("Comp_VMALL_DICTIONARY_EXCLUDE_079", Include) {
  checkAnswer(s"""Select count(Latest_DAY) from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""Select count(Latest_DAY) from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_080
test("Comp_VMALL_DICTIONARY_EXCLUDE_080", Include) {
  checkAnswer(s"""select count(DISTINCT Latest_DAY) as a from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(DISTINCT Latest_DAY) as a from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_081
test("Comp_VMALL_DICTIONARY_EXCLUDE_081", Include) {
  checkAnswer(s"""select sum(Latest_DAY)+10 as a ,Latest_DAY  from Comp_VMALL_DICTIONARY_EXCLUDE group by Latest_DAY order by a""",
    s"""select sum(Latest_DAY)+10 as a ,Latest_DAY  from Comp_VMALL_DICTIONARY_EXCLUDE_hive group by Latest_DAY order by a""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_082
test("Comp_VMALL_DICTIONARY_EXCLUDE_082", Include) {
  checkAnswer(s"""select max(Latest_DAY),min(Latest_DAY) from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select max(Latest_DAY),min(Latest_DAY) from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_083
test("Comp_VMALL_DICTIONARY_EXCLUDE_083", Include) {
  checkAnswer(s"""select sum(Latest_DAY) a  from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(Latest_DAY) a  from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_084
test("Comp_VMALL_DICTIONARY_EXCLUDE_084", Include) {
  checkAnswer(s"""select avg(Latest_DAY) a  from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select avg(Latest_DAY) a  from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_085
test("Comp_VMALL_DICTIONARY_EXCLUDE_085", Include) {
  checkAnswer(s"""select min(Latest_DAY) a  from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select min(Latest_DAY) a  from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_086
test("Comp_VMALL_DICTIONARY_EXCLUDE_086", Include) {
  sql(s"""select variance(Latest_DAY) as a   from (select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_087
test("Comp_VMALL_DICTIONARY_EXCLUDE_087", Include) {
  sql(s"""select var_pop(Latest_DAY)  as a from (select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_088
test("Comp_VMALL_DICTIONARY_EXCLUDE_088", Include) {
  sql(s"""select var_samp(Latest_DAY) as a  from (select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_089
test("Comp_VMALL_DICTIONARY_EXCLUDE_089", Include) {
  sql(s"""select stddev_pop(Latest_DAY) as a  from (select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_090
test("Comp_VMALL_DICTIONARY_EXCLUDE_090", Include) {
  sql(s"""select stddev_samp(Latest_DAY)  as a from (select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_091
test("Comp_VMALL_DICTIONARY_EXCLUDE_091", Include) {
  sql(s"""select covar_pop(Latest_DAY,Latest_DAY) as a  from (select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_092
test("Comp_VMALL_DICTIONARY_EXCLUDE_092", Include) {
  sql(s"""select covar_samp(Latest_DAY,Latest_DAY) as a  from (select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_093
test("Comp_VMALL_DICTIONARY_EXCLUDE_093", Include) {
  checkAnswer(s"""select corr(Latest_DAY,Latest_DAY)  as a from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select corr(Latest_DAY,Latest_DAY)  as a from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_094
test("Comp_VMALL_DICTIONARY_EXCLUDE_094", Include) {
  sql(s"""select percentile_approx(Latest_DAY,0.2) as a  from (select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_095
test("Comp_VMALL_DICTIONARY_EXCLUDE_095", Include) {
  sql(s"""select percentile_approx(Latest_DAY,0.2,5) as a  from (select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_096
test("Comp_VMALL_DICTIONARY_EXCLUDE_096", Include) {
  sql(s"""select percentile_approx(Latest_DAY,array(0.2,0.3,0.99))  as a from (select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_097
test("Comp_VMALL_DICTIONARY_EXCLUDE_097", Include) {
  sql(s"""select percentile_approx(Latest_DAY,array(0.2,0.3,0.99),5) as a from (select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_098
test("Comp_VMALL_DICTIONARY_EXCLUDE_098", Include) {
  sql(s"""select histogram_numeric(Latest_DAY,2)  as a from (select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_099
test("Comp_VMALL_DICTIONARY_EXCLUDE_099", Include) {
  sql(s"""select Latest_DAY, Latest_DAY+ 10 as a  from Comp_VMALL_DICTIONARY_EXCLUDE order by a""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_100
test("Comp_VMALL_DICTIONARY_EXCLUDE_100", Include) {
  checkAnswer(s"""select min(Latest_DAY), max(Latest_DAY+ 10) Total from Comp_VMALL_DICTIONARY_EXCLUDE group by  channelsId order by Total""",
    s"""select min(Latest_DAY), max(Latest_DAY+ 10) Total from Comp_VMALL_DICTIONARY_EXCLUDE_hive group by  channelsId order by Total""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_101
test("Comp_VMALL_DICTIONARY_EXCLUDE_101", Include) {
  sql(s"""select last(Latest_DAY) a from Comp_VMALL_DICTIONARY_EXCLUDE order by a""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_102
test("Comp_VMALL_DICTIONARY_EXCLUDE_102", Include) {
  sql(s"""select FIRST(Latest_DAY) a from (select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_103
test("Comp_VMALL_DICTIONARY_EXCLUDE_103", Include) {
  sql(s"""select Latest_DAY,count(Latest_DAY) a from Comp_VMALL_DICTIONARY_EXCLUDE group by Latest_DAY order by Latest_DAY""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_104
test("Comp_VMALL_DICTIONARY_EXCLUDE_104", Include) {
  sql(s"""select Lower(Latest_DAY) a  from Comp_VMALL_DICTIONARY_EXCLUDE order by a""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_105
test("Comp_VMALL_DICTIONARY_EXCLUDE_105", Include) {
  checkAnswer(s"""select distinct Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE order by Latest_DAY""",
    s"""select distinct Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE_hive order by Latest_DAY""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_106
test("Comp_VMALL_DICTIONARY_EXCLUDE_106", Include) {
  sql(s"""select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE order by Latest_DAY limit 101""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_107
test("Comp_VMALL_DICTIONARY_EXCLUDE_107", Include) {
  checkAnswer(s"""select Latest_DAY as a from Comp_VMALL_DICTIONARY_EXCLUDE  order by a asc limit 10""",
    s"""select Latest_DAY as a from Comp_VMALL_DICTIONARY_EXCLUDE_hive  order by a asc limit 10""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_108
test("Comp_VMALL_DICTIONARY_EXCLUDE_108", Include) {
  sql(s"""select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE where  (Latest_DAY == 1234567890123450.0000000000)  and (imei=='1AA1')""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_109
test("Comp_VMALL_DICTIONARY_EXCLUDE_109", Include) {
  sql(s"""select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_110
test("Comp_VMALL_DICTIONARY_EXCLUDE_110", Include) {
  sql(s"""select Latest_DAY  from Comp_VMALL_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_111
test("Comp_VMALL_DICTIONARY_EXCLUDE_111", Include) {
  sql(s"""select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_112
test("Comp_VMALL_DICTIONARY_EXCLUDE_112", Include) {
  sql(s"""select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_DAY >1234567890123450.0000000000  order by Latest_DAY""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_113
test("Comp_VMALL_DICTIONARY_EXCLUDE_113", Include) {
  checkAnswer(s"""select Latest_DAY  from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_DAY<>Latest_DAY""",
    s"""select Latest_DAY  from Comp_VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY<>Latest_DAY""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_114
test("Comp_VMALL_DICTIONARY_EXCLUDE_114", Include) {
  sql(s"""select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_DAY != Latest_areaId order by Latest_DAY""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_115
test("Comp_VMALL_DICTIONARY_EXCLUDE_115", Include) {
  sql(s"""select Latest_DAY, Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_areaId<Latest_DAY order by Latest_DAY""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_116
test("Comp_VMALL_DICTIONARY_EXCLUDE_116", Include) {
  sql(s"""select Latest_DAY, Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_DAY<=Latest_DAY  order by Latest_DAY""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_117
test("Comp_VMALL_DICTIONARY_EXCLUDE_117", Include) {
  checkAnswer(s"""select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_DAY <1000  order by Latest_DAY""",
    s"""select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY <1000  order by Latest_DAY""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_118
test("Comp_VMALL_DICTIONARY_EXCLUDE_118", Include) {
  sql(s"""select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_DAY >1000  order by Latest_DAY""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_119
test("Comp_VMALL_DICTIONARY_EXCLUDE_119", Include) {
  sql(s"""select Latest_DAY  from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_DAY IS NULL  order by Latest_DAY""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_120
test("Comp_VMALL_DICTIONARY_EXCLUDE_120", Include) {
  sql(s"""select Latest_DAY  from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_DAY IS NOT NULL  order by Latest_DAY""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_121
test("Comp_VMALL_DICTIONARY_EXCLUDE_121", Include) {
  sql(s"""Select count(gamePointId) from Comp_VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_122
test("Comp_VMALL_DICTIONARY_EXCLUDE_122", Include) {
  sql(s"""select count(DISTINCT gamePointId) as a from Comp_VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_123
test("Comp_VMALL_DICTIONARY_EXCLUDE_123", Include) {
  sql(s"""select sum(gamePointId)+10 as a ,gamePointId  from Comp_VMALL_DICTIONARY_EXCLUDE group by gamePointId order by a""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_124
test("Comp_VMALL_DICTIONARY_EXCLUDE_124", Include) {
  sql(s"""select max(gamePointId),min(gamePointId) from Comp_VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_125
test("Comp_VMALL_DICTIONARY_EXCLUDE_125", Include) {
  sql(s"""select sum(gamePointId) a  from Comp_VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_126
test("Comp_VMALL_DICTIONARY_EXCLUDE_126", Include) {
  sql(s"""select avg(gamePointId) a  from Comp_VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_127
test("Comp_VMALL_DICTIONARY_EXCLUDE_127", Include) {
  sql(s"""select min(gamePointId) a  from Comp_VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_128
test("Comp_VMALL_DICTIONARY_EXCLUDE_128", Include) {
  sql(s"""select variance(gamePointId) as a   from (select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_129
test("Comp_VMALL_DICTIONARY_EXCLUDE_129", Include) {
  sql(s"""select var_pop(gamePointId)  as a from (select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_130
test("Comp_VMALL_DICTIONARY_EXCLUDE_130", Include) {
  sql(s"""select var_samp(gamePointId) as a  from (select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_131
test("Comp_VMALL_DICTIONARY_EXCLUDE_131", Include) {
  sql(s"""select stddev_pop(gamePointId) as a  from (select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_132
test("Comp_VMALL_DICTIONARY_EXCLUDE_132", Include) {
  sql(s"""select stddev_samp(gamePointId)  as a from (select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_133
test("Comp_VMALL_DICTIONARY_EXCLUDE_133", Include) {
  sql(s"""select covar_pop(gamePointId,gamePointId) as a  from (select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_134
test("Comp_VMALL_DICTIONARY_EXCLUDE_134", Include) {
  sql(s"""select covar_samp(gamePointId,gamePointId) as a  from (select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_135
test("Comp_VMALL_DICTIONARY_EXCLUDE_135", Include) {
  sql(s"""select corr(gamePointId,gamePointId)  as a from Comp_VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_136
test("Comp_VMALL_DICTIONARY_EXCLUDE_136", Include) {
  sql(s"""select percentile_approx(gamePointId,0.2) as a  from (select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_137
test("Comp_VMALL_DICTIONARY_EXCLUDE_137", Include) {
  sql(s"""select percentile_approx(gamePointId,0.2,5) as a  from (select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_138
test("Comp_VMALL_DICTIONARY_EXCLUDE_138", Include) {
  sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99))  as a from (select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_139
test("Comp_VMALL_DICTIONARY_EXCLUDE_139", Include) {
  sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99),5) as a from (select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_140
test("Comp_VMALL_DICTIONARY_EXCLUDE_140", Include) {
  sql(s"""select histogram_numeric(gamePointId,2)  as a from (select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_141
test("Comp_VMALL_DICTIONARY_EXCLUDE_141", Include) {
  sql(s"""select gamePointId, gamePointId+ 10 as a  from Comp_VMALL_DICTIONARY_EXCLUDE order by a""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_142
test("Comp_VMALL_DICTIONARY_EXCLUDE_142", Include) {
  sql(s"""select min(gamePointId), max(gamePointId+ 10) Total from Comp_VMALL_DICTIONARY_EXCLUDE group by  channelsId order by Total""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_143
test("Comp_VMALL_DICTIONARY_EXCLUDE_143", Include) {
  sql(s"""select last(gamePointId) a from (select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE order by gamePointId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_144
test("Comp_VMALL_DICTIONARY_EXCLUDE_144", Include) {
  sql(s"""select FIRST(gamePointId) a from Comp_VMALL_DICTIONARY_EXCLUDE order by a""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_145
test("Comp_VMALL_DICTIONARY_EXCLUDE_145", Include) {
  sql(s"""select gamePointId,count(gamePointId) a from Comp_VMALL_DICTIONARY_EXCLUDE group by gamePointId order by gamePointId""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_146
test("Comp_VMALL_DICTIONARY_EXCLUDE_146", Include) {
  sql(s"""select Lower(gamePointId) a  from Comp_VMALL_DICTIONARY_EXCLUDE order by gamePointId""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_147
test("Comp_VMALL_DICTIONARY_EXCLUDE_147", Include) {
  sql(s"""select distinct gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE order by gamePointId""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_148
test("Comp_VMALL_DICTIONARY_EXCLUDE_148", Include) {
  sql(s"""select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE  order by gamePointId limit 101""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_149
test("Comp_VMALL_DICTIONARY_EXCLUDE_149", Include) {
  checkAnswer(s"""select gamePointId as a from Comp_VMALL_DICTIONARY_EXCLUDE  order by a asc limit 10""",
    s"""select gamePointId as a from Comp_VMALL_DICTIONARY_EXCLUDE_hive  order by a asc limit 10""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_150
test("Comp_VMALL_DICTIONARY_EXCLUDE_150", Include) {
  sql(s"""select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE where  (gamePointId == 4.70133553923674E43) and (imei=='1AA100084')""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_151
test("Comp_VMALL_DICTIONARY_EXCLUDE_151", Include) {
  sql(s"""select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE where gamePointId !=4.70133553923674E43  order by gamePointId""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_152
test("Comp_VMALL_DICTIONARY_EXCLUDE_152", Include) {
  sql(s"""select gamePointId  from Comp_VMALL_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_153
test("Comp_VMALL_DICTIONARY_EXCLUDE_153", Include) {
  sql(s"""select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE where gamePointId !=4.70133553923674E43""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_154
test("Comp_VMALL_DICTIONARY_EXCLUDE_154", Include) {
  checkAnswer(s"""select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE where gamePointId >4.70133553923674E43""",
    s"""select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE_hive where gamePointId >4.70133553923674E43""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_155
test("Comp_VMALL_DICTIONARY_EXCLUDE_155", Include) {
  checkAnswer(s"""select gamePointId  from Comp_VMALL_DICTIONARY_EXCLUDE where gamePointId<>gamePointId""",
    s"""select gamePointId  from Comp_VMALL_DICTIONARY_EXCLUDE_hive where gamePointId<>gamePointId""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_156
test("Comp_VMALL_DICTIONARY_EXCLUDE_156", Include) {
  sql(s"""select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE where gamePointId != Latest_areaId  order by gamePointId""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_157
test("Comp_VMALL_DICTIONARY_EXCLUDE_157", Include) {
  sql(s"""select gamePointId, gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_areaId<gamePointId  order by gamePointId""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_158
test("Comp_VMALL_DICTIONARY_EXCLUDE_158", Include) {
  sql(s"""select gamePointId, gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE where gamePointId<=gamePointId  order by gamePointId""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_159
test("Comp_VMALL_DICTIONARY_EXCLUDE_159", Include) {
  checkAnswer(s"""select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE where gamePointId <1000 order by gamePointId""",
    s"""select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE_hive where gamePointId <1000 order by gamePointId""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_160
test("Comp_VMALL_DICTIONARY_EXCLUDE_160", Include) {
  sql(s"""select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE where gamePointId >1000 order by gamePointId""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_161
test("Comp_VMALL_DICTIONARY_EXCLUDE_161", Include) {
  sql(s"""select gamePointId  from Comp_VMALL_DICTIONARY_EXCLUDE where gamePointId IS NULL order by gamePointId""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_162
test("Comp_VMALL_DICTIONARY_EXCLUDE_162", Include) {
  sql(s"""select gamePointId  from Comp_VMALL_DICTIONARY_EXCLUDE where gamePointId IS NOT NULL order by gamePointId""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_163
test("Comp_VMALL_DICTIONARY_EXCLUDE_163", Include) {
  sql(s"""Select count(productionDate) from Comp_VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_164
test("Comp_VMALL_DICTIONARY_EXCLUDE_164", Include) {
  checkAnswer(s"""select count(DISTINCT productionDate) as a from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(DISTINCT productionDate) as a from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_165
test("Comp_VMALL_DICTIONARY_EXCLUDE_165", Include) {
  sql(s"""select sum(productionDate)+10 as a ,productionDate  from Comp_VMALL_DICTIONARY_EXCLUDE group by productionDate order by productionDate""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_166
test("Comp_VMALL_DICTIONARY_EXCLUDE_166", Include) {
  checkAnswer(s"""select max(productionDate),min(productionDate) from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select max(productionDate),min(productionDate) from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_167
test("Comp_VMALL_DICTIONARY_EXCLUDE_167", Include) {
  sql(s"""select sum(productionDate) a  from Comp_VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_168
test("Comp_VMALL_DICTIONARY_EXCLUDE_168", Include) {
  checkAnswer(s"""select avg(productionDate) a  from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select avg(productionDate) a  from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_169
test("Comp_VMALL_DICTIONARY_EXCLUDE_169", Include) {
  checkAnswer(s"""select min(productionDate) a  from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select min(productionDate) a  from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_182
test("Comp_VMALL_DICTIONARY_EXCLUDE_182", Include) {
  sql(s"""select histogram_numeric(productionDate,2)  as a from (select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE order by productionDate) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_183
test("Comp_VMALL_DICTIONARY_EXCLUDE_183", Include) {
  checkAnswer(s"""select last(productionDate) a from Comp_VMALL_DICTIONARY_EXCLUDE order by a""",
    s"""select last(productionDate) a from Comp_VMALL_DICTIONARY_EXCLUDE_hive order by a""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_184
test("Comp_VMALL_DICTIONARY_EXCLUDE_184", Include) {
  sql(s"""select FIRST(productionDate) a from (select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE order by productionDate) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_185
test("Comp_VMALL_DICTIONARY_EXCLUDE_185", Include) {
  checkAnswer(s"""select productionDate,count(productionDate) a from Comp_VMALL_DICTIONARY_EXCLUDE group by productionDate order by productionDate""",
    s"""select productionDate,count(productionDate) a from Comp_VMALL_DICTIONARY_EXCLUDE_hive group by productionDate order by productionDate""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_186
test("Comp_VMALL_DICTIONARY_EXCLUDE_186", Include) {
  checkAnswer(s"""select Lower(productionDate) a  from Comp_VMALL_DICTIONARY_EXCLUDE order by productionDate""",
    s"""select Lower(productionDate) a  from Comp_VMALL_DICTIONARY_EXCLUDE_hive order by productionDate""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_187
test("Comp_VMALL_DICTIONARY_EXCLUDE_187", Include) {
  checkAnswer(s"""select distinct productionDate from Comp_VMALL_DICTIONARY_EXCLUDE order by productionDate""",
    s"""select distinct productionDate from Comp_VMALL_DICTIONARY_EXCLUDE_hive order by productionDate""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_188
test("Comp_VMALL_DICTIONARY_EXCLUDE_188", Include) {
  checkAnswer(s"""select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE order by productionDate limit 101""",
    s"""select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE_hive order by productionDate limit 101""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_189
test("Comp_VMALL_DICTIONARY_EXCLUDE_189", Include) {
  checkAnswer(s"""select productionDate as a from Comp_VMALL_DICTIONARY_EXCLUDE  order by a asc limit 10""",
    s"""select productionDate as a from Comp_VMALL_DICTIONARY_EXCLUDE_hive  order by a asc limit 10""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_190
test("Comp_VMALL_DICTIONARY_EXCLUDE_190", Include) {
  checkAnswer(s"""select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE where  (productionDate == '2015-07-01 12:07:28.0') and (productionDate=='2015-07-01 12:07:28.0')""",
    s"""select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE_hive where  (productionDate == '2015-07-01 12:07:28.0') and (productionDate=='2015-07-01 12:07:28.0')""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_191
test("Comp_VMALL_DICTIONARY_EXCLUDE_191", Include) {
  checkAnswer(s"""select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE where productionDate !='2015-07-01 12:07:28.0' order by productionDate""",
    s"""select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE_hive where productionDate !='2015-07-01 12:07:28.0' order by productionDate""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_192
test("Comp_VMALL_DICTIONARY_EXCLUDE_192", Include) {
  checkAnswer(s"""select productionDate  from Comp_VMALL_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
    s"""select productionDate  from Comp_VMALL_DICTIONARY_EXCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_193
test("Comp_VMALL_DICTIONARY_EXCLUDE_193", Include) {
  checkAnswer(s"""select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE where productionDate !='2015-07-01 12:07:28.0' order by productionDate""",
    s"""select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE_hive where productionDate !='2015-07-01 12:07:28.0' order by productionDate""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_194
test("Comp_VMALL_DICTIONARY_EXCLUDE_194", Include) {
  checkAnswer(s"""select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE where productionDate >'2015-07-01 12:07:28.0' order by productionDate""",
    s"""select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE_hive where productionDate >'2015-07-01 12:07:28.0' order by productionDate""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_195
test("Comp_VMALL_DICTIONARY_EXCLUDE_195", Include) {
  checkAnswer(s"""select productionDate  from Comp_VMALL_DICTIONARY_EXCLUDE where productionDate<>productionDate order by productionDate""",
    s"""select productionDate  from Comp_VMALL_DICTIONARY_EXCLUDE_hive where productionDate<>productionDate order by productionDate""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_196
test("Comp_VMALL_DICTIONARY_EXCLUDE_196", Include) {
  checkAnswer(s"""select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE where productionDate != Latest_areaId order by productionDate""",
    s"""select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE_hive where productionDate != Latest_areaId order by productionDate""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_197
test("Comp_VMALL_DICTIONARY_EXCLUDE_197", Include) {
  checkAnswer(s"""select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_areaId<productionDate order by productionDate""",
    s"""select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE_hive where Latest_areaId<productionDate order by productionDate""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_198
test("Comp_VMALL_DICTIONARY_EXCLUDE_198", Include) {
  checkAnswer(s"""select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE where productionDate<=productionDate order by productionDate""",
    s"""select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE_hive where productionDate<=productionDate order by productionDate""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_199
test("Comp_VMALL_DICTIONARY_EXCLUDE_199", Include) {
  sql(s"""select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE where productionDate <'2015-07-01 12:07:28.0' order by productionDate""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_200
test("Comp_VMALL_DICTIONARY_EXCLUDE_200", Include) {
  checkAnswer(s"""select productionDate  from Comp_VMALL_DICTIONARY_EXCLUDE where productionDate IS NULL""",
    s"""select productionDate  from Comp_VMALL_DICTIONARY_EXCLUDE_hive where productionDate IS NULL""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_201
test("Comp_VMALL_DICTIONARY_EXCLUDE_201", Include) {
  checkAnswer(s"""select productionDate  from Comp_VMALL_DICTIONARY_EXCLUDE where productionDate IS NOT NULL order by productionDate""",
    s"""select productionDate  from Comp_VMALL_DICTIONARY_EXCLUDE_hive where productionDate IS NOT NULL order by productionDate""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_202
test("Comp_VMALL_DICTIONARY_EXCLUDE_202", Include) {
  checkAnswer(s"""Select count(deviceInformationId) from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""Select count(deviceInformationId) from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_203
test("Comp_VMALL_DICTIONARY_EXCLUDE_203", Include) {
  checkAnswer(s"""select count(DISTINCT deviceInformationId) as a from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(DISTINCT deviceInformationId) as a from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_204
test("Comp_VMALL_DICTIONARY_EXCLUDE_204", Include) {
  checkAnswer(s"""select sum(deviceInformationId)+10 as a ,deviceInformationId  from Comp_VMALL_DICTIONARY_EXCLUDE group by deviceInformationId order by deviceInformationId""",
    s"""select sum(deviceInformationId)+10 as a ,deviceInformationId  from Comp_VMALL_DICTIONARY_EXCLUDE_hive group by deviceInformationId order by deviceInformationId""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_205
test("Comp_VMALL_DICTIONARY_EXCLUDE_205", Include) {
  checkAnswer(s"""select max(deviceInformationId),min(deviceInformationId) from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select max(deviceInformationId),min(deviceInformationId) from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_206
test("Comp_VMALL_DICTIONARY_EXCLUDE_206", Include) {
  checkAnswer(s"""select sum(deviceInformationId) a  from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(deviceInformationId) a  from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_207
test("Comp_VMALL_DICTIONARY_EXCLUDE_207", Include) {
  checkAnswer(s"""select avg(deviceInformationId) a  from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select avg(deviceInformationId) a  from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_208
test("Comp_VMALL_DICTIONARY_EXCLUDE_208", Include) {
  checkAnswer(s"""select min(deviceInformationId) a  from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select min(deviceInformationId) a  from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_209
test("Comp_VMALL_DICTIONARY_EXCLUDE_209", Include) {
  sql(s"""select variance(deviceInformationId) as a   from (select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_210
test("Comp_VMALL_DICTIONARY_EXCLUDE_210", Include) {
  sql(s"""select var_pop(deviceInformationId)  as a from Comp_VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_211
test("Comp_VMALL_DICTIONARY_EXCLUDE_211", Include) {
  sql(s"""select var_samp(deviceInformationId) as a  from Comp_VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_212
test("Comp_VMALL_DICTIONARY_EXCLUDE_212", Include) {
  sql(s"""select stddev_pop(deviceInformationId) as a  from (select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_213
test("Comp_VMALL_DICTIONARY_EXCLUDE_213", Include) {
  sql(s"""select stddev_samp(deviceInformationId)  as a from (select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_214
test("Comp_VMALL_DICTIONARY_EXCLUDE_214", Include) {
  sql(s"""select covar_pop(deviceInformationId,deviceInformationId) as a  from (select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_215
test("Comp_VMALL_DICTIONARY_EXCLUDE_215", Include) {
  sql(s"""select covar_samp(deviceInformationId,deviceInformationId) as a  from (select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_216
test("Comp_VMALL_DICTIONARY_EXCLUDE_216", Include) {
  checkAnswer(s"""select corr(deviceInformationId,deviceInformationId)  as a from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select corr(deviceInformationId,deviceInformationId)  as a from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_217
test("Comp_VMALL_DICTIONARY_EXCLUDE_217", Include) {
  sql(s"""select percentile_approx(deviceInformationId,0.2) as a  from (select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_218
test("Comp_VMALL_DICTIONARY_EXCLUDE_218", Include) {
  sql(s"""select percentile_approx(deviceInformationId,0.2,5) as a  from (select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_219
test("Comp_VMALL_DICTIONARY_EXCLUDE_219", Include) {
  sql(s"""select percentile_approx(deviceInformationId,array(0.2,0.3,0.99))  as a from (select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_220
test("Comp_VMALL_DICTIONARY_EXCLUDE_220", Include) {
  sql(s"""select percentile_approx(deviceInformationId,array(0.2,0.3,0.99),5) as a from (select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_221
test("Comp_VMALL_DICTIONARY_EXCLUDE_221", Include) {
  sql(s"""select histogram_numeric(deviceInformationId,2)  as a from (select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_222
test("Comp_VMALL_DICTIONARY_EXCLUDE_222", Include) {
  checkAnswer(s"""select deviceInformationId, deviceInformationId+ 10 as a  from Comp_VMALL_DICTIONARY_EXCLUDE order by deviceInformationId""",
    s"""select deviceInformationId, deviceInformationId+ 10 as a  from Comp_VMALL_DICTIONARY_EXCLUDE_hive order by deviceInformationId""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_223
test("Comp_VMALL_DICTIONARY_EXCLUDE_223", Include) {
  checkAnswer(s"""select min(deviceInformationId), max(deviceInformationId+ 10) Total from Comp_VMALL_DICTIONARY_EXCLUDE group by  channelsId order by Total""",
    s"""select min(deviceInformationId), max(deviceInformationId+ 10) Total from Comp_VMALL_DICTIONARY_EXCLUDE_hive group by  channelsId order by Total""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_224
test("Comp_VMALL_DICTIONARY_EXCLUDE_224", Include) {
  sql(s"""select last(deviceInformationId) a from Comp_VMALL_DICTIONARY_EXCLUDE order by a""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_225
test("Comp_VMALL_DICTIONARY_EXCLUDE_225", Include) {
  sql(s"""select FIRST(deviceInformationId) a from (select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_226
test("Comp_VMALL_DICTIONARY_EXCLUDE_226", Include) {
  checkAnswer(s"""select deviceInformationId,count(deviceInformationId) a from Comp_VMALL_DICTIONARY_EXCLUDE group by deviceInformationId order by deviceInformationId""",
    s"""select deviceInformationId,count(deviceInformationId) a from Comp_VMALL_DICTIONARY_EXCLUDE_hive group by deviceInformationId order by deviceInformationId""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_227
test("Comp_VMALL_DICTIONARY_EXCLUDE_227", Include) {
  checkAnswer(s"""select Lower(deviceInformationId) a  from Comp_VMALL_DICTIONARY_EXCLUDE order by deviceInformationId""",
    s"""select Lower(deviceInformationId) a  from Comp_VMALL_DICTIONARY_EXCLUDE_hive order by deviceInformationId""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_228
test("Comp_VMALL_DICTIONARY_EXCLUDE_228", Include) {
  checkAnswer(s"""select distinct deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE order by deviceInformationId""",
    s"""select distinct deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE_hive order by deviceInformationId""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_229
test("Comp_VMALL_DICTIONARY_EXCLUDE_229", Include) {
  checkAnswer(s"""select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE order by deviceInformationId limit 101""",
    s"""select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE_hive order by deviceInformationId limit 101""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_230
test("Comp_VMALL_DICTIONARY_EXCLUDE_230", Include) {
  checkAnswer(s"""select deviceInformationId as a from Comp_VMALL_DICTIONARY_EXCLUDE  order by a asc limit 10""",
    s"""select deviceInformationId as a from Comp_VMALL_DICTIONARY_EXCLUDE_hive  order by a asc limit 10""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_231
test("Comp_VMALL_DICTIONARY_EXCLUDE_231", Include) {
  checkAnswer(s"""select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE where  (deviceInformationId == 100084) and (deviceInformationId==100084)""",
    s"""select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE_hive where  (deviceInformationId == 100084) and (deviceInformationId==100084)""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_232
test("Comp_VMALL_DICTIONARY_EXCLUDE_232", Include) {
  checkAnswer(s"""select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE where deviceInformationId !='100084' order by deviceInformationId""",
    s"""select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId !='100084' order by deviceInformationId""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_233
test("Comp_VMALL_DICTIONARY_EXCLUDE_233", Include) {
  checkAnswer(s"""select deviceInformationId  from Comp_VMALL_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
    s"""select deviceInformationId  from Comp_VMALL_DICTIONARY_EXCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_234
test("Comp_VMALL_DICTIONARY_EXCLUDE_234", Include) {
  checkAnswer(s"""select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE where deviceInformationId !=100084 order by deviceInformationId""",
    s"""select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId !=100084 order by deviceInformationId""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_235
test("Comp_VMALL_DICTIONARY_EXCLUDE_235", Include) {
  checkAnswer(s"""select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE where deviceInformationId >'100084' order by deviceInformationId""",
    s"""select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId >'100084' order by deviceInformationId""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_236
test("Comp_VMALL_DICTIONARY_EXCLUDE_236", Include) {
  checkAnswer(s"""select deviceInformationId  from Comp_VMALL_DICTIONARY_EXCLUDE where deviceInformationId<>deviceInformationId order by deviceInformationId""",
    s"""select deviceInformationId  from Comp_VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId<>deviceInformationId order by deviceInformationId""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_237
test("Comp_VMALL_DICTIONARY_EXCLUDE_237", Include) {
  checkAnswer(s"""select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE where deviceInformationId != Latest_areaId order by deviceInformationId""",
    s"""select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId != Latest_areaId order by deviceInformationId""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_238
test("Comp_VMALL_DICTIONARY_EXCLUDE_238", Include) {
  checkAnswer(s"""select deviceInformationId, deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_areaId<deviceInformationId order by deviceInformationId""",
    s"""select deviceInformationId, deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE_hive where Latest_areaId<deviceInformationId order by deviceInformationId""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_239
test("Comp_VMALL_DICTIONARY_EXCLUDE_239", Include) {
  checkAnswer(s"""select deviceInformationId, deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE where deviceInformationId<=deviceInformationId order by deviceInformationId""",
    s"""select deviceInformationId, deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId<=deviceInformationId order by deviceInformationId""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_240
test("Comp_VMALL_DICTIONARY_EXCLUDE_240", Include) {
  checkAnswer(s"""select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE where deviceInformationId <1000 order by deviceInformationId""",
    s"""select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId <1000 order by deviceInformationId""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_241
test("Comp_VMALL_DICTIONARY_EXCLUDE_241", Include) {
  checkAnswer(s"""select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE where deviceInformationId >1000 order by deviceInformationId""",
    s"""select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId >1000 order by deviceInformationId""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_242
test("Comp_VMALL_DICTIONARY_EXCLUDE_242", Include) {
  checkAnswer(s"""select deviceInformationId  from Comp_VMALL_DICTIONARY_EXCLUDE where deviceInformationId IS NULL order by deviceInformationId""",
    s"""select deviceInformationId  from Comp_VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId IS NULL order by deviceInformationId""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_243
test("Comp_VMALL_DICTIONARY_EXCLUDE_243", Include) {
  checkAnswer(s"""select deviceInformationId  from Comp_VMALL_DICTIONARY_EXCLUDE where deviceInformationId IS NOT NULL order by deviceInformationId""",
    s"""select deviceInformationId  from Comp_VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId IS NOT NULL order by deviceInformationId""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_244
test("Comp_VMALL_DICTIONARY_EXCLUDE_244", Include) {
  checkAnswer(s"""select sum(imei)+10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(imei)+10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_245
test("Comp_VMALL_DICTIONARY_EXCLUDE_245", Include) {
  checkAnswer(s"""select sum(imei)*10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(imei)*10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_246
test("Comp_VMALL_DICTIONARY_EXCLUDE_246", Include) {
  checkAnswer(s"""select sum(imei)/10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(imei)/10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_247
test("Comp_VMALL_DICTIONARY_EXCLUDE_247", Include) {
  checkAnswer(s"""select sum(imei)-10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(imei)-10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_248
test("Comp_VMALL_DICTIONARY_EXCLUDE_248", Include) {
  checkAnswer(s"""select sum(contractNumber)+10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(contractNumber)+10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_249
test("Comp_VMALL_DICTIONARY_EXCLUDE_249", Include) {
  checkAnswer(s"""select sum(contractNumber)*10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(contractNumber)*10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_250
test("Comp_VMALL_DICTIONARY_EXCLUDE_250", Include) {
  checkAnswer(s"""select sum(contractNumber)/10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(contractNumber)/10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_251
test("Comp_VMALL_DICTIONARY_EXCLUDE_251", Include) {
  checkAnswer(s"""select sum(contractNumber)-10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(contractNumber)-10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_252
test("Comp_VMALL_DICTIONARY_EXCLUDE_252", Include) {
  checkAnswer(s"""select sum(Latest_DAY)+10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(Latest_DAY)+10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_253
test("Comp_VMALL_DICTIONARY_EXCLUDE_253", Include) {
  checkAnswer(s"""select sum(Latest_DAY)*10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(Latest_DAY)*10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_254
test("Comp_VMALL_DICTIONARY_EXCLUDE_254", Include) {
  checkAnswer(s"""select sum(Latest_DAY)/10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(Latest_DAY)/10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_255
test("Comp_VMALL_DICTIONARY_EXCLUDE_255", Include) {
  checkAnswer(s"""select sum(Latest_DAY)-10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(Latest_DAY)-10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_256
test("Comp_VMALL_DICTIONARY_EXCLUDE_256", Include) {
  sql(s"""select sum(gamePointId)+10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_257
test("Comp_VMALL_DICTIONARY_EXCLUDE_257", Include) {
  sql(s"""select sum(gamePointId)*10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_258
test("Comp_VMALL_DICTIONARY_EXCLUDE_258", Include) {
  sql(s"""select sum(gamePointId)/10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_259
test("Comp_VMALL_DICTIONARY_EXCLUDE_259", Include) {
  sql(s"""select sum(gamePointId)-10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_260
test("Comp_VMALL_DICTIONARY_EXCLUDE_260", Include) {
  checkAnswer(s"""select sum(productionDate)+10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(productionDate)+10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_261
test("Comp_VMALL_DICTIONARY_EXCLUDE_261", Include) {
  checkAnswer(s"""select sum(productionDate)*10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(productionDate)*10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_262
test("Comp_VMALL_DICTIONARY_EXCLUDE_262", Include) {
  checkAnswer(s"""select sum(productionDate)/10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(productionDate)/10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_263
test("Comp_VMALL_DICTIONARY_EXCLUDE_263", Include) {
  checkAnswer(s"""select sum(productionDate)-10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(productionDate)-10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_264
test("Comp_VMALL_DICTIONARY_EXCLUDE_264", Include) {
  checkAnswer(s"""select sum(deviceInformationId)+10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(deviceInformationId)+10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_265
test("Comp_VMALL_DICTIONARY_EXCLUDE_265", Include) {
  checkAnswer(s"""select sum(deviceInformationId)*10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(deviceInformationId)*10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_266
test("Comp_VMALL_DICTIONARY_EXCLUDE_266", Include) {
  checkAnswer(s"""select sum(deviceInformationId)/10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(deviceInformationId)/10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_267
test("Comp_VMALL_DICTIONARY_EXCLUDE_267", Include) {
  checkAnswer(s"""select sum(deviceInformationId)-10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(deviceInformationId)-10 as a   from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_292
test("Comp_VMALL_DICTIONARY_EXCLUDE_292", Include) {
  checkAnswer(s"""SELECT productiondate from Comp_VMALL_DICTIONARY_EXCLUDE where productiondate LIKE '2015-09-30%'""",
    s"""SELECT productiondate from Comp_VMALL_DICTIONARY_EXCLUDE_hive where productiondate LIKE '2015-09-30%'""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_293
test("Comp_VMALL_DICTIONARY_EXCLUDE_293", Include) {
  checkAnswer(s"""SELECT productiondate from Comp_VMALL_DICTIONARY_EXCLUDE where productiondate LIKE '% %'""",
    s"""SELECT productiondate from Comp_VMALL_DICTIONARY_EXCLUDE_hive where productiondate LIKE '% %'""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_294
test("Comp_VMALL_DICTIONARY_EXCLUDE_294", Include) {
  checkAnswer(s"""SELECT productiondate from Comp_VMALL_DICTIONARY_EXCLUDE where productiondate LIKE '%12:07:28'""",
    s"""SELECT productiondate from Comp_VMALL_DICTIONARY_EXCLUDE_hive where productiondate LIKE '%12:07:28'""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_295
test("Comp_VMALL_DICTIONARY_EXCLUDE_295", Include) {
  checkAnswer(s"""select contractnumber from Comp_VMALL_DICTIONARY_EXCLUDE where contractnumber like '922337204%' """,
    s"""select contractnumber from Comp_VMALL_DICTIONARY_EXCLUDE_hive where contractnumber like '922337204%' """)
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_296
test("Comp_VMALL_DICTIONARY_EXCLUDE_296", Include) {
  checkAnswer(s"""select contractnumber from Comp_VMALL_DICTIONARY_EXCLUDE where contractnumber like '%047800'""",
    s"""select contractnumber from Comp_VMALL_DICTIONARY_EXCLUDE_hive where contractnumber like '%047800'""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_297
test("Comp_VMALL_DICTIONARY_EXCLUDE_297", Include) {
  checkAnswer(s"""select contractnumber from Comp_VMALL_DICTIONARY_EXCLUDE where contractnumber like '%720%'""",
    s"""select contractnumber from Comp_VMALL_DICTIONARY_EXCLUDE_hive where contractnumber like '%720%'""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_298
test("Comp_VMALL_DICTIONARY_EXCLUDE_298", Include) {
  checkAnswer(s"""SELECT Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_DAY like '12345678%'""",
    s"""SELECT Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY like '12345678%'""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_299
test("Comp_VMALL_DICTIONARY_EXCLUDE_299", Include) {
  checkAnswer(s"""SELECT Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_DAY like '%5678%'""",
    s"""SELECT Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY like '%5678%'""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_300
test("Comp_VMALL_DICTIONARY_EXCLUDE_300", Include) {
  checkAnswer(s"""SELECT Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_DAY like '1234567%'""",
    s"""SELECT Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY like '1234567%'""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_301
test("Comp_VMALL_DICTIONARY_EXCLUDE_301", Include) {
  sql(s"""SELECT gamepointID from Comp_VMALL_DICTIONARY_EXCLUDE where gamepointID like '1.1098347722%'""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_302
test("Comp_VMALL_DICTIONARY_EXCLUDE_302", Include) {
  sql(s"""SELECT gamepointID from Comp_VMALL_DICTIONARY_EXCLUDE where gamepointID like '%8347722%'""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_303
test("Comp_VMALL_DICTIONARY_EXCLUDE_303", Include) {
  sql(s"""SELECT gamepointID from Comp_VMALL_DICTIONARY_EXCLUDE where gamepointID like '%7512E42'""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_304
test("Comp_VMALL_DICTIONARY_EXCLUDE_304", Include) {
  checkAnswer(s"""SELECT deviceinformationid from Comp_VMALL_DICTIONARY_EXCLUDE where deviceinformationid like '1000%'""",
    s"""SELECT deviceinformationid from Comp_VMALL_DICTIONARY_EXCLUDE_hive where deviceinformationid like '1000%'""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_305
test("Comp_VMALL_DICTIONARY_EXCLUDE_305", Include) {
  checkAnswer(s"""SELECT deviceinformationid from Comp_VMALL_DICTIONARY_EXCLUDE where deviceinformationid like '%00%'""",
    s"""SELECT deviceinformationid from Comp_VMALL_DICTIONARY_EXCLUDE_hive where deviceinformationid like '%00%'""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_306
test("Comp_VMALL_DICTIONARY_EXCLUDE_306", Include) {
  checkAnswer(s"""SELECT deviceinformationid from Comp_VMALL_DICTIONARY_EXCLUDE where deviceinformationid like '%0084'""",
    s"""SELECT deviceinformationid from Comp_VMALL_DICTIONARY_EXCLUDE_hive where deviceinformationid like '%0084'""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_307
test("Comp_VMALL_DICTIONARY_EXCLUDE_307", Include) {
  checkAnswer(s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE where imei like '1AA10%'""",
    s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE_hive where imei like '1AA10%'""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_308
test("Comp_VMALL_DICTIONARY_EXCLUDE_308", Include) {
  checkAnswer(s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE where imei like '%A10%'""",
    s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE_hive where imei like '%A10%'""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_309
test("Comp_VMALL_DICTIONARY_EXCLUDE_309", Include) {
  checkAnswer(s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE where imei like '%00084'""",
    s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE_hive where imei like '%00084'""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_310
test("Comp_VMALL_DICTIONARY_EXCLUDE_310", Include) {
  checkAnswer(s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE where imei in ('1AA100074','1AA100075','1AA100077')""",
    s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE_hive where imei in ('1AA100074','1AA100075','1AA100077')""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_311
test("Comp_VMALL_DICTIONARY_EXCLUDE_311", Include) {
  checkAnswer(s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE where imei not in ('1AA100074','1AA100075','1AA100077')""",
    s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE_hive where imei not in ('1AA100074','1AA100075','1AA100077')""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_312
test("Comp_VMALL_DICTIONARY_EXCLUDE_312", Include) {
  checkAnswer(s"""select deviceinformationid from Comp_VMALL_DICTIONARY_EXCLUDE where deviceinformationid in (100081,100078,10008)""",
    s"""select deviceinformationid from Comp_VMALL_DICTIONARY_EXCLUDE_hive where deviceinformationid in (100081,100078,10008)""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_313
test("Comp_VMALL_DICTIONARY_EXCLUDE_313", Include) {
  checkAnswer(s"""select deviceinformationid from Comp_VMALL_DICTIONARY_EXCLUDE where deviceinformationid not in (100081,100078,10008)""",
    s"""select deviceinformationid from Comp_VMALL_DICTIONARY_EXCLUDE_hive where deviceinformationid not in (100081,100078,10008)""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_314
test("Comp_VMALL_DICTIONARY_EXCLUDE_314", Include) {
  sql(s"""select productiondate from Comp_VMALL_DICTIONARY_EXCLUDE where productiondate in ('2015-10-04 12:07:28','2015-10-07%','2015-10-07 12:07:28')""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_315
test("Comp_VMALL_DICTIONARY_EXCLUDE_315", Include) {
  sql(s"""select productiondate from Comp_VMALL_DICTIONARY_EXCLUDE where productiondate not in ('2015-10-04 12:07:28','2015-10-07 12:07:28')""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_316
test("Comp_VMALL_DICTIONARY_EXCLUDE_316", Include) {
  sql(s"""select gamepointid from Comp_VMALL_DICTIONARY_EXCLUDE where gamepointid in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_317
test("Comp_VMALL_DICTIONARY_EXCLUDE_317", Include) {
  sql(s"""select gamepointid from Comp_VMALL_DICTIONARY_EXCLUDE where gamepointid not in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_318
test("Comp_VMALL_DICTIONARY_EXCLUDE_318", Include) {
  sql(s"""select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_DAY in (1234567890123530.0000000000,1234567890123520.0000000000)""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_319
test("Comp_VMALL_DICTIONARY_EXCLUDE_319", Include) {
  sql(s"""select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_DAY not in (1234567890123530.0000000000,1234567890123520.0000000000)""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_322
test("Comp_VMALL_DICTIONARY_EXCLUDE_322", Include) {
  checkAnswer(s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE where imei !='1AA100077'""",
    s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE_hive where imei !='1AA100077'""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_323
test("Comp_VMALL_DICTIONARY_EXCLUDE_323", Include) {
  checkAnswer(s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE where imei NOT LIKE '1AA100077'""",
    s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE_hive where imei NOT LIKE '1AA100077'""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_324
test("Comp_VMALL_DICTIONARY_EXCLUDE_324", Include) {
  checkAnswer(s"""select deviceinformationid from Comp_VMALL_DICTIONARY_EXCLUDE where deviceinformationid !=100078""",
    s"""select deviceinformationid from Comp_VMALL_DICTIONARY_EXCLUDE_hive where deviceinformationid !=100078""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_325
test("Comp_VMALL_DICTIONARY_EXCLUDE_325", Include) {
  checkAnswer(s"""select deviceinformationid from Comp_VMALL_DICTIONARY_EXCLUDE where deviceinformationid NOT LIKE 100079""",
    s"""select deviceinformationid from Comp_VMALL_DICTIONARY_EXCLUDE_hive where deviceinformationid NOT LIKE 100079""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_326
test("Comp_VMALL_DICTIONARY_EXCLUDE_326", Include) {
  checkAnswer(s"""select productiondate from Comp_VMALL_DICTIONARY_EXCLUDE where productiondate !='2015-10-07 12:07:28'""",
    s"""select productiondate from Comp_VMALL_DICTIONARY_EXCLUDE_hive where productiondate !='2015-10-07 12:07:28'""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_327
test("Comp_VMALL_DICTIONARY_EXCLUDE_327", Include) {
  sql(s"""select productiondate from Comp_VMALL_DICTIONARY_EXCLUDE where productiondate NOT LIKE '2015-10-07 12:07:28'""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_328
test("Comp_VMALL_DICTIONARY_EXCLUDE_328", Include) {
  sql(s"""select gamepointid from Comp_VMALL_DICTIONARY_EXCLUDE where gamepointid !=6.8591561117512E42""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_329
test("Comp_VMALL_DICTIONARY_EXCLUDE_329", Include) {
  sql(s"""select gamepointid from Comp_VMALL_DICTIONARY_EXCLUDE where gamepointid NOT LIKE 6.8591561117512E43""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_330
test("Comp_VMALL_DICTIONARY_EXCLUDE_330", Include) {
  checkAnswer(s"""select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_DAY != 1234567890123520.0000000000""",
    s"""select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY != 1234567890123520.0000000000""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_331
test("Comp_VMALL_DICTIONARY_EXCLUDE_331", Include) {
  sql(s"""select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_DAY NOT LIKE 1234567890123520.0000000000""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_335
test("Comp_VMALL_DICTIONARY_EXCLUDE_335", Include) {
  checkAnswer(s"""SELECT productiondate,IMEI from Comp_VMALL_DICTIONARY_EXCLUDE where IMEI RLIKE '1AA100077'""",
    s"""SELECT productiondate,IMEI from Comp_VMALL_DICTIONARY_EXCLUDE_hive where IMEI RLIKE '1AA100077'""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_336
test("Comp_VMALL_DICTIONARY_EXCLUDE_336", Include) {
  checkAnswer(s"""SELECT deviceinformationId from Comp_VMALL_DICTIONARY_EXCLUDE where deviceinformationId RLIKE '100079'""",
    s"""SELECT deviceinformationId from Comp_VMALL_DICTIONARY_EXCLUDE_hive where deviceinformationId RLIKE '100079'""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_337
test("Comp_VMALL_DICTIONARY_EXCLUDE_337", Include) {
  sql(s"""SELECT gamepointid from Comp_VMALL_DICTIONARY_EXCLUDE where gamepointid RLIKE '1.61922711065643E42'""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_338
test("Comp_VMALL_DICTIONARY_EXCLUDE_338", Include) {
  checkAnswer(s"""SELECT Latest_Day from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_Day RLIKE '1234567890123550.0000000000'""",
    s"""SELECT Latest_Day from Comp_VMALL_DICTIONARY_EXCLUDE_hive where Latest_Day RLIKE '1234567890123550.0000000000'""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_339
test("Comp_VMALL_DICTIONARY_EXCLUDE_339", Include) {
  checkAnswer(s"""SELECT contractnumber from Comp_VMALL_DICTIONARY_EXCLUDE where contractnumber RLIKE '9223372047800'""",
    s"""SELECT contractnumber from Comp_VMALL_DICTIONARY_EXCLUDE_hive where contractnumber RLIKE '9223372047800'""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_340
test("Comp_VMALL_DICTIONARY_EXCLUDE_340", Include) {
  sql(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_VMALL_DICTIONARY_EXCLUDE a join Comp_VMALL_DICTIONARY_EXCLUDE b on a.productiondate=b.productiondate""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_341
test("Comp_VMALL_DICTIONARY_EXCLUDE_341", Include) {
  sql(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_VMALL_DICTIONARY_EXCLUDE a join Comp_VMALL_DICTIONARY_EXCLUDE b on a.deviceinformationid=b.deviceinformationid""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_342
test("Comp_VMALL_DICTIONARY_EXCLUDE_342", Include) {
  sql(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_VMALL_DICTIONARY_EXCLUDE a join Comp_VMALL_DICTIONARY_EXCLUDE b on a.imei=b.imei""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_343
test("Comp_VMALL_DICTIONARY_EXCLUDE_343", Include) {
  sql(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_VMALL_DICTIONARY_EXCLUDE a join Comp_VMALL_DICTIONARY_EXCLUDE b on a.gamepointid=b.gamepointid""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_344
test("Comp_VMALL_DICTIONARY_EXCLUDE_344", Include) {
  sql(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_VMALL_DICTIONARY_EXCLUDE a join Comp_VMALL_DICTIONARY_EXCLUDE b on a.Latest_Day=b.Latest_Day""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_345
test("Comp_VMALL_DICTIONARY_EXCLUDE_345", Include) {
  sql(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_VMALL_DICTIONARY_EXCLUDE a join Comp_VMALL_DICTIONARY_EXCLUDE b on a.contractnumber=b.contractnumber""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_346
test("Comp_VMALL_DICTIONARY_EXCLUDE_346", Include) {
  checkAnswer(s"""select count( contractNumber ),sum( contractNumber ),count(distinct contractNumber ),avg( contractNumber ),max( contractNumber ),min( contractNumber ),1 from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select count( contractNumber ),sum( contractNumber ),count(distinct contractNumber ),avg( contractNumber ),max( contractNumber ),min( contractNumber ),1 from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_347
test("Comp_VMALL_DICTIONARY_EXCLUDE_347", Include) {
  checkAnswer(s"""select count( Latest_Day ),sum( Latest_Day ),count(distinct Latest_Day ),avg( Latest_Day ),max( Latest_Day ),min( Latest_Day ),1 from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select count( Latest_Day ),sum( Latest_Day ),count(distinct Latest_Day ),avg( Latest_Day ),max( Latest_Day ),min( Latest_Day ),1 from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_348
test("Comp_VMALL_DICTIONARY_EXCLUDE_348", Include) {
  sql(s"""select count( gamePointId),sum( gamePointId ),count(distinct gamePointId ),avg(gamePointId),max(gamePointId),min(gamePointId),1 from Comp_VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_349
test("Comp_VMALL_DICTIONARY_EXCLUDE_349", Include) {
  checkAnswer(s"""select count(deviceInformationId),sum(deviceInformationId),count(deviceInformationId),avg(deviceInformationId),max(deviceInformationId),min(deviceInformationId),1 from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(deviceInformationId),sum(deviceInformationId),count(deviceInformationId),avg(deviceInformationId),max(deviceInformationId),min(deviceInformationId),1 from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_350
test("Comp_VMALL_DICTIONARY_EXCLUDE_350", Include) {
  checkAnswer(s"""select count( productionDate),sum(  productionDate ),count(distinct  productionDate ),avg(  productionDate ),max(  productionDate),min(  productionDate ),1 from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select count( productionDate),sum(  productionDate ),count(distinct  productionDate ),avg(  productionDate ),max(  productionDate),min(  productionDate ),1 from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_351
test("Comp_VMALL_DICTIONARY_EXCLUDE_351", Include) {
  checkAnswer(s"""select count(IMEI ),sum(IMEI ),count(distinct IMEI ),avg(IMEI ),max(IMEI ),min(IMEI ),1 from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(IMEI ),sum(IMEI ),count(distinct IMEI ),avg(IMEI ),max(IMEI ),min(IMEI ),1 from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_352
test("Comp_VMALL_DICTIONARY_EXCLUDE_352", Include) {
  checkAnswer(s"""select sum(contractNumber),count(contractNumber),avg(contractNumber),sum(contractNumber)/count(contractNumber) from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(contractNumber),count(contractNumber),avg(contractNumber),sum(contractNumber)/count(contractNumber) from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_353
test("Comp_VMALL_DICTIONARY_EXCLUDE_353", Include) {
  checkAnswer(s"""select sum(Latest_Day),count(Latest_Day),avg(Latest_Day),sum(Latest_Day)/count(Latest_Day) from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(Latest_Day),count(Latest_Day),avg(Latest_Day),sum(Latest_Day)/count(Latest_Day) from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_354
test("Comp_VMALL_DICTIONARY_EXCLUDE_354", Include) {
  sql(s"""select sum(gamepointId),count(gamepointId),avg(gamepointID),sum(gamepointID)/count(gamepointID) from Comp_VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_355
test("Comp_VMALL_DICTIONARY_EXCLUDE_355", Include) {
  checkAnswer(s"""select sum(deviceinformationId),count(deviceinformationId),avg(deviceinformationId),sum(deviceinformationId)/count(deviceinformationId) from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(deviceinformationId),count(deviceinformationId),avg(deviceinformationId),sum(deviceinformationId)/count(deviceinformationId) from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_356
test("Comp_VMALL_DICTIONARY_EXCLUDE_356", Include) {
  checkAnswer(s"""select sum(IMEI),count(IMEI),avg(IMEI),sum(IMEI)/count(IMEI) from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(IMEI),count(IMEI),avg(IMEI),sum(IMEI)/count(IMEI) from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_357
test("Comp_VMALL_DICTIONARY_EXCLUDE_357", Include) {
  checkAnswer(s"""select sum(productionDate),count(productionDate),avg(productionDate),sum(productionDate)/count(productionDate) from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select sum(productionDate),count(productionDate),avg(productionDate),sum(productionDate)/count(productionDate) from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_358
test("Comp_VMALL_DICTIONARY_EXCLUDE_358", Include) {
  sql(s"""select contractNumber,Latest_DAY,gamePointId,productionDate,deviceInformationId,IMEI  from Comp_VMALL_DICTIONARY_EXCLUDE""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_359
test("Comp_VMALL_DICTIONARY_EXCLUDE_359", Include) {
  checkAnswer(s"""select count(MAC) from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(MAC) from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_360
test("Comp_VMALL_DICTIONARY_EXCLUDE_360", Include) {
  checkAnswer(s"""select count(gamePointId) from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(gamePointId) from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_361
test("Comp_VMALL_DICTIONARY_EXCLUDE_361", Include) {
  checkAnswer(s"""select count(contractNumber) from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(contractNumber) from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_362
test("Comp_VMALL_DICTIONARY_EXCLUDE_362", Include) {
  checkAnswer(s"""select count(Latest_DAY) from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(Latest_DAY) from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_363
test("Comp_VMALL_DICTIONARY_EXCLUDE_363", Include) {
  checkAnswer(s"""select count(productionDate) from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(productionDate) from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_364
test("Comp_VMALL_DICTIONARY_EXCLUDE_364", Include) {
  checkAnswer(s"""select count(deviceInformationId) from Comp_VMALL_DICTIONARY_EXCLUDE""",
    s"""select count(deviceInformationId) from Comp_VMALL_DICTIONARY_EXCLUDE_hive""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_365
test("Comp_VMALL_DICTIONARY_EXCLUDE_365", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_VMALL_DICTIONARY_EXCLUDE where  contractNumber  != '9223372047700'""",
    s"""select imei,deviceInformationId,MAC,deviceColor from Comp_VMALL_DICTIONARY_EXCLUDE_hive where  contractNumber  != '9223372047700'""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_366
test("Comp_VMALL_DICTIONARY_EXCLUDE_366", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_VMALL_DICTIONARY_EXCLUDE where  Latest_DAY  != '1234567890123480.0000000000' order by imei,deviceInformationId,MAC,deviceColor """,
    s"""select imei,deviceInformationId,MAC,deviceColor from Comp_VMALL_DICTIONARY_EXCLUDE_hive where  Latest_DAY  != '1234567890123480.0000000000' order by imei,deviceInformationId,MAC,deviceColor """)
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_367
test("Comp_VMALL_DICTIONARY_EXCLUDE_367", Include) {
  sql(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_VMALL_DICTIONARY_EXCLUDE where  gamePointId  != '2.27852521808948E36' order by imei """).collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_368
test("Comp_VMALL_DICTIONARY_EXCLUDE_368", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_VMALL_DICTIONARY_EXCLUDE where  productionDate  != '2015-09-18 12:07:28.0' order by imei,deviceInformationId,MAC,deviceColor""",
    s"""select imei,deviceInformationId,MAC,deviceColor from Comp_VMALL_DICTIONARY_EXCLUDE_hive where  productionDate  != '2015-09-18 12:07:28.0' order by imei,deviceInformationId,MAC,deviceColor""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_369
test("Comp_VMALL_DICTIONARY_EXCLUDE_369", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_VMALL_DICTIONARY_EXCLUDE where  deviceInformationId  != '100075' order by imei,deviceInformationId,MAC,deviceColor """,
    s"""select imei,deviceInformationId,MAC,deviceColor from Comp_VMALL_DICTIONARY_EXCLUDE_hive where  deviceInformationId  != '100075' order by imei,deviceInformationId,MAC,deviceColor """)
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_370
test("Comp_VMALL_DICTIONARY_EXCLUDE_370", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_VMALL_DICTIONARY_EXCLUDE where  contractNumber  not like '9223372047700' order by  imei,deviceInformationId,MAC,deviceColor """,
    s"""select imei,deviceInformationId,MAC,deviceColor from Comp_VMALL_DICTIONARY_EXCLUDE_hive where  contractNumber  not like '9223372047700' order by  imei,deviceInformationId,MAC,deviceColor """)
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_371
test("Comp_VMALL_DICTIONARY_EXCLUDE_371", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_VMALL_DICTIONARY_EXCLUDE where  Latest_DAY  not like '1234567890123480.0000000000' order by imei,deviceInformationId,MAC,deviceColor""",
    s"""select imei,deviceInformationId,MAC,deviceColor from Comp_VMALL_DICTIONARY_EXCLUDE_hive where  Latest_DAY  not like '1234567890123480.0000000000' order by imei,deviceInformationId,MAC,deviceColor""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_372
test("Comp_VMALL_DICTIONARY_EXCLUDE_372", Include) {
  sql(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_VMALL_DICTIONARY_EXCLUDE where  gamePointId  not like '2.27852521808948E36' order by imei""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_373
test("Comp_VMALL_DICTIONARY_EXCLUDE_373", Include) {
  sql(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_VMALL_DICTIONARY_EXCLUDE where  productionDate  not like '2015-09-18 12:07:28.0' order by imei """).collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_374
test("Comp_VMALL_DICTIONARY_EXCLUDE_374", Include) {
  checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_VMALL_DICTIONARY_EXCLUDE where  deviceInformationId  not like '100075' order by imei,deviceInformationId,MAC,deviceColor""",
    s"""select imei,deviceInformationId,MAC,deviceColor from Comp_VMALL_DICTIONARY_EXCLUDE_hive where  deviceInformationId  not like '100075' order by imei,deviceInformationId,MAC,deviceColor""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_375
test("Comp_VMALL_DICTIONARY_EXCLUDE_375", Include) {
  checkAnswer(s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE where imei is not null""",
    s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE_hive where imei is not null""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_376
test("Comp_VMALL_DICTIONARY_EXCLUDE_376", Include) {
  sql(s"""select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE where gamePointId is not null""").collect
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_377
test("Comp_VMALL_DICTIONARY_EXCLUDE_377", Include) {
  checkAnswer(s"""select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE where contractNumber is not null""",
    s"""select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE_hive where contractNumber is not null""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_378
test("Comp_VMALL_DICTIONARY_EXCLUDE_378", Include) {
  checkAnswer(s"""select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_DAY is not null""",
    s"""select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY is not null""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_379
test("Comp_VMALL_DICTIONARY_EXCLUDE_379", Include) {
  checkAnswer(s"""select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE where productionDate is not null""",
    s"""select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE_hive where productionDate is not null""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_380
test("Comp_VMALL_DICTIONARY_EXCLUDE_380", Include) {
  checkAnswer(s"""select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE where deviceInformationId is not null""",
    s"""select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId is not null""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_381
test("Comp_VMALL_DICTIONARY_EXCLUDE_381", Include) {
  checkAnswer(s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE where imei is  null""",
    s"""select imei from Comp_VMALL_DICTIONARY_EXCLUDE_hive where imei is  null""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_382
test("Comp_VMALL_DICTIONARY_EXCLUDE_382", Include) {
  checkAnswer(s"""select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE where gamePointId is  null""",
    s"""select gamePointId from Comp_VMALL_DICTIONARY_EXCLUDE_hive where gamePointId is  null""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_383
test("Comp_VMALL_DICTIONARY_EXCLUDE_383", Include) {
  checkAnswer(s"""select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE where contractNumber is  null""",
    s"""select contractNumber from Comp_VMALL_DICTIONARY_EXCLUDE_hive where contractNumber is  null""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_384
test("Comp_VMALL_DICTIONARY_EXCLUDE_384", Include) {
  checkAnswer(s"""select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE where Latest_DAY is  null""",
    s"""select Latest_DAY from Comp_VMALL_DICTIONARY_EXCLUDE_hive where Latest_DAY is  null""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_385
test("Comp_VMALL_DICTIONARY_EXCLUDE_385", Include) {
  checkAnswer(s"""select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE where productionDate is  null""",
    s"""select productionDate from Comp_VMALL_DICTIONARY_EXCLUDE_hive where productionDate is  null""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_386
test("Comp_VMALL_DICTIONARY_EXCLUDE_386", Include) {
  checkAnswer(s"""select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE where deviceInformationId is  null""",
    s"""select deviceInformationId from Comp_VMALL_DICTIONARY_EXCLUDE_hive where deviceInformationId is  null""")
}
       

//Comp_VMALL_DICTIONARY_EXCLUDE_387
test("Comp_VMALL_DICTIONARY_EXCLUDE_387", Include) {
  checkAnswer(s"""select count(*) from Comp_VMALL_DICTIONARY_EXCLUDE where imei = '1AA1'""",
    s"""select count(*) from Comp_VMALL_DICTIONARY_EXCLUDE_hive where imei = '1AA1'""")
}
       
override def afterAll {
sql("drop table if exists comp_vmall_dictionary_exclude")
sql("drop table if exists comp_vmall_dictionary_exclude_hive")
sql("drop table if exists Comp_VMALL_DICTIONARY_EXCLUDE")
sql("drop table if exists Comp_VMALL_DICTIONARY_EXCLUDE_hive")
}
}