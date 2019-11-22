
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
 * Test Class for QueriesCompactionTestCase to verify all scenerios
 */

class QueriesCompactionTestCase extends QueryTest with BeforeAndAfterAll {
         

  //Comp_DICTIONARY_INCLUDE_CreateCube
  test("Comp_DICTIONARY_INCLUDE_CreateCube", Include) {
    sql(s"""drop table if exists Comp_DICTIONARY_INCLUDE""").collect
    sql(s"""drop table if exists Comp_DICTIONARY_INCLUDE_hive""").collect

    sql(s"""create table  Comp_DICTIONARY_INCLUDE (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt)  STORED BY 'org.apache.carbondata.format'
  """).collect

    sql(s"""create table  Comp_DICTIONARY_INCLUDE_hive (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string,deliveryTime string,channelsId string,channelsName string,deliveryAreaId string,deliveryCountry string,deliveryProvince string,deliveryCity string,deliveryDistrict string,deliveryStreet string,oxSingleNumber string,contractNumber BigInt,ActiveCheckTime string,ActiveAreaId string,ActiveCountry string,ActiveProvince string,Activecity string,ActiveDistrict string,ActiveStreet string,ActiveOperatorId string,Active_releaseId string,Active_EMUIVersion string,Active_operaSysVersion string,Active_BacVerNumber string,Active_BacFlashVer string,Active_webUIVersion string,Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string,Active_operatorsVersion string,Active_phonePADPartitionedVersions string,Latest_YEAR int,Latest_MONTH int,Latest_DAY Decimal(30,10),Latest_HOUR string,Latest_areaId string,Latest_country string,Latest_province string,Latest_city string,Latest_district string,Latest_street string,Latest_releaseId string,Latest_EMUIVersion string,Latest_operaSysVersion string,Latest_BacVerNumber string,Latest_BacFlashVer string,Latest_webUIVersion string,Latest_webUITypeCarrVer string,Latest_webTypeDataVerNumber string,Latest_operatorsVersion string,Latest_phonePADPartitionedVersions string,Latest_operatorId string,gamePointId double,gamePointDescription string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect


  }


  //Comp_DICTIONARY_INCLUDE_DataLoad1
  test("Comp_DICTIONARY_INCLUDE_DataLoad1", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_INCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive1.csv' INTO table Comp_DICTIONARY_INCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_INCLUDE_DataLoad2
  test("Comp_DICTIONARY_INCLUDE_DataLoad2", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_INCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive2.csv' INTO table Comp_DICTIONARY_INCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_INCLUDE_DataLoad3
  test("Comp_DICTIONARY_INCLUDE_DataLoad3", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_INCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive3.csv' INTO table Comp_DICTIONARY_INCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_INCLUDE_DataLoad4
  test("Comp_DICTIONARY_INCLUDE_DataLoad4", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_INCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive4.csv' INTO table Comp_DICTIONARY_INCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_INCLUDE_DataLoad5
  test("Comp_DICTIONARY_INCLUDE_DataLoad5", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_INCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive5.csv' INTO table Comp_DICTIONARY_INCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_INCLUDE_DataLoad6
  test("Comp_DICTIONARY_INCLUDE_DataLoad6", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_INCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive6.csv' INTO table Comp_DICTIONARY_INCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_INCLUDE_DataLoad7
  test("Comp_DICTIONARY_INCLUDE_DataLoad7", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_INCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive7.csv' INTO table Comp_DICTIONARY_INCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_INCLUDE_DataLoad8
  test("Comp_DICTIONARY_INCLUDE_DataLoad8", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_INCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive8.csv' INTO table Comp_DICTIONARY_INCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_INCLUDE_DataLoad9
  test("Comp_DICTIONARY_INCLUDE_DataLoad9", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_INCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive9.csv' INTO table Comp_DICTIONARY_INCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_INCLUDE_DataLoad10
  test("Comp_DICTIONARY_INCLUDE_DataLoad10", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_INCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive10.csv' INTO table Comp_DICTIONARY_INCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_INCLUDE_DataLoad11
  test("Comp_DICTIONARY_INCLUDE_DataLoad11", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_INCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive11.csv' INTO table Comp_DICTIONARY_INCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_INCLUDE_DataLoad12
  test("Comp_DICTIONARY_INCLUDE_DataLoad12", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_INCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive12.csv' INTO table Comp_DICTIONARY_INCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_INCLUDE_DataLoad13
  test("Comp_DICTIONARY_INCLUDE_DataLoad13", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_INCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive13.csv' INTO table Comp_DICTIONARY_INCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_INCLUDE_DataLoad14
  test("Comp_DICTIONARY_INCLUDE_DataLoad14", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_INCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive14.csv' INTO table Comp_DICTIONARY_INCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_INCLUDE_DataLoad15
  test("Comp_DICTIONARY_INCLUDE_DataLoad15", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_INCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive15.csv' INTO table Comp_DICTIONARY_INCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_INCLUDE_DataLoad16
  test("Comp_DICTIONARY_INCLUDE_DataLoad16", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_INCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive16.csv' INTO table Comp_DICTIONARY_INCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_INCLUDE_DataLoad17
  test("Comp_DICTIONARY_INCLUDE_DataLoad17", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_INCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive17.csv' INTO table Comp_DICTIONARY_INCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_INCLUDE_DataLoad18
  test("Comp_DICTIONARY_INCLUDE_DataLoad18", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_INCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive18.csv' INTO table Comp_DICTIONARY_INCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_INCLUDE_DataLoad19
  test("Comp_DICTIONARY_INCLUDE_DataLoad19", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_INCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive19.csv' INTO table Comp_DICTIONARY_INCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_INCLUDE_DataLoad20
  test("Comp_DICTIONARY_INCLUDE_DataLoad20", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_INCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive20.csv' INTO table Comp_DICTIONARY_INCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_INCLUDE_MajorCompaction
  test("Comp_DICTIONARY_INCLUDE_MajorCompaction", Include) {

    sql(s"""alter table Comp_DICTIONARY_INCLUDE compact 'Major'""").collect
  }


  //Comp_DICTIONARY_EXCLUDE_CreateCube
  test("Comp_DICTIONARY_EXCLUDE_CreateCube", Include) {
    sql(s"""drop table if exists Comp_DICTIONARY_EXCLUDE""").collect
    sql(s"""drop table if exists Comp_DICTIONARY_EXCLUDE_hive""").collect

    sql(s"""create table  Comp_DICTIONARY_EXCLUDE (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt)  STORED BY 'org.apache.carbondata.format' """).collect

    sql(s"""create table  Comp_DICTIONARY_EXCLUDE_hive (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string,deliveryTime string,channelsId string,channelsName string,deliveryAreaId string,deliveryCountry string,deliveryProvince string,deliveryCity string,deliveryDistrict string,deliveryStreet string,oxSingleNumber string,contractNumber BigInt,ActiveCheckTime string,ActiveAreaId string,ActiveCountry string,ActiveProvince string,Activecity string,ActiveDistrict string,ActiveStreet string,ActiveOperatorId string,Active_releaseId string,Active_EMUIVersion string,Active_operaSysVersion string,Active_BacVerNumber string,Active_BacFlashVer string,Active_webUIVersion string,Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string,Active_operatorsVersion string,Active_phonePADPartitionedVersions string,Latest_YEAR int,Latest_MONTH int,Latest_DAY Decimal(30,10),Latest_HOUR string,Latest_areaId string,Latest_country string,Latest_province string,Latest_city string,Latest_district string,Latest_street string,Latest_releaseId string,Latest_EMUIVersion string,Latest_operaSysVersion string,Latest_BacVerNumber string,Latest_BacFlashVer string,Latest_webUIVersion string,Latest_webUITypeCarrVer string,Latest_webTypeDataVerNumber string,Latest_operatorsVersion string,Latest_phonePADPartitionedVersions string,Latest_operatorId string,gamePointId double,gamePointDescription string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect


  }


  //Comp_DICTIONARY_EXCLUDE_DataLoad1
  test("Comp_DICTIONARY_EXCLUDE_DataLoad1", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive21.csv' INTO table Comp_DICTIONARY_EXCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_EXCLUDE_DataLoad2
  test("Comp_DICTIONARY_EXCLUDE_DataLoad2", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive22.csv' INTO table Comp_DICTIONARY_EXCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_EXCLUDE_DataLoad3
  test("Comp_DICTIONARY_EXCLUDE_DataLoad3", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive23.csv' INTO table Comp_DICTIONARY_EXCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_EXCLUDE_DataLoad4
  test("Comp_DICTIONARY_EXCLUDE_DataLoad4", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive24.csv' INTO table Comp_DICTIONARY_EXCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_EXCLUDE_DataLoad5
  test("Comp_DICTIONARY_EXCLUDE_DataLoad5", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive25.csv' INTO table Comp_DICTIONARY_EXCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_EXCLUDE_DataLoad6
  test("Comp_DICTIONARY_EXCLUDE_DataLoad6", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive26.csv' INTO table Comp_DICTIONARY_EXCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_EXCLUDE_DataLoad7
  test("Comp_DICTIONARY_EXCLUDE_DataLoad7", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive27.csv' INTO table Comp_DICTIONARY_EXCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_EXCLUDE_DataLoad8
  test("Comp_DICTIONARY_EXCLUDE_DataLoad8", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive28.csv' INTO table Comp_DICTIONARY_EXCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_EXCLUDE_DataLoad9
  test("Comp_DICTIONARY_EXCLUDE_DataLoad9", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive29.csv' INTO table Comp_DICTIONARY_EXCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_EXCLUDE_DataLoad10
  test("Comp_DICTIONARY_EXCLUDE_DataLoad10", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"',  'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive30.csv' INTO table Comp_DICTIONARY_EXCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_EXCLUDE_DataLoad11
  test("Comp_DICTIONARY_EXCLUDE_DataLoad11", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive31.csv' INTO table Comp_DICTIONARY_EXCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_EXCLUDE_DataLoad12
  test("Comp_DICTIONARY_EXCLUDE_DataLoad12", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive32.csv' INTO table Comp_DICTIONARY_EXCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_EXCLUDE_DataLoad13
  test("Comp_DICTIONARY_EXCLUDE_DataLoad13", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive33.csv' INTO table Comp_DICTIONARY_EXCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_EXCLUDE_DataLoad14
  test("Comp_DICTIONARY_EXCLUDE_DataLoad14", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive34.csv' INTO table Comp_DICTIONARY_EXCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_EXCLUDE_DataLoad15
  test("Comp_DICTIONARY_EXCLUDE_DataLoad15", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive35.csv' INTO table Comp_DICTIONARY_EXCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_EXCLUDE_DataLoad16
  test("Comp_DICTIONARY_EXCLUDE_DataLoad16", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive36.csv' INTO table Comp_DICTIONARY_EXCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_EXCLUDE_DataLoad17
  test("Comp_DICTIONARY_EXCLUDE_DataLoad17", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive37.csv' INTO table Comp_DICTIONARY_EXCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_EXCLUDE_DataLoad18
  test("Comp_DICTIONARY_EXCLUDE_DataLoad18", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive38.csv' INTO table Comp_DICTIONARY_EXCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_EXCLUDE_DataLoad19
  test("Comp_DICTIONARY_EXCLUDE_DataLoad19", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive39.csv' INTO table Comp_DICTIONARY_EXCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_EXCLUDE_DataLoad20
  test("Comp_DICTIONARY_EXCLUDE_DataLoad20", Include) {

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20.csv' INTO table Comp_DICTIONARY_EXCLUDE options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/100_olap_C20_hive40.csv' INTO table Comp_DICTIONARY_EXCLUDE_hive """).collect


  }


  //Comp_DICTIONARY_EXCLUDE_MinorCompaction
  test("Comp_DICTIONARY_EXCLUDE_MinorCompaction", Include) {

    sql(s"""alter table Comp_DICTIONARY_EXCLUDE compact 'Minor'""").collect

  }


  //Comp_DICTIONARY_INCLUDE_001
  test("Comp_DICTIONARY_INCLUDE_001", Include) {

    checkAnswer(s"""Select count(imei) from Comp_DICTIONARY_INCLUDE""",
      s"""Select count(imei) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_001")

  }


  //Comp_DICTIONARY_INCLUDE_002
  test("Comp_DICTIONARY_INCLUDE_002", Include) {

    checkAnswer(s"""select count(DISTINCT imei) as a from Comp_DICTIONARY_INCLUDE""",
      s"""select count(DISTINCT imei) as a from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_002")

  }


  //Comp_DICTIONARY_INCLUDE_003
  test("Comp_DICTIONARY_INCLUDE_003", Include) {

    checkAnswer(s"""select sum(Latest_month)+10 as a ,imei  from Comp_DICTIONARY_INCLUDE group by imei order by imei""",
      s"""select sum(Latest_month)+10 as a ,imei  from Comp_DICTIONARY_INCLUDE_hive group by imei order by imei""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_003")

  }


  //Comp_DICTIONARY_INCLUDE_004
  test("Comp_DICTIONARY_INCLUDE_004", Include) {

    checkAnswer(s"""select max(imei),min(imei) from Comp_DICTIONARY_INCLUDE""",
      s"""select max(imei),min(imei) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_004")

  }


  //Comp_DICTIONARY_INCLUDE_005
  test("Comp_DICTIONARY_INCLUDE_005", Include) {

    checkAnswer(s"""select min(imei), max(imei) Total from Comp_DICTIONARY_INCLUDE group by  channelsId order by Total""",
      s"""select min(imei), max(imei) Total from Comp_DICTIONARY_INCLUDE_hive group by  channelsId order by Total""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_005")

  }


  //Comp_DICTIONARY_INCLUDE_006
  test("Comp_DICTIONARY_INCLUDE_006", Include) {

    checkAnswer(s"""select last(imei) a from Comp_DICTIONARY_INCLUDE  group by imei order by imei limit 1""",
      s"""select last(imei) a from Comp_DICTIONARY_INCLUDE_hive  group by imei order by imei limit 1""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_006")

  }


  //Comp_DICTIONARY_INCLUDE_007
  test("Comp_DICTIONARY_INCLUDE_007", Include) {

    sql(s"""select FIRST(imei) a from Comp_DICTIONARY_INCLUDE group by imei order by imei limit 1""").collect

  }


  //Comp_DICTIONARY_INCLUDE_008
  test("Comp_DICTIONARY_INCLUDE_008", Include) {

    checkAnswer(s"""select imei,count(imei) a from Comp_DICTIONARY_INCLUDE group by imei order by imei""",
      s"""select imei,count(imei) a from Comp_DICTIONARY_INCLUDE_hive group by imei order by imei""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_008")

  }


  //Comp_DICTIONARY_INCLUDE_009
  test("Comp_DICTIONARY_INCLUDE_009", Include) {

    checkAnswer(s"""select Lower(imei) a  from Comp_DICTIONARY_INCLUDE order by imei""",
      s"""select Lower(imei) a  from Comp_DICTIONARY_INCLUDE_hive order by imei""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_009")

  }


  //Comp_DICTIONARY_INCLUDE_010
  test("Comp_DICTIONARY_INCLUDE_010", Include) {

    checkAnswer(s"""select distinct imei from Comp_DICTIONARY_INCLUDE order by imei""",
      s"""select distinct imei from Comp_DICTIONARY_INCLUDE_hive order by imei""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_010")

  }


  //Comp_DICTIONARY_INCLUDE_011
  test("Comp_DICTIONARY_INCLUDE_011", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_INCLUDE order by imei limit 101 """,
      s"""select imei from Comp_DICTIONARY_INCLUDE_hive order by imei limit 101 """, "QueriesCompactionTestCase_DICTIONARY_INCLUDE_011")

  }


  //Comp_DICTIONARY_INCLUDE_012
  test("Comp_DICTIONARY_INCLUDE_012", Include) {

    checkAnswer(s"""select imei as a from Comp_DICTIONARY_INCLUDE  order by a asc limit 10""",
      s"""select imei as a from Comp_DICTIONARY_INCLUDE_hive  order by a asc limit 10""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_012")

  }


  //Comp_DICTIONARY_INCLUDE_013
  test("Comp_DICTIONARY_INCLUDE_013", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_INCLUDE where  (contractNumber == 9223372047700) and (imei=='1AA100004')""",
      s"""select imei from Comp_DICTIONARY_INCLUDE_hive where  (contractNumber == 9223372047700) and (imei=='1AA100004')""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_013")

  }


  //Comp_DICTIONARY_INCLUDE_014
  test("Comp_DICTIONARY_INCLUDE_014", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_INCLUDE where imei !='1AA100064' order by imei""",
      s"""select imei from Comp_DICTIONARY_INCLUDE_hive where imei !='1AA100064' order by imei""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_014")

  }


  //Comp_DICTIONARY_INCLUDE_015
  test("Comp_DICTIONARY_INCLUDE_015", Include) {

    checkAnswer(s"""select imei  from Comp_DICTIONARY_INCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select imei  from Comp_DICTIONARY_INCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_015")

  }


  //Comp_DICTIONARY_INCLUDE_016
  test("Comp_DICTIONARY_INCLUDE_016", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_INCLUDE where imei !='1AA100012' order by imei""",
      s"""select imei from Comp_DICTIONARY_INCLUDE_hive where imei !='1AA100012' order by imei""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_016")

  }


  //Comp_DICTIONARY_INCLUDE_017
  test("Comp_DICTIONARY_INCLUDE_017", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_INCLUDE where imei >'1AA100012' order by imei""",
      s"""select imei from Comp_DICTIONARY_INCLUDE_hive where imei >'1AA100012' order by imei""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_017")

  }


  //Comp_DICTIONARY_INCLUDE_018
  test("Comp_DICTIONARY_INCLUDE_018", Include) {

    checkAnswer(s"""select imei  from Comp_DICTIONARY_INCLUDE where imei<>imei""",
      s"""select imei  from Comp_DICTIONARY_INCLUDE_hive where imei<>imei""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_018")

  }


  //Comp_DICTIONARY_INCLUDE_019
  test("Comp_DICTIONARY_INCLUDE_019", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_INCLUDE where imei != Latest_areaId order by imei""",
      s"""select imei from Comp_DICTIONARY_INCLUDE_hive where imei != Latest_areaId order by imei""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_019")

  }


  //Comp_DICTIONARY_INCLUDE_020
  test("Comp_DICTIONARY_INCLUDE_020", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_INCLUDE where Latest_areaId<imei order by imei""",
      s"""select imei from Comp_DICTIONARY_INCLUDE_hive where Latest_areaId<imei order by imei""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_020")

  }


  //Comp_DICTIONARY_INCLUDE_021
  test("Comp_DICTIONARY_INCLUDE_021", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_INCLUDE where Latest_DAY<=imei order by imei""",
      s"""select imei from Comp_DICTIONARY_INCLUDE_hive where Latest_DAY<=imei order by imei""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_021")

  }


  //Comp_DICTIONARY_INCLUDE_022
  test("Comp_DICTIONARY_INCLUDE_022", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_INCLUDE where imei <'1AA10002' order by imei""",
      s"""select imei from Comp_DICTIONARY_INCLUDE_hive where imei <'1AA10002' order by imei""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_022")

  }


  //Comp_DICTIONARY_INCLUDE_023
  test("Comp_DICTIONARY_INCLUDE_023", Include) {

    checkAnswer(s"""select Latest_day  from Comp_DICTIONARY_INCLUDE where imei IS NULL""",
      s"""select Latest_day  from Comp_DICTIONARY_INCLUDE_hive where imei IS NULL""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_023")

  }


  //Comp_DICTIONARY_INCLUDE_024
  test("Comp_DICTIONARY_INCLUDE_024", Include) {

    checkAnswer(s"""select Latest_day  from Comp_DICTIONARY_INCLUDE where imei IS NOT NULL order by Latest_day""",
      s"""select Latest_day  from Comp_DICTIONARY_INCLUDE_hive where imei IS NOT NULL order by Latest_day""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_024")

  }


  //Comp_DICTIONARY_INCLUDE_025
  test("Comp_DICTIONARY_INCLUDE_025", Include) {

    checkAnswer(s"""Select count(imei),min(imei) from Comp_DICTIONARY_INCLUDE """,
      s"""Select count(imei),min(imei) from Comp_DICTIONARY_INCLUDE_hive """, "QueriesCompactionTestCase_DICTIONARY_INCLUDE_025")

  }


  //Comp_DICTIONARY_INCLUDE_026
  test("Comp_DICTIONARY_INCLUDE_026", Include) {

    checkAnswer(s"""select count(DISTINCT imei,latest_day) as a from Comp_DICTIONARY_INCLUDE""",
      s"""select count(DISTINCT imei,latest_day) as a from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_026")

  }


  //Comp_DICTIONARY_INCLUDE_027
  test("Comp_DICTIONARY_INCLUDE_027", Include) {

    checkAnswer(s"""select max(imei),min(imei),count(imei) from Comp_DICTIONARY_INCLUDE""",
      s"""select max(imei),min(imei),count(imei) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_027")

  }


  //Comp_DICTIONARY_INCLUDE_028
  test("Comp_DICTIONARY_INCLUDE_028", Include) {

    checkAnswer(s"""select sum(imei),avg(imei),count(imei) a  from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(imei),avg(imei),count(imei) a  from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_028")

  }


  //Comp_DICTIONARY_INCLUDE_029
  test("Comp_DICTIONARY_INCLUDE_029", Include) {

    sql(s"""select last(imei),Min(imei),max(imei)  a from (select imei from Comp_DICTIONARY_INCLUDE order by imei) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_030
  test("Comp_DICTIONARY_INCLUDE_030", Include) {

    sql(s"""select FIRST(imei),Last(imei) a from Comp_DICTIONARY_INCLUDE group by imei order by imei limit 1""").collect

  }


  //Comp_DICTIONARY_INCLUDE_031
  test("Comp_DICTIONARY_INCLUDE_031", Include) {

    checkAnswer(s"""select imei,count(imei) a from Comp_DICTIONARY_INCLUDE group by imei order by imei""",
      s"""select imei,count(imei) a from Comp_DICTIONARY_INCLUDE_hive group by imei order by imei""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_031")

  }


  //Comp_DICTIONARY_INCLUDE_032
  test("Comp_DICTIONARY_INCLUDE_032", Include) {

    checkAnswer(s"""select Lower(imei),upper(imei)  a  from Comp_DICTIONARY_INCLUDE order by imei""",
      s"""select Lower(imei),upper(imei)  a  from Comp_DICTIONARY_INCLUDE_hive order by imei""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_032")

  }


  //Comp_DICTIONARY_INCLUDE_033
  test("Comp_DICTIONARY_INCLUDE_033", Include) {

    checkAnswer(s"""select imei as a from Comp_DICTIONARY_INCLUDE  order by a asc limit 10""",
      s"""select imei as a from Comp_DICTIONARY_INCLUDE_hive  order by a asc limit 10""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_033")

  }


  //Comp_DICTIONARY_INCLUDE_034
  test("Comp_DICTIONARY_INCLUDE_034", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_INCLUDE where  (contractNumber == 9223372047700) and (imei=='1AA100012')""",
      s"""select imei from Comp_DICTIONARY_INCLUDE_hive where  (contractNumber == 9223372047700) and (imei=='1AA100012')""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_034")

  }


  //Comp_DICTIONARY_INCLUDE_035
  test("Comp_DICTIONARY_INCLUDE_035", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_INCLUDE where imei !='8imei' order by imei""",
      s"""select imei from Comp_DICTIONARY_INCLUDE_hive where imei !='8imei' order by imei""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_035")

  }


  //Comp_DICTIONARY_INCLUDE_036
  test("Comp_DICTIONARY_INCLUDE_036", Include) {

    checkAnswer(s"""select imei  from Comp_DICTIONARY_INCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select imei  from Comp_DICTIONARY_INCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_036")

  }


  //Comp_DICTIONARY_INCLUDE_037
  test("Comp_DICTIONARY_INCLUDE_037", Include) {

    checkAnswer(s"""Select count(contractNumber) from Comp_DICTIONARY_INCLUDE""",
      s"""Select count(contractNumber) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_037")

  }


  //Comp_DICTIONARY_INCLUDE_038
  test("Comp_DICTIONARY_INCLUDE_038", Include) {

    checkAnswer(s"""select count(DISTINCT contractNumber) as a from Comp_DICTIONARY_INCLUDE""",
      s"""select count(DISTINCT contractNumber) as a from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_038")

  }


  //Comp_DICTIONARY_INCLUDE_039
  test("Comp_DICTIONARY_INCLUDE_039", Include) {

    checkAnswer(s"""select sum(contractNumber)+10 as a ,contractNumber  from Comp_DICTIONARY_INCLUDE group by contractNumber""",
      s"""select sum(contractNumber)+10 as a ,contractNumber  from Comp_DICTIONARY_INCLUDE_hive group by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_039")

  }


  //Comp_DICTIONARY_INCLUDE_040
  test("Comp_DICTIONARY_INCLUDE_040", Include) {

    checkAnswer(s"""select max(contractNumber),min(contractNumber) from Comp_DICTIONARY_INCLUDE""",
      s"""select max(contractNumber),min(contractNumber) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_040")

  }


  //Comp_DICTIONARY_INCLUDE_041
  test("Comp_DICTIONARY_INCLUDE_041", Include) {

    checkAnswer(s"""select sum(contractNumber) a  from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(contractNumber) a  from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_041")

  }


  //Comp_DICTIONARY_INCLUDE_042
  test("Comp_DICTIONARY_INCLUDE_042", Include) {

    checkAnswer(s"""select avg(contractNumber) a  from Comp_DICTIONARY_INCLUDE""",
      s"""select avg(contractNumber) a  from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_042")

  }


  //Comp_DICTIONARY_INCLUDE_043
  test("Comp_DICTIONARY_INCLUDE_043", Include) {

    checkAnswer(s"""select min(contractNumber) a  from Comp_DICTIONARY_INCLUDE""",
      s"""select min(contractNumber) a  from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_043")

  }


  //Comp_DICTIONARY_INCLUDE_044
  test("Comp_DICTIONARY_INCLUDE_044", Include) {

    sql(s"""select variance(contractNumber) as a   from (select contractNumber from Comp_DICTIONARY_INCLUDE order by contractNumber) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_045
  test("Comp_DICTIONARY_INCLUDE_045", Include) {

    checkAnswer(s"""select var_pop(contractNumber) as a from (select * from Comp_DICTIONARY_INCLUDE order by contractNumber) t""",
      s"""select var_pop(contractNumber) as a from (select * from Comp_DICTIONARY_INCLUDE_hive order by contractNumber) t""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_045")

  }


  //Comp_DICTIONARY_INCLUDE_046
  test("Comp_DICTIONARY_INCLUDE_046", Include) {

    checkAnswer(s"""select var_samp(contractNumber) as a from  (select * from Comp_DICTIONARY_INCLUDE order by contractNumber) t""",
      s"""select var_samp(contractNumber) as a from  (select * from Comp_DICTIONARY_INCLUDE_hive order by contractNumber) t""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_046")

  }


  //Comp_DICTIONARY_INCLUDE_047
  test("Comp_DICTIONARY_INCLUDE_047", Include) {

    sql(s"""select stddev_pop(contractNumber) as a  from (select contractNumber from Comp_DICTIONARY_INCLUDE order by contractNumber) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_048
  test("Comp_DICTIONARY_INCLUDE_048", Include) {

    sql(s"""select stddev_samp(contractNumber)  as a from (select contractNumber from Comp_DICTIONARY_INCLUDE order by contractNumber) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_049
  test("Comp_DICTIONARY_INCLUDE_049", Include) {

    sql(s"""select covar_pop(contractNumber,contractNumber) as a  from (select contractNumber from Comp_DICTIONARY_INCLUDE order by contractNumber) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_050
  test("Comp_DICTIONARY_INCLUDE_050", Include) {

    sql(s"""select covar_samp(contractNumber,contractNumber) as a  from (select contractNumber from Comp_DICTIONARY_INCLUDE order by contractNumber) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_051
  test("Comp_DICTIONARY_INCLUDE_051", Include) {

    checkAnswer(s"""select corr(contractNumber,contractNumber)  as a from Comp_DICTIONARY_INCLUDE""",
      s"""select corr(contractNumber,contractNumber)  as a from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_051")

  }


  //Comp_DICTIONARY_INCLUDE_052
  test("Comp_DICTIONARY_INCLUDE_052", Include) {

    sql(s"""select percentile_approx(contractNumber,0.2) as a  from (select contractNumber from Comp_DICTIONARY_INCLUDE order by contractNumber) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_053
  test("Comp_DICTIONARY_INCLUDE_053", Include) {

    sql(s"""select percentile_approx(contractNumber,0.2,5) as a  from (select contractNumber from Comp_DICTIONARY_INCLUDE order by contractNumber) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_054
  test("Comp_DICTIONARY_INCLUDE_054", Include) {

    sql(s"""select percentile_approx(contractNumber,array(0.2,0.3,0.99))  as a from (select contractNumber from Comp_DICTIONARY_INCLUDE order by contractNumber) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_055
  test("Comp_DICTIONARY_INCLUDE_055", Include) {

    sql(s"""select percentile_approx(contractNumber,array(0.2,0.3,0.99),5) as a from (select contractNumber from Comp_DICTIONARY_INCLUDE order by contractNumber) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_056
  test("Comp_DICTIONARY_INCLUDE_056", Include) {

    sql(s"""select histogram_numeric(contractNumber,2)  as a from (select contractNumber from Comp_DICTIONARY_INCLUDE order by contractNumber) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_057
  test("Comp_DICTIONARY_INCLUDE_057", Include) {

    checkAnswer(s"""select contractNumber+ 10 as a  from Comp_DICTIONARY_INCLUDE order by a""",
      s"""select contractNumber+ 10 as a  from Comp_DICTIONARY_INCLUDE_hive order by a""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_057")

  }


  //Comp_DICTIONARY_INCLUDE_058
  test("Comp_DICTIONARY_INCLUDE_058", Include) {

    checkAnswer(s"""select min(contractNumber), max(contractNumber+ 10) Total from Comp_DICTIONARY_INCLUDE group by  channelsId order by Total""",
      s"""select min(contractNumber), max(contractNumber+ 10) Total from Comp_DICTIONARY_INCLUDE_hive group by  channelsId order by Total""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_058")

  }


  //Comp_DICTIONARY_INCLUDE_059
  test("Comp_DICTIONARY_INCLUDE_059", Include) {

    sql(s"""select last(contractNumber) a from Comp_DICTIONARY_INCLUDE  order by a""").collect

  }


  //Comp_DICTIONARY_INCLUDE_060
  test("Comp_DICTIONARY_INCLUDE_060", Include) {

    checkAnswer(s"""select FIRST(contractNumber) a from Comp_DICTIONARY_INCLUDE order by a""",
      s"""select FIRST(contractNumber) a from Comp_DICTIONARY_INCLUDE_hive order by a""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_060")

  }


  //Comp_DICTIONARY_INCLUDE_061
  test("Comp_DICTIONARY_INCLUDE_061", Include) {

    checkAnswer(s"""select contractNumber,count(contractNumber) a from Comp_DICTIONARY_INCLUDE group by contractNumber order by contractNumber""",
      s"""select contractNumber,count(contractNumber) a from Comp_DICTIONARY_INCLUDE_hive group by contractNumber order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_061")

  }


  //Comp_DICTIONARY_INCLUDE_062
  test("Comp_DICTIONARY_INCLUDE_062", Include) {

    checkAnswer(s"""select Lower(contractNumber) a  from Comp_DICTIONARY_INCLUDE order by contractNumber""",
      s"""select Lower(contractNumber) a  from Comp_DICTIONARY_INCLUDE_hive order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_062")

  }


  //Comp_DICTIONARY_INCLUDE_063
  test("Comp_DICTIONARY_INCLUDE_063", Include) {

    checkAnswer(s"""select distinct contractNumber from Comp_DICTIONARY_INCLUDE order by contractNumber""",
      s"""select distinct contractNumber from Comp_DICTIONARY_INCLUDE_hive order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_063")

  }


  //Comp_DICTIONARY_INCLUDE_064
  test("Comp_DICTIONARY_INCLUDE_064", Include) {

    checkAnswer(s"""select contractNumber from Comp_DICTIONARY_INCLUDE order by contractNumber limit 101""",
      s"""select contractNumber from Comp_DICTIONARY_INCLUDE_hive order by contractNumber limit 101""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_064")

  }


  //Comp_DICTIONARY_INCLUDE_065
  test("Comp_DICTIONARY_INCLUDE_065", Include) {

    checkAnswer(s"""select contractNumber as a from Comp_DICTIONARY_INCLUDE  order by a asc limit 10""",
      s"""select contractNumber as a from Comp_DICTIONARY_INCLUDE_hive  order by a asc limit 10""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_065")

  }


  //Comp_DICTIONARY_INCLUDE_066
  test("Comp_DICTIONARY_INCLUDE_066", Include) {

    checkAnswer(s"""select contractNumber from Comp_DICTIONARY_INCLUDE where  (contractNumber == 9223372047700) and (imei=='1AA100012')""",
      s"""select contractNumber from Comp_DICTIONARY_INCLUDE_hive where  (contractNumber == 9223372047700) and (imei=='1AA100012')""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_066")

  }


  //Comp_DICTIONARY_INCLUDE_067
  test("Comp_DICTIONARY_INCLUDE_067", Include) {

    checkAnswer(s"""select contractNumber from Comp_DICTIONARY_INCLUDE where contractNumber !=9223372047700 order by contractNumber""",
      s"""select contractNumber from Comp_DICTIONARY_INCLUDE_hive where contractNumber !=9223372047700 order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_067")

  }


  //Comp_DICTIONARY_INCLUDE_068
  test("Comp_DICTIONARY_INCLUDE_068", Include) {

    checkAnswer(s"""select contractNumber  from Comp_DICTIONARY_INCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color') order by contractNumber""",
      s"""select contractNumber  from Comp_DICTIONARY_INCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color') order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_068")

  }


  //Comp_DICTIONARY_INCLUDE_069
  test("Comp_DICTIONARY_INCLUDE_069", Include) {

    checkAnswer(s"""select contractNumber from Comp_DICTIONARY_INCLUDE where contractNumber !=9223372047700 order by contractNumber""",
      s"""select contractNumber from Comp_DICTIONARY_INCLUDE_hive where contractNumber !=9223372047700 order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_069")

  }


  //Comp_DICTIONARY_INCLUDE_070
  test("Comp_DICTIONARY_INCLUDE_070", Include) {

    checkAnswer(s"""select contractNumber from Comp_DICTIONARY_INCLUDE where contractNumber >9223372047700 order by contractNumber""",
      s"""select contractNumber from Comp_DICTIONARY_INCLUDE_hive where contractNumber >9223372047700 order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_070")

  }


  //Comp_DICTIONARY_INCLUDE_071
  test("Comp_DICTIONARY_INCLUDE_071", Include) {

    checkAnswer(s"""select contractNumber  from Comp_DICTIONARY_INCLUDE where contractNumber<>contractNumber""",
      s"""select contractNumber  from Comp_DICTIONARY_INCLUDE_hive where contractNumber<>contractNumber""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_071")

  }


  //Comp_DICTIONARY_INCLUDE_072
  test("Comp_DICTIONARY_INCLUDE_072", Include) {

    checkAnswer(s"""select contractNumber from Comp_DICTIONARY_INCLUDE where contractNumber != Latest_areaId order by contractNumber""",
      s"""select contractNumber from Comp_DICTIONARY_INCLUDE_hive where contractNumber != Latest_areaId order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_072")

  }


  //Comp_DICTIONARY_INCLUDE_073
  test("Comp_DICTIONARY_INCLUDE_073", Include) {

    checkAnswer(s"""select contractNumber, contractNumber from Comp_DICTIONARY_INCLUDE where Latest_areaId<contractNumber order by contractNumber""",
      s"""select contractNumber, contractNumber from Comp_DICTIONARY_INCLUDE_hive where Latest_areaId<contractNumber order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_073")

  }


  //Comp_DICTIONARY_INCLUDE_074
  test("Comp_DICTIONARY_INCLUDE_074", Include) {

    checkAnswer(s"""select contractNumber, contractNumber from Comp_DICTIONARY_INCLUDE where Latest_DAY<=contractNumber order by contractNumber""",
      s"""select contractNumber, contractNumber from Comp_DICTIONARY_INCLUDE_hive where Latest_DAY<=contractNumber order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_074")

  }


  //Comp_DICTIONARY_INCLUDE_075
  test("Comp_DICTIONARY_INCLUDE_075", Include) {

    checkAnswer(s"""select contractNumber from Comp_DICTIONARY_INCLUDE where contractNumber <1000 order by contractNumber""",
      s"""select contractNumber from Comp_DICTIONARY_INCLUDE_hive where contractNumber <1000 order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_075")

  }


  //Comp_DICTIONARY_INCLUDE_076
  test("Comp_DICTIONARY_INCLUDE_076", Include) {

    checkAnswer(s"""select contractNumber from Comp_DICTIONARY_INCLUDE where contractNumber >1000 order by contractNumber""",
      s"""select contractNumber from Comp_DICTIONARY_INCLUDE_hive where contractNumber >1000 order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_076")

  }


  //Comp_DICTIONARY_INCLUDE_077
  test("Comp_DICTIONARY_INCLUDE_077", Include) {

    checkAnswer(s"""select contractNumber  from Comp_DICTIONARY_INCLUDE where contractNumber IS NULL order by contractNumber""",
      s"""select contractNumber  from Comp_DICTIONARY_INCLUDE_hive where contractNumber IS NULL order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_077")

  }


  //Comp_DICTIONARY_INCLUDE_078
  test("Comp_DICTIONARY_INCLUDE_078", Include) {

    checkAnswer(s"""select contractNumber  from Comp_DICTIONARY_INCLUDE where Latest_DAY IS NOT NULL order by contractNumber""",
      s"""select contractNumber  from Comp_DICTIONARY_INCLUDE_hive where Latest_DAY IS NOT NULL order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_078")

  }


  //Comp_DICTIONARY_INCLUDE_079
  test("Comp_DICTIONARY_INCLUDE_079", Include) {

    checkAnswer(s"""Select count(Latest_DAY) from Comp_DICTIONARY_INCLUDE""",
      s"""Select count(Latest_DAY) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_079")

  }


  //Comp_DICTIONARY_INCLUDE_080
  test("Comp_DICTIONARY_INCLUDE_080", Include) {

    checkAnswer(s"""select count(DISTINCT Latest_DAY) as a from Comp_DICTIONARY_INCLUDE""",
      s"""select count(DISTINCT Latest_DAY) as a from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_080")

  }


  //Comp_DICTIONARY_INCLUDE_081
  test("Comp_DICTIONARY_INCLUDE_081", Include) {

    checkAnswer(s"""select sum(Latest_DAY)+10 as a ,Latest_DAY  from Comp_DICTIONARY_INCLUDE group by Latest_DAY order by a""",
      s"""select sum(Latest_DAY)+10 as a ,Latest_DAY  from Comp_DICTIONARY_INCLUDE_hive group by Latest_DAY order by a""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_081")

  }


  //Comp_DICTIONARY_INCLUDE_082
  test("Comp_DICTIONARY_INCLUDE_082", Include) {

    checkAnswer(s"""select max(Latest_DAY),min(Latest_DAY) from Comp_DICTIONARY_INCLUDE""",
      s"""select max(Latest_DAY),min(Latest_DAY) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_082")

  }


  //Comp_DICTIONARY_INCLUDE_083
  test("Comp_DICTIONARY_INCLUDE_083", Include) {

    checkAnswer(s"""select sum(Latest_DAY) a  from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(Latest_DAY) a  from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_083")

  }


  //Comp_DICTIONARY_INCLUDE_084
  test("Comp_DICTIONARY_INCLUDE_084", Include) {

    checkAnswer(s"""select avg(Latest_DAY) a  from Comp_DICTIONARY_INCLUDE""",
      s"""select avg(Latest_DAY) a  from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_084")

  }


  //Comp_DICTIONARY_INCLUDE_085
  test("Comp_DICTIONARY_INCLUDE_085", Include) {

    checkAnswer(s"""select min(Latest_DAY) a  from Comp_DICTIONARY_INCLUDE""",
      s"""select min(Latest_DAY) a  from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_085")

  }


  //Comp_DICTIONARY_INCLUDE_086
  test("Comp_DICTIONARY_INCLUDE_086", Include) {

    sql(s"""select variance(Latest_DAY) as a   from (select Latest_DAY from Comp_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_087
  test("Comp_DICTIONARY_INCLUDE_087", Include) {

    sql(s"""select var_pop(Latest_DAY)  as a from (select Latest_DAY from Comp_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_088
  test("Comp_DICTIONARY_INCLUDE_088", Include) {

    sql(s"""select var_samp(Latest_DAY) as a  from (select Latest_DAY from Comp_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_089
  test("Comp_DICTIONARY_INCLUDE_089", Include) {

    sql(s"""select stddev_pop(Latest_DAY) as a  from (select Latest_DAY from Comp_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_090
  test("Comp_DICTIONARY_INCLUDE_090", Include) {

    sql(s"""select stddev_samp(Latest_DAY)  as a from (select Latest_DAY from Comp_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_091
  test("Comp_DICTIONARY_INCLUDE_091", Include) {

    sql(s"""select covar_pop(Latest_DAY,Latest_DAY) as a  from (select Latest_DAY from Comp_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_092
  test("Comp_DICTIONARY_INCLUDE_092", Include) {

    sql(s"""select covar_samp(Latest_DAY,Latest_DAY) as a  from (select Latest_DAY from Comp_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_093
  test("Comp_DICTIONARY_INCLUDE_093", Include) {

    checkAnswer(s"""select corr(Latest_DAY,Latest_DAY)  as a from Comp_DICTIONARY_INCLUDE""",
      s"""select corr(Latest_DAY,Latest_DAY)  as a from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_093")

  }


  //Comp_DICTIONARY_INCLUDE_094
  test("Comp_DICTIONARY_INCLUDE_094", Include) {

    sql(s"""select percentile_approx(Latest_DAY,0.2) as a  from (select Latest_DAY from Comp_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_095
  test("Comp_DICTIONARY_INCLUDE_095", Include) {

    sql(s"""select percentile_approx(Latest_DAY,0.2,5) as a  from (select Latest_DAY from Comp_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_096
  test("Comp_DICTIONARY_INCLUDE_096", Include) {

    sql(s"""select percentile_approx(Latest_DAY,array(0.2,0.3,0.99))  as a from (select Latest_DAY from Comp_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_097
  test("Comp_DICTIONARY_INCLUDE_097", Include) {

    sql(s"""select percentile_approx(Latest_DAY,array(0.2,0.3,0.99),5) as a from (select Latest_DAY from Comp_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_098
  test("Comp_DICTIONARY_INCLUDE_098", Include) {

    sql(s"""select histogram_numeric(Latest_DAY,2)  as a from (select Latest_DAY from Comp_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_099
  test("Comp_DICTIONARY_INCLUDE_099", Include) {

    checkAnswer(s"""select Latest_DAY, Latest_DAY+ 10 as a  from Comp_DICTIONARY_INCLUDE order by a""",
      s"""select Latest_DAY, Latest_DAY+ 10 as a  from Comp_DICTIONARY_INCLUDE_hive order by a""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_099")

  }


  //Comp_DICTIONARY_INCLUDE_100
  test("Comp_DICTIONARY_INCLUDE_100", Include) {

    checkAnswer(s"""select min(Latest_DAY) d, max(Latest_DAY+ 10) Total from Comp_DICTIONARY_INCLUDE group by  channelsId order by Total,d""",
      s"""select min(Latest_DAY) d, max(Latest_DAY+ 10) Total from Comp_DICTIONARY_INCLUDE_hive group by  channelsId order by Total,d""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_100")

  }


  //Comp_DICTIONARY_INCLUDE_101
  test("Comp_DICTIONARY_INCLUDE_101", Include) {

    sql(s"""select last(Latest_DAY) a from Comp_DICTIONARY_INCLUDE order by a""").collect

  }


  //Comp_DICTIONARY_INCLUDE_102
  test("Comp_DICTIONARY_INCLUDE_102", Include) {

    sql(s"""select FIRST(Latest_DAY) a from (select Latest_DAY from Comp_DICTIONARY_INCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_103
  test("Comp_DICTIONARY_INCLUDE_103", Include) {

    checkAnswer(s"""select Latest_DAY,count(Latest_DAY) a from Comp_DICTIONARY_INCLUDE group by Latest_DAY order by Latest_DAY""",
      s"""select Latest_DAY,count(Latest_DAY) a from Comp_DICTIONARY_INCLUDE_hive group by Latest_DAY order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_103")

  }


  //Comp_DICTIONARY_INCLUDE_104
  test("Comp_DICTIONARY_INCLUDE_104", Include) {

    checkAnswer(s"""select Lower(Latest_DAY) a  from Comp_DICTIONARY_INCLUDE order by a""",
      s"""select Lower(Latest_DAY) a  from Comp_DICTIONARY_INCLUDE_hive order by a""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_104")

  }


  //Comp_DICTIONARY_INCLUDE_105
  test("Comp_DICTIONARY_INCLUDE_105", Include) {

    checkAnswer(s"""select distinct Latest_DAY from Comp_DICTIONARY_INCLUDE order by Latest_DAY""",
      s"""select distinct Latest_DAY from Comp_DICTIONARY_INCLUDE_hive order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_105")

  }


  //Comp_DICTIONARY_INCLUDE_106
  test("Comp_DICTIONARY_INCLUDE_106", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE order by Latest_DAY limit 101""",
      s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE_hive order by Latest_DAY limit 101""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_106")

  }


  //Comp_DICTIONARY_INCLUDE_107
  test("Comp_DICTIONARY_INCLUDE_107", Include) {

    checkAnswer(s"""select Latest_DAY as a from Comp_DICTIONARY_INCLUDE  order by a asc limit 10""",
      s"""select Latest_DAY as a from Comp_DICTIONARY_INCLUDE_hive  order by a asc limit 10""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_107")

  }


  //Comp_DICTIONARY_INCLUDE_108
  test("Comp_DICTIONARY_INCLUDE_108", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE where  (Latest_DAY == 1234567890123450.0000000000)  and (imei=='1AA1')""",
      s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE_hive where  (Latest_DAY == 1234567890123450.0000000000)  and (imei=='1AA1')""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_108")

  }


  //Comp_DICTIONARY_INCLUDE_109
  test("Comp_DICTIONARY_INCLUDE_109", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""",
      s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE_hive where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_109")

  }


  //Comp_DICTIONARY_INCLUDE_110
  test("Comp_DICTIONARY_INCLUDE_110", Include) {

    checkAnswer(s"""select Latest_DAY  from Comp_DICTIONARY_INCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select Latest_DAY  from Comp_DICTIONARY_INCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_110")

  }


  //Comp_DICTIONARY_INCLUDE_111
  test("Comp_DICTIONARY_INCLUDE_111", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""",
      s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE_hive where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_111")

  }


  //Comp_DICTIONARY_INCLUDE_112
  test("Comp_DICTIONARY_INCLUDE_112", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE where Latest_DAY >1234567890123450.0000000000  order by Latest_DAY""",
      s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE_hive where Latest_DAY >1234567890123450.0000000000  order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_112")

  }


  //Comp_DICTIONARY_INCLUDE_113
  test("Comp_DICTIONARY_INCLUDE_113", Include) {

    checkAnswer(s"""select Latest_DAY  from Comp_DICTIONARY_INCLUDE where Latest_DAY<>Latest_DAY""",
      s"""select Latest_DAY  from Comp_DICTIONARY_INCLUDE_hive where Latest_DAY<>Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_113")

  }


  //Comp_DICTIONARY_INCLUDE_114
  test("Comp_DICTIONARY_INCLUDE_114", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE where Latest_DAY != Latest_areaId order by Latest_DAY""",
      s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE_hive where Latest_DAY != Latest_areaId order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_114")

  }


  //Comp_DICTIONARY_INCLUDE_115
  test("Comp_DICTIONARY_INCLUDE_115", Include) {

    checkAnswer(s"""select Latest_DAY, Latest_DAY from Comp_DICTIONARY_INCLUDE where Latest_areaId<Latest_DAY order by Latest_DAY""",
      s"""select Latest_DAY, Latest_DAY from Comp_DICTIONARY_INCLUDE_hive where Latest_areaId<Latest_DAY order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_115")

  }


  //Comp_DICTIONARY_INCLUDE_116
  test("Comp_DICTIONARY_INCLUDE_116", Include) {

    checkAnswer(s"""select Latest_DAY, Latest_DAY from Comp_DICTIONARY_INCLUDE where Latest_DAY<=Latest_DAY  order by Latest_DAY""",
      s"""select Latest_DAY, Latest_DAY from Comp_DICTIONARY_INCLUDE_hive where Latest_DAY<=Latest_DAY  order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_116")

  }


  //Comp_DICTIONARY_INCLUDE_117
  test("Comp_DICTIONARY_INCLUDE_117", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE where Latest_DAY <1000  order by Latest_DAY""",
      s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE_hive where Latest_DAY <1000  order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_117")

  }


  //Comp_DICTIONARY_INCLUDE_118
  test("Comp_DICTIONARY_INCLUDE_118", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE where Latest_DAY >1000  order by Latest_DAY""",
      s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE_hive where Latest_DAY >1000  order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_118")

  }


  //Comp_DICTIONARY_INCLUDE_119
  test("Comp_DICTIONARY_INCLUDE_119", Include) {

    checkAnswer(s"""select Latest_DAY  from Comp_DICTIONARY_INCLUDE where Latest_DAY IS NULL  order by Latest_DAY""",
      s"""select Latest_DAY  from Comp_DICTIONARY_INCLUDE_hive where Latest_DAY IS NULL  order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_119")

  }


  //Comp_DICTIONARY_INCLUDE_120
  test("Comp_DICTIONARY_INCLUDE_120", Include) {

    checkAnswer(s"""select Latest_DAY  from Comp_DICTIONARY_INCLUDE where Latest_DAY IS NOT NULL  order by Latest_DAY""",
      s"""select Latest_DAY  from Comp_DICTIONARY_INCLUDE_hive where Latest_DAY IS NOT NULL  order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_120")

  }


  //Comp_DICTIONARY_INCLUDE_121
  test("Comp_DICTIONARY_INCLUDE_121", Include) {

    checkAnswer(s"""Select count(gamePointId) from Comp_DICTIONARY_INCLUDE""",
      s"""Select count(gamePointId) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_121")

  }


  //Comp_DICTIONARY_INCLUDE_122
  test("Comp_DICTIONARY_INCLUDE_122", Include) {

    checkAnswer(s"""select count(DISTINCT gamePointId) as a from Comp_DICTIONARY_INCLUDE""",
      s"""select count(DISTINCT gamePointId) as a from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_122")

  }


  //Comp_DICTIONARY_INCLUDE_123
  test("Comp_DICTIONARY_INCLUDE_123", Include) {

    sql(s"""select sum(gamePointId)+10 as a ,gamePointId  from Comp_DICTIONARY_INCLUDE group by gamePointId order by a""").collect

  }


  //Comp_DICTIONARY_INCLUDE_124
  test("Comp_DICTIONARY_INCLUDE_124", Include) {

    checkAnswer(s"""select max(gamePointId),min(gamePointId) from Comp_DICTIONARY_INCLUDE""",
      s"""select max(gamePointId),min(gamePointId) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_124")

  }


  //Comp_DICTIONARY_INCLUDE_125
  test("Comp_DICTIONARY_INCLUDE_125", Include) {

    sql(s"""select sum(gamePointId) a  from Comp_DICTIONARY_INCLUDE""").collect

  }


  //Comp_DICTIONARY_INCLUDE_126
  test("Comp_DICTIONARY_INCLUDE_126", Include) {

    sql(s"""select avg(gamePointId) a  from Comp_DICTIONARY_INCLUDE""").collect

  }


  //Comp_DICTIONARY_INCLUDE_127
  test("Comp_DICTIONARY_INCLUDE_127", Include) {

    checkAnswer(s"""select min(gamePointId) a  from Comp_DICTIONARY_INCLUDE""",
      s"""select min(gamePointId) a  from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_127")

  }


  //Comp_DICTIONARY_INCLUDE_128
  test("Comp_DICTIONARY_INCLUDE_128", Include) {

    sql(s"""select variance(gamePointId) as a   from (select gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_129
  test("Comp_DICTIONARY_INCLUDE_129", Include) {

    sql(s"""select var_pop(gamePointId)  as a from (select gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_130
  test("Comp_DICTIONARY_INCLUDE_130", Include) {

    sql(s"""select var_samp(gamePointId) as a  from (select gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_131
  test("Comp_DICTIONARY_INCLUDE_131", Include) {

    sql(s"""select stddev_pop(gamePointId) as a  from (select gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_132
  test("Comp_DICTIONARY_INCLUDE_132", Include) {

    sql(s"""select stddev_samp(gamePointId)  as a from (select gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_133
  test("Comp_DICTIONARY_INCLUDE_133", Include) {

    sql(s"""select covar_pop(gamePointId,gamePointId) as a  from (select gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_134
  test("Comp_DICTIONARY_INCLUDE_134", Include) {

    sql(s"""select covar_samp(gamePointId,gamePointId) as a  from (select gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_135
  test("Comp_DICTIONARY_INCLUDE_135", Include) {

    checkAnswer(s"""select corr(gamePointId,gamePointId)  as a from Comp_DICTIONARY_INCLUDE""",
      s"""select corr(gamePointId,gamePointId)  as a from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_135")

  }


  //Comp_DICTIONARY_INCLUDE_136
  test("Comp_DICTIONARY_INCLUDE_136", Include) {

    sql(s"""select percentile_approx(gamePointId,0.2) as a  from (select gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_137
  test("Comp_DICTIONARY_INCLUDE_137", Include) {

    sql(s"""select percentile_approx(gamePointId,0.2,5) as a  from (select gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_138
  test("Comp_DICTIONARY_INCLUDE_138", Include) {

    sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99))  as a from (select gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_139
  test("Comp_DICTIONARY_INCLUDE_139", Include) {

    sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99),5) as a from (select gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_140
  test("Comp_DICTIONARY_INCLUDE_140", Include) {

    sql(s"""select histogram_numeric(gamePointId,2)  as a from (select gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_141
  test("Comp_DICTIONARY_INCLUDE_141", Include) {

    checkAnswer(s"""select gamePointId, gamePointId+ 10 as a  from Comp_DICTIONARY_INCLUDE order by a""",
      s"""select gamePointId, gamePointId+ 10 as a  from Comp_DICTIONARY_INCLUDE_hive order by a""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_141")

  }


  //Comp_DICTIONARY_INCLUDE_142
  test("Comp_DICTIONARY_INCLUDE_142", Include) {

    checkAnswer(s"""select min(gamePointId), max(gamePointId+ 10) Total from Comp_DICTIONARY_INCLUDE group by  channelsId order by Total""",
      s"""select min(gamePointId), max(gamePointId+ 10) Total from Comp_DICTIONARY_INCLUDE_hive group by  channelsId order by Total""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_142")

  }


  //Comp_DICTIONARY_INCLUDE_143
  test("Comp_DICTIONARY_INCLUDE_143", Include) {

    sql(s"""select last(gamePointId) a from (select gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_144
  test("Comp_DICTIONARY_INCLUDE_144", Include) {

    sql(s"""select FIRST(gamePointId) a from Comp_DICTIONARY_INCLUDE order by a""").collect

  }


  //Comp_DICTIONARY_INCLUDE_145
  test("Comp_DICTIONARY_INCLUDE_145", Include) {

    checkAnswer(s"""select gamePointId,count(gamePointId) a from Comp_DICTIONARY_INCLUDE group by gamePointId order by gamePointId""",
      s"""select gamePointId,count(gamePointId) a from Comp_DICTIONARY_INCLUDE_hive group by gamePointId order by gamePointId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_145")

  }


  //Comp_DICTIONARY_INCLUDE_146
  test("Comp_DICTIONARY_INCLUDE_146", Include) {

    checkAnswer(s"""select Lower(gamePointId) a  from Comp_DICTIONARY_INCLUDE order by gamePointId""",
      s"""select Lower(gamePointId) a  from Comp_DICTIONARY_INCLUDE_hive order by gamePointId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_146")

  }


  //Comp_DICTIONARY_INCLUDE_147
  test("Comp_DICTIONARY_INCLUDE_147", Include) {

    checkAnswer(s"""select distinct gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId""",
      s"""select distinct gamePointId from Comp_DICTIONARY_INCLUDE_hive order by gamePointId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_147")

  }


  //Comp_DICTIONARY_INCLUDE_148
  test("Comp_DICTIONARY_INCLUDE_148", Include) {

    checkAnswer(s"""select gamePointId from Comp_DICTIONARY_INCLUDE  order by gamePointId limit 101""",
      s"""select gamePointId from Comp_DICTIONARY_INCLUDE_hive  order by gamePointId limit 101""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_148")

  }


  //Comp_DICTIONARY_INCLUDE_149
  test("Comp_DICTIONARY_INCLUDE_149", Include) {

    checkAnswer(s"""select gamePointId as a from Comp_DICTIONARY_INCLUDE  order by a asc limit 10""",
      s"""select gamePointId as a from Comp_DICTIONARY_INCLUDE_hive  order by a asc limit 10""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_149")

  }


  //Comp_DICTIONARY_INCLUDE_150
  test("Comp_DICTIONARY_INCLUDE_150", Include) {

    checkAnswer(s"""select gamePointId from Comp_DICTIONARY_INCLUDE where  (gamePointId == 4.70133553923674E43) and (imei=='1AA100084')""",
      s"""select gamePointId from Comp_DICTIONARY_INCLUDE_hive where  (gamePointId == 4.70133553923674E43) and (imei=='1AA100084')""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_150")

  }


  //Comp_DICTIONARY_INCLUDE_151
  test("Comp_DICTIONARY_INCLUDE_151", Include) {

    checkAnswer(s"""select gamePointId from Comp_DICTIONARY_INCLUDE where gamePointId !=4.70133553923674E43  order by gamePointId""",
      s"""select gamePointId from Comp_DICTIONARY_INCLUDE_hive where gamePointId !=4.70133553923674E43  order by gamePointId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_151")

  }


  //Comp_DICTIONARY_INCLUDE_152
  test("Comp_DICTIONARY_INCLUDE_152", Include) {

    checkAnswer(s"""select gamePointId  from Comp_DICTIONARY_INCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select gamePointId  from Comp_DICTIONARY_INCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_152")

  }


  //Comp_DICTIONARY_INCLUDE_153
  test("Comp_DICTIONARY_INCLUDE_153", Include) {

    checkAnswer(s"""select gamePointId from Comp_DICTIONARY_INCLUDE where gamePointId !=4.70133553923674E43""",
      s"""select gamePointId from Comp_DICTIONARY_INCLUDE_hive where gamePointId !=4.70133553923674E43""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_153")

  }


  //Comp_DICTIONARY_INCLUDE_154
  test("Comp_DICTIONARY_INCLUDE_154", Include) {

    checkAnswer(s"""select gamePointId from Comp_DICTIONARY_INCLUDE where gamePointId >4.70133553923674E43""",
      s"""select gamePointId from Comp_DICTIONARY_INCLUDE_hive where gamePointId >4.70133553923674E43""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_154")

  }


  //Comp_DICTIONARY_INCLUDE_155
  test("Comp_DICTIONARY_INCLUDE_155", Include) {

    checkAnswer(s"""select gamePointId  from Comp_DICTIONARY_INCLUDE where gamePointId<>gamePointId""",
      s"""select gamePointId  from Comp_DICTIONARY_INCLUDE_hive where gamePointId<>gamePointId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_155")

  }


  //Comp_DICTIONARY_INCLUDE_156
  test("Comp_DICTIONARY_INCLUDE_156", Include) {

    checkAnswer(s"""select gamePointId from Comp_DICTIONARY_INCLUDE where gamePointId != Latest_areaId  order by gamePointId""",
      s"""select gamePointId from Comp_DICTIONARY_INCLUDE_hive where gamePointId != Latest_areaId  order by gamePointId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_156")

  }


  //Comp_DICTIONARY_INCLUDE_157
  test("Comp_DICTIONARY_INCLUDE_157", Include) {

    checkAnswer(s"""select gamePointId, gamePointId from Comp_DICTIONARY_INCLUDE where Latest_areaId<gamePointId  order by gamePointId""",
      s"""select gamePointId, gamePointId from Comp_DICTIONARY_INCLUDE_hive where Latest_areaId<gamePointId  order by gamePointId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_157")

  }


  //Comp_DICTIONARY_INCLUDE_158
  test("Comp_DICTIONARY_INCLUDE_158", Include) {

    checkAnswer(s"""select gamePointId, gamePointId from Comp_DICTIONARY_INCLUDE where gamePointId<=gamePointId  order by gamePointId""",
      s"""select gamePointId, gamePointId from Comp_DICTIONARY_INCLUDE_hive where gamePointId<=gamePointId  order by gamePointId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_158")

  }


  //Comp_DICTIONARY_INCLUDE_159
  test("Comp_DICTIONARY_INCLUDE_159", Include) {

    checkAnswer(s"""select gamePointId from Comp_DICTIONARY_INCLUDE where gamePointId <1000 order by gamePointId""",
      s"""select gamePointId from Comp_DICTIONARY_INCLUDE_hive where gamePointId <1000 order by gamePointId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_159")

  }


  //Comp_DICTIONARY_INCLUDE_160
  test("Comp_DICTIONARY_INCLUDE_160", Include) {

    checkAnswer(s"""select gamePointId from Comp_DICTIONARY_INCLUDE where gamePointId >1000 order by gamePointId""",
      s"""select gamePointId from Comp_DICTIONARY_INCLUDE_hive where gamePointId >1000 order by gamePointId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_160")

  }


  //Comp_DICTIONARY_INCLUDE_161
  test("Comp_DICTIONARY_INCLUDE_161", Include) {

    checkAnswer(s"""select gamePointId  from Comp_DICTIONARY_INCLUDE where gamePointId IS NULL order by gamePointId""",
      s"""select gamePointId  from Comp_DICTIONARY_INCLUDE_hive where gamePointId IS NULL order by gamePointId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_161")

  }


  //Comp_DICTIONARY_INCLUDE_162
  test("Comp_DICTIONARY_INCLUDE_162", Include) {

    checkAnswer(s"""select gamePointId  from Comp_DICTIONARY_INCLUDE where gamePointId IS NOT NULL order by gamePointId""",
      s"""select gamePointId  from Comp_DICTIONARY_INCLUDE_hive where gamePointId IS NOT NULL order by gamePointId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_162")

  }


  //Comp_DICTIONARY_INCLUDE_163
  test("Comp_DICTIONARY_INCLUDE_163", Include) {

    checkAnswer(s"""Select count(productionDate) from Comp_DICTIONARY_INCLUDE""",
      s"""Select count(productionDate) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_163")

  }


  //Comp_DICTIONARY_INCLUDE_164
  test("Comp_DICTIONARY_INCLUDE_164", Include) {

    checkAnswer(s"""select count(DISTINCT productionDate) as a from Comp_DICTIONARY_INCLUDE""",
      s"""select count(DISTINCT productionDate) as a from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_164")

  }


  //Comp_DICTIONARY_INCLUDE_165
  test("Comp_DICTIONARY_INCLUDE_165", Include) {

    checkAnswer(s"""select sum(productionDate)+10 as a ,productionDate  from Comp_DICTIONARY_INCLUDE group by productionDate order by productionDate""",
      s"""select sum(productionDate)+10 as a ,productionDate  from Comp_DICTIONARY_INCLUDE_hive group by productionDate order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_165")

  }


  //Comp_DICTIONARY_INCLUDE_166
  test("Comp_DICTIONARY_INCLUDE_166", Include) {

    checkAnswer(s"""select max(productionDate),min(productionDate) from Comp_DICTIONARY_INCLUDE""",
      s"""select max(productionDate),min(productionDate) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_166")

  }


  //Comp_DICTIONARY_INCLUDE_167
  test("Comp_DICTIONARY_INCLUDE_167", Include) {

    checkAnswer(s"""select sum(productionDate) a  from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(productionDate) a  from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_167")

  }


  //Comp_DICTIONARY_INCLUDE_168
  test("Comp_DICTIONARY_INCLUDE_168", Include) {

    checkAnswer(s"""select avg(productionDate) a  from Comp_DICTIONARY_INCLUDE""",
      s"""select avg(productionDate) a  from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_168")

  }


  //Comp_DICTIONARY_INCLUDE_169
  test("Comp_DICTIONARY_INCLUDE_169", Include) {

    checkAnswer(s"""select min(productionDate) a  from Comp_DICTIONARY_INCLUDE""",
      s"""select min(productionDate) a  from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_169")

  }


  //Comp_DICTIONARY_INCLUDE_170
  test("Comp_DICTIONARY_INCLUDE_170", Include) {

    sql(s"""select variance(gamePointId) as a from (select gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_171
  test("Comp_DICTIONARY_INCLUDE_171", Include) {

    sql(s"""select var_pop(gamePointId)  as a from Comp_DICTIONARY_INCLUDE""").collect

  }


  //Comp_DICTIONARY_INCLUDE_172
  test("Comp_DICTIONARY_INCLUDE_172", Include) {

    sql(s"""select var_samp(gamePointId) as a  from Comp_DICTIONARY_INCLUDE""").collect

  }


  //Comp_DICTIONARY_INCLUDE_173
  test("Comp_DICTIONARY_INCLUDE_173", Include) {

    sql(s"""select stddev_pop(gamePointId) as a  from (select gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_174
  test("Comp_DICTIONARY_INCLUDE_174", Include) {

    sql(s"""select stddev_samp(gamePointId)  as a from (select gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_175
  test("Comp_DICTIONARY_INCLUDE_175", Include) {

    sql(s"""select covar_pop(gamePointId,gamePointId) as a  from (select gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_176
  test("Comp_DICTIONARY_INCLUDE_176", Include) {

    sql(s"""select covar_samp(gamePointId,gamePointId) as a  from (select gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_177
  test("Comp_DICTIONARY_INCLUDE_177", Include) {

    checkAnswer(s"""select corr(gamePointId,gamePointId)  as a from Comp_DICTIONARY_INCLUDE""",
      s"""select corr(gamePointId,gamePointId)  as a from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_177")

  }


  //Comp_DICTIONARY_INCLUDE_178
  test("Comp_DICTIONARY_INCLUDE_178", Include) {

    sql(s"""select percentile_approx(gamePointId,0.2) as a  from (select gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_179
  test("Comp_DICTIONARY_INCLUDE_179", Include) {

    sql(s"""select percentile_approx(gamePointId,0.2,5) as a  from (select gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_180
  test("Comp_DICTIONARY_INCLUDE_180", Include) {

    sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99))  as a from (select gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_181
  test("Comp_DICTIONARY_INCLUDE_181", Include) {

    sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99),5) as a from (select gamePointId from Comp_DICTIONARY_INCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_182
  test("Comp_DICTIONARY_INCLUDE_182", Include) {

    sql(s"""select histogram_numeric(productionDate,2)  as a from (select productionDate from Comp_DICTIONARY_INCLUDE order by productionDate) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_183
  test("Comp_DICTIONARY_INCLUDE_183", Include) {

    checkAnswer(s"""select last(productionDate) a from Comp_DICTIONARY_INCLUDE order by a""",
      s"""select last(productionDate) a from Comp_DICTIONARY_INCLUDE_hive order by a""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_183")

  }


  //Comp_DICTIONARY_INCLUDE_184
  test("Comp_DICTIONARY_INCLUDE_184", Include) {

    sql(s"""select FIRST(productionDate) a from Comp_DICTIONARY_INCLUDE  order by a""").collect

  }


  //Comp_DICTIONARY_INCLUDE_185
  test("Comp_DICTIONARY_INCLUDE_185", Include) {

    checkAnswer(s"""select productionDate,count(productionDate) a from Comp_DICTIONARY_INCLUDE group by productionDate order by productionDate""",
      s"""select productionDate,count(productionDate) a from Comp_DICTIONARY_INCLUDE_hive group by productionDate order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_185")

  }


  //Comp_DICTIONARY_INCLUDE_186
  test("Comp_DICTIONARY_INCLUDE_186", Include) {

    checkAnswer(s"""select Lower(productionDate) a  from Comp_DICTIONARY_INCLUDE order by productionDate""",
      s"""select Lower(productionDate) a  from Comp_DICTIONARY_INCLUDE_hive order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_186")

  }


  //Comp_DICTIONARY_INCLUDE_187
  test("Comp_DICTIONARY_INCLUDE_187", Include) {

    checkAnswer(s"""select distinct productionDate from Comp_DICTIONARY_INCLUDE order by productionDate""",
      s"""select distinct productionDate from Comp_DICTIONARY_INCLUDE_hive order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_187")

  }


  //Comp_DICTIONARY_INCLUDE_188
  test("Comp_DICTIONARY_INCLUDE_188", Include) {

    checkAnswer(s"""select productionDate from Comp_DICTIONARY_INCLUDE order by productionDate limit 101""",
      s"""select productionDate from Comp_DICTIONARY_INCLUDE_hive order by productionDate limit 101""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_188")

  }


  //Comp_DICTIONARY_INCLUDE_189
  test("Comp_DICTIONARY_INCLUDE_189", Include) {

    checkAnswer(s"""select productionDate as a from Comp_DICTIONARY_INCLUDE  order by a asc limit 10""",
      s"""select productionDate as a from Comp_DICTIONARY_INCLUDE_hive  order by a asc limit 10""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_189")

  }


  //Comp_DICTIONARY_INCLUDE_190
  test("Comp_DICTIONARY_INCLUDE_190", Include) {

    checkAnswer(s"""select productionDate from Comp_DICTIONARY_INCLUDE where  (productionDate == '2015-07-01 12:07:28.0') and (productionDate=='2015-07-01 12:07:28.0')""",
      s"""select productionDate from Comp_DICTIONARY_INCLUDE_hive where  (productionDate == '2015-07-01 12:07:28.0') and (productionDate=='2015-07-01 12:07:28.0')""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_190")

  }


  //Comp_DICTIONARY_INCLUDE_191
  test("Comp_DICTIONARY_INCLUDE_191", Include) {

    checkAnswer(s"""select productionDate from Comp_DICTIONARY_INCLUDE where productionDate !='2015-07-01 12:07:28.0' order by productionDate""",
      s"""select productionDate from Comp_DICTIONARY_INCLUDE_hive where productionDate !='2015-07-01 12:07:28.0' order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_191")

  }


  //Comp_DICTIONARY_INCLUDE_192
  test("Comp_DICTIONARY_INCLUDE_192", Include) {

    checkAnswer(s"""select productionDate  from Comp_DICTIONARY_INCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select productionDate  from Comp_DICTIONARY_INCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_192")

  }


  //Comp_DICTIONARY_INCLUDE_193
  test("Comp_DICTIONARY_INCLUDE_193", Include) {

    checkAnswer(s"""select productionDate from Comp_DICTIONARY_INCLUDE where productionDate !='2015-07-01 12:07:28.0' order by productionDate""",
      s"""select productionDate from Comp_DICTIONARY_INCLUDE_hive where productionDate !='2015-07-01 12:07:28.0' order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_193")

  }


  //Comp_DICTIONARY_INCLUDE_194
  test("Comp_DICTIONARY_INCLUDE_194", Include) {

    checkAnswer(s"""select productionDate from Comp_DICTIONARY_INCLUDE where productionDate >'2015-07-01 12:07:28.0' order by productionDate""",
      s"""select productionDate from Comp_DICTIONARY_INCLUDE_hive where productionDate >'2015-07-01 12:07:28.0' order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_194")

  }


  //Comp_DICTIONARY_INCLUDE_195
  test("Comp_DICTIONARY_INCLUDE_195", Include) {

    checkAnswer(s"""select productionDate  from Comp_DICTIONARY_INCLUDE where productionDate<>productionDate order by productionDate""",
      s"""select productionDate  from Comp_DICTIONARY_INCLUDE_hive where productionDate<>productionDate order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_195")

  }


  //Comp_DICTIONARY_INCLUDE_196
  test("Comp_DICTIONARY_INCLUDE_196", Include) {

    checkAnswer(s"""select productionDate from Comp_DICTIONARY_INCLUDE where productionDate != Latest_areaId order by productionDate""",
      s"""select productionDate from Comp_DICTIONARY_INCLUDE_hive where productionDate != Latest_areaId order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_196")

  }


  //Comp_DICTIONARY_INCLUDE_197
  test("Comp_DICTIONARY_INCLUDE_197", Include) {

    checkAnswer(s"""select productionDate from Comp_DICTIONARY_INCLUDE where Latest_areaId<productionDate order by productionDate""",
      s"""select productionDate from Comp_DICTIONARY_INCLUDE_hive where Latest_areaId<productionDate order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_197")

  }


  //Comp_DICTIONARY_INCLUDE_198
  test("Comp_DICTIONARY_INCLUDE_198", Include) {

    checkAnswer(s"""select productionDate from Comp_DICTIONARY_INCLUDE where productionDate<=productionDate order by productionDate""",
      s"""select productionDate from Comp_DICTIONARY_INCLUDE_hive where productionDate<=productionDate order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_198")

  }


  //Comp_DICTIONARY_INCLUDE_199
  test("Comp_DICTIONARY_INCLUDE_199", Include) {

    checkAnswer(s"""select productionDate from Comp_DICTIONARY_INCLUDE where productionDate <cast('2015-07-01 12:07:28.0' as timestamp) order by productionDate""",
      s"""select productionDate from Comp_DICTIONARY_INCLUDE_hive where productionDate <cast('2015-07-01 12:07:28.0' as timestamp) order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_199")

  }


  //Comp_DICTIONARY_INCLUDE_200
  test("Comp_DICTIONARY_INCLUDE_200", Include) {

    checkAnswer(s"""select productionDate  from Comp_DICTIONARY_INCLUDE where productionDate IS NULL""",
      s"""select productionDate  from Comp_DICTIONARY_INCLUDE_hive where productionDate IS NULL""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_200")

  }


  //Comp_DICTIONARY_INCLUDE_201
  test("Comp_DICTIONARY_INCLUDE_201", Include) {

    checkAnswer(s"""select productionDate  from Comp_DICTIONARY_INCLUDE where productionDate IS NOT NULL order by productionDate""",
      s"""select productionDate  from Comp_DICTIONARY_INCLUDE_hive where productionDate IS NOT NULL order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_201")

  }


  //Comp_DICTIONARY_INCLUDE_202
  test("Comp_DICTIONARY_INCLUDE_202", Include) {

    checkAnswer(s"""Select count(deviceInformationId) from Comp_DICTIONARY_INCLUDE""",
      s"""Select count(deviceInformationId) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_202")

  }


  //Comp_DICTIONARY_INCLUDE_203
  test("Comp_DICTIONARY_INCLUDE_203", Include) {

    checkAnswer(s"""select count(DISTINCT deviceInformationId) as a from Comp_DICTIONARY_INCLUDE""",
      s"""select count(DISTINCT deviceInformationId) as a from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_203")

  }


  //Comp_DICTIONARY_INCLUDE_204
  test("Comp_DICTIONARY_INCLUDE_204", Include) {

    checkAnswer(s"""select sum(deviceInformationId)+10 as a ,deviceInformationId  from Comp_DICTIONARY_INCLUDE group by deviceInformationId order by deviceInformationId""",
      s"""select sum(deviceInformationId)+10 as a ,deviceInformationId  from Comp_DICTIONARY_INCLUDE_hive group by deviceInformationId order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_204")

  }


  //Comp_DICTIONARY_INCLUDE_205
  test("Comp_DICTIONARY_INCLUDE_205", Include) {

    checkAnswer(s"""select max(deviceInformationId),min(deviceInformationId) from Comp_DICTIONARY_INCLUDE""",
      s"""select max(deviceInformationId),min(deviceInformationId) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_205")

  }


  //Comp_DICTIONARY_INCLUDE_206
  test("Comp_DICTIONARY_INCLUDE_206", Include) {

    checkAnswer(s"""select sum(deviceInformationId) a  from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(deviceInformationId) a  from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_206")

  }


  //Comp_DICTIONARY_INCLUDE_207
  test("Comp_DICTIONARY_INCLUDE_207", Include) {

    checkAnswer(s"""select avg(deviceInformationId) a  from Comp_DICTIONARY_INCLUDE""",
      s"""select avg(deviceInformationId) a  from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_207")

  }


  //Comp_DICTIONARY_INCLUDE_208
  test("Comp_DICTIONARY_INCLUDE_208", Include) {

    checkAnswer(s"""select min(deviceInformationId) a  from Comp_DICTIONARY_INCLUDE""",
      s"""select min(deviceInformationId) a  from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_208")

  }


  //Comp_DICTIONARY_INCLUDE_209
  test("Comp_DICTIONARY_INCLUDE_209", Include) {

    sql(s"""select variance(deviceInformationId) as a   from (select deviceInformationId from Comp_DICTIONARY_INCLUDE order by deviceInformationId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_210
  ignore("Comp_DICTIONARY_INCLUDE_210", Include) {

    checkAnswer(s"""select var_pop(deviceInformationId)  as a from Comp_DICTIONARY_INCLUDE""",
      s"""select var_pop(deviceInformationId)  as a from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_210")

  }


  //Comp_DICTIONARY_INCLUDE_211
  ignore("Comp_DICTIONARY_INCLUDE_211", Include) {

    checkAnswer(s"""select var_samp(deviceInformationId) as a  from Comp_DICTIONARY_INCLUDE""",
      s"""select var_samp(deviceInformationId) as a  from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_211")

  }


  //Comp_DICTIONARY_INCLUDE_212
  test("Comp_DICTIONARY_INCLUDE_212", Include) {

    sql(s"""select stddev_pop(deviceInformationId) as a  from (select deviceInformationId from Comp_DICTIONARY_INCLUDE order by deviceInformationId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_213
  test("Comp_DICTIONARY_INCLUDE_213", Include) {

    sql(s"""select stddev_samp(deviceInformationId)  as a from (select deviceInformationId from Comp_DICTIONARY_INCLUDE order by deviceInformationId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_214
  test("Comp_DICTIONARY_INCLUDE_214", Include) {

    sql(s"""select covar_pop(deviceInformationId,deviceInformationId) as a  from (select deviceInformationId from Comp_DICTIONARY_INCLUDE order by deviceInformationId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_215
  test("Comp_DICTIONARY_INCLUDE_215", Include) {

    sql(s"""select covar_samp(deviceInformationId,deviceInformationId) as a  from (select deviceInformationId from Comp_DICTIONARY_INCLUDE order by deviceInformationId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_216
  test("Comp_DICTIONARY_INCLUDE_216", Include) {

    checkAnswer(s"""select corr(deviceInformationId,deviceInformationId)  as a from Comp_DICTIONARY_INCLUDE""",
      s"""select corr(deviceInformationId,deviceInformationId)  as a from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_216")

  }


  //Comp_DICTIONARY_INCLUDE_217
  test("Comp_DICTIONARY_INCLUDE_217", Include) {

    sql(s"""select percentile_approx(deviceInformationId,0.2) as a  from (select deviceInformationId from Comp_DICTIONARY_INCLUDE order by deviceInformationId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_218
  test("Comp_DICTIONARY_INCLUDE_218", Include) {

    sql(s"""select percentile_approx(deviceInformationId,0.2,5) as a  from (select deviceInformationId from Comp_DICTIONARY_INCLUDE order by deviceInformationId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_219
  test("Comp_DICTIONARY_INCLUDE_219", Include) {

    sql(s"""select percentile_approx(deviceInformationId,array(0.2,0.3,0.99))  as a from (select deviceInformationId from Comp_DICTIONARY_INCLUDE order by deviceInformationId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_220
  test("Comp_DICTIONARY_INCLUDE_220", Include) {

    sql(s"""select percentile_approx(deviceInformationId,array(0.2,0.3,0.99),5) as a from (select deviceInformationId from Comp_DICTIONARY_INCLUDE order by deviceInformationId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_221
  test("Comp_DICTIONARY_INCLUDE_221", Include) {

    sql(s"""select histogram_numeric(deviceInformationId,2)  as a from (select deviceInformationId from Comp_DICTIONARY_INCLUDE order by deviceInformationId) t""").collect

  }


  //Comp_DICTIONARY_INCLUDE_222
  test("Comp_DICTIONARY_INCLUDE_222", Include) {

    checkAnswer(s"""select deviceInformationId, deviceInformationId+ 10 as a  from Comp_DICTIONARY_INCLUDE order by deviceInformationId""",
      s"""select deviceInformationId, deviceInformationId+ 10 as a  from Comp_DICTIONARY_INCLUDE_hive order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_222")

  }


  //Comp_DICTIONARY_INCLUDE_223
  test("Comp_DICTIONARY_INCLUDE_223", Include) {

    checkAnswer(s"""select min(deviceInformationId), max(deviceInformationId+ 10) Total from Comp_DICTIONARY_INCLUDE group by  channelsId order by Total""",
      s"""select min(deviceInformationId), max(deviceInformationId+ 10) Total from Comp_DICTIONARY_INCLUDE_hive group by  channelsId order by Total""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_223")

  }


  //Comp_DICTIONARY_INCLUDE_224
  test("Comp_DICTIONARY_INCLUDE_224", Include) {

    sql(s"""select last(deviceInformationId) a from Comp_DICTIONARY_INCLUDE order by a""").collect

  }


  //Comp_DICTIONARY_INCLUDE_225
  test("Comp_DICTIONARY_INCLUDE_225", Include) {

    sql(s"""select FIRST(deviceInformationId) a from Comp_DICTIONARY_INCLUDE order by a""").collect

  }


  //Comp_DICTIONARY_INCLUDE_226
  test("Comp_DICTIONARY_INCLUDE_226", Include) {

    checkAnswer(s"""select deviceInformationId,count(deviceInformationId) a from Comp_DICTIONARY_INCLUDE group by deviceInformationId order by deviceInformationId""",
      s"""select deviceInformationId,count(deviceInformationId) a from Comp_DICTIONARY_INCLUDE_hive group by deviceInformationId order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_226")

  }


  //Comp_DICTIONARY_INCLUDE_227
  test("Comp_DICTIONARY_INCLUDE_227", Include) {

    checkAnswer(s"""select Lower(deviceInformationId) a  from Comp_DICTIONARY_INCLUDE order by deviceInformationId""",
      s"""select Lower(deviceInformationId) a  from Comp_DICTIONARY_INCLUDE_hive order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_227")

  }


  //Comp_DICTIONARY_INCLUDE_228
  test("Comp_DICTIONARY_INCLUDE_228", Include) {

    checkAnswer(s"""select distinct deviceInformationId from Comp_DICTIONARY_INCLUDE order by deviceInformationId""",
      s"""select distinct deviceInformationId from Comp_DICTIONARY_INCLUDE_hive order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_228")

  }


  //Comp_DICTIONARY_INCLUDE_229
  test("Comp_DICTIONARY_INCLUDE_229", Include) {

    checkAnswer(s"""select deviceInformationId from Comp_DICTIONARY_INCLUDE order by deviceInformationId limit 101""",
      s"""select deviceInformationId from Comp_DICTIONARY_INCLUDE_hive order by deviceInformationId limit 101""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_229")

  }


  //Comp_DICTIONARY_INCLUDE_230
  test("Comp_DICTIONARY_INCLUDE_230", Include) {

    checkAnswer(s"""select deviceInformationId as a from Comp_DICTIONARY_INCLUDE  order by a asc limit 10""",
      s"""select deviceInformationId as a from Comp_DICTIONARY_INCLUDE_hive  order by a asc limit 10""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_230")

  }


  //Comp_DICTIONARY_INCLUDE_231
  test("Comp_DICTIONARY_INCLUDE_231", Include) {

    checkAnswer(s"""select deviceInformationId from Comp_DICTIONARY_INCLUDE where  (deviceInformationId == 100084) and (deviceInformationId==100084)""",
      s"""select deviceInformationId from Comp_DICTIONARY_INCLUDE_hive where  (deviceInformationId == 100084) and (deviceInformationId==100084)""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_231")

  }


  //Comp_DICTIONARY_INCLUDE_232
  test("Comp_DICTIONARY_INCLUDE_232", Include) {

    checkAnswer(s"""select deviceInformationId from Comp_DICTIONARY_INCLUDE where deviceInformationId !='100084' order by deviceInformationId""",
      s"""select deviceInformationId from Comp_DICTIONARY_INCLUDE_hive where deviceInformationId !='100084' order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_232")

  }


  //Comp_DICTIONARY_INCLUDE_233
  test("Comp_DICTIONARY_INCLUDE_233", Include) {

    checkAnswer(s"""select deviceInformationId  from Comp_DICTIONARY_INCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select deviceInformationId  from Comp_DICTIONARY_INCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_233")

  }


  //Comp_DICTIONARY_INCLUDE_234
  test("Comp_DICTIONARY_INCLUDE_234", Include) {

    checkAnswer(s"""select deviceInformationId from Comp_DICTIONARY_INCLUDE where deviceInformationId !=100084 order by deviceInformationId""",
      s"""select deviceInformationId from Comp_DICTIONARY_INCLUDE_hive where deviceInformationId !=100084 order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_234")

  }


  //Comp_DICTIONARY_INCLUDE_235
  test("Comp_DICTIONARY_INCLUDE_235", Include) {

    checkAnswer(s"""select deviceInformationId from Comp_DICTIONARY_INCLUDE where deviceInformationId >100084 order by deviceInformationId""",
      s"""select deviceInformationId from Comp_DICTIONARY_INCLUDE_hive where deviceInformationId >100084 order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_235")

  }


  //Comp_DICTIONARY_INCLUDE_236
  test("Comp_DICTIONARY_INCLUDE_236", Include) {

    checkAnswer(s"""select deviceInformationId  from Comp_DICTIONARY_INCLUDE where deviceInformationId<>deviceInformationId order by deviceInformationId""",
      s"""select deviceInformationId  from Comp_DICTIONARY_INCLUDE_hive where deviceInformationId<>deviceInformationId order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_236")

  }


  //Comp_DICTIONARY_INCLUDE_237
  test("Comp_DICTIONARY_INCLUDE_237", Include) {

    checkAnswer(s"""select deviceInformationId from Comp_DICTIONARY_INCLUDE where deviceInformationId != Latest_areaId order by deviceInformationId""",
      s"""select deviceInformationId from Comp_DICTIONARY_INCLUDE_hive where deviceInformationId != Latest_areaId order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_237")

  }


  //Comp_DICTIONARY_INCLUDE_238
  test("Comp_DICTIONARY_INCLUDE_238", Include) {

    checkAnswer(s"""select deviceInformationId, deviceInformationId from Comp_DICTIONARY_INCLUDE where Latest_areaId<deviceInformationId order by deviceInformationId""",
      s"""select deviceInformationId, deviceInformationId from Comp_DICTIONARY_INCLUDE_hive where Latest_areaId<deviceInformationId order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_238")

  }


  //Comp_DICTIONARY_INCLUDE_239
  test("Comp_DICTIONARY_INCLUDE_239", Include) {

    checkAnswer(s"""select deviceInformationId, deviceInformationId from Comp_DICTIONARY_INCLUDE where deviceInformationId<=deviceInformationId order by deviceInformationId""",
      s"""select deviceInformationId, deviceInformationId from Comp_DICTIONARY_INCLUDE_hive where deviceInformationId<=deviceInformationId order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_239")

  }


  //Comp_DICTIONARY_INCLUDE_240
  test("Comp_DICTIONARY_INCLUDE_240", Include) {

    checkAnswer(s"""select deviceInformationId from Comp_DICTIONARY_INCLUDE where deviceInformationId <1000 order by deviceInformationId""",
      s"""select deviceInformationId from Comp_DICTIONARY_INCLUDE_hive where deviceInformationId <1000 order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_240")

  }


  //Comp_DICTIONARY_INCLUDE_241
  test("Comp_DICTIONARY_INCLUDE_241", Include) {

    checkAnswer(s"""select deviceInformationId from Comp_DICTIONARY_INCLUDE where deviceInformationId >1000 order by deviceInformationId""",
      s"""select deviceInformationId from Comp_DICTIONARY_INCLUDE_hive where deviceInformationId >1000 order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_241")

  }


  //Comp_DICTIONARY_INCLUDE_242
  test("Comp_DICTIONARY_INCLUDE_242", Include) {

    checkAnswer(s"""select deviceInformationId  from Comp_DICTIONARY_INCLUDE where deviceInformationId IS NULL order by deviceInformationId""",
      s"""select deviceInformationId  from Comp_DICTIONARY_INCLUDE_hive where deviceInformationId IS NULL order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_242")

  }


  //Comp_DICTIONARY_INCLUDE_243
  test("Comp_DICTIONARY_INCLUDE_243", Include) {

    checkAnswer(s"""select deviceInformationId  from Comp_DICTIONARY_INCLUDE where deviceInformationId IS NOT NULL order by deviceInformationId""",
      s"""select deviceInformationId  from Comp_DICTIONARY_INCLUDE_hive where deviceInformationId IS NOT NULL order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_243")

  }


  //Comp_DICTIONARY_INCLUDE_244
  test("Comp_DICTIONARY_INCLUDE_244", Include) {

    checkAnswer(s"""select sum(imei)+10 as a   from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(imei)+10 as a   from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_244")

  }


  //Comp_DICTIONARY_INCLUDE_245
  test("Comp_DICTIONARY_INCLUDE_245", Include) {

    checkAnswer(s"""select sum(imei)*10 as a   from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(imei)*10 as a   from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_245")

  }


  //Comp_DICTIONARY_INCLUDE_246
  test("Comp_DICTIONARY_INCLUDE_246", Include) {

    checkAnswer(s"""select sum(imei)/10 as a   from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(imei)/10 as a   from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_246")

  }


  //Comp_DICTIONARY_INCLUDE_247
  test("Comp_DICTIONARY_INCLUDE_247", Include) {

    checkAnswer(s"""select sum(imei)-10 as a   from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(imei)-10 as a   from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_247")

  }


  //Comp_DICTIONARY_INCLUDE_248
  test("Comp_DICTIONARY_INCLUDE_248", Include) {

    checkAnswer(s"""select sum(contractNumber)+10 as a   from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(contractNumber)+10 as a   from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_248")

  }


  //Comp_DICTIONARY_INCLUDE_249
  test("Comp_DICTIONARY_INCLUDE_249", Include) {

    checkAnswer(s"""select sum(contractNumber)*10 as a   from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(contractNumber)*10 as a   from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_249")

  }


  //Comp_DICTIONARY_INCLUDE_250
  test("Comp_DICTIONARY_INCLUDE_250", Include) {

    checkAnswer(s"""select sum(contractNumber)/10 as a   from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(contractNumber)/10 as a   from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_250")

  }


  //Comp_DICTIONARY_INCLUDE_251
  test("Comp_DICTIONARY_INCLUDE_251", Include) {

    checkAnswer(s"""select sum(contractNumber)-10 as a   from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(contractNumber)-10 as a   from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_251")

  }


  //Comp_DICTIONARY_INCLUDE_252
  test("Comp_DICTIONARY_INCLUDE_252", Include) {

    checkAnswer(s"""select sum(Latest_DAY)+10 as a   from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(Latest_DAY)+10 as a   from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_252")

  }


  //Comp_DICTIONARY_INCLUDE_253
  test("Comp_DICTIONARY_INCLUDE_253", Include) {

    checkAnswer(s"""select sum(Latest_DAY)*10 as a   from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(Latest_DAY)*10 as a   from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_253")

  }


  //Comp_DICTIONARY_INCLUDE_254
  test("Comp_DICTIONARY_INCLUDE_254", Include) {

    checkAnswer(s"""select sum(Latest_DAY)/10 as a   from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(Latest_DAY)/10 as a   from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_254")

  }


  //Comp_DICTIONARY_INCLUDE_255
  test("Comp_DICTIONARY_INCLUDE_255", Include) {

    checkAnswer(s"""select sum(Latest_DAY)-10 as a   from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(Latest_DAY)-10 as a   from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_255")

  }


  //Comp_DICTIONARY_INCLUDE_256
  test("Comp_DICTIONARY_INCLUDE_256", Include) {

    sql(s"""select sum(gamePointId)+10 as a   from Comp_DICTIONARY_INCLUDE""").collect

  }


  //Comp_DICTIONARY_INCLUDE_257
  test("Comp_DICTIONARY_INCLUDE_257", Include) {

    sql(s"""select sum(gamePointId)*10 as a   from Comp_DICTIONARY_INCLUDE""").collect

  }


  //Comp_DICTIONARY_INCLUDE_258
  test("Comp_DICTIONARY_INCLUDE_258", Include) {

    sql(s"""select sum(gamePointId)/10 as a   from Comp_DICTIONARY_INCLUDE""").collect

  }


  //Comp_DICTIONARY_INCLUDE_259
  test("Comp_DICTIONARY_INCLUDE_259", Include) {

    sql(s"""select sum(gamePointId)-10 as a   from Comp_DICTIONARY_INCLUDE""").collect

  }


  //Comp_DICTIONARY_INCLUDE_260
  test("Comp_DICTIONARY_INCLUDE_260", Include) {

    checkAnswer(s"""select sum(productionDate)+10 as a   from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(productionDate)+10 as a   from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_260")

  }


  //Comp_DICTIONARY_INCLUDE_261
  test("Comp_DICTIONARY_INCLUDE_261", Include) {

    checkAnswer(s"""select sum(productionDate)*10 as a   from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(productionDate)*10 as a   from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_261")

  }


  //Comp_DICTIONARY_INCLUDE_262
  test("Comp_DICTIONARY_INCLUDE_262", Include) {

    checkAnswer(s"""select sum(productionDate)/10 as a   from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(productionDate)/10 as a   from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_262")

  }


  //Comp_DICTIONARY_INCLUDE_263
  test("Comp_DICTIONARY_INCLUDE_263", Include) {

    checkAnswer(s"""select sum(productionDate)-10 as a   from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(productionDate)-10 as a   from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_263")

  }


  //Comp_DICTIONARY_INCLUDE_264
  test("Comp_DICTIONARY_INCLUDE_264", Include) {

    checkAnswer(s"""select sum(deviceInformationId)+10 as a   from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(deviceInformationId)+10 as a   from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_264")

  }


  //Comp_DICTIONARY_INCLUDE_265
  test("Comp_DICTIONARY_INCLUDE_265", Include) {

    checkAnswer(s"""select sum(deviceInformationId)*10 as a   from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(deviceInformationId)*10 as a   from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_265")

  }


  //Comp_DICTIONARY_INCLUDE_266
  test("Comp_DICTIONARY_INCLUDE_266", Include) {

    checkAnswer(s"""select sum(deviceInformationId)/10 as a   from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(deviceInformationId)/10 as a   from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_266")

  }


  //Comp_DICTIONARY_INCLUDE_267
  test("Comp_DICTIONARY_INCLUDE_267", Include) {

    checkAnswer(s"""select sum(deviceInformationId)-10 as a   from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(deviceInformationId)-10 as a   from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_267")

  }


  //Comp_DICTIONARY_INCLUDE_292
  test("Comp_DICTIONARY_INCLUDE_292", Include) {

    checkAnswer(s"""SELECT productiondate from Comp_DICTIONARY_INCLUDE where productiondate LIKE '2015-09-30%'""",
      s"""SELECT productiondate from Comp_DICTIONARY_INCLUDE_hive where productiondate LIKE '2015-09-30%'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_292")

  }


  //Comp_DICTIONARY_INCLUDE_293
  test("Comp_DICTIONARY_INCLUDE_293", Include) {

    checkAnswer(s"""SELECT productiondate from Comp_DICTIONARY_INCLUDE where productiondate LIKE '% %'""",
      s"""SELECT productiondate from Comp_DICTIONARY_INCLUDE_hive where productiondate LIKE '% %'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_293")

  }


  //Comp_DICTIONARY_INCLUDE_294
  test("Comp_DICTIONARY_INCLUDE_294", Include) {

    checkAnswer(s"""SELECT productiondate from Comp_DICTIONARY_INCLUDE where productiondate LIKE '%12:07:28'""",
      s"""SELECT productiondate from Comp_DICTIONARY_INCLUDE_hive where productiondate LIKE '%12:07:28'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_294")

  }


  //Comp_DICTIONARY_INCLUDE_295
  test("Comp_DICTIONARY_INCLUDE_295", Include) {

    checkAnswer(s"""select contractnumber from Comp_DICTIONARY_INCLUDE where contractnumber like '922337204%' """,
      s"""select contractnumber from Comp_DICTIONARY_INCLUDE_hive where contractnumber like '922337204%' """, "QueriesCompactionTestCase_DICTIONARY_INCLUDE_295")

  }


  //Comp_DICTIONARY_INCLUDE_296
  test("Comp_DICTIONARY_INCLUDE_296", Include) {

    checkAnswer(s"""select contractnumber from Comp_DICTIONARY_INCLUDE where contractnumber like '%047800'""",
      s"""select contractnumber from Comp_DICTIONARY_INCLUDE_hive where contractnumber like '%047800'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_296")

  }


  //Comp_DICTIONARY_INCLUDE_297
  test("Comp_DICTIONARY_INCLUDE_297", Include) {

    checkAnswer(s"""select contractnumber from Comp_DICTIONARY_INCLUDE where contractnumber like '%720%'""",
      s"""select contractnumber from Comp_DICTIONARY_INCLUDE_hive where contractnumber like '%720%'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_297")

  }


  //Comp_DICTIONARY_INCLUDE_298
  test("Comp_DICTIONARY_INCLUDE_298", Include) {

    checkAnswer(s"""SELECT Latest_DAY from Comp_DICTIONARY_INCLUDE where Latest_DAY like '12345678%'""",
      s"""SELECT Latest_DAY from Comp_DICTIONARY_INCLUDE_hive where Latest_DAY like '12345678%'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_298")

  }


  //Comp_DICTIONARY_INCLUDE_299
  test("Comp_DICTIONARY_INCLUDE_299", Include) {

    checkAnswer(s"""SELECT Latest_DAY from Comp_DICTIONARY_INCLUDE where Latest_DAY like '%5678%'""",
      s"""SELECT Latest_DAY from Comp_DICTIONARY_INCLUDE_hive where Latest_DAY like '%5678%'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_299")

  }


  //Comp_DICTIONARY_INCLUDE_300
  test("Comp_DICTIONARY_INCLUDE_300", Include) {

    checkAnswer(s"""SELECT Latest_DAY from Comp_DICTIONARY_INCLUDE where Latest_DAY like '1234567%'""",
      s"""SELECT Latest_DAY from Comp_DICTIONARY_INCLUDE_hive where Latest_DAY like '1234567%'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_300")

  }


  //Comp_DICTIONARY_INCLUDE_301
  test("Comp_DICTIONARY_INCLUDE_301", Include) {

    checkAnswer(s"""SELECT gamepointID from Comp_DICTIONARY_INCLUDE where gamepointID like '1.1098347722%'""",
      s"""SELECT gamepointID from Comp_DICTIONARY_INCLUDE_hive where gamepointID like '1.1098347722%'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_301")

  }


  //Comp_DICTIONARY_INCLUDE_302
  test("Comp_DICTIONARY_INCLUDE_302", Include) {

    checkAnswer(s"""SELECT gamepointID from Comp_DICTIONARY_INCLUDE where gamepointID like '%8347722%'""",
      s"""SELECT gamepointID from Comp_DICTIONARY_INCLUDE_hive where gamepointID like '%8347722%'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_302")

  }


  //Comp_DICTIONARY_INCLUDE_303
  test("Comp_DICTIONARY_INCLUDE_303", Include) {

    checkAnswer(s"""SELECT gamepointID from Comp_DICTIONARY_INCLUDE where gamepointID like '%7512E42'""",
      s"""SELECT gamepointID from Comp_DICTIONARY_INCLUDE_hive where gamepointID like '%7512E42'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_303")

  }


  //Comp_DICTIONARY_INCLUDE_304
  test("Comp_DICTIONARY_INCLUDE_304", Include) {

    checkAnswer(s"""SELECT deviceinformationid from Comp_DICTIONARY_INCLUDE where deviceinformationid like '1000%'""",
      s"""SELECT deviceinformationid from Comp_DICTIONARY_INCLUDE_hive where deviceinformationid like '1000%'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_304")

  }


  //Comp_DICTIONARY_INCLUDE_305
  test("Comp_DICTIONARY_INCLUDE_305", Include) {

    checkAnswer(s"""SELECT deviceinformationid from Comp_DICTIONARY_INCLUDE where deviceinformationid like '%00%'""",
      s"""SELECT deviceinformationid from Comp_DICTIONARY_INCLUDE_hive where deviceinformationid like '%00%'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_305")

  }


  //Comp_DICTIONARY_INCLUDE_306
  test("Comp_DICTIONARY_INCLUDE_306", Include) {

    checkAnswer(s"""SELECT deviceinformationid from Comp_DICTIONARY_INCLUDE where deviceinformationid like '%0084'""",
      s"""SELECT deviceinformationid from Comp_DICTIONARY_INCLUDE_hive where deviceinformationid like '%0084'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_306")

  }


  //Comp_DICTIONARY_INCLUDE_307
  test("Comp_DICTIONARY_INCLUDE_307", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_INCLUDE where imei like '1AA10%'""",
      s"""select imei from Comp_DICTIONARY_INCLUDE_hive where imei like '1AA10%'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_307")

  }


  //Comp_DICTIONARY_INCLUDE_308
  test("Comp_DICTIONARY_INCLUDE_308", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_INCLUDE where imei like '%A10%'""",
      s"""select imei from Comp_DICTIONARY_INCLUDE_hive where imei like '%A10%'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_308")

  }


  //Comp_DICTIONARY_INCLUDE_309
  test("Comp_DICTIONARY_INCLUDE_309", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_INCLUDE where imei like '%00084'""",
      s"""select imei from Comp_DICTIONARY_INCLUDE_hive where imei like '%00084'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_309")

  }


  //Comp_DICTIONARY_INCLUDE_310
  test("Comp_DICTIONARY_INCLUDE_310", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_INCLUDE where imei in ('1AA100074','1AA100075','1AA100077')""",
      s"""select imei from Comp_DICTIONARY_INCLUDE_hive where imei in ('1AA100074','1AA100075','1AA100077')""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_310")

  }


  //Comp_DICTIONARY_INCLUDE_311
  test("Comp_DICTIONARY_INCLUDE_311", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_INCLUDE where imei not in ('1AA100074','1AA100075','1AA100077')""",
      s"""select imei from Comp_DICTIONARY_INCLUDE_hive where imei not in ('1AA100074','1AA100075','1AA100077')""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_311")

  }


  //Comp_DICTIONARY_INCLUDE_312
  test("Comp_DICTIONARY_INCLUDE_312", Include) {

    checkAnswer(s"""select deviceinformationid from Comp_DICTIONARY_INCLUDE where deviceinformationid in (100081,100078,10008)""",
      s"""select deviceinformationid from Comp_DICTIONARY_INCLUDE_hive where deviceinformationid in (100081,100078,10008)""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_312")

  }


  //Comp_DICTIONARY_INCLUDE_313
  test("Comp_DICTIONARY_INCLUDE_313", Include) {

    checkAnswer(s"""select deviceinformationid from Comp_DICTIONARY_INCLUDE where deviceinformationid not in (100081,100078,10008)""",
      s"""select deviceinformationid from Comp_DICTIONARY_INCLUDE_hive where deviceinformationid not in (100081,100078,10008)""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_313")

  }


  //Comp_DICTIONARY_INCLUDE_314
  test("Comp_DICTIONARY_INCLUDE_314", Include) {

    checkAnswer(s"""select productiondate from Comp_DICTIONARY_INCLUDE where productiondate in ('2015-10-04 12:07:28','2015-10-07%','2015-10-07 12:07:28')""",
      s"""select productiondate from Comp_DICTIONARY_INCLUDE_hive where productiondate in ('2015-10-04 12:07:28','2015-10-07%','2015-10-07 12:07:28')""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_314")

  }


  //Comp_DICTIONARY_INCLUDE_315
  test("Comp_DICTIONARY_INCLUDE_315", Include) {

    checkAnswer(s"""select productiondate from Comp_DICTIONARY_INCLUDE where productiondate not in (cast('2015-10-04 12:07:28' as timestamp),cast('2015-10-07 12:07:28' as timestamp))""",
      s"""select productiondate from Comp_DICTIONARY_INCLUDE_hive where productiondate not in (cast('2015-10-04 12:07:28' as timestamp),cast('2015-10-07 12:07:28' as timestamp))""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_315")

  }


  //Comp_DICTIONARY_INCLUDE_316
  test("Comp_DICTIONARY_INCLUDE_316", Include) {

    checkAnswer(s"""select gamepointid from Comp_DICTIONARY_INCLUDE where gamepointid in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""",
      s"""select gamepointid from Comp_DICTIONARY_INCLUDE_hive where gamepointid in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_316")

  }


  //Comp_DICTIONARY_INCLUDE_317
  test("Comp_DICTIONARY_INCLUDE_317", Include) {

    checkAnswer(s"""select gamepointid from Comp_DICTIONARY_INCLUDE where gamepointid not in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""",
      s"""select gamepointid from Comp_DICTIONARY_INCLUDE_hive where gamepointid not in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_317")

  }


  //Comp_DICTIONARY_INCLUDE_318
  test("Comp_DICTIONARY_INCLUDE_318", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE where Latest_DAY in (1234567890123530.0000000000,1234567890123520.0000000000)""",
      s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE_hive where Latest_DAY in (1234567890123530.0000000000,1234567890123520.0000000000)""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_318")

  }


  //Comp_DICTIONARY_INCLUDE_319
  test("Comp_DICTIONARY_INCLUDE_319", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE where Latest_DAY not in (1234567890123530.0000000000,1234567890123520.0000000000)""",
      s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE_hive where Latest_DAY not in (1234567890123530.0000000000,1234567890123520.0000000000)""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_319")

  }


  //Comp_DICTIONARY_INCLUDE_322
  test("Comp_DICTIONARY_INCLUDE_322", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_INCLUDE where imei !='1AA100077'""",
      s"""select imei from Comp_DICTIONARY_INCLUDE_hive where imei !='1AA100077'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_322")

  }


  //Comp_DICTIONARY_INCLUDE_323
  test("Comp_DICTIONARY_INCLUDE_323", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_INCLUDE where imei NOT LIKE '1AA100077'""",
      s"""select imei from Comp_DICTIONARY_INCLUDE_hive where imei NOT LIKE '1AA100077'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_323")

  }


  //Comp_DICTIONARY_INCLUDE_324
  test("Comp_DICTIONARY_INCLUDE_324", Include) {

    checkAnswer(s"""select deviceinformationid from Comp_DICTIONARY_INCLUDE where deviceinformationid !=100078""",
      s"""select deviceinformationid from Comp_DICTIONARY_INCLUDE_hive where deviceinformationid !=100078""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_324")

  }


  //Comp_DICTIONARY_INCLUDE_325
  test("Comp_DICTIONARY_INCLUDE_325", Include) {

    checkAnswer(s"""select deviceinformationid from Comp_DICTIONARY_INCLUDE where deviceinformationid NOT LIKE 100079""",
      s"""select deviceinformationid from Comp_DICTIONARY_INCLUDE_hive where deviceinformationid NOT LIKE 100079""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_325")

  }


  //Comp_DICTIONARY_INCLUDE_326
  test("Comp_DICTIONARY_INCLUDE_326", Include) {

    checkAnswer(s"""select productiondate from Comp_DICTIONARY_INCLUDE where productiondate !='2015-10-07 12:07:28'""",
      s"""select productiondate from Comp_DICTIONARY_INCLUDE_hive where productiondate !='2015-10-07 12:07:28'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_326")

  }


  //Comp_DICTIONARY_INCLUDE_327
  ignore("Comp_DICTIONARY_INCLUDE_327", Include) {

    checkAnswer(s"""select productiondate from Comp_DICTIONARY_INCLUDE where productiondate NOT LIKE '2015-10-07 12:07:28'""",
      s"""select productiondate from Comp_DICTIONARY_INCLUDE_hive where productiondate NOT LIKE '2015-10-07 12:07:28'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_327")

  }


  //Comp_DICTIONARY_INCLUDE_328
  test("Comp_DICTIONARY_INCLUDE_328", Include) {

    checkAnswer(s"""select gamepointid from Comp_DICTIONARY_INCLUDE where gamepointid !=6.8591561117512E42""",
      s"""select gamepointid from Comp_DICTIONARY_INCLUDE_hive where gamepointid !=6.8591561117512E42""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_328")

  }


  //Comp_DICTIONARY_INCLUDE_329
  test("Comp_DICTIONARY_INCLUDE_329", Include) {

    checkAnswer(s"""select gamepointid from Comp_DICTIONARY_INCLUDE where gamepointid NOT LIKE 6.8591561117512E43""",
      s"""select gamepointid from Comp_DICTIONARY_INCLUDE_hive where gamepointid NOT LIKE 6.8591561117512E43""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_329")

  }


  //Comp_DICTIONARY_INCLUDE_330
  test("Comp_DICTIONARY_INCLUDE_330", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE where Latest_DAY != 1234567890123520.0000000000""",
      s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE_hive where Latest_DAY != 1234567890123520.0000000000""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_330")

  }


  //Comp_DICTIONARY_INCLUDE_331
  test("Comp_DICTIONARY_INCLUDE_331", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE where Latest_DAY NOT LIKE 1234567890123520.0000000000""",
      s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE_hive where Latest_DAY NOT LIKE 1234567890123520.0000000000""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_331")

  }


  //Comp_DICTIONARY_INCLUDE_335
  test("Comp_DICTIONARY_INCLUDE_335", Include) {

    checkAnswer(s"""SELECT productiondate,IMEI from Comp_DICTIONARY_INCLUDE where IMEI RLIKE '1AA100077'""",
      s"""SELECT productiondate,IMEI from Comp_DICTIONARY_INCLUDE_hive where IMEI RLIKE '1AA100077'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_335")

  }


  //Comp_DICTIONARY_INCLUDE_336
  test("Comp_DICTIONARY_INCLUDE_336", Include) {

    checkAnswer(s"""SELECT deviceinformationId from Comp_DICTIONARY_INCLUDE where deviceinformationId RLIKE '100079'""",
      s"""SELECT deviceinformationId from Comp_DICTIONARY_INCLUDE_hive where deviceinformationId RLIKE '100079'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_336")

  }


  //Comp_DICTIONARY_INCLUDE_337
  test("Comp_DICTIONARY_INCLUDE_337", Include) {

    checkAnswer(s"""SELECT gamepointid from Comp_DICTIONARY_INCLUDE where gamepointid RLIKE '1.61922711065643E42'""",
      s"""SELECT gamepointid from Comp_DICTIONARY_INCLUDE_hive where gamepointid RLIKE '1.61922711065643E42'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_337")

  }


  //Comp_DICTIONARY_INCLUDE_338
  test("Comp_DICTIONARY_INCLUDE_338", Include) {

    checkAnswer(s"""SELECT Latest_Day from Comp_DICTIONARY_INCLUDE where Latest_Day RLIKE '1234567890123550.0000000000'""",
      s"""SELECT Latest_Day from Comp_DICTIONARY_INCLUDE_hive where Latest_Day RLIKE '1234567890123550.0000000000'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_338")

  }


  //Comp_DICTIONARY_INCLUDE_339
  test("Comp_DICTIONARY_INCLUDE_339", Include) {

    checkAnswer(s"""SELECT contractnumber from Comp_DICTIONARY_INCLUDE where contractnumber RLIKE '9223372047800'""",
      s"""SELECT contractnumber from Comp_DICTIONARY_INCLUDE_hive where contractnumber RLIKE '9223372047800'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_339")

  }


  //Comp_DICTIONARY_INCLUDE_340
  test("Comp_DICTIONARY_INCLUDE_340", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_INCLUDE a join Comp_DICTIONARY_INCLUDE b on a.productiondate=b.productiondate""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_INCLUDE_hive a join Comp_DICTIONARY_INCLUDE_hive b on a.productiondate=b.productiondate""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_340")

  }


  //Comp_DICTIONARY_INCLUDE_341
  test("Comp_DICTIONARY_INCLUDE_341", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_INCLUDE a join Comp_DICTIONARY_INCLUDE b on a.deviceinformationid=b.deviceinformationid""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_INCLUDE_hive a join Comp_DICTIONARY_INCLUDE_hive b on a.deviceinformationid=b.deviceinformationid""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_341")

  }


  //Comp_DICTIONARY_INCLUDE_342
  test("Comp_DICTIONARY_INCLUDE_342", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_INCLUDE a join Comp_DICTIONARY_INCLUDE b on a.imei=b.imei""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_INCLUDE_hive a join Comp_DICTIONARY_INCLUDE_hive b on a.imei=b.imei""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_342")

  }


  //Comp_DICTIONARY_INCLUDE_343
  test("Comp_DICTIONARY_INCLUDE_343", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_INCLUDE a join Comp_DICTIONARY_INCLUDE b on a.gamepointid=b.gamepointid""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_INCLUDE_hive a join Comp_DICTIONARY_INCLUDE_hive b on a.gamepointid=b.gamepointid""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_343")

  }


  //Comp_DICTIONARY_INCLUDE_344
  test("Comp_DICTIONARY_INCLUDE_344", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_INCLUDE a join Comp_DICTIONARY_INCLUDE b on a.Latest_Day=b.Latest_Day limit 5""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_INCLUDE_hive a join Comp_DICTIONARY_INCLUDE_hive b on a.Latest_Day=b.Latest_Day limit 5""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_344")

  }


  //Comp_DICTIONARY_INCLUDE_345
  test("Comp_DICTIONARY_INCLUDE_345", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_INCLUDE a join Comp_DICTIONARY_INCLUDE b on a.contractnumber=b.contractnumber limit 5""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_INCLUDE_hive a join Comp_DICTIONARY_INCLUDE_hive b on a.contractnumber=b.contractnumber limit 5""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_345")

  }


  //Comp_DICTIONARY_INCLUDE_346
  test("Comp_DICTIONARY_INCLUDE_346", Include) {

    checkAnswer(s"""select count( contractNumber ),sum( contractNumber ),count(distinct contractNumber ),avg( contractNumber ),max( contractNumber ),min( contractNumber ),1 from Comp_DICTIONARY_INCLUDE limit 5""",
      s"""select count( contractNumber ),sum( contractNumber ),count(distinct contractNumber ),avg( contractNumber ),max( contractNumber ),min( contractNumber ),1 from Comp_DICTIONARY_INCLUDE_hive limit 5""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_346")

  }


  //Comp_DICTIONARY_INCLUDE_347
  test("Comp_DICTIONARY_INCLUDE_347", Include) {

    checkAnswer(s"""select count( Latest_Day ),sum( Latest_Day ),count(distinct Latest_Day ),avg( Latest_Day ),max( Latest_Day ),min( Latest_Day ),1 from Comp_DICTIONARY_INCLUDE""",
      s"""select count( Latest_Day ),sum( Latest_Day ),count(distinct Latest_Day ),avg( Latest_Day ),max( Latest_Day ),min( Latest_Day ),1 from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_347")

  }


  //Comp_DICTIONARY_INCLUDE_348
  test("Comp_DICTIONARY_INCLUDE_348", Include) {

    sql(s"""select count( gamePointId),sum( gamePointId ),count(distinct gamePointId ),avg(gamePointId),max(gamePointId),min(gamePointId),1 from Comp_DICTIONARY_INCLUDE""").collect

  }


  //Comp_DICTIONARY_INCLUDE_349
  test("Comp_DICTIONARY_INCLUDE_349", Include) {

    checkAnswer(s"""select count(deviceInformationId),sum(deviceInformationId),count(deviceInformationId),avg(deviceInformationId),max(deviceInformationId),min(deviceInformationId),1 from Comp_DICTIONARY_INCLUDE""",
      s"""select count(deviceInformationId),sum(deviceInformationId),count(deviceInformationId),avg(deviceInformationId),max(deviceInformationId),min(deviceInformationId),1 from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_349")

  }


  //Comp_DICTIONARY_INCLUDE_350
  test("Comp_DICTIONARY_INCLUDE_350", Include) {

    checkAnswer(s"""select count( productionDate),sum(  productionDate ),count(distinct  productionDate ),avg(  productionDate ),max(  productionDate),min(  productionDate ),1 from Comp_DICTIONARY_INCLUDE""",
      s"""select count( productionDate),sum(  productionDate ),count(distinct  productionDate ),avg(  productionDate ),max(  productionDate),min(  productionDate ),1 from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_350")

  }


  //Comp_DICTIONARY_INCLUDE_351
  test("Comp_DICTIONARY_INCLUDE_351", Include) {

    checkAnswer(s"""select count(IMEI ),sum(IMEI ),count(distinct IMEI ),avg(IMEI ),max(IMEI ),min(IMEI ),1 from Comp_DICTIONARY_INCLUDE""",
      s"""select count(IMEI ),sum(IMEI ),count(distinct IMEI ),avg(IMEI ),max(IMEI ),min(IMEI ),1 from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_351")

  }


  //Comp_DICTIONARY_INCLUDE_352
  test("Comp_DICTIONARY_INCLUDE_352", Include) {

    checkAnswer(s"""select sum(contractNumber),count(contractNumber),avg(contractNumber),sum(contractNumber)/count(contractNumber) from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(contractNumber),count(contractNumber),avg(contractNumber),sum(contractNumber)/count(contractNumber) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_352")

  }


  //Comp_DICTIONARY_INCLUDE_353
  test("Comp_DICTIONARY_INCLUDE_353", Include) {

    checkAnswer(s"""select sum(Latest_Day),count(Latest_Day),avg(Latest_Day),sum(Latest_Day)/count(Latest_Day) from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(Latest_Day),count(Latest_Day),avg(Latest_Day),sum(Latest_Day)/count(Latest_Day) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_353")

  }


  //Comp_DICTIONARY_INCLUDE_354
  test("Comp_DICTIONARY_INCLUDE_354", Include) {

    sql(s"""select sum(gamepointId),count(gamepointId),avg(gamepointID),sum(gamepointID)/count(gamepointID) from Comp_DICTIONARY_INCLUDE""").collect

  }


  //Comp_DICTIONARY_INCLUDE_355
  test("Comp_DICTIONARY_INCLUDE_355", Include) {

    checkAnswer(s"""select sum(deviceinformationId),count(deviceinformationId),avg(deviceinformationId),sum(deviceinformationId)/count(deviceinformationId) from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(deviceinformationId),count(deviceinformationId),avg(deviceinformationId),sum(deviceinformationId)/count(deviceinformationId) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_355")

  }


  //Comp_DICTIONARY_INCLUDE_356
  test("Comp_DICTIONARY_INCLUDE_356", Include) {

    checkAnswer(s"""select sum(IMEI),count(IMEI),avg(IMEI),sum(IMEI)/count(IMEI) from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(IMEI),count(IMEI),avg(IMEI),sum(IMEI)/count(IMEI) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_356")

  }


  //Comp_DICTIONARY_INCLUDE_357
  test("Comp_DICTIONARY_INCLUDE_357", Include) {

    checkAnswer(s"""select sum(productionDate),count(productionDate),avg(productionDate),sum(productionDate)/count(productionDate) from Comp_DICTIONARY_INCLUDE""",
      s"""select sum(productionDate),count(productionDate),avg(productionDate),sum(productionDate)/count(productionDate) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_357")

  }


  //Comp_DICTIONARY_INCLUDE_358
  test("Comp_DICTIONARY_INCLUDE_358", Include) {

    checkAnswer(s"""select contractNumber,Latest_DAY,gamePointId,productionDate,deviceInformationId,IMEI  from Comp_DICTIONARY_INCLUDE""",
      s"""select contractNumber,Latest_DAY,gamePointId,productionDate,deviceInformationId,IMEI  from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_358")

  }


  //Comp_DICTIONARY_INCLUDE_359
  test("Comp_DICTIONARY_INCLUDE_359", Include) {

    checkAnswer(s"""select count(MAC) from Comp_DICTIONARY_INCLUDE""",
      s"""select count(MAC) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_359")

  }


  //Comp_DICTIONARY_INCLUDE_360
  test("Comp_DICTIONARY_INCLUDE_360", Include) {

    checkAnswer(s"""select count(gamePointId) from Comp_DICTIONARY_INCLUDE""",
      s"""select count(gamePointId) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_360")

  }


  //Comp_DICTIONARY_INCLUDE_361
  test("Comp_DICTIONARY_INCLUDE_361", Include) {

    checkAnswer(s"""select count(contractNumber) from Comp_DICTIONARY_INCLUDE""",
      s"""select count(contractNumber) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_361")

  }


  //Comp_DICTIONARY_INCLUDE_362
  test("Comp_DICTIONARY_INCLUDE_362", Include) {

    checkAnswer(s"""select count(Latest_DAY) from Comp_DICTIONARY_INCLUDE""",
      s"""select count(Latest_DAY) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_362")

  }


  //Comp_DICTIONARY_INCLUDE_363
  test("Comp_DICTIONARY_INCLUDE_363", Include) {

    checkAnswer(s"""select count(productionDate) from Comp_DICTIONARY_INCLUDE""",
      s"""select count(productionDate) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_363")

  }


  //Comp_DICTIONARY_INCLUDE_364
  test("Comp_DICTIONARY_INCLUDE_364", Include) {

    checkAnswer(s"""select count(deviceInformationId) from Comp_DICTIONARY_INCLUDE""",
      s"""select count(deviceInformationId) from Comp_DICTIONARY_INCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_364")

  }


  //Comp_DICTIONARY_INCLUDE_365
  test("Comp_DICTIONARY_INCLUDE_365", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_INCLUDE where  contractNumber  != '9223372047700'""",
      s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_INCLUDE_hive where  contractNumber  != '9223372047700'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_365")

  }


  //Comp_DICTIONARY_INCLUDE_366
  test("Comp_DICTIONARY_INCLUDE_366", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_INCLUDE where  Latest_DAY  != '1234567890123480.0000000000' order by imei,deviceInformationId,MAC,deviceColor""",
      s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_INCLUDE_hive where  Latest_DAY  != '1234567890123480.0000000000' order by imei,deviceInformationId,MAC,deviceColor""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_366")

  }


  //Comp_DICTIONARY_INCLUDE_367
  test("Comp_DICTIONARY_INCLUDE_367", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_INCLUDE where  gamePointId  != '2.27852521808948E36' order by imei,deviceInformationId,MAC,deviceColor """,
      s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_INCLUDE_hive where  gamePointId  != '2.27852521808948E36' order by imei,deviceInformationId,MAC,deviceColor """, "QueriesCompactionTestCase_DICTIONARY_INCLUDE_367")

  }


  //Comp_DICTIONARY_INCLUDE_368
  test("Comp_DICTIONARY_INCLUDE_368", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_INCLUDE where  productionDate  != '2015-09-18 12:07:28.0' order by imei,deviceInformationId,MAC,deviceColor""",
      s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_INCLUDE_hive where  productionDate  != '2015-09-18 12:07:28.0' order by imei,deviceInformationId,MAC,deviceColor""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_368")

  }


  //Comp_DICTIONARY_INCLUDE_369
  test("Comp_DICTIONARY_INCLUDE_369", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_INCLUDE where  deviceInformationId  != '100075' order by imei,deviceInformationId,MAC,deviceColor """,
      s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_INCLUDE_hive where  deviceInformationId  != '100075' order by imei,deviceInformationId,MAC,deviceColor """, "QueriesCompactionTestCase_DICTIONARY_INCLUDE_369")

  }


  //Comp_DICTIONARY_INCLUDE_370
  test("Comp_DICTIONARY_INCLUDE_370", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_INCLUDE where  contractNumber  not like '9223372047700' order by  imei,deviceInformationId,MAC,deviceColor """,
      s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_INCLUDE_hive where  contractNumber  not like '9223372047700' order by  imei,deviceInformationId,MAC,deviceColor """, "QueriesCompactionTestCase_DICTIONARY_INCLUDE_370")

  }


  //Comp_DICTIONARY_INCLUDE_371
  test("Comp_DICTIONARY_INCLUDE_371", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_INCLUDE where  Latest_DAY  not like '1234567890123480.0000000000' order by imei,deviceInformationId,MAC,deviceColor """,
      s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_INCLUDE_hive where  Latest_DAY  not like '1234567890123480.0000000000' order by imei,deviceInformationId,MAC,deviceColor """, "QueriesCompactionTestCase_DICTIONARY_INCLUDE_371")

  }


  //Comp_DICTIONARY_INCLUDE_372
  test("Comp_DICTIONARY_INCLUDE_372", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_INCLUDE where  gamePointId  not like '2.27852521808948E36' order by imei,deviceInformationId,MAC,deviceColor""",
      s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_INCLUDE_hive where  gamePointId  not like '2.27852521808948E36' order by imei,deviceInformationId,MAC,deviceColor""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_372")

  }


  //Comp_DICTIONARY_INCLUDE_373
  ignore("Comp_DICTIONARY_INCLUDE_373", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_INCLUDE where  productionDate  not like cast('2015-09-18 12:07:28.0' as timestamp) order by imei,deviceInformationId,MAC,deviceColor""",
      s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_INCLUDE_hive where  productionDate  not like cast('2015-09-18 12:07:28.0' as timestamp) order by imei,deviceInformationId,MAC,deviceColor""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_373")

  }


  //Comp_DICTIONARY_INCLUDE_374
  test("Comp_DICTIONARY_INCLUDE_374", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_INCLUDE where  deviceInformationId  not like '100075' order by imei,deviceInformationId,MAC,deviceColor """,
      s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_INCLUDE_hive where  deviceInformationId  not like '100075' order by imei,deviceInformationId,MAC,deviceColor """, "QueriesCompactionTestCase_DICTIONARY_INCLUDE_374")

  }


  //Comp_DICTIONARY_INCLUDE_375
  test("Comp_DICTIONARY_INCLUDE_375", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_INCLUDE where imei is not null""",
      s"""select imei from Comp_DICTIONARY_INCLUDE_hive where imei is not null""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_375")

  }


  //Comp_DICTIONARY_INCLUDE_376
  test("Comp_DICTIONARY_INCLUDE_376", Include) {

    checkAnswer(s"""select gamePointId from Comp_DICTIONARY_INCLUDE where gamePointId is not null""",
      s"""select gamePointId from Comp_DICTIONARY_INCLUDE_hive where gamePointId is not null""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_376")

  }


  //Comp_DICTIONARY_INCLUDE_377
  test("Comp_DICTIONARY_INCLUDE_377", Include) {

    checkAnswer(s"""select contractNumber from Comp_DICTIONARY_INCLUDE where contractNumber is not null""",
      s"""select contractNumber from Comp_DICTIONARY_INCLUDE_hive where contractNumber is not null""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_377")

  }


  //Comp_DICTIONARY_INCLUDE_378
  test("Comp_DICTIONARY_INCLUDE_378", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE where Latest_DAY is not null""",
      s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE_hive where Latest_DAY is not null""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_378")

  }


  //Comp_DICTIONARY_INCLUDE_379
  test("Comp_DICTIONARY_INCLUDE_379", Include) {

    checkAnswer(s"""select productionDate from Comp_DICTIONARY_INCLUDE where productionDate is not null""",
      s"""select productionDate from Comp_DICTIONARY_INCLUDE_hive where productionDate is not null""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_379")

  }


  //Comp_DICTIONARY_INCLUDE_380
  test("Comp_DICTIONARY_INCLUDE_380", Include) {

    checkAnswer(s"""select deviceInformationId from Comp_DICTIONARY_INCLUDE where deviceInformationId is not null""",
      s"""select deviceInformationId from Comp_DICTIONARY_INCLUDE_hive where deviceInformationId is not null""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_380")

  }


  //Comp_DICTIONARY_INCLUDE_381
  test("Comp_DICTIONARY_INCLUDE_381", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_INCLUDE where imei is  null""",
      s"""select imei from Comp_DICTIONARY_INCLUDE_hive where imei is  null""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_381")

  }


  //Comp_DICTIONARY_INCLUDE_382
  test("Comp_DICTIONARY_INCLUDE_382", Include) {

    checkAnswer(s"""select gamePointId from Comp_DICTIONARY_INCLUDE where gamePointId is  null""",
      s"""select gamePointId from Comp_DICTIONARY_INCLUDE_hive where gamePointId is  null""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_382")

  }


  //Comp_DICTIONARY_INCLUDE_383
  test("Comp_DICTIONARY_INCLUDE_383", Include) {

    checkAnswer(s"""select contractNumber from Comp_DICTIONARY_INCLUDE where contractNumber is  null""",
      s"""select contractNumber from Comp_DICTIONARY_INCLUDE_hive where contractNumber is  null""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_383")

  }


  //Comp_DICTIONARY_INCLUDE_384
  test("Comp_DICTIONARY_INCLUDE_384", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE where Latest_DAY is  null""",
      s"""select Latest_DAY from Comp_DICTIONARY_INCLUDE_hive where Latest_DAY is  null""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_384")

  }


  //Comp_DICTIONARY_INCLUDE_385
  test("Comp_DICTIONARY_INCLUDE_385", Include) {

    checkAnswer(s"""select productionDate from Comp_DICTIONARY_INCLUDE where productionDate is  null""",
      s"""select productionDate from Comp_DICTIONARY_INCLUDE_hive where productionDate is  null""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_385")

  }


  //Comp_DICTIONARY_INCLUDE_386
  test("Comp_DICTIONARY_INCLUDE_386", Include) {

    checkAnswer(s"""select deviceInformationId from Comp_DICTIONARY_INCLUDE where deviceInformationId is  null""",
      s"""select deviceInformationId from Comp_DICTIONARY_INCLUDE_hive where deviceInformationId is  null""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_386")

  }


  //Comp_DICTIONARY_INCLUDE_387
  test("Comp_DICTIONARY_INCLUDE_387", Include) {

    checkAnswer(s"""select count(*) from Comp_DICTIONARY_INCLUDE where imei = '1AA1'""",
      s"""select count(*) from Comp_DICTIONARY_INCLUDE_hive where imei = '1AA1'""", "QueriesCompactionTestCase_DICTIONARY_INCLUDE_387")

  }


  //Comp_DICTIONARY_EXCLUDE_001
  test("Comp_DICTIONARY_EXCLUDE_001", Include) {

    checkAnswer(s"""Select count(imei) from Comp_DICTIONARY_EXCLUDE""",
      s"""Select count(imei) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_001")

  }


  //Comp_DICTIONARY_EXCLUDE_002
  test("Comp_DICTIONARY_EXCLUDE_002", Include) {

    checkAnswer(s"""select count(DISTINCT imei) as a from Comp_DICTIONARY_EXCLUDE""",
      s"""select count(DISTINCT imei) as a from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_002")

  }


  //Comp_DICTIONARY_EXCLUDE_003
  test("Comp_DICTIONARY_EXCLUDE_003", Include) {

    checkAnswer(s"""select sum(Latest_month)+10 as a ,imei  from Comp_DICTIONARY_EXCLUDE group by imei order by imei""",
      s"""select sum(Latest_month)+10 as a ,imei  from Comp_DICTIONARY_EXCLUDE_hive group by imei order by imei""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_003")

  }


  //Comp_DICTIONARY_EXCLUDE_004
  test("Comp_DICTIONARY_EXCLUDE_004", Include) {

    checkAnswer(s"""select max(imei),min(imei) from Comp_DICTIONARY_EXCLUDE""",
      s"""select max(imei),min(imei) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_004")

  }


  //Comp_DICTIONARY_EXCLUDE_005
  test("Comp_DICTIONARY_EXCLUDE_005", Include) {

    checkAnswer(s"""select min(imei), max(imei) Total from Comp_DICTIONARY_EXCLUDE group by  channelsId order by Total""",
      s"""select min(imei), max(imei) Total from Comp_DICTIONARY_EXCLUDE_hive group by  channelsId order by Total""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_005")

  }


  //Comp_DICTIONARY_EXCLUDE_006
  test("Comp_DICTIONARY_EXCLUDE_006", Include) {

    checkAnswer(s"""select last(imei) a from Comp_DICTIONARY_EXCLUDE  group by imei order by imei limit 1""",
      s"""select last(imei) a from Comp_DICTIONARY_EXCLUDE_hive  group by imei order by imei limit 1""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_006")

  }


  //Comp_DICTIONARY_EXCLUDE_007
  test("Comp_DICTIONARY_EXCLUDE_007", Include) {

    sql(s"""select FIRST(imei) a from Comp_DICTIONARY_EXCLUDE group by imei order by imei limit 1""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_008
  test("Comp_DICTIONARY_EXCLUDE_008", Include) {

    checkAnswer(s"""select imei,count(imei) a from Comp_DICTIONARY_EXCLUDE group by imei order by imei""",
      s"""select imei,count(imei) a from Comp_DICTIONARY_EXCLUDE_hive group by imei order by imei""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_008")

  }


  //Comp_DICTIONARY_EXCLUDE_009
  test("Comp_DICTIONARY_EXCLUDE_009", Include) {

    checkAnswer(s"""select Lower(imei) a  from Comp_DICTIONARY_EXCLUDE order by imei""",
      s"""select Lower(imei) a  from Comp_DICTIONARY_EXCLUDE_hive order by imei""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_009")

  }


  //Comp_DICTIONARY_EXCLUDE_010
  test("Comp_DICTIONARY_EXCLUDE_010", Include) {

    checkAnswer(s"""select distinct imei from Comp_DICTIONARY_EXCLUDE order by imei""",
      s"""select distinct imei from Comp_DICTIONARY_EXCLUDE_hive order by imei""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_010")

  }


  //Comp_DICTIONARY_EXCLUDE_011
  test("Comp_DICTIONARY_EXCLUDE_011", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_EXCLUDE order by imei limit 101 """,
      s"""select imei from Comp_DICTIONARY_EXCLUDE_hive order by imei limit 101 """, "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_011")

  }


  //Comp_DICTIONARY_EXCLUDE_012
  test("Comp_DICTIONARY_EXCLUDE_012", Include) {

    checkAnswer(s"""select imei as a from Comp_DICTIONARY_EXCLUDE  order by a asc limit 10""",
      s"""select imei as a from Comp_DICTIONARY_EXCLUDE_hive  order by a asc limit 10""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_012")

  }


  //Comp_DICTIONARY_EXCLUDE_013
  test("Comp_DICTIONARY_EXCLUDE_013", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_EXCLUDE where  (contractNumber == 9223372047700) and (imei=='1AA100004')""",
      s"""select imei from Comp_DICTIONARY_EXCLUDE_hive where  (contractNumber == 9223372047700) and (imei=='1AA100004')""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_013")

  }


  //Comp_DICTIONARY_EXCLUDE_014
  test("Comp_DICTIONARY_EXCLUDE_014", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_EXCLUDE where imei !='1AA100064' order by imei""",
      s"""select imei from Comp_DICTIONARY_EXCLUDE_hive where imei !='1AA100064' order by imei""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_014")

  }


  //Comp_DICTIONARY_EXCLUDE_015
  test("Comp_DICTIONARY_EXCLUDE_015", Include) {

    checkAnswer(s"""select imei  from Comp_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select imei  from Comp_DICTIONARY_EXCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_015")

  }


  //Comp_DICTIONARY_EXCLUDE_016
  test("Comp_DICTIONARY_EXCLUDE_016", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_EXCLUDE where imei !='1AA100012' order by imei""",
      s"""select imei from Comp_DICTIONARY_EXCLUDE_hive where imei !='1AA100012' order by imei""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_016")

  }


  //Comp_DICTIONARY_EXCLUDE_017
  test("Comp_DICTIONARY_EXCLUDE_017", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_EXCLUDE where imei >'1AA100012' order by imei""",
      s"""select imei from Comp_DICTIONARY_EXCLUDE_hive where imei >'1AA100012' order by imei""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_017")

  }


  //Comp_DICTIONARY_EXCLUDE_018
  test("Comp_DICTIONARY_EXCLUDE_018", Include) {

    checkAnswer(s"""select imei  from Comp_DICTIONARY_EXCLUDE where imei<>imei""",
      s"""select imei  from Comp_DICTIONARY_EXCLUDE_hive where imei<>imei""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_018")

  }


  //Comp_DICTIONARY_EXCLUDE_019
  test("Comp_DICTIONARY_EXCLUDE_019", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_EXCLUDE where imei != Latest_areaId order by imei""",
      s"""select imei from Comp_DICTIONARY_EXCLUDE_hive where imei != Latest_areaId order by imei""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_019")

  }


  //Comp_DICTIONARY_EXCLUDE_020
  test("Comp_DICTIONARY_EXCLUDE_020", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_EXCLUDE where Latest_areaId<imei order by imei""",
      s"""select imei from Comp_DICTIONARY_EXCLUDE_hive where Latest_areaId<imei order by imei""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_020")

  }


  //Comp_DICTIONARY_EXCLUDE_021
  test("Comp_DICTIONARY_EXCLUDE_021", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_EXCLUDE where Latest_DAY<=imei order by imei""",
      s"""select imei from Comp_DICTIONARY_EXCLUDE_hive where Latest_DAY<=imei order by imei""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_021")

  }


  //Comp_DICTIONARY_EXCLUDE_022
  test("Comp_DICTIONARY_EXCLUDE_022", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_EXCLUDE where imei <'1AA10002' order by imei""",
      s"""select imei from Comp_DICTIONARY_EXCLUDE_hive where imei <'1AA10002' order by imei""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_022")

  }


  //Comp_DICTIONARY_EXCLUDE_023
  test("Comp_DICTIONARY_EXCLUDE_023", Include) {

    checkAnswer(s"""select Latest_day  from Comp_DICTIONARY_EXCLUDE where imei IS NULL""",
      s"""select Latest_day  from Comp_DICTIONARY_EXCLUDE_hive where imei IS NULL""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_023")

  }


  //Comp_DICTIONARY_EXCLUDE_024
  test("Comp_DICTIONARY_EXCLUDE_024", Include) {

    checkAnswer(s"""select Latest_day  from Comp_DICTIONARY_EXCLUDE where imei IS NOT NULL order by Latest_day""",
      s"""select Latest_day  from Comp_DICTIONARY_EXCLUDE_hive where imei IS NOT NULL order by Latest_day""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_024")

  }


  //Comp_DICTIONARY_EXCLUDE_025
  test("Comp_DICTIONARY_EXCLUDE_025", Include) {

    checkAnswer(s"""Select count(imei),min(imei) from Comp_DICTIONARY_EXCLUDE """,
      s"""Select count(imei),min(imei) from Comp_DICTIONARY_EXCLUDE_hive """, "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_025")

  }


  //Comp_DICTIONARY_EXCLUDE_026
  test("Comp_DICTIONARY_EXCLUDE_026", Include) {

    checkAnswer(s"""select count(DISTINCT imei,latest_day) as a from Comp_DICTIONARY_EXCLUDE""",
      s"""select count(DISTINCT imei,latest_day) as a from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_026")

  }


  //Comp_DICTIONARY_EXCLUDE_027
  test("Comp_DICTIONARY_EXCLUDE_027", Include) {

    checkAnswer(s"""select max(imei),min(imei),count(imei) from Comp_DICTIONARY_EXCLUDE""",
      s"""select max(imei),min(imei),count(imei) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_027")

  }


  //Comp_DICTIONARY_EXCLUDE_028
  test("Comp_DICTIONARY_EXCLUDE_028", Include) {

    checkAnswer(s"""select sum(imei),avg(imei),count(imei) a  from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(imei),avg(imei),count(imei) a  from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_028")

  }


  //Comp_DICTIONARY_EXCLUDE_029
  test("Comp_DICTIONARY_EXCLUDE_029", Include) {

    sql(s"""select last(imei),Min(imei),max(imei)  a from Comp_DICTIONARY_EXCLUDE  order by a""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_030
  test("Comp_DICTIONARY_EXCLUDE_030", Include) {

    sql(s"""select FIRST(imei),Last(imei) a from Comp_DICTIONARY_EXCLUDE group by imei order by imei limit 1""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_031
  test("Comp_DICTIONARY_EXCLUDE_031", Include) {

    checkAnswer(s"""select imei,count(imei) a from Comp_DICTIONARY_EXCLUDE group by imei order by imei""",
      s"""select imei,count(imei) a from Comp_DICTIONARY_EXCLUDE_hive group by imei order by imei""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_031")

  }


  //Comp_DICTIONARY_EXCLUDE_032
  test("Comp_DICTIONARY_EXCLUDE_032", Include) {

    checkAnswer(s"""select Lower(imei),upper(imei)  a  from Comp_DICTIONARY_EXCLUDE order by imei""",
      s"""select Lower(imei),upper(imei)  a  from Comp_DICTIONARY_EXCLUDE_hive order by imei""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_032")

  }


  //Comp_DICTIONARY_EXCLUDE_033
  test("Comp_DICTIONARY_EXCLUDE_033", Include) {

    checkAnswer(s"""select imei as a from Comp_DICTIONARY_EXCLUDE  order by a asc limit 10""",
      s"""select imei as a from Comp_DICTIONARY_EXCLUDE_hive  order by a asc limit 10""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_033")

  }


  //Comp_DICTIONARY_EXCLUDE_034
  test("Comp_DICTIONARY_EXCLUDE_034", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_EXCLUDE where  (contractNumber == 9223372047700) and (imei=='1AA100012')""",
      s"""select imei from Comp_DICTIONARY_EXCLUDE_hive where  (contractNumber == 9223372047700) and (imei=='1AA100012')""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_034")

  }


  //Comp_DICTIONARY_EXCLUDE_035
  test("Comp_DICTIONARY_EXCLUDE_035", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_EXCLUDE where imei !='8imei' order by imei""",
      s"""select imei from Comp_DICTIONARY_EXCLUDE_hive where imei !='8imei' order by imei""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_035")

  }


  //Comp_DICTIONARY_EXCLUDE_036
  test("Comp_DICTIONARY_EXCLUDE_036", Include) {

    checkAnswer(s"""select imei  from Comp_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select imei  from Comp_DICTIONARY_EXCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_036")

  }


  //Comp_DICTIONARY_EXCLUDE_037
  test("Comp_DICTIONARY_EXCLUDE_037", Include) {

    checkAnswer(s"""Select count(contractNumber) from Comp_DICTIONARY_EXCLUDE""",
      s"""Select count(contractNumber) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_037")

  }


  //Comp_DICTIONARY_EXCLUDE_038
  test("Comp_DICTIONARY_EXCLUDE_038", Include) {

    checkAnswer(s"""select count(DISTINCT contractNumber) as a from Comp_DICTIONARY_EXCLUDE""",
      s"""select count(DISTINCT contractNumber) as a from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_038")

  }


  //Comp_DICTIONARY_EXCLUDE_039
  test("Comp_DICTIONARY_EXCLUDE_039", Include) {

    checkAnswer(s"""select sum(contractNumber)+10 as a ,contractNumber  from Comp_DICTIONARY_EXCLUDE group by contractNumber""",
      s"""select sum(contractNumber)+10 as a ,contractNumber  from Comp_DICTIONARY_EXCLUDE_hive group by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_039")

  }


  //Comp_DICTIONARY_EXCLUDE_040
  test("Comp_DICTIONARY_EXCLUDE_040", Include) {

    checkAnswer(s"""select max(contractNumber),min(contractNumber) from Comp_DICTIONARY_EXCLUDE""",
      s"""select max(contractNumber),min(contractNumber) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_040")

  }


  //Comp_DICTIONARY_EXCLUDE_041
  test("Comp_DICTIONARY_EXCLUDE_041", Include) {

    checkAnswer(s"""select sum(contractNumber) a  from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(contractNumber) a  from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_041")

  }


  //Comp_DICTIONARY_EXCLUDE_042
  test("Comp_DICTIONARY_EXCLUDE_042", Include) {

    checkAnswer(s"""select avg(contractNumber) a  from Comp_DICTIONARY_EXCLUDE""",
      s"""select avg(contractNumber) a  from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_042")

  }


  //Comp_DICTIONARY_EXCLUDE_043
  test("Comp_DICTIONARY_EXCLUDE_043", Include) {

    checkAnswer(s"""select min(contractNumber) a  from Comp_DICTIONARY_EXCLUDE""",
      s"""select min(contractNumber) a  from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_043")

  }


  //Comp_DICTIONARY_EXCLUDE_044
  test("Comp_DICTIONARY_EXCLUDE_044", Include) {

    sql(s"""select variance(contractNumber) as a   from (select contractNumber from Comp_DICTIONARY_EXCLUDE order by contractNumber) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_045
  test("Comp_DICTIONARY_EXCLUDE_045", Include) {

    checkAnswer(s"""select var_pop(contractNumber) as a from (select * from Comp_DICTIONARY_EXCLUDE order by contractNumber) t""",
      s"""select var_pop(contractNumber) as a from (select * from Comp_DICTIONARY_EXCLUDE_hive order by contractNumber) t""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_045")

  }


  //Comp_DICTIONARY_EXCLUDE_046
  test("Comp_DICTIONARY_EXCLUDE_046", Include) {

    checkAnswer(s"""select var_samp(contractNumber) as a from (select * from Comp_DICTIONARY_EXCLUDE order by contractNumber) t""",
      s"""select var_samp(contractNumber) as a from (select * from Comp_DICTIONARY_EXCLUDE_hive order by contractNumber) t""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_046")

  }


  //Comp_DICTIONARY_EXCLUDE_047
  test("Comp_DICTIONARY_EXCLUDE_047", Include) {

    sql(s"""select stddev_pop(contractNumber) as a  from (select contractNumber from Comp_DICTIONARY_EXCLUDE order by contractNumber) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_048
  test("Comp_DICTIONARY_EXCLUDE_048", Include) {

    sql(s"""select stddev_samp(contractNumber)  as a from (select contractNumber from Comp_DICTIONARY_EXCLUDE order by contractNumber) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_049
  test("Comp_DICTIONARY_EXCLUDE_049", Include) {

    sql(s"""select covar_pop(contractNumber,contractNumber) as a  from (select contractNumber from Comp_DICTIONARY_EXCLUDE order by contractNumber) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_050
  test("Comp_DICTIONARY_EXCLUDE_050", Include) {

    sql(s"""select covar_samp(contractNumber,contractNumber) as a  from (select contractNumber from Comp_DICTIONARY_EXCLUDE order by contractNumber) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_051
  test("Comp_DICTIONARY_EXCLUDE_051", Include) {

    checkAnswer(s"""select corr(contractNumber,contractNumber)  as a from Comp_DICTIONARY_EXCLUDE""",
      s"""select corr(contractNumber,contractNumber)  as a from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_051")

  }


  //Comp_DICTIONARY_EXCLUDE_052
  test("Comp_DICTIONARY_EXCLUDE_052", Include) {

    sql(s"""select percentile_approx(contractNumber,0.2) as a  from (select contractNumber from Comp_DICTIONARY_EXCLUDE order by contractNumber) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_053
  test("Comp_DICTIONARY_EXCLUDE_053", Include) {

    sql(s"""select percentile_approx(contractNumber,0.2,5) as a  from (select contractNumber from Comp_DICTIONARY_EXCLUDE order by contractNumber) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_054
  test("Comp_DICTIONARY_EXCLUDE_054", Include) {

    sql(s"""select percentile_approx(contractNumber,array(0.2,0.3,0.99))  as a from (select contractNumber from Comp_DICTIONARY_EXCLUDE order by contractNumber) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_055
  test("Comp_DICTIONARY_EXCLUDE_055", Include) {

    sql(s"""select percentile_approx(contractNumber,array(0.2,0.3,0.99),5) as a from (select contractNumber from Comp_DICTIONARY_EXCLUDE order by contractNumber) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_056
  test("Comp_DICTIONARY_EXCLUDE_056", Include) {

    sql(s"""select histogram_numeric(contractNumber,2)  as a from (select contractNumber from Comp_DICTIONARY_EXCLUDE order by contractNumber) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_057
  test("Comp_DICTIONARY_EXCLUDE_057", Include) {

    checkAnswer(s"""select contractNumber+ 10 as a  from Comp_DICTIONARY_EXCLUDE order by a""",
      s"""select contractNumber+ 10 as a  from Comp_DICTIONARY_EXCLUDE_hive order by a""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_057")

  }


  //Comp_DICTIONARY_EXCLUDE_058
  test("Comp_DICTIONARY_EXCLUDE_058", Include) {

    checkAnswer(s"""select min(contractNumber), max(contractNumber+ 10) Total from Comp_DICTIONARY_EXCLUDE group by  channelsId order by Total""",
      s"""select min(contractNumber), max(contractNumber+ 10) Total from Comp_DICTIONARY_EXCLUDE_hive group by  channelsId order by Total""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_058")

  }


  //Comp_DICTIONARY_EXCLUDE_059
  test("Comp_DICTIONARY_EXCLUDE_059", Include) {

    sql(s"""select last(contractNumber) a from Comp_DICTIONARY_EXCLUDE  order by a""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_060
  test("Comp_DICTIONARY_EXCLUDE_060", Include) {

    checkAnswer(s"""select FIRST(contractNumber) a from Comp_DICTIONARY_EXCLUDE order by a""",
      s"""select FIRST(contractNumber) a from Comp_DICTIONARY_EXCLUDE_hive order by a""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_060")

  }


  //Comp_DICTIONARY_EXCLUDE_061
  test("Comp_DICTIONARY_EXCLUDE_061", Include) {

    checkAnswer(s"""select contractNumber,count(contractNumber) a from Comp_DICTIONARY_EXCLUDE group by contractNumber order by contractNumber""",
      s"""select contractNumber,count(contractNumber) a from Comp_DICTIONARY_EXCLUDE_hive group by contractNumber order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_061")

  }


  //Comp_DICTIONARY_EXCLUDE_062
  test("Comp_DICTIONARY_EXCLUDE_062", Include) {

    checkAnswer(s"""select Lower(contractNumber) a  from Comp_DICTIONARY_EXCLUDE order by contractNumber""",
      s"""select Lower(contractNumber) a  from Comp_DICTIONARY_EXCLUDE_hive order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_062")

  }


  //Comp_DICTIONARY_EXCLUDE_063
  test("Comp_DICTIONARY_EXCLUDE_063", Include) {

    checkAnswer(s"""select distinct contractNumber from Comp_DICTIONARY_EXCLUDE order by contractNumber""",
      s"""select distinct contractNumber from Comp_DICTIONARY_EXCLUDE_hive order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_063")

  }


  //Comp_DICTIONARY_EXCLUDE_064
  test("Comp_DICTIONARY_EXCLUDE_064", Include) {

    checkAnswer(s"""select contractNumber from Comp_DICTIONARY_EXCLUDE order by contractNumber limit 101""",
      s"""select contractNumber from Comp_DICTIONARY_EXCLUDE_hive order by contractNumber limit 101""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_064")

  }


  //Comp_DICTIONARY_EXCLUDE_065
  test("Comp_DICTIONARY_EXCLUDE_065", Include) {

    checkAnswer(s"""select contractNumber as a from Comp_DICTIONARY_EXCLUDE  order by a asc limit 10""",
      s"""select contractNumber as a from Comp_DICTIONARY_EXCLUDE_hive  order by a asc limit 10""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_065")

  }


  //Comp_DICTIONARY_EXCLUDE_066
  test("Comp_DICTIONARY_EXCLUDE_066", Include) {

    checkAnswer(s"""select contractNumber from Comp_DICTIONARY_EXCLUDE where  (contractNumber == 9223372047700) and (imei=='1AA100012')""",
      s"""select contractNumber from Comp_DICTIONARY_EXCLUDE_hive where  (contractNumber == 9223372047700) and (imei=='1AA100012')""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_066")

  }


  //Comp_DICTIONARY_EXCLUDE_067
  test("Comp_DICTIONARY_EXCLUDE_067", Include) {

    checkAnswer(s"""select contractNumber from Comp_DICTIONARY_EXCLUDE where contractNumber !=9223372047700 order by contractNumber""",
      s"""select contractNumber from Comp_DICTIONARY_EXCLUDE_hive where contractNumber !=9223372047700 order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_067")

  }


  //Comp_DICTIONARY_EXCLUDE_068
  test("Comp_DICTIONARY_EXCLUDE_068", Include) {

    checkAnswer(s"""select contractNumber  from Comp_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color') order by contractNumber""",
      s"""select contractNumber  from Comp_DICTIONARY_EXCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color') order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_068")

  }


  //Comp_DICTIONARY_EXCLUDE_069
  test("Comp_DICTIONARY_EXCLUDE_069", Include) {

    checkAnswer(s"""select contractNumber from Comp_DICTIONARY_EXCLUDE where contractNumber !=9223372047700 order by contractNumber""",
      s"""select contractNumber from Comp_DICTIONARY_EXCLUDE_hive where contractNumber !=9223372047700 order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_069")

  }


  //Comp_DICTIONARY_EXCLUDE_070
  test("Comp_DICTIONARY_EXCLUDE_070", Include) {

    checkAnswer(s"""select contractNumber from Comp_DICTIONARY_EXCLUDE where contractNumber >9223372047700 order by contractNumber""",
      s"""select contractNumber from Comp_DICTIONARY_EXCLUDE_hive where contractNumber >9223372047700 order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_070")

  }


  //Comp_DICTIONARY_EXCLUDE_071
  test("Comp_DICTIONARY_EXCLUDE_071", Include) {

    checkAnswer(s"""select contractNumber  from Comp_DICTIONARY_EXCLUDE where contractNumber<>contractNumber""",
      s"""select contractNumber  from Comp_DICTIONARY_EXCLUDE_hive where contractNumber<>contractNumber""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_071")

  }


  //Comp_DICTIONARY_EXCLUDE_072
  test("Comp_DICTIONARY_EXCLUDE_072", Include) {

    checkAnswer(s"""select contractNumber from Comp_DICTIONARY_EXCLUDE where contractNumber != Latest_areaId order by contractNumber""",
      s"""select contractNumber from Comp_DICTIONARY_EXCLUDE_hive where contractNumber != Latest_areaId order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_072")

  }


  //Comp_DICTIONARY_EXCLUDE_073
  test("Comp_DICTIONARY_EXCLUDE_073", Include) {

    checkAnswer(s"""select contractNumber, contractNumber from Comp_DICTIONARY_EXCLUDE where Latest_areaId<contractNumber order by contractNumber""",
      s"""select contractNumber, contractNumber from Comp_DICTIONARY_EXCLUDE_hive where Latest_areaId<contractNumber order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_073")

  }


  //Comp_DICTIONARY_EXCLUDE_074
  test("Comp_DICTIONARY_EXCLUDE_074", Include) {

    checkAnswer(s"""select contractNumber, contractNumber from Comp_DICTIONARY_EXCLUDE where Latest_DAY<=contractNumber order by contractNumber""",
      s"""select contractNumber, contractNumber from Comp_DICTIONARY_EXCLUDE_hive where Latest_DAY<=contractNumber order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_074")

  }


  //Comp_DICTIONARY_EXCLUDE_075
  test("Comp_DICTIONARY_EXCLUDE_075", Include) {

    checkAnswer(s"""select contractNumber from Comp_DICTIONARY_EXCLUDE where contractNumber <1000 order by contractNumber""",
      s"""select contractNumber from Comp_DICTIONARY_EXCLUDE_hive where contractNumber <1000 order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_075")

  }


  //Comp_DICTIONARY_EXCLUDE_076
  test("Comp_DICTIONARY_EXCLUDE_076", Include) {

    checkAnswer(s"""select contractNumber from Comp_DICTIONARY_EXCLUDE where contractNumber >1000 order by contractNumber""",
      s"""select contractNumber from Comp_DICTIONARY_EXCLUDE_hive where contractNumber >1000 order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_076")

  }


  //Comp_DICTIONARY_EXCLUDE_077
  test("Comp_DICTIONARY_EXCLUDE_077", Include) {

    checkAnswer(s"""select contractNumber  from Comp_DICTIONARY_EXCLUDE where contractNumber IS NULL order by contractNumber""",
      s"""select contractNumber  from Comp_DICTIONARY_EXCLUDE_hive where contractNumber IS NULL order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_077")

  }


  //Comp_DICTIONARY_EXCLUDE_078
  test("Comp_DICTIONARY_EXCLUDE_078", Include) {

    checkAnswer(s"""select contractNumber  from Comp_DICTIONARY_EXCLUDE where Latest_DAY IS NOT NULL order by contractNumber""",
      s"""select contractNumber  from Comp_DICTIONARY_EXCLUDE_hive where Latest_DAY IS NOT NULL order by contractNumber""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_078")

  }


  //Comp_DICTIONARY_EXCLUDE_079
  test("Comp_DICTIONARY_EXCLUDE_079", Include) {

    checkAnswer(s"""Select count(Latest_DAY) from Comp_DICTIONARY_EXCLUDE""",
      s"""Select count(Latest_DAY) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_079")

  }


  //Comp_DICTIONARY_EXCLUDE_080
  test("Comp_DICTIONARY_EXCLUDE_080", Include) {

    checkAnswer(s"""select count(DISTINCT Latest_DAY) as a from Comp_DICTIONARY_EXCLUDE""",
      s"""select count(DISTINCT Latest_DAY) as a from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_080")

  }


  //Comp_DICTIONARY_EXCLUDE_081
  test("Comp_DICTIONARY_EXCLUDE_081", Include) {

    checkAnswer(s"""select sum(Latest_DAY)+10 as a ,Latest_DAY  from Comp_DICTIONARY_EXCLUDE group by Latest_DAY order by a""",
      s"""select sum(Latest_DAY)+10 as a ,Latest_DAY  from Comp_DICTIONARY_EXCLUDE_hive group by Latest_DAY order by a""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_081")

  }


  //Comp_DICTIONARY_EXCLUDE_082
  test("Comp_DICTIONARY_EXCLUDE_082", Include) {

    checkAnswer(s"""select max(Latest_DAY),min(Latest_DAY) from Comp_DICTIONARY_EXCLUDE""",
      s"""select max(Latest_DAY),min(Latest_DAY) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_082")

  }


  //Comp_DICTIONARY_EXCLUDE_083
  test("Comp_DICTIONARY_EXCLUDE_083", Include) {

    checkAnswer(s"""select sum(Latest_DAY) a  from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(Latest_DAY) a  from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_083")

  }


  //Comp_DICTIONARY_EXCLUDE_084
  test("Comp_DICTIONARY_EXCLUDE_084", Include) {

    checkAnswer(s"""select avg(Latest_DAY) a  from Comp_DICTIONARY_EXCLUDE""",
      s"""select avg(Latest_DAY) a  from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_084")

  }


  //Comp_DICTIONARY_EXCLUDE_085
  test("Comp_DICTIONARY_EXCLUDE_085", Include) {

    checkAnswer(s"""select min(Latest_DAY) a  from Comp_DICTIONARY_EXCLUDE""",
      s"""select min(Latest_DAY) a  from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_085")

  }


  //Comp_DICTIONARY_EXCLUDE_086
  test("Comp_DICTIONARY_EXCLUDE_086", Include) {

    sql(s"""select variance(Latest_DAY) as a   from (select Latest_DAY from Comp_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_087
  test("Comp_DICTIONARY_EXCLUDE_087", Include) {

    sql(s"""select var_pop(Latest_DAY)  as a from (select Latest_DAY from Comp_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_088
  test("Comp_DICTIONARY_EXCLUDE_088", Include) {

    sql(s"""select var_samp(Latest_DAY) as a  from (select Latest_DAY from Comp_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_089
  test("Comp_DICTIONARY_EXCLUDE_089", Include) {

    sql(s"""select stddev_pop(Latest_DAY) as a  from (select Latest_DAY from Comp_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_090
  test("Comp_DICTIONARY_EXCLUDE_090", Include) {

    sql(s"""select stddev_samp(Latest_DAY)  as a from (select Latest_DAY from Comp_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_091
  test("Comp_DICTIONARY_EXCLUDE_091", Include) {

    sql(s"""select covar_pop(Latest_DAY,Latest_DAY) as a  from (select Latest_DAY from Comp_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_092
  test("Comp_DICTIONARY_EXCLUDE_092", Include) {

    sql(s"""select covar_samp(Latest_DAY,Latest_DAY) as a  from (select Latest_DAY from Comp_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_093
  test("Comp_DICTIONARY_EXCLUDE_093", Include) {

    checkAnswer(s"""select corr(Latest_DAY,Latest_DAY)  as a from Comp_DICTIONARY_EXCLUDE""",
      s"""select corr(Latest_DAY,Latest_DAY)  as a from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_093")

  }


  //Comp_DICTIONARY_EXCLUDE_094
  test("Comp_DICTIONARY_EXCLUDE_094", Include) {

    sql(s"""select percentile_approx(Latest_DAY,0.2) as a  from (select Latest_DAY from Comp_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_095
  test("Comp_DICTIONARY_EXCLUDE_095", Include) {

    sql(s"""select percentile_approx(Latest_DAY,0.2,5) as a  from (select Latest_DAY from Comp_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_096
  test("Comp_DICTIONARY_EXCLUDE_096", Include) {

    sql(s"""select percentile_approx(Latest_DAY,array(0.2,0.3,0.99))  as a from (select Latest_DAY from Comp_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_097
  test("Comp_DICTIONARY_EXCLUDE_097", Include) {

    sql(s"""select percentile_approx(Latest_DAY,array(0.2,0.3,0.99),5) as a from (select Latest_DAY from Comp_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_098
  test("Comp_DICTIONARY_EXCLUDE_098", Include) {

    sql(s"""select histogram_numeric(Latest_DAY,2)  as a from (select Latest_DAY from Comp_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_099
  test("Comp_DICTIONARY_EXCLUDE_099", Include) {

    checkAnswer(s"""select Latest_DAY, Latest_DAY+ 10 as a  from Comp_DICTIONARY_EXCLUDE order by a""",
      s"""select Latest_DAY, Latest_DAY+ 10 as a  from Comp_DICTIONARY_EXCLUDE_hive order by a""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_099")

  }


  //Comp_DICTIONARY_EXCLUDE_100
  test("Comp_DICTIONARY_EXCLUDE_100", Include) {

    checkAnswer(s"""select min(Latest_DAY) d, max(Latest_DAY+ 10) Total from Comp_DICTIONARY_EXCLUDE group by  channelsId order by d,Total""",
      s"""select min(Latest_DAY) d, max(Latest_DAY+ 10) Total from Comp_DICTIONARY_EXCLUDE_hive group by  channelsId order by d,Total""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_100")

  }


  //Comp_DICTIONARY_EXCLUDE_101
  test("Comp_DICTIONARY_EXCLUDE_101", Include) {

    sql(s"""select last(Latest_DAY) a from Comp_DICTIONARY_EXCLUDE order by a""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_102
  test("Comp_DICTIONARY_EXCLUDE_102", Include) {

    sql(s"""select FIRST(Latest_DAY) a from (select Latest_DAY from Comp_DICTIONARY_EXCLUDE order by Latest_DAY) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_103
  test("Comp_DICTIONARY_EXCLUDE_103", Include) {

    checkAnswer(s"""select Latest_DAY,count(Latest_DAY) a from Comp_DICTIONARY_EXCLUDE group by Latest_DAY order by Latest_DAY""",
      s"""select Latest_DAY,count(Latest_DAY) a from Comp_DICTIONARY_EXCLUDE_hive group by Latest_DAY order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_103")

  }


  //Comp_DICTIONARY_EXCLUDE_104
  test("Comp_DICTIONARY_EXCLUDE_104", Include) {

    checkAnswer(s"""select Lower(Latest_DAY) a  from Comp_DICTIONARY_EXCLUDE order by a""",
      s"""select Lower(Latest_DAY) a  from Comp_DICTIONARY_EXCLUDE_hive order by a""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_104")

  }


  //Comp_DICTIONARY_EXCLUDE_105
  test("Comp_DICTIONARY_EXCLUDE_105", Include) {

    checkAnswer(s"""select distinct Latest_DAY from Comp_DICTIONARY_EXCLUDE order by Latest_DAY""",
      s"""select distinct Latest_DAY from Comp_DICTIONARY_EXCLUDE_hive order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_105")

  }


  //Comp_DICTIONARY_EXCLUDE_106
  test("Comp_DICTIONARY_EXCLUDE_106", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE order by Latest_DAY limit 101""",
      s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE_hive order by Latest_DAY limit 101""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_106")

  }


  //Comp_DICTIONARY_EXCLUDE_107
  test("Comp_DICTIONARY_EXCLUDE_107", Include) {

    checkAnswer(s"""select Latest_DAY as a from Comp_DICTIONARY_EXCLUDE  order by a asc limit 10""",
      s"""select Latest_DAY as a from Comp_DICTIONARY_EXCLUDE_hive  order by a asc limit 10""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_107")

  }


  //Comp_DICTIONARY_EXCLUDE_108
  test("Comp_DICTIONARY_EXCLUDE_108", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE where  (Latest_DAY == 1234567890123450.0000000000)  and (imei=='1AA1')""",
      s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE_hive where  (Latest_DAY == 1234567890123450.0000000000)  and (imei=='1AA1')""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_108")

  }


  //Comp_DICTIONARY_EXCLUDE_109
  test("Comp_DICTIONARY_EXCLUDE_109", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""",
      s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE_hive where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_109")

  }


  //Comp_DICTIONARY_EXCLUDE_110
  test("Comp_DICTIONARY_EXCLUDE_110", Include) {

    checkAnswer(s"""select Latest_DAY  from Comp_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select Latest_DAY  from Comp_DICTIONARY_EXCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_110")

  }


  //Comp_DICTIONARY_EXCLUDE_111
  test("Comp_DICTIONARY_EXCLUDE_111", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""",
      s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE_hive where Latest_DAY !=1234567890123450.0000000000  order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_111")

  }


  //Comp_DICTIONARY_EXCLUDE_112
  test("Comp_DICTIONARY_EXCLUDE_112", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE where Latest_DAY >1234567890123450.0000000000  order by Latest_DAY""",
      s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE_hive where Latest_DAY >1234567890123450.0000000000  order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_112")

  }


  //Comp_DICTIONARY_EXCLUDE_113
  test("Comp_DICTIONARY_EXCLUDE_113", Include) {

    checkAnswer(s"""select Latest_DAY  from Comp_DICTIONARY_EXCLUDE where Latest_DAY<>Latest_DAY""",
      s"""select Latest_DAY  from Comp_DICTIONARY_EXCLUDE_hive where Latest_DAY<>Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_113")

  }


  //Comp_DICTIONARY_EXCLUDE_114
  test("Comp_DICTIONARY_EXCLUDE_114", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE where Latest_DAY != Latest_areaId order by Latest_DAY""",
      s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE_hive where Latest_DAY != Latest_areaId order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_114")

  }


  //Comp_DICTIONARY_EXCLUDE_115
  test("Comp_DICTIONARY_EXCLUDE_115", Include) {

    checkAnswer(s"""select Latest_DAY, Latest_DAY from Comp_DICTIONARY_EXCLUDE where Latest_areaId<Latest_DAY order by Latest_DAY""",
      s"""select Latest_DAY, Latest_DAY from Comp_DICTIONARY_EXCLUDE_hive where Latest_areaId<Latest_DAY order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_115")

  }


  //Comp_DICTIONARY_EXCLUDE_116
  test("Comp_DICTIONARY_EXCLUDE_116", Include) {

    checkAnswer(s"""select Latest_DAY, Latest_DAY from Comp_DICTIONARY_EXCLUDE where Latest_DAY<=Latest_DAY  order by Latest_DAY""",
      s"""select Latest_DAY, Latest_DAY from Comp_DICTIONARY_EXCLUDE_hive where Latest_DAY<=Latest_DAY  order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_116")

  }


  //Comp_DICTIONARY_EXCLUDE_117
  test("Comp_DICTIONARY_EXCLUDE_117", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE where Latest_DAY <1000  order by Latest_DAY""",
      s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE_hive where Latest_DAY <1000  order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_117")

  }


  //Comp_DICTIONARY_EXCLUDE_118
  test("Comp_DICTIONARY_EXCLUDE_118", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE where Latest_DAY >1000  order by Latest_DAY""",
      s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE_hive where Latest_DAY >1000  order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_118")

  }


  //Comp_DICTIONARY_EXCLUDE_119
  test("Comp_DICTIONARY_EXCLUDE_119", Include) {

    checkAnswer(s"""select Latest_DAY  from Comp_DICTIONARY_EXCLUDE where Latest_DAY IS NULL  order by Latest_DAY""",
      s"""select Latest_DAY  from Comp_DICTIONARY_EXCLUDE_hive where Latest_DAY IS NULL  order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_119")

  }


  //Comp_DICTIONARY_EXCLUDE_120
  test("Comp_DICTIONARY_EXCLUDE_120", Include) {

    checkAnswer(s"""select Latest_DAY  from Comp_DICTIONARY_EXCLUDE where Latest_DAY IS NOT NULL  order by Latest_DAY""",
      s"""select Latest_DAY  from Comp_DICTIONARY_EXCLUDE_hive where Latest_DAY IS NOT NULL  order by Latest_DAY""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_120")

  }


  //Comp_DICTIONARY_EXCLUDE_121
  test("Comp_DICTIONARY_EXCLUDE_121", Include) {

    checkAnswer(s"""Select count(gamePointId) from Comp_DICTIONARY_EXCLUDE""",
      s"""Select count(gamePointId) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_121")

  }


  //Comp_DICTIONARY_EXCLUDE_122
  test("Comp_DICTIONARY_EXCLUDE_122", Include) {

    checkAnswer(s"""select count(DISTINCT gamePointId) as a from Comp_DICTIONARY_EXCLUDE""",
      s"""select count(DISTINCT gamePointId) as a from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_122")

  }


  //Comp_DICTIONARY_EXCLUDE_123
  test("Comp_DICTIONARY_EXCLUDE_123", Include) {

    sql(s"""select sum(gamePointId)+10 as a ,gamePointId  from Comp_DICTIONARY_EXCLUDE group by gamePointId order by a""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_124
  test("Comp_DICTIONARY_EXCLUDE_124", Include) {

    checkAnswer(s"""select max(gamePointId),min(gamePointId) from Comp_DICTIONARY_EXCLUDE""",
      s"""select max(gamePointId),min(gamePointId) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_124")

  }


  //Comp_DICTIONARY_EXCLUDE_125
  test("Comp_DICTIONARY_EXCLUDE_125", Include) {

    sql(s"""select sum(gamePointId) a  from Comp_DICTIONARY_EXCLUDE""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_126
  test("Comp_DICTIONARY_EXCLUDE_126", Include) {

    sql(s"""select avg(gamePointId) a  from Comp_DICTIONARY_EXCLUDE""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_127
  test("Comp_DICTIONARY_EXCLUDE_127", Include) {

    checkAnswer(s"""select min(gamePointId) a  from Comp_DICTIONARY_EXCLUDE""",
      s"""select min(gamePointId) a  from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_127")

  }


  //Comp_DICTIONARY_EXCLUDE_128
  test("Comp_DICTIONARY_EXCLUDE_128", Include) {

    sql(s"""select variance(gamePointId) as a   from (select gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_129
  test("Comp_DICTIONARY_EXCLUDE_129", Include) {

    sql(s"""select var_pop(gamePointId)  as a from (select gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_130
  test("Comp_DICTIONARY_EXCLUDE_130", Include) {

    sql(s"""select var_samp(gamePointId) as a  from (select gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_131
  test("Comp_DICTIONARY_EXCLUDE_131", Include) {

    sql(s"""select stddev_pop(gamePointId) as a  from (select gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_132
  test("Comp_DICTIONARY_EXCLUDE_132", Include) {

    sql(s"""select stddev_samp(gamePointId)  as a from (select gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_133
  test("Comp_DICTIONARY_EXCLUDE_133", Include) {

    sql(s"""select covar_pop(gamePointId,gamePointId) as a  from (select gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_134
  test("Comp_DICTIONARY_EXCLUDE_134", Include) {

    sql(s"""select covar_samp(gamePointId,gamePointId) as a  from (select gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_135
  test("Comp_DICTIONARY_EXCLUDE_135", Include) {

    checkAnswer(s"""select corr(gamePointId,gamePointId)  as a from Comp_DICTIONARY_EXCLUDE""",
      s"""select corr(gamePointId,gamePointId)  as a from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_135")

  }


  //Comp_DICTIONARY_EXCLUDE_136
  test("Comp_DICTIONARY_EXCLUDE_136", Include) {

    sql(s"""select percentile_approx(gamePointId,0.2) as a  from (select gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_137
  test("Comp_DICTIONARY_EXCLUDE_137", Include) {

    sql(s"""select percentile_approx(gamePointId,0.2,5) as a  from (select gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_138
  test("Comp_DICTIONARY_EXCLUDE_138", Include) {

    sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99))  as a from (select gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_139
  test("Comp_DICTIONARY_EXCLUDE_139", Include) {

    sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99),5) as a from (select gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_140
  test("Comp_DICTIONARY_EXCLUDE_140", Include) {

    sql(s"""select histogram_numeric(gamePointId,2)  as a from (select gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_141
  test("Comp_DICTIONARY_EXCLUDE_141", Include) {

    checkAnswer(s"""select gamePointId, gamePointId+ 10 as a  from Comp_DICTIONARY_EXCLUDE order by a""",
      s"""select gamePointId, gamePointId+ 10 as a  from Comp_DICTIONARY_EXCLUDE_hive order by a""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_141")

  }


  //Comp_DICTIONARY_EXCLUDE_142
  test("Comp_DICTIONARY_EXCLUDE_142", Include) {

    checkAnswer(s"""select min(gamePointId), max(gamePointId+ 10) Total from Comp_DICTIONARY_EXCLUDE group by  channelsId order by Total""",
      s"""select min(gamePointId), max(gamePointId+ 10) Total from Comp_DICTIONARY_EXCLUDE_hive group by  channelsId order by Total""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_142")

  }


  //Comp_DICTIONARY_EXCLUDE_143
  test("Comp_DICTIONARY_EXCLUDE_143", Include) {

    sql(s"""select last(gamePointId) a from (select gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_144
  test("Comp_DICTIONARY_EXCLUDE_144", Include) {

    sql(s"""select FIRST(gamePointId) a from Comp_DICTIONARY_EXCLUDE order by a""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_145
  test("Comp_DICTIONARY_EXCLUDE_145", Include) {

    checkAnswer(s"""select gamePointId,count(gamePointId) a from Comp_DICTIONARY_EXCLUDE group by gamePointId order by gamePointId""",
      s"""select gamePointId,count(gamePointId) a from Comp_DICTIONARY_EXCLUDE_hive group by gamePointId order by gamePointId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_145")

  }


  //Comp_DICTIONARY_EXCLUDE_146
  test("Comp_DICTIONARY_EXCLUDE_146", Include) {

    checkAnswer(s"""select Lower(gamePointId) a  from Comp_DICTIONARY_EXCLUDE order by gamePointId""",
      s"""select Lower(gamePointId) a  from Comp_DICTIONARY_EXCLUDE_hive order by gamePointId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_146")

  }


  //Comp_DICTIONARY_EXCLUDE_147
  test("Comp_DICTIONARY_EXCLUDE_147", Include) {

    checkAnswer(s"""select distinct gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId""",
      s"""select distinct gamePointId from Comp_DICTIONARY_EXCLUDE_hive order by gamePointId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_147")

  }


  //Comp_DICTIONARY_EXCLUDE_148
  test("Comp_DICTIONARY_EXCLUDE_148", Include) {

    checkAnswer(s"""select gamePointId from Comp_DICTIONARY_EXCLUDE  order by gamePointId limit 101""",
      s"""select gamePointId from Comp_DICTIONARY_EXCLUDE_hive  order by gamePointId limit 101""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_148")

  }


  //Comp_DICTIONARY_EXCLUDE_149
  test("Comp_DICTIONARY_EXCLUDE_149", Include) {

    checkAnswer(s"""select gamePointId as a from Comp_DICTIONARY_EXCLUDE  order by a asc limit 10""",
      s"""select gamePointId as a from Comp_DICTIONARY_EXCLUDE_hive  order by a asc limit 10""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_149")

  }


  //Comp_DICTIONARY_EXCLUDE_150
  test("Comp_DICTIONARY_EXCLUDE_150", Include) {

    checkAnswer(s"""select gamePointId from Comp_DICTIONARY_EXCLUDE where  (gamePointId == 4.70133553923674E43) and (imei=='1AA100084')""",
      s"""select gamePointId from Comp_DICTIONARY_EXCLUDE_hive where  (gamePointId == 4.70133553923674E43) and (imei=='1AA100084')""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_150")

  }


  //Comp_DICTIONARY_EXCLUDE_151
  test("Comp_DICTIONARY_EXCLUDE_151", Include) {

    checkAnswer(s"""select gamePointId from Comp_DICTIONARY_EXCLUDE where gamePointId !=4.70133553923674E43  order by gamePointId""",
      s"""select gamePointId from Comp_DICTIONARY_EXCLUDE_hive where gamePointId !=4.70133553923674E43  order by gamePointId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_151")

  }


  //Comp_DICTIONARY_EXCLUDE_152
  test("Comp_DICTIONARY_EXCLUDE_152", Include) {

    checkAnswer(s"""select gamePointId  from Comp_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select gamePointId  from Comp_DICTIONARY_EXCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_152")

  }


  //Comp_DICTIONARY_EXCLUDE_153
  test("Comp_DICTIONARY_EXCLUDE_153", Include) {

    checkAnswer(s"""select gamePointId from Comp_DICTIONARY_EXCLUDE where gamePointId !=4.70133553923674E43""",
      s"""select gamePointId from Comp_DICTIONARY_EXCLUDE_hive where gamePointId !=4.70133553923674E43""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_153")

  }


  //Comp_DICTIONARY_EXCLUDE_154
  test("Comp_DICTIONARY_EXCLUDE_154", Include) {

    checkAnswer(s"""select gamePointId from Comp_DICTIONARY_EXCLUDE where gamePointId >4.70133553923674E43""",
      s"""select gamePointId from Comp_DICTIONARY_EXCLUDE_hive where gamePointId >4.70133553923674E43""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_154")

  }


  //Comp_DICTIONARY_EXCLUDE_155
  test("Comp_DICTIONARY_EXCLUDE_155", Include) {

    checkAnswer(s"""select gamePointId  from Comp_DICTIONARY_EXCLUDE where gamePointId<>gamePointId""",
      s"""select gamePointId  from Comp_DICTIONARY_EXCLUDE_hive where gamePointId<>gamePointId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_155")

  }


  //Comp_DICTIONARY_EXCLUDE_156
  test("Comp_DICTIONARY_EXCLUDE_156", Include) {

    checkAnswer(s"""select gamePointId from Comp_DICTIONARY_EXCLUDE where gamePointId != Latest_areaId  order by gamePointId""",
      s"""select gamePointId from Comp_DICTIONARY_EXCLUDE_hive where gamePointId != Latest_areaId  order by gamePointId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_156")

  }


  //Comp_DICTIONARY_EXCLUDE_157
  test("Comp_DICTIONARY_EXCLUDE_157", Include) {

    checkAnswer(s"""select gamePointId, gamePointId from Comp_DICTIONARY_EXCLUDE where Latest_areaId<gamePointId  order by gamePointId""",
      s"""select gamePointId, gamePointId from Comp_DICTIONARY_EXCLUDE_hive where Latest_areaId<gamePointId  order by gamePointId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_157")

  }


  //Comp_DICTIONARY_EXCLUDE_158
  test("Comp_DICTIONARY_EXCLUDE_158", Include) {

    checkAnswer(s"""select gamePointId, gamePointId from Comp_DICTIONARY_EXCLUDE where gamePointId<=gamePointId  order by gamePointId""",
      s"""select gamePointId, gamePointId from Comp_DICTIONARY_EXCLUDE_hive where gamePointId<=gamePointId  order by gamePointId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_158")

  }


  //Comp_DICTIONARY_EXCLUDE_159
  test("Comp_DICTIONARY_EXCLUDE_159", Include) {

    checkAnswer(s"""select gamePointId from Comp_DICTIONARY_EXCLUDE where gamePointId <1000 order by gamePointId""",
      s"""select gamePointId from Comp_DICTIONARY_EXCLUDE_hive where gamePointId <1000 order by gamePointId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_159")

  }


  //Comp_DICTIONARY_EXCLUDE_160
  test("Comp_DICTIONARY_EXCLUDE_160", Include) {

    checkAnswer(s"""select gamePointId from Comp_DICTIONARY_EXCLUDE where gamePointId >1000 order by gamePointId""",
      s"""select gamePointId from Comp_DICTIONARY_EXCLUDE_hive where gamePointId >1000 order by gamePointId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_160")

  }


  //Comp_DICTIONARY_EXCLUDE_161
  test("Comp_DICTIONARY_EXCLUDE_161", Include) {

    checkAnswer(s"""select gamePointId  from Comp_DICTIONARY_EXCLUDE where gamePointId IS NULL order by gamePointId""",
      s"""select gamePointId  from Comp_DICTIONARY_EXCLUDE_hive where gamePointId IS NULL order by gamePointId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_161")

  }


  //Comp_DICTIONARY_EXCLUDE_162
  test("Comp_DICTIONARY_EXCLUDE_162", Include) {

    checkAnswer(s"""select gamePointId  from Comp_DICTIONARY_EXCLUDE where gamePointId IS NOT NULL order by gamePointId""",
      s"""select gamePointId  from Comp_DICTIONARY_EXCLUDE_hive where gamePointId IS NOT NULL order by gamePointId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_162")

  }


  //Comp_DICTIONARY_EXCLUDE_163
  test("Comp_DICTIONARY_EXCLUDE_163", Include) {

    checkAnswer(s"""Select count(productionDate) from Comp_DICTIONARY_EXCLUDE""",
      s"""Select count(productionDate) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_163")

  }


  //Comp_DICTIONARY_EXCLUDE_164
  test("Comp_DICTIONARY_EXCLUDE_164", Include) {

    checkAnswer(s"""select count(DISTINCT productionDate) as a from Comp_DICTIONARY_EXCLUDE""",
      s"""select count(DISTINCT productionDate) as a from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_164")

  }


  //Comp_DICTIONARY_EXCLUDE_165
  test("Comp_DICTIONARY_EXCLUDE_165", Include) {

    checkAnswer(s"""select sum(productionDate)+10 as a ,productionDate  from Comp_DICTIONARY_EXCLUDE group by productionDate order by productionDate""",
      s"""select sum(productionDate)+10 as a ,productionDate  from Comp_DICTIONARY_EXCLUDE_hive group by productionDate order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_165")

  }


  //Comp_DICTIONARY_EXCLUDE_166
  test("Comp_DICTIONARY_EXCLUDE_166", Include) {

    checkAnswer(s"""select max(productionDate),min(productionDate) from Comp_DICTIONARY_EXCLUDE""",
      s"""select max(productionDate),min(productionDate) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_166")

  }


  //Comp_DICTIONARY_EXCLUDE_167
  test("Comp_DICTIONARY_EXCLUDE_167", Include) {

    checkAnswer(s"""select sum(productionDate) a  from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(productionDate) a  from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_167")

  }


  //Comp_DICTIONARY_EXCLUDE_168
  test("Comp_DICTIONARY_EXCLUDE_168", Include) {

    checkAnswer(s"""select avg(productionDate) a  from Comp_DICTIONARY_EXCLUDE""",
      s"""select avg(productionDate) a  from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_168")

  }


  //Comp_DICTIONARY_EXCLUDE_169
  test("Comp_DICTIONARY_EXCLUDE_169", Include) {

    checkAnswer(s"""select min(productionDate) a  from Comp_DICTIONARY_EXCLUDE""",
      s"""select min(productionDate) a  from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_169")

  }


  //Comp_DICTIONARY_EXCLUDE_170
  test("Comp_DICTIONARY_EXCLUDE_170", Include) {

    sql(s"""select variance(gamePointId) as a from (select gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_171
  test("Comp_DICTIONARY_EXCLUDE_171", Include) {

    sql(s"""select var_pop(gamePointId)  as a from Comp_DICTIONARY_EXCLUDE""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_172
  test("Comp_DICTIONARY_EXCLUDE_172", Include) {

    sql(s"""select var_samp(gamePointId) as a  from Comp_DICTIONARY_EXCLUDE""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_173
  test("Comp_DICTIONARY_EXCLUDE_173", Include) {

    sql(s"""select stddev_pop(gamePointId) as a  from (select gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_174
  test("Comp_DICTIONARY_EXCLUDE_174", Include) {

    sql(s"""select stddev_samp(gamePointId)  as a from (select gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_175
  test("Comp_DICTIONARY_EXCLUDE_175", Include) {

    sql(s"""select covar_pop(gamePointId,gamePointId) as a  from (select gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_176
  test("Comp_DICTIONARY_EXCLUDE_176", Include) {

    sql(s"""select covar_samp(gamePointId,gamePointId) as a  from (select gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_177
  test("Comp_DICTIONARY_EXCLUDE_177", Include) {

    checkAnswer(s"""select corr(gamePointId,gamePointId)  as a from Comp_DICTIONARY_EXCLUDE""",
      s"""select corr(gamePointId,gamePointId)  as a from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_177")

  }


  //Comp_DICTIONARY_EXCLUDE_178
  test("Comp_DICTIONARY_EXCLUDE_178", Include) {

    sql(s"""select percentile_approx(gamePointId,0.2) as a  from (select gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_179
  test("Comp_DICTIONARY_EXCLUDE_179", Include) {

    sql(s"""select percentile_approx(gamePointId,0.2,5) as a  from (select gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_180
  test("Comp_DICTIONARY_EXCLUDE_180", Include) {

    sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99))  as a from (select gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_181
  test("Comp_DICTIONARY_EXCLUDE_181", Include) {

    sql(s"""select percentile_approx(gamePointId,array(0.2,0.3,0.99),5) as a from (select gamePointId from Comp_DICTIONARY_EXCLUDE order by gamePointId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_182
  test("Comp_DICTIONARY_EXCLUDE_182", Include) {

    sql(s"""select histogram_numeric(productionDate,2)  as a from (select productionDate from Comp_DICTIONARY_EXCLUDE order by productionDate) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_183
  test("Comp_DICTIONARY_EXCLUDE_183", Include) {

    sql(s"""select last(productionDate) a from Comp_DICTIONARY_EXCLUDE order by a""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_184
  test("Comp_DICTIONARY_EXCLUDE_184", Include) {

    sql(s"""select FIRST(productionDate) a from (select productionDate from Comp_DICTIONARY_EXCLUDE order by productionDate) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_185
  test("Comp_DICTIONARY_EXCLUDE_185", Include) {

    checkAnswer(s"""select productionDate,count(productionDate) a from Comp_DICTIONARY_EXCLUDE group by productionDate order by productionDate""",
      s"""select productionDate,count(productionDate) a from Comp_DICTIONARY_EXCLUDE_hive group by productionDate order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_185")

  }


  //Comp_DICTIONARY_EXCLUDE_186
  test("Comp_DICTIONARY_EXCLUDE_186", Include) {

    checkAnswer(s"""select Lower(productionDate) a  from Comp_DICTIONARY_EXCLUDE order by productionDate""",
      s"""select Lower(productionDate) a  from Comp_DICTIONARY_EXCLUDE_hive order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_186")

  }


  //Comp_DICTIONARY_EXCLUDE_187
  test("Comp_DICTIONARY_EXCLUDE_187", Include) {

    checkAnswer(s"""select distinct productionDate from Comp_DICTIONARY_EXCLUDE order by productionDate""",
      s"""select distinct productionDate from Comp_DICTIONARY_EXCLUDE_hive order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_187")

  }


  //Comp_DICTIONARY_EXCLUDE_188
  test("Comp_DICTIONARY_EXCLUDE_188", Include) {

    checkAnswer(s"""select productionDate from Comp_DICTIONARY_EXCLUDE order by productionDate limit 101""",
      s"""select productionDate from Comp_DICTIONARY_EXCLUDE_hive order by productionDate limit 101""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_188")

  }


  //Comp_DICTIONARY_EXCLUDE_189
  test("Comp_DICTIONARY_EXCLUDE_189", Include) {

    checkAnswer(s"""select productionDate as a from Comp_DICTIONARY_EXCLUDE  order by a asc limit 10""",
      s"""select productionDate as a from Comp_DICTIONARY_EXCLUDE_hive  order by a asc limit 10""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_189")

  }


  //Comp_DICTIONARY_EXCLUDE_190
  test("Comp_DICTIONARY_EXCLUDE_190", Include) {

    checkAnswer(s"""select productionDate from Comp_DICTIONARY_EXCLUDE where  (productionDate == '2015-07-01 12:07:28.0') and (productionDate=='2015-07-01 12:07:28.0')""",
      s"""select productionDate from Comp_DICTIONARY_EXCLUDE_hive where  (productionDate == '2015-07-01 12:07:28.0') and (productionDate=='2015-07-01 12:07:28.0')""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_190")

  }


  //Comp_DICTIONARY_EXCLUDE_191
  test("Comp_DICTIONARY_EXCLUDE_191", Include) {

    checkAnswer(s"""select productionDate from Comp_DICTIONARY_EXCLUDE where productionDate !='2015-07-01 12:07:28.0' order by productionDate""",
      s"""select productionDate from Comp_DICTIONARY_EXCLUDE_hive where productionDate !='2015-07-01 12:07:28.0' order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_191")

  }


  //Comp_DICTIONARY_EXCLUDE_192
  test("Comp_DICTIONARY_EXCLUDE_192", Include) {

    checkAnswer(s"""select productionDate  from Comp_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select productionDate  from Comp_DICTIONARY_EXCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_192")

  }


  //Comp_DICTIONARY_EXCLUDE_193
  test("Comp_DICTIONARY_EXCLUDE_193", Include) {

    checkAnswer(s"""select productionDate from Comp_DICTIONARY_EXCLUDE where productionDate !='2015-07-01 12:07:28.0' order by productionDate""",
      s"""select productionDate from Comp_DICTIONARY_EXCLUDE_hive where productionDate !='2015-07-01 12:07:28.0' order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_193")

  }


  //Comp_DICTIONARY_EXCLUDE_194
  test("Comp_DICTIONARY_EXCLUDE_194", Include) {

    checkAnswer(s"""select productionDate from Comp_DICTIONARY_EXCLUDE where productionDate >'2015-07-01 12:07:28.0' order by productionDate""",
      s"""select productionDate from Comp_DICTIONARY_EXCLUDE_hive where productionDate >'2015-07-01 12:07:28.0' order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_194")

  }


  //Comp_DICTIONARY_EXCLUDE_195
  test("Comp_DICTIONARY_EXCLUDE_195", Include) {

    checkAnswer(s"""select productionDate  from Comp_DICTIONARY_EXCLUDE where productionDate<>productionDate order by productionDate""",
      s"""select productionDate  from Comp_DICTIONARY_EXCLUDE_hive where productionDate<>productionDate order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_195")

  }


  //Comp_DICTIONARY_EXCLUDE_196
  test("Comp_DICTIONARY_EXCLUDE_196", Include) {

    checkAnswer(s"""select productionDate from Comp_DICTIONARY_EXCLUDE where productionDate != Latest_areaId order by productionDate""",
      s"""select productionDate from Comp_DICTIONARY_EXCLUDE_hive where productionDate != Latest_areaId order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_196")

  }


  //Comp_DICTIONARY_EXCLUDE_197
  test("Comp_DICTIONARY_EXCLUDE_197", Include) {

    checkAnswer(s"""select productionDate from Comp_DICTIONARY_EXCLUDE where Latest_areaId<productionDate order by productionDate""",
      s"""select productionDate from Comp_DICTIONARY_EXCLUDE_hive where Latest_areaId<productionDate order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_197")

  }


  //Comp_DICTIONARY_EXCLUDE_198
  test("Comp_DICTIONARY_EXCLUDE_198", Include) {

    checkAnswer(s"""select productionDate from Comp_DICTIONARY_EXCLUDE where productionDate<=productionDate order by productionDate""",
      s"""select productionDate from Comp_DICTIONARY_EXCLUDE_hive where productionDate<=productionDate order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_198")

  }


  //Comp_DICTIONARY_EXCLUDE_199
  test("Comp_DICTIONARY_EXCLUDE_199", Include) {

    checkAnswer(s"""select productionDate from Comp_DICTIONARY_EXCLUDE where productionDate <cast('2015-07-01 12:07:28.0' as timestamp) order by productionDate""",
      s"""select productionDate from Comp_DICTIONARY_EXCLUDE_hive where productionDate <cast('2015-07-01 12:07:28.0' as timestamp) order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_199")

  }


  //Comp_DICTIONARY_EXCLUDE_200
  test("Comp_DICTIONARY_EXCLUDE_200", Include) {

    checkAnswer(s"""select productionDate  from Comp_DICTIONARY_EXCLUDE where productionDate IS NULL""",
      s"""select productionDate  from Comp_DICTIONARY_EXCLUDE_hive where productionDate IS NULL""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_200")

  }


  //Comp_DICTIONARY_EXCLUDE_201
  test("Comp_DICTIONARY_EXCLUDE_201", Include) {

    checkAnswer(s"""select productionDate  from Comp_DICTIONARY_EXCLUDE where productionDate IS NOT NULL order by productionDate""",
      s"""select productionDate  from Comp_DICTIONARY_EXCLUDE_hive where productionDate IS NOT NULL order by productionDate""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_201")

  }


  //Comp_DICTIONARY_EXCLUDE_202
  test("Comp_DICTIONARY_EXCLUDE_202", Include) {

    checkAnswer(s"""Select count(deviceInformationId) from Comp_DICTIONARY_EXCLUDE""",
      s"""Select count(deviceInformationId) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_202")

  }


  //Comp_DICTIONARY_EXCLUDE_203
  test("Comp_DICTIONARY_EXCLUDE_203", Include) {

    checkAnswer(s"""select count(DISTINCT deviceInformationId) as a from Comp_DICTIONARY_EXCLUDE""",
      s"""select count(DISTINCT deviceInformationId) as a from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_203")

  }


  //Comp_DICTIONARY_EXCLUDE_204
  test("Comp_DICTIONARY_EXCLUDE_204", Include) {

    checkAnswer(s"""select sum(deviceInformationId)+10 as a ,deviceInformationId  from Comp_DICTIONARY_EXCLUDE group by deviceInformationId order by deviceInformationId""",
      s"""select sum(deviceInformationId)+10 as a ,deviceInformationId  from Comp_DICTIONARY_EXCLUDE_hive group by deviceInformationId order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_204")

  }


  //Comp_DICTIONARY_EXCLUDE_205
  test("Comp_DICTIONARY_EXCLUDE_205", Include) {

    checkAnswer(s"""select max(deviceInformationId),min(deviceInformationId) from Comp_DICTIONARY_EXCLUDE""",
      s"""select max(deviceInformationId),min(deviceInformationId) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_205")

  }


  //Comp_DICTIONARY_EXCLUDE_206
  test("Comp_DICTIONARY_EXCLUDE_206", Include) {

    checkAnswer(s"""select sum(deviceInformationId) a  from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(deviceInformationId) a  from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_206")

  }


  //Comp_DICTIONARY_EXCLUDE_207
  test("Comp_DICTIONARY_EXCLUDE_207", Include) {

    checkAnswer(s"""select avg(deviceInformationId) a  from Comp_DICTIONARY_EXCLUDE""",
      s"""select avg(deviceInformationId) a  from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_207")

  }


  //Comp_DICTIONARY_EXCLUDE_208
  test("Comp_DICTIONARY_EXCLUDE_208", Include) {

    checkAnswer(s"""select min(deviceInformationId) a  from Comp_DICTIONARY_EXCLUDE""",
      s"""select min(deviceInformationId) a  from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_208")

  }


  //Comp_DICTIONARY_EXCLUDE_209
  test("Comp_DICTIONARY_EXCLUDE_209", Include) {

    sql(s"""select variance(deviceInformationId) as a   from (select deviceInformationId from Comp_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_210
  ignore("Comp_DICTIONARY_EXCLUDE_210", Include) {

    checkAnswer(s"""select var_pop(deviceInformationId)  as a from Comp_DICTIONARY_EXCLUDE""",
      s"""select var_pop(deviceInformationId)  as a from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_210")

  }


  //Comp_DICTIONARY_EXCLUDE_211
  ignore("Comp_DICTIONARY_EXCLUDE_211", Include) {

    checkAnswer(s"""select var_samp(deviceInformationId) as a  from Comp_DICTIONARY_EXCLUDE""",
      s"""select var_samp(deviceInformationId) as a  from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_211")

  }


  //Comp_DICTIONARY_EXCLUDE_212
  test("Comp_DICTIONARY_EXCLUDE_212", Include) {

    sql(s"""select stddev_pop(deviceInformationId) as a  from (select deviceInformationId from Comp_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_213
  test("Comp_DICTIONARY_EXCLUDE_213", Include) {

    sql(s"""select stddev_samp(deviceInformationId)  as a from (select deviceInformationId from Comp_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_214
  test("Comp_DICTIONARY_EXCLUDE_214", Include) {

    sql(s"""select covar_pop(deviceInformationId,deviceInformationId) as a  from (select deviceInformationId from Comp_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_215
  test("Comp_DICTIONARY_EXCLUDE_215", Include) {

    sql(s"""select covar_samp(deviceInformationId,deviceInformationId) as a  from (select deviceInformationId from Comp_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_216
  test("Comp_DICTIONARY_EXCLUDE_216", Include) {

    checkAnswer(s"""select corr(deviceInformationId,deviceInformationId)  as a from Comp_DICTIONARY_EXCLUDE""",
      s"""select corr(deviceInformationId,deviceInformationId)  as a from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_216")

  }


  //Comp_DICTIONARY_EXCLUDE_217
  test("Comp_DICTIONARY_EXCLUDE_217", Include) {

    sql(s"""select percentile_approx(deviceInformationId,0.2) as a  from (select deviceInformationId from Comp_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_218
  test("Comp_DICTIONARY_EXCLUDE_218", Include) {

    sql(s"""select percentile_approx(deviceInformationId,0.2,5) as a  from (select deviceInformationId from Comp_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_219
  test("Comp_DICTIONARY_EXCLUDE_219", Include) {

    sql(s"""select percentile_approx(deviceInformationId,array(0.2,0.3,0.99))  as a from (select deviceInformationId from Comp_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_220
  test("Comp_DICTIONARY_EXCLUDE_220", Include) {

    sql(s"""select percentile_approx(deviceInformationId,array(0.2,0.3,0.99),5) as a from (select deviceInformationId from Comp_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_221
  test("Comp_DICTIONARY_EXCLUDE_221", Include) {

    sql(s"""select histogram_numeric(deviceInformationId,2)  as a from (select deviceInformationId from Comp_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_222
  test("Comp_DICTIONARY_EXCLUDE_222", Include) {

    checkAnswer(s"""select deviceInformationId, deviceInformationId+ 10 as a  from Comp_DICTIONARY_EXCLUDE order by deviceInformationId""",
      s"""select deviceInformationId, deviceInformationId+ 10 as a  from Comp_DICTIONARY_EXCLUDE_hive order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_222")

  }


  //Comp_DICTIONARY_EXCLUDE_223
  test("Comp_DICTIONARY_EXCLUDE_223", Include) {

    checkAnswer(s"""select min(deviceInformationId), max(deviceInformationId+ 10) Total from Comp_DICTIONARY_EXCLUDE group by  channelsId order by Total""",
      s"""select min(deviceInformationId), max(deviceInformationId+ 10) Total from Comp_DICTIONARY_EXCLUDE_hive group by  channelsId order by Total""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_223")

  }


  //Comp_DICTIONARY_EXCLUDE_224
  test("Comp_DICTIONARY_EXCLUDE_224", Include) {

    sql(s"""select last(deviceInformationId) a from Comp_DICTIONARY_EXCLUDE order by a""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_225
  test("Comp_DICTIONARY_EXCLUDE_225", Include) {

    sql(s"""select FIRST(deviceInformationId) a from (select deviceInformationId from Comp_DICTIONARY_EXCLUDE order by deviceInformationId) t""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_226
  test("Comp_DICTIONARY_EXCLUDE_226", Include) {

    checkAnswer(s"""select deviceInformationId,count(deviceInformationId) a from Comp_DICTIONARY_EXCLUDE group by deviceInformationId order by deviceInformationId""",
      s"""select deviceInformationId,count(deviceInformationId) a from Comp_DICTIONARY_EXCLUDE_hive group by deviceInformationId order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_226")

  }


  //Comp_DICTIONARY_EXCLUDE_227
  test("Comp_DICTIONARY_EXCLUDE_227", Include) {

    checkAnswer(s"""select Lower(deviceInformationId) a  from Comp_DICTIONARY_EXCLUDE order by deviceInformationId""",
      s"""select Lower(deviceInformationId) a  from Comp_DICTIONARY_EXCLUDE_hive order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_227")

  }


  //Comp_DICTIONARY_EXCLUDE_228
  test("Comp_DICTIONARY_EXCLUDE_228", Include) {

    checkAnswer(s"""select distinct deviceInformationId from Comp_DICTIONARY_EXCLUDE order by deviceInformationId""",
      s"""select distinct deviceInformationId from Comp_DICTIONARY_EXCLUDE_hive order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_228")

  }


  //Comp_DICTIONARY_EXCLUDE_229
  test("Comp_DICTIONARY_EXCLUDE_229", Include) {

    checkAnswer(s"""select deviceInformationId from Comp_DICTIONARY_EXCLUDE order by deviceInformationId limit 101""",
      s"""select deviceInformationId from Comp_DICTIONARY_EXCLUDE_hive order by deviceInformationId limit 101""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_229")

  }


  //Comp_DICTIONARY_EXCLUDE_230
  test("Comp_DICTIONARY_EXCLUDE_230", Include) {

    checkAnswer(s"""select deviceInformationId as a from Comp_DICTIONARY_EXCLUDE  order by a asc limit 10""",
      s"""select deviceInformationId as a from Comp_DICTIONARY_EXCLUDE_hive  order by a asc limit 10""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_230")

  }


  //Comp_DICTIONARY_EXCLUDE_231
  test("Comp_DICTIONARY_EXCLUDE_231", Include) {

    checkAnswer(s"""select deviceInformationId from Comp_DICTIONARY_EXCLUDE where  (deviceInformationId == 100084) and (deviceInformationId==100084)""",
      s"""select deviceInformationId from Comp_DICTIONARY_EXCLUDE_hive where  (deviceInformationId == 100084) and (deviceInformationId==100084)""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_231")

  }


  //Comp_DICTIONARY_EXCLUDE_232
  test("Comp_DICTIONARY_EXCLUDE_232", Include) {

    checkAnswer(s"""select deviceInformationId from Comp_DICTIONARY_EXCLUDE where deviceInformationId !='100084' order by deviceInformationId""",
      s"""select deviceInformationId from Comp_DICTIONARY_EXCLUDE_hive where deviceInformationId !='100084' order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_232")

  }


  //Comp_DICTIONARY_EXCLUDE_233
  test("Comp_DICTIONARY_EXCLUDE_233", Include) {

    checkAnswer(s"""select deviceInformationId  from Comp_DICTIONARY_EXCLUDE where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""",
      s"""select deviceInformationId  from Comp_DICTIONARY_EXCLUDE_hive where (deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and deviceColor='0Device Color')""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_233")

  }


  //Comp_DICTIONARY_EXCLUDE_234
  test("Comp_DICTIONARY_EXCLUDE_234", Include) {

    checkAnswer(s"""select deviceInformationId from Comp_DICTIONARY_EXCLUDE where deviceInformationId !=100084 order by deviceInformationId""",
      s"""select deviceInformationId from Comp_DICTIONARY_EXCLUDE_hive where deviceInformationId !=100084 order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_234")

  }


  //Comp_DICTIONARY_EXCLUDE_235
  test("Comp_DICTIONARY_EXCLUDE_235", Include) {

    checkAnswer(s"""select deviceInformationId from Comp_DICTIONARY_EXCLUDE where deviceInformationId >'100084' order by deviceInformationId""",
      s"""select deviceInformationId from Comp_DICTIONARY_EXCLUDE_hive where deviceInformationId >'100084' order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_235")

  }


  //Comp_DICTIONARY_EXCLUDE_236
  test("Comp_DICTIONARY_EXCLUDE_236", Include) {

    checkAnswer(s"""select deviceInformationId  from Comp_DICTIONARY_EXCLUDE where deviceInformationId<>deviceInformationId order by deviceInformationId""",
      s"""select deviceInformationId  from Comp_DICTIONARY_EXCLUDE_hive where deviceInformationId<>deviceInformationId order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_236")

  }


  //Comp_DICTIONARY_EXCLUDE_237
  test("Comp_DICTIONARY_EXCLUDE_237", Include) {

    checkAnswer(s"""select deviceInformationId from Comp_DICTIONARY_EXCLUDE where deviceInformationId != Latest_areaId order by deviceInformationId""",
      s"""select deviceInformationId from Comp_DICTIONARY_EXCLUDE_hive where deviceInformationId != Latest_areaId order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_237")

  }


  //Comp_DICTIONARY_EXCLUDE_238
  test("Comp_DICTIONARY_EXCLUDE_238", Include) {

    checkAnswer(s"""select deviceInformationId, deviceInformationId from Comp_DICTIONARY_EXCLUDE where Latest_areaId<deviceInformationId order by deviceInformationId""",
      s"""select deviceInformationId, deviceInformationId from Comp_DICTIONARY_EXCLUDE_hive where Latest_areaId<deviceInformationId order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_238")

  }


  //Comp_DICTIONARY_EXCLUDE_239
  test("Comp_DICTIONARY_EXCLUDE_239", Include) {

    checkAnswer(s"""select deviceInformationId, deviceInformationId from Comp_DICTIONARY_EXCLUDE where deviceInformationId<=deviceInformationId order by deviceInformationId""",
      s"""select deviceInformationId, deviceInformationId from Comp_DICTIONARY_EXCLUDE_hive where deviceInformationId<=deviceInformationId order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_239")

  }


  //Comp_DICTIONARY_EXCLUDE_240
  test("Comp_DICTIONARY_EXCLUDE_240", Include) {

    checkAnswer(s"""select deviceInformationId from Comp_DICTIONARY_EXCLUDE where deviceInformationId <1000 order by deviceInformationId""",
      s"""select deviceInformationId from Comp_DICTIONARY_EXCLUDE_hive where deviceInformationId <1000 order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_240")

  }


  //Comp_DICTIONARY_EXCLUDE_241
  test("Comp_DICTIONARY_EXCLUDE_241", Include) {

    checkAnswer(s"""select deviceInformationId from Comp_DICTIONARY_EXCLUDE where deviceInformationId >1000 order by deviceInformationId""",
      s"""select deviceInformationId from Comp_DICTIONARY_EXCLUDE_hive where deviceInformationId >1000 order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_241")

  }


  //Comp_DICTIONARY_EXCLUDE_242
  test("Comp_DICTIONARY_EXCLUDE_242", Include) {

    checkAnswer(s"""select deviceInformationId  from Comp_DICTIONARY_EXCLUDE where deviceInformationId IS NULL order by deviceInformationId""",
      s"""select deviceInformationId  from Comp_DICTIONARY_EXCLUDE_hive where deviceInformationId IS NULL order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_242")

  }


  //Comp_DICTIONARY_EXCLUDE_243
  test("Comp_DICTIONARY_EXCLUDE_243", Include) {

    checkAnswer(s"""select deviceInformationId  from Comp_DICTIONARY_EXCLUDE where deviceInformationId IS NOT NULL order by deviceInformationId""",
      s"""select deviceInformationId  from Comp_DICTIONARY_EXCLUDE_hive where deviceInformationId IS NOT NULL order by deviceInformationId""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_243")

  }


  //Comp_DICTIONARY_EXCLUDE_244
  test("Comp_DICTIONARY_EXCLUDE_244", Include) {

    checkAnswer(s"""select sum(imei)+10 as a   from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(imei)+10 as a   from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_244")

  }


  //Comp_DICTIONARY_EXCLUDE_245
  test("Comp_DICTIONARY_EXCLUDE_245", Include) {

    checkAnswer(s"""select sum(imei)*10 as a   from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(imei)*10 as a   from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_245")

  }


  //Comp_DICTIONARY_EXCLUDE_246
  test("Comp_DICTIONARY_EXCLUDE_246", Include) {

    checkAnswer(s"""select sum(imei)/10 as a   from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(imei)/10 as a   from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_246")

  }


  //Comp_DICTIONARY_EXCLUDE_247
  test("Comp_DICTIONARY_EXCLUDE_247", Include) {

    checkAnswer(s"""select sum(imei)-10 as a   from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(imei)-10 as a   from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_247")

  }


  //Comp_DICTIONARY_EXCLUDE_248
  test("Comp_DICTIONARY_EXCLUDE_248", Include) {

    checkAnswer(s"""select sum(contractNumber)+10 as a   from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(contractNumber)+10 as a   from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_248")

  }


  //Comp_DICTIONARY_EXCLUDE_249
  test("Comp_DICTIONARY_EXCLUDE_249", Include) {

    checkAnswer(s"""select sum(contractNumber)*10 as a   from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(contractNumber)*10 as a   from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_249")

  }


  //Comp_DICTIONARY_EXCLUDE_250
  test("Comp_DICTIONARY_EXCLUDE_250", Include) {

    checkAnswer(s"""select sum(contractNumber)/10 as a   from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(contractNumber)/10 as a   from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_250")

  }


  //Comp_DICTIONARY_EXCLUDE_251
  test("Comp_DICTIONARY_EXCLUDE_251", Include) {

    checkAnswer(s"""select sum(contractNumber)-10 as a   from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(contractNumber)-10 as a   from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_251")

  }


  //Comp_DICTIONARY_EXCLUDE_252
  test("Comp_DICTIONARY_EXCLUDE_252", Include) {

    checkAnswer(s"""select sum(Latest_DAY)+10 as a   from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(Latest_DAY)+10 as a   from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_252")

  }


  //Comp_DICTIONARY_EXCLUDE_253
  test("Comp_DICTIONARY_EXCLUDE_253", Include) {

    checkAnswer(s"""select sum(Latest_DAY)*10 as a   from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(Latest_DAY)*10 as a   from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_253")

  }


  //Comp_DICTIONARY_EXCLUDE_254
  test("Comp_DICTIONARY_EXCLUDE_254", Include) {

    checkAnswer(s"""select sum(Latest_DAY)/10 as a   from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(Latest_DAY)/10 as a   from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_254")

  }


  //Comp_DICTIONARY_EXCLUDE_255
  test("Comp_DICTIONARY_EXCLUDE_255", Include) {

    checkAnswer(s"""select sum(Latest_DAY)-10 as a   from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(Latest_DAY)-10 as a   from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_255")

  }


  //Comp_DICTIONARY_EXCLUDE_256
  test("Comp_DICTIONARY_EXCLUDE_256", Include) {

    sql(s"""select sum(gamePointId)+10 as a   from Comp_DICTIONARY_EXCLUDE""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_257
  test("Comp_DICTIONARY_EXCLUDE_257", Include) {

    sql(s"""select sum(gamePointId)*10 as a   from Comp_DICTIONARY_EXCLUDE""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_258
  test("Comp_DICTIONARY_EXCLUDE_258", Include) {

    sql(s"""select sum(gamePointId)/10 as a   from Comp_DICTIONARY_EXCLUDE""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_259
  test("Comp_DICTIONARY_EXCLUDE_259", Include) {

    sql(s"""select sum(gamePointId)-10 as a   from Comp_DICTIONARY_EXCLUDE""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_260
  test("Comp_DICTIONARY_EXCLUDE_260", Include) {

    checkAnswer(s"""select sum(productionDate)+10 as a   from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(productionDate)+10 as a   from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_260")

  }


  //Comp_DICTIONARY_EXCLUDE_261
  test("Comp_DICTIONARY_EXCLUDE_261", Include) {

    checkAnswer(s"""select sum(productionDate)*10 as a   from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(productionDate)*10 as a   from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_261")

  }


  //Comp_DICTIONARY_EXCLUDE_262
  test("Comp_DICTIONARY_EXCLUDE_262", Include) {

    checkAnswer(s"""select sum(productionDate)/10 as a   from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(productionDate)/10 as a   from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_262")

  }


  //Comp_DICTIONARY_EXCLUDE_263
  test("Comp_DICTIONARY_EXCLUDE_263", Include) {

    checkAnswer(s"""select sum(productionDate)-10 as a   from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(productionDate)-10 as a   from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_263")

  }


  //Comp_DICTIONARY_EXCLUDE_264
  test("Comp_DICTIONARY_EXCLUDE_264", Include) {

    checkAnswer(s"""select sum(deviceInformationId)+10 as a   from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(deviceInformationId)+10 as a   from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_264")

  }


  //Comp_DICTIONARY_EXCLUDE_265
  test("Comp_DICTIONARY_EXCLUDE_265", Include) {

    checkAnswer(s"""select sum(deviceInformationId)*10 as a   from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(deviceInformationId)*10 as a   from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_265")

  }


  //Comp_DICTIONARY_EXCLUDE_266
  test("Comp_DICTIONARY_EXCLUDE_266", Include) {

    checkAnswer(s"""select sum(deviceInformationId)/10 as a   from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(deviceInformationId)/10 as a   from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_266")

  }


  //Comp_DICTIONARY_EXCLUDE_267
  test("Comp_DICTIONARY_EXCLUDE_267", Include) {

    checkAnswer(s"""select sum(deviceInformationId)-10 as a   from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(deviceInformationId)-10 as a   from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_267")

  }


  //Comp_DICTIONARY_EXCLUDE_292
  test("Comp_DICTIONARY_EXCLUDE_292", Include) {

    checkAnswer(s"""SELECT productiondate from Comp_DICTIONARY_EXCLUDE where productiondate LIKE '2015-09-30%'""",
      s"""SELECT productiondate from Comp_DICTIONARY_EXCLUDE_hive where productiondate LIKE '2015-09-30%'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_292")

  }


  //Comp_DICTIONARY_EXCLUDE_293
  test("Comp_DICTIONARY_EXCLUDE_293", Include) {

    checkAnswer(s"""SELECT productiondate from Comp_DICTIONARY_EXCLUDE where productiondate LIKE '% %'""",
      s"""SELECT productiondate from Comp_DICTIONARY_EXCLUDE_hive where productiondate LIKE '% %'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_293")

  }


  //Comp_DICTIONARY_EXCLUDE_294
  test("Comp_DICTIONARY_EXCLUDE_294", Include) {

    checkAnswer(s"""SELECT productiondate from Comp_DICTIONARY_EXCLUDE where productiondate LIKE '%12:07:28'""",
      s"""SELECT productiondate from Comp_DICTIONARY_EXCLUDE_hive where productiondate LIKE '%12:07:28'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_294")

  }


  //Comp_DICTIONARY_EXCLUDE_295
  test("Comp_DICTIONARY_EXCLUDE_295", Include) {

    checkAnswer(s"""select contractnumber from Comp_DICTIONARY_EXCLUDE where contractnumber like '922337204%' """,
      s"""select contractnumber from Comp_DICTIONARY_EXCLUDE_hive where contractnumber like '922337204%' """, "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_295")

  }


  //Comp_DICTIONARY_EXCLUDE_296
  test("Comp_DICTIONARY_EXCLUDE_296", Include) {

    checkAnswer(s"""select contractnumber from Comp_DICTIONARY_EXCLUDE where contractnumber like '%047800'""",
      s"""select contractnumber from Comp_DICTIONARY_EXCLUDE_hive where contractnumber like '%047800'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_296")

  }


  //Comp_DICTIONARY_EXCLUDE_297
  test("Comp_DICTIONARY_EXCLUDE_297", Include) {

    checkAnswer(s"""select contractnumber from Comp_DICTIONARY_EXCLUDE where contractnumber like '%720%'""",
      s"""select contractnumber from Comp_DICTIONARY_EXCLUDE_hive where contractnumber like '%720%'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_297")

  }


  //Comp_DICTIONARY_EXCLUDE_298
  test("Comp_DICTIONARY_EXCLUDE_298", Include) {

    checkAnswer(s"""SELECT Latest_DAY from Comp_DICTIONARY_EXCLUDE where Latest_DAY like '12345678%'""",
      s"""SELECT Latest_DAY from Comp_DICTIONARY_EXCLUDE_hive where Latest_DAY like '12345678%'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_298")

  }


  //Comp_DICTIONARY_EXCLUDE_299
  test("Comp_DICTIONARY_EXCLUDE_299", Include) {

    checkAnswer(s"""SELECT Latest_DAY from Comp_DICTIONARY_EXCLUDE where Latest_DAY like '%5678%'""",
      s"""SELECT Latest_DAY from Comp_DICTIONARY_EXCLUDE_hive where Latest_DAY like '%5678%'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_299")

  }


  //Comp_DICTIONARY_EXCLUDE_300
  test("Comp_DICTIONARY_EXCLUDE_300", Include) {

    checkAnswer(s"""SELECT Latest_DAY from Comp_DICTIONARY_EXCLUDE where Latest_DAY like '1234567%'""",
      s"""SELECT Latest_DAY from Comp_DICTIONARY_EXCLUDE_hive where Latest_DAY like '1234567%'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_300")

  }


  //Comp_DICTIONARY_EXCLUDE_301
  test("Comp_DICTIONARY_EXCLUDE_301", Include) {

    checkAnswer(s"""SELECT gamepointID from Comp_DICTIONARY_EXCLUDE where gamepointID like '1.1098347722%'""",
      s"""SELECT gamepointID from Comp_DICTIONARY_EXCLUDE_hive where gamepointID like '1.1098347722%'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_301")

  }


  //Comp_DICTIONARY_EXCLUDE_302
  test("Comp_DICTIONARY_EXCLUDE_302", Include) {

    checkAnswer(s"""SELECT gamepointID from Comp_DICTIONARY_EXCLUDE where gamepointID like '%8347722%'""",
      s"""SELECT gamepointID from Comp_DICTIONARY_EXCLUDE_hive where gamepointID like '%8347722%'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_302")

  }


  //Comp_DICTIONARY_EXCLUDE_303
  test("Comp_DICTIONARY_EXCLUDE_303", Include) {

    checkAnswer(s"""SELECT gamepointID from Comp_DICTIONARY_EXCLUDE where gamepointID like '%7512E42'""",
      s"""SELECT gamepointID from Comp_DICTIONARY_EXCLUDE_hive where gamepointID like '%7512E42'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_303")

  }


  //Comp_DICTIONARY_EXCLUDE_304
  test("Comp_DICTIONARY_EXCLUDE_304", Include) {

    checkAnswer(s"""SELECT deviceinformationid from Comp_DICTIONARY_EXCLUDE where deviceinformationid like '1000%'""",
      s"""SELECT deviceinformationid from Comp_DICTIONARY_EXCLUDE_hive where deviceinformationid like '1000%'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_304")

  }


  //Comp_DICTIONARY_EXCLUDE_305
  test("Comp_DICTIONARY_EXCLUDE_305", Include) {

    checkAnswer(s"""SELECT deviceinformationid from Comp_DICTIONARY_EXCLUDE where deviceinformationid like '%00%'""",
      s"""SELECT deviceinformationid from Comp_DICTIONARY_EXCLUDE_hive where deviceinformationid like '%00%'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_305")

  }


  //Comp_DICTIONARY_EXCLUDE_306
  test("Comp_DICTIONARY_EXCLUDE_306", Include) {

    checkAnswer(s"""SELECT deviceinformationid from Comp_DICTIONARY_EXCLUDE where deviceinformationid like '%0084'""",
      s"""SELECT deviceinformationid from Comp_DICTIONARY_EXCLUDE_hive where deviceinformationid like '%0084'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_306")

  }


  //Comp_DICTIONARY_EXCLUDE_307
  test("Comp_DICTIONARY_EXCLUDE_307", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_EXCLUDE where imei like '1AA10%'""",
      s"""select imei from Comp_DICTIONARY_EXCLUDE_hive where imei like '1AA10%'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_307")

  }


  //Comp_DICTIONARY_EXCLUDE_308
  test("Comp_DICTIONARY_EXCLUDE_308", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_EXCLUDE where imei like '%A10%'""",
      s"""select imei from Comp_DICTIONARY_EXCLUDE_hive where imei like '%A10%'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_308")

  }


  //Comp_DICTIONARY_EXCLUDE_309
  test("Comp_DICTIONARY_EXCLUDE_309", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_EXCLUDE where imei like '%00084'""",
      s"""select imei from Comp_DICTIONARY_EXCLUDE_hive where imei like '%00084'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_309")

  }


  //Comp_DICTIONARY_EXCLUDE_310
  test("Comp_DICTIONARY_EXCLUDE_310", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_EXCLUDE where imei in ('1AA100074','1AA100075','1AA100077')""",
      s"""select imei from Comp_DICTIONARY_EXCLUDE_hive where imei in ('1AA100074','1AA100075','1AA100077')""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_310")

  }


  //Comp_DICTIONARY_EXCLUDE_311
  test("Comp_DICTIONARY_EXCLUDE_311", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_EXCLUDE where imei not in ('1AA100074','1AA100075','1AA100077')""",
      s"""select imei from Comp_DICTIONARY_EXCLUDE_hive where imei not in ('1AA100074','1AA100075','1AA100077')""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_311")

  }


  //Comp_DICTIONARY_EXCLUDE_312
  test("Comp_DICTIONARY_EXCLUDE_312", Include) {

    checkAnswer(s"""select deviceinformationid from Comp_DICTIONARY_EXCLUDE where deviceinformationid in (100081,100078,10008)""",
      s"""select deviceinformationid from Comp_DICTIONARY_EXCLUDE_hive where deviceinformationid in (100081,100078,10008)""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_312")

  }


  //Comp_DICTIONARY_EXCLUDE_313
  test("Comp_DICTIONARY_EXCLUDE_313", Include) {

    checkAnswer(s"""select deviceinformationid from Comp_DICTIONARY_EXCLUDE where deviceinformationid not in (100081,100078,10008)""",
      s"""select deviceinformationid from Comp_DICTIONARY_EXCLUDE_hive where deviceinformationid not in (100081,100078,10008)""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_313")

  }


  //Comp_DICTIONARY_EXCLUDE_314
  test("Comp_DICTIONARY_EXCLUDE_314", Include) {

    checkAnswer(s"""select productiondate from Comp_DICTIONARY_EXCLUDE where productiondate in ('2015-10-04 12:07:28','2015-10-07%','2015-10-07 12:07:28')""",
      s"""select productiondate from Comp_DICTIONARY_EXCLUDE_hive where productiondate in ('2015-10-04 12:07:28','2015-10-07%','2015-10-07 12:07:28')""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_314")

  }


  //Comp_DICTIONARY_EXCLUDE_315
  test("Comp_DICTIONARY_EXCLUDE_315", Include) {

    checkAnswer(s"""select productiondate from Comp_DICTIONARY_EXCLUDE where productiondate not in (cast('2015-10-04 12:07:28' as timestamp),cast('2015-10-07 12:07:28' as timestamp))""",
      s"""select productiondate from Comp_DICTIONARY_EXCLUDE_hive where productiondate not in (cast('2015-10-04 12:07:28' as timestamp),cast('2015-10-07 12:07:28' as timestamp))""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_315")

  }


  //Comp_DICTIONARY_EXCLUDE_316
  test("Comp_DICTIONARY_EXCLUDE_316", Include) {

    checkAnswer(s"""select gamepointid from Comp_DICTIONARY_EXCLUDE where gamepointid in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""",
      s"""select gamepointid from Comp_DICTIONARY_EXCLUDE_hive where gamepointid in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_316")

  }


  //Comp_DICTIONARY_EXCLUDE_317
  test("Comp_DICTIONARY_EXCLUDE_317", Include) {

    checkAnswer(s"""select gamepointid from Comp_DICTIONARY_EXCLUDE where gamepointid not in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""",
      s"""select gamepointid from Comp_DICTIONARY_EXCLUDE_hive where gamepointid not in (5.02870412391492E39,3.82247669125491E41,6.8591561117512E42)""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_317")

  }


  //Comp_DICTIONARY_EXCLUDE_318
  test("Comp_DICTIONARY_EXCLUDE_318", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE where Latest_DAY in (1234567890123530.0000000000,1234567890123520.0000000000)""",
      s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE_hive where Latest_DAY in (1234567890123530.0000000000,1234567890123520.0000000000)""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_318")

  }


  //Comp_DICTIONARY_EXCLUDE_319
  test("Comp_DICTIONARY_EXCLUDE_319", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE where Latest_DAY not in (1234567890123530.0000000000,1234567890123520.0000000000)""",
      s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE_hive where Latest_DAY not in (1234567890123530.0000000000,1234567890123520.0000000000)""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_319")

  }


  //Comp_DICTIONARY_EXCLUDE_322
  test("Comp_DICTIONARY_EXCLUDE_322", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_EXCLUDE where imei !='1AA100077'""",
      s"""select imei from Comp_DICTIONARY_EXCLUDE_hive where imei !='1AA100077'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_322")

  }


  //Comp_DICTIONARY_EXCLUDE_323
  test("Comp_DICTIONARY_EXCLUDE_323", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_EXCLUDE where imei NOT LIKE '1AA100077'""",
      s"""select imei from Comp_DICTIONARY_EXCLUDE_hive where imei NOT LIKE '1AA100077'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_323")

  }


  //Comp_DICTIONARY_EXCLUDE_324
  test("Comp_DICTIONARY_EXCLUDE_324", Include) {

    checkAnswer(s"""select deviceinformationid from Comp_DICTIONARY_EXCLUDE where deviceinformationid !=100078""",
      s"""select deviceinformationid from Comp_DICTIONARY_EXCLUDE_hive where deviceinformationid !=100078""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_324")

  }


  //Comp_DICTIONARY_EXCLUDE_325
  test("Comp_DICTIONARY_EXCLUDE_325", Include) {

    checkAnswer(s"""select deviceinformationid from Comp_DICTIONARY_EXCLUDE where deviceinformationid NOT LIKE 100079""",
      s"""select deviceinformationid from Comp_DICTIONARY_EXCLUDE_hive where deviceinformationid NOT LIKE 100079""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_325")

  }


  //Comp_DICTIONARY_EXCLUDE_326
  test("Comp_DICTIONARY_EXCLUDE_326", Include) {

    checkAnswer(s"""select productiondate from Comp_DICTIONARY_EXCLUDE where productiondate !='2015-10-07 12:07:28'""",
      s"""select productiondate from Comp_DICTIONARY_EXCLUDE_hive where productiondate !='2015-10-07 12:07:28'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_326")

  }


  //Comp_DICTIONARY_EXCLUDE_327
  ignore("Comp_DICTIONARY_EXCLUDE_327", Include) {

    checkAnswer(s"""select productiondate from Comp_DICTIONARY_EXCLUDE where productiondate NOT LIKE '2015-10-07 12:07:28'""",
      s"""select productiondate from Comp_DICTIONARY_EXCLUDE_hive where productiondate NOT LIKE '2015-10-07 12:07:28'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_327")

  }


  //Comp_DICTIONARY_EXCLUDE_328
  test("Comp_DICTIONARY_EXCLUDE_328", Include) {

    checkAnswer(s"""select gamepointid from Comp_DICTIONARY_EXCLUDE where gamepointid !=6.8591561117512E42""",
      s"""select gamepointid from Comp_DICTIONARY_EXCLUDE_hive where gamepointid !=6.8591561117512E42""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_328")

  }


  //Comp_DICTIONARY_EXCLUDE_329
  test("Comp_DICTIONARY_EXCLUDE_329", Include) {

    checkAnswer(s"""select gamepointid from Comp_DICTIONARY_EXCLUDE where gamepointid NOT LIKE 6.8591561117512E43""",
      s"""select gamepointid from Comp_DICTIONARY_EXCLUDE_hive where gamepointid NOT LIKE 6.8591561117512E43""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_329")

  }


  //Comp_DICTIONARY_EXCLUDE_330
  test("Comp_DICTIONARY_EXCLUDE_330", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE where Latest_DAY != 1234567890123520.0000000000""",
      s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE_hive where Latest_DAY != 1234567890123520.0000000000""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_330")

  }


  //Comp_DICTIONARY_EXCLUDE_331
  test("Comp_DICTIONARY_EXCLUDE_331", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE where Latest_DAY NOT LIKE 1234567890123520.0000000000""",
      s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE_hive where Latest_DAY NOT LIKE 1234567890123520.0000000000""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_331")

  }


  //Comp_DICTIONARY_EXCLUDE_335
  test("Comp_DICTIONARY_EXCLUDE_335", Include) {

    checkAnswer(s"""SELECT productiondate,IMEI from Comp_DICTIONARY_EXCLUDE where IMEI RLIKE '1AA100077'""",
      s"""SELECT productiondate,IMEI from Comp_DICTIONARY_EXCLUDE_hive where IMEI RLIKE '1AA100077'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_335")

  }


  //Comp_DICTIONARY_EXCLUDE_336
  test("Comp_DICTIONARY_EXCLUDE_336", Include) {

    checkAnswer(s"""SELECT deviceinformationId from Comp_DICTIONARY_EXCLUDE where deviceinformationId RLIKE '100079'""",
      s"""SELECT deviceinformationId from Comp_DICTIONARY_EXCLUDE_hive where deviceinformationId RLIKE '100079'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_336")

  }


  //Comp_DICTIONARY_EXCLUDE_337
  test("Comp_DICTIONARY_EXCLUDE_337", Include) {

    checkAnswer(s"""SELECT gamepointid from Comp_DICTIONARY_EXCLUDE where gamepointid RLIKE '1.61922711065643E42'""",
      s"""SELECT gamepointid from Comp_DICTIONARY_EXCLUDE_hive where gamepointid RLIKE '1.61922711065643E42'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_337")

  }


  //Comp_DICTIONARY_EXCLUDE_338
  test("Comp_DICTIONARY_EXCLUDE_338", Include) {

    checkAnswer(s"""SELECT Latest_Day from Comp_DICTIONARY_EXCLUDE where Latest_Day RLIKE '1234567890123550.0000000000'""",
      s"""SELECT Latest_Day from Comp_DICTIONARY_EXCLUDE_hive where Latest_Day RLIKE '1234567890123550.0000000000'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_338")

  }


  //Comp_DICTIONARY_EXCLUDE_339
  test("Comp_DICTIONARY_EXCLUDE_339", Include) {

    checkAnswer(s"""SELECT contractnumber from Comp_DICTIONARY_EXCLUDE where contractnumber RLIKE '9223372047800'""",
      s"""SELECT contractnumber from Comp_DICTIONARY_EXCLUDE_hive where contractnumber RLIKE '9223372047800'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_339")

  }


  //Comp_DICTIONARY_EXCLUDE_340
  test("Comp_DICTIONARY_EXCLUDE_340", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_EXCLUDE a join Comp_DICTIONARY_EXCLUDE b on a.productiondate=b.productiondate""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_EXCLUDE_hive a join Comp_DICTIONARY_EXCLUDE_hive b on a.productiondate=b.productiondate""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_340")

  }


  //Comp_DICTIONARY_EXCLUDE_341
  test("Comp_DICTIONARY_EXCLUDE_341", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_EXCLUDE a join Comp_DICTIONARY_EXCLUDE b on a.deviceinformationid=b.deviceinformationid""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_EXCLUDE_hive a join Comp_DICTIONARY_EXCLUDE_hive b on a.deviceinformationid=b.deviceinformationid""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_341")

  }


  //Comp_DICTIONARY_EXCLUDE_342
  test("Comp_DICTIONARY_EXCLUDE_342", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_EXCLUDE a join Comp_DICTIONARY_EXCLUDE b on a.imei=b.imei""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_EXCLUDE_hive a join Comp_DICTIONARY_EXCLUDE_hive b on a.imei=b.imei""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_342")

  }


  //Comp_DICTIONARY_EXCLUDE_343
  test("Comp_DICTIONARY_EXCLUDE_343", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_EXCLUDE a join Comp_DICTIONARY_EXCLUDE b on a.gamepointid=b.gamepointid""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_EXCLUDE_hive a join Comp_DICTIONARY_EXCLUDE_hive b on a.gamepointid=b.gamepointid""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_343")

  }


  //Comp_DICTIONARY_EXCLUDE_344
  test("Comp_DICTIONARY_EXCLUDE_344", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_EXCLUDE a join Comp_DICTIONARY_EXCLUDE b on a.Latest_Day=b.Latest_Day""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_EXCLUDE_hive a join Comp_DICTIONARY_EXCLUDE_hive b on a.Latest_Day=b.Latest_Day""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_344")

  }


  //Comp_DICTIONARY_EXCLUDE_345
  test("Comp_DICTIONARY_EXCLUDE_345", Include) {

    checkAnswer(s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_EXCLUDE a join Comp_DICTIONARY_EXCLUDE b on a.contractnumber=b.contractnumber""",
      s"""select  b.contractNumber,b.Latest_DAY,b.gamePointId,b.productionDate,b.deviceInformationId,b.IMEI from Comp_DICTIONARY_EXCLUDE_hive a join Comp_DICTIONARY_EXCLUDE_hive b on a.contractnumber=b.contractnumber""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_345")

  }


  //Comp_DICTIONARY_EXCLUDE_346
  test("Comp_DICTIONARY_EXCLUDE_346", Include) {

    checkAnswer(s"""select count( contractNumber ),sum( contractNumber ),count(distinct contractNumber ),avg( contractNumber ),max( contractNumber ),min( contractNumber ),1 from Comp_DICTIONARY_EXCLUDE""",
      s"""select count( contractNumber ),sum( contractNumber ),count(distinct contractNumber ),avg( contractNumber ),max( contractNumber ),min( contractNumber ),1 from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_346")

  }


  //Comp_DICTIONARY_EXCLUDE_347
  test("Comp_DICTIONARY_EXCLUDE_347", Include) {

    checkAnswer(s"""select count( Latest_Day ),sum( Latest_Day ),count(distinct Latest_Day ),avg( Latest_Day ),max( Latest_Day ),min( Latest_Day ),1 from Comp_DICTIONARY_EXCLUDE""",
      s"""select count( Latest_Day ),sum( Latest_Day ),count(distinct Latest_Day ),avg( Latest_Day ),max( Latest_Day ),min( Latest_Day ),1 from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_347")

  }


  //Comp_DICTIONARY_EXCLUDE_348
  test("Comp_DICTIONARY_EXCLUDE_348", Include) {

    sql(s"""select count( gamePointId),sum( gamePointId ),count(distinct gamePointId ),avg(gamePointId),max(gamePointId),min(gamePointId),1 from Comp_DICTIONARY_EXCLUDE""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_349
  test("Comp_DICTIONARY_EXCLUDE_349", Include) {

    checkAnswer(s"""select count(deviceInformationId),sum(deviceInformationId),count(deviceInformationId),avg(deviceInformationId),max(deviceInformationId),min(deviceInformationId),1 from Comp_DICTIONARY_EXCLUDE""",
      s"""select count(deviceInformationId),sum(deviceInformationId),count(deviceInformationId),avg(deviceInformationId),max(deviceInformationId),min(deviceInformationId),1 from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_349")

  }


  //Comp_DICTIONARY_EXCLUDE_350
  test("Comp_DICTIONARY_EXCLUDE_350", Include) {

    checkAnswer(s"""select count( productionDate),sum(  productionDate ),count(distinct  productionDate ),avg(  productionDate ),max(  productionDate),min(  productionDate ),1 from Comp_DICTIONARY_EXCLUDE""",
      s"""select count( productionDate),sum(  productionDate ),count(distinct  productionDate ),avg(  productionDate ),max(  productionDate),min(  productionDate ),1 from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_350")

  }


  //Comp_DICTIONARY_EXCLUDE_351
  test("Comp_DICTIONARY_EXCLUDE_351", Include) {

    checkAnswer(s"""select count(IMEI ),sum(IMEI ),count(distinct IMEI ),avg(IMEI ),max(IMEI ),min(IMEI ),1 from Comp_DICTIONARY_EXCLUDE""",
      s"""select count(IMEI ),sum(IMEI ),count(distinct IMEI ),avg(IMEI ),max(IMEI ),min(IMEI ),1 from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_351")

  }


  //Comp_DICTIONARY_EXCLUDE_352
  test("Comp_DICTIONARY_EXCLUDE_352", Include) {

    checkAnswer(s"""select sum(contractNumber),count(contractNumber),avg(contractNumber),sum(contractNumber)/count(contractNumber) from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(contractNumber),count(contractNumber),avg(contractNumber),sum(contractNumber)/count(contractNumber) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_352")

  }


  //Comp_DICTIONARY_EXCLUDE_353
  test("Comp_DICTIONARY_EXCLUDE_353", Include) {

    checkAnswer(s"""select sum(Latest_Day),count(Latest_Day),avg(Latest_Day),sum(Latest_Day)/count(Latest_Day) from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(Latest_Day),count(Latest_Day),avg(Latest_Day),sum(Latest_Day)/count(Latest_Day) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_353")

  }


  //Comp_DICTIONARY_EXCLUDE_354
  test("Comp_DICTIONARY_EXCLUDE_354", Include) {

    sql(s"""select sum(gamepointId),count(gamepointId),avg(gamepointID),sum(gamepointID)/count(gamepointID) from Comp_DICTIONARY_EXCLUDE""").collect

  }


  //Comp_DICTIONARY_EXCLUDE_355
  test("Comp_DICTIONARY_EXCLUDE_355", Include) {

    checkAnswer(s"""select sum(deviceinformationId),count(deviceinformationId),avg(deviceinformationId),sum(deviceinformationId)/count(deviceinformationId) from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(deviceinformationId),count(deviceinformationId),avg(deviceinformationId),sum(deviceinformationId)/count(deviceinformationId) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_355")

  }


  //Comp_DICTIONARY_EXCLUDE_356
  test("Comp_DICTIONARY_EXCLUDE_356", Include) {

    checkAnswer(s"""select sum(IMEI),count(IMEI),avg(IMEI),sum(IMEI)/count(IMEI) from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(IMEI),count(IMEI),avg(IMEI),sum(IMEI)/count(IMEI) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_356")

  }


  //Comp_DICTIONARY_EXCLUDE_357
  test("Comp_DICTIONARY_EXCLUDE_357", Include) {

    checkAnswer(s"""select sum(productionDate),count(productionDate),avg(productionDate),sum(productionDate)/count(productionDate) from Comp_DICTIONARY_EXCLUDE""",
      s"""select sum(productionDate),count(productionDate),avg(productionDate),sum(productionDate)/count(productionDate) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_357")

  }


  //Comp_DICTIONARY_EXCLUDE_358
  test("Comp_DICTIONARY_EXCLUDE_358", Include) {

    checkAnswer(s"""select contractNumber,Latest_DAY,gamePointId,productionDate,deviceInformationId,IMEI  from Comp_DICTIONARY_EXCLUDE""",
      s"""select contractNumber,Latest_DAY,gamePointId,productionDate,deviceInformationId,IMEI  from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_358")

  }


  //Comp_DICTIONARY_EXCLUDE_359
  test("Comp_DICTIONARY_EXCLUDE_359", Include) {

    checkAnswer(s"""select count(MAC) from Comp_DICTIONARY_EXCLUDE""",
      s"""select count(MAC) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_359")

  }


  //Comp_DICTIONARY_EXCLUDE_360
  test("Comp_DICTIONARY_EXCLUDE_360", Include) {

    checkAnswer(s"""select count(gamePointId) from Comp_DICTIONARY_EXCLUDE""",
      s"""select count(gamePointId) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_360")

  }


  //Comp_DICTIONARY_EXCLUDE_361
  test("Comp_DICTIONARY_EXCLUDE_361", Include) {

    checkAnswer(s"""select count(contractNumber) from Comp_DICTIONARY_EXCLUDE""",
      s"""select count(contractNumber) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_361")

  }


  //Comp_DICTIONARY_EXCLUDE_362
  test("Comp_DICTIONARY_EXCLUDE_362", Include) {

    checkAnswer(s"""select count(Latest_DAY) from Comp_DICTIONARY_EXCLUDE""",
      s"""select count(Latest_DAY) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_362")

  }


  //Comp_DICTIONARY_EXCLUDE_363
  test("Comp_DICTIONARY_EXCLUDE_363", Include) {

    checkAnswer(s"""select count(productionDate) from Comp_DICTIONARY_EXCLUDE""",
      s"""select count(productionDate) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_363")

  }


  //Comp_DICTIONARY_EXCLUDE_364
  test("Comp_DICTIONARY_EXCLUDE_364", Include) {

    checkAnswer(s"""select count(deviceInformationId) from Comp_DICTIONARY_EXCLUDE""",
      s"""select count(deviceInformationId) from Comp_DICTIONARY_EXCLUDE_hive""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_364")

  }


  //Comp_DICTIONARY_EXCLUDE_365
  test("Comp_DICTIONARY_EXCLUDE_365", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_EXCLUDE where  contractNumber  != '9223372047700'""",
      s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_EXCLUDE_hive where  contractNumber  != '9223372047700'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_365")

  }


  //Comp_DICTIONARY_EXCLUDE_366
  test("Comp_DICTIONARY_EXCLUDE_366", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_EXCLUDE where  Latest_DAY  != '1234567890123480.0000000000' order by imei,deviceInformationId,MAC,deviceColor """,
      s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_EXCLUDE_hive where  Latest_DAY  != '1234567890123480.0000000000' order by imei,deviceInformationId,MAC,deviceColor """, "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_366")

  }


  //Comp_DICTIONARY_EXCLUDE_367
  test("Comp_DICTIONARY_EXCLUDE_367", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_EXCLUDE where  gamePointId  != '2.27852521808948E36' order by imei,deviceInformationId,MAC,deviceColor """,
      s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_EXCLUDE_hive where  gamePointId  != '2.27852521808948E36' order by imei,deviceInformationId,MAC,deviceColor """, "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_367")

  }


  //Comp_DICTIONARY_EXCLUDE_368
  test("Comp_DICTIONARY_EXCLUDE_368", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_EXCLUDE where  productionDate  != '2015-09-18 12:07:28.0' order by imei,deviceInformationId,MAC,deviceColor""",
      s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_EXCLUDE_hive where  productionDate  != '2015-09-18 12:07:28.0' order by imei,deviceInformationId,MAC,deviceColor""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_368")

  }


  //Comp_DICTIONARY_EXCLUDE_369
  test("Comp_DICTIONARY_EXCLUDE_369", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_EXCLUDE where  deviceInformationId  != '100075' order by imei,deviceInformationId,MAC,deviceColor """,
      s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_EXCLUDE_hive where  deviceInformationId  != '100075' order by imei,deviceInformationId,MAC,deviceColor """, "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_369")

  }


  //Comp_DICTIONARY_EXCLUDE_370
  test("Comp_DICTIONARY_EXCLUDE_370", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_EXCLUDE where  contractNumber  not like '9223372047700' order by  imei,deviceInformationId,MAC,deviceColor """,
      s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_EXCLUDE_hive where  contractNumber  not like '9223372047700' order by  imei,deviceInformationId,MAC,deviceColor """, "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_370")

  }


  //Comp_DICTIONARY_EXCLUDE_371
  test("Comp_DICTIONARY_EXCLUDE_371", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_EXCLUDE where  Latest_DAY  not like '1234567890123480.0000000000' order by imei,deviceInformationId,MAC,deviceColor""",
      s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_EXCLUDE_hive where  Latest_DAY  not like '1234567890123480.0000000000' order by imei,deviceInformationId,MAC,deviceColor""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_371")

  }


  //Comp_DICTIONARY_EXCLUDE_372
  test("Comp_DICTIONARY_EXCLUDE_372", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_EXCLUDE where  gamePointId  not like '2.27852521808948E36' order by imei,deviceInformationId,MAC,deviceColor""",
      s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_EXCLUDE_hive where  gamePointId  not like '2.27852521808948E36' order by imei,deviceInformationId,MAC,deviceColor""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_372")

  }


  //Comp_DICTIONARY_EXCLUDE_373
  ignore("Comp_DICTIONARY_EXCLUDE_373", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_EXCLUDE where  productionDate  not like cast('2015-09-18 12:07:28.0' as timestamp) order by imei,deviceInformationId,MAC,deviceColor""",
      s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_EXCLUDE_hive where  productionDate  not like cast('2015-09-18 12:07:28.0' as timestamp) order by imei,deviceInformationId,MAC,deviceColor""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_373")

  }


  //Comp_DICTIONARY_EXCLUDE_374
  test("Comp_DICTIONARY_EXCLUDE_374", Include) {

    checkAnswer(s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_EXCLUDE where  deviceInformationId  not like '100075' order by imei,deviceInformationId,MAC,deviceColor""",
      s"""select imei,deviceInformationId,MAC,deviceColor from Comp_DICTIONARY_EXCLUDE_hive where  deviceInformationId  not like '100075' order by imei,deviceInformationId,MAC,deviceColor""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_374")

  }


  //Comp_DICTIONARY_EXCLUDE_375
  test("Comp_DICTIONARY_EXCLUDE_375", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_EXCLUDE where imei is not null""",
      s"""select imei from Comp_DICTIONARY_EXCLUDE_hive where imei is not null""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_375")

  }


  //Comp_DICTIONARY_EXCLUDE_376
  test("Comp_DICTIONARY_EXCLUDE_376", Include) {

    checkAnswer(s"""select gamePointId from Comp_DICTIONARY_EXCLUDE where gamePointId is not null""",
      s"""select gamePointId from Comp_DICTIONARY_EXCLUDE_hive where gamePointId is not null""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_376")

  }


  //Comp_DICTIONARY_EXCLUDE_377
  test("Comp_DICTIONARY_EXCLUDE_377", Include) {

    checkAnswer(s"""select contractNumber from Comp_DICTIONARY_EXCLUDE where contractNumber is not null""",
      s"""select contractNumber from Comp_DICTIONARY_EXCLUDE_hive where contractNumber is not null""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_377")

  }


  //Comp_DICTIONARY_EXCLUDE_378
  test("Comp_DICTIONARY_EXCLUDE_378", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE where Latest_DAY is not null""",
      s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE_hive where Latest_DAY is not null""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_378")

  }


  //Comp_DICTIONARY_EXCLUDE_379
  test("Comp_DICTIONARY_EXCLUDE_379", Include) {

    checkAnswer(s"""select productionDate from Comp_DICTIONARY_EXCLUDE where productionDate is not null""",
      s"""select productionDate from Comp_DICTIONARY_EXCLUDE_hive where productionDate is not null""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_379")

  }


  //Comp_DICTIONARY_EXCLUDE_380
  test("Comp_DICTIONARY_EXCLUDE_380", Include) {

    checkAnswer(s"""select deviceInformationId from Comp_DICTIONARY_EXCLUDE where deviceInformationId is not null""",
      s"""select deviceInformationId from Comp_DICTIONARY_EXCLUDE_hive where deviceInformationId is not null""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_380")

  }


  //Comp_DICTIONARY_EXCLUDE_381
  test("Comp_DICTIONARY_EXCLUDE_381", Include) {

    checkAnswer(s"""select imei from Comp_DICTIONARY_EXCLUDE where imei is  null""",
      s"""select imei from Comp_DICTIONARY_EXCLUDE_hive where imei is  null""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_381")

  }


  //Comp_DICTIONARY_EXCLUDE_382
  test("Comp_DICTIONARY_EXCLUDE_382", Include) {

    checkAnswer(s"""select gamePointId from Comp_DICTIONARY_EXCLUDE where gamePointId is  null""",
      s"""select gamePointId from Comp_DICTIONARY_EXCLUDE_hive where gamePointId is  null""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_382")

  }


  //Comp_DICTIONARY_EXCLUDE_383
  test("Comp_DICTIONARY_EXCLUDE_383", Include) {

    checkAnswer(s"""select contractNumber from Comp_DICTIONARY_EXCLUDE where contractNumber is  null""",
      s"""select contractNumber from Comp_DICTIONARY_EXCLUDE_hive where contractNumber is  null""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_383")

  }


  //Comp_DICTIONARY_EXCLUDE_384
  test("Comp_DICTIONARY_EXCLUDE_384", Include) {

    checkAnswer(s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE where Latest_DAY is  null""",
      s"""select Latest_DAY from Comp_DICTIONARY_EXCLUDE_hive where Latest_DAY is  null""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_384")

  }


  //Comp_DICTIONARY_EXCLUDE_385
  test("Comp_DICTIONARY_EXCLUDE_385", Include) {

    checkAnswer(s"""select productionDate from Comp_DICTIONARY_EXCLUDE where productionDate is  null""",
      s"""select productionDate from Comp_DICTIONARY_EXCLUDE_hive where productionDate is  null""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_385")

  }


  //Comp_DICTIONARY_EXCLUDE_386
  test("Comp_DICTIONARY_EXCLUDE_386", Include) {

    checkAnswer(s"""select deviceInformationId from Comp_DICTIONARY_EXCLUDE where deviceInformationId is  null""",
      s"""select deviceInformationId from Comp_DICTIONARY_EXCLUDE_hive where deviceInformationId is  null""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_386")

  }


  //Comp_DICTIONARY_EXCLUDE_387
  test("Comp_DICTIONARY_EXCLUDE_387", Include) {

    checkAnswer(s"""select count(*) from Comp_DICTIONARY_EXCLUDE where imei = '1AA1'""",
      s"""select count(*) from Comp_DICTIONARY_EXCLUDE_hive where imei = '1AA1'""", "QueriesCompactionTestCase_DICTIONARY_EXCLUDE_387")

  }

  test("Compaction_Bug_JIRA_1422") {

    sql("DROP TABLE IF EXISTS minortest")

    // Create table
    sql(
      s"""
         CREATE TABLE minortest (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES ("TABLE_BLOCKSIZE"= "256 MB")
       """.stripMargin)

    sql(
      s"""
        LOAD DATA inpath '$resourcesPath/Data/uniqdata/2000_UniqData.csv' INTO table minortest OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')
      """.stripMargin).show()

    sql(
      s"""
        LOAD DATA inpath '$resourcesPath/Data/uniqdata/2000_UniqData.csv' INTO table minortest OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')
      """.stripMargin).show()
    sql(
      s"""
        LOAD DATA inpath '$resourcesPath/Data/uniqdata/2000_UniqData.csv' INTO table minortest OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')
      """.stripMargin).show()
    sql(
      s"""
        LOAD DATA inpath '$resourcesPath/Data/uniqdata/2000_UniqData.csv' INTO table minortest OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')
      """.stripMargin).show()

    sql("""alter table minortest compact 'minor'""")
    sql("DROP TABLE IF EXISTS minortest")
  }

  override def afterAll {
  sql("drop table if exists Comp_DICTIONARY_INCLUDE")
  sql("drop table if exists Comp_DICTIONARY_INCLUDE_hive")
  sql("drop table if exists Comp_DICTIONARY_EXCLUDE")
  sql("drop table if exists Comp_DICTIONARY_EXCLUDE_hive")
  }
}