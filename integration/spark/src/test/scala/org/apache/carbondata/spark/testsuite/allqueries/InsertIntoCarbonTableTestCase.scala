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

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath

class InsertIntoCarbonTableTestCase extends QueryTest with BeforeAndAfterAll {
  // scalastyle:off lineLength
  var timeStampPropOrig: String = _
  override def beforeAll {
    timeStampPropOrig = CarbonProperties.getInstance().getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)

    sql("drop table if exists THive")
    sql("create table THive (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    sql(s"LOAD DATA local INPATH '$resourcesPath/100_olap.csv' INTO TABLE THive")
    sql("drop table if exists OverwriteTable_t1")
    sql("drop table if exists OverwriteTable_t2")
  }

  test("insert from hive") {
     sql("drop table if exists TCarbon")
     sql("create table TCarbon (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt) STORED AS carbondata")
     checkAnswer(
       sql("insert into TCarbon select * from THive"),
        Seq(Row("0"))
     )
     checkAnswer(
         sql("select imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription from THive order by imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription"),
         sql("select imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription from TCarbon order by imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription")
     )
  }

  test("insert from carbon-select columns") {
     sql("drop table if exists TCarbonSource")
     sql("drop table if exists TCarbon")
     sql("create table TCarbonSource (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt) STORED AS carbondata")
     sql("LOAD DATA INPATH '" + resourcesPath + "/100_olap.csv' INTO table TCarbonSource options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointDescription,gamePointId,contractNumber')")
     sql("create table TCarbon (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt) STORED AS carbondata")
     sql("insert into TCarbon select * from TCarbonSource")
     checkAnswer(
         sql("select imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription from TCarbonSource order by imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription"),
         sql("select imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription from TCarbon order by imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription")
     )
     val result = sql("show segments for table TCarbon").collect()(0).get(1).toString()
     if(!"Success".equalsIgnoreCase(result)) {
       assert(false)
     }

  }

  test("insert from orc-select columns with complex columns having null values and sort scope as global sort") {
    sql("drop table if exists TORCSource")
    sql("drop table if exists TCarbon")
    sql("create table TORCSource(name string,col1 array<String>,col2 map<String,String>,col3 struct<school:string, age:int>,fee int) STORED AS orc")
    sql("insert into TORCSource values('karan',null,null,null,2)")
    sql("create table TCarbon(name string,col1 array<String>,col2 map<String,String>,col3 struct<school:string, age:int>,fee int) STORED AS carbondata TBLPROPERTIES ('SORT_COLUMNS'='name','TABLE_BLOCKSIZE'='128','TABLE_BLOCKLET_SIZE'='128','SORT_SCOPE'='global_SORT')")
    sql("insert overwrite table TCarbon select name,col1,col2,col3,fee from TORCSource")
    checkAnswer(sql("select * from TORCSource"), sql("select * from TCarbon"))
    sql("drop table if exists TORCSource")
    sql("drop table if exists TCarbon")
  }

  test("insert from orc-select columns with complex columns having null values and sort scope as local sort") {
    sql("drop table if exists TORCSource")
    sql("drop table if exists TCarbon")
    sql("create table TORCSource(name string,col1 array<String>,col2 map<String,String>,col3 struct<school:string, age:int>,fee int) STORED AS orc")
    sql("insert into TORCSource values('karan',null,null,null,2)")
    sql("create table TCarbon(name string,col1 array<String>,col2 map<String,String>,col3 struct<school:string, age:int>,fee int) STORED AS carbondata TBLPROPERTIES ('SORT_COLUMNS'='name','TABLE_BLOCKSIZE'='128','TABLE_BLOCKLET_SIZE'='128','SORT_SCOPE'='local_SORT')")
    sql("insert overwrite table TCarbon select name,col1,col2,col3,fee from TORCSource")
    checkAnswer(sql("select * from TORCSource"), sql("select * from TCarbon"))
    sql("drop table if exists TORCSource")
    sql("drop table if exists TCarbon")
  }

  test("insert from orc-select columns with complex columns having null values and sort scope as global sort and bad record handling enabled") {
    val initialPropertyValue = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "true")
    sql("drop table if exists TORCSource")
    sql("drop table if exists TCarbon")
    sql("create table TORCSource(name string,col1 array<String>,col2 map<String,String>,col3 struct<school:string, age:int>,fee int) STORED AS orc")
    sql("insert into TORCSource values('karan',null,null,null,2)")
    sql("create table TCarbon(name string,col1 array<String>,col2 map<String,String>,col3 struct<school:string, age:int>,fee int) STORED AS carbondata TBLPROPERTIES ('SORT_COLUMNS'='name','TABLE_BLOCKSIZE'='128','TABLE_BLOCKLET_SIZE'='128','SORT_SCOPE'='global_SORT')")
    sql("insert overwrite table TCarbon select name,col1,col2,col3,fee from TORCSource")
    checkAnswer(sql("select * from TORCSource"), sql("select * from TCarbon"))
    sql("drop table if exists TORCSource")
    sql("drop table if exists TCarbon")
    if (initialPropertyValue == null) {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT,
          CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT_DEFAULT)
    } else {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT,
          initialPropertyValue)
    }
  }

  test("insert from orc-select columns with complex columns having null values and sort scope as local sort and bad record handling enabled") {
    val initialPropertyValue = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "true")
    sql("drop table if exists TORCSource")
    sql("drop table if exists TCarbon")
    sql("create table TORCSource(name string,col1 array<String>,col2 map<String,String>,col3 struct<school:string, age:int>,fee int) STORED AS orc")
    sql("insert into TORCSource values('karan',null,null,null,2)")
    sql("create table TCarbon(name string,col1 array<String>,col2 map<String,String>,col3 struct<school:string, age:int>,fee int) STORED AS carbondata TBLPROPERTIES ('SORT_COLUMNS'='name','TABLE_BLOCKSIZE'='128','TABLE_BLOCKLET_SIZE'='128','SORT_SCOPE'='local_SORT')")
    sql("insert overwrite table TCarbon select name,col1,col2,col3,fee from TORCSource")
    checkAnswer(sql("select * from TORCSource"), sql("select * from TCarbon"))
    sql("drop table if exists TORCSource")
    sql("drop table if exists TCarbon")
    if (initialPropertyValue == null) {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT,
          CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT_DEFAULT)
    } else {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT,
          initialPropertyValue)
    }
  }

  test("insert from carbon-select * columns") {
     sql("drop table if exists TCarbonSource")
     sql("drop table if exists TCarbon")
     sql("create table TCarbonSource (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt) STORED AS carbondata")
     sql("LOAD DATA INPATH '" + resourcesPath + "/100_olap.csv' INTO table TCarbonSource options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointDescription,gamePointId,contractNumber')")
     sql("create table TCarbon (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt) STORED AS carbondata")
     sql("insert into TCarbon select * from TCarbonSource")
     checkAnswer(
         sql("select * from TCarbonSource"),
         sql("select * from TCarbon")
     )
  }
  test("insert->carbon column is more then hive-fails") {
     sql("drop table if exists TCarbon")
     sql("create table TCarbon (imei string,deviceInformationId int,MAC string,deviceColor string,gamePointId double,contractNumber BigInt) STORED AS carbondata")
    intercept[Exception] {
      sql("insert into TCarbon select imei,deviceInformationId,MAC,deviceColor,gamePointId from THive")
    }
  }
  test("insert->insert wrong data types-pass") {
     sql("drop table if exists TCarbon")
     sql("create table TCarbon (imei string,deviceInformationId int,MAC string) STORED AS carbondata")
     sql("drop table if exists TCarbonLocal")
     sql("create table TCarbonLocal (imei string,deviceInformationId int,MAC string) STORED AS carbondata")
     sql("LOAD DATA INPATH '" + resourcesPath + "/100_olap.csv' INTO table TCarbonLocal options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='imei,MAC,deviceInformationId')")
     sql("insert into TCarbon select imei,MAC,deviceInformationId from THive")
     checkAnswer(
         sql("select imei,deviceInformationId,MAC from TCarbon"),
         sql("select imei,deviceInformationId,MAC from TCarbonLocal")
     )
  }

  test("insert->insert with functions") {
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("DROP TABLE IF EXISTS carbon_table1")
    // Create table
    sql(
      s"""
         | CREATE TABLE carbon_table(
         |    shortField smallint,
         |    intField int,
         |    bigintField bigint,
         |    doubleField double,
         |    stringField string,
         |    timestampField timestamp,
         |    decimalField decimal(18,2),
         |    dateField date,
         |    charField string,
         |    floatField float
         | )
         | STORED AS carbondata
       """.stripMargin)

    sql(
      s"""
         | CREATE TABLE carbon_table1(
         |    shortField smallint,
         |    intField int,
         |    bigintField bigint,
         |    doubleField double,
         |    stringField string,
         |    timestampField timestamp,
         |    decimalField decimal(18,2),
         |    dateField date,
         |    charField string,
         |    floatField float
         | )
         | STORED AS carbondata
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${resourcesPath + "/data_with_all_types.csv"}'
         | INTO TABLE carbon_table
         | options('FILEHEADER'='shortField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField')
       """.stripMargin)

    sql("""insert into table carbon_table1 select shortField,intField,bigintField,doubleField,ASCII(stringField),
                timestampField,decimalField,dateField,charField,floatField from carbon_table
              """)
  }

  test("insert->insert with different names and aliases") {
    sql("DROP TABLE IF EXISTS uniqdata")
    sql("DROP TABLE IF EXISTS student")

    // Create table
    sql(
      s"""
         CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED AS carbondata TBLPROPERTIES ("TABLE_BLOCKSIZE"= "256 MB")
       """.stripMargin)
    sql(s"""CREATE TABLE student (CUST_ID2 int,CUST_ADDR String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED AS carbondata TBLPROPERTIES ("TABLE_BLOCKSIZE"= "256 MB")""")

    sql(s"""LOAD DATA inpath '${resourcesPath + "/data_with_all_types.csv"}' INTO table uniqdata options('DELIMITER'=',', 'FILEHEADER'='CUST_ID, CUST_NAME, ACTIVE_EMUI_VERSION, DOB, DOJ, BIGINT_COLUMN1, BIGINT_COLUMN2, DECIMAL_COLUMN1, DECIMAL_COLUMN2, Double_COLUMN1, Double_COLUMN2, INTEGER_COLUMN1')""")
    sql("""insert into student select * from uniqdata""")
  }

  test("insert into existing load-pass") {
     sql("drop table if exists TCarbon")
     sql("create table TCarbon (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt) STORED AS carbondata")
     sql("LOAD DATA INPATH '" + resourcesPath + "/100_olap.csv' INTO table TCarbon options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointDescription,gamePointId,contractNumber')")
     sql("insert into TCarbon select * from THive")
     sql("LOAD DATA local INPATH '" + resourcesPath + "/100_olap.csv' INTO TABLE THive")
     checkAnswer(
         sql("select imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription from THive order by imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription"),
         sql("select imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription from TCarbon order by imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription")
     )
  }
// TODO This is very unstable test in jenkins CI. Need to fix it.
//  test("insert into carbon table from carbon table union query") {
//    sql("drop table if exists loadtable")
//    sql("drop table if exists insertTable")
//    sql("create table loadtable (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt) STORED AS carbondata")
//    sql("LOAD DATA INPATH '" + resourcesPath + "/100_olap.csv' INTO table loadtable options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointDescription,gamePointId,contractNumber')")
//    sql("create table insertTable (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt) STORED AS carbondata")
//    sql("insert into insertTable select * from loadtable union select * from loadtable ")
//    checkAnswer(
//      sql("select imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription from loadtable"),
//      sql("select imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription from insertTable")
//    )
//  }

  test("insert select from same table") {
    sql("drop table if exists CarbonDest")
    sql("create table CarbonDest (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt) STORED AS carbondata")

    sql("drop table if exists HiveDest")
    sql("create table HiveDest (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")

    sql("insert into CarbonDest select * from THive")
    sql("insert into CarbonDest select * from CarbonDest")

    sql("insert into HiveDest select * from THive")
    sql("insert into HiveDest select * from HiveDest")

    checkAnswer(
      sql("select imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription from HiveDest order by imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription"),
      sql("select imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription from CarbonDest order by imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription")
    )
  }

  test("insert overwrite") {
    sql("drop table if exists CarbonOverwrite")
    sql("create table CarbonOverwrite (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt) STORED AS carbondata")

    sql("drop table if exists HiveOverwrite")
    sql("create table HiveOverwrite (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")

    sql("insert into CarbonOverwrite select * from THive")
    sql("insert into CarbonOverwrite select * from THive")
    sql("insert into HiveOverwrite select * from THive")
    sql("insert into HiveOverwrite select * from THive")
    checkAnswer(sql("select count(*) from CarbonOverwrite"), sql("select count(*) from HiveOverwrite"))
    sql("insert overwrite table CarbonOverwrite select * from THive")
    sql("insert overwrite table HiveOverwrite select * from THive")
    checkAnswer(sql("select count(*) from CarbonOverwrite"), sql("select count(*) from HiveOverwrite"))
    assert(checkSegment("CarbonOverwrite"))
  }

  test("Load overwrite") {
    sql("drop table if exists TCarbonSourceOverwrite")
    sql("create table TCarbonSourceOverwrite (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt) STORED AS carbondata")

    sql("drop table if exists HiveOverwrite")
    sql("create table HiveOverwrite (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")

    sql("LOAD DATA INPATH '" + resourcesPath + "/100_olap.csv' INTO table TCarbonSourceOverwrite options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointDescription,gamePointId,contractNumber')")
    sql("LOAD DATA INPATH '" + resourcesPath + "/100_olap.csv' INTO table TCarbonSourceOverwrite options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointDescription,gamePointId,contractNumber')")
    sql(s"LOAD DATA local INPATH '$resourcesPath/100_olap.csv' INTO TABLE HiveOverwrite")
    sql(s"LOAD DATA local INPATH '$resourcesPath/100_olap.csv' INTO TABLE HiveOverwrite")
    checkAnswer(sql("select count(*) from TCarbonSourceOverwrite"), sql("select count(*) from HiveOverwrite"))
    sql("LOAD DATA INPATH '" + resourcesPath + "/100_olap.csv' overwrite INTO table TCarbonSourceOverwrite options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointDescription,gamePointId,contractNumber')")
    sql(s"LOAD DATA local INPATH '$resourcesPath/100_olap.csv' overwrite INTO TABLE HiveOverwrite")
    checkAnswer(sql("select count(*) from TCarbonSourceOverwrite"), sql("select count(*) from HiveOverwrite"))
    assert(checkSegment("TCarbonSourceOverwrite"))
  }

  test("Load overwrite fail handle") {
    sql("drop table if exists TCarbonSourceOverwrite")
    sql("create table TCarbonSourceOverwrite (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt) STORED AS carbondata")

    sql("LOAD DATA INPATH '" + resourcesPath + "/100_olap.csv' INTO table TCarbonSourceOverwrite options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointDescription,gamePointId,contractNumber')")
    val rowCount = sql("select imei from TCarbonSourceOverwrite").count()
    try {
      sql("LOAD DATA INPATH '" + resourcesPath +
          "/100_olap.csv' overwrite INTO table TCarbonSourceOverwrite options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointDescription,gamePointId,contractNumber', 'bad_records_action'='fail')")
      assert(false, "Bad records exists and logger action is fail, so it should fail")
    } catch {
      case e: Exception =>
        assert(true)
    }
    sql("LOAD DATA INPATH '" + resourcesPath + "/100_olap.csv' overwrite INTO table TCarbonSourceOverwrite options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointDescription,gamePointId,contractNumber')")
    assert(rowCount == sql("select imei from TCarbonSourceOverwrite").count())

  }

  test("insert overwrite in group by scenario with t1 no record and t2 no record") {
    queryExecution("overwriteTable1_noRecord.csv", "overwriteTable2_noRecord.csv")
    sql ("insert overwrite table OverwriteTable_t2 select id,name,sum(salary) as TotalSalary,'98' as age from OverwriteTable_t1 group by id,name,salary")
    val exists_t1 = checkSegment("OverwriteTable_t1")
    val exists_t2 = checkSegment("OverwriteTable_t2")
    assert(!exists_t1)
    assert(!exists_t2)
    assert(sql("select * from OverwriteTable_t1").count() == sql("select * from OverwriteTable_t2").count())
    checkAnswer(sql("select * from OverwriteTable_t2"),
      Seq())
    checkAnswer(sql("select * from OverwriteTable_t1"),
      sql("select * from OverwriteTable_t2"))
  }


  test("insert overwrite in group by scenario with t1 no record and t2 some record") {
    queryExecution("overwriteTable1_noRecord.csv", "overwriteTable2_someRecord.csv")
    sql ("insert overwrite table OverwriteTable_t2 select id,name,sum(salary) as TotalSalary,'98' as age from OverwriteTable_t1 group by id,name,salary")
    val exists_t1 = checkSegment("OverwriteTable_t1")
    val exists_t2 = checkSegment("OverwriteTable_t2")
    assert(!exists_t1)
    assert(!exists_t2)
    assert(sql("select * from OverwriteTable_t1").count() == sql("select * from OverwriteTable_t2").count())
    checkAnswer(sql("select * from OverwriteTable_t2"),
      Seq())
    checkAnswer(sql("select * from OverwriteTable_t1"),
      sql("select * from OverwriteTable_t2"))
  }

  test("insert overwrite in group by scenario having record in both table") {
    queryExecution("overwriteTable1_someRecord.csv", "overwriteTable2_someRecord.csv")
    sql ("insert overwrite table OverwriteTable_t2 select id,name,sum(salary) as TotalSalary,'98' as age from OverwriteTable_t1 group by id,name,salary")
    val exists_t1 = checkSegment("OverwriteTable_t1")
    val exists_t2 = checkSegment("OverwriteTable_t2")
    assert(exists_t1)
    assert(exists_t2)
    assert(sql("select * from OverwriteTable_t1").count() == sql("select * from OverwriteTable_t2").count())
    checkAnswer(sql("select count(*) from OverwriteTable_t1"), sql("select count(*) from OverwriteTable_t2"))
  }

  test("insert overwrite in group by scenario t1 some record and t2 no record") {
    queryExecution("overwriteTable1_someRecord.csv", "overwriteTable2_noRecord.csv")
    sql("insert overwrite table OverwriteTable_t2 select id,name,sum(salary) as TotalSalary,'98' as age from OverwriteTable_t1 group by id,name,salary")
    val exists_t1 = checkSegment("OverwriteTable_t1")
    val exists_t2 = checkSegment("OverwriteTable_t2")
    assert(exists_t1)
    assert(exists_t2)
    assert(sql("select * from OverwriteTable_t1").count() == sql("select * from OverwriteTable_t2").count())
    checkAnswer(sql("select count(*) from OverwriteTable_t1"), sql("select count(*) from OverwriteTable_t2"))
  }

  test("insert overwrite without group by scenario t1 no record and t2 no record") {
    queryExecution("overwriteTable1_noRecord.csv", "overwriteTable2_noRecord.csv")
    sql ("insert overwrite table OverwriteTable_t2 select id,name,salary as TotalSalary,'98' as age from OverwriteTable_t1")
    val exists_t1 = checkSegment("OverwriteTable_t1")
    val exists_t2 = checkSegment("OverwriteTable_t2")
    assert(!exists_t1)
    assert(!exists_t2)
    assert(sql("select * from OverwriteTable_t1").count() == sql("select * from OverwriteTable_t2").count())
    checkAnswer(sql("select * from OverwriteTable_t2"),
      Seq())
    checkAnswer(sql("select * from OverwriteTable_t1"),
      sql("select * from OverwriteTable_t2"))
  }


  test("insert overwrite without group by scenario with t1 no record and t2 some record") {
    queryExecution("overwriteTable1_noRecord.csv", "overwriteTable2_someRecord.csv")
    sql ("insert overwrite table OverwriteTable_t2 select id,name,salary as TotalSalary,'98' as age from OverwriteTable_t1")
    val exists_t1 = checkSegment("OverwriteTable_t1")
    val exists_t2 = checkSegment("OverwriteTable_t2")
    assert(!exists_t1)
    assert(!exists_t2)
    assert(sql("select * from OverwriteTable_t1").count() == sql("select * from OverwriteTable_t2").count())
    checkAnswer(sql("select * from OverwriteTable_t2"),
      Seq())
    checkAnswer(sql("select * from OverwriteTable_t1"),
      sql("select * from OverwriteTable_t2"))
  }

  test("insert overwrite without group by scenario having record in both table") {
    queryExecution("overwriteTable1_someRecord.csv", "overwriteTable2_someRecord.csv")
    sql ("insert overwrite table OverwriteTable_t2 select id,name,salary as TotalSalary,'98' as age from OverwriteTable_t1")
    val exists_t1 = checkSegment("OverwriteTable_t1")
    val exists_t2 = checkSegment("OverwriteTable_t2")
    assert(exists_t1)
    assert(exists_t2)
    assert(sql("select * from OverwriteTable_t1").count() == sql("select * from OverwriteTable_t2").count())
    checkAnswer(sql("select count(*) from OverwriteTable_t1"), sql("select count(*) from OverwriteTable_t2"))
  }

  private def queryExecution(csvFileName1: String, csvFileName2: String): Unit = {
    sql("drop table if exists OverwriteTable_t1")
    sql("drop table if exists OverwriteTable_t2")
    sql("create table OverwriteTable_t1(id int,name String,salary int) STORED AS carbondata ")
    sql("LOAD DATA INPATH '" + resourcesPath + s"/$csvFileName1' INTO table OverwriteTable_t1")
    sql("create table OverwriteTable_t2(id int,name String,salary int,age String) STORED AS carbondata")
    sql("LOAD DATA INPATH '" + resourcesPath + s"/$csvFileName2' INTO table OverwriteTable_t2")
  }

  private def checkSegment(tableName: String): Boolean = {
    val storePath_t1 = s"$storeLocation/${tableName.toLowerCase()}"
    val detailses = SegmentStatusManager.readTableStatusFile(CarbonTablePath.getTableStatusFilePath(storePath_t1))
    detailses.map(_.getSegmentStatus == SegmentStatus.SUCCESS).exists(f => f)
  }

  test("test show segments after clean files for insert overwrite") {
    sql("drop table if exists show_insert")
    sql("create table show_insert (name String, age int) STORED AS carbondata")
    sql("insert into show_insert select 'abc',1")
    sql("insert into show_insert select 'abc',1")
    sql("insert into show_insert select 'abc',1")
    sql("insert overwrite table show_insert select * from show_insert")
    assert(sql("show segments for table show_insert").collect().length == 4)
     CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    sql("clean files for table show_insert options('force'='true')")
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED)
    assert(sql("show segments for table show_insert").collect().length == 1)
  }

  test("insert and select table column number not equal") {
    sql("DROP TABLE IF EXISTS table1")
    sql("DROP TABLE IF EXISTS table2")
    sql("create table table1 (col1 string, col2 string) partitioned by(pt string) STORED AS carbondata")
    sql("create table table2 (t2_c1 string, t2_c2 string, t2_c3 string) partitioned by(pt string)")
    sql("insert overwrite table table2 partition(pt=20200101) values('v11', 'v12', 'v13')")
    val e = intercept[Exception] {
      sql("insert into table1 select * from table2")
    }
    assert(e.getMessage.contains(
      "requires that the data to be inserted have the same number of columns as the target table"))
  }

  test("test insert into partitioned table with int type to double type") {
    sql("DROP TABLE IF EXISTS table1")
    sql("CREATE TABLE table1 (cnt double) partitioned by (pt string) stored as carbondata")
    sql("insert overwrite table table1 partition(pt='2020') select 10")
    checkAnswer(
      sql("select * from table1"),
      sql("select 10.0, '2020'")
    )
    sql(s"DROP TABLE IF EXISTS table1")
  }

  test("test insert into partitioned table with static partition") {
    sql("DROP TABLE IF EXISTS table1")
    sql("DROP TABLE IF EXISTS select_from")
    sql("CREATE TABLE select_from (i int, b string) stored as carbondata")
    sql("CREATE TABLE table1 (i int) partitioned by (a int, b string) stored as carbondata")
    sql("insert into table select_from select 1, 'a'")
    sql("insert into table table1 partition(a='100',b) select 1, b from select_from")
    checkAnswer(
      sql("select * from table1"),
      sql("select 1, 100, 'a'")
    )
    sql("DROP TABLE IF EXISTS table1")
    sql("DROP TABLE IF EXISTS select_from")
  }

  test("test loading data into partitioned table with segment's updateDeltaEndTimestamp not change") {
    val tableName = "test_partitioned_table"
    sql(s"drop table if exists $tableName")
    sql(s"""
           |create table if not exists $tableName(
           |  id bigint,
           |  name string
           |)
           |STORED AS carbondata
           |partitioned by (dt string)
           |""".stripMargin)
    val carbonTable = CarbonEnv.getCarbonTable(
      Option(CarbonCommonConstants.DATABASE_DEFAULT_NAME), tableName)(sqlContext.sparkSession)
    val dt1 = "dt1"
    sql(s"insert overwrite table $tableName partition(dt='$dt1') select 1, 'a'")

    val dt2 = "dt2"
    sql(s"insert overwrite table $tableName partition(dt='$dt2') select 1, 'a'")
    val dt1Metas = SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)

    assert(dt1Metas.length == 2)
    val dt1Seg1 = dt1Metas(0)
    val dt2Seg1 = dt1Metas(1)

    sql(s"insert overwrite table $tableName partition(dt='$dt2') select 5, 'z'")
    val dt2Metas = SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
    assert(dt2Metas.length == 3)
    val dt2Seg30 = dt2Metas(0)
    val dt2Seg31 = dt2Metas(1)
    val dt2Seg2OverWrite = dt2Metas(2)

    assert(dt1Seg1.getUpdateDeltaEndTimestamp == dt2Seg30.getUpdateDeltaEndTimestamp)
    assert(dt2Seg1.getUpdateDeltaEndTimestamp == dt2Seg31.getUpdateDeltaEndTimestamp)
    assert(dt2Seg31.getUpdateDeltaEndTimestamp != dt2Seg2OverWrite.getUpdateDeltaEndTimestamp)
    sql(s"drop table if exists $tableName")
  }

  override def afterAll {
    sql("drop table if exists load")
    sql("drop table if exists inser")
    sql("DROP TABLE IF EXISTS THive")
    sql("DROP TABLE IF EXISTS TCarbon")
    sql("drop table if exists TCarbonLocal")
    sql("drop table if exists TCarbonSource")
    sql("drop table if exists loadtable")
    sql("drop table if exists insertTable")
    sql("drop table if exists CarbonDest")
    sql("drop table if exists HiveDest")
    sql("drop table if exists CarbonOverwrite")
    sql("drop table if exists HiveOverwrite")
    sql("drop table if exists tcarbonsourceoverwrite")
    sql("drop table if exists carbon_table1")
    sql("drop table if exists carbon_table")
    sql("DROP TABLE IF EXISTS student")
    sql("DROP TABLE IF EXISTS uniqdata")
    sql("DROP TABLE IF EXISTS show_insert")
    sql("drop table if exists OverwriteTable_t1")
    sql("drop table if exists OverwriteTable_t2")
    sql("drop table if exists table1")
    sql("drop table if exists table2")

    if (timeStampPropOrig != null) {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, timeStampPropOrig)
    }
  }
  // scalastyle:on lineLength
}
