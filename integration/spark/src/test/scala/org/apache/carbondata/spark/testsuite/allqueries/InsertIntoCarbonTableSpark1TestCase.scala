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

import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class InsertIntoCarbonTableSpark1TestCase extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("drop table if exists THive")
    sql("create table THive (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    sql(s"LOAD DATA local INPATH '$resourcesPath/100_olap.csv' INTO TABLE THive")
  }


  test("insert from carbon-select columns-source table has more column then target column") {
    val timeStampPropOrig = CarbonProperties.getInstance().getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT)
     CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
     
     sql("drop table if exists loadTable")
     sql("drop table if exists insertTable")
     sql("CREATE TABLE loadTable(imei string,age int,task bigint,num double,level decimal(10,3),productdate timestamp,name string,point int)STORED BY 'org.apache.carbondata.format'")
     sql("LOAD DATA INPATH '" + resourcesPath + "/shortolap.csv' INTO TABLE loadTable options ('DELIMITER'=',', 'QUOTECHAR'='\"','FILEHEADER' = 'imei,age,task,num,level,productdate,name,point')")
     sql("CREATE TABLE insertTable(imei string,age int,task bigint,num double,level decimal(10,3),productdate timestamp) STORED BY 'org.apache.carbondata.format'")
     sql("insert into insertTable select * from loadTable")
     checkAnswer(
         sql("select * from insertTable"),
         sql("select imei,age,task,num,level,productdate from loadTable")
     ) 
     sql("drop table if exists loadTable")
     sql("drop table if exists insertTable")
     CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, timeStampPropOrig)
  }

  test("insert->hive column more than carbon column->success") {
     sql("drop table if exists TCarbon")
     sql("create table TCarbon (imei string,deviceInformationId int,MAC string,deviceColor string,gamePointId double,contractNumber BigInt) STORED BY 'org.apache.carbondata.format'")
  
     sql("insert into TCarbon select imei,deviceInformationId,MAC,deviceColor,gamePointId,contractNumber,device_backColor,modelId,CUPAudit,CPIClocked from THive")
     checkAnswer(
         sql("select imei,deviceInformationId,MAC,deviceColor,gamePointId,contractNumber from THive"),
         sql("select imei,deviceInformationId,MAC,deviceColor,gamePointId,contractNumber from TCarbon")
     )
  }

//  test("insert->insert empty data -pass") {
//     sql("drop table if exists TCarbon")
//     sql("create table TCarbon (imei string,deviceInformationId int,MAC string) STORED BY 'org.apache.carbondata.format'")
//     sql("insert into TCarbon select imei,deviceInformationId,MAC from THive where MAC='wrongdata'")
//     val result = sql("select imei,deviceInformationId,MAC from TCarbon where MAC='wrongdata'").collect()
//     checkAnswer(
//         sql("select imei,deviceInformationId,MAC from THive where MAC='wrongdata'"),
//         sql("select imei,deviceInformationId,MAC from TCarbon where MAC='wrongdata'")
//     )
//  }

  override def afterAll {
    sql("drop table if exists loadTable")
    sql("drop table if exists insertTable")
    sql("DROP TABLE IF EXISTS THive")
    sql("DROP TABLE IF EXISTS TCarbon")
  }
}
