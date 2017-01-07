/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.spark.testsuite.allqueries

import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.{Row, SaveMode}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
  * Test Class for all query on multiple datatypes
  *
  */
class AllDataTypesTestCaseAggregate extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    clean

    sql("create table if not exists Carbon_automation_test (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string, gamePointId int,contractNumber int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='Latest_MONTH,Latest_DAY,deviceInformationId')")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("LOAD DATA LOCAL INPATH '" + resourcesPath + "/100_olap.csv' INTO table Carbon_automation_test options('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")
  }

  def clean {
    sql("drop table if exists Carbon_automation_test")
  }
  
  override def afterAll {
    clean
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }


  //TC_113
  test("select percentile_approx(deviceInformationId,0.2) as a  from Carbon_automation_test")({
    checkAnswer(
      sql("select percentile_approx(deviceInformationId,0.2) as a  from Carbon_automation_test"),
      Seq(Row(100005.8)))
  })

  test("CARBONDATA-60-union-defect")({
    sql("drop table if exists carbonunion")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 1000).map(x => (x+"", (x+100)+"")).toDF("c1", "c2")
    df.registerTempTable("sparkunion")
    df.write
      .format("carbondata")
      .mode(SaveMode.Overwrite)
      .option("tableName", "carbonunion")
      .save()
    checkAnswer(
      sql("select c1,count(c1) from (select c1 as c1,c2 as c2 from carbonunion union all select c2 as c1,c1 as c2 from carbonunion)t where c1='200' group by c1"),
      sql("select c1,count(c1) from (select c1 as c1,c2 as c2 from sparkunion union all select c2 as c1,c1 as c2 from sparkunion)t where c1='200' group by c1"))
    sql("drop table if exists carbonunion")
    sql("drop table if exists sparkunion")
  })

}