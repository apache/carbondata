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
package org.apache.carbondata.spark.testsuite.datetype

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException


class DateTypeTest extends QueryTest with BeforeAndAfterAll{

  override def afterAll: Unit = {
    sql("DROP TABLE IF EXISTS Carbon_automation_testdate")
    sql("DROP TABLE IF EXISTS Carbon_automation_testtimestamp")
  }

  test("must throw exception for date data type in dictionary_exclude") {
    try {
      sql(
        "create table if not exists Carbon_automation_testdate (imei string,doj Date," +
        "deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId " +
        "string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string," +
        "series string,productionDate timestamp,bomCode string,internalModels string, " +
        "deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, " +
        "deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict " +
        "string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, " +
        "ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, " +
        "ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId " +
        "string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber " +
        "string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer " +
        "string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, " +
        "Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY" +
        " int, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province " +
        "string, Latest_city string, Latest_district string, Latest_street string, " +
        "Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, " +
        "Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, " +
        "Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
        "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
        "Latest_operatorId string, gamePointDescription string, gamePointId int,contractNumber " +
        "int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='doj')")

      assert(false)
    }
    catch {
      case exception: MalformedCarbonCommandException => assert(true)
    }
  }
  test("must throw exception for timestamp data type in dictionary_exclude") {
    sql(
      "create table if not exists Carbon_automation_testtimestamp (imei string,doj timestamp," +
      "deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId " +
      "string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string," +
      "series string,productionDate timestamp,bomCode string,internalModels string, " +
      "deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, " +
      "deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict " +
      "string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, " +
      "ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, " +
      "ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId " +
      "string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber " +
      "string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer " +
      "string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, " +
      "Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY" +
      " int, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province " +
      "string, Latest_city string, Latest_district string, Latest_street string, " +
      "Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, " +
      "Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, " +
      "Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
      "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
      "Latest_operatorId string, gamePointDescription string, gamePointId int,contractNumber " +
      "int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='doj')")

    assert(true)
  }
}
