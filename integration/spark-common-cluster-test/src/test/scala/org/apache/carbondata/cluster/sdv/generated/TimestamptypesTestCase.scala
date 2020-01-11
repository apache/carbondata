
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

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util._
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test Class for timestamptypesTestCase to verify all scenerios
 */

class TimestamptypesTestCase extends QueryTest with BeforeAndAfterAll {
         

  //timestamp in yyyy.MMM.dd HH:mm:ss
  test("TimeStampType_001", Include) {
     sql(s""" create table if not exists ddMMMyyyy (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId int,productionDate Timestamp,deliveryDate timestamp,deliverycharge decimal(10,2)) STORED AS carbondata""").collect
   sql(s""" LOAD DATA INPATH '$resourcesPath/Data/vardhandaterestructddMMMyyyy.csv' INTO TABLE ddMMMyyyy OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
    checkAnswer(s"""select count(*) from ddMMMyyyy""",
      Seq(Row(99)), "timestamptypesTestCase_TimeStampType_001")
     sql(s"""drop table ddMMMyyyy""").collect
  }


  //timestamp in dd.MM.yyyy HH:mm:ss
  ignore("TimeStampType_002", Include) {
     sql(s""" create table if not exists ddMMyyyy (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId int,productionDate Timestamp,deliveryDate timestamp,deliverycharge decimal(10,2)) STORED AS carbondata""").collect
   sql(s""" LOAD DATA INPATH '$resourcesPath/Data/vardhandaterestructddMMyyyy.csv' INTO TABLE ddMMyyyy OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
    checkAnswer(s"""select count(*) from ddMMyyyy""",
      Seq(Row(99)), "timestamptypesTestCase_TimeStampType_002")
     sql(s"""drop table ddMMyyyy""").collect
  }


  //timestamp in yyyy.MM.dd HH:mm:ss
  ignore("TimeStampType_003", Include) {
     sql(s""" create table if not exists yyyyMMdd (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId int,productionDate Timestamp,deliveryDate timestamp,deliverycharge decimal(10,2)) STORED AS carbondata""").collect
   sql(s""" LOAD DATA INPATH '$resourcesPath/Data/vardhandaterestructyyyyMMdd.csv' INTO TABLE yyyyMMdd OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
    checkAnswer(s"""select count(*) from yyyyMMdd""",
      Seq(Row(99)), "timestamptypesTestCase_TimeStampType_003")
     sql(s"""drop table yyyyMMdd""").collect
  }


  //timestamp in dd.MMM.yyyy HH:mm:ss
  test("TimeStampType_004", Include) {
     sql(s""" create table if not exists yyyyMMMdd (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId int,productionDate Timestamp,deliveryDate timestamp,deliverycharge decimal(10,2)) STORED AS carbondata""").collect
   sql(s""" LOAD DATA INPATH '$resourcesPath/Data/vardhandaterestructyyyyMMMdd.csv' INTO TABLE yyyyMMMdd OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
    checkAnswer(s"""select count(*) from yyyyMMMdd""",
      Seq(Row(99)), "timestamptypesTestCase_TimeStampType_004")
     sql(s"""drop table yyyyMMMdd""").collect
  }

  val prop = CarbonProperties.getInstance()
  val p1 = prop.getProperty("carbon.timestamp.format", CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)

  override protected def beforeAll() {
    // Adding new properties
    prop.addProperty("carbon.timestamp.format", "yyyy.MM.dd HH:mm:ss")
  }

  override def afterAll: Unit = {
    //Reverting to old
    prop.addProperty("carbon.timestamp.format", p1)
  }

}