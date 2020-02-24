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

package org.apache.carbondata.integration.spark.testsuite.complexType

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test class of creating and loading for carbon table with double
 *
 */
class TestComplexTypeQuery extends QueryTest with BeforeAndAfterAll {

  var timestampFormat: String = _
  override def beforeAll: Unit = {
    timestampFormat = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
    CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
    CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,
        "force")
    sql("drop table if exists complexcarbontable")
    sql("drop table if exists complexhivetable")
    sql("drop table if exists complex_filter")
    sql("drop table if exists structusingstructCarbon")
    sql("drop table if exists structusingstructHive")
    sql("drop table if exists structusingarraycarbon")
    sql("drop table if exists structusingarrayhive")
    sql("drop table if exists complexcarbonwithspecialchardelimiter")
    sql("drop table if exists complexhivewithspecialchardelimiter")
    sql(
      "create table complexcarbontable(deviceInformationId int, channelsId string, ROMSize " +
      "string, ROMName String, purchasedate string, mobile struct<imei:string, imsi:string>, MAC " +
      "array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, " +
      "ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>, " +
      "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId " +
      "double,contractNumber double)  STORED AS carbondata")
    sql("LOAD DATA local inpath '" + resourcesPath +
        "/complextypesample.csv' INTO table complexcarbontable  OPTIONS('DELIMITER'=',', " +
        "'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId,ROMSize,ROMName," +
        "purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber', " +
        "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')");
    sql(
      "create table complexhivetable(deviceInformationId int, channelsId string, ROMSize string, " +
      "ROMName String, purchasedate string, mobile struct<imei:string, imsi:string>, MAC " +
      "array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, " +
      "ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>, " +
      "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId " +
      "double,contractNumber double)row format delimited fields terminated by ',' collection " +
      "items terminated by '$' map keys terminated by ':'")
    sql(s"LOAD DATA local inpath '$resourcesPath/complextypesample.csv' INTO table " +
        s"complexhivetable")
    sql(
      "create table complex_filter(test1 int, test2 array<String>,test3 array<bigint>,test4 " +
      "array<int>,test5 array<string>,test6 array<timestamp>,test7 array<string>) " +
      "STORED AS carbondata")
    sql("LOAD DATA INPATH '" + resourcesPath +
        "/array1.csv'  INTO TABLE complex_filter options ('DELIMITER'=',', 'QUOTECHAR'='\"', " +
        "'COMPLEX_DELIMITER_LEVEL_1'='$', 'FILEHEADER'= 'test1,test2,test3,test4,test5,test6," +
        "test7')")

    sql(
      "create table structusingarraycarbon (MAC struct<MAC1:array<string>," +
      "ActiveCountry:array<string>>) STORED AS carbondata");
    sql("LOAD DATA local INPATH '" + resourcesPath +
        "/struct_all.csv' INTO table structusingarraycarbon options ('DELIMITER'=',', " +
        "'QUOTECHAR'='\"', 'FILEHEADER'='MAC','COMPLEX_DELIMITER_LEVEL_1'='$'," +
        "'COMPLEX_DELIMITER_LEVEL_2'='&')")
    sql(
      "create table structusingarrayhive (MAC struct<MAC1:array<string>," +
      "ActiveCountry:array<string>>)row format delimited fields terminated by ',' collection " +
      "items terminated by '$' map keys terminated by '&'");
    sql("LOAD DATA local INPATH '" + resourcesPath +
        "/struct_all.csv' INTO table structusingarrayhive")

    sql(
      "create table structusingstructCarbon(name struct<middlename:string, " +
      "othernames:struct<firstname:string,lastname:string>,age:int> ) STORED AS carbondata")
    sql("LOAD DATA local INPATH '" + resourcesPath +
        "/structusingstruct.csv' INTO table structusingstructCarbon options ('DELIMITER'=',', " +
        "'QUOTECHAR'='\"', 'FILEHEADER'='name','COMPLEX_DELIMITER_LEVEL_1'='$'," +
        "'COMPLEX_DELIMITER_LEVEL_2'='&')")
    sql(
      "create table structusingstructhive(name struct<middlename:string, " +
      "othernames:struct<firstname:string,lastname:string>,age:int> )row format delimited fields " +
      "terminated by ',' collection items terminated by '$' map keys terminated by '&'")
    sql("LOAD DATA local INPATH '" + resourcesPath +
        "/structusingstruct.csv' INTO table structusingstructhive")

  }

  test("test for create table with complex type") {
    try {
      sql("drop table if exists carbon_table")
      sql(
        ("CREATE TABLE CARBON_TABLE(stringField string,complexData array<string>) " +
         "STORED AS carbondata")
          .stripMargin)
      assert(true)
    }
    catch {
      case exception: Exception => assert(false)
    }
  }

  test(
    "Test ^ * special character data loading for complex types") {
    sql(
      "create table complexcarbonwithspecialchardelimiter(deviceInformationId int, channelsId " +
      "string, ROMSize string, ROMName String, purchasedate string, mobile struct<imei:string, " +
      "imsi:string>, MAC array<string>, locationinfo array<struct<ActiveAreaId:int, " +
      "ActiveCountry:string, ActiveProvince:string, Activecity:string, ActiveDistrict:string, " +
      "ActiveStreet:string>>, proddate struct<productionDate:string," +
      "activeDeactivedate:array<string>>, gamePointId double,contractNumber double) " +
      "STORED AS carbondata")
    sql("LOAD DATA local inpath '" + resourcesPath +
        "/complextypespecialchardelimiter.csv' INTO table complexcarbonwithspecialchardelimiter  " +
        "OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId," +
        "ROMSize,ROMName,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId," +
        "contractNumber', 'COMPLEX_DELIMITER_LEVEL_1'='^', 'COMPLEX_DELIMITER_LEVEL_2'='*')")
    sql(
      "create table complexhivewithspecialchardelimiter(deviceInformationId int, channelsId " +
      "string, ROMSize string, ROMName String, purchasedate string, mobile struct<imei:string, " +
      "imsi:string>, MAC array<string>, locationinfo array<struct<ActiveAreaId:int, " +
      "ActiveCountry:string, ActiveProvince:string, Activecity:string, ActiveDistrict:string, " +
      "ActiveStreet:string>>, proddate struct<productionDate:string," +
      "activeDeactivedate:array<string>>, gamePointId double,contractNumber double)row format " +
      "delimited fields terminated by ',' collection items terminated by '^' map keys terminated " +
      "by '*'")
    sql("LOAD DATA local inpath '" + resourcesPath +
        "/complextypespecialchardelimiter.csv' INTO table complexhivewithspecialchardelimiter")
    checkAnswer(sql("select * from complexcarbonwithspecialchardelimiter"),
      sql("select * from complexhivewithspecialchardelimiter"))
    sql("drop table if exists complexcarbonwithspecialchardelimiter")
    sql("drop table if exists complexhivewithspecialchardelimiter")
  }

  test("complex filter set1") {
    checkAnswer(
      sql("select test3[1] from complex_filter where test4[1] not like'%1%' order by test1"),
      Seq(Row(5678), Row(1234))
    )
  }
  test("complex filter set2") {
    checkAnswer(
      sql("select test2[0] from complex_filter  where  test3[0] like '%1234%'"),
      Seq(Row("hello"))
    )
  }
  test("select * from structusingarraycarbon") {
    checkAnswer(sql("select * from structusingarraycarbon"),
      sql("select * from structusingarrayhive"))
  }

  test("select * from structusingstructCarbon") {
    checkAnswer(sql("select * from structusingstructCarbon"),
      sql("select * from structusingstructhive"))
  }

  test("select * from complexcarbontable") {
    checkAnswer(sql("select * from complexcarbontable"),
      sql("select * from complexhivetable"))
  }

  test("select mobile, proddate, deviceInformationId  from complexcarbontable") {
    checkAnswer(sql("select mobile, proddate, deviceInformationId  from complexcarbontable"),
      sql("select mobile, proddate, deviceInformationId  from complexhivetable"))
  }

  test("select mobile, MAC, deviceInformationId, purchasedate from complexcarbontable") {
    checkAnswer(sql("select mobile, MAC, deviceInformationId, purchasedate from " +
                    "complexcarbontable"),
      sql("select mobile, MAC, deviceInformationId, purchasedate from complexhivetable"))
  }

  test("select mobile, ROMSize, deviceInformationId from complexcarbontable") {
    checkAnswer(sql("select mobile, ROMSize, deviceInformationId from complexcarbontable"),
      sql("select mobile, ROMSize, deviceInformationId from complexhivetable"))
  }

  test("select locationinfo, purchasedate, deviceInformationId from complexcarbontable") {
    checkAnswer(sql("select locationinfo, purchasedate, deviceInformationId from " +
                    "complexcarbontable"),
      sql("select locationinfo, purchasedate, deviceInformationId from complexhivetable"))
  }
  test("select locationinfo, ROMName, purchasedate, deviceinformationId from complexcarbontable") {
    checkAnswer(sql(
      "select locationinfo, ROMName, purchasedate, deviceinformationId from complexcarbontable"),
      sql("select locationinfo, ROMName, purchasedate, deviceinformationId from complexhivetable"))
  }
  test("select MAC from complexcarbontable where MAC[0] = 'MAC1'") {
    checkAnswer(sql("select MAC from complexcarbontable where MAC[0] = 'MAC1'"),
      sql("select MAC from complexhivetable where MAC[0] = 'MAC1'"))
  }
  test("select mobile from complexcarbontable where mobile.imei like '1AA%'") {
    checkAnswer(sql("select mobile from complexcarbontable where mobile.imei like '1AA%'"),
      sql("select mobile from complexhivetable where mobile.imei like '1AA%'"))
  }


  test(
    "select locationinfo from complexcarbontable where locationinfo[0].ActiveAreaId > 2 AND " +
    "locationinfo[0].ActiveAreaId < 7") {
    checkAnswer(sql(
      "select locationinfo from complexcarbontable where locationinfo[0].ActiveAreaId > 2 AND " +
      "locationinfo[0].ActiveAreaId < 7"),
      sql(
        "select locationinfo from complexhivetable where locationinfo[0].ActiveAreaId > 2 AND " +
        "locationinfo[0].ActiveAreaId < 7"))
  }
  test(
    "select locationinfo from complexcarbontable where locationinfo[0].ActiveAreaId >= 2 AND " +
    "locationinfo[0].ActiveAreaId <= 7") {
    checkAnswer(sql(
      "select locationinfo from complexcarbontable where locationinfo[0].ActiveAreaId >= 2 AND " +
      "locationinfo[0].ActiveAreaId <= 7"),
      sql(
        "select locationinfo from complexhivetable where locationinfo[0].ActiveAreaId >= 2 AND " +
        "locationinfo[0].ActiveAreaId <= 7"))
  }
  test(
    "select locationinfo from complexcarbontable where (locationinfo[0].ActiveAreaId +5 )> 6 AND " +
    "(locationinfo[0].ActiveAreaId+10) < 20") {
    checkAnswer(sql(
      "select locationinfo from complexcarbontable where (locationinfo[0].ActiveAreaId +5 )> 6 " +
      "AND (locationinfo[0].ActiveAreaId+10) < 20"),
      sql(
        "select locationinfo from complexhivetable where (locationinfo[0].ActiveAreaId +5 )> 6 " +
        "AND (locationinfo[0].ActiveAreaId+10) < 20"))
  }
  test("select count(mobile),channelsId from complexcarbontable group by mobile,channelsId") {
    checkAnswer(sql(
      "select count(mobile),channelsId from complexcarbontable group by mobile,channelsId"),
      sql("select count(mobile),channelsId from complexhivetable group by mobile,channelsId"))
  }

  test(
    "select count(mobile),channelsId from complexcarbontable group by mobile,channelsId order by " +
    "channelsId") {
    checkAnswer(sql(
      "select count(mobile),channelsId from complexcarbontable group by mobile,channelsId order " +
      "by channelsId"),
      sql(
        "select count(mobile),channelsId from complexhivetable group by mobile,channelsId order " +
        "by channelsId"))
  }
  test(
    "select count(mobile),channelsId from complexcarbontable group by mobile,channelsId order by " +
    "channelsId limit 10") {
    checkAnswer(sql(
      "select count(mobile),channelsId from complexcarbontable group by mobile,channelsId order " +
      "by channelsId limit 10"),
      sql(
        "select count(mobile),channelsId from complexhivetable group by mobile,channelsId order " +
        "by channelsId limit 10"))
  }
  test(
    "select count(mobile),channelsId from complexcarbontable where MAC[0] = 'MAC1'  group by " +
    "mobile,channelsId order by channelsId limit 10") {
    checkAnswer(sql(
      "select count(mobile),channelsId from complexcarbontable where MAC[0] = 'MAC1'  group by " +
      "mobile,channelsId order by channelsId limit 10"),
      sql(
        "select count(mobile),channelsId from complexhivetable where MAC[0] = 'MAC1'  group by " +
        "mobile,channelsId order by channelsId limit 10"))
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, timestampFormat)
    sql("drop table if exists complexcarbontable")
    sql("drop table if exists complexhivetable")
    sql("drop table if exists structusingstructCarbon")
    sql("drop table if exists structusingstructHive")
    sql("drop table if exists structusingarraycarbon")
    sql("drop table if exists structusingarrayhive")
    sql("drop table if exists complex_filter")
    sql("drop table if exists carbon_table")
  }
}
