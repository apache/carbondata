package org.carbondata.integration.spark.testsuite.detailquery

import org.apache.spark.sql.common.util.CarbonHiveContext.sql
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for aggregate query on multiple datatypes
 * @author N00902756
 *
 */
class ComplexDataTypesTestCase extends QueryTest with BeforeAndAfterAll {

  /*override def beforeAll {
    sql("create cube complextypes dimensions(deviceInformationId integer, channelsId string, ROMSize string, purchasedate string, mobile struct<imei string, imsi string>, MAC array<string>, locationinfo array<struct<ActiveAreaId integer, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string>>, proddate struct<productionDate string,activeDeactivedate array<string>>) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (deviceInformationId) , PARTITION_COUNT=1] )")
    sql("LOAD DATA fact from './src/test/resources/complexdata.csv' INTO CUBE complextypes PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"', FILEHEADER 'deviceInformationId,channelsId,ROMSize,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber', COMPLEX_DELIMITER_LEVEL_1 '$', COMPLEX_DELIMITER_LEVEL_2 ':')");
    sql("create table complexTypeshive(deviceInformationId int, channelsId string, ROMSize string, purchasedate string, mobile struct<imei:string,imsi:string>, MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>, proddate struct<productionDate:string,activeDeactivedate:array<string>>,gamePointId double,contractNumber double) row format delimited fields terminated by ',' collection items terminated by '$' map keys terminated by ':'")
    sql("load data local inpath './src/test/resources/complexdata.csv' into table complexTypeshive");
  }

  test("deviceInformationId detailed query") {
    checkAnswer(
      sql("select deviceInformationId from complextypes"),
      sql("select deviceInformationId from complexTypeshive"))
  }

  test("channelsId detailed query") {
    checkAnswer(
      sql("select channelsId from complextypes"),
      sql("select channelsId from complexTypeshive"))
  }

  test("ROMSize detailed query") {
    checkAnswer(
      sql("select ROMSize from complextypes"),
      sql("select ROMSize from complexTypeshive"))
  }

  test("purchasedate detailed query") {
    checkAnswer(
      sql("select purchasedate from complextypes"),
      sql("select purchasedate from complexTypeshive"))
  }

  test("mobile detailed query") {
    checkAnswer(
      sql("select mobile from complextypes"),
      sql("select mobile from complexTypeshive"))
  }

  test("mobile.imei detailed query") {
    checkAnswer(
      sql("select mobile.imei from complextypes"),
      sql("select mobile.imei from complexTypeshive"))
  }

  test("mobile.imsi detailed query") {
    checkAnswer(
      sql("select mobile.imsi from complextypes"),
      sql("select mobile.imsi from complexTypeshive"))
  }

  test("MAC detailed query") {
    checkAnswer(
      sql("select MAC from complextypes"),
      sql("select MAC from complexTypeshive"))
  }

  test("locationinfo detailed query") {
    checkAnswer(
      sql("select locationinfo from complextypes"),
      sql("select locationinfo from complexTypeshive"))
  }

  test("proddate detailed query") {
    checkAnswer(
      sql("select proddate from complextypes"),
      sql("select proddate from complexTypeshive"))
  }

  test("proddate.activeDeactivedate detailed query") {
    checkAnswer(
      sql("select proddate.activeDeactivedate from complextypes"),
      sql("select proddate.activeDeactivedate from complexTypeshive"))
  }

  test("gamePointId,contractNumber detailed query") {
    checkAnswer(
      sql("select gamePointId,contractNumber from complextypes"),
      sql("select gamePointId,contractNumber from complexTypeshive"))
  }

  test("* detailed query") {
    checkAnswer(
      sql("select * from complextypes"),
      sql("select * from complexTypeshive"))
  }

  override def afterAll {
    sql("drop cube complextypes")
    sql("drop table complexTypeshive")
  }*/

}
