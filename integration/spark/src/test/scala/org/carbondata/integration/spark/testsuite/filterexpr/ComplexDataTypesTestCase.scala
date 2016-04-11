package org.carbondata.integration.spark.testsuite.filterexpr

import org.apache.spark.sql.common.util.CarbonHiveContext.sql
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for aggregate query on multiple datatypes
 * @author N00902756
 *
 */
class ComplexDataTypesTestCase extends QueryTest with BeforeAndAfterAll {

 /* override def beforeAll {
    sql("create cube complextypes dimensions(deviceInformationId integer, channelsId string, ROMSize string, purchasedate string, mobile struct<imei string, imsi string>, MAC array<string>, locationinfo array<struct<ActiveAreaId integer, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string>>, proddate struct<productionDate string,activeDeactivedate array<string>>) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (deviceInformationId) , PARTITION_COUNT=1] )")
    sql("LOAD DATA fact from './src/test/resources/complexdata.csv' INTO CUBE complextypes PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"', FILEHEADER 'deviceInformationId,channelsId,ROMSize,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber', COMPLEX_DELIMITER_LEVEL_1 '$', COMPLEX_DELIMITER_LEVEL_2 ':')");
    sql("create table complexTypeshive(deviceInformationId int, channelsId string, ROMSize string, purchasedate string, mobile struct<imei:string,imsi:string>, MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>, proddate struct<productionDate:string,activeDeactivedate:array<string>>,gamePointId double,contractNumber double) row format delimited fields terminated by ',' collection items terminated by '$' map keys terminated by ':'")
    sql("load data local inpath './src/test/resources/complexdata.csv' into table complexTypeshive");
  }

  test("Filter on primitive & select complex type") {
    checkAnswer(
      sql("select mobile, proddate.activeDeactivedate, MAC[0] from complextypes where channelsId > 2000"),
      sql("select mobile, proddate.activeDeactivedate, MAC[0] from complexTypeshive where channelsId > 2000"))
  }

  test("Filter on complex array type with array_contians") {
    checkAnswer(
      sql("select mobile, proddate.activeDeactivedate, MAC[0] from complextypes where array_contains(MAC, 'MAC1%')"),
      sql("select mobile, proddate.activeDeactivedate, MAC[0] from complexTypeshive where array_contains(MAC, 'MAC1%')"))
  }

  test("Filter on complex array type with index filter") {
    checkAnswer(
      sql("select mobile, proddate.activeDeactivedate, MAC[0] from complextypes where MAC[0] like 'MAC1%'"),
      sql("select mobile, proddate.activeDeactivedate, MAC[0] from complexTypeshive where MAC[0] like 'MAC1%'"))
  }

  test("Filter on complex struct type") {
    checkAnswer(
      sql("select mobile, proddate.activeDeactivedate, MAC[0] from complextypes where mobile.imei = '1AA1' or mobile.imsi = ''"),
      sql("select mobile, proddate.activeDeactivedate, MAC[0] from complexTypeshive where mobile.imei = '1AA1' or mobile.imsi = ''"))
  }

  override def afterAll {
    sql("drop cube complextypes")
    sql("drop table complexTypeshive")
  }
*/
}
