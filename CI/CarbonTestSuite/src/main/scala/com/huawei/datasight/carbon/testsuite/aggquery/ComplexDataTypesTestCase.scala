package com.huawei.datasight.carbon.testsuite.aggquery

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext.sql
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for aggregate query on multiple datatypes
 * @author N00902756
 *
 */
class ComplexDataTypesTestCase extends QueryTest with BeforeAndAfterAll {
  
  import org.apache.spark.sql.common.util.CarbonHiveContext.implicits._
  
  override def beforeAll
  {
	  sql("create cube complextypes dimensions(deviceInformationId integer, channelsId string, ROMSize string, purchasedate string, mobile struct<imei string, imsi string>, MAC array<string>, locationinfo array<struct<ActiveAreaId integer, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string>>, proddate struct<productionDate string,activeDeactivedate array<string>>) measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (deviceInformationId) , PARTITION_COUNT=1] )")
	  sql("LOAD DATA fact from './TestData/complexdata.csv' INTO CUBE complextypes PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"', FILEHEADER 'deviceInformationId,channelsId,ROMSize,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber', COMPLEX_DELIMITER_LEVEL_1 '$', COMPLEX_DELIMITER_LEVEL_2 ':')");
	  sql("create table complexTypeshive(deviceInformationId int, channelsId string, ROMSize string, purchasedate string, mobile struct<imei:string,imsi:string>, MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>, proddate struct<productionDate:string,activeDeactivedate:array<string>>,gamePointId double,contractNumber double) row format delimited fields terminated by ',' collection items terminated by '$' map keys terminated by ':'")
	  sql("load data local inpath './TestData/complexdata.csv' into table complexTypeshive");
  }
  
  test("primitive type aggregation and group by primitive type") 
  {
    checkAnswer(
      sql("select deviceInformationId, count(ROMSize) from complextypes group by deviceInformationId"),
      sql("select deviceInformationId, count(ROMSize) from complexTypeshive group by deviceInformationId"))
  }
  
  test("complex type aggregation and group by primitive type") 
  {
    checkAnswer(
      sql("select channelsId, count(proddate) from complextypes group by channelsId"),
      sql("select channelsId, count(proddate) from complexTypeshive group by channelsId"))
  }
  
  test("primitive type aggregation and group by complex type") 
  {
    checkAnswer(
      sql("select MAC, count(deviceInformationId) from complextypes group by MAC"),
      sql("select MAC, count(deviceInformationId) from complexTypeshive group by MAC"))
  }
  
  test("complex type aggregation and group by complex type") 
  {
    checkAnswer(
      sql("select mobile, count(proddate) from complextypes group by mobile"),
      sql("select mobile, count(proddate) from complexTypeshive group by mobile"))
  }
   
  override def afterAll
  {
	  sql("drop cube complextypes")
	  sql("drop table complexTypeshive")
  }
  
}
