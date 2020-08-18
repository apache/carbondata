package org.apache.carbondata.spark.testsuite.secondaryindex

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.spark.testsuite.secondaryindex.TestSecondaryIndexUtils
.isFilterPushedDownToSI;

class TestNotEqualQueryWithSecondaryIndex extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    sql("drop table if exists TCarbon")
    sql("create table TCarbon (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt) STORED AS carbondata")
    sql("LOAD DATA INPATH '" + resourcesPath + "/100_olap.csv' INTO table TCarbon options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointDescription,gamePointId,contractNumber')")
    sql("create index index_on_insert on table TCarbon (deviceColor) AS 'carbondata'")
  }

  test("testing not equal on column with SI") {
    val df1 = sql("select count(*) from TCarbon where deviceColor != 'black'").queryExecution.sparkPlan
    // will not hit SI
    assert(!isFilterPushedDownToSI(df1))
  }

  test("testing  equal to on column with SI") {
    val df1 = sql("select count(*) from TCarbon where deviceColor == 'black'").queryExecution.sparkPlan
    // will hit SI
    assert(isFilterPushedDownToSI(df1))
  }

  test("testing 2 equal to on column with SI") {
    val df1 = sql("select count(*) from TCarbon where deviceColor == 'black' and marketName == 'india'").queryExecution.sparkPlan
    // will hit SI
    assert(isFilterPushedDownToSI(df1))
  }

  test("testing  equal to on column with SI wih another not equal on non SI column") {
    val df1 = sql("select count(*) from TCarbon where deviceColor == 'black' and marketname != 'india'").queryExecution.sparkPlan
    // will hit SI
    assert(isFilterPushedDownToSI(df1))
  }

  test("testing not equal to on column with SI with another equal") {
    val df1 = sql("select count(*) from TCarbon where deviceColor != 'black' and marketName == 'india'").queryExecution.sparkPlan
    // will not hit SI
    assert(!isFilterPushedDownToSI(df1))
  }

  test("testing not equal to on column with SI with another not equal") {
    val df1 = sql("select count(*) from TCarbon where deviceColor != 'black' and marketName != 'india'").queryExecution.sparkPlan
    // will not hit SI
    assert(!isFilterPushedDownToSI(df1))
  }

  override  def afterAll: Unit = {
    sql("DROP INDEX if exists index_on_insert on TCarbon")
    sql("DROP TABLE if exists TCarbon")
  }

}
