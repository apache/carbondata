package org.apache.carbondata.spark.testsuite.badrecordloger

import java.io.File

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Created by rahul on 26/9/17.
 */
class TestBadRecordForComplexDataType extends QueryTest with BeforeAndAfterAll {
  val rootPath = new File(this.getClass.getResource("/").getPath
                          + "/../..").getCanonicalPath
  val dataPath = s"$resourcesPath/complexdatawithheader.csv"
  override def beforeAll: Unit = {
    sql(s"DROP TABLE IF EXISTS complexTypeTable")
    sql(s"""CREATE TABLE complexTypeTable (
                 deviceInformationId int,
                 channelsId string,
                 ROMSize string,
                 purchasedate string,
                 mobile struct<imei:string,
                              imsi:string>,
                 MAC array<string>,
                 locationinfo array<struct<ActiveAreaId:int,
                                           ActiveCountry:string,
                                           ActiveProvince:string,
                                           Activecity:string,
                                           ActiveDistrict:string,
                                           ActiveStreet:string>>,
                  proddate struct<productionDate: string,
                                 activeDeactivedate: array<string>>,
                  gamePointId double,
                  contractNumber double)
              STORED BY 'org.apache.carbondata.format' """)

    val badrecordpath = s"$rootPath/examples/spark2/target/store"

    sql(s"load data local inpath '$dataPath' into table complexTypeTable " +
        "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
  }

  test("complex data without bad-record enabled"){
    checkAnswer(sql("select count(*) from complexTypeTable"),Seq(Row(9)))
  }

  test("struct with bad record1"){
    sql(s"DROP TABLE IF EXISTS complexTypeTable1")
    sql(s"""CREATE TABLE complexTypeTable1 (
                 deviceInformationId int,
                 channelsId string,
                 ROMSize string,
                 purchasedate string,
                 mobile struct<imei:int,
                              imsi:string>,
                 MAC array<string>,
                 locationinfo array<struct<ActiveAreaId:int,
                                           ActiveCountry:string,
                                           ActiveProvince:string,
                                           Activecity:string,
                                           ActiveDistrict:string,
                                           ActiveStreet:string>>,
                  proddate struct<productionDate: string,
                                 activeDeactivedate: array<string>>,
                  gamePointId double,
                  contractNumber double)
              STORED BY 'org.apache.carbondata.format' """)

    val badrecordpath = s"$rootPath/examples/spark2/target/store"
val caughtYou = intercept[Exception] {
  sql(s"load data local inpath '$dataPath' into table complexTypeTable1 " +
      s"options ('BAD_RECORD_PATH'='$badrecordpath','bad_records_logger_enable'='true'," +
      " 'bad_records_action'='REDIRECT','COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

}
    assert(caughtYou.getMessage.equals("No Data to load"))
  }

  test("struct with bad record2"){
    sql(s"DROP TABLE IF EXISTS complexTypeTable2")
    sql(s"""CREATE TABLE complexTypeTable2 (
                 deviceInformationId int,
                 channelsId string,
                 ROMSize string,
                 purchasedate string,
                 mobile struct<imei:string,
                              imsi:string>,
                 MAC array<string>,
                 locationinfo array<struct<ActiveAreaId:int,
                                           ActiveCountry:int,
                                           ActiveProvince:string,
                                           Activecity:string,
                                           ActiveDistrict:string,
                                           ActiveStreet:string>>,
                  proddate struct<productionDate: string,
                                 activeDeactivedate: array<string>>,
                  gamePointId double,
                  contractNumber double)
              STORED BY 'org.apache.carbondata.format' """)

    val badrecordpath = s"$rootPath/examples/spark2/target/store"
    val caughtYou = intercept[Exception] {
    sql(s"load data local inpath '$dataPath' into table complexTypeTable2 " +
        s"options ('BAD_RECORD_PATH'='$badrecordpath','bad_records_logger_enable'='true'," +
        " 'bad_records_action'='REDIRECT','COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")
    }
    assert(caughtYou.getMessage.equals("No Data to load"))
  }

  test("struct with bad record3"){
    sql(s"DROP TABLE IF EXISTS complexTypeTable3")

    //Table created with proddate struct<productionDate: date .
    //Format is given as dd-MM-YYYY . But CSV contains 2 record with  dd/MM/YYYY format
    sql(s"""CREATE TABLE complexTypeTable3 (
                 deviceInformationId int,
                 channelsId string,
                 ROMSize string,
                 purchasedate string,
                 mobile struct<imei:string,
                              imsi:string>,
                 MAC array<string>,
                 locationinfo array<struct<ActiveAreaId:int,
                                           ActiveCountry:string,
                                           ActiveProvince:string,
                                           Activecity:string,
                                           ActiveDistrict:string,
                                           ActiveStreet:string>>,
                  proddate struct<productionDate: date,
                                 activeDeactivedate: array<string>>,
                  gamePointId double,
                  contractNumber double)
              STORED BY 'org.apache.carbondata.format' """)

    val badrecordpath = s"$rootPath/examples/spark2/target/store"

    sql(s"load data local inpath '$dataPath' into table complexTypeTable3 " +
        s"options ('BAD_RECORD_PATH'='$badrecordpath','bad_records_logger_enable'='true'," +
        " 'bad_records_action'='REDIRECT','COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")
    checkAnswer(sql("select count(*) from complexTypeTable3"),Seq(Row(7)))
  }

  test("struct with bad record4") {
    try{
    sql(s"DROP TABLE IF EXISTS complexTypeTable4")
    sql(
      s"""CREATE TABLE complexTypeTable4 (
                 deviceInformationId int,
                 channelsId string,
                 ROMSize string,
                 purchasedate string,
                 mobile struct<imei:string,
                              imsi:string>,
                 MAC array<string>,
                 locationinfo array<struct<ActiveAreaId:int,
                                           ActiveCountry:string,
                                           ActiveProvince:string,
                                           Activecity:string,
                                           ActiveDistrict:string,
                                           ActiveStreet:string>>,
                  proddate struct<productionDate: date,
                                 activeDeactivedate: array<string>>,
                  gamePointId double,
                  contractNumber double)
              STORED BY 'org.apache.carbondata.format' """)

    val badrecordpath = s"$rootPath/examples/spark2/target/store"
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "dd:MM:YYYY")

    val caughtYou = intercept[Exception] {
      sql(s"load data local inpath '$dataPath' into table complexTypeTable4 " +
          s"options ('BAD_RECORD_PATH'='$badrecordpath','bad_records_logger_enable'='true'," +
          " 'bad_records_action'='REDIRECT','COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")
    }
    assert(caughtYou.getMessage.equals("No Data to load"))
  }finally{
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    }
    }

  test("array with bad record1"){
    sql(s"DROP TABLE IF EXISTS complexTypeTable5")
    sql(s"""CREATE TABLE complexTypeTable5 (
                 deviceInformationId int,
                 channelsId string,
                 ROMSize string,
                 purchasedate string,
                 mobile struct<imei:string,
                              imsi:string>,
                 MAC array<int>,
                 locationinfo array<struct<ActiveAreaId:int,
                                           ActiveCountry:string,
                                           ActiveProvince:string,
                                           Activecity:string,
                                           ActiveDistrict:string,
                                           ActiveStreet:string>>,
                  proddate struct<productionDate: string,
                                 activeDeactivedate: array<string>>,
                  gamePointId double,
                  contractNumber double)
              STORED BY 'org.apache.carbondata.format' """)

    val badrecordpath = s"$rootPath/examples/spark2/target/store"

    val caughtYou = intercept[Exception] {
      sql(s"load data local inpath '$dataPath' into table complexTypeTable5 " +
        s"options ('BAD_RECORD_PATH'='$badrecordpath','bad_records_logger_enable'='true'," +
        " 'bad_records_action'='REDIRECT','COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")
    }
    assert(caughtYou.getMessage.equals("No Data to load"))
  }

  test("array with bad record2"){
    try{
    sql(s"DROP TABLE IF EXISTS complexTypeTable6")
    sql(s"""CREATE TABLE complexTypeTable6 (
                 deviceInformationId int,
                 channelsId string,
                 ROMSize string,
                 purchasedate string,
                 mobile struct<imei:string,
                              imsi:string>,
                 MAC array<string>,
                 locationinfo array<struct<ActiveAreaId:int,
                                           ActiveCountry:string,
                                           ActiveProvince:string,
                                           Activecity:string,
                                           ActiveDistrict:string,
                                           ActiveStreet:string>>,
                  proddate struct<productionDate: string,
                                 activeDeactivedate: array<date>>,
                  gamePointId double,
                  contractNumber double)
              STORED BY 'org.apache.carbondata.format' """)

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "dd/MM/YYYY")

    val badrecordpath = s"$rootPath/examples/spark2/target/store"
    val caughtYou = intercept[Exception] {
      sql(s"load data local inpath '$dataPath' into table complexTypeTable6 " +
          s"options ('BAD_RECORD_PATH'='$badrecordpath','bad_records_logger_enable'='true'," +
          " 'bad_records_action'='REDIRECT','COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")
    }
    assert(caughtYou.getMessage.equals("No Data to load"))
  }finally
  {
    CarbonProperties.getInstance()
  }
  }

  test("array with bad record3"){
    sql(s"DROP TABLE IF EXISTS complexTypeTable7")

    //Table created with  activeDeactivedate: array<date>> .
    //Format is given as dd-MM-YYYY . But CSV contains 1 record with  dd/MM/YYYY format
    sql(s"""CREATE TABLE complexTypeTable7 (
                 deviceInformationId int,
                 channelsId string,
                 ROMSize string,
                 purchasedate string,
                 mobile struct<imei:string,
                              imsi:string>,
                 MAC array<string>,
                 locationinfo array<struct<ActiveAreaId:int,
                                           ActiveCountry:string,
                                           ActiveProvince:string,
                                           Activecity:string,
                                           ActiveDistrict:string,
                                           ActiveStreet:string>>,
                  proddate struct<productionDate: string,
                                 activeDeactivedate: array<date>>,
                  gamePointId double,
                  contractNumber double)
              STORED BY 'org.apache.carbondata.format' """)

    val badrecordpath = s"$rootPath/examples/spark2/target/store"
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "dd-MM-YYYY")

    sql(s"load data local inpath '$dataPath' into table complexTypeTable7 " +
        s"options ('BAD_RECORD_PATH'='$badrecordpath','bad_records_logger_enable'='true'," +
        " 'bad_records_action'='IGNORE','COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")
    checkAnswer(sql("select count(*) from complexTypeTable7"),Seq(Row(8)))
  }

  test("array with bad record Fail 1"){
    try{
      sql(s"DROP TABLE IF EXISTS complexTypeTable8")
      sql(s"""CREATE TABLE complexTypeTable8 (
                 deviceInformationId int,
                 channelsId string,
                 ROMSize string,
                 purchasedate string,
                 mobile struct<imei:string,
                              imsi:string>,
                 MAC array<string>,
                 locationinfo array<struct<ActiveAreaId:int,
                                           ActiveCountry:string,
                                           ActiveProvince:string,
                                           Activecity:string,
                                           ActiveDistrict:string,
                                           ActiveStreet:string>>,
                  proddate struct<productionDate: string,
                                 activeDeactivedate: array<date>>,
                  gamePointId double,
                  contractNumber double)
              STORED BY 'org.apache.carbondata.format' """)

      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "dd/MM/YYYY")

      val badrecordpath = s"$rootPath/examples/spark2/target/store"
      val caughtYou = intercept[Exception] {
        sql(s"load data local inpath '$dataPath' into table complexTypeTable8 " +
            s"options ('BAD_RECORD_PATH'='$badrecordpath','bad_records_logger_enable'='true'," +
            " 'bad_records_action'='FAIL','COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")
      }
      assert(caughtYou.getMessage.contains("Data load failed due to bad record"))
    }finally
    {
      CarbonProperties.getInstance()
    }
  }

  test("struct with bad record Fail 1"){
    sql(s"DROP TABLE IF EXISTS complexTypeTable1")
    sql(s"""CREATE TABLE complexTypeTable1 (
                 deviceInformationId int,
                 channelsId string,
                 ROMSize string,
                 purchasedate string,
                 mobile struct<imei:int,
                              imsi:string>,
                 MAC array<string>,
                 locationinfo array<struct<ActiveAreaId:int,
                                           ActiveCountry:string,
                                           ActiveProvince:string,
                                           Activecity:string,
                                           ActiveDistrict:string,
                                           ActiveStreet:string>>,
                  proddate struct<productionDate: string,
                                 activeDeactivedate: array<string>>,
                  gamePointId double,
                  contractNumber double)
              STORED BY 'org.apache.carbondata.format' """)

    val badrecordpath = s"$rootPath/examples/spark2/target/store"
    val caughtYou = intercept[Exception] {
      sql(s"load data local inpath '$dataPath' into table complexTypeTable1 " +
          s"options ('BAD_RECORD_PATH'='$badrecordpath','bad_records_logger_enable'='true'," +
          " 'bad_records_action'='FAIL','COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    }
    assert(caughtYou.getMessage.equals("Data load failed due to bad record: The value with column name mobile and column data type INT is not a valid INT type.Please enable bad record logger to know the detail reason."))
  }

}
