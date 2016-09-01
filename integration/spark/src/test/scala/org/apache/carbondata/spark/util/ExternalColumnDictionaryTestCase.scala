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
package org.apache.carbondata.spark.util

import java.io.File

import org.apache.carbondata.core.carbon.CarbonDataLoadSchema
import org.apache.carbondata.processing.etl.DataLoadingException
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.load.CarbonLoadModel
import org.apache.spark.sql.{CarbonEnv, CarbonRelation}
import org.apache.spark.sql.common.util.CarbonHiveContext
import org.apache.spark.sql.common.util.CarbonHiveContext.sql
import org.apache.spark.sql.common.util.QueryTest

import org.scalatest.BeforeAndAfterAll

  /**
 * test case for external column dictionary generation
  * also support complicated type
  */
class ExternalColumnDictionaryTestCase extends QueryTest with BeforeAndAfterAll {

  var extComplexRelation: CarbonRelation = _
  var verticalDelimiteRelation: CarbonRelation = _
  var loadSqlRelation: CarbonRelation = _
  var filePath: String = _
  var pwd: String = _
  var complexFilePath1: String = _
  var complexFilePath2: String = _
  var extColDictFilePath1: String = _
  var extColDictFilePath2: String = _
  var extColDictFilePath3: String = _
  var header: String = _
  var header2: String = _

  def buildTestData() = {
    pwd = new File(this.getClass.getResource("/").getPath + "/../../").
        getCanonicalPath.replace("\\", "/")
    filePath = pwd + "/src/test/resources/sample.csv"
    complexFilePath1 = pwd + "/src/test/resources/complexdata2.csv"
    complexFilePath2 = pwd + "/src/test/resources/verticalDelimitedData.csv"
    extColDictFilePath1 = "deviceInformationId:" + pwd +
      "/src/test/resources/deviceInformationId.csv,"
      "mobile.imei:" + pwd + "/src/test/resources/mobileimei.csv,"
      "mac:" + pwd + "/src/test/resources/mac.csv,"
      "locationInfo.ActiveCountry:" + pwd + "/src/test/resources/locationInfoActiveCountry.csv"
    extColDictFilePath2 = "deviceInformationId:" + pwd +
      "/src/test/resources/deviceInformationId2.csv"
    extColDictFilePath3 = "channelsId:" + pwd + "/src/test/resources/channelsId.csv"
    header = "deviceInformationId,channelsId,ROMSize,purchasedate,mobile,MAC," +
      "locationinfo,proddate,gamePointId,contractNumber"
    header2 = "deviceInformationId|channelsId|contractNumber"
  }

  def buildTable() = {
    try {
      sql("""CREATE TABLE extComplextypes (deviceInformationId int,
     channelsId string, ROMSize string, purchasedate string,
     mobile struct<imei:string, imsi:string>, MAC array<string>,
     locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string,
     ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>,
     proddate struct<productionDate:string,activeDeactivedate:array<string>>,
     gamePointId double,contractNumber double)
     STORED BY 'org.apache.carbondata.format'
     TBLPROPERTIES('DICTIONARY_INCLUDE' = 'deviceInformationId')
      """)
    } catch {
      case ex: Throwable => logError(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }

    try {
      sql("""CREATE TABLE verticalDelimitedTable (deviceInformationId int,
     channelsId string,contractNumber double)
     STORED BY 'org.apache.carbondata.format'
     TBLPROPERTIES('DICTIONARY_INCLUDE' = 'deviceInformationId')
      """)
    } catch {
      case ex: Throwable => logError(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }

    try {
      sql("""CREATE TABLE loadSqlTest (deviceInformationId int,
     channelsId string, ROMSize string, purchasedate string,
     mobile struct<imei:string, imsi:string>, MAC array<string>,
     locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string,
     ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>,
     proddate struct<productionDate:string,activeDeactivedate:array<string>>,
     gamePointId double,contractNumber double)
     STORED BY 'org.apache.carbondata.format'
     TBLPROPERTIES('DICTIONARY_INCLUDE' = 'deviceInformationId')
      """)
    } catch {
      case ex: Throwable => logError(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }
  }

  def buildRelation() = {
    val catalog = CarbonEnv.getInstance(CarbonHiveContext).carbonCatalog
    extComplexRelation = catalog.lookupRelation1(Option("default"), "extComplextypes")(CarbonHiveContext)
      .asInstanceOf[CarbonRelation]
    verticalDelimiteRelation = catalog.lookupRelation1(Option("default"), "verticalDelimitedTable")(CarbonHiveContext)
      .asInstanceOf[CarbonRelation]
    loadSqlRelation = catalog.lookupRelation1(Option("default"), "loadSqlTest")(CarbonHiveContext)
      .asInstanceOf[CarbonRelation]
  }

  def buildCarbonLoadModel(relation: CarbonRelation,
      filePath:String,
      header: String,
      extColFilePath: String,
      csvDelimiter: String = ","): CarbonLoadModel = {
    val carbonLoadModel = new CarbonLoadModel
    carbonLoadModel.setTableName(relation.tableMeta.carbonTableIdentifier.getDatabaseName)
    carbonLoadModel.setDatabaseName(relation.tableMeta.carbonTableIdentifier.getTableName)
    val table = relation.tableMeta.carbonTable
    val carbonSchema = new CarbonDataLoadSchema(table)
    carbonLoadModel.setDatabaseName(table.getDatabaseName)
    carbonLoadModel.setTableName(table.getFactTableName)
    carbonLoadModel.setCarbonDataLoadSchema(carbonSchema)
    carbonLoadModel.setFactFilePath(filePath)
    carbonLoadModel.setCsvHeader(header)
    carbonLoadModel.setCsvDelimiter(csvDelimiter)
    carbonLoadModel.setComplexDelimiterLevel1("\\$")
    carbonLoadModel.setComplexDelimiterLevel2("\\:")
    carbonLoadModel.setColDictFilePath(extColFilePath)
    carbonLoadModel.setQuoteChar("\"");
    carbonLoadModel
  }

  override def beforeAll {
    buildTestData
    buildTable
    buildRelation
  }

  test("Generate global dictionary from external column file") {
    // load the first time
    var carbonLoadModel = buildCarbonLoadModel(extComplexRelation, complexFilePath1,
      header, extColDictFilePath1)
    GlobalDictionaryUtil.generateGlobalDictionary(CarbonHiveContext, carbonLoadModel,
      extComplexRelation.tableMeta.storePath)
    // check whether the dictionary is generated
    DictionaryTestCaseUtil.checkDictionary(
      extComplexRelation, "deviceInformationId", "10086")

    // load the second time
    carbonLoadModel = buildCarbonLoadModel(extComplexRelation, complexFilePath1,
      header, extColDictFilePath2)
    GlobalDictionaryUtil.generateGlobalDictionary(CarbonHiveContext, carbonLoadModel,
      extComplexRelation.tableMeta.storePath)
    // check the old dictionary and whether the new distinct value is generated
    DictionaryTestCaseUtil.checkDictionary(
      extComplexRelation, "deviceInformationId", "10086")
    DictionaryTestCaseUtil.checkDictionary(
      extComplexRelation, "deviceInformationId", "10011")
  }

  test("When csv delimiter is not comma") {
    //  when csv delimiter is comma
    var carbonLoadModel = buildCarbonLoadModel(extComplexRelation, complexFilePath1,
      header, extColDictFilePath3)
    GlobalDictionaryUtil.generateGlobalDictionary(CarbonHiveContext, carbonLoadModel,
      extComplexRelation.tableMeta.storePath)
    // check whether the dictionary is generated
    DictionaryTestCaseUtil.checkDictionary(
      extComplexRelation, "channelsId", "1421|")

    //  when csv delimiter is not comma
    carbonLoadModel = buildCarbonLoadModel(verticalDelimiteRelation, complexFilePath2,
      header2, extColDictFilePath3, "|")
    GlobalDictionaryUtil.generateGlobalDictionary(CarbonHiveContext, carbonLoadModel,
      verticalDelimiteRelation.tableMeta.storePath)
    // check whether the dictionary is generated
    DictionaryTestCaseUtil.checkDictionary(
      verticalDelimiteRelation, "channelsId", "1431,")
  }

  test("LOAD DML with COLUMNDICT option") {
    try {
      sql(s"""
      LOAD DATA LOCAL INPATH "$complexFilePath1" INTO TABLE loadSqlTest
      OPTIONS('FILEHEADER'='$header', 'COLUMNDICT'='$extColDictFilePath1')
        """)
    } catch {
      case ex: Exception =>
        logError(ex.getMessage + "\r\n" + ex.getStackTraceString)
        assert(false)
    }
    DictionaryTestCaseUtil.checkDictionary(
      loadSqlRelation, "deviceInformationId", "10086")
  }

  test("COLUMNDICT and ALL_DICTIONARY_PATH can not be used together") {
    try {
      sql(s"""
        LOAD DATA LOCAL INPATH "$complexFilePath1" INTO TABLE loadSqlTest
        OPTIONS('COLUMNDICT'='$extColDictFilePath1',"ALL_DICTIONARY_PATH"='$extColDictFilePath1')
        """)
      assert(false)
    } catch {
      case ex: MalformedCarbonCommandException =>
        assertResult(ex.getMessage)("Error: COLUMNDICT and ALL_DICTIONARY_PATH can not be used together " +
          "in options")
      case _ => assert(false)
    }
  }

  test("Measure can not use COLUMNDICT") {
    try {
      sql(s"""
      LOAD DATA LOCAL INPATH "$complexFilePath1" INTO TABLE loadSqlTest
      OPTIONS('COLUMNDICT'='gamePointId:$filePath')
      """)
      assert(false)
    } catch {
      case ex: DataLoadingException =>
        assertResult(ex.getMessage)("Column gamePointId is not a key column. Only key column can be part " +
          "of dictionary and used in COLUMNDICT option.")
      case _ => assert(false)
    }
  }

  override def afterAll: Unit = {
    sql("DROP TABLE extComplextypes")
    sql("DROP TABLE verticalDelimitedTable")
    sql("DROP TABLE loadSqlTest")
  }
}
