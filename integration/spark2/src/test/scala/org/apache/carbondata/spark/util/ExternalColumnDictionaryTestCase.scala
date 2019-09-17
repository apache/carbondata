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

import org.apache.spark.sql.common.util.Spark2QueryTest
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.processing.exception.DataLoadingException
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel, CarbonLoadModelBuilder, LoadOption}
import org.apache.carbondata.processing.util.TableOptionConstant

/**
 * test case for external column dictionary generation
 * also support complicated type
 */
class ExternalColumnDictionaryTestCase extends Spark2QueryTest with BeforeAndAfterAll {
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

    filePath = s"${ resourcesPath }/sample.csv"
    complexFilePath1 = s"${ resourcesPath }/complexdata2.csv"
    complexFilePath2 = s"${ resourcesPath }/verticalDelimitedData.csv"
    extColDictFilePath1 = s"deviceInformationId:${ resourcesPath }/deviceInformationId.csv," +
                          s"mobile.imei:${ resourcesPath }/mobileimei.csv," +
                          s"mac:${ resourcesPath }/mac.csv," +
                          s"locationInfo.ActiveCountry:${ resourcesPath
                          }/locationInfoActiveCountry.csv"
    extColDictFilePath2 = s"deviceInformationId:${ resourcesPath }/deviceInformationId2.csv"
    extColDictFilePath3 = s"channelsId:${ resourcesPath }/channelsId.csv"
    header = "deviceInformationId,channelsId,ROMSize,purchasedate,mobile,MAC," +
             "locationinfo,proddate,gamePointId,contractNumber"
    header2 = "deviceInformationId,channelsId,contractNumber"
  }

  def buildTable() = {
    try {
      sql(
        """CREATE TABLE extComplextypes (deviceInformationId int,
     channelsId string, ROMSize string, purchasedate string,
     mobile struct<imei:string, imsi:string>, MAC array<string>,
     locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string,
     ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>,
     proddate struct<productionDate:string,activeDeactivedate:array<string>>,
     gamePointId double,contractNumber double)
     STORED BY 'org.apache.carbondata.format'
     TBLPROPERTIES('DICTIONARY_INCLUDE' = 'deviceInformationId, channelsId')
        """)
    } catch {
      case ex: Throwable => LOGGER.error(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }

    try {
      sql(
        """CREATE TABLE verticalDelimitedTable (deviceInformationId int,
     channelsId string,contractNumber double)
     STORED BY 'org.apache.carbondata.format'
     TBLPROPERTIES('DICTIONARY_INCLUDE' = 'deviceInformationId, channelsId')
        """)
    } catch {
      case ex: Throwable => LOGGER.error(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }

    try {
      sql(
        """CREATE TABLE loadSqlTest (deviceInformationId int,
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
      case ex: Throwable => LOGGER.error(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }
  }

  def buildRelation() = {
    val warehouse = s"$resourcesPath/target/warehouse"
    val storeLocation = s"$resourcesPath/target/store"
    val metaStoreDB = s"$resourcesPath/target"
    CarbonProperties.getInstance()
      .addProperty("carbon.custom.distribution", "true")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,"FORCE")

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonSessionExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.network.timeout", "600s")
      .config("spark.executor.heartbeatInterval", "600s")
      .config("carbon.enable.vector.reader","false")
      .config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")
      .getOrCreate()
    val catalog = CarbonEnv.getInstance(spark).carbonMetaStore
    extComplexRelation = catalog
      .lookupRelation(Option(CarbonCommonConstants.DATABASE_DEFAULT_NAME),
        "extComplextypes")(spark)
      .asInstanceOf[CarbonRelation]
    verticalDelimiteRelation = catalog
      .lookupRelation(Option(CarbonCommonConstants.DATABASE_DEFAULT_NAME),
        "verticalDelimitedTable")(spark)
      .asInstanceOf[CarbonRelation]
    loadSqlRelation = catalog.lookupRelation(Option(CarbonCommonConstants.DATABASE_DEFAULT_NAME),
      "loadSqlTest")(spark)
      .asInstanceOf[CarbonRelation]
  }

  def buildCarbonLoadModel(
      relation: CarbonRelation,
      filePath: String,
      header: String,
      extColFilePath: String,
      csvDelimiter: String = ","): CarbonLoadModel = {
    val carbonLoadModel = new CarbonLoadModel
    carbonLoadModel.setTableName(relation.carbonTable.getDatabaseName)
    carbonLoadModel.setDatabaseName(relation.carbonTable.getTableName)
    val table = relation.carbonTable
    val carbonSchema = new CarbonDataLoadSchema(table)
    carbonLoadModel.setDatabaseName(table.getDatabaseName)
    carbonLoadModel.setTableName(table.getTableName)
    carbonLoadModel.setCarbonTransactionalTable(table.isTransactionalTable)
    carbonLoadModel.setTablePath(relation.carbonTable.getTablePath)
    carbonLoadModel.setCarbonDataLoadSchema(carbonSchema)
    carbonLoadModel.setFactFilePath(filePath)
    carbonLoadModel.setCsvHeader(header)
    carbonLoadModel.setCsvDelimiter(csvDelimiter)
    carbonLoadModel.setComplexDelimiter("$")
    carbonLoadModel.setComplexDelimiter(":")
    carbonLoadModel.setColDictFilePath(extColFilePath)
    carbonLoadModel.setQuoteChar("\"");
    carbonLoadModel.setSerializationNullFormat(
      TableOptionConstant.SERIALIZATION_NULL_FORMAT.getName + ",\\N")
    carbonLoadModel.setDefaultTimestampFormat(CarbonProperties.getInstance().getProperty(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT))
    carbonLoadModel.setDefaultDateFormat(CarbonProperties.getInstance().getProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT))
    carbonLoadModel.setCsvHeaderColumns(
      LoadOption.getCsvHeaderColumns(carbonLoadModel, FileFactory.getConfiguration))
    carbonLoadModel.setMaxColumns("100")
    // Create table and metadata folders if not exist
    val metadataDirectoryPath = CarbonTablePath.getMetadataPath(table.getTablePath)
    val fileType = FileFactory.getFileType(metadataDirectoryPath)
    if (!FileFactory.isFileExist(metadataDirectoryPath, fileType)) {
      FileFactory.mkdirs(metadataDirectoryPath, fileType)
    }
    import scala.collection.JavaConverters._
    val columnCompressor = table.getTableInfo.getFactTable.getTableProperties.asScala
      .getOrElse(CarbonCommonConstants.COMPRESSOR,
        CompressorFactory.getInstance().getCompressor.getName)
    carbonLoadModel.setColumnCompressor(columnCompressor)
    carbonLoadModel
  }

  override def beforeAll {
    cleanAllTables
    buildTestData
    buildTable
    buildRelation
  }

  test("Generate global dictionary from external column file") {
    // load the first time
    var carbonLoadModel = buildCarbonLoadModel(extComplexRelation, complexFilePath1,
      header, extColDictFilePath1)
    GlobalDictionaryUtil.generateGlobalDictionary(
      sqlContext,
      carbonLoadModel,
      FileFactory.getConfiguration)
    // check whether the dictionary is generated
    DictionaryTestCaseUtil.checkDictionary(
      extComplexRelation, "deviceInformationId", "10086")

    // load the second time
    carbonLoadModel = buildCarbonLoadModel(extComplexRelation, complexFilePath1,
      header, extColDictFilePath2)
    GlobalDictionaryUtil.generateGlobalDictionary(
      sqlContext,
      carbonLoadModel,
      FileFactory.getConfiguration)
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
    GlobalDictionaryUtil.generateGlobalDictionary(
      sqlContext,
      carbonLoadModel,
      FileFactory.getConfiguration)
    // check whether the dictionary is generated
    DictionaryTestCaseUtil.checkDictionary(
      extComplexRelation, "channelsId", "1421|")

    //  when csv delimiter is not comma
    carbonLoadModel = buildCarbonLoadModel(verticalDelimiteRelation, complexFilePath2,
      header2, extColDictFilePath3, "|")
    GlobalDictionaryUtil.generateGlobalDictionary(
      sqlContext,
      carbonLoadModel,
      FileFactory.getConfiguration)
    // check whether the dictionary is generated
    DictionaryTestCaseUtil.checkDictionary(
      verticalDelimiteRelation, "channelsId", "1431,")
  }

  test("LOAD DML with COLUMNDICT option") {
    try {
      sql(
        s"""
      LOAD DATA LOCAL INPATH "$complexFilePath1" INTO TABLE loadSqlTest
      OPTIONS('FILEHEADER'='$header', 'COLUMNDICT'='$extColDictFilePath1', 'single_pass'='true')
        """)
    } catch {
      case ex: Exception =>
        LOGGER.error(ex.getMessage + "\r\n" + ex.getStackTraceString)
        assert(false)
    }
    DictionaryTestCaseUtil.checkDictionary(
      loadSqlRelation, "deviceInformationId", "10086")
  }

  test("COLUMNDICT and ALL_DICTIONARY_PATH can not be used together") {
    val ex = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
        LOAD DATA LOCAL INPATH "$complexFilePath1" INTO TABLE loadSqlTest
        OPTIONS('COLUMNDICT'='$extColDictFilePath1',"ALL_DICTIONARY_PATH"='$extColDictFilePath1')
        """)
    }
    assertResult(ex.getMessage)(
      "Error: COLUMNDICT and ALL_DICTIONARY_PATH can not be used together " +
        "in options")
  }

  test("Measure can not use COLUMNDICT") {
    val ex = intercept[DataLoadingException] {
      sql(
        s"""
      LOAD DATA LOCAL INPATH "$complexFilePath1" INTO TABLE loadSqlTest
      OPTIONS('single_pass'='true','FILEHEADER'='$header', 'COLUMNDICT'='gamePointId:$filePath')
      """)
    }
    assertResult(ex.getMessage)(
      "Column gamePointId is not a key column. Only key column can be part " +
        "of dictionary and used in COLUMNDICT option.")
  }

  def cleanAllTables: Unit = {
    sql("DROP TABLE IF EXISTS extComplextypes")
    sql("DROP TABLE IF EXISTS verticalDelimitedTable")
    sql("DROP TABLE IF EXISTS loadSqlTest")
  }

  override def afterAll: Unit = {
    cleanAllTables
  }
}
