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
package org.apache.carbondata.spark.util

import org.apache.spark.sql.common.util.Spark2QueryTest
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel, CarbonLoadModelBuilder, LoadOption}
import org.apache.carbondata.processing.util.TableOptionConstant

/**
  * Test Case for org.apache.carbondata.integration.spark.util.GlobalDictionaryUtil
  */
class AllDictionaryTestCase extends Spark2QueryTest with BeforeAndAfterAll {
  var pwd: String = _
  var sampleRelation: CarbonRelation = _
  var complexRelation: CarbonRelation = _
  var sampleAllDictionaryFile: String = _
  var complexAllDictionaryFile: String = _

  def buildCarbonLoadModel(relation: CarbonRelation,
    filePath: String,
    header: String,
    allDictFilePath: String): CarbonLoadModel = {
    val carbonLoadModel = new CarbonLoadModel
    carbonLoadModel.setTableName(relation.carbonTable.getDatabaseName)
    carbonLoadModel.setDatabaseName(relation.carbonTable.getTableName)
    carbonLoadModel.setTablePath(relation.metaData.carbonTable.getTablePath)
    val table = relation.carbonTable
    val carbonSchema = new CarbonDataLoadSchema(table)
    carbonLoadModel.setDatabaseName(table.getDatabaseName)
    carbonLoadModel.setTableName(table.getTableName)
    carbonLoadModel.setCarbonDataLoadSchema(carbonSchema)
    carbonLoadModel.setFactFilePath(filePath)
    carbonLoadModel.setCsvHeader(header)
    carbonLoadModel.setCsvDelimiter(",")
    carbonLoadModel.setComplexDelimiter("$")
    carbonLoadModel.setComplexDelimiter(":")
    carbonLoadModel.setAllDictPath(allDictFilePath)
    carbonLoadModel.setSerializationNullFormat(
          TableOptionConstant.SERIALIZATION_NULL_FORMAT.getName + ",\\N")
    carbonLoadModel.setDefaultTimestampFormat(CarbonProperties.getInstance().getProperty(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT))
    carbonLoadModel.setCsvHeaderColumns(
      LoadOption.getCsvHeaderColumns(carbonLoadModel, FileFactory.getConfiguration))
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
    sql("drop table if exists sample")
    sql("drop table if exists complextypes")
    sql("drop table if exists tabletest")
    buildTestData
    // second time comment this line
    buildTable
    buildRelation
  }

  def buildTestData() = {
    sampleAllDictionaryFile = s"${resourcesPath}/alldictionary/sample/20160423/1400_1405/*.dictionary"
    complexAllDictionaryFile = s"${resourcesPath}/alldictionary/complex/20160423/1400_1405/*.dictionary"
  }

  def buildTable() = {
    try {
      sql(
        "CREATE TABLE IF NOT EXISTS sample (id STRING, name STRING, city STRING, " +
          "age INT) STORED BY 'org.apache.carbondata.format' " +
          "TBLPROPERTIES('dictionary_include'='city')"
      )
    } catch {
      case ex: Throwable => LOGGER.error(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }
    try {
      sql(
        "create table complextypes (deviceInformationId string, channelsId string, " +
          "ROMSize string, purchasedate string, mobile struct<imei: string, imsi: string>, MAC " +
          "array<string>, locationinfo array<struct<ActiveAreaId: INT, ActiveCountry: string, " +
          "ActiveProvince: string, Activecity: string, ActiveDistrict: string, ActiveStreet: " +
          "string>>, proddate struct<productionDate: string,activeDeactivedate: array<string>>, " +
          "gamePointId INT,contractNumber INT) STORED BY 'org.apache.carbondata.format'" +
          "TBLPROPERTIES('DICTIONARY_EXCLUDE'='ROMSize', 'dictionary_include'='channelsId')"
      )
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
    sampleRelation = catalog.lookupRelation(Option(CarbonCommonConstants.DATABASE_DEFAULT_NAME),
      "sample")(spark).asInstanceOf[CarbonRelation]
    complexRelation = catalog.lookupRelation(Option(CarbonCommonConstants.DATABASE_DEFAULT_NAME),
      "complextypes")(spark).asInstanceOf[CarbonRelation]
  }

  test("Support generate global dictionary from all dictionary files") {
    val header = "id,name,city,age"
    val carbonLoadModel = buildCarbonLoadModel(sampleRelation, null, header, sampleAllDictionaryFile)
    GlobalDictionaryUtil.generateGlobalDictionary(
      sqlContext,
      carbonLoadModel,
      FileFactory.getConfiguration)

    DictionaryTestCaseUtil.
      checkDictionary(sampleRelation, "city", "shenzhen")
  }

  test("Support generate global dictionary from all dictionary files for complex type") {
    val header = "deviceInformationId,channelsId,ROMSize,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber"
    val carbonLoadModel = buildCarbonLoadModel(complexRelation, null, header, complexAllDictionaryFile)
    GlobalDictionaryUtil.generateGlobalDictionary(
      sqlContext,
      carbonLoadModel,
      FileFactory.getConfiguration)

    DictionaryTestCaseUtil.
      checkDictionary(complexRelation, "channelsId", "1650")
  }

  test("test create table thorugh 'using carbondata' and load data") {
    sql(
      "CREATE TABLE tabletest (empno INT, workgroupcategory STRING, deptno INT, projectcode INT, " +
      "attendance INT) USING carbondata")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE tabletest OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='')""".stripMargin)
    checkAnswer(sql("select count(*) from tabletest"), Row(10))
  }

  override def afterAll {
    sql("drop table sample")
    sql("drop table complextypes")
    sql("drop table tabletest")
  }
}
