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

import org.apache.spark.sql.{CarbonEnv, CarbonRelation}
import org.apache.spark.sql.common.util.CarbonHiveContext
import org.apache.spark.sql.common.util.CarbonHiveContext.sql
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.carbon.CarbonDataLoadSchema
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.processing.constants.TableOptionConstant
import org.apache.carbondata.processing.model.CarbonLoadModel


/**
  * Test Case for org.apache.carbondata.spark.util.GlobalDictionaryUtil
  */
class GlobalDictionaryUtilTestCase extends QueryTest with BeforeAndAfterAll {

  var sampleRelation: CarbonRelation = _
  var dimSampleRelation: CarbonRelation = _
  var complexRelation: CarbonRelation = _
  var incrementalLoadTableRelation: CarbonRelation = _
  var filePath: String = _
  var workDirectory: String = _
  var dimFilePath: String = _
  var complexfilePath: String = _
  var complexfilePath1: String = _
  var complexfilePath2: String = _

  def buildCarbonLoadModel(relation: CarbonRelation,
    filePath: String,
    dimensionFilePath: String,
    header: String): CarbonLoadModel = {
    val carbonLoadModel = new CarbonLoadModel
    carbonLoadModel.setTableName(relation.tableMeta.carbonTableIdentifier.getDatabaseName)
    carbonLoadModel.setDatabaseName(relation.tableMeta.carbonTableIdentifier.getTableName)
    // carbonLoadModel.setSchema(relation.tableMeta.schema)
    val table = relation.tableMeta.carbonTable
    val carbonSchema = new CarbonDataLoadSchema(table)
    carbonLoadModel.setDatabaseName(table.getDatabaseName)
    carbonLoadModel.setTableName(table.getFactTableName)
    carbonLoadModel.setCarbonDataLoadSchema(carbonSchema)
    carbonLoadModel.setFactFilePath(filePath)
    carbonLoadModel.setDimFolderPath(dimensionFilePath)
    carbonLoadModel.setCsvHeader(header)
    carbonLoadModel.setCsvDelimiter(",")
    carbonLoadModel.setComplexDelimiterLevel1("\\$")
    carbonLoadModel.setComplexDelimiterLevel2("\\:")
    carbonLoadModel.setStorePath(relation.tableMeta.storePath)
    carbonLoadModel.setQuoteChar("\"")
    carbonLoadModel.setSerializationNullFormat(
      TableOptionConstant.SERIALIZATION_NULL_FORMAT.getName + ",\\N")
    carbonLoadModel
  }

  override def beforeAll {
    buildTestData
    // second time comment this line
    buildTable
    buildRelation
  }

  def buildTestData() = {
    workDirectory = new File(this.getClass.getResource("/").getPath + "/../../").getCanonicalPath.replace("\\", "/")
    filePath = workDirectory + "/src/test/resources/sample.csv"
    dimFilePath = "dimTableSample:" + workDirectory + "/src/test/resources/dimTableSample.csv"
    complexfilePath1 = workDirectory + "/src/test/resources/complexdata1.csv"
    complexfilePath2 = workDirectory + "/src/test/resources/complexdata2.csv"
    complexfilePath = workDirectory + "/src/test/resources/complexdata.csv"
  }

  def buildTable() = {
    try {
      sql(
        "CREATE TABLE IF NOT EXISTS sample (id STRING, name STRING, city STRING, " +
          "age INT) STORED BY 'org.apache.carbondata.format'"
      )
    } catch {
      case ex: Throwable => LOGGER.error(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }
    try {
      sql(
        "CREATE TABLE IF NOT EXISTS dimSample (id STRING, name STRING, city STRING, " +
          "age INT) STORED BY 'org.apache.carbondata.format'" +
        "TBLPROPERTIES('DICTIONARY_EXCLUDE'='id,name')"
      )
    } catch {
      case ex: Throwable => LOGGER.error(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }
    try {
      sql(
        "create table complextypes (deviceInformationId INT, channelsId string, " +
          "ROMSize string, purchasedate string, mobile struct<imei: string, imsi: string>, MAC " +
          "array<string>, locationinfo array<struct<ActiveAreaId: INT, ActiveCountry: string, " +
          "ActiveProvince: string, Activecity: string, ActiveDistrict: string, ActiveStreet: " +
          "string>>, proddate struct<productionDate: string,activeDeactivedate: array<string>>, " +
          "gamePointId INT,contractNumber INT) STORED BY 'org.apache.carbondata.format'" +
          "TBLPROPERTIES('DICTIONARY_EXCLUDE'='ROMSize')"

      )
    } catch {
      case ex: Throwable => LOGGER.error(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }

    try {
      sql(
        "create table incrementalLoadTable (deviceInformationId INT, channelsId " +
          "string, ROMSize string, purchasedate string, mobile struct<imei: string, imsi: string>, " +
          "MAC array<string>, locationinfo array<struct<ActiveAreaId: INT, ActiveCountry: " +
          "string, ActiveProvince: string, Activecity: string, ActiveDistrict: string, ActiveStreet: " +
          "string>>, proddate struct<productionDate: string,activeDeactivedate: array<string>>, " +
          "gamePointId INT,contractNumber INT) STORED BY 'org.apache.carbondata.format'"+
          "TBLPROPERTIES('DICTIONARY_INCLUDE'='deviceInformationId')"
      )
    } catch {
      case ex: Throwable => LOGGER.error(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }
  }

  def buildRelation() = {
    val catalog = CarbonEnv.get.carbonMetastore
    sampleRelation = catalog.lookupRelation1(Option(CarbonCommonConstants.DATABASE_DEFAULT_NAME),
      "sample")(CarbonHiveContext)
      .asInstanceOf[CarbonRelation]
    dimSampleRelation = catalog
      .lookupRelation1(Option(CarbonCommonConstants.DATABASE_DEFAULT_NAME), "dimSample")(CarbonHiveContext)
      .asInstanceOf[CarbonRelation]
    complexRelation = catalog
      .lookupRelation1(Option(CarbonCommonConstants.DATABASE_DEFAULT_NAME), "complextypes")(CarbonHiveContext)
      .asInstanceOf[CarbonRelation]
    incrementalLoadTableRelation = catalog
      .lookupRelation1(Option(CarbonCommonConstants.DATABASE_DEFAULT_NAME), "incrementalLoadTable")(CarbonHiveContext)
      .asInstanceOf[CarbonRelation]
  }

  test("[issue-80]Global Dictionary Generation") {

    val carbonLoadModel = buildCarbonLoadModel(sampleRelation, filePath, null, null)
    GlobalDictionaryUtil
      .generateGlobalDictionary(CarbonHiveContext,
        carbonLoadModel,
        sampleRelation.tableMeta.storePath
      )

    // test for dimension table
    // TODO - Need to fill and send the dimension table data as per new DimensionRelation in
    // CarbonDataLoadModel
    // carbonLoadModel = buildCarbonLoadModel(dimSampleRelation, filePath, dimFilePath, null)
    // GlobalDictionaryUtil.generateGlobalDictionary(CarbonHiveContext, carbonLoadModel,
    // dimSampleRelation.tableMeta.dataPath, false)
  }

  test("[Issue-190]load csv file without header And support complex type") {
    val header = "deviceInformationId,channelsId,ROMSize,purchasedate,mobile,MAC,locationinfo," +
      "proddate,gamePointId,contractNumber"
    val carbonLoadModel = buildCarbonLoadModel(complexRelation, complexfilePath, null, header)
    GlobalDictionaryUtil
      .generateGlobalDictionary(CarbonHiveContext,
        carbonLoadModel,
        complexRelation.tableMeta.storePath
      )
  }

  test("[Issue-232]Issue in incremental data load for dictionary generation") {
    val header = "deviceInformationId,channelsId,ROMSize,purchasedate,mobile,MAC,locationinfo," +
      "proddate,gamePointId,contractNumber"
    // load 1
    var carbonLoadModel = buildCarbonLoadModel(incrementalLoadTableRelation,
      complexfilePath1,
      null,
      header
    )
    GlobalDictionaryUtil
      .generateGlobalDictionary(CarbonHiveContext,
        carbonLoadModel,
        sampleRelation.tableMeta.storePath
      )
    DictionaryTestCaseUtil.
      checkDictionary(incrementalLoadTableRelation, "deviceInformationId", "100010")

    // load 2
    carbonLoadModel = buildCarbonLoadModel(incrementalLoadTableRelation,
      complexfilePath2,
      null,
      header
    )
    GlobalDictionaryUtil
      .generateGlobalDictionary(CarbonHiveContext,
        carbonLoadModel,
        sampleRelation.tableMeta.storePath
      )
    DictionaryTestCaseUtil.
      checkDictionary(incrementalLoadTableRelation, "deviceInformationId", "100077")
  }

}
