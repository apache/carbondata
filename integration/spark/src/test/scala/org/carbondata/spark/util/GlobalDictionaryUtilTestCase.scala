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
package org.carbondata.spark.util

import java.io.File

import org.apache.spark.sql.{CarbonEnv, CarbonRelation}
import org.apache.spark.sql.common.util.CarbonHiveContext
import org.apache.spark.sql.common.util.CarbonHiveContext.sql
import org.apache.spark.sql.common.util.QueryTest

import org.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier
import org.carbondata.core.carbon.{CarbonDataLoadSchema, CarbonTableIdentifier}
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.spark.load.CarbonLoadModel
import org.carbondata.spark.load.CarbonLoaderUtil

import org.scalatest.BeforeAndAfterAll

/**
  * Test Case for org.carbondata.spark.util.GlobalDictionaryUtil
  *
  * @date: Apr 10, 2016 10:34:58 PM
  * @See org.carbondata.spark.util.GlobalDictionaryUtil
  */
class GlobalDictionaryUtilTestCase extends QueryTest with BeforeAndAfterAll {

  var sampleRelation: CarbonRelation = _
  var dimSampleRelation: CarbonRelation = _
  var complexRelation: CarbonRelation = _
  var incrementalLoadTableRelation: CarbonRelation = _
  var filePath: String = _
  var pwd: String = _
  var dimFilePath: String = _
  var complexfilePath: String = _
  var complexfilePath1: String = _
  var complexfilePath2: String = _

  def buildCarbonLoadModel(relation: CarbonRelation,
    filePath: String,
    dimensionFilePath: String,
    header: String): CarbonLoadModel = {
    val carbonLoadModel = new CarbonLoadModel
    carbonLoadModel.setTableName(relation.cubeMeta.carbonTableIdentifier.getDatabaseName)
    carbonLoadModel.setDatabaseName(relation.cubeMeta.carbonTableIdentifier.getTableName)
    // carbonLoadModel.setSchema(relation.cubeMeta.schema)
    val table = relation.cubeMeta.carbonTable
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
    carbonLoadModel.setStorePath(relation.cubeMeta.storePath)
    carbonLoadModel
  }

  override def beforeAll {
    buildTestData
    // second time comment this line
    buildTable
    buildRelation
  }

  def buildTestData() = {
    pwd = new File(this.getClass.getResource("/").getPath + "/../../").getCanonicalPath
    filePath = pwd + "/src/test/resources/sample.csv"
    dimFilePath = "dimTableSample:" + pwd + "/src/test/resources/dimTableSample.csv"
    complexfilePath1 = pwd + "/src/test/resources/complexdata1.csv"
    complexfilePath2 = pwd + "/src/test/resources/complexdata2.csv"
    complexfilePath = pwd + "/src/test/resources/complexdata.csv"
  }

  def buildTable() = {
    try {
      sql(
        "CREATE CUBE IF NOT EXISTS sample DIMENSIONS (id STRING, name STRING, city STRING) " +
          "MEASURES (age INTEGER) OPTIONS(PARTITIONER[CLASS='org.carbondata.spark" +
          ".partition.api.impl.SampleDataPartitionerImpl',COLUMNS=(id),PARTITION_COUNT=1])"
      )
    } catch {
      case ex: Throwable => logError(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }
    try {
      sql(
        "CREATE CUBE IF NOT EXISTS dimSample DIMENSIONS (id STRING, name STRING, city STRING) " +
          "MEASURES (age INTEGER) WITH dimTableSample RELATION(Fact.id=id) INCLUDE(id,name) " +
          "OPTIONS(PARTITIONER[CLASS='org.carbondata.spark.partition.api.impl" +
          ".SampleDataPartitionerImpl',COLUMNS=(id),PARTITION_COUNT=1])"
      )
    } catch {
      case ex: Throwable => logError(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }
    try {
      sql(
        "create cube complextypes dimensions(deviceInformationId integer, channelsId string, " +
          "ROMSize string, purchasedate string, mobile struct<imei string, imsi string>, MAC " +
          "array<string>, locationinfo array<struct<ActiveAreaId integer, ActiveCountry string, " +
          "ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet " +
          "string>>, proddate struct<productionDate string,activeDeactivedate array<string>>) " +
          "measures(gamePointId numeric,contractNumber numeric) OPTIONS ( " +
          "NO_DICTIONARY (ROMSize) PARTITIONER [CLASS = " +
          "'org.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl' ," +
          "COLUMNS= (deviceInformationId) , PARTITION_COUNT=1] )"
      )
    } catch {
      case ex: Throwable => logError(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }

    try {
      sql(
        "create cube incrementalLoadTable dimensions(deviceInformationId integer, channelsId " +
          "string, ROMSize string, purchasedate string, mobile struct<imei string, imsi string>, " +
          "MAC array<string>, locationinfo array<struct<ActiveAreaId integer, ActiveCountry " +
          "string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet " +
          "string>>, proddate struct<productionDate string,activeDeactivedate array<string>>) " +
          "measures(gamePointId numeric,contractNumber numeric) OPTIONS (PARTITIONER [CLASS = " +
          "'org.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl' ," +
          "COLUMNS= (deviceInformationId) , PARTITION_COUNT=1] )"
      )
    } catch {
      case ex: Throwable => logError(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }
  }

  def buildRelation() = {
    val catalog = CarbonEnv.getInstance(CarbonHiveContext).carbonCatalog
    sampleRelation = catalog.lookupRelation1(Option("default"), "sample", None)(CarbonHiveContext)
      .asInstanceOf[CarbonRelation]
    dimSampleRelation = catalog
      .lookupRelation1(Option("default"), "dimSample", None)(CarbonHiveContext)
      .asInstanceOf[CarbonRelation]
    complexRelation = catalog
      .lookupRelation1(Option("default"), "complextypes", None)(CarbonHiveContext)
      .asInstanceOf[CarbonRelation]
    incrementalLoadTableRelation = catalog
      .lookupRelation1(Option("default"), "incrementalLoadTable", None)(CarbonHiveContext)
      .asInstanceOf[CarbonRelation]
  }

  def checkDictionary(relation: CarbonRelation, columnName: String, value: String) {
    val table = relation.cubeMeta.carbonTable
    val dimension = table.getDimensionByName(table.getFactTableName, columnName)
    val tableIdentifier = new CarbonTableIdentifier(table.getDatabaseName, table.getFactTableName, "uniqueid")

    val columnIdentifier = new DictionaryColumnUniqueIdentifier(tableIdentifier,
      dimension.getColumnId, dimension.getDataType
    )
    val dict = CarbonLoaderUtil.getDictionary(columnIdentifier,
      CarbonHiveContext.hdfsCarbonBasePath
    )
    assert(dict.getSurrogateKey(value) != CarbonCommonConstants.INVALID_SURROGATE_KEY)
  }

  test("[issue-80]Global Dictionary Generation") {

    var carbonLoadModel = buildCarbonLoadModel(sampleRelation, filePath, null, null)
    GlobalDictionaryUtil
      .generateGlobalDictionary(CarbonHiveContext,
        carbonLoadModel,
        sampleRelation.cubeMeta.storePath
      )

    // test for dimension table
    // TODO - Need to fill and send the dimension table data as per new DimensionRelation in
    // CarbonDataLoadModel
    // carbonLoadModel = buildCarbonLoadModel(dimSampleRelation, filePath, dimFilePath, null)
    // GlobalDictionaryUtil.generateGlobalDictionary(CarbonHiveContext, carbonLoadModel,
    // dimSampleRelation.cubeMeta.dataPath, false)
  }

  test("[Issue-190]load csv file without header And support complex type") {
    val header = "deviceInformationId,channelsId,ROMSize,purchasedate,mobile,MAC,locationinfo," +
      "proddate,gamePointId,contractNumber"
    var carbonLoadModel = buildCarbonLoadModel(complexRelation, complexfilePath, null, header)
    GlobalDictionaryUtil
      .generateGlobalDictionary(CarbonHiveContext,
        carbonLoadModel,
        complexRelation.cubeMeta.storePath
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
        sampleRelation.cubeMeta.storePath
      )
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
        sampleRelation.cubeMeta.storePath
      )
    checkDictionary(incrementalLoadTableRelation, "deviceInformationId", "100077")
  }

}
