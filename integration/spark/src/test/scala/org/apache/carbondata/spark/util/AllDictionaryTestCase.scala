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

import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.{CarbonEnv, CarbonRelation}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.carbon.CarbonDataLoadSchema
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.processing.constants.TableOptionConstant
import org.apache.carbondata.processing.model.CarbonLoadModel

/**
  * Test Case for org.apache.carbondata.integration.spark.util.GlobalDictionaryUtil
  */
class AllDictionaryTestCase extends QueryTest with BeforeAndAfterAll {

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
    carbonLoadModel.setTableName(relation.tableMeta.carbonTableIdentifier.getDatabaseName)
    carbonLoadModel.setDatabaseName(relation.tableMeta.carbonTableIdentifier.getTableName)
    val table = relation.tableMeta.carbonTable
    val carbonSchema = new CarbonDataLoadSchema(table)
    carbonLoadModel.setDatabaseName(table.getDatabaseName)
    carbonLoadModel.setTableName(table.getFactTableName)
    carbonLoadModel.setCarbonDataLoadSchema(carbonSchema)
    carbonLoadModel.setFactFilePath(filePath)
    carbonLoadModel.setCsvHeader(header)
    carbonLoadModel.setCsvDelimiter(",")
    carbonLoadModel.setComplexDelimiterLevel1("\\$")
    carbonLoadModel.setComplexDelimiterLevel2("\\:")
    carbonLoadModel.setAllDictPath(allDictFilePath)
    carbonLoadModel.setSerializationNullFormat(
          TableOptionConstant.SERIALIZATION_NULL_FORMAT.getName + ",\\N")
    carbonLoadModel
  }

  override def beforeAll {
    sql("drop table if exists sample")
    sql("drop table if exists complextypes")
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
          "age INT) STORED BY 'org.apache.carbondata.format'"
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
          "TBLPROPERTIES('DICTIONARY_EXCLUDE'='ROMSize')"
      )
    } catch {
      case ex: Throwable => LOGGER.error(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }
  }

  def buildRelation() = {
    val catalog = CarbonEnv.get.carbonMetastore
    sampleRelation = catalog.lookupRelation1(Option(CarbonCommonConstants.DATABASE_DEFAULT_NAME),
      "sample")(sqlContext).asInstanceOf[CarbonRelation]
    complexRelation = catalog.lookupRelation1(Option(CarbonCommonConstants.DATABASE_DEFAULT_NAME),
      "complextypes")(sqlContext).asInstanceOf[CarbonRelation]
  }

  test("Support generate global dictionary from all dictionary files") {
    val header = "id,name,city,age"
    val carbonLoadModel = buildCarbonLoadModel(sampleRelation, null, header, sampleAllDictionaryFile)
    GlobalDictionaryUtil
      .generateGlobalDictionary(sqlContext,
        carbonLoadModel,
        sampleRelation.tableMeta.storePath)

    DictionaryTestCaseUtil.
      checkDictionary(sampleRelation, "city", "shenzhen")
  }

  test("Support generate global dictionary from all dictionary files for complex type") {
    val header = "deviceInformationId,channelsId,ROMSize,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber"
    val carbonLoadModel = buildCarbonLoadModel(complexRelation, null, header, complexAllDictionaryFile)
    GlobalDictionaryUtil
      .generateGlobalDictionary(sqlContext,
      carbonLoadModel,
      complexRelation.tableMeta.storePath)

    DictionaryTestCaseUtil.
      checkDictionary(complexRelation, "channelsId", "1650")
  }
  
  override def afterAll {
    sql("drop table sample")
    sql("drop table complextypes")
  }
}
