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

import java.util.concurrent.Executors
import java.util.concurrent.Callable

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.{CarbonEnv, CarbonRelation}
import org.apache.spark.sql.common.util.CarbonHiveContext
import org.apache.spark.sql.common.util.CarbonHiveContext.sql
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.ext.PathFactory
import org.apache.carbondata.core.carbon.CarbonDataLoadSchema
import org.apache.carbondata.core.datastorage.store.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.processing.constants.TableOptionConstant
import org.apache.carbondata.processing.model.CarbonLoadModel

class GlobalDictionaryUtilConcurrentTestCase extends QueryTest with BeforeAndAfterAll {

  var sampleRelation: CarbonRelation = _
  var workDirectory: String = _

  def buildCarbonLoadModel(relation: CarbonRelation,
                           filePath: String,
                           dimensionFilePath: String,
                           header: String): CarbonLoadModel = {
    val carbonLoadModel = new CarbonLoadModel
    carbonLoadModel.setTableName(relation.tableMeta.carbonTableIdentifier.getDatabaseName)
    carbonLoadModel.setDatabaseName(relation.tableMeta.carbonTableIdentifier.getTableName)
    // carbonLoadModel.setSchema(relation.cubeMeta.schema)
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
  }
  def buildTable() = {
    try {
      sql(
        "CREATE TABLE IF NOT EXISTS employee (empid STRING) STORED BY 'org.apache.carbondata.format'")
    } catch {
      case ex: Throwable => LOGGER.error(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }
  }

  def buildRelation() = {
    val catalog = CarbonEnv.get.carbonMetastore
    sampleRelation = catalog.lookupRelation1(Option(CarbonCommonConstants.DATABASE_DEFAULT_NAME),
      "employee")(CarbonHiveContext)
      .asInstanceOf[CarbonRelation]
  }
  def writedummydata(filePath: String, recCount: Int) = {
    var a: Int = 0
    var records: StringBuilder = StringBuilder.newBuilder
    for (a <- 0 to recCount) {
      records.append(a).append("\n")
    }
    val dis = FileFactory.getDataOutputStream(filePath, FileFactory.getFileType(filePath))
    dis.writeBytes(records.toString())
    dis.close()
  }
  test("concurrent dictionary generation") {
    CarbonProperties.getInstance.addProperty(CarbonCommonConstants.MAX_QUERY_EXECUTION_TIME, "-1")
    val noOfFiles = 5
    val files = new ListBuffer[String]()
    val loadModels = new ListBuffer[CarbonLoadModel]()
    for (i <- 0 until noOfFiles) {
      val filePath: String = workDirectory + s"/src/test/resources/singlecolumn_${10 * (i + 1)}.csv"
      files += filePath
      loadModels += buildCarbonLoadModel(sampleRelation, filePath, null, "empid")
      writedummydata(filePath, 10 * (i + 1))
    }
    try {
      val dictGenerators = new java.util.ArrayList[Callable[String]](noOfFiles)
      for (i <- 0 until noOfFiles) {
        dictGenerators.add(new DictGenerator(loadModels(i)))
      }
      val executorService = Executors.newFixedThreadPool(10);
      val results = executorService.invokeAll(dictGenerators);
      for (i <- 0 until noOfFiles) {
        val res = results.get(i).get
        assert("Pass".equals(res))
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        assert(false)
    }
    val carbonTableIdentifier = sampleRelation.tableMeta.carbonTable.getCarbonTableIdentifier
    val columnIdentifier = sampleRelation.tableMeta.carbonTable.getDimensionByName("employee", "empid").getColumnIdentifier
    val carbonTablePath = PathFactory.getInstance()
        .getCarbonTablePath(sampleRelation.tableMeta.storePath, carbonTableIdentifier);
    val dictPath = carbonTablePath.getDictionaryFilePath(columnIdentifier.getColumnId)
    val dictFile = FileFactory.getCarbonFile(dictPath, FileFactory.getFileType(dictPath))
    val offSet = dictFile.getSize
    val sortIndexPath = carbonTablePath.getSortIndexFilePath(columnIdentifier.getColumnId, offSet)
    val sortIndexFile = FileFactory.getCarbonFile(sortIndexPath, FileFactory.getFileType(sortIndexPath))
    assert(sortIndexFile.exists())
    val sortIndexFiles = carbonTablePath.getSortIndexFiles(sortIndexFile.getParentFile, columnIdentifier.getColumnId)
    assert(sortIndexFiles.length >= 1)
    deleteFiles(files)
  }

  def deleteFiles(files: ListBuffer[String]) {
    for (i <- 0 until files.length) {
      val file = FileFactory.getCarbonFile(files(i), FileFactory.getFileType(files(i)))
      file.delete()
    }
  }
  override def afterAll {
    sql("drop table if exists employee")
    CarbonProperties.getInstance.addProperty(CarbonCommonConstants.MAX_QUERY_EXECUTION_TIME,
        Integer.toString(CarbonCommonConstants.DEFAULT_MAX_QUERY_EXECUTION_TIME))
  }
  class DictGenerator(loadModel: CarbonLoadModel) extends Callable[String] {
   override def call:String = {
     var result = "Pass"
      try {
        GlobalDictionaryUtil
          .generateGlobalDictionary(CarbonHiveContext,
            loadModel,
            sampleRelation.tableMeta.storePath)
      } catch {
        case ex: Exception => 
          result = ex.getMessage
          ex.printStackTrace()
      }
      result
    }
  }
}
