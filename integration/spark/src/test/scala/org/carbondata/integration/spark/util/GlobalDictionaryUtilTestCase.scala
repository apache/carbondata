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
package org.carbondata.integration.spark.util


import java.io.File

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.{CarbonHiveContext, QueryTest}
import org.apache.spark.sql.{CarbonEnv, CarbonRelation}
import org.carbondata.core.carbon.CarbonDataLoadSchema
import org.carbondata.integration.spark.load.CarbonLoadModel
import org.scalatest.BeforeAndAfterAll

/**
 * Test Case for org.carbondata.integration.spark.util.GlobalDictionaryUtil
 *
 * @date: Apr 10, 2016 10:34:58 PM
 * @See org.carbondata.integration.spark.util.GlobalDictionaryUtil
 */
class GlobalDictionaryUtilTestCase extends QueryTest with BeforeAndAfterAll  {
  
  var sampleRelation: CarbonRelation = _
  var dimSampleRelation: CarbonRelation = _
  var filePath: String = _
  var pwd: String = _
  var dimFilePath: String = _
  
  def buildTestData() = {
    pwd = new File(this.getClass.getResource("/").getPath+"/../../").getCanonicalPath
    filePath = pwd + "/src/test/resources/sample.csv"
    dimFilePath = "dimTableSample:" + pwd + "/src/test/resources/dimTableSample.csv"
  }
  
  def buildTestContext() = {
    try{
      sql("CREATE CUBE IF NOT EXISTS sample DIMENSIONS (id STRING, name STRING, city STRING) MEASURES (age INTEGER) OPTIONS(PARTITIONER[CLASS='org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl',COLUMNS=(id),PARTITION_COUNT=1])")
    }catch{
      case ex: Throwable => logError(ex.getMessage +"\r\n" + ex.getStackTraceString)    
    }
    try{
      sql("CREATE CUBE IF NOT EXISTS sample1 DIMENSIONS (id STRING, name STRING, city STRING) MEASURES (age INTEGER) OPTIONS(PARTITIONER[CLASS='org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl',COLUMNS=(id),PARTITION_COUNT=1])")
    }catch{
      case ex: Throwable => logError(ex.getMessage +"\r\n" + ex.getStackTraceString)    
    }
    try{
      sql("CREATE CUBE IF NOT EXISTS dimSample DIMENSIONS (id STRING, name STRING, city STRING) MEASURES (age INTEGER) WITH dimTableSample RELATION(Fact.id=id) INCLUDE(id,name) OPTIONS(PARTITIONER[CLASS='org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl',COLUMNS=(id),PARTITION_COUNT=1])")
    }catch{
      case ex: Throwable => logError(ex.getMessage +"\r\n" + ex.getStackTraceString)    
    }
    sampleRelation = CarbonEnv.getInstance(CarbonHiveContext).carbonCatalog.lookupRelation1(Option("default"), "sample", None)(CarbonHiveContext).asInstanceOf[CarbonRelation]
    dimSampleRelation = CarbonEnv.getInstance(CarbonHiveContext).carbonCatalog.lookupRelation1(Option("default"), "dimSample", None)(CarbonHiveContext).asInstanceOf[CarbonRelation]
  }

  def buildCarbonLoadModel(relation: CarbonRelation, dimensionFilePath: String): CarbonLoadModel = {
    val carbonLoadModel = new CarbonLoadModel
    carbonLoadModel.setTableName(relation.cubeMeta.carbonTableIdentifier.getDatabaseName)
    carbonLoadModel.setDatabaseName(relation.cubeMeta.carbonTableIdentifier.getTableName)
    //carbonLoadModel.setSchema(relation.cubeMeta.schema)
    val carbonSchema = new CarbonDataLoadSchema(relation.cubeMeta.carbonTable)
    carbonLoadModel.setCarbonDataLoadSchema(carbonSchema)
    carbonLoadModel.setFactFilePath(filePath)
    carbonLoadModel.setDimFolderPath(dimensionFilePath)
    carbonLoadModel
  }

  override def beforeAll {
    buildTestData
    buildTestContext
  }
  
  test("[issue-80]Global Dictionary Generation"){

    sql("LOAD DATA fact from '" + filePath + "' INTO CUBE sample1 PARTITIONDATA(DELIMITER ',', QUOTECHAR '')")
    
    var carbonLoadModel = buildCarbonLoadModel(sampleRelation, null)
    var rtn = GlobalDictionaryUtil.generateGlobalDictionary(CarbonHiveContext, carbonLoadModel, sampleRelation.cubeMeta.dataPath, false)
    assert( rtn === 1)
    //test for dimension table
    //TODO - Need to fill and send the dimension table data as per new DimensionRelation in CarbonDataLoadModel
    //carbonLoadModel = buildCarbonLoadModel(dimSampleRelation, dimFilePath)
    //rtn = GlobalDictionaryUtil.generateGlobalDictionary(CarbonHiveContext, carbonLoadModel, dimSampleRelation.cubeMeta.dataPath, false)
    //assert( rtn === 1)
  }
}
