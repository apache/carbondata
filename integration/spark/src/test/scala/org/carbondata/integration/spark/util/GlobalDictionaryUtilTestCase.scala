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


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.CarbonContext
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import org.apache.spark.sql.SQLContext
import org.carbondata.integration.spark.load.CarbonLoadModel
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.CarbonRelation
import java.io.File
import org.apache.spark.Logging

/**
 * Test Case for org.carbondata.integration.spark.util.GlobalDictionaryUtil
 *
 * @author: QiangCai
 * @date: Apr 10, 2016 10:34:58 PM
 * @See org.carbondata.integration.spark.util.GlobalDictionaryUtil
 */
class GlobalDictionaryUtilTestCase extends FunSuite with BeforeAndAfter with Logging {
  
  var sc: SparkContext = _
  var sqlContext: SQLContext = _
  var storeLocation: String = _
  var hiveMetaStoreDB: String = _
  var sampleRelation: CarbonRelation = _
  var dimSampleRelation: CarbonRelation = _
  var filePath: String = _
  var pwd: String = _
  var dimFilePath: String = _
  
  def buildTestData() = {
    pwd = new File(this.getClass.getResource("/").getPath+"/../../").getCanonicalPath
    storeLocation = pwd + "/target/store"
    hiveMetaStoreDB = pwd + "/target/metastore_db"
    filePath = pwd + "/src/test/resources/sample.csv"
    dimFilePath = "dimTableSample:" + pwd + "/src/test/resources/dimTableSample.csv"
  }
  
  def buildTestContext() = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.STORE_LOCATION_HDFS, storeLocation)
    sc = new SparkContext(
        new SparkConf()
          .set("spark.driver.allowMultipleContexts","true")
          .setAppName("GloablDictionaryTestCase")
          .setMaster("local[2]"))
    sqlContext = new CarbonContext(sc, storeLocation)
    sqlContext.setConf("carbon.kettle.home", new File(pwd + "/../../processing/carbonplugins").getCanonicalPath)
    sqlContext.setConf("hive.metastore.warehouse.dir", pwd +"/target/hivemetadata")
    sqlContext.setConf("javax.jdo.option.ConnectionURL","jdbc:derby:;databaseName="+hiveMetaStoreDB+";create=true")
    try{
      sqlContext.sql("CREATE CUBE IF NOT EXISTS sample DIMENSIONS (id STRING, name_1 STRING, city STRING) MEASURES (age INTEGER) OPTIONS(PARTITIONER[CLASS='org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl',COLUMNS=(id),PARTITION_COUNT=1])")
      sqlContext.sql("CREATE CUBE IF NOT EXISTS dimSample DIMENSIONS (id STRING, name STRING, city STRING) MEASURES (age INTEGER) WITH dimTableSample RELATION(Fact.id=id) INCLUDE(id,name) OPTIONS(PARTITIONER[CLASS='org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl',COLUMNS=(id),PARTITION_COUNT=1])")
    }catch{
      case ex: Throwable => logError(ex.getMessage +"\r\n" + ex.getStackTraceString)    
    }
    sampleRelation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(Option("default"), "sample", None)(sqlContext).asInstanceOf[CarbonRelation]
    dimSampleRelation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(Option("default"), "dimSample", None)(sqlContext).asInstanceOf[CarbonRelation]
  }

  def buildCarbonLoadModel(relation: CarbonRelation, dimensionFilePath: String): CarbonLoadModel = {
    val carbonLoadModel = new CarbonLoadModel
    carbonLoadModel.setTableName(relation.cubeMeta.cubeName)
    carbonLoadModel.setSchemaName(relation.cubeMeta.schemaName)
    carbonLoadModel.setSchema(relation.cubeMeta.schema)
    carbonLoadModel.setFactFilePath(filePath)
    carbonLoadModel.setDimFolderPath(dimensionFilePath)
    carbonLoadModel
  }
  
  before {
    buildTestData
    buildTestContext
  }
  
  test("[issue-80]Global Dictionary Generation"){
    var carbonLoadModel = buildCarbonLoadModel(sampleRelation, null)
    var rtn = GlobalDictionaryUtil.generateGlobalDictionary(sqlContext, carbonLoadModel, false)
    assert( rtn === 1)
    //test for dimension table
    carbonLoadModel = buildCarbonLoadModel(dimSampleRelation, dimFilePath)
    rtn = GlobalDictionaryUtil.generateGlobalDictionary(sqlContext, carbonLoadModel, false)
    assert( rtn === 1)
  }

}
