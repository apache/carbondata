package org.carbondata.integration.spark.util


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.CarbonContext
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import org.junit.Before
import org.apache.spark.sql.SQLContext
import org.carbondata.integration.spark.load.CarbonLoadModel
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.carbondata.core.carbon.CarbonDef.Schema;
import org.carbondata.core.carbon.CarbonDef.Dimension

class GlobalDictionaryTestCase extends FunSuite with BeforeAndAfter {
  
  var oc: SparkContext = _
  var sc: SQLContext = _
  var storeLocation: String = _ 
  var schema: Schema = _ 
  var table: String = _
  var filePath: String = _
  
  def buildTestData() = {
    storeLocation = "target"
    filePath = "src/test/resources/sample.csv"
    table = "sample"
    
    schema = new Schema 
    schema.name = "default"
    schema.dimensions = new Array[Dimension](4)
    schema.dimensions(0) = new Dimension()
    schema.dimensions(0).name = "id"
    schema.dimensions(1) = new Dimension()
    schema.dimensions(1).name = "name"
    schema.dimensions(2) = new Dimension()
    schema.dimensions(2).name = "city"
    schema.dimensions(3) = new Dimension()
    schema.dimensions(3).name = "age"
  }
  
  def buildTestContext() = {
     CarbonProperties.getInstance().addProperty(CarbonCommonConstants.STORE_LOCATION_HDFS, storeLocation)
    oc = new SparkContext(
        new SparkConf()
        .setAppName("GloablDictionaryTestCase")
        .setMaster("local[2]"))
    sc = new SQLContext(oc)
  }
   
  def generateGlobalDictionary(): Int = {
    val carbonLoadModel = new CarbonLoadModel
    carbonLoadModel.setTableName(table)
    carbonLoadModel.setSchemaName(schema.name)
    carbonLoadModel.setSchema(schema)
    carbonLoadModel.setFactFilePath(filePath)
    GlobalDictionaryUtil.generateGlobalDictionary(sc, carbonLoadModel)
  }
  
  before {
    buildTestData
    buildTestContext
  }
  
  test("[issue-80]Global Dictionary Generation"){
    assert(generateGlobalDictionary() === 1 )
  }
}

object GlobalDictionaryTestCase{
  def main(args: Array[String]): Unit = {
    val testCase = new GlobalDictionaryTestCase
    testCase.buildTestData
    testCase.buildTestContext
    testCase.generateGlobalDictionary
  }
}

