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
import org.carbondata.core.carbon.CarbonDef.Schema
import org.carbondata.core.carbon.CarbonDef.Dimension
import org.carbondata.core.metadata.CarbonMetadata
import org.carbondata.core.carbon.CarbonDef.Cube
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.CarbonRelation
import org.carbondata.core.carbon.CarbonDef.CubeDimension
import java.io.File
import org.apache.spark.Logging

class GlobalDictionaryTestCase extends FunSuite with BeforeAndAfter with Logging {
  
  var sc: SparkContext = _
  var sqlContext: SQLContext = _
  var storeLocation: String = _ 
  var relation: CarbonRelation = _
  var filePath: String = _
  var pwd: String = _
  
  def buildTestData() = {
    pwd = new File(this.getClass.getResource("/").getPath+"/../../").getCanonicalPath
    storeLocation = pwd + "/target/store"
    filePath = pwd + "/src/test/resources/sample.csv"
  }
  
  def buildTestContext() = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.STORE_LOCATION_HDFS, storeLocation)
    sc = new SparkContext(
        new SparkConf()
        .setAppName("GloablDictionaryTestCase")
        .setMaster("local[2]"))
    sqlContext = new CarbonContext(sc, storeLocation)
    sqlContext.setConf("carbon.kettle.home", new File(pwd + "/../../processing/carbonplugins").getCanonicalPath)
    sqlContext.setConf("hive.metastore.warehouse.dir", pwd +"/target/hivemetadata")
    try{
      sqlContext.sql("CREATE CUBE sample DIMENSIONS (id STRING, name_1 STRING, city STRING) MEASURES (age INTEGER) OPTIONS(PARTITIONER[CLASS='org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl',COLUMNS=(id),PARTITION_COUNT=1])")
    }catch{
      case ex: Throwable => logError(ex.getMessage +"\r\n" + ex.getStackTraceString)    
    }
    relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(Option("default"), "sample", None)(sqlContext).asInstanceOf[CarbonRelation]
  }
  
  before {
    buildTestData
    buildTestContext
  }
  
  test("[issue-80]Global Dictionary Generation"){
    val carbonLoadModel = new CarbonLoadModel
    carbonLoadModel.setTableName(relation.cubeMeta.cubeName)
    carbonLoadModel.setSchemaName(relation.cubeMeta.schemaName)
    carbonLoadModel.setSchema(relation.cubeMeta.schema)
    carbonLoadModel.setFactFilePath(filePath)
    
    val sql = new SQLContext(sc)
    val rtn = GlobalDictionaryUtil.generateGlobalDictionary(sql, carbonLoadModel, false)
    assert(rtn === 1 )
  }
}

