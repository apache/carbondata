package org.carbondata.integration.spark.util

import java.io.File
import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.carbon.CarbonDef.Dimension
import org.carbondata.core.datastorage.store.impl.FileFactory
import org.carbondata.core.util.CarbonProperties
import org.carbondata.integration.spark.load.CarbonLoadModel
import org.carbondata.integration.spark.rdd.ColumnPartitioner
import org.carbondata.integration.spark.rdd.CarbonBlockDistinctValuesCombineRDD
import org.carbondata.integration.spark.rdd.CarbonGlobalDictionaryGenerateRDD
import org.apache.spark.Logging

object GlobalDictionaryUtil extends Logging {

  def pruneColumnsAndIndex(dimensions: Array[Dimension], columns: Array[String]) = {
    val indexBuffer = new ArrayBuffer[Int]
    val columnBuffer = new ArrayBuffer[String]
    for (dim <- dimensions) {
      breakable {
        for (i <- 0 until columns.length) {
          if (dim.name.equalsIgnoreCase(columns(i))) {
            indexBuffer += i
            columnBuffer += dim.name
            break
          }
        }
      }
    }
    (indexBuffer.toArray, columnBuffer.toArray)
  }

  def getGlobalDictionaryFolder(storeLocation: String, schemaName: String, tableName: String) = {    
    //TODO need change to use CarbonDictionaryUtil after pr#64 was merged
    storeLocation + File.separator +
      schemaName + File.separator +
      tableName + File.separator +
      "metadata" + File.separator +
      "Dictionary"
  }

  def getGlobalDictionaryFileName(storeLocation: String, schemaName: String, tableName: String, columnName: String) = {
    //TODO need change to use CarbonDictionaryUtil after pr#64 was merged
    getGlobalDictionaryFolder(storeLocation, schemaName, tableName) + File.separator +
      tableName + "_" + columnName
  }

  def writeGlobalDictionaryToFile(filePath: String, set: HashSet[String]) = {
    //TODO need change to use CarbonDictionaryWriter after pr#64 merged 
    if(set.size > 0 ){      
      val out = FileFactory.getDataOutputStream(filePath, FileFactory.getFileType(filePath), 10240)
      val setIter = set.toIterator
      var value = ""
      while (setIter.hasNext) {
        value = setIter.next
        out.writeInt(value.length)
        out.writeBytes(value)
      }
      out.close
      //update meta data
    }
  }

  def readGlobalDictionaryFromFile(folder: String, table: String, column: Array[String]) = {
    //TODO load dictionary file if exists
     val dicts = new Array[HashMap[String, Int]](column.length)
      for (i <- 0 until column.length) {
        dicts(i) = new HashMap[String, Int]()
      }
     dicts
  }
  
  /** generate global dictionary with SQLContext and CarbonLoadModel
    *
    * @param sqlContext 
    * @param carbonLoadModel 
    * @return a integer 1: successfully -1: failed
    */
  def generateGlobalDictionary(sqlContext: SQLContext, carbonLoadModel: CarbonLoadModel) = {
    var rtn = 1
    try{
      val hdfsLocation = CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION_HDFS)
      val tableName = carbonLoadModel.getTableName
      //create dictionary folder if not exists
      val dictfolderPath = getGlobalDictionaryFolder(hdfsLocation, carbonLoadModel.getSchemaName, tableName)  
      val folderHolder = FileFactory.getCarbonFile(dictfolderPath, FileFactory.getFileType(dictfolderPath))
      if(!folderHolder.exists) folderHolder.mkdirs
      
      //load data by using dataSource com.databricks.spark.csv
      val df = sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .load(carbonLoadModel.getFactFilePath)
      //prune columns and get indexes
      val (indexes, columns) = pruneColumnsAndIndex(carbonLoadModel.getSchema.dimensions, df.columns)
      //combine distinct value in a block and partition by column
      val inputRDD = new CarbonBlockDistinctValuesCombineRDD(df.rdd, dictfolderPath, tableName, indexes, columns).partitionBy(new ColumnPartitioner(columns.length))
      //generate global dictionary files
      val statusList = new CarbonGlobalDictionaryGenerateRDD(inputRDD, carbonLoadModel, columns, hdfsLocation).collect()
      //check result status
      if(statusList.exists(x => CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(x._2))){
        rtn = -2
        logError("generate global dictionary files failed")
      }else{
        logInfo("generate global dictionary successfully")
      }
    }catch{
      case ex:Exception =>
        rtn = -1
        logError("generate global dictionary failed \r\n" + ex.getMessage+"\r\n" + ex.getStackTraceString)
    }
    rtn
  }
}