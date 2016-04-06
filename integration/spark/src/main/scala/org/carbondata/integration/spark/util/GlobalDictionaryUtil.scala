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
import org.apache.spark.sql.CarbonEnv
import org.carbondata.core.carbon.CarbonTableIdentifier
import org.carbondata.core.util.CarbonProperties
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.datastorage.store.impl.FileFactory
import org.carbondata.core.writer.CarbonDictionaryWriter
import org.carbondata.core.writer.CarbonDictionaryWriterImpl
import org.carbondata.core.util.CarbonDictionaryUtil
import org.carbondata.core.util.CarbonUtil
import java.io.IOException

object GlobalDictionaryUtil extends Logging {

  def pruneColumnsAndIndex(dimensions: Array[String], columns: Array[String]) = {
    val columnBuffer = new ArrayBuffer[String]
    for (dim <- dimensions) {
      breakable {
        for (i <- 0 until columns.length) {
          if (dim.equalsIgnoreCase(columns(i))) {
            columnBuffer += dim
            break
          }
        }
      }
    }
    columnBuffer.toArray
  }

  def writeGlobalDictionaryToFile(hdfsStorePath: String, table: CarbonTableIdentifier, columnName: String, iter: Iterator[String]) = {
    //TODO better to use factory method
    val writer: CarbonDictionaryWriter = new CarbonDictionaryWriterImpl(hdfsStorePath, table, columnName, false)
    try {
      while (iter.hasNext) {
        writer.write(iter.next)
      }
    } finally {
      writer.close
    }
  }

  def readGlobalDictionaryFromFile(folder: String, table: String, column: Array[String]) = {
    //TODO need change to use CarbonDictionaryReader after another PR merged 
    val dicts = new Array[HashMap[String, Int]](column.length)
    val hasDicts = new Array[Boolean](column.length)
    for (i <- 0 until column.length) {
      dicts(i) = new HashMap[String, Int]()
      // need to add some codes to use CarbonDictionaryReader
      
      if (dicts(i).size == 0) {
        hasDicts(i) = false
      }
    }
    (dicts, hasDicts)
  }

  /**
   * generate global dictionary with SQLContext and CarbonLoadModel
   *
   * @param sqlContext
   * @param carbonLoadModel
   * @return a integer 1: successfully
   */
  def generateGlobalDictionary(sqlContext: SQLContext, carbonLoadModel: CarbonLoadModel) = {
    var rtn = 1
    try {
      val hdfsLocation = CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION_HDFS)
      val table = new CarbonTableIdentifier(carbonLoadModel.getSchemaName, carbonLoadModel.getTableName)
      //create dictionary folder if not exists
      val dictfolderPath = CarbonDictionaryUtil.getDirectoryPath(table, hdfsLocation, false)
      val created = CarbonUtil.checkAndCreateFolder(dictfolderPath)
      if (!created) {
        logError("Dictionary Folder creation status :: " + created)
        throw new IOException("Failed to created dictionary folder");
      }
      //load data by using dataSource com.databricks.spark.csv
      //TODO need new SQLContext to use spark-csv
      var df = new SQLContext(sqlContext.sparkContext).read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("comment", null)
        .load(carbonLoadModel.getFactFilePath)
      //columns which need to generate global dictionary file   
      val requireColumns = pruneColumnsAndIndex(carbonLoadModel.getSchema.cubes(0).dimensions.map(_.name), df.columns)

      if (requireColumns.size >= 1) {
        //select column to push down pruning
        df = df.select(requireColumns.head, requireColumns.tail: _*)
        //combine distinct value in a block and partition by column
        val inputRDD = new CarbonBlockDistinctValuesCombineRDD(df.rdd, dictfolderPath, table, requireColumns).partitionBy(new ColumnPartitioner(requireColumns.length))
        //generate global dictionary files
        val statusList = new CarbonGlobalDictionaryGenerateRDD(inputRDD, table, requireColumns, hdfsLocation).collect()
        //check result status
        if (statusList.exists(x => CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(x._2))) {
          logError("generate global dictionary files failed")
          throw new Exception("Failed to generate global dictionary files")
        } else {
          logInfo("generate global dictionary successfully")
        }
      } else {
        logInfo("have no column need to generate global dictionary")
      }
    } catch {
      case ex: Exception =>
        logError("generate global dictionary failed")
        throw ex
    }
    rtn
  }
}