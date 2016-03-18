package com.huawei.datasight.spark.rdd

import com.huawei.datasight.molap.core.load.LoadMetadataDetails
import com.huawei.datasight.molap.load.DeletedLoadMetadata
import com.huawei.datasight.molap.spark.util.MolapQueryUtil
import com.huawei.datasight.spark.DeletedLoadResult
import com.huawei.unibi.molap.constants.MolapCommonConstants
import com.huawei.unibi.molap.dataprocessor.dataretention.DataRetentionHandler
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.cubemodel.Partitioner

import scala.collection.JavaConversions._

class MolapDeleteLoadByDateRDD[K, V](
    sc: SparkContext,
    result: DeletedLoadResult[K, V],
    schemaName: String,
    cubeName: String,
    dateField: String,
    dateFieldActualName: String,
    dateValue: String,
    partitioner: Partitioner,
    factTableName: String,
    dimTableName: String,
    hdfsStoreLocation: String,
    loadMetadataDetails: List[LoadMetadataDetails],
    currentRestructFolder: Integer)
  extends RDD[(K, V)](sc, Nil) with Logging {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  override def getPartitions: Array[Partition] = {
    println(partitioner.nodeList)
    val splits = MolapQueryUtil.getTableSplits(schemaName, cubeName, null, partitioner)
    val result = new Array[Partition](splits.length)
    for (i <- 0 until result.length) {
      result(i) = new MolapLoadPartition(id, i, splits(i))
    }
    result
  }

  override def compute(theSplit: Partition, context: TaskContext) = {
    val iter = new Iterator[(K, V)] {
      val deletedMetaData = new DeletedLoadMetadata()
      val split = theSplit.asInstanceOf[MolapLoadPartition]
      logInfo("Input split: " + split.serializableHadoopSplit.value)

      logInfo("Input split: " + split.serializableHadoopSplit.value)
      val partitionID = split.serializableHadoopSplit.value.getPartition().getUniqueID()

      //TODO call MOLAP delete API
      println("Applying data retention as per date value " + dateValue)
      var dateFormat = ""
      try {
        val dateValueAsDate = DateTimeUtils.stringToTime(dateValue)
        dateFormat = MolapCommonConstants.MOLAP_TIMESTAMP_DEFAULT_FORMAT
      } catch {
        case e: Exception => logInfo("Unable to parse with default time format " + dateValue)
      }
      val dataRetentionInvoker = new DataRetentionHandler(schemaName + '_' + partitionID, cubeName + '_' + partitionID, factTableName, dimTableName, hdfsStoreLocation, dateField, dateFieldActualName, dateValue, dateFormat, currentRestructFolder, loadMetadataDetails)
      val mapOfRetentionValues = dataRetentionInvoker.updateFactFileBasedOnDataRetentionPolicy().toIterator
      var finished = false

      override def hasNext: Boolean = {
        mapOfRetentionValues.hasNext
      }

      override def next(): (K, V) = {
        val (loadid, status) = mapOfRetentionValues.next
        println("loadid :" + loadid + "status :" + status)
        result.getKey(loadid, status)
      }
    }
    iter
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[MolapLoadPartition]
    val s = theSplit.serializableHadoopSplit.value.getLocations //.filter(_ != "localhost")
    logInfo("Host Name : " + s(0) + s.length)
    s
  }
}

