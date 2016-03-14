


package com.huawei.datasight.spark.rdd

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.hadoop.conf.Configuration
import org.apache.spark.Logging
import org.apache.spark.Partition
import org.apache.spark.SerializableWritable
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import com.huawei.unibi.molap.engine.executer.MolapQueryExecutorModel
import com.huawei.datasight.spark.KeyVal
import com.huawei.datasight.molap.spark.util.MolapQueryUtil
import com.huawei.datasight.molap.spark.splits.TableSplit
import com.huawei.unibi.molap.util.MolapProperties
import com.huawei.unibi.molap.engine.scanner.impl.MolapKey
import com.huawei.unibi.molap.engine.scanner.impl.MolapValue
import com.huawei.datasight.molap.partition.api.impl.QueryPartitionHelper
import scala.collection.JavaConversions._
import com.huawei.datasight.molap.load.MolapLoadModel
import com.huawei.datasight.molap.load.MolapLoaderUtil
import org.apache.spark.sql.cubemodel.Partitioner
import com.huawei.datasight.molap.partition.api.impl.CSVFilePartitioner
import com.huawei.iweb.platform.logging.impl.StandardLogService
import com.huawei.datasight.spark.PartitionResult


class MolapDataPartition(rddId: Int, val idx: Int, @transient val tableSplit: TableSplit)
  extends Partition {

  val serializableHadoopSplit = new SerializableWritable[TableSplit](tableSplit)

  override def hashCode(): Int = 41 * (41 + rddId) + idx

  override val index: Int = idx
}

/**
  * This RDD class is used to  create splits the fact csv store to various partitions as per configuration  and compute each split in the respective node located in the
  * server.
  * .
  *
  * @author R00900208
  */
class MolapDataPartitionRDD[K, V](
                                   sc: SparkContext,
                                   //keyClass: KeyVal[K,V],
                                   results: PartitionResult[K, V],
                                   schemaName: String,
                                   cubeName: String,
                                   sourcePath: String,
                                   targetFolder: String,
                                   requiredColumns: Array[String],
                                   headers: String,
                                   delimiter: String,
                                   quoteChar: String,
                                   escapeChar: String,
                                   multiLine: Boolean,
                                   partitioner: Partitioner)
  extends RDD[(K, V)](sc, Nil)
    with Logging {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")
  
  /**
    * Create the split for each region server.
    */
  override def getPartitions: Array[Partition] = {
    val splits = MolapQueryUtil.getPartitionSplits(sourcePath, partitioner.nodeList, partitioner.partitionCount)
    //
    val result = new Array[Partition](splits.length)
    for (i <- 0 until result.length) {
      result(i) = new MolapDataPartition(id, i, splits(i))
    }
    result
  }

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }

  /**
    * It fires the query of respective co-processor and get the data and form the iterator.So here all the will be present to iterator physically.So
    * if co-processor returns big data then it may have memory issues.
    */
  override def compute(theSplit: Partition, context: TaskContext) = {
    val iter = new Iterator[(K, V)] {
      val split = theSplit.asInstanceOf[MolapDataPartition]
      StandardLogService.setThreadName(split.serializableHadoopSplit.value.getPartition().getUniqueID(), null);
      logInfo("Input split: " + split.serializableHadoopSplit.value)

      //      MolapProperties.getInstance().addProperty("molap.storelocation", dataPath+'_'+split.tableSplit.getPartition().getUniqueID());
      val csvPart = new CSVFilePartitioner(partitioner.partitionClass, sourcePath)
      csvPart.splitFile(schemaName, cubeName, split.serializableHadoopSplit.value.getPartition().getFilesPath, targetFolder, partitioner.nodeList.toList, partitioner.partitionCount, partitioner.partitionColumn, requiredColumns, delimiter, quoteChar, headers, escapeChar, multiLine)
      // Register an on-task-completion callback to close the input stream.
      context.addOnCompleteCallback(() => close())
      var finished = false

      override def hasNext: Boolean = {
        if (!finished) {
          finished = true
          finished
        }
        else {
          !finished
        }
      }

      override def next(): (K, V) = {
        //        if (!hasNext) {
        //          throw new java.util.NoSuchElementException("End of stream")
        //        }
        //        havePair = false
        //val row = new MolapKey(null)
        //val value = new MolapValue(null)
        results.getKey(partitioner.partitionCount, csvPart.isPartialSuccess)
      }

      private def close() {
        try {
          //          reader.close()
        } catch {
          case e: Exception => logWarning("Exception in RecordReader.close()", e)
        }
      }
    }
    iter
  }


  /**
    * Get the preferred locations where to lauch this task.
    */
  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[MolapDataPartition]
    val s = theSplit.serializableHadoopSplit.value.getLocations //.filter(_ != "localhost")
    logInfo("Host Name : " + s(0) + s.length)
    s
  }

  //  def getConf: Configuration = confBroadcast.value.value


}

