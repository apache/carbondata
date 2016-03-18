


package com.huawei.datasight.spark.rdd

import com.huawei.datasight.molap.partition.api.impl.CSVFilePartitioner
import com.huawei.datasight.molap.spark.splits.TableSplit
import com.huawei.datasight.molap.spark.util.MolapQueryUtil
import com.huawei.datasight.spark.PartitionResult
import com.huawei.iweb.platform.logging.impl.StandardLogService
import org.apache.spark.{Logging, Partition, SerializableWritable, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cubemodel.Partitioner

import scala.collection.JavaConversions._


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
  extends RDD[(K, V)](sc, Nil) with Logging {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")

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

  override def compute(theSplit: Partition, context: TaskContext) = {
    val iter = new Iterator[(K, V)] {
      val split = theSplit.asInstanceOf[MolapDataPartition]
      StandardLogService.setThreadName(split.serializableHadoopSplit.value.getPartition().getUniqueID(), null);
      logInfo("Input split: " + split.serializableHadoopSplit.value)

      val csvPart = new CSVFilePartitioner(partitioner.partitionClass, sourcePath)
      csvPart.splitFile(schemaName, cubeName, split.serializableHadoopSplit.value.getPartition().getFilesPath, targetFolder, partitioner.nodeList.toList, partitioner.partitionCount, partitioner.partitionColumn, requiredColumns, delimiter, quoteChar, headers, escapeChar, multiLine)

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
        results.getKey(partitioner.partitionCount, csvPart.isPartialSuccess)
      }
    }
    iter
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[MolapDataPartition]
    val s = theSplit.serializableHadoopSplit.value.getLocations //.filter(_ != "localhost")
    logInfo("Host Name : " + s(0) + s.length)
    s
  }
}

