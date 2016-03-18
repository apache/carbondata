package com.huawei.datasight.spark.rdd

import com.huawei.datasight.molap.spark.util.MolapQueryUtil
import com.huawei.datasight.spark.KeyVal
import com.huawei.unibi.molap.engine.scanner.impl.{MolapKey, MolapValue}
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cubemodel.Partitioner

import scala.collection.JavaConversions.asScalaBuffer


class MolapDropAggregateTableRDD[K, V](
    sc: SparkContext,
    keyClass: KeyVal[K, V],
    schemaName: String,
    cubeName: String,
    partitioner: Partitioner)
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
      val split = theSplit.asInstanceOf[MolapLoadPartition]
      logInfo("Input split: " + split.serializableHadoopSplit.value)
      //TODO call MOLAP delete API

      // Register an on-task-completion callback to close the input stream.
      context.addOnCompleteCallback(() => close())
      var havePair = false
      var finished = false

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          finished = !false
          havePair = !finished
        }
        !finished
      }

      override def next(): (K, V) = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        val row = new MolapKey(null)
        val value = new MolapValue(null)
        keyClass.getKey(row, value)
      }

      private def close() {
        try {
          //          reader.close()
        } catch {
          case e: Exception => logWarning("Exception in RecordReader.close()", e)
        }
      }
    }
    println("********Dropping Aggregate Table***************");
    iter
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[MolapLoadPartition]
    val s = theSplit.serializableHadoopSplit.value.getLocations //.filter(_ != "localhost")
    logInfo("Host Name : " + s(0) + s.length)
    s
  }
}

