package com.huawei.datasight.spark.rdd

import com.huawei.datasight.molap.spark.util.MolapQueryUtil
import com.huawei.datasight.spark.KeyVal
import com.huawei.unibi.molap.engine.datastorage.InMemoryCubeStore
import com.huawei.unibi.molap.engine.scanner.impl.{MolapKey, MolapValue}
import com.huawei.unibi.molap.metadata.MolapMetadata
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cubemodel.Partitioner

import scala.collection.JavaConversions._

class MolapDropCubeRDD[K, V](
    sc: SparkContext,
    keyClass: KeyVal[K, V],
    schemaName: String,
    cubeName: String,
    partitioner: Partitioner)
  extends RDD[(K, V)](sc, Nil) with Logging {
  
  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  override def getPartitions: Array[Partition] = {
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

      val partitionCount = partitioner.partitionCount
      for (a <- 0 until partitionCount) {
        val cubeUniqueName = schemaName + "_" + a + "_" + cubeName + "_" + a
        val cube = MolapMetadata.getInstance().getCube(cubeUniqueName)
        if (InMemoryCubeStore.getInstance().getCubeNames().contains(cubeUniqueName)) {
          InMemoryCubeStore.getInstance().clearCache(cubeUniqueName)
          val tables = cube.getMetaTableNames()
          tables.foreach { tableName =>
            val tabelUniqueName = cubeUniqueName + '_' + tableName
            InMemoryCubeStore.getInstance().clearTableAndCurrentRSMap(tabelUniqueName)
          }
        }
      }

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
    }
    iter
  }
}

