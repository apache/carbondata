


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
import com.huawei.unibi.molap.olap.MolapDef
import org.apache.spark.sql.cubemodel.Partitioner
import com.huawei.datasight.molap.load.MolapLoaderUtil


class MolapCachePartition(rddId: Int, val idx: Int, @transient val tableSplit: TableSplit)
  extends Partition {

  val serializableHadoopSplit = new SerializableWritable[TableSplit](tableSplit)

  override def hashCode(): Int = 41 * (41 + rddId) + idx

  override val index: Int = idx
}

/**
  * This RDD class is used to  create splits as per the region servers of Hbase  and compute each split in the respective node located in the same server by
  * using co-processor of Hbase.
  *
  * @author R00900208
  */
class MolapDataCacheRDD[K, V](
                               sc: SparkContext,
                               keyClass: KeyVal[K, V],
                               schema: MolapDef.Schema,
                               baseStoreLocation: String,
                               cubeName: String,
                               schemaName: String,
                               partitioner: Partitioner,
                               columinar: Boolean,
                               cubeCreationTime: Long)
  extends RDD[(K, V)](sc, Nil)
    with Logging {
  
  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  // A Hadoop Configuration can be about 10 KB, which is pretty big, so broadcast it
  //  private val confBroadcast = sc.broadcast(new SerializableWritable(conf))
  // private val serializableConf = new SerializableWritable(conf)

  private val jobtrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }


  /**
    * Create the split for each region server.
    */
  override def getPartitions: Array[Partition] = {

    //    val executor = MolapQueryUtil.getExecutor(molapQueryModel)
    //
    //    val aggModel = executor.getAggModels(molapQueryModel)

    val splits = MolapQueryUtil.getTableSplits(schemaName, cubeName, null, partitioner)
    //
    val result = new Array[Partition](splits.length)
    for (i <- 0 until result.length) {
      result(i) = new MolapCachePartition(id, i, splits(i))
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
      val currentRestructNumber = MolapLoaderUtil.getCurrentRestructFolder(schemaName, cubeName, schema)
      val split = theSplit.asInstanceOf[MolapCachePartition]
      logInfo("Input split: " + split.serializableHadoopSplit.value)

      if (columinar) {
        println("**************** Loading cube Columnar");
        MolapProperties.getInstance().addProperty("molap.is.columnar.storage", "true");
        MolapProperties.getInstance().addProperty("molap.dimension.split.value.in.columnar", "1");
        MolapProperties.getInstance().addProperty("molap.is.fullyfilled.bits", "true");
        MolapProperties.getInstance().addProperty("is.int.based.indexer", "true");
        MolapProperties.getInstance().addProperty("aggregate.columnar.keyblock", "true");
        MolapProperties.getInstance().addProperty("high.cardinality.value", "100000");
        MolapProperties.getInstance().addProperty("is.compressed.keyblock", "false");
        MolapProperties.getInstance().addProperty("molap.leaf.node.size", "120000");
      }
      MolapProperties.getInstance().addProperty("molap.storelocation", baseStoreLocation);
      MolapQueryUtil.createDataSource(currentRestructNumber, schema, null, split.serializableHadoopSplit.value.getPartition().getUniqueID(), null, null, null, cubeCreationTime);

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
    iter
  }


  /**
    * Get the preferred locations where to lauch this task.
    */
  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[MolapCachePartition]
    val s = theSplit.serializableHadoopSplit.value.getLocations //.filter(_ != "localhost")
    logInfo("Host Name : " + s(0) + s.length)
    s
  }

  //  def getConf: Configuration = confBroadcast.value.value


}

