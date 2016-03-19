/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */




package com.huawei.datasight.spark.rdd

import java.text.SimpleDateFormat
import java.util.ArrayList
import java.util.HashMap
import java.util.Map
import java.util.Date
import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.Logging
import org.apache.spark.Partition
import org.apache.spark.SerializableWritable
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import com.huawei.datasight.molap.spark.splits.TableSplit
import com.huawei.datasight.molap.spark.util.MolapQueryUtil
import com.huawei.datasight.spark.KeyVal
import com.huawei.unibi.molap.engine.executer.MolapQueryExecutorModel
import com.huawei.unibi.molap.engine.scanner.impl.MolapKey
import com.huawei.unibi.molap.engine.scanner.impl.MolapValue
import com.huawei.unibi.molap.metadata.MolapMetadata
import com.huawei.unibi.molap.olap.MolapDef
import com.huawei.unibi.molap.util.MolapProperties
import com.huawei.unibi.molap.engine.executer
import com.huawei.unibi.molap.iterator.MolapIterator
import com.huawei.unibi.molap.engine.result.RowResult
import com.huawei.unibi.molap.engine.querystats.{PartitionStatsCollector, PartitionDetail}
import com.huawei.unibi.molap.util.MolapUtil
import com.huawei.iweb.platform.logging.impl.StandardLogService
import com.huawei.datasight.molap.spark.util.MolapSparkInterFaceLogEvent
import com.huawei.iweb.platform.logging.LogServiceFactory
import com.huawei.unibi.molap.engine.datastorage.InMemoryCubeStore
import com.huawei.datasight.molap.spark.util.MolapQueryUtil



class MolapPartition(rddId: Int, val index: Int, @transient val tableSplit: TableSplit)
  extends Partition {

  val serializableHadoopSplit = new SerializableWritable[TableSplit](tableSplit)

  override def hashCode(): Int = 41 * (41 + rddId) + index
}

/**
  * This RDD class is used to  create splits as per the region servers of Hbase  and compute each split in the respective node located in the same server by
  * using co-processor of Hbase.
  *
  * @author R00900208
  */
class MolapDataRDD[K, V](
                          sc: SparkContext,
                          molapQueryModel: MolapQueryExecutorModel,
                          schema: MolapDef.Schema,
                          dataPath: String,
                          keyClass: KeyVal[K, V],
                          @transient conf: Configuration,
                          splits: Array[TableSplit],
                          columinar: Boolean,
                          cubeCreationTime: Long,
                          schemaLastUpdatedTime: Long,
                          baseStoreLocation: String)
  extends RDD[(K, V)](sc, Nil)
    with Logging {

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

    //    val splits = MolapQueryUtil.getTableSplits(molapQueryModel.getFactTable())
    //
    val result = new Array[Partition](splits.length)
    for (i <- 0 until result.length) {
      result(i) = new MolapPartition(id, i, splits(i))
    }
    result
  }

  /**
    * It fires the query of respective co-processor and get the data and form the iterator.So here all the will be present to iterator physically.So
    * if co-processor returns big data then it may have memory issues.
    */
  override def compute(theSplit: Partition, context: TaskContext) = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass().getName());
    var cubeUniqueName: String = ""
    var levelCacheKeys: scala.collection.immutable.List[String] = Nil
    val iter = new Iterator[(K, V)] {
      var rowIterator: MolapIterator[RowResult] = _
      var partitionDetail: PartitionDetail = _
      var partitionStatsCollector: PartitionStatsCollector = _
      var queryStartTime: Long = 0
      try {
        //Below code is To collect statistics for partition
        val split = theSplit.asInstanceOf[MolapPartition]
        val part = split.serializableHadoopSplit.value.getPartition().getUniqueID()
        partitionStatsCollector = PartitionStatsCollector.getInstance
        partitionDetail = new PartitionDetail(part)
        partitionStatsCollector.addPartitionDetail(molapQueryModel.getQueryId, partitionDetail)
        cubeUniqueName = molapQueryModel.getCube().getSchemaName() + '_' + part + '_' + molapQueryModel.getCube().getOnlyCubeName() + '_' + part
        val metaDataPath: String = molapQueryModel.getCube().getMetaDataFilepath()
        var currentRestructNumber = MolapUtil.checkAndReturnCurrentRestructFolderNumber(metaDataPath, "RS_", false)
        if (-1 == currentRestructNumber) {
          currentRestructNumber = 0
        }
        queryStartTime = System.currentTimeMillis
        molapQueryModel.setPartitionId(part)

        logInfo("Input split: " + split.serializableHadoopSplit.value)

        if (part != null) {
          val molapPropertiesFilePath = System.getProperty("molap.properties.filepath", null)
          logInfo("*************************" + molapPropertiesFilePath)
          if (null == molapPropertiesFilePath) {
            System.setProperty("molap.properties.filepath", System.getProperty("user.dir") + '/' + "conf" + '/' + "molap.properties");
          }
          val listOfLoadFolders = MolapQueryUtil.getSliceLoads(molapQueryModel, dataPath, part)
          val listOfUpdatedLoadFolders = molapQueryModel.getListOfValidUpdatedSlices
          val listOfAllLoadFolders = MolapQueryUtil.getListOfSlices(molapQueryModel.getLoadMetadataDetails)
          MolapProperties.getInstance().addProperty("molap.storelocation", baseStoreLocation);
          MolapProperties.getInstance().addProperty("molap.cache.used", "false");
          val cube = InMemoryCubeStore.getInstance.loadCubeMetadataIfRequired(schema, schema.cubes(0), part, schemaLastUpdatedTime)
          MolapQueryUtil.createDataSource(currentRestructNumber, schema, cube, part, listOfLoadFolders, listOfUpdatedLoadFolders, molapQueryModel.getFactTable(), dataPath, cubeCreationTime)
          if (InMemoryCubeStore.getInstance.isLevelCacheEnabled()) {
            levelCacheKeys = MolapQueryUtil.validateAndLoadRequiredSlicesInMemory(molapQueryModel, listOfAllLoadFolders, cubeUniqueName).toList
          }
          //          if (columinar) {
          logInfo("IF Columnar: " + columinar)
          MolapProperties.getInstance().addProperty("molap.is.columnar.storage", "true");
          MolapProperties.getInstance().addProperty("molap.dimension.split.value.in.columnar", "1");
          MolapProperties.getInstance().addProperty("molap.is.fullyfilled.bits", "true");
          MolapProperties.getInstance().addProperty("is.int.based.indexer", "true");
          MolapProperties.getInstance().addProperty("aggregate.columnar.keyblock", "true");
          MolapProperties.getInstance().addProperty("high.cardinality.value", "100000");
          MolapProperties.getInstance().addProperty("is.compressed.keyblock", "false");
          MolapProperties.getInstance().addProperty("molap.leaf.node.size", "120000");
          
          //          }

          molapQueryModel.setCube(cube)
          molapQueryModel.setOutLocation(dataPath)
        }
     MolapQueryUtil.updateDimensionWithHighCardinalityVal(schema,molapQueryModel)
        
     if(MolapQueryUtil.isQuickFilter(molapQueryModel)){
         rowIterator = MolapQueryUtil.getQueryExecuter(molapQueryModel.getCube(), molapQueryModel.getFactTable()).executeDimension(molapQueryModel);
        }else{
        rowIterator = MolapQueryUtil.getQueryExecuter(molapQueryModel.getCube(), molapQueryModel.getFactTable()).execute(molapQueryModel);
        }
      }
      catch {
        case e: Exception =>
          LOGGER.error(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, e)
          updateCubeAndLevelCacheStatus(levelCacheKeys)
          if (null != e.getMessage)
            sys.error("Exception occurred in query execution :: " + e.getMessage)
          else
            sys.error("Exception occurred in query execution.Please check logs.")
      }

      def updateCubeAndLevelCacheStatus(levelCacheKeys: scala.collection.immutable.List[String]) = {
        levelCacheKeys.foreach { key =>
          InMemoryCubeStore.getInstance.updateLevelAccessCountInLRUCache(key)
        }
      }

      //Create iterators for key and value.
      //      val iter1 = {if(molapQueryModel.gethIterator().getKeys() == null) new ArrayList[MolapKey]().iterator() else molapQueryModel.gethIterator().getKeys().iterator()};
      //      val iter2 = {if(molapQueryModel.gethIterator().getValues() == null)  new ArrayList[MolapValue]().iterator() else molapQueryModel.gethIterator().getValues().iterator()}

      // Register an on-task-completion callback to close the input stream.
      context.addOnCompleteCallback(() => close())
      var havePair = false
      var finished = false

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          finished = !rowIterator.hasNext()
          //          finished = !iter2.hasNext()
          havePair = !finished
        }
        if (finished) {
          updateCubeAndLevelCacheStatus(levelCacheKeys)
        }
        !finished
      }

      override def next(): (K, V) = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        val row = rowIterator.next()
        val key = row.getKey()
        val value = row.getValue()
        keyClass.getKey(key, value)
      }

      //merging partition stats to accumulator
      val partAcc = molapQueryModel.getPartitionAccumulator
      partAcc.add(partitionDetail)
      partitionStatsCollector.removePartitionDetail(molapQueryModel.getQueryId)
      println("*************************** Total Time Taken to execute the query in Molap Side: " + (System.currentTimeMillis - queryStartTime));

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
    val theSplit = split.asInstanceOf[MolapPartition]
    theSplit.serializableHadoopSplit.value.getLocations.filter(_ != "localhost")
    //    theSplit.serializableHadoopSplit.value.getLocations.filter(_ != "localhost")


  }

  //  def getConf: Configuration = confBroadcast.value.value


}
