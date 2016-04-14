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


package org.carbondata.integration.spark.rdd

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, Partition, SerializableWritable, SparkContext, TaskContext}
import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.iterator.CarbonIterator
import org.carbondata.core.carbon.CarbonDef
import org.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.carbondata.integration.spark.KeyVal
import org.carbondata.integration.spark.splits.TableSplit
import org.carbondata.integration.spark.util.{CarbonQueryUtil, CarbonSparkInterFaceLogEvent}
import org.carbondata.query.datastorage.InMemoryTableStore
import org.carbondata.query.executer.CarbonQueryExecutorModel
import org.carbondata.query.querystats.{PartitionDetail, PartitionStatsCollector}
import org.carbondata.query.result.RowResult

import scala.collection.JavaConversions._

class CarbonPartition(rddId: Int, val index: Int, @transient val tableSplit: TableSplit)
  extends Partition {

  val serializableHadoopSplit = new SerializableWritable[TableSplit](tableSplit)

  override def hashCode(): Int = 41 * (41 + rddId) + index
}

/**
 * This RDD is used to perform query.
 */
class CarbonDataRDD[K, V](
                          sc: SparkContext,
                          carbonQueryModel: CarbonQueryExecutorModel,
                          schema: CarbonDef.Schema,
                          dataPath: String,
                          keyClass: KeyVal[K, V],
                          @transient conf: Configuration,
                          splits: Array[TableSplit],
                          columinar: Boolean,
                          cubeCreationTime: Long,
                          schemaLastUpdatedTime: Long,
                          baseStoreLocation: String)
  extends RDD[(K, V)](sc, Nil) with Logging {

  private val jobtrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  override def getPartitions: Array[Partition] = {
    val result = new Array[Partition](splits.length)
    for (i <- 0 until result.length) {
      result(i) = new CarbonPartition(id, i, splits(i))
    }
    result
  }

  override def compute(theSplit: Partition, context: TaskContext) = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass().getName());
    var cubeUniqueName: String = ""
    var levelCacheKeys: scala.collection.immutable.List[String] = Nil
    val iter = new Iterator[(K, V)] {
      var rowIterator: CarbonIterator[RowResult] = _
      var partitionDetail: PartitionDetail = _
      var partitionStatsCollector: PartitionStatsCollector = _
      var queryStartTime: Long = 0
      try {
        //Below code is To collect statistics for partition
        val split = theSplit.asInstanceOf[CarbonPartition]
        val part = split.serializableHadoopSplit.value.getPartition().getUniqueID()
        partitionStatsCollector = PartitionStatsCollector.getInstance
        partitionDetail = new PartitionDetail(part)
        partitionStatsCollector.addPartitionDetail(carbonQueryModel.getQueryId, partitionDetail)
        cubeUniqueName = carbonQueryModel.getCube().getSchemaName() + '_' + part + '_' + carbonQueryModel.getCube().getOnlyCubeName() + '_' + part
        val metaDataPath: String = carbonQueryModel.getCube().getMetaDataFilepath()
        var currentRestructNumber = CarbonUtil.checkAndReturnCurrentRestructFolderNumber(metaDataPath, "RS_", false)
        if (-1 == currentRestructNumber) {
          currentRestructNumber = 0
        }
        queryStartTime = System.currentTimeMillis
        carbonQueryModel.setPartitionId(part)

        logInfo("Input split: " + split.serializableHadoopSplit.value)

        if (part != null) {
          val carbonPropertiesFilePath = System.getProperty("carbon.properties.filepath", null)
          logInfo("*************************" + carbonPropertiesFilePath)
          if (null == carbonPropertiesFilePath) {
            System.setProperty("carbon.properties.filepath", System.getProperty("user.dir") + '/' + "conf" + '/' + "carbon.properties");
          }
          val listOfAllLoadFolders = carbonQueryModel.getListOfAllLoadFolder
          CarbonProperties.getInstance().addProperty("carbon.storelocation", baseStoreLocation);
          CarbonProperties.getInstance().addProperty("carbon.cache.used", "false");
          val cube = InMemoryTableStore.getInstance.loadCubeMetadataIfRequired(schema, schema.cubes(0), part, schemaLastUpdatedTime)
          val queryScopeObject = CarbonQueryUtil.createDataSource(currentRestructNumber, schema, cube, part, listOfAllLoadFolders, carbonQueryModel.getFactTable(), dataPath, cubeCreationTime, carbonQueryModel.getLoadMetadataDetails)
          carbonQueryModel.setQueryScopeObject(queryScopeObject)
          if (InMemoryTableStore.getInstance.isLevelCacheEnabled()) {
            levelCacheKeys = CarbonQueryUtil.validateAndLoadRequiredSlicesInMemory(carbonQueryModel, listOfAllLoadFolders, cubeUniqueName).toList
          }

          logInfo("IF Columnar: " + columinar)
          CarbonProperties.getInstance().addProperty("carbon.is.columnar.storage", "true");
          CarbonProperties.getInstance().addProperty("carbon.dimension.split.value.in.columnar", "1");
          CarbonProperties.getInstance().addProperty("carbon.is.fullyfilled.bits", "true");
          CarbonProperties.getInstance().addProperty("is.int.based.indexer", "true");
          CarbonProperties.getInstance().addProperty("aggregate.columnar.keyblock", "true");
          CarbonProperties.getInstance().addProperty("high.cardinality.value", "100000");
          CarbonProperties.getInstance().addProperty("is.compressed.keyblock", "false");
          CarbonProperties.getInstance().addProperty("carbon.leaf.node.size", "120000");

          carbonQueryModel.setCube(cube)
          carbonQueryModel.setOutLocation(dataPath)
        }
        CarbonQueryUtil.updateDimensionWithNoDictionaryVal(schema, carbonQueryModel)

        if (CarbonQueryUtil.isQuickFilter(carbonQueryModel)) {
          rowIterator = CarbonQueryUtil.getQueryExecuter(carbonQueryModel.getCube(), carbonQueryModel.getFactTable(), carbonQueryModel.getQueryScopeObject).executeDimension(carbonQueryModel);
        } else {
          rowIterator = CarbonQueryUtil.getQueryExecuter(carbonQueryModel.getCube(), carbonQueryModel.getFactTable(), carbonQueryModel.getQueryScopeObject).execute(carbonQueryModel);
        }
      } catch {
        case e: Exception =>
          LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, e)
          updateCubeAndLevelCacheStatus(levelCacheKeys)
          if (null != e.getMessage) {
            sys.error("Exception occurred in query execution :: " + e.getMessage)
          } else {
            sys.error("Exception occurred in query execution.Please check logs.")
          }
      }

      def updateCubeAndLevelCacheStatus(levelCacheKeys: scala.collection.immutable.List[String]) = {
        levelCacheKeys.foreach { key =>
          InMemoryTableStore.getInstance.updateLevelAccessCountInLRUCache(key)
        }
      }

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
      val partAcc = carbonQueryModel.getPartitionAccumulator
      partAcc.add(partitionDetail)
      partitionStatsCollector.removePartitionDetail(carbonQueryModel.getQueryId)
      println("*************************** Total Time Taken to execute the query in Carbon Side: " +
        (System.currentTimeMillis - queryStartTime))
    }
    iter
  }


  /**
   * Get the preferred locations where to launch this task.
   */
  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[CarbonPartition]
    theSplit.serializableHadoopSplit.value.getLocations.filter(_ != "localhost")
  }
}
