/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.spark.rdd

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.spark.{Partition, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.PartitionUtils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.PartitionInfo
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.processing.model.CarbonLoadModel
import org.apache.carbondata.processing.spliter.RowResultSpliterProcessor
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil
import org.apache.carbondata.spark.SplitResult
import org.apache.carbondata.spark.load.CarbonLoaderUtil

class AlterTableSplitPartitionRDD[K, V](
    sc: SparkContext,
    result: SplitResult[K, V],
    partitionIds: Seq[String],
    segmentId: String,
    bucketId: Int,
    carbonLoadModel: CarbonLoadModel,
    identifier: AbsoluteTableIdentifier,
    storePath: String,
    partitionInfo: PartitionInfo,
    oldPartitionIdList: List[Int],
    prev: RDD[Array[AnyRef]]) extends RDD[(K, V)](prev) {

    sc.setLocalProperty("spark.scheduler.pool", "DDL")
    sc.setLocalProperty("spark.job.interruptOnCancel", "true")

    var storeLocation: String = null
    var splitResult: String = null
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val databaseName = carbonTable.getDatabaseName
    val factTableName = carbonTable.getFactTableName

    override protected def getPartitions: Array[Partition] = firstParent[Array[AnyRef]].partitions

    override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
        val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
        val rows = firstParent[Array[AnyRef]].iterator(split, context).toList.asJava
        val iter = new Iterator[(K, V)] {
            val partitionId = partitionInfo.getPartitionId(split.index)
            val loadStartTime = CarbonUpdateUtil.readCurrentTime
            carbonLoadModel.setFactTimeStamp(loadStartTime)
            carbonLoadModel.setTaskNo(String.valueOf(partitionId))
            carbonLoadModel.setSegmentId(segmentId)
            carbonLoadModel.setPartitionId("0")
            val tempLocationKey = CarbonDataProcessorUtil
              .getTempStoreLocationKey(carbonLoadModel.getDatabaseName,
                  carbonLoadModel.getTableName,
                  carbonLoadModel.getTaskNo,
                  false,
                  true)
            // this property is used to determine whether temp location for carbon is inside
            // container temp dir or is yarn application directory.
            val carbonUseLocalDir = CarbonProperties.getInstance()
              .getProperty("carbon.use.local.dir", "false")

            if (carbonUseLocalDir.equalsIgnoreCase("true")) {

                val storeLocations = CarbonLoaderUtil.getConfiguredLocalDirs(SparkEnv.get.conf)
                if (null != storeLocations && storeLocations.nonEmpty) {
                    storeLocation = storeLocations(Random.nextInt(storeLocations.length))
                }
                if (storeLocation == null) {
                    storeLocation = System.getProperty("java.io.tmpdir")
                }
            } else {
                storeLocation = System.getProperty("java.io.tmpdir")
            }
            storeLocation = storeLocation + '/' + System.nanoTime() + '/' + split.index
            CarbonProperties.getInstance().addProperty(tempLocationKey, storeLocation)
            LOGGER.info(s"Temp storeLocation taken is $storeLocation")

            val tempStoreLoc = CarbonDataProcessorUtil.getLocalDataFolderLocation(databaseName,
                factTableName,
                carbonLoadModel.getTaskNo,
                "0",
                segmentId,
                false,
                true
            )

            val splitStatus = if (rows.isEmpty) {
                LOGGER.info("After repartition this split, NO target rows to write back.")
                true
            } else {
                val segmentProperties = PartitionUtils.getSegmentProperties(identifier, segmentId,
                    partitionIds.toList, oldPartitionIdList)
                val processor = new RowResultSpliterProcessor(
                    carbonTable,
                    carbonLoadModel,
                    segmentProperties,
                    tempStoreLoc,
                    bucketId
                )
                processor.execute(rows)
            }

            val splitResult = segmentId
            var finished = false

            override def hasNext: Boolean = {
                !finished
            }

            override def next(): (K, V) = {
                finished = true
                result.getKey(splitResult, splitStatus)
            }
        }
        iter
    }
}
