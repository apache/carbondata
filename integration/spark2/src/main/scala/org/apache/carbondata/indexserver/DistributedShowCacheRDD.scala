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
package org.apache.carbondata.indexserver

import scala.collection.JavaConverters._

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.DistributionUtil

import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapFactory
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider
import org.apache.carbondata.hadoop.CarbonInputSplit
import org.apache.carbondata.spark.rdd.CarbonRDD

class DistributedShowCacheRDD(@transient private val ss: SparkSession, tableName: String)
  extends CarbonRDD[String](ss, Nil) {

  val executorsList: Array[String] = DistributionUtil.getExecutors(ss.sparkContext).flatMap {
    case (host, executors) =>
      executors.map {
        executor => s"executor_${host}_$executor"
      }
  }.toArray

  override protected def internalGetPartitions: Array[Partition] = {
    executorsList.zipWithIndex.map {
      case (executor, idx) =>
        // create a dummy split for each executor to accumulate the cache size.
        val dummySplit = new CarbonInputSplit()
        dummySplit.setLocation(Array(executor))
        new DataMapRDDPartition(id, idx, List(dummySplit), Array(executor))
    }
  }

  override def internalCompute(split: Partition, context: TaskContext): Iterator[String] = {
    val dataMaps = DataMapStoreManager.getInstance().getAllDataMaps.asScala
    val tableList = tableName.split(",")
    val iterator = dataMaps.collect {
      case (table, tableDataMaps) if table.isEmpty ||
                                     (tableName.nonEmpty && tableList.contains(table)) =>
        val sizeAndIndexLengths = tableDataMaps.asScala
          .map { dataMap =>
            // if datamap name is null then it means that the TableDataMap is of BlockletDatamap
            // type.
            val dataMapName = if (dataMap.getDataMapFactory.isInstanceOf[BlockletDataMapFactory]) {
              table
            } else {
              dataMap.getDataMapSchema.getRelationIdentifier.getDatabaseName + "_" + dataMap
              .getDataMapSchema.getDataMapName
            }
            s"${ dataMapName }:${ dataMap.getDataMapFactory.getCacheSize }:${
              dataMap.getDataMapSchema.getProviderName} "
          }
        sizeAndIndexLengths
    }.flatten.toIterator
    iterator
  }
}
