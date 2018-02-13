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

import java.io.IOException

import org.apache.spark.sql.execution.command.{AlterPartitionModel, SplitPartitionCallableModel}
import org.apache.spark.util.PartitionUtils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.spark.{AlterPartitionResultImpl, PartitionFactory}

object PartitionSplitter {

  val logger = LogServiceFactory.getLogService(PartitionSplitter.getClass.getName)

  def triggerPartitionSplit(splitPartitionCallableModel: SplitPartitionCallableModel): Unit = {

     val alterPartitionModel = new AlterPartitionModel(splitPartitionCallableModel.carbonLoadModel,
       splitPartitionCallableModel.segmentId,
       splitPartitionCallableModel.oldPartitionIds,
       splitPartitionCallableModel.sqlContext
     )
     val partitionId = splitPartitionCallableModel.partitionId
     val carbonLoadModel = splitPartitionCallableModel.carbonLoadModel
     val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
     val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
     val tableName = carbonTable.getTableName
     val databaseName = carbonTable.getDatabaseName
     val bucketInfo = carbonTable.getBucketingInfo(tableName)
     var finalSplitStatus = false
     val bucketNumber = bucketInfo match {
       case null => 1
       case _ => bucketInfo.getNumOfRanges
     }
     val partitionInfo = carbonTable.getPartitionInfo(tableName)
     val partitioner = PartitionFactory.getPartitioner(partitionInfo)

     for (i <- 0 until bucketNumber) {
       val bucketId = i
       val rdd = new CarbonScanPartitionRDD(
         alterPartitionModel,
         absoluteTableIdentifier,
         Seq(partitionId),
         bucketId
       ).partitionBy(partitioner).map(_._2)

       val splitStatus = new AlterTableLoadPartitionRDD(alterPartitionModel,
         new AlterPartitionResultImpl(),
         Seq(partitionId),
         bucketId,
         absoluteTableIdentifier,
         rdd).collect()

       if (splitStatus.length == 0) {
         finalSplitStatus = false
       } else {
         finalSplitStatus = splitStatus.forall(_._2)
       }
       if (!finalSplitStatus) {
         logger.audit(s"Add/Split Partition request failed for table " +
                      s"${ databaseName }.${ tableName }")
         logger.error(s"Add/Split Partition request failed for table " +
                      s"${ databaseName }.${ tableName }")
       }
     }
     if (finalSplitStatus) {
       try {
         PartitionUtils.
           deleteOriginalCarbonFile(alterPartitionModel, absoluteTableIdentifier,
             Seq(partitionId).toList, databaseName, tableName, partitionInfo)
       } catch {
         case e: IOException => sys.error(s"Exception while delete original carbon files " +
         e.getMessage)
       }
       logger.audit(s"Add/Split Partition request completed for table " +
                    s"${ databaseName }.${ tableName }")
       logger.info(s"Add/Split Partition request completed for table " +
                   s"${ databaseName }.${ tableName }")
     }
  }
}
