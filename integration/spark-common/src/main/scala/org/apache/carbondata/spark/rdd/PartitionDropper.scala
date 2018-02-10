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

import org.apache.spark.sql.execution.command.{AlterPartitionModel, DropPartitionCallableModel}
import org.apache.spark.util.PartitionUtils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.spark.{AlterPartitionResultImpl, PartitionFactory}

object PartitionDropper {

  val logger = LogServiceFactory.getLogService(PartitionDropper.getClass.getName)

  def triggerPartitionDrop(dropPartitionCallableModel: DropPartitionCallableModel): Unit = {
    val alterPartitionModel = new AlterPartitionModel(dropPartitionCallableModel.carbonLoadModel,
      dropPartitionCallableModel.segmentId,
      dropPartitionCallableModel.oldPartitionIds,
      dropPartitionCallableModel.sqlContext
    )
    val partitionId = dropPartitionCallableModel.partitionId
    val oldPartitionIds = dropPartitionCallableModel.oldPartitionIds
    val dropWithData = dropPartitionCallableModel.dropWithData
    val carbonTable = dropPartitionCallableModel.carbonTable
    val dbName = carbonTable.getDatabaseName
    val tableName = carbonTable.getTableName
    val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
    val partitionInfo = carbonTable.getPartitionInfo(tableName)
    val partitioner = PartitionFactory.getPartitioner(partitionInfo)

    var finalDropStatus = false
    val bucketInfo = carbonTable.getBucketingInfo(tableName)
    val bucketNumber = bucketInfo match {
      case null => 1
      case _ => bucketInfo.getNumberOfBuckets
    }
    val partitionIndex = oldPartitionIds.indexOf(Integer.valueOf(partitionId))
    val targetPartitionId = partitionInfo.getPartitionType match {
      case PartitionType.RANGE => if (partitionIndex == oldPartitionIds.length - 1) {
        "0"
      } else {
        String.valueOf(oldPartitionIds(partitionIndex + 1))
      }
      case PartitionType.LIST => "0"
    }

    if (!dropWithData) {
      try {
        for (i <- 0 until bucketNumber) {
          val bucketId = i
          val rdd = new CarbonScanPartitionRDD(alterPartitionModel,
            absoluteTableIdentifier,
            Seq(partitionId, targetPartitionId),
            bucketId
          ).partitionBy(partitioner).map(_._2)

          val dropStatus = new AlterTableLoadPartitionRDD(alterPartitionModel,
            new AlterPartitionResultImpl(),
            Seq(partitionId),
            bucketId,
            absoluteTableIdentifier,
            rdd).collect()

          if (dropStatus.length == 0) {
            finalDropStatus = false
          } else {
            finalDropStatus = dropStatus.forall(_._2)
          }
          if (!finalDropStatus) {
            logger.audit(s"Drop Partition request failed for table " +
                         s"${ dbName }.${ tableName }")
            logger.error(s"Drop Partition request failed for table " +
                         s"${ dbName }.${ tableName }")
          }
        }

        if (finalDropStatus) {
          try {
            PartitionUtils.deleteOriginalCarbonFile(alterPartitionModel, absoluteTableIdentifier,
              Seq(partitionId, targetPartitionId).toList, dbName,
              tableName, partitionInfo)
          } catch {
            case e: IOException => sys.error(s"Exception while delete original carbon files " +
                                             e.getMessage)
          }
          logger.audit(s"Drop Partition request completed for table " +
                       s"${ dbName }.${ tableName }")
          logger.info(s"Drop Partition request completed for table " +
                      s"${ dbName }.${ tableName }")
        }
      } catch {
        case e: Exception => sys.error(s"Exception in dropping partition action: ${ e.getMessage }")
      }
    } else {
      PartitionUtils.deleteOriginalCarbonFile(alterPartitionModel, absoluteTableIdentifier,
        Seq(partitionId).toList, dbName, tableName, partitionInfo)
      logger.audit(s"Drop Partition request completed for table " +
                   s"${ dbName }.${ tableName }")
      logger.info(s"Drop Partition request completed for table " +
                  s"${ dbName }.${ tableName }")
    }
  }
}
