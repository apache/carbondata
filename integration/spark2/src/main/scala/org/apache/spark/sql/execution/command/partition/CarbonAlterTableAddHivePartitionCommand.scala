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

package org.apache.spark.sql.execution.command.partition

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.execution.command.{AlterTableAddPartitionCommand, AlterTableDropPartitionCommand, AtomicRunnableCommand}
import org.apache.spark.sql.optimizer.CarbonFilters

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{SegmentDetailVO, SegmentManager, SegmentManagerHelper, SegmentStatus}
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{OperationContext, OperationListenerBus, PostAlterTableHivePartitionCommandEvent, PreAlterTableHivePartitionCommandEvent}
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.util.CarbonLoaderUtil

/**
 * Adding the partition to the hive and create a new segment if the location has data.
 *
 */
case class CarbonAlterTableAddHivePartitionCommand(
    tableName: TableIdentifier,
    partitionSpecsAndLocs: Seq[(TablePartitionSpec, Option[String])],
    ifNotExists: Boolean)
  extends AtomicRunnableCommand {

  var partitionSpecsAndLocsTobeAdded : util.List[PartitionSpec] = _
  var table: CarbonTable = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    table = CarbonEnv.getCarbonTable(tableName)(sparkSession)
    if (table.isHivePartitionTable) {
      if (table.isChildDataMap) {
        throw new UnsupportedOperationException("Cannot add partition directly on aggregate tables")
      }
      val partitionWithLoc = partitionSpecsAndLocs.filter(_._2.isDefined)
      if (partitionWithLoc.nonEmpty) {
        val partitionSpecs = partitionWithLoc.map{ case (part, location) =>
          new PartitionSpec(
            new util.ArrayList(part.map(p => p._1 + "=" + p._2).toList.asJava),
            location.get)
        }
        // Get all the partitions which are not already present in hive.
        val currParts = CarbonFilters.getCurrentPartitions(sparkSession, tableName).get
        partitionSpecsAndLocsTobeAdded =
          new util.ArrayList(partitionSpecs.filterNot { part =>
          currParts.exists(p => part.equals(p))
        }.asJava)
      }
      val operationContext = new OperationContext
      val preAlterTableHivePartitionCommandEvent = PreAlterTableHivePartitionCommandEvent(
        sparkSession,
        table)
      OperationListenerBus.getInstance()
        .fireEvent(preAlterTableHivePartitionCommandEvent, operationContext)
      AlterTableAddPartitionCommand(tableName, partitionSpecsAndLocs, ifNotExists).run(sparkSession)
      val postAlterTableHivePartitionCommandEvent = PostAlterTableHivePartitionCommandEvent(
        sparkSession,
        table)
      OperationListenerBus.getInstance()
        .fireEvent(postAlterTableHivePartitionCommandEvent, operationContext)
    } else {
      throw new UnsupportedOperationException(
        "Cannot add partition directly on non partitioned table")
    }
    Seq.empty[Row]
  }


  override def undoMetadata(sparkSession: SparkSession, exception: Exception): Seq[Row] = {
    AlterTableDropPartitionCommand(
      tableName,
      partitionSpecsAndLocs.map(_._1),
      ifExists = true,
      purge = false,
      retainData = true).run(sparkSession)
    val msg = s"Got exception $exception when processing data of add partition." +
              "Dropping partitions to the metadata"
    LogServiceFactory.getLogService(this.getClass.getCanonicalName).error(msg)
    Seq.empty[Row]
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    // Partitions with physical data should be registered to as a new segment.
    if (partitionSpecsAndLocsTobeAdded != null && partitionSpecsAndLocsTobeAdded.size() > 0) {
      val segmentFile = SegmentFileStore.getSegmentFileForPhysicalDataPartitions(table.getTablePath,
        partitionSpecsAndLocsTobeAdded)
      if (segmentFile != null) {
        val indexToSchemas = SegmentFileStore.getSchemaFiles(segmentFile, table.getTablePath)
        val tableColums = table.getTableInfo.getFactTable.getListOfColumns.asScala
        var isSameSchema = indexToSchemas.asScala.exists{ case(key, columnSchemas) =>
          columnSchemas.asScala.exists { col =>
            tableColums.exists(p => p.getColumnUniqueId.equals(col.getColumnUniqueId))
          } && columnSchemas.size() == tableColums.length
        }
        if (!isSameSchema) {
          throw new UnsupportedOperationException(
            "Schema of index files located in location is not matching with current table schema")
        }
        val loadModel = new CarbonLoadModel
        loadModel.setCarbonTransactionalTable(true)
        loadModel.setCarbonDataLoadSchema(new CarbonDataLoadSchema(table))
        // Create new entry in tablestatus file
        val segmentVO =
          SegmentManagerHelper
          .createSegmentVO(
            loadModel.getSegmentId,
            SegmentStatus.INSERT_IN_PROGRESS,
            loadModel.getFactTimeStamp)
        val newSegment = new SegmentManager()
          .createNewSegment(loadModel.getCarbonDataLoadSchema.getCarbonTable
            .getAbsoluteTableIdentifier, segmentVO)
        loadModel.setCurrentDetailVO(newSegment)
        val detailVO = new SegmentDetailVO().setSegmentId(loadModel.getSegmentId)
        val segmentFileName =
          SegmentFileStore.genSegmentFileName(
            loadModel.getSegmentId, String.valueOf(loadModel.getFactTimeStamp)) +
          CarbonTablePath.SEGMENT_EXT
        detailVO.setSegmentFileName(segmentFileName)
        val segmentsLoc = CarbonTablePath.getSegmentFilesLocation(table.getTablePath)
        CarbonUtil.checkAndCreateFolderWithPermission(segmentsLoc)
        val segmentPath = segmentsLoc + CarbonCommonConstants.FILE_SEPARATOR + segmentFileName
        SegmentFileStore.writeSegmentFile(segmentFile, segmentPath)
        detailVO.setStatus(
          SegmentStatus.SUCCESS.toString).setLoadEndTime(System.currentTimeMillis())
        // Add size to the entry
        CarbonLoaderUtil.addDataIndexSizeIntoMetaEntry(detailVO, loadModel.getSegmentId, table)
        // Make the load as success in table status
        new SegmentManager().commitLoadSegment(table.getAbsoluteTableIdentifier, detailVO)
      }
    }
    Seq.empty[Row]
  }

}
