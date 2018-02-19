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
import org.apache.carbondata.core.statusmanager.SegmentStatus
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
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
      AlterTableAddPartitionCommand(tableName, partitionSpecsAndLocs, ifNotExists).run(sparkSession)
    }
    Seq.empty[Row]
  }


  override def undoMetadata(sparkSession: SparkSession, exception: Exception): Seq[Row] = {
    AlterTableDropPartitionCommand(tableName, partitionSpecsAndLocs.map(_._1), true, false, true)
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
        val loadModel = new CarbonLoadModel
        loadModel.setCarbonDataLoadSchema(new CarbonDataLoadSchema(table))
        // Create new entry in tablestatus file
        CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(loadModel, false)
        val newMetaEntry = loadModel.getCurrentLoadMetadataDetail
        val segmentFileName =
          loadModel.getSegmentId + "_" + loadModel.getFactTimeStamp + CarbonTablePath.SEGMENT_EXT
        newMetaEntry.setSegmentFile(segmentFileName)
        val segmentsLoc = CarbonTablePath.getSegmentFilesLocation(table.getTablePath)
        CarbonUtil.checkAndCreateFolder(segmentsLoc)
        val segmentPath = segmentsLoc + CarbonCommonConstants.FILE_SEPARATOR + segmentFileName
        new SegmentFileStore().writeSegmentFile(segmentFile, segmentPath)
        CarbonLoaderUtil.populateNewLoadMetaEntry(
          newMetaEntry,
          SegmentStatus.SUCCESS,
          loadModel.getFactTimeStamp,
          true)
        // Add size to the entry
        CarbonLoaderUtil.addDataIndexSizeIntoMetaEntry(newMetaEntry, loadModel.getSegmentId, table)
        // Make the load as success in table status
        CarbonLoaderUtil.recordNewLoadMetadata(newMetaEntry, loadModel, false, false)
      }
    }
    Seq.empty[Row]
  }

}
