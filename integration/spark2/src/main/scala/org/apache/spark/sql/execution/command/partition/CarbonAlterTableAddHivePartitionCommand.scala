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
import org.apache.spark.sql.execution.command.{AlterTableAddPartitionCommand, AlterTableDropPartitionCommand, AlterTableModel, AtomicRunnableCommand}
import org.apache.spark.sql.optimizer.CarbonFilters

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.SegmentStatus
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{AlterTableMergeIndexEvent, OperationContext, OperationListenerBus, PostAlterTableHivePartitionCommandEvent, PreAlterTableHivePartitionCommandEvent}
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
    setAuditTable(table)
    setAuditInfo(Map("partition" -> partitionSpecsAndLocs.mkString(", ")))
    if (table.isHivePartitionTable) {
      if (table.isChildTableForMV) {
        throw new UnsupportedOperationException("Cannot add partition directly on child tables")
      }
      val partitionWithLoc = partitionSpecsAndLocs.filter(_._2.isDefined)
      if (partitionWithLoc.nonEmpty) {
        val partitionSpecs = partitionWithLoc.map{ case (part, location) =>
          new PartitionSpec(
            new util.ArrayList(part.map(p => p._1.toLowerCase + "=" + p._2).toList.asJava),
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
        val isSameSchema = indexToSchemas.asScala.exists{ case(key, columnSchemas) =>
          columnSchemas.asScala.exists { col =>
            tableColums.exists(p => p.getColumnUniqueId.equals(col.getColumnUniqueId))
          } && columnSchemas.size() == tableColums.length
        }
        if (!isSameSchema) {
          throw new UnsupportedOperationException(
            "Schema of index files located in location is not matching with current table schema")
        }
        val loadModel = new CarbonLoadModel
        val columnCompressor = table.getTableInfo.getFactTable.getTableProperties.asScala
          .getOrElse(CarbonCommonConstants.COMPRESSOR,
            CompressorFactory.getInstance().getCompressor.getName)
        loadModel.setColumnCompressor(columnCompressor)
        loadModel.setCarbonTransactionalTable(true)
        loadModel.setCarbonDataLoadSchema(new CarbonDataLoadSchema(table))
        // Create new entry in tablestatus file
        CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(loadModel, false)
        val newMetaEntry = loadModel.getCurrentLoadMetadataDetail
        val segmentFileName =
          SegmentFileStore.genSegmentFileName(
            loadModel.getSegmentId, String.valueOf(loadModel.getFactTimeStamp)) +
          CarbonTablePath.SEGMENT_EXT
        newMetaEntry.setSegmentFile(segmentFileName)
        val segmentsLoc = CarbonTablePath.getSegmentFilesLocation(table.getTablePath)
        CarbonUtil.checkAndCreateFolderWithPermission(segmentsLoc)
        val segmentPath = segmentsLoc + CarbonCommonConstants.FILE_SEPARATOR + segmentFileName
        SegmentFileStore.writeSegmentFile(segmentFile, segmentPath)
        CarbonLoaderUtil.populateNewLoadMetaEntry(
          newMetaEntry,
          SegmentStatus.SUCCESS,
          loadModel.getFactTimeStamp,
          true)
        // Add size to the entry
        CarbonLoaderUtil.addDataIndexSizeIntoMetaEntry(newMetaEntry, loadModel.getSegmentId, table)
        // Make the load as success in table status
        CarbonLoaderUtil.recordNewLoadMetadata(newMetaEntry, loadModel, false, false)

        // Normally, application will use Carbon SDK to write files into a partition folder, then
        // add the folder to partitioned carbon table.
        // If there are many threads writes to the same partition folder, there will be many
        // carbon index files, and it is not good for query performance since all index files
        // need to be read to spark driver.
        // So, here trigger to merge the index files by sending an event
        val alterTableModel = AlterTableModel(
          dbName = Some(table.getDatabaseName),
          tableName = table.getTableName,
          segmentUpdateStatusManager = None,
          compactionType = "", // to trigger index merge, this is not required
          factTimeStamp = Some(System.currentTimeMillis()),
          alterSql = null,
          customSegmentIds = Some(Seq(loadModel.getSegmentId).toList))
        val mergeIndexEvent = AlterTableMergeIndexEvent(sparkSession, table, alterTableModel)
        OperationListenerBus.getInstance.fireEvent(mergeIndexEvent, new OperationContext)
      }
    }
    Seq.empty[Row]
  }

  override protected def opName: String = "ADD HIVE PARTITION"
}
