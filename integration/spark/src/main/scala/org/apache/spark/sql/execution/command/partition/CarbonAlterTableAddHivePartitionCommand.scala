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
import scala.util.control.Breaks.{break, breakable}

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.execution.command.{AlterTableAddPartitionCommand, AlterTableDropPartitionCommand, AlterTableModel, AtomicRunnableCommand}
import org.apache.spark.sql.execution.command.management.CommonLoadUtils
import org.apache.spark.sql.optimizer.CarbonFilters

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{FileFormat, SegmentStatus}
import org.apache.carbondata.core.util.{CarbonUtil, ObjectSerializationUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{withEvents, AlterTableMergeIndexEvent, OperationContext, OperationListenerBus, PostAlterTableHivePartitionCommandEvent, PreAlterTableHivePartitionCommandEvent}
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
      if (table.isMV) {
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
      withEvents(PreAlterTableHivePartitionCommandEvent(sparkSession, table),
        PostAlterTableHivePartitionCommandEvent(sparkSession, table)) {
        AlterTableAddPartitionCommand(tableName, partitionSpecsAndLocs, ifNotExists).run(
          sparkSession)
      }
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
        val loadModel = new CarbonLoadModel
        val columnCompressor = table.getTableInfo.getFactTable.getTableProperties.asScala
          .getOrElse(CarbonCommonConstants.COMPRESSOR,
            CompressorFactory.getInstance().getCompressor.getName)
        loadModel.setColumnCompressor(columnCompressor)
        loadModel.setCarbonTransactionalTable(true)
        loadModel.setCarbonDataLoadSchema(new CarbonDataLoadSchema(table))
        // create operationContext to fire load events
        val operationContext: OperationContext = new OperationContext
        var hasIndexFiles = false
        breakable {
          partitionSpecsAndLocsTobeAdded.asScala.foreach(partitionSpecAndLocs => {
            val location = partitionSpecAndLocs.getLocation.toString
            val carbonFile = FileFactory.getCarbonFile(location)
            val listFiles: Array[CarbonFile] = carbonFile.listFiles(new CarbonFileFilter() {
              override def accept(file: CarbonFile): Boolean = {
                file.getName.endsWith(CarbonTablePath.INDEX_FILE_EXT) ||
                file.getName.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)
              }
            })
            if (listFiles != null && listFiles.length > 0) {
              hasIndexFiles = true
              break()
            }
          })
       }
      // only if the partition path has some index files,
      // make entry in table status and trigger merge index event.
      if (hasIndexFiles) {
        // Create new entry in tablestatus file
        CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(loadModel, false)
        // Normally, application will use Carbon SDK to write files into a partition folder, then
        // add the folder to partitioned carbon table.
        // If there are many threads writes to the same partition folder, there will be many
        // carbon index files, and it is not good for query performance since all index files
        // need to be read to spark driver.
        // So, here trigger to merge the index files by sending an event
        val customSegmentIds = if (loadModel.getCurrentLoadMetadataDetail
          .getFileFormat
          .equals(FileFormat.ROW_V1)) {
          Some(Seq("").toList)
        } else {
          Some(Seq(loadModel.getSegmentId).toList)
        }
        operationContext.setProperty("partitionPath",
          partitionSpecsAndLocsTobeAdded.asScala.map(_.getLocation.toString).asJava)
        operationContext.setProperty("carbon.currentpartition",
          ObjectSerializationUtil.convertObjectToString(partitionSpecsAndLocsTobeAdded))
        val alterTableModel = AlterTableModel(
          dbName = Some(table.getDatabaseName),
          tableName = table.getTableName,
          segmentUpdateStatusManager = None,
          compactionType = "", // to trigger index merge, this is not required
          factTimeStamp = Some(System.currentTimeMillis()),
          customSegmentIds = customSegmentIds)
        val mergeIndexEvent = AlterTableMergeIndexEvent(sparkSession, table, alterTableModel)
        OperationListenerBus.getInstance.fireEvent(mergeIndexEvent, operationContext)
        val newMetaEntry = loadModel.getCurrentLoadMetadataDetail
        val segmentFileName =
          SegmentFileStore.genSegmentFileName(
            loadModel.getSegmentId, String.valueOf(loadModel.getFactTimeStamp)) +
          CarbonTablePath.SEGMENT_EXT
        newMetaEntry.setSegmentFile(segmentFileName)
        // set path to identify it as external added partition
        newMetaEntry.setPath(partitionSpecsAndLocsTobeAdded.asScala
          .map(_.getLocation.toString).mkString(","))
        val segmentsLoc = CarbonTablePath.getSegmentFilesLocation(table.getTablePath)
        CarbonUtil.checkAndCreateFolderWithPermission(segmentsLoc)
        val segmentPath = segmentsLoc + CarbonCommonConstants.FILE_SEPARATOR + segmentFileName
        val segmentFile = SegmentFileStore.getSegmentFileForPhysicalDataPartitions(table
          .getTablePath,
          partitionSpecsAndLocsTobeAdded)
        if (segmentFile != null) {
          val indexToSchemas = SegmentFileStore.getSchemaFiles(segmentFile, table.getTablePath)
          val tableColumns = table.getTableInfo.getFactTable.getListOfColumns.asScala
          val isSameSchema = indexToSchemas.asScala.exists { case (key, columnSchemas) =>
            columnSchemas.asScala.exists { col =>
              tableColumns.exists(p => p.getColumnUniqueId.equals(col.getColumnUniqueId))
            } && columnSchemas.size() == tableColumns.length
          }
          if (!isSameSchema) {
            throw new UnsupportedOperationException(
              "Schema of index files located in location is not matching with current table schema")
          }
          val (tableIndexes, indexOperationContext) = CommonLoadUtils.firePreLoadEvents(
            sparkSession = sparkSession,
            carbonLoadModel = loadModel,
            uuid = "",
            factPath = "",
            new util.HashMap[String, String](),
            new util.HashMap[String, String](),
            isOverwriteTable = false,
            isDataFrame = false,
            updateModel = None,
            operationContext = operationContext)
          SegmentFileStore.writeSegmentFile(segmentFile, segmentPath)
          CarbonLoaderUtil.populateNewLoadMetaEntry(
            newMetaEntry,
            SegmentStatus.SUCCESS,
            loadModel.getFactTimeStamp,
            true)
          // Add size to the entry
          CarbonLoaderUtil.addDataIndexSizeIntoMetaEntry(newMetaEntry,
            loadModel.getSegmentId, table)
          // Make the load as success in table status
          CarbonLoaderUtil.recordNewLoadMetadata(newMetaEntry, loadModel, false, false)
          // fire event to load data to materialized views
          CommonLoadUtils.firePostLoadEvents(sparkSession,
            loadModel,
            tableIndexes,
            indexOperationContext,
            table,
            operationContext)
        }
      }
    }
    Seq.empty[Row]
  }

  override protected def opName: String = "ADD HIVE PARTITION"
}
