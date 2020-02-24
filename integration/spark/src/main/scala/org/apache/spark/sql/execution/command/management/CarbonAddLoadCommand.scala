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

package org.apache.spark.sql.execution.command.management

import java.io.File
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.{AnalysisException, CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.carbondata.execution.datasources.CarbonSparkDataSourceUtil.convertSparkToCarbonDataType
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.{Checker, MetadataCommand}
import org.apache.spark.sql.execution.strategy.MixedFormatHandler
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.types.StructType

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.{DataMapStoreManager, Segment}
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.indexstore.{PartitionSpec => CarbonPartitionSpec}
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{FileFormat, LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{BuildDataMapPostExecutionEvent, BuildDataMapPreExecutionEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadTablePostExecutionEvent, LoadTablePostStatusUpdateEvent, LoadTablePreExecutionEvent, LoadTablePreStatusUpdateEvent}
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.sdk.file.{Field, Schema}
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory.clearDataMapFiles


/**
 * User can add external data folder as a segment to a transactional table.
 * In case of external carbon data folder user no need to specify the format in options. But for
 * other formats like parquet user must specify the format=parquet in options.
 */
case class CarbonAddLoadCommand(
    databaseNameOp: Option[String],
    tableName: String,
    options: Map[String, String])
  extends MetadataCommand {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val relation = CarbonEnv
      .getInstance(sparkSession)
      .carbonMetaStore
      .lookupRelation(databaseNameOp, tableName)(sparkSession)
      .asInstanceOf[CarbonRelation]
    val tableSchema = StructType.fromAttributes(relation.output)
    val carbonTable = relation.carbonTable
    setAuditTable(carbonTable)
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }

    if (carbonTable.isChildTableForMV) {
      throw new MalformedCarbonCommandException("Unsupported operation on MV table")
    }
    // if insert overwrite in progress, do not allow add segment
    if (SegmentStatusManager.isOverwriteInProgressInTable(carbonTable)) {
      throw new ConcurrentOperationException(carbonTable, "insert overwrite", "delete segment")
    }

    val inputPath = options.getOrElse(
      "path", throw new UnsupportedOperationException("PATH is mandatory"))

    // If a path is already added then we should block the adding of the same path again.
    val allSegments = SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
    if (allSegments.exists(a => a.getPath != null && a.getPath.equalsIgnoreCase(inputPath))) {
      throw new AnalysisException(s"path already exists in table status file, can not add same " +
                                  s"segment path repeatedly: $inputPath")
    }

    val format = options.getOrElse("format", "carbondata")
    val isCarbonFormat = format.equalsIgnoreCase("carbondata") || format.equalsIgnoreCase("carbon")

    // If in the given location no carbon index files are found then we should throw an exception
    if (isCarbonFormat && SegmentFileStore.getListOfCarbonIndexFiles(inputPath).isEmpty) {
      throw new AnalysisException("CarbonIndex files not present in the location")
    }

    // infer schema and collect FileStatus for all partitions
    val (inputPathSchema, lastLevelDirFileMap) =
      MixedFormatHandler.collectInfo(sparkSession, options, inputPath)
    val inputPathCarbonFields = inputPathSchema.fields.map { field =>
      val dataType = convertSparkToCarbonDataType(field.dataType)
      new Field(field.name, dataType)
    }
    val carbonTableSchema = new Schema(tableSchema.fields.map { field =>
      val dataType = convertSparkToCarbonDataType(field.dataType)
      new Field(field.name, dataType)
    })

    // update schema if has partition
    val inputPathTableFields = if (carbonTable.isHivePartitionTable) {
      val partitions = options.getOrElse("partition",
        throw new AnalysisException(
          "partition option is required when adding segment to partition table")
      )
      // extract partition given by user, partition option should be form of "a:int, b:string"
      val partitionFields = partitions
        .split(",")
        .map { input =>
          if (input.nonEmpty) {
            val nameAndDataType = input.trim.toLowerCase.split(":")
            if (nameAndDataType.size == 2) {
              new Field(nameAndDataType(0), nameAndDataType(1))
            } else {
              throw new AnalysisException(s"invalid partition option: ${ options.toString() }")
            }
          }
        }
      // validate against the partition in carbon table
      val carbonTablePartition = getCarbonTablePartition(sparkSession)
      if (!partitionFields.sameElements(carbonTablePartition)) {
        throw new AnalysisException(
          s"""
             |Partition is not same. Carbon table partition is :
             |${carbonTablePartition.mkString(",")} and input segment partition is :
             |${partitionFields.mkString(",")}
             |""".stripMargin)
      }
      inputPathCarbonFields ++ partitionFields
    } else {
      if (options.contains("partition")) {
        throw new AnalysisException(
          s"Invalid option: partition, $tableName is not a partitioned table")
      }
      inputPathCarbonFields
    }

    // validate the schema including partition columns
    val schemaMatched = carbonTableSchema.getFields.forall { field =>
      inputPathTableFields.exists(_.equals(field))
    }
    if (!schemaMatched) {
      throw new AnalysisException(s"Schema is not same. Table schema is : " +
                                  s"${tableSchema} and segment schema is : ${inputPathSchema}")
    }

    // all validation is done, update the metadata accordingly
    if (carbonTable.isHivePartitionTable) {
      // for each partition in input path, create a new segment in carbon table
      val partitionSpecs = collectPartitionSpecList(
        sparkSession, carbonTable.getTablePath, inputPath, lastLevelDirFileMap.keys.toSeq)
      // check the collected partition from input segment path should comply to
      // partitions in carbon table
      val carbonTablePartition = getCarbonTablePartition(sparkSession)
      if (partitionSpecs.head.getPartitions.size() != carbonTablePartition.length) {
        throw new AnalysisException(
          s"""
             |input segment path does not comply to partitions in carbon table:
             |${carbonTablePartition.mkString(",")}
             |""".stripMargin)
      }
      partitionSpecs.foreach { partitionSpec =>
        val dataFiles = lastLevelDirFileMap.getOrElse(partitionSpec.getLocation.toString,
          throw new RuntimeException(s"partition folder not found: ${partitionSpec.getLocation}"))
        writeMetaForSegment(sparkSession, carbonTable, inputPath, Some(partitionSpec), dataFiles)
      }
    } else {
      writeMetaForSegment(sparkSession, carbonTable, inputPath)
    }

    Seq.empty
  }

  private def getCarbonTablePartition(sparkSession: SparkSession): Array[Field] = {
    sparkSession.sessionState.catalog
      .getTableMetadata(TableIdentifier(tableName, databaseNameOp))
      .partitionSchema
      .fields
      .map(f => new Field(f.name, convertSparkToCarbonDataType(f.dataType)))
  }

  /**
   * Write metadata for external segment, including table status file and segment file
   *
   * @param sparkSession spark session
   * @param carbonTable carbon table
   * @param segmentPath external segment path specified by user
   * @param partitionSpecOp partition info extracted from the path
   * @param partitionDataFiles all data files in the partition
   */
  private def writeMetaForSegment(
      sparkSession: SparkSession,
      carbonTable: CarbonTable,
      segmentPath: String,
      partitionSpecOp: Option[CarbonPartitionSpec] = None,
      partitionDataFiles: Seq[FileStatus] = Seq.empty
  ): Unit = {
    val model = new CarbonLoadModel
    model.setCarbonTransactionalTable(true)
    model.setCarbonDataLoadSchema(new CarbonDataLoadSchema(carbonTable))
    model.setDatabaseName(carbonTable.getDatabaseName)
    model.setTableName(carbonTable.getTableName)
    val operationContext = new OperationContext
    operationContext.setProperty("isLoadOrCompaction", false)
    val loadTablePreExecutionEvent: LoadTablePreExecutionEvent =
      new LoadTablePreExecutionEvent(
        carbonTable.getCarbonTableIdentifier,
        model)
    operationContext.setProperty("isOverwrite", false)
    OperationListenerBus.getInstance.fireEvent(loadTablePreExecutionEvent, operationContext)
    // Add pre event listener for index datamap
    val tableDataMaps = DataMapStoreManager.getInstance().getAllDataMap(carbonTable)
    val dataMapOperationContext = new OperationContext()
    if (tableDataMaps.size() > 0) {
      val dataMapNames: mutable.Buffer[String] =
        tableDataMaps.asScala.map(dataMap => dataMap.getDataMapSchema.getDataMapName)
      val buildDataMapPreExecutionEvent: BuildDataMapPreExecutionEvent =
        BuildDataMapPreExecutionEvent(
          sparkSession, carbonTable.getAbsoluteTableIdentifier, dataMapNames)
      OperationListenerBus.getInstance().fireEvent(buildDataMapPreExecutionEvent,
        dataMapOperationContext)
    }

    val newLoadMetaEntry = new LoadMetadataDetails
    model.setFactTimeStamp(CarbonUpdateUtil.readCurrentTime)
    CarbonLoaderUtil.populateNewLoadMetaEntry(newLoadMetaEntry,
      SegmentStatus.INSERT_IN_PROGRESS,
      model.getFactTimeStamp,
      false)
    newLoadMetaEntry.setPath(segmentPath)
    val format = options.getOrElse("format", "carbondata")
    val isCarbonFormat = format.equalsIgnoreCase("carbondata") ||
                         format.equalsIgnoreCase("carbon")
    if (!isCarbonFormat) {
      newLoadMetaEntry.setFileFormat(new FileFormat(format))
    }

    CarbonLoaderUtil.recordNewLoadMetadata(newLoadMetaEntry, model, true, false)
    val segment = new Segment(
      model.getSegmentId,
      SegmentFileStore.genSegmentFileName(
        model.getSegmentId,
        System.nanoTime().toString) + CarbonTablePath.SEGMENT_EXT,
      segmentPath,
      new util.HashMap[String, String](options.asJava))
    val writeSegment =
      if (isCarbonFormat) {
        SegmentFileStore.writeSegmentFile(carbonTable, segment)
      } else {
        SegmentFileStore.writeSegmentFileForOthers(
          carbonTable, segment, partitionSpecOp.orNull, partitionDataFiles.asJava)
      }

    // This event will trigger merge index job, only trigger it if it is carbon file
    if (isCarbonFormat) {
      operationContext.setProperty(
        carbonTable.getTableUniqueName + "_Segment",
        model.getSegmentId)
      val loadTablePreStatusUpdateEvent: LoadTablePreStatusUpdateEvent =
        new LoadTablePreStatusUpdateEvent(
          carbonTable.getCarbonTableIdentifier,
          model)
      OperationListenerBus.getInstance().fireEvent(loadTablePreStatusUpdateEvent, operationContext)
    }

    val success = if (writeSegment) {
      SegmentFileStore.updateTableStatusFile(
        carbonTable,
        model.getSegmentId,
        segment.getSegmentFileName,
        carbonTable.getCarbonTableIdentifier.getTableId,
        new SegmentFileStore(carbonTable.getTablePath, segment.getSegmentFileName),
        SegmentStatus.SUCCESS)
    } else {
      false
    }

    val postExecutionEvent = if (success) {
      val loadTablePostStatusUpdateEvent: LoadTablePostStatusUpdateEvent =
        new LoadTablePostStatusUpdateEvent(model)
      val commitComplete = try {
        OperationListenerBus.getInstance()
          .fireEvent(loadTablePostStatusUpdateEvent, operationContext)
        true
      } catch {
        case ex: Exception =>
          LOGGER.error("Problem while committing data maps", ex)
          false
      }
      commitComplete
    } else {
      success
    }

    if (!postExecutionEvent || !success) {
      CarbonLoaderUtil.updateTableStatusForFailure(model, "uniqueTableStatusId")
      LOGGER.info("********starting clean up**********")
      // delete segment is applicable for transactional table
      CarbonLoaderUtil.deleteSegment(model, model.getSegmentId.toInt)
      // delete corresponding segment file from metadata
      val segmentFile = CarbonTablePath.getSegmentFilesLocation(carbonTable.getTablePath) +
                        File.separator + segment.getSegmentFileName
      FileFactory.deleteFile(segmentFile)
      clearDataMapFiles(carbonTable, model.getSegmentId)
      LOGGER.info("********clean up done**********")
      LOGGER.error("Data load failed due to failure in table status updation.")
      throw new Exception("Data load failed due to failure in table status updation.")
    }
    DataMapStatusManager.disableAllLazyDataMaps(carbonTable)
    val loadTablePostExecutionEvent: LoadTablePostExecutionEvent =
      new LoadTablePostExecutionEvent(
        carbonTable.getCarbonTableIdentifier,
        model)
    OperationListenerBus.getInstance.fireEvent(loadTablePostExecutionEvent, operationContext)
    if (tableDataMaps.size() > 0) {
      val buildDataMapPostExecutionEvent = BuildDataMapPostExecutionEvent(sparkSession,
        carbonTable.getAbsoluteTableIdentifier, null, Seq(model.getSegmentId), false)
      OperationListenerBus.getInstance()
        .fireEvent(buildDataMapPostExecutionEvent, dataMapOperationContext)
    }
  }

  // extract partition column and value, for example, given
  // path1 = path/to/partition/a=1/b=earth
  // path2 = path/to/partition/a=2/b=moon
  // will extract a list of CarbonPartitionSpec:
  //   CarbonPartitionSpec {("a=1","b=earth"), "path/to/partition"}
  //   CarbonPartitionSpec {("a=2","b=moon"), "path/to/partition"}
  def collectPartitionSpecList(
      sparkSession: SparkSession,
      tablePath: String,
      inputPath: String,
      partitionPaths: Seq[String]
  ): Seq[CarbonPartitionSpec] = {
    partitionPaths.map { path =>
      try {
        val partitionOnlyPath = path.substring(inputPath.length + 1)
        val partitionColumnAndValue = partitionOnlyPath.split("/").toList.asJava
        new CarbonPartitionSpec(partitionColumnAndValue, path)
      } catch {
        case t: Throwable => throw new RuntimeException(s"invalid partition path: $path")
      }
    }
  }

  override protected def opName: String = "ADD SEGMENT WITH PATH"
}
