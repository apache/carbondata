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
import scala.collection.mutable.ListBuffer

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
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.index.{IndexStoreManager, Segment}
import org.apache.carbondata.core.indexstore.{PartitionSpec => CarbonPartitionSpec}
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.datatype.Field
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.{CarbonUpdateUtil, SegmentUpdateDetails}
import org.apache.carbondata.core.reader.CarbonDeleteDeltaFileReaderImpl
import org.apache.carbondata.core.statusmanager.{FileFormat, LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.core.view.{MVSchema, MVStatus}
import org.apache.carbondata.events.{BuildIndexPostExecutionEvent, BuildIndexPreExecutionEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadTablePostExecutionEvent, LoadTablePostStatusUpdateEvent, LoadTablePreExecutionEvent, LoadTablePreStatusUpdateEvent}
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.sdk.file.Schema
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory.clearIndexFiles
import org.apache.carbondata.view.MVManagerInSpark


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
    val (tableSchema, carbonTable) = Checker.getSchemaAndTable(sparkSession, databaseNameOp,
      tableName)
    setAuditTable(carbonTable)
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }

    if (carbonTable.isMV) {
      throw new MalformedCarbonCommandException("Unsupported operation on MV table")
    }
    // if insert overwrite in progress, do not allow add segment
    if (SegmentStatusManager.isOverwriteInProgressInTable(carbonTable)) {
      throw new ConcurrentOperationException(carbonTable, "insert overwrite", "add segment")
    }

    var givenPath = options.getOrElse(
      "path", throw new UnsupportedOperationException("PATH is mandatory"))
    if (givenPath.length == 0) {
      throw new UnsupportedOperationException("PATH cannot be empty")
    }
    // remove file separator if already present
    if (givenPath.charAt(givenPath.length - 1) == '/') {
      givenPath = givenPath.substring(0, givenPath.length - 1)
    }
    val inputPath = givenPath

    // If a path is already added then we should block the adding of the same path again.
    val allSegments = SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
    // If the segment has been already loaded from the same path or the segment is already present
    // in the table and its status is SUCCESS orPARTIALLY_SUCCESS, throw an exception as we should
    // block the adding of the same path again.
    if (allSegments.exists(a => ((a.getPath != null && a.getPath.equalsIgnoreCase(inputPath)) ||
                                 CarbonTablePath.getSegmentPath(carbonTable.getTablePath,
                                   a.getLoadName).equalsIgnoreCase(inputPath)) &&
                                (a.getSegmentStatus == SegmentStatus.SUCCESS ||
                                 a.getSegmentStatus == SegmentStatus.LOAD_PARTIAL_SUCCESS))) {
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
    val (tableIndexes, indexOperationContext) = CommonLoadUtils.firePreLoadEvents(sparkSession,
      model,
      "",
      segmentPath,
      options.asJava,
      options.asJava,
      false,
      false,
      None,
      operationContext)

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
    val deltaFiles = FileFactory.getCarbonFile(segmentPath).listFiles(new CarbonFileFilter() {
      override def accept(file: CarbonFile): Boolean = file.getName
        .endsWith(CarbonCommonConstants.DELETE_DELTA_FILE_EXT)
    })
    val updateTimestamp = System.currentTimeMillis().toString
    var isUpdateStatusRequired = false
    if (deltaFiles.nonEmpty) {
      LOGGER.warn("Adding a modified load to the table. If there is any updated segment for this" +
        "load, please add updated segment also.")
      val blockNameToDeltaFilesMap =
        collection.mutable.Map[String, collection.mutable.ListBuffer[(CarbonFile, String)]]()
      deltaFiles.foreach { deltaFile =>
        val tmpDeltaFilePath = deltaFile.getAbsolutePath
          .replace(CarbonCommonConstants.WINDOWS_FILE_SEPARATOR,
            CarbonCommonConstants.FILE_SEPARATOR)
        val deltaFilePathElements = tmpDeltaFilePath.split(CarbonCommonConstants.FILE_SEPARATOR)
        if (deltaFilePathElements != null && deltaFilePathElements.nonEmpty) {
          val deltaFileName = deltaFilePathElements(deltaFilePathElements.length - 1)
          val blockName = CarbonTablePath.DataFileUtil
            .getBlockNameFromDeleteDeltaFile(deltaFileName)
          if (blockNameToDeltaFilesMap.contains(blockName)) {
            blockNameToDeltaFilesMap(blockName) += ((deltaFile, deltaFileName))
          } else {
            val deltaFileList = new ListBuffer[(CarbonFile, String)]()
            deltaFileList += ((deltaFile, deltaFileName))
            blockNameToDeltaFilesMap.put(blockName, deltaFileList)
          }
        }
      }
      val segmentUpdateDetails = new util.ArrayList[SegmentUpdateDetails]()
      val columnCompressor = CompressorFactory.getInstance.getCompressor.getName
      blockNameToDeltaFilesMap.foreach { entry =>
        val segmentUpdateDetail = new SegmentUpdateDetails()
        segmentUpdateDetail.setBlockName(entry._1)
        segmentUpdateDetail.setActualBlockName(
          entry._1 + CarbonCommonConstants.POINT + columnCompressor +
            CarbonCommonConstants.FACT_FILE_EXT)
        segmentUpdateDetail.setSegmentName(model.getSegmentId)
        val blockNameElements = entry._1.split(CarbonCommonConstants.HYPHEN)
        if (blockNameElements != null && blockNameElements.nonEmpty) {
          val segmentId = blockNameElements(blockNameElements.length - 1)
          // Segment ID in cases of SDK is null
          if (segmentId.equals("null")) {
            readAllDeltaFiles(entry._2, segmentUpdateDetail)
          } else {
            setValidDeltaFileAndDeletedRowCount(entry._2, segmentUpdateDetail)
          }
        }
        segmentUpdateDetails.add(segmentUpdateDetail)
      }
      CarbonUpdateUtil.updateSegmentStatus(segmentUpdateDetails,
        carbonTable,
        updateTimestamp,
        false,
        true)
      isUpdateStatusRequired = true
      newLoadMetaEntry.setUpdateDeltaStartTimestamp(updateTimestamp)
      newLoadMetaEntry.setUpdateDeltaEndTimestamp(updateTimestamp)
    }

    CarbonLoaderUtil.recordNewLoadMetadata(newLoadMetaEntry, model, true, false, updateTimestamp,
      isUpdateStatusRequired)
    val segment = new Segment(
      model.getSegmentId,
      SegmentFileStore.genSegmentFileName(
        model.getSegmentId,
        System.nanoTime().toString) + CarbonTablePath.SEGMENT_EXT,
      segmentPath,
      new util.HashMap[String, String](options.asJava))
    // This event will trigger merge index job, only trigger it if it is carbon file
    if (isCarbonFormat) {
      CarbonLoaderUtil.mergeIndexFilesInAddLoadSegment(carbonTable,
        model.getSegmentId,
        segmentPath,
        model.getFactTimeStamp.toString)
      // clear Block index Cache
      SegmentFileStore.clearBlockIndexCache(carbonTable, model.getSegmentId)
    }
    val writeSegment =
      if (isCarbonFormat) {
        SegmentFileStore.writeSegmentFile(carbonTable, segment)
      } else {
        SegmentFileStore.writeSegmentFileForOthers(
          carbonTable, segment, partitionSpecOp.orNull, partitionDataFiles.asJava)
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
          LOGGER.error("Problem while committing indexes", ex)
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
      CarbonLoaderUtil.deleteSegmentForFailure(model)
      // delete corresponding segment file from metadata
      val segmentFile = CarbonTablePath.getSegmentFilesLocation(carbonTable.getTablePath) +
                        File.separator + segment.getSegmentFileName
      FileFactory.deleteFile(segmentFile)
      clearIndexFiles(carbonTable, model.getSegmentId)
      LOGGER.info("********clean up done**********")
      LOGGER.error("Data load failed due to failure in table status update.")
      throw new Exception("Data load failed due to failure in table status update.")
    }
    MVManagerInSpark.disableMVOnTable(sparkSession, carbonTable)
    CommonLoadUtils.firePostLoadEvents(sparkSession,
      model,
      tableIndexes,
      indexOperationContext,
      carbonTable,
      operationContext)
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

  /**
   * If there are more than one deleteDelta File present  for a block. Then This method
   * will pick the deltaFile with highest timestamp, because the default threshold for horizontal
   * compaction is 1. It is assumed that threshold for horizontal compaction is not changed from
   * default value. So there will always be only one valid delete delta file present for a block.
   * It also sets the number of deleted rows for a segment.
   */
  def setValidDeltaFileAndDeletedRowCount(
      deleteDeltaFiles : ListBuffer[(CarbonFile, String)],
      segmentUpdateDetails : SegmentUpdateDetails) : Unit = {
    var maxDeltaStamp : Long = -1
    var deletedRowsCount : Long = 0
    var validDeltaFile : CarbonFile = null
    deleteDeltaFiles.foreach { deltaFile =>
      val currentFileTimestamp = CarbonTablePath.DataFileUtil
        .getTimeStampFromDeleteDeltaFile(deltaFile._2)
      if (currentFileTimestamp.toLong > maxDeltaStamp) {
        maxDeltaStamp = currentFileTimestamp.toLong
        validDeltaFile = deltaFile._1
      }
    }
    val blockDetails =
      new CarbonDeleteDeltaFileReaderImpl(validDeltaFile.getAbsolutePath).readJson()
    blockDetails.getBlockletDetails.asScala.foreach { blocklet =>
      deletedRowsCount = deletedRowsCount + blocklet.getDeletedRows.size()
    }
    segmentUpdateDetails.setDeleteDeltaStartTimestamp(maxDeltaStamp.toString)
    segmentUpdateDetails.setDeleteDeltaEndTimestamp(maxDeltaStamp.toString)
    segmentUpdateDetails.setDeletedRowsInBlock(deletedRowsCount.toString)
  }

  /**
   * As horizontal compaction not supported for SDK segments. So all delta files are valid
   */
  def readAllDeltaFiles(
      deleteDeltaFiles : ListBuffer[(CarbonFile, String)],
      segmentUpdateDetails : SegmentUpdateDetails) : Unit = {
    var minDeltaStamp : Long = System.currentTimeMillis()
    var maxDeltaStamp : Long = -1
    var deletedRowsCount : Long = 0
    deleteDeltaFiles.foreach { deltaFile =>
      val currentFileTimestamp = CarbonTablePath.DataFileUtil
        .getTimeStampFromDeleteDeltaFile(deltaFile._2)
      minDeltaStamp = Math.min(minDeltaStamp, currentFileTimestamp.toLong)
      maxDeltaStamp = Math.max(maxDeltaStamp, currentFileTimestamp.toLong)
      segmentUpdateDetails.addDeltaFileStamp(currentFileTimestamp)
      val blockDetails =
        new CarbonDeleteDeltaFileReaderImpl(deltaFile._1.getAbsolutePath).readJson()
      blockDetails.getBlockletDetails.asScala.foreach { blocklet =>
        deletedRowsCount = deletedRowsCount + blocklet.getDeletedRows.size()
      }
    }
    segmentUpdateDetails.setDeleteDeltaStartTimestamp(minDeltaStamp.toString)
    segmentUpdateDetails.setDeleteDeltaEndTimestamp(maxDeltaStamp.toString)
    segmentUpdateDetails.setDeletedRowsInBlock(deletedRowsCount.toString)
  }

  override protected def opName: String = "ADD SEGMENT WITH PATH"
}
