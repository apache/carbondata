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

import org.apache.spark.sql.{AnalysisException, CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.carbondata.execution.datasources.CarbonSparkDataSourceUtil.convertSparkToCarbonDataType
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
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{FileFormat, LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{BuildDataMapPostExecutionEvent, BuildDataMapPreExecutionEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadTablePostExecutionEvent, LoadTablePostStatusUpdateEvent, LoadTablePreExecutionEvent, LoadTablePreStatusUpdateEvent}
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.sdk.file.{Field, Schema}
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory.clearDataMapFiles
import org.apache.carbondata.spark.util.CarbonScalaUtil


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

    if (carbonTable.getTableInfo.getFactTable.getListOfColumns.asScala.exists(
      c => c.hasEncoding(Encoding.DICTIONARY) && !c.hasEncoding(Encoding.DIRECT_DICTIONARY))) {
      throw new MalformedCarbonCommandException(
        "Unsupported operation on global dictionary columns table")
    }
    if (carbonTable.isChildTable || carbonTable.isChildDataMap) {
      throw new MalformedCarbonCommandException("Unsupported operation on MV/Pre-aggrergated table")
    }
    // if insert overwrite in progress, do not allow add segment
    if (SegmentStatusManager.isOverwriteInProgressInTable(carbonTable)) {
      throw new ConcurrentOperationException(carbonTable, "insert overwrite", "delete segment")
    }
    val segmentPath = options.getOrElse(
      "path", throw new UnsupportedOperationException("PATH is manadatory"))

    val segSchema = MixedFormatHandler.getSchema(sparkSession, options, segmentPath)

    val segCarbonSchema = new Schema(segSchema.fields.map { field =>
      val dataType = convertSparkToCarbonDataType(field.dataType)
      new Field(field.name, dataType)
    })

    val tableCarbonSchema = new Schema(tableSchema.fields.map { field =>
      val dataType = convertSparkToCarbonDataType(field.dataType)
      new Field(field.name, dataType)
    })


    if (!tableCarbonSchema.getFields.forall(f => segCarbonSchema.getFields.exists(_.equals(f)))) {
      throw new AnalysisException(s"Schema is not same. Table schema is : " +
                                  s"${tableSchema} and segment schema is : ${segSchema}")
    }

    val model = new CarbonLoadModel
    model.setCarbonTransactionalTable(true)
    model.setCarbonDataLoadSchema(new CarbonDataLoadSchema(carbonTable))
    model.setDatabaseName(carbonTable.getDatabaseName)
    model.setTableName(carbonTable.getTableName)
    val operationContext = new OperationContext
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
        new BuildDataMapPreExecutionEvent(sparkSession,
          carbonTable.getAbsoluteTableIdentifier, dataMapNames)
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
    val isCarbonFormat = format.equals("carbondata") || format.equals("carbon")
    if (!isCarbonFormat) {
      newLoadMetaEntry.setFileFormat(new FileFormat(format))
    }

    CarbonLoaderUtil.recordNewLoadMetadata(newLoadMetaEntry, model, true, false)
    val segment = new Segment(model.getSegmentId,
      SegmentFileStore.genSegmentFileName(
        model.getSegmentId,
        System.nanoTime().toString) + CarbonTablePath.SEGMENT_EXT,
      segmentPath,
      new util.HashMap[String, String](options.asJava))
    val writeSegment =
      if (isCarbonFormat) {
        SegmentFileStore.writeSegmentFile(carbonTable, segment)
      } else {
        SegmentFileStore.writeSegmentFileForOthers(carbonTable, segment)
      }

    operationContext.setProperty(carbonTable.getTableUniqueName + "_Segment",
      model.getSegmentId)
    val loadTablePreStatusUpdateEvent: LoadTablePreStatusUpdateEvent =
      new LoadTablePreStatusUpdateEvent(
        carbonTable.getCarbonTableIdentifier,
        model)
    OperationListenerBus.getInstance().fireEvent(loadTablePreStatusUpdateEvent, operationContext)

    val success = if (writeSegment) {
       SegmentFileStore.updateSegmentFile(
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
      FileFactory.deleteFile(segmentFile, FileFactory.getFileType(segmentFile))
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
    Seq.empty
  }



  override protected def opName: String = "ADD SEGMENT WITH PATH"
}
