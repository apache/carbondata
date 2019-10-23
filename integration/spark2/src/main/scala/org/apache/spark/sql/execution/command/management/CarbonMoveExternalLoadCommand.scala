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

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.CarbonCopyFilesRDD
import org.apache.spark.sql.{AnalysisException, CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.{Checker, DataCommand}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonTablePath


/**
 * User can add external data folder as a segment to a transactional table.
 * In case of external carbon data folder user no need to specify the format in options. But for
 * other formats like parquet user must specify the format=parquet in options.
 */
case class CarbonMoveExternalLoadCommand(
    databaseNameOp: Option[String],
    tableName: String,
    segmentName: String)
  extends DataCommand {

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    setAuditTable(carbonTable)
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }

    // if insert overwrite in progress, do not allow add segment
    if (SegmentStatusManager.isOverwriteInProgressInTable(carbonTable)) {
      throw new ConcurrentOperationException(carbonTable, "insert overwrite", "move segment")
    }

    val details =
      SegmentStatusManager.readLoadMetadata(
        CarbonTablePath.getMetadataPath(carbonTable.getTablePath))
    val detail = details.find(_.getLoadName.equalsIgnoreCase(segmentName))
      .getOrElse(throw new AnalysisException(s"Segment name $segmentName doesn't exist"))

    if (StringUtils.isEmpty(detail.getPath)) {
      throw new AnalysisException(s"Segment $segmentName is not a external segment")
    }

    if (!(detail.getSegmentStatus == SegmentStatus.SUCCESS ||
        detail.getSegmentStatus == SegmentStatus.LOAD_PARTIAL_SUCCESS)) {
      throw new AnalysisException(s"Segment $segmentName status is ${detail.getSegmentStatus}")
    }

    val store = new SegmentFileStore(carbonTable.getTablePath, detail.getSegmentFile)
    val location = store.getLocationMap.asScala.head._1
    val destLocation = CarbonTablePath.getSegmentPath(carbonTable.getTablePath, segmentName)
    val configuration = sparkSession.sessionState.newHadoopConf()
    FileFactory.mkdirs(destLocation, configuration)
    val segment = new Segment(segmentName,
      detail.getSegmentFile,
      location,
      store.getSegmentFile.getOptions)

    new CarbonCopyFilesRDD(sparkSession, segment, destLocation).collect()
    store.getSegmentFile.getOptions.put("path", destLocation)
    val segmentDest = new Segment(segmentName,
      SegmentFileStore.genSegmentFileName(detail.getLoadName,
        System.nanoTime().toString) + CarbonTablePath.SEGMENT_EXT,
      destLocation,
      store.getSegmentFile.getOptions)

    val isSuccess =
      SegmentFileStore.writeSegmentFile(carbonTable, segmentDest)

    if (isSuccess) {
      SegmentFileStore.updateSegmentFile(
        carbonTable,
        segmentName,
        segmentDest.getSegmentFileName,
        carbonTable.getCarbonTableIdentifier.getTableId,
        new SegmentFileStore(carbonTable.getTablePath, segmentDest.getSegmentFileName),
        SegmentStatus.SUCCESS, true)
      FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(location, configuration))
    } else {
      throw new AnalysisException("Move segment with dest path failed.")
    }
    Seq.empty
  }

  override protected def opName: String =
    "ALTER SEGMENT ON TABLE tableName MOVE SEGMENT 'segmentID'"
}
