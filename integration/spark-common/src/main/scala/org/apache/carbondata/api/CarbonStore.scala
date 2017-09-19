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

package org.apache.carbondata.api

import java.lang.Long
import java.text.SimpleDateFormat

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.types.TimestampType

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.rdd.DataManagementFunc

object CarbonStore {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def showSegments(
      dbName: String,
      tableName: String,
      limit: Option[String],
      tableFolderPath: String,
      configuration: Configuration): Seq[Row] = {
    val loadMetadataDetailsArray =
      SegmentStatusManager.readLoadMetadata(configuration, tableFolderPath)
    if (loadMetadataDetailsArray.nonEmpty) {
      val parser = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP)
      var loadMetadataDetailsSortedArray = loadMetadataDetailsArray.sortWith { (l1, l2) =>
        java.lang.Double.parseDouble(l1.getLoadName) > java.lang.Double.parseDouble(l2.getLoadName)
      }
      if (limit.isDefined) {
        loadMetadataDetailsSortedArray = loadMetadataDetailsSortedArray
          .filter(load => load.getVisibility.equalsIgnoreCase("true"))
        val limitLoads = limit.get
        try {
          val lim = Integer.parseInt(limitLoads)
          loadMetadataDetailsSortedArray = loadMetadataDetailsSortedArray.slice(0, lim)
        } catch {
          case _: NumberFormatException => sys.error(s" Entered limit is not a valid Number")
        }
      }

      loadMetadataDetailsSortedArray
          .filter(_.getVisibility.equalsIgnoreCase("true"))
          .map { load =>
            Row(
              load.getLoadName,
              load.getLoadStatus,
              new java.sql.Timestamp(load.getLoadStartTime),
              new java.sql.Timestamp(load.getLoadEndTime)
            )
          }.toSeq
    } else {
      Seq.empty
    }
  }

  def cleanFiles(
      dbName: String,
      tableName: String,
      storePath: String,
      carbonTable: CarbonTable,
      forceTableClean: Boolean,
      configuration: Configuration): Unit = {
    LOGGER.audit(s"The clean files request has been received for $dbName.$tableName")
    try {
      DataManagementFunc.cleanFiles(dbName, tableName, storePath, carbonTable, forceTableClean,
        configuration)
      LOGGER.audit(s"Clean files operation is success for $dbName.$tableName.")
    } catch {
      case ex: Exception =>
        sys.error(ex.getMessage)
    }
    Seq.empty
  }

  // validates load ids
  private def validateLoadIds(loadids: Seq[String]): Unit = {
    if (loadids.isEmpty) {
      val errorMessage = "Error: Segment id(s) should not be empty."
      throw new MalformedCarbonCommandException(errorMessage)
    }
  }

  def deleteLoadById(
      loadids: Seq[String],
      dbName: String,
      tableName: String,
      carbonTable: CarbonTable,
      configuration: Configuration): Unit = {

    LOGGER.audit(s"Delete segment by Id request has been received for $dbName.$tableName")
    validateLoadIds(loadids)

    val path = carbonTable.getMetaDataFilepath

    try {
      val invalidLoadIds = SegmentStatusManager.updateDeletionStatus(
        carbonTable.getAbsoluteTableIdentifier, loadids.asJava, path, configuration).asScala
      if (invalidLoadIds.isEmpty) {
        LOGGER.audit(s"Delete segment by Id is successfull for $dbName.$tableName.")
      } else {
        sys.error(s"Delete segment by Id is failed. Invalid ID is: ${invalidLoadIds.mkString(",")}")
      }
    } catch {
      case ex: Exception =>
        sys.error(ex.getMessage)
    }
    Seq.empty
  }

  def deleteLoadByDate(
      timestamp: String,
      dbName: String,
      tableName: String,
      carbonTable: CarbonTable,
      configuration: Configuration): Unit = {
    LOGGER.audit(s"Delete segment by Id request has been received for $dbName.$tableName")

    val time = validateTimeFormat(timestamp)
    val path = carbonTable.getMetaDataFilepath

    try {
      val invalidLoadTimestamps =
        SegmentStatusManager.updateDeletionStatus(
          carbonTable.getAbsoluteTableIdentifier,
          timestamp,
          path,
          time,
          configuration).asScala
      if (invalidLoadTimestamps.isEmpty) {
        LOGGER.audit(s"Delete segment by date is successful for $dbName.$tableName.")
      } else {
        sys.error("Delete segment by date is failed. No matching segment found.")
      }
    } catch {
      case ex: Exception =>
        sys.error(ex.getMessage)
    }
  }

  // this function is for test only
  def isSegmentValid(
      dbName: String,
      tableName: String,
      storePath: String,
      segmentId: String,
      configuration: Configuration): Boolean = {
    val identifier = AbsoluteTableIdentifier.from(storePath, dbName, tableName)
    val validAndInvalidSegments: SegmentStatusManager.ValidAndInvalidSegmentsInfo =
      new SegmentStatusManager(identifier, configuration).getValidAndInvalidSegments
    return validAndInvalidSegments.getValidSegments.contains(segmentId)
  }

  private def validateTimeFormat(timestamp: String): Long = {
    val timeObj = Cast(Literal(timestamp), TimestampType).eval()
    if (null == timeObj) {
      val errorMessage = "Error: Invalid load start time format: " + timestamp
      throw new MalformedCarbonCommandException(errorMessage)
    }
    timeObj.asInstanceOf[Long]
  }

}
