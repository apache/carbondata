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

package org.apache.spark.sql.execution.command.mutation

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.strategy.MixedFormatHandler
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.hive.HiveSessionCatalog
import org.apache.spark.storage.StorageLevel

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.features.TableOperation
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.view.{MVSchema, MVStatus}
import org.apache.carbondata.events.{Event, OperationContext, OperationListenerBus, UpdateTablePostEvent}
import org.apache.carbondata.view.MVManagerInSpark


/**
 * Util for IUD common function
 */
object IUDCommonUtil {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def tryHorizontalCompaction(sparkSession: SparkSession,
      carbonTable: CarbonTable,
      updatedSegmentList: Set[String]): Unit = {
    var hasCompactionException = false
    var compactTimestamp = ""
    try {
      HorizontalCompaction.tryHorizontalCompaction(
        sparkSession, carbonTable, updatedSegmentList)
    } catch {
      case e: HorizontalCompactionException =>
        LOGGER.error(
          "Update operation passed. Exception in Horizontal Compaction. Please check logs." + e)
        // In case of failure , clean all related delta files
        compactTimestamp = e.compactionTimeStamp.toString
        hasCompactionException = true
    } finally {
      if (hasCompactionException) {
        CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, compactTimestamp)
      }
    }
  }

  def refreshMVandIndex(sparkSession: SparkSession,
      carbonTable: CarbonTable, operationContext: OperationContext, event: Event): Unit = {
    if (CarbonProperties.getInstance().isMVEnabled) {
      var hasMaintainMVException = false
      val viewManager = MVManagerInSpark.get(sparkSession)
      var viewSchemas: util.List[MVSchema] = new util.ArrayList
      try {
        // Truncate materialized views on the current table.
        viewSchemas = viewManager.getSchemasOnTable(carbonTable)
        if (!viewSchemas.isEmpty) {
          viewManager.onTruncate(viewSchemas)
        }
        // Load materialized views on the current table.
        OperationListenerBus.getInstance.fireEvent(event, operationContext)
      } catch {
        case e: Exception =>
          hasMaintainMVException = true
          LOGGER.error("Maintain MV in Update operation failed. Please check logs." + e)
      } finally {
        if (hasMaintainMVException) {
          viewManager.setStatus(viewSchemas, MVStatus.DISABLED)
        }
      }
    }
  }

  def uniqueValueCheck(dataset: Dataset[Row]): Unit = {
    val ds = dataset.select(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID)
      .groupBy(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID)
      .count()
      .select("count")
      .filter(col("count") > lit(1))
      .limit(1)
      .collect()
    // tupleId represents the source rows that are going to get replaced.
    // If same tupleId appeared more than once means key has more than one value to replace.
    // which is undefined behavior.
    if (ds.length > 0 && ds(0).getLong(0) > 1) {
      throw new UnsupportedOperationException(
        " update cannot be supported for 1 to N mapping, as more than one value present " +
          "for the update key")
    }
  }

  def checkPreconditionsForDelete(sparkSession: SparkSession,
      logicalPlan: LogicalPlan,
      carbonTable: CarbonTable): Unit = {
    IUDCommonUtil.checkIfSegmentListIsSet(sparkSession, logicalPlan)
    IUDCommonUtil.checkIsTransactionTable(carbonTable)
    IUDCommonUtil.checkIsHeterogeneousSegmentTable(carbonTable)
    IUDCommonUtil.checkIsIndexedTable(carbonTable, TableOperation.DELETE)
    IUDCommonUtil.checkIsLoadInProgressInTable(carbonTable)
  }

  def checkPreconditionsForUpdate(sparkSession: SparkSession,
      logicalPlan: LogicalPlan,
      carbonTable: CarbonTable,
      columns: List[String]): Unit = {
    IUDCommonUtil.checkIfSegmentListIsSet(sparkSession, logicalPlan)
    IUDCommonUtil.checkIsTransactionTable(carbonTable)
    IUDCommonUtil.checkIfSpatialColumnsExists(carbonTable, columns)
    IUDCommonUtil.checkIfColumnWithComplexTypeExists(carbonTable, columns)
    IUDCommonUtil.checkIsHeterogeneousSegmentTable(carbonTable)
    IUDCommonUtil.checkIsIndexedTable(carbonTable, TableOperation.UPDATE)
    IUDCommonUtil.checkIsLoadInProgressInTable(carbonTable)
  }

  def checkIsHeterogeneousSegmentTable(carbonTable: CarbonTable): Unit = {
    if (MixedFormatHandler.otherFormatSegmentsExist(carbonTable.getMetadataPath)) {
      throw new MalformedCarbonCommandException(
        s"Unsupported operation on table containing mixed format segments")
    }
  }

  def checkIsIndexedTable(carbonTable: CarbonTable, operation: TableOperation): Unit = {
    if (!carbonTable.canAllow(carbonTable, operation)) {
      throw new MalformedCarbonCommandException(
        "update/delete operation is not supported for index")
    }
  }

  def checkIsLoadInProgressInTable(carbonTable: CarbonTable): Unit = {
    if (SegmentStatusManager.isLoadInProgressInTable(carbonTable)) {
      throw new ConcurrentOperationException(carbonTable, "loading", "data update")
    }
  }

  def checkIsTransactionTable(carbonTable: CarbonTable): Unit = {
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }
  }

  def checkIfColumnWithComplexTypeExists(carbonTable: CarbonTable, columns: List[String]): Unit = {
    columns.foreach { col =>
      val dataType = carbonTable.getColumnByName(col).getColumnSchema.getDataType
      if (dataType.isComplexType) {
        throw new UnsupportedOperationException("Unsupported operation on Complex data type")
      }
    }
  }

  def checkIfSpatialColumnsExists(carbonTable: CarbonTable, columns: List[String]): Unit = {
    val properties = carbonTable.getTableInfo.getFactTable.getTableProperties.asScala
    val indexProperty = properties.get(CarbonCommonConstants.SPATIAL_INDEX)
    if (indexProperty.isDefined) {
      indexProperty.get.split(",").map(_.trim).foreach { element =>
        val srcColumns
        = properties.get(CarbonCommonConstants.SPATIAL_INDEX + s".$element.sourcecolumns")
        val common = columns.intersect(srcColumns.get.split(",").map(_.trim))
        if (common.nonEmpty || columns.contains(element)) {
          throw new MalformedCarbonCommandException(s"Columns present in " +
            s"${CarbonCommonConstants.SPATIAL_INDEX} table property cannot be altered/updated")
        }
      }
    }
  }

  /**
   * iterates the plan  and check whether CarbonCommonConstants.CARBON_INPUT_SEGMENTS set for any
   * any table or not
   * @param sparkSession
   * @param logicalPlan
   */
  def checkIfSegmentListIsSet(sparkSession: SparkSession, logicalPlan: LogicalPlan): Unit = {
    val carbonProperties = CarbonProperties.getInstance()
    logicalPlan.foreach {
      case unresolvedRelation: UnresolvedRelation =>
        val dbAndTb =
          sparkSession.sessionState.catalog.asInstanceOf[HiveSessionCatalog].getCurrentDatabase +
          "." + unresolvedRelation.tableIdentifier.table
        val segmentProperties = carbonProperties
          .getProperty(CarbonCommonConstants.CARBON_INPUT_SEGMENTS + dbAndTb, "")
        if (!(segmentProperties.equals("") || segmentProperties.trim.equals("*"))) {
          throw new MalformedCarbonCommandException("carbon.input.segments." + dbAndTb +
                                                    "should not be set for table used in DELETE " +
                                                    "query. Please reset the property to carbon" +
                                                    ".input.segments." +
                                                    dbAndTb + "=*")
        }
      case logicalRelation: LogicalRelation if (logicalRelation.relation
        .isInstanceOf[CarbonDatasourceHadoopRelation]) =>
        val dbAndTb =
          logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable
            .getDatabaseName + "." +
          logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable
            .getTableName
        val segmentProperty = carbonProperties
          .getProperty(CarbonCommonConstants.CARBON_INPUT_SEGMENTS + dbAndTb, "")
        if (!(segmentProperty.equals("") || segmentProperty.trim.equals("*"))) {
          throw new MalformedCarbonCommandException("carbon.input.segments." + dbAndTb +
                                                    "should not be set for table used in UPDATE " +
                                                    "query. Please reset the property to carbon" +
                                                    ".input.segments." +
                                                    dbAndTb + "=*")
        }
      case filter: Filter => filter.subqueries.toList
        .foreach(subquery => checkIfSegmentListIsSet(sparkSession, subquery))
      case _ =>
    }
  }
}
