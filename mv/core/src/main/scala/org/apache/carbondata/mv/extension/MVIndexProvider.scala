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
package org.apache.carbondata.mv.extension

import java.io.IOException

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonUtils, SparkSession}
import org.apache.spark.sql.execution.command.management.CarbonInsertIntoCommand
import org.apache.spark.sql.execution.command.table.CarbonDropTableCommand

import org.apache.carbondata.common.annotations.InterfaceAudience
import org.apache.carbondata.common.exceptions.sql.MalformedMVCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.index.{CarbonIndexProvider, IndexStoreManager, MVCatalog}
import org.apache.carbondata.core.index.dev.{Index, IndexFactory}
import org.apache.carbondata.core.index.status.IndexStatusManager
import org.apache.carbondata.core.indexstore.Blocklet
import org.apache.carbondata.core.metadata.schema.index.{IndexClassProvider, IndexProperty}
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, IndexSchema}
import org.apache.carbondata.mv.rewrite.{SummaryDataset, SummaryDatasetCatalog}
import org.apache.carbondata.processing.util.CarbonLoaderUtil

@InterfaceAudience.Internal
class MVIndexProvider(
    sparkSession: SparkSession,
    indexSchema: IndexSchema)
  extends CarbonIndexProvider(null, indexSchema) {

  protected var dropTableCommand: CarbonDropTableCommand = null

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  @throws[MalformedMVCommandException]
  @throws[IOException]
  override def initMeta(ctasSqlStatement: String): Unit = {
    if (ctasSqlStatement == null) {
      throw new MalformedMVCommandException(
        "select statement is mandatory")
    }
    MVHelper.createMVDataMap(
      sparkSession,
      indexSchema,
      ctasSqlStatement,
      true)
    try {
      val catalog = IndexStoreManager.getInstance().getMVCatalog(this,
        IndexClassProvider.MV.getShortName, false).asInstanceOf[SummaryDatasetCatalog]
      if (catalog != null && !catalog.mvSession.sparkSession.equals(sparkSession)) {
        IndexStoreManager.getInstance.registerMVCatalog(this, indexSchema, true)
      } else {
        IndexStoreManager.getInstance.registerMVCatalog(this, indexSchema, false)
      }
      if (indexSchema.isLazy) {
        IndexStatusManager.disableIndex(indexSchema.getIndexName)
      }
    } catch {
      case exception: Exception =>
        dropTableCommand = new CarbonDropTableCommand(true,
          new Some[String](indexSchema.getRelationIdentifier.getDatabaseName),
          indexSchema.getRelationIdentifier.getTableName,
          true)
        dropTableCommand.run(sparkSession)
        IndexStoreManager.getInstance().dropIndexSchema(indexSchema.getIndexName)
        throw exception
    }
  }

  override def initData(): Unit = {
    if (!indexSchema.isLazy) {
      if (rebuild()) {
        IndexStatusManager.enableIndex(indexSchema.getIndexName)
      }
    }
  }

  @throws[IOException]
  override def cleanMeta(): Unit = {
    dropTableCommand = new CarbonDropTableCommand(true,
      new Some[String](indexSchema.getRelationIdentifier.getDatabaseName),
      indexSchema.getRelationIdentifier.getTableName,
      true)
    dropTableCommand.processMetadata(sparkSession)
    // First, drop schema and unregister MV from catalog, because if in
    // case, unregister fails, schema will not be deleted from system and cannot
    // create MV also again
    try {
      IndexStoreManager.getInstance().dropIndexSchema(indexSchema.getIndexName)
    } catch {
      case e: IOException =>
        throw e
    } finally {
      IndexStoreManager.getInstance.unRegisterIndex(indexSchema)
    }
  }

  override def cleanData(): Unit = {
    if (dropTableCommand != null) {
      dropTableCommand.processData(sparkSession)
    }
  }

  @throws[IOException]
  override def rebuildInternal(newLoadName: String,
      segmentMap: java.util.Map[String, java.util.List[String]],
      dataMapTable: CarbonTable): Boolean = {
    val ctasQuery = indexSchema.getCtasQuery
    if (ctasQuery != null) {
      val identifier = indexSchema.getRelationIdentifier
      val updatedQuery = MVParser.getMVQuery(ctasQuery, sparkSession)
      var isOverwriteTable = false
      val isFullRefresh =
        if (null != indexSchema.getProperties.get(IndexProperty.FULL_REFRESH)) {
          indexSchema.getProperties.get(IndexProperty.FULL_REFRESH).toBoolean
        } else {
          false
        }
      if (isFullRefresh) {
        isOverwriteTable = true
      }
      // Set specified segments for incremental load
      val segmentMapIterator = segmentMap.entrySet().iterator()
      while (segmentMapIterator.hasNext) {
        val entry = segmentMapIterator.next()
        setSegmentsToLoadDataMap(entry.getKey, entry.getValue)
      }
      val header = dataMapTable.getTableInfo.getFactTable.getListOfColumns.asScala
        .filter { column =>
          !column.getColumnName
            .equalsIgnoreCase(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE)
        }.sortBy(_.getSchemaOrdinal).map(_.getColumnName).mkString(",")
      val insertIntoCommand = CarbonInsertIntoCommand(
        databaseNameOp = Some(identifier.getDatabaseName),
        tableName = identifier.getTableName,
        options = scala.collection.immutable.Map("fileheader" -> header),
        isOverwriteTable,
        logicalPlan = updatedQuery.queryExecution.analyzed,
        tableInfo = dataMapTable.getTableInfo,
        internalOptions = Map("mergedSegmentName" -> newLoadName,
          CarbonCommonConstants.IS_INTERNAL_LOAD_CALL -> "true"))
      try {
        insertIntoCommand.run(sparkSession)
      } catch {
        case ex: Exception =>
          // If load to MV table fails, disable the MV and if newLoad is still
          // in INSERT_IN_PROGRESS state, mark for delete the newLoad and update table status file
          IndexStatusManager.disableIndex(indexSchema.getIndexName)
          LOGGER.error("Data Load failed for Index: ", ex)
          CarbonLoaderUtil.updateTableStatusInCaseOfFailure(
            newLoadName,
            dataMapTable.getAbsoluteTableIdentifier,
            dataMapTable.getTableName,
            dataMapTable.getDatabaseName,
            dataMapTable.getTablePath,
            dataMapTable.getMetadataPath)
          throw ex
      } finally {
        unsetMainTableSegments()
      }
    }
    true
  }

  /**
   * This method will set main table segments which needs to be loaded to the MV
   */
  private def setSegmentsToLoadDataMap(tableUniqueName: String,
      mainTableSegmentList: java.util.List[String]): Unit = {
    CarbonUtils
      .threadSet(CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
                 tableUniqueName, mainTableSegmentList.asScala.mkString(","))
  }

  private def unsetMainTableSegments(): Unit = {
    val relationIdentifiers = indexSchema.getParentTables.asScala
    for (relationIdentifier <- relationIdentifiers) {
      CarbonUtils
        .threadUnset(CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
                     relationIdentifier.getDatabaseName + "." +
                     relationIdentifier.getTableName)
    }
  }

  override def createMVCatalog : MVCatalog[SummaryDataset] =
    new SummaryDatasetCatalog(sparkSession)

  override def getIndexFactory: IndexFactory[_ <: Index[_ <: Blocklet]] = {
    throw new UnsupportedOperationException
  }

  override def supportRebuild(): Boolean = true
}
