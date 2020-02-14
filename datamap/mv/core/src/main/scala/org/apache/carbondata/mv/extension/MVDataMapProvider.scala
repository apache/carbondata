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
import org.apache.spark.sql.execution.command.management.CarbonInsertIntoWithDf
import org.apache.spark.sql.execution.command.table.CarbonDropTableCommand
import org.apache.spark.sql.parser.CarbonSparkSqlParserUtil
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.annotations.InterfaceAudience
import org.apache.carbondata.common.exceptions.sql.MalformedMaterializedViewException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.{DataMapCatalog, DataMapProvider, DataMapStoreManager}
import org.apache.carbondata.core.datamap.dev.{DataMap, DataMapFactory}
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.indexstore.Blocklet
import org.apache.carbondata.core.metadata.schema.datamap.DataMapProperty
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.mv.rewrite.{SummaryDataset, SummaryDatasetCatalog}
import org.apache.carbondata.processing.util.CarbonLoaderUtil

@InterfaceAudience.Internal
class MVDataMapProvider(
    sparkSession: SparkSession,
    dataMapSchema: DataMapSchema)
  extends DataMapProvider(null, dataMapSchema) {

  protected var dropTableCommand: CarbonDropTableCommand = null

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  @throws[MalformedMaterializedViewException]
  @throws[IOException]
  override def initMeta(ctasSqlStatement: String): Unit = {
    if (ctasSqlStatement == null) {
      throw new MalformedMaterializedViewException(
        "select statement is mandatory")
    }
    MVHelper.createMVDataMap(
      sparkSession,
      dataMapSchema,
      ctasSqlStatement,
      true)
    try {
      DataMapStoreManager.getInstance.registerDataMapCatalog(this, dataMapSchema)
      if (dataMapSchema.isLazy) {
        DataMapStatusManager.disableDataMap(dataMapSchema.getDataMapName)
      }
    } catch {
      case exception: Exception =>
        dropTableCommand = new CarbonDropTableCommand(true,
          new Some[String](dataMapSchema.getRelationIdentifier.getDatabaseName),
          dataMapSchema.getRelationIdentifier.getTableName,
          true)
        dropTableCommand.run(sparkSession)
        DataMapStoreManager.getInstance().dropDataMapSchema(dataMapSchema.getDataMapName)
        throw exception
    }
  }

  override def initData(): Unit = {
    if (!dataMapSchema.isLazy) {
      if (rebuild()) {
        DataMapStatusManager.enableDataMap(dataMapSchema.getDataMapName)
      }
    }
  }

  @throws[IOException]
  override def cleanMeta(): Unit = {
    dropTableCommand = new CarbonDropTableCommand(true,
      new Some[String](dataMapSchema.getRelationIdentifier.getDatabaseName),
      dataMapSchema.getRelationIdentifier.getTableName,
      true)
    dropTableCommand.processMetadata(sparkSession)
    // First, drop datamapschema and unregister datamap from catalog, because if in
    // case, unregister fails, datamapschema will not be deleted from system and cannot
    // create datamap also again
    try {
      DataMapStoreManager.getInstance().dropDataMapSchema(dataMapSchema.getDataMapName)
    } catch {
      case e: IOException =>
        throw e
    } finally {
      DataMapStoreManager.getInstance.unRegisterDataMapCatalog(dataMapSchema)
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
    val ctasQuery = dataMapSchema.getCtasQuery
    if (ctasQuery != null) {
      val identifier = dataMapSchema.getRelationIdentifier
      val updatedQuery = MVParser.getMVQuery(ctasQuery, sparkSession)
      var isOverwriteTable = false
      val isFullRefresh =
        if (null != dataMapSchema.getProperties.get(DataMapProperty.FULL_REFRESH)) {
          dataMapSchema.getProperties.get(DataMapProperty.FULL_REFRESH).toBoolean
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
      val insertWithDf = CarbonInsertIntoWithDf(
        databaseNameOp = Some(identifier.getDatabaseName),
        tableName = identifier.getTableName,
        options = scala.collection.immutable.Map("fileheader" -> header),
        isOverwriteTable,
        dataFrame = updatedQuery,
        updateModel = None,
        tableInfoOp = None,
        internalOptions = Map("mergedSegmentName" -> newLoadName,
          CarbonCommonConstants.IS_INTERNAL_LOAD_CALL -> "true"),
        partition = Map.empty)
      try {
        insertWithDf.process(sparkSession)
      } catch {
        case ex: Exception =>
          // If load to dataMap table fails, disable the dataMap and if newLoad is still
          // in INSERT_IN_PROGRESS state, mark for delete the newLoad and update table status file
          DataMapStatusManager.disableDataMap(dataMapSchema.getDataMapName)
          LOGGER.error("Data Load failed for DataMap: ", ex)
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
   * This method will set main table segments which needs to be loaded to mv dataMap
   */
  private def setSegmentsToLoadDataMap(tableUniqueName: String,
      mainTableSegmentList: java.util.List[String]): Unit = {
    CarbonUtils
      .threadSet(CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
                 tableUniqueName, mainTableSegmentList.asScala.mkString(","))
  }

  private def unsetMainTableSegments(): Unit = {
    val relationIdentifiers = dataMapSchema.getParentTables.asScala
    for (relationIdentifier <- relationIdentifiers) {
      CarbonUtils
        .threadUnset(CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
                     relationIdentifier.getDatabaseName + "." +
                     relationIdentifier.getTableName)
    }
  }

  override def createDataMapCatalog : DataMapCatalog[SummaryDataset] =
    new SummaryDatasetCatalog(sparkSession)

  override def getDataMapFactory: DataMapFactory[_ <: DataMap[_ <: Blocklet]] = {
    throw new UnsupportedOperationException
  }

  override def supportRebuild(): Boolean = true
}
