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

package org.apache.spark.sql.execution.command.index

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.hive.{CarbonHiveIndexMetadataUtil, CarbonRelation}
import org.apache.spark.sql.index.{CarbonIndexUtil, IndexTableUtil}
import org.apache.spark.sql.secondaryindex.command.IndexModel

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedIndexCommandException}
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.status.IndexStatus
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.metadata.ColumnarFormatVersion
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.index.CarbonIndexProvider
import org.apache.carbondata.core.metadata.schema.datamap.DataMapProperty
import org.apache.carbondata.core.metadata.schema.indextable.{IndexMetadata, IndexTableInfo}
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.datamap.IndexProvider

/**
 * Below command class will be used to create index on table
 * and updating the parent table about the index information
 */
case class CarbonCreateIndexCommand(
    indexModel: IndexModel,
    indexProviderName: String,
    properties: Map[String, String],
    ifNotExistsSet: Boolean = false,
    var deferredRebuild: Boolean = false)
  extends AtomicRunnableCommand {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
  private var provider: IndexProvider = _
  private var parentTable: CarbonTable = _
  private var indexSchema: DataMapSchema = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val indexName = indexModel.indexName
    val parentTableName = indexModel.tableName
    // get parent table
    parentTable = CarbonEnv.getCarbonTable(indexModel.dbName, parentTableName)(sparkSession)
    val errMsg = s"Parent Table `$parentTableName` is not found. " +
                 s"To create index, main table is required"
    if (parentTable == null) {
      throw new MalformedIndexCommandException(errMsg)
    }
    val dbName = parentTable.getDatabaseName
    indexSchema = new DataMapSchema(indexName, indexProviderName)

    val property = properties.map(x => (x._1.trim, x._2.trim)).asJava
    val indexProperties = new java.util.LinkedHashMap[String, String](property)
    indexProperties.put(DataMapProperty.DEFERRED_REBUILD, deferredRebuild.toString)
    indexProperties.put(CarbonCommonConstants.INDEX_COLUMNS, indexModel.columnNames.mkString(","))
    indexProperties.put(CarbonCommonConstants.INDEX_PROVIDER, indexProviderName)

    setAuditTable(parentTable)
    setAuditInfo(Map("provider" -> indexProviderName, "indexName" -> indexName) ++ properties)

    if (!parentTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }

    if (parentTable.isMVTable || parentTable.isIndexTable) {
      throw new MalformedIndexCommandException(
        "Cannot create index on child table `" + indexName + "`")
    }

    if (CarbonUtil.getFormatVersion(parentTable) != ColumnarFormatVersion.V3) {
      throw new MalformedCarbonCommandException(
        s"Unsupported operation on table with V1 or V2 format data")
    }

    // get metadata lock to avoid concurrent create index operations
    val metadataLock = CarbonLockFactory.getCarbonLockObj(
      parentTable.getAbsoluteTableIdentifier,
      LockUsage.METADATA_LOCK)

    try {
      if (metadataLock.lockWithRetries()) {
        LOGGER.info(s"Acquired the metadata lock for table $dbName.$parentTableName")
        // get carbon table again to reflect any changes during lock acquire.
        parentTable =
          CarbonEnv.getInstance(sparkSession).carbonMetaStore
            .lookupRelation(Some(dbName), parentTableName)(sparkSession)
            .asInstanceOf[CarbonRelation].carbonTable
        if (parentTable == null) {
          throw new MalformedIndexCommandException(errMsg)
        }
        val oldIndexMetaData = parentTable.getIndexMetadata
        // check whether the column has index created already
        if (null != oldIndexMetaData) {
          val indexExistsInCarbon = oldIndexMetaData.getIndexTables.asScala.contains(indexName)
          if (indexExistsInCarbon) {
            throw new MalformedIndexCommandException(
              "Index with name `" + indexName + "` already exists on table `" + parentTableName +
              "`")
          }
        }
        // set properties
        indexSchema.setProperties(indexProperties)
        provider = new IndexProvider(parentTable, indexSchema, sparkSession)

        if (deferredRebuild && !provider.supportRebuild()) {
          throw new MalformedIndexCommandException(
          "DEFERRED REFRESH is not supported on this index " + indexModel.indexName +
          " with provider " + indexProviderName)
        } else if (deferredRebuild && provider.supportRebuild()) {
          indexProperties.put(CarbonCommonConstants.INDEX_STATUS, IndexStatus.DISABLED.name())
        }

        val isBloomFilter = CarbonIndexProvider.BLOOMFILTER.getIndexProviderName
          .equalsIgnoreCase(indexProviderName)

        val existingIndexColumn4ThisProvider =
          if (null != oldIndexMetaData &&
              null != oldIndexMetaData.getIndexesMap.get(indexProviderName)) {
            oldIndexMetaData.getIndexesMap.get(indexProviderName).values().asScala.flatMap {
              key => key.get(CarbonCommonConstants.INDEX_COLUMNS).split(",").toList
            }.toList
          } else {
            Seq.empty
          }

        parentTable.getIndexedColumns(indexModel.columnNames.toArray).asScala.foreach { column =>
          if (existingIndexColumn4ThisProvider.contains(column.getColName)) {
            throw new MalformedIndexCommandException(String.format(
              "column '%s' already has %s index created",
              column.getColName, indexProviderName))
          }
          if (isBloomFilter) {
            if (column.getDataType == DataTypes.BINARY) {
              throw new MalformedIndexCommandException(
                s"BloomFilter does not support Binary datatype column: ${
                  column.getColName
                }")
            }
            // For bloomfilter, the index column datatype cannot be complex type
            if (column.isComplex) {
              throw new MalformedIndexCommandException(
                s"BloomFilter does not support complex datatype column: ${
                  column.getColName
                }")
            }
          }
        }

        var oldIndexInfo = parentTable.getIndexInfo
        if (null == oldIndexInfo) {
          oldIndexInfo = ""
        }
        val indexInfo = IndexTableUtil.checkAndAddIndexTable(
          oldIndexInfo,
          new IndexTableInfo(dbName, indexName,
            indexProperties),
          false)

        // set index information in parent table
        val parentIndexMetadata = if (
          parentTable.getTableInfo.getFactTable.getTableProperties
            .get(parentTable.getCarbonTableIdentifier.getTableId) != null) {
          IndexMetadata.deserialize(parentTable.getTableInfo.getFactTable.getTableProperties
            .get(parentTable.getCarbonTableIdentifier.getTableId))
        } else {
          new IndexMetadata(false)
        }
        parentIndexMetadata.addIndexTableInfo(indexProviderName,
          indexName,
          indexProperties)
        parentTable.getTableInfo.getFactTable.getTableProperties
          .put(parentTable.getCarbonTableIdentifier.getTableId, parentIndexMetadata.serialize)

        sparkSession.sql(
          s"""ALTER TABLE $dbName.$parentTableName SET SERDEPROPERTIES ('indexInfo' =
             |'$indexInfo')""".stripMargin).collect()

        CarbonHiveIndexMetadataUtil.refreshTable(dbName, parentTableName, sparkSession)
      } else {
        LOGGER.error(s"Not able to acquire the metadata lock for table" +
                     s" $dbName.$parentTableName")
      }
    } finally {
      metadataLock.unlock()
    }
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    if (provider != null) {
      provider.setMainTable(parentTable)
      provider.initData()
      if (!deferredRebuild) {
        provider.rebuild()
        // enable bloom or lucene index
        // get metadata lock to avoid concurrent create index operations
        val metadataLock = CarbonLockFactory.getCarbonLockObj(
          parentTable.getAbsoluteTableIdentifier,
          LockUsage.METADATA_LOCK)
        try {
          if (metadataLock.lockWithRetries()) {
            LOGGER.info(s"Acquired the metadata lock for table ${
              parentTable
                .getDatabaseName
            }.${ parentTable.getTableName }")
            // get carbon table again to reflect any changes during lock acquire.
            parentTable =
              CarbonEnv.getInstance(sparkSession).carbonMetaStore
                .lookupRelation(indexModel.dbName, indexModel.tableName)(sparkSession)
                .asInstanceOf[CarbonRelation].carbonTable
            val oldIndexInfo = parentTable.getIndexInfo
            val indexInfo = IndexTableUtil.checkAndAddIndexTable(
              oldIndexInfo,
              new IndexTableInfo(parentTable.getDatabaseName, indexModel.indexName,
                indexSchema.getProperties),
              false)
            val enabledIndexInfo = IndexTableInfo.enableIndex(indexInfo, indexModel.indexName)

            // set index information in parent table
            val parentIndexMetadata =
              IndexMetadata.deserialize(parentTable.getTableInfo.getFactTable.getTableProperties
                .get(parentTable.getCarbonTableIdentifier.getTableId))
            parentIndexMetadata.updateIndexStatus(indexProviderName,
              indexModel.indexName,
              IndexStatus.ENABLED.name())
            parentTable.getTableInfo.getFactTable.getTableProperties
              .put(parentTable.getCarbonTableIdentifier.getTableId, parentIndexMetadata.serialize)

            sparkSession.sql(
              s"""ALTER TABLE ${ parentTable.getDatabaseName }.${ parentTable.getTableName } SET
                 |SERDEPROPERTIES ('indexInfo' = '$enabledIndexInfo')""".stripMargin).collect()
          }
        } finally {
          metadataLock.unlock()
        }
      }
      CarbonIndexUtil
        .addOrModifyTableProperty(
          parentTable,
          Map("indexExists" -> "true"), needLock = false)(sparkSession)

      CarbonHiveIndexMetadataUtil.refreshTable(parentTable.getDatabaseName,
        parentTable.getTableName,
        sparkSession)
    }
    Seq.empty
  }

  override def undoMetadata(sparkSession: SparkSession, exception: Exception): Seq[Row] = {
    DropIndexCommand(ifExistsSet = true,
      indexModel.dbName,
      indexModel.tableName,
      indexModel.indexName)
    Seq.empty
  }

  override protected def opName: String = "CREATE INDEX"
}

