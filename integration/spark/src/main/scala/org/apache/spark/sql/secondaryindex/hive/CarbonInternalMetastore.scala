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

package org.apache.spark.sql.secondaryindex.hive

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark.sql.{AnalysisException, CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.hive.{CarbonHiveIndexMetadataUtil, CarbonRelation}
import org.apache.spark.sql.index.CarbonIndexUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.index.IndexStoreManager
import org.apache.carbondata.core.metadata.index.IndexType
import org.apache.carbondata.core.metadata.schema.indextable.{IndexMetadata, IndexTableInfo}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonUtil

object CarbonInternalMetastore {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * This method can be used to delete the index table and update the parent table and hive
   * metadata.
   *
   * @param removeEntryFromParentTable if true then the index table info would be removed from
   *                                   the Parent table
   */
  def dropIndexTable(indexTableIdentifier: TableIdentifier, indexCarbonTable: CarbonTable,
      tableStorePath: String,
      parentCarbonTable: CarbonTable,
      removeEntryFromParentTable: Boolean)(sparkSession: SparkSession) {
    val dbName = indexTableIdentifier.database.get
    val tableName = indexTableIdentifier.table
    try {
      if (indexCarbonTable != null) {
        LOGGER.info(s"Deleting index table $dbName.$tableName")
        CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .dropTable(indexCarbonTable.getAbsoluteTableIdentifier)(sparkSession)
        if (removeEntryFromParentTable && parentCarbonTable != null) {
          val parentTableName = parentCarbonTable.getTableName
          val relation: LogicalPlan = CarbonEnv.getInstance(sparkSession).carbonMetaStore
            .lookupRelation(Some(dbName), parentTableName)(sparkSession)
          val indexInfo = if (relation != null) {
            relation.asInstanceOf[CarbonRelation].carbonTable.getIndexInfo
          } else {
            sys.error(s"Table $dbName.$parentTableName does not exists")
          }
          sparkSession.sessionState
            .catalog
            .dropTable(indexTableIdentifier, ignoreIfNotExists = true, purge = false)
          // even if folders are deleted from carbon store it can happen that table exists in hive
          CarbonHiveIndexMetadataUtil
            .invalidateAndUpdateIndexInfo(indexTableIdentifier, indexInfo, parentCarbonTable)(
              sparkSession)
          // clear parent table from meta store cache as it is also required to be
          // refreshed when SI table is dropped
          IndexStoreManager.getInstance()
            .clearIndex(indexCarbonTable.getAbsoluteTableIdentifier)
          removeTableFromMetadataCache(dbName, indexCarbonTable.getTableName)(sparkSession)
          removeTableFromMetadataCache(dbName, parentTableName)(sparkSession)
        }
      }
    } finally {
      // Even if some exception occurs we will try to remove the table from catalog to avoid
      // stale state.
      sparkSession.sessionState.catalog
        .dropTable(indexTableIdentifier, ignoreIfNotExists = true, purge = false)
      sparkSession.sessionState.catalog.refreshTable(indexTableIdentifier)
      LOGGER.info(s"Deleted index table $dbName.$tableName")
    }
  }

  def removeTableFromMetadataCache(dbName: String, tableName: String)
    (sparkSession: SparkSession): Unit = {
    CarbonEnv.getInstance(sparkSession).carbonMetaStore.removeTableFromMetadata(dbName, tableName)
  }

  /**
   * This method will delete the index tables silently. We want this because even if one index
   * delete fails, we need to try delete on all other index tables as well.
   *
   */
  def deleteIndexSilent(carbonTableIdentifier: TableIdentifier,
      storePath: String,
      parentCarbonTable: CarbonTable)(sparkSession: SparkSession): Unit = {
    val dbName = carbonTableIdentifier.database
    val indexTable = carbonTableIdentifier.table
    var indexCarbonTable: CarbonTable = null
    try {
      indexCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
        .lookupRelation(dbName, indexTable)(sparkSession)
        .asInstanceOf[CarbonRelation].carbonTable
    } catch {
      case e: Exception =>
        LOGGER.error("Exception occurred while drop index table for : " +
                     s"$dbName.$indexTable : ${ e.getMessage }")
    }
    finally {
      try {
        dropIndexTable(carbonTableIdentifier,
          indexCarbonTable,
          storePath,
          parentCarbonTable,
          removeEntryFromParentTable = true
        )(sparkSession)
      } catch {
        case e: Exception =>
          LOGGER.error("Exception occurred while drop index table for : " +
                       s"$dbName.$indexTable : ${ e.getMessage }")
      }
    }
  }

  def refreshTableStatusVersion(carbonTable: CarbonTable)(sparkSession: SparkSession): Unit = {
    if (!carbonTable.isTransactionalTable || carbonTable.isExternalTable) {
      return
    }
    if (null == carbonTable.getTableInfo.getFactTable.getTableProperties.get("latestversion")) {
      val hiveTable = sparkSession.sessionState
        .catalog.getTableMetadata(TableIdentifier(carbonTable.getTableName,
        Some(carbonTable.getDatabaseName)))
      val version = hiveTable.storage.properties.getOrElse(
        "latestversion", "")
      carbonTable.getTableInfo.getFactTable.getTableProperties.put("latestversion", version)
    }
  }

  def refreshIndexInfo(dbName: String, tableName: String,
      carbonTable: CarbonTable, needLock: Boolean = true)(sparkSession: SparkSession): Unit = {
    // check if secondary index table exists
    val indexTableExists = CarbonIndexUtil.isIndexTableExists(carbonTable)
    // check if cg and fg index exists
    val indexExists = CarbonIndexUtil.isIndexExists(carbonTable)
    // In case of non transactional table, no need to change the table schema.
    if (!carbonTable.isTransactionalTable) {
      return
    }
    // tables created without property "indexTableExists", will return null, for those tables enter
    // into below block, gather the actual data from hive and then set this property to true/false
    // then once the property has a value true/false, make decision based on the property value
    if (null != carbonTable && (null == indexTableExists || indexTableExists.toBoolean)) {
      // When Index information is not loaded in main table, then it will fetch
      // index info from hive metastore and set it in the carbon table.
      val indexTableMap =
      new ConcurrentHashMap[String, java.util.Map[String, java.util.Map[String, String]]]
      try {
        val (isIndexTable, parentTableName, indexInfo, parentTablePath, parentTableId, schema) =
          indexInfoFromHive(dbName, tableName)(sparkSession)
        if (isIndexTable.equals("true")) {
          val indexMeta = new IndexMetadata(indexTableMap,
            parentTableName,
            true,
            parentTablePath,
            parentTableId)
          carbonTable.getTableInfo.getFactTable.getTableProperties
            .put(carbonTable.getCarbonTableIdentifier.getTableId, indexMeta.serialize)
        } else {
          IndexTableInfo.fromGson(indexInfo)
            .foreach { indexTableInfo =>
              var indexProperties = indexTableInfo.getIndexProperties
              val indexProvider = if (null != indexProperties) {
                indexProperties.get(CarbonCommonConstants.INDEX_PROVIDER)
              } else {
                // in case if SI table has been created before the change CARBONDATA-3765,
                // indexProperties variable will not be present. On direct upgrade of SI store,
                // indexProperties will be null, in that case, create indexProperties from indexCols
                indexProperties = new java.util.HashMap[String, String]()
                indexProperties.put(CarbonCommonConstants.INDEX_COLUMNS,
                  indexTableInfo.getIndexCols.asScala.mkString(","))
                val provider = IndexType.SI.getIndexProviderName
                indexProperties.put(CarbonCommonConstants.INDEX_PROVIDER, provider)
                provider
              }
              if (indexTableMap.isEmpty) {
                val indexTableInfoMap = new java.util.HashMap[String, java.util.Map[String, String]]
                indexTableInfoMap.put(indexTableInfo.getTableName, indexProperties)
                indexTableMap.put(indexProvider, indexTableInfoMap)
              } else if (null == indexTableMap.get(indexProvider)) {
                val indexTableInfoMap = new java.util.HashMap[String, java.util.Map[String, String]]
                indexTableInfoMap.put(indexTableInfo.getTableName, indexProperties)
                indexTableMap.put(indexProvider, indexTableInfoMap)
              } else {
                indexTableMap.get(indexProvider).put(indexTableInfo.getTableName, indexProperties)
              }
            }
          val indexMetadata = new IndexMetadata(
            indexTableMap,
            parentTableName,
            isIndexTable.toBoolean,
            parentTablePath, parentTableId)
          carbonTable.getTableInfo.getFactTable.getTableProperties
            .put(carbonTable.getCarbonTableIdentifier.getTableId, indexMetadata.serialize)
        }
        if (null == indexTableExists && !isIndexTable.equals("true")) {
          val indexTables = CarbonIndexUtil.getSecondaryIndexes(carbonTable)
          val tableIdentifier = new TableIdentifier(carbonTable.getTableName,
            Some(carbonTable.getDatabaseName))
          if (indexTables.isEmpty) {
            // modify the tableProperties of mainTable by adding "indexTableExists" property
            // to false as there is no index table for this table
            if (indexExists == null) {
              CarbonIndexUtil
                .addOrModifyTableProperty(carbonTable,
                  Map("indexTableExists" -> "false", "indexExists" -> "false"), needLock)(
                  sparkSession)
            } else {
              CarbonIndexUtil
                .addOrModifyTableProperty(carbonTable,
                  Map("indexTableExists" -> "false", "indexExists" -> "true"), needLock)(
                  sparkSession)
            }
          } else {
            // modify the tableProperties of mainTable by adding "indexTableExists" property
            // to true as there are some index table for this table
            if (indexExists == null) {
              CarbonIndexUtil
                .addOrModifyTableProperty(carbonTable,
                  Map("indexTableExists" -> "true", "indexExists" -> "false"), needLock)(
                  sparkSession)
            } else {
              CarbonIndexUtil
                .addOrModifyTableProperty(carbonTable,
                  Map("indexTableExists" -> "true", "indexExists" -> "true"), needLock)(
                  sparkSession)
            }
          }
        }
      } catch {
        case e: Exception =>
          // In case of creating a table, hive table will not be available.
          LOGGER.error(e.getMessage)
      }
    }
    // add cg and fg index info to table properties
    if (null != indexExists) {
      if (null != carbonTable && indexExists.toBoolean) {
        val indexTableMap = new ConcurrentHashMap[String, java.util.Map[String, java.util
        .Map[String, String]]]
        val (isIndexTable, parentTableName, indexInfo, parentTablePath, parentTableId, schema) =
          indexInfoFromHive(dbName, tableName)(sparkSession)
        if (!indexInfo.equalsIgnoreCase("[]")) {
          IndexTableInfo.fromGson(indexInfo)
            .foreach { indexTableInfo =>
              val indexProvider = indexTableInfo.getIndexProperties
                .get(CarbonCommonConstants.INDEX_PROVIDER)
              if (indexTableMap.isEmpty) {
                val indexTableInfoMap = new java.util.HashMap[String, java.util.Map[String, String]]
                indexTableInfoMap.put(indexTableInfo.getTableName,
                  indexTableInfo.getIndexProperties)
                indexTableMap.put(indexProvider, indexTableInfoMap)
              } else if (null == indexTableMap.get(indexProvider)) {
                val indexTableInfoMap = new java.util.HashMap[String, java.util.Map[String, String]]
                indexTableInfoMap.put(indexTableInfo.getTableName,
                  indexTableInfo.getIndexProperties)
                indexTableMap.put(indexProvider, indexTableInfoMap)
              } else {
                indexTableMap.get(indexProvider).put(indexTableInfo.getTableName,
                  indexTableInfo.getIndexProperties)
              }
            }
          val indexMetadata = new IndexMetadata(
            indexTableMap,
            parentTableName,
            isIndexTable.toBoolean,
            parentTablePath, parentTableId)
          carbonTable.getTableInfo.getFactTable.getTableProperties
            .put(carbonTable.getCarbonTableIdentifier.getTableId, indexMetadata.serialize)
        }
      }
    }
  }

  def indexInfoFromHive(databaseName: String, tableName: String)
    (sparkSession: SparkSession): (String, String, String, String, String, String) = {
    val hiveTable = sparkSession.sessionState.catalog
      .getTableMetadata(TableIdentifier(tableName, Some(databaseName)))
    val indexList = hiveTable.storage.properties.getOrElse(
      "indexInfo", IndexTableInfo.toGson(new Array[IndexTableInfo](0)))

    val datasourceOptions = optionsValueFromParts(hiveTable)

    val isIndexTable = datasourceOptions.getOrElse("isIndexTable", "false")
    val parentTableName = datasourceOptions.getOrElse("parentTableName", "")
    val parentTablePath = if (!parentTableName.isEmpty) {
      CarbonEnv
        .getCarbonTable(TableIdentifier(parentTableName, Some(databaseName)))(sparkSession)
        .getTablePath
    } else {
      ""
    }
    val parentTableId = datasourceOptions.getOrElse("parentTableId", "")
    (isIndexTable, parentTableName, indexList, parentTablePath, parentTableId, hiveTable.schema
      .json)
  }

  private def optionsValueFromParts(table: CatalogTable): Map[String, String] = {
    val optionsCombined = new java.util.HashMap[String, String]
    val optionsKeys: Option[String] =
      table.storage.properties.get("spark.sql.sources.options.keys.numParts").map { numParts =>
        combinePartsFromSerdeProps(numParts, "spark.sql.sources.options.keys", table).mkString
      }
    optionsKeys match {
      case Some(optKeys) =>
        optKeys.split(",").foreach { optKey =>
          table.storage.properties.get(s"$optKey.numParts").map { numParts =>
            optionsCombined.put(optKey,
              combinePartsFromSerdeProps(numParts, optKey, table).mkString)
          }
        }
        optionsCombined.asScala.toMap
      case None =>
        LOGGER.info(s"spark.sql.sources.options.keys expected, but read nothing")
        table.storage.properties
    }
  }

  private def combinePartsFromSerdeProps(numParts: String,
      key: String, table: CatalogTable): Seq[String] = {
    val keysParts = (0 until numParts.toInt).map { index =>
      val keysPart =
        table.storage.properties.get(s"$key.part.$index").orNull
      if (keysPart == null) {
        throw new AnalysisException(
          s"Could not read $key from the metastore because it is corrupted " +
          s"(missing part $index of the $key, $numParts parts are expected).")
      }
      keysPart
    }
    keysParts
  }

  def deleteTableDirectory(carbonTable: CarbonTable): Unit = {
    if (FileFactory.isFileExist(carbonTable.getTablePath) &&
      !(carbonTable.isExternalTable || carbonTable.isFileLevelFormat)) {
      val file = FileFactory.getCarbonFile(carbonTable.getTablePath)
      CarbonUtil.deleteFoldersAndFilesSilent(file)
    }
  }
}
