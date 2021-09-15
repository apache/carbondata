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

package org.apache.spark.util

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.{CarbonParserUtil, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.execution.command.DataTypeInfo
import org.apache.spark.sql.hive.{CarbonRelation, CarbonSessionCatalogUtil}
import org.apache.spark.sql.index.CarbonIndexUtil

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.constants.SortScopeOptions.SortScope
import org.apache.carbondata.core.datastore.block.SegmentPropertiesAndSchemaHolder
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.exception.InvalidConfigurationException
import org.apache.carbondata.core.index.IndexStoreManager
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.datatype.{DataTypes, DecimalType}
import org.apache.carbondata.core.metadata.index.IndexType
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonColumn, CarbonDimension, ColumnSchema}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.format.{SchemaEvolutionEntry, TableInfo}
import org.apache.carbondata.spark.util.{CarbonScalaUtil, CommonUtil}

object AlterTableUtil {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Validates that the table exists and acquires meta lock on it.
   *
   * @param dbName
   * @param tableName
   * @param sparkSession
   * @return
   */
  def validateTableAndAcquireLock(dbName: String,
      tableName: String,
      locksToBeAcquired: List[String])
    (sparkSession: SparkSession): List[ICarbonLock] = {
    val relation =
      CarbonEnv.getInstance(sparkSession).carbonMetaStore
        .lookupRelation(Option(dbName), tableName)(sparkSession)
        .asInstanceOf[CarbonRelation]
    if (relation == null) {
      sys.error(s"Table $dbName.$tableName does not exist")
    }
    // acquire the lock first
    val table = relation.carbonTable
    val acquiredLocks = ListBuffer[ICarbonLock]()
    try {
      locksToBeAcquired.foreach { lock =>
        acquiredLocks += CarbonLockUtil.getLockObject(table.getAbsoluteTableIdentifier, lock)
      }
      acquiredLocks.toList
    } catch {
      case e: Exception =>
        releaseLocks(acquiredLocks.toList)
        throw e
    }
  }

  /**
   * This method will release the locks acquired for an operation
   *
   * @param locks
   */
  def releaseLocks(locks: List[ICarbonLock]): Unit = {
    locks.foreach { carbonLock =>
      if (carbonLock.unlock()) {
        LOGGER.info("Alter table lock released successfully")
      } else {
        LOGGER.error("Unable to release lock during alter table operation")
      }
    }
  }

  /**
   * update schema when SORT_COLUMNS are be changed
   */
  private def updateSchemaForSortColumns(
      thriftTable: TableInfo,
      lowerCasePropertiesMap: mutable.Map[String, String]): SchemaEvolutionEntry = {
    var schemaEvolutionEntry: SchemaEvolutionEntry = null
    val sortColumnsOption = lowerCasePropertiesMap.get(CarbonCommonConstants.SORT_COLUMNS)
    if (sortColumnsOption.isDefined) {
      val sortColumnsString = CarbonUtil.unquoteChar(sortColumnsOption.get).trim
      val columns = thriftTable.getFact_table.getTable_columns
      // remove old sort_columns property from ColumnSchema
      val columnSeq =
        columns
          .asScala
          .map { column =>
            val columnProperties = column.getColumnProperties
            if (columnProperties != null) {
              columnProperties.remove(CarbonCommonConstants.SORT_COLUMNS)
            }
            column
          }
          .zipWithIndex
      if (!sortColumnsString.isEmpty) {
        val newSortColumns = sortColumnsString.split(',').map(_.trim)
        // map sort_columns index in column list
        val sortColumnsIndexMap = newSortColumns
          .zipWithIndex
          .map { entry =>
            val column = columnSeq.find(_._1.getColumn_name.equalsIgnoreCase(entry._1)).get
            var columnProperties = column._1.getColumnProperties
            if (columnProperties == null) {
              columnProperties = new util.HashMap[String, String]()
              column._1.setColumnProperties(columnProperties)
            }
            // change sort_columns to dimension
            if (!column._1.isDimension) {
              column._1.setDimension(true)
              columnProperties.put(CarbonCommonConstants.COLUMN_DRIFT, "true")
            }
            // add sort_columns property
            columnProperties.put(CarbonCommonConstants.SORT_COLUMNS, "true")
            (column._2, entry._2)
          }
          .toMap
        var index = newSortColumns.length
        // re-order all columns, move sort_columns to the head of column list
        val newColumns = columnSeq
          .map { entry =>
            val sortColumnIndexOption = sortColumnsIndexMap.get(entry._2)
            val newIndex = if (sortColumnIndexOption.isDefined) {
              sortColumnIndexOption.get
            } else {
              val tempIndex = index
              index += 1
              tempIndex
            }
            (newIndex, entry._1)
          }
          .sortWith(_._1 < _._1)
          .map(_._2)
          .asJava
        // use new columns
        columns.clear()
        columns.addAll(newColumns)
        schemaEvolutionEntry = new SchemaEvolutionEntry(System.currentTimeMillis())
      }
    }
    schemaEvolutionEntry
  }

  /**
   * update schema when LONG_STRING_COLUMNS are changed
   */
  private def updateSchemaForLongStringColumns(
      thriftTable: TableInfo,
      longStringColumns: String) = {
    val longStringColumnsString = CarbonUtil.unquoteChar(longStringColumns).trim
    val newColumns = CarbonUtil.reorderColumnsForLongString(thriftTable
      .getFact_table
      .getTable_columns,
      longStringColumnsString)
    thriftTable.getFact_table.setTable_columns(newColumns)
  }

  /**
   * @param carbonTable
   * @param schemaEvolutionEntry
   * @param thriftTable
   * @param sparkSession
   */
  def updateSchemaInfo(carbonTable: CarbonTable,
      schemaEvolutionEntry: SchemaEvolutionEntry = null,
      thriftTable: TableInfo)
    (sparkSession: SparkSession): TableIdentifier = {
    val dbName = carbonTable.getDatabaseName
    val tableName = carbonTable.getTableName
    CarbonEnv.getInstance(sparkSession).carbonMetaStore
      .updateTableSchemaForAlter(carbonTable.getCarbonTableIdentifier,
        carbonTable.getCarbonTableIdentifier,
        thriftTable,
        schemaEvolutionEntry,
        carbonTable.getAbsoluteTableIdentifier.getTablePath)(sparkSession)
    val tableIdentifier = TableIdentifier(tableName, Some(dbName))
    CarbonEnv.getInstance(sparkSession).carbonMetaStore
      .lookupRelation(tableIdentifier)(sparkSession)
    tableIdentifier
  }

  /**
   * This method reverts the changes to the schema if the rename table command fails.
   */
  def revertRenameTableChanges(
      newTableName: String,
      oldCarbonTable: CarbonTable,
      timeStamp: Long)
    (sparkSession: SparkSession): Unit = {
    val tablePath = oldCarbonTable.getTablePath
    val tableId = oldCarbonTable.getCarbonTableIdentifier.getTableId
    val oldCarbonTableIdentifier = oldCarbonTable.getCarbonTableIdentifier
    val database = oldCarbonTable.getDatabaseName
    val newCarbonTableIdentifier = new CarbonTableIdentifier(database, newTableName, tableId)
    val metastore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    if (FileFactory.isFileExist(tablePath)) {
      val tableInfo = metastore.getThriftTableInfo(oldCarbonTable)
      val evolutionEntryList = tableInfo.fact_table.schema_evolution.schema_evolution_history
      val updatedTime = evolutionEntryList.get(evolutionEntryList.size() - 1).time_stamp
      if (updatedTime == timeStamp) {
        LOGGER.info(s"Reverting changes for $database.${oldCarbonTable.getTableName}")
        val absoluteTableIdentifier = AbsoluteTableIdentifier.from(
          tablePath,
          newCarbonTableIdentifier)
        metastore.revertTableSchemaInAlterFailure(oldCarbonTableIdentifier,
          tableInfo, absoluteTableIdentifier)(sparkSession)
        metastore.removeTableFromMetadata(database, newTableName)
      }
    }
  }

  private def revertSchema(dbName: String,
      tableName: String,
      timeStamp: Long,
      sparkSession: SparkSession)
    (operation: (TableInfo, util.List[SchemaEvolutionEntry]) => Unit): Unit = {
    val metastore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    val carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
    val thriftTable: TableInfo = metastore.getThriftTableInfo(carbonTable)
    val evolutionEntryList = thriftTable.fact_table.schema_evolution.schema_evolution_history
    val updatedTime = evolutionEntryList.get(evolutionEntryList.size() - 1).time_stamp
    if (updatedTime == timeStamp) {
      LOGGER.info(s"Reverting changes for $dbName.$tableName")
      operation(thriftTable, evolutionEntryList)
      metastore.revertTableSchemaInAlterFailure(carbonTable.getCarbonTableIdentifier,
        thriftTable, carbonTable.getAbsoluteTableIdentifier)(sparkSession)
    }
  }

  /**
   * This method reverts the changes to the schema if add column command fails.
   *
   * @param dbName
   * @param tableName
   * @param timeStamp
   * @param sparkSession
   */
  def revertAddColumnChanges(dbName: String, tableName: String, timeStamp: Long)
    (sparkSession: SparkSession): Unit = {
    revertSchema(dbName, tableName, timeStamp, sparkSession) { (thriftTable, evolutionEntryList) =>
      val addedSchemas = evolutionEntryList.get(evolutionEntryList.size() - 1).added
      thriftTable.fact_table.table_columns.removeAll(addedSchemas)
    }
  }

  /**
   * This method reverts the schema changes if drop table command fails.
   *
   * @param dbName
   * @param tableName
   * @param timeStamp
   * @param sparkSession
   */
  def revertDropColumnChanges(dbName: String, tableName: String, timeStamp: Long)
    (sparkSession: SparkSession): Unit = {
    revertSchema(dbName, tableName, timeStamp, sparkSession) { (thriftTable, evolutionEntryList) =>
      val removedSchemas = evolutionEntryList.get(evolutionEntryList.size() - 1).removed
      thriftTable.fact_table.table_columns.asScala.foreach { columnSchema =>
        removedSchemas.asScala.foreach { removedSchemas =>
          if (columnSchema.invisible && removedSchemas.column_id == columnSchema.column_id) {
            columnSchema.setInvisible(false)
          }
        }
      }
    }
  }

  /**
   * This method reverts the changes to schema if the data type change command fails.
   *
   * @param dbName
   * @param tableName
   * @param timeStamp
   * @param sparkSession
   */
  def revertColumnRenameAndDataTypeChanges(dbName: String, tableName: String, timeStamp: Long)
    (sparkSession: SparkSession): Unit = {
    revertSchema(dbName, tableName, timeStamp, sparkSession) { (thriftTable, evolutionEntryList) =>
      val removedColumns = evolutionEntryList.get(evolutionEntryList.size() - 1).removed
      thriftTable.fact_table.table_columns.asScala.foreach { columnSchema =>
        removedColumns.asScala.foreach { removedColumn =>
          if (columnSchema.column_id.equalsIgnoreCase(removedColumn.column_id) &&
              !columnSchema.isInvisible) {
            columnSchema.setData_type(removedColumn.data_type)
            columnSchema.setPrecision(removedColumn.precision)
            columnSchema.setScale(removedColumn.scale)
          }
        }
      }
    }
  }

  /**
   * This method modifies the table properties if column rename happened
   * @param tableProperties tableProperties of the table
   * @param oldColumnName old Column name before rename
   * @param newColumnName new column name to rename
   */
  def modifyTablePropertiesAfterColumnRename(
      tableProperties: mutable.Map[String, String],
      oldColumnName: String,
      newColumnName: String): Unit = {
    val columnNameProperties = Set("NO_INVERTED_INDEX",
      "INVERTED_INDEX",
      "INDEX_COLUMNS",
      "COLUMN_META_CACHE",
      "LOCAL_DICTIONARY_INCLUDE",
      "LOCAL_DICTIONARY_EXCLUDE",
      "RANGE_COLUMN",
      "SORT_COLUMNS",
      "LONG_STRING_COLUMNS",
      "BUCKET_COLUMNS")
    tableProperties.foreach { tableProperty =>
      if (columnNameProperties.contains(tableProperty._1.toUpperCase)) {
        val tablePropertyKey = tableProperty._1
        val tablePropertyValue = tableProperty._2
        val newTablePropertyValue = tablePropertyValue.split(",").map(
          s => if (s.equalsIgnoreCase(oldColumnName)) newColumnName else s
        ).mkString(",")
        tableProperties.put(tablePropertyKey, newTablePropertyValue)
      }
    }
  }

  /**
   * This method create a new SchemaEvolutionEntry and adds to SchemaEvolutionEntry List
   *
   * @param addedColumnsList    list of added column schemas
   * @param deletedColumnsList  list of deleted column schemas
   * @return
   */
  def addNewSchemaEvolutionEntry(
      schemaEvolutionEntry: SchemaEvolutionEntry,
      addedColumnsList: List[org.apache.carbondata.format.ColumnSchema],
      deletedColumnsList: List[org.apache.carbondata.format.ColumnSchema]): SchemaEvolutionEntry = {
    val timeStamp = System.currentTimeMillis()
    val newSchemaEvolutionEntry = if (schemaEvolutionEntry == null) {
      new SchemaEvolutionEntry(timeStamp)
    } else {
      schemaEvolutionEntry
    }
    newSchemaEvolutionEntry.setAdded(addedColumnsList.asJava)
    newSchemaEvolutionEntry.setRemoved(deletedColumnsList.asJava)
    newSchemaEvolutionEntry
  }

  def readLatestTableSchema(carbonTable: CarbonTable)(sparkSession: SparkSession): TableInfo = {
    // get the latest carbon table
    val metastore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    val thriftTableInfo: TableInfo = metastore.getThriftTableInfo(carbonTable)
    val schemaConverter = new ThriftWrapperSchemaConverterImpl()
    // read the latest schema file
    val wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(thriftTableInfo,
      carbonTable.getDatabaseName, carbonTable.getTableName, carbonTable.getTablePath)
    schemaConverter.fromWrapperToExternalTableInfo(
      wrapperTableInfo, carbonTable.getDatabaseName, carbonTable.getTableName)
  }

  /**
   * This method add/modify the table comments.
   *
   * @param tableIdentifier
   * @param properties
   * @param propKeys
   * @param set
   * @param sparkSession
   */
  def modifyTableProperties(tableIdentifier: TableIdentifier, properties: Map[String, String],
      propKeys: Seq[String], set: Boolean)
    (sparkSession: SparkSession, catalog: SessionCatalog): Unit = {
    val tableName = tableIdentifier.table
    val dbName = tableIdentifier.database.getOrElse(sparkSession.catalog.currentDatabase)
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK, LockUsage.COMPACTION_LOCK)
    var locks = List.empty[ICarbonLock]
    try {
      locks = AlterTableUtil
        .validateTableAndAcquireLock(dbName, tableName, locksToBeAcquired)(sparkSession)
      val carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
      val lowerCasePropertiesMap: mutable.Map[String, String] = mutable.Map.empty
      // convert all the keys to lower case
      properties.foreach { entry =>
        lowerCasePropertiesMap.put(entry._1.toLowerCase, entry._2)
      }
      val thriftTable = readLatestTableSchema(carbonTable)(sparkSession)
      val tblPropertiesMap = thriftTable.fact_table.getTableProperties.asScala

      // validate for spatial index column
      CommonUtil.validateForSpatialTypeColumn(tblPropertiesMap ++ lowerCasePropertiesMap)

      // validate the required cache level properties
      validateColumnMetaCacheAndCacheLevel(carbonTable, lowerCasePropertiesMap)

      // validate the local dictionary properties
      validateLocalDictionaryProperties(lowerCasePropertiesMap, tblPropertiesMap, carbonTable)

      // validate the load min size properties
      validateLoadMinSizeProperties(carbonTable, lowerCasePropertiesMap)

      // validate the range column properties
      validateRangeColumnProperties(carbonTable, lowerCasePropertiesMap)

      CommonUtil.validateGlobalSortPartitions(lowerCasePropertiesMap)

      // validate the Sort Scope and Sort Columns
      validateSortScopeAndSortColumnsProperties(carbonTable,
        lowerCasePropertiesMap,
        tblPropertiesMap)

      // validate the Compaction Level Threshold
      validateCompactionLevelThresholdProperties(carbonTable, lowerCasePropertiesMap)

      val cacheExpiration = lowerCasePropertiesMap.get(CarbonCommonConstants
        .INDEX_CACHE_EXPIRATION_TIME_IN_SECONDS)
      if (cacheExpiration.isDefined) {
        CommonUtil.validateCacheExpiration(lowerCasePropertiesMap, CarbonCommonConstants
          .INDEX_CACHE_EXPIRATION_TIME_IN_SECONDS)
      }

      // if SORT_COLUMN is changed, it will move them to the head of column list
      // Make an schemaEvolution entry as we changed the schema with different column order with
      // alter set sort columns
      val schemaEvolutionEntry = updateSchemaForSortColumns(thriftTable, lowerCasePropertiesMap)
      // validate long string columns
      val longStringColumns = lowerCasePropertiesMap.get("long_string_columns");
      if (longStringColumns.isDefined) {
        validateLongStringColumns(longStringColumns.get, carbonTable)
        // update schema for long string columns
        updateSchemaForLongStringColumns(thriftTable, longStringColumns.get)
      } else if (propKeys.exists(_.equalsIgnoreCase("long_string_columns") && !set)) {
        if (tblPropertiesMap.exists(prop => prop._1.equalsIgnoreCase("long_string_columns"))) {
          updateSchemaForLongStringColumns(thriftTable, "")
        }
      }
      // validate set for streaming table
      val streamingOption = lowerCasePropertiesMap.get("streaming")
      if (streamingOption.isDefined && set) {
        if (carbonTable.isIndexTable) {
          throw new UnsupportedOperationException("Set streaming table is " +
            "not allowed on the index table.")
        }
        val indexTables = CarbonIndexUtil.getSecondaryIndexes(carbonTable)
        if (!indexTables.isEmpty) {
          throw new UnsupportedOperationException("Set streaming table is " +
            "not allowed for tables which are having index(s).")
        }
      }
      // below map will be used for cache invalidation. As tblProperties map is getting modified
      // in the next few steps the original map need to be retained for any decision making
      val existingTablePropertiesMap = mutable.Map(tblPropertiesMap.toSeq: _*)
      if (set) {
        // This overrides old newProperties and update the comment parameter of thriftTable
        // with the newly added/modified comment since thriftTable also holds comment as its
        // direct property.
        lowerCasePropertiesMap.foreach { property =>
          if (validateTableProperties(property._1) ||
              (property._1.startsWith(CarbonCommonConstants.SPATIAL_INDEX) &&
               property._1.endsWith("instance"))) {
            tblPropertiesMap.put(property._1, property._2)
          } else {
            val errorMessage = "Error: Invalid option(s): " + property._1.toString()
            throw new MalformedCarbonCommandException(errorMessage)
          }
        }
        // check if duplicate columns are present in both local dictionary include and exclude
        CarbonScalaUtil.validateDuplicateColumnsForLocalDict(tblPropertiesMap)
      } else {
        // This removes the comment parameter from thriftTable
        // since thriftTable also holds comment as its property.
        propKeys.foreach { propKey =>
          if (validateTableProperties(propKey)) {
            // This check is required because for old tables we need to keep same behavior for it,
            // meaning, local dictionary should be disabled. To enable we can use set command for
            // older tables. So no need to remove from table properties map for unset just to ensure
            // for older table behavior. So in case of unset, if enable property is already present
            // in map, then just set it to default value of local dictionary which is true.
            if (propKey.equalsIgnoreCase(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE)) {
              tblPropertiesMap
                .put(propKey.toLowerCase, CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE_DEFAULT)
              lowerCasePropertiesMap
                .put(propKey.toLowerCase, CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE_DEFAULT)
            } else if (propKey.equalsIgnoreCase(CarbonCommonConstants.SORT_SCOPE)) {
              tblPropertiesMap
                .put(propKey.toLowerCase, CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)
              lowerCasePropertiesMap
                .put(propKey.toLowerCase, CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE_DEFAULT)
            } else if (propKey.equalsIgnoreCase(CarbonCommonConstants.SORT_COLUMNS)) {
              val errorMessage = "Error: Invalid option(s): " + propKey +
                                 ", please set SORT_COLUMNS as empty instead of unset"
              throw new MalformedCarbonCommandException(errorMessage)
            } else {
              tblPropertiesMap.remove(propKey.toLowerCase)
            }
          } else {
            val errorMessage = "Error: Invalid option(s): " + propKey
            throw new MalformedCarbonCommandException(errorMessage)
          }
        }
        // check if duplicate columns are present in both local dictionary include and exclude
        CarbonScalaUtil.validateDuplicateColumnsForLocalDict(tblPropertiesMap)
      }
      val tableIdentifier = updateSchemaInfo(
        carbonTable = carbonTable,
        schemaEvolutionEntry,
        thriftTable = thriftTable)(sparkSession)
      CarbonSessionCatalogUtil.alterTableProperties(
        sparkSession, tableIdentifier, lowerCasePropertiesMap.toMap, propKeys)
      sparkSession.catalog.refreshTable(tableIdentifier.quotedString)
      // check and clear the block/blocklet cache
      checkAndClearBlockletCache(carbonTable,
        existingTablePropertiesMap,
        lowerCasePropertiesMap,
        propKeys,
        set)
      LOGGER.info(s"Alter table newProperties is successful for table $dbName.$tableName")
    } catch {
      case e: Exception =>
        sys.error(s"Alter table newProperties operation failed: ${e.getMessage}")
    } finally {
      // release lock after command execution completion
      AlterTableUtil.releaseLocks(locks)
    }
  }

  private def validateTableProperties(propKey: String): Boolean = {
    val supportedOptions = Seq("STREAMING",
      "COMMENT",
      "COLUMN_META_CACHE",
      "CACHE_LEVEL",
      "COMPACTION_LEVEL_THRESHOLD",
      "LOCAL_DICTIONARY_ENABLE",
      "LOCAL_DICTIONARY_THRESHOLD",
      "LOCAL_DICTIONARY_INCLUDE",
      "LOCAL_DICTIONARY_EXCLUDE",
      "LOAD_MIN_SIZE_INMB",
      "RANGE_COLUMN",
      "SORT_SCOPE",
      "SORT_COLUMNS",
      "MINOR_COMPACTION_SIZE",
      "MAJOR_COMPACTION_SIZE",
      "GLOBAL_SORT_PARTITIONS",
      "LONG_STRING_COLUMNS",
      "INDEX_CACHE_EXPIRATION_SECONDS",
      "DATEFORMAT",
      "TIMESTAMPFORMAT")
    supportedOptions.contains(propKey.toUpperCase)
  }

  /**
   * this method validates the local dictionary properties for alter set
   *
   * @param lowerCasePropertiesMap
   * @param tblPropertiesMap
   * @param carbonTable
   */
  private def validateLocalDictionaryProperties(lowerCasePropertiesMap: mutable.Map[String, String],
      tblPropertiesMap: mutable.Map[String, String],
      carbonTable: CarbonTable): Unit = {
    lowerCasePropertiesMap.foreach { property =>
      if (property._1.equalsIgnoreCase(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE)) {
        if (!CarbonScalaUtil.validateLocalDictionaryEnable(property._2)) {
          lowerCasePropertiesMap
            .put(property._1.toLowerCase, CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE_DEFAULT)
        } else {
          lowerCasePropertiesMap.put(property._1, property._2)
        }
      }
      if (property._1.equalsIgnoreCase(CarbonCommonConstants.LOCAL_DICTIONARY_INCLUDE) ||
          property._1.equalsIgnoreCase(CarbonCommonConstants.LOCAL_DICTIONARY_EXCLUDE)) {
        ValidateSetTablePropertiesForLocalDict(tblPropertiesMap, carbonTable, property)
      }

      if (property._1
        .equalsIgnoreCase(CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD)) {
        if (!CarbonScalaUtil.validateLocalDictionaryThreshold(property._2)) {
          lowerCasePropertiesMap
            .put(property._1,
              CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD_DEFAULT)
        } else {
          lowerCasePropertiesMap.put(property._1, property._2)
        }
      }
    }
  }

  /**
   * validate column meta cache and cache level properties if configured by the user
   *
   * @param carbonTable
   * @param propertiesMap
   */
  private def validateColumnMetaCacheAndCacheLevel(carbonTable: CarbonTable,
      propertiesMap: mutable.Map[String, String]): Unit = {
    // validate column meta cache property
    if (propertiesMap.get(CarbonCommonConstants.COLUMN_META_CACHE).isDefined) {
      val schemaList: util.List[ColumnSchema] = CarbonUtil
        .getColumnSchemaList(carbonTable.getVisibleDimensions.asScala
          .asJava, carbonTable.getVisibleMeasures)
      val tableColumns: Seq[String] = schemaList.asScala
        .map(columnSchema => columnSchema.getColumnName)
      CommonUtil
        .validateColumnMetaCacheFields(carbonTable.getDatabaseName,
          carbonTable.getTableName,
          tableColumns,
          propertiesMap.get(CarbonCommonConstants.COLUMN_META_CACHE).get,
          propertiesMap)
      val columnsToBeCached = propertiesMap.get(CarbonCommonConstants.COLUMN_META_CACHE).get
      validateForComplexTypeColumn(carbonTable, columnsToBeCached)
    }
    // validate cache level property
    if (propertiesMap.get(CarbonCommonConstants.CACHE_LEVEL).isDefined) {
      CommonUtil.validateCacheLevel(
        propertiesMap.get(CarbonCommonConstants.CACHE_LEVEL).get,
        propertiesMap)
    }
  }

  def validateRangeColumnProperties(carbonTable: CarbonTable,
      propertiesMap: mutable.Map[String, String]): Unit = {
    if (propertiesMap.get(CarbonCommonConstants.RANGE_COLUMN).isDefined) {
      val rangeColumnProp = propertiesMap.get(CarbonCommonConstants.RANGE_COLUMN).get
      if (rangeColumnProp.contains(",")) {
        val errorMsg = "range_column not support multiple columns"
        throw new MalformedCarbonCommandException(errorMsg)
      }
      val rangeColumn = carbonTable.getColumnByName(rangeColumnProp)
      if (rangeColumn == null) {
        throw new MalformedCarbonCommandException(
          s"Table property ${ CarbonCommonConstants.RANGE_COLUMN }: ${ rangeColumnProp }" +
          s" is not exists in the table")
      }
      val dataType = rangeColumn.getDataType.getName;
      if (CarbonParserUtil.validateUnsupportedDataTypeForRangeColumn(dataType)) {
        throw new MalformedCarbonCommandException(
          s"RANGE_COLUMN doesn't support $dataType data type: " + rangeColumnProp)
      } else {
        propertiesMap.put(CarbonCommonConstants.RANGE_COLUMN, rangeColumn.getColName)
      }
    }
  }

  def validateSortScopeAndSortColumnsProperties(carbonTable: CarbonTable,
                                                propertiesMap: mutable.Map[String, String],
                                                tblPropertiesMap: mutable.Map[String, String]
                                               ): Unit = {
    CommonUtil.validateSortScope(propertiesMap)
    CommonUtil.validateSortColumns(carbonTable, propertiesMap)
    val indexProp = tblPropertiesMap.get(CarbonCommonConstants.SPATIAL_INDEX)
    if (indexProp.isDefined) {
      indexProp.get.split(",").map(_.trim).foreach { indexName =>
        val SOURCE_COLUMNS = s"${ CarbonCommonConstants.SPATIAL_INDEX }.$indexName.sourcecolumns"
        val sourceColumns = tblPropertiesMap(SOURCE_COLUMNS).split(",").map(_.trim)
        // Add spatial index column as a sort column if it is not already present in it.
        CarbonScalaUtil.insertColumnToSortColumns(indexName, sourceColumns, propertiesMap)
      }
    }
    // match SORT_SCOPE and SORT_COLUMNS
    val newSortScope = propertiesMap.get(CarbonCommonConstants.SORT_SCOPE)
    val newSortColumns = propertiesMap.get(CarbonCommonConstants.SORT_COLUMNS)
    if (newSortScope.isDefined) {
      // 1. check SORT_COLUMNS when SORT_SCOPE is not changed to NO_SORT
      if (!SortScope.NO_SORT.name().equalsIgnoreCase(newSortScope.get)) {
        if (newSortColumns.isDefined) {
          if (StringUtils.isBlank(CarbonUtil.unquoteChar(newSortColumns.get))) {
            throw new InvalidConfigurationException(
              s"Cannot set SORT_COLUMNS as empty when setting SORT_SCOPE as ${newSortScope.get} ")
          }
        } else {
          if (carbonTable.getNumberOfSortColumns == 0) {
            throw new InvalidConfigurationException(
              s"Cannot set SORT_SCOPE as ${newSortScope.get} when table has no SORT_COLUMNS")
          }
        }
      }
    } else if (newSortColumns.isDefined) {
      // 2. check SORT_SCOPE when SORT_COLUMNS is changed to empty
      if (StringUtils.isBlank(CarbonUtil.unquoteChar(newSortColumns.get))) {
        if (!SortScope.NO_SORT.equals(carbonTable.getSortScope)) {
          throw new InvalidConfigurationException(
            s"Cannot set SORT_COLUMNS as empty when SORT_SCOPE is ${carbonTable.getSortScope} ")
        }
      }
    }
  }

  def validateCompactionLevelThresholdProperties(carbonTable: CarbonTable,
      propertiesMap: mutable.Map[String, String]): Unit = {
    val newCompactionLevelThreshold =
      propertiesMap.get(CarbonCommonConstants.TABLE_COMPACTION_LEVEL_THRESHOLD)
    if (newCompactionLevelThreshold.isDefined) {
      // check compaction level threshold is in the specified range and in the format of number
      if (CarbonProperties.getInstance().getIntArray(newCompactionLevelThreshold.get).length == 0) {
        throw new InvalidConfigurationException(
          s"Cannot set COMPACTION_LEVEL_THRESHOLD as ${newCompactionLevelThreshold.get}")
      }
    }
  }

  /**
   * This method will validate if there is any complex type column in the columns to be cached
   *
   * @param carbonTable
   * @param cachedColumns
   */
  private def validateForComplexTypeColumn(carbonTable: CarbonTable,
      cachedColumns: String): Unit = {
    if (cachedColumns.nonEmpty) {
      cachedColumns.split(",").foreach { column =>
        val dimension = carbonTable.getDimensionByName(column)
        if (null != dimension && dimension.isComplex) {
          val errorMessage =
            s"$column is a complex type column and complex type is not allowed for " +
            s"the option(s): ${ CarbonCommonConstants.COLUMN_META_CACHE }"
          throw new MalformedCarbonCommandException(errorMessage)
        }
      }
    }
  }

  /**
   * This method will check and clear the driver block/blocklet cache
   *
   * @param carbonTable
   * @param existingTableProperties
   * @param newProperties
   * @param propKeys
   * @param set
   */
  private def checkAndClearBlockletCache(carbonTable: CarbonTable,
      existingTableProperties: scala.collection.mutable.Map[String, String],
      newProperties: mutable.Map[String, String],
      propKeys: Seq[String],
      set: Boolean): Unit = {
    if (set) {
      clearBlockletCacheForCachingProperties(carbonTable, existingTableProperties, newProperties)
    } else {
      // convert all the unset keys to lower case
      val propertiesToBeRemoved = propKeys.map(key => key.toLowerCase)
      // This is unset scenario and the cache needs to be cleaned only when
      // 1. column_meta_cache property is getting unset and existing table properties contains
      // this property
      // 2. cache_level property is being unset and existing table properties contains this property
      // and the existing value is not equal to default value because after un-setting the property
      // the cache should be loaded again with default value
      if (propertiesToBeRemoved.contains(CarbonCommonConstants.COLUMN_META_CACHE) &&
          existingTableProperties.get(CarbonCommonConstants.COLUMN_META_CACHE).isDefined) {
        clearCache(carbonTable)
      } else if (propertiesToBeRemoved.contains(CarbonCommonConstants.CACHE_LEVEL)) {
        val cacheLevel = existingTableProperties.get(CarbonCommonConstants.CACHE_LEVEL)
        if (cacheLevel.isDefined &&
            !cacheLevel.equals(CarbonCommonConstants.CACHE_LEVEL_DEFAULT_VALUE)) {
          clearCache(carbonTable)
        }
      }
    }
  }

  private def clearCache(carbonTable: CarbonTable): Unit = {
    // clear indexes cache
    IndexStoreManager.getInstance().clearIndex(carbonTable.getAbsoluteTableIdentifier)
    // clear segmentProperties Cache
    SegmentPropertiesAndSchemaHolder.getInstance()
      .invalidate(carbonTable.getAbsoluteTableIdentifier)
  }

  /**
   * This method will validate the column_meta_cache and cache_level properties and clear the
   * driver block/blocklet cache
   *
   * @param carbonTable
   * @param tblPropertiesMap
   * @param newProperties
   */
  private def clearBlockletCacheForCachingProperties(
      carbonTable: CarbonTable,
      tblPropertiesMap: scala.collection.mutable.Map[String, String],
      newProperties: mutable.Map[String, String]): Unit = {
    // check if column meta cache is defined. if defined then validate and clear the BTree cache
    // if required
    val columnMetaCacheProperty = newProperties.get(CarbonCommonConstants.COLUMN_META_CACHE)
    columnMetaCacheProperty match {
      case Some(newColumnsToBeCached) =>
        if (!checkIfColumnsAreAlreadyCached(carbonTable, tblPropertiesMap
          .get(CarbonCommonConstants.COLUMN_META_CACHE), newColumnsToBeCached)) {
          clearCache(carbonTable)
        }
      case None =>
      // don't do anything
    }
    // check if column meta cache is defined. if defined then validate and clear the BTree cache
    // if required
    val cacheLevelProperty = newProperties.get(CarbonCommonConstants.CACHE_LEVEL)
    cacheLevelProperty match {
      case Some(newCacheLevelValue) =>
        if (!isCacheLevelValid(tblPropertiesMap.get(CarbonCommonConstants.CACHE_LEVEL),
          newCacheLevelValue)) {
          clearCache(carbonTable)
        }
      case None =>
      // don't do anything
    }
  }

  /**
   * Method to verify if the existing cache level is same as the new cache level
   *
   * @param existingCacheLevelValue
   * @param newCacheLevelValue
   * @return
   */
  private def isCacheLevelValid(existingCacheLevelValue: Option[String],
      newCacheLevelValue: String): Boolean = {
    existingCacheLevelValue match {
      case Some(existingValue) =>
        existingValue.equals(newCacheLevelValue)
      case None =>
        false
    }
  }

  /**
   * Check the new columns to be cached with the already cached columns. If count of new columns
   * and already cached columns is same and all the new columns are already cached then
   * false will be returned else true
   *
   * @param carbonTable
   * @param existingCacheColumns
   * @param newColumnsToBeCached
   * @return
   */
  private def checkIfColumnsAreAlreadyCached(
      carbonTable: CarbonTable,
      existingCacheColumns: Option[String],
      newColumnsToBeCached: String): Boolean = {
    val newColumns = newColumnsToBeCached.split(",").map(x => x.trim.toLowerCase)
    val isCached = existingCacheColumns match {
      case Some(value) =>
        val existingProperty = value.split(",").map(x => x.trim.toLowerCase)
        compareColumns(existingProperty, newColumns)
      case None =>
        // By default all the columns in the table will be cached. This case is to compare all the
        // table columns already cached to the newly specified cached columns
        val schemaList: util.List[ColumnSchema] = CarbonUtil
          .getColumnSchemaList(carbonTable.getVisibleDimensions, carbonTable.getVisibleMeasures)
        val tableColumns: Array[String] = schemaList.asScala
          .map(columnSchema => columnSchema.getColumnName).toArray
        compareColumns(tableColumns, newColumns)
    }
    isCached
  }

  /**
   * compare the existing cache columns and the new columns to be cached
   *
   * @param existingCachedColumns
   * @param newColumnsToBeCached
   * @return
   */
  private def compareColumns(existingCachedColumns: Array[String],
      newColumnsToBeCached: Array[String]): Boolean = {
    val allColumnsMatch = if (existingCachedColumns.length == newColumnsToBeCached.length) {
      existingCachedColumns.filter(col => !newColumnsToBeCached.contains(col)).length == 0
    } else {
      false
    }
    allColumnsMatch
  }

  /**
   * Validate LONG_STRING_COLUMNS property specified in Alter command
   *
   * @param longStringColumns
   * @param carbonTable
   */
  def validateLongStringColumns(longStringColumns: String,
      carbonTable: CarbonTable): Unit = {
    // don't allow duplicate column names
    val longStringCols = longStringColumns.split(",").map(column => column.trim.toLowerCase)
    if (longStringCols.distinct.lengthCompare(longStringCols.size) != 0) {
      val duplicateColumns = longStringCols
        .diff(longStringCols.distinct).distinct
      val errMsg =
        "LONG_STRING_COLUMNS contains Duplicate Columns: " +
        duplicateColumns.mkString(",") + ". Please check the DDL."
      throw new MalformedCarbonCommandException(errMsg)
    }
    // check if the column specified exists in table schema and must be of string data type
    val colSchemas = carbonTable.getTableInfo.getFactTable.getListOfColumns.asScala
    longStringCols.foreach { col =>
      if (!colSchemas.exists(x => x.getColumnName.equalsIgnoreCase(col))) {
        val errorMsg = s"LONG_STRING_COLUMNS column: $col does not exist in table. Please check " +
                       s"the DDL."
        throw new MalformedCarbonCommandException(errorMsg)
      } else if (colSchemas.exists(x => x.getColumnName.equalsIgnoreCase(col) &&
                                        x.isComplexColumn)) {
        val errMsg = s"Complex child column $col cannot be set as LONG_STRING_COLUMNS."
        throw new MalformedCarbonCommandException(errMsg)
      } else if (colSchemas.exists(x => x.getColumnName.equalsIgnoreCase(col) &&
                                        !x.getDataType.toString
                                          .equalsIgnoreCase("STRING"))) {
        val errMsg = s"LONG_STRING_COLUMNS column: $col is not a string datatype column"
        throw new MalformedCarbonCommandException(errMsg)
      }
    }
    // should not be present in sort columns
    val sortCols = carbonTable.getSortColumns
    if (sortCols != null) {
      for (col <- longStringCols) {
        if (sortCols.contains(col)) {
          val errMsg =
            "LONG_STRING_COLUMNS cannot be present in sort columns: " + col
          throw new MalformedCarbonCommandException(errMsg)
        }
      }
    }
    // should not be present in index tables
    val secondaryIndexMap = carbonTable.getIndexesMap.get(IndexType.SI.getIndexProviderName)
    if (secondaryIndexMap != null) {
      secondaryIndexMap.asScala.foreach(indexTable => {
        indexTable._2.asScala(CarbonCommonConstants.INDEX_COLUMNS).split(",").foreach(col =>
          if (longStringCols.contains(col.toLowerCase)) {
            throw new MalformedCarbonCommandException(s"Cannot Alter column $col to " +
              s"Long_string_column, as the column exists in a secondary index with name " +
              s"${indexTable._1}. LONG_STRING_COLUMNS is not allowed on secondary index.")
          }
        )
      })
    }
  }

  /**
   * Validate LOCAL_DICT_COLUMNS property specified in Alter command
   * @param tblPropertiesMap
   * @param carbonTable
   * @param property
   */
  def ValidateSetTablePropertiesForLocalDict(tblPropertiesMap: mutable.Map[String, String],
      carbonTable: CarbonTable,
      property: (String, String)): Unit = {
    var localDictColumns: Seq[String] = Seq[String]()

    val allColumns = carbonTable.getTableInfo.getFactTable.getListOfColumns.asScala
    localDictColumns = property._2.toString.toLowerCase.split(",").map(_.trim)

    CarbonScalaUtil.validateLocalDictionaryColumns(tblPropertiesMap, localDictColumns)

    // check if the column specified exists in table schema
    localDictColumns.foreach { distCol =>
      if (!allColumns.exists(x => x.getColumnName.equalsIgnoreCase(distCol.trim))) {
        val errorMsg = "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: " + distCol.trim +
                       " does not exist in table. Please check the DDL."
        throw new MalformedCarbonCommandException(errorMsg)
      }
    }

    /**
     * Verify if specified column is of no-dictionary string or varchar dataType
     */
    localDictColumns.foreach { dictCol =>
      if (allColumns.exists(col => col.getColumnName.equalsIgnoreCase(dictCol) &&
                                   !col.getDataType.toString
                                     .equalsIgnoreCase("STRING") &&
                                   !col.getDataType.toString
                                     .equalsIgnoreCase("VARCHAR") &&
                                   !col.getDataType.isComplexType)) {
        val errMsg = "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: " + dictCol.trim +
                     " is not a string/complex/varchar datatype column. LOCAL_DICTIONARY_INCLUDE" +
                     "/LOCAL_DICTIONARY_EXCLUDE should be no " +
                     "dictionary string/complex/varchar datatype column."
        throw new MalformedCarbonCommandException(errMsg)
      }
    }
    var countOfDictCols = 0
    var trav = 0
    // Validate whether any of the child columns of complex dataType column is a string or
    // varchar dataType column
    if (property._1.equalsIgnoreCase(CarbonCommonConstants.LOCAL_DICTIONARY_INCLUDE)) {
      // Validate whether any of the child columns of complex dataType column is a string column
      localDictColumns.foreach { dictColumn =>
        for (elem <- allColumns.indices) {
          var column = allColumns(elem)
          if (column.getColumnName.equalsIgnoreCase(dictColumn) && column.getNumberOfChild > 0 &&
              !validateChildColumns(allColumns, column.getNumberOfChild, elem. +(1))) {
            val errMsg =
              "None of the child columns specified in the complex dataType column(s) in " +
              "local_dictionary_include are not of string dataType."
            throw new MalformedCarbonCommandException(errMsg)
          }
        }
      }
    }

    /**
     * check whether any child column present in complex type column is string or varchar type
     *
     * @param schemas
     * @return
     */
    def validateChildColumns(schemas: mutable.Buffer[ColumnSchema],
        colCount: Int, traverse: Int): Boolean = {
      trav = traverse
      var column: ColumnSchema = null
      for (i <- 0 until colCount) {
        column = schemas(trav)
        if (column.getNumberOfChild > 0) {
          validateChildColumns(schemas, column.getNumberOfChild, trav. +(1))
        } else {
          if (column.isDimensionColumn && (column.getDataType.equals(DataTypes.STRING) ||
                                           column.getDataType.equals(DataTypes.VARCHAR))) {
            countOfDictCols += 1
          }
          trav = trav + 1
        }
      }
      if (countOfDictCols > 0) {
        return true
      }
      false
    }
  }

  private def validateLoadMinSizeProperties(carbonTable: CarbonTable,
      propertiesMap: mutable.Map[String, String]): Unit = {
    // validate load min size property
    if (propertiesMap.get(CarbonCommonConstants.CARBON_LOAD_MIN_SIZE_INMB).isDefined) {
      CommonUtil.validateLoadMinSize(propertiesMap, CarbonCommonConstants.CARBON_LOAD_MIN_SIZE_INMB)
    }
  }

  def validateColumnsWithSpatialIndexProperties(carbonTable: CarbonTable, alterColumns: Seq[String])
  : Unit = {
    // Do not allow spatial index column and its source columns to be altered
    val properties = carbonTable.getTableInfo.getFactTable.getTableProperties.asScala
    val indexProperty = properties.get(CarbonCommonConstants.SPATIAL_INDEX)
    if (indexProperty.isDefined) {
      indexProperty.get.split(",").map(_.trim).foreach { element =>
        val srcColumns
        = properties.get(CarbonCommonConstants.SPATIAL_INDEX + s".$element.sourcecolumns")
        val common = alterColumns.intersect(srcColumns.get.split(",").map(_.trim))
        if (common.nonEmpty || alterColumns.contains(element)) {
          throw new MalformedCarbonCommandException(s"Columns present in " +
            s"${CarbonCommonConstants.SPATIAL_INDEX} table property cannot be altered/updated")
        }
      }
    }
  }

  /**
   * This method checks the structure of the old and new complex columns, and-
   * 1. throws exception if the number of complex-levels in both columns does not match
   * 2. throws exception if the number of children of both columns does not match
   * 3. creates alteredColumnNamesMap: new_column_name -> datatype. Here new_column_name are those
   *    names of the columns that are altered.
   * These maps will later be used while altering the table schema
   */
  def validateComplexStructure(oldDimensionList: List[CarbonDimension],
      newDimensionList: List[DataTypeInfo],
      alteredColumnNamesMap: mutable.LinkedHashMap[String, String],
      alteredDatatypesMap: mutable.LinkedHashMap[String, String]): Unit = {
    if (oldDimensionList == null && newDimensionList == null) {
      throw new UnsupportedOperationException("Both old and new dimensions are null")
    } else if (oldDimensionList == null || newDimensionList == null) {
      throw new UnsupportedOperationException("Either the old or the new dimension is null")
    } else if (oldDimensionList.size != newDimensionList.size) {
      throw new UnsupportedOperationException(
        "Number of children of old and new complex columns are not the same")
    } else {
      for ((newDimensionInfo, i) <- newDimensionList.zipWithIndex) {
        val oldDimensionInfo = oldDimensionList(i)
        val old_column_name = oldDimensionInfo
          .getColName.split(CarbonCommonConstants.POINT.toCharArray).last
        val old_column_datatype = oldDimensionInfo.getDataType.getName
        val new_column_name = newDimensionInfo
          .columnName.split(CarbonCommonConstants.POINT.toCharArray).last
        var new_column_datatype = newDimensionInfo.dataType

        // check if column datatypes are altered. If altered, validate them
        if (!old_column_datatype.equalsIgnoreCase(new_column_datatype)) {
          this.validateColumnDataType(newDimensionInfo, oldDimensionInfo)
          alteredDatatypesMap += (oldDimensionInfo.getColName -> new_column_datatype)
        } else if (old_column_datatype.equalsIgnoreCase(CarbonCommonConstants.DECIMAL) &&
                   old_column_datatype.equalsIgnoreCase(new_column_datatype)) {
          val oldPrecision = oldDimensionInfo.getDataType().asInstanceOf[DecimalType].getPrecision
          val oldScale = oldDimensionInfo.getDataType().asInstanceOf[DecimalType].getScale
          if (oldPrecision != newDimensionInfo.precision || oldScale != newDimensionInfo.scale) {
            this.validateColumnDataType(newDimensionInfo, oldDimensionInfo)
            new_column_datatype = "decimal(" + newDimensionInfo.precision + "," +
                                  newDimensionInfo.scale + ")"
            alteredDatatypesMap += (oldDimensionInfo.getColName -> new_column_datatype)
          }
        }

        // check if column names are altered
        if (!old_column_name.equalsIgnoreCase(new_column_name)) {
          alteredColumnNamesMap += (oldDimensionInfo.getColName -> newDimensionInfo.columnName)
        }
        if (isComplexType(new_column_datatype) || isComplexType(old_column_datatype)) {
          validateComplexStructure(oldDimensionInfo.getListOfChildDimensions.asScala.toList,
            newDimensionInfo.getChildren(), alteredColumnNamesMap, alteredDatatypesMap)
        }
      }
    }
  }

  // To identify if the datatype name is of complex type.
  def isComplexType(dataTypeName: String): Boolean = {
    dataTypeName.equalsIgnoreCase(CarbonCommonConstants.ARRAY) ||
    dataTypeName.equalsIgnoreCase(CarbonCommonConstants.STRUCT) ||
    dataTypeName.equalsIgnoreCase(CarbonCommonConstants.MAP)
  }

  /**
   * This method will validate a column for its data type and check whether the column data type
   * can be modified and update if conditions are met.
   */
  def validateColumnDataType(
      dataTypeInfo: DataTypeInfo,
      carbonColumn: CarbonColumn): Unit = {
    carbonColumn.getDataType.getName.toLowerCase() match {
      case CarbonCommonConstants.INT =>
        if (!dataTypeInfo.dataType.equalsIgnoreCase(CarbonCommonConstants.BIGINT) &&
            !dataTypeInfo.dataType.equalsIgnoreCase(CarbonCommonConstants.LONG)) {
          sys.error(s"Given column ${ carbonColumn.getColName } with data type " +
                    s"${ carbonColumn.getDataType.getName } cannot be modified. " +
                    s"Int can only be changed to bigInt or long")
        }
      case CarbonCommonConstants.DECIMAL =>
        if (!dataTypeInfo.dataType.equalsIgnoreCase(CarbonCommonConstants.DECIMAL)) {
          sys.error(s"Given column ${ carbonColumn.getColName } with data type" +
                    s" ${ carbonColumn.getDataType.getName } cannot be modified." +
                    s" Decimal can be only be changed to Decimal of higher precision")
        }
        if (dataTypeInfo.precision <= carbonColumn.getColumnSchema.getPrecision) {
          sys.error(s"Given column ${ carbonColumn.getColName } cannot be modified. " +
                    s"Specified precision value ${ dataTypeInfo.precision } should be " +
                    s"greater than current precision value " +
                    s"${ carbonColumn.getColumnSchema.getPrecision }")
        } else if (dataTypeInfo.scale < carbonColumn.getColumnSchema.getScale) {
          sys.error(s"Given column ${ carbonColumn.getColName } cannot be modified. " +
                    s"Specified scale value ${ dataTypeInfo.scale } should be greater or " +
                    s"equal to current scale value ${ carbonColumn.getColumnSchema.getScale }")
        } else {
          // difference of precision and scale specified by user should not be less than the
          // difference of already existing precision and scale else it will result in data loss
          val carbonColumnPrecisionScaleDiff = carbonColumn.getColumnSchema.getPrecision -
                                               carbonColumn.getColumnSchema.getScale
          val dataInfoPrecisionScaleDiff = dataTypeInfo.precision - dataTypeInfo.scale
          if (dataInfoPrecisionScaleDiff < carbonColumnPrecisionScaleDiff) {
            sys.error(s"Given column ${ carbonColumn.getColName } cannot be modified. " +
                      s"Specified precision and scale values will lead to data loss")
          }
        }
      case _ =>
        if (!carbonColumn.getDataType.getName.equalsIgnoreCase(dataTypeInfo.dataType)) {
          sys.error(s"Given column ${ carbonColumn.getColName } with data type " +
                    s"${ carbonColumn.getDataType.getName } cannot be modified. " +
                    s"Only Int and Decimal data types are allowed for modification")
        }
    }
  }

}
