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
import org.apache.spark.SparkConf
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.hive.{CarbonRelation, CarbonSessionCatalogUtil}
import org.apache.spark.sql.hive.HiveExternalCatalog._

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.constants.SortScopeOptions.SortScope
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.block.SegmentPropertiesAndSchemaHolder
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.exception.InvalidConfigurationException
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.converter.{SchemaConverter, ThriftWrapperSchemaConverterImpl}
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.util.CarbonUtil
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
      lowerCasePropertiesMap: mutable.Map[String, String],
      schemaConverter: SchemaConverter
  ) = {
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
      }
    }
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
    (sparkSession: SparkSession): (TableIdentifier, String) = {
    val dbName = carbonTable.getDatabaseName
    val tableName = carbonTable.getTableName
    CarbonEnv.getInstance(sparkSession).carbonMetaStore
      .updateTableSchemaForAlter(carbonTable.getCarbonTableIdentifier,
        carbonTable.getCarbonTableIdentifier,
        thriftTable,
        schemaEvolutionEntry,
        carbonTable.getAbsoluteTableIdentifier.getTablePath)(sparkSession)
    val tableIdentifier = TableIdentifier(tableName, Some(dbName))
    sparkSession.catalog.refreshTable(tableIdentifier.quotedString)
    val schema = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      .lookupRelation(tableIdentifier)(sparkSession).schema.json
    val schemaParts = prepareSchemaJsonForAlterTable(sparkSession.sparkContext.getConf, schema)
    (tableIdentifier, schemaParts)
  }

  /**
   * This method will split schema string into multiple parts of configured size and
   * registers the parts as keys in tableProperties which will be read by spark to prepare
   * Carbon Table fields
   *
   * @param sparkConf
   * @param schemaJsonString
   * @return
   */
  private def prepareSchemaJsonForAlterTable(sparkConf: SparkConf,
      schemaJsonString: String): String = {
    val threshold = sparkConf
      .getInt(CarbonCommonConstants.SPARK_SCHEMA_STRING_LENGTH_THRESHOLD,
        CarbonCommonConstants.SPARK_SCHEMA_STRING_LENGTH_THRESHOLD_DEFAULT)
    // Split the JSON string.
    val parts = schemaJsonString.grouped(threshold).toSeq
    var schemaParts: Seq[String] = Seq.empty
    schemaParts = schemaParts :+ s"'$DATASOURCE_SCHEMA_NUMPARTS'='${ parts.size }'"
    parts.zipWithIndex.foreach { case (part, index) =>
      schemaParts = schemaParts :+ s"'$DATASOURCE_SCHEMA_PART_PREFIX$index'='$part'"
    }
    schemaParts.mkString(",")
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
    val fileType = FileFactory.getFileType(tablePath)
    if (FileFactory.isFileExist(tablePath, fileType)) {
      val tableInfo = metastore.getThriftTableInfo(oldCarbonTable)
      val evolutionEntryList = tableInfo.fact_table.schema_evolution.schema_evolution_history
      val updatedTime = evolutionEntryList.get(evolutionEntryList.size() - 1).time_stamp
      if (updatedTime == timeStamp) {
        LOGGER.error(s"Reverting changes for $database.${oldCarbonTable.getTableName}")
        val absoluteTableIdentifier = AbsoluteTableIdentifier.from(
          tablePath,
          newCarbonTableIdentifier)
        metastore.revertTableSchemaInAlterFailure(oldCarbonTableIdentifier,
          tableInfo, absoluteTableIdentifier)(sparkSession)
        metastore.removeTableFromMetadata(database, newTableName)
      }
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
    val metastore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    val carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
    val thriftTable: TableInfo = metastore.getThriftTableInfo(carbonTable)
    val evolutionEntryList = thriftTable.fact_table.schema_evolution.schema_evolution_history
    val updatedTime = evolutionEntryList.get(evolutionEntryList.size() - 1).time_stamp
    if (updatedTime == timeStamp) {
      LOGGER.info(s"Reverting changes for $dbName.$tableName")
      val addedSchemas = evolutionEntryList.get(evolutionEntryList.size() - 1).added
      thriftTable.fact_table.table_columns.removeAll(addedSchemas)
      metastore
        .revertTableSchemaInAlterFailure(carbonTable.getCarbonTableIdentifier,
          thriftTable, carbonTable.getAbsoluteTableIdentifier)(sparkSession)
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
    val metastore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    val carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
    val thriftTable: TableInfo = metastore.getThriftTableInfo(carbonTable)
    val evolutionEntryList = thriftTable.fact_table.schema_evolution.schema_evolution_history
    val updatedTime = evolutionEntryList.get(evolutionEntryList.size() - 1).time_stamp
    if (updatedTime == timeStamp) {
      LOGGER.error(s"Reverting changes for $dbName.$tableName")
      val removedSchemas = evolutionEntryList.get(evolutionEntryList.size() - 1).removed
      thriftTable.fact_table.table_columns.asScala.foreach { columnSchema =>
        removedSchemas.asScala.foreach { removedSchemas =>
          if (columnSchema.invisible && removedSchemas.column_id == columnSchema.column_id) {
            columnSchema.setInvisible(false)
          }
        }
      }
      metastore
        .revertTableSchemaInAlterFailure(carbonTable.getCarbonTableIdentifier,
          thriftTable, carbonTable.getAbsoluteTableIdentifier)(sparkSession)
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
    val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    val carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
    val thriftTable: TableInfo = metaStore.getThriftTableInfo(carbonTable)
    val evolutionEntryList = thriftTable.fact_table.schema_evolution.schema_evolution_history
    val updatedTime = evolutionEntryList.get(evolutionEntryList.size() - 1).time_stamp
    if (updatedTime == timeStamp) {
      LOGGER.error(s"Reverting changes for $dbName.$tableName")
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
      metaStore
        .revertTableSchemaInAlterFailure(carbonTable.getCarbonTableIdentifier,
          thriftTable, carbonTable.getAbsoluteTableIdentifier)(sparkSession)
    }
  }

  /**
   * This method modifies the table properties if column rename happened
   * @param tableProperties tableProperties of the table
   * @param oldColumnName old COlumnname before rename
   * @param newColumnName new column name to rename
   */
  def modifyTablePropertiesAfterColumnRename(
      tableProperties: mutable.Map[String, String],
      oldColumnName: String,
      newColumnName: String): Unit = {
    tableProperties.foreach { tableProperty =>
      if (tableProperty._2.contains(oldColumnName)) {
        val tablePropertyKey = tableProperty._1
        val tablePropertyValue = tableProperty._2
        tableProperties
          .put(tablePropertyKey, tablePropertyValue.replace(oldColumnName, newColumnName))
      }
    }
  }

  /**
   * This method create a new SchemaEvolutionEntry and adds to SchemaEvolutionEntry List
   *
   * @param schemaEvolutionEntry List to add new SchemaEvolutionEntry
   * @param addColumnSchema          added new column schema
   * @param deletedColumnSchema      old column schema which is deleted
   * @return
   */
  def addNewSchemaEvolutionEntry(
      schemaEvolutionEntry: SchemaEvolutionEntry,
      timeStamp: Long,
      addColumnSchema: org.apache.carbondata.format.ColumnSchema,
      deletedColumnSchema: org.apache.carbondata.format.ColumnSchema): SchemaEvolutionEntry = {
    var schemaEvolutionEntry = new SchemaEvolutionEntry(timeStamp)
    schemaEvolutionEntry.setAdded(List(addColumnSchema).asJava)
    schemaEvolutionEntry.setRemoved(List(deletedColumnSchema).asJava)
    schemaEvolutionEntry
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
      val metastore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      val carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
      val lowerCasePropertiesMap: mutable.Map[String, String] = mutable.Map.empty
      // convert all the keys to lower case
      properties.foreach { entry =>
        lowerCasePropertiesMap.put(entry._1.toLowerCase, entry._2)
      }
      // validate the required cache level properties
      validateColumnMetaCacheAndCacheLevel(carbonTable, lowerCasePropertiesMap)
      // get the latest carbon table
      // read the latest schema file
      val thriftTableInfo: TableInfo = metastore.getThriftTableInfo(carbonTable)
      val schemaConverter = new ThriftWrapperSchemaConverterImpl()
      val wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(
        thriftTableInfo,
        dbName,
        tableName,
        carbonTable.getTablePath)
      val thriftTable = schemaConverter.fromWrapperToExternalTableInfo(
        wrapperTableInfo, dbName, tableName)
      val tblPropertiesMap: mutable.Map[String, String] =
        thriftTable.fact_table.getTableProperties.asScala

      // validate the local dictionary properties
      validateLocalDictionaryProperties(lowerCasePropertiesMap, tblPropertiesMap, carbonTable)

      // validate the load min size properties
      validateLoadMinSizeProperties(carbonTable, lowerCasePropertiesMap)

      // validate the range column properties
      validateRangeColumnProperties(carbonTable, lowerCasePropertiesMap)

      // validate the Sort Scope and Sort Columns
      validateSortScopeAndSortColumnsProperties(carbonTable, lowerCasePropertiesMap)
      // if SORT_COLUMN is changed, it will move them to the head of column list
      updateSchemaForSortColumns(thriftTable, lowerCasePropertiesMap, schemaConverter)
      // below map will be used for cache invalidation. As tblProperties map is getting modified
      // in the next few steps the original map need to be retained for any decision making
      val existingTablePropertiesMap = mutable.Map(tblPropertiesMap.toSeq: _*)
      if (set) {
        // This overrides old newProperties and update the comment parameter of thriftTable
        // with the newly added/modified comment since thriftTable also holds comment as its
        // direct property.
        lowerCasePropertiesMap.foreach { property =>
          if (validateTableProperties(property._1)) {
            tblPropertiesMap.put(property._1, property._2)
          } else {
            val errorMessage = "Error: Invalid option(s): " + property._1.toString()
            throw new MalformedCarbonCommandException(errorMessage)
          }
        }
        // check if duplicate columns are present in both local dictionary include and exclude
        CarbonScalaUtil.validateDuplicateLocalDictIncludeExcludeColmns(tblPropertiesMap)
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
            } else if (propKey.equalsIgnoreCase(CarbonCommonConstants.SORT_SCOPE)) {
              tblPropertiesMap
                .put(propKey.toLowerCase, CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)
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
        CarbonScalaUtil.validateDuplicateLocalDictIncludeExcludeColmns(tblPropertiesMap)
      }
      val (tableIdentifier, schemParts) = updateSchemaInfo(
        carbonTable = carbonTable,
        thriftTable = thriftTable)(sparkSession)
      CarbonSessionCatalogUtil.alterTable(tableIdentifier, schemParts, None, sparkSession)
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
      "LOCAL_DICTIONARY_ENABLE",
      "LOCAL_DICTIONARY_THRESHOLD",
      "LOCAL_DICTIONARY_INCLUDE",
      "LOCAL_DICTIONARY_EXCLUDE",
      "LOAD_MIN_SIZE_INMB",
      "RANGE_COLUMN",
      "SORT_SCOPE",
      "SORT_COLUMNS",
      "GLOBAL_SORT_PARTITIONS")
    supportedOptions.contains(propKey.toUpperCase)
  }

  /**
   * this method validates the local dictioanry properties for alter set
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
      // Column meta cache is not allowed for child tables and dataMaps
      if (carbonTable.isChildDataMap) {
        throw new MalformedCarbonCommandException(s"Table property ${
          CarbonCommonConstants.COLUMN_META_CACHE} is not allowed for child datamaps")
      }
      val schemaList: util.List[ColumnSchema] = CarbonUtil
        .getColumnSchemaList(carbonTable.getDimensionByTableName(carbonTable.getTableName),
          carbonTable.getMeasureByTableName(carbonTable.getTableName))
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
      // Cache level is not allowed for child tables and dataMaps
      if (carbonTable.isChildDataMap) {
        throw new MalformedCarbonCommandException(s"Table property ${
          CarbonCommonConstants.CACHE_LEVEL} is not allowed for child datamaps")
      }
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
      val rangeColumn = carbonTable.getColumnByName(carbonTable.getTableName, rangeColumnProp)
      if (rangeColumn == null) {
        throw new MalformedCarbonCommandException(
          s"Table property ${ CarbonCommonConstants.RANGE_COLUMN }: ${ rangeColumnProp }" +
          s" is not exists in the table")
      } else {
        propertiesMap.put(CarbonCommonConstants.RANGE_COLUMN, rangeColumn.getColName)
      }
    }
  }

  def validateSortScopeAndSortColumnsProperties(carbonTable: CarbonTable,
      propertiesMap: mutable.Map[String, String]): Unit = {
    CommonUtil.validateSortScope(propertiesMap)
    CommonUtil.validateSortColumns(carbonTable, propertiesMap)
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
        val dimension = carbonTable.getDimensionByName(carbonTable.getTableName, column)
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
    // clear dataMap cache
    DataMapStoreManager.getInstance().clearDataMaps(carbonTable.getAbsoluteTableIdentifier)
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
          .getColumnSchemaList(carbonTable.getDimensionByTableName(carbonTable.getTableName),
            carbonTable.getMeasureByTableName(carbonTable.getTableName))
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
   * Validate LOCAL_DICT_COLUMNS property specified in Alter command
   * @param tblPropertiesMap
   * @param carbonTable
   * @param property
   */
  def ValidateSetTablePropertiesForLocalDict(tblPropertiesMap: mutable.Map[String, String],
      carbonTable: CarbonTable,
      property: (String, String)): Unit = {
    var primitiveComplexChildColumns = new mutable.HashSet[String]
    var localDictColumns: Seq[String] = Seq[String]()
    var dictIncludeColumns: Seq[String] = Seq[String]()

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
                                   !col.getDataType.toString
                                     .equalsIgnoreCase("STRUCT") &&
                                   !col.getDataType.toString
                                     .equalsIgnoreCase("ARRAY"))) {
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
      localDictColumns.foreach { dictColm =>
        for (elem <- allColumns.indices) {
          var column = allColumns(elem)
          if (column.getColumnName.equalsIgnoreCase(dictColm) && column.getNumberOfChild > 0 &&
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
      // load min size is not allowed for child tables and dataMaps
      if (carbonTable.isChildDataMap) {
        throw new MalformedCarbonCommandException(s"Table property ${
          CarbonCommonConstants.CARBON_LOAD_MIN_SIZE_INMB} is not allowed for child datamaps")
      }
      CommonUtil.validateLoadMinSize(propertiesMap,
        CarbonCommonConstants.CARBON_LOAD_MIN_SIZE_INMB)
    }
  }
}
