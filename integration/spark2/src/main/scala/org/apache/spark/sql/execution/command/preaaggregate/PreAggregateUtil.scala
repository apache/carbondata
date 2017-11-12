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
package org.apache.spark.sql.execution.command.preaaggregate

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkConf
import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Cast}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.{ColumnTableRelation, DataMapField, Field}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.hive.HiveExternalCatalog.{DATASOURCE_SCHEMA_NUMPARTS, DATASOURCE_SCHEMA_PART_PREFIX}
import org.apache.spark.sql.types.DataType

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.format.TableInfo
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.util.CommonUtil

/**
 * Utility class for keeping all the utility method for pre-aggregate
 */
object PreAggregateUtil {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def getParentCarbonTable(plan: LogicalPlan): CarbonTable = {
    plan match {
      case Aggregate(_, aExp, SubqueryAlias(_, l: LogicalRelation, _))
        if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
        l.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonRelation.metaData.carbonTable
      case _ => throw new MalformedCarbonCommandException("table does not exist")
    }
  }

  /**
   * Below method will be used to validate the select plan
   * and get the required fields from select plan
   * Currently only aggregate query is support any other type of query will fail
   *
   * @param plan
   * @param selectStmt
   * @return list of fields
   */
  def validateActualSelectPlanAndGetAttrubites(plan: LogicalPlan,
      selectStmt: String): scala.collection.mutable.LinkedHashMap[Field, DataMapField] = {
    val fieldToDataMapFieldMap = scala.collection.mutable.LinkedHashMap.empty[Field, DataMapField]
    plan match {
      case Aggregate(_, aExp, SubqueryAlias(_, l: LogicalRelation, _))
        if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
        val carbonTable = l.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonRelation
          .metaData.carbonTable
        val parentTableName = carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier
          .getTableName
        val parentDatabaseName = carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier
          .getDatabaseName
        val parentTableId = carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier
          .getTableId
        if (!carbonTable.getTableInfo.getParentRelationIdentifiers.isEmpty) {
          throw new MalformedCarbonCommandException(
            "Pre Aggregation is not supported on Pre-Aggregated Table")
        }
        aExp.map {
          case Alias(attr: AggregateExpression, _) =>
            if (attr.isDistinct) {
              throw new MalformedCarbonCommandException(
                "Distinct is not supported On Pre Aggregation")
            }
            fieldToDataMapFieldMap ++= (validateAggregateFunctionAndGetFields(carbonTable,
              attr.aggregateFunction,
              parentTableName,
              parentDatabaseName,
              parentTableId))
          case attr: AttributeReference =>
            fieldToDataMapFieldMap += getField(attr.name,
              attr.dataType,
              parentColumnId = carbonTable.getColumnByName(parentTableName, attr.name).getColumnId,
              parentTableName = parentTableName,
              parentDatabaseName = parentDatabaseName, parentTableId = parentTableId)
          case Alias(attr: AttributeReference, _) =>
            fieldToDataMapFieldMap += getField(attr.name,
              attr.dataType,
              parentColumnId = carbonTable.getColumnByName(parentTableName, attr.name).getColumnId,
              parentTableName = parentTableName,
              parentDatabaseName = parentDatabaseName, parentTableId = parentTableId)
          case _ =>
            throw new MalformedCarbonCommandException(s"Unsupported Select Statement:${
              selectStmt } ")
        }
        Some(carbonTable)
      case _ =>
        throw new MalformedCarbonCommandException(s"Unsupported Select Statement:${ selectStmt } ")
    }
    fieldToDataMapFieldMap
  }

  /**
   * Below method will be used to validate about the aggregate function
   * which is applied on select query.
   * Currently sum, max, min, count, avg is supported
   * in case of any other aggregate function it will throw error
   * In case of avg it will return two fields one for count
   * and other of sum of that column to support rollup
   *
   * @param carbonTable
   * @param aggFunctions
   * @param parentTableName
   * @param parentDatabaseName
   * @param parentTableId
   * @return list of fields
   */
  def validateAggregateFunctionAndGetFields(carbonTable: CarbonTable,
      aggFunctions: AggregateFunction,
      parentTableName: String,
      parentDatabaseName: String,
      parentTableId: String) : scala.collection.mutable.ListBuffer[(Field, DataMapField)] = {
    val list = scala.collection.mutable.ListBuffer.empty[(Field, DataMapField)]
    aggFunctions match {
      case sum@Sum(attr: AttributeReference) =>
        list += getField(attr.name,
          attr.dataType,
          sum.prettyName,
          carbonTable.getColumnByName(parentTableName, attr.name).getColumnId,
          parentTableName,
          parentDatabaseName, parentTableId = parentTableId)
      case sum@Sum(Cast(attr: AttributeReference, changeDataType: DataType)) =>
        list += getField(attr.name,
          changeDataType,
          sum.prettyName,
          carbonTable.getColumnByName(parentTableName, attr.name).getColumnId,
          parentTableName,
          parentDatabaseName, parentTableId = parentTableId)
      case count@Count(Seq(attr: AttributeReference)) =>
        list += getField(attr.name,
          attr.dataType,
          count.prettyName,
          carbonTable.getColumnByName(parentTableName, attr.name).getColumnId,
          parentTableName,
          parentDatabaseName, parentTableId = parentTableId)
      case min@Min(attr: AttributeReference) =>
        list += getField(attr.name,
          attr.dataType,
          min.prettyName,
          carbonTable.getColumnByName(parentTableName, attr.name).getColumnId,
          parentTableName,
          parentDatabaseName, parentTableId = parentTableId)
      case min@Min(Cast(attr: AttributeReference, changeDataType: DataType)) =>
        list += getField(attr.name,
          changeDataType,
          min.prettyName,
          carbonTable.getColumnByName(parentTableName, attr.name).getColumnId,
          parentTableName,
          parentDatabaseName, parentTableId = parentTableId)
      case max@Max(attr: AttributeReference) =>
        list += getField(attr.name,
          attr.dataType,
          max.prettyName,
          carbonTable.getColumnByName(parentTableName, attr.name).getColumnId,
          parentTableName,
          parentDatabaseName, parentTableId = parentTableId)
      case max@Max(Cast(attr: AttributeReference, changeDataType: DataType)) =>
        list += getField(attr.name,
          changeDataType,
          max.prettyName,
          carbonTable.getColumnByName(parentTableName, attr.name).getColumnId,
          parentTableName,
          parentDatabaseName, parentTableId = parentTableId)
      case Average(attr: AttributeReference) =>
        getField(attr.name,
          attr.dataType,
          "sum",
          carbonTable.getColumnByName(parentTableName, attr.name).getColumnId,
          parentTableName,
          parentDatabaseName, parentTableId = parentTableId)
        list += getField(attr.name,
          attr.dataType,
          "count",
          carbonTable.getColumnByName(parentTableName, attr.name).getColumnId,
          parentTableName,
          parentDatabaseName, parentTableId = parentTableId)
      case Average(Cast(attr: AttributeReference, changeDataType: DataType)) =>
        list += getField(attr.name,
          changeDataType,
          "sum",
          carbonTable.getColumnByName(parentTableName, attr.name).getColumnId,
          parentTableName,
          parentDatabaseName, parentTableId = parentTableId)
        list += getField(attr.name,
          changeDataType,
          "count",
          carbonTable.getColumnByName(parentTableName, attr.name).getColumnId,
          parentTableName,
          parentDatabaseName, parentTableId = parentTableId)
      case _ =>
        throw new MalformedCarbonCommandException("Un-Supported Aggregation Type")
    }
  }

  /**
   * Below method will be used to get the fields object for pre aggregate table
   *
   * @param columnName
   * @param dataType
   * @param aggregateType
   * @param parentColumnId
   * @param parentTableName
   * @param parentDatabaseName
   * @param parentTableId
   * @return fields object
   */
  def getField(columnName: String,
      dataType: DataType,
      aggregateType: String = "",
      parentColumnId: String,
      parentTableName: String,
      parentDatabaseName: String,
      parentTableId: String): (Field, DataMapField) = {
    val actualColumnName = if (aggregateType.equals("")) {
      parentTableName + '_' + columnName
    } else {
      parentTableName + '_' + columnName + '_' + aggregateType
    }
    val rawSchema = '`' + actualColumnName + '`' + ' ' + dataType.typeName
    val columnTableRelation = ColumnTableRelation(parentColumnName = columnName,
      parentColumnId = parentColumnId,
      parentTableName = parentTableName,
      parentDatabaseName = parentDatabaseName, parentTableId = parentTableId)
    val dataMapField = DataMapField(aggregateType, Some(columnTableRelation))
    if (dataType.typeName.startsWith("decimal")) {
      val (precision, scale) = CommonUtil.getScaleAndPrecision(dataType.catalogString)
      (Field(column = actualColumnName,
        dataType = Some(dataType.typeName),
        name = Some(actualColumnName),
        children = None,
        precision = precision,
        scale = scale,
        rawSchema = rawSchema), dataMapField)
    } else {
      (Field(column = actualColumnName,
        dataType = Some(dataType.typeName),
        name = Some(actualColumnName),
        children = None,
        rawSchema = rawSchema), dataMapField)
    }
  }

  /**
   * Below method will be used to update the main table about the pre aggregate table information
   * in case of any exception it will throw error so pre aggregate table creation will fail
   *
   * @param dbName
   * @param tableName
   * @param childSchema
   * @param sparkSession
   */
  def updateMainTable(dbName: String, tableName: String,
      childSchema: DataMapSchema, sparkSession: SparkSession): Unit = {
    val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK,
      LockUsage.DROP_TABLE_LOCK)
    var locks = List.empty[ICarbonLock]
    var carbonTable: CarbonTable = null
    var numberOfCurrentChild: Int = 0
    try {
      val metastore = CarbonEnv.getInstance(sparkSession).carbonMetastore
      carbonTable = metastore
        .lookupRelation(Some(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation]
        .tableMeta.carbonTable
      locks = acquireLock(dbName, tableName, locksToBeAcquired, carbonTable)
      // get the latest carbon table and check for column existence
      // read the latest schema file
      val carbonTablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getStorePath,
        carbonTable.getCarbonTableIdentifier)
      val thriftTableInfo: TableInfo = metastore.getThriftTableInfo(carbonTablePath)(sparkSession)
      val schemaConverter = new ThriftWrapperSchemaConverterImpl()
      val wrapperTableInfo = schemaConverter
        .fromExternalToWrapperTableInfo(thriftTableInfo,
          dbName,
          tableName,
          carbonTable.getStorePath)
      numberOfCurrentChild = wrapperTableInfo.getDataMapSchemaList.size
      wrapperTableInfo.getDataMapSchemaList.add(childSchema)
      val thriftTable = schemaConverter
        .fromWrapperToExternalTableInfo(wrapperTableInfo, dbName, tableName)
      updateSchemaInfo(carbonTable,
        thriftTable)(sparkSession)
      LOGGER.info(s"Parent table updated is successful for table $dbName.$tableName")
    } catch {
      case e: Exception =>
        LOGGER.error(e, "Pre Aggregate Parent table update failed reverting changes")
        revertMainTableChanges(dbName, tableName, numberOfCurrentChild)(sparkSession)
        throw e
    } finally {
      // release lock after command execution completion
      releaseLocks(locks)
    }
    Seq.empty
  }

  /**
   * Below method will be used to update the main table schema
   *
   * @param carbonTable
   * @param thriftTable
   * @param sparkSession
   */
  def updateSchemaInfo(carbonTable: CarbonTable,
      thriftTable: TableInfo)(sparkSession: SparkSession): Unit = {
    val dbName = carbonTable.getDatabaseName
    val tableName = carbonTable.getFactTableName
    CarbonEnv.getInstance(sparkSession).carbonMetastore
      .updateTableSchemaForDataMap(carbonTable.getCarbonTableIdentifier,
        carbonTable.getCarbonTableIdentifier,
        thriftTable,
        carbonTable.getAbsoluteTableIdentifier.getTablePath)(sparkSession)
    val tableIdentifier = TableIdentifier(tableName, Some(dbName))
    sparkSession.catalog.refreshTable(tableIdentifier.quotedString)
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
  private def prepareSchemaJson(sparkConf: SparkConf,
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
   * Validates that the table exists and acquires meta lock on it.
   *
   * @param dbName
   * @param tableName
   * @return
   */
  def acquireLock(dbName: String,
      tableName: String,
      locksToBeAcquired: List[String],
      table: CarbonTable): List[ICarbonLock] = {
    // acquire the lock first
    val acquiredLocks = ListBuffer[ICarbonLock]()
    try {
      locksToBeAcquired.foreach { lock =>
        acquiredLocks += CarbonLockUtil.getLockObject(table.getCarbonTableIdentifier, lock)
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
        LOGGER.info("Pre agg table lock released successfully")
      } else {
        LOGGER.error("Unable to release lock during Pre agg table cretion")
      }
    }
  }

  /**
   * This method reverts the changes to the schema if add column command fails.
   *
   * @param dbName
   * @param tableName
   * @param numberOfChildSchema
   * @param sparkSession
   */
  def revertMainTableChanges(dbName: String, tableName: String, numberOfChildSchema: Int)
    (sparkSession: SparkSession): Unit = {
    val metastore = CarbonEnv.getInstance(sparkSession).carbonMetastore
    val carbonTable = metastore
      .lookupRelation(Some(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation].tableMeta
      .carbonTable
    carbonTable.getTableLastUpdatedTime
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getStorePath,
      carbonTable.getCarbonTableIdentifier)
    val thriftTable: TableInfo = metastore.getThriftTableInfo(carbonTablePath)(sparkSession)
    if (thriftTable.dataMapSchemas.size > numberOfChildSchema) {
      metastore
        .revertTableSchemaForPreAggCreationFailure(carbonTable.getCarbonTableIdentifier,
          thriftTable, carbonTable.getAbsoluteTableIdentifier.getTablePath)(sparkSession)
    }
  }

  def getChildCarbonTable(databaseName: String, tableName: String)
    (sparkSession: SparkSession): Option[CarbonTable] = {
    val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetastore
    metaStore.getTableFromMetadataCache(databaseName, tableName) match {
      case Some(tableMeta) => Some(tableMeta.carbonTable)
      case None => try {
        Some(metaStore.lookupRelation(Some(databaseName), tableName)(sparkSession)
          .asInstanceOf[CarbonRelation].metaData.carbonTable)
      } catch {
        case _: Exception =>
          None
      }
    }
  }

  def checkMainTableLoad(carbonTable: CarbonTable): Boolean = {
    SegmentStatusManager.readLoadMetadata(
      carbonTable.getMetaDataFilepath).nonEmpty
  }
}
