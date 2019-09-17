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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, CarbonEnv, CarbonUtils, SparkSession, _}
import org.apache.spark.sql.CarbonExpressions.{CarbonSubqueryAlias => SubqueryAlias}
import org.apache.spark.sql.CarbonExpressions.MatchCastExpression
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedFunction, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, _}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.{ColumnTableRelation, DataMapField, Field}
import org.apache.spark.sql.execution.command.management.CarbonLoadDataCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.{CarbonMetaStore, CarbonRelation}
import org.apache.spark.sql.parser.CarbonSpark2SqlParser
import org.apache.spark.sql.types.DataType

import org.apache.carbondata.common.exceptions.MetadataProcessException
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.schema.datamap.DataMapProperty
import org.apache.carbondata.core.metadata.schema.table.{AggregationDataMapSchema, CarbonTable, DataMapSchema, TableSchema}
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.format.TableInfo
import org.apache.carbondata.spark.util.CommonUtil

/**
 * Utility class for keeping all the utility method for pre-aggregate
 */
object PreAggregateUtil {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def getParentCarbonTable(plan: LogicalPlan): CarbonTable = {
    plan match {
      case Aggregate(_, _, SubqueryAlias(_, logicalRelation: LogicalRelation))
        if logicalRelation.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
        logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation].
          carbonRelation.metaData.carbonTable
      case Aggregate(_, _, logicalRelation: LogicalRelation)
        if logicalRelation.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
        logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation].
          carbonRelation.metaData.carbonTable
      case _ => throw new MalformedCarbonCommandException("table does not exist")
    }
  }

  /**
   * Below method will be used to validate the select plan
   * and get the required fields from select plan
   * Currently only aggregate query is support, any other type of query will fail
   *
   * @param plan
   * @param selectStmt
   * @return list of fields
   */
  def validateActualSelectPlanAndGetAttributes(plan: LogicalPlan,
      selectStmt: String): scala.collection.mutable.LinkedHashMap[Field, DataMapField] = {
    plan match {
      case Aggregate(groupByExp, aggExp, SubqueryAlias(_, logicalRelation: LogicalRelation)) =>
        getFieldsFromPlan(groupByExp, aggExp, logicalRelation, selectStmt)
      case Aggregate(groupByExp, aggExp, logicalRelation: LogicalRelation) =>
        getFieldsFromPlan(groupByExp, aggExp, logicalRelation, selectStmt)
    }
  }

  /**
   * Below method will be used to get the fields from expressions
   * @param groupByExp grouping expression
   * @param aggExp aggregate expression
   * @param logicalRelation logical relation
   * @param selectStmt select statement
   * @return fields from expressions
   */
  def getFieldsFromPlan(groupByExp: Seq[Expression],
      aggExp: Seq[NamedExpression], logicalRelation: LogicalRelation, selectStmt: String):
  scala.collection.mutable.LinkedHashMap[Field, DataMapField] = {
    val fieldToDataMapFieldMap = scala.collection.mutable.LinkedHashMap.empty[Field, DataMapField]
    if (!logicalRelation.relation.isInstanceOf[CarbonDatasourceHadoopRelation]) {
      throw new MalformedCarbonCommandException("Un-supported table")
    }
    val carbonTable = logicalRelation.relation.
      asInstanceOf[CarbonDatasourceHadoopRelation].carbonRelation
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
    var counter = 0
    aggExp.map {
      case Alias(attr: AggregateExpression, name) =>
        if (attr.isDistinct) {
          throw new MalformedCarbonCommandException(
            "Distinct is not supported On Pre Aggregation")
        }
        fieldToDataMapFieldMap ++= validateAggregateFunctionAndGetFields(
          carbonTable,
          attr.aggregateFunction,
          parentTableName,
          parentDatabaseName,
          parentTableId,
          "column_" + counter)
        counter = counter + 1
      case attr: AttributeReference =>
        val columnRelation = getColumnRelation(
          attr.name,
          parentTableId,
          parentTableName,
          parentDatabaseName,
          carbonTable)
        fieldToDataMapFieldMap += createField(
          attr.name,
          attr.dataType,
          parentTableName = parentTableName,
          columnTableRelationList = Seq(columnRelation))
      case Alias(attr: AttributeReference, _) =>
        val columnRelation = getColumnRelation(
          attr.name,
          parentTableId,
          parentTableName,
          parentDatabaseName,
          carbonTable)
        fieldToDataMapFieldMap += createField(
          attr.name,
          attr.dataType,
          parentTableName = parentTableName,
          columnTableRelationList = Seq(columnRelation))
      case _@Alias(s: ScalaUDF, name) if name.equals("preAgg") =>
      case _ =>
        throw new MalformedCarbonCommandException(s"Unsupported Select Statement:${
          selectStmt } ")
    }
    groupByExp map {
      case attr: AttributeReference =>
        val columnRelation = getColumnRelation(
          attr.name,
          parentTableId,
          parentTableName,
          parentDatabaseName,
          carbonTable)
        fieldToDataMapFieldMap += createField(
          attr.name,
          attr.dataType,
          parentTableName = parentTableName,
          columnTableRelationList = Seq(columnRelation))
      case _ =>
        throw new MalformedCarbonCommandException(s"Unsupported Function in select Statement:${
          selectStmt }")
    }
    fieldToDataMapFieldMap
  }

  /**
   * Below method will be used to get the column relation
   * with the parent column which will be used during query and data loading
   * @param parentColumnName parent column name
   * @param parentTableId parent column id
   * @param parentTableName parent table name
   * @param parentDatabaseName parent database name
   * @param carbonTable carbon table
   * @return column relation object
   */
  def getColumnRelation(parentColumnName: String,
      parentTableId: String,
      parentTableName: String,
      parentDatabaseName: String,
      carbonTable: CarbonTable) : ColumnTableRelation = {
    val parentColumnId = carbonTable.getColumnByName(parentTableName, parentColumnName).getColumnId
    val columnTableRelation = ColumnTableRelation(parentColumnName = parentColumnName.toLowerCase(),
      parentColumnId = parentColumnId,
      parentTableName = parentTableName,
      parentDatabaseName = parentDatabaseName, parentTableId = parentTableId)
    columnTableRelation
  }

  /**
   * Below method will be used to validate about the aggregate function
   * which is applied on select query.
   * Currently sum, max, min, count, avg is supported
   * in case of any other aggregate function it will throw error
   * In case of avg it will return two fields one for count
   * and other of sum of that column to support rollup
   *
   * @param carbonTable parent carbon table
   * @param aggFunctions aggregation function
   * @param parentTableName parent table name
   * @param parentDatabaseName parent database name
   * @param parentTableId parent column id
   * @param newColumnName
   * In case of any expression this will be used as a column name for pre aggregate
   * @return list of fields
   */
  def validateAggregateFunctionAndGetFields(carbonTable: CarbonTable,
      aggFunctions: AggregateFunction,
      parentTableName: String,
      parentDatabaseName: String,
      parentTableId: String,
      newColumnName: String) : scala.collection.mutable.ListBuffer[(Field, DataMapField)] = {
    val list = scala.collection.mutable.ListBuffer.empty[(Field, DataMapField)]
    aggFunctions match {
      case sum@Sum(MatchCastExpression(exp: Expression, changeDataType: DataType)) =>
        list += createFieldForAggregateExpression(
          exp,
          changeDataType,
          carbonTable,
          newColumnName,
          sum.prettyName)
      case sum@Sum(exp: Expression) =>
        list += createFieldForAggregateExpression(
          exp,
          sum.dataType,
          carbonTable,
          newColumnName,
          sum.prettyName)
      case count@Count(Seq(MatchCastExpression(exp: Expression, changeDataType: DataType))) =>
        list += createFieldForAggregateExpression(
          exp,
          changeDataType,
          carbonTable,
          newColumnName,
          count.prettyName)
      case count@Count(Seq(exp: Expression)) =>
        list += createFieldForAggregateExpression(
          exp,
          count.dataType,
          carbonTable,
          newColumnName,
          count.prettyName)
      case min@Min(MatchCastExpression(exp: Expression, changeDataType: DataType)) =>
        list += createFieldForAggregateExpression(
          exp,
          changeDataType,
          carbonTable,
          newColumnName,
          min.prettyName)
      case min@Min(exp: Expression) =>
        list += createFieldForAggregateExpression(
          exp,
          min.dataType,
          carbonTable,
          newColumnName,
          min.prettyName)
      case max@Max(MatchCastExpression(exp: Expression, changeDataType: DataType)) =>
        list += createFieldForAggregateExpression(
          exp,
          changeDataType,
          carbonTable,
          newColumnName,
          max.prettyName)
      case max@Max(exp: Expression) =>
        list += createFieldForAggregateExpression(
          exp,
          max.dataType,
          carbonTable,
          newColumnName,
          max.prettyName)
      case Average(MatchCastExpression(exp: Expression, changeDataType: DataType)) =>
        list += createFieldForAggregateExpression(
          exp,
          changeDataType,
          carbonTable,
          newColumnName,
          "sum")
        list += createFieldForAggregateExpression(
          exp,
          changeDataType,
          carbonTable,
          newColumnName,
          "count")
      case avg@Average(exp: Expression) =>
        list += createFieldForAggregateExpression(
          exp,
          avg.dataType,
          carbonTable,
          newColumnName,
          "sum")
        list += createFieldForAggregateExpression(
          exp,
          avg.dataType,
          carbonTable,
          newColumnName,
          "count")
      case others@_ =>
        throw new MalformedCarbonCommandException(s"Un-Supported Aggregation Type: ${
          others.prettyName}")
    }
  }

  /**
   * Below method will be used to get the field and its data map field object
   * for aggregate expression
   * @param expression expression in aggregate function
   * @param dataType data type
   * @param carbonTable parent carbon table
   * @param newColumnName column name of aggregate table
   * @param aggregationName aggregate function name
   * @return field and its metadata tuple
   */
  def createFieldForAggregateExpression(
      expression: Expression,
      dataType: DataType,
      carbonTable: CarbonTable,
      newColumnName: String,
      aggregationName: String): (Field, DataMapField) = {
    val parentColumnsName = new ArrayBuffer[String]()
    expression.transform {
      case attr: AttributeReference =>
        parentColumnsName += attr.name
        attr
    }
    val arrayBuffer = parentColumnsName.map { name =>
       getColumnRelation(name,
        carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableId,
        carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableName,
        carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName,
        carbonTable)
    }
    // if parent column relation is of size more than one that means aggregate table
    // column is derived from multiple column of main table or if size is zero then it means
    // column is present in select statement is some constants for example count(*)
    // and if expression is not a instance of attribute reference
    // then use column name which is passed
    val columnName =
    if ((parentColumnsName.size > 1 || parentColumnsName.isEmpty) &&
        !expression.isInstanceOf[AttributeReference]) {
      newColumnName
    } else {
      if (expression.isInstanceOf[GetStructField] || expression.isInstanceOf[GetArrayItem]) {
        throw new UnsupportedOperationException(
          "Preaggregate is unsupported for ComplexData type column: " +
          expression.simpleString.replaceAll("#[0-9]*", ""))
      } else {
        expression.asInstanceOf[AttributeReference].name
      }
    }
    createField(columnName,
      dataType,
      aggregationName,
      carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableName,
      arrayBuffer)
  }

  /**
   * Below method will be used to get the fields object for pre aggregate table
   *
   * @param columnName
   * @param dataType
   * @param aggregateType
   * @param parentTableName
   * @param columnTableRelationList
   *                                List of column relation with parent
   * @return fields object
   */
  def createField(columnName: String,
      dataType: DataType,
      aggregateType: String = "",
      parentTableName: String,
      columnTableRelationList: Seq[ColumnTableRelation]): (Field, DataMapField) = {
    var actualColumnName = if (aggregateType.equals("")) {
      parentTableName + '_' + columnName
    } else {
      parentTableName + '_' + columnName + '_' + aggregateType
    }
    val rawSchema = '`' + actualColumnName + '`' + ' ' + dataType.typeName
    val dataMapField = DataMapField(aggregateType, Some(columnTableRelationList))
    actualColumnName = actualColumnName.toLowerCase()
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
   * @return the existing TableInfo object before updating, it can be used to recover if any
   *         operation failed later
   */
  def updateMainTable(carbonTable: CarbonTable,
      childSchema: DataMapSchema, sparkSession: SparkSession): TableInfo = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK,
      LockUsage.DROP_TABLE_LOCK)
    var locks = List.empty[ICarbonLock]
    val dbName = carbonTable.getDatabaseName
    val tableName = carbonTable.getTableName
    try {
      val metastore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      locks = acquireLock(dbName, tableName, locksToBeAcquired, carbonTable)
      // get the latest carbon table and check for column existence
      // read the latest schema file
      val thriftTableInfo: TableInfo = metastore.getThriftTableInfo(carbonTable)
      val schemaConverter = new ThriftWrapperSchemaConverterImpl()
      val wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(
        thriftTableInfo,
        dbName,
        tableName,
        carbonTable.getTablePath)
      if (wrapperTableInfo.getDataMapSchemaList.asScala.
        exists(f => f.getDataMapName.equalsIgnoreCase(childSchema.getDataMapName))) {
        throw new MetadataProcessException("DataMap name already exist")
      }
      wrapperTableInfo.getDataMapSchemaList.add(childSchema)
      val thriftTable = schemaConverter.fromWrapperToExternalTableInfo(
        wrapperTableInfo, dbName, tableName)
      updateSchemaInfo(carbonTable, thriftTable)(sparkSession)
      LOGGER.info(s"Parent table updated is successful for table $dbName.$tableName")
      thriftTableInfo
    } catch {
      case e: Exception =>
        LOGGER.error("Pre Aggregate Parent table update failed reverting changes", e)
        throw e
    } finally {
      // release lock after command execution completion
      releaseLocks(locks)
    }
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
    val tableName = carbonTable.getTableName
    CarbonEnv.getInstance(sparkSession).carbonMetaStore
      .updateTableSchema(carbonTable.getCarbonTableIdentifier,
        carbonTable.getCarbonTableIdentifier,
        thriftTable,
        carbonTable.getAbsoluteTableIdentifier.getTablePath)(sparkSession)
    val tableIdentifier = TableIdentifier(tableName, Some(dbName))
    sparkSession.catalog.refreshTable(tableIdentifier.quotedString)
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
    val metastore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    val carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
    carbonTable.getTableLastUpdatedTime
    val thriftTable: TableInfo = metastore.getThriftTableInfo(carbonTable)
    if (thriftTable.dataMapSchemas.size > numberOfChildSchema) {
      metastore.revertTableSchemaForPreAggCreationFailure(
        carbonTable.getAbsoluteTableIdentifier, thriftTable)(sparkSession)
    }
  }

  /**
   * Below method will be used to update logical plan
   * this is required for creating pre aggregate tables,
   * so @CarbonPreAggregateRules will not be applied during creation
   * @param logicalPlan actual logical plan
   * @return updated plan
   */
  def updatePreAggQueyPlan(logicalPlan: LogicalPlan): LogicalPlan = {
    val updatedPlan = logicalPlan.transform {
      case _@Project(projectList, child) =>
        val buffer = new ArrayBuffer[NamedExpression]()
        buffer ++= projectList
        buffer += UnresolvedAlias(Alias(UnresolvedFunction("preAgg",
          Seq.empty, isDistinct = false), "preAgg")())
        Project(buffer, child)
      case Aggregate(groupByExp, aggExp, l: UnresolvedRelation) =>
        val buffer = new ArrayBuffer[NamedExpression]()
        buffer ++= aggExp
        buffer += UnresolvedAlias(Alias(UnresolvedFunction("preAgg",
          Seq.empty, isDistinct = false), "preAgg")())
        Aggregate(groupByExp, buffer, l)
    }
    updatedPlan
  }
    /**
   * This method will start load process on the data map
   */
  def startDataLoadForDataMap(
      parentTableIdentifier: TableIdentifier,
      segmentToLoad: String,
      validateSegments: Boolean,
      loadCommand: CarbonLoadDataCommand,
      isOverwrite: Boolean,
      sparkSession: SparkSession): Boolean = {
    CarbonUtils.threadSet(
      CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
      parentTableIdentifier.database.getOrElse(sparkSession.catalog.currentDatabase) + "." +
      parentTableIdentifier.table,
      segmentToLoad)
    CarbonUtils.threadSet(
      CarbonCommonConstants.VALIDATE_CARBON_INPUT_SEGMENTS +
      parentTableIdentifier.database.getOrElse(sparkSession.catalog.currentDatabase) + "." +
      parentTableIdentifier.table, validateSegments.toString)
    CarbonUtils.threadSet(CarbonCommonConstants.SUPPORT_DIRECT_QUERY_ON_DATAMAP,
      "true")
    try {
      loadCommand.processData(sparkSession)
      true
    } catch {
      case ex: Exception =>
        LOGGER.error("Data Load failed for DataMap: ", ex)
        false
    } finally {
      CarbonUtils.threadUnset(
        CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
        parentTableIdentifier.database.getOrElse(sparkSession.catalog.currentDatabase) + "." +
        parentTableIdentifier.table)
      CarbonUtils.threadUnset(
        CarbonCommonConstants.VALIDATE_CARBON_INPUT_SEGMENTS +
        parentTableIdentifier.database.getOrElse(sparkSession.catalog.currentDatabase) + "." +
        parentTableIdentifier.table)
    }
  }

  def createChildSelectQuery(tableSchema: TableSchema, databaseName: String): String = {
    val aggregateColumns = scala.collection.mutable.ArrayBuffer.empty[String]
    val groupingExpressions = scala.collection.mutable.ArrayBuffer.empty[String]
    val columns = tableSchema.getListOfColumns.asScala
      .filter(f => !f.getColumnName.equals(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE))
    //  schema ordinal should be considered
    columns.sortBy(_.getSchemaOrdinal).foreach { a =>
      if (a.getAggFunction.nonEmpty) {
        aggregateColumns += s"${a.getAggFunction match {
          case "count" => "sum"
          case _ => a.getAggFunction}}(${a.getColumnName})"
      } else {
        groupingExpressions += a.getColumnName
        aggregateColumns += a.getColumnName
      }
    }
    val groupByString = if (groupingExpressions.nonEmpty) {
      s" group by ${ groupingExpressions.mkString(",") }"
    } else { "" }
    s"select ${ aggregateColumns.mkString(",") } " +
    s"from $databaseName.${ tableSchema.getTableName }" + groupByString
  }

  /**
   * Below method will be used to get the select query when rollup policy is
   * applied in case of timeseries table
   * @param tableSchema main data map schema
   * @param selectedDataMapSchema selected data map schema for rollup
   * @return select query based on rolloup
   */
  def createTimeseriesSelectQueryForRollup(
      tableSchema: TableSchema,
      selectedDataMapSchema: AggregationDataMapSchema,
      databaseName: String): String = {
    val aggregateColumns = scala.collection.mutable.ArrayBuffer.empty[String]
    val groupingExpressions = scala.collection.mutable.ArrayBuffer.empty[String]
    val columns = tableSchema.getListOfColumns.asScala
      .filter(f => !f.getColumnName.equals(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE))
      .sortBy(_.getSchemaOrdinal)
    columns.foreach { a =>
      if (a.getAggFunction.nonEmpty) {
        aggregateColumns += s"${a.getAggFunction match {
          case "count" => "sum"
          case others@_ => others}}(${selectedDataMapSchema.getAggChildColByParent(
          a.getParentColumnTableRelations.get(0).getColumnName, a.getAggFunction).getColumnName})"
      } else if (a.getTimeSeriesFunction.nonEmpty) {
        groupingExpressions += s"timeseries(${
          selectedDataMapSchema
            .getNonAggChildColBasedByParent(a.getParentColumnTableRelations.
              get(0).getColumnName).getColumnName
        } , '${ a.getTimeSeriesFunction }')"
        aggregateColumns += s"timeseries(${
          selectedDataMapSchema
            .getNonAggChildColBasedByParent(a.getParentColumnTableRelations.
              get(0).getColumnName).getColumnName
        } , '${ a.getTimeSeriesFunction }')"
      } else {
        groupingExpressions += selectedDataMapSchema
          .getNonAggChildColBasedByParent(a.getParentColumnTableRelations.
            get(0).getColumnName).getColumnName
        aggregateColumns += selectedDataMapSchema
          .getNonAggChildColBasedByParent(a.getParentColumnTableRelations.
            get(0).getColumnName).getColumnName
      }
    }
    s"select ${ aggregateColumns.mkString(",")
    } from $databaseName.${selectedDataMapSchema.getChildSchema.getTableName } " +
    s"group by ${ groupingExpressions.mkString(",") }"
  }

  /**
   * Below method will be used to creating select query for timeseries
   * for lowest level for aggergation like second level, in that case it will
   * hit the maintable
   * @param tableSchema data map schema
   * @param parentTableName parent schema
   * @return select query for loading
   */
  def createTimeSeriesSelectQueryFromMain(tableSchema: TableSchema,
      parentTableName: String,
      databaseName: String): String = {
    val aggregateColumns = scala.collection.mutable.ArrayBuffer.empty[String]
    val groupingExpressions = scala.collection.mutable.ArrayBuffer.empty[String]
    val columns = tableSchema.getListOfColumns.asScala
      .filter(f => !f.getColumnName.equals(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE))
      .sortBy(_.getSchemaOrdinal)
    columns.foreach {a =>
        if (a.getAggFunction.nonEmpty) {
          aggregateColumns +=
          s"${ a.getAggFunction }(${ a.getParentColumnTableRelations.get(0).getColumnName })"
        } else if (a.getTimeSeriesFunction.nonEmpty) {
          groupingExpressions +=
          s"timeseries(${ a.getParentColumnTableRelations.get(0).getColumnName },'${
            a.getTimeSeriesFunction}')"
          aggregateColumns +=
          s"timeseries(${ a.getParentColumnTableRelations.get(0).getColumnName },'${
            a.getTimeSeriesFunction
          }')"
        } else {
          groupingExpressions += a.getParentColumnTableRelations.get(0).getColumnName
          aggregateColumns += a.getParentColumnTableRelations.get(0).getColumnName
        }
    }
    s"select ${
      aggregateColumns.mkString(",")
    } from $databaseName.${ parentTableName } group by ${ groupingExpressions.mkString(",") }"

  }
    /**
   * Below method will be used to select rollup table in case of
   * timeseries data map loading
   * @param list list of timeseries datamap
   * @param dataMapSchema datamap schema
   * @return select table name
   */
  def getRollupDataMapNameForTimeSeries(
      list: scala.collection.mutable.ListBuffer[AggregationDataMapSchema],
      dataMapSchema: AggregationDataMapSchema): Option[AggregationDataMapSchema] = {
    if (list.isEmpty) {
      None
    } else {
      val rollupDataMapSchema = scala.collection.mutable.ListBuffer.empty[AggregationDataMapSchema]
      list.foreach{f =>
        if (dataMapSchema.canSelectForRollup(f)) {
          rollupDataMapSchema += f
        } }
      rollupDataMapSchema.lastOption
    }
  }

  /**
   * Below method will be used to validate aggregate function and get the attribute information
   * which is applied on select query.
   * Currently sum, max, min, count, avg is supported
   * in case of any other aggregate function it will return empty sequence
   * In case of avg it will return two fields one for count
   * and other of sum of that column to support rollup
   *
   * @param aggExp aggregate expression
   * @return list of fields
   */
  def validateAggregateFunctionAndGetFields(aggExp: AggregateExpression)
  : Seq[AggregateExpression] = {
    aggExp.aggregateFunction match {
      case Sum(MatchCastExpression(exp: Expression, changeDataType: DataType)) =>
        Seq(AggregateExpression(Sum(Cast(
          exp,
          changeDataType)),
          aggExp.mode,
          aggExp.isDistinct))
      case Sum(_: Expression) =>
        Seq(aggExp)
      case Count(MatchCastExpression(exp: Seq[_], changeDataType: DataType)) =>
        Seq(AggregateExpression(Count(Cast(
          exp,
          changeDataType)),
          aggExp.mode,
          aggExp.isDistinct))
      case Count(_: Seq[Expression]) =>
        Seq(aggExp)
      case Min(MatchCastExpression(exp: Expression, changeDataType: DataType)) =>
        Seq(AggregateExpression(Min(Cast(
          exp,
          changeDataType)),
          aggExp.mode,
          aggExp.isDistinct))
      case Min(exp: Expression) =>
        Seq(aggExp)
      case Max(MatchCastExpression(exp: Expression, changeDataType: DataType)) =>
        Seq(AggregateExpression(Max(Cast(
          exp,
          changeDataType)),
          aggExp.mode,
          aggExp.isDistinct))
      case Max(exp: Expression) =>
        Seq(aggExp)
      // in case of average need to return two columns
      // sum and count of the column to added during table creation to support rollup
      case Average(MatchCastExpression(exp: Expression, changeDataType: DataType)) =>
        Seq(AggregateExpression(Sum(Cast(
          exp,
          changeDataType)),
          aggExp.mode,
          aggExp.isDistinct),
          AggregateExpression(Count(exp),
            aggExp.mode,
            aggExp.isDistinct))
      // in case of average need to return two columns
      // sum and count of the column to added during table creation to support rollup
      case Average(exp: Expression) =>
        Seq(AggregateExpression(Sum(exp),
          aggExp.mode,
          aggExp.isDistinct),
          AggregateExpression(Count(exp),
            aggExp.mode,
            aggExp.isDistinct))
      case _ =>
        Seq.empty
    }
  }

  /**
   * Below method will be used to get the logical plan from aggregate expression
   * @param aggExp aggregate expression
   * @param tableName parent table name
   * @param databaseName database name
   * @param logicalRelation logical relation
   * @return logical plan
   */
  def getLogicalPlanFromAggExp(aggExp: AggregateExpression,
      tableName: String,
      databaseName: String,
      logicalRelation: LogicalRelation,
      sparkSession: SparkSession,
      parser: CarbonSpark2SqlParser): LogicalPlan = {
    // adding the preAGG UDF, so pre aggregate data loading rule and query rule will not
    // be applied
    val query = parser.addPreAggFunction(s"Select ${ aggExp.sql } from $databaseName.$tableName")
    // updating the logical relation of logical plan to so when two logical plan
    // will be compared it will not consider relation
    updateLogicalRelation(sparkSession.sql(query).logicalPlan, logicalRelation)
  }

  /**
   * Below method will be used to update the logical plan of expression
   * with parent table logical relation
   * @param logicalPlan logial plan
   * @param logicalRelation maintable logical relation
   * @return updated plan
   */
  def updateLogicalRelation(logicalPlan: LogicalPlan,
      logicalRelation: LogicalRelation): LogicalPlan = {
    logicalPlan transform {
      case l: LogicalRelation =>
        l.copy(relation = logicalRelation.relation)
    }
  }

  /**
   * Normalize the exprIds in the given expression, by updating the exprId in `AttributeReference`
   * with its referenced ordinal from input attributes. It's similar to `BindReferences` but we
   * do not use `BindReferences` here as the plan may take the expression as a parameter with type
   * `Attribute`, and replace it with `BoundReference` will cause error.
   */
  def normalizeExprId[T <: Expression](e: T, input: AttributeSeq): T = {
    e.transformUp {
      case ar: AttributeReference =>
        val ordinal = input.indexOf(ar.exprId)
        if (ordinal == -1) {
          ar
        } else {
          ar.withExprId(ExprId(ordinal))
        }
    }.canonicalized.asInstanceOf[T]
  }

  /**
   * Gives child query from schema
   * @param aggDataMapSchema
   * @return
   */
  def getChildQuery(aggDataMapSchema: AggregationDataMapSchema): String = {
    new String(
      CarbonUtil.decodeStringToBytes(
        aggDataMapSchema.getProperties.get(DataMapProperty.CHILD_SELECT_QUERY).replace("&", "=")),
      CarbonCommonConstants.DEFAULT_CHARSET)
  }

  /**
   * This method will start load process on the data map
   */
  def createLoadCommandForChild(
      columns: java.util.List[ColumnSchema],
      dataMapIdentifier: TableIdentifier,
      dataFrame: DataFrame,
      isOverwrite: Boolean,
      sparkSession: SparkSession,
      options: mutable.Map[String, String],
      timeseriesParentTableName: String = ""): CarbonLoadDataCommand = {
    val headers = columns.asScala.filter { column =>
      !column.getColumnName.equalsIgnoreCase(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE)
    }.sortBy(_.getSchemaOrdinal).map(_.getColumnName).mkString(",")
    val loadCommand = CarbonLoadDataCommand(dataMapIdentifier.database,
      dataMapIdentifier.table,
      null,
      Nil,
      Map("fileheader" -> headers),
      isOverwriteTable = isOverwrite,
      dataFrame = None,
      internalOptions = Map(CarbonCommonConstants.IS_INTERNAL_LOAD_CALL -> "true",
        "timeseriesParent" -> timeseriesParentTableName),
      logicalPlan = Some(dataFrame.queryExecution.logical))
    loadCommand
  }

  def getDataFrame(sparkSession: SparkSession, child: LogicalPlan): DataFrame = {
    Dataset.ofRows(sparkSession, child)
  }

}
