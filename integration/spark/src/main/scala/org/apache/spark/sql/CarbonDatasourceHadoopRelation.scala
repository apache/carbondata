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

package org.apache.spark.sql

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.CarbonInputMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.{And => LogicalAnd, AttributeReference, Expression => LogicalExpression, GetStructField, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.management.CarbonInsertIntoCommand
import org.apache.spark.sql.execution.strategy.PushDownHelper
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.index.CarbonIndexUtil
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.secondaryindex.optimizer.{CarbonCostBasedOptimizer, CarbonSecondaryIndexOptimizer, SIBinaryFilterPushDownOperation, SIFilterPushDownOperation, SIUnaryFilterPushDownOperation}
import org.apache.spark.sql.sources.{And, BaseRelation, Filter, InsertableRelation, Or}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.index.IndexFilter
import org.apache.carbondata.core.index.dev.secondaryindex.SIExpressionTree.CarbonSIExpression
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.index.IndexType
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.scan.expression.logical.AndExpression
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.hadoop.CarbonProjection
import org.apache.carbondata.spark.rdd.CarbonScanRDD

case class CarbonDatasourceHadoopRelation(
    sparkSession: SparkSession,
    paths: Array[String],
    parameters: Map[String, String],
    tableSchema: Option[StructType],
    limit: Int = -1)
  extends BaseRelation with InsertableRelation {

  val caseInsensitiveMap: Map[String, String] = parameters.map(f => (f._1.toLowerCase, f._2))
  lazy val identifier: AbsoluteTableIdentifier = AbsoluteTableIdentifier.from(
    FileFactory.getUpdatedFilePath(paths.head),
    CarbonEnv.getDatabaseName(caseInsensitiveMap.get("dbname"))(sparkSession),
    caseInsensitiveMap("tablename"))
  CarbonThreadUtil.updateSessionInfoToCurrentThread(sparkSession)

  @transient lazy val carbonRelation: CarbonRelation =
    CarbonEnv.getInstance(sparkSession).carbonMetaStore.
    createCarbonRelation(parameters, identifier, sparkSession)


  @transient lazy val carbonTable: CarbonTable = carbonRelation.carbonTable

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = tableSchema.getOrElse(carbonRelation.schema)

  def buildScan(requiredColumns: Array[String],
      filterComplex: Seq[LogicalExpression],
      projects: Seq[NamedExpression],
      filters: Array[Filter],
      partitions: Seq[PartitionSpec]): RDD[InternalRow] = {
    val reorderedFilter = CarbonFilters.reorderFilter(filters, carbonTable)
    val filterExpression: Option[Expression] = reorderedFilter.flatMap { filter =>
      CarbonFilters.createCarbonFilter(schema, filter,
        carbonTable.getTableInfo.getFactTable.getTableProperties.asScala)
    }.reduceOption(new AndExpression(_, _))

    val projection = new CarbonProjection

    // As Filter push down for Complex datatype is not supported, if filter is applied on complex
    // column, then Projection push down on Complex Columns will not take effect. Hence, check if
    // filter contains Struct Complex Column.
    val complexFilterExists = filterComplex.map(col =>
      col.map(_.isInstanceOf[GetStructField]))

    if (!complexFilterExists.exists(f => f.contains(true))) {
      PushDownHelper.pushDownProjection(requiredColumns, projects, projection)
    } else {
      requiredColumns.foreach(projection.addColumn)
    }
    val indexTablesScanTree = if (!CarbonProperties.getInstance()
      .isEnabledSIRewritePlan(carbonTable.getDatabaseName(), carbonTable.getTableName()) &&
                                  filterComplex.nonEmpty &&
                                  carbonTable.getIndexTableNames(IndexType.SI.getIndexProviderName)
                                    .size() > 0) {
      buildIndexTableScanTree(filterComplex, carbonTable)
    } else {
      null
    }

    val inputMetricsStats: CarbonInputMetrics = new CarbonInputMetrics
    val filter = filterExpression.map(new IndexFilter(carbonTable, _, true)).orNull
    if (filter != null && filters.length == 1) {
      // push down the limit if only one filter
      filter.setLimit(limit)
    }
    new CarbonScanRDD(
      sparkSession,
      projection,
      filter,
      identifier,
      carbonTable.getTableInfo.serialize(),
      carbonTable.getTableInfo,
      inputMetricsStats,
      partitions,
      indexTablesScanTree = indexTablesScanTree)
  }

  private def buildIndexTableScanTree(filters: Seq[LogicalExpression],
      carbonTable: CarbonTable): CarbonSIExpression = {
    var filterAttributes: Set[String] = Set.empty
    var matchingIndexTables: Seq[String] = Seq.empty
    val siOptimizer = new CarbonSecondaryIndexOptimizer(sparkSession)

    // Removed is Not Null filter from all filters and other attributes are selected
    // isNotNull filter will return all the unique values except null from table,
    // For High Cardinality columns, this filter is of no use, hence skipping it.
    filters foreach (siOptimizer.removeIsNotNullAttribute(_, false) collect {
      case attr: AttributeReference =>
        filterAttributes = filterAttributes. + (attr.name.toLowerCase)
    })

    matchingIndexTables = CarbonCostBasedOptimizer
      .identifyRequiredTables(filterAttributes.asJava,
        CarbonIndexUtil.getSecondaryIndexesMap(carbonTable).mapValues(_.toList.asJava).asJava)
      .asScala

    // filter out all the index tables which are disabled
    val enabledMatchingIndexTables = matchingIndexTables.filter(table => sparkSession.sessionState
      .catalog
      .getTableMetadata(TableIdentifier(table, Some(carbonTable.getDatabaseName)))
      .storage
      .properties
      .getOrElse("isSITableEnabled", "true")
      .equalsIgnoreCase("true"))

    // map for index table name to its column name mapping
    val indexTableToColumnsMapping: mutable.Map[String, Set[String]] =
      new mutable.HashMap[String, Set[String]]()
    // map for index table to logical relation mapping
    val indexTableToLogicalRelationMapping: mutable.Map[String, LogicalPlan] =
      new mutable.HashMap[String, LogicalPlan]()
    // mapping of all the index tables and its columns created on the main table
    val allIndexTableToColumnMapping = CarbonIndexUtil.getSecondaryIndexesMap(carbonTable)

    enabledMatchingIndexTables.foreach { matchedTable =>
      // create index table to index column mapping
      val indexTableColumns = allIndexTableToColumnMapping.getOrElse(matchedTable, Array())
      indexTableToColumnsMapping.put(matchedTable, indexTableColumns.toSet)

      // create index table to logical plan mapping
      val indexTableLogicalPlan = siOptimizer.retrievePlan(sparkSession.sessionState
        .catalog
        .lookupRelation(TableIdentifier(matchedTable, Some(carbonTable.getDatabaseName))))(
        sparkSession)
      indexTableToLogicalRelationMapping.put(matchedTable, indexTableLogicalPlan)
    }

    val filterTree: SIFilterPushDownOperation = null
    val (newSIFilterTree, _, _) = siOptimizer.createIndexTableFilterCondition(filterTree,
      filters.reduceOption(LogicalAnd).get,
      indexTableToColumnsMapping,
      indexTableToLogicalRelationMapping,
      false)
    newSIFilterTree match {
      case SIUnaryFilterPushDownOperation(_, _, carbonSIExpression) => carbonSIExpression
      case SIBinaryFilterPushDownOperation(_, _, _, carbonSIExpression) => carbonSIExpression
      case _ => null
    }
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = new Array[Filter](0)

  override def toString: String = {
    "CarbonDatasourceHadoopRelation"
  }

  override def sizeInBytes: Long = carbonRelation.sizeInBytes

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (carbonRelation.output.size > CarbonCommonConstants.DEFAULT_MAX_NUMBER_OF_COLUMNS) {
      CarbonException.analysisException("Maximum supported column by carbon is: " +
        CarbonCommonConstants.DEFAULT_MAX_NUMBER_OF_COLUMNS)
    }
    if (data.logicalPlan.output.size >= carbonRelation.output.size) {
      CarbonInsertIntoCommand(
        databaseNameOp = Some(this.carbonRelation.databaseName),
        tableName = this.carbonRelation.tableName,
        options = scala.collection.immutable
          .Map("fileheader" -> this.tableSchema.get.fields.map(_.name).mkString(",")),
        isOverwriteTable = overwrite,
        logicalPlan = data.logicalPlan,
        tableInfo = this.carbonRelation.carbonTable.getTableInfo).run(sparkSession)
    } else {
      CarbonException.analysisException(
        "Cannot insert into target table because number of columns mismatch")
    }
  }

}
