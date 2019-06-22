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
package org.apache.spark.sql.hive

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.carbondata.execution.datasources.CarbonSparkDataSourceUtil
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.strategy.{CarbonLateDecodeStrategy, DDLStrategy, StreamingTableStrategy}
import org.apache.spark.sql.internal.{SQLConf, SessionResourceLoader, SessionState, SessionStateBuilder}
import org.apache.spark.sql.optimizer.{CarbonIUDRule, CarbonLateDecodeRule, CarbonUDFTransformRule}
import org.apache.spark.sql.parser.CarbonSparkSqlParser
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{CarbonEnv, SparkSession}

import org.apache.carbondata.core.metadata.schema.table.column.{ColumnSchema => ColumnSchema}
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.format.TableInfo
import org.apache.carbondata.spark.util.CarbonScalaUtil

/**
 * This class will have carbon catalog and refresh the relation from cache if the carbontable in
 * carbon catalog is not same as cached carbon relation's carbon table
 *
 * @param externalCatalog
 * @param globalTempViewManager
 * @param sparkSession
 * @param functionResourceLoader
 * @param functionRegistry
 * @param conf
 * @param hadoopConf
 */
class InMemorySessionCatalog(
    externalCatalog: ExternalCatalog,
    globalTempViewManager: GlobalTempViewManager,
    functionRegistry: FunctionRegistry,
    sparkSession: SparkSession,
    conf: SQLConf,
    hadoopConf: Configuration,
    parser: ParserInterface,
    functionResourceLoader: FunctionResourceLoader)
  extends SessionCatalog(
    externalCatalog,
    globalTempViewManager,
    functionRegistry,
    conf,
    hadoopConf,
    parser,
    functionResourceLoader
  ) with CarbonSessionCatalog {

  override def alterTableRename(oldTableIdentifier: TableIdentifier,
      newTableIdentifier: TableIdentifier,
      newTablePath: String): Unit = {
    sparkSession.sessionState.catalog.renameTable(oldTableIdentifier, newTableIdentifier)
  }

  override def alterTable(tableIdentifier: TableIdentifier,
      schemaParts: String,
      cols: Option[Seq[ColumnSchema]]): Unit = {
    // NOt Required in case of In-memory catalog
  }

  override def alterAddColumns(tableIdentifier: TableIdentifier,
      schemaParts: String,
      newColumns: Option[Seq[ColumnSchema]]): Unit = {
    val catalogTable = sparkSession.sessionState.catalog.getTableMetadata(tableIdentifier)
    val structType = catalogTable.schema
    var newStructType = structType
    newColumns.get.foreach {cols =>
      newStructType = structType
        .add(cols.getColumnName,
          CarbonSparkDataSourceUtil.convertCarbonToSparkDataType(cols.getDataType))
    }
    alterSchema(newStructType, catalogTable, tableIdentifier)
  }

  override def alterDropColumns(tableIdentifier: TableIdentifier,
      schemaParts: String,
      dropCols: Option[Seq[ColumnSchema]]): Unit = {
    val catalogTable = sparkSession.sessionState.catalog.getTableMetadata(tableIdentifier)
    val fields = catalogTable.schema.fields.filterNot { field =>
      dropCols.get.exists { col =>
        col.getColumnName.equalsIgnoreCase(field.name)
      }
    }
    alterSchema(new StructType(fields), catalogTable, tableIdentifier)
  }

  override def alterColumnChangeDataTypeOrRename(tableIdentifier: TableIdentifier,
      schemaParts: String,
      columns: Option[Seq[ColumnSchema]]): Unit = {
    val catalogTable = sparkSession.sessionState.catalog.getTableMetadata(tableIdentifier)
    val a = catalogTable.schema.fields.flatMap { field =>
      columns.get.map { col =>
        if (col.getColumnName.equalsIgnoreCase(field.name)) {
          StructField(col.getColumnName,
            CarbonSparkDataSourceUtil.convertCarbonToSparkDataType(col.getDataType))
        } else {
          field
        }
      }
    }
    alterSchema(new StructType(a), catalogTable, tableIdentifier)
  }

  private def alterSchema(structType: StructType,
      catalogTable: CatalogTable,
      tableIdentifier: TableIdentifier): Unit = {
    val copy = catalogTable.copy(schema = structType)
    sparkSession.sessionState.catalog.alterTable(copy)
    sparkSession.sessionState.catalog.refreshTable(tableIdentifier)
  }

  lazy val carbonEnv = {
    val env = new CarbonEnv
    env.init(sparkSession)
    env
  }

  def getCarbonEnv() : CarbonEnv = {
    carbonEnv
  }

  // Initialize all listeners to the Operation bus.
  CarbonEnv.init

  def getThriftTableInfo(tablePath: String): TableInfo = {
    val tableMetadataFile = CarbonTablePath.getSchemaFilePath(tablePath)
    CarbonUtil.readSchemaFile(tableMetadataFile)
  }

  override def lookupRelation(name: TableIdentifier): LogicalPlan = {
    val rtnRelation = super.lookupRelation(name)
    val isRelationRefreshed =
      CarbonSessionUtil.refreshRelation(rtnRelation, name)(sparkSession)
    if (isRelationRefreshed) {
      super.lookupRelation(name)
    } else {
      rtnRelation
    }
  }

  /**
   * returns hive client from HiveExternalCatalog
   *
   * @return
   */
  def getClient(): org.apache.spark.sql.hive.client.HiveClient = {
    null
  }

  override def createPartitions(
      tableName: TableIdentifier,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    try {
      val table = CarbonEnv.getCarbonTable(tableName)(sparkSession)
      val updatedParts = CarbonScalaUtil.updatePartitions(parts, table)
      super.createPartitions(tableName, updatedParts, ignoreIfExists)
    } catch {
      case e: Exception =>
        super.createPartitions(tableName, parts, ignoreIfExists)
    }
  }

  /**
   * This is alternate way of getting partition information. It first fetches all partitions from
   * hive and then apply filter instead of querying hive along with filters.
   * @param partitionFilters
   * @param sparkSession
   * @param identifier
   * @return
   */
  override def getPartitionsAlternate(partitionFilters: Seq[Expression],
      sparkSession: SparkSession,
      identifier: TableIdentifier) = {
    CarbonSessionUtil.prunePartitionsByFilter(partitionFilters, sparkSession, identifier)
  }

  /**
   * Update the storageformat with new location information
   */
  override def updateStorageLocation(
      path: Path,
      storage: CatalogStorageFormat,
      newTableName: String,
      dbName: String): CatalogStorageFormat = {
    storage.copy(locationUri = Some(path.toUri))
  }
}

class CarbonInMemorySessionStateBuilder (sparkSession: SparkSession,
    parentState: Option[SessionState] = None)
  extends SessionStateBuilder(sparkSession, parentState) {

  override lazy val sqlParser: ParserInterface = new CarbonSparkSqlParser(conf, sparkSession)

  experimentalMethods.extraStrategies =
    Seq(new StreamingTableStrategy(sparkSession),
      new CarbonLateDecodeStrategy,
      new DDLStrategy(sparkSession)
    )
  experimentalMethods.extraOptimizations = Seq(new CarbonIUDRule,
    new CarbonUDFTransformRule,
    new CarbonLateDecodeRule)

  /**
   * Internal catalog for managing table and database states.
   */
  override protected lazy val catalog: InMemorySessionCatalog = {
    val catalog = new InMemorySessionCatalog(
      externalCatalog,
      session.sharedState.globalTempViewManager,
      functionRegistry,
      sparkSession,
      conf,
      SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
      sqlParser,
      resourceLoader)
    parentState.foreach(_.catalog.copyStateTo(catalog))
    catalog
  }

  private def externalCatalog: ExternalCatalog =
    session.sharedState.externalCatalog.asInstanceOf[ExternalCatalog]

  override protected lazy val resourceLoader: SessionResourceLoader = {
    new SessionResourceLoader(session)
  }

  override lazy val optimizer: Optimizer = new CarbonOptimizer(catalog, conf, experimentalMethods)

  override protected def analyzer: Analyzer = {
    new CarbonAnalyzer(catalog,
      conf,
      sparkSession,
      getAnalyzer(super.analyzer))
  }

  /**
   * This method adds carbon rules to Hive Analyzer and returns new analyzer
   *
   * @param analyzer SessionStateBuilder analyzer
   * @return
   */
  def getAnalyzer(analyzer: Analyzer): Analyzer = {
    new Analyzer(catalog, conf) {

      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
        analyzer.extendedResolutionRules ++
        Seq(CarbonIUDAnalysisRule(sparkSession)) ++
        Seq(CarbonPreInsertionCasts(sparkSession)) ++ customResolutionRules

      override val extendedCheckRules: Seq[LogicalPlan => Unit] =
        analyzer.extendedCheckRules

      override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
        analyzer.postHocResolutionRules
    }
  }

  override protected def newBuilder: NewBuilder = new CarbonInMemorySessionStateBuilder(_, _)
}

