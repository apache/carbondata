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
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{FindDataSourceTable, PreWriteCheck, ResolveSQLOnFile, _}
import org.apache.spark.sql.execution.strategy.{CarbonLateDecodeStrategy, DDLStrategy, StreamingTableStrategy}
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.{SQLConf, SessionState}
import org.apache.spark.sql.optimizer.{CarbonIUDRule, CarbonLateDecodeRule, CarbonUDFTransformRule}
import org.apache.spark.sql.parser.CarbonSparkSqlParser

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
class CarbonHiveSessionCatalog(
    externalCatalog: HiveExternalCatalog,
    globalTempViewManager: GlobalTempViewManager,
    functionRegistry: FunctionRegistry,
    sparkSession: SparkSession,
    conf: SQLConf,
    hadoopConf: Configuration,
    parser: ParserInterface,
    functionResourceLoader: FunctionResourceLoader)
  extends HiveSessionCatalog (
    externalCatalog,
    globalTempViewManager,
    new HiveMetastoreCatalog(sparkSession),
    functionRegistry,
    conf,
    hadoopConf,
    parser,
    functionResourceLoader
  ) with CarbonSessionCatalog {

  private lazy val carbonEnv = {
    val env = new CarbonEnv
    env.init(sparkSession)
    env
  }
  /**
   * return's the carbonEnv instance
   * @return
   */
  override def getCarbonEnv() : CarbonEnv = {
    carbonEnv
  }

  // Initialize all listeners to the Operation bus.
  CarbonEnv.initListeners()

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
  override def getClient(): org.apache.spark.sql.hive.client.HiveClient = {
    sparkSession.asInstanceOf[CarbonSession].sharedState.externalCatalog
      .asInstanceOf[HiveExternalCatalog].client
  }

  def alterTableRename(oldTableIdentifier: TableIdentifier,
      newTableIdentifier: TableIdentifier,
      newTablePath: String): Unit = {
    getClient().runSqlHive(
      s"ALTER TABLE ${ oldTableIdentifier.database.get }.${ oldTableIdentifier.table } " +
      s"RENAME TO ${ oldTableIdentifier.database.get }.${ newTableIdentifier.table }")
    getClient().runSqlHive(
      s"ALTER TABLE ${ oldTableIdentifier.database.get }.${ newTableIdentifier.table} " +
      s"SET SERDEPROPERTIES" +
      s"('tableName'='${ newTableIdentifier.table }', " +
      s"'dbName'='${ oldTableIdentifier.database.get }', 'tablePath'='${ newTablePath }')")
  }

  override def alterTable(tableIdentifier: TableIdentifier,
      schemaParts: String,
      cols: Option[Seq[org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema]])
  : Unit = {
    getClient()
      .runSqlHive(s"ALTER TABLE ${tableIdentifier.database.get}.${ tableIdentifier.table } " +
                  s"SET TBLPROPERTIES(${ schemaParts })")
  }

  override def alterAddColumns(tableIdentifier: TableIdentifier,
      schemaParts: String,
      cols: Option[Seq[org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema]])
  : Unit = {
    alterTable(tableIdentifier, schemaParts, cols)
  }

  override def alterDropColumns(tableIdentifier: TableIdentifier,
      schemaParts: String,
      cols: Option[Seq[org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema]])
  : Unit = {
    alterTable(tableIdentifier, schemaParts, cols)
  }

  override def alterColumnChangeDataType(tableIdentifier: TableIdentifier,
      schemaParts: String,
      cols: Option[Seq[org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema]])
  : Unit = {
    alterTable(tableIdentifier, schemaParts, cols)
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

/**
 * Session state implementation to override sql parser and adding strategies
 *
 * @param sparkSession
 */
class CarbonSessionStateBuilder(sparkSession: SparkSession,
    parentState: Option[SessionState] = None)
  extends HiveSessionStateBuilder(sparkSession, parentState) {

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
  /**
   * Create a [[CarbonSessionStateBuilder]].
   */
  override protected lazy val catalog: CarbonHiveSessionCatalog = {
    val catalog = new CarbonHiveSessionCatalog(
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

  private def externalCatalog: HiveExternalCatalog =
    session.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog]

  /**
   * Create a Hive aware resource loader.
   */
  override protected lazy val resourceLoader: HiveSessionResourceLoader = {
    val client: HiveClient = externalCatalog.client.newSession()
    new HiveSessionResourceLoader(session, client)
  }

  override lazy val optimizer: Optimizer = new CarbonOptimizer(catalog, conf, experimentalMethods)

  override protected def analyzer: Analyzer = new CarbonAnalyzer(catalog, conf, sparkSession,
    new Analyzer(catalog, conf) {

      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
        new ResolveHiveSerdeTable(session) +:
        new FindDataSourceTable(session) +:
        new ResolveSQLOnFile(session) +:
        new CarbonIUDAnalysisRule(sparkSession) +:
        new CarbonPreInsertionCasts(sparkSession) +: customResolutionRules

      override val extendedCheckRules: Seq[LogicalPlan => Unit] =
      PreWriteCheck :: HiveOnlyCheck :: Nil

      override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
        new DetermineTableStats(session) +:
        RelationConversions(conf, catalog) +:
        PreprocessTableCreation(session) +:
        PreprocessTableInsertion(conf) +:
        DataSourceAnalysis(conf) +:
        HiveAnalysis +:
        customPostHocResolutionRules
    }
  )

  override protected def newBuilder: NewBuilder = new CarbonSessionStateBuilder(_, _)
}