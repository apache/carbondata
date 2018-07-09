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
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTablePartition, FunctionResourceLoader, GlobalTempViewManager, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BoundReference, Expression, InterpretedPredicate, PredicateSubquery, ScalarSubquery}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.{ParserInterface, SqlBaseParser}
import org.apache.spark.sql.catalyst.parser.ParserUtils.{string, _}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.{CreateTableContext, ShowTablesContext}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.{CatalystConf, TableIdentifier}
import org.apache.spark.sql.execution.command.table.{CarbonExplainCommand, CarbonShowTablesCommand}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.strategy.{CarbonLateDecodeStrategy, DDLStrategy, StreamingTableStrategy}
import org.apache.spark.sql.execution.{SparkOptimizer, SparkSqlAstBuilder}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.optimizer.{CarbonIUDRule, CarbonLateDecodeRule, CarbonUDFTransformRule}
import org.apache.spark.sql.parser.{CarbonHelperSqlAstBuilder, CarbonSpark2SqlParser, CarbonSparkSqlParser}
import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, CarbonEnv, ExperimentalMethods, SparkSession, Strategy}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.util.CarbonProperties
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
    sparkSession: SparkSession,
    functionResourceLoader: FunctionResourceLoader,
    functionRegistry: FunctionRegistry,
    conf: SQLConf,
    hadoopConf: Configuration)
  extends HiveSessionCatalog(
    externalCatalog,
    globalTempViewManager,
    sparkSession,
    functionResourceLoader,
    functionRegistry,
    conf,
    hadoopConf) with CarbonSessionCatalog {

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

  def alterTableRename(oldTableIdentifier: TableIdentifier,
      newTableIdentifier: TableIdentifier,
      newTablePath: String): Unit = {
    getClient().runSqlHive(
      s"ALTER TABLE ${ oldTableIdentifier.database.get }.${ oldTableIdentifier.table }" +
      s" RENAME TO ${ oldTableIdentifier.database.get }.${ newTableIdentifier.table }")
    getClient().runSqlHive(
      s"ALTER TABLE ${ oldTableIdentifier.database.get }.${ newTableIdentifier.table }" +
      s" SET SERDEPROPERTIES" +
      s"('tableName'='${ newTableIdentifier.table }', " +
      s"'dbName'='${ oldTableIdentifier.database.get }', 'tablePath'='${ newTablePath }')")
  }

  def alterTable(tableIdentifier: TableIdentifier,
      schemaParts: String,
      cols: Option[Seq[org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema]])
  : Unit = {
    getClient()
      .runSqlHive(s"ALTER TABLE ${tableIdentifier.database.get}.${tableIdentifier.table } " +
                  s"SET TBLPROPERTIES(${ schemaParts })")
  }

  def alterAddColumns(tableIdentifier: TableIdentifier,
      schemaParts: String,
      cols: Option[Seq[org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema]])
  : Unit = {
    alterTable(tableIdentifier, schemaParts, cols)
  }

  def alterDropColumns(tableIdentifier: TableIdentifier,
      schemaParts: String,
      cols: Option[Seq[org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema]])
  : Unit = {
    alterTable(tableIdentifier, schemaParts, cols)
  }

  def alterColumnChangeDataType(tableIdentifier: TableIdentifier,
      schemaParts: String,
      cols: Option[Seq[org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema]])
  : Unit = {
    alterTable(tableIdentifier, schemaParts, cols)
  }

  // Initialize all listeners to the Operation bus.
  CarbonEnv.init(sparkSession)

  /**
   * This method will invalidate carbonrelation from cache if carbon table is updated in
   * carbon catalog
   *
   * @param name
   * @param alias
   * @return
   */
  override def lookupRelation(name: TableIdentifier,
      alias: Option[String]): LogicalPlan = {
    val rtnRelation = super.lookupRelation(name, alias)
    var toRefreshRelation = false
    rtnRelation match {
      case SubqueryAlias(_,
      LogicalRelation(carbonDatasourceHadoopRelation: CarbonDatasourceHadoopRelation, _, _), _) =>
        toRefreshRelation = refreshRelationFromCache(name, alias, carbonDatasourceHadoopRelation)
      case LogicalRelation(carbonDatasourceHadoopRelation: CarbonDatasourceHadoopRelation, _, _) =>
        toRefreshRelation = refreshRelationFromCache(name, alias, carbonDatasourceHadoopRelation)
      case _ =>
    }

    if (toRefreshRelation) {
      super.lookupRelation(name, alias)
    } else {
      rtnRelation
    }
  }

  private def refreshRelationFromCache(identifier: TableIdentifier,
      alias: Option[String],
      carbonDatasourceHadoopRelation: CarbonDatasourceHadoopRelation): Boolean = {
    var isRefreshed = false
    val storePath = CarbonProperties.getStorePath
    carbonEnv.carbonMetastore.
      checkSchemasModifiedTimeAndReloadTable(identifier)

    val table = carbonEnv.carbonMetastore.getTableFromMetadataCache(
      carbonDatasourceHadoopRelation.carbonTable.getDatabaseName,
      carbonDatasourceHadoopRelation.carbonTable.getTableName)
    if (table.isEmpty || (table.isDefined &&
        table.get.getTableLastUpdatedTime !=
          carbonDatasourceHadoopRelation.carbonTable.getTableLastUpdatedTime)) {
      refreshTable(identifier)
      DataMapStoreManager.getInstance().
        clearDataMaps(AbsoluteTableIdentifier.from(storePath,
          identifier.database.getOrElse("default"), identifier.table))
      isRefreshed = true
      logInfo(s"Schema changes have been detected for table: $identifier")
    }
    isRefreshed
  }

  /**
   * returns hive client from session state
   *
   * @return
   */
  override def getClient(): org.apache.spark.sql.hive.client.HiveClient = {
    sparkSession.sessionState.asInstanceOf[CarbonSessionState].metadataHive
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
  def getPartitionsAlternate(
      partitionFilters: Seq[Expression],
      sparkSession: SparkSession,
      identifier: TableIdentifier) = {
    val allPartitions = sparkSession.sessionState.catalog.listPartitions(identifier)
    val catalogTable = sparkSession.sessionState.catalog.getTableMetadata(identifier)
    val partitionSchema = catalogTable.partitionSchema
    if (partitionFilters.nonEmpty) {
      val boundPredicate =
        InterpretedPredicate.create(partitionFilters.reduce(And).transform {
          case att: AttributeReference =>
            val index = partitionSchema.indexWhere(_.name == att.name)
            BoundReference(index, partitionSchema(index).dataType, nullable = true)
        })
      allPartitions.filter { p => boundPredicate(p.toRow(partitionSchema)) }
    } else {
      allPartitions
    }
  }

  /**
   * Update the storageformat with new location information
   */
  override def updateStorageLocation(
      path: Path,
      storage: CatalogStorageFormat,
      newTableName: String,
      dbName: String): CatalogStorageFormat = {
    storage.copy(locationUri = Some(path.toString))
  }
}

/**
 * Session state implementation to override sql parser and adding strategies
 * @param sparkSession
 */
class CarbonSessionState(sparkSession: SparkSession) extends HiveSessionState(sparkSession) {

  override lazy val sqlParser: ParserInterface = new CarbonSparkSqlParser(conf, sparkSession)

  experimentalMethods.extraStrategies = extraStrategies

  experimentalMethods.extraOptimizations = extraOptimizations

  def extraStrategies: Seq[Strategy] = {
    Seq(
      new StreamingTableStrategy(sparkSession),
      new CarbonLateDecodeStrategy,
      new DDLStrategy(sparkSession)
    )
  }

  def extraOptimizations: Seq[Rule[LogicalPlan]] = {
    Seq(new CarbonIUDRule,
      new CarbonUDFTransformRule,
      new CarbonLateDecodeRule)
  }

  override lazy val optimizer: Optimizer = new CarbonOptimizer(catalog, conf, experimentalMethods)

  def extendedAnalyzerRules: Seq[Rule[LogicalPlan]] = Nil
  def internalAnalyzerRules: Seq[Rule[LogicalPlan]] = {
    catalog.ParquetConversions ::
    catalog.OrcConversions ::
    CarbonPreInsertionCasts(sparkSession) ::
    CarbonIUDAnalysisRule(sparkSession) ::
    AnalyzeCreateTable(sparkSession) ::
    PreprocessTableInsertion(conf) ::
    DataSourceAnalysis(conf) ::
    (if (conf.runSQLonFile) {
      new ResolveDataSource(sparkSession) :: Nil
    } else {  Nil })
  }

  override lazy val analyzer: Analyzer =
    new CarbonAnalyzer(catalog, conf, sparkSession,
      new Analyzer(catalog, conf) {
        override val extendedResolutionRules =
          if (extendedAnalyzerRules.nonEmpty) {
            extendedAnalyzerRules ++ internalAnalyzerRules
          } else {
            internalAnalyzerRules
          }
        override val extendedCheckRules = Seq(
          PreWriteCheck(conf, catalog))
      }
  )

  /**
   * Internal catalog for managing table and database states.
   */
  override lazy val catalog = {
    new CarbonHiveSessionCatalog(
      sparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog],
      sparkSession.sharedState.globalTempViewManager,
      sparkSession,
      functionResourceLoader,
      functionRegistry,
      conf,
      newHadoopConf())
  }
}

class CarbonAnalyzer(catalog: SessionCatalog,
    conf: CatalystConf,
    sparkSession: SparkSession,
    analyzer: Analyzer) extends Analyzer(catalog, conf) {
  override def execute(plan: LogicalPlan): LogicalPlan = {
    var logicalPlan = analyzer.execute(plan)
    logicalPlan = CarbonPreAggregateDataLoadingRules(sparkSession).apply(logicalPlan)
    CarbonPreAggregateQueryRules(sparkSession).apply(logicalPlan)
  }
}

class CarbonOptimizer(
    catalog: SessionCatalog,
    conf: SQLConf,
    experimentalMethods: ExperimentalMethods)
  extends SparkOptimizer(catalog, conf, experimentalMethods) {

  override def execute(plan: LogicalPlan): LogicalPlan = {
    val transFormedPlan: LogicalPlan = CarbonOptimizerUtil.transformForScalarSubQuery(plan)
    super.execute(transFormedPlan)
  }
}

object CarbonOptimizerUtil {
  def transformForScalarSubQuery(plan: LogicalPlan) : LogicalPlan = {
    // In case scalar subquery add flag in relation to skip the decoder plan in optimizer rule,
    // And optimize whole plan at once.
    val transFormedPlan = plan.transform {
      case filter: Filter =>
        filter.transformExpressions {
          case s: ScalarSubquery =>
            val tPlan = s.plan.transform {
              case lr: LogicalRelation
                if lr.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
                lr.relation.asInstanceOf[CarbonDatasourceHadoopRelation].isSubquery += true
                lr
            }
            ScalarSubquery(tPlan, s.children, s.exprId)
          case p: PredicateSubquery =>
            val tPlan = p.plan.transform {
              case lr: LogicalRelation
                if lr.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
                lr.relation.asInstanceOf[CarbonDatasourceHadoopRelation].isSubquery += true
                lr
            }
            PredicateSubquery(tPlan, p.children, p.nullAware, p.exprId)
        }
    }
    transFormedPlan
  }
}

class CarbonSqlAstBuilder(conf: SQLConf, parser: CarbonSpark2SqlParser, sparkSession: SparkSession)
  extends SparkSqlAstBuilder(conf) {

  val helper = new CarbonHelperSqlAstBuilder(conf, parser, sparkSession)

  override def visitCreateTable(ctx: CreateTableContext): LogicalPlan = {
    val fileStorage = helper.getFileStorage(ctx.createFileFormat)

    if (fileStorage.equalsIgnoreCase("'carbondata'") ||
        fileStorage.equalsIgnoreCase("carbondata") ||
        fileStorage.equalsIgnoreCase("'carbonfile'") ||
        fileStorage.equalsIgnoreCase("'org.apache.carbondata.format'")) {
      val createTableTuple = (ctx.createTableHeader, ctx.skewSpec, ctx.bucketSpec,
        ctx.partitionColumns, ctx.columns, ctx.tablePropertyList, ctx.locationSpec(),
        Option(ctx.STRING()).map(string),
        ctx.AS, ctx.query, fileStorage)
        helper.createCarbonTable(createTableTuple)
    } else {
      super.visitCreateTable(ctx)
    }
  }

  override def visitShowTables(ctx: ShowTablesContext): LogicalPlan = {
    withOrigin(ctx) {
      if (CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_SHOW_DATAMAPS,
          CarbonCommonConstants.CARBON_SHOW_DATAMAPS_DEFAULT).toBoolean) {
        super.visitShowTables(ctx)
      } else {
        CarbonShowTablesCommand(
          Option(ctx.db).map(_.getText),
          Option(ctx.pattern).map(string))
      }
    }
  }

  override def visitExplain(ctx: SqlBaseParser.ExplainContext): LogicalPlan = {
    CarbonExplainCommand(super.visitExplain(ctx))
  }
}
