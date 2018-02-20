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
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{CatalogTablePartition, FunctionResourceLoader, GlobalTempViewManager, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BoundReference, Expression, InterpretedPredicate, PredicateSubquery, ScalarSubquery}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.ParserUtils._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.CreateTableContext
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.{CatalystConf, TableIdentifier}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.strategy.{CarbonLateDecodeStrategy, DDLStrategy, StreamingTableStrategy}
import org.apache.spark.sql.execution.{SparkOptimizer, SparkSqlAstBuilder}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.optimizer.{CarbonIUDRule, CarbonLateDecodeRule, CarbonUDFTransformRule}
import org.apache.spark.sql.parser.{CarbonHelperSqlAstBuilder, CarbonSpark2SqlParser, CarbonSparkSqlParser}
import org.apache.spark.sql._

import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.util.{CarbonProperties, ThreadLocalSessionInfo}
import org.apache.carbondata.datamap.preaggregate.PreaggregateMVDataMapRules
import org.apache.carbondata.datamap.{DataMapManager, MVDataMapRules}
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
class CarbonSessionCatalog(
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
    hadoopConf) {

  lazy val carbonEnv = {
    val env = new CarbonEnv
    env.init(sparkSession)
    env
  }

  // Initialize all listeners to the Operation bus.
  CarbonEnv.initListeners()

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
  def getClient(): org.apache.spark.sql.hive.client.HiveClient = {
    sparkSession.sessionState.asInstanceOf[CarbonSessionState].metadataHive
  }

  override def createPartitions(
      tableName: TableIdentifier,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    try {
      val table = CarbonEnv.getCarbonTable(tableName)(sparkSession)
      // Get the properties from thread local
      val carbonSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
      if (carbonSessionInfo != null) {
        val updatedParts = CarbonScalaUtil.updatePartitions(carbonSessionInfo, parts, table)
        super.createPartitions(tableName, updatedParts, ignoreIfExists)
      } else {
        super.createPartitions(tableName, parts, ignoreIfExists)
      }
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

  override lazy val optimizer: Optimizer = new CarbonOptimizer(
    catalog, conf, sparkSession, experimentalMethods)

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
    new CarbonSessionCatalog(
      sparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog],
      sparkSession.sharedState.globalTempViewManager,
      sparkSession,
      functionResourceLoader,
      functionRegistry,
      conf,
      newHadoopConf())
  }
}

class CarbonAnalyzer(
    catalog: SessionCatalog,
    conf: CatalystConf,
    sparkSession: SparkSession,
    analyzer: Analyzer) extends Analyzer(catalog, conf) {
  override def execute(plan: LogicalPlan): LogicalPlan = {
    var logicalPlan = analyzer.execute(plan)

    val mvDataMapRules = sparkSession.asInstanceOf[CarbonSession].getMVDataMapRules
    mvDataMapRules.foreach { mvDataMap =>
      val rules = mvDataMap.getAnalyzerRules(sparkSession)
      rules.foreach(rule => logicalPlan = rule.apply(logicalPlan))
    }
    logicalPlan
  }
}

class CarbonOptimizer(
    catalog: SessionCatalog,
    conf: SQLConf,
    sparkSession: SparkSession,
    experimentalMethods: ExperimentalMethods)
  extends SparkOptimizer(catalog, conf, experimentalMethods) {

  override def execute(plan: LogicalPlan): LogicalPlan = {
    var transFormedPlan: LogicalPlan = CarbonOptimizerUtil.transformForScalarSubQuery(plan)
    transFormedPlan = super.execute(transFormedPlan)

    val mvDataMapRules = sparkSession.asInstanceOf[CarbonSession].getMVDataMapRules
    mvDataMapRules.foreach { mvDataMap =>
      val rules = mvDataMap.getOptimizerRules(sparkSession)
      rules.foreach(rule => transFormedPlan = rule.apply(transFormedPlan))
    }
    transFormedPlan
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
        fileStorage.equalsIgnoreCase("'org.apache.carbondata.format'")) {
      helper.createCarbonTable(
        tableHeader = ctx.createTableHeader,
        skewSpecContext = ctx.skewSpec,
        bucketSpecContext = ctx.bucketSpec,
        partitionColumns = ctx.partitionColumns,
        columns = ctx.columns,
        tablePropertyList = ctx.tablePropertyList,
        locationSpecContext = ctx.locationSpec(),
        tableComment = Option(ctx.STRING()).map(string),
        ctas = ctx.AS,
        query = ctx.query)
    } else {
      super.visitCreateTable(ctx)
    }
  }
}
