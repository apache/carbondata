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

import java.lang.reflect.Constructor

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, CarbonEnv, ExperimentalMethods, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{FunctionResourceLoader, GlobalTempViewManager, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{PredicateSubquery, ScalarSubquery}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.ParserUtils._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.CreateTableContext
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SparkOptimizer, SparkSqlAstBuilder}
import org.apache.spark.sql.execution.command.datamap.{DataMapDropTablePostListener, DropDataMapPostListener}
import org.apache.spark.sql.execution.command.preaaggregate._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.strategy.{CarbonLateDecodeStrategy, DDLStrategy, StreamingTableStrategy}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.optimizer.CarbonLateDecodeRule
import org.apache.spark.sql.parser.{CarbonHelperSqlAstBuilder, CarbonSpark2SqlParser, CarbonSparkSqlParser}
import org.apache.spark.util.Utils

import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.util.CarbonProperties

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
      checkSchemasModifiedTimeAndReloadTables()

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
}

/**
 * Session state implementation to override sql parser and adding strategies
 * @param sparkSession
 */
class CarbonSessionState(sparkSession: SparkSession) extends HiveSessionState(sparkSession) {

  override lazy val sqlParser: ParserInterface = new CarbonSparkSqlParser(conf, sparkSession)

  experimentalMethods.extraStrategies =
    Seq(
      new StreamingTableStrategy(sparkSession),
      new CarbonLateDecodeStrategy,
      new DDLStrategy(sparkSession)
    )
  experimentalMethods.extraOptimizations = Seq(new CarbonLateDecodeRule)

  override lazy val optimizer: Optimizer = new CarbonOptimizer(catalog, conf, experimentalMethods)

  def extendedAnalyzerRules: Seq[Rule[LogicalPlan]] = Nil
  def internalAnalyzerRules: Seq[Rule[LogicalPlan]] = {
    catalog.ParquetConversions ::
    catalog.OrcConversions ::
    CarbonPreInsertionCasts(sparkSession) ::
    CarbonPreAggregateQueryRules(sparkSession) ::
    CarbonPreAggregateDataLoadingRules ::
    CarbonIUDAnalysisRule(sparkSession) ::
    AnalyzeCreateTable(sparkSession) ::
    PreprocessTableInsertion(conf) ::
    DataSourceAnalysis(conf) ::
    (if (conf.runSQLonFile) {
      new ResolveDataSource(sparkSession) :: Nil
    } else {  Nil }
      )
  }

  override lazy val analyzer: Analyzer = {
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
  }

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

class CarbonSqlAstBuilder(conf: SQLConf, parser: CarbonSpark2SqlParser) extends
  SparkSqlAstBuilder(conf) {

  val helper = new CarbonHelperSqlAstBuilder(conf, parser)

  override def visitCreateTable(ctx: CreateTableContext): LogicalPlan = {
    val fileStorage = helper.getFileStorage(ctx.createFileFormat)

    if (fileStorage.equalsIgnoreCase("'carbondata'") ||
        fileStorage.equalsIgnoreCase("'org.apache.carbondata.format'")) {
      helper.createCarbonTable(ctx.createTableHeader,
          ctx.skewSpec,
          ctx.bucketSpec,
          ctx.partitionColumns,
          ctx.columns,
          ctx.tablePropertyList,
          ctx.locationSpec,
          Option(ctx.STRING()).map(string),
          ctx.AS)
    } else {
      super.visitCreateTable(ctx)
    }
  }
}
