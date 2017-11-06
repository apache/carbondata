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
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.ScalarSubquery
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.ParserUtils.string
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.{CreateHiveTableContext, CreateTableContext}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{FindDataSourceTable, LogicalRelation, PreWriteCheck, ResolveSQLOnFile, _}
import org.apache.spark.sql.execution.strategy.{CarbonLateDecodeStrategy, DDLStrategy, StreamingTableStrategy}
import org.apache.spark.sql.execution.{SparkOptimizer, SparkSqlAstBuilder}
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.{SQLConf, SessionState}
import org.apache.spark.sql.optimizer.CarbonLateDecodeRule
import org.apache.spark.sql.parser.{CarbonHelperSqlAstBuilder, CarbonSpark2SqlParser, CarbonSparkSqlParser}

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
    functionRegistry: FunctionRegistry,
    sparkSession: SparkSession,
    conf: SQLConf,
    hadoopConf: Configuration,
    parser: ParserInterface,
    functionResourceLoader: FunctionResourceLoader)
  extends HiveSessionCatalog(
    externalCatalog,
    globalTempViewManager,
    new HiveMetastoreCatalog(sparkSession),
    functionRegistry,
    conf,
    hadoopConf,
    parser,
    functionResourceLoader
  ) {

  lazy val carbonEnv = {
    val env = new CarbonEnv
    env.init(sparkSession)
    env
  }

  def getCarbonEnv() : CarbonEnv = {
    carbonEnv
  }


  private def refreshRelationFromCache(identifier: TableIdentifier,
      carbonDatasourceHadoopRelation: CarbonDatasourceHadoopRelation): Boolean = {
    var isRefreshed = false
    val storePath = CarbonProperties.getStorePath
    carbonEnv.carbonMetastore.checkSchemasModifiedTimeAndReloadTables()

    val table = carbonEnv.carbonMetastore.getTableFromMetadataCache(
      carbonDatasourceHadoopRelation.carbonTable.getDatabaseName,
      carbonDatasourceHadoopRelation.carbonTable.getTableName)
    if (table.isEmpty || (table.isDefined && table.get.getTableLastUpdatedTime !=
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


  override def lookupRelation(name: TableIdentifier): LogicalPlan = {
    val rtnRelation = super.lookupRelation(name)
    var toRefreshRelation = false
    rtnRelation match {
      case SubqueryAlias(_,
      LogicalRelation(carbonDatasourceHadoopRelation: CarbonDatasourceHadoopRelation, _, _)) =>
        toRefreshRelation = refreshRelationFromCache(name, carbonDatasourceHadoopRelation)
      case LogicalRelation(carbonDatasourceHadoopRelation: CarbonDatasourceHadoopRelation, _, _) =>
        toRefreshRelation = refreshRelationFromCache(name, carbonDatasourceHadoopRelation)
      case _ =>
    }

    if (toRefreshRelation) {
      super.lookupRelation(name)
    } else {
      rtnRelation
    }
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
  experimentalMethods.extraOptimizations = Seq(new CarbonLateDecodeRule)

  /**
   * Internal catalog for managing table and database states.
   */
  /**
   * Create a [[CarbonSessionCatalogBuild]].
   */
  override protected lazy val catalog: CarbonSessionCatalog = {
    val catalog = new CarbonSessionCatalog(
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

  override protected def analyzer: Analyzer = {
    new Analyzer(catalog, conf) {

      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
        new ResolveHiveSerdeTable(session) +:
        new FindDataSourceTable(session) +:
        new ResolveSQLOnFile(session) +:
        new CarbonIUDAnalysisRule(sparkSession) +:
        new CarbonPreAggregateQueryRules(sparkSession) +:
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
  }

  override protected def newBuilder: NewBuilder = new CarbonSessionStateBuilder(_, _)

}


class CarbonOptimizer(
    catalog: SessionCatalog,
    conf: SQLConf,
    experimentalMethods: ExperimentalMethods)
  extends SparkOptimizer(catalog, conf, experimentalMethods) {

  override def execute(plan: LogicalPlan): LogicalPlan = {
    // In case scalar subquery add flag in relation to skip the decoder plan in optimizer rule, And
    // optimize whole plan at once.
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
        }
    }
    super.execute(transFormedPlan)
  }
}

class CarbonSqlAstBuilder(conf: SQLConf, parser: CarbonSpark2SqlParser) extends
  SparkSqlAstBuilder(conf) {

  val helper = new CarbonHelperSqlAstBuilder(conf, parser)

  override def visitCreateHiveTable(ctx: CreateHiveTableContext): LogicalPlan = {
    val fileStorage = helper.getFileStorage(ctx.createFileFormat)

    if (fileStorage.equalsIgnoreCase("'carbondata'") ||
        fileStorage.equalsIgnoreCase("'org.apache.carbondata.format'")) {
      helper.createCarbonTable(ctx.createTableHeader,
          ctx.skewSpec,
          ctx.bucketSpec,
          ctx.partitionColumns,
          ctx.columns,
          ctx.tablePropertyList,
          Option(ctx.STRING()).map(string))
    } else {
      super.visitCreateHiveTable(ctx)
    }
  }

  override def visitCreateTable(ctx: CreateTableContext): LogicalPlan = {
    super.visitCreateTable(ctx)
  }
}
