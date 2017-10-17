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
import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, CarbonEnv, ExperimentalMethods, SparkSession, Strategy}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{FunctionResourceLoader, GlobalTempViewManager, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{PredicateSubquery, ScalarSubquery}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkOptimizer
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.strategy.{CarbonLateDecodeStrategy, DDLStrategy}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.optimizer.CarbonLateDecodeRule
import org.apache.spark.sql.parser.CarbonSparkSqlParser
import org.apache.spark.util.Utils

import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier

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
    hadoopConf: Configuration,
    parser: ParserInterface)
  extends HiveSessionCatalog(
    externalCatalog,
    globalTempViewManager,
    sparkSession,
    functionResourceLoader,
    functionRegistry,
    conf,
    hadoopConf,
    parser) {

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
          LogicalRelation(carbonDatasourceHadoopRelation: CarbonDatasourceHadoopRelation, _, _),
          _) =>
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
    val storePath = CarbonEnv.getInstance(sparkSession).storePath
    carbonEnv.carbonMetastore.
      checkSchemasModifiedTimeAndReloadTables(storePath)

    val tableMeta = carbonEnv.carbonMetastore
      .getTableFromMetadataCache(carbonDatasourceHadoopRelation.carbonTable.getDatabaseName,
        carbonDatasourceHadoopRelation.carbonTable.getFactTableName)
    if (tableMeta.isEmpty || (tableMeta.isDefined &&
        tableMeta.get.carbonTable.getTableLastUpdatedTime !=
          carbonDatasourceHadoopRelation.carbonTable.getTableLastUpdatedTime)) {
      refreshTable(identifier)
      DataMapStoreManager.getInstance().
        clearDataMap(AbsoluteTableIdentifier.from(storePath,
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

  experimentalMethods.extraStrategies = extraStrategies
  experimentalMethods.extraOptimizations = extraOptimizations

  def extraStrategies: Seq[Strategy] = {
    Seq(new CarbonLateDecodeStrategy,
      new DDLStrategy(sparkSession))
  }

  override lazy val optimizer: Optimizer = new CarbonOptimizer(catalog, conf, experimentalMethods)

  override lazy val analyzer: Analyzer = {
    new Analyzer(catalog, conf) {
      override val extendedResolutionRules =
        catalog.ParquetConversions ::
        catalog.OrcConversions ::
        CarbonPreInsertionCasts ::
        CarbonIUDAnalysisRule(sparkSession) ::
        AnalyzeCreateTable(sparkSession) ::
        PreprocessTableInsertion(conf) ::
        DataSourceAnalysis(conf) ::
        (if (conf.runSQLonFile) {
          new ResolveDataSource(sparkSession) :: Nil
        } else {
          Nil
        })

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
      newHadoopConf(),
      sqlParser)
  }

  def extraOptimizations: Seq[Rule[LogicalPlan]] = {
    Seq(new CarbonLateDecodeRule)
  }
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
    super.execute(transFormedPlan)
  }
}

object SessionStateFactory {

  def getSessionState(sparkSession: SparkSession, className: String): HiveSessionState = {
    className match {
      case "org.apache.spark.sql.si.hive.CarbonInternalSessionState" =>
        // use reflection to create an object for internal session state
        val clazz = Utils.classForName(className)
        val constructors = clazz.getConstructors()(0)
        val sessionStateInstance = constructors.newInstance(sparkSession)
          .asInstanceOf[HiveSessionState]
        sessionStateInstance
      case _ => new CarbonSessionState(sparkSession)
    }
  }
}

