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
import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, CarbonEnv, ExperimentalMethods, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{FunctionResourceLoader, GlobalTempViewManager, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{PredicateSubquery, ScalarSubquery}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.CarbonLateDecodeStrategy
import org.apache.spark.sql.execution.command.DDLStrategy
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.SparkOptimizer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.optimizer.CarbonLateDecodeRule
import org.apache.spark.sql.parser.CarbonSparkSqlParser

import org.apache.carbondata.processing.merger.TableMeta

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
    super.lookupRelation(name, alias) match {
      case SubqueryAlias(_,
          LogicalRelation(carbonDatasourceHadoopRelation: CarbonDatasourceHadoopRelation, _, _),
          _) =>
        refreshRelationFromCache(name, alias, carbonDatasourceHadoopRelation)
      case LogicalRelation(carbonDatasourceHadoopRelation: CarbonDatasourceHadoopRelation, _, _) =>
        refreshRelationFromCache(name, alias, carbonDatasourceHadoopRelation)
      case relation => relation
    }
  }

  private def refreshRelationFromCache(name: TableIdentifier,
      alias: Option[String],
      carbonDatasourceHadoopRelation: CarbonDatasourceHadoopRelation): LogicalPlan = {
    carbonEnv.carbonMetastore.checkSchemasModifiedTimeAndReloadTables
    carbonEnv.carbonMetastore
      .getTableFromMetadata(carbonDatasourceHadoopRelation.carbonTable.getDatabaseName,
        carbonDatasourceHadoopRelation.carbonTable.getFactTableName) match {
      case tableMeta: TableMeta =>
        if (tableMeta.carbonTable.getTableLastUpdatedTime !=
            carbonDatasourceHadoopRelation.carbonTable.getTableLastUpdatedTime) {
          refreshTable(name)
        }
      case _ => refreshTable(name)
    }
    super.lookupRelation(name, alias)
  }
}

/**
 * Session state implementation to override sql parser and adding strategies
 * @param sparkSession
 */
class CarbonSessionState(sparkSession: SparkSession) extends HiveSessionState(sparkSession) {

  override lazy val sqlParser: ParserInterface = new CarbonSparkSqlParser(conf)

  experimentalMethods.extraStrategies =
    Seq(new CarbonLateDecodeStrategy, new DDLStrategy(sparkSession))
  experimentalMethods.extraOptimizations = Seq(new CarbonLateDecodeRule)

  override lazy val optimizer: Optimizer = new CarbonOptimizer(catalog, conf, experimentalMethods)

  override lazy val analyzer: Analyzer = {
    new Analyzer(catalog, conf) {
      override val extendedResolutionRules =
        catalog.ParquetConversions ::
        catalog.OrcConversions ::
        CarbonPreInsertionCasts ::
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
      newHadoopConf())
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
