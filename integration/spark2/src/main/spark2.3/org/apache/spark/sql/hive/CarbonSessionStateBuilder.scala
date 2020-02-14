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

import java.util.concurrent.Callable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTablePartition, FunctionResourceLoader, GlobalTempViewManager}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.execution.strategy.{CarbonLateDecodeStrategy, DDLStrategy, StreamingTableStrategy}
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.{SQLConf, SessionState}
import org.apache.spark.sql.optimizer.{CarbonIUDRule, CarbonUDFTransformRule}
import org.apache.spark.sql.parser.CarbonSparkSqlParser
import org.apache.spark.sql.{CarbonEnv, SparkSession}

import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema

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
  CarbonEnv.init

  override def lookupRelation(name: TableIdentifier): LogicalPlan = {
    var rtnRelation = super.lookupRelation(name)
    val isRelationRefreshed =
      CarbonSessionUtil.refreshRelationAndSetStats(rtnRelation, name)(sparkSession)
    if (isRelationRefreshed) {
      rtnRelation = super.lookupRelation(name)
      // Reset the stats after lookup.
      CarbonSessionUtil.refreshRelationAndSetStats(rtnRelation, name)(sparkSession)
    }
    rtnRelation
  }

  override def getCachedPlan(t: QualifiedTableName,
      c: Callable[LogicalPlan]): LogicalPlan = {
    val plan = super.getCachedPlan(t, c)
    CarbonSessionUtil.updateCachedPlan(plan)
  }

  /**
   * returns hive client from HiveExternalCatalog
   *
   * @return
   */
  override def getClient(): org.apache.spark.sql.hive.client.HiveClient = {
    CarbonSessionCatalogUtil.getClient(sparkSession)
  }

  override def alterAddColumns(tableIdentifier: TableIdentifier,
      schemaParts: String,
      cols: Option[Seq[ColumnSchema]]): Unit = {
    CarbonSessionCatalogUtil.alterAddColumns(tableIdentifier, schemaParts, cols, sparkSession)
  }

  override def alterDropColumns(tableIdentifier: TableIdentifier,
      schemaParts: String,
      cols: Option[Seq[ColumnSchema]]): Unit = {
    CarbonSessionCatalogUtil.alterDropColumns(tableIdentifier, schemaParts, cols, sparkSession)
  }

  override def alterColumnChangeDataTypeOrRename(tableIdentifier: TableIdentifier,
      schemaParts: String,
      cols: Option[Seq[ColumnSchema]]): Unit = {
    CarbonSessionCatalogUtil.alterColumnChangeDataTypeOrRename(
      tableIdentifier, schemaParts, cols, sparkSession)
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
      sparkSession: SparkSession, identifier: TableIdentifier): Seq[CatalogTablePartition] = {
    CarbonSessionCatalogUtil.getPartitionsAlternate(partitionFilters, sparkSession, identifier)
  }

  /**
   * Update the storageformat with new location information
   */
  override def updateStorageLocation(
      path: Path,
      storage: CatalogStorageFormat,
      newTableName: String,
      dbName: String): CatalogStorageFormat = {
    CarbonSessionCatalogUtil.updateStorageLocation(path, storage, newTableName, dbName)
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

  experimentalMethods.extraStrategies =Seq(new StreamingTableStrategy(sparkSession),
      new CarbonLateDecodeStrategy, new DDLStrategy(sparkSession))
  experimentalMethods.extraOptimizations = Seq(new CarbonIUDRule, new CarbonUDFTransformRule)

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

  override protected def analyzer: Analyzer =
    new CarbonAnalyzer(catalog, conf, sparkSession, super.analyzer)
}

