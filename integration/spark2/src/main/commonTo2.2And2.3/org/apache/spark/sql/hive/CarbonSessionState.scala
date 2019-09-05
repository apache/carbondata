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

import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SessionState

import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.spark.util.CarbonScalaUtil

object CarbonSessionCatalogUtil {

  def lookupRelation(name: TableIdentifier, sparkSession: SparkSession): LogicalPlan = {
    var rtnRelation = sparkSession.sessionState.catalog.lookupRelation(name)
    val isRelationRefreshed =
      CarbonSessionUtil.refreshRelationAndSetStats(rtnRelation, name)(sparkSession)
    if (isRelationRefreshed) {
      rtnRelation = sparkSession.sessionState.catalog.lookupRelation(name)
      // Reset the stats after lookup.
      CarbonSessionUtil.refreshRelationAndSetStats(rtnRelation, name)(sparkSession)
    }
    rtnRelation
  }


  /**
   * Method used to update the table name
   * @param oldTableIdentifier old table identifier
   * @param newTableIdentifier new table identifier
   * @param newTablePath new table path
   */
  def alterTableRename(oldTableIdentifier: TableIdentifier,
                       newTableIdentifier: TableIdentifier,
                       newTablePath: String,
                       sparkSession: SparkSession): Unit = {
    getClient(sparkSession).runSqlHive(
      s"ALTER TABLE ${ oldTableIdentifier.database.get }.${ oldTableIdentifier.table } " +
        s"RENAME TO ${ oldTableIdentifier.database.get }.${ newTableIdentifier.table }")
    getClient(sparkSession).runSqlHive(
      s"ALTER TABLE ${ oldTableIdentifier.database.get }.${ newTableIdentifier.table } " +
        s"SET SERDEPROPERTIES" +
        s"('tableName'='${ newTableIdentifier.table }', " +
        s"'dbName'='${ oldTableIdentifier.database.get }', 'tablePath'='${ newTablePath }')")
  }

  /**
   * Below method will be used to update serd properties
   * @param tableIdentifier table identifier
   * @param schemaParts schema parts
   * @param cols cols
   */
  def alterTable(tableIdentifier: TableIdentifier,
                 schemaParts: String,
                 cols: Option[Seq[ColumnSchema]],
                 sparkSession: SparkSession): Unit = {
    getClient(sparkSession)
      .runSqlHive(s"ALTER TABLE ${ tableIdentifier.database.get }.${ tableIdentifier.table } " +
        s"SET TBLPROPERTIES(${ schemaParts })")
  }


  def getCachedPlan(t: QualifiedTableName,
      c: Callable[LogicalPlan], sparkSession: SparkSession): LogicalPlan = {
    val plan = sparkSession.sessionState.catalog.getCachedPlan(t, c)
    CarbonSessionUtil.updateCachedPlan(plan)
  }

  /**
   * returns hive client from HiveExternalCatalog
   *
   * @return
   */
  def getClient(sparkSession: SparkSession): org.apache.spark.sql.hive.client.HiveClient = {
    sparkSession.sharedState.externalCatalog
      .asInstanceOf[HiveExternalCatalog].client
  }

  def alterAddColumns(tableIdentifier: TableIdentifier,
      schemaParts: String,
      cols: Option[Seq[ColumnSchema]], sparkSession: SparkSession): Unit = {
    updateCatalogTableForAlter(tableIdentifier, schemaParts, cols, sparkSession)
  }

  def alterDropColumns(tableIdentifier: TableIdentifier,
      schemaParts: String,
      cols: Option[Seq[ColumnSchema]], sparkSession: SparkSession): Unit = {
    updateCatalogTableForAlter(tableIdentifier, schemaParts, cols, sparkSession)
  }

  def alterColumnChangeDataTypeOrRename(tableIdentifier: TableIdentifier,
      schemaParts: String,
      cols: Option[Seq[ColumnSchema]], sparkSession: SparkSession): Unit = {
    updateCatalogTableForAlter(tableIdentifier, schemaParts, cols, sparkSession)
  }

  /**
   * This method alters table to set serde properties and updates the catalog table with new updated
   * schema for all the alter operations like add column, drop column, change datatype or rename
   * column
   * @param tableIdentifier
   * @param schemaParts
   * @param cols
   */
  private def updateCatalogTableForAlter(tableIdentifier: TableIdentifier,
      schemaParts: String,
      cols: Option[Seq[ColumnSchema]], sparkSession: SparkSession): Unit = {
    alterTable(tableIdentifier, schemaParts, cols, sparkSession)
    CarbonSessionUtil
      .alterExternalCatalogForTableWithUpdatedSchema(tableIdentifier,
        cols,
        schemaParts,
        sparkSession)
  }

  def createPartitions(
      tableName: TableIdentifier,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean, sparkSession: SparkSession): Unit = {
    try {
      val table = CarbonEnv.getCarbonTable(tableName)(sparkSession)
      val updatedParts = CarbonScalaUtil.updatePartitions(parts, table)
      sparkSession.sessionState.catalog.createPartitions(tableName, updatedParts, ignoreIfExists)
    } catch {
      case e: Exception =>
        sparkSession.sessionState.catalog.createPartitions(tableName, parts, ignoreIfExists)
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
  def getPartitionsAlternate(partitionFilters: Seq[Expression],
      sparkSession: SparkSession,
      identifier: TableIdentifier) = {
    CarbonSessionUtil.prunePartitionsByFilter(partitionFilters, sparkSession, identifier)
  }

  /**
   * Update the storageformat with new location information
   */
  def updateStorageLocation(
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

  override protected def analyzer: Analyzer = {
    new CarbonAnalyzer(catalog,
      conf,
      sparkSession,
      super.analyzer)
  }


}
