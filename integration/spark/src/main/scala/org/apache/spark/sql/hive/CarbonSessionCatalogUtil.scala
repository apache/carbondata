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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{CarbonToSparkAdapter, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.parser.CarbonSparkSqlParserUtil

import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema

object CarbonSessionCatalogUtil {

  /**
   * Method used to update the table name
   * @param oldTableIdentifier old table identifier
   * @param newTableIdentifier new table identifier
   * @param newTablePath new table path
   */
  def alterTableRename(
      oldTableIdentifier: TableIdentifier,
      newTableIdentifier: TableIdentifier,
      newTablePath: String,
      sparkSession: SparkSession,
      isExternal: Boolean
  ): Unit = {
    if (!isExternal) {
      getClient(sparkSession).runSqlHive(
        s"ALTER TABLE ${ oldTableIdentifier.database.get }.${ oldTableIdentifier.table } " +
        s"SET TBLPROPERTIES('EXTERNAL'='TRUE')")
    }
    getClient(sparkSession).runSqlHive(
      s"ALTER TABLE ${ oldTableIdentifier.database.get }.${ oldTableIdentifier.table } " +
      s"RENAME TO ${ newTableIdentifier.database.get }.${ newTableIdentifier.table }")
    if (!isExternal) {
      getClient(sparkSession).runSqlHive(
        s"ALTER TABLE ${ newTableIdentifier.database.get }.${ newTableIdentifier.table } " +
        s"SET TBLPROPERTIES('EXTERNAL'='FALSE')")
    }
    getClient(sparkSession).runSqlHive(
      s"ALTER TABLE ${ newTableIdentifier.database.get }.${ newTableIdentifier.table } " +
      s"SET SERDEPROPERTIES" +
      s"('tableName'='${ newTableIdentifier.table }', " +
      s"'dbName'='${ newTableIdentifier.database.get }', 'tablePath'='${ newTablePath }')")
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

  def alterTableProperties(
      sparkSession: SparkSession,
      tableIdentifier: TableIdentifier,
      properties: Map[String, String],
      propKeys: Seq[String]
  ): Unit = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableIdentifier)
    var newProperties = table.storage.properties
    if (!propKeys.isEmpty) {
      val updatedPropKeys = propKeys.map(_.toLowerCase)
      newProperties = newProperties.filter { case (k, _) => !updatedPropKeys.contains(k) }
    }
    if (!properties.isEmpty) {
      newProperties = newProperties ++ CarbonSparkSqlParserUtil.normalizeProperties(properties)
    }
    val newTable = table.copy(
      storage = table.storage.copy(properties = newProperties)
    )
    catalog.alterTable(newTable)
  }

  /**
   * returns hive client from HiveExternalCatalog
   *
   * @return
   */
  def getClient(sparkSession: SparkSession): org.apache.spark.sql.hive.client.HiveClient = {
    //    For Spark2.2 we need to use unified Spark thrift server instead of carbon thrift
    //    server. CarbonSession is not available anymore so HiveClient is created directly
    //    using sparkSession.sharedState which internally contains all required carbon rules,
    //    optimizers pluged-in through SessionStateBuilder in spark-defaults.conf.
    //    spark.sql.session.state.builder=org.apache.spark.sql.hive.CarbonSessionStateBuilder
    CarbonToSparkAdapter.getHiveExternalCatalog(sparkSession).client
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
      cols: Option[Seq[ColumnSchema]],
      sparkSession: SparkSession): Unit = {
    alterTable(tableIdentifier, schemaParts, cols, sparkSession)
    CarbonSessionUtil.alterExternalCatalogForTableWithUpdatedSchema(
      tableIdentifier, cols, schemaParts, sparkSession)
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
      identifier: TableIdentifier): Seq[CatalogTablePartition] = {
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
