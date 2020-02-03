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
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTablePartition}
import org.apache.spark.sql.catalyst.expressions.Expression

import org.apache.carbondata.common.annotations.{InterfaceAudience, InterfaceStability}
import org.apache.carbondata.core.metadata.schema.table.column.{ColumnSchema => ColumnSchema}

/**
 * This interface defines those common api used by carbon for spark-2.1 and spark-2.2 integration,
 * but are not defined in SessionCatalog or HiveSessionCatalog to give contract to the
 * Concrete implementation classes.
 * For example CarbonSessionCatalog defined in 2.1 and 2.2.
 *
 */
@InterfaceAudience.Internal
@InterfaceStability.Stable
trait CarbonSessionCatalog {
  /**
   * implementation to be provided by each CarbonSessionCatalog based on on used ExternalCatalog
   *
   * @return
   */
  def getClient(): org.apache.spark.sql.hive.client.HiveClient

  /**
   * The method returns the CarbonEnv instance
   *
   * @return
   */
  def getCarbonEnv(): CarbonEnv

  /**
   * This is alternate way of getting partition information. It first fetches all partitions from
   * hive and then apply filter instead of querying hive along with filters.
   *
   * @param partitionFilters
   * @param sparkSession
   * @param identifier
   * @return
   */
  def getPartitionsAlternate(partitionFilters: Seq[Expression], sparkSession: SparkSession,
      identifier: TableIdentifier): Seq[CatalogTablePartition]

  /**
   * Update the storageformat with new location information
   */
  def updateStorageLocation(
      path: Path,
      storage: CatalogStorageFormat,
      newTableName: String,
      dbName: String): CatalogStorageFormat

  /**
   * Below method will be used to add new column
   * @param tableIdentifier table identifier
   * @param schemaParts schema parts
   * @param cols cols
   */
  def alterAddColumns(tableIdentifier: TableIdentifier,
      schemaParts: String,
      cols: Option[Seq[ColumnSchema]]): Unit

  /**
   * Below method will be used to drop column
   * @param tableIdentifier table identifier
   * @param schemaParts schema parts
   * @param cols cols
   */
  def alterDropColumns(tableIdentifier: TableIdentifier,
      schemaParts: String,
      cols: Option[Seq[ColumnSchema]]): Unit

  /**
   * Below method will be used to alter data type of column in schema
   * @param tableIdentifier table identifier
   * @param schemaParts schema parts
   * @param cols cols
   */
  def alterColumnChangeDataTypeOrRename(tableIdentifier: TableIdentifier,
      schemaParts: String,
      cols: Option[Seq[ColumnSchema]]): Unit
}
