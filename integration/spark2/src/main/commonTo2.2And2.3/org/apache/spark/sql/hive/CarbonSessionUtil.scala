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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition, ExternalCatalogUtils}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, CarbonEnv, SparkSession}
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.util.CarbonReflectionUtils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.SparkTypeConverter

import scala.collection.mutable.ArrayBuffer

/**
 * This class refresh the relation from cache if the carbontable in
 * carbon catalog is not same as cached carbon relation's carbon table.
 */
object CarbonSessionUtil {

  val LOGGER = LogServiceFactory.getLogService("CarbonSessionUtil")

  /**
   * The method refreshes the cache entry
   *
   * @param rtnRelation [[LogicalPlan]] represents the given table or view.
   * @param name        tableName
   * @param sparkSession
   * @return
   */
  def refreshRelation(rtnRelation: LogicalPlan, name: TableIdentifier)
    (sparkSession: SparkSession): Boolean = {
    var isRelationRefreshed = false
    rtnRelation match {
      case SubqueryAlias(_,
      MatchLogicalRelation(_: CarbonDatasourceHadoopRelation, _, _)
      ) =>
        isRelationRefreshed = CarbonEnv.refreshRelationFromCache(name)(sparkSession)
      case MatchLogicalRelation(_: CarbonDatasourceHadoopRelation, _, _) =>
        isRelationRefreshed = CarbonEnv.refreshRelationFromCache(name)(sparkSession)
      case SubqueryAlias(_, relation) if
      relation.getClass.getName.equals("org.apache.spark.sql.catalyst.catalog.CatalogRelation") ||
      relation.getClass.getName
        .equals("org.apache.spark.sql.catalyst.catalog.HiveTableRelation") ||
      relation.getClass.getName.equals(
        "org.apache.spark.sql.catalyst.catalog.UnresolvedCatalogRelation"
      ) =>
        val catalogTable =
          CarbonReflectionUtils.getFieldOfCatalogTable(
            "tableMeta",
            relation
          ).asInstanceOf[CatalogTable]
        catalogTable.provider match {
          case Some(provider)
            if provider.equals("org.apache.spark.sql.CarbonSource") ||
               provider.equalsIgnoreCase("carbondata") =>
            // Update stats to none in case of carbon table as we are not expecting any stats from
            // Hive. Hive gives wrong stats for carbon table.
            catalogTable.stats match {
              case Some(stats) =>
                CarbonReflectionUtils.setFieldToCaseClass(catalogTable, "stats", None)
              case _ =>
            }
            isRelationRefreshed =
              CarbonEnv.refreshRelationFromCache(catalogTable.identifier)(sparkSession)
          case _ =>
        }
      case _ =>
    }
    isRelationRefreshed
  }

  /**
   * This is alternate way of getting partition information. It first fetches all partitions from
   * hive and then apply filter instead of querying hive along with filters.
   *
   * @param partitionFilters
   * @param sparkSession
   * @param identifier
   * @return
   */
  def prunePartitionsByFilter(partitionFilters: Seq[Expression],
      sparkSession: SparkSession,
      identifier: TableIdentifier): Seq[CatalogTablePartition] = {
    val allPartitions = sparkSession.sessionState.catalog.listPartitions(identifier)
    ExternalCatalogUtils.prunePartitionsByFilter(
      sparkSession.sessionState.catalog.getTableMetadata(identifier),
      allPartitions,
      partitionFilters,
      sparkSession.sessionState.conf.sessionLocalTimeZone
    )
  }

  /**
   * This method alter the table for datatype change or column rename operation, and update the
   * external catalog directly
   *
   * @param tableIdentifier tableIdentifier for table
   * @param cols            all the column of table, which are updated with datatype change of
   *                        new column name
   * @param schemaParts     schemaParts
   * @param sparkSession    sparkSession
   */
  def alterExternalCatalogForTableWithUpdatedSchema(tableIdentifier: TableIdentifier,
      cols: Option[Seq[ColumnSchema]],
      schemaParts: String,
      sparkSession: SparkSession): Unit = {
    val carbonTable = CarbonEnv.getCarbonTable(tableIdentifier)(sparkSession)
    val colArray: scala.collection.mutable.ArrayBuffer[StructField] = ArrayBuffer()
    cols.get.foreach(column =>
      if (!column.isInvisible) {
        val structFiled =
          if (null != column.getColumnProperties &&
              column.getColumnProperties.get("comment") != null) {
            StructField(column.getColumnName,
              SparkTypeConverter
                .convertCarbonToSparkDataType(column,
                  carbonTable),
              true,
              // update the column comment if present in the schema
              new MetadataBuilder().putString("comment", column.getColumnProperties.get("comment"))
                .build())
          } else {
            StructField(column.getColumnName,
              SparkTypeConverter
                .convertCarbonToSparkDataType(column,
                  carbonTable))
          }
        colArray += structFiled
      }
    )
    // Alter the data schema of a table identified by the provided database and table name.
    // updated schema should contain all the existing data columns. This alterTableDataSchema API
    // should be called after any alter with existing schema which updates the catalog table with
    // new updated schema
    sparkSession.sessionState.catalog.externalCatalog
      .alterTableDataSchema(tableIdentifier.database.get,
        tableIdentifier.table,
        StructType(colArray))
  }

}
