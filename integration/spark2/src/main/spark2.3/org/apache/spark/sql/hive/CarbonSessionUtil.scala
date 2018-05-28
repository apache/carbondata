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
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, CarbonEnv, SparkSession}
import org.apache.spark.util.CarbonReflectionUtils
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.expressions.Expression

import org.apache.carbondata.common.logging.LogServiceFactory

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
      LogicalRelation(_: CarbonDatasourceHadoopRelation, _, _,_)
      ) =>
        isRelationRefreshed = CarbonEnv.refreshRelationFromCache(name)(sparkSession)
      case LogicalRelation(_: CarbonDatasourceHadoopRelation, _, _,_) =>
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
        isRelationRefreshed =
          CarbonEnv.refreshRelationFromCache(catalogTable.identifier)(sparkSession)
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

}
