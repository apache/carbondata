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
package org.apache.carbondata.mv.datamap

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Command, DeserializeToObject, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema
import org.apache.carbondata.core.util.ThreadLocalSessionInfo
import org.apache.carbondata.datamap.DataMapManager
import org.apache.carbondata.mv.rewrite.{SummaryDataset, SummaryDatasetCatalog}

/**
 * Analyzer rule to rewrite the query for MV datamap
 *
 * @param sparkSession
 */
class MVAnalyzerRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  // TODO Find way better way to get the provider.
  private val dataMapProvider =
    DataMapManager.get().getDataMapProvider(null,
      new DataMapSchema("", DataMapClassProvider.MV.getShortName), sparkSession)

  private val LOGGER = LogServiceFactory.getLogService(classOf[MVAnalyzerRule].getName)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    var needAnalysis = true
    plan.transformAllExpressions {
      // first check if any preAgg scala function is applied it is present is in plan
      // then call is from create preaggregate table class so no need to transform the query plan
      // TODO Add different UDF name
      case al@Alias(udf: ScalaUDF, name) if name.equalsIgnoreCase("preAgg") =>
        needAnalysis = false
        al
      // in case of query if any unresolve alias is present then wait for plan to be resolved
      // return the same plan as we can tranform the plan only when everything is resolved
      case unresolveAlias@UnresolvedAlias(_, _) =>
        needAnalysis = false
        unresolveAlias
      case attr@UnresolvedAttribute(_) =>
        needAnalysis = false
        attr
    }
    plan.transform {
      case aggregate@Aggregate(grp, aExp, child) =>
        // check for if plan is for dataload for preaggregate table, then skip applying mv
        val isPreAggLoad = aExp.exists { p =>
          if (p.isInstanceOf[UnresolvedAlias]) {
            false
          } else {
            p.name.equals("preAggLoad") || p.name.equals("preAgg")
          }
        }
        if (isPreAggLoad) {
          needAnalysis = false
        }
        Aggregate(grp, aExp, child)
    }
    val catalog = DataMapStoreManager.getInstance().getDataMapCatalog(dataMapProvider,
      DataMapClassProvider.MV.getShortName).asInstanceOf[SummaryDatasetCatalog]
    if (needAnalysis && catalog != null && isValidPlan(plan, catalog)) {
      val modularPlan = catalog.mvSession.sessionState.rewritePlan(plan).withMVTable
      if (modularPlan.find(_.rewritten).isDefined) {
        val compactSQL = modularPlan.asCompactSQL
        val analyzed = sparkSession.sql(compactSQL).queryExecution.analyzed
        analyzed
      } else {
        plan
      }
    } else {
      plan
    }
  }

  /**
   * Whether the plan is valid for doing modular plan matching and datamap replacing.
   */
  def isValidPlan(plan: LogicalPlan, catalog: SummaryDatasetCatalog): Boolean = {
    if (!plan.isInstanceOf[Command]  && !plan.isInstanceOf[DeserializeToObject]) {
      val catalogs = extractCatalogs(plan)
      !isDataMapReplaced(catalog.listAllValidSchema(), catalogs) &&
      isDataMapExists(catalog.listAllValidSchema(), catalogs) &&
      !isSegmentSetForMainTable(catalogs)
    } else {
      false
    }

  }
  /**
   * Check whether datamap table already updated in the query.
   *
   * @param mvdataSetArray Array of available mvdataset which include modular plans
   * @return Boolean whether already datamap replaced in the plan or not
   */
  def isDataMapReplaced(
      mvdataSetArray: Array[SummaryDataset],
      catalogs: Seq[Option[CatalogTable]]): Boolean = {
    catalogs.exists { c =>
      mvdataSetArray.exists { mv =>
        val identifier = mv.dataMapSchema.getRelationIdentifier
        identifier.getTableName.equals(c.get.identifier.table) &&
        identifier.getDatabaseName.equals(c.get.database)
      }
    }
  }

  /**
   * Check whether any suitable datamaps(like datamap which parent tables are present in the plan)
   * exists for this plan.
   *
   * @param mvs
   * @return
   */
  def isDataMapExists(mvs: Array[SummaryDataset], catalogs: Seq[Option[CatalogTable]]): Boolean = {
    catalogs.exists { c =>
      mvs.exists { mv =>
        mv.dataMapSchema.getParentTables.asScala.exists { identifier =>
          identifier.getTableName.equals(c.get.identifier.table) &&
          identifier.getDatabaseName.equals(c.get.database)
        }
      }
    }
  }

  private def extractCatalogs(plan: LogicalPlan): Seq[Option[CatalogTable]] = {
    val catalogs = plan collect {
      case l: LogicalRelation => l.catalogTable
    }
    catalogs
  }

  /**
   * Check if any segments are set for main table for Query. If any segments are set, then
   * skip mv datamap table for query
   */
  def isSegmentSetForMainTable(catalogs: Seq[Option[CatalogTable]]): Boolean = {
    catalogs.foreach { c =>
      val carbonSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
      if (carbonSessionInfo != null) {
        val segmentsToQuery = carbonSessionInfo.getSessionParams
          .getProperty(CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
                       c.get.identifier.database.get + "." +
                       c.get.identifier.table, "")
        if (segmentsToQuery.isEmpty || segmentsToQuery.equalsIgnoreCase("*")) {
          return false
        } else {
          return true
        }
      } else {
        return false
      }
    }
    false
  }

}
