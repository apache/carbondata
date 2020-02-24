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
package org.apache.carbondata.mv.extension

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, ScalaUDF}
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
import org.apache.carbondata.mv.plans.modular.{ModularPlan, Select}
import org.apache.carbondata.mv.rewrite.{MVUdf, SummaryDataset, SummaryDatasetCatalog}

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
      // first check if any mv UDF is applied it is present is in plan
      // then call is from create MV so no need to transform the query plan
      // TODO Add different UDF name
      case al@Alias(udf: ScalaUDF, name) if name.equalsIgnoreCase(MVUdf.MV_SKIP_RULE_UDF) =>
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
            p.name.equals(MVUdf.MV_SKIP_RULE_UDF)
          }
        }
        if (isPreAggLoad) {
          needAnalysis = false
        }
        Aggregate(grp, aExp, child)
    }
    if (needAnalysis) {
      var catalog = DataMapStoreManager.getInstance().getDataMapCatalog(dataMapProvider,
        DataMapClassProvider.MV.getShortName).asInstanceOf[SummaryDatasetCatalog]
      // when first time DataMapCatalogs are initialized, it stores session info also,
      // but when carbon session is newly created, catalog map will not be cleared,
      // so if session info is different, remove the entry from map.
      if (catalog != null && !catalog.mvSession.sparkSession.equals(sparkSession)) {
        DataMapStoreManager.getInstance().clearDataMapCatalog()
        catalog = DataMapStoreManager.getInstance().getDataMapCatalog(dataMapProvider,
          DataMapClassProvider.MV.getShortName).asInstanceOf[SummaryDatasetCatalog]
      }
      if (catalog != null && isValidPlan(plan, catalog)) {
        val modularPlan = catalog.mvSession.sessionState.rewritePlan(plan).withMVTable
        if (modularPlan.find(_.rewritten).isDefined) {
          var compactSQL = modularPlan.asCompactSQL
          compactSQL = reWriteTheUDFInSQLWithQualifierName(modularPlan, compactSQL)
          val analyzed = sparkSession.sql(compactSQL).queryExecution.analyzed
          analyzed
        } else {
          plan
        }
      } else {
        plan
      }
    } else {
      plan
    }
  }

  /**
   * This method is specially handled for timeseries on MV, because when we use timeseries UDF which
   * is a scala UDF, so after plan matching when query is made. We get as below query for example
   *
   * SELECT gen_subsumer_0.`UDF:timeseries(projectjoindate, hour)` AS `UDF:timeseries(projectjoi...
   * FROM
   * (SELECT datamap1_table.`UDF:timeseries_projectjoindate_hour` AS `UDF:timeseries(projectjoin...
   * FROM
   *     default.datamap1_table
   * GROUP BY datamap1_table.`UDF:timeseries_projectjoindate_hour`) gen_subsumer_0
   * WHERE
   * (UDF:timeseries(projectjoindate, hour) = TIMESTAMP('2016-02-23 09:00:00.0'))
   *
   * Here for Where filter expression is of type ScalaUDF, so when we do .sql() to prepare SQL, we
   * get without qualifier name(Refer org.apache.spark.sql.catalyst.expressions.NonSQLExpression)
   * which is 'gen_subsumer_0', so this funtion rewrites with qualifier name and returns, so that
   * parsing does not fail in spark, for rewritten MV query.
   * @param plan Modular Plan
   * @param compactSQL compactSQL generated from Modular plan
   * @return Rewritten plan with the qualifier names for where clauses in query.
   */
  private def reWriteTheUDFInSQLWithQualifierName(plan: ModularPlan, compactSQL: String): String = {
    var outPutUDFColumn = ""
    var reWrittenQuery = compactSQL
    plan match {
      case select: Select =>
        select.outputList.collect {
          case a: Alias if a.child.isInstanceOf[Attribute] =>
            val childName = a.child.asInstanceOf[Attribute].name
            if (childName.startsWith("UDF:timeseries")) {
              outPutUDFColumn = childName
            }
        }
        var queryArray: Array[String] = Array.empty
        if (!outPutUDFColumn.equalsIgnoreCase("") && compactSQL.contains("WHERE")) {
          queryArray = compactSQL.split("\n")
          queryArray(queryArray.indexOf("WHERE") + 1) = queryArray(
            queryArray.indexOf("WHERE") + 1).toLowerCase.replace(outPutUDFColumn.toLowerCase,
            s"gen_subsumer_0.`$outPutUDFColumn`")
          reWrittenQuery = queryArray.mkString("\n")
        }
        reWrittenQuery
      case _ =>
        compactSQL
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
