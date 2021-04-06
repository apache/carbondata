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
package org.apache.spark.sql.optimizer

import scala.collection.JavaConverters._

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Command, DeserializeToObject, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, ThreadLocalSessionInfo}
import org.apache.carbondata.mv.plans.modular.{ModularPlan, Select}
import org.apache.carbondata.view.{MVCatalogInSpark, MVManagerInSpark, MVSchemaWrapper}
import org.apache.carbondata.view.MVFunctions.DUMMY_FUNCTION

/**
 * Analyzer rule to rewrite the query for MV
 */
class MVRewriteRule(session: SparkSession) extends Rule[LogicalPlan] {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  override def apply(logicalPlan: LogicalPlan): LogicalPlan = {
    // check if query needs to be rewritten with mv
    if (!CarbonProperties.getInstance().isMVEnabled) {
      return logicalPlan
    }
    // only query need to check this rule
    logicalPlan match {
      case _: Command => return logicalPlan
      case _: LocalRelation => return logicalPlan
      case _ =>
    }
    try {
      tryRewritePlan(logicalPlan)
    } catch {
      case e =>
        // if exception is thrown while rewriting the query, will fallback to original query plan.
        MVRewriteRule.LOGGER
          .warn("Failed to rewrite plan with mv. Enable debug log to check the Exception")
        if (MVRewriteRule.LOGGER.isDebugEnabled) {
          MVRewriteRule.LOGGER.debug(e.getMessage)
        }
        logicalPlan
    }
  }

  private def tryRewritePlan(logicalPlan: LogicalPlan): LogicalPlan = {
    var canApply = true
    logicalPlan.transformAllExpressions {
      // first check if any mv UDF is applied it is present is in plan
      // then call is from create MV so no need to transform the query plan
      case alias@Alias(_: ScalaUDF, name) if name.equalsIgnoreCase(DUMMY_FUNCTION) =>
        canApply = false
        alias
      // in case of query if any unresolve alias is present then wait for plan to be resolved
      // return the same plan as we can tranform the plan only when everything is resolved
      case alias@UnresolvedAlias(_, _) =>
        canApply = false
        alias
      case attribute@UnresolvedAttribute(_) =>
        canApply = false
        attribute
    }
    logicalPlan.transform {
      case Aggregate(groupBy, aggregations, child) =>
        // check for if plan is for data load for pre-aggregate table, then skip applying mv
        val haveDummyFunction = aggregations.exists {
          aggregation =>
            if (aggregation.isInstanceOf[UnresolvedAlias]) {
              false
            } else {
              aggregation.name.equals(DUMMY_FUNCTION)
            }
        }
        if (haveDummyFunction) {
          canApply = false
        }
        Aggregate(groupBy, aggregations, child)
      case localRelation@LocalRelation(_, _, _) =>
        canApply = false
        localRelation
    }
    if (!canApply) {
      return logicalPlan
    }
    val viewCatalog = MVManagerInSpark.getOrReloadMVCatalog(session)
    if (viewCatalog != null && hasSuitableMV(logicalPlan, viewCatalog)) {
      LOGGER.debug(s"Query Rewrite has been initiated for the plan: " +
                   s"${ logicalPlan.toString().trim }")
      val viewRewrite = new MVRewrite(viewCatalog, logicalPlan, session)
      val rewrittenPlan = viewRewrite.rewrittenPlan
      if (rewrittenPlan.find(_.rewritten).isDefined) {
        session.sql(rewriteFunctionWithQualifierName(rewrittenPlan)).queryExecution.analyzed
      } else {
        logicalPlan
      }
    } else {
      logicalPlan
    }
  }

  /**
   * This method is specially handled for timeseries on MV, because when we use timeseries UDF which
   * is a scala UDF, so after plan matching when query is made. We get as below query for example
   *
   * SELECT gen_subsumer_0.`UDF:timeseries(projectjoindate, hour)` AS `UDF:timeseries(projectjoi...
   * FROM
   * (SELECT mv1.`UDF:timeseries_projectjoindate_hour` AS `UDF:timeseries(projectjoin...
   * FROM
   *     default.mv1
   * GROUP BY mv1.`UDF:timeseries_projectjoindate_hour`) gen_subsumer_0
   * WHERE
   * (UDF:timeseries(projectjoindate, hour) = TIMESTAMP('2016-02-23 09:00:00.0'))
   *
   * Here for Where filter expression is of type ScalaUDF, so when we do .sql() to prepare SQL, we
   * get without qualifier name(Refer org.apache.spark.sql.catalyst.expressions.NonSQLExpression)
   * which is 'gen_subsumer_0', so this funtion rewrites with qualifier name and returns, so that
   * parsing does not fail in spark, for rewritten MV query.
   * @param modularPlan Modular Plan
   * @return Rewritten plan with the qualifier names for where clauses in query.
   */
  private def rewriteFunctionWithQualifierName(modularPlan: ModularPlan): String = {
    val compactSQL = modularPlan.asCompactSQL
    val finalCompactSQL = modularPlan match {
      case select: Select =>
        var outputColumn = ""
        select.outputList.collect {
          case alias: Alias if alias.child.isInstanceOf[Attribute] =>
            val childName = alias.child.asInstanceOf[Attribute].name
            // from spark 3.1, udf name will not be prefixed with "UDF:"
            if (childName.startsWith("UDF:timeseries") || childName.startsWith("timeseries(")) {
              outputColumn = childName
            }
        }
        var queryArray: Array[String] = Array.empty
        if (!outputColumn.equalsIgnoreCase("") && compactSQL.contains("WHERE")) {
          queryArray = compactSQL.split("\n")
          queryArray(queryArray.indexOf("WHERE") + 1) = queryArray(
            queryArray.indexOf("WHERE") + 1).toLowerCase.replace(outputColumn.toLowerCase,
            s"gen_subsumer_0.`$outputColumn`")
          queryArray.mkString("\n")
        } else {
          compactSQL
        }
      case _ =>
        compactSQL
    }
    LOGGER.debug(s"Rewritten Query: { ${finalCompactSQL.trim} }")
    finalCompactSQL
  }

  /**
   * Whether the plan is valid for doing modular plan matching and mv replacing.
   */
  private def hasSuitableMV(logicalPlan: LogicalPlan,
      mvCatalog: MVCatalogInSpark): Boolean = {
    if (!logicalPlan.isInstanceOf[Command] && !logicalPlan.isInstanceOf[DeserializeToObject]) {
      val catalogTables = logicalPlan collect {
        case relation: LogicalRelation if relation.catalogTable.isDefined => relation.catalogTable
        case relation: HiveTableRelation => Option(relation.tableMeta)
      }
      val validSchemas = mvCatalog.getValidSchemas()
      catalogTables.nonEmpty &&
      !isRewritten(validSchemas, catalogTables) &&
      !isRelatedTableSegmentsSetAsInput(catalogTables) &&
      isRelatedAndSyncWithParentTables(mvCatalog, validSchemas, catalogTables)
    } else {
      false
    }

  }
  /**
   * Check whether mv table already updated in the query.
   *
   * @param viewSchemas Array of available mv which include modular plans
   * @return Boolean whether already mv replaced in the plan or not
   */
  private def isRewritten(
                           viewSchemas: Array[MVSchemaWrapper],
                           tables: Seq[Option[CatalogTable]]): Boolean = {
    tables.exists {
      table =>
        viewSchemas.exists {
          viewSchemaWrapper =>
            val viewIdentifier = viewSchemaWrapper.viewSchema.getIdentifier
            viewIdentifier.getTableName.equals(table.get.identifier.table) &&
            viewIdentifier.getDatabaseName.equals(table.get.database)
        }
    }
  }

  /**
   * Check whether any suitable mvs(like mv which related tables are present in the plan)
   * exists for this plan.
   *
   * @return
   */
  private def isRelatedAndSyncWithParentTables(catalog: MVCatalogInSpark,
      mvSchemas: Array[MVSchemaWrapper],
      tables: Seq[Option[CatalogTable]]): Boolean = {
    tables.exists {
      table =>
        mvSchemas.exists {
          mvSchema =>
          mvSchema.viewSchema.getRelatedTables.asScala.exists {
            mvIdentifier =>
              mvIdentifier.getTableName.equals(table.get.identifier.table) &&
              mvIdentifier.getDatabaseName.equals(table.get.database)
          } && catalog.isMVInSync(mvSchema.viewSchema)
        }
    }
  }

  /**
   * Check if any segments are set for related table for Query. If any segments are set, then
   * skip mv mv table for query
   */
  private def isRelatedTableSegmentsSetAsInput(tables: Seq[Option[CatalogTable]]): Boolean = {
    tables.foreach {
      table =>
        val sessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
        if (sessionInfo != null) {
          val segmentsKey = CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
                            table.get.identifier.database.get + "." +
                            table.get.identifier.table
          val segmentsToQuery = sessionInfo.getSessionParams.getProperty(segmentsKey, "")
          return !segmentsToQuery.isEmpty && !segmentsToQuery.equalsIgnoreCase("*")
        } else {
          return false
        }
      }
      false
  }

}

object MVRewriteRule {

  private val LOGGER =
    LogServiceFactory.getLogService(classOf[MVRewriteRule].getCanonicalName)

}
