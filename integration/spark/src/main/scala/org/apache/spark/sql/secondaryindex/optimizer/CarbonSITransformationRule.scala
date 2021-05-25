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
package org.apache.spark.sql.secondaryindex.optimizer

import org.apache.log4j.Logger
import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, CarbonThreadUtil, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand
import org.apache.spark.util.SparkUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.index.IndexType
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.util.CarbonSparkUtil

/**
 * Rule for rewriting plan if query has a filter on index table column
 */
class CarbonSITransformationRule(sparkSession: SparkSession)
  extends Rule[LogicalPlan] with PredicateHelper {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  val secondaryIndexOptimizer: CarbonSecondaryIndexOptimizer =
    new CarbonSecondaryIndexOptimizer(sparkSession)

  def apply(plan: LogicalPlan): LogicalPlan = {
    var hasSecondaryIndexTable = false
    plan.collect {
      case l: LogicalRelation if (!hasSecondaryIndexTable &&
                                  l.relation.isInstanceOf[CarbonDatasourceHadoopRelation]) =>
        val carbonTable = l.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable
        hasSecondaryIndexTable = if (!CarbonProperties.getInstance()
          .isCoarseGrainSecondaryIndex(carbonTable.getDatabaseName, carbonTable.getTableName)) {
          carbonTable.getIndexTableNames(IndexType.SI.getIndexProviderName).size() > 0
        } else {
          false
        }
    }
    if (hasSecondaryIndexTable && checkIfRuleNeedToBeApplied(plan)) {
      secondaryIndexOptimizer.transformFilterToJoin(plan, isProjectionNeeded(plan))
    } else {
      plan
    }
  }

  private def checkIfRuleNeedToBeApplied(plan: LogicalPlan): Boolean = {
    var isRuleNeedToBeApplied = false
    val relations = CarbonSparkUtil.collectCarbonRelation(plan)
    val isCreateAsSelect = isCreateTableAsSelect(plan)
    if (relations.nonEmpty && !isCreateAsSelect) {
      plan.collect {
        case join: Join =>
          join.condition match {
            case Some(x) =>
              x match {
                case _: EqualTo =>
                  return isRuleNeedToBeApplied
                  join
                case _ =>
                  join
              }
            case _ =>
          }
        case _ =>
          isRuleNeedToBeApplied = true
          plan
      }
    }
    isRuleNeedToBeApplied
  }

  /**
   * Method to check whether the plan is for create/insert non carbon table(hive, parquet etc).
   * In this case, transformed plan need to add the extra projection, as positionId and
   * positionReference columns will also be added to the output of the plan irrespective of
   * whether the query has requested these columns or not
   *
   * @param plan
   * @return
   */
  private def isProjectionNeeded(plan: LogicalPlan): Boolean = {
    var needProjection = false
    if (SparkUtil.isSparkVersionXAndAbove("2.3")) {
      plan collect {
        case create: CreateHiveTableAsSelectCommand =>
          needProjection = true
        case CreateDataSourceTableAsSelectCommand(_, _, _, _) =>
          needProjection = true
        case create: LogicalPlan if (create.getClass.getSimpleName
          .equals("OptimizedCreateHiveTableAsSelectCommand")) =>
          needProjection = true
        case insert: InsertIntoHadoopFsRelationCommand =>
          if (!insert.fileFormat.toString.equals("carbon")) {
            needProjection = true
          }
      }
    }
    needProjection
  }

  private def isCreateTableAsSelect(plan: LogicalPlan): Boolean = {
    var isCreateTableAsSelectFlow = false
    if (SparkUtil.isSparkVersionXAndAbove("2.3")) {
      plan collect {
        case CreateHiveTableAsSelectCommand(_, _, _, _) =>
          isCreateTableAsSelectFlow = true
        case CreateDataSourceTableAsSelectCommand(_, _, _, _) =>
          isCreateTableAsSelectFlow = true
        case create: LogicalPlan if (create.getClass.getSimpleName
          .equals("OptimizedCreateHiveTableAsSelectCommand")) =>
          isCreateTableAsSelectFlow = true
      }
    }
    isCreateTableAsSelectFlow
  }
}
