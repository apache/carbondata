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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.profiler.{Optimizer, Profiler}
import org.apache.spark.sql.types._

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.stats.QueryStatistic
import org.apache.carbondata.core.util.{CarbonProperties, CarbonTimeStatisticsFactory, ThreadLocalSessionInfo}

/**
 * Carbon Optimizer to add dictionary decoder. It does two jobs.
 * 1. Change the datatype for dictionary encoded column
 * 2. Add the dictionary decoder operator at appropriate place.
 */
class CarbonLateDecodeRule extends Rule[LogicalPlan] with PredicateHelper {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  private var relations: Seq[CarbonDecoderRelation] = _

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (checkIfRuleNeedToBeApplied(plan, true)) {
      val recorder = CarbonTimeStatisticsFactory.createExecutorRecorder("")
      val queryStatistic = new QueryStatistic()
      val result = transformCarbonPlan(plan, relations)
      queryStatistic.addStatistics("Time taken for Carbon Optimizer to optimize: ",
        System.currentTimeMillis)
      recorder.recordStatistics(queryStatistic)
      recorder.logStatistics()
      Profiler.invokeIfEnable {
        Profiler.send(
          Optimizer(
            CarbonSession.threadStatementId.get(),
            queryStatistic.getStartTime,
            queryStatistic.getTimeTaken
          )
        )
      }
      result
    } else {
      LOGGER.info("Skip CarbonOptimizer")
      plan
    }
  }

  def checkIfRuleNeedToBeApplied(plan: LogicalPlan, removeSubQuery: Boolean = false): Boolean = {
    relations = collectCarbonRelation(plan)
    validateQueryDirectlyOnDataMap(relations)
    if (relations.nonEmpty) {
      // In case scalar subquery skip the transformation and update the flag.
      if (relations.exists(_.carbonRelation.isSubquery.nonEmpty)) {
        if (removeSubQuery) {
          relations.foreach { carbonDecoderRelation =>
            if (carbonDecoderRelation.carbonRelation.isSubquery.nonEmpty) {
              carbonDecoderRelation.carbonRelation.isSubquery.remove(0)
            }
          }
        }
        LOGGER.info("skip CarbonOptimizer for scalar/predicate sub query")
        return false
      }
      true
    } else {
      LOGGER.info("skip CarbonOptimizer")
      false
    }
  }

  /**
   * Below method will be used to validate if query is directly fired on pre aggregate
   * data map or not
   * @param relations all relations from query
   *
   */
  def validateQueryDirectlyOnDataMap(relations: Seq[CarbonDecoderRelation]): Unit = {
    var isPreAggDataMapExists = false
    // first check if pre aggregate data map exists or not
    relations.foreach { relation =>
      if (relation.carbonRelation.carbonTable.isChildDataMap) {
        isPreAggDataMapExists = true
      }
    }
    var isThrowException = false
    // if relation contains pre aggregate data map
    if (isPreAggDataMapExists) {
      val carbonSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
      if (null != carbonSessionInfo) {
        lazy val sessionPropertyValue = CarbonProperties.getInstance
          .getProperty(CarbonCommonConstants.SUPPORT_DIRECT_QUERY_ON_DATAMAP,
            CarbonCommonConstants.SUPPORT_DIRECT_QUERY_ON_DATAMAP_DEFAULTVALUE)
        // Check if property is set in thread params which would mean this is an internal call
        // (from load or compaction) and should be of highest priority. Otherwise get from
        // session(like user has dynamically given the value using set command). If not found in
        // session then look for the property in carbon.properties file, otherwise use default
        // value 'false'.
        val supportQueryOnDataMap = CarbonEnv
          .getThreadParam(CarbonCommonConstants.SUPPORT_DIRECT_QUERY_ON_DATAMAP,
            sessionPropertyValue).toBoolean
        if (!supportQueryOnDataMap) {
          isThrowException = true
        }
      }
    }
    if (isThrowException) {
      throw new AnalysisException("Query On DataMap not supported because "
        + CarbonCommonConstants.SUPPORT_DIRECT_QUERY_ON_DATAMAP + " is false. " +
        "Please change the value to true by set command or other if you want to query on DataMap.")
    }
  }

  private def collectCarbonRelation(plan: LogicalPlan): Seq[CarbonDecoderRelation] = {
    plan collect {
      case l: LogicalRelation if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
        CarbonDecoderRelation(l.attributeMap,
          l.relation.asInstanceOf[CarbonDatasourceHadoopRelation])
    }
  }

  case class ExtraNodeInfo(var hasCarbonRelation: Boolean)

  /**
   * Steps for changing the plan.
   * 1. It finds out the join condition columns and dimension aggregate columns which are need to
   * be decoded just before that plan executes.
   * 2. Plan starts encode by adding the decoder to the plan where it needs the decoded data
   * like dimension aggregate columns decoder under aggregator and join condition decoder under
   * join children.
   */
  def transformCarbonPlan(plan: LogicalPlan,
      relations: Seq[CarbonDecoderRelation]): LogicalPlan = {
    if (plan.isInstanceOf[RunnableCommand]) {
      return plan
    }
    updateProjection(plan)
  }

  /**
   * Check whether given column is derived from complex column.
   */
  def isComplexColumn(attribute: Attribute, output: Seq[Attribute]): Boolean = {
    val attrName = attribute.name.replace("`", "")
    output.exists { a =>
      a.dataType match {
        case s: StructType =>
          s.fields.map(sf => a.name + "." + sf.name).exists(n => {
            attrName.contains(n)
          })
        case ar : ArrayType =>
          attrName.contains(a.name + "[") || attrName.contains(a.name + ".")
        case m: MapType =>
          attrName.contains(a.name + "[")
        case _ => false
      }
    }
  }

  private def updateProjection(plan: LogicalPlan): LogicalPlan = {
    val updateDtrFn = plan transform {
      case p@Project(projectList: Seq[NamedExpression], cd) =>
        if (cd.isInstanceOf[Filter] || cd.isInstanceOf[LogicalRelation]) {
          p.transformAllExpressions {
            case a@Alias(exp, _)
              if !exp.deterministic && !exp.isInstanceOf[CustomDeterministicExpression] =>
              CarbonToSparkAdapter.createAliasRef(CustomDeterministicExpression(exp),
                a.name,
                a.exprId,
                a.qualifier,
                a.explicitMetadata,
                Some(a))
            case exp: NamedExpression
              if !exp.deterministic && !exp.isInstanceOf[CustomDeterministicExpression] =>
              CustomDeterministicExpression(exp)
          }
        } else {
          p
        }
      case f@Filter(condition: Expression, cd) =>
        if (cd.isInstanceOf[Project] || cd.isInstanceOf[LogicalRelation]) {
          f.transformAllExpressions {
            case a@Alias(exp, _)
              if !exp.deterministic && !exp.isInstanceOf[CustomDeterministicExpression] =>
              CarbonToSparkAdapter.createAliasRef(CustomDeterministicExpression(exp),
                a.name,
                a.exprId,
                a.qualifier,
                a.explicitMetadata,
                Some(a))
            case exp: NamedExpression
              if !exp.deterministic && !exp.isInstanceOf[CustomDeterministicExpression] =>
              CustomDeterministicExpression(exp)
          }
        } else {
          f
        }
    }

    updateDtrFn
  }

  def qualifierPresence(plan: LogicalPlan, attr: Attribute): Boolean = {
    var present = false
    plan collect {
      case l: LogicalRelation if l.attributeMap.contains(attr) =>
        present = true
    }
    present
  }
}

case class CarbonDecoderRelation(
    attributeMap: AttributeMap[AttributeReference],
    carbonRelation: CarbonDatasourceHadoopRelation) {

  val extraAttrs = new ArrayBuffer[Attribute]()

  def addAttribute(attr: Attribute): Unit = {
    extraAttrs += attr
  }

  def contains(attr: Attribute): Boolean = {
    val exists =
      attributeMap.exists(entry => entry._1.name.equalsIgnoreCase(attr.name) &&
                                   entry._1.exprId.equals(attr.exprId)) ||
      extraAttrs.exists(entry => entry.name.equalsIgnoreCase(attr.name) &&
                                 entry.exprId.equals(attr.exprId))
    exists
  }

  def fillAttributeMap(attrMap: java.util.HashMap[AttributeReferenceWrapper,
    CarbonDecoderRelation]): Unit = {
    attributeMap.foreach { attr =>
      attrMap.put(AttributeReferenceWrapper(attr._1), this)
    }
  }

  override def toString: String = carbonRelation.carbonTable.getTableUniqueName
}

