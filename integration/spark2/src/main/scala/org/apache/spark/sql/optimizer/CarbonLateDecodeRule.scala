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

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
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
import org.apache.carbondata.spark.CarbonAliasDecoderRelation


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
            CarbonUtils.threadStatementId.get(),
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
    if (relations.nonEmpty && !isOptimized(plan)) {
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

  private def isOptimized(plan: LogicalPlan): Boolean = {
    plan find {
      case cd: CarbonDictionaryCatalystDecoder => true
      case other => false
    } isDefined
  }

  case class ExtraNodeInfo(var hasCarbonRelation: Boolean)

  private def fillNodeInfo(
      plan: LogicalPlan,
      extraNodeInfos: java.util.HashMap[LogicalPlan, ExtraNodeInfo]): ExtraNodeInfo = {
    plan match {
      case l: LogicalRelation if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
        val extraNodeInfo = ExtraNodeInfo(true)
        extraNodeInfo
      case others =>
        val extraNodeInfo = ExtraNodeInfo(false)
        others.children.foreach { childPlan =>
          val childExtraNodeInfo = fillNodeInfo(childPlan, extraNodeInfos)
          if (childExtraNodeInfo.hasCarbonRelation) {
            extraNodeInfo.hasCarbonRelation = true
          }
        }
        // only put no carbon relation plan
        if (!extraNodeInfo.hasCarbonRelation) {
          extraNodeInfos.put(plan, extraNodeInfo)
        }
        extraNodeInfo
    }
  }

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
    var decoder = false
    val mapOfNonCarbonPlanNodes = new java.util.HashMap[LogicalPlan, ExtraNodeInfo]
    fillNodeInfo(plan, mapOfNonCarbonPlanNodes)
    val aliasMap = CarbonAliasDecoderRelation()
    // collect alias information before hand.
    collectInformationOnAttributes(plan, aliasMap)

    def hasCarbonRelation(currentPlan: LogicalPlan): Boolean = {
      val extraNodeInfo = mapOfNonCarbonPlanNodes.get(currentPlan)
      if (extraNodeInfo == null) {
        true
      } else {
        extraNodeInfo.hasCarbonRelation
      }
    }

    val attrMap = new util.HashMap[AttributeReferenceWrapper, CarbonDecoderRelation]()
    relations.foreach(_.fillAttributeMap(attrMap))

    def addTempDecoder(currentPlan: LogicalPlan): LogicalPlan = {

      def transformAggregateExpression(agg: Aggregate,
          attrsOnGroup: util.HashSet[AttributeReferenceWrapper] = null): LogicalPlan = {
        val attrsOndimAggs = new util.HashSet[AttributeReferenceWrapper]
        if (attrsOnGroup != null) {
          attrsOndimAggs.addAll(attrsOnGroup)
        }
        agg.aggregateExpressions.map {
          case attr: AttributeReference =>
          case a@Alias(attr: AttributeReference, name) =>
          case aggExp: AggregateExpression =>
            aggExp.transform {
              case aggExp: AggregateExpression =>
                collectDimensionAggregates(aggExp, attrsOndimAggs, aliasMap, attrMap)
                aggExp
            }
          case others =>
            others.collect {
              case attr: AttributeReference
                if isDictionaryEncoded(attr, attrMap, aliasMap) =>
                attrsOndimAggs.add(AttributeReferenceWrapper(aliasMap.getOrElse(attr, attr)))
            }
        }
        var child = agg.child
        // Incase if the child also aggregate then push down decoder to child
        if (attrsOndimAggs.size() > 0 && !child.equals(agg)) {
          child = CarbonDictionaryTempDecoder(attrsOndimAggs,
            new util.HashSet[AttributeReferenceWrapper](),
            agg.child)
        }
        if (!decoder && attrsOnGroup == null) {
          decoder = true
          CarbonDictionaryTempDecoder(new util.HashSet[AttributeReferenceWrapper](),
            new util.HashSet[AttributeReferenceWrapper](),
            Aggregate(agg.groupingExpressions, agg.aggregateExpressions, child),
            isOuter = true)
        } else {
          Aggregate(agg.groupingExpressions, agg.aggregateExpressions, child)
        }
      }
      currentPlan match {
        case limit@GlobalLimit(_, LocalLimit(_, child: Sort)) =>
          if (!decoder) {
            decoder = true
            CarbonDictionaryTempDecoder(new util.HashSet[AttributeReferenceWrapper](),
              new util.HashSet[AttributeReferenceWrapper](),
              limit,
              isOuter = true)
          } else {
            limit
          }
        case sort: Sort if !sort.child.isInstanceOf[CarbonDictionaryTempDecoder] =>
          val attrsOnSort = new util.HashSet[AttributeReferenceWrapper]()
          sort.order.map { s =>
            s.collect {
              case attr: AttributeReference
                if isDictionaryEncoded(attr, attrMap, aliasMap) =>
                attrsOnSort.add(AttributeReferenceWrapper(aliasMap.getOrElse(attr, attr)))
            }
          }
          var child = sort.child
          if (attrsOnSort.size() > 0 && !child.isInstanceOf[Sort]) {
            child = CarbonDictionaryTempDecoder(attrsOnSort,
              new util.HashSet[AttributeReferenceWrapper](), sort.child)
          } else {
            // In case of select * from query it gets logical relation and there is no way
            // to convert the datatypes of attributes, so just add this dummy decoder to convert
            // to dictionary datatypes.
            child match {
              case l: LogicalRelation =>
                child = CarbonDictionaryTempDecoder(new util.HashSet[AttributeReferenceWrapper](),
                  new util.HashSet[AttributeReferenceWrapper](), sort.child)
              case Filter(cond, l: LogicalRelation) =>
                child = CarbonDictionaryTempDecoder(new util.HashSet[AttributeReferenceWrapper](),
                  new util.HashSet[AttributeReferenceWrapper](), sort.child)
              case _ =>
            }
          }

          if (!decoder) {
            decoder = true
            CarbonDictionaryTempDecoder(new util.HashSet[AttributeReferenceWrapper](),
              new util.HashSet[AttributeReferenceWrapper](),
              Sort(sort.order, sort.global, child),
              isOuter = true)
          } else {
            Sort(sort.order, sort.global, child)
          }
        case union: Union
          if !union.children.exists(_.isInstanceOf[CarbonDictionaryTempDecoder]) =>
          val children = union.children.map { child =>
            val condAttrs = new util.HashSet[AttributeReferenceWrapper]
            val localAliasMap = CarbonAliasDecoderRelation()
            // collect alias information for the child plan again. It is required as global alias
            // may have duplicated in case of aliases
            collectInformationOnAttributes(child, localAliasMap)
            child.output.foreach(attr =>
              if (isDictionaryEncoded(attr, attrMap, localAliasMap)) {
                condAttrs.add(AttributeReferenceWrapper(localAliasMap.getOrElse(attr, attr)))
              }
            )

            if (hasCarbonRelation(child) && condAttrs.size() > 0 &&
                !child.isInstanceOf[CarbonDictionaryCatalystDecoder]) {
              CarbonDictionaryTempDecoder(condAttrs,
                new util.HashSet[AttributeReferenceWrapper](),
                child, false, Some(localAliasMap))
            } else {
              child
            }
          }
          if (!decoder) {
            decoder = true
            CarbonDictionaryTempDecoder(new util.HashSet[AttributeReferenceWrapper](),
              new util.HashSet[AttributeReferenceWrapper](),
              Union(children),
              isOuter = true)
          } else {
            Union(children)
          }
        case agg: Aggregate if !agg.child.isInstanceOf[CarbonDictionaryTempDecoder] =>
          transformAggregateExpression(agg)
        case expand: Expand if !expand.child.isInstanceOf[CarbonDictionaryTempDecoder] =>
          val attrsOnExpand = new util.HashSet[AttributeReferenceWrapper]
          expand.projections.map {s =>
            s.map {
              case attr: AttributeReference =>
              case a@Alias(attr: AttributeReference, name) =>
              case others =>
                others.collect {
                  case attr: AttributeReference
                    if isDictionaryEncoded(attr, attrMap, aliasMap) =>
                    attrsOnExpand.add(AttributeReferenceWrapper(aliasMap.getOrElse(attr, attr)))
                }
            }
          }
          var child = expand.child
          if (attrsOnExpand.size() > 0 && !child.isInstanceOf[Expand]) {
            child = CarbonDictionaryTempDecoder(attrsOnExpand,
              new util.HashSet[AttributeReferenceWrapper](),
              expand.child)
          }
          if (!decoder) {
            decoder = true
            CarbonDictionaryTempDecoder(new util.HashSet[AttributeReferenceWrapper](),
              new util.HashSet[AttributeReferenceWrapper](),
              Expand(expand.projections, expand.output, child),
              isOuter = true)
          } else {
            Expand(expand.projections, expand.output, child)
          }
        case filter: Filter if !filter.child.isInstanceOf[CarbonDictionaryTempDecoder] =>
          val attrsOnConds = new util.HashSet[AttributeReferenceWrapper]
          // In case the child is join then we cannot push down the filters so decode them earlier
          if (filter.child.isInstanceOf[Join] || filter.child.isInstanceOf[Sort]) {
            filter.condition.collect {
              case attr: AttributeReference =>
                attrsOnConds.add(AttributeReferenceWrapper(aliasMap.getOrElse(attr, attr)))
            }
          } else {
            CarbonFilters
              .selectFilters(splitConjunctivePredicates(filter.condition), attrsOnConds, aliasMap)
          }

          var child = filter.child
          if (attrsOnConds.size() > 0 && !child.isInstanceOf[Filter]) {
            child = CarbonDictionaryTempDecoder(attrsOnConds,
              new util.HashSet[AttributeReferenceWrapper](),
              filter.child)
          }

          if (!decoder) {
            decoder = true
            CarbonDictionaryTempDecoder(new util.HashSet[AttributeReferenceWrapper](),
              new util.HashSet[AttributeReferenceWrapper](),
              Filter(filter.condition, child),
              isOuter = true)
          } else {
            Filter(filter.condition, child)
          }

        case j: Join
          if !(j.left.isInstanceOf[CarbonDictionaryTempDecoder] ||
               j.right.isInstanceOf[CarbonDictionaryTempDecoder]) =>
          val attrsOnJoin = new util.HashSet[Attribute]
          j.condition match {
            case Some(expression) =>
              expression.collect {
                case attr: AttributeReference
                  if isDictionaryEncoded(attr, attrMap, aliasMap) =>
                  attrsOnJoin.add(aliasMap.getOrElse(attr, attr))
              }
            case _ =>
          }

          val leftCondAttrs = new util.HashSet[AttributeReferenceWrapper]
          val rightCondAttrs = new util.HashSet[AttributeReferenceWrapper]
          val join = if (attrsOnJoin.size() > 0) {

            attrsOnJoin.asScala.map { attr =>
              if (qualifierPresence(j.left, attr)) {
                leftCondAttrs.add(AttributeReferenceWrapper(attr))
              }
              if (qualifierPresence(j.right, attr)) {
                rightCondAttrs.add(AttributeReferenceWrapper(attr))
              }
            }
            var leftPlan = j.left
            var rightPlan = j.right
            if (leftCondAttrs.size() > 0 &&
                !leftPlan.isInstanceOf[CarbonDictionaryCatalystDecoder]) {
              leftPlan = leftPlan match {
                case agg: Aggregate =>
                  CarbonDictionaryTempDecoder(leftCondAttrs,
                    new util.HashSet[AttributeReferenceWrapper](),
                    transformAggregateExpression(agg, leftCondAttrs))
                case _ =>
                  CarbonDictionaryTempDecoder(leftCondAttrs,
                    new util.HashSet[AttributeReferenceWrapper](),
                    j.left)
              }
            }
            if (rightCondAttrs.size() > 0 &&
                !rightPlan.isInstanceOf[CarbonDictionaryCatalystDecoder]) {
              rightPlan = rightPlan match {
                case agg: Aggregate =>
                  CarbonDictionaryTempDecoder(rightCondAttrs,
                    new util.HashSet[AttributeReferenceWrapper](),
                    transformAggregateExpression(agg, rightCondAttrs))
                case _ =>
                  CarbonDictionaryTempDecoder(rightCondAttrs,
                    new util.HashSet[AttributeReferenceWrapper](),
                    j.right)
              }
            }
            Join(leftPlan, rightPlan, j.joinType, j.condition)
          } else {
            j
          }
          if (!decoder) {
            decoder = true
            CarbonDictionaryTempDecoder(new util.HashSet[AttributeReferenceWrapper](),
              new util.HashSet[AttributeReferenceWrapper](),
              join,
              isOuter = true)
          } else {
            join
          }

        case p: Project
          if relations.nonEmpty && !p.child.isInstanceOf[CarbonDictionaryTempDecoder] =>
          val attrsOnProjects = new util.HashSet[AttributeReferenceWrapper]
          p.projectList.map {
            case attr: AttributeReference =>
            case a@Alias(attr: AttributeReference, name) =>
            case others =>
              others.collect {
                case attr: AttributeReference
                  if isDictionaryEncoded(attr, attrMap, aliasMap) =>
                  attrsOnProjects.add(AttributeReferenceWrapper(aliasMap.getOrElse(attr, attr)))
              }
          }
          var child = p.child
          if (attrsOnProjects.size() > 0 && !child.isInstanceOf[Project]) {
            child = CarbonDictionaryTempDecoder(attrsOnProjects,
              new util.HashSet[AttributeReferenceWrapper](),
              p.child)
          }
          if (!decoder) {
            decoder = true
            CarbonDictionaryTempDecoder(new util.HashSet[AttributeReferenceWrapper](),
              new util.HashSet[AttributeReferenceWrapper](),
              Project(p.projectList, child),
              isOuter = true)
          } else {
            Project(p.projectList, child)
          }

        case wd: Window if !wd.child.isInstanceOf[CarbonDictionaryTempDecoder] =>
          val attrsOnProjects = new util.HashSet[AttributeReferenceWrapper]
          wd.output.map {
            case attr: AttributeReference =>
            case others =>
              others.collect {
                case attr: AttributeReference
                  if isDictionaryEncoded(attr, attrMap, aliasMap) =>
                  attrsOnProjects.add(AttributeReferenceWrapper(aliasMap.getOrElse(attr, attr)))
              }
          }
          wd.windowExpressions.map {
            case others =>
              others.collect {
                case attr: AttributeReference
                  if isDictionaryEncoded(attr, attrMap, aliasMap) =>
                  attrsOnProjects.add(AttributeReferenceWrapper(aliasMap.getOrElse(attr, attr)))
              }
          }
          wd.partitionSpec.map{
            case attr: AttributeReference =>
            case others =>
              others.collect {
                case attr: AttributeReference
                  if isDictionaryEncoded(attr, attrMap, aliasMap) =>
                  attrsOnProjects.add(AttributeReferenceWrapper(aliasMap.getOrElse(attr, attr)))
              }
          }
          wd.orderSpec.map { s =>
            s.collect {
              case attr: AttributeReference
                if isDictionaryEncoded(attr, attrMap, aliasMap) =>
                attrsOnProjects.add(AttributeReferenceWrapper(aliasMap.getOrElse(attr, attr)))
            }
          }
          wd.partitionSpec.map { s =>
            s.collect {
              case attr: AttributeReference
                if isDictionaryEncoded(attr, attrMap, aliasMap) =>
                attrsOnProjects.add(AttributeReferenceWrapper(aliasMap.getOrElse(attr, attr)))
            }
          }
          var child = wd.child
          if (attrsOnProjects.size() > 0 && !child.isInstanceOf[Project]) {
            child = CarbonDictionaryTempDecoder(attrsOnProjects,
              new util.HashSet[AttributeReferenceWrapper](),
              wd.child)
          }
          if (!decoder) {
            decoder = true
            CarbonDictionaryTempDecoder(new util.HashSet[AttributeReferenceWrapper](),
              new util.HashSet[AttributeReferenceWrapper](),
              Window(wd.windowExpressions, wd.partitionSpec, wd.orderSpec, child),
              isOuter = true)
          } else {
            Window(wd.windowExpressions, wd.partitionSpec, wd.orderSpec, child)
          }

        case l: LogicalRelation if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
          if (!decoder) {
            decoder = true
            CarbonDictionaryTempDecoder(new util.HashSet[AttributeReferenceWrapper](),
              new util.HashSet[AttributeReferenceWrapper](), l, isOuter = true)
          } else {
            l
          }

        case others => others
      }
    }

    val transFormedPlan =
      plan transformDown {
        case cd: CarbonDictionaryTempDecoder if cd.isOuter =>
          decoder = true
          cd
        case currentPlan =>
          if (hasCarbonRelation(currentPlan)) {
            addTempDecoder(currentPlan)
          } else {
            currentPlan
          }
      }


    transFormedPlan transform {
      // If project list attributes are not present as part of decoder to be decoded attributes
      // then add them to notDecodeCarryForward list, otherwise there is a chance of skipping
      // decoding of those columns in case of join case.
      // If left and right plans both uses same attribute but from left side it is
      // not decoded and right side it is decoded then we should decide based on the above project
      // list plan.
      case project@Project(projectList, child: Join) =>
        val allAttr = new util.HashSet[AttributeReferenceWrapper]()
        val allDecoder = child.collect {
          case cd : CarbonDictionaryTempDecoder =>
            allAttr.addAll(cd.attrList)
            cd
        }
        if (allDecoder.nonEmpty && !allAttr.isEmpty) {
          val notForward = allAttr.asScala.filterNot {attrWrapper =>
            val attr = attrWrapper.attr
            projectList.exists(f => attr.name.equalsIgnoreCase(f.name) && attr.exprId == f.exprId)
          }
          allDecoder.head.notDecodeCarryForward.addAll(notForward.asJava)
        }
        project
    }
    val processor = new CarbonDecoderProcessor
    processor.updateDecoders(processor.getDecoderList(transFormedPlan))
    updateProjection(updateTempDecoder(transFormedPlan, aliasMap, attrMap))
  }

  private def isDictionaryEncoded(attribute: Attribute,
      attributeMap: util.HashMap[AttributeReferenceWrapper, CarbonDecoderRelation],
      aliasMap: CarbonAliasDecoderRelation): Boolean = {

    val uattr = aliasMap.getOrElse(attribute, attribute)
    val relation = Option(attributeMap.get(AttributeReferenceWrapper(uattr)))
    if (relation.isDefined) {
      relation.get.dictionaryMap.get(uattr.name) match {
        case Some(true) => true
        case _ => false
      }
    } else {
      false
    }
  }

  private def needDataTypeUpdate(exp: Expression): Boolean = {
    var needChangeDatatype: Boolean = true
    exp.transform {
      case attr: AttributeReference => attr
      case a@Alias(attr: AttributeReference, _) => a
      case others =>
        // datatype need to change for dictionary columns if only alias
        // or attribute ref present.
        // If anything else present, no need to change data type.
        needChangeDatatype = false
        others
    }
    needChangeDatatype
  }

  private def updateTempDecoder(plan: LogicalPlan,
      aliasMapOriginal: CarbonAliasDecoderRelation,
      attrMap: java.util.HashMap[AttributeReferenceWrapper, CarbonDecoderRelation]):
  LogicalPlan = {
    var allAttrsNotDecode: util.Set[AttributeReferenceWrapper] =
      new util.HashSet[AttributeReferenceWrapper]()
    val marker = new CarbonPlanMarker
    var aliasMap: CarbonAliasDecoderRelation = aliasMapOriginal
    plan transformDown {
      case cd: CarbonDictionaryTempDecoder if !cd.processed =>
        cd.processed = true
        allAttrsNotDecode = cd.attrsNotDecode
        aliasMap = cd.aliasMap.getOrElse(aliasMapOriginal)
        marker.pushMarker(allAttrsNotDecode)
        if (cd.isOuter) {
          CarbonDictionaryCatalystDecoder(relations,
            ExcludeProfile(cd.getAttrsNotDecode.asScala.toSeq),
            aliasMap,
            isOuter = true,
            cd.child)
        } else {
          CarbonDictionaryCatalystDecoder(relations,
            IncludeProfile(cd.getAttrList.asScala.toSeq),
            aliasMap,
            isOuter = false,
            cd.child)
        }
      case cd: CarbonDictionaryCatalystDecoder =>
        cd
      case sort: Sort =>
        val sortExprs = sort.order.map { s =>
          if (needDataTypeUpdate(s)) {
            s.transform {
              case attr: AttributeReference =>
                updateDataType(attr, attrMap, allAttrsNotDecode, aliasMap)
            }.asInstanceOf[SortOrder]
          } else {
            s
          }
        }
        Sort(sortExprs, sort.global, sort.child)
      case agg: Aggregate if !agg.child.isInstanceOf[CarbonDictionaryCatalystDecoder] =>
        val aggExps = agg.aggregateExpressions.map { aggExp =>
          if (needDataTypeUpdate(aggExp)) {
            aggExp.transform {
              case attr: AttributeReference =>
                updateDataType(attr, attrMap, allAttrsNotDecode, aliasMap)
            }
          } else {
            aggExp
          }
        }.asInstanceOf[Seq[NamedExpression]]
        val grpExps = agg.groupingExpressions.map { gexp =>
          if (needDataTypeUpdate(gexp)) {
            gexp.transform {
              case attr: AttributeReference =>
                updateDataType(attr, attrMap, allAttrsNotDecode, aliasMap)
            }
          } else {
            gexp
          }
        }
        Aggregate(grpExps, aggExps, agg.child)
      case expand: Expand =>
        // can't use needDataTypeUpdate here as argument is of type Expand
        var needChangeDatatype: Boolean = true
        expand.transformExpressions {
          case attr: AttributeReference => attr
          case a@Alias(attr: AttributeReference, _) => a
          case others =>
            // datatype need to change for dictionary columns if only alias
            // or attribute ref present.
            // If anything else present, no need to change data type.
            needChangeDatatype = false
            others
        }
        if (needChangeDatatype) {
          val ex = expand.transformExpressions {
            case attr: AttributeReference =>
              updateDataType(attr, attrMap, allAttrsNotDecode, aliasMap)
          }
          // Update the datatype of literal type as per the output type, otherwise codegen fails.
          val updatedProj = ex.projections.map { projs =>
            projs.zipWithIndex.map { case (p, index) =>
              p.transform {
                case l: Literal
                  if l.dataType != ex.output(index).dataType &&
                     !isComplexColumn(ex.output(index), ex.child.output) =>
                  Literal(l.value, ex.output(index).dataType)
              }
            }
          }
          Expand(updatedProj, ex.output, ex.child)
        } else {
          expand
        }
      case filter: Filter =>
        filter
      case j: Join =>
        marker.pushBinaryMarker(allAttrsNotDecode)
        j
      case u: Union =>
        marker.pushBinaryMarker(allAttrsNotDecode)
        u
      case p: Project if relations.nonEmpty =>
        val prExps = p.projectList.map { prExp =>
          if (needDataTypeUpdate(prExp)) {
            prExp.transform {
              case attr: AttributeReference =>
                updateDataType(attr, attrMap, allAttrsNotDecode, aliasMap)
            }.asInstanceOf[NamedExpression]
          } else {
            prExp
          }
        }
        Project(prExps, p.child)
      case wd: Window if relations.nonEmpty =>
        val prExps = wd.output.map { prExp =>
          if (needDataTypeUpdate(prExp)) {
            prExp.transform {
              case attr: AttributeReference =>
                updateDataType(attr, attrMap, allAttrsNotDecode, aliasMap)
            }
          } else {
            prExp
          }
        }.asInstanceOf[Seq[Attribute]]
        val wdExps = wd.windowExpressions.map { gexp =>
          if (needDataTypeUpdate(gexp)) {
            gexp.transform {
              case attr: AttributeReference =>
                updateDataType(attr, attrMap, allAttrsNotDecode, aliasMap)
            }
          } else {
            gexp
          }
        }.asInstanceOf[Seq[NamedExpression]]
        val partitionSpec = wd.partitionSpec.map{ exp =>
          if (needDataTypeUpdate(exp)) {
            exp.transform {
              case attr: AttributeReference =>
                updateDataType(attr, attrMap, allAttrsNotDecode, aliasMap)
            }
          } else {
            exp
          }
        }
        val orderSpec = wd.orderSpec.map { exp =>
          if (needDataTypeUpdate(exp)) {
            exp.transform {
              case attr: AttributeReference =>
                updateDataType(attr, attrMap, allAttrsNotDecode, aliasMap)
            }
          } else {
            exp
          }
        }.asInstanceOf[Seq[SortOrder]]
        Window(wdExps, partitionSpec, orderSpec, wd.child)

      case l: LogicalRelation if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
        allAttrsNotDecode = marker.revokeJoin()
        l
      case others => others
    }
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
    val transFormedPlan = plan transform {
      case p@Project(projectList: Seq[NamedExpression], cd: CarbonDictionaryCatalystDecoder) =>
        if (cd.child.isInstanceOf[Filter] || cd.child.isInstanceOf[LogicalRelation]) {
          Project(projectList: Seq[NamedExpression], cd.child)
        } else {
          p
        }
      case f@Filter(condition: Expression, cd: CarbonDictionaryCatalystDecoder) =>
        if (cd.child.isInstanceOf[Project] || cd.child.isInstanceOf[LogicalRelation]) {
          Filter(condition, cd.child)
        } else {
          f
        }
    }
    // Remove unnecessary decoders
    val finalPlan = transFormedPlan transform {
      case cd@ CarbonDictionaryCatalystDecoder(_, profile, _, false, child) =>
        if (profile.isInstanceOf[IncludeProfile] && profile.isEmpty) {
          child match {
            case l: LogicalRelation => cd
            case Filter(condition, l: LogicalRelation) => cd
            case _ => child
          }
        } else {
          cd
        }
    }

    val updateDtrFn = finalPlan transform {
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

  private def collectInformationOnAttributes(plan: LogicalPlan,
      aliasMap: CarbonAliasDecoderRelation) {
    plan transformAllExpressions  {
      case a@Alias(exp, name) =>
        exp match {
          case attr: Attribute => aliasMap.put(a.toAttribute, attr)
          case _ => aliasMap.put(a.toAttribute, AttributeReference("", StringType)())
        }
        a
    }
    // collect the output of expand and add projections attributes as alias to it.
    plan.collect {
      case expand: Expand =>
        expand.projections.foreach {s =>
          s.zipWithIndex.foreach { f =>
            f._1 match {
              case attr: AttributeReference =>
                aliasMap.put(expand.output(f._2).toAttribute, attr)
              case a@Alias(attr: AttributeReference, name) =>
                aliasMap.put(expand.output(f._2).toAttribute, attr)
              case others =>
            }
          }
        }
    }
  }

  // Collect aggregates on dimensions so that we can add decoder to it.
  private def collectDimensionAggregates(aggExp: AggregateExpression,
      attrsOndimAggs: util.HashSet[AttributeReferenceWrapper],
      aliasMap: CarbonAliasDecoderRelation,
      attrMap: java.util.HashMap[AttributeReferenceWrapper, CarbonDecoderRelation]) {
    aggExp collect {
      case attr: AttributeReference if isDictionaryEncoded(attr, attrMap, aliasMap) =>
        attrsOndimAggs.add(AttributeReferenceWrapper(aliasMap.getOrElse(attr, attr)))
    }
  }

  /**
   * Update the attribute datatype with [IntegerType] if the carbon column is encoded with
   * dictionary.
   *
   */
  private def updateDataType(attr: Attribute,
      attrMap: java.util.HashMap[AttributeReferenceWrapper, CarbonDecoderRelation],
      allAttrsNotDecode: java.util.Set[AttributeReferenceWrapper],
      aliasMap: CarbonAliasDecoderRelation): Attribute = {
    val uAttr = aliasMap.getOrElse(attr, attr)
    val relation = Option(attrMap.get(AttributeReferenceWrapper(uAttr)))
    if (relation.isDefined) {
      relation.get.dictionaryMap.get(uAttr.name) match {
        case Some(true)
          if !allAttrsNotDecode.contains(AttributeReferenceWrapper(uAttr)) =>
          val newAttr = AttributeReference(attr.name,
            IntegerType,
            attr.nullable,
            attr.metadata)(attr.exprId)
          newAttr
        case _ => attr
      }
    } else {
      attr
    }
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

  lazy val dictionaryMap = carbonRelation.carbonRelation.metaData.dictionaryMap

  override def toString: String = carbonRelation.carbonTable.getTableUniqueName
}

