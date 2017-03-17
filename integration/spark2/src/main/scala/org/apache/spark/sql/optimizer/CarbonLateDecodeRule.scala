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
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.stats.QueryStatistic
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory
import org.apache.carbondata.spark.{CarbonAliasDecoderRelation, CarbonFilters}


/**
 * Carbon Optimizer to add dictionary decoder. It does two jobs.
 * 1. Change the datatype for dictionary encoded column
 * 2. Add the dictionary decoder operator at appropriate place.
 */
class CarbonLateDecodeRule extends Rule[LogicalPlan] with PredicateHelper {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
  private var relations: Seq[CarbonDecoderRelation] = _

  private def collectCarbonRelation(plan: LogicalPlan): Seq[CarbonDecoderRelation] = {
    plan collect {
      case l: LogicalRelation if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
        CarbonDecoderRelation(l.attributeMap,
        l.relation.asInstanceOf[CarbonDatasourceHadoopRelation])
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    relations = collectCarbonRelation(plan)
    if (relations.nonEmpty && !isOptimized(plan)) {
      // In case scalar subquery skip the transformation and update the flag.
      if (relations.exists(_.carbonRelation.isSubquery)) {
        relations.foreach(p => p.carbonRelation.isSubquery = false)
        LOGGER.info("Skip CarbonOptimizer for scalar sub query")
        return plan
      }
      LOGGER.info("Starting to optimize plan")
      val recorder = CarbonTimeStatisticsFactory.createExecutorRecorder("")
      val queryStatistic = new QueryStatistic()
      val result = transformCarbonPlan(plan, relations)
      queryStatistic.addStatistics("Time taken for Carbon Optimizer to optimize: ",
        System.currentTimeMillis)
      recorder.recordStatistics(queryStatistic)
      recorder.logStatistics()
      result
    } else {
      LOGGER.info("Skip CarbonOptimizer")
      plan
    }
  }

  def isOptimized(plan: LogicalPlan): Boolean = {
    plan find {
      case cd: CarbonDictionaryCatalystDecoder => true
      case other => false
    } isDefined
  }

  case class ExtraNodeInfo(var hasCarbonRelation: Boolean)

  def fillNodeInfo(
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
        // only put no carbon realtion plan
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
   * 2. Plan starts transform by adding the decoder to the plan where it needs the decoded data
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
            child.output.foreach(attr =>
              if (isDictionaryEncoded(attr, attrMap, aliasMap)) {
                condAttrs.add(AttributeReferenceWrapper(aliasMap.getOrElse(attr, attr)))
              }
            )

            if (hasCarbonRelation(child) && condAttrs.size() > 0 &&
              !child.isInstanceOf[CarbonDictionaryCatalystDecoder]) {
              CarbonDictionaryTempDecoder(condAttrs,
                new util.HashSet[AttributeReferenceWrapper](),
                child)
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
          val attrsOndimAggs = new util.HashSet[AttributeReferenceWrapper]
          agg.aggregateExpressions.map {
            case attr: AttributeReference =>
            case a@Alias(attr: AttributeReference, name) =>
            case Alias(AggregateExpression(Count(Seq(attr: AttributeReference)), _, _, _), _) =>
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
          if (!decoder) {
            decoder = true
            CarbonDictionaryTempDecoder(new util.HashSet[AttributeReferenceWrapper](),
              new util.HashSet[AttributeReferenceWrapper](),
              Aggregate(agg.groupingExpressions, agg.aggregateExpressions, child),
              isOuter = true)
          } else {
            Aggregate(agg.groupingExpressions, agg.aggregateExpressions, child)
          }
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
          if (attrsOnJoin.size() > 0) {

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
              leftPlan = CarbonDictionaryTempDecoder(leftCondAttrs,
                new util.HashSet[AttributeReferenceWrapper](),
                j.left)
            }
            if (rightCondAttrs.size() > 0 &&
                !rightPlan.isInstanceOf[CarbonDictionaryCatalystDecoder]) {
              rightPlan = CarbonDictionaryTempDecoder(rightCondAttrs,
                new util.HashSet[AttributeReferenceWrapper](),
                j.right)
            }
            if (!decoder) {
              decoder = true
              CarbonDictionaryTempDecoder(new util.HashSet[AttributeReferenceWrapper](),
                new util.HashSet[AttributeReferenceWrapper](),
                Join(leftPlan, rightPlan, j.joinType, j.condition),
                isOuter = true)
            } else {
              Join(leftPlan, rightPlan, j.joinType, j.condition)
            }
          } else {
            j
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

    val processor = new CarbonDecoderProcessor
    processor.updateDecoders(processor.getDecoderList(transFormedPlan))
    updateProjection(updateTempDecoder(transFormedPlan, aliasMap, attrMap))
  }

  private def updateTempDecoder(plan: LogicalPlan,
      aliasMap: CarbonAliasDecoderRelation,
      attrMap: java.util.HashMap[AttributeReferenceWrapper, CarbonDecoderRelation]):
  LogicalPlan = {
    var allAttrsNotDecode: util.Set[AttributeReferenceWrapper] =
      new util.HashSet[AttributeReferenceWrapper]()
    val marker = new CarbonPlanMarker
    plan transformDown {
      case cd: CarbonDictionaryTempDecoder if !cd.processed =>
        cd.processed = true
        allAttrsNotDecode = cd.attrsNotDecode
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
          s.transform {
            case attr: AttributeReference =>
              updateDataType(attr, attrMap, allAttrsNotDecode, aliasMap)
          }.asInstanceOf[SortOrder]
        }
        Sort(sortExprs, sort.global, sort.child)
      case agg: Aggregate if !agg.child.isInstanceOf[CarbonDictionaryCatalystDecoder] =>
        val aggExps = agg.aggregateExpressions.map { aggExp =>
          aggExp.transform {
            case attr: AttributeReference =>
              updateDataType(attr, attrMap, allAttrsNotDecode, aliasMap)
          }
        }.asInstanceOf[Seq[NamedExpression]]

        val grpExps = agg.groupingExpressions.map { gexp =>
          gexp.transform {
            case attr: AttributeReference =>
              updateDataType(attr, attrMap, allAttrsNotDecode, aliasMap)
          }
        }
        Aggregate(grpExps, aggExps, agg.child)
      case expand: Expand =>
        expand.transformExpressions {
          case attr: AttributeReference =>
            updateDataType(attr, attrMap, allAttrsNotDecode, aliasMap)
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
          prExp.transform {
            case attr: AttributeReference =>
              updateDataType(attr, attrMap, allAttrsNotDecode, aliasMap)
          }
        }.asInstanceOf[Seq[NamedExpression]]
        Project(prExps, p.child)
      case wd: Window if relations.nonEmpty =>
        val prExps = wd.output.map { prExp =>
          prExp.transform {
            case attr: AttributeReference =>
              updateDataType(attr, attrMap, allAttrsNotDecode, aliasMap)
          }
        }.asInstanceOf[Seq[Attribute]]
        val wdExps = wd.windowExpressions.map { gexp =>
          gexp.transform {
            case attr: AttributeReference =>
              updateDataType(attr, attrMap, allAttrsNotDecode, aliasMap)
          }
        }.asInstanceOf[Seq[NamedExpression]]
        val partitionSpec = wd.partitionSpec.map{ exp =>
          exp.transform {
            case attr: AttributeReference =>
              updateDataType(attr, attrMap, allAttrsNotDecode, aliasMap)
          }
        }
        val orderSpec = wd.orderSpec.map { exp =>
          exp.transform {
            case attr: AttributeReference =>
              updateDataType(attr, attrMap, allAttrsNotDecode, aliasMap)
          }
        }.asInstanceOf[Seq[SortOrder]]
        Window(wdExps, partitionSpec, orderSpec, wd.child)

      case l: LogicalRelation if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
        allAttrsNotDecode = marker.revokeJoin()
        l
      case others => others
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
      case CarbonDictionaryCatalystDecoder(_, profile, _, false, child)
        if profile.isInstanceOf[IncludeProfile] && profile.isEmpty =>
        child
    }
    finalPlan
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

  private def isDictionaryEncoded(attr: Attribute,
      attrMap: java.util.HashMap[AttributeReferenceWrapper, CarbonDecoderRelation],
      aliasMap: CarbonAliasDecoderRelation): Boolean = {
    val uAttr = aliasMap.getOrElse(attr, attr)
    val relation = Option(attrMap.get(AttributeReferenceWrapper(uAttr)))
    if (relation.isDefined) {
      relation.get.dictionaryMap.get(uAttr.name) match {
        case Some(true) => true
        case _ => false
      }
    } else {
      false
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
}
