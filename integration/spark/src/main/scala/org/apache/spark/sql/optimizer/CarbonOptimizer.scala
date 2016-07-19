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
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{IntegerType, StringType}

import org.carbondata.spark.CarbonFilters

/**
 * Carbon Optimizer to add dictionary decoder.
 */
object CarbonOptimizer {

  def optimizer(optimizer: Optimizer, conf: CarbonSQLConf, version: String): Optimizer = {
    CodeGenerateFactory.getInstance().optimizerFactory.createOptimizer(optimizer, conf)
  }

  def execute(plan: LogicalPlan, optimizer: Optimizer): LogicalPlan = {
    val executedPlan: LogicalPlan = optimizer.execute(plan)
    val relations = CarbonOptimizer.collectCarbonRelation(plan)
    if (relations.nonEmpty) {
      new ResolveCarbonFunctions(relations).apply(executedPlan)
    } else {
      executedPlan
    }
  }

  // get the carbon relation from plan.
  def collectCarbonRelation(plan: LogicalPlan): Seq[CarbonDecoderRelation] = {
    plan collect {
      case l: LogicalRelation if l.relation.isInstanceOf[CarbonDatasourceRelation] =>
        CarbonDecoderRelation(l.attributeMap, l.relation.asInstanceOf[CarbonDatasourceRelation])
    }
  }
}

/**
 * It does two jobs. 1. Change the datatype for dictionary encoded column 2. Add the dictionary
 * decoder plan.
 */
class ResolveCarbonFunctions(relations: Seq[CarbonDecoderRelation])
  extends Rule[LogicalPlan] with PredicateHelper {

  def apply(plan: LogicalPlan): LogicalPlan = {
    transformCarbonPlan(plan, relations)
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
    var decoder = false
    val aliasMap = CarbonAliasDecoderRelation()
    // collect alias information before hand.
    collectInformationOnAttributes(plan, aliasMap)
    val transFormedPlan =
      plan transformDown {
        case cd: CarbonDictionaryTempDecoder if cd.isOuter =>
          decoder = true
          cd
        case sort: Sort if !sort.child.isInstanceOf[CarbonDictionaryTempDecoder] =>
          val attrsOnSort = new util.HashSet[Attribute]()
          sort.order.map { s =>
            s.collect {
              case attr: AttributeReference
                if isDictionaryEncoded(attr, relations, aliasMap) =>
                attrsOnSort.add(aliasMap.getOrElse(attr, attr))
            }
          }
          var child = sort.child
          if (attrsOnSort.size() > 0 && !child.isInstanceOf[Sort]) {
            child = CarbonDictionaryTempDecoder(attrsOnSort,
              new util.HashSet[Attribute](), sort.child)
          }
          if (!decoder) {
            decoder = true
            CarbonDictionaryTempDecoder(new util.HashSet[Attribute](),
              new util.HashSet[Attribute](),
              Sort(sort.order, sort.global, child),
              isOuter = true)
          } else {
            Sort(sort.order, sort.global, child)
          }

        case agg: Aggregate if !agg.child.isInstanceOf[CarbonDictionaryTempDecoder] =>
          val attrsOndimAggs = new util.HashSet[Attribute]
          agg.aggregateExpressions.map {
            case attr: AttributeReference =>
            case a@Alias(attr: AttributeReference, name) => aliasMap.put(a.toAttribute, attr)
            case aggExp: AggregateExpression =>
              aggExp.transform {
                case aggExp: AggregateExpression =>
                  collectDimensionAggregates(aggExp, attrsOndimAggs, aliasMap)
                  aggExp
                case a@Alias(attr: Attribute, name) =>
                  aliasMap.put(a.toAttribute, attr)
                  a
              }
            case others =>
              others.collect {
                case a@ Alias(attr: AttributeReference, _) => aliasMap.put(a.toAttribute, attr)
                case a@Alias(exp, _) if !exp.isInstanceOf[AttributeReference] =>
                  aliasMap.put(a.toAttribute, new AttributeReference("", StringType)())
                case attr: AttributeReference
                  if isDictionaryEncoded(attr, relations, aliasMap) =>
                  attrsOndimAggs.add(aliasMap.getOrElse(attr, attr))
              }
          }
          var child = agg.child
          // Incase if the child also aggregate then push down decoder to child
          if (attrsOndimAggs.size() > 0 && !child.equals(agg)) {
            child = CarbonDictionaryTempDecoder(attrsOndimAggs,
              new util.HashSet[Attribute](),
              agg.child)
          }
          if (!decoder) {
            decoder = true
            CarbonDictionaryTempDecoder(new util.HashSet[Attribute](),
              new util.HashSet[Attribute](),
              Aggregate(agg.groupingExpressions, agg.aggregateExpressions, child),
              isOuter = true)
          } else {
            Aggregate(agg.groupingExpressions, agg.aggregateExpressions, child)
          }
        case expand: Expand if !expand.child.isInstanceOf[CarbonDictionaryTempDecoder] =>
          val attrsOnExpand = new util.HashSet[Attribute]
          expand.projections.map {s =>
            s.map {
              case attr: AttributeReference =>
              case a@Alias(attr: AttributeReference, name) => aliasMap.put(a.toAttribute, attr)
              case others =>
                others.collect {
                  case attr: AttributeReference
                    if isDictionaryEncoded(attr, relations, aliasMap) =>
                    attrsOnExpand.add(aliasMap.getOrElse(attr, attr))
                }
            }
          }
          var child = expand.child
          if (attrsOnExpand.size() > 0 && !child.isInstanceOf[Expand]) {
            child = CarbonDictionaryTempDecoder(attrsOnExpand,
              new util.HashSet[Attribute](),
              expand.child)
          }
          if (!decoder) {
            decoder = true
            CarbonDictionaryTempDecoder(new util.HashSet[Attribute](),
              new util.HashSet[Attribute](),
              CodeGenerateFactory.getInstance().expandFactory.createExpand(expand, child),
              isOuter = true)
          } else {
            CodeGenerateFactory.getInstance().expandFactory.createExpand(expand, child)
          }
        case filter: Filter if !filter.child.isInstanceOf[CarbonDictionaryTempDecoder] =>
          val attrsOnConds = new util.HashSet[Attribute]
          CarbonFilters
            .selectFilters(splitConjunctivePredicates(filter.condition), attrsOnConds, aliasMap)

          var child = filter.child
          if (attrsOnConds.size() > 0 && !child.isInstanceOf[Filter]) {
            child = CarbonDictionaryTempDecoder(attrsOnConds,
              new util.HashSet[Attribute](),
              filter.child)
          }

          if (!decoder) {
            decoder = true
            CarbonDictionaryTempDecoder(new util.HashSet[Attribute](),
              new util.HashSet[Attribute](),
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
                  if isDictionaryEncoded(attr, relations, aliasMap) =>
                  attrsOnJoin.add(aliasMap.getOrElse(attr, attr))
              }
            case _ =>
          }

          val leftCondAttrs = new util.HashSet[Attribute]
          val rightCondAttrs = new util.HashSet[Attribute]
          if (attrsOnJoin.size() > 0) {

            attrsOnJoin.asScala.map { attr =>
              if (qualifierPresence(j.left, attr)) {
                leftCondAttrs.add(attr)
              }
              if (qualifierPresence(j.right, attr)) {
                rightCondAttrs.add(attr)
              }
            }
            var leftPlan = j.left
            var rightPlan = j.right
            if (leftCondAttrs.size() > 0 &&
                !leftPlan.isInstanceOf[CarbonDictionaryCatalystDecoder]) {
              leftPlan = CarbonDictionaryTempDecoder(leftCondAttrs,
                new util.HashSet[Attribute](),
                j.left)
            }
            if (rightCondAttrs.size() > 0 &&
                !rightPlan.isInstanceOf[CarbonDictionaryCatalystDecoder]) {
              rightPlan = CarbonDictionaryTempDecoder(rightCondAttrs,
                new util.HashSet[Attribute](),
                j.right)
            }
            if (!decoder) {
              decoder = true
              CarbonDictionaryTempDecoder(new util.HashSet[Attribute](),
                new util.HashSet[Attribute](),
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
          val attrsOnProjects = new util.HashSet[Attribute]
          p.projectList.map {
            case attr: AttributeReference =>
            case a@Alias(attr: AttributeReference, name) => aliasMap.put(a.toAttribute, attr)
            case others =>
              others.collect {
                case attr: AttributeReference
                  if isDictionaryEncoded(attr, relations, aliasMap) =>
                  attrsOnProjects.add(aliasMap.getOrElse(attr, attr))
              }
          }
          var child = p.child
          if (attrsOnProjects.size() > 0 && !child.isInstanceOf[Project]) {
            child = CarbonDictionaryTempDecoder(attrsOnProjects,
              new util.HashSet[Attribute](),
              p.child)
          }
          if (!decoder) {
            decoder = true
            CarbonDictionaryTempDecoder(new util.HashSet[Attribute](),
              new util.HashSet[Attribute](),
              Project(p.projectList, child),
              isOuter = true)
          } else {
            Project(p.projectList, child)
          }

        case wd: Window if !wd.child.isInstanceOf[CarbonDictionaryTempDecoder] =>
          val attrsOnProjects = new util.HashSet[Attribute]
          wd.projectList.map {
            case attr: AttributeReference =>
            case others =>
              others.collect {
                case attr: AttributeReference
                  if isDictionaryEncoded(attr, relations, aliasMap) =>
                  attrsOnProjects.add(aliasMap.getOrElse(attr, attr))
              }
          }
          wd.windowExpressions.map {
            case others =>
              others.collect {
                case attr: AttributeReference
                  if isDictionaryEncoded(attr, relations, aliasMap) =>
                  attrsOnProjects.add(aliasMap.getOrElse(attr, attr))
              }
          }
          wd.partitionSpec.map{
            case attr: AttributeReference =>
            case others =>
              others.collect {
                case attr: AttributeReference
                  if isDictionaryEncoded(attr, relations, aliasMap) =>
                  attrsOnProjects.add(aliasMap.getOrElse(attr, attr))
              }
          }
          wd.orderSpec.map { s =>
            s.collect {
              case attr: AttributeReference
                if isDictionaryEncoded(attr, relations, aliasMap) =>
                attrsOnProjects.add(aliasMap.getOrElse(attr, attr))
            }
          }
          wd.partitionSpec.map { s =>
            s.collect {
              case attr: AttributeReference
                if isDictionaryEncoded(attr, relations, aliasMap) =>
                attrsOnProjects.add(aliasMap.getOrElse(attr, attr))
            }
          }
          var child = wd.child
          if (attrsOnProjects.size() > 0 && !child.isInstanceOf[Project]) {
            child = CarbonDictionaryTempDecoder(attrsOnProjects,
              new util.HashSet[Attribute](),
              wd.child)
          }
          if (!decoder) {
            decoder = true
            CarbonDictionaryTempDecoder(new util.HashSet[Attribute](),
              new util.HashSet[Attribute](),
              Window(wd.projectList, wd.windowExpressions, wd.partitionSpec, wd.orderSpec, child),
              isOuter = true)
          } else {
            Window(wd.projectList, wd.windowExpressions, wd.partitionSpec, wd.orderSpec, child)
          }

        case l: LogicalRelation if l.relation.isInstanceOf[CarbonDatasourceRelation] =>
          if (!decoder) {
            decoder = true
            CarbonDictionaryTempDecoder(new util.HashSet[Attribute](),
              new util.HashSet[Attribute](), l, isOuter = true)
          } else {
            l
          }

        case others => others
      }

    val processor = new CarbonDecoderProcessor
    processor.updateDecoders(processor.getDecoderList(transFormedPlan))
    updateProjection(updateTempDecoder(transFormedPlan, aliasMap))
  }

  private def updateTempDecoder(plan: LogicalPlan,
      aliasMap: CarbonAliasDecoderRelation): LogicalPlan = {
    var allAttrsNotDecode: util.Set[Attribute] = new util.HashSet[Attribute]()
    val marker = new CarbonPlanMarker
    plan transformDown {
      case cd: CarbonDictionaryTempDecoder if !cd.processed =>
        cd.processed = true
        allAttrsNotDecode = cd.attrsNotDecode
        marker.pushMarker(cd.attrsNotDecode)
        if (cd.isOuter) {
          CarbonDictionaryCatalystDecoder(relations,
            ExcludeProfile(cd.attrsNotDecode.asScala.toSeq),
            aliasMap,
            isOuter = true,
            cd.child)
        } else {
          CarbonDictionaryCatalystDecoder(relations,
            IncludeProfile(cd.attrList.asScala.toSeq),
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
              updateDataType(attr, relations, allAttrsNotDecode, aliasMap)
          }.asInstanceOf[SortOrder]
        }
        Sort(sortExprs, sort.global, sort.child)
      case agg: Aggregate if !agg.child.isInstanceOf[CarbonDictionaryCatalystDecoder] =>
        val aggExps = agg.aggregateExpressions.map { aggExp =>
          aggExp.transform {
            case attr: AttributeReference =>
              updateDataType(attr, relations, allAttrsNotDecode, aliasMap)
          }
        }.asInstanceOf[Seq[NamedExpression]]

        val grpExps = agg.groupingExpressions.map { gexp =>
          gexp.transform {
            case attr: AttributeReference =>
              updateDataType(attr, relations, allAttrsNotDecode, aliasMap)
          }
        }
        Aggregate(grpExps, aggExps, agg.child)
      case filter: Filter =>
        val filterExps = filter.condition transform {
          case attr: AttributeReference =>
            updateDataType(attr, relations, allAttrsNotDecode, aliasMap)
        }
        Filter(filterExps, filter.child)
      case j: Join =>
        marker.pushJoinMarker(allAttrsNotDecode)
        j
      case p: Project if relations.nonEmpty =>
        val prExps = p.projectList.map { prExp =>
          prExp.transform {
            case attr: AttributeReference =>
              updateDataType(attr, relations, allAttrsNotDecode, aliasMap)
          }
        }.asInstanceOf[Seq[NamedExpression]]
        Project(prExps, p.child)
      case wd: Window if relations.nonEmpty =>
        val prExps = wd.projectList.map { prExp =>
          prExp.transform {
            case attr: AttributeReference =>
              updateDataType(attr, relations, allAttrsNotDecode, aliasMap)
          }
        }.asInstanceOf[Seq[Attribute]]
        val wdExps = wd.windowExpressions.map { gexp =>
          gexp.transform {
            case attr: AttributeReference =>
              updateDataType(attr, relations, allAttrsNotDecode, aliasMap)
          }
        }.asInstanceOf[Seq[NamedExpression]]
        val partitionSpec = wd.partitionSpec.map{ exp =>
          exp.transform {
            case attr: AttributeReference =>
              updateDataType(attr, relations, allAttrsNotDecode, aliasMap)
          }
        }
        val orderSpec = wd.orderSpec.map { exp =>
          exp.transform {
            case attr: AttributeReference =>
              updateDataType(attr, relations, allAttrsNotDecode, aliasMap)
          }
        }.asInstanceOf[Seq[SortOrder]]
        Window(prExps, wdExps, partitionSpec, orderSpec, wd.child)

      case l: LogicalRelation if l.relation.isInstanceOf[CarbonDatasourceRelation] =>
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
    plan transformUp {
      case project: Project =>
        project.projectList.map { p =>
          p transform {
            case a@Alias(attr: Attribute, name) =>
              aliasMap.put(a.toAttribute, attr)
              a
            case a@Alias(_, name) =>
              aliasMap.put(a.toAttribute, new AttributeReference("", StringType)())
              a
          }
        }
        project
      case agg: Aggregate =>
        agg.aggregateExpressions.map { aggExp =>
          aggExp.transform {
            case a@Alias(attr: Attribute, name) =>
              aliasMap.put(a.toAttribute, attr)
              a
            case a@Alias(_, name) =>
              aliasMap.put(a.toAttribute, new AttributeReference("", StringType)())
              a
          }
        }
        agg
    }
  }

  // Collect aggregates on dimensions so that we can add decoder to it.
  private def collectDimensionAggregates(aggExp: AggregateExpression,
      attrsOndimAggs: util.HashSet[Attribute],
      aliasMap: CarbonAliasDecoderRelation) {
    aggExp collect {
      case attr: AttributeReference if isDictionaryEncoded(attr, relations, aliasMap) =>
        attrsOndimAggs.add(aliasMap.getOrElse(attr, attr))
      case a@Alias(attr: Attribute, name) => aliasMap.put(a.toAttribute, attr)
    }
  }

  /**
   * Update the attribute datatype with [IntegerType] if the carbon column is encoded with
   * dictionary.
   *
   */
  private def updateDataType(attr: Attribute,
      relations: Seq[CarbonDecoderRelation],
      allAttrsNotDecode: util.Set[Attribute],
      aliasMap: CarbonAliasDecoderRelation): Attribute = {
    val uAttr = aliasMap.getOrElse(attr, attr)
    val relation = relations.find(p => p.contains(uAttr))
    if (relation.isDefined) {
      relation.get.carbonRelation.carbonRelation.metaData.dictionaryMap.get(uAttr.name) match {
        case Some(true) if !allAttrsNotDecode.asScala.exists(p => p.name.equals(uAttr.name)) =>
          val newAttr = AttributeReference(attr.name,
            IntegerType,
            attr.nullable,
            attr.metadata)(attr.exprId, attr.qualifiers)
          relation.get.addAttribute(newAttr)
          newAttr
        case _ => attr
      }
    } else {
      attr
    }
  }

  private def isDictionaryEncoded(attr: Attribute,
      relations: Seq[CarbonDecoderRelation],
      aliasMap: CarbonAliasDecoderRelation): Boolean = {
    val uAttr = aliasMap.getOrElse(attr, attr)
    val relation = relations.find(p => p.contains(uAttr))
    if (relation.isDefined) {
      relation.get.carbonRelation.carbonRelation.metaData.dictionaryMap.get(uAttr.name) match {
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
    carbonRelation: CarbonDatasourceRelation) {
  val extraAttrs = new ArrayBuffer[Attribute]()

  def addAttribute(attr: Attribute): Unit = {
    extraAttrs += attr
  }

  def contains(attr: Attribute): Boolean = {
    var exists =
      attributeMap.exists(entry => entry._1.name.equalsIgnoreCase(attr.name) &&
                                   entry._1.exprId.equals(attr.exprId)) ||
      extraAttrs.exists(entry => entry.name.equalsIgnoreCase(attr.name) &&
                                 entry.exprId.equals(attr.exprId))
    if(!exists) {
      exists = attributeMap.exists(entry => entry._1.name.equalsIgnoreCase(attr.name)) ||
               extraAttrs.exists(entry => entry.name.equalsIgnoreCase(attr.name) )
    }
    exists
  }
}

case class CarbonAliasDecoderRelation() {

  val attrMap = new ArrayBuffer[(Attribute, Attribute)]()

  def put(key: Attribute, value: Attribute): Unit = {
    attrMap += ((key, value))
  }

  def getOrElse(key: Attribute, default: Attribute): Attribute = {
    val value = attrMap.find(p =>
      p._1.name.equalsIgnoreCase(key.name) && p._1.exprId.equals(key.exprId))
    value match {
      case Some((k, v)) => v
      case _ => default
    }
  }
}
