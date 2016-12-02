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

package org.apache.spark.sql.execution

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.CatalystTypeConverters._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Attribute, _}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.optimizer.CarbonDecoderRelation
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.carbondata.spark.CarbonAliasDecoderRelation




/**
 * Carbon strategy for late decode (convert dictionary key to value as late as possible), which
 * can improve the aggregation performance and reduce memory usage
 */
private[sql] class CarbonLateDecodeStrategy extends SparkStrategy {
  val PUSHED_FILTERS = "PushedFilters"

  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case PhysicalOperation(projects, filters, l: LogicalRelation)
        if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
        val relation = l.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
        pruneFilterProject(
          l,
          projects,
          filters,
          (a, f, needDecoder) => toCatalystRDD(l, a, relation.buildScan(
            a.map(_.name).toArray, f), needDecoder)) ::
            Nil
      case CarbonDictionaryCatalystDecoder(relations, profile, aliasMap, _, child) =>
        CarbonDictionaryDecoder(relations,
          profile,
          aliasMap,
          planLater(child)
        ) :: Nil
      case _ => Nil
    }
  }


  def getDecoderRDD(logicalRelation: LogicalRelation,
                    projectExprsNeedToDecode: ArrayBuffer[AttributeReference],
                    rdd: RDD[Row],
                    output: Seq[Attribute]): RDD[Row] = {
    val table = logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
    val relation = CarbonDecoderRelation(logicalRelation.attributeMap,
      logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation])
    val attrs = projectExprsNeedToDecode.map { attr =>
      val newAttr = AttributeReference(attr.name,
        attr.dataType,
        attr.nullable,
        attr.metadata)(attr.exprId, Option(table.carbonRelation.tableName))
      relation.addAttribute(newAttr)
      newAttr
    }
    new CarbonDecoderRDD(Seq(relation), IncludeProfile(attrs),
      CarbonAliasDecoderRelation(), rdd, output)
  }

  private[this] def toCatalystRDD(
                                   relation: LogicalRelation,
                                   output: Seq[Attribute],
                                   rdd: RDD[Row],
                                   needoDecode: ArrayBuffer[AttributeReference]):
  RDD[InternalRow] = {
    val newRdd = if (needoDecode.size > 0) {
      getDecoderRDD(relation, needoDecode, rdd, output)
    } else {
      rdd
    }
    if (relation.relation.needConversion) {
      execution.RDDConversions.rowToRowRdd(newRdd, output.map(_.dataType))
    } else {
      newRdd.asInstanceOf[RDD[InternalRow]]
    }
  }

  protected def pruneFilterProject(
                                    relation: LogicalRelation,
                                    projects: Seq[NamedExpression],
                                    filterPredicates: Seq[Expression],
                                    scanBuilder: (Seq[Attribute], Array[Filter],
                                        ArrayBuffer[AttributeReference]) =>
                                        RDD[InternalRow]) = {
    pruneFilterProjectRaw(
      relation,
      projects,
      filterPredicates,
      (requestedColumns, _, pushedFilters, a) => {
        scanBuilder(requestedColumns, pushedFilters.toArray, a)
      })
  }

  protected def pruneFilterProjectRaw(
                                       relation: LogicalRelation,
                                       projects: Seq[NamedExpression],
                                       filterPredicates: Seq[Expression],
                                       scanBuilder: (Seq[Attribute], Seq[Expression],
                                           Seq[Filter], ArrayBuffer[AttributeReference]) =>
                                           RDD[InternalRow]) = {

    val projectSet = AttributeSet(projects.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))

    val candidatePredicates = filterPredicates.map {
      _ transform {
        case a: AttributeReference => relation.attributeMap(a) // Match original case of attributes.
      }
    }

    val (unhandledPredicates, pushedFilters) =
      selectFilters(relation.relation, candidatePredicates)

    // A set of column attributes that are only referenced by pushed down filters.  We can eliminate
    // them from requested columns.
    val handledSet = {
      val handledPredicates = filterPredicates.filterNot(unhandledPredicates.contains)
      val unhandledSet = AttributeSet(unhandledPredicates.flatMap(_.references))
      AttributeSet(handledPredicates.flatMap(_.references)) --
          (projectSet ++ unhandledSet).map(relation.attributeMap)
    }

    // Combines all Catalyst filter `Expression`s that are either not convertible to data source
    // `Filter`s or cannot be handled by `relation`.
    val filterCondition = unhandledPredicates.reduceLeftOption(expressions.And)
    val table = relation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
    val map = table.carbonRelation.metaData.dictionaryMap

    val metadata: Map[String, String] = {
      val pairs = ArrayBuffer.empty[(String, String)]

      if (pushedFilters.nonEmpty) {
        pairs += (PUSHED_FILTERS -> pushedFilters.mkString("[", ", ", "]"))
      }
      pairs.toMap
    }


    val needDecoder = ArrayBuffer[AttributeReference]()
    filterCondition match {
      case Some(exp: Expression) =>
        exp.references.collect {
          case attr: AttributeReference =>
            val dict = map.get(attr.name)
            if (dict.isDefined && dict.get) {
              needDecoder += attr
            }
        }
      case None =>
    }

    projects.map {
      case attr: AttributeReference =>
      case Alias(attr: AttributeReference, _) =>
      case others =>
        others.references.map { f =>
          val dictionary = map.get(f.name)
          if (dictionary.isDefined && dictionary.get) {
            needDecoder += f.asInstanceOf[AttributeReference]
          }
        }
    }

    if (projects.map(_.toAttribute) == projects &&
        projectSet.size == projects.size &&
        filterSet.subsetOf(projectSet)) {
      // When it is possible to just use column pruning to get the right projection and
      // when the columns of this projection are enough to evaluate all filter conditions,
      // just do a scan followed by a filter, with no extra project.
      val requestedColumns = projects
          // Safe due to if above.
          .asInstanceOf[Seq[Attribute]]
          // Match original case of attributes.
          .map(relation.attributeMap)
          // Don't request columns that are only referenced by pushed filters.
          .filterNot(handledSet.contains)
      val updateRequestedColumns = updateRequestedColumnsFunc(requestedColumns, table, needDecoder)

      val updateProject = projects.map { expr =>
        var attr = expr.toAttribute.asInstanceOf[AttributeReference]
        if (!needDecoder.exists(_.name.equalsIgnoreCase(attr.name))) {
          val dict = map.get(attr.name)
          if (dict.isDefined && dict.get) {
            attr = AttributeReference(attr.name, IntegerType, attr.nullable, attr.metadata)(attr
                .exprId, attr.qualifier)
          }
        }
        attr
      }
      val scan = execution.DataSourceScanExec.create(
        updateProject,
        scanBuilder(updateRequestedColumns, candidatePredicates, pushedFilters, needDecoder),
        relation.relation, metadata, relation.metastoreTableIdentifier)
      filterCondition.map(execution.FilterExec(_, scan)).getOrElse(scan)
    } else {
      // Don't request columns that are only referenced by pushed filters.
      val requestedColumns =
      (projectSet ++ filterSet -- handledSet).map(relation.attributeMap).toSeq
      val updateRequestedColumns = updateRequestedColumnsFunc(requestedColumns, table, needDecoder)
      val scan = execution.DataSourceScanExec.create(
        updateRequestedColumns,
        scanBuilder(updateRequestedColumns, candidatePredicates, pushedFilters, needDecoder),
        relation.relation, metadata, relation.metastoreTableIdentifier)
      execution.ProjectExec(
        projects, filterCondition.map(execution.FilterExec(_, scan)).getOrElse(scan))
    }
  }

  def updateRequestedColumnsFunc(requestedColumns: Seq[AttributeReference],
      relation: CarbonDatasourceHadoopRelation,
      needDecoder: ArrayBuffer[AttributeReference]): Seq[AttributeReference] = {
    val map = relation.carbonRelation.metaData.dictionaryMap
    requestedColumns.map { attr =>
      if (needDecoder.exists(_.name.equalsIgnoreCase(attr.name))) {
        attr
      } else {
        val dict = map.get(attr.name)
        if (dict.isDefined && dict.get) {
          AttributeReference(attr.name,
            IntegerType,
            attr.nullable,
            attr.metadata)(attr.exprId, attr.qualifier)
        } else {
          attr
        }
      }
    }
  }


  protected[sql] def selectFilters(
                                    relation: BaseRelation,
                                    predicates: Seq[Expression]): (Seq[Expression], Seq[Filter]) = {

    // For conciseness, all Catalyst filter expressions of type `expressions.Expression` below are
    // called `predicate`s, while all data source filters of type `sources.Filter` are simply called
    // `filter`s.

    val translated: Seq[(Expression, Filter)] =
      for {
        predicate <- predicates
        filter <- translateFilter(predicate)
      } yield predicate -> filter

    // A map from original Catalyst expressions to corresponding translated data source filters.
    val translatedMap: Map[Expression, Filter] = translated.toMap

    // Catalyst predicate expressions that cannot be translated to data source filters.
    val unrecognizedPredicates = predicates.filterNot(translatedMap.contains)

    // Data source filters that cannot be handled by `relation`. The semantic of a unhandled filter
    // at here is that a data source may not be able to apply this filter to every row
    // of the underlying dataset.
    val unhandledFilters = relation.unhandledFilters(translatedMap.values.toArray).toSet

    val (unhandled, handled) = translated.partition {
      case (predicate, filter) =>
        unhandledFilters.contains(filter)
    }

    // Catalyst predicate expressions that can be translated to data source filters, but cannot be
    // handled by `relation`.
    val (unhandledPredicates, _) = unhandled.unzip

    // Translated data source filters that can be handled by `relation`
    val (_, handledFilters) = handled.unzip

    // translated contains all filters that have been converted to the public Filter interface.
    // We should always push them to the data source no matter whether the data source can apply
    // a filter to every row or not.
    val (_, translatedFilters) = translated.unzip

    (unrecognizedPredicates ++ unhandledPredicates, translatedFilters)
  }

  /**
   * Tries to translate a Catalyst [[Expression]] into data source [[Filter]].
   * @return a `Some[Filter]` if the input [[Expression]] is convertible, otherwise a `None`.
   */
  protected[sql] def translateFilter(predicate: Expression): Option[Filter] = {
    predicate match {
      case expressions.EqualTo(a: Attribute, Literal(v, t)) =>
        Some(sources.EqualTo(a.name, convertToScala(v, t)))
      case expressions.EqualTo(Literal(v, t), a: Attribute) =>
        Some(sources.EqualTo(a.name, convertToScala(v, t)))

      case expressions.EqualNullSafe(a: Attribute, Literal(v, t)) =>
        Some(sources.EqualNullSafe(a.name, convertToScala(v, t)))
      case expressions.EqualNullSafe(Literal(v, t), a: Attribute) =>
        Some(sources.EqualNullSafe(a.name, convertToScala(v, t)))

      case expressions.GreaterThan(a: Attribute, Literal(v, t)) =>
        Some(sources.GreaterThan(a.name, convertToScala(v, t)))
      case expressions.GreaterThan(Literal(v, t), a: Attribute) =>
        Some(sources.LessThan(a.name, convertToScala(v, t)))

      case expressions.LessThan(a: Attribute, Literal(v, t)) =>
        Some(sources.LessThan(a.name, convertToScala(v, t)))
      case expressions.LessThan(Literal(v, t), a: Attribute) =>
        Some(sources.GreaterThan(a.name, convertToScala(v, t)))

      case expressions.GreaterThanOrEqual(a: Attribute, Literal(v, t)) =>
        Some(sources.GreaterThanOrEqual(a.name, convertToScala(v, t)))
      case expressions.GreaterThanOrEqual(Literal(v, t), a: Attribute) =>
        Some(sources.LessThanOrEqual(a.name, convertToScala(v, t)))

      case expressions.LessThanOrEqual(a: Attribute, Literal(v, t)) =>
        Some(sources.LessThanOrEqual(a.name, convertToScala(v, t)))
      case expressions.LessThanOrEqual(Literal(v, t), a: Attribute) =>
        Some(sources.GreaterThanOrEqual(a.name, convertToScala(v, t)))

      case expressions.InSet(a: Attribute, set) =>
        val toScala = CatalystTypeConverters.createToScalaConverter(a.dataType)
        Some(sources.In(a.name, set.toArray.map(toScala)))

      // Because we only convert In to InSet in Optimizer when there are more than certain
      // items. So it is possible we still get an In expression here that needs to be pushed
      // down.
      case expressions.In(a: Attribute, list) if !list.exists(!_.isInstanceOf[Literal]) =>
        val hSet = list.map(e => e.eval(EmptyRow))
        val toScala = CatalystTypeConverters.createToScalaConverter(a.dataType)
        Some(sources.In(a.name, hSet.toArray.map(toScala)))

      case expressions.IsNull(a: Attribute) =>
        Some(sources.IsNull(a.name))
      case expressions.IsNotNull(a: Attribute) =>
        Some(sources.IsNotNull(a.name))

      case expressions.And(left, right) =>
        (translateFilter(left) ++ translateFilter(right)).reduceOption(sources.And)

      case expressions.Or(left, right) =>
        for {
          leftFilter <- translateFilter(left)
          rightFilter <- translateFilter(right)
        } yield sources.Or(leftFilter, rightFilter)

      case expressions.Not(child) =>
        translateFilter(child).map(sources.Not)

      case expressions.StartsWith(a: Attribute, Literal(v: UTF8String, StringType)) =>
        Some(sources.StringStartsWith(a.name, v.toString))

      case expressions.EndsWith(a: Attribute, Literal(v: UTF8String, StringType)) =>
        Some(sources.StringEndsWith(a.name, v.toString))

      case expressions.Contains(a: Attribute, Literal(v: UTF8String, StringType)) =>
        Some(sources.StringContains(a.name, v.toString))
      case _ => None
    }
  }
}
