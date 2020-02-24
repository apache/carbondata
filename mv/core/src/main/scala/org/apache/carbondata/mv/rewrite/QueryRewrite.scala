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

package org.apache.carbondata.mv.rewrite

import java.util
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.CarbonToSparkAdapter
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, Literal, NamedExpression, ScalaUDF, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.unsafe.types.UTF8String

import org.apache.carbondata.core.metadata.schema.datamap.DataMapProperty
import org.apache.carbondata.mv.plans.modular.{GroupBy, Matchable, ModularPlan, Select}
import org.apache.carbondata.mv.session.MVSession
import org.apache.carbondata.mv.timeseries.TimeSeriesFunction

/**
 * The primary workflow for rewriting relational queries using Spark libraries.
 * Designed to allow easy access to the intermediate phases of query rewrite for developers.
 *
 * While this is not a public class, we should avoid changing the function names for the sake of
 * changing them, because a lot of developers use the feature for debugging.
 */
class QueryRewrite private (
    state: MVSession,
    logical: LogicalPlan,
    nextSubqueryId: AtomicLong) {
  self =>

  def this(state: MVSession, logical: LogicalPlan) =
    this(state, logical, new AtomicLong(0))

  def newSubsumerName(): String = s"gen_subsumer_${nextSubqueryId.getAndIncrement()}"

  /**
   * Rewrite the updated mv query with corresponding MV table.
   */
  private def rewriteWithMVTable(rewrittenPlan: ModularPlan, rewrite: QueryRewrite): ModularPlan = {
    if (rewrittenPlan.find(_.rewritten).isDefined) {
      var updatedMVTablePlan = rewrittenPlan transform {
        case s: Select =>
          updateDataMap(s, rewrite)
        case g: GroupBy =>
          updateDataMap(g, rewrite)
      }
      if (rewrittenPlan.isRolledUp) {
        // If the rewritten query is rolled up, then rewrite the query based on the original modular
        // plan. Make a new outputList based on original modular plan and wrap rewritten plan with
        // select & group-by nodes with new outputList.

        // For example:
        // Given User query:
        // SELECT timeseries(col,'day') from maintable group by timeseries(col,'day')
        // If plan is rewritten as per 'hour' granularity of datamap1,
        // then rewritten query will be like,
        // SELECT datamap1_table.`UDF:timeseries_projectjoindate_hour` AS `UDF:timeseries
        // (projectjoindate, hour)`
        // FROM
        // default.datamap1_table
        // GROUP BY datamap1_table.`UDF:timeseries_projectjoindate_hour`
        //
        // Now, rewrite the rewritten plan as per the 'day' granularity
        // SELECT timeseries(gen_subsumer_0.`UDF:timeseries(projectjoindate, hour)`,'day' ) AS
        // `UDF:timeseries(projectjoindate, day)`
        //  FROM
        //  (SELECT datamap2_table.`UDF:timeseries_projectjoindate_hour` AS `UDF:timeseries
        //  (projectjoindate, hour)`
        //  FROM
        //    default.datamap2_table
        //  GROUP BY datamap2_table.`UDF:timeseries_projectjoindate_hour`) gen_subsumer_0
        // GROUP BY timeseries(gen_subsumer_0.`UDF:timeseries(projectjoindate, hour)`,'day' )
        rewrite.modularPlan match {
          case select: Select =>
            val outputList = select.outputList
            val rolledUpOutputList = updatedMVTablePlan.asInstanceOf[Select].outputList
            var finalOutputList: Seq[NamedExpression] = Seq.empty
            val mapping = outputList zip rolledUpOutputList
            val newSubsume = rewrite.newSubsumerName()

            mapping.foreach { outputLists =>
              val name: String = getAliasName(outputLists._2)
              outputLists._1 match {
                case a@Alias(scalaUdf: ScalaUDF, aliasName) =>
                  if (scalaUdf.function.isInstanceOf[TimeSeriesFunction]) {
                    val newName = newSubsume + ".`" + name + "`"
                    val transformedUdf = transformTimeSeriesUdf(scalaUdf, newName)
                    finalOutputList = finalOutputList.:+(Alias(transformedUdf, aliasName)(a.exprId,
                      a.qualifier).asInstanceOf[NamedExpression])
                  }
                case Alias(attr: AttributeReference, _) =>
                  finalOutputList = finalOutputList.:+(
                    CarbonToSparkAdapter.createAttributeReference(attr, name, newSubsume))
                case attr: AttributeReference =>
                  finalOutputList = finalOutputList.:+(
                    CarbonToSparkAdapter.createAttributeReference(attr, name, newSubsume))
              }
            }
            val newChildren = new collection.mutable.ArrayBuffer[ModularPlan]()
            val newAliasMap = new collection.mutable.HashMap[Int, String]()

            val sel_plan = select.copy(outputList = finalOutputList,
              inputList = finalOutputList,
              predicateList = Seq.empty)
            newChildren += sel_plan
            newAliasMap += (newChildren.indexOf(sel_plan) -> newSubsume)
            updatedMVTablePlan = select.copy(outputList = finalOutputList,
              inputList = finalOutputList,
              aliasMap = newAliasMap.toMap,
              predicateList = Seq.empty,
              children = Seq(updatedMVTablePlan)).setRewritten()

          case groupBy: GroupBy =>
            updatedMVTablePlan match {
              case select: Select =>
                val selectOutputList = groupBy.outputList
                val rolledUpOutputList = updatedMVTablePlan.asInstanceOf[Select].outputList
                var finalOutputList: Seq[NamedExpression] = Seq.empty
                var predicateList: Seq[Expression] = Seq.empty
                val mapping = selectOutputList zip rolledUpOutputList
                val newSubsume = rewrite.newSubsumerName()

                mapping.foreach { outputLists =>
                  val aliasName: String = getAliasName(outputLists._2)
                  outputLists._1 match {
                    case a@Alias(scalaUdf: ScalaUDF, _) =>
                      if (scalaUdf.function.isInstanceOf[TimeSeriesFunction]) {
                        val newName = newSubsume + ".`" + aliasName + "`"
                        val transformedUdf = transformTimeSeriesUdf(scalaUdf, newName)
                        groupBy.predicateList.foreach {
                          case udf: ScalaUDF if udf.isInstanceOf[ScalaUDF] =>
                            predicateList = predicateList.:+(transformedUdf)
                          case attr: AttributeReference =>
                            predicateList = predicateList.:+(
                              CarbonToSparkAdapter.createAttributeReference(attr,
                                attr.name,
                                newSubsume))
                        }
                        finalOutputList = finalOutputList.:+(Alias(transformedUdf, a.name)(a
                          .exprId, a.qualifier).asInstanceOf[NamedExpression])
                      }
                    case attr: AttributeReference =>
                      finalOutputList = finalOutputList.:+(
                        CarbonToSparkAdapter.createAttributeReference(attr, aliasName, newSubsume))
                    case Alias(attr: AttributeReference, _) =>
                      finalOutputList = finalOutputList.:+(
                        CarbonToSparkAdapter.createAttributeReference(attr, aliasName, newSubsume))
                    case a@Alias(agg: AggregateExpression, name) =>
                      val newAgg = agg.transform {
                        case attr: AttributeReference =>
                          CarbonToSparkAdapter.createAttributeReference(attr, name, newSubsume)
                      }
                      finalOutputList = finalOutputList.:+(Alias(newAgg, name)(a.exprId,
                        a.qualifier).asInstanceOf[NamedExpression])
                    case other => other
                  }
                }
                val newChildren = new collection.mutable.ArrayBuffer[ModularPlan]()
                val newAliasMap = new collection.mutable.HashMap[Int, String]()

                val sel_plan = select.copy(outputList = finalOutputList,
                  inputList = finalOutputList,
                  predicateList = Seq.empty)
                newChildren += sel_plan
                newAliasMap += (newChildren.indexOf(sel_plan) -> newSubsume)
                updatedMVTablePlan = select.copy(outputList = finalOutputList,
                  inputList = finalOutputList,
                  aliasMap = newAliasMap.toMap,
                  children = Seq(updatedMVTablePlan)).setRewritten()
                updatedMVTablePlan = groupBy.copy(outputList = finalOutputList,
                  inputList = finalOutputList,
                  predicateList = predicateList,
                  alias = Some(newAliasMap.mkString),
                  child = updatedMVTablePlan).setRewritten()
                updatedMVTablePlan = select.copy(outputList = finalOutputList,
                  inputList = finalOutputList,
                  children = Seq(updatedMVTablePlan)).setRewritten()
            }
        }
      }
      updatedMVTablePlan
    } else {
      rewrittenPlan
    }
  }

  private def getAliasName(exp: NamedExpression): String = {
    exp match {
      case Alias(_, name) =>
        name
      case attr: AttributeReference =>
        attr.name
    }
  }

  private def transformTimeSeriesUdf(scalaUdf: ScalaUDF, newName: String): Expression = {
    scalaUdf.transformDown {
      case attr: AttributeReference =>
        AttributeReference(newName, attr.dataType)(
          exprId = attr.exprId,
          qualifier = attr.qualifier)
      case l: Literal =>
        Literal(UTF8String.fromString("'" + l.toString() + "'"),
          org.apache.spark.sql.types.DataTypes.StringType)
    }
  }

  /**
   * Update the modular plan as per the datamap table relation inside it.
   *
   * @param subsumer plan to be updated
   * @return Updated modular plan.
   */
  private def updateDataMap(subsumer: ModularPlan, rewrite: QueryRewrite): ModularPlan = {
    subsumer match {
      case s: Select if s.dataMapTableRelation.isDefined =>
        val relation =
          s.dataMapTableRelation.get.asInstanceOf[MVPlanWrapper].plan.asInstanceOf[Select]
        val outputList = getUpdatedOutputList(relation.outputList, s.dataMapTableRelation)
        // when the output list contains multiple projection of same column, but relation
        // contains distinct columns, mapping may go wrong with columns, so select distinct
        val mappings = s.outputList.distinct zip outputList
        val oList = for ((o1, o2) <- mappings) yield {
          if (o1.name != o2.name) Alias(o2, o1.name)(exprId = o1.exprId) else o2
        }
        relation.copy(outputList = oList).setRewritten()
      case g: GroupBy if g.dataMapTableRelation.isDefined =>
        val relation =
          g.dataMapTableRelation.get.asInstanceOf[MVPlanWrapper].plan.asInstanceOf[Select]
        val in = relation.asInstanceOf[Select].outputList
        val outputList = getUpdatedOutputList(relation.outputList, g.dataMapTableRelation)
        val mappings = g.outputList zip outputList
        val oList = for ((left, right) <- mappings) yield {
          left match {
            case Alias(agg@AggregateExpression(fun@Sum(child), _, _, _), name) =>
              val uFun = fun.copy(child = right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@Max(child), _, _, _), name) =>
              val uFun = fun.copy(child = right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@Min(child), _, _, _), name) =>
              val uFun = fun.copy(child = right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@Count(Seq(child)), _, _, _), name) =>
              val uFun = Sum(right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@Corr(l, r), _, _, _), name) =>
              val uFun = Sum(right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@VariancePop(child), _, _, _), name) =>
              val uFun = Sum(right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@VarianceSamp(child), _, _, _), name) =>
              val uFun = Sum(right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@StddevSamp(child), _, _, _), name) =>
              val uFun = Sum(right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@StddevPop(child), _, _, _), name) =>
              val uFun = Sum(right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@CovPopulation(l, r), _, _, _), name) =>
              val uFun = Sum(right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@CovSample(l, r), _, _, _), name) =>
              val uFun = Sum(right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@Skewness(child), _, _, _), name) =>
              val uFun = Sum(right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case Alias(agg@AggregateExpression(fun@Kurtosis(child), _, _, _), name) =>
              val uFun = Sum(right)
              Alias(agg.copy(aggregateFunction = uFun), left.name)(exprId = left.exprId)
            case _ =>
              if (left.name != right.name) Alias(right, left.name)(exprId = left.exprId) else right
          }
        }
        val updatedPredicates = g.predicateList.map { f =>
          mappings.find{ case (k, y) =>
            k match {
              case a: Alias if f.isInstanceOf[Alias] =>
                a.child.semanticEquals(f.children.head)
              case a: Alias => a.child.semanticEquals(f)
              case other => other.semanticEquals(f)
            }
          } match {
            case Some(r) => r._2
            case _ => f
          }
        }
        g.copy(outputList = oList,
          inputList = in,
          predicateList = updatedPredicates,
          child = relation,
          dataMapTableRelation = None).setRewritten()

      case select: Select =>
        select.children match {
          case Seq(g: GroupBy) if g.dataMapTableRelation.isDefined =>
            val relation =
              g.dataMapTableRelation.get.asInstanceOf[MVPlanWrapper].plan.asInstanceOf[Select]
            val aliasMap = getAttributeMap(relation.outputList, g.outputList)
            // Update the flagspec as per the mv table attributes.
            val updatedFlagSpec: Seq[Seq[Any]] = updateFlagSpec(
              keepAlias = false,
              select,
              relation,
              aliasMap)
            if (isFullRefresh(g.dataMapTableRelation.get.asInstanceOf[MVPlanWrapper])) {
              val outputList = getUpdatedOutputList(relation.outputList, g.dataMapTableRelation)
              val mappings = g.outputList zip outputList
              val oList = for ((o1, o2) <- mappings) yield {
                if (o1.name != o2.name) Alias(o2, o1.name)(exprId = o1.exprId) else o2
              }

              val outList = select.outputList.map{ f =>
                oList.find(_.name.equals(f.name)).get
              }
              // Directly keep the relation as child.
              select.copy(
                outputList = outList,
                children = Seq(relation),
                aliasMap = relation.aliasMap,
                flagSpec = updatedFlagSpec).setRewritten()
            } else {
              // First find the indices from the child outlist.
              val indices = select.outputList.map{c =>
                g.outputList.indexWhere{
                  case al : Alias if c.isInstanceOf[Alias] =>
                    al.child.semanticEquals(c.asInstanceOf[Alias].child)
                  case al: Alias if al.child.semanticEquals(c) => true
                  case other if c.isInstanceOf[Alias] =>
                    other.semanticEquals(c.asInstanceOf[Alias].child)
                  case other =>
                    other.semanticEquals(c) || other.toAttribute.semanticEquals(c)
                }
              }
              val child = updateDataMap(g, rewrite).asInstanceOf[Matchable]
              // Get the outList from converted child outList using already selected indices
              val outputSel =
                indices.map(child.outputList(_)).zip(select.outputList).map { case (l, r) =>
                  l match {
                    case a: Alias if r.isInstanceOf[Alias] =>
                      Alias(a.child, r.name)(exprId = r.exprId)
                    case a: Alias => a
                    case other if r.isInstanceOf[Alias] =>
                      Alias(other, r.name)(exprId = r.exprId)
                    case other => other
                  }
                }
              // TODO Remove the unnecessary columns from selection.
              // Only keep columns which are required by parent.
              val inputSel = child.outputList
              select.copy(
                outputList = outputSel,
                inputList = inputSel,
                flagSpec = updatedFlagSpec,
                children = Seq(child)).setRewritten()
            }

          case _ => select
        }

      case other => other
    }
  }

  private def createAttrReference(ref: NamedExpression, name: String): Alias = {
    CarbonToSparkAdapter.createAliasRef(ref, name, exprId = ref.exprId)
  }

  private def getAttributeMap(subsumer: Seq[NamedExpression],
      subsume: Seq[NamedExpression]): Map[AttributeKey, NamedExpression] = {
    // when datamap is created with duplicate columns like select sum(age),sum(age) from table,
    // the subsumee will have duplicate, so handle that case here
    if (subsumer.length == subsume.groupBy(_.name).size) {
      subsume.zip(subsumer).flatMap { case (left, right) =>
        var tuples = left collect {
          case attr: AttributeReference =>
            (AttributeKey(attr), createAttrReference(right, attr.name))
        }
        left match {
          case a: Alias =>
            tuples = Seq((AttributeKey(a.child), createAttrReference(right, a.name))) ++ tuples
          case _ =>
        }
        Seq((AttributeKey(left), createAttrReference(right, left.name))) ++ tuples
      }.toMap
    } else {
      throw new UnsupportedOperationException("Cannot create mapping with unequal sizes")
    }
  }

  /**
   * Updates the flagspec of given select plan with attributes of relation select plan
   */
  private def updateFlagSpec(keepAlias: Boolean,
      select: Select,
      relation: Select,
      aliasMap: Map[AttributeKey, NamedExpression]): Seq[Seq[Any]] = {
    val updatedFlagSpec = select.flagSpec.map { f =>
      f.map {
        case list: ArrayBuffer[_] =>
          list.map { case s: SortOrder =>
            val expressions =
              updateOutPutList(
                Seq(s.child.asInstanceOf[Attribute]),
                relation,
                aliasMap,
                keepAlias = false)
            SortOrder(expressions.head, s.direction)
          }
        // In case of limit it goes to other.
        case other => other
      }
    }
    updatedFlagSpec
  }

  private def updateOutPutList(
      subsumerOutputList: Seq[NamedExpression],
      dataMapRltn: Select,
      aliasMap: Map[AttributeKey, NamedExpression],
      keepAlias: Boolean): Seq[NamedExpression] = {
    var outputSel =
      updateSubsumeAttrs(
        subsumerOutputList,
        aliasMap,
        Some(dataMapRltn.aliasMap.values.head),
        keepAlias).asInstanceOf[Seq[NamedExpression]]
    outputSel.zip(subsumerOutputList).map{ case (l, r) =>
      l match {
        case attr: AttributeReference =>
          CarbonToSparkAdapter.createAliasRef(attr, r.name, r.exprId)
        case a@Alias(attr: AttributeReference, name) =>
          CarbonToSparkAdapter.createAliasRef(attr, r.name, r.exprId)
        case other => other
      }
    }
  }

  /**
   * Updates the expressions as per the subsumer output expressions. It is needed to update the
   * expressions as per the datamap table relation
   *
   * @param expressions        expressions which are needed to update
   * @param aliasName          table alias name
   * @return Updated expressions
   */
  private def updateSubsumeAttrs(
      expressions: Seq[Expression],
      attrMap: Map[AttributeKey, NamedExpression],
      aliasName: Option[String],
      keepAlias: Boolean = false): Seq[Expression] = {

    def getAttribute(exp: Expression) = {
      exp match {
        case Alias(agg: AggregateExpression, name) =>
          agg.aggregateFunction.collect {
            case attr: AttributeReference =>
              CarbonToSparkAdapter.createAttributeReference(attr.name,
                attr.dataType,
                attr.nullable,
                attr.metadata,
                attr.exprId,
                aliasName,
                attr)
          }.head
        case Alias(child, name) =>
          child
        case other => other
      }
    }

    expressions.map {
      case alias@Alias(agg: AggregateExpression, name) =>
        attrMap.get(AttributeKey(agg)).map { exp =>
          CarbonToSparkAdapter.createAliasRef(
            getAttribute(exp),
            name,
            alias.exprId,
            alias.qualifier,
            alias.explicitMetadata,
            Some(alias))
        }.getOrElse(alias)

      case attr: AttributeReference =>
        val uattr = attrMap.get(AttributeKey(attr)).map{a =>
          if (keepAlias) {
            CarbonToSparkAdapter.createAttributeReference(
              name = a.name,
              dataType = a.dataType,
              nullable = a.nullable,
              metadata = a.metadata,
              exprId = a.exprId,
              qualifier = attr.qualifier)
          } else {
            a
          }
        }.getOrElse(attr)
        uattr
      case alias@Alias(expression: Expression, name) =>
        attrMap.get(AttributeKey(expression)).map { exp =>
          CarbonToSparkAdapter
            .createAliasRef(getAttribute(exp), name, alias.exprId, alias.qualifier,
              alias.explicitMetadata, Some(alias))
        }.getOrElse(alias)
      case expression: Expression =>
        val uattr = attrMap.get(AttributeKey(expression))
        uattr.getOrElse(expression)
    }
  }

  /**
   * It checks whether full referesh for the table is required. It means we no need to apply
   * aggregation function or group by functions on the mv table.
   */
  private def isFullRefresh(mvPlanWrapper: MVPlanWrapper): Boolean = {
    val fullRefresh = mvPlanWrapper.dataMapSchema.getProperties.get(DataMapProperty.FULL_REFRESH)
    if (fullRefresh != null) {
      fullRefresh.toBoolean
    } else {
      false
    }
  }

  private def getUpdatedOutputList(outputList: Seq[NamedExpression],
      dataMapTableRelation: Option[ModularPlan]): Seq[NamedExpression] = {
    dataMapTableRelation.collect {
      case mv: MVPlanWrapper =>
        val dataMapSchema = mv.dataMapSchema
        val columnsOrderMap = dataMapSchema.getColumnsOrderMap
        if (null != columnsOrderMap && !columnsOrderMap.isEmpty) {
          val updatedOutputList = new util.ArrayList[NamedExpression]()
          var i = 0
          while (i < columnsOrderMap.size()) {
            updatedOutputList
              .add(outputList.filter(f => f.name.equalsIgnoreCase(columnsOrderMap.get(i))).head)
            i = i + 1
          }
          updatedOutputList.asScala
        } else {
          outputList
        }
      case _ => outputList
    }.get
  }

  lazy val optimizedPlan: LogicalPlan =
    state.sessionState.optimizer.execute(logical)

  lazy val modularPlan: ModularPlan =
    state.sessionState.modularizer.modularize(optimizedPlan).next().harmonized

  lazy val withSummaryData: ModularPlan =
    state.sessionState.navigator.rewriteWithSummaryDatasets(modularPlan, self)

  lazy val withMVTable: ModularPlan = rewriteWithMVTable(withSummaryData, this)

  lazy val toCompactSQL: String = withSummaryData.asCompactSQL

  lazy val toOneLineSQL: String = withSummaryData.asOneLineSQL
}

case class AttributeKey(exp: Expression) {

  override def equals(other: Any): Boolean = other match {
    case attrKey: AttributeKey =>
      exp.semanticEquals(attrKey.exp)
    case _ => false
  }

  // Basically we want to use it as simple linked list so hashcode is hardcoded.
  override def hashCode: Int = 1

}
