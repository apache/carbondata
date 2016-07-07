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

package org.apache.spark.sql

import java.util.HashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.unsafe.types.UTF8String

import org.carbondata.query.carbon.wrappers.ByteArrayWrapper

/**
 * :: DeveloperApi ::
 * Groups input data by `groupingExpressions` and computes the `aggregateExpressions` for each
 * group.
 *
 * @param partial              if true then aggregation is done partially on local data without
 *                             shuffling to
 *                             ensure all values where `groupingExpressions` are equal are present.
 * @param groupingExpressions  expressions that are evaluated to determine grouping.
 * @param aggregateExpressions expressions that are computed for each group.
 * @param child                the input data source.
 */
@DeveloperApi
case class CarbonRawAggregate(
  partial: Boolean,
  groupingExpressions: Seq[Expression],
  aggregateExpressions: Seq[NamedExpression],
  child: SparkPlan)
  extends UnaryNode {

  override private[sql] lazy val metrics = Map(
    "numInputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  override def requiredChildDistribution: List[Distribution] = {
    if (partial) {
      UnspecifiedDistribution :: Nil
    } else {
      if (groupingExpressions == Nil) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(groupingExpressions) :: Nil
      }
    }
  }

  override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)

  /**
   * An aggregate that needs to be computed for each row in a group.
   *
   * @param unbound         Unbound version of this aggregate, used for result substitution.
   * @param aggregate       A bound copy of this aggregate used to create a new aggregation buffer.
   * @param resultAttribute An attribute used to refer to the result of this aggregate in the final
   *                        output.
   */
  case class ComputedAggregate(
    unbound: AggregateExpression1,
    aggregate: AggregateExpression1,
    resultAttribute: AttributeReference)

  /** A list of aggregates that need to be computed for each group. */
  private[this] val computedAggregates = aggregateExpressions.flatMap { agg =>
    agg.collect {
      case a: AggregateExpression1 =>
        ComputedAggregate(
          a,
          BindReferences.bindReference(a, child.output),
          AttributeReference(s"aggResult:$a", a.dataType, a.nullable)())
    }
  }.toArray

  val order = aggregateExpressions.flatMap { agg =>
    agg.collect {
      case a: Expression =>
        BindReferences.bindReference(a, child.output).collect {
          case b: BoundReference => b.ordinal
        }
    }.flatMap(f => f)
  }.toArray

  /** The schema of the result of all aggregate evaluations */
  private[this] val computedSchema = computedAggregates.map(_.resultAttribute)

  /** Creates a new aggregate buffer for a group. */
  private[this] def newAggregateBuffer(): Array[AggregateFunction1] = {
    val buffer = new Array[AggregateFunction1](computedAggregates.length)
    var i = 0
    while (i < computedAggregates.length) {
      buffer(i) = computedAggregates(i).aggregate.newInstance()
      i += 1
    }
    buffer
  }

  /** Named attributes used to substitute grouping attributes into the final result. */
  private[this] val namedGroups = groupingExpressions.map {
    case ne: NamedExpression => ne -> ne.toAttribute
    case e => e -> Alias(e, s"groupingExpr:$e")().toAttribute
  }

  /**
   * A map of substitutions that are used to insert the aggregate expressions and grouping
   * expression into the final result expression.
   */
  private[this] val resultMap =
    (computedAggregates.map { agg => agg.unbound -> agg.resultAttribute } ++ namedGroups).toMap

  /**
   * Substituted version of aggregateExpressions expressions which are used to compute final
   * output rows given a group and the result of all aggregate computations.
   */
  private[this] val resultExpressions = aggregateExpressions.map { agg =>
    agg.transform {
      case e: Expression if resultMap.contains(e) => resultMap(e)
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    attachTree(this, "execute") {
      val numInputRows = longMetric("numInputRows")
      val numOutputRows = longMetric("numOutputRows")
      val pOrder = order.zipWithIndex.map {f =>
        if (f._2 > 0 && order(f._2-1) == f._1) {
          f._1 + 1
        } else {
          f._1
        }
      }
      if (groupingExpressions.isEmpty) {
        child.execute().mapPartitions { iter =>
          val buffer = newAggregateBuffer()
          var currentRow: CarbonRawMutableRow = null
          while (iter.hasNext) {
            currentRow = iter.next().asInstanceOf[CarbonRawMutableRow]
            while (currentRow.hasNext) {
              currentRow.next()
              numInputRows += 1
              var i = 0
              while (i < buffer.length) {
                buffer(i).update(currentRow)
                i += 1
              }
            }
          }
          val resultProjection = new InterpretedProjection(resultExpressions, computedSchema)
          val aggregateResults = new GenericMutableRow(computedAggregates.length)

          var i = 0
          while (i < buffer.length) {
            aggregateResults(i) = buffer(i).eval(EmptyRow)
            i += 1
          }

          numOutputRows += 1
          Iterator(resultProjection(aggregateResults))
        }
      } else {
        child.execute().mapPartitions { iter =>
          val hashTable = new HashMap[ByteArrayWrapper, Array[AggregateFunction1]](10000)
          var currentRow: CarbonRawMutableRow = null
          while (iter.hasNext) {
            currentRow = iter.next().asInstanceOf[CarbonRawMutableRow]
            while (currentRow.hasNext) {
              numInputRows += 1
              currentRow.next()
              val currentGroup = currentRow.getKey
              var currentBuffer = hashTable.get(currentGroup)
              if (currentBuffer == null) {
                currentBuffer = newAggregateBuffer()
                hashTable.put(currentGroup, currentBuffer)
              }

              var i = 0
              while (i < currentBuffer.length) {
                currentBuffer(i).update(currentRow)
                i += 1
              }
            }
          }

          new Iterator[InternalRow] {
            private[this] val hashTableIter = hashTable.entrySet().iterator()
            private[this] val aggregateResults = new Array[Any](computedAggregates.length)

            override final def hasNext: Boolean = hashTableIter.hasNext

            override final def next(): InternalRow = {
              val currentEntry = hashTableIter.next()
              val currentGroup = currentEntry.getKey
              val currentBuffer = currentEntry.getValue
              numOutputRows += 1
              var i = 0
              while (i < currentBuffer.length) {
                // Evaluating an aggregate buffer returns the result.  No row is required since we
                // already added all rows in the group using update.
                aggregateResults(i) = currentBuffer(i).eval(EmptyRow)
                i += 1
              }
              new GenericMutableRow(
                currentRow.parseKey(
                  currentGroup, aggregateResults.asInstanceOf[Array[Object]], pOrder).map(toType))
            }
          }
        }
      }
    }
  }

  def toType(obj: Any): Any = {
    obj match {
      case s: String => UTF8String.fromString(s)
      case _ => obj
    }
  }
}
