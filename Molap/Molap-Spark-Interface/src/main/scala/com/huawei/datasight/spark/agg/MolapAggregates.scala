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

package com.huawei.datasight.spark.agg

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator
import com.huawei.unibi.molap.engine.aggregator.impl._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AggregateExpression1, AggregateFunction1, Alias, AttributeSet, BoundReference, Cast, Expression, LeafExpression, Literal, PartialAggregate1, SplitEvaluation, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

import scala.language.implicitConversions

case class CountMolap(child: Expression) extends UnaryExpression with PartialAggregate1 {
  override def references = child.references

  override def nullable = false

  override def dataType = MeasureAggregatorUDT

  override def toString = s"COUNT($child)"

  override def asPartial: SplitEvaluation = {
    val partialCount = Alias(CountMolap(child), "PartialCount")()
    SplitEvaluation(CountMolapFinal(partialCount.toAttribute, LongType), partialCount :: Nil)
  }

  override def newInstance() = new CountFunctionMolap(child, this, false)

  implicit def toAggregates(aggregate: MeasureAggregator): Double = aggregate.getValue()
}

case class CountMolapFinal(child: Expression, origDataType: DataType) extends AggregateExpression1 {
  override def children = child :: Nil

  override def references = child.references

  override def nullable = false

  override def dataType = origDataType

  override def toString = s"COUNT($child)"

  override def newInstance() = new CountFunctionMolap(child, this, true)
}


case class CountDistinctMolap(child: Expression) extends PartialAggregate1 {
  override def children = child :: Nil

  override def nullable = false

  override def dataType = MeasureAggregatorUDT

  override def toString = s"COUNT(DISTINCT ($child)"

  override def asPartial = {
    val partialSet = Alias(CountDistinctMolap(child), "partialSets")()
    SplitEvaluation(
      CountDistinctMolapFinal(partialSet.toAttribute, LongType),
      partialSet :: Nil)
  }

  override def newInstance() = new CountDistinctFunctionMolap(child, this)
}

case class CountDistinctMolapFinal(inputSet: Expression, origDataType: DataType) extends AggregateExpression1 {
  override def children = inputSet :: Nil

  override def nullable = false

  override def dataType = origDataType

  override def toString = s"COUNTFINAL(DISTINCT ${inputSet}})"

  override def newInstance() = new CountDistinctFunctionMolapFinal(inputSet, this)
}

case class AverageMolap(child: Expression, castedDataType: DataType = null) extends UnaryExpression with PartialAggregate1 {
  override def references = child.references

  override def nullable = false

  override def dataType = MeasureAggregatorUDT

  override def toString = s"AVGMolap($child)"

  override def asPartial: SplitEvaluation = {
    val partialSum = Alias(AverageMolap(child), "PartialAverage")()
    SplitEvaluation(
      AverageMolapFinal(partialSum.toAttribute, DoubleType),
      partialSum :: Nil)
  }

  override def newInstance() = new AverageFunctionMolap(child, this, false)
}

case class AverageMolapFinal(child: Expression, origDataType: DataType) extends AggregateExpression1 {
  override def children = child :: Nil

  override def references = child.references

  override def nullable = false

  override def dataType = DoubleType

  override def toString = s"AVG($child)"

  override def newInstance() = new AverageFunctionMolap(child, this, true)
}

case class SumMolap(child: Expression, castedDataType: DataType = null) extends UnaryExpression with PartialAggregate1 {
  override def references = child.references

  override def nullable = false

  override def dataType = MeasureAggregatorUDT

  override def toString = s"SUMMolap($child)"

  override def asPartial: SplitEvaluation = {
    val partialSum = Alias(SumMolap(child), "PartialSum")()
    SplitEvaluation(
      SumMolapFinal(partialSum.toAttribute, if (castedDataType != null) castedDataType else child.dataType),
      partialSum :: Nil)
  }

  override def newInstance() = new SumFunctionMolap(child, this, false)

  implicit def toAggregates(aggregate: MeasureAggregator): Double = aggregate.getValue()
}

case class SumMolapFinal(child: Expression, origDataType: DataType) extends AggregateExpression1 {
  override def children = child :: Nil

  override def references = child.references

  override def nullable = false

  override def dataType = origDataType

  override def toString = s"SUMMolapFinal($child)"

  override def newInstance() = new SumFunctionMolap(child, this, true)
}

case class MaxMolap(child: Expression, castedDataType: DataType = null) extends UnaryExpression with PartialAggregate1 {
  override def references = child.references

  override def nullable = false

  override def dataType = MeasureAggregatorUDT

  override def toString = s"MaxMolap($child)"

  //to do partialSum to PartialMax many places
  override def asPartial: SplitEvaluation = {
    val partialSum = Alias(MaxMolap(child), "PartialMax")()
    SplitEvaluation(
      MaxMolapFinal(partialSum.toAttribute, if (castedDataType != null) castedDataType else child.dataType),
      partialSum :: Nil)
  }

  override def newInstance() = new MaxFunctionMolap(child, this, false)
}

case class MaxMolapFinal(child: Expression, origDataType: DataType) extends AggregateExpression1 {
  override def children = child :: Nil

  override def references = child.references

  override def nullable = false

  override def dataType = origDataType

  override def toString = s"MaxMolapFinal($child)"

  override def newInstance() = new MaxFunctionMolap(child, this, true)
}

case class MinMolap(child: Expression, castedDataType: DataType = null) extends UnaryExpression with PartialAggregate1 {
  override def references = child.references

  override def nullable = false

  override def dataType = MeasureAggregatorUDT

  override def toString = s"MinMolap($child)"

  override def asPartial: SplitEvaluation = {
    val partialSum = Alias(MinMolap(child), "PartialMin")()
    SplitEvaluation(
      MinMolapFinal(partialSum.toAttribute, if (castedDataType != null) castedDataType else child.dataType),
      partialSum :: Nil)
  }

  override def newInstance() = new MinFunctionMolap(child, this, false)
}

case class MinMolapFinal(child: Expression, origDataType: DataType) extends AggregateExpression1 {
  override def children = child :: Nil

  override def references = child.references

  override def nullable = false

  override def dataType = origDataType

  override def toString = s"MinMolapFinal($child)"

  override def newInstance() = new MinFunctionMolap(child, this, true)
}

case class SumDistinctMolap(child: Expression, castedDataType: DataType = null)
  extends UnaryExpression with PartialAggregate1 {

  override def references = child.references

  override def nullable = false

  override def dataType = MeasureAggregatorUDT

  override def toString = s"PARTIAL_SUM_DISTINCT($child)"

  override def asPartial: SplitEvaluation = {
    val partialSum = Alias(SumDistinctMolap(child), "PartialSumDistinct")()
    SplitEvaluation(
      SumDistinctFinalMolap(partialSum.toAttribute, if (castedDataType != null) castedDataType else child.dataType),
      partialSum :: Nil)
  }

  override def newInstance() = new SumDisctinctFunctionMolap(child, this, false)
}

case class SumDistinctFinalMolap(child: Expression, origDataType: DataType)
  extends AggregateExpression1 {
  override def children = child :: Nil

  override def references = child.references

  override def nullable = false

  override def dataType = origDataType

  override def toString = s"FINAL_SUM_DISTINCT($child)"

  override def newInstance() = new SumDisctinctFunctionMolap(child, this, true)
}

case class FirstMolap(child: Expression, origDataType: DataType = MeasureAggregatorUDT) extends UnaryExpression with PartialAggregate1 {
  override def references = child.references

  override def nullable = child.nullable

  override def dataType = MeasureAggregatorUDT

  override def toString = s"FIRST($child)"

  override def asPartial: SplitEvaluation = {
    val partialFirst = Alias(FirstMolap(child), "PartialFirst")()
    SplitEvaluation(
      FirstMolap(partialFirst.toAttribute, child.dataType),
      partialFirst :: Nil)
  }

  override def newInstance() = new FirstFunctionMolap(child, this)
}

case class AverageFunctionMolap(expr: Expression, base: AggregateExpression1, finalAgg: Boolean)
  extends AggregateFunction1 {

  def this() = this(null, null, false) // Required for serialization.

  private var avg: MeasureAggregator = null

  override def update(input: InternalRow): Unit = {
    val br = expr.collectFirst({ case a@BoundReference(_, _, _) => a })
    val resolution =
      if (br.isDefined) {
        input.get(br.get.ordinal, MeasureAggregatorUDT)
      } else {
        expr.eval(input)
      }
    val agg = resolution match {
      case s: MeasureAggregator => s
      case s => {
        val dc = new AvgAggregator; if (s != null) {
          dc.agg(s.toString.toDouble, null, 0, 0); dc.setNewValue(s.toString.toDouble)
        }; dc
      }
    }
    if (avg == null) avg = agg else avg.merge(agg)
  }

  override def eval(input: InternalRow): Any =
    if (finalAgg) {
      if (avg.isFirstTime()) null
      else Cast(Literal(avg.getValue), base.dataType).eval(null)
    } else {
      avg
    }
}

case class CountFunctionMolap(expr: Expression, base: AggregateExpression1, finalAgg: Boolean) extends AggregateFunction1 {
  def this() = this(null, null, false) // Required for serialization.

  private var count: MeasureAggregator = null

  override def update(input: InternalRow): Unit = {
    val br = expr.collectFirst({ case a@BoundReference(_, _, _) => a })
    val resolution =
      if (br.isDefined) {
        input.get(br.get.ordinal, MeasureAggregatorUDT)
      } else {
        expr.eval(input)
      }
    val agg = resolution match {
      case m: MeasureAggregator => m
      case others =>
        val agg1: MeasureAggregator = new CountAggregator
        if (others != null) {
          agg1.agg(0, null, 0, 0)
        }
        agg1
    }
    if (count == null) count = agg else count.merge(agg)
  }

  override def eval(input: InternalRow): Any = if (finalAgg && count != null) if (count.isFirstTime()) 0L else Cast(Literal(count.getValue), base.dataType).eval(null) else count
}


case class SumFunctionMolap(expr: Expression, base: AggregateExpression1, finalAgg: Boolean) extends AggregateFunction1 {
  def this() = this(null, null, false) // Required for serialization.

  private var sum: MeasureAggregator = null

  override def update(input: InternalRow): Unit = {
    val br = expr.collectFirst({ case a@BoundReference(_, _, _) => a })
    val resolution =
      if (br.isDefined) {
        input.get(br.get.ordinal, MeasureAggregatorUDT)
      } else {
        expr.eval(input)
      }
    val agg = resolution match {
      case s: MeasureAggregator => s
      case s => {
        val dc = new SumAggregator; if (s != null) {
          dc.agg(s.toString.toDouble, null, 0, 0); dc.setNewValue(s.toString.toDouble)
        }; dc
      }
    }
    if (sum == null) sum = agg else sum.merge(agg)
  }

  override def eval(input: InternalRow): Any =
    if (finalAgg && sum != null)
      if (sum.isFirstTime())
        null
      else Cast(Literal(sum.getValue), base.dataType).eval(input)

    else sum
}

case class MaxFunctionMolap(expr: Expression, base: AggregateExpression1, finalAgg: Boolean) extends AggregateFunction1 {
  def this() = this(null, null, false) // Required for serialization.

  private var max: MeasureAggregator = null

  override def update(input: InternalRow): Unit = {
    val br = expr.collectFirst({ case a@BoundReference(_, _, _) => a })
    val resolution =
      if (br.isDefined) {
        input.get(br.get.ordinal, MeasureAggregatorUDT)
      } else {
        expr.eval(input)
      }
    val agg = resolution match {
      case s: MeasureAggregator => s
      case s => {
        val dc = new MaxAggregator; if (s != null) {
          dc.agg(s.toString.toDouble, null, 0, 0); dc.setNewValue(s.toString.toDouble)
        }; dc
      }
    }
    if (max == null) max = agg else max.merge(agg)
  }

  override def eval(input: InternalRow): Any = if (finalAgg && max != null) if (max.isFirstTime()) null else Cast(Literal(max.getValueObject), base.dataType).eval(null) else max //.eval(null)
}

case class MinFunctionMolap(expr: Expression, base: AggregateExpression1, finalAgg: Boolean) extends AggregateFunction1 {
  def this() = this(null, null, false) // Required for serialization.

  private var min: MeasureAggregator = null

  override def update(input: InternalRow): Unit = {
    val br = expr.collectFirst({ case a@BoundReference(_, _, _) => a })
    val resolution =
      if (br.isDefined) {
        input.get(br.get.ordinal, MeasureAggregatorUDT)
      } else {
        expr.eval(input)
      }
    val agg = resolution match {
      case s: MeasureAggregator => s
      case s => {
        val dc = new MinAggregator;
        if (s != null) {
          dc.agg(s.toString.toDouble, null, 0, 0);
          dc.setNewValue(s.toString.toDouble)
        };
        dc
      }
    }
    if (min == null) min = agg else min.merge(agg)
  }

  override def eval(input: InternalRow): Any = {
    if (finalAgg && min != null) {
      if (min.isFirstTime()) null
      else Cast(Literal(min.getValueObject), base.dataType).eval(null)
    } else {
      min
    }
  }
}

case class SumDisctinctFunctionMolap(expr: Expression, base: AggregateExpression1, isFinal: Boolean)
  extends AggregateFunction1 {

  def this() = this(null, null, false) // Required for serialization.

  private var distinct: MeasureAggregator = null

  override def update(input: InternalRow): Unit = {

    val br = expr.collectFirst({ case a@BoundReference(_, _, _) => a })
    val resolution =
      if (br.isDefined) {
        input.get(br.get.ordinal, MeasureAggregatorUDT)
      } else {
        expr.eval(input)
      }
    val agg = resolution match {
      case s: MeasureAggregator => s
      case null => null
      case s => {
        val dc = new SumDistinctAggregator; dc.setNewValue(s.toString.toDouble); dc
      }
    }
    if (agg == null) distinct
    else if (distinct == null) distinct = agg else distinct.merge(agg)
  }

  override def eval(input: InternalRow): Any =
    if (isFinal && distinct != null) // in case of empty load it was failing so added null check.
    {
      Cast(Literal(distinct.getValueObject), base.dataType).eval(null)
    }
    else
      distinct
}

case class CountDistinctFunctionMolap(expr: Expression, base: AggregateExpression1)
  extends AggregateFunction1 {

  def this() = this(null, null) // Required for serialization.

  private var count: MeasureAggregator = null

  override def update(input: InternalRow): Unit = {
    val br = expr.collectFirst({ case a@BoundReference(_, _, _) => a })
    val resolution =
      if (br.isDefined) {
        input.get(br.get.ordinal, MeasureAggregatorUDT)
      } else {
        expr.eval(input)
      }
    val agg = resolution match {
      case s: MeasureAggregator => s
      case null => null
      case s => {
        val dc = new DistinctCountAggregatorObjectSet; dc.setNewValue(s.toString); dc
      }
    }
    if (agg == null) count
    else if (count == null) count = agg else count.merge(agg)
  }

  override def eval(input: InternalRow): Any = count
}

case class CountDistinctFunctionMolapFinal(expr: Expression, base: AggregateExpression1)
  extends AggregateFunction1 {

  def this() = this(null, null) // Required for serialization.

  private var count: MeasureAggregator = null

  override def update(input: InternalRow): Unit = {
    val br = expr.collectFirst({ case a@BoundReference(_, _, _) => a })
    val resolution =
      if (br.isDefined) {
        input.get(br.get.ordinal, MeasureAggregatorUDT)
      } else {
        expr.eval(input)
      }
    val agg = resolution match {
      case s: MeasureAggregator => s
      case null => null
      case s => {
        val dc = new DistinctCountAggregatorObjectSet; dc.setNewValue(s.toString); dc
      }
    }
    if (agg == null) count
    else if (count == null) count = agg else count.merge(agg)
  }

  override def eval(input: InternalRow): Any =
    if (count == null)
      Cast(Literal(0), base.dataType).eval(null)
    else if (count.isFirstTime())
      Cast(Literal(0), base.dataType).eval(null)
    else
      Cast(Literal(count.getValue), base.dataType).eval(null)
}

case class FirstFunctionMolap(expr: Expression, base: AggregateExpression1) extends AggregateFunction1 {
  def this() = this(null, null) // Required for serialization.

  var result: Any = null

  override def update(input: InternalRow): Unit = {
    if (result == null) {
      val br = expr.collectFirst({ case a@BoundReference(_, _, _) => a })
      val resolution =
        if (br.isDefined) {
          input.get(br.get.ordinal, MeasureAggregatorUDT)
        } else {
          expr.eval(input)
        }

      result = resolution
    }
  }

  override def eval(input: InternalRow): Any = Cast(Literal(result), base.dataType).eval(null)
}

case class FlattenExpr(expr: Expression) extends Expression with CodegenFallback {
  self: Product =>

  override def children = Seq(expr)

  override def dataType = expr.dataType

  override def nullable = expr.nullable

  override def references = AttributeSet(expr.flatMap(_.references.iterator))

  override def foldable = expr.foldable

  override def toString = "Flatten(" + expr.toString + ")"

  type EvaluatedType = Any

  override def eval(input: InternalRow): Any = {
    expr.eval(input) match {
      case d: MeasureAggregator => d.getValue()
      case others => others
    }
  }
}

case class FlatAggregatorsExpr(expr: Expression) extends Expression with CodegenFallback {
  self: Product =>

  override def children = Seq(expr)

  override def dataType = expr.dataType

  override def nullable = expr.nullable

  override def references = AttributeSet(expr.flatMap(_.references.iterator))

  override def foldable = expr.foldable

  override def toString = "FlattenAggregators(" + expr.toString + ")"

  type EvaluatedType = Any

  override def eval(input: InternalRow): Any = {
    expr.eval(input) match {
      case d: MeasureAggregator => {
        d.setNewValue(d.getValue())
        d
      }
      case others => others
    }
  }
}

case class PositionLiteral(expr: Expression, intermediateDataType: DataType) extends LeafExpression with CodegenFallback {
  override def dataType = expr.dataType

  override def nullable = false

  type EvaluatedType = Any
  var position = -1;

  def setPosition(pos: Int) = position = pos

  override def toString: String = s"PositionLiteral($position : $expr)";

  override def eval(input: InternalRow): Any = {
    if (position != -1)
      input.get(position, intermediateDataType)
    else
      expr.eval(input)
  }
}

