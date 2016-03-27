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

package org.carbondata.integration.spark.agg

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AggregateExpression1, AggregateFunction1, Alias, AttributeSet, BoundReference, Cast, Expression, LeafExpression, Literal, PartialAggregate1, SplitEvaluation, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import org.carbondata.query.aggregator.MeasureAggregator
import org.carbondata.query.aggregator.impl._

import scala.language.implicitConversions

case class CountCarbon(child: Expression) extends UnaryExpression with PartialAggregate1 {
  override def references = child.references

  override def nullable = false

  override def dataType = MeasureAggregatorUDT

  override def toString = s"COUNT($child)"

  override def asPartial: SplitEvaluation = {
    val partialCount = Alias(CountCarbon(child), "PartialCount")()
    SplitEvaluation(CountCarbonFinal(partialCount.toAttribute, LongType), partialCount :: Nil)
  }

  override def newInstance() = new CountFunctionCarbon(child, this, false)

  implicit def toAggregates(aggregate: MeasureAggregator): Double = aggregate.getDoubleValue()
}

case class CountCarbonFinal(child: Expression, origDataType: DataType) extends AggregateExpression1 {
  override def children = child :: Nil

  override def references = child.references

  override def nullable = false

  override def dataType = origDataType

  override def toString = s"COUNT($child)"

  override def newInstance() = new CountFunctionCarbon(child, this, true)
}


case class CountDistinctCarbon(child: Expression) extends PartialAggregate1 {
  override def children = child :: Nil

  override def nullable = false

  override def dataType = MeasureAggregatorUDT

  override def toString = s"COUNT(DISTINCT ($child)"

  override def asPartial = {
    val partialSet = Alias(CountDistinctCarbon(child), "partialSets")()
    SplitEvaluation(
      CountDistinctCarbonFinal(partialSet.toAttribute, LongType),
      partialSet :: Nil)
  }

  override def newInstance() = new CountDistinctFunctionCarbon(child, this)
}

case class CountDistinctCarbonFinal(inputSet: Expression, origDataType: DataType) extends AggregateExpression1 {
  override def children = inputSet :: Nil

  override def nullable = false

  override def dataType = origDataType

  override def toString = s"COUNTFINAL(DISTINCT ${inputSet}})"

  override def newInstance() = new CountDistinctFunctionCarbonFinal(inputSet, this)
}

case class AverageCarbon(child: Expression, castedDataType: DataType = null) extends UnaryExpression with PartialAggregate1 {
  override def references = child.references

  override def nullable = false

  override def dataType = MeasureAggregatorUDT

  override def toString = s"AVGMolap($child)"

  override def asPartial: SplitEvaluation = {
    val partialSum = Alias(AverageCarbon(child), "PartialAverage")()
    SplitEvaluation(
      AverageCarbonFinal(partialSum.toAttribute, if (child.dataType == IntegerType) DoubleType else child.dataType),
      partialSum :: Nil)
  }

  override def newInstance() = new AverageFunctionCarbon(child, this, false)
}

case class AverageCarbonFinal(child: Expression, origDataType: DataType) extends AggregateExpression1 {
  override def children = child :: Nil

  override def references = child.references

  override def nullable = false

  override def dataType = origDataType

  override def toString = s"AVG($child)"

  override def newInstance() = new AverageFunctionCarbon(child, this, true)
}

case class SumCarbon(child: Expression, castedDataType: DataType = null) extends UnaryExpression with PartialAggregate1 {
  override def references = child.references

  override def nullable = false

  override def dataType = MeasureAggregatorUDT

  override def toString = s"SUMMolap($child)"

  override def asPartial: SplitEvaluation = {
    val partialSum = Alias(SumCarbon(child), "PartialSum")()
    SplitEvaluation(
      SumCarbonFinal(partialSum.toAttribute, if (castedDataType != null) castedDataType else child.dataType),
      partialSum :: Nil)
  }

  override def newInstance() = new SumFunctionCarbon(child, this, false)

  implicit def toAggregates(aggregate: MeasureAggregator): Double = aggregate.getDoubleValue()
}

case class SumCarbonFinal(child: Expression, origDataType: DataType) extends AggregateExpression1 {
  override def children = child :: Nil

  override def references = child.references

  override def nullable = false

  override def dataType = origDataType

  override def toString = s"SUMMolapFinal($child)"

  override def newInstance() = new SumFunctionCarbon(child, this, true)
}

case class MaxCarbon(child: Expression, castedDataType: DataType = null) extends UnaryExpression with PartialAggregate1 {
  override def references = child.references

  override def nullable = false

  override def dataType = MeasureAggregatorUDT

  override def toString = s"MaxCarbon($child)"

  //to do partialSum to PartialMax many places
  override def asPartial: SplitEvaluation = {
    val partialSum = Alias(MaxCarbon(child), "PartialMax")()
    SplitEvaluation(
      MaxCarbonFinal(partialSum.toAttribute, if (castedDataType != null) castedDataType else child.dataType),
      partialSum :: Nil)
  }

  override def newInstance() = new MaxFunctionCarbon(child, this, false)
}

case class MaxCarbonFinal(child: Expression, origDataType: DataType) extends AggregateExpression1 {
  override def children = child :: Nil

  override def references = child.references

  override def nullable = false

  override def dataType = origDataType

  override def toString = s"MaxCarbonFinal($child)"

  override def newInstance() = new MaxFunctionCarbon(child, this, true)
}

case class MinCarbon(child: Expression, castedDataType: DataType = null) extends UnaryExpression with PartialAggregate1 {
  override def references = child.references

  override def nullable = false

  override def dataType = MeasureAggregatorUDT

  override def toString = s"MinCarbon($child)"

  override def asPartial: SplitEvaluation = {
    val partialSum = Alias(MinCarbon(child), "PartialMin")()
    SplitEvaluation(
      MinCarbonFinal(partialSum.toAttribute, if (castedDataType != null) castedDataType else child.dataType),
      partialSum :: Nil)
  }

  override def newInstance() = new MinFunctionCarbon(child, this, false)
}

case class MinCarbonFinal(child: Expression, origDataType: DataType) extends AggregateExpression1 {
  override def children = child :: Nil

  override def references = child.references

  override def nullable = false

  override def dataType = origDataType

  override def toString = s"MinCarbonFinal($child)"

  override def newInstance() = new MinFunctionCarbon(child, this, true)
}

case class SumDistinctCarbon(child: Expression, castedDataType: DataType = null)
  extends UnaryExpression with PartialAggregate1 {

  override def references = child.references

  override def nullable = false

  override def dataType = MeasureAggregatorUDT

  override def toString = s"PARTIAL_SUM_DISTINCT($child)"

  override def asPartial: SplitEvaluation = {
    val partialSum = Alias(SumDistinctCarbon(child), "PartialSumDistinct")()
    SplitEvaluation(
      SumDistinctFinalCarbon(partialSum.toAttribute, if (castedDataType != null) castedDataType else child.dataType),
      partialSum :: Nil)
  }

  override def newInstance() = new SumDisctinctFunctionCarbon(child, this, false)
}

case class SumDistinctFinalCarbon(child: Expression, origDataType: DataType)
  extends AggregateExpression1 {
  override def children = child :: Nil

  override def references = child.references

  override def nullable = false

  override def dataType = origDataType

  override def toString = s"FINAL_SUM_DISTINCT($child)"

  override def newInstance() = new SumDisctinctFunctionCarbon(child, this, true)
}

case class FirstCarbon(child: Expression, origDataType: DataType = MeasureAggregatorUDT) extends UnaryExpression with PartialAggregate1 {
  override def references = child.references

  override def nullable = child.nullable

  override def dataType = MeasureAggregatorUDT

  override def toString = s"FIRST($child)"

  override def asPartial: SplitEvaluation = {
    val partialFirst = Alias(FirstCarbon(child), "PartialFirst")()
    SplitEvaluation(
      FirstCarbon(partialFirst.toAttribute, child.dataType),
      partialFirst :: Nil)
  }

  override def newInstance() = new FirstFunctionCarbon(child, this)
}

case class AverageFunctionCarbon(expr: Expression, base: AggregateExpression1, finalAgg: Boolean)
  extends AggregateFunction1 {

  def this() = this(null, null, false) // Required for serialization.

  //  var count: Int = _
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
        var dc: MeasureAggregator = null
        if(s != null)
        {
          if (s.isInstanceOf[java.math.BigDecimal])
          {
            dc = new AvgBigDecimalAggregator
            dc.agg(new java.math.BigDecimal(s.toString))
            dc.setNewValue(new java.math.BigDecimal(s.toString))
          }
          else if (s.isInstanceOf[Long])
          {
            dc = new AvgLongAggregator
            dc.agg(s.toString.toLong)
            dc.setNewValue(s.toString.toLong)
          }
          else
          {
            dc = new AvgDoubleAggregator
            dc.agg(s.toString.toDouble)
            dc.setNewValue(s.toString.toDouble)
          }
        }
        else
        {
          dc = new AvgDoubleAggregator()
        }
        dc
      }
    }
    if (avg == null) avg = agg else avg.merge(agg)
  }

  override def eval(input: InternalRow): Any =
    if (finalAgg)
      if (avg.isFirstTime())
        null
      else
      {
        if (avg.isInstanceOf[AvgBigDecimalAggregator]) {
          Cast(Literal(avg.getBigDecimalValue), base.dataType).eval(null)
        }
        else if (avg.isInstanceOf[AvgLongAggregator]) {
          Cast(Literal(avg.getLongValue), base.dataType).eval(null)
    }
        else {
          Cast(Literal(avg.getDoubleValue), base.dataType).eval(null)
        }
      }
    else avg
}

case class CountFunctionCarbon(expr: Expression, base: AggregateExpression1, finalAgg: Boolean) extends AggregateFunction1 {
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
          agg1.agg(0)
          //agg1.setNewValue(others.toString.toDouble)
        }
        agg1
    }
    if (count == null) count = agg else count.merge(agg)
  }

  override def eval(input: InternalRow): Any = if (finalAgg && count != null) if (count.isFirstTime()) 0L else Cast(Literal(count.getDoubleValue), base.dataType).eval(null) else count

  //override def eval(input: Row): Any = if(finalAgg) if(count.isFirstTime()) 0 else count.getValue.toLong else count
}


case class SumFunctionCarbon(expr: Expression, base: AggregateExpression1, finalAgg: Boolean) extends AggregateFunction1 {
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
        var dc: MeasureAggregator = null
        if(s != null)
        {
          if (s.isInstanceOf[java.math.BigDecimal])
          {
            dc = new SumBigDecimalAggregator
            dc.agg(new java.math.BigDecimal(s.toString))
            dc.setNewValue(new java.math.BigDecimal(s.toString))
          }
          else if (s.isInstanceOf[Long])
          {
            dc = new SumLongAggregator
            dc.agg(s.toString.toLong)
            dc.setNewValue(s.toString.toLong)
          }
          else
          {
            dc = new SumDoubleAggregator
            dc.agg(s.toString.toDouble)
            dc.setNewValue(s.toString.toDouble)
          }
        }
        else
        {
          dc = new SumDoubleAggregator
        }
        dc
      }
    }
    if (sum == null) sum = agg else sum.merge(agg)
  }

  override def eval(input: InternalRow): Any =
    if (finalAgg && sum != null)
      if (sum.isFirstTime())
        null
      else {
        if (sum.isInstanceOf[SumBigDecimalAggregator]) {
          Cast(Literal(sum.getBigDecimalValue), base.dataType).eval(input)
        }
        else if (sum.isInstanceOf[SumLongAggregator]) {
          Cast(Literal(sum.getLongValue), base.dataType).eval(input)
        }
        else {
          Cast(Literal(sum.getDoubleValue), base.dataType).eval(input)
        }
      }

    else sum
}

case class MaxFunctionCarbon(expr: Expression, base: AggregateExpression1, finalAgg: Boolean) extends AggregateFunction1 {
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
          dc.agg(s.toString.toDouble);dc.setNewValue(s.toString.toDouble)
        };dc
      }
    }
    if (max == null) max = agg else max.merge(agg)
  }

  override def eval(input: InternalRow): Any = if (finalAgg && max != null) if (max.isFirstTime()) null else Cast(Literal(max.getValueObject), base.dataType).eval(null) else max //.eval(null)
}

case class MinFunctionCarbon(expr: Expression, base: AggregateExpression1, finalAgg: Boolean) extends AggregateFunction1 {
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
        val dc = new MinAggregator; if (s != null) {
          dc.agg(s.toString.toDouble);dc.setNewValue(s.toString.toDouble)
        };dc
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

case class SumDisctinctFunctionCarbon(expr: Expression, base: AggregateExpression1, isFinal: Boolean)
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
        var dc: MeasureAggregator = null
        if (s.isInstanceOf[Double])
        {
          dc = new SumDistinctDoubleAggregator
          dc.setNewValue(s.toString.toDouble)
        }
        else if (s.isInstanceOf[Int])
        {
          dc = new SumDistinctLongAggregator
          dc.setNewValue(s.toString.toLong)
        }
        else if (s.isInstanceOf[java.math.BigDecimal])
        {
          dc = new SumDistinctBigDecimalAggregator
          dc.setNewValue(new java.math.BigDecimal(s.toString))
        }
        dc
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

case class CountDistinctFunctionCarbon(expr: Expression, base: AggregateExpression1)
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

case class CountDistinctFunctionCarbonFinal(expr: Expression, base: AggregateExpression1)
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
      Cast(Literal(count.getDoubleValue), base.dataType).eval(null)
}

case class FirstFunctionCarbon(expr: Expression, base: AggregateExpression1) extends AggregateFunction1 {
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
      case d: MeasureAggregator => d.getDoubleValue()
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
        d.setNewValue(d.getDoubleValue())
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

