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

package org.carbondata.spark.agg

import scala.language.implicitConversions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

import org.carbondata.query.aggregator.MeasureAggregator
import org.carbondata.query.aggregator.impl._

case class CountCarbon(child: Expression) extends UnaryExpression with PartialAggregate1 {
  override def references: AttributeSet = child.references

  override def nullable: Boolean = false

  override def dataType: DataType = MeasureAggregatorUDT

  override def toString: String = s"COUNT($child)"

  override def asPartial: SplitEvaluation = {
    val partialCount = Alias(CountCarbon(child), "PartialCount")()
    SplitEvaluation(CountCarbonFinal(partialCount.toAttribute, LongType), partialCount :: Nil)
  }

  override def newInstance(): AggregateFunction1 = new CountFunctionCarbon(child, this, false)

  implicit def toAggregates(aggregate: MeasureAggregator): Double = aggregate.getDoubleValue()
}

case class CountCarbonFinal(child: Expression, origDataType: DataType)
  extends AggregateExpression1 {

  override def children: Seq[Expression] = child :: Nil

  override def references: AttributeSet = child.references

  override def nullable: Boolean = false

  override def dataType: DataType = origDataType

  override def toString: String = s"COUNT($child)"

  override def newInstance(): AggregateFunction1 = new CountFunctionCarbon(child, this, true)
}


case class CountDistinctCarbon(child: Expression) extends PartialAggregate1 {
  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = false

  override def dataType: DataType = MeasureAggregatorUDT

  override def toString: String = s"COUNT(DISTINCT ($child)"

  override def asPartial: SplitEvaluation = {
    val partialSet = Alias(CountDistinctCarbon(child), "partialSets")()
    SplitEvaluation(
      CountDistinctCarbonFinal(partialSet.toAttribute, LongType),
      partialSet :: Nil)
  }

  override def newInstance(): AggregateFunction1 = new CountDistinctFunctionCarbon(child, this)
}

case class CountDistinctCarbonFinal(inputSet: Expression, origDataType: DataType)
  extends AggregateExpression1 {
  override def children: Seq[Expression] = inputSet :: Nil

  override def nullable: Boolean = false

  override def dataType: DataType = origDataType

  override def toString: String = s"COUNTFINAL(DISTINCT ${ inputSet }})"

  override def newInstance(): AggregateFunction1 = {
    new CountDistinctFunctionCarbonFinal(inputSet, this)
  }
}

case class AverageCarbon(child: Expression, castedDataType: DataType = null)
  extends UnaryExpression with PartialAggregate1 {
  override def references: AttributeSet = child.references

  override def nullable: Boolean = false

  override def dataType: DataType = MeasureAggregatorUDT

  override def toString: String = s"AVGCarbon($child)"

  override def asPartial: SplitEvaluation = {
    val partialSum = Alias(AverageCarbon(child), "PartialAverage")()
    SplitEvaluation(
      AverageCarbonFinal(partialSum.toAttribute,
        if (child.dataType == IntegerType) DoubleType else child.dataType),
      partialSum :: Nil)
  }

  override def newInstance(): AggregateFunction1 = new AverageFunctionCarbon(child, this, false)
}

case class AverageCarbonFinal(child: Expression, origDataType: DataType)
  extends AggregateExpression1 {
  override def children: Seq[Expression] = child :: Nil

  override def references: AttributeSet = child.references

  override def nullable: Boolean = false

  override def dataType: DataType = origDataType

  override def toString: String = s"AVG($child)"

  override def newInstance(): AggregateFunction1 = new AverageFunctionCarbon(child, this, true)
}

case class SumCarbon(child: Expression, castedDataType: DataType = null)
  extends UnaryExpression with PartialAggregate1 {
  override def references: AttributeSet = child.references

  override def nullable: Boolean = false

  override def dataType: DataType = MeasureAggregatorUDT

  override def toString: String = s"SUMCarbon($child)"

  override def asPartial: SplitEvaluation = {
    val partialSum = Alias(SumCarbon(child), "PartialSum")()
    SplitEvaluation(
      SumCarbonFinal(partialSum.toAttribute,
        if (castedDataType != null) castedDataType else child.dataType),
      partialSum :: Nil)
  }

  override def newInstance(): AggregateFunction1 = new SumFunctionCarbon(child, this, false)

  implicit def toAggregates(aggregate: MeasureAggregator): Double = aggregate.getDoubleValue()
}

case class SumCarbonFinal(child: Expression, origDataType: DataType) extends AggregateExpression1 {
  override def children: Seq[Expression] = child :: Nil

  override def references: AttributeSet = child.references

  override def nullable: Boolean = false

  override def dataType: DataType = origDataType

  override def toString: String = s"SUMCarbonFinal($child)"

  override def newInstance(): AggregateFunction1 = new SumFunctionCarbon(child, this, true)
}

case class MaxCarbon(child: Expression, castedDataType: DataType = null)
  extends UnaryExpression with PartialAggregate1 {
  override def references: AttributeSet = child.references

  override def nullable: Boolean = false

  override def dataType: DataType = MeasureAggregatorUDT

  override def toString: String = s"MaxCarbon($child)"

  // to do partialSum to PartialMax many places
  override def asPartial: SplitEvaluation = {
    val partialSum = Alias(MaxCarbon(child), "PartialMax")()
    SplitEvaluation(
      MaxCarbonFinal(partialSum.toAttribute,
        if (castedDataType != null) castedDataType else child.dataType),
      partialSum :: Nil)
  }

  override def newInstance(): AggregateFunction1 = new MaxFunctionCarbon(child, this, false)
}

case class MaxCarbonFinal(child: Expression, origDataType: DataType) extends AggregateExpression1 {
  override def children: Seq[Expression] = child :: Nil

  override def references: AttributeSet = child.references

  override def nullable: Boolean = false

  override def dataType: DataType = origDataType

  override def toString: String = s"MaxCarbonFinal($child)"

  override def newInstance(): AggregateFunction1 = new MaxFunctionCarbon(child, this, true)
}

case class MinCarbon(child: Expression, castedDataType: DataType = null)
  extends UnaryExpression with PartialAggregate1 {
  override def references: AttributeSet = child.references

  override def nullable: Boolean = false

  override def dataType: DataType = MeasureAggregatorUDT

  override def toString: String = s"MinCarbon($child)"

  override def asPartial: SplitEvaluation = {
    val partialSum = Alias(MinCarbon(child), "PartialMin")()
    SplitEvaluation(
      MinCarbonFinal(partialSum.toAttribute,
        if (castedDataType != null) castedDataType else child.dataType),
      partialSum :: Nil)
  }

  override def newInstance(): AggregateFunction1 = new MinFunctionCarbon(child, this, false)
}

case class MinCarbonFinal(child: Expression, origDataType: DataType) extends AggregateExpression1 {
  override def children: Seq[Expression] = child :: Nil

  override def references: AttributeSet = child.references

  override def nullable: Boolean = false

  override def dataType: DataType = origDataType

  override def toString: String = s"MinCarbonFinal($child)"

  override def newInstance(): AggregateFunction1 = new MinFunctionCarbon(child, this, true)
}

case class SumDistinctCarbon(child: Expression, castedDataType: DataType = null)
  extends UnaryExpression with PartialAggregate1 {

  override def references: AttributeSet = child.references

  override def nullable: Boolean = false

  override def dataType: DataType = MeasureAggregatorUDT

  override def toString: String = s"PARTIAL_SUM_DISTINCT($child)"

  override def asPartial: SplitEvaluation = {
    val partialSum = Alias(SumDistinctCarbon(child), "PartialSumDistinct")()
    SplitEvaluation(
      SumDistinctFinalCarbon(partialSum.toAttribute,
        if (castedDataType != null) {
          castedDataType
        } else {
          child.dataType
        }),
      partialSum :: Nil)
  }

  override def newInstance(): AggregateFunction1 = {
    new SumDisctinctFunctionCarbon(child, this, false)
  }
}

case class SumDistinctFinalCarbon(child: Expression, origDataType: DataType)
  extends AggregateExpression1 {
  override def children: Seq[Expression] = child :: Nil

  override def references: AttributeSet = child.references

  override def nullable: Boolean = false

  override def dataType: DataType = origDataType

  override def toString: String = s"FINAL_SUM_DISTINCT($child)"

  override def newInstance(): AggregateFunction1 = new SumDisctinctFunctionCarbon(child, this, true)
}

case class FirstCarbon(child: Expression, origDataType: DataType = MeasureAggregatorUDT)
  extends UnaryExpression with PartialAggregate1 {
  override def references: AttributeSet = child.references

  override def nullable: Boolean = child.nullable

  override def dataType: DataType = MeasureAggregatorUDT

  override def toString: String = s"FIRST($child)"

  override def asPartial: SplitEvaluation = {
    val partialFirst = Alias(FirstCarbon(child), "PartialFirst")()
    SplitEvaluation(
      FirstCarbon(partialFirst.toAttribute, child.dataType),
      partialFirst :: Nil)
  }

  override def newInstance(): AggregateFunction1 = new FirstFunctionCarbon(child, this)
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
      case s =>
        var dc: MeasureAggregator = null
        if (s != null) {
          s match {
            case v: java.math.BigDecimal =>
              dc = new AvgBigDecimalAggregator
              dc.agg(new java.math.BigDecimal(s.toString))
              dc.setNewValue(new java.math.BigDecimal(s.toString))
            case l: Long =>
              dc = new AvgLongAggregator
              dc.agg(s.toString.toLong)
              dc.setNewValue(s.toString.toLong)
            case _ =>
              dc = new AvgDoubleAggregator
              dc.agg(s.toString.toDouble)
              dc.setNewValue(s.toString.toDouble)
          }
        }
        else {
          dc = new AvgDoubleAggregator()
        }
        dc
    }
    if (avg == null) {
      avg = agg
    } else {
      avg.merge(agg)
    }
  }

  override def eval(input: InternalRow): Any = {
    if (finalAgg) {
      if (avg.isFirstTime) {
        null
      } else {
        avg match {
          case avg: AvgBigDecimalAggregator =>
            Cast(Literal(avg.getBigDecimalValue), base.dataType).eval(null)
          case avg: AvgLongAggregator =>
            Cast(Literal(avg.getLongValue), base.dataType).eval(null)
          case _ =>
            Cast(Literal(avg.getDoubleValue), base.dataType).eval(null)
        }
      }
    } else {
      avg
    }
  }
}

case class CountFunctionCarbon(expr: Expression, base: AggregateExpression1, finalAgg: Boolean)
  extends AggregateFunction1 {
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
        }
        agg1
    }
    if (count == null) {
      count = agg
    } else {
      count.merge(agg)
    }
  }

  override def eval(input: InternalRow): Any = {
    if (finalAgg && count != null) {
      if (count.isFirstTime) {
        0L
      } else {
        Cast(Literal(count.getDoubleValue), base.dataType).eval(null)
      }
    } else {
      count
    }
  }

}


case class SumFunctionCarbon(expr: Expression, base: AggregateExpression1, finalAgg: Boolean)
  extends AggregateFunction1 {
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
      case s =>
        var dc: MeasureAggregator = null
        if (s != null) {
          s match {
            case bd: java.math.BigDecimal =>
              dc = new SumBigDecimalAggregator
              dc.agg(new java.math.BigDecimal(s.toString))
              dc.setNewValue(new java.math.BigDecimal(s.toString))
            case l: Long =>
              dc = new SumLongAggregator
              dc.agg(s.toString.toLong)
              dc.setNewValue(s.toString.toLong)
            case _ =>
              dc = new SumDoubleAggregator
              dc.agg(s.toString.toDouble)
              dc.setNewValue(s.toString.toDouble)
          }
        }
        else {
          dc = new SumDoubleAggregator
        }
        dc
    }
    if (sum == null) {
      sum = agg
    } else {
      sum.merge(agg)
    }
  }

  override def eval(input: InternalRow): Any = {
    if (finalAgg && sum != null) {
      if (sum.isFirstTime) {
        null
      } else {
        sum match {
          case s: SumBigDecimalAggregator =>
            Cast(Literal(sum.getBigDecimalValue), base.dataType).eval(input)
          case s: SumLongAggregator =>
            Cast(Literal(sum.getLongValue), base.dataType).eval(input)
          case _ =>
            Cast(Literal(sum.getDoubleValue), base.dataType).eval(input)
        }
      }
    } else {
      sum
    }
  }
}

case class MaxFunctionCarbon(expr: Expression, base: AggregateExpression1, finalAgg: Boolean)
  extends AggregateFunction1 {
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
      case s =>
        val dc = new MaxAggregator
        if (s != null) {
          dc.agg(s.toString.toDouble)
          dc.setNewValue(s.toString.toDouble)
        }
        dc
    }
    if (max == null) {
      max = agg
    } else {
      max.merge(agg)
    }
  }

  override def eval(input: InternalRow): Any = {
    if (finalAgg && max != null) {
      if (max.isFirstTime) {
        null
      } else {
        Cast(Literal(max.getValueObject), base.dataType).eval(null)
      }
    } else {
      max
    }
  }
}

case class MinFunctionCarbon(expr: Expression, base: AggregateExpression1, finalAgg: Boolean)
  extends AggregateFunction1 {
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
      case s =>
        val dc: MeasureAggregator = new MinAggregator
        if (s != null) {
          dc.agg(s.toString.toDouble)
          dc.setNewValue(s.toString.toDouble)
        }
        dc
    }
    if (min == null) {
      min = agg
    } else {
      min.merge(agg)
    }
  }

  override def eval(input: InternalRow): Any = {
    if (finalAgg && min != null) {
      if (min.isFirstTime) {
        null
      } else {
        Cast(Literal(min.getValueObject), base.dataType).eval(null)
      }
    } else {
      min
    }
  }
}

case class SumDisctinctFunctionCarbon(expr: Expression, base: AggregateExpression1,
    isFinal: Boolean)
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
      case s =>
        var dc: MeasureAggregator = null
        s match {
          case Double =>
            dc = new SumDistinctDoubleAggregator
            dc.setNewValue(s.toString.toDouble)
          case Int =>
            dc = new SumDistinctLongAggregator
            dc.setNewValue(s.toString.toLong)
          case bd: java.math.BigDecimal =>
            dc = new SumDistinctBigDecimalAggregator
            dc.setNewValue(new java.math.BigDecimal(s.toString))
          case _ =>
        }
        dc
    }
    if (distinct == null) {
      distinct = agg
    } else {
      distinct.merge(agg)
    }
  }

  override def eval(input: InternalRow): Any =
  // in case of empty load it was failing so added null check.
  {
    if (isFinal && distinct != null) {
      if (distinct.isFirstTime) {
        null
      }
      else {
      Cast(Literal(distinct.getValueObject), base.dataType).eval(null)
      }
    }
    else {
      distinct
    }
  }
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
      case s =>
        val dc = new DistinctCountAggregatorObjectSet
        dc.setNewValue(s.toString)
        dc
    }
    if (count == null) {
      count = agg
    } else {
      count.merge(agg)
    }
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
      case s =>
        val dc = new DistinctCountAggregatorObjectSet
        dc.setNewValue(s.toString)
        dc
    }
    if (count == null) {
      count = agg
    } else {
      count.merge(agg)
    }
  }

  override def eval(input: InternalRow): Any = {
    if (count == null) {
      Cast(Literal(0), base.dataType).eval(null)
    } else if (count.isFirstTime) {
      Cast(Literal(0), base.dataType).eval(null)
    } else {
      Cast(Literal(count.getDoubleValue), base.dataType).eval(null)
    }
  }
}

case class FirstFunctionCarbon(expr: Expression, base: AggregateExpression1)
  extends AggregateFunction1 {
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

  override def children: Seq[Expression] = Seq(expr)

  override def dataType: DataType = expr.dataType

  override def nullable: Boolean = expr.nullable

  override def references: AttributeSet = AttributeSet(expr.flatMap(_.references.iterator))

  override def foldable: Boolean = expr.foldable

  override def toString: String = "Flatten(" + expr.toString + ")"

  type EvaluatedType = Any

  override def eval(input: InternalRow): Any = {
    expr.eval(input) match {
      case d: MeasureAggregator => d.getDoubleValue
      case others => others
    }
  }
}

case class FlatAggregatorsExpr(expr: Expression) extends Expression with CodegenFallback {
  self: Product =>

  override def children: Seq[Expression] = Seq(expr)

  override def dataType: DataType = expr.dataType

  override def nullable: Boolean = expr.nullable

  override def references: AttributeSet = AttributeSet(expr.flatMap(_.references.iterator))

  override def foldable: Boolean = expr.foldable

  override def toString: String = "FlattenAggregators(" + expr.toString + ")"

  type EvaluatedType = Any

  override def eval(input: InternalRow): Any = {
    expr.eval(input) match {
      case d: MeasureAggregator =>
        d.setNewValue(d.getDoubleValue)
        d
      case others => others
    }
  }
}

case class PositionLiteral(expr: Expression, intermediateDataType: DataType)
  extends LeafExpression with CodegenFallback {
  override def dataType: DataType = expr.dataType

  override def nullable: Boolean = false

  type EvaluatedType = Any
  var position = -1

  def setPosition(pos: Int): Unit = position = pos

  override def toString: String = s"PositionLiteral($position : $expr)"

  override def eval(input: InternalRow): Any = {
    if (position != -1) {
      input.get(position, intermediateDataType)
    } else {
      expr.eval(input)
    }
  }
}

