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

import java.text.{ParseException, SimpleDateFormat}
import java.util
import java.util.{Locale, TimeZone}

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, EmptyRow, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, LessThan, LessThanOrEqual, Literal, Not}
import org.apache.spark.sql.CastExpr
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, TimestampType}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

object CastExpressionOptimization {


  def typeCastStringToLong(v: Any): Any = {
    val parser: SimpleDateFormat = new SimpleDateFormat(
      CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT))
    try {
      val value = parser.parse(v.toString).getTime() * 1000L
      value
    } catch {
      case e: ParseException =>
        try {
          val parsenew: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz")
          parsenew.parse(v.toString).getTime() * 1000L
        } catch {
          case e: ParseException =>
            val gmtDay = new SimpleDateFormat("yyyy-MM-dd", Locale.US)
            gmtDay.setTimeZone(TimeZone.getTimeZone("GMT"))
            try {
              gmtDay.parse(v.toString).getTime()
            } catch {
              case e: ParseException =>
                v
              case e: Exception =>
                v
            }
          case e: Exception =>
            v
        }
      case e: Exception =>
        v
    }
  }

  def typeCastStringToLongList(list: Seq[Expression]): Seq[Expression] = {
    val tempList = new util.ArrayList[Expression]()
    list.foreach { value =>
      val output = typeCastStringToLong(value)
      if (!output.equals(value)) {
        tempList.add(output.asInstanceOf[Expression])
      }
    }
    if (tempList.size() != list.size) {
      list
    } else {
      tempList.asScala
    }
  }

  def typeCastDoubleToIntList(list: Seq[Expression]): Seq[Expression] = {
    val tempList = new util.ArrayList[Expression]()
    list.foreach { value =>
      val output = value.asInstanceOf[Double].toInt
      if (value.asInstanceOf[Double].toInt.equals(output)) {
        tempList.add(output.asInstanceOf[Expression])
      }
    }
    if (tempList.size() != list.size) {
      list
    } else {
      tempList.asScala
    }
  }

  /**
   * This routines tries to apply rules on Cast Filter Predicates and if the rules applied and the
   * values can be toss back to native datatypes the cast is removed. Current two rules are applied
   * a) Left : timestamp column      Right : String Value
   * Input from Spark : cast (col as string) <> 'String Literal'
   * Change to        : Column <> 'Long value of Timestamp String'
   *
   * b) Left : Integer Column        Right : String Value
   * Input from Spark : cast (col as double) <> 'Double Literal'
   * Change to        : Column <> 'Int value'
   *
   * @param expr
   * @return
   */
  def checkIfCastCanBeRemove(expr: Expression): Option[sources.Filter] = {
    expr match {
      case c@EqualTo(Cast(a: Attribute, _), Literal(v, t)) =>
        a.dataType match {
          case ts: TimestampType if t.sameType(StringType) =>
            val value = typeCastStringToLong(v)
            if (!value.equals(v)) {
              Some(sources.EqualTo(a.name, value))
            } else {
              Some(CastExpr(c))
            }
          case i: IntegerType if t.sameType(DoubleType) =>
            val value = v.asInstanceOf[Double].toInt
            if (value.toDouble.equals(v)) {
              Some(sources.EqualTo(a.name, value))
            } else {
              Some(CastExpr(c))
            }
          case _ => Some(CastExpr(c))
        }
      case c@EqualTo(Literal(v, t), Cast(a: Attribute, _)) =>
        a.dataType match {
          case ts: TimestampType if t.sameType(StringType) =>
            val value = typeCastStringToLong(v)
            if (!value.equals(v)) {
              Some(sources.EqualTo(a.name, value))
            } else {
              Some(CastExpr(c))
            }
          case i: IntegerType if t.sameType(DoubleType) =>
            val value = v.asInstanceOf[Double].toInt
            if (value.toDouble.equals(v)) {
              Some(sources.EqualTo(a.name, value))
            } else {
              Some(CastExpr(c))
            }
          case _ => Some(CastExpr(c))
        }
      case c@Not(EqualTo(Cast(a: Attribute, _), Literal(v, t))) =>
        a.dataType match {
          case ts: TimestampType if t.sameType(StringType) =>
            val value = typeCastStringToLong(v)
            if (!value.equals(v)) {
              Some(sources.Not(sources.EqualTo(a.name, value)))
            } else {
              Some(CastExpr(c))
            }
          case i: IntegerType if t.sameType(DoubleType) =>
            val value = v.asInstanceOf[Double].toInt
            if (value.toDouble.equals(v)) {
              Some(sources.Not(sources.EqualTo(a.name, value)))
            } else {
              Some(CastExpr(c))
            }
          case _ => Some(CastExpr(c))
        }
      case c@Not(EqualTo(Literal(v, t), Cast(a: Attribute, _))) =>
        a.dataType match {
          case ts: TimestampType if t.sameType(StringType) =>
            val value = typeCastStringToLong(v)
            if (!value.equals(v)) {
              Some(sources.Not(sources.EqualTo(a.name, value)))
            } else {
              Some(CastExpr(c))
            }
          case i: IntegerType if t.sameType(DoubleType) =>
            val value = v.asInstanceOf[Double].toInt
            if (value.toDouble.equals(v)) {
              Some(sources.Not(sources.EqualTo(a.name, value)))
            } else {
              Some(CastExpr(c))
            }
          case _ => Some(CastExpr(c))
        }
      case c@Not(In(Cast(a: Attribute, _), list)) =>
        a.dataType match {
          case ts: TimestampType if list.head.dataType.sameType(StringType) =>
            val value = typeCastStringToLongList(list)
            if (!value.equals(list)) {
              val hSet = value.map(e => e.eval(EmptyRow))
              Some(sources.Not(sources.In(a.name, hSet.toArray)))
            } else {
              Some(CastExpr(c))
            }
          case i: IntegerType if list.head.dataType.sameType(DoubleType) =>
            val value = typeCastDoubleToIntList(list)
            if (!value.equals(list)) {
              val hSet = value.map(e => e.eval(EmptyRow))
              Some(sources.Not(sources.In(a.name, hSet.toArray)))
            } else {
              Some(CastExpr(c))
            }
          case _ => Some(CastExpr(c))
        }
      case c@In(Cast(a: Attribute, _), list) =>
        a.dataType match {
          case ts: TimestampType if list.head.dataType.sameType(StringType) =>
            val value = typeCastStringToLongList(list)
            if (!value.equals(list)) {
              val hSet = value.map(e => e.eval(EmptyRow))
              Some(sources.In(a.name, hSet.toArray))
            } else {
              Some(CastExpr(c))
            }
          case i: IntegerType if list.head.dataType.sameType(DoubleType) =>
            val value = typeCastDoubleToIntList(list)
            if (!value.equals(list)) {
              val hSet = value.map(e => e.eval(EmptyRow))
              Some(sources.In(a.name, hSet.toArray))
            } else {
              Some(CastExpr(c))
            }
          case _ => Some(CastExpr(c))
        }
      case c@GreaterThan(Cast(a: Attribute, _), Literal(v, t)) =>
        a.dataType match {
          case ts: TimestampType if t.sameType(StringType) =>
            val value = typeCastStringToLong(v)
            if (!value.equals(v)) {
              Some(sources.GreaterThan(a.name, value))
            } else {
              Some(CastExpr(c))
            }
          case i: IntegerType if t.sameType(DoubleType) =>
            val value = v.asInstanceOf[Double].toInt
            if (value.toDouble.equals(v)) {
              Some(sources.GreaterThan(a.name, value))
            } else {
              Some(CastExpr(c))
            }
          case _ => Some(CastExpr(c))
        }
      case c@GreaterThan(Literal(v, t), Cast(a: Attribute, _)) =>
        a.dataType match {
          case ts: TimestampType if t.sameType(StringType) =>
            val value = typeCastStringToLong(v)
            if (!value.equals(v)) {
              Some(sources.LessThan(a.name, value))
            } else {
              Some(CastExpr(c))
            }
          case i: IntegerType if t.sameType(DoubleType) =>
            val value = v.asInstanceOf[Double].toInt
            if (value.toDouble.equals(v)) {
              Some(sources.LessThan(a.name, value))
            } else {
              Some(CastExpr(c))
            }
          case _ => Some(CastExpr(c))
        }
      case c@LessThan(Cast(a: Attribute, _), Literal(v, t)) =>
        a.dataType match {
          case ts: TimestampType if t.sameType(StringType) =>
            val value = typeCastStringToLong(v)
            if (!value.equals(v)) {
              Some(sources.LessThan(a.name, value))
            } else {
              Some(CastExpr(c))
            }
          case i: IntegerType if t.sameType(DoubleType) =>
            val value = v.asInstanceOf[Double].toInt
            if (value.toDouble.equals(v)) {
              Some(sources.LessThan(a.name, value))
            } else {
              Some(CastExpr(c))
            }
          case _ => Some(CastExpr(c))
        }
      case c@LessThan(Literal(v, t), Cast(a: Attribute, _)) =>
        a.dataType match {
          case ts: TimestampType if t.sameType(StringType) =>
            val value = typeCastStringToLong(v)
            if (!value.equals(v)) {
              Some(sources.GreaterThan(a.name, value))
            } else {
              Some(CastExpr(c))
            }
          case i: IntegerType if t.sameType(DoubleType) =>
            val value = v.asInstanceOf[Double].toInt
            if (value.toDouble.equals(v)) {
              Some(sources.GreaterThan(a.name, value))
            } else {
              Some(CastExpr(c))
            }
          case _ => Some(CastExpr(c))
        }
      case c@GreaterThanOrEqual(Cast(a: Attribute, _), Literal(v, t)) =>
        a.dataType match {
          case ts: TimestampType if t.sameType(StringType) =>
            val value = typeCastStringToLong(v)
            if (!value.equals(v)) {
              Some(sources.GreaterThanOrEqual(a.name, value))
            } else {
              Some(CastExpr(c))
            }
          case i: IntegerType if t.sameType(DoubleType) =>
            val value = v.asInstanceOf[Double].toInt
            if (value.toDouble.equals(v)) {
              Some(sources.GreaterThanOrEqual(a.name, value))
            } else {
              Some(CastExpr(c))
            }
          case _ => Some(CastExpr(c))
        }
      case c@GreaterThanOrEqual(Literal(v, t), Cast(a: Attribute, _)) =>
        a.dataType match {
          case ts: TimestampType if t.sameType(StringType) =>
            val value = typeCastStringToLong(v)
            if (!value.equals(v)) {
              Some(sources.LessThanOrEqual(a.name, value))
            } else {
              Some(CastExpr(c))
            }
          case i: IntegerType if t.sameType(DoubleType) =>
            val value = v.asInstanceOf[Double].toInt
            if (value.toDouble.equals(v)) {
              Some(sources.LessThanOrEqual(a.name, value))
            } else {
              Some(CastExpr(c))
            }
          case _ => Some(CastExpr(c))
        }
      case c@LessThanOrEqual(Cast(a: Attribute, _), Literal(v, t)) =>
        a.dataType match {
          case ts: TimestampType if t.sameType(StringType) =>
            val value = typeCastStringToLong(v)
            if (!value.equals(v)) {
              Some(sources.LessThanOrEqual(a.name, value))
            } else {
              Some(CastExpr(c))
            }
          case i: IntegerType if t.sameType(DoubleType) =>
            val value = v.asInstanceOf[Double].toInt
            if (value.toDouble.equals(v)) {
              Some(sources.LessThanOrEqual(a.name, value))
            } else {
              Some(CastExpr(c))
            }
          case _ => Some(CastExpr(c))
        }
      case c@LessThanOrEqual(Literal(v, t), Cast(a: Attribute, _)) =>
        a.dataType match {
          case ts: TimestampType if t.sameType(StringType) =>
            val value = typeCastStringToLong(v)
            if (!value.equals(v)) {
              Some(sources.GreaterThanOrEqual(a.name, value))
            } else {
              Some(CastExpr(c))
            }
          case i: IntegerType if t.sameType(DoubleType) =>
            val value = v.asInstanceOf[Double].toInt
            if (value.toDouble.equals(v)) {
              Some(sources.GreaterThanOrEqual(a.name, value))
            } else {
              Some(CastExpr(c))
            }
          case _ => Some(CastExpr(c))
        }
    }
  }
}
