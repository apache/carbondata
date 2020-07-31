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

import org.apache.spark.sql.catalyst.expressions.{Attribute, EmptyRow, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, LessThan, LessThanOrEqual, Literal, Not}
import org.apache.spark.sql.CastExpr
import org.apache.spark.sql.FalseExpr
import org.apache.spark.sql.sources
import org.apache.spark.sql.types._
import org.apache.spark.sql.CarbonExpressions.{MatchCast => Cast}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources.Filter
import org.apache.spark.unsafe.types.UTF8String

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

object CastExpressionOptimization {

  def typeCastStringToLong(v: Any, dataType: DataType): Any = {
    if (dataType == TimestampType || dataType == DateType) {
      val value = if (dataType == TimestampType) {
        DateTimeUtils.stringToTimestamp(UTF8String.fromString(v.toString))
      } else {
        None
      }
      if (value.isDefined) {
        value.get
      } else {
        var parser: SimpleDateFormat = null
        if (dataType == TimestampType) {
          parser = new SimpleDateFormat(CarbonProperties.getInstance
            .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
              CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT))
        } else if (dataType == DateType) {
          parser = new SimpleDateFormat(CarbonProperties.getInstance
            .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
              CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT))
          parser.setTimeZone(TimeZone.getTimeZone("GMT"))
        }
        try {
          val value = parser.parse(v.toString).getTime() * 1000L
          value
        } catch {
          case e: ParseException =>
            try {
              val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz")
              format.parse(v.toString).getTime() * 1000L
            } catch {
              case e: ParseException =>
                val gmtDay = new SimpleDateFormat("yyyy-MM-dd", Locale.US)
                gmtDay.setTimeZone(TimeZone.getTimeZone("GMT"))
                try {
                  gmtDay.parse(v.toString).getTime() * 1000L
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
    }
    else {
      throw new UnsupportedOperationException("Unsupported DataType being evaluated.")
    }
  }


  def typeCastStringToLongList(list: Seq[Expression], dataType: DataType): Seq[Expression] = {
    val tempList = new util.ArrayList[Expression]()
    list.foreach { value =>
      val output = typeCastStringToLong(value, dataType)
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

  def typeCastIntToShortList(list: Seq[Expression]): Seq[Expression] = {
    val tempList = new util.ArrayList[Expression]()
    list.foreach { value =>
      val output = value.asInstanceOf[Integer].toShort
      if (value.asInstanceOf[Integer].toShort.equals(output)) {
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
   * values can be toss back to native data types the cast is removed.
   * Current two rules are applied
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

    def checkBinaryExpression(attributeType: DataType,
        value: Any,
        valueType: DataType,
        nonEqual: Boolean = true): Option[Filter] = {
      attributeType match {
        case ts@(_: DateType | _: TimestampType) if valueType.sameType(StringType) =>
          val filter = updateFilterForTimeStamp(value, expr, ts)
          if (nonEqual) {
            updateFilterForNonEqualTimeStamp(value, expr, filter)
          } else {
            filter
          }
        case _: IntegerType if valueType.sameType(DoubleType) =>
          updateFilterForInt(value, expr)
        case _: ShortType if valueType.sameType(IntegerType) =>
          updateFilterForShort(value, expr)
        case arr: ArrayType if !nonEqual =>
          checkBinaryExpression(arr.elementType, value, valueType, nonEqual)
        case _ => Some(CastExpr(expr))
      }
    }

    def checkInValueList(attributeName: String,
        list: Seq[Expression],
        newList: Seq[Expression],
        hasNot: Boolean = true): Option[Filter] = {
      if (!newList.equals(list)) {
        val hSet = list.map(e => e.eval(EmptyRow))
        if (hasNot) {
          Some(sources.Not(sources.In(attributeName, hSet.toArray)))
        } else {
          Some(sources.In(attributeName, hSet.toArray))
        }
      } else {
        Some(CastExpr(expr))
      }
    }

    def checkInExpression(attribute: Attribute,
        list: Seq[Expression],
        hasNot: Boolean = true): Option[Filter] = {
      attribute.dataType match {
        case ts@(_: DateType | _: TimestampType) if list.head.dataType.sameType(StringType) =>
          checkInValueList(attribute.name, list, typeCastStringToLongList(list, ts), hasNot)
        case _: IntegerType if list.head.dataType.sameType(DoubleType) =>
          checkInValueList(attribute.name, list, typeCastDoubleToIntList(list), hasNot)
        case _: ShortType if list.head.dataType.sameType(IntegerType) =>
          checkInValueList(attribute.name, list, typeCastIntToShortList(list), hasNot)
        case _ => Some(CastExpr(expr))
      }
    }

    expr match {
      case EqualTo(Cast(a: Attribute, _), Literal(v, t)) =>
        checkBinaryExpression(a.dataType, v, t, false)
      case EqualTo(Literal(v, t), Cast(a: Attribute, _)) =>
        checkBinaryExpression(a.dataType, v, t, false)
      case Not(EqualTo(Cast(a: Attribute, _), Literal(v, t))) =>
        checkBinaryExpression(a.dataType, v, t, false)
      case Not(EqualTo(Literal(v, t), Cast(a: Attribute, _))) =>
        checkBinaryExpression(a.dataType, v, t, false)
      case Not(In(Cast(a: Attribute, _), list)) =>
        checkInExpression(a, list)
      case In(Cast(a: Attribute, _), list) =>
        checkInExpression(a, list, false)
      case GreaterThan(Cast(a: Attribute, _), Literal(v, t)) =>
        checkBinaryExpression(a.dataType, v, t)
      case GreaterThan(Literal(v, t), Cast(a: Attribute, _)) =>
        checkBinaryExpression(a.dataType, v, t)
      case LessThan(Cast(a: Attribute, _), Literal(v, t)) =>
        checkBinaryExpression(a.dataType, v, t)
      case LessThan(Literal(v, t), Cast(a: Attribute, _)) =>
        checkBinaryExpression(a.dataType, v, t)
      case GreaterThanOrEqual(Cast(a: Attribute, _), Literal(v, t)) =>
        checkBinaryExpression(a.dataType, v, t)
      case GreaterThanOrEqual(Literal(v, t), Cast(a: Attribute, _)) =>
        checkBinaryExpression(a.dataType, v, t)
      case LessThanOrEqual(Cast(a: Attribute, _), Literal(v, t)) =>
        checkBinaryExpression(a.dataType, v, t)
      case LessThanOrEqual(Literal(v, t), Cast(a: Attribute, _)) =>
        checkBinaryExpression(a.dataType, v, t)
    }
  }

  /**
   * the method removes the cast for short type columns
   *
   * @param actualValue
   * @param exp
   * @return
   */
  def updateFilterForShort(actualValue: Any, exp: Expression): Option[sources.Filter] = {
    val newValue = actualValue.asInstanceOf[Integer].toShort
    if (newValue.toInt.equals(actualValue)) {
      updateFilterBasedOnFilterType(exp, newValue)
    } else {
      Some(CastExpr(exp))
    }
  }

  /**
   * the method removes the cast for int type columns
   *
   * @param actualValue
   * @param exp
   * @return
   */
  def updateFilterForInt(actualValue: Any, exp: Expression): Option[sources.Filter] = {
    val newValue = actualValue.asInstanceOf[Double].toInt
    if (newValue.toDouble.equals(actualValue)) {
      updateFilterBasedOnFilterType(exp, newValue)
    } else {
      Some(CastExpr(exp))
    }
  }

  /**
   *
   * @param actualValue actual value of filter
   * @param exp         expression
   * @param filter      Filter Expression
   * @return return CastExpression or same Filter
   */
  def updateFilterForNonEqualTimeStamp(actualValue: Any, exp: Expression, filter: Option[Filter]):
  Option[sources.Filter] = {
    filter.get match {
      case FalseExpr() if (validTimeComparisionForSpark(actualValue)) =>
        Some(CastExpr(exp))
      case _ =>
        filter
    }
  }

  /**
   * Spark compares data based on double also.
   * Ex. select * ...where time >0 , this will return all data
   * So better  give to Spark as Cast Expression.
   *
   * @param numericTimeValue
   * @return if valid double return true,else false
   */
  def validTimeComparisionForSpark(numericTimeValue: Any): Boolean = {
    try {
      numericTimeValue.toString.toDouble
      true
    } catch {
      case _: Throwable => false
    }
  }


  /**
   * the method removes the cast for timestamp type columns
   *
   * @param actualValue
   * @param exp
   * @return
   */
  def updateFilterForTimeStamp(actualValue: Any, exp: Expression, dt: DataType):
  Option[sources.Filter] = {
    val newValue = typeCastStringToLong(actualValue, dt)
    if (!newValue.equals(actualValue)) {
      updateFilterBasedOnFilterType(exp, newValue)
    } else {
      Some(FalseExpr())
    }

  }


  /**
   * the method removes the cast for the respective filter type
   *
   * @param exp
   * @param newValue
   * @return
   */
  def updateFilterBasedOnFilterType(exp: Expression,
      newValue: Any): Some[Filter with Product with Serializable] = {
    exp match {
      case c@EqualTo(Cast(a: Attribute, _), Literal(v, t)) =>
        Some(sources.EqualTo(a.name, newValue))
      case c@EqualTo(Literal(v, t), Cast(a: Attribute, _)) =>
        Some(sources.EqualTo(a.name, newValue))
      case c@Not(EqualTo(Cast(a: Attribute, _), Literal(v, t))) =>
        Some(sources.Not(sources.EqualTo(a.name, newValue)))
      case c@Not(EqualTo(Literal(v, t), Cast(a: Attribute, _))) =>
        Some(sources.Not(sources.EqualTo(a.name, newValue)))
      case GreaterThan(Cast(a: Attribute, _), Literal(v, t)) =>
        Some(sources.GreaterThan(a.name, newValue))
      case GreaterThan(Literal(v, t), Cast(a: Attribute, _)) =>
        Some(sources.LessThan(a.name, newValue))
      case c@LessThan(Cast(a: Attribute, _), Literal(v, t)) =>
        Some(sources.LessThan(a.name, newValue))
      case c@LessThan(Literal(v, t), Cast(a: Attribute, _)) =>
        Some(sources.GreaterThan(a.name, newValue))
      case c@GreaterThanOrEqual(Cast(a: Attribute, _), Literal(v, t)) =>
        Some(sources.GreaterThanOrEqual(a.name, newValue))
      case c@GreaterThanOrEqual(Literal(v, t), Cast(a: Attribute, _)) =>
        Some(sources.LessThanOrEqual(a.name, newValue))
      case c@LessThanOrEqual(Cast(a: Attribute, _), Literal(v, t)) =>
        Some(sources.LessThanOrEqual(a.name, newValue))
      case c@LessThanOrEqual(Literal(v, t), Cast(a: Attribute, _)) =>
        Some(sources.GreaterThanOrEqual(a.name, newValue))
      case _ => Some(CastExpr(exp))
    }
  }
}
