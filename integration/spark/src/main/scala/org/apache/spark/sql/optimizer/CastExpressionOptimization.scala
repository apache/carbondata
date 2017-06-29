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

import java.text.SimpleDateFormat
import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.CastExpr
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, EmptyRow, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, LessThan, LessThanOrEqual, Literal, Not}
import org.apache.spark.sql.types._

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.util.CommonUtil

object CastExpressionOptimization {

  /**
   * @param v string value to be converted
   * @return  fomatted vaue based
   */
  def typeCastStringToLong(v: Any): Option[Any] = {
    try {
      val value = CommonUtil.parseStringTimestampToSparkFormat(v)
      Some(value)
    } catch {
      case e: Exception =>
        None
    }
  }

  /**
   * @param v string value to be converted
   * @return  fomatted vaue based
   */
  def typeCastStringToLongForDateType(v: Any): Option[Any] = {
    try {
      val value = CommonUtil.parseStringDateToSparkFormat(v)
      Some(value)
    } catch {
      case e: Exception =>
        None
    }
  }

  /**
   * @param list  List of Expression for In Filter
   * @return  formated Litral Expression list value
   */

  def typeCastStringToLongList(list: Seq[Expression]): Seq[Expression] = {
    val tempList = new util.ArrayList[Expression]()
    list.foreach(
        expr => typeCastStringToLong(expr.eval(EmptyRow)).foreach(
          result => tempList.add(Literal.create(result, TimestampType))
        )
    )
  tempList.asScala
  }

  /**
   * @param list  List of Expression  for In Filter
   * @return  formated Litral Expression list value
   */

  def typeCastStringToLongListForDateType(list: Seq[Expression]): Seq[Expression] = {
    val tempList = new util.ArrayList[Expression]()
    list.foreach(
      expr => typeCastStringToLongForDateType(expr.eval(EmptyRow)).foreach(
        result => tempList.add(Literal.create(result, DateType))
      )
    )
    tempList.asScala
  }

  /**
   * This routines tries to apply rules on Cast Filter Predicates and if the rules applied and the
   * values can be toss back to native datatypes the cast is removed. Current two rules are applied
   * a) Left : timestamp column      Right : String Value
   * Input from Spark : cast (col as string) <> 'String Literal'
   * Change to        : Column <> 'Long value of Timestamp String'
   *  * b) Left : Datetype column      Right : String Value
   * Input from Spark : cast (col as string) <> 'String Literal'
   * Change to        : Column <> 'Long value of Timestamp String'
   * Exception is thrown for complex data type is it will not match any expression
   * @param expr
   * @return
   */
  def checkIfCastCanBeRemoved(expr: Expression): Option[Expression] = {
    try {
      expr match {
        case c@EqualTo(Cast(a: Attribute, _), Literal(v, t)) =>
          a.dataType match {
            case ts: TimestampType if t.sameType(StringType) =>
              typeCastStringToLong(v)
                .map(v => EqualTo(a: Attribute, Literal.create(v, ts)))
            case ts: DateType if t.sameType(StringType) =>
              typeCastStringToLongForDateType(v)
                .map(v => EqualTo(a: Attribute, Literal.create(v, ts)))

            case _ => None
          }
        case c@EqualTo(Literal(v, t), Cast(a: Attribute, _)) =>
          a.dataType match {
            case ts: TimestampType if t.sameType(StringType) =>
              typeCastStringToLong(v)
                .map(v => EqualTo(a: Attribute, Literal.create(v, ts)))

            case ts: DateType if t.sameType(StringType) =>
              typeCastStringToLongForDateType(v)
                .map(v => EqualTo(a: Attribute, Literal.create(v, ts)))

            case _ => None
          }

        case c@Not(EqualTo(Cast(a: Attribute, _), Literal(v, t))) =>
          a.dataType match {
            case ts: TimestampType if t.sameType(StringType) =>
              typeCastStringToLong(v)
                .map(v => Not(EqualTo(a: Attribute, Literal.create(v, ts))))

            case ts: DateType if t.sameType(StringType) =>
              typeCastStringToLongForDateType(v)
                .map(v => Not(EqualTo(a: Attribute, Literal.create(v, ts))))

            case _ => None
          }
        case c@Not(EqualTo(Literal(v, t), Cast(a: Attribute, _))) =>
          a.dataType match {
            case ts: TimestampType if t.sameType(StringType) =>
              typeCastStringToLong(v)
                .map(v => Not(EqualTo(a: Attribute, Literal.create(v, ts))))

            case ts: DateType if t.sameType(StringType) =>
              typeCastStringToLongForDateType(v)
                .map(v => Not(EqualTo(a: Attribute, Literal.create(v, ts))))
            case _ => None
          }

        case c@In(Cast(a: Attribute, _), list) =>
          a.dataType match {
            case ts: TimestampType if list.head.dataType.sameType(StringType) =>
              val value = typeCastStringToLongList(list)
              if (value.size == list.size) {
                Some(In(a: Attribute, value.toList))
              } else {
                None
              }
            case ts: DateType if list.head.dataType.sameType(StringType) =>
              val value = typeCastStringToLongListForDateType(list)
              if (value.size == list.size) {
                Some(In(a: Attribute, value.toList))
              } else {
                None
              }

            case _ => None
          }

        case c@Not(In(Cast(a: Attribute, _), list)) =>
          a.dataType match {
            case ts: TimestampType if list.head.dataType.sameType(StringType) =>
              val value = typeCastStringToLongList(list)
              if (value.size == list.size) {
                Some(Not(In(a: Attribute, value.toList)))
              } else {
                None
              }

            case ts: DateType if list.head.dataType.sameType(StringType) =>
              val value = typeCastStringToLongListForDateType(list)
              if (value.size == list.size) {
                Some(Not(In(a: Attribute, value.toList)))
              } else {
                None
              }
            case _ => None
          }
        case c@GreaterThan(Cast(a: Attribute, _), Literal(v, t)) =>
          a.dataType match {
            case ts: TimestampType if t.sameType(StringType) =>
              typeCastStringToLong(v)
                .map(v => GreaterThan(a: Attribute, Literal.create(v, ts)))
            case ts: DateType if t.sameType(StringType) =>
              typeCastStringToLongForDateType(v)
                .map(v => GreaterThan(a: Attribute, Literal.create(v, ts)))
            case _ => None
          }
        case c@GreaterThan(Literal(v, t), Cast(a: Attribute, _)) =>
          a.dataType match {
            case ts: TimestampType if t.sameType(StringType) =>
              typeCastStringToLong(v)
                .map(v => GreaterThan(a: Attribute, Literal.create(v, ts)))
            case ts: DateType if t.sameType(StringType) =>
              typeCastStringToLongForDateType(v)
                .map(v => GreaterThan(a: Attribute, Literal.create(v, ts)))
            case _ => None
          }

        case c@LessThan(Cast(a: Attribute, _), Literal(v, t)) =>
          a.dataType match {
            case ts: TimestampType if t.sameType(StringType) =>
              typeCastStringToLong(v)
                .map(v => LessThan(a: Attribute, Literal.create(v, ts)))
            case ts: DateType if t.sameType(StringType) =>
              typeCastStringToLongForDateType(v)
                .map(v => LessThan(a: Attribute, Literal.create(v, ts)))
            case _ => None
          }
        case c@LessThan(Literal(v, t), Cast(a: Attribute, _)) =>
          a.dataType match {
            case ts: TimestampType if t.sameType(StringType) =>
              typeCastStringToLong(v)
                .map(v => LessThan(a: Attribute, Literal.create(v, ts)))
            case ts: DateType if t.sameType(StringType) =>
              typeCastStringToLongForDateType(v)
                .map(v => LessThan(a: Attribute, Literal.create(v, ts)))
            case _ => None
          }
        case c@GreaterThanOrEqual(Cast(a: Attribute, _), Literal(v, t)) =>
          a.dataType match {
            case ts: TimestampType if t.sameType(StringType) =>
              typeCastStringToLong(v)
                .map(v => GreaterThanOrEqual(a: Attribute, Literal.create(v, ts)))
            case ts: DateType if t.sameType(StringType) =>
              typeCastStringToLongForDateType(v)
                .map(v => GreaterThanOrEqual(a: Attribute, Literal.create(v, ts)))
            case _ => None
          }
        case c@GreaterThanOrEqual(Literal(v, t), Cast(a: Attribute, _)) =>
          a.dataType match {
            case ts: TimestampType if t.sameType(StringType) =>
              typeCastStringToLong(v)
                .map(v => GreaterThanOrEqual(a: Attribute, Literal.create(v, ts)))
            case ts: DateType if t.sameType(StringType) =>
              typeCastStringToLongForDateType(v)
                .map(v => GreaterThanOrEqual(a: Attribute, Literal.create(v, ts)))
            case _ => None
          }

        case c@LessThanOrEqual(Cast(a: Attribute, _), Literal(v, t)) =>
          a.dataType match {
            case ts: TimestampType if t.sameType(StringType) =>
              typeCastStringToLong(v)
                .map(v => LessThanOrEqual(a: Attribute, Literal.create(v, ts)))
            case ts: DateType if t.sameType(StringType) =>
              typeCastStringToLongForDateType(v)
                .map(v => LessThanOrEqual(a: Attribute, Literal.create(v, ts)))
            case _ => None
          }
        case c@LessThanOrEqual(Literal(v, t), Cast(a: Attribute, _)) =>
          a.dataType match {
            case ts: TimestampType if t.sameType(StringType) =>
              typeCastStringToLong(v)
                .map(v => LessThanOrEqual(a: Attribute, Literal.create(v, ts)))
            case ts: DateType if t.sameType(StringType) =>
              typeCastStringToLongForDateType(v)
                .map(v => LessThanOrEqual(a: Attribute, Literal.create(v, ts)))
            case _ => None
          }
        case _ => None
      }
    } catch {
    case e: Exception =>
     None
    }

    }
}
