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

package org.apache.carbondata.spark

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.CarbonBoundReference
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.optimizer.{AttributeReferenceWrapper, CarbonAliasDecoderRelation}
import org.apache.spark.sql.sources
import org.apache.spark.sql.SparkUnknownExpression
import org.apache.spark.sql.types._

import org.apache.carbondata.core.carbon.metadata.datatype.DataType
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn
import org.apache.carbondata.scan.expression.{ColumnExpression => CarbonColumnExpression, Expression => CarbonExpression, LiteralExpression => CarbonLiteralExpression}
import org.apache.carbondata.scan.expression.arithmetic.{AddExpression, DivideExpression, MultiplyExpression, SubstractExpression}
import org.apache.carbondata.scan.expression.conditional._
import org.apache.carbondata.scan.expression.logical.{AndExpression, OrExpression}
import org.apache.carbondata.spark.util.CarbonScalaUtil

/**
 * All filter conversions are done here.
 */
object CarbonFilters {

  /**
   * Converts data sources filters to carbon filter predicates.
   */
  def createCarbonFilter(schema: StructType,
      predicate: sources.Filter): Option[CarbonExpression] = {
    val dataTypeOf = schema.map(f => f.name -> f.dataType).toMap

    def createFilter(predicate: sources.Filter): Option[CarbonExpression] = {
      predicate match {

        case sources.EqualTo(name, value) =>
          Some(new EqualToExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, value)))
        case sources.Not(sources.EqualTo(name, value)) =>
          Some(new NotEqualsExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, value)))

        case sources.EqualNullSafe(name, value) =>
          Some(new EqualToExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, value)))
        case sources.Not(sources.EqualNullSafe(name, value)) =>
          Some(new NotEqualsExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, value)))

        case sources.GreaterThan(name, value) =>
          Some(new GreaterThanExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, value)))
        case sources.LessThan(name, value) =>
          Some(new LessThanExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, value)))
        case sources.GreaterThanOrEqual(name, value) =>
          Some(new GreaterThanEqualToExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, value)))
        case sources.LessThanOrEqual(name, value) =>
          Some(new LessThanEqualToExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, value)))

        case sources.In(name, values) =>
          Some(new InExpression(getCarbonExpression(name),
            new ListExpression(
              convertToJavaList(values.map(f => getCarbonLiteralExpression(name, f)).toList))))
        case sources.Not(sources.In(name, values)) =>
          Some(new NotInExpression(getCarbonExpression(name),
            new ListExpression(
              convertToJavaList(values.map(f => getCarbonLiteralExpression(name, f)).toList))))

        case sources.And(lhs, rhs) =>
          (createFilter(lhs) ++ createFilter(rhs)).reduceOption(new AndExpression(_, _))

        case sources.Or(lhs, rhs) =>
          for {
            lhsFilter <- createFilter(lhs)
            rhsFilter <- createFilter(rhs)
          } yield {
            new OrExpression(lhsFilter, rhsFilter)
          }

        case _ => None
      }
    }

    def getCarbonExpression(name: String) = {
      new CarbonColumnExpression(name,
        CarbonScalaUtil.convertSparkToCarbonDataType(dataTypeOf(name)))
    }

    def getCarbonLiteralExpression(name: String, value: Any): CarbonExpression = {
      new CarbonLiteralExpression(value,
        CarbonScalaUtil.convertSparkToCarbonDataType(dataTypeOf(name)))
    }

    createFilter(predicate)
  }


  // Check out which filters can be pushed down to carbon, remaining can be handled in spark layer.
  // Mostly dimension filters are only pushed down since it is faster in carbon.
  def selectFilters(filters: Seq[Expression],
      attrList: java.util.HashSet[AttributeReferenceWrapper],
      aliasMap: CarbonAliasDecoderRelation): Unit = {
    def translate(expr: Expression, or: Boolean = false): Option[sources.Filter] = {
      expr match {
        case or@ Or(left, right) =>

          val leftFilter = translate(left, true)
          val rightFilter = translate(right, true)
          if (leftFilter.isDefined && rightFilter.isDefined) {
            Some( sources.Or(leftFilter.get, rightFilter.get))
          } else {
            or.collect {
              case attr: AttributeReference =>
                attrList.add(AttributeReferenceWrapper(aliasMap.getOrElse(attr, attr)))
            }
            None
          }

        case And(left, right) =>
          (translate(left) ++ translate(right)).reduceOption(sources.And)

        case EqualTo(a: Attribute, Literal(v, t)) =>
          Some(sources.EqualTo(a.name, v))
        case EqualTo(l@Literal(v, t), a: Attribute) =>
          Some(sources.EqualTo(a.name, v))
        case EqualTo(Cast(a: Attribute, _), Literal(v, t)) =>
          Some(sources.EqualTo(a.name, v))
        case EqualTo(Literal(v, t), Cast(a: Attribute, _)) =>
          Some(sources.EqualTo(a.name, v))

        case Not(EqualTo(a: Attribute, Literal(v, t))) => new
            Some(sources.Not(sources.EqualTo(a.name, v)))
        case Not(EqualTo(Literal(v, t), a: Attribute)) => new
            Some(sources.Not(sources.EqualTo(a.name, v)))
        case Not(EqualTo(Cast(a: Attribute, _), Literal(v, t))) => new
            Some(sources.Not(sources.EqualTo(a.name, v)))
        case Not(EqualTo(Literal(v, t), Cast(a: Attribute, _))) => new
            Some(sources.Not(sources.EqualTo(a.name, v)))
        case IsNotNull(child)
            if (isCarbonSupportedDataTypes(child)) =>
            Some(sources.IsNotNull(null))
        case Not(In(a: Attribute, list)) if !list.exists(!_.isInstanceOf[Literal]) =>
          val hSet = list.map(e => e.eval(EmptyRow))
          Some(sources.Not(sources.In(a.name, hSet.toArray)))
        case In(a: Attribute, list) if !list.exists(!_.isInstanceOf[Literal]) =>
          val hSet = list.map(e => e.eval(EmptyRow))
          Some(sources.In(a.name, hSet.toArray))
        case Not(In(Cast(a: Attribute, _), list))
          if !list.exists(!_.isInstanceOf[Literal]) =>
          val hSet = list.map(e => e.eval(EmptyRow))
          Some(sources.Not(sources.In(a.name, hSet.toArray)))
        case In(Cast(a: Attribute, _), list) if !list.exists(!_.isInstanceOf[Literal]) =>
          val hSet = list.map(e => e.eval(EmptyRow))
          Some(sources.In(a.name, hSet.toArray))

        case GreaterThan(a: Attribute, Literal(v, t)) =>
          Some(sources.GreaterThan(a.name, v))
        case GreaterThan(Literal(v, t), a: Attribute) =>
          Some(sources.LessThan(a.name, v))
        case GreaterThan(Cast(a: Attribute, _), Literal(v, t)) =>
          Some(sources.GreaterThan(a.name, v))
        case GreaterThan(Literal(v, t), Cast(a: Attribute, _)) =>
          Some(sources.LessThan(a.name, v))

        case LessThan(a: Attribute, Literal(v, t)) =>
          Some(sources.LessThan(a.name, v))
        case LessThan(Literal(v, t), a: Attribute) =>
          Some(sources.GreaterThan(a.name, v))
        case LessThan(Cast(a: Attribute, _), Literal(v, t)) =>
          Some(sources.LessThan(a.name, v))
        case LessThan(Literal(v, t), Cast(a: Attribute, _)) =>
          Some(sources.GreaterThan(a.name, v))

        case GreaterThanOrEqual(a: Attribute, Literal(v, t)) =>
          Some(sources.GreaterThanOrEqual(a.name, v))
        case GreaterThanOrEqual(Literal(v, t), a: Attribute) =>
          Some(sources.LessThanOrEqual(a.name, v))
        case GreaterThanOrEqual(Cast(a: Attribute, _), Literal(v, t)) =>
          Some(sources.GreaterThanOrEqual(a.name, v))
        case GreaterThanOrEqual(Literal(v, t), Cast(a: Attribute, _)) =>
          Some(sources.LessThanOrEqual(a.name, v))

        case LessThanOrEqual(a: Attribute, Literal(v, t)) =>
          Some(sources.LessThanOrEqual(a.name, v))
        case LessThanOrEqual(Literal(v, t), a: Attribute) =>
          Some(sources.GreaterThanOrEqual(a.name, v))
        case LessThanOrEqual(Cast(a: Attribute, _), Literal(v, t)) =>
          Some(sources.LessThanOrEqual(a.name, v))
        case LessThanOrEqual(Literal(v, t), Cast(a: Attribute, _)) =>
          Some(sources.GreaterThanOrEqual(a.name, v))

        case others =>
          if (!or) {
            others.collect {
              case attr: AttributeReference =>
                attrList.add(AttributeReferenceWrapper(aliasMap.getOrElse(attr, attr)))
            }
          }
          None
      }
    }
    filters.flatMap(translate(_, false)).toArray
  }

  def processExpression(exprs: Seq[Expression],
      carbonTable: CarbonTable): Option[CarbonExpression] = {
    def transformExpression(expr: Expression, or: Boolean = false): Option[CarbonExpression] = {
      expr match {
        case or@Or(left, right)
          if (isCarbonSupportedDataTypes(left) && isCarbonSupportedDataTypes(right)) =>
          Some(new OrExpression(transformExpression(left).get, transformExpression(right).get))
        case And(left, right)
          if (isCarbonSupportedDataTypes(left) && isCarbonSupportedDataTypes(right)) =>
          Some(new AndExpression(transformExpression(left).get, transformExpression(right).get))
        case EqualTo(left, right)
          if (isCarbonSupportedDataTypes(left) && isCarbonSupportedDataTypes(right)) =>
          Some(new EqualToExpression(transformExpression(left).get, transformExpression(right).get))
        case Not(EqualTo(left, right))
          if (isCarbonSupportedDataTypes(left) && isCarbonSupportedDataTypes(right)) =>
          Some(new NotEqualsExpression(transformExpression(left).get,
            transformExpression(right).get))
        case IsNotNull(child)
          if (isCarbonSupportedDataTypes(child)) =>
          Some(new NotEqualsExpression(transformExpression(child).get,
            transformExpression(Literal(null)).get,
            true))
        case IsNull(child)
          if (isCarbonSupportedDataTypes(child)) =>
          Some(new EqualToExpression(transformExpression(child).get,
            transformExpression(Literal(null)).get,
            true))
        case Not(In(a: Attribute, list)) if !list.exists(!_.isInstanceOf[Literal]) =>
          Some(new NotInExpression(transformExpression(a).get,
            new ListExpression(convertToJavaList(list.map(transformExpression(_).get)))))
        case In(left, right)
          if (isCarbonSupportedDataTypes(left)) =>
          Some(new InExpression(transformExpression(left).get,
            new ListExpression(right.map(transformExpression(_).get).asJava)
          ))
        case Add(left, right)
          if (isCarbonSupportedDataTypes(left) && isCarbonSupportedDataTypes(right)) =>
          Some(new AddExpression(transformExpression(left).get, transformExpression(right).get))
        case Subtract(left, right)
          if (isCarbonSupportedDataTypes(left) && isCarbonSupportedDataTypes(right)) =>
          Some(new SubstractExpression(transformExpression(left).get,
            transformExpression(right).get))
        case Multiply(left, right)
          if (isCarbonSupportedDataTypes(left) && isCarbonSupportedDataTypes(right)) =>
          Some(new MultiplyExpression(transformExpression(left).get,
            transformExpression(right).get))
        case Divide(left, right)
          if (isCarbonSupportedDataTypes(left) && isCarbonSupportedDataTypes(right)) =>
          Some(new DivideExpression(transformExpression(left).get, transformExpression(right).get))
        case GreaterThan(left, right)
          if (isCarbonSupportedDataTypes(left) && isCarbonSupportedDataTypes(right)) =>
          Some(new GreaterThanExpression(transformExpression(left).get,
            transformExpression(right).get))
        case LessThan(left, right)
          if (isCarbonSupportedDataTypes(left) && isCarbonSupportedDataTypes(right)) =>
          Some(new LessThanExpression(transformExpression(left).get,
            transformExpression(right).get))
        case GreaterThanOrEqual(left, right)
          if (isCarbonSupportedDataTypes(left) && isCarbonSupportedDataTypes(right)) =>
          Some(new GreaterThanEqualToExpression(transformExpression(left).get,
            transformExpression(right).get))
        case LessThanOrEqual(left, right)
          if (isCarbonSupportedDataTypes(left) && isCarbonSupportedDataTypes(right)) =>
          Some(new LessThanEqualToExpression(transformExpression(left).get,
            transformExpression(right).get))
        // convert StartWith('abc') or like(col 'abc%') to col >= 'abc' and col < 'abd'
        case StartsWith(left, right@Literal(pattern, dataType))
          if (pattern.toString.size > 0 &&
              isCarbonSupportedDataTypes(left) && isCarbonSupportedDataTypes(right)) =>
          val l = new GreaterThanEqualToExpression(
            transformExpression(left).get, transformExpression(right).get
          )
          val value = pattern.toString
          val maxValueLimit = value.substring(0, value.length - 1) +
                              (value.charAt(value.length - 1).toInt + 1).toChar
          val r = new LessThanExpression(
            transformExpression(left).get,
            new CarbonLiteralExpression(maxValueLimit,
              CarbonScalaUtil.convertSparkToCarbonDataType(dataType)
            )
          )
          Some(new AndExpression(l, r))
        case AttributeReference(name, dataType, _, _) => Some(new CarbonColumnExpression(name
          .toString,
          CarbonScalaUtil.convertSparkToCarbonDataType(getActualCarbonDataType(name, carbonTable))
        ))
        case Literal(name, dataType) =>
          Some(new CarbonLiteralExpression(name,
            CarbonScalaUtil.convertSparkToCarbonDataType(dataType)))
        case StringTrim(child) => transformExpression(child)
        case aggExpr: AggregateExpression =>
          throw new UnsupportedOperationException(s"Cannot evaluate expression: $aggExpr")
        case _ =>
          Some(new SparkUnknownExpression(expr.transform {
            case AttributeReference(name, dataType, _, _) =>
              CarbonBoundReference(new CarbonColumnExpression(name.toString,
                CarbonScalaUtil.convertSparkToCarbonDataType(dataType)
              ), dataType, expr.nullable
              )
          })
          )
      }
    }
    exprs.flatMap(transformExpression(_, false)).reduceOption(new AndExpression(_, _))
  }

  def isCarbonSupportedDataTypes(expr: Expression): Boolean = {
    expr.dataType match {
      case StringType => true
      case IntegerType => true
      case LongType => true
      case DoubleType => true
      case FloatType => true
      case BooleanType => true
      case TimestampType => true
      case ArrayType(_, _) => true
      case StructType(_) => true
      case DecimalType() => true
      case _ => false
    }
  }

  private def getActualCarbonDataType(column: String, carbonTable: CarbonTable) = {
    var carbonColumn: CarbonColumn =
      carbonTable.getDimensionByName(carbonTable.getFactTableName, column)
    val dataType = if (carbonColumn != null) {
      carbonColumn.getDataType
    } else {
      carbonColumn = carbonTable.getMeasureByName(carbonTable.getFactTableName, column)
      carbonColumn.getDataType match {
        case DataType.INT => DataType.LONG
        case DataType.LONG => DataType.LONG
        case DataType.DECIMAL => DataType.DECIMAL
        case _ => DataType.DOUBLE
      }
    }
    CarbonScalaUtil.convertCarbonToSparkDataType(dataType)
  }

  // Convert scala list to java list, Cannot use scalaList.asJava as while deserializing it is
  // not able find the classes inside scala list and gives ClassNotFoundException.
  private def convertToJavaList(
      scalaList: Seq[CarbonExpression]): java.util.List[CarbonExpression] = {
    val javaList = new java.util.ArrayList[CarbonExpression]()
    scalaList.foreach(javaList.add)
    javaList
  }

  def preProcessExpressions(expressions: Seq[Expression]): Seq[Expression] = {
    expressions match {
      case left :: right :: rest => preProcessExpressions(List(And(left, right)) ::: rest)
      case List(left, right) => List(And(left, right))

      case _ => expressions
    }
  }
}
