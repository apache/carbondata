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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.CastExpressionOptimization
import org.apache.spark.sql.optimizer.AttributeReferenceWrapper
import org.apache.spark.sql.CarbonBoundReference
import org.apache.spark.sql.CastExpr
import org.apache.spark.sql.sources
import org.apache.spark.sql.types._

import org.apache.carbondata.core.metadata.datatype.DataType
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn
import org.apache.carbondata.core.scan.expression.{ColumnExpression => CarbonColumnExpression, Expression => CarbonExpression, LiteralExpression => CarbonLiteralExpression}
import org.apache.carbondata.core.scan.expression.conditional._
import org.apache.carbondata.core.scan.expression.logical.{AndExpression, FalseExpression, OrExpression}
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
        case sources.IsNull(name) =>
          Some(new EqualToExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, null), true))
        case sources.IsNotNull(name) =>
          Some(new NotEqualsExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, null), true))
        case sources.And(lhs, rhs) =>
          (createFilter(lhs) ++ createFilter(rhs)).reduceOption(new AndExpression(_, _))
        case sources.Or(lhs, rhs) =>
          for {
            lhsFilter <- createFilter(lhs)
            rhsFilter <- createFilter(rhs)
          } yield {
            new OrExpression(lhsFilter, rhsFilter)
          }
        case sources.StringStartsWith(name, value) if value.length > 0 =>
          val l = new GreaterThanEqualToExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, value))
          val maxValueLimit = value.substring(0, value.length - 1) +
                              (value.charAt(value.length - 1).toInt + 1).toChar
          val r = new LessThanExpression(
            getCarbonExpression(name), getCarbonLiteralExpression(name, maxValueLimit))
          Some(new AndExpression(l, r))
        case CastExpr(expr: Expression) =>
          Some(transformExpression(expr))
        case _ => None
      }
    }

    def getCarbonExpression(name: String) = {
      new CarbonColumnExpression(name,
        CarbonScalaUtil.convertSparkToCarbonDataType(dataTypeOf(name)))
    }

    def getCarbonLiteralExpression(name: String, value: Any): CarbonExpression = {
      val dataTypeOfAttribute = CarbonScalaUtil.convertSparkToCarbonDataType(dataTypeOf(name))
      val dataType = if (Option(value).isDefined
                         && dataTypeOfAttribute == DataType.STRING
                         && value.isInstanceOf[Double]) {
        DataType.DOUBLE
      } else {
        dataTypeOfAttribute
      }
      new CarbonLiteralExpression(value, dataType)
    }

    createFilter(predicate)
  }


  // Check out which filters can be pushed down to carbon, remaining can be handled in spark layer.
  // Mostly dimension filters are only pushed down since it is faster in carbon.
  // TODO - The Filters are first converted Intermediate sources filters expression and then these
  // expressions are again converted back to CarbonExpression. Instead of two step process of
  // evaluating the filters it can be merged into a single one.
  def selectFilters(filters: Seq[Expression],
      attrList: java.util.HashSet[AttributeReferenceWrapper],
      aliasMap: CarbonAliasDecoderRelation): Unit = {
    def translate(expr: Expression, or: Boolean = false): Option[sources.Filter] = {
      expr match {
        case or@Or(left, right) =>

          val leftFilter = translate(left, or = true)
          val rightFilter = translate(right, or = true)
          if (leftFilter.isDefined && rightFilter.isDefined) {
            Some(sources.Or(leftFilter.get, rightFilter.get))
          } else {
            or.collect {
              case attr: AttributeReference =>
                attrList.add(AttributeReferenceWrapper(aliasMap.getOrElse(attr, attr)))
            }
            None
          }
        case And(left, right) =>
          val leftFilter = translate(left, or)
          val rightFilter = translate(right, or)
          if (or) {
            if (leftFilter.isDefined && rightFilter.isDefined) {
              (leftFilter ++ rightFilter).reduceOption(sources.And)
            } else {
              None
            }
          } else {
            (leftFilter ++ rightFilter).reduceOption(sources.And)
          }
        case EqualTo(a: Attribute, Literal(v, t)) =>
          Some(sources.EqualTo(a.name, v))
        case EqualTo(l@Literal(v, t), a: Attribute) =>
          Some(sources.EqualTo(a.name, v))
        case c@EqualTo(Cast(a: Attribute, _), Literal(v, t)) =>
          CastExpressionOptimization.checkIfCastCanBeRemove(c)
        case c@EqualTo(Literal(v, t), Cast(a: Attribute, _)) =>
          CastExpressionOptimization.checkIfCastCanBeRemove(c)
        case Not(EqualTo(a: Attribute, Literal(v, t))) =>
          Some(sources.Not(sources.EqualTo(a.name, v)))
        case Not(EqualTo(Literal(v, t), a: Attribute)) =>
          Some(sources.Not(sources.EqualTo(a.name, v)))
        case c@Not(EqualTo(Cast(a: Attribute, _), Literal(v, t))) =>
          CastExpressionOptimization.checkIfCastCanBeRemove(c)
        case c@Not(EqualTo(Literal(v, t), Cast(a: Attribute, _))) =>
          CastExpressionOptimization.checkIfCastCanBeRemove(c)
        case IsNotNull(a: Attribute) =>
          Some(sources.IsNotNull(a.name))
        case IsNull(a: Attribute) =>
          Some(sources.IsNull(a.name))
        case Not(In(a: Attribute, list)) if !list.exists(!_.isInstanceOf[Literal]) =>
          val hSet = list.map(e => e.eval(EmptyRow))
          Some(sources.Not(sources.In(a.name, hSet.toArray)))
        case In(a: Attribute, list) if !list.exists(!_.isInstanceOf[Literal]) =>
          val hSet = list.map(e => e.eval(EmptyRow))
          Some(sources.In(a.name, hSet.toArray))
        case c@Not(In(Cast(a: Attribute, _), list)) if !list.exists(!_.isInstanceOf[Literal]) =>
          Some(CastExpr(c))
        case c@In(Cast(a: Attribute, _), list) if !list.exists(!_.isInstanceOf[Literal]) =>
            Some(CastExpr(c))
        case GreaterThan(a: Attribute, Literal(v, t)) =>
          Some(sources.GreaterThan(a.name, v))
        case GreaterThan(Literal(v, t), a: Attribute) =>
          Some(sources.LessThan(a.name, v))
        case c@GreaterThan(Cast(a: Attribute, _), Literal(v, t)) =>
          CastExpressionOptimization.checkIfCastCanBeRemove(c)
        case c@GreaterThan(Literal(v, t), Cast(a: Attribute, _)) =>
          CastExpressionOptimization.checkIfCastCanBeRemove(c)
        case LessThan(a: Attribute, Literal(v, t)) =>
          Some(sources.LessThan(a.name, v))
        case LessThan(Literal(v, t), a: Attribute) =>
          Some(sources.GreaterThan(a.name, v))
        case c@LessThan(Cast(a: Attribute, _), Literal(v, t)) =>
          CastExpressionOptimization.checkIfCastCanBeRemove(c)
        case c@LessThan(Literal(v, t), Cast(a: Attribute, _)) =>
          CastExpressionOptimization.checkIfCastCanBeRemove(c)
        case GreaterThanOrEqual(a: Attribute, Literal(v, t)) =>
          Some(sources.GreaterThanOrEqual(a.name, v))
        case GreaterThanOrEqual(Literal(v, t), a: Attribute) =>
          Some(sources.LessThanOrEqual(a.name, v))
        case c@GreaterThanOrEqual(Cast(a: Attribute, _), Literal(v, t)) =>
          CastExpressionOptimization.checkIfCastCanBeRemove(c)
        case c@GreaterThanOrEqual(Literal(v, t), Cast(a: Attribute, _)) =>
          CastExpressionOptimization.checkIfCastCanBeRemove(c)
        case LessThanOrEqual(a: Attribute, Literal(v, t)) =>
          Some(sources.LessThanOrEqual(a.name, v))
        case LessThanOrEqual(Literal(v, t), a: Attribute) =>
          Some(sources.GreaterThanOrEqual(a.name, v))
        case c@LessThanOrEqual(Cast(a: Attribute, _), Literal(v, t)) =>
          CastExpressionOptimization.checkIfCastCanBeRemove(c)
        case c@LessThanOrEqual(Literal(v, t), Cast(a: Attribute, _)) =>
          CastExpressionOptimization.checkIfCastCanBeRemove(c)
        case StartsWith(a: Attribute, Literal(v, t)) =>
          Some(sources.StringStartsWith(a.name, v.toString))
        case c@Cast(a: Attribute, _) =>
          Some(CastExpr(c))
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

  def transformExpression(expr: Expression): CarbonExpression = {
    expr match {
      case Or(left, right)
        if (isCarbonSupportedDataTypes(left) && isCarbonSupportedDataTypes(right)) => new
          OrExpression(transformExpression(left), transformExpression(right))
      case And(left, right)
        if (isCarbonSupportedDataTypes(left) && isCarbonSupportedDataTypes(right)) => new
          AndExpression(transformExpression(left), transformExpression(right))
      case EqualTo(left, right)
        if (isCarbonSupportedDataTypes(left) && isCarbonSupportedDataTypes(right)) => new
          EqualToExpression(transformExpression(left), transformExpression(right))
      case Not(EqualTo(left, right))
        if (isCarbonSupportedDataTypes(left) && isCarbonSupportedDataTypes(right)) => new
          NotEqualsExpression(transformExpression(left), transformExpression(right))
      case IsNotNull(child)
        if (isCarbonSupportedDataTypes(child)) => new
          NotEqualsExpression(transformExpression(child), transformExpression(Literal(null)), true)
      case IsNull(child)
        if (isCarbonSupportedDataTypes(child)) => new
          EqualToExpression(transformExpression(child), transformExpression(Literal(null)), true)
      case Not(In(left, right)) if (isCarbonSupportedDataTypes(left)) =>
        if (right.contains(null)) {
          new FalseExpression(transformExpression(left))
        }
        else {
          new NotInExpression(transformExpression(left),
            new ListExpression(convertToJavaList(right.map(transformExpression)))
          )
        }
      case In(left, right) if (isCarbonSupportedDataTypes(left)) =>
        new InExpression(transformExpression(left),
          new ListExpression(convertToJavaList(right.filter(_ != null).filter(!isNullLiteral(_))
            .map(transformExpression))))
      case InSet(left, right) if (isCarbonSupportedDataTypes(left)) =>
        val validData = right.filter(_ != null).map { x =>
          val e = Literal(x.toString)
          transformExpression(e)
        }.toList
        new InExpression(transformExpression(left),
          new ListExpression(convertToJavaList(validData)))
      case Not(InSet(left, right)) if (isCarbonSupportedDataTypes(left)) =>
        if (right.contains(null)) {
          new FalseExpression(transformExpression(left))
        }
        else {
          val r = right.map { x =>
            val strVal = if (null == x) {
              x
            } else {
              x.toString
            }
            val e = Literal(strVal)
            transformExpression(e)
          }.toList
          new NotInExpression(transformExpression(left), new ListExpression(convertToJavaList(r)))
        }
      case GreaterThan(left, right)
        if (isCarbonSupportedDataTypes(left) && isCarbonSupportedDataTypes(right)) =>
        new GreaterThanExpression(transformExpression(left), transformExpression(right))
      case GreaterThanOrEqual(left, right)
        if (isCarbonSupportedDataTypes(left) && isCarbonSupportedDataTypes(right)) =>
        new GreaterThanEqualToExpression(transformExpression(left), transformExpression(right))
      case LessThan(left, right)
        if (isCarbonSupportedDataTypes(left) && isCarbonSupportedDataTypes(right)) =>
        new LessThanExpression(transformExpression(left), transformExpression(right))
      case LessThanOrEqual(left, right)
        if (isCarbonSupportedDataTypes(left) && isCarbonSupportedDataTypes(right)) =>
        new LessThanEqualToExpression(transformExpression(left), transformExpression(right))
      case AttributeReference(name, dataType, _, _) =>
        new CarbonColumnExpression(name.toString,
          CarbonScalaUtil.convertSparkToCarbonDataType(dataType))
      case Literal(name, dataType) => new
          CarbonLiteralExpression(name, CarbonScalaUtil.convertSparkToCarbonDataType(dataType))
      case StartsWith(left, right@Literal(pattern, dataType)) if pattern.toString.size > 0 &&
                                                                 isCarbonSupportedDataTypes(left) &&
                                                                 isCarbonSupportedDataTypes
                                                                 (right) =>
        val l = new GreaterThanEqualToExpression(transformExpression(left),
          transformExpression(right))
        val maxValueLimit = pattern.toString.substring(0, pattern.toString.length - 1) +
                            (pattern.toString.charAt(pattern.toString.length - 1).toInt + 1)
                              .toChar
        val r = new LessThanExpression(
          transformExpression(left),
          new CarbonLiteralExpression(maxValueLimit,
            CarbonScalaUtil.convertSparkToCarbonDataType(dataType)))
        new AndExpression(l, r)
      case StringTrim(child) => transformExpression(child)
      case _ =>
        new SparkUnknownExpression(expr.transform {
          case AttributeReference(name, dataType, _, _) =>
            CarbonBoundReference(new CarbonColumnExpression(name.toString,
              CarbonScalaUtil.convertSparkToCarbonDataType(dataType)), dataType, expr.nullable)
        }
        )
    }
  }


  private def isNullLiteral(exp: Expression): Boolean = {
    if (null != exp
        && exp.isInstanceOf[Literal]
        && (exp.asInstanceOf[Literal].dataType == org.apache.spark.sql.types.DataTypes.NullType)
        || (exp.asInstanceOf[Literal].value == null)) {
      true
    } else {
      false
    }
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
        case DataType.INT => DataType.INT
        case DataType.SHORT => DataType.SHORT
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
