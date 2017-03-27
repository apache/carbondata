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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.optimizer.AttributeReferenceWrapper
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.StructType

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
  def selectFilters(filters: Seq[Expression],
      attrList: java.util.HashSet[AttributeReferenceWrapper],
      aliasMap: CarbonAliasDecoderRelation): Unit = {
    def translate(expr: Expression, or: Boolean = false): Option[sources.Filter] = {
      expr match {
        case or@ Or(left, right) =>

          val leftFilter = translate(left, or = true)
          val rightFilter = translate(right, or = true)
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
        case Not(EqualTo(a: Attribute, Literal(v, t))) =>
          Some(sources.Not(sources.EqualTo(a.name, v)))
        case Not(EqualTo(Literal(v, t), a: Attribute)) =>
          Some(sources.Not(sources.EqualTo(a.name, v)))
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
        case GreaterThan(a: Attribute, Literal(v, t)) =>
          Some(sources.GreaterThan(a.name, v))
        case GreaterThan(Literal(v, t), a: Attribute) =>
          Some(sources.LessThan(a.name, v))
        case LessThan(a: Attribute, Literal(v, t)) =>
          Some(sources.LessThan(a.name, v))
        case LessThan(Literal(v, t), a: Attribute) =>
          Some(sources.GreaterThan(a.name, v))
        case GreaterThanOrEqual(a: Attribute, Literal(v, t)) =>
          Some(sources.GreaterThanOrEqual(a.name, v))
        case GreaterThanOrEqual(Literal(v, t), a: Attribute) =>
          Some(sources.LessThanOrEqual(a.name, v))
        case LessThanOrEqual(a: Attribute, Literal(v, t)) =>
          Some(sources.LessThanOrEqual(a.name, v))
        case LessThanOrEqual(Literal(v, t), a: Attribute) =>
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
      attributesNeedToDecode: java.util.HashSet[AttributeReference],
      unprocessedExprs: ArrayBuffer[Expression],
      carbonTable: CarbonTable): Option[CarbonExpression] = {
    def transformExpression(expr: Expression, or: Boolean = false): Option[CarbonExpression] = {
      expr match {
        case orFilter@ Or(left, right) =>
          val leftFilter = transformExpression(left, or = true)
          val rightFilter = transformExpression(right, or = true)
          if (leftFilter.isDefined && rightFilter.isDefined) {
            Some(new OrExpression(leftFilter.get, rightFilter.get))
          } else {
            if (!or) {
              orFilter.collect {
                case attr: AttributeReference => attributesNeedToDecode.add(attr)
              }
              unprocessedExprs += orFilter
            }
            None
          }

        case And(left, right) =>
          val leftFilter = transformExpression(left, or)
          val rightFilter = transformExpression(right, or)
          if (or) {
            if (leftFilter.isDefined && rightFilter.isDefined) {
              (leftFilter ++ rightFilter).reduceOption(new AndExpression(_, _))
            } else {
              None
            }
          } else {
            (leftFilter ++ rightFilter).reduceOption(new AndExpression(_, _))
          }


        case EqualTo(a: Attribute, l@Literal(v, t)) =>
          Some(
            new EqualToExpression(
              transformExpression(a).get,
              transformExpression(l).get
            )
          )
        case EqualTo(l@Literal(v, t), a: Attribute) =>
          Some(
            new EqualToExpression(
              transformExpression(a).get,
              transformExpression(l).get
            )
          )

        case Not(EqualTo(a: Attribute, l@Literal(v, t))) =>
          Some(
            new NotEqualsExpression(
              transformExpression(a).get,
              transformExpression(l).get
            )
          )
        case Not(EqualTo(l@Literal(v, t), a: Attribute)) =>
          Some(
            new NotEqualsExpression(
              transformExpression(a).get,
              transformExpression(l).get
            )
          )
        case IsNotNull(child: Attribute) =>
            Some(new NotEqualsExpression(transformExpression(child).get,
             transformExpression(Literal(null)).get, true))
        case IsNull(child: Attribute) =>
            Some(new EqualToExpression(transformExpression(child).get,
             transformExpression(Literal(null)).get, true))
        case Not(In(a: Attribute, list))
          if !list.exists(!_.isInstanceOf[Literal]) =>
          if (list.exists(x => isNullLiteral(x.asInstanceOf[Literal]))) {
            Some(new FalseExpression(transformExpression(a).get))
          } else {
            Some(new NotInExpression(transformExpression(a).get,
              new ListExpression(convertToJavaList(list.map(transformExpression(_).get)))))
          }
        case In(a: Attribute, list) if !list.exists(!_.isInstanceOf[Literal]) =>
          Some(new InExpression(transformExpression(a).get,
            new ListExpression(convertToJavaList(list.map(transformExpression(_).get)))))

        case GreaterThan(a: Attribute, l@Literal(v, t)) =>
          Some(new GreaterThanExpression(transformExpression(a).get, transformExpression(l).get))
        case GreaterThan(l@Literal(v, t), a: Attribute) =>
          Some(new LessThanExpression(transformExpression(a).get, transformExpression(l).get))

        case LessThan(a: Attribute, l@Literal(v, t)) =>
          Some(new LessThanExpression(transformExpression(a).get, transformExpression(l).get))
        case LessThan(l@Literal(v, t), a: Attribute) =>
          Some(new GreaterThanExpression(transformExpression(a).get, transformExpression(l).get))

        case GreaterThanOrEqual(a: Attribute, l@Literal(v, t)) =>
          Some(new GreaterThanEqualToExpression(transformExpression(a).get,
            transformExpression(l).get))
        case GreaterThanOrEqual(l@Literal(v, t), a: Attribute) =>
          Some(new LessThanEqualToExpression(transformExpression(a).get,
            transformExpression(l).get))

        case LessThanOrEqual(a: Attribute, l@Literal(v, t)) =>
          Some(new LessThanEqualToExpression(transformExpression(a).get,
            transformExpression(l).get))
        case LessThanOrEqual(l@Literal(v, t), a: Attribute) =>
          Some(new GreaterThanEqualToExpression(transformExpression(a).get,
            transformExpression(l).get))

        case AttributeReference(name, dataType, _, _) =>
          Some(new CarbonColumnExpression(name,
            CarbonScalaUtil.convertSparkToCarbonDataType(
              getActualCarbonDataType(name, carbonTable))))
        case Literal(name, dataType) => Some(new
            CarbonLiteralExpression(name, CarbonScalaUtil.convertSparkToCarbonDataType(dataType)))
        case others =>
          if (!or) {
            others.collect {
              case attr: AttributeReference => attributesNeedToDecode.add(attr)
            }
            unprocessedExprs += others
          }
          None
      }
    }
    exprs.flatMap(transformExpression(_, false)).reduceOption(new AndExpression(_, _))
  }
  private def isNullLiteral(exp: Expression): Boolean = {
    if (null != exp
        &&  exp.isInstanceOf[Literal]
        && (exp.asInstanceOf[Literal].dataType == org.apache.spark.sql.types.DataTypes.NullType)
        || (exp.asInstanceOf[Literal].value == null)) {
      true
    } else {
      false
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
}
