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

package org.carbondata.spark

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.sources
import org.apache.spark.sql.FakeCarbonCast
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.optimizer.CarbonAliasDecoderRelation
import org.apache.spark.sql.types.StructType

import org.carbondata.core.carbon.metadata.datatype.DataType
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn
import org.carbondata.scan.expression.{ColumnExpression => CarbonColumnExpression, Expression => CarbonExpression, LiteralExpression => CarbonLiteralExpression}
import org.carbondata.scan.expression.conditional._
import org.carbondata.scan.expression.logical.{AndExpression, OrExpression}
import org.carbondata.spark.util.CarbonScalaUtil

/**
 * All filter coversions are done here.
 */
object CarbonFilters {

  /**
   * Converts data sources filters to Parquet filter predicates.
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

        case sources.In(name, values) =>
          Some(new InExpression(getCarbonExpression(name),
            new ListExpression(values.map(f => getCarbonLiteralExpression(name, f)).toList.asJava)))
        case sources.Not(sources.In(name, values)) =>
          Some(new NotInExpression(getCarbonExpression(name),
            new ListExpression(values.map(f => getCarbonLiteralExpression(name, f)).toList.asJava)))

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
      attrList: java.util.HashSet[Attribute],
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
              case attr: AttributeReference => attrList.add(aliasMap.getOrElse(attr, attr))
            }
            None
          }

        case And(left, right) =>
          (translate(left) ++ translate(right)).reduceOption(sources.And)

        case EqualTo(a: Attribute, Literal(v, t)) =>
          Some(sources.EqualTo(a.name, v))
        case EqualTo(FakeCarbonCast(l@Literal(v, t), b), a: Attribute) =>
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

        case others =>
          if (!or) {
            others.collect {
              case attr: AttributeReference =>
                attrList.add(aliasMap.getOrElse(attr, attr))
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
        case or@ Or(left, right) =>
          val leftFilter = transformExpression(left, true)
          val rightFilter = transformExpression(right, true)
          if (leftFilter.isDefined && rightFilter.isDefined) {
            Some(new OrExpression(leftFilter.get, rightFilter.get))
          } else {
            or.collect {
              case attr: AttributeReference => attributesNeedToDecode.add(attr)
            }
            unprocessedExprs += or
            None
          }

        case And(left, right) =>
          (transformExpression(left) ++ transformExpression(right)).reduceOption(new
              AndExpression(_, _))

        case EqualTo(a: Attribute, FakeCarbonCast(l@Literal(v, t), b)) => new
            Some(new EqualToExpression(transformExpression(a).get, transformExpression(l).get))
        case EqualTo(FakeCarbonCast(l@Literal(v, t), b), a: Attribute) => new
            Some(new EqualToExpression(transformExpression(a).get, transformExpression(l).get))
        case EqualTo(Cast(a: Attribute, _), FakeCarbonCast(l@Literal(v, t), b)) => new
            Some(new EqualToExpression(transformExpression(a).get, transformExpression(l).get))
        case EqualTo(FakeCarbonCast(l@Literal(v, t), b), Cast(a: Attribute, _)) => new
            Some(new EqualToExpression(transformExpression(a).get, transformExpression(l).get))

        case Not(EqualTo(a: Attribute, FakeCarbonCast(l@Literal(v, t), b))) => new
            Some(new NotEqualsExpression(transformExpression(a).get, transformExpression(l).get))
        case Not(EqualTo(FakeCarbonCast(l@Literal(v, t), b), a: Attribute)) => new
            Some(new NotEqualsExpression(transformExpression(a).get, transformExpression(l).get))
        case Not(EqualTo(Cast(a: Attribute, _), FakeCarbonCast(l@Literal(v, t), b))) => new
            Some(new NotEqualsExpression(transformExpression(a).get, transformExpression(l).get))
        case Not(EqualTo(FakeCarbonCast(l@Literal(v, t), b), Cast(a: Attribute, _))) => new
            Some(new NotEqualsExpression(transformExpression(a).get, transformExpression(l).get))

        case Not(In(a: Attribute, list)) if !list.exists(!_.isInstanceOf[FakeCarbonCast]) =>
          Some(new NotInExpression(transformExpression(a).get,
            new ListExpression(list.map(transformExpression(_).get).asJava)))
        case In(a: Attribute, list) if !list.exists(!_.isInstanceOf[FakeCarbonCast]) =>
          Some(new InExpression(transformExpression(a).get,
            new ListExpression(list.map(transformExpression(_).get).asJava)))
        case Not(In(Cast(a: Attribute, _), list))
          if !list.exists(!_.isInstanceOf[FakeCarbonCast]) =>
          Some(new NotInExpression(transformExpression(a).get,
            new ListExpression(list.map(transformExpression(_).get).asJava)))
        case In(Cast(a: Attribute, _), list) if !list.exists(!_.isInstanceOf[FakeCarbonCast]) =>
          Some(new InExpression(transformExpression(a).get,
            new ListExpression(list.map(transformExpression(_).get).asJava)))

        case AttributeReference(name, dataType, _, _) =>
          Some(new CarbonColumnExpression(name,
            CarbonScalaUtil.convertSparkToCarbonDataType(
              getActualCarbonDataType(name, carbonTable))))
        case FakeCarbonCast(literal, dataType) => transformExpression(literal)
        case Literal(name, dataType) => Some(new
            CarbonLiteralExpression(name, CarbonScalaUtil.convertSparkToCarbonDataType(dataType)))
        case Cast(left, right) if !left.isInstanceOf[Literal] => transformExpression(left)
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

  private def getActualCarbonDataType(column: String, carbonTable: CarbonTable) = {
    var carbonColumn: CarbonColumn =
      carbonTable.getDimensionByName(carbonTable.getFactTableName, column)
    val dataType = if (carbonColumn != null) {
      carbonColumn.getDataType
    } else {
      carbonColumn = carbonTable.getMeasureByName(carbonTable.getFactTableName, column)
      carbonColumn.getDataType match {
        case DataType.LONG => DataType.LONG
        case DataType.DECIMAL => DataType.DECIMAL
        case _ => DataType.DOUBLE
      }
    }
    CarbonScalaUtil.convertCarbonToSparkDataType(dataType)
  }
}
