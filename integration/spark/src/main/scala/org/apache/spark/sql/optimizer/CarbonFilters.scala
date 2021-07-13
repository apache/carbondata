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

import java.util.ArrayList

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.spark.sql.{CarbonBoundReference, CarbonDatasourceHadoopRelation, CarbonEnv,
  SparkSession, SparkUnknownExpression}
import org.apache.spark.sql.carbondata.execution.datasources.CarbonSparkDataSourceUtil
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.apache.spark.sql.catalyst.expressions.{And, ArrayContains, Attribute,
  AttributeReference, Cast, Contains, EmptyRow, EndsWith, EqualTo, GreaterThan,
  GreaterThanOrEqual, In, InSet, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Not, Or,
  ScalaUDF, StartsWith, StringTrim}
import org.apache.spark.sql.catalyst.expressions.{Expression => SparkExpression}
import org.apache.spark.sql.execution.CastExpressionOptimization
import org.apache.spark.sql.execution.command.mutation.merge.udf.BlockPathsUDF
import org.apache.spark.sql.hive.{CarbonHiveIndexMetadataUtil, CarbonSessionCatalogUtil}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DecimalType, DoubleType, FloatType,
  IntegerType, LongType, MapType, StringType, StructType, TimestampType}
import org.apache.spark.sql.types.{DataType => SparkDataType}
import org.apache.spark.sql.util.SparkSQLUtil
import org.apache.spark.util.{CarbonReflectionUtils, SparkUtil}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.metadata.datatype.{DataType, DataTypes}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.scan.expression.{ColumnExpression, Expression,
  LiteralExpression, MatchExpression}
import org.apache.carbondata.core.scan.expression.conditional.{CDCBlockImplicitExpression,
  EqualToExpression, GreaterThanEqualToExpression, GreaterThanExpression, ImplicitExpression,
  InExpression, LessThanEqualToExpression, LessThanExpression, ListExpression, NotEqualsExpression,
  NotInExpression, StartsWithExpression}
import org.apache.carbondata.core.scan.expression.logical.{AndExpression, FalseExpression,
  OrExpression}
import org.apache.carbondata.core.scan.filter.intf.ExpressionType
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.geo.{GeoUtils, InPolygonListUDF, InPolygonRangeListUDF,
  InPolygonUDF, InPolylineListUDF}
import org.apache.carbondata.geo.scan.expression.{PolygonExpression, PolygonListExpression,
  PolygonRangeListExpression, PolylineListExpression}
import org.apache.carbondata.index.{TextMatchMaxDocUDF, TextMatchUDF}

/**
 * All filter conversions are done here.
 */
object CarbonFilters {

  /**
   * translate spark [[Expression]] into carbon [[Expression]].
   *
   * @return a `Some[Expression]` if the input [[Expression]] is convertible, otherwise a `None`.
   */
  protected[sql] def translateExpression(
      relation: CarbonDatasourceHadoopRelation,
      predicate: SparkExpression,
      columnTypes: Map[String, SparkDataType],
      isOr: Boolean = false): Option[Expression] = {

    predicate match {
      case u: ScalaUDF =>
        translateUDF(u.children, u.function, relation)
      case Or(left, right) =>
        translateOr(left, right, relation, columnTypes)
      case And(left, right) =>
        translateAnd(left, right, relation, columnTypes, isOr)
      case EqualTo(a: Attribute, Literal(v, _)) =>
        translateEqualTo(a.name, v, columnTypes)
      case EqualTo(Literal(v, _), a: Attribute) =>
        translateEqualTo(a.name, v, columnTypes)
      case c@EqualTo(Cast(_: Attribute, _, _), _: Literal) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case c@EqualTo(_: Literal, Cast(_: Attribute, _, _)) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case Not(EqualTo(a: Attribute, Literal(v, _))) =>
        translateNotEqualTo(a.name, v, columnTypes)
      case Not(EqualTo(Literal(v, _), a: Attribute)) =>
        translateNotEqualTo(a.name, v, columnTypes)
      case c@Not(EqualTo(Cast(_: Attribute, _, _), _: Literal)) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case c@Not(EqualTo(_: Literal, Cast(_: Attribute, _, _))) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case IsNotNull(a: Attribute) =>
        translateNotEqualTo(a.name, null, columnTypes, true)
      case IsNull(a: Attribute) =>
        translateEqualTo(a.name, null, columnTypes, true)
      case Not(In(a: Attribute, list)) if list.forall(_.isInstanceOf[Literal]) =>
        translateNotIn(a.name, list.map(e => e.eval(EmptyRow)), columnTypes)
      case In(a: Attribute, list) if list.forall(_.isInstanceOf[Literal]) =>
        translateIn(a.name, list.map(e => e.eval(EmptyRow)), columnTypes)
      case c@Not(In(Cast(_: Attribute, _, _), list)) if list.forall(_.isInstanceOf[Literal]) =>
        Some(transformExpression(c))
      case c@In(Cast(_: Attribute, _, _), list) if list.forall(_.isInstanceOf[Literal]) =>
        Some(transformExpression(c))
      case InSet(a: Attribute, set) =>
        translateIn(a.name, set.toSeq, columnTypes)
      case Not(InSet(a: Attribute, set)) =>
        translateNotIn(a.name, set.toSeq, columnTypes)
      case GreaterThan(a: Attribute, Literal(v, _)) =>
        translateGreaterThan(a.name, v, columnTypes)
      case GreaterThan(Literal(v, _), a: Attribute) =>
        translateLessThan(a.name, v, columnTypes)
      case c@GreaterThan(Cast(_: Attribute, _, _), _: Literal) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case c@GreaterThan(_: Literal, Cast(_: Attribute, _, _)) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case LessThan(a: Attribute, Literal(v, _)) =>
        translateLessThan(a.name, v, columnTypes)
      case LessThan(Literal(v, _), a: Attribute) =>
        translateGreaterThan(a.name, v, columnTypes)
      case c@LessThan(Cast(_: Attribute, _, _), _: Literal) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case c@LessThan(_: Literal, Cast(_: Attribute, _, _)) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case GreaterThanOrEqual(a: Attribute, Literal(v, _)) =>
        translateGreaterThanEqual(a.name, v, columnTypes)
      case GreaterThanOrEqual(Literal(v, _), a: Attribute) =>
        translateLessThanEqual(a.name, v, columnTypes)
      case c@GreaterThanOrEqual(Cast(_: Attribute, _, _), _: Literal) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case c@GreaterThanOrEqual(_: Literal, Cast(_: Attribute, _, _)) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case LessThanOrEqual(a: Attribute, Literal(v, _)) =>
        translateLessThanEqual(a.name, v, columnTypes)
      case LessThanOrEqual(Literal(v, _), a: Attribute) =>
        translateGreaterThanEqual(a.name, v, columnTypes)
      case c@LessThanOrEqual(Cast(_: Attribute, _, _), Literal(v, t)) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case c@LessThanOrEqual(_: Literal, Cast(_: Attribute, _, _)) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case StartsWith(a: Attribute, Literal(v, _)) if v.toString.nonEmpty =>
        translateStartsWith(a.name, v, columnTypes)
      case c@EndsWith(_: Attribute, _: Literal) =>
        Some(getSparkUnknownExpression(c, ExpressionType.ENDSWITH))
      case c@Contains(_: Attribute, _: Literal) =>
        Some(getSparkUnknownExpression(c, ExpressionType.CONTAINSWITH))
      case Literal(v, _) if v == null =>
        Some(new FalseExpression(null))
      case ArrayContains(a: Attribute, Literal(v, _)) =>
        translateArrayContains(a, v, columnTypes)
      case ac@ArrayContains(Cast(_: Attribute, _, _), _: Literal) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(EqualTo(ac.left, ac.right))
      case _ => None
    }
  }

  private def translateArrayContains(a: Attribute,
      v: Any,
      columnTypes: Map[String, SparkDataType]) = {
    a.dataType match {
      case arrayType: ArrayType =>
        if (SparkUtil.isPrimitiveType(arrayType.elementType)) {
          val dt = translateDataType(a.name, columnTypes)
          Some(new EqualToExpression(translateColumn(a.name, dt), translateLiteral(v, dt)))
        } else {
          None
        }
      case _ =>
        None
    }
  }

  private def translateStartsWith(
      name: String,
      v: Any,
      columnTypes: Map[String, SparkDataType]): Option[StartsWithExpression] = {
    val dt = translateDataType(name, columnTypes)
    Some(new StartsWithExpression(translateColumn(name, dt), translateLiteral(v.toString, dt)))
  }

  private def translateLessThanEqual(
      name: String,
      v: Any,
      columnTypes: Map[String, SparkDataType]): Option[LessThanEqualToExpression] = {
    val dt = translateDataType(name, columnTypes)
    Some(new LessThanEqualToExpression(translateColumn(name, dt), translateLiteral(v, dt)))
  }

  private def translateGreaterThanEqual(
      name: String,
      v: Any,
      columnTypes: Map[String, SparkDataType]): Option[GreaterThanEqualToExpression] = {
    val dt = translateDataType(name, columnTypes)
    Some(new GreaterThanEqualToExpression(translateColumn(name, dt), translateLiteral(v, dt)))
  }

  private def translateLessThan(
      name: String,
      v: Any,
      columnTypes: Map[String, SparkDataType]): Option[LessThanExpression] = {
    val dt = translateDataType(name, columnTypes)
    Some(new LessThanExpression(translateColumn(name, dt), translateLiteral(v, dt)))
  }

  private def translateGreaterThan(
      name: String,
      v: Any,
      columnTypes: Map[String, SparkDataType]): Option[GreaterThanExpression] = {
    val dt = translateDataType(name, columnTypes)
    Some(new GreaterThanExpression(translateColumn(name, dt), translateLiteral(v, dt)))
  }

  def translateUDF(
      children: Seq[SparkExpression],
      function: AnyRef,
      relation: CarbonDatasourceHadoopRelation): Option[Expression] = {
    function match {
      case _: TextMatchUDF =>
        if (children.size > 1) {
          throw new MalformedCarbonCommandException(
            "TEXT_MATCH UDF syntax: TEXT_MATCH('luceneQuerySyntax')")
        }
        Some(new MatchExpression(children.head.toString()))
      case _: TextMatchMaxDocUDF =>
        if (children.size > 2) {
          throw new MalformedCarbonCommandException(
            "TEXT_MATCH UDF syntax: TEXT_MATCH_LIMIT('luceneQuerySyntax')")
        }
        Some(new MatchExpression(children.head.toString(),
          Try(children.last.toString().toInt).getOrElse(Integer.MAX_VALUE)))
      case _: InPolygonUDF =>
        if (children.size > 1) {
          throw new MalformedCarbonCommandException("Expect one string in polygon")
        }
        val (columnName, instance) = getGeoHashHandler(relation.carbonTable)
        Some(new PolygonExpression(children.head.toString(), columnName, instance))
      case _: InPolygonListUDF =>
        if (children.size != 2) {
          throw new MalformedCarbonCommandException("Expect two string in polygon list")
        }
        var polyGonStr = children.head.toString()
        // check if the expression is polygon list or select query
        val isPolyGonQuery = polyGonStr.toLowerCase().startsWith("select")
        if (isPolyGonQuery) {
          // collect the polygon list by executing query
          polyGonStr = getPolygonOrPolyLine(polyGonStr)
        }
        val (columnName, instance) = getGeoHashHandler(relation.carbonTable)
        Some(new PolygonListExpression(polyGonStr, children.last.toString(),
          columnName, instance))
      case _: InPolylineListUDF =>
        if (children.size != 2) {
          throw new MalformedCarbonCommandException("Expect two string in polyline list")
        }
        var polyLineStr = children.head.toString()
        // check if the expression is PolyLine list or select query
        val isPolyGonQuery = polyLineStr.toLowerCase().startsWith("select")
        if (isPolyGonQuery) {
          // collect the polyline linestring by executing query
          polyLineStr = getPolygonOrPolyLine(polyLineStr, isLineString = true)
        }
        val (columnName, instance) = getGeoHashHandler(relation.carbonTable)
        if (scala.util.Try(children.last.toString().toFloat).isFailure) {
          throw new MalformedCarbonCommandException("Expect buffer size to be of float type")
        }
        val bufferSize = children.last.toString().toFloat
        if (bufferSize <= 0) {
          throw new MalformedCarbonCommandException("Expect buffer size to be a positive value")
        }
        Some(new PolylineListExpression(polyLineStr,
          children.last.toString().toFloat, columnName, instance))
      case _: InPolygonRangeListUDF =>
        if (children.size != 2) {
          throw new MalformedCarbonCommandException("Expect two string in polygon range list")
        }
        val (columnName, instance) = getGeoHashHandler(relation.carbonTable)
        Some(new PolygonRangeListExpression(children.head.toString(), children.last.toString(),
          columnName, instance))
      case _: BlockPathsUDF =>
        if (children.size > 1) {
          throw new MalformedCarbonCommandException(
            "Expected one comma separated values of block paths")
        }
        Some(new CDCBlockImplicitExpression(children.head.toString()))
      case _ => None
    }
  }

  private def getGeoHashHandler(carbonTable: CarbonTable) = {
    val tableProperties = carbonTable.getTableInfo.getFactTable.getTableProperties.asScala
    GeoUtils.getGeoHashHandler(tableProperties)
  }

  def getPolygonOrPolyLine(query: String, isLineString: Boolean = false): String = {
    val dataFrame = SparkSQLUtil.getSparkSession.sql(query)
    // check if more than one column present in query projection
    if (dataFrame.columns.length != 1) {
      val udf = if (isLineString) {
        "PolyLine"
      } else {
        "Polygon"
      }
      throw new UnsupportedOperationException(
        s"More than one column exists in the query for $udf List Udf")
    }
    dataFrame.collect()
      .mkString(",")
      .replace("[", "")
      .replace("]", "")
  }

  def translateOr(
      left : SparkExpression,
      right: SparkExpression,
      relation: CarbonDatasourceHadoopRelation,
      columnTypes: Map[String, SparkDataType]) : Option[OrExpression] = {
    val leftExpression = translateExpression(relation, left, columnTypes, true)
    val rightExpression = translateExpression(relation, right, columnTypes, true)
    if (leftExpression.isDefined && rightExpression.isDefined) {
      Some(new OrExpression(leftExpression.get, rightExpression.get))
    } else {
      None
    }
  }

  def translateAnd(
      left : SparkExpression,
      right: SparkExpression,
      relation: CarbonDatasourceHadoopRelation,
      columnTypes: Map[String, SparkDataType],
      isOr: Boolean): Option[Expression] = {
    val leftExpression = translateExpression(relation, left, columnTypes, isOr)
    val rightExpression = translateExpression(relation, right, columnTypes, isOr)
    if (isOr) {
      if (leftExpression.isDefined && rightExpression.isDefined) {
        (leftExpression ++ rightExpression).reduceOption(new AndExpression(_, _))
      } else {
        None
      }
    } else {
      (leftExpression ++ rightExpression).reduceOption(new AndExpression(_, _))
    }
  }

  def translateDataType(name: String, columnTypes: Map[String, SparkDataType]): DataType = {
    CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(columnTypes(name.toLowerCase()))
  }

  def translateColumn(name: String, dataType: SparkDataType): ColumnExpression = {
    translateColumn(name, CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(dataType))
  }

  def translateColumn(name: String, dataType: DataType): ColumnExpression = {
    new ColumnExpression(name, dataType)
  }

  def translateLiteral(value: Any, dataType: SparkDataType): Expression = {
    translateLiteral(value, CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(dataType))
  }

  def translateLiteral(value: Any, dataTypeOfAttribute: DataType): Expression = {
    val dataType = if (Option(value).isDefined
      && dataTypeOfAttribute == DataTypes.STRING
      && value.isInstanceOf[Double]) {
      DataTypes.DOUBLE
    } else {
      dataTypeOfAttribute
    }
    val dataValue = if (dataTypeOfAttribute.equals(DataTypes.BINARY)
      && Option(value).isDefined) {
      new String(value.asInstanceOf[Array[Byte]])
    } else {
      value
    }
    new LiteralExpression(dataValue, dataType)
  }

  def translateEqualTo(
      name: String,
      value: Any,
      columnTypes: Map[String, SparkDataType],
      isNull: Boolean = false): Option[EqualToExpression] = {
    val dt = translateDataType(name, columnTypes)
    Some(new EqualToExpression(translateColumn(name, dt), translateLiteral(value, dt), isNull))
  }

  def translateNotEqualTo(
      name: String,
      value: Any,
      columnTypes: Map[String, SparkDataType],
      isNotNull: Boolean = false): Option[NotEqualsExpression] = {
    val dt = translateDataType(name, columnTypes)
    Some(new NotEqualsExpression(translateColumn(name, dt), translateLiteral(value, dt), isNotNull))
  }

  private def translateIn(name: String,
      list: Seq[Any],
      columnTypes: Map[String, SparkDataType]): Option[Expression] = {
    val dt = translateDataType(name, columnTypes)
    if (list.length == 1 && list.head == null) {
      Some(new FalseExpression(translateColumn(name, dt)))
    } else {
      Some(new InExpression(translateColumn(name, dt),
        new ListExpression(convertToJavaList(list.filterNot(_ == null)
            .map(filterValues => translateLiteral(filterValues, dt))))))
    }
  }

  private def translateNotIn(
      name: String,
      list: Seq[Any],
      columnTypes: Map[String, SparkDataType]): Option[Expression] = {
    val dt = translateDataType(name, columnTypes)
    if (list.contains(null)) {
      Some(new FalseExpression(translateColumn(name, dt)))
    } else {
      Some(new NotInExpression(translateColumn(name, dt),
        new ListExpression(list.map(f => translateLiteral(f, dt)).asJava)))
    }
  }

  private def getSparkUnknownExpression(expr: SparkExpression,
      exprType: ExpressionType = ExpressionType.UNKNOWN) = {
    new SparkUnknownExpression(expr.transform {
      case AttributeReference(name, dataType, _, _) =>
        CarbonBoundReference(new ColumnExpression(name,
          CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(dataType)),
          dataType, expr.nullable)
    }, exprType)
  }

  /**
   * This API checks whether StringTrim object is compatible with
   * carbon,carbon only deals with the space any other symbol should
   * be ignored.So condition is SPARK version < 2.3.
   * If it is 2.3 then trimStr field should be empty
   *
   * @param stringTrim
   * @return
   */
  def isStringTrimCompatibleWithCarbon(stringTrim: StringTrim): Boolean = {
    val trimStr = CarbonReflectionUtils.getField("trimStr", stringTrim)
      .asInstanceOf[Option[Expression]]
    if (trimStr.isDefined) {
      false
    } else {
      true
    }
  }

  def transformExpression(expr: SparkExpression): Expression = {
    expr match {
      case plan if CarbonHiveIndexMetadataUtil.checkNIUDF(plan) =>
        transformExpression(CarbonHiveIndexMetadataUtil.getNIChildren(plan))
      case Or(left, right) if canPushDown(left, right) =>
        new OrExpression(transformExpression(left), transformExpression(right))
      case And(left, right) if canPushDown(left, right) =>
        new AndExpression(transformExpression(left), transformExpression(right))
      case EqualTo(left, right) if canPushDown(left, right) =>
        new EqualToExpression(transformExpression(left), transformExpression(right))
      case Not(EqualTo(left, right)) if canPushDown(left, right) =>
        new NotEqualsExpression(transformExpression(left), transformExpression(right))
      case IsNotNull(child) if canPushDown(child) =>
        new NotEqualsExpression(transformExpression(child), transformExpression(Literal(null)),
          true)
      case IsNull(child) if canPushDown(child) =>
        new EqualToExpression(transformExpression(child), transformExpression(Literal(null)), true)
      case Not(In(left, right)) if canPushDown(left) =>
        if (right.contains(null)) {
          new FalseExpression(transformExpression(left))
        } else {
          new NotInExpression(transformExpression(left),
            new ListExpression(convertToJavaList(right.map(transformExpression)))
          )
        }
      case In(left, right) if canPushDown(left) =>
        left match {
          case left: AttributeReference
            if left.name.equalsIgnoreCase(CarbonCommonConstants.POSITION_ID) =>
            new InExpression(transformExpression(left),
              new ImplicitExpression(convertToJavaList(right.filter(_ != null)
                .filter(!isNullLiteral(_))
                .map(transformExpression))))
          case _ =>
            new InExpression(transformExpression(left), new ListExpression(convertToJavaList(
              right.filter(_ != null).filter(!isNullLiteral(_)).map(transformExpression))))
        }
      case InSet(left, right) if canPushDown(left) =>
        val validData = right.filter(_ != null).map { x =>
          transformExpression(Literal(x.toString))
        }.toList
        new InExpression(transformExpression(left),
          new ListExpression(convertToJavaList(validData)))
      case Not(InSet(left, right)) if canPushDown(left) =>
        if (right.contains(null)) {
          new FalseExpression(transformExpression(left))
        } else {
          val r = right.map { x =>
            val strVal = if (null == x) {
              x
            } else {
              x.toString
            }
            transformExpression(Literal(strVal))
          }.toList
          new NotInExpression(transformExpression(left), new ListExpression(convertToJavaList(r)))
        }
      case GreaterThan(left, right) if canPushDown(left, right) =>
        new GreaterThanExpression(transformExpression(left), transformExpression(right))
      case GreaterThanOrEqual(left, right) if canPushDown(left, right) =>
        new GreaterThanEqualToExpression(transformExpression(left), transformExpression(right))
      case LessThan(left, right) if canPushDown(left, right) =>
        new LessThanExpression(transformExpression(left), transformExpression(right))
      case LessThanOrEqual(left, right) if canPushDown(left, right) =>
        new LessThanEqualToExpression(transformExpression(left), transformExpression(right))
      case AttributeReference(name, dataType, _, _) =>
        new ColumnExpression(name, CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(dataType))
      case Literal(name, dataType) => new
          LiteralExpression(name,
            CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(dataType))
      case StartsWith(left, right@Literal(pattern, dataType))
        if pattern.toString.nonEmpty && canPushDown(left, right) =>
        val l = new GreaterThanEqualToExpression(transformExpression(left),
          transformExpression(right))
        val patternText = pattern.toString
        val lastChar = (patternText.charAt(patternText.length - 1).toInt + 1).toChar
        val maxValueLimit = patternText.substring(0, patternText.length - 1) + lastChar
        val r = new LessThanExpression(transformExpression(left),
          new LiteralExpression(maxValueLimit,
            CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(dataType)))
        new AndExpression(l, r)
      case strTrim: StringTrim if isStringTrimCompatibleWithCarbon(strTrim) =>
        transformExpression(strTrim)
      case _ => getSparkUnknownExpression(expr)
    }
  }

  private def isNullLiteral(exp: SparkExpression): Boolean = {
    if (null != exp
        && exp.isInstanceOf[Literal]
        && (exp.asInstanceOf[Literal].dataType == org.apache.spark.sql.types.DataTypes.NullType)
        || (exp.asInstanceOf[Literal].value == null)) {
      true
    } else {
      false
    }
  }

  def canPushDown(expr: SparkExpression): Boolean = {
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
      case MapType(_, _, _) => true
      case DecimalType() => true
      case _ => false
    }
  }

  def canPushDown(left: SparkExpression, right: SparkExpression): Boolean = {
    canPushDown(left) && canPushDown(right)
  }

  // Convert scala list to java list, Cannot use scalaList.asJava as while deserializing it is
  // not able find the classes inside scala list and gives ClassNotFoundException.
  private def convertToJavaList(scalaList: Seq[Expression]): java.util.List[Expression] = {
    CarbonSparkDataSourceUtil.convertToJavaList(scalaList)
  }

  def preProcessExpressions(expressions: Seq[SparkExpression]): Seq[SparkExpression] = {
    expressions match {
      case left :: right :: rest => preProcessExpressions(List(And(left, right)) ::: rest)
      case List(left, right) => List(And(left, right))

      case _ => expressions
    }
  }

  def getCurrentPartitions(sparkSession: SparkSession,
      tableIdentifier: TableIdentifier): Option[Seq[PartitionSpec]] = {
    getPartitions(
      Seq.empty,
      sparkSession,
      tableIdentifier)
  }

  def getCurrentPartitions(sparkSession: SparkSession,
      carbonTable: CarbonTable): Option[Seq[PartitionSpec]] = {
    getPartitions(
      Seq.empty,
      sparkSession,
      carbonTable)
  }

  def getPartitions(partitionFilters: Seq[SparkExpression],
      sparkSession: SparkSession,
      identifier: TableIdentifier): Option[Seq[PartitionSpec]] = {
    val carbonTable = CarbonEnv.getCarbonTable(identifier)(sparkSession)
    getPartitions(partitionFilters, sparkSession, carbonTable)
  }

  /**
   * Fetches partition information from hive
   */
  def getPartitions(partitionFilters: Seq[SparkExpression],
      sparkSession: SparkSession,
      carbonTable: CarbonTable): Option[Seq[PartitionSpec]] = {
    if (!carbonTable.isHivePartitionTable) {
      return None
    }
    val partitions = getCatalogTablePartitions(partitionFilters, sparkSession, carbonTable)
    Some(convertToPartitionSpec(partitions))
  }

  def convertToPartitionSpec(partitions: Seq[CatalogTablePartition]): Seq[PartitionSpec] = {
    partitions.map { partition =>
      new PartitionSpec(
        new ArrayList[String](partition.spec.seq.map { case (column, value) =>
          column + "=" + value
        }.toList.asJava), partition.location)
    }
  }

  def getCatalogTablePartitions(
      partitionFilters: Seq[SparkExpression],
      sparkSession: SparkSession,
      carbonTable: CarbonTable): Seq[CatalogTablePartition] = {
    val identifier = TableIdentifier(carbonTable.getTableName, Some(carbonTable.getDatabaseName))
    try {
      if (CarbonProperties.getInstance().
        getProperty(CarbonCommonConstants.CARBON_READ_PARTITION_HIVE_DIRECT,
          CarbonCommonConstants.CARBON_READ_PARTITION_HIVE_DIRECT_DEFAULT).toBoolean) {
        // read partitions directly from hive metastore using filters
        sparkSession.sessionState.catalog.listPartitionsByFilter(identifier, partitionFilters)
      } else {
        // Read partitions alternatively by first get all partitions then filter them
        CarbonSessionCatalogUtil.getPartitionsAlternate(
          partitionFilters,
          sparkSession,
          carbonTable)
      }
    } catch {
      case e: Exception =>
        // Get partition information alternatively.
        CarbonSessionCatalogUtil.getPartitionsAlternate(
          partitionFilters,
          sparkSession,
          carbonTable)
    }
  }

  def getCatalogTablePartitions(
      partitionFilters: Seq[SparkExpression],
      sparkSession: SparkSession,
      identifier: TableIdentifier
  ): (Seq[CatalogTablePartition], Seq[PartitionSpec], Seq[SparkExpression]) = {
    val carbonTable = CarbonEnv.getCarbonTable(identifier)(sparkSession)
    if (!carbonTable.isHivePartitionTable) {
      return (null, null, partitionFilters)
    }
    val partitions = getCatalogTablePartitions(partitionFilters, sparkSession, carbonTable)
    if (partitions.isEmpty) {
      (Seq.empty, Seq.empty, partitionFilters)
    } else {
      (partitions, convertToPartitionSpec(partitions), partitionFilters)
    }
  }
}
