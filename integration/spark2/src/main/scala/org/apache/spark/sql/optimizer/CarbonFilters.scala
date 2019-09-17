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

import java.util

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.CastExpressionOptimization
import org.apache.spark.sql.types._
import org.apache.spark.sql.CarbonContainsWith
import org.apache.spark.sql.CarbonEndsWith
import org.apache.spark.sql.CarbonExpressions.{MatchCast => Cast}
import org.apache.spark.sql.carbondata.execution.datasources.CarbonSparkDataSourceUtil
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.CarbonSessionCatalogUtil
import org.apache.spark.util.{CarbonReflectionUtils, SparkUtil}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.datatype.{DataTypes => CarbonDataTypes}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.scan.expression.{ColumnExpression => CarbonColumnExpression, Expression => CarbonExpression, LiteralExpression => CarbonLiteralExpression, MatchExpression}
import org.apache.carbondata.core.scan.expression.conditional._
import org.apache.carbondata.core.scan.expression.logical.{AndExpression, FalseExpression, OrExpression}
import org.apache.carbondata.core.scan.filter.intf.ExpressionType
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.ThreadLocalSessionInfo
import org.apache.carbondata.datamap.{TextMatch, TextMatchLimit}
import org.apache.carbondata.spark.CarbonAliasDecoderRelation



/**
 * All filter conversions are done here.
 */
object CarbonFilters {

  val carbonProperties = CarbonProperties.getInstance()
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
          if (values.length == 1 && values(0) == null) {
            Some(new FalseExpression(getCarbonExpression(name)))
          } else {
            Some(new InExpression(getCarbonExpression(name),
              new ListExpression(
                convertToJavaList(values.filterNot(_ == null)
                  .map(filterValues => getCarbonLiteralExpression(name, filterValues)).toList))))
          }
        case sources.Not(sources.In(name, values)) =>
          if (values.contains(null)) {
            Some(new FalseExpression(getCarbonExpression(name)))
          } else {
            Some(new NotInExpression(getCarbonExpression(name),
              new ListExpression(
                convertToJavaList(values.map(f => getCarbonLiteralExpression(name, f)).toList))))
          }
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
          Some(new StartsWithExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, value)))
        case CarbonEndsWith(expr: Expression) =>
          Some(new SparkUnknownExpression(expr.transform {
            case AttributeReference(name, dataType, _, _) =>
              CarbonBoundReference(new CarbonColumnExpression(name.toString,
                CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(dataType)),
                dataType, expr.nullable)
          }, ExpressionType.ENDSWITH))
        case CarbonContainsWith(expr: Expression) =>
          Some(new SparkUnknownExpression(expr.transform {
            case AttributeReference(name, dataType, _, _) =>
              CarbonBoundReference(new CarbonColumnExpression(name.toString,
                CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(dataType)),
                dataType, expr.nullable)
          }, ExpressionType.CONTAINSWITH))
        case CastExpr(expr: Expression) =>
          Some(transformExpression(expr))
        case FalseExpr() =>
          Some(new FalseExpression(null))
        case TextMatch(queryString) =>
          Some(new MatchExpression(queryString))
        case TextMatchLimit(queryString, maxDoc) =>
          Some(new MatchExpression(queryString, Try(maxDoc.toInt).getOrElse(Integer.MAX_VALUE)))
        case _ => None
      }
    }

    def getCarbonExpression(name: String) = {
      new CarbonColumnExpression(name,
        CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(dataTypeOf(name)))
    }

    def getCarbonLiteralExpression(name: String, value: Any): CarbonExpression = {
      val dataTypeOfAttribute =
        CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(dataTypeOf(name))
      val dataType = if (Option(value).isDefined
                         && dataTypeOfAttribute == CarbonDataTypes.STRING
                         && value.isInstanceOf[Double]) {
        CarbonDataTypes.DOUBLE
      } else {
        dataTypeOfAttribute
      }
      val dataValue = if (dataTypeOfAttribute.equals(CarbonDataTypes.BINARY)
              && Option(value).isDefined) {
        new String(value.asInstanceOf[Array[Byte]])
      } else {
        value
      }
      new CarbonLiteralExpression(dataValue, dataType)
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
        case InSet(a: Attribute, set) =>
          Some(sources.In(a.name, set.toArray))
        case Not(InSet(a: Attribute, set)) =>
          Some(sources.Not(sources.In(a.name, set.toArray)))
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
        case s@StartsWith(a: Attribute, Literal(v, t)) =>
          Some(sources.StringStartsWith(a.name, v.toString))
        case c@EndsWith(a: Attribute, Literal(v, t)) =>
          Some(CarbonEndsWith(c))
        case c@Contains(a: Attribute, Literal(v, t)) =>
          Some(CarbonContainsWith(c))
        case c@Cast(a: Attribute, _) =>
          Some(CastExpr(c))
        case c@Literal(v, t) if v == null =>
          Some(FalseExpr())
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
    var isCompatible = true
    if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      val trimStr = CarbonReflectionUtils.getField("trimStr", stringTrim)
        .asInstanceOf[Option[Expression]]
      if (trimStr.isDefined) {
        isCompatible = false
      }
    }
    isCompatible
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
        left match {
          case left: AttributeReference if (left.name
            .equalsIgnoreCase(CarbonCommonConstants.POSITION_ID)) =>
            new InExpression(transformExpression(left),
              new ImplicitExpression(convertToJavaList(right.filter(_ != null)
                .filter(!isNullLiteral(_))
                .map(transformExpression))))
          case _ =>
            new InExpression(transformExpression(left),
              new ListExpression(convertToJavaList(right.filter(_ != null).filter(!isNullLiteral(_))
                .map(transformExpression))))
        }
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
          CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(dataType))
      case Literal(name, dataType) => new
          CarbonLiteralExpression(name,
            CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(dataType))
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
            CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(dataType)))
        new AndExpression(l, r)
      case strTrim: StringTrim if isStringTrimCompatibleWithCarbon(strTrim) =>
        transformExpression(strTrim)
      case _ =>
        new SparkUnknownExpression(expr.transform {
          case AttributeReference(name, dataType, _, _) =>
            CarbonBoundReference(new CarbonColumnExpression(name.toString,
              CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(dataType)),
              dataType, expr.nullable)
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
      case MapType(_, _, _) => true
      case DecimalType() => true
      case _ => false
    }
  }

  // Convert scala list to java list, Cannot use scalaList.asJava as while deserializing it is
  // not able find the classes inside scala list and gives ClassNotFoundException.
  private def convertToJavaList(
      scalaList: Seq[CarbonExpression]): java.util.List[CarbonExpression] = {
    CarbonSparkDataSourceUtil.convertToJavaList(scalaList)
  }

  def preProcessExpressions(expressions: Seq[Expression]): Seq[Expression] = {
    expressions match {
      case left :: right :: rest => preProcessExpressions(List(And(left, right)) ::: rest)
      case List(left, right) => List(And(left, right))

      case _ => expressions
    }
  }

  def getCurrentPartitions(sparkSession: SparkSession,
      tableIdentifier: TableIdentifier): Option[Seq[PartitionSpec]] = {
    CarbonFilters.getPartitions(
      Seq.empty,
      sparkSession,
      tableIdentifier)
  }

  def getCurrentPartitions(sparkSession: SparkSession,
      carbonTable: CarbonTable): Option[Seq[PartitionSpec]] = {
    CarbonFilters.getPartitions(
      Seq.empty,
      sparkSession,
      carbonTable)
  }

  def getPartitions(partitionFilters: Seq[Expression],
      sparkSession: SparkSession,
      identifier: TableIdentifier): Option[Seq[PartitionSpec]] = {
    val carbonTable = CarbonEnv.getCarbonTable(identifier)(sparkSession)
    getPartitions(partitionFilters, sparkSession, carbonTable)
  }
  /**
   * Fetches partition information from hive
   * @param partitionFilters
   * @param sparkSession
   * @param carbonTable
   * @return
   */
  def getPartitions(partitionFilters: Seq[Expression],
      sparkSession: SparkSession,
      carbonTable: CarbonTable): Option[Seq[PartitionSpec]] = {
    val identifier = TableIdentifier(carbonTable.getTableName, Some(carbonTable.getDatabaseName))
    if (!carbonTable.isHivePartitionTable) {
      return None
    }
    // first try to read partitions in case if the trigger comes from the aggregation table load.
    val partitionsForAggTable = getPartitionsForAggTable(sparkSession, carbonTable)
    if (partitionsForAggTable.isDefined) {
      return partitionsForAggTable
    }
    val partitions = {
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
            identifier)
        }
      } catch {
        case e: Exception =>
          // Get partition information alternatively.
          CarbonSessionCatalogUtil.getPartitionsAlternate(
            partitionFilters,
            sparkSession,
            identifier)
      }
    }
    Some(partitions.map { partition =>
      new PartitionSpec(
        new util.ArrayList[String]( partition.spec.seq.map{case (column, value) =>
          column + "=" + value}.toList.asJava), partition.location)
    })
  }

  /**
   * In case of loading aggregate tables it needs to be get only from the main table load in
   * progress segment. So we should read from the segment file of that segment
   */
  def getPartitionsForAggTable(sparkSession: SparkSession,
      table: CarbonTable): Option[Seq[PartitionSpec]] = {
    // when validate segments is disabled then only read from partitionmap
    val carbonSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (carbonSessionInfo != null) {
      val validateSegments = carbonSessionInfo.getThreadParams
        .getProperty(CarbonCommonConstants.VALIDATE_CARBON_INPUT_SEGMENTS +
                     table.getDatabaseName + "." +
                     table.getTableName, "true").toBoolean
      if (!validateSegments) {
        val segmentNumbersFromProperty = carbonSessionInfo.getThreadParams
          .getProperty(CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
                       table.getDatabaseName + "." + table.getTableName, "*")
        // In case of compaction multiple segments will be passed as CARBON_INPUT_SEGMENTS.
        // Therefore partitionSpec will be extracted from all segments.
        val segments = segmentNumbersFromProperty.split(",").flatMap { a =>
          val segment = Segment.toSegment(a, null)
          val segmentFile = new SegmentFileStore(table.getTablePath, segment.getSegmentFileName)
          segmentFile.getPartitionSpecs.asScala
        }
        Some(segments.toSet.toSeq)
      } else {
        None
      }
    } else {
      None
    }
  }


}
