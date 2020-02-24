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
import scala.collection.mutable
import scala.util.Try

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.CarbonContainsWith
import org.apache.spark.sql.CarbonEndsWith
import org.apache.spark.sql.carbondata.execution.datasources.CarbonSparkDataSourceUtil
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.{CarbonHiveMetadataUtil, CarbonSessionCatalogUtil}
import org.apache.spark.util.{CarbonReflectionUtils, SparkUtil}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.metadata.datatype.{DataTypes => CarbonDataTypes}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.scan.expression.{ColumnExpression => CarbonColumnExpression, Expression => CarbonExpression, LiteralExpression => CarbonLiteralExpression}
import org.apache.carbondata.core.scan.expression.conditional._
import org.apache.carbondata.core.scan.expression.logical.{AndExpression, FalseExpression, OrExpression}
import org.apache.carbondata.core.scan.expression.MatchExpression
import org.apache.carbondata.core.scan.filter.intf.ExpressionType
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.datamap.{TextMatch, TextMatchLimit}
import org.apache.carbondata.geo.{GeoUtils, InPolygon}
import org.apache.carbondata.geo.scan.expression.{PolygonExpression => CarbonPolygonExpression}

/**
 * All filter conversions are done here.
 */
object CarbonFilters {

  val carbonProperties = CarbonProperties.getInstance()
  /**
   * Converts data sources filters to carbon filter predicates.
   */
  def createCarbonFilter(schema: StructType,
      predicate: sources.Filter,
      tableProperties: mutable.Map[String, String]): Option[CarbonExpression] = {
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
        case InPolygon(queryString) =>
          val (columnName, handler) = GeoUtils.getGeoHashHandler(tableProperties)
          Some(new CarbonPolygonExpression(queryString, columnName, handler))
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

  def transformExpression(expr: Expression): CarbonExpression = {
    expr match {
      case plan if (CarbonHiveMetadataUtil.checkNIUDF(plan)) =>
        transformExpression(CarbonHiveMetadataUtil.getNIChildren(plan))
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
        new ArrayList[String]( partition.spec.seq.map{case (column, value) =>
          column + "=" + value}.toList.asJava), partition.location)
    })
  }

}
