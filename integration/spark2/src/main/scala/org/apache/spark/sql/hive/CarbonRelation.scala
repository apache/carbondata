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
package org.apache.spark.sql.hive

import java.util.LinkedHashSet

import scala.Array.canBuildFrom
import scala.collection.JavaConverters._
import scala.util.parsing.combinator.RegexParsers

import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.util.SparkTypeConverter

import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonTablePath

/**
 * Represents logical plan for one carbon table
 */
case class CarbonRelation(
    databaseName: String,
    tableName: String,
    var metaData: CarbonMetaData,
    carbonTable: CarbonTable)
  extends LeafNode with MultiInstanceRelation {

  override def newInstance(): LogicalPlan = {
    CarbonRelation(databaseName, tableName, metaData, carbonTable)
      .asInstanceOf[this.type]
  }

  val dimensionsAttr: Seq[AttributeReference] = {
    val sett = new LinkedHashSet(
      carbonTable.getDimensionByTableName(carbonTable.getTableName)
        .asScala.asJava)
    sett.asScala.toSeq.map(dim => {
      val dimval = metaData.carbonTable
        .getDimensionByName(metaData.carbonTable.getTableName, dim.getColName)
      val output: DataType = dimval.getDataType.getName.toLowerCase match {
        case "array" =>
          CarbonMetastoreTypes.toDataType(
            s"array<${SparkTypeConverter.getArrayChildren(carbonTable, dim.getColName)}>")
        case "struct" =>
          CarbonMetastoreTypes.toDataType(
            s"struct<${SparkTypeConverter.getStructChildren(carbonTable, dim.getColName)}>")
        case dType =>
          val dataType = addDecimalScaleAndPrecision(dimval, dType)
          CarbonMetastoreTypes.toDataType(dataType)
      }

      AttributeReference(
        dim.getColName,
        output,
        nullable = true)()
    })
  }

  val measureAttr = {
    val factTable = carbonTable.getTableName
    new LinkedHashSet(
      carbonTable.getMeasureByTableName(carbonTable.getTableName).asScala.asJava).asScala.toSeq
      .map { x =>
      val metastoreType = metaData.carbonTable.getMeasureByName(factTable, x.getColName)
        .getDataType.getName.toLowerCase match {
        case "decimal" => "decimal(" + x.getPrecision + "," + x.getScale + ")"
        case others => others
      }
      AttributeReference(
        x.getColName,
        CarbonMetastoreTypes.toDataType(metastoreType),
        nullable = true)()
    }
  }

  override val output = {
    val columns = carbonTable.getCreateOrderColumn(carbonTable.getTableName)
      .asScala
    // convert each column to Attribute
    columns.filter(!_.isInvisible).map { column =>
      if (column.isDimension()) {
        val output: DataType = column.getDataType.getName.toLowerCase match {
          case "array" =>
            CarbonMetastoreTypes.toDataType(
              s"array<${SparkTypeConverter.getArrayChildren(carbonTable, column.getColName)}>")
          case "struct" =>
            CarbonMetastoreTypes.toDataType(
              s"struct<${SparkTypeConverter.getStructChildren(carbonTable, column.getColName)}>")
          case dType =>
            val dataType = SparkTypeConverter.addDecimalScaleAndPrecision(column, dType)
            CarbonMetastoreTypes.toDataType(dataType)
        }
        AttributeReference(column.getColName, output, nullable = true )(
          qualifier = Option(tableName + "." + column.getColName))
      } else {
        val output = CarbonMetastoreTypes.toDataType {
          column.getDataType.getName.toLowerCase match {
            case "decimal" => "decimal(" + column.getColumnSchema.getPrecision + "," + column
              .getColumnSchema.getScale + ")"
            case others => others
          }
        }
        AttributeReference(column.getColName, output, nullable = true)(
          qualifier = Option(tableName + "." + column.getColName))
      }
    }
  }

  // TODO: Use data from the footers.
  // TODO For 2.1
  //  override lazy val statistics = Statistics(sizeInBytes = this.sizeInBytes)
  // Todo for 2.2
  //  override def computeStats(conf: SQLConf): Statistics = Statistics(sizeInBytes =
  //  this.sizeInBytes)

  // override lazy val statistics = Statistics(sizeInBytes = this.sizeInBytes)

  override def equals(other: Any): Boolean = {
    other match {
      case p: CarbonRelation =>
        p.databaseName == databaseName && p.output == output && p.tableName == tableName
      case _ => false
    }
  }

  def addDecimalScaleAndPrecision(dimval: CarbonDimension, dataType: String): String = {
    var dType = dataType
    if (DataTypes.isDecimal(dimval.getDataType)) {
      dType +=
      "(" + dimval.getColumnSchema.getPrecision + "," + dimval.getColumnSchema.getScale + ")"
    }
    dType
  }

  private var tableStatusLastUpdateTime = 0L

  private var sizeInBytesLocalValue = 0L

  def sizeInBytes: Long = {
    val tableStatusNewLastUpdatedTime = SegmentStatusManager.getTableStatusLastModifiedTime(
      carbonTable.getAbsoluteTableIdentifier)
    if (tableStatusLastUpdateTime != tableStatusNewLastUpdatedTime) {
      if (new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier)
        .getValidAndInvalidSegments.getValidSegments.isEmpty) {
        sizeInBytesLocalValue = 0L
      } else {
        val tablePath = carbonTable.getTablePath
        val fileType = FileFactory.getFileType(tablePath)
        if (FileFactory.isFileExist(tablePath, fileType)) {
          // get the valid segments
          val segments = new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier)
            .getValidAndInvalidSegments.getValidSegments.asScala
          var size = 0L
          // for each segment calculate the size
          segments.foreach {validSeg =>
            if (validSeg.getSegmentFileName != null) {
              size = size + CarbonUtil.getSizeOfSegment(
                carbonTable.getTablePath,
                new Segment(validSeg.getSegmentNo, validSeg.getSegmentFileName))
            } else {
              size = size + FileFactory.getDirectorySize(
                CarbonTablePath.getSegmentPath(tablePath, validSeg.getSegmentNo))
            }
          }
          // update the new table status time
          tableStatusLastUpdateTime = tableStatusNewLastUpdatedTime
          // update the new size
          sizeInBytesLocalValue = size
        }
      }
    }
    sizeInBytesLocalValue
  }

}

object CarbonMetastoreTypes extends RegexParsers {
  protected lazy val primitiveType: Parser[DataType] =
    "string" ^^^ StringType |
    "float" ^^^ FloatType |
    "int" ^^^ IntegerType |
    "tinyint" ^^^ ShortType |
    "short" ^^^ ShortType |
    "double" ^^^ DoubleType |
    "long" ^^^ LongType |
    "binary" ^^^ BinaryType |
    "boolean" ^^^ BooleanType |
    fixedDecimalType |
    "decimal" ^^^ "decimal" ^^^ DecimalType(10, 0) |
    "varchar\\((\\d+)\\)".r ^^^ StringType |
    "date" ^^^ DateType |
    "timestamp" ^^^ TimestampType

  protected lazy val fixedDecimalType: Parser[DataType] =
    "decimal" ~> "(" ~> "^[1-9]\\d*".r ~ ("," ~> "^[0-9]\\d*".r <~ ")") ^^ {
      case precision ~ scale =>
        DecimalType(precision.toInt, scale.toInt)
    }

  protected lazy val arrayType: Parser[DataType] =
    "array" ~> "<" ~> dataType <~ ">" ^^ {
      case tpe => ArrayType(tpe)
    }

  protected lazy val mapType: Parser[DataType] =
    "map" ~> "<" ~> dataType ~ "," ~ dataType <~ ">" ^^ {
      case t1 ~ _ ~ t2 => MapType(t1, t2)
    }

  protected lazy val structField: Parser[StructField] =
    "[a-zA-Z0-9_]*".r ~ ":" ~ dataType ^^ {
      case name ~ _ ~ tpe => StructField(name, tpe, nullable = true)
    }

  protected lazy val structType: Parser[DataType] =
    "struct" ~> "<" ~> repsep(structField, ",") <~ ">" ^^ {
      case fields => StructType(fields)
    }

  protected lazy val dataType: Parser[DataType] =
    arrayType |
    mapType |
    structType |
    primitiveType

  def toDataType(metastoreType: String): DataType = {
    parseAll(dataType, metastoreType) match {
      case Success(result, _) => result
      case _: NoSuccess =>
        CarbonException.analysisException(s"Unsupported dataType: $metastoreType")
    }
  }

  def toMetastoreType(dt: DataType): String = {
    dt match {
      case ArrayType(elementType, _) => s"array<${ toMetastoreType(elementType) }>"
      case StructType(fields) =>
        s"struct<${
          fields.map(f => s"${ f.name }:${ toMetastoreType(f.dataType) }")
            .mkString(",")
        }>"
      case StringType => "string"
      case FloatType => "float"
      case IntegerType => "int"
      case ShortType => "tinyint"
      case DoubleType => "double"
      case LongType => "bigint"
      case BinaryType => "binary"
      case BooleanType => "boolean"
      case DecimalType() => "decimal"
      case TimestampType => "timestamp"
      case DateType => "date"
    }
  }
}
