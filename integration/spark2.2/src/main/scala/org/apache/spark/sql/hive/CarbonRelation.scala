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

import java.util
import java.util.LinkedHashSet

import scala.Array.canBuildFrom
import scala.collection.JavaConverters._
import scala.util.parsing.combinator.RegexParsers

import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.types._

import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.datatype.DataTypes.DECIMAL
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonColumn, CarbonDimension}
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.processing.merger.TableMeta

/**
 * Represents logical plan for one carbon table
 */
case class CarbonRelation(
    databaseName: String,
    tableName: String,
    var metaData: CarbonMetaData,
    tableMeta: TableMeta)
  extends LeafNode with MultiInstanceRelation {

  def recursiveMethod(dimName: String, childDim: CarbonDimension): String = {
    childDim.getDataType.getName.toLowerCase match {
      case "array" => s"${
        childDim.getColName.substring(dimName.length + 1)
      }:array<${ getArrayChildren(childDim.getColName) }>"
      case "struct" => s"${
        childDim.getColName.substring(dimName.length + 1)
      }:struct<${ getStructChildren(childDim.getColName) }>"
      case dType => s"${ childDim.getColName.substring(dimName.length + 1) }:${ dType }"
    }
  }

  def getArrayChildren(dimName: String): String = {
    metaData.carbonTable.getChildren(dimName).asScala.map(childDim => {
      childDim.getDataType.getName.toLowerCase match {
        case "array" => s"array<${ getArrayChildren(childDim.getColName) }>"
        case "struct" => s"struct<${ getStructChildren(childDim.getColName) }>"
        case dType => addDecimalScaleAndPrecision(childDim, dType)
      }
    }).mkString(",")
  }

  def getStructChildren(dimName: String): String = {
    metaData.carbonTable.getChildren(dimName).asScala.map(childDim => {
      childDim.getDataType.getName.toLowerCase match {
        case "array" => s"${
          childDim.getColName.substring(dimName.length + 1)
        }:array<${ getArrayChildren(childDim.getColName) }>"
        case "struct" => s"${
          childDim.getColName.substring(dimName.length + 1)
        }:struct<${ metaData.carbonTable.getChildren(childDim.getColName)
          .asScala.map(f => s"${ recursiveMethod(childDim.getColName, f) }").mkString(",")
        }>"
        case dType => s"${ childDim.getColName
          .substring(dimName.length() + 1) }:${ addDecimalScaleAndPrecision(childDim, dType) }"
      }
    }).mkString(",")
  }

  override def newInstance(): LogicalPlan = {
    CarbonRelation(databaseName, tableName, metaData, tableMeta)
      .asInstanceOf[this.type]
  }

  val dimensionsAttr = {
    val sett = new LinkedHashSet(
      tableMeta.carbonTable.getDimensionByTableName(tableMeta.carbonTableIdentifier.getTableName)
        .asScala.asJava)
    sett.asScala.toSeq.map(dim => {
      val dimval = metaData.carbonTable
        .getDimensionByName(metaData.carbonTable.getFactTableName, dim.getColName)
      val output: DataType = dimval.getDataType.getName.toLowerCase match {
        case "array" =>
          CarbonMetastoreTypes.toDataType(s"array<${ getArrayChildren(dim.getColName) }>")
        case "struct" =>
          CarbonMetastoreTypes.toDataType(s"struct<${ getStructChildren(dim.getColName) }>")
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
    val factTable = tableMeta.carbonTable.getFactTableName
    new LinkedHashSet(
      tableMeta.carbonTable.
        getMeasureByTableName(tableMeta.carbonTable.getFactTableName).
        asScala.asJava).asScala.toSeq.map { x =>
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
    val columns = tableMeta.carbonTable.getCreateOrderColumn(tableMeta.carbonTable.getFactTableName)
      .asScala
    // convert each column to Attribute
    columns.filter(!_.isInvisible).map { column =>
      if (column.isDimension()) {
        val output: DataType = column.getDataType.getName.toLowerCase match {
          case "array" =>
            CarbonMetastoreTypes.toDataType(s"array<${getArrayChildren(column.getColName)}>")
          case "struct" =>
            CarbonMetastoreTypes.toDataType(s"struct<${getStructChildren(column.getColName)}>")
          case dType =>
            val dataType = addDecimalScaleAndPrecision(column, dType)
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

  def addDecimalScaleAndPrecision(dimval: CarbonColumn, dataType: String): String = {
    var dType = dataType
    if (dimval.getDataType == DECIMAL) {
      dType +=
      "(" + dimval.getColumnSchema.getPrecision + "," + dimval.getColumnSchema.getScale + ")"
    }
    dType
  }

  // TODO: Use data from the footers.
  override lazy val statistics = Statistics(sizeInBytes = this.sizeInBytes)

  override def equals(other: Any): Boolean = {
    other match {
      case p: CarbonRelation =>
        p.databaseName == databaseName && p.output == output && p.tableName == tableName
      case _ => false
    }
  }

  def addDecimalScaleAndPrecision(dimval: CarbonDimension, dataType: String): String = {
    var dType = dataType
    if (dimval.getDataType == DECIMAL) {
      dType +=
      "(" + dimval.getColumnSchema.getPrecision + "," + dimval.getColumnSchema.getScale + ")"
    }
    dType
  }

  private var tableStatusLastUpdateTime = 0L

  private var sizeInBytesLocalValue = 0L

  def sizeInBytes: Long = {
    val tableStatusNewLastUpdatedTime = SegmentStatusManager.getTableStatusLastModifiedTime(
      tableMeta.carbonTable.getAbsoluteTableIdentifier)

    if (tableStatusLastUpdateTime != tableStatusNewLastUpdatedTime) {
      val tablePath = CarbonStorePath.getCarbonTablePath(
        tableMeta.storePath,
        tableMeta.carbonTableIdentifier).getPath
      val fileType = FileFactory.getFileType(tablePath)
      if(FileFactory.isFileExist(tablePath, fileType)) {
        tableStatusLastUpdateTime = tableStatusNewLastUpdatedTime
        sizeInBytesLocalValue = FileFactory.getDirectorySize(tablePath)
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
      case failure: NoSuccess => sys.error(s"Unsupported dataType: $metastoreType")
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
