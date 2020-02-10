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

import scala.collection.JavaConverters._

import org.apache.spark.sql.CarbonToSparkAdapter
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.{CarbonMetastoreTypes, SparkTypeConverter}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonColumn, CarbonDimension}
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.path.CarbonTablePath

/**
 * Represents logical plan for one carbon table
 */
case class CarbonRelation(
    databaseName: String,
    tableName: String,
    carbonTable: CarbonTable)
  extends LeafNode with MultiInstanceRelation {

  override def newInstance(): LogicalPlan = {
    CarbonRelation(databaseName, tableName, carbonTable).asInstanceOf[this.type]
  }

  val dimensionsAttr: Seq[AttributeReference] = {
    val sett = new LinkedHashSet(carbonTable.getVisibleDimensions.asScala.filterNot(_.isIndexColumn)
      .asJava)
    sett.asScala.toSeq.map(dim => {
      val dimval = carbonTable.getDimensionByName(dim.getColName)
      val output: DataType = dimval.getDataType.getName.toLowerCase match {
        case "array" =>
          CarbonMetastoreTypes.toDataType(
            s"array<${SparkTypeConverter.getArrayChildren(carbonTable, dim.getColName)}>")
        case "struct" =>
          CarbonMetastoreTypes.toDataType(
            s"struct<${SparkTypeConverter.getStructChildren(carbonTable, dim.getColName)}>")
        case "map" =>
          CarbonMetastoreTypes.toDataType(
            s"map<${SparkTypeConverter.getMapChildren(carbonTable, dim.getColName)}>")
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
      carbonTable.getVisibleMeasures.asScala.asJava).asScala.toSeq
      .map { x =>
      val metastoreType = carbonTable.getMeasureByName(x.getColName)
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
    val columns = carbonTable.getCreateOrderColumn()
      .asScala
    val partitionColumnSchemas = if (carbonTable.getPartitionInfo() != null) {
      carbonTable.getPartitionInfo.getColumnSchemaList.asScala
    } else {
      Nil
    }
    val otherColumns = columns.filterNot(a => partitionColumnSchemas.contains(a.getColumnSchema))
    val partitionColumns = columns.filter(a => partitionColumnSchemas.contains(a.getColumnSchema))

    // get column Metadata
    def getColumnMetaData(column: CarbonColumn) = {
      val columnMetaData =
        if (null != column.getColumnProperties && !column.getColumnProperties.isEmpty &&
            null != column.getColumnProperties.get(CarbonCommonConstants.COLUMN_COMMENT)) {
        new MetadataBuilder().putString(CarbonCommonConstants.COLUMN_COMMENT,
          column.getColumnProperties.get(CarbonCommonConstants.COLUMN_COMMENT)).build()
      } else {
        Metadata.empty
      }
      columnMetaData
    }

    // convert each column to Attribute
    (otherColumns ++= partitionColumns).filter(!_.isInvisible).map { column: CarbonColumn =>
      if (column.isDimension()) {
        val output: DataType = column.getDataType.getName.toLowerCase match {
          case "array" =>
            CarbonMetastoreTypes.toDataType(
              s"array<${SparkTypeConverter.getArrayChildren(carbonTable, column.getColName)}>")
          case "struct" =>
            CarbonMetastoreTypes.toDataType(
              s"struct<${SparkTypeConverter.getStructChildren(carbonTable, column.getColName)}>")
          case "map" =>
            CarbonMetastoreTypes.toDataType(
              s"map<${SparkTypeConverter.getMapChildren(carbonTable, column.getColName)}>")
          case dType =>
            val dataType = SparkTypeConverter.addDecimalScaleAndPrecision(column, dType)
            CarbonMetastoreTypes.toDataType(dataType)
        }
        CarbonToSparkAdapter.createAttributeReference(
          column.getColName, output, nullable = true, getColumnMetaData(column),
          NamedExpression.newExprId, qualifier = Option(tableName + "." + column.getColName))
      } else {
        val output = CarbonMetastoreTypes.toDataType {
          column.getDataType.getName.toLowerCase match {
            case "decimal" => "decimal(" + column.getColumnSchema.getPrecision + "," + column
              .getColumnSchema.getScale + ")"
            case others => others
          }
        }
        CarbonToSparkAdapter.createAttributeReference(
          column.getColName, output, nullable = true, getColumnMetaData(column),
          NamedExpression.newExprId, qualifier = Option(tableName + "." + column.getColName))
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
    if (carbonTable.isExternalTable) {
      val tablePath = carbonTable.getTablePath
      if (FileFactory.isFileExist(tablePath)) {
        sizeInBytesLocalValue = FileFactory.getDirectorySize(tablePath)
      }
    } else {
      val tableStatusNewLastUpdatedTime = SegmentStatusManager.getTableStatusLastModifiedTime(
        carbonTable.getAbsoluteTableIdentifier)
      if (tableStatusLastUpdateTime != tableStatusNewLastUpdatedTime) {
        val allSegments = new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier)
          .getValidAndInvalidSegments(carbonTable.isChildTableForMV)
        if (allSegments.getValidSegments.isEmpty) {
          sizeInBytesLocalValue = 0L
        } else {
          val tablePath = carbonTable.getTablePath
          if (FileFactory.isFileExist(tablePath)) {
            // get the valid segments
            val segments = allSegments.getValidSegments.asScala
            var size = 0L
            // for each segment calculate the size
            segments.foreach { validSeg =>
              // for older store
              if (null != validSeg.getLoadMetadataDetails.getDataSize &&
                  null != validSeg.getLoadMetadataDetails.getIndexSize) {
                size = size + validSeg.getLoadMetadataDetails.getDataSize.toLong +
                       validSeg.getLoadMetadataDetails.getIndexSize.toLong
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
    }

    sizeInBytesLocalValue
  }

}
