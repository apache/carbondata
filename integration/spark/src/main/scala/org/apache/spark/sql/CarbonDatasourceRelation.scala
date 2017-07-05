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

package org.apache.spark.sql

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.hive.{CarbonMetaData, CarbonMetastoreTypes}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, StructType}

import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonColumn, CarbonDimension}
import org.apache.carbondata.core.metadata.schema.table.column.CarbonImplicitDimension
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.processing.merger.TableMeta
import org.apache.carbondata.spark.{CarbonOption, _}

/**
 * Carbon relation provider compliant to data source api.
 * Creates carbon relations
 */
class CarbonSource extends RelationProvider
    with CreatableRelationProvider with HadoopFsRelationProvider with DataSourceRegister {

  override def shortName(): String = "carbondata"

  /**
   * Returns a new base relation with the given parameters.
   * Note: the parameters' keywords are case insensitive and this insensitivity is enforced
   * by the Map that is passed to the function.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    // if path is provided we can directly create Hadoop relation. \
    // Otherwise create datasource relation
    parameters.get("path") match {
      case Some(path) => CarbonDatasourceHadoopRelation(sqlContext, Array(path), parameters, None)
      case _ =>
        val options = new CarbonOption(parameters)
        val tableIdentifier = options.tableIdentifier.split("""\.""").toSeq
        val identifier = tableIdentifier match {
          case Seq(name) => TableIdentifier(name, None)
          case Seq(db, name) => TableIdentifier(name, Some(db))
        }
        CarbonDatasourceRelation(identifier, None)(sqlContext)
    }
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: SchemaRDD): BaseRelation = {

    // To avoid derby problem, dataframe need to be writen and read using CarbonContext
    require(sqlContext.isInstanceOf[CarbonContext], "Error in saving dataframe to carbon file, " +
        "must use CarbonContext to save dataframe")

    // User should not specify path since only one store is supported in carbon currently,
    // after we support multi-store, we can remove this limitation
    require(!parameters.contains("path"), "'path' should not be specified, " +
        "the path to store carbon file is the 'storePath' specified when creating CarbonContext")

    val options = new CarbonOption(parameters)
    val storePath = CarbonContext.getInstance(sqlContext.sparkContext).storePath
    val tablePath = new Path(storePath + "/" + options.dbName + "/" + options.tableName)
    val isExists = tablePath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
        .exists(tablePath)
    val (doSave, doAppend) = (mode, isExists) match {
      case (SaveMode.ErrorIfExists, true) =>
        sys.error(s"ErrorIfExists mode, path $storePath already exists.")
      case (SaveMode.Overwrite, true) =>
        val cc = CarbonContext.getInstance(sqlContext.sparkContext)
        cc.sql(s"DROP TABLE IF EXISTS ${ options.dbName }.${ options.tableName }")
        (true, false)
      case (SaveMode.Overwrite, false) | (SaveMode.ErrorIfExists, false) =>
        (true, false)
      case (SaveMode.Append, _) =>
        (false, true)
      case (SaveMode.Ignore, exists) =>
        (!exists, false)
    }

    if (doSave) {
      // save data when the save mode is Overwrite.
      new CarbonDataFrameWriter(data).saveAsCarbonFile(parameters)
    } else if (doAppend) {
      new CarbonDataFrameWriter(data).appendToCarbonFile(parameters)
    }

    createRelation(sqlContext, parameters)
  }

  override def createRelation(sqlContext: SQLContext,
      paths: Array[String],
      dataSchema: Option[StructType],
      partitionColumns: Option[StructType],
      parameters: Map[String, String]): HadoopFsRelation = {
    CarbonDatasourceHadoopRelation(sqlContext, paths, parameters, dataSchema)
  }
}

/**
 *  Creates carbon relation compliant to data source api.
 * This relation is stored to hive metastore
 */
private[sql] case class CarbonDatasourceRelation(
    tableIdentifier: TableIdentifier,
    alias: Option[String])
    (@transient context: SQLContext)
    extends BaseRelation with Serializable {

  lazy val carbonRelation: CarbonRelation = {
    CarbonEnv.get
        .carbonMetastore.lookupRelation1(tableIdentifier, None)(sqlContext)
        .asInstanceOf[CarbonRelation]
  }

  def getDatabaseName(): String = tableIdentifier.database.getOrElse("default")

  def getTable(): String = tableIdentifier.table

  def schema: StructType = carbonRelation.schema

  def sqlContext: SQLContext = context

  override def sizeInBytes: Long = carbonRelation.sizeInBytes
}

/**
 * Represents logical plan for one carbon table
 */
case class CarbonRelation(
    databaseName: String,
    tableName: String,
    var metaData: CarbonMetaData,
    tableMeta: TableMeta,
    alias: Option[String])(@transient sqlContext: SQLContext)
    extends LeafNode with MultiInstanceRelation {

  def recursiveMethod(dimName: String, childDim: CarbonDimension): String = {
    childDim.getDataType.toString.toLowerCase match {
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
      childDim.getDataType.toString.toLowerCase match {
        case "array" => s"array<${ getArrayChildren(childDim.getColName) }>"
        case "struct" => s"struct<${ getStructChildren(childDim.getColName) }>"
        case dType => addDecimalScaleAndPrecision(childDim, dType)
      }
    }).mkString(",")
  }

  def getStructChildren(dimName: String): String = {
    metaData.carbonTable.getChildren(dimName).asScala.map(childDim => {
      childDim.getDataType.toString.toLowerCase match {
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
    CarbonRelation(databaseName, tableName, metaData, tableMeta, alias)(sqlContext)
        .asInstanceOf[this.type]
  }

  val dimensionsAttr = {
    val sett = new java.util.LinkedHashSet(tableMeta.carbonTable
      .getDimensionByTableName(tableMeta.carbonTableIdentifier.getTableName).asScala.asJava)
    sett.asScala.toSeq.filter(dim => !dim.isInvisible ||
                                     (dim.isInvisible && dim.isInstanceOf[CarbonImplicitDimension]))
      .map(dim => {
      val dimval = metaData.carbonTable
          .getDimensionByName(metaData.carbonTable.getFactTableName, dim.getColName)
      val output: DataType = dimval.getDataType
          .toString.toLowerCase match {
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
        nullable = true)(qualifiers = tableName +: alias.toSeq)
    })
  }

  val measureAttr = {
    val factTable = tableMeta.carbonTable.getFactTableName
    new java.util.LinkedHashSet(
      tableMeta.carbonTable.
          getMeasureByTableName(tableMeta.carbonTable.getFactTableName).
          asScala.asJava).asScala.toSeq.filter(!_.getColumnSchema.isInvisible)
        .map(x => AttributeReference(x.getColName, CarbonMetastoreTypes.toDataType(
          metaData.carbonTable.getMeasureByName(factTable, x.getColName).getDataType.toString
              .toLowerCase match {
            case "float" => "double"
            case "decimal" => "decimal(" + x.getPrecision + "," + x.getScale + ")"
            case others => others
          }),
          nullable = true)(qualifiers = tableName +: alias.toSeq))
  }

  override val output = {
    val columns = tableMeta.carbonTable.getCreateOrderColumn(tableMeta.carbonTable.getFactTableName)
        .asScala
    columns.filter(!_.isInvisible).map { column =>
      if (column.isDimension()) {
        val output: DataType = column.getDataType.toString.toLowerCase match {
          case "array" =>
            CarbonMetastoreTypes.toDataType(s"array<${getArrayChildren(column.getColName)}>")
          case "struct" =>
            CarbonMetastoreTypes.toDataType(s"struct<${getStructChildren(column.getColName)}>")
          case dType =>
            val dataType = addDecimalScaleAndPrecision(column, dType)
            CarbonMetastoreTypes.toDataType(dataType)
        }
        AttributeReference(column.getColName, output,
          nullable = true
        )(qualifiers = tableName +: alias.toSeq)
      } else {
        AttributeReference(column.getColName, CarbonMetastoreTypes.toDataType(
          column.getDataType.toString
            .toLowerCase match {
            case "float" => "double"
            case "decimal" => "decimal(" + column.getColumnSchema.getPrecision + "," + column
              .getColumnSchema.getScale + ")"
            case others => others
          }
        ),
          nullable = true
        )(qualifiers = tableName +: alias.toSeq)
      }
    }
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

  def addDecimalScaleAndPrecision(dimval: CarbonColumn, dataType: String): String = {
    var dType = dataType
    if (dimval.getDataType == org.apache.carbondata.core.metadata.datatype.DataType.DECIMAL) {
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

