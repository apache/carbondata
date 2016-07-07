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

import java.util.LinkedHashSet

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.apache.hadoop.fs.Path
import org.apache.spark._
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.hive.{CarbonMetaData, CarbonMetastoreTypes, TableMeta}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, StructType}

import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.datastorage.store.impl.FileFactory
import org.carbondata.spark.{CarbonOption, _}

/**
 * Carbon relation provider compliant to data source api.
 * Creates carbon relations
 */
class CarbonSource
  extends RelationProvider with CreatableRelationProvider with HadoopFsRelationProvider {

  /**
   * Returns a new base relation with the given parameters.
   * Note: the parameters' keywords are case insensitive and this insensitivity is enforced
   * by the Map that is passed to the function.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    if (parameters.get("tablePath") != None) {
      val options = new CarbonOption(parameters)
      val tableIdentifier = options.tableIdentifier.split("""\.""").toSeq
      CarbonDatasourceRelation(tableIdentifier, None)(sqlContext)
    } else if (parameters.get("path") != None) {
      CarbonDatasourceHadoopRelation(sqlContext, Array(parameters.get("path").get), parameters)
    } else {
      sys.error("Carbon table path not found")
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
        cc.sql(s"DROP CUBE IF EXISTS ${ options.dbName }.${ options.tableName }")
        (true, false)
      case (SaveMode.Overwrite, false) | (SaveMode.ErrorIfExists, false) =>
        (true, false)
      case (SaveMode.Append, _) =>
        (false, true)
      case (SaveMode.Ignore, exists) =>
        (!exists, false)
    }

    if (doSave) {
      // Only save data when the save mode is Overwrite.
      data.saveAsCarbonFile(parameters)
    } else if (doAppend) {
      data.appendToCarbonFile(parameters)
    }

    createRelation(sqlContext, parameters)
  }

  override def createRelation(sqlContext: SQLContext,
      paths: Array[String],
      dataSchema: Option[StructType],
      partitionColumns: Option[StructType],
      parameters: Map[String, String]): HadoopFsRelation = {
    CarbonDatasourceHadoopRelation(sqlContext, paths, parameters)
  }
}

/**
 * Creates carbon relation compliant to data source api.
 * This relation is stored to hive metastore
 */
private[sql] case class CarbonDatasourceRelation(
    tableIdentifier: Seq[String],
    alias: Option[String])
  (@transient context: SQLContext)
  extends BaseRelation with Serializable with Logging {

  def carbonRelation: CarbonRelation = {
    CarbonEnv.getInstance(context)
      .carbonCatalog.lookupRelation2(tableIdentifier, None)(sqlContext)
      .asInstanceOf[CarbonRelation]
  }

  def schema: StructType = carbonRelation.schema

  def sqlContext: SQLContext = context

  override val sizeInBytes: Long = {
    val tablePath = carbonRelation.cubeMeta.storePath + CarbonCommonConstants.FILE_SEPARATOR +
                    carbonRelation.cubeMeta.carbonTableIdentifier.getDatabaseName +
                    CarbonCommonConstants.FILE_SEPARATOR +
                    carbonRelation.cubeMeta.carbonTableIdentifier.getTableName
    val fileType = FileFactory.getFileType(tablePath)
    if(FileFactory.isFileExist(tablePath, fileType)) {
      FileFactory.getDirectorySize(tablePath)
    } else {
      0L
    }
  }
}

/**
 * Represents logical plan for one carbon cube
 */
case class CarbonRelation(
    schemaName: String,
    cubeName: String,
    metaData: CarbonMetaData,
    cubeMeta: TableMeta,
    alias: Option[String])(@transient sqlContext: SQLContext)
  extends LeafNode with MultiInstanceRelation {

  def tableName: String = cubeName

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
        case dType => dType
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
        case dType => s"${ childDim.getColName.substring(dimName.length() + 1) }:${ dType }"
      }
    }).mkString(",")
  }

  override def newInstance(): LogicalPlan = {
    CarbonRelation(schemaName, cubeName, metaData, cubeMeta, alias)(sqlContext)
      .asInstanceOf[this.type]
  }

  val dimensionsAttr = {
    val sett = new LinkedHashSet(
      cubeMeta.carbonTable.getDimensionByTableName(cubeMeta.carbonTableIdentifier.getTableName)
        .asScala.asJava)
    sett.asScala.toSeq.filter(!_.getColumnSchema.isInvisible).map(dim => {
      val output: DataType = metaData.carbonTable
        .getDimensionByName(metaData.carbonTable.getFactTableName, dim.getColName).getDataType
        .toString.toLowerCase match {
        case "array" => CarbonMetastoreTypes
          .toDataType(s"array<${ getArrayChildren(dim.getColName) }>")
        case "struct" => CarbonMetastoreTypes
          .toDataType(s"struct<${ getStructChildren(dim.getColName) }>")
        case dType => CarbonMetastoreTypes.toDataType(dType)
      }

      AttributeReference(
        dim.getColName,
        output,
        nullable = true)(qualifiers = tableName +: alias.toSeq)
    })
  }

  val measureAttr = {
    val factTable = cubeMeta.carbonTable.getFactTableName
    new LinkedHashSet(
      cubeMeta.carbonTable.
        getMeasureByTableName(cubeMeta.carbonTable.getFactTableName).
        asScala.asJava).asScala.toSeq.filter(!_.getColumnSchema.isInvisible)
        .map(x => AttributeReference(x.getColName, CarbonMetastoreTypes.toDataType(
        metaData.carbonTable.getMeasureByName(factTable, x.getColName).getDataType.toString
          .toLowerCase match {
          case "int" => "double"
          case "decimal" => "decimal(" + x.getPrecision + "," + x.getScale + ")"
          case others => others
        }),
      nullable = true)(qualifiers = tableName +: alias.toSeq))
  }

  override val output = dimensionsAttr ++ measureAttr

  // TODO: Use data from the footers.
  override lazy val statistics = Statistics(sizeInBytes = sqlContext.conf.defaultSizeInBytes)

  override def equals(other: Any): Boolean = {
    other match {
      case p: CarbonRelation =>
        p.schemaName == schemaName && p.output == output && p.cubeName == cubeName
      case _ => false
    }
  }

}



