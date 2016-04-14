/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql

import java.util.LinkedHashSet
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Statistics}
import org.apache.spark.sql.hive.{TableMeta, CarbonMetaData, CarbonMetastoreTypes}
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}
import org.apache.spark.sql.types.{DataType, StructType}
import org.carbondata.core.carbon.CarbonDef
import scala.collection.JavaConversions.{asScalaBuffer, asScalaSet, seqAsJavaList}
import scala.language.implicitConversions
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension


/**
  * Carbon relation provider compliant to data source api.
  * Creates carbon relations
  */
class CarbonSource extends RelationProvider {
  /**
    * Returns a new base relation with the given parameters.
    * Note: the parameters' keywords are case insensitive and this insensitivity is enforced
    * by the Map that is passed to the function.
    */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {

    // On Spark SQL option keys are case insensitive, so use lower case to check
    val optSet = Set("cubename")
    val keySet = parameters.keySet.map(_.toLowerCase)

    if (!(optSet.intersect(keySet).size == 1)) {
      throw new Exception(
        s"""There are invalid on options, usages:
            |CREATE TABLE table_name
            |USING org.apache.spark.sql.CarbonSource
            |OPTIONS(
            | CubeName "schema_name.cube_name"
            |)""".stripMargin
      )
    }

    val tableIdentifier = parameters.get("cubename").get.split("""\.""").toSeq
    CarbonDatasourceRelation(tableIdentifier, None)(sqlContext)
  }
}

/**
  * Creates carbon relation compliant to data source api.
  * This relation is stored to hive metastore
  */
private[sql] case class CarbonDatasourceRelation(
                                        tableIdentifier: Seq[String],
                                        alias: Option[String])(@transient context: SQLContext)
  extends BaseRelation with Serializable with Logging {

  def carbonRelation =
    CarbonEnv.getInstance(context).carbonCatalog.lookupRelation2(tableIdentifier, None)(sqlContext).asInstanceOf[CarbonRelation]

  def schema: StructType = carbonRelation.schema

  def sqlContext: SQLContext = context
}


/**
  * Represents logical plan for one carbon cube
  */
case class CarbonRelation(schemaName: String,
                        cubeName: String,
                        metaData: CarbonMetaData,
                        cubeMeta: TableMeta,
                        alias: Option[String])(@transient sqlContext: SQLContext)
  extends LeafNode with MultiInstanceRelation {

  def tableName = cubeName

  def recursiveMethod(dimName: String) : String = {
	metaData.carbonTable.getChildren(dimName).map(childDim => {
		childDim.getDataType().toString.toLowerCase match {
			case "array" => s"array<${getArrayChildren(childDim.getColName)}>"
			case "struct" => s"struct<${getStructChildren(childDim.getColName)}>"
			case dType => s"${childDim.getColName()}:${dType}"
		}
	}).mkString(",")
  }
  
  def getArrayChildren(dimName: String) : String = {
	metaData.carbonTable.getChildren(dimName).map(childDim => {
		childDim.getDataType().toString.toLowerCase match {
		  case "array" => s"array<${getArrayChildren(childDim.getColName())}>"
			case "struct" => s"struct<${getStructChildren(childDim.getColName())}>"
			case dType => dType
		}
	}).mkString(",")
  }
  
  def getStructChildren(dimName: String) : String = {
    metaData.carbonTable.getChildren(dimName).map(childDim => {
		childDim.getDataType().toString.toLowerCase match {
		    case "array" => s"${childDim.getColName().substring(dimName.length()+1)}:array<${getArrayChildren(childDim.getColName())}>"
        case "struct" => s"struct<${metaData.carbonTable.getChildren(childDim.getColName).map(f => s"${recursiveMethod(f.getColName)}")}>"
        case dType => s"${childDim.getColName.substring(dimName.length()+1)}:${dType}"
      }
	  }).mkString(",")
  }

  override def newInstance() = CarbonRelation(schemaName, cubeName, metaData, cubeMeta, alias)(sqlContext).asInstanceOf[this.type]

  val dimensionsAttr = {
    val sett = new LinkedHashSet(cubeMeta.carbonTable.getDimensionByTableName(cubeMeta.tableName).toSeq)
    sett.toSeq.map(dim => 
    {
    	val output: DataType = metaData.carbonTable.getDimensionByName(metaData.carbonTable.getFactTableName,dim.getColName).getDataType().toString.toLowerCase match {
    	  case "array" => CarbonMetastoreTypes.toDataType(s"array<${getArrayChildren(dim.getColName)}>")
    	  case "struct" => CarbonMetastoreTypes.toDataType(s"struct<${getStructChildren(dim.getColName)}>")
//    	  case "int" => CarbonMetastoreTypes.toDataType("double")
    	  case dType => CarbonMetastoreTypes.toDataType(dType)
    	  }

      AttributeReference(
        dim.getColName,
        output,
        nullable = true)(qualifiers = tableName +: alias.toSeq)
    })
  }

  val measureAttr = {
//    val filteredMeasureAttr = cubeMeta.carbonTable.getmeasures.filter { aMsr => (null == aMsr.visible) || (aMsr.visible) }
    val factTable = cubeMeta.carbonTable.getFactTableName
    new LinkedHashSet(cubeMeta.carbonTable.getMeasureByTableName(cubeMeta.carbonTable.getFactTableName).toSeq).toSeq.map(x => AttributeReference(
      x.getColName,
      CarbonMetastoreTypes.toDataType(metaData.carbonTable.getMeasureByName(factTable, x.getColName).getDataType.toString.toLowerCase match
      {
        case "int" => "double"
        case _ => metaData.carbonTable.getMeasureByName(factTable, x.getColName).getDataType.toString.toLowerCase
      }),
      nullable = true)(qualifiers = tableName +: alias.toSeq))
  }

  override val output = (dimensionsAttr ++ measureAttr).toSeq

  // TODO: Use data from the footers.
  override lazy val statistics = Statistics(sizeInBytes = sqlContext.conf.defaultSizeInBytes)

  override def equals(other: Any) = other match {
    case p: CarbonRelation =>
      p.schemaName == schemaName && p.output == output && p.cubeName == cubeName
    case _ => false
  }

}



