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

import com.huawei.unibi.molap.olap.MolapDef
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Statistics}
import org.apache.spark.sql.hive.{CubeMeta, OlapMetaData, OlapMetastoreTypes}
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}
import org.apache.spark.sql.types.{DataType, StructType}

import scala.collection.JavaConversions.{asScalaBuffer, asScalaSet, seqAsJavaList}
import scala.language.implicitConversions


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
    CarbonRelation(tableIdentifier, None)(sqlContext)
  }
}

/**
  * Creates carbon relation compliant to data source api.
  * This relation is stored to hive metastore
  */
private[sql] case class CarbonRelation(
                                        tableIdentifier: Seq[String],
                                        alias: Option[String])(@transient context: SQLContext)
  extends BaseRelation with Serializable with Logging {

  def olapRelation =
    CarbonEnv.getInstance(context).carbonCatalog.lookupRelation2(tableIdentifier, None)(sqlContext).asInstanceOf[OlapRelation]

  def schema: StructType = olapRelation.schema

  def sqlContext: SQLContext = context
}


/**
  * Represents logical plan for one carbon cube
  */
case class OlapRelation(schemaName: String,
                        cubeName: String,
                        metaData: OlapMetaData,
                        cubeMeta: CubeMeta,
                        alias: Option[String])(@transient sqlContext: SQLContext)
  extends LeafNode with MultiInstanceRelation {

  def tableName = cubeName

  def recursiveMethod(dimName: String) : String = {
	metaData.cube.getChildren(dimName).map(childDim => {
		childDim.getDataType().toString.toLowerCase match {
			case "array" => s"array<${getArrayChildren(childDim.getColName)}>"
			case "struct" => s"struct<${getStructChildren(childDim.getColName)}>"
			case dType => s"${childDim.getColName()}:${dType}"
		}
	}).mkString(",")
  }
  
  def getArrayChildren(dimName: String) : String = {
	metaData.cube.getChildren(dimName).map(childDim => {
		childDim.getDataType().toString.toLowerCase match {
		    case "array" => s"array<${getArrayChildren(childDim.getColName())}>"
			case "struct" => s"struct<${getStructChildren(childDim.getColName())}>"
			case dType => dType
		}
	}).mkString(",")
  }
  
  def getStructChildren(dimName: String) : String = {
    metaData.cube.getChildren(dimName).map(childDim => {
		childDim.getDataType().toString.toLowerCase match {
		    case "array" => s"${childDim.getColName().substring(childDim.getParentName.length()+1)}:array<${getArrayChildren(childDim.getColName())}>"
        case "struct" => s"struct<${metaData.cube.getChildren(childDim.getColName).map(f => s"${recursiveMethod(f.getColName)}")}>"
        case dType => s"${childDim.getColName.substring(childDim.getParentName.length()+1)}:${dType}"
      }
	  }).mkString(",")
  }

  override def newInstance() = OlapRelation(schemaName, cubeName, metaData, cubeMeta, alias)(sqlContext).asInstanceOf[this.type]

  val dimensionsAttr = {
    val filteredDimAttr = cubeMeta.schema.cubes(0).dimensions.filter { aDim => (null == aDim.asInstanceOf[MolapDef.Dimension].hierarchies(0).levels(0).visible) ||
      (aDim.asInstanceOf[MolapDef.Dimension].hierarchies(0).levels(0).visible)
    }
    
     
    val sett = new LinkedHashSet(filteredDimAttr.toSeq)
    sett.toSeq.map(dim => 
    {
    	val output: DataType = metaData.cube.getDimension(dim.name).getDataType().toString.toLowerCase match {
    	  case "array" => OlapMetastoreTypes.toDataType(s"array<${getArrayChildren(dim.name)}>")
    	  case "struct" => OlapMetastoreTypes.toDataType(s"struct<${getStructChildren(dim.name)}>")
    	  case dType => OlapMetastoreTypes.toDataType(dType)
    	  }

      AttributeReference(
        dim.name,
        output,
        nullable = true)(qualifiers = tableName +: alias.toSeq)
    })
  }

  val measureAttr = {
    val filteredMeasureAttr = cubeMeta.schema.cubes(0).measures.filter { aMsr => (null == aMsr.visible) || (aMsr.visible) }
    new LinkedHashSet(filteredMeasureAttr.toSeq).toSeq.map(x => AttributeReference(
      x.name,
      OlapMetastoreTypes.toDataType("double"),
      nullable = true)(qualifiers = tableName +: alias.toSeq))
  }

  override val output = (dimensionsAttr ++ measureAttr).toSeq

  // TODO: Use data from the footers.
  override lazy val statistics = Statistics(sizeInBytes = sqlContext.conf.defaultSizeInBytes)

  override def equals(other: Any) = other match {
    case p: OlapRelation =>
      p.schemaName == schemaName && p.output == output && p.cubeName == cubeName
    case _ => false
  }

}



