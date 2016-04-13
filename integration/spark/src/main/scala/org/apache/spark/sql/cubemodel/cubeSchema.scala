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

package org.apache.spark.sql.cubemodel

import java.text.SimpleDateFormat

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.{RunnableCommand, SparkPlan}
import org.apache.spark.sql.hive.{CarbonMetastoreCatalog, HiveContext}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{CarbonContext, CarbonEnv, CarbonRelation, DataFrame, Row, SQLContext, getDB}
import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.datastorage.store.impl.FileFactory
import org.carbondata.core.metadata.CarbonMetadata
import org.carbondata.core.carbon.CarbonDef
import org.carbondata.core.carbon.CarbonDef.{AggTable, CubeDimension}
import org.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.carbondata.integration.spark.load.{CarbonLoadModel, CarbonLoaderUtil, DeleteLoadFromMetadata}
import org.carbondata.integration.spark.partition.api.impl.QueryPartitionHelper
import org.carbondata.integration.spark.rdd.CarbonDataRDDFactory
import org.carbondata.integration.spark.util.{CarbonQueryUtil, CarbonScalaUtil, CarbonSparkInterFaceLogEvent}
import org.carbondata.processing.suggest.autoagg.model.Request
import org.carbondata.processing.suggest.autoagg.util.CommonUtil
import org.carbondata.processing.suggest.autoagg.{AutoAggSuggestionFactory, AutoAggSuggestionService}
import org.carbondata.processing.suggest.datastats.model.LoadModel
import org.carbondata.processing.util.CarbonSchemaParser

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import org.carbondata.integration.spark.util.GlobalDictionaryUtil
import org.carbondata.core.locks.{CarbonLockFactory, LockUsage}

case class CubeModel(
                      ifNotExistsSet: Boolean,
                      //SchmemaNameOp is used to temporarily to hold schema and then set to var schemaName
                      var schemaName: String,
                      schemaNameOp: Option[String],
                      cubeName: String,
                      dimCols: Seq[Field],
                      msrCols: Seq[Field],
                      fromKeyword: String,
                      withKeyword: String,
                      source: Object,
                      factFieldsList: Option[FilterCols],
                      dimRelations: Seq[DimensionRelation],
                      simpleDimRelations: Seq[DimensionRelation],
    highcardinalitydims:Option[Seq[String]],
                      aggregation: Seq[Aggregation],
                      partitioner: Option[Partitioner])


case class Field(column: String, dataType: Option[String], name: Option[String], children : Option[List[Field]], parent: String = null,storeType: Option[String]=Some("columnar"))

case class ArrayDataType(dataType: String)

case class StructDataType(dataTypes: List[String])

case class StructField(column: String, dataType: String)

case class FieldMapping(levelName: String, columnName: String)

case class HierarchyMapping(hierName: String, hierType: String, levels: Seq[String])

case class ComplexField(complexType: String, primitiveField: Option[Field], complexField: Option[ComplexField])

case class Cardinality(levelName: String, cardinality: Int)

case class Aggregation(msrName: String, aggType: String)

case class AggregateTableAttributes(colName: String, aggType: String = null)

case class Partitioner(partitionClass: String, partitionColumn: Array[String], partitionCount: Int, nodeList: Array[String])

case class DimensionRelation(tableName: String, dimSource: Object, relation: Relation, includeKey: Option[String], cols: Option[Seq[String]])

case class Relation(leftColumn: String, rightColumn: String)

case class Level(name: String, val column: String, cardinality: Int, dataType: String, parent: String = null, storeType: String ="Columnar",levelType: String = "Regular")

case class Measure(name: String, column: String, dataType: String, aggregator: String = "SUM", visible: Boolean = true)

case class Hierarchy(name: String, primaryKey: Option[String], levels: Seq[Level], tableName: Option[String], normalized: Boolean = false)

case class Dimension(name: String, hierarchies: Seq[Hierarchy], foreignKey: Option[String], dimType: String = "StandardDimension", visible: Boolean = true,var highCardinality: Boolean = false)

case class FilterCols(includeKey: String, fieldList: Seq[String])

case class Cube(schemaName: String, cubeName: String, tableName: String, dimensions: Seq[Dimension], measures: Seq[Measure], partitioner: Partitioner)

case class Default(key: String, value: String)

case class DataLoadTableFileMapping(table: String, loadPath: String)

object CubeProcessor {
  def apply(cm: CubeModel, sqlContext: SQLContext): Cube = {
    new CubeProcessor(cm, sqlContext).process
  }
}

class CubeProcessor(cm: CubeModel, sqlContext: SQLContext) {
  val timeDims = Seq("TimeYears", "TimeMonths", "TimeDays", "TimeHours", "TimeMinutes")
  val numericTypes = Seq(CarbonCommonConstants.INTEGER_TYPE, CarbonCommonConstants.DOUBLE_TYPE, CarbonCommonConstants.LONG_TYPE, CarbonCommonConstants.FLOAT_TYPE)

  def getAllChildren(fieldChildren : Option[List[Field]]) : Seq[Level]  = {
      var levels: Seq[Level] = Seq[Level]()
	   fieldChildren.map(fields => {
	        fields.map(field => {
	        	  if(field.parent != null)
		    		 levels ++= Seq(Level(field.name.getOrElse(field.column), field.column, Int.MaxValue, field.dataType.getOrElse(CarbonCommonConstants.STRING), field.parent,field.storeType.getOrElse("Columnar")))
		    	 else
		    		 levels ++= Seq(Level(field.name.getOrElse(field.column), field.column, Int.MaxValue, field.dataType.getOrElse(CarbonCommonConstants.STRING),field.storeType.getOrElse("Columnar")))
	        	  if(field.children.get != null)
	        		  levels ++= getAllChildren(field.children)
	      	})
	   })
	   levels
  }
  
  def process(): Cube = {

    var levels = Seq[Level]()
    var measures = Seq[Measure]()
    var dimSrcDimensions = Seq[Dimension]()
    val LOGGER = LogServiceFactory.getLogService(CubeProcessor.getClass().getName())

    // Create Cube DDL with Schema defination
//    levels = 
      cm.dimCols.map(field =>
      {
    	 if(field.parent != null)
    		 levels ++= Seq(Level(field.name.getOrElse(field.column), field.column, Int.MaxValue, field.dataType.getOrElse(CarbonCommonConstants.STRING), field.parent,field.storeType.getOrElse(CarbonCommonConstants.COLUMNAR)))
    	 else
    		 levels ++= Seq(Level(field.name.getOrElse(field.column), field.column, Int.MaxValue, field.dataType.getOrElse(CarbonCommonConstants.STRING),field.parent,field.storeType.getOrElse(CarbonCommonConstants.COLUMNAR)))
    	 if(field.children.get != null)
    		 levels ++= getAllChildren(field.children)
      })
    measures = cm.msrCols.map(field => Measure(field.name.getOrElse(field.column), field.column, field.dataType.getOrElse(CarbonCommonConstants.NUMERIC)))

    if (cm.withKeyword.equalsIgnoreCase(CarbonCommonConstants.WITH) && cm.simpleDimRelations.size > 0) {
      cm.simpleDimRelations.foreach(relationEntry => {

        // Split the levels and seperate levels with dimension levels
        val split = levels.partition(x => relationEntry.cols.get.contains(x.name))

        val dimLevels = split._1
        levels = split._2

        def getMissingRelationLevel(): Level = Level(relationEntry.relation.rightColumn, relationEntry.relation.rightColumn, Int.MaxValue, CarbonCommonConstants.STRING)

        val dimHierarchies = dimLevels.map(field =>
          Hierarchy(relationEntry.tableName, Some(dimLevels.find(dl =>
            dl.name.equalsIgnoreCase(relationEntry.relation.rightColumn))
            .getOrElse(getMissingRelationLevel).column), Seq(field), Some(relationEntry.tableName)))
        dimSrcDimensions = dimSrcDimensions ++ dimHierarchies.map(field => Dimension(field.levels.head.name, Seq(field), Some(relationEntry.relation.leftColumn)))
      })
    }

    // Check if there is any duplicate measures or dimensions. Its based on the dimension name and measure name
    levels.groupBy(_.name).foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Duplicate dimensions found with name : $name")
      LOGGER.audit(s"Validation failed for Create/Alter Cube Operation - Duplicate dimensions found with name : $name")
      sys.error(s"Duplicate dimensions found with name : $name")
    })

    levels.groupBy(_.column).foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Duplicate dimensions found with column name : $name")
      LOGGER.audit(s"Validation failed for Create/Alter Cube Operation - Duplicate dimensions found with column name : $name")
      sys.error(s"Duplicate dimensions found with column name : $name")
    })

    measures.groupBy(_.name).foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Duplicate measures found with name : $name")
      LOGGER.audit(s"Validation failed for Create/Alter Cube Operation - Duplicate measures found with name : $name")
      sys.error(s"Duplicate measures found with name : $name")
    })

    measures.groupBy(_.column).foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Duplicate measures found with column name : $name")
      LOGGER.audit(s"Validation failed for Create/Alter Cube Operation - Duplicate measures found with column name : $name")
      sys.error(s"Duplicate measures found with column name : $name")
    })

    val levelsArray = levels.map(_.name)
    val levelsNdMesures = levelsArray ++ measures.map(_.name)

    cm.aggregation.foreach(a => {
      if (levelsArray.contains(a.msrName)) {
        val fault = a.msrName
        LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Aggregator should not be defined for dimension fields [$fault]")
        LOGGER.audit(s"Validation failed for Create/Alter Cube Operation - Aggregator should not be defined for dimension fields [$fault]")
        sys.error(s"Aggregator should not be defined for dimension fields [$fault]")
      }
    })

    levelsNdMesures.groupBy(x => x).foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Dimension and Measure defined with same name : $name")
      LOGGER.audit(s"Validation failed for Create/Alter Cube Operation - Dimension and Measure defined with same name : $name")
      sys.error(s"Dimension and Measure defined with same name : $name")
    })

    dimSrcDimensions.foreach(d => {
      d.hierarchies.foreach(h => {
        h.levels.foreach(l => {
          levels = levels.dropWhile(lev => lev.name.equalsIgnoreCase(l.name))
        })
      })
    })
    
//    val hierarchies = levels.map(field => Hierarchy(field.name, None, Seq(field), None))
    
//    val hierarchies = levels.groupBy(
//        _.name.split('.')(0)
//        ).map(
//        fields => 
//          Hierarchy(fields._1, None, fields._2, None)
//        ).toSeq
    
    val groupedSeq = levels.groupBy(_.name.split('.')(0))
    val hierarchies = levels.filter(level => !level.name.contains(".")).map(parentLevel => Hierarchy(parentLevel.name, None, groupedSeq.get(parentLevel.name).get, None))
    var dimensions = hierarchies.map(field => Dimension(field.name, Seq(field), None))

    dimensions = dimensions ++ dimSrcDimensions
//    val highCardinalityDims=cm.highcardinalitydims.get
    val highCardinalityDims=cm.highcardinalitydims.getOrElse(Seq())
    for(dimension <- dimensions)
    {
      
      if(highCardinalityDims.contains(dimension.name))
      {
        dimension.highCardinality=true
      }
      
    }
    
    var newOrderedDims = scala.collection.mutable.ListBuffer[Dimension]()
    val highCardDims = scala.collection.mutable.ListBuffer[Dimension]()
    val complexDims = scala.collection.mutable.ListBuffer[Dimension]()
     for(dimension <- dimensions)
    {
      if(highCardinalityDims.contains(dimension.name))
      {
       // dimension.highCardinality=true
        highCardDims.add(dimension)
      }
      else if(dimension.hierarchies(0).levels.length > 1)
      {
    	  complexDims.add(dimension)
      }
      else
      {
       newOrderedDims.add(dimension)
      }
      
    }
    
    newOrderedDims = newOrderedDims ++ highCardDims ++ complexDims

    dimensions = newOrderedDims
    
    if (measures.length <= 0) {
      measures = measures ++ Seq(Measure(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE, CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE, CarbonCommonConstants.NUMERIC, CarbonCommonConstants.SUM, false))
    }

    //Update measures with aggregators if specified.
    val msrsUpdatedWithAggregators = cm.aggregation match {
      case aggs: Seq[Aggregation] =>
        measures.map { f =>
          val matchedMapping = aggs.filter(agg => f.name.equals(agg.msrName))
          if (matchedMapping.length == 0) f
          else Measure(f.name, f.column, f.dataType, matchedMapping.head.aggType)
        }
      case _ => measures
    }

    val partitioner = cm.partitioner match {
      case Some(part: Partitioner) =>
        var definedpartCols = part.partitionColumn
        val columnBuffer = new ArrayBuffer[String]
        part.partitionColumn.foreach { col =>
          dimensions.foreach { dim =>
            dim.hierarchies.foreach { hier =>
              hier.levels.foreach { lev =>
                if (lev.name.equalsIgnoreCase(col)) {
                  definedpartCols = definedpartCols.dropWhile(c => c.equals(col))
                  columnBuffer.add(lev.name)
                }
              }
            }
          }
        }


        //Special Case, where Partition count alone is sent to Carbon for dataloading
        if (part.partitionClass.isEmpty() && part.partitionColumn(0).isEmpty()) {
          Partitioner("org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl", Array(""), part.partitionCount, null)
        }
        else if (definedpartCols.size > 0) {
          val msg = definedpartCols.mkString(", ")
          LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"partition columns specified are not part of Dimension columns : $msg")
          LOGGER.audit(s"Validation failed for Create/Alter Cube Operation - partition columns specified are not part of Dimension columns : $msg")
          sys.error(s"partition columns specified are not part of Dimension columns : $msg")
        }
        else {

          try {
            Class.forName(part.partitionClass).newInstance()
          } catch {
            case e: Exception => {
              val cl = part.partitionClass
              LOGGER.audit(s"Validation failed for Create/Alter Cube Operation - partition class specified can not be found or loaded : $cl")
              sys.error(s"partition class specified can not be found or loaded : $cl")
            }
          }

          Partitioner(part.partitionClass, columnBuffer.toArray, part.partitionCount, null)
        }
      case None =>
        Partitioner("org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl", Array(""), 20, null)
    }

    Cube(cm.schemaName, cm.cubeName, cm.cubeName, dimensions, msrsUpdatedWithAggregators, partitioner)
  }

  // For filtering INCLUDE and EXCLUDE fields if any is defined for Dimention relation
  def filterRelIncludeCols(relationEntry: DimensionRelation, p: (String, String)): Boolean = {
    if (relationEntry.includeKey.get.equalsIgnoreCase(CarbonCommonConstants.INCLUDE)) {
      relationEntry.cols.get.map(x => x.toLowerCase()).contains(p._1.toLowerCase())
    } else {
      !relationEntry.cols.get.map(x => x.toLowerCase()).contains(p._1.toLowerCase())
    }
  }

  private def getUpdatedLevels(levels: Seq[Level], fms: Seq[FieldMapping]): Seq[Level] = {
    levels.map { f =>
      val matchedMapping = fms.filter(fm => f.name == fm.levelName)
      if (matchedMapping.length == 0) {
        f
      } else {
        Level(f.name, matchedMapping.head.columnName, f.cardinality, f.dataType)
      }
    }
  }
}

private[sql] case class ShowCreateCube(cm: CubeModel, override val output: Seq[Attribute])
  extends RunnableCommand {

  val numericTypes = Seq(CarbonCommonConstants.INTEGER_TYPE, CarbonCommonConstants.DOUBLE_TYPE, CarbonCommonConstants.LONG_TYPE, CarbonCommonConstants.FLOAT_TYPE)

  def run(sqlContext: SQLContext): Seq[Row] = {

    var levels = Seq[Level]()
    var levelsToCheckDuplicate = Seq[Level]()
    var measures = Seq[Measure]()
    var dimFileDimensions = Seq[Dimension]()
    val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.cubemodel.cubeSchema")
    var command = new StringBuilder
    var relation = new StringBuilder

    cm.schemaName = getDB.getDatabaseName(cm.schemaNameOp, sqlContext)

    command = command.append("CREATE CUBE ").append(cm.schemaName).append(".").append(cm.cubeName).append(" ")
    relation = relation.append("")

    if (cm.fromKeyword.equalsIgnoreCase(CarbonCommonConstants.FROM)) {
      val df = getDataFrame(cm.source, sqlContext)

      // Will maintain the list of all the columns specified by the user. In case if relation is defined. we need to retain the mapping column in case if its in this list
      var specifiedCols = Seq[String]()

      // For filtering INCLUDE and EXCLUDE fields defined for Measures and Dimensions
      def filterIncludeCols(p: (String, String), fCols: FilterCols): Boolean = {
        if (fCols.includeKey.equalsIgnoreCase(CarbonCommonConstants.INCLUDE)) {
          fCols.fieldList.map(x => x.toLowerCase()).contains(p._1.toLowerCase())
        } else {
          !fCols.fieldList.map(x => x.toLowerCase()).contains(p._1.toLowerCase())
        }
      }

      // For filtering the fields defined in Measures and Dimensions fields
      def filterDefinedCols(p: (String, String), definedCols: Seq[Field]) = {
        var isDefined = false
        definedCols.foreach(f => {
          if (f.dataType.isDefined) sys.error(s"Specifying Data types is not supported for the fields in the DDL with CSV file or Table : [$f]")
          if (f.column.equalsIgnoreCase(p._1)) isDefined = true
        })
        isDefined
      }

      val rawColumns = if (cm.factFieldsList.isDefined) {
        val cols = df.dtypes.map(f => (f._1.trim(), f._2)).filter(filterIncludeCols(_, cm.factFieldsList.get))
        specifiedCols = cols.map(_._1)
        cols
      } else {
        df.dtypes.map(f => 
          if(f._2.startsWith("ArrayType") || f._2.startsWith("StructType"))
          {
        	  val fieldIndex = df.schema.getFieldIndex(f._1).get
        	  (f._1.trim(), df.schema.fields(fieldIndex).dataType.simpleString)
          }
          else
        	  (f._1.trim(), f._2))
        
      }

      val columns = rawColumns.filter(c => !c._2.equalsIgnoreCase(CarbonCommonConstants.BINARY_TYPE))
      if (rawColumns.size > columns.size) LOGGER.info(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, "BinaryType is not supported. Ignoring all the Binary fields.")

      val (numericColArray, nonNumericColArray) = columns.partition(p => numericTypes.map(x => x.toLowerCase()).contains(p._2.toLowerCase()))

      // If dimensions are defined along with Fact CSV/table, consider only defined dimensions
      val dimColArray = if (cm.dimCols.size > 0) {
        val dcolArray = columns.filter(filterDefinedCols(_, cm.dimCols))
        val listedCols = dcolArray.map(_._1)
        specifiedCols = specifiedCols ++ listedCols
        dcolArray
      } else {
        nonNumericColArray
      }

      // If measures are defined along with Fact CSV/table, consider only defined measures
      val measureColArray = if (cm.msrCols.size > 0) {
        val mColArray = columns.filter(filterDefinedCols(_, cm.msrCols))
        val listedCols = mColArray.map(_._1)
        specifiedCols = specifiedCols ++ listedCols
        mColArray
      } else {
        if (cm.dimCols.size > 0)
          numericColArray.filterNot(filterDefinedCols(_, cm.dimCols))
        else numericColArray
      }

      measures = measureColArray.map(field => {
        if (cm.msrCols.size > 0) {
          val definedField = cm.msrCols.filter(f => f.column.equalsIgnoreCase(field._1))
          if (definedField.size > 0 && definedField.head.name.isDefined) Measure(definedField.head.name.getOrElse(field._1), field._1, CarbonScalaUtil.convertSparkToCarbonSchemaDataType(field._2))
          else Measure(field._1, field._1, CarbonScalaUtil.convertSparkToCarbonSchemaDataType(field._2))
        }
        else Measure(field._1, field._1, CarbonScalaUtil.convertSparkToCarbonSchemaDataType(field._2))
      })

      levels = dimColArray.map(field => {
        if (cm.dimCols.size > 0) {
          val definedField = cm.dimCols.filter(f => f.column.equalsIgnoreCase(field._1))
          if (definedField.size > 0 && definedField.head.name.isDefined) Level(definedField.head.name.getOrElse(field._1), field._1, Int.MaxValue, CarbonScalaUtil.convertSparkToCarbonSchemaDataType(field._2))
          else Level(field._1, field._1, Int.MaxValue, CarbonScalaUtil.convertSparkToCarbonSchemaDataType(field._2))
        }
        else Level(field._1, field._1, Int.MaxValue, CarbonScalaUtil.convertSparkToCarbonSchemaDataType(field._2))
      })

      if (cm.dimRelations.size > 0) {
        cm.dimRelations.foreach(relationEntry => {

          val relDf = getDataFrame(relationEntry.dimSource, sqlContext)

          var right = false

          for (field <- relDf.columns.map(f => f.trim())) {
            if (field.equalsIgnoreCase(relationEntry.relation.rightColumn)) {
              right = true
            }
          }

          if (!right) {
            val rcl = relationEntry.relation.rightColumn
            LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Dimension field defined in the relation [$rcl] is not present in the Dimension source")
            sys.error(s"Dimension field defined in the relation [$rcl] is not present in the Dimension source")
          }

          val rawRelColumns = if (relationEntry.cols.isDefined) {
            relDf.dtypes.map(f => (f._1.trim(), f._2)).filter(filterRelIncludeCols(relationEntry, _))
          } else {
            relDf.dtypes.map(f => (f._1.trim(), f._2))
          }

          val relColumns = rawRelColumns.filter(c => !c._2.equalsIgnoreCase(CarbonCommonConstants.BINARY_TYPE))
          if (rawRelColumns.size > relColumns.size) LOGGER.info(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, "BinaryType is not supported. Ignoring all the Binary fields.")

          // Remove the relation column from fact table as it is already considered in dimension table
          levels = levels.dropWhile(p => p.column.equalsIgnoreCase(relationEntry.relation.leftColumn) && !specifiedCols.map(x => x.toLowerCase()).contains(p.column.toLowerCase()))
          measures = measures.dropWhile(p => p.column.equalsIgnoreCase(relationEntry.relation.leftColumn) && !specifiedCols.map(x => x.toLowerCase()).contains(p.column.toLowerCase()))

          var dimFileLevels: Seq[Level] = Seq[Level]() 
            relColumns.map(field => {
//            		dimFileLevels ++ = CarbonScalaUtil.convertSparkColumnToCarbonLevel(field)
            		Level(field._1, field._1, Int.MaxValue, CarbonScalaUtil.convertSparkToCarbonSchemaDataType(field._2))
            	}
              )
          val dimFileHierarchies = dimFileLevels.map(field => Hierarchy(relationEntry.tableName, Some(dimFileLevels.find(dl => dl.name.equalsIgnoreCase(relationEntry.relation.rightColumn)).get.column), Seq(field), Some(relationEntry.tableName)))
          dimFileDimensions = dimFileDimensions ++ dimFileHierarchies.map(field => Dimension(field.levels.head.name, Seq(field), Some(relationEntry.relation.leftColumn)))

          levelsToCheckDuplicate = levelsToCheckDuplicate ++ dimFileLevels

          if (relation.length > 0) relation = relation.append(", ")
          relation = relation.append(relationEntry.tableName).append(" RELATION (FACT.").append(relationEntry.relation.leftColumn).append("=").append(relationEntry.relation.rightColumn).append(") INCLUDE (")

          val includeFields = relColumns.map(field => field._1)
          relation = relation.append(includeFields.mkString(", ")).append(")")

        })
      }

      levelsToCheckDuplicate = levelsToCheckDuplicate ++ levels
    }
    else {
      // Create Cube DDL with Schema defination 
      levels = cm.dimCols.map(field => Level(field.name.getOrElse(field.column), field.column, Int.MaxValue, field.dataType.getOrElse(CarbonCommonConstants.STRING)))
      measures = cm.msrCols.map(field => Measure(field.name.getOrElse(field.column), field.column, field.dataType.getOrElse(CarbonCommonConstants.NUMERIC)))
      levelsToCheckDuplicate = levels

      if (cm.withKeyword.equalsIgnoreCase(CarbonCommonConstants.WITH) && cm.simpleDimRelations.size > 0) {
        cm.simpleDimRelations.foreach(relationEntry => {

          val split = levels.partition(x => relationEntry.cols.get.contains(x.name))
          val dimFileLevels = split._1

          if (dimFileLevels.filter(l => l.name.equalsIgnoreCase(relationEntry.relation.rightColumn)).length <= 0) {
            val rcl = relationEntry.relation.rightColumn
            LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Dimension field defined in the relation [$rcl] is not present in the Dimension source")
            sys.error(s"Dimension field defined in the relation [$rcl] is not present in the Dimension source")
          }

          val dimFileHierarchies = dimFileLevels.map(field => Hierarchy(relationEntry.tableName, Some(dimFileLevels.find(dl => dl.name.equalsIgnoreCase(relationEntry.relation.rightColumn)).get.column), Seq(field), Some(relationEntry.tableName)))
          dimFileDimensions = dimFileDimensions ++ dimFileHierarchies.map(field => Dimension(field.levels.head.name, Seq(field), Some(relationEntry.relation.leftColumn)))

          if (relation.length > 0) relation = relation.append(", ")
          relation = relation.append(relationEntry.tableName).append(" RELATION (FACT.").append(relationEntry.relation.leftColumn).append("=").append(relationEntry.relation.rightColumn).append(") INCLUDE (")
          relation = relation.append(relationEntry.cols.get.mkString(", ")).append(")")
        })

      }
    }

    // Check if there is any duplicate measures or dimensions. Its based on the dimension name and measure name
    levelsToCheckDuplicate.groupBy(_.name).foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Duplicate dimensions found with name : $name")
      sys.error(s"Duplicate dimensions found with name : $name")
    })

    measures.groupBy(_.name).foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Duplicate measures found with name : $name")
      sys.error(s"Duplicate measures found with name : $name")
    })

    val levelsArray = levelsToCheckDuplicate.map(_.name)
    val levelsNdMesures = levelsArray ++ measures.map(_.name)

    cm.aggregation.foreach(a => {
      if (levelsArray.contains(a.msrName)) {
        val fault = a.msrName
        LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Aggregator should not be defined for dimension fields [$fault]")
        sys.error(s"Aggregator should not be defined for dimension fields [$fault]")
      }
    })

    levelsNdMesures.groupBy(x => x).foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Dimension and Measure defined with same name : $name")
      sys.error(s"Dimension and Measure defined with same name : $name")
    })

    if (levelsArray.size <= 0) {
      sys.error("No Dimensions defined. Cube should have atleast one dimesnion !")
    }

    val dims = levelsToCheckDuplicate.map(l => l.name + " " + l.dataType)
    command = command.append("DIMENSIONS (").append(dims.mkString(", ")).append(") ")

    if (measures.size > 0) {
      val mesrs = measures.map(m => m.name + " " + m.dataType)
      command = command.append("MEASURES (").append(mesrs.mkString(", ")).append(")")
    }

    if (relation.length > 0) {
      command = command.append(" WITH ").append(relation)
    }

    if (cm.aggregation.size > 0 || cm.partitioner.isDefined) {
      command = command.append(" OPTIONS( ")

      if (cm.aggregation.size > 0) {
        val aggs = cm.aggregation.map(a => a.msrName + "=" + a.aggType)
        command = command.append("AGGREGATION[ ").append(aggs.mkString(", ")).append(" ] ")
        if (cm.partitioner.isDefined) command = command.append(", ")
      }

      if (cm.partitioner.isDefined) {
        val partn = cm.partitioner.get
        command = command.append("PARTITIONER[ CLASS='").append(partn.partitionClass).append("', COLUMNS=(").append(partn.partitionColumn.mkString(", ")).append("), PARTITION_COUNT=").append(partn.partitionCount).append(" ]")
      }

      command = command.append(" )")
    }

    command = command.append(";")

    val hierarchies = levels.map(field => Hierarchy(field.name, None, Seq(field), None))
    var dimensions = hierarchies.map(field => Dimension(field.name, Seq(field), None))
    dimensions = dimensions ++ dimFileDimensions

    cm.partitioner match {
      case Some(part: Partitioner) =>
        var definedpartCols = part.partitionColumn
        val columnBuffer = new ArrayBuffer[String]
        part.partitionColumn.foreach { col =>
          dimensions.foreach { dim =>
            dim.hierarchies.foreach { hier =>
              hier.levels.foreach { lev =>
                if (lev.name.equalsIgnoreCase(col)) {
                  definedpartCols = definedpartCols.dropWhile(c => c.equals(col))
                  columnBuffer.add(lev.name)
                }
              }
            }
          }
        }

        try {
          Class.forName(part.partitionClass).newInstance()
        } catch {
          case e: Exception => {
            val cl = part.partitionClass
            sys.error(s"partition class specified can not be found or loaded : $cl")
          }
        }

        if (definedpartCols.size > 0) {
          val msg = definedpartCols.mkString(", ")
          LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"partition columns specified are not part of Dimension columns : $msg")
          sys.error(s"partition columns specified are not part of Dimension columns : $msg")
        }

      case None =>
    }
//    println(command.toString)
    Seq(Row(command.toString))
  }

  def getDataFrame(factSource: Object, sqlContext: SQLContext): DataFrame = {

    val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.cubemodel.cubeSchema")
    var dataFrame: DataFrame = null

    if (factSource.isInstanceOf[String]) {
      val factFile = CarbonUtil.checkAndAppendHDFSUrl(factSource.asInstanceOf[String])
      val fileType = FileFactory.getFileType(factFile)

      if (FileFactory.isFileExist(factFile, fileType)) {
        dataFrame = sqlContext.load("com.databricks.spark.csv", Map("path" -> factFile, "header" -> "true", "inferSchema" -> "true"))
      }
      else {
        LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Input source file $factFile does not exists")
        sys.error(s"Input source file $factFile does not exists")
      }
    }
    else if (factSource.isInstanceOf[Seq[String]]) {
      val tableInfo = factSource.asInstanceOf[Seq[String]]
      val dbName = if (tableInfo.size > 1) tableInfo(0) else getDB.getDatabaseName(None, sqlContext)
      val tableName = if (tableInfo.size > 1) tableInfo(1) else tableInfo(0);

      if (sqlContext.tableNames(dbName).map(x => x.toLowerCase()).contains(tableName.toLowerCase())) {
        if (dbName.size > 0) {
          dataFrame = DataFrame(sqlContext, sqlContext.catalog.lookupRelation(Seq(dbName, tableName)))
        }
        else {
          dataFrame = DataFrame(sqlContext, sqlContext.catalog.lookupRelation(Seq(tableName)))
        }
      }
      else {
        LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Input source table $tableName does not exists")
        sys.error(s"Input source table $tableName does not exists")
      }
    }
    dataFrame
  }

  // For filtering INCLUDE and EXCLUDE fields if any is defined for Dimention relation
  def filterRelIncludeCols(relationEntry: DimensionRelation, p: (String, String)): Boolean = {
    if (relationEntry.includeKey.get.equalsIgnoreCase(CarbonCommonConstants.INCLUDE)) {
      relationEntry.cols.get.map(x => x.toLowerCase()).contains(p._1.toLowerCase())
    } else {
      !relationEntry.cols.get.map(x => x.toLowerCase()).contains(p._1.toLowerCase())
    }
  }

  private def getUpdatedLevels(levels: Seq[Level], fms: Seq[FieldMapping]): Seq[Level] = {
    levels.map { f =>
      val matchedMapping = fms.filter(fm => f.name == fm.levelName)
      if (matchedMapping.length == 0) {
        f
      } else {
        Level(f.name, matchedMapping.head.columnName, f.cardinality, f.dataType)
      }
    }
  }
}


/**
  * These are the assumptions made
  * 1.We have a single hierarchy under a dimension tag and a single level under a hierarchy tag
  * 2.The names of dimensions and measures are case insensitive
  * 3.CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE is always added as a measure.So we need to ignore this to check duplicates
  */
private[sql] case class AlterCube(
                                   cm: CubeModel,
                                   dropCols: Seq[String],
                                   defaultVals: Seq[Default]) extends RunnableCommand {

//  override def getControlPrivileges(sqlContext: SQLContext): Set[PrivObject] = {
//    Set(new PrivObject(
//      ObjectType.TABLE,
//      getDB.getDatabaseName(cm.schemaNameOp, sqlContext),
//      cm.cubeName,
//      null,
//      Set(PrivType.OWNER_PRIV)))
//  }

  def run(sqlContext: SQLContext) = {
    val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.cubemodel.cubeSchema")
    cm.schemaName = getDB.getDatabaseName(cm.schemaNameOp, sqlContext)
    val schemaName = cm.schemaName
    val cubeName = cm.cubeName
    LOGGER.audit(s"Altering cube with Schema name [$schemaName] and cube name [$cubeName]")

    val tmpCube = CarbonMetadata.getInstance().getCube(schemaName + "_" + cubeName)
    if (null == tmpCube) {
      LOGGER.audit(s"Altering cube with Schema name [$schemaName] and cube name [$cubeName] failed. Cube $schemaName.$cubeName does not exist")
      LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Cube $schemaName.$cubeName does not exist")
      sys.error(s"Cube $schemaName.$cubeName does not exist")
    }

     val carbonLock = CarbonLockFactory.getCarbonLockObj(tmpCube.getMetaDataFilepath(),LockUsage.METADATA_LOCK)
    try {
      if (carbonLock.lockWithRetries()) {
        logInfo("Successfully able to get the cube metadata file lock")
      }
      else {
        LOGGER.audit(s"Altering cube with Schema name [$schemaName] and cube name [$cubeName] failed")
        sys.error("Cube is locked for updation. Please try after some time")
      }
      val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(
        Option(cm.schemaName),
        cm.cubeName,
        None)(sqlContext).asInstanceOf[CarbonRelation]

      if (null == relation) {
        LOGGER.audit(s"Altering cube with Schema name [$schemaName] and cube name [$cubeName] failed")
        LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Cube $schemaName.$cubeName does not exist")
        sys.error(s"Cube $schemaName.$cubeName does not exist")
      }
      val foundDropColsList = new ArrayBuffer[String]
      val dropColsList = new ArrayBuffer[String]
      dropCols.foreach { eachElem => if (!dropColsList.contains(eachElem.toLowerCase())) {
        dropColsList.add(eachElem.toLowerCase())
      }
      }
      val curTime = System.nanoTime()
      //check if the dimension to drop is a partition column.We do not allow to delete partition column
      val partitionCols = relation.cubeMeta.partitioner.partitionColumn

      if (partitionCols.exists(partCol => dropColsList.contains(partCol.toLowerCase()))) {
        val partitionColsString = partitionCols.mkString(",")
        LOGGER.audit(s"Altering cube with Schema name [$schemaName] and cube name [$cubeName] failed")
        LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Cannot Delete the partition column(s).")
        sys.error(s"Cannot Delete the partition column(s).Partition columns are [$partitionColsString]")
      }

      val metaDataPath = CarbonMetadata.getInstance()
        .getCube(relation.cubeMeta.schemaName + '_' + relation.cubeMeta.cubeName)
        .getMetaDataFilepath
      val fileType = FileFactory.getFileType(metaDataPath)
      val file = FileFactory.getCarbonFile(metaDataPath, fileType)

      val (_, _, _, tmpSchemaXML, _,_) = CarbonEnv.getInstance(sqlContext).carbonCatalog.readCubeMetaDataFile(file, fileType)

      val carbonDefSchema = CarbonMetastoreCatalog.parseStringToSchema(tmpSchemaXML)

      val validDropMsrList = new ArrayBuffer[String]
      val validDropDimList = new ArrayBuffer[String]

      //remove the deleted measures from the schema
      processDroppedMeasures(carbonDefSchema, curTime, dropColsList.toList, foundDropColsList, validDropMsrList)
      //remove the deleted dimensions from the schema
      processDroppedDimensions(carbonDefSchema, curTime, dropColsList.toList, foundDropColsList, validDropDimList)

      //if there are invalid dimensions/measures fail the command
      if ((dropColsList diff foundDropColsList).length > 0) {
        val buffer: StringBuffer = new StringBuffer()
        //we need to add the elements from dropCols which are the original names(including case) given by user in the DROP command
        dropCols.foreach { elem => if (!foundDropColsList.contains(elem.toLowerCase())) buffer.append(elem).append(",") }
        var notFoundString: String = buffer.toString()
        //trim the last ,
        notFoundString = notFoundString.substring(0, notFoundString.length() - 1)
        LOGGER.audit(s"Altering cube with Schema name [$schemaName] and cube name [$cubeName] Failed")
        LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Altering cube with Schema name [$schemaName] and cube name [$cubeName] Failed.The following dimensions/measures are not present in the cube : [$notFoundString]")
        sys.error(s"Altering cube with Schema name [$schemaName] and cube name [$cubeName] Failed.The following dimensions/measures are not present in the cube : [$notFoundString]")
      }

      //get added measures,dimensions and add to schema.
      val cubeXML = getNewDimensionMeasures(cm, sqlContext)

      //        var needNewRSFolder = false
      //
      //        if (cubeXML.dimensions.length > 0 || cubeXML.measures.length > 0) {
      //          needNewRSFolder = true
      //        }

      //check if any duplicate measures or dimensions are being added to the cube.
      val duplicatesFound = checkDuplicateDimsAndMsrs(carbonDefSchema.cubes(0), cubeXML.dimensions, cubeXML.measures)
      if (duplicatesFound) {
        LOGGER.audit(s"Altering cube with Schema name [$schemaName] and cube name [$cubeName] failed")
        LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Cube already contains the dimensions/measures being added")
        sys.error(s"Cube already contains the dimensions/measures being added.")
      }

      if (cm.withKeyword.equalsIgnoreCase(CarbonCommonConstants.WITH) && cm.simpleDimRelations.size > 0) {
        if (!validateDimRelations(carbonDefSchema.cubes(0), cubeXML)) {
          LOGGER.audit(s"Altering cube with Schema name [$schemaName] and cube name [$cubeName] failed")
          LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG,
            s"There is an error in the relation defined. Dimension field defined in the relation is not included in the Dimension source Or the existing relation is not present in the Cube")
          sys.error(s"There is an error in the relation defined. Dimension field defined in the relation is not included in the Dimension source Or the existing relation is not present in the Cube.")
        }
      }

      //      relation.cubeMeta.schema.cubes(0).dimensions = relation.cubeMeta.schema.cubes(0).dimensions ++ cubeXML.dimensions
      //      relation.cubeMeta.schema.cubes(0).measures = relation.cubeMeta.schema.cubes(0).measures ++ cubeXML.measures

      carbonDefSchema.cubes(0).dimensions = carbonDefSchema.cubes(0).dimensions ++ cubeXML.dimensions
      carbonDefSchema.cubes(0).measures = carbonDefSchema.cubes(0).measures ++ cubeXML.measures

      //need to remove the existing cube and load it again
      //      CarbonMetadata.getInstance().loadCube(relation.cubeMeta.schema, relation.cubeMeta.schema.cubes(0))
      //      relation.cubeMeta.cube = CarbonMetadata.getInstance().getCube(relation.cubeMeta.schemaName + '_' + relation.cubeMeta.cubeName)

      var status = true
      //if (needNewRSFolder) {
      val defaultValsMap = defaultVals.map(x => x.key -> x.value)(collection.breakOut): Map[String, String]

      status = CarbonDataRDDFactory.alterCube(
        sqlContext.asInstanceOf[HiveContext],
        sqlContext.sparkContext,
        relation.cubeMeta.schema,
        carbonDefSchema,
        relation.cubeMeta.schemaName,
        relation.cubeMeta.cubeName,
        relation.cubeMeta.dataPath,
        cubeXML.dimensions,
        cubeXML.measures,
        validDropDimList,
        validDropMsrList,
        curTime,
        defaultValsMap,
        relation.cubeMeta.partitioner)
      //   }

      if (status) {
        //          if (!needNewRSFolder) {
        //            CarbonEnv.getInstance(sqlContext).carbonCatalog.updateCube(/*relation.cubeMeta.schema*/carbonDefSchema, false)(sqlContext)
        //          }

        val updatedRelation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(
          Option(cm.schemaName),
          cm.cubeName,
          None)(sqlContext).asInstanceOf[CarbonRelation]

        //        relation.cubeMeta.schema.cubes(0).dimensions = relation.cubeMeta.schema.cubes(0).dimensions ++ cubeXML.dimensions
        //        relation.cubeMeta.schema.cubes(0).measures = relation.cubeMeta.schema.cubes(0).measures ++ cubeXML.measures

        //need to remove the existing cube and load it again
        CarbonMetadata.getInstance().loadCube(updatedRelation.cubeMeta.schema,
            updatedRelation.cubeMeta.schema.name,
            updatedRelation.cubeMeta.schema.cubes(0).name,
            updatedRelation.cubeMeta.schema.cubes(0))
        updatedRelation.cubeMeta.cube = CarbonMetadata.getInstance()
          .getCube(updatedRelation.cubeMeta.schemaName + '_' + updatedRelation.cubeMeta.cubeName)

        LOGGER.audit(s"Altering cube with Schema name [$schemaName] and cube name [$cubeName] succeeded")
      }
      else {
        LOGGER.audit(s"Altering cube with Schema name [$schemaName] and cube name [$cubeName] Failed")
        LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Altering cube with Schema name [$schemaName] and cube name [$cubeName] Failed")
        sys.error(s"Altering cube with Schema name [$schemaName] and cube name [$cubeName] Failed")
      }
    } finally {
      if (carbonLock != null) {
        if (carbonLock.unlock()) {
          logInfo("Cube MetaData Unlocked Successfully after altering")
        } else {
          logError("Unable to unlock Cube MetaData")
        }
      }
    }

    Seq.empty
  }

  private def validateDimRelations(existingCube: CarbonDef.Cube, newCube: CarbonDef.Cube): Boolean = {

    val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.cubemodel.cubeSchema")

    def matchPrimaryForeignKeys(foundDimRelations: ArrayBuffer[DimensionRelation], aDim: CubeDimension): Boolean = {
      val rel = aDim.asInstanceOf[CarbonDef.Dimension].hierarchies(0).relation
      if (null != rel) {
        val tableName = rel.asInstanceOf[CarbonDef.Table].name
        return cm.simpleDimRelations.find(p => {
          if (p.tableName == tableName) {
            foundDimRelations += p
            true
          } else {
            false
          }
        }).map(dim => {
          (dim.relation.rightColumn == aDim.asInstanceOf[CarbonDef.Dimension].hierarchies(0).primaryKey
            && dim.relation.leftColumn == aDim.asInstanceOf[CarbonDef.Dimension].foreignKey)
        }).getOrElse(false)
      }
      false
    }

    val result = true

    if (!result) {
      result
    } else {
      val tables = cm.simpleDimRelations.map(d => d.tableName)

      val filteredExistingDims = existingCube.dimensions.filter(dim => {
        val rel = dim.asInstanceOf[CarbonDef.Dimension].hierarchies(0).relation
        if (null != rel) {
          val tableName = rel.asInstanceOf[CarbonDef.Table].name
          if (null != tableName && !tableName.isEmpty) {
            tables.contains(tableName)
          } else {
            false
          }
        }
        else false
      })

      val foundDimRelations: ArrayBuffer[DimensionRelation] = new ArrayBuffer[DimensionRelation]()
      var matchResult = filteredExistingDims.forall(aDim => {
        matchPrimaryForeignKeys(foundDimRelations, aDim)
      })

      if (matchResult) {
        val notFoundDimRelations = cm.simpleDimRelations diff foundDimRelations
        val levels = cm.dimCols.map(field =>
          Level(field.name.getOrElse(field.column),
            field.column, Int.MaxValue,
            field.dataType.getOrElse(CarbonCommonConstants.STRING)))

        notFoundDimRelations.foreach(eachDimRelation => {
          val split = levels.partition(x => eachDimRelation.cols.get.contains(x.name))
          val dimLevels = split._1
          if (dimLevels.filter(l => l.name.equalsIgnoreCase(eachDimRelation.relation.rightColumn)).length <= 0) {
            val rcl = eachDimRelation.relation.rightColumn
            LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Dimension field defined in the relation [$rcl] is not included in the Dimension source")
            //sys.error(s"Dimension field defined in the relation [$rcl] is not included in the Dimension source")
            matchResult = false
          }
        })
        matchResult
      } else {
        matchResult
      }
    }
  }

  private def checkDuplicateDimsAndMsrs(cube: CarbonDef.Cube, newDims: Array[CubeDimension], newMsrs: Array[CarbonDef.Measure]): Boolean = {
    var dimNameList = new ListBuffer[String]()
    var dimColumnList = new ListBuffer[String]()
    var msrNameList = new ListBuffer[String]()
    var msrColumnList = new ListBuffer[String]()

    for (dim <- cube.dimensions) {
      dimNameList += dim.name.toLowerCase()
      dimColumnList += dim.asInstanceOf[CarbonDef.Dimension].hierarchies(0).levels(0).column.toLowerCase()
    }
    
    for (msr <- cube.measures) {
      msrNameList += msr.name.toLowerCase()
      msrColumnList += msr.column.toLowerCase()
    }
    
    newDims.foreach { newDim =>
      val name = newDim.name.toLowerCase()
      val column = newDim.asInstanceOf[CarbonDef.Dimension].hierarchies(0).levels(0).column.toLowerCase()
      //Same column name for two Dimention/Measures is not currently supported, can later be corrected.
      if (dimNameList.contains(name) || dimColumnList.contains(name) || dimNameList.contains(column) || dimColumnList.contains(column)
          ||msrNameList.contains(name) || msrColumnList.contains(name) || msrNameList.contains(column) || msrColumnList.contains(column)) 
      {
        return true
      }
    }    

    newMsrs.foreach { newMsr =>
      val name = newMsr.name.toLowerCase()
      val column = newMsr.column.toLowerCase()
      //Same column name for two Dimention/Measures is not currently supported, can later be corrected.
      if (dimNameList.contains(name) || dimColumnList.contains(name) || dimNameList.contains(column) || dimColumnList.contains(column)
          || msrNameList.contains(name) || msrColumnList.contains(name) || msrNameList.contains(column) || msrColumnList.contains(column))
      {
        return true
      }
    }
    return false
  }

  private def getNewDimensionMeasures(cm: CubeModel, sqlContext: SQLContext): CarbonDef.Cube = {
    // use the same CubeProcessor used by CreateCube so that the  validations are common
    val cube = CubeProcessor(cm, sqlContext)
    val cubeXML = new CarbonDef.Cube

    cubeXML.dimensions = Seq[CarbonDef.Dimension]().toArray
    cubeXML.measures = Seq[CarbonDef.Measure]().toArray

    addNewMeasuresToCube(cubeXML, cube)

    //CubeProcessor by default adds a DEFAULT_INVISIBLE_DUMMY_MEASURE.This is required for CreateCube but not for adding a
    //new measure.Hence after forming the measures object, we remove it from the list
    cubeXML.measures = cubeXML.measures.filter { x => x.name != CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE }
    addNewDimensionsToCube(cubeXML, cube)

    cubeXML
  }

  private def addNewMeasuresToCube(cubeXML: CarbonDef.Cube, cube: Cube) {
    cubeXML.measures = cube.measures.map { x =>
      val measure = new CarbonDef.Measure
      measure.name = x.name
      measure.datatype = x.dataType
      measure.aggregator = x.aggregator.toLowerCase
      measure.column = x.column
      measure.visible = true
      measure
    }.toArray
  }

  private def addNewDimensionsToCube(cubeXML: CarbonDef.Cube, cube: Cube) {
    cubeXML.dimensions = cube.dimensions.map { dim =>
      val dimXml = new CarbonDef.Dimension
      dimXml.name = dim.name
      setV(dimXml, "type", dim.dimType)
      dimXml.visible = true
      dim.foreignKey match {
        case Some(fKey: String) =>
          dimXml.foreignKey = fKey
        case others =>
      }

      dimXml.hierarchies = dim.hierarchies.map { hier =>
        val hierXml = new CarbonDef.Hierarchy
        hierXml.name = hier.name
        hierXml.hasAll = true
        hierXml.visible = true
        hierXml.normalized = false
        hier.tableName match {
          case Some(tble: String) =>
            val table = new CarbonDef.Table
            table.name = tble
            hierXml.relation = table
          case others =>
        }

        hier.primaryKey match {
          case Some(pKey: String) => hierXml.primaryKey = pKey
          case others =>
        }

        hierXml.memberReaderParameters = Seq().toArray

        hierXml.levels = hier.levels.map { level =>
          val levelXml = new CarbonDef.Level
          //levelXml.levelCardinality=level.cardinality
          levelXml.name = level.name
          levelXml.column = level.column
          levelXml.levelType = level.levelType
          //TODO: find away to assign type in scala
          //levelXml.type = level.dataType
          setV(levelXml, "type", level.dataType)
          levelXml.visible = true
          levelXml.columnIndex = -1
          levelXml.keyOrdinal = -1
          levelXml.levelCardinality = -1
          levelXml.ordinalColumnIndex = -1
          levelXml.nameColumnIndex = -1
          levelXml.uniqueMembers = false
          levelXml.hideMemberIf = "Never"
          levelXml.isParent = true
          levelXml.properties = Seq().toArray
          levelXml
        }.toArray
        hierXml
      }.toArray
      dimXml
    }.toArray
  }

  private def setV(ref: Any, name: String, value: Any): Unit = {
    ref.getClass.getFields.find(_.getName == name).get.set(ref, value.asInstanceOf[AnyRef])
  }

  private def processDroppedMeasures(
                                      carbonDefSchema: CarbonDef.Schema,
                                      curTime: Long, dropColsList: List[String],
                                      foundDropColsList: ArrayBuffer[String],
                                      validDropMsrList: ArrayBuffer[String]) = {
    carbonDefSchema.cubes(0).measures = carbonDefSchema.cubes(0).measures.map { aMsr =>
      if (dropColsList.contains(aMsr.name.toLowerCase())) {
        if (!foundDropColsList.contains(aMsr.name.toLowerCase())) {
          foundDropColsList.add(aMsr.name.toLowerCase())
          validDropMsrList.add(aMsr.name)
        }
        aMsr.visible = false
        updateAggTablesMeasures(curTime, aMsr, carbonDefSchema.cubes(0).fact.asInstanceOf[CarbonDef.Table].aggTables)
        aMsr.name = aMsr.name + "_" + curTime
        aMsr.column = aMsr.column + '_' + curTime
      }
      aMsr
    }
  }

  private def processDroppedDimensions(
                                        carbonDefSchema: CarbonDef.Schema,
                                        curTime: Long,
                                        dropColsList: List[String],
                                        foundDropColsList: ArrayBuffer[String],
                                        validDropDimList: ArrayBuffer[String]) = {
    //the assumption here is that the dimension,hierarchy and level names will be same.
    carbonDefSchema.cubes(0).dimensions = carbonDefSchema.cubes(0).dimensions.map { eachDim =>
      //iterate through hierarchies and levels and mark the level as visible=false.Not only the dimension itself
      (eachDim.asInstanceOf[CarbonDef.Dimension]).hierarchies = (eachDim.asInstanceOf[CarbonDef.Dimension]).hierarchies.map { eachHierarchy =>
        eachHierarchy.levels = eachHierarchy.levels.map { eachLevel =>
          if (dropColsList.contains(eachLevel.name.toLowerCase())) {
            if (!foundDropColsList.contains(eachLevel.name.toLowerCase())) {
              foundDropColsList.add(eachLevel.name.toLowerCase())
              validDropDimList.add(eachLevel.name)
            }
            eachLevel.visible = false
            updateAggTablesDimensions(curTime, eachLevel.name, eachHierarchy.name, eachDim.name, carbonDefSchema.cubes(0).fact.asInstanceOf[CarbonDef.Table].aggTables)
            eachLevel.name = eachLevel.name + '_' + curTime
            eachLevel.column = eachLevel.column + '_' + curTime
            eachHierarchy.visible = false
            eachHierarchy.name = eachHierarchy.name + '_' + curTime
            eachDim.visible = false
            eachDim.name = eachDim.name + '_' + curTime
          }
          eachLevel
        }
        eachHierarchy
      }
      eachDim
    }
  }

  private def updateAggTablesDimensions(curTime: Long, levelName: String, hierName: String, dimName: String, aggTables: Array[AggTable]): Array[AggTable] = {
    val oldAggLevel: String = s"[$dimName].[$hierName].[$levelName]"
    val newAggLevel: String = /*s"[$dimName_$curTime].[$hierName'_$curTime].[$levelName'_$curTime]"*/ '[' + dimName + '_' + curTime + "].[" + hierName + '_' + curTime + "].[" + levelName + '_' + curTime + ']'

    aggTables.map { anAggTable => {
      anAggTable.levels = anAggTable.getAggLevels.map { anAggLevel => {
        if (anAggLevel.name.equals(oldAggLevel) /*endsWith('[' + levelName + ']')*/ ) {
          val newName: String = levelName + "_" + curTime
          val index = anAggLevel.name.lastIndexOf('[')
          if (-1 != index) {
            anAggLevel.name = newAggLevel /*anAggLevel.name.substring(0, index + 1) + newName + ']'*/
          } else {
            sys.error(s"Invalid agg table $anAggTable.getName in $cm.schemaName.$cm.cubeName.Agg Levels are of wrong format")
          }
        }
        anAggLevel
      }
      }
      anAggTable
    }
    }
  }

  private def updateAggTablesMeasures(curTime: Long, factMeas: CarbonDef.Measure, aggTables: Array[AggTable]): Array[AggTable] = {
    aggTables.map { anAggTable => {
      anAggTable.measures = anAggTable.getAggMeasures.map { anAggMeasure => {
        if (anAggMeasure.name.endsWith('[' + factMeas.name + ']')) {
          val newName: String = factMeas.name + "_" + curTime
          val index = anAggMeasure.name.lastIndexOf('[')
          if (-1 != index) {
            anAggMeasure.name = anAggMeasure.name.substring(0, index + 1) + newName + ']'
            anAggMeasure.column = newName
          } else {
            sys.error(s"Invalid agg table $anAggTable.getName in $cm.schemaName.$cm.cubeName.Agg measures are of wrong format")
          }
        }
        anAggMeasure
      }
      }
      anAggTable
    }
    }
  }
}


private[sql] case class CreateCube(cm: CubeModel) extends RunnableCommand {

//  override def getControlPrivileges(sqlContext: SQLContext): Set[PrivObject] = {
//    //CREATE privilege@ database level
//    Set(new PrivObject(
//      ObjectType.DATABASE,
//      getDB.getDatabaseName(cm.schemaNameOp, sqlContext),
//      null,
//      null,
//      Set(PrivType.OWNER_PRIV)))
//  }

  def run(sqlContext: SQLContext) = {
    val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.cubemodel.cubeSchema");
    cm.schemaName = getDB.getDatabaseName(cm.schemaNameOp, sqlContext)
    val s = cm.schemaName
    val c = cm.cubeName
    LOGGER.audit(s"Creating cube with Schema name [$s] and cube name [$c]")

    if (cm.withKeyword.equalsIgnoreCase(CarbonCommonConstants.WITH) && cm.simpleDimRelations.size > 0) {
      val levels = cm.dimCols.map(field => Level(field.name.getOrElse(field.column), field.column, Int.MaxValue, field.dataType.getOrElse(CarbonCommonConstants.STRING)))
      cm.simpleDimRelations.foreach(relationEntry => {

        // Split the levels and seperate levels with dimension levels
        val split = levels.partition(x => relationEntry.cols.get.contains(x.name))
        val dimLevels = split._1

        if (dimLevels.filter(l => l.name.equalsIgnoreCase(relationEntry.relation.rightColumn)).length <= 0) {
          val rcl = relationEntry.relation.rightColumn
          LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Dimension field defined in the relation [$rcl] is not included in the Dimension source")
          LOGGER.audit(s"Validation failed for Create Cube Operation - Dimension field defined in the relation [$rcl] is not included in the Dimension source")
          sys.error(s"Dimension field defined in the relation [$rcl] is not included in the Dimension source")
        }
      })
    }

    val cube = CubeProcessor(cm, sqlContext)

    if (cube.dimensions.size <= 0) {
      sys.error("No Dimensions found. Cube should have atleast one dimesnion !")
    }

    val cubeName = cm.cubeName
    val dbName = cm.schemaName

    if (sqlContext.tableNames(cm.schemaName).map(x => x.toLowerCase()).contains(cm.cubeName.toLowerCase())) {
      if (!cm.ifNotExistsSet) {
        LOGGER.audit(s"Cube ceation with Schema name [$dbName] and cube name [$cubeName] failed. Table [$cubeName] already exists under database [$dbName]")
        sys.error(s"Table [$cubeName] already exists under database [$dbName]")
      }
    }
    else {
      val schema = new CarbonDef.Schema();
      schema.parameters = Seq[CarbonDef.Parameter]().toArray
      schema.cubes = Seq[CarbonDef.Cube]().toArray
      schema.dimensions = Seq[CarbonDef.Dimension]().toArray
      schema.namedSets = Seq[CarbonDef.NamedSet]().toArray
      schema.roles = Seq[CarbonDef.Role]().toArray
      schema.userDefinedFunctions =
        Seq[CarbonDef.UserDefinedFunction]().toArray
      schema.virtualCubes = Seq[CarbonDef.VirtualCube]().toArray
      schema.name = cube.schemaName
      val cubexML = new CarbonDef.Cube()
      cubexML.name = cube.cubeName;

      cubexML.dimensions = Seq[CarbonDef.Dimension]().toArray
      cubexML.measures = Seq[CarbonDef.Measure]().toArray
      cubexML.calculatedMembers = Seq[CarbonDef.CalculatedMember]().toArray
      cubexML.namedSets = Seq[CarbonDef.NamedSet]().toArray
      cubexML.cache = true
      cubexML.enabled = true
      cubexML.visible = true
      schema.cubes = Seq(cubexML).toArray
      val msrs = cube.measures.map { x =>
        val measure = new CarbonDef.Measure
        measure.name = x.name
        measure.datatype = x.dataType
        measure.aggregator = x.aggregator.toLowerCase
        measure.column = x.column
        measure.visible = x.visible
        measure
      }
      cubexML.measures = msrs.toArray
      val fact = new CarbonDef.Table
      fact.name = cube.tableName
      fact.alias = ""
      cubexML.fact = fact
      cubexML.dimensions = cube.dimensions.map { dim =>
        val dimXml = new CarbonDef.Dimension
        dimXml.name = dim.name
        dimXml.visible = dim.visible
        dimXml.highCardinality = dim.highCardinality
        setV(dimXml, "type", dim.dimType)
        

        dim.foreignKey match {
          case Some(fKey: String) =>
            dimXml.foreignKey = fKey
          case others =>
        }

        dimXml.hierarchies = dim.hierarchies.map { hier =>
          val hierXml = new CarbonDef.Hierarchy
          hierXml.name = hier.name
          hierXml.hasAll = true
          hierXml.normalized = hier.normalized

          hier.tableName match {
            case Some(tble: String) =>
              val table = new CarbonDef.Table
              table.name = tble
              hierXml.relation = table
            case others =>
          }

          hier.primaryKey match {
            case Some(pKey: String) => hierXml.primaryKey = pKey
            case others =>
          }

          hierXml.levels = hier.levels.map { level =>
            val levelXml = new CarbonDef.Level
            //levelXml.levelCardinality=level.cardinality
            levelXml.name = level.name
            levelXml.column = level.column
            levelXml.levelType = level.levelType
            if("row".equals(level.storeType))
            {
              levelXml.columnar=false
            }
            if(level.parent != null)
              levelXml.parentname = level.parent
            //TODO: find away to assign type in scala
            //levelXml.type = level.dataType
            setV(levelXml, "type", level.dataType)
            levelXml
          }.toArray
          hierXml
        }.toArray
        dimXml
      }.toArray

      //val schemaWithAggs = GenerateAggTables(schema).apply
      //println(schemaWithAggs.toXML())
//      println(schema.toXML())

      //Add schema to catalog and persist
      val catalog = CarbonEnv.getInstance(sqlContext).carbonCatalog
      val cubePath = catalog.createCube(cube.schemaName, cube.cubeName, schema.toXML(), cube.partitioner, false)(sqlContext)
      try {
        sqlContext.sql(
          s"""CREATE TABLE $dbName.$cubeName USING org.apache.spark.sql.CarbonSource""" +
            s""" OPTIONS (cubename "$dbName.$cubeName", path "$cubePath") """).collect
      } catch {
        case e: Exception =>

          val schemaName = cube.schemaName
          val cubeName = cube.cubeName
          val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation2(Seq(schemaName, cubeName))(sqlContext).asInstanceOf[CarbonRelation]
          if (relation != null) {
            LOGGER.audit(s"Deleting cube [$cubeName] under schema [$schemaName] as create TABLE failed")
            CarbonEnv.getInstance(sqlContext).carbonCatalog.dropCube(
              relation.cubeMeta.partitioner.partitionCount,
              relation.cubeMeta.dataPath,
              schemaName,
              cubeName)(sqlContext)
          }


          LOGGER.audit(s"Cube ceation with Schema name [$s] and cube name [$c] failed")
          throw e
      }

      LOGGER.audit(s"Cube created with Schema name [$s] and cube name [$c]")
    }

    Seq.empty
  }

  def setV(ref: Any, name: String, value: Any): Unit =
    ref.getClass.getFields.find(_.getName == name).get.set(ref, value.asInstanceOf[AnyRef]) //.get.invoke(ref, value.asInstanceOf[AnyRef])))))))
}

private[sql] case class DeleteLoadsById(
                                         loadids: Seq[String],
                                         schemaNameOp: Option[String],
                                         cubeName: String) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.cubemodel.cubeSchema");

  def run(sqlContext: SQLContext) = {

    LOGGER.audit("The delete load by Id request has been received.");
    val schemaName = getDB.getDatabaseName(schemaNameOp, sqlContext)

    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(
      Option(schemaName),
      cubeName,
      None)(sqlContext).asInstanceOf[CarbonRelation]
    if (relation == null) {
      LOGGER.audit(s"The delete load by Id is failed. Cube $schemaName.$cubeName does not exist");
      sys.error(s"Cube $schemaName.$cubeName does not exist")
    }

    var cube = CarbonMetadata.getInstance().getCube(schemaName + '_' + cubeName)

    if (null == cube) {
      var relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(
        Option(schemaName),
        cubeName,
        None)(sqlContext).asInstanceOf[CarbonRelation]
      var schema = relation.cubeMeta.schema
      var mondrianCube = CarbonSchemaParser.getMondrianCube(schema, cubeName);
      CarbonMetadata.getInstance().loadCube(schema, schema.name, mondrianCube.name, mondrianCube);
      cube = CarbonMetadata.getInstance().getCube(schemaName + '_' + cubeName);
    }
    var path = cube.getMetaDataFilepath()


    var invalidLoadIds = DeleteLoadFromMetadata.updateDeletionStatus(loadids, path)

    if (!invalidLoadIds.isEmpty) {
      if (invalidLoadIds.length == loadids.length) {
        LOGGER.audit("The delete load by Id is failed. Failed to delete the following load(s). LoadSeqId-" + invalidLoadIds);
        sys.error("Load deletion is failed. Failed to delete the following load(s). LoadSeqId-" + invalidLoadIds)
      }
      else {
        LOGGER.audit("The delete load by Id is failed. Failed to delete the following load(s). LoadSeqId-" + invalidLoadIds);
        sys.error("Load deletion is partial success. Failed to delete the following load(s). LoadSeqId-" + invalidLoadIds)
      }
    }

    LOGGER.audit("The delete load by Id is successfull.");
    Seq.empty

  }

}

private[sql] case class LoadCubeAPI(schemaName: String, cubeName: String, factPath: String, dimFilesPath: String, partionValues: Map[String, String]) extends RunnableCommand {

  def run(sqlContext: SQLContext) = {
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(
      Option(schemaName), cubeName, None)(sqlContext).asInstanceOf[CarbonRelation]
    if (relation == null) sys.error(s"Cube $schemaName.$cubeName does not exist")
    val carbonLoadModel = new CarbonLoadModel()
    carbonLoadModel.setCubeName(cubeName)
    carbonLoadModel.setSchemaName(schemaName)
    if (dimFilesPath == "") {
      carbonLoadModel.setDimFolderPath(null)
    }
    else {
      carbonLoadModel.setDimFolderPath(dimFilesPath)
    }
    carbonLoadModel.setFactFilePath(factPath)

    val table = relation.cubeMeta.schema.cubes(0).fact.asInstanceOf[CarbonDef.Table]
    carbonLoadModel.setAggTables(table.aggTables.map(t => t.asInstanceOf[CarbonDef.AggName].name))
    carbonLoadModel.setTableName(table.name)
    carbonLoadModel.setSchema(relation.cubeMeta.schema);
    var storeLocation = CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION_TEMP_PATH, System.getProperty("java.io.tmpdir"))
    storeLocation = storeLocation + "/carbonstore/" + System.currentTimeMillis()

    val columinar = sqlContext.getConf("carbon.is.columnar.storage", "true").toBoolean
    var kettleHomePath = sqlContext.getConf("carbon.kettle.home", null)
    if (kettleHomePath == null) sys.error(s"carbon.kettle.home is not set")

    val carbonLock = CarbonLockFactory.getCarbonLockObj(CarbonMetadata.getInstance().getCube(schemaName + "_" + cubeName).getMetaDataFilepath(),LockUsage.METADATA_LOCK)
    try {
      CarbonDataRDDFactory.loadCarbonData(sqlContext, carbonLoadModel, storeLocation, relation.cubeMeta.dataPath, kettleHomePath, relation.cubeMeta.partitioner, columinar, false);

    } finally {
      if (carbonLock != null) {
        if (carbonLock.unlock()) {
          logInfo("Cube MetaData Unlocked Successfully after data load")
        } else {
          logError("Unable to unlock Cube MetaData")
        }
      }
    }
    Seq.empty
  }
}

private[sql] case class LoadCube(
                                  schemaNameOp: Option[String],
                                  cubeName: String,
                                  factPathFromUser: String,
                                  dimFilesPath: Seq[DataLoadTableFileMapping],
                                  partionValues: Map[String, String]) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.cubemodel.cubeSchema")


  def run(sqlContext: SQLContext) = {

    val schemaName = getDB.getDatabaseName(schemaNameOp, sqlContext)
    if (null == CarbonMetadata.getInstance().getCube(schemaName + "_" + cubeName)) {
      logError("Data loading failed. cube not found: " + schemaName + "_" + cubeName)
      LOGGER.audit("Data loading failed. cube not found: " + schemaName + "_" + cubeName)
      sys.error("Data loading failed. cube not found: " + schemaName + "_" + cubeName)
    }
    val carbonLock = CarbonLockFactory.getCarbonLockObj(CarbonMetadata.getInstance().getCube(schemaName + "_" + cubeName).getMetaDataFilepath(),LockUsage.METADATA_LOCK)
    try {
      if (carbonLock.lockWithRetries()) {
        logInfo("Successfully able to get the cube metadata file lock")
      }
      else {
        sys.error("Cube is locked for updation. Please try after some time")
      }

      val factPath = CarbonUtil.checkAndAppendHDFSUrl(factPathFromUser)
      val relation =
        CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(Option(schemaName), cubeName, None)(sqlContext).asInstanceOf[CarbonRelation]
      if (relation == null) sys.error(s"Cube $schemaName.$cubeName does not exist")
      val carbonLoadModel = new CarbonLoadModel()
      carbonLoadModel.setCubeName(relation.cubeMeta.cubeName)
      carbonLoadModel.setSchemaName(relation.cubeMeta.schemaName)
      if (dimFilesPath.isEmpty) {
        carbonLoadModel.setDimFolderPath(null)
      }
      else {
        val x = dimFilesPath.map(f => f.table + ":" + CarbonUtil.checkAndAppendHDFSUrl(f.loadPath))
        carbonLoadModel.setDimFolderPath(x.mkString(","))
      }

      val table = relation.cubeMeta.schema.cubes(0).fact.asInstanceOf[CarbonDef.Table]
      carbonLoadModel.setAggTables(table.aggTables.map(t => t.asInstanceOf[CarbonDef.AggName].name))
      carbonLoadModel.setTableName(table.name)
      carbonLoadModel.setSchema(relation.cubeMeta.schema);
      var storeLocation = CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION_TEMP_PATH, System.getProperty("java.io.tmpdir"))
      

      var partitionLocation = relation.cubeMeta.dataPath + "/partition/" + relation.cubeMeta.schemaName + "/" + relation.cubeMeta.cubeName + "/"
      val fileType = FileFactory.getFileType(partitionLocation)
      if (FileFactory.isFileExist(partitionLocation, fileType)) {
        val file = FileFactory.getCarbonFile(partitionLocation, fileType)
        CarbonUtil.deleteFoldersAndFiles(file)
      }
      partitionLocation += System.currentTimeMillis()
      FileFactory.mkdirs(partitionLocation, fileType)

      storeLocation = storeLocation + "/carbonstore/" + System.nanoTime()

      val columinar = sqlContext.getConf("carbon.is.columnar.storage", "true").toBoolean
      var kettleHomePath = sqlContext.getConf("carbon.kettle.home", null)
      if (null == kettleHomePath) {
        kettleHomePath = CarbonProperties.getInstance.getProperty("carbon.kettle.home");
      }
      if (kettleHomePath == null) sys.error(s"carbon.kettle.home is not set")

      val delimiter = partionValues.getOrElse("delimiter", "")
      val quoteChar = partionValues.getOrElse("quotechar", "")
      val fileHeader = partionValues.getOrElse("fileheader", "")
      val escapeChar = partionValues.getOrElse("escapechar", "")
      val multiLine = partionValues.getOrElse("multiline", false)
      val complex_delimiter_level_1 = partionValues.getOrElse("complex_delimiter_level_1", "\\$")
      val complex_delimiter_level_2 = partionValues.getOrElse("complex_delimiter_level_2", "\\:")
      var booleanValForMultiLine = false
      if (multiLine.equals("true")) {
        booleanValForMultiLine = true
      }
      
      if(delimiter.equalsIgnoreCase(complex_delimiter_level_1) || 
          complex_delimiter_level_1.equalsIgnoreCase(complex_delimiter_level_2) ||
          delimiter.equalsIgnoreCase(complex_delimiter_level_2))
      {
    	  sys.error(s"Field Delimiter & Complex types delimiter are same")
      }
      else
      {
    	  carbonLoadModel.setComplexDelimiterLevel1(CarbonUtil.escapeComplexDelimiterChar(complex_delimiter_level_1))
    	  carbonLoadModel.setComplexDelimiterLevel2(CarbonUtil.escapeComplexDelimiterChar(complex_delimiter_level_2))
      }

      var partitionStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
      try {
        //First system has to partition the data first and then call the load data
        if (null == relation.cubeMeta.partitioner.partitionColumn || relation.cubeMeta.partitioner.partitionColumn(0).isEmpty) {
          LOGGER.info(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, "Initiating Direct Load for the Cube : (" +
            schemaName + "." + cubeName + ")")
          carbonLoadModel.setFactFilePath(factPath)
          carbonLoadModel.setCsvDelimiter(CarbonUtil.unescapeChar(delimiter))
          carbonLoadModel.setCsvHeader(fileHeader)
          carbonLoadModel.setDirectLoad(true)
        }
        else {
          LOGGER.info(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, "Initiating Data Partitioning for the Cube : (" +
            schemaName + "." + cubeName + ")")
          carbonLoadModel.setFactFilePath(partitionLocation)
          partitionStatus = CarbonContext.partitionData(
            schemaName,
            cubeName,
            factPath,
            partitionLocation,
            delimiter,
            quoteChar,
            fileHeader,
            escapeChar, booleanValForMultiLine)(sqlContext.asInstanceOf[HiveContext])
        }
        GlobalDictionaryUtil.generateGlobalDictionary(sqlContext, carbonLoadModel, false)
        CarbonDataRDDFactory.loadCarbonData(sqlContext, carbonLoadModel, storeLocation, relation.cubeMeta.dataPath, kettleHomePath,
          relation.cubeMeta.partitioner, columinar, false, partitionStatus);
        try {
          val loadMetadataFilePath = CarbonLoaderUtil
            .extractLoadMetadataFileLocation(carbonLoadModel)

          val details = CarbonUtil
            .readLoadMetadata(loadMetadataFilePath)
          if (null != details) {
            var listOfLoadFolders = CarbonLoaderUtil.getListOfValidSlices(details)
            // CarbonProperties.getInstance.getProperty("carbon.kettle.home","false")
            if (null != listOfLoadFolders && listOfLoadFolders.size() > 0 && CarbonProperties.getInstance.getProperty(CarbonCommonConstants.LOADCUBE_DATALOAD, "false") == "true") {
              var hc: HiveContext = sqlContext.asInstanceOf[HiveContext]
              hc.sql(" select count(*) from " + schemaName + "." + cubeName).collect()

            }
          }
        }
        catch {
          case ex: Exception =>
            LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, "Btree loading is failed after data loading process.")


        }
      }
      catch {
        case ex: Exception =>
          LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, ex)
          LOGGER.audit("Dataload failure. Please check the logs")
          sys.error("Dataload failure. Please check the logs")
      }
      finally {
        //Once the data load is successfule delete the unwanted partition files
        try {
          val file = FileFactory.getCarbonFile(partitionLocation, FileFactory.getFileType(partitionLocation))
          CarbonUtil.deleteFoldersAndFiles(file)
        } catch {
          case ex: Exception =>
            LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, ex)
            LOGGER.audit("Dataload failure. Problem deleting the partition folder")
            sys.error("Problem deleting the partition folder")
        }

        // CarbonUtil.deleteFoldersAndFiles(carbonFiles);
      }
    } finally {
      if (carbonLock != null) {
        if (carbonLock.unlock()) {
          logInfo("Cube MetaData Unlocked Successfully after data load")
        } else {
          logError("Unable to unlock Cube MetaData")
        }
      }
    }
    Seq.empty
  }

}

private[sql] case class AddAggregatesToCube(
                                             schemaNameOp: Option[String],
                                             cubeName: String,
                                             aggregateAttributes: Seq[AggregateTableAttributes])
  extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.cubemodel.cubeSchema")


  def run(sqlContext: SQLContext) = {
    val schemaName = getDB.getDatabaseName(schemaNameOp, sqlContext)
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(Option(schemaName), cubeName, None)(sqlContext).asInstanceOf[CarbonRelation]
    if (relation == null) sys.error(s"Cube $schemaName.$cubeName does not exist")
    if (aggregateAttributes.size == 0) sys.error(s"No columns found in the query. Please provide the valid column names to create an aggregate table successfully")
    val carbonLock =  CarbonLockFactory.getCarbonLockObj(CarbonMetadata.getInstance().getCube(schemaName + "_" + cubeName).getMetaDataFilepath(),LockUsage.METADATA_LOCK)
    try {
      if (carbonLock.lockWithRetries()) {
        logInfo("Successfully able to get the cube metadata file lock")
      } else {
        LOGGER.audit(s"The aggregate table creation request failed as cube is locked for updation")
        sys.error("Cube is locked for updation. Please try after some time")
      }
      val aggTableName = CarbonEnv.getInstance(sqlContext).carbonCatalog.getAggregateTableName(relation.cubeMeta.schema, relation.cubeMeta.cube.getFactTableName)
      LOGGER.audit(s"The aggregate table creation request has been received :: $aggTableName")
      var cube = CarbonMetadata.getInstance().getCube(schemaName + "_" + cubeName)
      val path = cube.getMetaDataFilepath()

      val fileType = FileFactory.getFileType(path)
      val file = FileFactory.getCarbonFile(path, fileType)
      val (schemaName1, cubeNameInSchema, dataPath1, schema1, partitioner1,cubeCreationTime) = CarbonEnv.getInstance(sqlContext).carbonCatalog.readCubeMetaDataFile(file, fileType)

      val mondSchema = CarbonMetastoreCatalog.parseStringToSchema(schema1)

      val schema = CarbonEnv.getInstance(sqlContext).carbonCatalog.updateCubeWithAggregates(mondSchema, schemaName, cubeNameInSchema, aggTableName, aggregateAttributes.toList)
      LoadAggregationTable(schema, schemaName, cubeNameInSchema, aggTableName).run(sqlContext)
      CarbonEnv.getInstance(sqlContext).carbonCatalog.updateCubeWithAggregatesDetail(relation.cubeMeta.schema, mondSchema)
      LOGGER.audit(s"The aggregate table creation request is successful :: $aggTableName")
    } finally {
      if (carbonLock != null) {
        if (carbonLock.unlock()) {
          logInfo("Cube MetaData Unlocked Successfully after data load")
        } else {
          logError("Unable to unlock Cube MetaData")
        }
      }
    }
    Seq.empty
  }
}

private[sql] case class PartitionData(schemaName: String, cubeName: String, factPath: String, targetPath: String, delimiter: String, quoteChar: String, fileHeader: String, escapeChar: String, multiLine: Boolean) extends RunnableCommand {

  var partitionStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS

  def run(sqlContext: SQLContext) = {
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(Option(schemaName), cubeName, None)(sqlContext).asInstanceOf[CarbonRelation]
    val targetFolder = targetPath //relation.cubeMeta.dataPath.subSequence(0, relation.cubeMeta.dataPath.indexOf("/store"))+"/"+cubeName+System.currentTimeMillis()
    partitionStatus = CarbonDataRDDFactory.partitionCarbonData(sqlContext.sparkContext, schemaName, cubeName, factPath, targetFolder, CarbonQueryUtil.getAllColumns(relation.cubeMeta.schema), fileHeader, delimiter, quoteChar, escapeChar, multiLine, relation.cubeMeta.partitioner)
    if (partitionStatus == CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS) {
      logInfo("Bad Record Found while partitioning data")
    }
    Seq.empty
  }
}

private[sql] case class LoadAggregationTable(
                                              newSchema: CarbonDef.Schema,
                                              schemaName: String,
                                              cubeName: String,
                                              aggTableName: String) extends RunnableCommand {

  def run(sqlContext: SQLContext) = {
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(
      Option(schemaName),
      cubeName,
      None)(sqlContext).asInstanceOf[CarbonRelation]
    if (relation == null) sys.error(s"Cube $schemaName.$cubeName does not exist")
    val carbonLoadModel = new CarbonLoadModel()
    carbonLoadModel.setCubeName(cubeName)
    carbonLoadModel.setSchemaName(schemaName)
    val table = relation.cubeMeta.schema.cubes(0).fact.asInstanceOf[CarbonDef.Table]
    carbonLoadModel.setAggTableName(aggTableName)
    carbonLoadModel.setTableName(table.name)
    carbonLoadModel.setSchema(newSchema);
    carbonLoadModel.setAggLoadRequest(true)
    var storeLocation = CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION_TEMP_PATH, System.getProperty("java.io.tmpdir"))
    storeLocation = storeLocation + "/carbonstore/" + System.currentTimeMillis()
    val columinar = sqlContext.getConf("carbon.is.columnar.storage", "true").toBoolean
    var kettleHomePath = sqlContext.getConf("carbon.kettle.home", null)
    if (null == kettleHomePath) {
      kettleHomePath = CarbonProperties.getInstance.getProperty("carbon.kettle.home");
    }
    if (kettleHomePath == null) sys.error(s"carbon.kettle.home is not set")
    CarbonDataRDDFactory.loadCarbonData(
      sqlContext,
      carbonLoadModel,
      storeLocation,
      relation.cubeMeta.dataPath,
      kettleHomePath,
      relation.cubeMeta.partitioner, columinar, true);
    Seq.empty
  }
}


//private[sql] case class UseDB(schemaName: String) extends RunnableCommand {
//
//  def run(sqlContext: SQLContext) = {
////    println(schemaName)
////    CarbonEnv.carbonCatalog.currentSchema = schemaName
////    Seq.empty
//  }
//}

/*private[sql] case class ShowSchemas(schemaLike: Option[String], output: Seq[Attribute])(
    @transient context: CarbonContext) extends LeafNode with Command {
 override protected lazy val sideEffectResult: Seq[Row] = context.catalog2.showSchemas(schemaLike).map(Row(_))

  override def otherCopyArgs = context :: Nil
}*/

//private[sql] case class ShowSchemas(
//    schemaLike: Option[String], 
//    override val output: Seq[Attribute]
//    )(@transient context: CarbonContext) extends RunnableCommand {
//
//  override def run(sqlContext: SQLContext): Seq[Row] ={ 
//    import sqlContext.implicits._
//    val res = (context.catalog.s.getAllDatabases(schemaLike) ++
//      CarbonEnv.carbonCatalog.showSchemas(schemaLike)).toSet[String].toSeq//.map(Row(_))
//    var rd = context.sc.parallelize(res).toDF("col1")
//    if(schemaLike.isDefined)
//      rd = rd.filter("col1 like \""+schemaLike.get+"\"")
//    rd.collect.toSeq
//  }
//}

private[sql] case class ShowAllCubesInSchema(
                                              schemaNameOp: Option[String],
                                              override val output: Seq[Attribute]
                                            ) extends RunnableCommand  {
//
//  override def getControlPrivileges(sqlContext: SQLContext): Set[PrivObject] = {
//    //SELECT privilege @ database level // TODO: handle use database case
//    Set(new PrivObject(
//      ObjectType.DATABASE,
//      getDB.getDatabaseName(schemaNameOp, sqlContext),
//      null,
//      null,
//      Set(PrivType.SELECT_NOGRANT)))
//  }

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val schemaName = getDB.getDatabaseName(schemaNameOp, sqlContext)
    CarbonEnv.getInstance(sqlContext).carbonCatalog.getCubes(Some(schemaName))(sqlContext).map(x => Row(x._1, sqlContext.asInstanceOf[HiveContext].catalog.tableExists(Seq(schemaName, x._1))))
  }
}

private[sql] case class ShowAllCubes( override val output: Seq[Attribute]) 
                  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    CarbonEnv.getInstance(sqlContext).carbonCatalog.getAllCubes()(sqlContext)
//      .filter(x =>
//        if (allowedDatabases.isDefined) allowedDatabases.get.contains(x._1)
//        else true
//      )
      .map(x => Row(x._1, x._2, sqlContext.asInstanceOf[HiveContext].catalog.tableExists(Seq(x._1, x._2)))).toSeq
  }
}

private[sql] case class SuggestAggregates(
                                           script: Option[String],
                                           sugType: Option[String],
                                           schemaNameOp: Option[String],
                                           cubeName: String,
                                           override val output: Seq[Attribute]) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.cubemodel.cubeSchema");
    val schemaName = getDB.getDatabaseName(schemaNameOp, sqlContext)
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(Some(schemaName), cubeName, None)(sqlContext).asInstanceOf[CarbonRelation]
    try {
      val schema: CarbonDef.Schema = relation.cubeMeta.schema
      val cubes: Array[CarbonDef.Cube] = relation.cubeMeta.schema.cubes.filter { x => cubeName.equalsIgnoreCase(x.name) }
      val loadModel: LoadModel = new LoadModel
      loadModel.setSchemaName(relation.cubeMeta.schemaName)
      loadModel.setCubeName(relation.cubeMeta.cubeName)
      val table = cubes(0).fact.asInstanceOf[CarbonDef.Table]
      loadModel.setTableName(table.name)
      loadModel.setDataPath(relation.cubeMeta.dataPath)
      loadModel.setMetaDataPath(relation.metaData.cube.getMetaDataFilepath)
      // check if there is any valid loads.if there is no valid load means, there is no data loadeded and hence return empty suggestion
      CommonUtil.setListOfValidSlices(relation.metaData.cube.getMetaDataFilepath, loadModel)
      CommonUtil.fillSchemaAndCubeDetail(loadModel)
      if (loadModel.getValidSlices.size() == 0) {
        return Seq(Row("Data Not loaded", "No Suggestion"))
      }
      val cubeCreationTime = CarbonEnv.getInstance(sqlContext).carbonCatalog.getCubeCreationTime(schemaName, cubeName)
      val schemaLastUpdatedTime = CarbonEnv.getInstance(sqlContext).carbonCatalog.getSchemaLastUpdatedTime(schemaName, cubeName)
      loadModel.setCubeCreationtime(cubeCreationTime)
      loadModel.setSchemaLastUpdatedTime(schemaLastUpdatedTime)
      val aggSuggestion = getAggSuggestion(script, loadModel)
      aggSuggestion
    }
    catch {
      case e: Exception =>
        LOGGER.audit(s"Error while querying for aggregate table suggestion for [$schemaName] and cube name [$cubeName] failed :" + e)
        if(null != e.getMessage)
        {
          sys.error(s"Aggregate table suggestion request for [$schemaName] and cube [$cubeName] is failed."+e.getMessage)
        }
        else
        {
          sys.error(s"Aggregate table suggestion request for [$schemaName] and cube [$cubeName] is failed.")
        }
    }

  }

  def getAggSuggestion(script: Option[String], loadModel: LoadModel): Seq[Row] = {
    if (!script.isEmpty) {
      if (!sugType.isEmpty) {
        val aggSuggReqType: Request = Request.getRequest(sugType.get)
        val aggService: AutoAggSuggestionService = AutoAggSuggestionFactory.getAggregateService(aggSuggReqType)
        val aggCombs = aggService.getAggregateScripts(loadModel)
        aggCombs.map { x => Row(aggSuggReqType.getAggSuggestionType, x) }

      }
      else {
        val dataAggService: AutoAggSuggestionService = AutoAggSuggestionFactory.getAggregateService(Request.DATA_STATS)
        val queryAggService: AutoAggSuggestionService = AutoAggSuggestionFactory.getAggregateService(Request.QUERY_STATS)
        val dataAggCombs = dataAggService.getAggregateScripts(loadModel)
        val queryAggCombs = queryAggService.getAggregateScripts(loadModel)
        val agg_suggestion = new scala.collection.mutable.ListBuffer[Row]()
        dataAggCombs.map { x => agg_suggestion.add(Row(Request.DATA_STATS.getAggSuggestionType, x)) }
        queryAggCombs.map { x => agg_suggestion.add(Row(Request.QUERY_STATS.getAggSuggestionType, x)) }
        agg_suggestion.toSeq
      }
    }
    else {
      if (!sugType.isEmpty) {
        val aggSuggType: Request = Request.getRequest(sugType.get)
        val aggService: AutoAggSuggestionService = AutoAggSuggestionFactory.getAggregateService(aggSuggType)
        val aggCombs = aggService.getAggregateDimensions(loadModel)
        aggCombs.map { x => Row(aggSuggType.getAggSuggestionType, x) }

      }
      else {
        val dataAggService: AutoAggSuggestionService = AutoAggSuggestionFactory.getAggregateService(Request.DATA_STATS)
        val queryAggService: AutoAggSuggestionService = AutoAggSuggestionFactory.getAggregateService(Request.QUERY_STATS)
        val dataAggCombs = dataAggService.getAggregateDimensions(loadModel)
        val queryAggCombs = queryAggService.getAggregateDimensions(loadModel)
        val agg_suggestion = new scala.collection.mutable.ListBuffer[Row]()
        dataAggCombs.map { x => agg_suggestion.add(Row(Request.DATA_STATS.getAggSuggestionType, x)) }
        queryAggCombs.map { x => agg_suggestion.add(Row(Request.QUERY_STATS.getAggSuggestionType, x)) }
        agg_suggestion.toSeq

      }
    }
  }
}

private[sql] case class ShowAllTablesDetail(
                                             schemaNameOp: Option[String],
                                             override val output: Seq[Attribute]
                                           ) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val dSchemaName = getDB.getDatabaseName(schemaNameOp, sqlContext)
    sqlContext.catalog.getTables(Some(dSchemaName)).map(x => Row(null, dSchemaName, x._1, "TABLE", ""))
  }
}

private[sql] case class MergeCube(schemaName: String, cubeName: String, tableName: String) extends RunnableCommand {

  def run(sqlContext: SQLContext) = {
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation2(Seq(schemaName, cubeName), None)(sqlContext).asInstanceOf[CarbonRelation]
    if (relation == null) sys.error(s"Cube $schemaName.$cubeName does not exist")
    val carbonLoadModel = new CarbonLoadModel()
    carbonLoadModel.setCubeName(cubeName)
    carbonLoadModel.setSchemaName(schemaName)
    val table = relation.cubeMeta.schema.cubes(0).fact.asInstanceOf[CarbonDef.Table]
    var isTablePresent = false;
    if (table.name.equals(tableName)) isTablePresent = true
    if (!isTablePresent) {
      val aggTables = table.aggTables.map(t => t.asInstanceOf[CarbonDef.AggName].name)
      var aggTable = null
      for (aggTable <- aggTables if (aggTable.equals(tableName)))
        isTablePresent = true
    }
    if (!isTablePresent) sys.error("Invalid table name!")
    carbonLoadModel.setTableName(tableName)
    carbonLoadModel.setSchema(relation.cubeMeta.schema);
    var storeLocation = CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION_TEMP_PATH, System.getProperty("java.io.tmpdir"))
    storeLocation = storeLocation + "/carbonstore/" + System.currentTimeMillis()
    CarbonDataRDDFactory.mergeCarbonData(sqlContext, carbonLoadModel, storeLocation, relation.cubeMeta.dataPath, relation.cubeMeta.partitioner)
    Seq.empty
  }
}

private[sql] case class DropCubeCommand(ifExistsSet: Boolean, schemaNameOp: Option[String], cubeName: String)
  extends RunnableCommand {

  def run(sqlContext: SQLContext) = {
    val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.cubemodel.cubeSchema");
    val schemaName = getDB.getDatabaseName(schemaNameOp, sqlContext)

    val tmpCube = CarbonMetadata.getInstance().getCube(schemaName + "_" + cubeName)
    if (null == tmpCube) {
      if (!ifExistsSet) {
        LOGGER.audit(s"Dropping cube with Schema name [$schemaName] and cube name [$cubeName] failed")
        LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Cube $schemaName.$cubeName does not exist")
        sys.error(s"Cube $schemaName.$cubeName does not exist")
      }
    }
    else {
      val carbonLock =  CarbonLockFactory.getCarbonLockObj(tmpCube.getMetaDataFilepath(),LockUsage.METADATA_LOCK)
      try {
        if (carbonLock.lockWithRetries()) {
          logInfo("Successfully able to get the cube metadata file lock")
        }
        else {
          LOGGER.audit(s"Dropping cube with Schema name [$schemaName] and cube name [$cubeName] failed as the Cube is locked")
          sys.error("Cube is locked for updation. Please try after some time")
        }

        val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation2(Seq(schemaName, cubeName))(sqlContext).asInstanceOf[CarbonRelation]

        if (relation == null) {
          if (!ifExistsSet) sys.error(s"Cube $schemaName.$cubeName does not exist")
        } else {
          LOGGER.audit(s"Deleting cube [$cubeName] under schema [$schemaName]")

          CarbonEnv.getInstance(sqlContext).carbonCatalog.dropCube(
            relation.cubeMeta.partitioner.partitionCount,
            relation.cubeMeta.dataPath,
            relation.cubeMeta.schemaName,
            relation.cubeMeta.cubeName)(sqlContext)
          CarbonDataRDDFactory.dropCube(sqlContext.sparkContext, schemaName, cubeName, relation.cubeMeta.partitioner)
          QueryPartitionHelper.getInstance().removePartition(schemaName, cubeName);

          LOGGER.audit(s"Deleted cube [$cubeName] under schema [$schemaName]")
        }
      }
      finally {
        if (carbonLock != null) {
          if (carbonLock.unlock()) {
            logInfo("Cube MetaData Unlocked Successfully after dropping the cube")
            CarbonUtil.deleteFoldersAndFiles(tmpCube.getMetaDataFilepath())
          } else {
            logError("Unable to unlock Cube MetaData")
          }
        }
      }
    }

    Seq.empty
  }
}

private[sql] case class DropAggregateTableCommand(ifExistsSet: Boolean,
                                                  schemaNameOp: Option[String],
                                                  tableName: String) extends RunnableCommand  {

  def run(sqlContext: SQLContext) = {
    val schemaName = getDB.getDatabaseName(schemaNameOp, sqlContext)
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.
      lookupRelation1(Some(schemaName), tableName, None)(sqlContext).asInstanceOf[CarbonRelation]

    if (relation == null) {
      if (!ifExistsSet) sys.error(s"Aggregate Table $schemaName.$tableName does not exist")
    }
    else {
      CarbonDataRDDFactory.dropAggregateTable(
        sqlContext.sparkContext,
        schemaName,
        tableName,
        relation.cubeMeta.partitioner)
    }

    Seq.empty
  }
}

private[sql] case class ShowLoads(
                                   schemaNameOp: Option[String],
                                   cubeName: String,
                                   limit: Option[String],
                                   override val output: Seq[Attribute]) extends RunnableCommand {


  override def run(sqlContext: SQLContext): Seq[Row] = {
    val schemaName = getDB.getDatabaseName(schemaNameOp, sqlContext)
    var cube = CarbonMetadata.getInstance().getCube(schemaName + '_' + cubeName)

    if (null == cube) {
      val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(Option(schemaName), cubeName, None)(sqlContext)
        .asInstanceOf[CarbonRelation]

      if (relation == null) {
        sys.error(s"Cube $schemaName.$cubeName does not exist")
      }

      val schema = relation.cubeMeta.schema
      val mondrianCube = CarbonSchemaParser.getMondrianCube(schema, cubeName);
      CarbonMetadata.getInstance().loadCube(schema,schema.name, mondrianCube.name, mondrianCube);
      cube = CarbonMetadata.getInstance().getCube(schemaName + '_' + cubeName);
    }
    val path = cube.getMetaDataFilepath()


    val loadMetadataDetailsArray = CarbonUtil.readLoadMetadata(path)

    if (loadMetadataDetailsArray.length != 0) {

      val parser = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP)

      var loadMetadataDetailsSortedArray = loadMetadataDetailsArray.sortWith((l1, l2) => Integer.parseInt(l1.getLoadName()) > Integer.parseInt(l2.getLoadName()))


      if (!limit.isEmpty) {
        loadMetadataDetailsSortedArray = loadMetadataDetailsSortedArray.filter(load => load.getVisibility().equalsIgnoreCase("true"))
        val limitLoads = limit.get
        try {
          val lim = Integer.parseInt(limitLoads)
          loadMetadataDetailsSortedArray = loadMetadataDetailsSortedArray.slice(0, lim)
        }
        catch {
          case ex: NumberFormatException => sys.error(s" Entered limit is not a valid Number")
        }

      }

      loadMetadataDetailsSortedArray.filter(load => load.getVisibility().equalsIgnoreCase("true")).map(load =>
        Row(
          load.getLoadName(),
          load.getLoadStatus(),
          new java.sql.Timestamp(parser.parse(load.getLoadStartTime()).getTime()),
          new java.sql.Timestamp(parser.parse(load.getTimestamp()).getTime()))).toSeq
    } else {
      Seq.empty

    }
  }

}

private[sql] case class ShowAggregateTables(
                                             schemaNameOp: Option[String],
                                             override val output: Seq[Attribute]) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] =
    Seq(Row("AggTable1"), Row("AggTable2"))
}

private[sql] case class DescribeCommandFormatted(
                                                  child: SparkPlan,
                                                  override val output: Seq[Attribute],
                                                  tblIdentifier: Seq[String])
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation2(tblIdentifier, None)(sqlContext).asInstanceOf[CarbonRelation]
    var results: Seq[(String, String, String)] = child.schema.fields.map { field =>
      val cmtKey = "comment"
      val comment = if (relation.metaData.dims.contains(field.name)) "DIMENSION" else "MEASURE"
      (field.name, field.dataType.simpleString, comment)
    }
    results ++= Seq(("", "", ""), ("##Detailed Table Information", "", ""))
    results ++= Seq(("Schema Name : ", relation.cubeMeta.schemaName, ""))
    results ++= Seq(("Cube Name : ", relation.cubeMeta.cubeName, ""))
    results ++= Seq(("CARBON Store Path : ", relation.cubeMeta.dataPath, ""))
    val partitioner = relation.cubeMeta.partitioner
    results ++= Seq(("#Partition Details : ", "", ""))
    results ++= Seq(("", "Partitioner Class :", relation.cubeMeta.partitioner.partitionClass))
    results ++= Seq(("", "Partitioner Column :", relation.cubeMeta.partitioner.partitionColumn.mkString(", ")))
    results ++= Seq(("", "Partition Count :", relation.cubeMeta.partitioner.partitionCount.toString))
    results ++= Seq(("", "", ""), ("#Aggregate Tables", "", ""))
    val factTable = relation.cubeMeta.schema.cubes(0).fact.asInstanceOf[CarbonDef.Table]
    val aggTables = factTable.aggTables
    if (aggTables.size == 0) {
      results ++= Seq(("NONE", "", ""))
    } else {
      val visibleColumns = child.schema.fields.map(field => field.name)
      aggTables.map(table => {
        results ++= Seq(("", "", ""), ("Agg Table :" + table.asInstanceOf[CarbonDef.AggName].name, "#Columns", "#AggregateType"))
        table.getAggLevels().map(level => if (visibleColumns.contains(level.column)) {
          results ++= Seq(("", level.column, ""))
        })
        table.getAggMeasures().map(measure => if (visibleColumns.contains(measure.column)) {
          results ++= Seq(("", measure.column, measure.aggregator))
        })
      }
      )
    }

    val dimTableMap = new scala.collection.mutable.HashMap[String, scala.collection.mutable.Set[(String, String, String)]]
    relation.cubeMeta.schema.cubes(0).dimensions.filter { visibleDim => visibleDim.visible == null || visibleDim.visible }.map(dimension => {
      val dim = dimension.asInstanceOf[CarbonDef.Dimension]
      val level = dim.hierarchies(0).levels(0)
      if (dim.foreignKey != null) {
        val dimensionTableName = dim.hierarchies(0).relation.asInstanceOf[CarbonDef.Table].name
        var dimMap = dimTableMap.get(dimensionTableName)
        if (dimMap.isDefined) {
          var seqDim: scala.collection.mutable.Set[(String, String, String)] = dimMap.get
          seqDim ++= scala.collection.mutable.Set((level.column, level.name, factTable.name + "." + dim.foreignKey + " = " + dimensionTableName + "." + dim.hierarchies(0).primaryKey))
        }
        else {
          dimTableMap.put(dimensionTableName, scala.collection.mutable.Set((level.column, level.name, factTable.name + "." + dim.foreignKey + " = " + dimensionTableName + "." + dim.hierarchies(0).primaryKey)))
        }
      }
      else {
        var dimMap = dimTableMap.get(factTable.name)
        if (dimMap.isDefined) {
          var seqDim: scala.collection.mutable.Set[(String, String, String)] = dimMap.get
          seqDim ++= scala.collection.mutable.Set((level.column, level.name, ""))
        }
        else {
          dimTableMap.put(factTable.name, scala.collection.mutable.Set((level.column, level.name, "")))
        }
      }
    })


    dimTableMap.foreach {
      case (key, value) => {
        if (key != factTable.name)
          results ++= Seq(("", "", ""), ("##Dimensions from table : " + key, "", ""), ("#Table columns  :", "#Imported to cube as :", "#Relation")) ++ value.toSeq
      }
    }

    val factTableMap = dimTableMap.get(factTable.name)
    if (factTableMap.isDefined) {
      results ++= Seq(("", "", ""), ("##Fact Table Name : " + factTable.name, "", ""), ("#Table columns  :", "#Imported to cube as :", "")) ++ factTableMap.get.toSeq
    }

    //results ++= Seq(("#Measure Column Name", "#Fact Table Column","#Aggregator Type"))
    relation.cubeMeta.schema.cubes(0).measures.filter { visibleMsr => visibleMsr.visible == null || visibleMsr.visible }.map(msr => {
      results ++= Seq((msr.name, msr.column, ""))
    }
    )

    results.map { case (name, dataType, comment) =>
      Row(name, dataType, comment)
    }
  }
}

private[sql] case class DescribeNativeCommand(sql: String,
                                              override val output: Seq[Attribute]) extends RunnableCommand {
  override def run(sqlContext: SQLContext): Seq[Row] = {
    val output = sqlContext.asInstanceOf[HiveContext].catalog.client.runSqlHive(sql).toSeq
    output.map(x => {
      val row = x.split("\t", -3)
      Row(row(0), row(1), row(2))
    }
    ).tail
  }
}

private[sql] case class DeleteLoadByDate(
                                          schemaNameOp: Option[String],
                                          cubeName: String,
                                          dateField: String,
                                          dateValue: String
                                        ) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.cubemodel.cubeSchema");

  def run(sqlContext: SQLContext) = {

    LOGGER.audit("The delete load by date request has been received.");
    val schemaName = getDB.getDatabaseName(schemaNameOp, sqlContext)
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(Some(schemaName), cubeName, None)(sqlContext).asInstanceOf[CarbonRelation]
    var level: String = "";

    if (relation == null) {
      LOGGER.audit(s"The delete load by date is failed. Cube $schemaName.$cubeName does not exist");
      sys.error(s"Cube $schemaName.$cubeName does not exist")
    }

    val matches: Seq[AttributeReference] = relation.dimensionsAttr.filter(filter => ((filter.name.equalsIgnoreCase(dateField)) && filter.dataType.isInstanceOf[TimestampType])).toList

    if (matches.isEmpty) {
      LOGGER.audit("The delete load by date is failed. Cube $schemaName.$cubeName does not contain date field " + dateField);
      sys.error(s"Cube $schemaName.$cubeName does not contain date field " + dateField)
    }
    else {
      level = matches.get(0).name;
    }
    var tableName = relation.metaData.cube.getDimension(level).getTableName

    val actualColName = relation.metaData.cube.getDimension(level).getColName
    CarbonDataRDDFactory.deleteLoadByDate(
      sqlContext,
      relation.cubeMeta.schema,
      schemaName,
      cubeName,
      tableName,
      CarbonEnv.getInstance(sqlContext).carbonCatalog.metadataPath,
      level,
      actualColName,
      dateValue,
      relation.cubeMeta.partitioner)
    LOGGER.audit("The delete load by date is successfull.");
    Seq.empty
  }
}


private[sql] case class CleanFiles(
                                    schemaNameOp: Option[String],
                                    cubeName: String) extends RunnableCommand  {

  val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.cubemodel.cubeSchema");

  def run(sqlContext: SQLContext) = {
    LOGGER.audit("The clean files request has been received.");
    val schemaName = getDB.getDatabaseName(schemaNameOp, sqlContext)
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(Some(schemaName), cubeName, None)(sqlContext).
      asInstanceOf[CarbonRelation]
    if (relation == null) {
      LOGGER.audit(s"The clean files request is failed. Cube $schemaName.$cubeName does not exist");
      sys.error(s"Cube $schemaName.$cubeName does not exist")
    }

    val carbonLoadModel = new CarbonLoadModel()
    carbonLoadModel.setCubeName(relation.cubeMeta.cubeName)
    carbonLoadModel.setSchemaName(relation.cubeMeta.schemaName)
    val table = relation.cubeMeta.schema.cubes(0).fact.asInstanceOf[CarbonDef.Table]
    carbonLoadModel.setAggTables(table.aggTables.map(t => t.asInstanceOf[CarbonDef.AggName].name))
    carbonLoadModel.setTableName(table.name)

    CarbonDataRDDFactory.cleanFiles(
      sqlContext.sparkContext,
      carbonLoadModel,
      relation.cubeMeta.dataPath,
      relation.cubeMeta.partitioner)
    LOGGER.audit("The clean files request is successfull.");
    Seq.empty
  }
}


