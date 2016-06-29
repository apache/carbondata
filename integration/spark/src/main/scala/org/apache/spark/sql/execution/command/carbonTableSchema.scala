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

package org.apache.spark.sql.execution.command

import java.io.File
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, UUID}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.Random

import org.apache.spark.SparkEnv
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast, Literal}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLTimestamp
import org.apache.spark.sql.execution.{RunnableCommand, SparkPlan}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.util.FileUtils
import org.codehaus.jackson.map.ObjectMapper

import org.carbondata.common.factory.CarbonCommonFactory
import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.carbon.CarbonDataLoadSchema
import org.carbondata.core.carbon.metadata.CarbonMetadata
import org.carbondata.core.carbon.metadata.datatype.DataType
import org.carbondata.core.carbon.metadata.encoder.Encoding
import org.carbondata.core.carbon.metadata.schema.{SchemaEvolution, SchemaEvolutionEntry}
import org.carbondata.core.carbon.metadata.schema.table.{CarbonTable, TableInfo, TableSchema}
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension
import org.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.datastorage.store.impl.FileFactory
import org.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.carbondata.integration.spark.merger.CompactionType
import org.carbondata.lcm.status.SegmentStatusManager
import org.carbondata.spark.exception.MalformedCarbonCommandException
import org.carbondata.spark.load._
import org.carbondata.spark.partition.api.impl.QueryPartitionHelper
import org.carbondata.spark.rdd.CarbonDataRDDFactory
import org.carbondata.spark.util.{CarbonScalaUtil, CommonUtil, GlobalDictionaryUtil}
import org.carbondata.spark.CarbonSparkFactory


case class tableModel(
    ifNotExistsSet: Boolean,
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
    highcardinalitydims: Option[Seq[String]],
    aggregation: Seq[Aggregation],
    partitioner: Option[Partitioner],
    columnGroups: Seq[String],
    colProps: Option[util.Map[String, util.List[ColumnProperty]]] = None)

case class Field(column: String, dataType: Option[String], name: Option[String],
    children: Option[List[Field]], parent: String = null,
    storeType: Option[String] = Some("columnar"),
    var precision: Int = 0, var scale: Int = 0)

case class ArrayDataType(dataType: String)

case class StructDataType(dataTypes: List[String])

case class StructField(column: String, dataType: String)

case class FieldMapping(levelName: String, columnName: String)

case class HierarchyMapping(hierName: String, hierType: String, levels: Seq[String])

case class ColumnProperty(key: String, value: String)

case class ComplexField(complexType: String, primitiveField: Option[Field],
    complexField: Option[ComplexField])

case class Cardinality(levelName: String, cardinality: Int)

case class Aggregation(msrName: String, aggType: String)

case class AggregateTableAttributes(colName: String, aggType: String = null)

case class Partitioner(partitionClass: String, partitionColumn: Array[String], partitionCount: Int,
    nodeList: Array[String])

case class PartitionerField(partitionColumn: String, dataType: Option[String],
    columnComment: String)

case class DimensionRelation(tableName: String, dimSource: Object, relation: Relation,
    includeKey: Option[String], cols: Option[Seq[String]])

case class Relation(leftColumn: String, rightColumn: String)

case class LoadSchema(tableInfo: TableInfo, dimensionTables: Array[DimensionRelation])

case class Level(name: String, column: String, cardinality: Int, dataType: String,
    parent: String = null, storeType: String = "Columnar",
    levelType: String = "Regular")

case class Measure(name: String, column: String, dataType: String, aggregator: String = "SUM",
    visible: Boolean = true)

case class Hierarchy(name: String, primaryKey: Option[String], levels: Seq[Level],
    tableName: Option[String], normalized: Boolean = false)

case class Dimension(name: String, hierarchies: Seq[Hierarchy], foreignKey: Option[String],
    dimType: String = "StandardDimension", visible: Boolean = true,
    var highCardinality: Boolean = false)

case class FilterCols(includeKey: String, fieldList: Seq[String])

case class Cube(schemaName: String, cubeName: String, tableName: String, dimensions: Seq[Dimension],
    measures: Seq[Measure], partitioner: Partitioner)

case class Default(key: String, value: String)

case class DataLoadTableFileMapping(table: String, loadPath: String)

case class CarbonMergerMapping(storeLocation: String, hdfsStoreLocation: String,
  partitioner: Partitioner, metadataFilePath: String, mergedLoadName: String,
  kettleHomePath: String, cubeCreationTime: Long, schemaName: String,
  factTableName: String, validSegments: Array[String], tableId: String)

case class AlterTableModel(dbName: Option[String], tableName: String, compactionType: String)

case class CompactionModel(compactionSize: Long,
  compactionType: CompactionType,
  carbonTable: CarbonTable,
  cubeCreationTime: Long)

object TableNewProcessor {
  def apply(cm: tableModel, sqlContext: SQLContext): TableInfo = {
    new TableNewProcessor(cm, sqlContext).process
  }
}

class TableNewProcessor(cm: tableModel, sqlContext: SQLContext) {

  var index = 0
  var rowGroup = 0
  def getAllChildren(fieldChildren: Option[List[Field]]): Seq[ColumnSchema] = {
    var allColumns: Seq[ColumnSchema] = Seq[ColumnSchema]()
    fieldChildren.foreach(fields => {
      fields.foreach(field => {
        val encoders = new java.util.ArrayList[Encoding]()
        encoders.add(Encoding.DICTIONARY)
        val columnSchema: ColumnSchema = getColumnSchema(
          normalizeType(field.dataType.getOrElse("")), field.name.getOrElse(field.column), index,
          isCol = true, encoders, isDimensionCol = true, rowGroup, field.precision, field.scale)
        allColumns ++= Seq(columnSchema)
        index = index + 1
        rowGroup = rowGroup + 1
        if (field.children.get != null) {
          columnSchema.setNumberOfChild(field.children.get.size)
          allColumns ++= getAllChildren(field.children)
        }
      })
    })
    allColumns
  }

  def getColumnSchema(dataType: DataType, colName: String, index: Integer, isCol: Boolean,
      encoders: java.util.List[Encoding], isDimensionCol: Boolean,
      colGroup: Integer, precision: Integer, scale: Integer): ColumnSchema = {
    val columnSchema = new ColumnSchema()
    columnSchema.setDataType(dataType)
    columnSchema.setColumnName(colName)
    val highCardinalityDims = cm.highcardinalitydims.getOrElse(Seq())
    if (highCardinalityDims.contains(colName)) {
      encoders.remove(encoders.remove(Encoding.DICTIONARY))
    }
    if (dataType == DataType.TIMESTAMP) {
      encoders.add(Encoding.DIRECT_DICTIONARY)
    }
    var colPropMap = new java.util.HashMap[String, String]()
    if (None != cm.colProps && null != cm.colProps.get.get(colName)) {
      val colProps = cm.colProps.get.get(colName)
      colProps.asScala.foreach { x => colPropMap.put(x.key, x.value) }
    }
    columnSchema.setColumnProperties(colPropMap)
    columnSchema.setEncodingList(encoders)
    val colUniqueIdGenerator = CarbonCommonFactory.getColumnUniqueIdGenerator
    val columnUniqueId = colUniqueIdGenerator.generateUniqueId(cm.schemaName,
      columnSchema)
    columnSchema.setColumnUniqueId(columnUniqueId)
    columnSchema.setColumnReferenceId(columnUniqueId)
    columnSchema.setColumnar(isCol)
    columnSchema.setDimensionColumn(isDimensionCol)
    columnSchema.setColumnGroup(colGroup)
    columnSchema.setPrecision(precision)
    columnSchema.setScale(scale)
    // TODO: Need to fill RowGroupID, converted type
    // & Number of Children after DDL finalization
    columnSchema
  }

  // process create dml fields and create wrapper TableInfo object
  def process: TableInfo = {
    val LOGGER = LogServiceFactory.getLogService(TableNewProcessor.getClass.getName)
    var allColumns = Seq[ColumnSchema]()
    var index = 0
    cm.dimCols.foreach(field => {
      val encoders = new java.util.ArrayList[Encoding]()
      encoders.add(Encoding.DICTIONARY)
      val columnSchema: ColumnSchema = getColumnSchema(normalizeType(field.dataType.getOrElse("")),
        field.name.getOrElse(field.column),
        index,
        isCol = true,
        encoders,
        isDimensionCol = true,
        -1,
        field.precision,
        field.scale)
      allColumns ++= Seq(columnSchema)
      index = index + 1
      if (field.children.isDefined && field.children.get != null) {
        columnSchema.setNumberOfChild(field.children.get.size)
        allColumns ++= getAllChildren(field.children)
      }
    })

    cm.msrCols.foreach(field => {
      val encoders = new java.util.ArrayList[Encoding]()
      val coloumnSchema: ColumnSchema = getColumnSchema(normalizeType(field.dataType.getOrElse("")),
        field.name.getOrElse(field.column),
        index,
        isCol = true,
        encoders,
        isDimensionCol = false,
        -1,
        field.precision,
        field.scale)
      val measureCol = coloumnSchema

      allColumns ++= Seq(measureCol)
      index = index + 1
    })

    // Check if there is any duplicate measures or dimensions.
    // Its based on the dimension name and measure name
    allColumns.groupBy(_.getColumnName).foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(s"Duplicate column found with name : $name")
      LOGGER.audit(
        s"Validation failed for Create/Alter Table Operation for ${cm.schemaName}.${cm.cubeName}" +
        s"Duplicate column found with name : $name")
      sys.error(s"Duplicate dimensions found with name : $name")
    })

    val highCardinalityDims = cm.highcardinalitydims.getOrElse(Seq())

    checkColGroupsValidity(cm.columnGroups, allColumns, highCardinalityDims)

    updateColumnGroupsInFields(cm.columnGroups, allColumns)

    var newOrderedDims = scala.collection.mutable.ListBuffer[ColumnSchema]()
    val complexDims = scala.collection.mutable.ListBuffer[ColumnSchema]()
    val measures = scala.collection.mutable.ListBuffer[ColumnSchema]()
    for (column <- allColumns) {
      if (highCardinalityDims.contains(column.getColumnName)) {
        newOrderedDims += column
      }
      else if (column.isComplex) {
        complexDims += column
      }
      else if (column.isDimensionColumn) {
        newOrderedDims += column
      }
      else {
        measures += column
      }

    }

    // Adding dummy measure if no measure is provided
    if (measures.size < 1) {
      val encoders = new java.util.ArrayList[Encoding]()
      val coloumnSchema: ColumnSchema = getColumnSchema(DataType.DOUBLE,
        CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE,
        index,
        true,
        encoders,
        false,
        -1, 0, 0)
      coloumnSchema.setInvisible(true)
      val measureColumn = coloumnSchema
      measures += measureColumn
      allColumns = allColumns ++ measures
    }
    val columnValidator = CarbonSparkFactory.getCarbonColumnValidator()
    columnValidator.validateColumns(allColumns)
    newOrderedDims = newOrderedDims ++ complexDims ++ measures

    cm.partitioner match {
      case Some(part: Partitioner) =>
        var definedpartCols = part.partitionColumn
        val columnBuffer = new ArrayBuffer[String]
        part.partitionColumn.foreach { col =>
          newOrderedDims.foreach { dim =>
            if (dim.getColumnName.equalsIgnoreCase(col)) {
              definedpartCols = definedpartCols.dropWhile { c => c.equals(col) }
              columnBuffer += col
            }
          }
        }

        // Special Case, where Partition count alone is sent to Carbon for dataloading
        if (part.partitionClass.isEmpty) {
          if (part.partitionColumn(0).isEmpty) {
            Partitioner(
              "org.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl",
              Array(""), part.partitionCount, null)
          }
          else {
            // case where partition cols are set and partition class is not set.
            // so setting the default value.
            Partitioner(
              "org.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl",
              part.partitionColumn, part.partitionCount, null)
          }
        }
        else if (definedpartCols.nonEmpty) {
          val msg = definedpartCols.mkString(", ")
          LOGGER.error(s"partition columns specified are not part of Dimension columns : $msg")
          LOGGER.audit(
            s"Validation failed for Create/Alter Table Operation for " +
              s"${cm.schemaName}.${cm.cubeName} " +
            s"partition columns specified are not part of Dimension columns : $msg")
          sys.error(s"partition columns specified are not part of Dimension columns : $msg")
        }
        else {

          try {
            Class.forName(part.partitionClass).newInstance()
          } catch {
            case e: Exception =>
              val cl = part.partitionClass
              LOGGER.audit(
                s"Validation failed for Create/Alter Table Operation for " +
                  s"${cm.schemaName}.${cm.cubeName} " +
                s"partition class specified can not be found or loaded : $cl")
              sys.error(s"partition class specified can not be found or loaded : $cl")
          }

          Partitioner(part.partitionClass, columnBuffer.toArray, part.partitionCount, null)
        }
      case None =>
        Partitioner("org.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl",
          Array(""), 20, null)
    }
    val tableInfo = new TableInfo()
    val tableSchema = new TableSchema()
    val schemaEvol = new SchemaEvolution()
    schemaEvol
      .setSchemaEvolutionEntryList(new util.ArrayList[SchemaEvolutionEntry]())
    tableSchema.setTableId(UUID.randomUUID().toString)
    tableSchema.setTableName(cm.cubeName)
    tableSchema.setListOfColumns(allColumns.asJava)
    tableSchema.setSchemaEvalution(schemaEvol)
    tableInfo.setDatabaseName(cm.schemaName)
    tableInfo.setTableUniqueName(cm.schemaName + "_" + cm.cubeName)
    tableInfo.setLastUpdatedTime(System.currentTimeMillis())
    tableInfo.setFactTable(tableSchema)
    tableInfo.setAggregateTableList(new util.ArrayList[TableSchema]())
    tableInfo
  }

  private def normalizeType(dataType: String): DataType = {
    dataType match {
      case "String" => DataType.STRING
      case "int" => DataType.INT
      case "Integer" => DataType.INT
      case "Long" => DataType.LONG
      case "BigInt" => DataType.LONG
      case "Numeric" => DataType.DOUBLE
      case "Double" => DataType.DOUBLE
      case "Decimal" => DataType.DECIMAL
      case "Timestamp" => DataType.TIMESTAMP
      case "Array" => DataType.ARRAY
      case "Struct" => DataType.STRUCT
      case _ => sys.error("Unsupported data type : " + dataType)
    }
  }

  //  For checking if the specified col group columns are specified in fields list.
  protected def checkColGroupsValidity(colGrps: Seq[String],
      allCols: Seq[ColumnSchema],
      highCardCols: Seq[String]): Unit = {
    if (null != colGrps) {
      colGrps.foreach(columngroup => {
        val rowCols = columngroup.split(",")
        rowCols.foreach(colForGrouping => {
          var found: Boolean = false
          // check for dimensions + measures
          allCols.foreach(eachCol => {
            if (eachCol.getColumnName.equalsIgnoreCase(colForGrouping.trim())) {
              found = true
            }
          })
          // check for No Dicitonary dimensions
          highCardCols.foreach(noDicCol => {
            if (colForGrouping.trim.equalsIgnoreCase(noDicCol)) {
              found = true
            }
          })

          if (!found) {
            sys.error(s"column $colForGrouping is not present in Field list")
          }
        })
      })
    }
  }

  // For updating the col group details for fields.
  private def updateColumnGroupsInFields(colGrps: Seq[String], allCols: Seq[ColumnSchema]): Unit = {
    if (null != colGrps) {
      var colGroupId = -1
      colGrps.foreach(columngroup => {
        colGroupId += 1
        val rowCols = columngroup.split(",")
        rowCols.foreach(row => {

          allCols.foreach(eachCol => {

            if (eachCol.getColumnName.equalsIgnoreCase(row.trim)) {
              eachCol.setColumnGroup(colGroupId)
              eachCol.setColumnar(false)
            }
          })
        })
      })
    }
  }
}

object TableProcessor {
  def apply(cm: tableModel, sqlContext: SQLContext): Cube = {
    new TableProcessor(cm, sqlContext).process()
  }
}

class TableProcessor(cm: tableModel, sqlContext: SQLContext) {
  val timeDims = Seq("TimeYears", "TimeMonths", "TimeDays", "TimeHours", "TimeMinutes")
  val numericTypes = Seq(CarbonCommonConstants.INTEGER_TYPE, CarbonCommonConstants.DOUBLE_TYPE,
    CarbonCommonConstants.LONG_TYPE, CarbonCommonConstants.FLOAT_TYPE)

  def getAllChildren(fieldChildren: Option[List[Field]]): Seq[Level] = {
    var levels: Seq[Level] = Seq[Level]()
    fieldChildren.foreach(fields => {
      fields.foreach(field => {
        if (field.parent != null) {
          levels ++= Seq(Level(field.name.getOrElse(field.column), field.column, Int.MaxValue,
            field.dataType.getOrElse(CarbonCommonConstants.STRING), field.parent,
            field.storeType.getOrElse("Columnar")))
        } else {
          levels ++= Seq(Level(field.name.getOrElse(field.column), field.column, Int.MaxValue,
            field.dataType.getOrElse(CarbonCommonConstants.STRING),
            field.storeType.getOrElse("Columnar")))
        }
        if (field.children.get != null) {
          levels ++= getAllChildren(field.children)
        }
      })
    })
    levels
  }

  def process(): Cube = {

    var levels = Seq[Level]()
    var measures = Seq[Measure]()
    var dimSrcDimensions = Seq[Dimension]()
    val LOGGER = LogServiceFactory.getLogService(TableProcessor.getClass.getName)

    // Create Table DDL with Database defination
    cm.dimCols.foreach(field => {
      if (field.parent != null) {
        levels ++= Seq(Level(field.name.getOrElse(field.column), field.column, Int.MaxValue,
          field.dataType.getOrElse(CarbonCommonConstants.STRING), field.parent,
          field.storeType.getOrElse(CarbonCommonConstants.COLUMNAR)))
      } else {
        levels ++= Seq(Level(field.name.getOrElse(field.column), field.column, Int.MaxValue,
          field.dataType.getOrElse(CarbonCommonConstants.STRING), field.parent,
          field.storeType.getOrElse(CarbonCommonConstants.COLUMNAR)))
      }
      if (field.children.get != null) {
        levels ++= getAllChildren(field.children)
      }
    })
    measures = cm.msrCols.map(field => Measure(field.name.getOrElse(field.column), field.column,
      field.dataType.getOrElse(CarbonCommonConstants.NUMERIC)))

    if (cm.withKeyword.equalsIgnoreCase(CarbonCommonConstants.WITH) &&
        cm.simpleDimRelations.nonEmpty) {
      cm.simpleDimRelations.foreach(relationEntry => {

        // Split the levels and seperate levels with dimension levels
        val split = levels.partition(x => relationEntry.cols.get.contains(x.name))

        val dimLevels = split._1
        levels = split._2

        def getMissingRelationLevel: Level = {
          Level(relationEntry.relation.rightColumn,
            relationEntry.relation.rightColumn, Int.MaxValue, CarbonCommonConstants.STRING)
        }

        val dimHierarchies = dimLevels.map(field =>
          Hierarchy(relationEntry.tableName, Some(dimLevels.find(dl =>
            dl.name.equalsIgnoreCase(relationEntry.relation.rightColumn))
            .getOrElse(getMissingRelationLevel).column),
            Seq(field), Some(relationEntry.tableName)))
        dimSrcDimensions = dimSrcDimensions ++ dimHierarchies.map(
          field => Dimension(field.levels.head.name, Seq(field),
            Some(relationEntry.relation.leftColumn)))
      })
    }

    // Check if there is any duplicate measures or dimensions.
    // Its based on the dimension name and measure name
    levels.groupBy(_.name).foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(s"Duplicate dimensions found with name : $name")
      LOGGER.audit(
        s"Validation failed for Create/Alter Table Operation for ${cm.schemaName}.${cm.cubeName} " +
        s"Duplicate dimensions found with name : $name")
      sys.error(s"Duplicate dimensions found with name : $name")
    })

    levels.groupBy(_.column).foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(s"Duplicate dimensions found with column name : $name")
      LOGGER.audit(
        s"Validation failed for Create/Alter Table Operation for ${cm.schemaName}.${cm.cubeName} " +
        s"Duplicate dimensions found with column name : $name")
      sys.error(s"Duplicate dimensions found with column name : $name")
    })

    measures.groupBy(_.name).foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(s"Duplicate measures found with name : $name")
      LOGGER.audit(
        s"Validation failed for Create/Alter Table Operation for ${cm.schemaName}.${cm.cubeName} " +
        s"Duplicate measures found with name : $name")
      sys.error(s"Duplicate measures found with name : $name")
    })

    measures.groupBy(_.column).foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(s"Duplicate measures found with column name : $name")
      LOGGER.audit(
        s"Validation failed for Create/Alter Table Operation for ${cm.schemaName}.${cm.cubeName} " +
        s"Duplicate measures found with column name : $name")
      sys.error(s"Duplicate measures found with column name : $name")
    })

    val levelsArray = levels.map(_.name)
    val levelsNdMesures = levelsArray ++ measures.map(_.name)

    cm.aggregation.foreach(a => {
      if (levelsArray.contains(a.msrName)) {
        val fault = a.msrName
        LOGGER.error(s"Aggregator should not be defined for dimension fields [$fault]")
        LOGGER.audit(
          s"Validation failed for Create/Alter Table Operation for " +
            s"${cm.schemaName}.${cm.cubeName} " +
          s"Aggregator should not be defined for dimension fields [$fault]")
        sys.error(s"Aggregator should not be defined for dimension fields [$fault]")
      }
    })

    levelsNdMesures.groupBy(x => x).foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(s"Dimension and Measure defined with same name : $name")
      LOGGER.audit(
        s"Validation failed for Create/Alter Table Operation for ${cm.schemaName}.${cm.cubeName} " +
        s"Dimension and Measure defined with same name : $name")
      sys.error(s"Dimension and Measure defined with same name : $name")
    })

    dimSrcDimensions.foreach(d => {
      d.hierarchies.foreach(h => {
        h.levels.foreach(l => {
          levels = levels.dropWhile(lev => lev.name.equalsIgnoreCase(l.name))
        })
      })
    })

    val groupedSeq = levels.groupBy(_.name.split('.')(0))
    val hierarchies = levels.filter(level => !level.name.contains(".")).map(
      parentLevel => Hierarchy(parentLevel.name, None, groupedSeq.get(parentLevel.name).get, None))
    var dimensions = hierarchies.map(field => Dimension(field.name, Seq(field), None))

    dimensions = dimensions ++ dimSrcDimensions
    val highCardinalityDims = cm.highcardinalitydims.getOrElse(Seq())
    for (dimension <- dimensions) {

      if (highCardinalityDims.contains(dimension.name)) {
        dimension.highCardinality = true
      }

    }

    if (measures.length <= 0) {
      measures = measures ++ Seq(Measure(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE,
        CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE, CarbonCommonConstants.NUMERIC,
        CarbonCommonConstants.SUM, visible = false))
    }

    // Update measures with aggregators if specified.
    val msrsUpdatedWithAggregators = cm.aggregation match {
      case aggs: Seq[Aggregation] =>
        measures.map { f =>
          val matchedMapping = aggs.filter(agg => f.name.equals(agg.msrName))
          if (matchedMapping.isEmpty) {
            f
          }
          else {
            Measure(f.name, f.column, f.dataType, matchedMapping.head.aggType)
          }
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
                  columnBuffer += lev.name
                }
              }
            }
          }
        }


        // Special Case, where Partition count alone is sent to Carbon for dataloading
        if (part.partitionClass.isEmpty && part.partitionColumn(0).isEmpty) {
          Partitioner(
            "org.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl",
            Array(""), part.partitionCount, null)
        }
        else if (definedpartCols.nonEmpty) {
          val msg = definedpartCols.mkString(", ")
          LOGGER.error(s"partition columns specified are not part of Dimension columns : $msg")
          LOGGER.audit(
            s"Validation failed for Create/Alter Table Operation - " +
            s"partition columns specified are not part of Dimension columns : $msg")
          sys.error(s"partition columns specified are not part of Dimension columns : $msg")
        }
        else {

          try {
            Class.forName(part.partitionClass).newInstance()
          } catch {
            case e: Exception =>
              val cl = part.partitionClass
              LOGGER.audit(
                s"Validation failed for Create/Alter Table Operation for " +
                  s"${cm.schemaName}.${cm.cubeName} " +
                s"partition class specified can not be found or loaded : $cl")
              sys.error(s"partition class specified can not be found or loaded : $cl")
          }

          Partitioner(part.partitionClass, columnBuffer.toArray, part.partitionCount, null)
        }
      case None =>
        Partitioner("org.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl",
          Array(""), 20, null)
    }

    Cube(cm.schemaName, cm.cubeName, cm.cubeName, dimensions, msrsUpdatedWithAggregators,
      partitioner)
  }

  // For filtering INCLUDE and EXCLUDE fields if any is defined for Dimention relation
  def filterRelIncludeCols(relationEntry: DimensionRelation, p: (String, String)): Boolean = {
    if (relationEntry.includeKey.get.equalsIgnoreCase(CarbonCommonConstants.INCLUDE)) {
      relationEntry.cols.get.map(x => x.toLowerCase()).contains(p._1.toLowerCase())
    } else {
      !relationEntry.cols.get.map(x => x.toLowerCase()).contains(p._1.toLowerCase())
    }
  }

}

private[sql] case class ShowCreateTable(cm: tableModel, override val output: Seq[Attribute])
  extends RunnableCommand {

  val numericTypes = Seq(CarbonCommonConstants.INTEGER_TYPE, CarbonCommonConstants.DOUBLE_TYPE,
    CarbonCommonConstants.LONG_TYPE, CarbonCommonConstants.FLOAT_TYPE)

  def run(sqlContext: SQLContext): Seq[Row] = {

    var levels = Seq[Level]()
    var levelsToCheckDuplicate = Seq[Level]()
    var measures = Seq[Measure]()
    var dimFileDimensions = Seq[Dimension]()
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    var command = new StringBuilder
    var relation = new StringBuilder

    cm.schemaName = getDB.getDatabaseName(cm.schemaNameOp, sqlContext)

    command = command.append("CREATE Table ").append(cm.schemaName).append(".").append(cm.cubeName)
      .append(" ")
    relation = relation.append("")

    if (cm.fromKeyword.equalsIgnoreCase(CarbonCommonConstants.FROM)) {
      val df = getDataFrame(cm.source, sqlContext)

      // Will maintain the list of all the columns specified by the user.
      // In case if relation is defined. we need to retain the mapping column
      // in case if its in this list
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
          if (f.dataType.isDefined) {
            sys.error(
              s"Specifying Data types is not supported for the fields " +
              s"in the DDL with CSV file or Table : [$f]")
          }
          if (f.column.equalsIgnoreCase(p._1)) {
            isDefined = true
          }
        })
        isDefined
      }

      val rawColumns = if (cm.factFieldsList.isDefined) {
        val cols = df.dtypes.map(f => (f._1.trim(), f._2))
          .filter(filterIncludeCols(_, cm.factFieldsList.get))
        specifiedCols = cols.map(_._1)
        cols
      } else {
        df.dtypes.map(f =>
          if (f._2.startsWith("ArrayType") || f._2.startsWith("StructType")) {
            val fieldIndex = df.schema.getFieldIndex(f._1).get
            (f._1.trim(), df.schema.fields(fieldIndex).dataType.simpleString)
          }
          else {
            (f._1.trim(), f._2)
          })
      }

      val columns = rawColumns
        .filter(c => !c._2.equalsIgnoreCase(CarbonCommonConstants.BINARY_TYPE))
      if (rawColumns.length > columns.length) {
        LOGGER
          .info("BinaryType is not supported. Ignoring all the Binary fields.")
      }

      val (numericColArray, nonNumericColArray) = columns
        .partition(p => numericTypes.map(x => x.toLowerCase()).contains(p._2.toLowerCase()))

      // If dimensions are defined along with Fact CSV/table, consider only defined dimensions
      val dimColArray = if (cm.dimCols.nonEmpty) {
        val dcolArray = columns.filter(filterDefinedCols(_, cm.dimCols))
        val listedCols = dcolArray.map(_._1)
        specifiedCols = specifiedCols ++ listedCols
        dcolArray
      } else {
        nonNumericColArray
      }

      // If measures are defined along with Fact CSV/table, consider only defined measures
      val measureColArray = if (cm.msrCols.nonEmpty) {
        val mColArray = columns.filter(filterDefinedCols(_, cm.msrCols))
        val listedCols = mColArray.map(_._1)
        specifiedCols = specifiedCols ++ listedCols
        mColArray
      } else {
        if (cm.dimCols.nonEmpty) {
          numericColArray.filterNot(filterDefinedCols(_, cm.dimCols))
        } else {
          numericColArray
        }
      }

      measures = measureColArray.map(field => {
        if (cm.msrCols.nonEmpty) {
          val definedField = cm.msrCols.filter(f => f.column.equalsIgnoreCase(field._1))
          if (definedField.nonEmpty && definedField.head.name.isDefined) {
            Measure(
              definedField.head.name.getOrElse(field._1), field._1,
              CarbonScalaUtil.convertSparkToCarbonSchemaDataType(field._2))
          }
          else {
            Measure(field._1, field._1,
              CarbonScalaUtil.convertSparkToCarbonSchemaDataType(field._2))
          }
        }
        else {
          Measure(field._1, field._1,
            CarbonScalaUtil.convertSparkToCarbonSchemaDataType(field._2))
        }
      })

      levels = dimColArray.map(field => {
        if (cm.dimCols.nonEmpty) {
          val definedField = cm.dimCols.filter(f => f.column.equalsIgnoreCase(field._1))
          if (definedField.nonEmpty && definedField.head.name.isDefined) {
            Level(
              definedField.head.name.getOrElse(field._1), field._1, Int.MaxValue,
              CarbonScalaUtil.convertSparkToCarbonSchemaDataType(field._2))
          }
          else {
            Level(field._1, field._1, Int.MaxValue,
              CarbonScalaUtil.convertSparkToCarbonSchemaDataType(field._2))
          }
        }
        else {
          Level(field._1, field._1, Int.MaxValue,
            CarbonScalaUtil.convertSparkToCarbonSchemaDataType(field._2))
        }
      })

      if (cm.dimRelations.nonEmpty) {
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
            LOGGER.error(s"Dimension field defined in the relation [$rcl] " +
                         s"is not present in the Dimension source")
            sys.error(
              s"Dimension field defined in the relation [$rcl] " +
              s"is not present in the Dimension source")
          }

          val rawRelColumns = if (relationEntry.cols.isDefined) {
            relDf.dtypes.map(f => (f._1.trim(), f._2))
              .filter(filterRelIncludeCols(relationEntry, _))
          } else {
            relDf.dtypes.map(f => (f._1.trim(), f._2))
          }

          val relColumns = rawRelColumns
            .filter(c => !c._2.equalsIgnoreCase(CarbonCommonConstants.BINARY_TYPE))
          if (rawRelColumns.length > relColumns.length) {
            LOGGER
              .info("BinaryType is not supported. Ignoring all the Binary fields.")
          }

          // Remove the relation column from fact table as it
          // is already considered in dimension table
          levels = levels.dropWhile(
            p => p.column.equalsIgnoreCase(relationEntry.relation.leftColumn) &&
                 !specifiedCols.map(x => x.toLowerCase()).contains(p.column.toLowerCase()))
          measures = measures.dropWhile(
            p => p.column.equalsIgnoreCase(relationEntry.relation.leftColumn) &&
                 !specifiedCols.map(x => x.toLowerCase()).contains(p.column.toLowerCase()))

          val dimFileLevels: Seq[Level] = Seq[Level]()
          relColumns.map(field => {
            Level(field._1, field._1, Int.MaxValue,
              CarbonScalaUtil.convertSparkToCarbonSchemaDataType(field._2))
          }
          )
          val dimFileHierarchies = dimFileLevels.map(field => Hierarchy(relationEntry.tableName,
            Some(
              dimFileLevels.find(dl => dl.name.equalsIgnoreCase(relationEntry.relation.rightColumn))
                .get.column), Seq(field), Some(relationEntry.tableName)))
          dimFileDimensions = dimFileDimensions ++ dimFileHierarchies.map(
            field => Dimension(field.levels.head.name, Seq(field),
              Some(relationEntry.relation.leftColumn)))

          levelsToCheckDuplicate = levelsToCheckDuplicate ++ dimFileLevels

          if (relation.nonEmpty) {
            relation = relation.append(", ")
          }
          relation = relation.append(relationEntry.tableName).append(" RELATION (FACT.")
            .append(relationEntry.relation.leftColumn).append("=")
            .append(relationEntry.relation.rightColumn).append(") INCLUDE (")

          val includeFields = relColumns.map(field => field._1)
          relation = relation.append(includeFields.mkString(", ")).append(")")

        })
      }

      levelsToCheckDuplicate = levelsToCheckDuplicate ++ levels
    }
    else {
      // Create Table DDL with Database defination
      levels = cm.dimCols.map(
        field => Level(field.name.getOrElse(field.column), field.column, Int.MaxValue,
          field.dataType.getOrElse(CarbonCommonConstants.STRING)))
      measures = cm.msrCols.map(field => Measure(field.name.getOrElse(field.column), field.column,
        field.dataType.getOrElse(CarbonCommonConstants.NUMERIC)))
      levelsToCheckDuplicate = levels

      if (cm.withKeyword.equalsIgnoreCase(CarbonCommonConstants.WITH) &&
          cm.simpleDimRelations.nonEmpty) {
        cm.simpleDimRelations.foreach(relationEntry => {

          val split = levels.partition(x => relationEntry.cols.get.contains(x.name))
          val dimFileLevels = split._1

          if (
            dimFileLevels.count(l => l.name.equalsIgnoreCase(relationEntry.relation.rightColumn)) <=
            0) {
            val rcl = relationEntry.relation.rightColumn
            LOGGER.error(s"Dimension field defined in the relation [$rcl] " +
                         s"is not present in the Dimension source")
            sys.error(
              s"Dimension field defined in the relation [$rcl] " +
              s"is not present in the Dimension source")
          }

          val dimFileHierarchies = dimFileLevels.map(field => Hierarchy(relationEntry.tableName,
            Some(
              dimFileLevels.find(dl => dl.name.equalsIgnoreCase(relationEntry.relation.rightColumn))
                .get.column), Seq(field), Some(relationEntry.tableName)))
          dimFileDimensions = dimFileDimensions ++ dimFileHierarchies.map(
            field => Dimension(field.levels.head.name, Seq(field),
              Some(relationEntry.relation.leftColumn)))

          if (relation.nonEmpty) {
            relation = relation.append(", ")
          }
          relation = relation.append(relationEntry.tableName).append(" RELATION (FACT.")
            .append(relationEntry.relation.leftColumn).append("=")
            .append(relationEntry.relation.rightColumn).append(") INCLUDE (")
          relation = relation.append(relationEntry.cols.get.mkString(", ")).append(")")
        })

      }
    }

    // Check if there is any duplicate measures or dimensions.
    // Its based on the dimension name and measure name
    levelsToCheckDuplicate.groupBy(_.name).foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(s"Duplicate dimensions found with name : $name")
      sys.error(s"Duplicate dimensions found with name : $name")
    })

    measures.groupBy(_.name).foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(s"Duplicate measures found with name : $name")
      sys.error(s"Duplicate measures found with name : $name")
    })

    val levelsArray = levelsToCheckDuplicate.map(_.name)
    val levelsNdMesures = levelsArray ++ measures.map(_.name)

    cm.aggregation.foreach(a => {
      if (levelsArray.contains(a.msrName)) {
        val fault = a.msrName
        LOGGER.error(s"Aggregator should not be defined for dimension fields [$fault]")
        sys.error(s"Aggregator should not be defined for dimension fields [$fault]")
      }
    })

    levelsNdMesures.groupBy(x => x).foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(s"Dimension and Measure defined with same name : $name")
      sys.error(s"Dimension and Measure defined with same name : $name")
    })

    if (levelsArray.size <= 0) {
      sys.error("No Dimensions defined. Table should have atleast one dimesnion !")
    }

    val dims = levelsToCheckDuplicate.map(l => l.name + " " + l.dataType)
    command = command.append("DIMENSIONS (").append(dims.mkString(", ")).append(") ")

    if (measures.nonEmpty) {
      val mesrs = measures.map(m => m.name + " " + m.dataType)
      command = command.append("MEASURES (").append(mesrs.mkString(", ")).append(")")
    }

    if (relation.nonEmpty) {
      command = command.append(" WITH ").append(relation)
    }

    if (cm.aggregation.nonEmpty || cm.partitioner.isDefined) {
      command = command.append(" OPTIONS( ")

      if (cm.aggregation.nonEmpty) {
        val aggs = cm.aggregation.map(a => a.msrName + "=" + a.aggType)
        command = command.append("AGGREGATION[ ").append(aggs.mkString(", ")).append(" ] ")
        if (cm.partitioner.isDefined) {
          command = command.append(", ")
        }
      }

      if (cm.partitioner.isDefined) {
        val partn = cm.partitioner.get
        command = command.append("PARTITIONER[ CLASS='").append(partn.partitionClass)
          .append("', COLUMNS=(").append(partn.partitionColumn.mkString(", "))
          .append("), PARTITION_COUNT=").append(partn.partitionCount).append(" ]")
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
                  columnBuffer += lev.name
                }
              }
            }
          }
        }

        try {
          Class.forName(part.partitionClass).newInstance()
        } catch {
          case e: Exception =>
            val cl = part.partitionClass
            sys.error(s"partition class specified can not be found or loaded : $cl")
        }

        if (definedpartCols.nonEmpty) {
          val msg = definedpartCols.mkString(", ")
          LOGGER.error(s"partition columns specified are not part of Dimension columns : $msg")
          sys.error(s"partition columns specified are not part of Dimension columns : $msg")
        }

      case None =>
    }
    Seq(Row(command.toString))
  }

  def getDataFrame(factSource: Object, sqlContext: SQLContext): DataFrame = {

    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    var dataFrame: DataFrame = null

    factSource match {
      case factFile: String =>
        val fileType = FileFactory.getFileType(factFile)

        if (FileFactory.isFileExist(factFile, fileType)) {
          dataFrame = sqlContext.read.format("com.databricks.spark.csv").options(
            Map("path" -> factFile, "header" -> "true", "inferSchema" -> "true")).load(factFile)
        }
        else {
          LOGGER.error(s"Input source file $factFile does not exists")
          sys.error(s"Input source file $factFile does not exists")
        }
      case tableInfo: Seq[String] =>
        val dbName = if (tableInfo.size > 1) {
          tableInfo.head
        } else {
          getDB.getDatabaseName(None, sqlContext)
        }
        val tableName = if (tableInfo.size > 1) {
          tableInfo(1)
        } else {
          tableInfo.head
        }

        if (sqlContext.tableNames(dbName).map(x => x.toLowerCase())
          .contains(tableName.toLowerCase())) {
          if (dbName.nonEmpty) {
            dataFrame = DataFrame(sqlContext,
              sqlContext.catalog.lookupRelation(Seq(dbName, tableName)))
          }
          else {
            dataFrame = DataFrame(sqlContext, sqlContext.catalog.lookupRelation(Seq(tableName)))
          }
        }
        else {
          LOGGER.error(s"Input source table $tableName does not exists")
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

}


// These are the assumptions made
// 1.We have a single hierarchy under a dimension tag and a single level under a hierarchy tag
// 2.The names of dimensions and measures are case insensitive
// 3.CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE is always added as a measure.
// So we need to ignore this to check duplicates
private[sql] case class AlterTable(
    cm: tableModel,
    dropCols: Seq[String],
    defaultVals: Seq[Default]) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    // TODO : Implement it.
    Seq.empty
  }
}

/**
 * Command for the compaction in alter table command
 *
 * @param alterTableModel
 */
private[sql] case class AlterTableCompaction(alterTableModel: AlterTableModel) extends
  RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    // TODO : Implement it.
    var tableName = alterTableModel.tableName
    val schemaName = getDB.getDatabaseName(alterTableModel.dbName, sqlContext)
    if (null == org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
      .getCarbonTable(schemaName + "_" + tableName)) {
      logError("alter table failed. table not found: " + schemaName + "." + tableName)
      sys.error("alter table failed. table not found: " + schemaName + "." + tableName)
    }

    val relation =
      CarbonEnv.getInstance(sqlContext).carbonCatalog
        .lookupRelation1(Option(schemaName), tableName, None)(sqlContext)
        .asInstanceOf[CarbonRelation]
    if (relation == null) {
      sys.error(s"Table $schemaName.$tableName does not exist")
    }
    val carbonLoadModel = new CarbonLoadModel()


    val table = relation.cubeMeta.carbonTable
    carbonLoadModel.setAggTables(table.getAggregateTablesName.asScala.toArray)
    carbonLoadModel.setTableName(table.getFactTableName)
    val dataLoadSchema = new CarbonDataLoadSchema(table)
    // Need to fill dimension relation
    carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
    carbonLoadModel.setTableName(relation.cubeMeta.carbonTableIdentifier.getTableName)
    carbonLoadModel.setDatabaseName(relation.cubeMeta.carbonTableIdentifier.getDatabaseName)
    carbonLoadModel.setStorePath(relation.cubeMeta.storePath)

    val partitioner = relation.cubeMeta.partitioner

    var kettleHomePath = sqlContext.getConf("carbon.kettle.home", null)
    if (null == kettleHomePath) {
      kettleHomePath = CarbonProperties.getInstance.getProperty("carbon.kettle.home")
    }
    if (kettleHomePath == null) {
      sys.error(s"carbon.kettle.home is not set")
    }
    var storeLocation = CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.STORE_LOCATION_TEMP_PATH,
        System.getProperty("java.io.tmpdir")
      )
    storeLocation = storeLocation + "/carbonstore/" + System.nanoTime()

    CarbonDataRDDFactory
      .alterTableForCompaction(sqlContext,
        alterTableModel,
        carbonLoadModel,
        partitioner,
        relation.cubeMeta.storePath,
        kettleHomePath,
        storeLocation
      )

    Seq.empty
  }
}

private[sql] case class CreateCube(cm: tableModel) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    cm.schemaName = getDB.getDatabaseName(cm.schemaNameOp, sqlContext)
    val tbName = cm.cubeName
    val dbName = cm.schemaName
    LOGGER.audit(s"Creating Table with Database name [$dbName] and Table name [$tbName]")

    val tableInfo: TableInfo = TableNewProcessor(cm, sqlContext)

    if (tableInfo.getFactTable.getListOfColumns.size <= 0) {
      sys.error("No Dimensions found. Table should have at least one dimesnion !")
    }

    if (sqlContext.tableNames(dbName).exists(_.equalsIgnoreCase(tbName))) {
      if (!cm.ifNotExistsSet) {
        LOGGER.audit(
          s"Table creation with Database name [$dbName] and Table name [$tbName] failed. " +
          s"Table [$tbName] already exists under database [$dbName]")
        sys.error(s"Table [$tbName] already exists under database [$dbName]")
      }
    }
    else {

      // Add Database to catalog and persist
      val catalog = CarbonEnv.getInstance(sqlContext).carbonCatalog
      // Need to fill partitioner class when we support partition
      val tablePath = catalog.createCubeFromThrift(tableInfo, dbName, tbName, null)(sqlContext)
      try {
        sqlContext.sql(
          s"""CREATE TABLE $dbName.$tbName USING org.apache.spark.sql.CarbonSource""" +
          s""" OPTIONS (cubename "$dbName.$tbName", tablePath "$tablePath") """)
              .collect
      } catch {
        case e: Exception =>

          val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog
            .lookupRelation2(Seq(dbName, tbName))(sqlContext).asInstanceOf[CarbonRelation]
          if (relation != null) {
            LOGGER.audit(s"Deleting Table [$tbName] under Database [$dbName]" +
                         "as create TABLE failed")
            CarbonEnv.getInstance(sqlContext).carbonCatalog
              .dropCube(relation.cubeMeta.partitioner.partitionCount,
                relation.cubeMeta.storePath,
                dbName,
                tbName)(sqlContext)
          }

          LOGGER.audit(s"Table creation with Database name [$dbName] " +
            s"and Table name [$tbName] failed")
          throw e
      }

      LOGGER.audit(s"Table created with Database name [$dbName] and Table name [$tbName]")
    }

    Seq.empty
  }

  def setV(ref: Any, name: String, value: Any): Unit = {
    ref.getClass.getFields.find(_.getName == name).get
      .set(ref, value.asInstanceOf[AnyRef])
  }
}

private[sql] case class DeleteLoadsById(
    loadids: Seq[String],
    schemaNameOp: Option[String],
    tableName: String) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sqlContext: SQLContext): Seq[Row] = {

    val schemaName = getDB.getDatabaseName(schemaNameOp, sqlContext)
    LOGGER.audit(s"Delete load by Id request has been received for $schemaName.$tableName")

    // validate load ids first
    validateLoadIds

    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(
      Option(schemaName),
      tableName,
      None)(sqlContext).asInstanceOf[CarbonRelation]
    if (relation == null) {
      LOGGER.audit(s"Delete load by Id is failed. Table $schemaName.$tableName does not exist")
      sys.error(s"Table $schemaName.$tableName does not exist")
    }

    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(schemaName + '_' + tableName)

    if (null == carbonTable) {
      CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(
        Option(schemaName),
        tableName,
        None)(sqlContext).asInstanceOf[CarbonRelation]
    }
    val path = carbonTable.getMetaDataFilepath

    var segmentStatusManager =
      new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier)

    val invalidLoadIds = segmentStatusManager.updateDeletionStatus(loadids.asJava, path).asScala

    if (invalidLoadIds.isEmpty) {

      LOGGER.audit(s"Delete load by Id is successfull for $schemaName.$tableName.")
    }
    else {
      sys.error("Delete load by Id is failed. No matching load id found. SegmentSeqId(s) - "
                + invalidLoadIds)
    }

    Seq.empty

  }

  // validates load ids
  private def validateLoadIds: Unit = {
    if (loadids.isEmpty) {
      val errorMessage = "Error: Load id(s) should not be empty."
      throw new MalformedCarbonCommandException(errorMessage)

    }
  }
}

private[sql] case class DeleteLoadsByLoadDate(
   schemaNameOp: Option[String],
  tableName: String,
  dateField: String,
  loadDate: String) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.cubemodel.cubeSchema")

  def run(sqlContext: SQLContext): Seq[Row] = {

    val schemaName = getDB.getDatabaseName(schemaNameOp, sqlContext)
    LOGGER.audit(s"Delete load by load date request has been received for $schemaName.$tableName")

    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(
      Option(schemaName),
      tableName,
     None
    )(sqlContext).asInstanceOf[CarbonRelation]
    if (relation == null) {
      LOGGER
        .audit(s"Delete load by load date is failed. Table $schemaName.$tableName does not " +
         s"exist")
      sys.error(s"Table $schemaName.$tableName does not exist")
    }

    val timeObj = Cast(Literal(loadDate), TimestampType).eval()
    if(null == timeObj) {
      val errorMessage = "Error: Invalid load start time format " + loadDate
      throw new MalformedCarbonCommandException(errorMessage)
    }

    var carbonTable = org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
      .getCarbonTable(schemaName + '_' + tableName)
    var segmentStatusManager = new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier)

    if (null == carbonTable) {
      var relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(
        Option(schemaName),
        tableName,
        None
      )(sqlContext).asInstanceOf[CarbonRelation]
    }
    var path = carbonTable.getMetaDataFilepath()

    var invalidLoadTimestamps = segmentStatusManager
      .updateDeletionStatus(loadDate, path, timeObj.asInstanceOf[java.lang.Long]).asScala
    if(invalidLoadTimestamps.isEmpty) {
      LOGGER.audit(s"Delete load by load date is successfull for $schemaName.$tableName.")
    }
    else {
      sys.error("Delete load by load date is failed. No matching load found.")
    }
    Seq.empty

  }

}

private[sql] case class LoadCube(
    schemaNameOp: Option[String],
    tableName: String,
    factPathFromUser: String,
    dimFilesPath: Seq[DataLoadTableFileMapping],
    partionValues: Map[String, String],
    isOverwriteExist: Boolean = false,
    var inputSqlString: String = null) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)


  def run(sqlContext: SQLContext): Seq[Row] = {

    val schemaName = getDB.getDatabaseName(schemaNameOp, sqlContext)
    if (isOverwriteExist) {
      sys.error("Overwrite is not supported for carbon table with " + schemaName + "." + tableName)
    }
    if (null == org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
      .getCarbonTable(schemaName + "_" + tableName)) {
      logError("Data loading failed. table not found: " + schemaName + "." + tableName)
      LOGGER.audit(s"Data loading failed. table not found: $schemaName.$tableName")
      sys.error("Data loading failed. table not found: " + schemaName + "." + tableName)
    }
    CarbonProperties.getInstance().addProperty("zookeeper.enable.lock", "false")
    val carbonLock = CarbonLockFactory.getCarbonLockObj(org.carbondata.core.
      carbon.metadata.CarbonMetadata.getInstance().getCarbonTable(schemaName + "_" + tableName).
      getMetaDataFilepath, LockUsage.METADATA_LOCK)
    try {
      if (carbonLock.lockWithRetries()) {
        logInfo("Successfully able to get the table metadata file lock")
      }
      else {
        sys.error("Table is locked for updation. Please try after some time")
      }

      val factPath = FileUtils.getPaths(CarbonUtil.checkAndAppendHDFSUrl(factPathFromUser))
      val relation =
        CarbonEnv.getInstance(sqlContext).carbonCatalog
          .lookupRelation1(Option(schemaName), tableName, None)(sqlContext)
          .asInstanceOf[CarbonRelation]
      if (relation == null) {
        sys.error(s"Table $schemaName.$tableName does not exist")
      }
      val carbonLoadModel = new CarbonLoadModel()
      carbonLoadModel.setTableName(relation.cubeMeta.carbonTableIdentifier.getTableName)
      carbonLoadModel.setDatabaseName(relation.cubeMeta.carbonTableIdentifier.getDatabaseName)
      carbonLoadModel.setStorePath(relation.cubeMeta.storePath)
      if (dimFilesPath.isEmpty) {
        carbonLoadModel.setDimFolderPath(null)
      }
      else {
        val x = dimFilesPath.map(f => f.table + ":" + CarbonUtil.checkAndAppendHDFSUrl(f.loadPath))
        carbonLoadModel.setDimFolderPath(x.mkString(","))
      }

      val table = relation.cubeMeta.carbonTable
      carbonLoadModel.setAggTables(table.getAggregateTablesName.asScala.toArray)
      carbonLoadModel.setTableName(table.getFactTableName)
      val dataLoadSchema = new CarbonDataLoadSchema(table)
      // Need to fill dimension relation
      carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
      var storeLocation = ""
      var configuredStore = CarbonLoaderUtil.getConfiguredLocalDirs(SparkEnv.get.conf)
      if (null != configuredStore && configuredStore.length > 0) {
        storeLocation = configuredStore(Random.nextInt(configuredStore.length))
      }
      if (storeLocation == null) {
        storeLocation = System.getProperty("java.io.tmpdir")
      }

      var partitionLocation = relation.cubeMeta.storePath + "/partition/" +
                              relation.cubeMeta.carbonTableIdentifier.getDatabaseName + "/" +
                              relation.cubeMeta.carbonTableIdentifier.getTableName + "/"

      storeLocation = storeLocation + "/carbonstore/" + System.nanoTime()

      val columinar = sqlContext.getConf("carbon.is.columnar.storage", "true").toBoolean
      var kettleHomePath = sqlContext.getConf("carbon.kettle.home", null)
      if (null == kettleHomePath) {
        kettleHomePath = CarbonProperties.getInstance.getProperty("carbon.kettle.home")
      }
      if (kettleHomePath == null) {
        sys.error(s"carbon.kettle.home is not set")
      }

      val delimiter = partionValues.getOrElse("delimiter", ",")
      val quoteChar = partionValues.getOrElse("quotechar", "\"")
      val fileHeader = partionValues.getOrElse("fileheader", "")
      val escapeChar = partionValues.getOrElse("escapechar", "\\")
      val complex_delimiter_level_1 = partionValues.getOrElse("complex_delimiter_level_1", "\\$")
      val complex_delimiter_level_2 = partionValues.getOrElse("complex_delimiter_level_2", "\\:")
      val multiLine = partionValues.getOrElse("multiline", "false").trim.toLowerCase match {
        case "true" => true
        case "false" => false
        case illegal =>
          val errorMessage = "Illegal syntax found: [" + illegal + "] .The value multiline in " +
            "load DDL which you set can only be 'true' or 'false', please check your input DDL."
          throw new MalformedCarbonCommandException(errorMessage)
      }

      carbonLoadModel.setEscapeChar(escapeChar)
      if (delimiter.equalsIgnoreCase(complex_delimiter_level_1) ||
          complex_delimiter_level_1.equalsIgnoreCase(complex_delimiter_level_2) ||
          delimiter.equalsIgnoreCase(complex_delimiter_level_2)) {
        sys.error(s"Field Delimiter & Complex types delimiter are same")
      }
      else {
        carbonLoadModel.setComplexDelimiterLevel1(
          CarbonUtil.escapeComplexDelimiterChar(complex_delimiter_level_1))
        carbonLoadModel.setComplexDelimiterLevel2(
          CarbonUtil.escapeComplexDelimiterChar(complex_delimiter_level_2))
      }

      var partitionStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
      try {
        // First system has to partition the data first and then call the load data
        if (null == relation.cubeMeta.partitioner.partitionColumn ||
            relation.cubeMeta.partitioner.partitionColumn(0).isEmpty) {
          LOGGER.info("Initiating Direct Load for the Table : (" +
                      schemaName + "." + tableName + ")")
          carbonLoadModel.setFactFilePath(factPath)
          carbonLoadModel.setCsvDelimiter(CarbonUtil.unescapeChar(delimiter))
          carbonLoadModel.setCsvHeader(fileHeader)
          carbonLoadModel.setDirectLoad(true)
        }
        else {
          val fileType = FileFactory.getFileType(partitionLocation)
          if (FileFactory.isFileExist(partitionLocation, fileType)) {
            val file = FileFactory.getCarbonFile(partitionLocation, fileType)
            CarbonUtil.deleteFoldersAndFiles(file)
          }
          partitionLocation += System.currentTimeMillis()
          FileFactory.mkdirs(partitionLocation, fileType)
          LOGGER.info("Initiating Data Partitioning for the Table : (" +
                      schemaName + "." + tableName + ")")
          partitionStatus = CarbonContext.partitionData(
            schemaName,
            tableName,
            factPath,
            partitionLocation,
            delimiter,
            quoteChar,
            fileHeader,
            escapeChar, multiLine)(sqlContext.asInstanceOf[HiveContext])
          carbonLoadModel.setFactFilePath(FileUtils.getPaths(partitionLocation))
        }
        GlobalDictionaryUtil
          .generateGlobalDictionary(sqlContext, carbonLoadModel, relation.cubeMeta.storePath)
        CarbonDataRDDFactory
          .loadCarbonData(sqlContext, carbonLoadModel, storeLocation, relation.cubeMeta.storePath,
            kettleHomePath,
            relation.cubeMeta.partitioner, columinar, isAgg = false, partitionStatus)
      }
      catch {
        case ex: Exception =>
          LOGGER.error(ex)
          LOGGER.audit(s"Dataload failure for $schemaName.$tableName. Please check the logs")
          throw ex
      }
      finally {
        // Once the data load is successful delete the unwanted partition files
        try {
          val fileType = FileFactory.getFileType(partitionLocation)
          if (FileFactory.isFileExist(partitionLocation, fileType)) {
            val file = FileFactory
              .getCarbonFile(partitionLocation, fileType)
            CarbonUtil.deleteFoldersAndFiles(file)
          }
        } catch {
          case ex: Exception =>
            LOGGER.error(ex)
            LOGGER.audit(s"Dataload failure for $schemaName.$tableName. " +
              "Problem deleting the partition folder")
            throw ex
        }

      }
    } finally {
      if (carbonLock != null) {
        if (carbonLock.unlock()) {
          logInfo("Table MetaData Unlocked Successfully after data load")
        } else {
          logError("Unable to unlock Table MetaData")
        }
      }
    }
    Seq.empty
  }

}

private[sql] case class AddAggregatesToTable(
    schemaNameOp: Option[String],
    cubeName: String,
    aggregateAttributes: Seq[AggregateTableAttributes])
  extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)


  def run(sqlContext: SQLContext): Seq[Row] = {
    // TODO: Implement it
    Seq.empty
  }
}

private[sql] case class PartitionData(databaseName: String, tableName: String, factPath: String,
    targetPath: String, delimiter: String, quoteChar: String,
    fileHeader: String, escapeChar: String, multiLine: Boolean)
  extends RunnableCommand {

  var partitionStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS

  def run(sqlContext: SQLContext): Seq[Row] = {
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(
      Option(databaseName), tableName, None)(sqlContext).asInstanceOf[CarbonRelation]
    val dimNames = relation.cubeMeta.carbonTable
      .getDimensionByTableName(tableName).asScala.map(_.getColName)
    val msrNames = relation.cubeMeta.carbonTable
      .getDimensionByTableName(tableName).asScala.map(_.getColName)
    val targetFolder = targetPath
    partitionStatus = CarbonDataRDDFactory.partitionCarbonData(
      sqlContext.sparkContext, databaseName,
      tableName, factPath, targetFolder, (dimNames ++ msrNames).toArray
      , fileHeader, delimiter,
      quoteChar, escapeChar, multiLine, relation.cubeMeta.partitioner)
    if (partitionStatus == CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS) {
      logInfo("Bad Record Found while partitioning data")
    }
    Seq.empty
  }
}

private[sql] case class LoadAggregationTable(
    newSchema: CarbonTable,
    schemaName: String,
    cubeName: String,
    aggTableName: String) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(
      Option(schemaName),
      cubeName,
      None)(sqlContext).asInstanceOf[CarbonRelation]
    if (relation == null) {
      sys.error(s"Table $schemaName.$cubeName does not exist")
    }
    val carbonLoadModel = new CarbonLoadModel()
    carbonLoadModel.setTableName(cubeName)
    val table = relation.cubeMeta.carbonTable
    carbonLoadModel.setAggTableName(aggTableName)
    carbonLoadModel.setTableName(table.getFactTableName)
    carbonLoadModel.setAggLoadRequest(true)
    var storeLocation = CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.STORE_LOCATION_TEMP_PATH,
        System.getProperty("java.io.tmpdir"))
    storeLocation = storeLocation + "/carbonstore/" + System.currentTimeMillis()
    val columinar = sqlContext.getConf("carbon.is.columnar.storage", "true").toBoolean
    var kettleHomePath = sqlContext.getConf("carbon.kettle.home", null)
    if (null == kettleHomePath) {
      kettleHomePath = CarbonProperties.getInstance.getProperty("carbon.kettle.home")
    }
    if (kettleHomePath == null) {
      sys.error(s"carbon.kettle.home is not set")
    }
    CarbonDataRDDFactory.loadCarbonData(
      sqlContext,
      carbonLoadModel,
      storeLocation,
      relation.cubeMeta.storePath,
      kettleHomePath,
      relation.cubeMeta.partitioner, columinar, isAgg = true)
    Seq.empty
  }
}


private[sql] case class ShowAllTablesInSchema(
    schemaNameOp: Option[String],
    override val output: Seq[Attribute]
) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val schemaName = getDB.getDatabaseName(schemaNameOp, sqlContext)
    CarbonEnv.getInstance(sqlContext).carbonCatalog.getCubes(Some(schemaName))(sqlContext).map(
      x => Row(x._1,
        sqlContext.asInstanceOf[HiveContext].catalog.tableExists(Seq(schemaName, x._1))))
  }
}

private[sql] case class ShowAllTables(override val output: Seq[Attribute])
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    CarbonEnv.getInstance(sqlContext).carbonCatalog.getAllCubes()(sqlContext)
      .map { x =>
        Row(x._1, x._2, sqlContext.asInstanceOf[HiveContext].catalog.tableExists(Seq(x._1, x._2)))
      }
  }

}

private[sql] case class ShowAllTablesDetail(
    schemaNameOp: Option[String],
    override val output: Seq[Attribute]
) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val dSchemaName = getDB.getDatabaseName(schemaNameOp, sqlContext)
    sqlContext.catalog.getTables(Some(dSchemaName))
      .map(x => Row(null, dSchemaName, x._1, "TABLE", ""))
  }
}

private[sql] case class MergeTable(schemaName: String, cubeName: String, tableName: String)
  extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog
      .lookupRelation2(Seq(schemaName, cubeName), None)(sqlContext).asInstanceOf[CarbonRelation]
    if (relation == null) {
      sys.error(s"Table $schemaName.$cubeName does not exist")
    }
    val carbonLoadModel = new CarbonLoadModel()
    carbonLoadModel.setTableName(cubeName)
    carbonLoadModel.setDatabaseName(schemaName)
    val table = relation.cubeMeta.carbonTable
    var isTablePresent = false
    if (table.getFactTableName.equals(tableName)) {
      isTablePresent = true
    }
    if (!isTablePresent) {
      val aggTables = table.getAggregateTablesName.asScala.toArray
      var aggTable = null
      for (aggTable <- aggTables if aggTable.equals(tableName)) {
        isTablePresent = true
      }
    }
    if (!isTablePresent) {
      sys.error("Invalid table name!")
    }
    carbonLoadModel.setTableName(tableName)
    val dataLoadSchema = new CarbonDataLoadSchema(relation.cubeMeta.carbonTable)
    // Need to fill dimension relation
    // dataLoadSchema.setDimensionRelationList(x$1)
    carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
    var storeLocation = CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.STORE_LOCATION_TEMP_PATH,
        System.getProperty("java.io.tmpdir"))
    storeLocation = storeLocation + "/carbonstore/" + System.currentTimeMillis()
    CarbonDataRDDFactory
      .mergeCarbonData(sqlContext, carbonLoadModel, storeLocation, relation.cubeMeta.storePath,
        relation.cubeMeta.partitioner)
    Seq.empty
  }
}

private[sql] case class DropCubeCommand(ifExistsSet: Boolean, schemaNameOp: Option[String],
    cubeName: String)
  extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val schemaName = getDB.getDatabaseName(schemaNameOp, sqlContext)
    val tmpTable = org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
      .getCarbonTable(schemaName + "_" + cubeName)
    if (null == tmpTable) {
      if (!ifExistsSet) {
        LOGGER
          .audit(s"Dropping carbon table $schemaName.$cubeName failed")
        LOGGER.error(s"Carbon Table $schemaName.$cubeName metadata does not exist")
      }
      if (sqlContext.tableNames(schemaName).map(x => x.toLowerCase())
        .contains(cubeName.toLowerCase())) {
        try {
          sqlContext.asInstanceOf[HiveContext].catalog.client.
            runSqlHive(s"DROP TABLE IF EXISTS $schemaName.$cubeName")
        } catch {
          case e: RuntimeException =>
            LOGGER.audit(
              s"Error While deleting the table $schemaName.$cubeName during drop carbon table" +
              e.getMessage)
        }
      } else if (!ifExistsSet) {
        sys.error(s"Carbon Table $schemaName.$cubeName does not exist")
      }
    } else {
      CarbonProperties.getInstance().addProperty("zookeeper.enable.lock", "false")
      val carbonLock = CarbonLockFactory
        .getCarbonLockObj(tmpTable .getMetaDataFilepath, LockUsage.METADATA_LOCK)
      try {
        if (carbonLock.lockWithRetries()) {
          logInfo("Successfully able to get the table metadata file lock")
        } else {
          LOGGER.audit(s"Dropping table $schemaName.$cubeName failed as the Table is locked")
          sys.error("Table is locked for updation. Please try after some time")
        }

        val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog
          .lookupRelation2(Seq(schemaName, cubeName))(sqlContext).asInstanceOf[CarbonRelation]

        if (relation == null) {
          if (!ifExistsSet) {
            sys.error(s"Table $schemaName.$cubeName does not exist")
          }
        } else {
          LOGGER.audit(s"Deleting table [$cubeName] under database [$schemaName]")

          CarbonEnv.getInstance(sqlContext).carbonCatalog
            .dropCube(relation.cubeMeta.partitioner.partitionCount,
              relation.cubeMeta.storePath,
              relation.cubeMeta.carbonTableIdentifier.getDatabaseName,
              relation.cubeMeta.carbonTableIdentifier.getTableName)(sqlContext)
          CarbonDataRDDFactory
            .dropCube(sqlContext.sparkContext, schemaName, cubeName,
              relation.cubeMeta.partitioner)
          QueryPartitionHelper.getInstance().removePartition(schemaName, cubeName)

          LOGGER.audit(s"Deleted table $schemaName.$cubeName")
        }
      }
      finally {
        if (carbonLock != null) {
          if (carbonLock.unlock()) {
            logInfo("Table MetaData Unlocked Successfully after dropping the table")
            val fileType = FileFactory.getFileType(tmpTable .getMetaDataFilepath)
            if (FileFactory.isFileExist(tmpTable .getMetaDataFilepath, fileType)) {
              val file = FileFactory.getCarbonFile(tmpTable .getMetaDataFilepath, fileType)
              CarbonUtil.deleteFoldersAndFiles(file.getParentFile)
            }
            // delete bad record log after drop cube
            val badLogPath = CarbonUtil.getBadLogPath(schemaName +  File.separator + cubeName)
            val badLogFileType = FileFactory.getFileType(badLogPath)
            if (FileFactory.isFileExist(badLogPath, badLogFileType)) {
              val file = FileFactory.getCarbonFile(badLogPath, badLogFileType)
              CarbonUtil.deleteFoldersAndFiles(file)
            }
          } else {
            logError("Unable to unlock Table MetaData")
          }
        }
      }
    }

    Seq.empty
  }
}

private[sql] case class DropAggregateTableCommand(ifExistsSet: Boolean,
    schemaNameOp: Option[String],
    tableName: String) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    val schemaName = getDB.getDatabaseName(schemaNameOp, sqlContext)
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.
      lookupRelation1(Some(schemaName), tableName, None)(sqlContext).asInstanceOf[CarbonRelation]

    if (relation == null) {
      if (!ifExistsSet) {
        sys.error(s"Aggregate Table $schemaName.$tableName does not exist")
      }
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
    tableName: String,
    limit: Option[String],
    override val output: Seq[Attribute]) extends RunnableCommand {


  override def run(sqlContext: SQLContext): Seq[Row] = {
    val schemaName = getDB.getDatabaseName(schemaNameOp, sqlContext)
    val tableUniqueName = schemaName + "_" + tableName
    val carbonTable = org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
      .getCarbonTable(tableUniqueName)
    if (carbonTable == null) {
      sys.error(s"$schemaName.$tableName is not found")
    }
    val path = carbonTable.getMetaDataFilepath()

    var segmentStatusManager = new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier)

    val loadMetadataDetailsArray = segmentStatusManager.readLoadMetadata(path)

    if (loadMetadataDetailsArray.nonEmpty) {

      val parser = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP)

      var loadMetadataDetailsSortedArray = loadMetadataDetailsArray.sortWith(
        (l1, l2) => java.lang.Double.parseDouble(l1.getLoadName) > java.lang.Double
          .parseDouble(l2.getLoadName)
      )


      if (limit.isDefined) {
        loadMetadataDetailsSortedArray = loadMetadataDetailsSortedArray
          .filter(load => load.getVisibility.equalsIgnoreCase("true"))
        val limitLoads = limit.get
        try {
          val lim = Integer.parseInt(limitLoads)
          loadMetadataDetailsSortedArray = loadMetadataDetailsSortedArray.slice(0, lim)
        }
        catch {
          case ex: NumberFormatException => sys.error(s" Entered limit is not a valid Number")
        }

      }

      loadMetadataDetailsSortedArray.filter(load => load.getVisibility.equalsIgnoreCase("true"))
        .map(load =>
          Row(
            load.getLoadName,
            load.getLoadStatus,
            new java.sql.Timestamp(parser.parse(load.getLoadStartTime).getTime),
            new java.sql.Timestamp(parser.parse(load.getTimestamp).getTime))).toSeq
    } else {
      Seq.empty

    }
  }

}

private[sql] case class ShowAggregateTables(
    schemaNameOp: Option[String],
    override val output: Seq[Attribute])
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    Seq(Row("AggTable1"), Row("AggTable2"))
  }
}

private[sql] case class DescribeCommandFormatted(
    child: SparkPlan,
    override val output: Seq[Attribute],
    tblIdentifier: Seq[String])
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog
      .lookupRelation2(tblIdentifier, None)(sqlContext).asInstanceOf[CarbonRelation]
    val mapper = new ObjectMapper()
    val colProps = StringBuilder.newBuilder
    var results: Seq[(String, String, String)] = child.schema.fields.map { field =>
      val comment = if (relation.metaData.dims.contains(field.name)) {
        val dimension = relation.metaData.carbonTable.getDimensionByName(
            relation.cubeMeta.carbonTableIdentifier.getTableName,
            field.name)
        if (null != dimension.getColumnProperties && dimension.getColumnProperties.size() > 0) {
          val colprop = mapper.writeValueAsString(dimension.getColumnProperties)
          colProps.append(field.name).append(".")
          .append(mapper.writeValueAsString(dimension.getColumnProperties))
          .append(",")
        }
        if (dimension.hasEncoding(Encoding.DICTIONARY) &&
            !dimension.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
          "DICTIONARY, KEY COLUMN"
        } else {
          "KEY COLUMN"
        }
      } else {
        ("MEASURE")
      }
      (field.name, field.dataType.simpleString, comment)
    }
    val colPropStr = if (colProps.toString().trim().length() > 0) {
      // drops additional comma at end
      colProps.toString().dropRight(1)
    } else {
      colProps.toString()
    }
    results ++= Seq(("", "", ""), ("##Detailed Table Information", "", ""))
    results ++= Seq(("Database Name : ", relation.cubeMeta.carbonTableIdentifier
      .getDatabaseName, "")
    )
    results ++= Seq(("Table Name : ", relation.cubeMeta.carbonTableIdentifier.getTableName, ""))
    results ++= Seq(("CARBON Store Path : ", relation.cubeMeta.storePath, ""))
    results ++= Seq(("", "", ""), ("#Aggregate Tables", "", ""))
    val carbonTable = relation.cubeMeta.carbonTable
    val aggTables = carbonTable.getAggregateTablesName
    if (aggTables.size == 0) {
      results ++= Seq(("NONE", "", ""))
    } else {
      aggTables.asScala.foreach(aggTable => {
        results ++= Seq(("", "", ""),
          ("Agg Table :" + aggTable, "#Columns", "#AggregateType")
        )
        carbonTable.getDimensionByTableName(aggTable).asScala.foreach(dim => {
          results ++= Seq(("", dim.getColName, ""))
        })
        carbonTable.getMeasureByTableName(aggTable).asScala.foreach(measure => {
          results ++= Seq(("", measure.getColName, measure.getAggregateFunction))
        })
      }
      )
    }
    results ++= Seq(("", "", ""), ("##Detailed Column property", "", ""))
    if (colPropStr.length() > 0) {
      results ++= Seq((colPropStr, "", ""))
    } else {
      results ++= Seq(("NONE", "", ""))
    }
    results.map { case (name, dataType, comment) =>
      Row(f"$name%-36s $dataType%-80s $comment%-72s")
    }
  }

  private def getColumnGroups(dimensions: List[CarbonDimension]): Seq[(String, String, String)] = {
    var results: Seq[(String, String, String)] =
        Seq(("", "", ""), ("##Column Group Information", "", ""))
    val groupedDimensions = dimensions.groupBy(x => x.columnGroupId()).filter {
      case (groupId, _) => groupId != -1
    }.toSeq.sortBy(_._1)
    val groups = groupedDimensions.map(colGroups => {
      colGroups._2.map(dim => dim.getColName).mkString(", ")
    })
    var index = 1
    groups.map { x =>
      results = results:+(s"Column Group $index", x, "")
      index = index + 1
    }
    results
  }
}

private[sql] case class DescribeNativeCommand(sql: String,
    override val output: Seq[Attribute])
  extends RunnableCommand {
  override def run(sqlContext: SQLContext): Seq[Row] = {
    val output = sqlContext.asInstanceOf[HiveContext].catalog.client.runSqlHive(sql)
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

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sqlContext: SQLContext): Seq[Row] = {

    val schemaName = getDB.getDatabaseName(schemaNameOp, sqlContext)
    LOGGER.audit(s"The delete load by date request has been received for $schemaName.$cubeName")
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog
      .lookupRelation1(Some(schemaName), cubeName, None)(sqlContext).asInstanceOf[CarbonRelation]
    var level: String = ""
    var carbonTable = org.carbondata.core.carbon.metadata.CarbonMetadata
         .getInstance().getCarbonTable(schemaName + '_' + cubeName)
    if (relation == null) {
      LOGGER.audit(s"The delete load by date is failed. Table $schemaName.$cubeName does not exist")
      sys.error(s"Table $schemaName.$cubeName does not exist")
    }

    val matches: Seq[AttributeReference] = relation.dimensionsAttr.filter(
      filter => filter.name.equalsIgnoreCase(dateField) &&
                filter.dataType.isInstanceOf[TimestampType]).toList

    if (matches.isEmpty) {
      LOGGER.audit(
        "The delete load by date is failed. " +
        s"Table $schemaName.$cubeName does not contain date field :" + dateField)
      sys.error(s"Table $schemaName.$cubeName does not contain date field " + dateField)
    }
    else {
      level = matches.asJava.get(0).name
    }
    val tableName = relation.metaData.carbonTable.getFactTableName

    val actualColName = relation.metaData.carbonTable.getDimensionByName(tableName, level)
      .getColName
    CarbonDataRDDFactory.deleteLoadByDate(
      sqlContext,
      new CarbonDataLoadSchema(carbonTable),
      schemaName,
      cubeName,
      tableName,
      CarbonEnv.getInstance(sqlContext).carbonCatalog.storePath,
      level,
      actualColName,
      dateValue,
      relation.cubeMeta.partitioner)
    LOGGER.audit(s"The delete load by date $dateValue is successful for $schemaName.$cubeName.")
    Seq.empty
  }
}


private[sql] case class CleanFiles(
    schemaNameOp: Option[String],
    cubeName: String) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sqlContext: SQLContext): Seq[Row] = {
    val schemaName = getDB.getDatabaseName(schemaNameOp, sqlContext)
    LOGGER.audit(s"Clean files request has been received for $schemaName.$cubeName")
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog
      .lookupRelation1(Some(schemaName), cubeName, None)(sqlContext).
      asInstanceOf[CarbonRelation]
    if (relation == null) {
      LOGGER.audit(s"Clean files request is failed. Table $schemaName.$cubeName does not exist")
      sys.error(s"Table $schemaName.$cubeName does not exist")
    }

    val carbonLoadModel = new CarbonLoadModel()
    carbonLoadModel.setTableName(relation.cubeMeta.carbonTableIdentifier.getTableName)
    carbonLoadModel.setDatabaseName(relation.cubeMeta.carbonTableIdentifier.getDatabaseName)
    val table = relation.cubeMeta.carbonTable
    carbonLoadModel.setAggTables(table.getAggregateTablesName.asScala.toArray)
    carbonLoadModel.setTableName(table.getFactTableName)
    carbonLoadModel.setStorePath(relation.cubeMeta.storePath)
    val dataLoadSchema = new CarbonDataLoadSchema(table)
    carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
    CarbonDataRDDFactory.cleanFiles(
      sqlContext.sparkContext,
      carbonLoadModel,
      relation.cubeMeta.storePath,
      relation.cubeMeta.partitioner)
    LOGGER.audit("Clean files request is successfull.")
    Seq.empty
  }
}
