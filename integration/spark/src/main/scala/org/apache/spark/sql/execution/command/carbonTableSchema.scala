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
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.Random

import org.apache.spark.SparkEnv
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast, Literal}
import org.apache.spark.sql.execution.{RunnableCommand, SparkPlan}
import org.apache.spark.sql.hive.{CarbonHiveMetadataUtil, HiveContext}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.util.FileUtils
import org.codehaus.jackson.map.ObjectMapper

import org.apache.carbondata.common.factory.CarbonCommonFactory
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.carbon.CarbonDataLoadSchema
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata
import org.apache.carbondata.core.carbon.metadata.datatype.DataType
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding
import org.apache.carbondata.core.carbon.metadata.schema.{SchemaEvolution, SchemaEvolutionEntry}
import org.apache.carbondata.core.carbon.metadata.schema.table.{CarbonTable, TableInfo, TableSchema}
import org.apache.carbondata.core.carbon.metadata.schema.table.column.{CarbonDimension, ColumnSchema}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastorage.store.impl.FileFactory
import org.apache.carbondata.core.load.LoadMetadataDetails
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.integration.spark.merger.CompactionType
import org.apache.carbondata.lcm.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.lcm.status.SegmentStatusManager
import org.apache.carbondata.processing.etl.DataLoadingException
import org.apache.carbondata.spark.CarbonSparkFactory
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.load._
import org.apache.carbondata.spark.partition.api.impl.QueryPartitionHelper
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory
import org.apache.carbondata.spark.util.{CarbonScalaUtil, GlobalDictionaryUtil}


case class tableModel(
    ifNotExistsSet: Boolean,
    var databaseName: String,
    databaseNameOp: Option[String],
    tableName: String,
    dimCols: Seq[Field],
    msrCols: Seq[Field],
    fromKeyword: String,
    withKeyword: String,
    source: Object,
    factFieldsList: Option[FilterCols],
    dimRelations: Seq[DimensionRelation],
    simpleDimRelations: Seq[DimensionRelation],
    highcardinalitydims: Option[Seq[String]],
    noInvertedIdxCols: Option[Seq[String]],
    aggregation: Seq[Aggregation],
    partitioner: Option[Partitioner],
    columnGroups: Seq[String],
    colProps: Option[util.Map[String, util.List[ColumnProperty]]] = None)

case class Field(column: String, var dataType: Option[String], name: Option[String],
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

case class Table(databaseName: String, tableName: String, dimensions: Seq[Dimension],
    measures: Seq[Measure], partitioner: Partitioner)

case class Default(key: String, value: String)

case class DataLoadTableFileMapping(table: String, loadPath: String)

case class CarbonMergerMapping(storeLocation: String, hdfsStoreLocation: String,
  partitioner: Partitioner, metadataFilePath: String, mergedLoadName: String,
  kettleHomePath: String, tableCreationTime: Long, databaseName: String,
  factTableName: String, validSegments: Array[String], tableId: String)

case class NodeInfo(TaskId: String, noOfBlocks: Int)


case class AlterTableModel(dbName: Option[String], tableName: String,
  compactionType: String, alterSql: String)

case class CompactionModel(compactionSize: Long,
  compactionType: CompactionType,
  carbonTable: CarbonTable,
  tableCreationTime: Long,
  isDDLTrigger: Boolean)

case class CompactionCallableModel(hdfsStoreLocation: String, carbonLoadModel: CarbonLoadModel,
  partitioner: Partitioner, storeLocation: String, carbonTable: CarbonTable, kettleHomePath: String,
  cubeCreationTime: Long, loadsToMerge: util.List[LoadMetadataDetails], sqlContext: SQLContext,
  compactionType: CompactionType)

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
    val colPropMap = new java.util.HashMap[String, String]()
    if (None != cm.colProps && null != cm.colProps.get.get(colName)) {
      val colProps = cm.colProps.get.get(colName)
      colProps.asScala.foreach { x => colPropMap.put(x.key, x.value) }
    }
    columnSchema.setColumnProperties(colPropMap)
    columnSchema.setEncodingList(encoders)
    val colUniqueIdGenerator = CarbonCommonFactory.getColumnUniqueIdGenerator
    val columnUniqueId = colUniqueIdGenerator.generateUniqueId(cm.databaseName,
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
        s"Validation failed for Create/Alter Table Operation " +
        s"for ${cm.databaseName}.${cm.tableName}" +
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

    // Setting the boolen value of useInvertedIndex in column shcehma
    val noInvertedIndexCols = cm.noInvertedIdxCols.getOrElse(Seq())
    for (column <- allColumns) {
      // When the column is measure or the specified no inverted index column in DDL,
      // set useInvertedIndex to false, otherwise true.
      if (noInvertedIndexCols.contains(column.getColumnName) ||
        cm.msrCols.exists(_.column.equalsIgnoreCase(column.getColumnName))) {
        column.setUseInvertedIndex(false)
      } else {
        column.setUseInvertedIndex(true)
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
              "org.apache.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl",
              Array(""), part.partitionCount, null)
          }
          else {
            // case where partition cols are set and partition class is not set.
            // so setting the default value.
            Partitioner(
              "org.apache.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl",
              part.partitionColumn, part.partitionCount, null)
          }
        }
        else if (definedpartCols.nonEmpty) {
          val msg = definedpartCols.mkString(", ")
          LOGGER.error(s"partition columns specified are not part of Dimension columns : $msg")
          LOGGER.audit(
            s"Validation failed for Create/Alter Table Operation for " +
              s"${cm.databaseName}.${cm.tableName} " +
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
                  s"${cm.databaseName}.${cm.tableName} " +
                s"partition class specified can not be found or loaded : $cl")
              sys.error(s"partition class specified can not be found or loaded : $cl")
          }

          Partitioner(part.partitionClass, columnBuffer.toArray, part.partitionCount, null)
        }
      case None =>
        Partitioner("org.apache.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl",
          Array(""), 20, null)
    }
    val tableInfo = new TableInfo()
    val tableSchema = new TableSchema()
    val schemaEvol = new SchemaEvolution()
    schemaEvol
      .setSchemaEvolutionEntryList(new util.ArrayList[SchemaEvolutionEntry]())
    tableSchema.setTableId(UUID.randomUUID().toString)
    tableSchema.setTableName(cm.tableName)
    tableSchema.setListOfColumns(allColumns.asJava)
    tableSchema.setSchemaEvalution(schemaEvol)
    tableInfo.setDatabaseName(cm.databaseName)
    tableInfo.setTableUniqueName(cm.databaseName + "_" + cm.tableName)
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
      case "tinyint" => DataType.SHORT
      case "short" => DataType.SHORT
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
  def apply(cm: tableModel, sqlContext: SQLContext): Table = {
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

  def process(): Table = {

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
        s"Validation failed for Create/Alter Table Operation " +
        s"for ${cm.databaseName}.${cm.tableName} " +
        s"Duplicate dimensions found with name : $name")
      sys.error(s"Duplicate dimensions found with name : $name")
    })

    levels.groupBy(_.column).foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(s"Duplicate dimensions found with column name : $name")
      LOGGER.audit(
        s"Validation failed for Create/Alter Table Operation " +
        s"for ${cm.databaseName}.${cm.tableName} " +
        s"Duplicate dimensions found with column name : $name")
      sys.error(s"Duplicate dimensions found with column name : $name")
    })

    measures.groupBy(_.name).foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(s"Duplicate measures found with name : $name")
      LOGGER.audit(
        s"Validation failed for Create/Alter Table Operation " +
        s"for ${cm.databaseName}.${cm.tableName} " +
        s"Duplicate measures found with name : $name")
      sys.error(s"Duplicate measures found with name : $name")
    })

    measures.groupBy(_.column).foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(s"Duplicate measures found with column name : $name")
      LOGGER.audit(
        s"Validation failed for Create/Alter Table Operation " +
        s"for ${cm.databaseName}.${cm.tableName} " +
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
            s"${cm.databaseName}.${cm.tableName} " +
          s"Aggregator should not be defined for dimension fields [$fault]")
        sys.error(s"Aggregator should not be defined for dimension fields [$fault]")
      }
    })

    levelsNdMesures.groupBy(x => x).foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(s"Dimension and Measure defined with same name : $name")
      LOGGER.audit(
        s"Validation failed for Create/Alter Table Operation " +
        s"for ${cm.databaseName}.${cm.tableName} " +
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
            "org.apache.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl",
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
                  s"${cm.databaseName}.${cm.tableName} " +
                s"partition class specified can not be found or loaded : $cl")
              sys.error(s"partition class specified can not be found or loaded : $cl")
          }

          Partitioner(part.partitionClass, columnBuffer.toArray, part.partitionCount, null)
        }
      case None =>
        Partitioner("org.apache.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl",
          Array(""), 20, null)
    }

    Table(cm.databaseName, cm.tableName, dimensions, msrsUpdatedWithAggregators, partitioner)
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
    val tableName = alterTableModel.tableName
    val databaseName = getDB.getDatabaseName(alterTableModel.dbName, sqlContext)
    if (null == org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
      .getCarbonTable(databaseName + "_" + tableName)) {
      logError("alter table failed. table not found: " + databaseName + "." + tableName)
      sys.error("alter table failed. table not found: " + databaseName + "." + tableName)
    }

    val relation =
      CarbonEnv.getInstance(sqlContext).carbonCatalog
        .lookupRelation1(Option(databaseName), tableName)(sqlContext)
        .asInstanceOf[CarbonRelation]
    if (relation == null) {
      sys.error(s"Table $databaseName.$tableName does not exist")
    }
    val carbonLoadModel = new CarbonLoadModel()


    val table = relation.tableMeta.carbonTable
    carbonLoadModel.setAggTables(table.getAggregateTablesName.asScala.toArray)
    carbonLoadModel.setTableName(table.getFactTableName)
    val dataLoadSchema = new CarbonDataLoadSchema(table)
    // Need to fill dimension relation
    carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
    carbonLoadModel.setTableName(relation.tableMeta.carbonTableIdentifier.getTableName)
    carbonLoadModel.setDatabaseName(relation.tableMeta.carbonTableIdentifier.getDatabaseName)
    carbonLoadModel.setStorePath(relation.tableMeta.storePath)

    val partitioner = relation.tableMeta.partitioner
    val kettleHomePath = CarbonScalaUtil.getKettleHome(sqlContext)

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
        relation.tableMeta.storePath,
        kettleHomePath,
        storeLocation
      )

    Seq.empty
  }
}

private[sql] case class CreateTable(cm: tableModel) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    cm.databaseName = getDB.getDatabaseName(cm.databaseNameOp, sqlContext)
    val tbName = cm.tableName
    val dbName = cm.databaseName
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
      val tablePath = catalog.createTableFromThrift(tableInfo, dbName, tbName, null)(sqlContext)
      try {
        sqlContext.sql(
          s"""CREATE TABLE $dbName.$tbName USING carbondata""" +
          s""" OPTIONS (tableName "$dbName.$tbName", tablePath "$tablePath") """)
              .collect
      } catch {
        case e: Exception =>

          val identifier: TableIdentifier = TableIdentifier(tbName, Some(dbName))
          val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog
            .lookupRelation1(identifier)(sqlContext).asInstanceOf[CarbonRelation]
          if (relation != null) {
            LOGGER.audit(s"Deleting Table [$tbName] under Database [$dbName]" +
                         "as create TABLE failed")
            CarbonEnv.getInstance(sqlContext).carbonCatalog
              .dropTable(relation.tableMeta.partitioner.partitionCount,
                relation.tableMeta.storePath,
                identifier)(sqlContext)
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
    databaseNameOp: Option[String],
    tableName: String) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sqlContext: SQLContext): Seq[Row] = {

    val databaseName = getDB.getDatabaseName(databaseNameOp, sqlContext)
    LOGGER.audit(s"Delete load by Id request has been received for $databaseName.$tableName")

    // validate load ids first
    validateLoadIds
    val dbName = getDB.getDatabaseName(databaseNameOp, sqlContext)

    val identifier = TableIdentifier(tableName, Option(dbName))
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(
      identifier, None)(sqlContext).asInstanceOf[CarbonRelation]
    if (relation == null) {
      LOGGER.audit(s"Delete load by Id is failed. Table $dbName.$tableName does not exist")
      sys.error(s"Table $dbName.$tableName does not exist")
    }

    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(dbName + '_' + tableName)

    if (null == carbonTable) {
      CarbonEnv.getInstance(sqlContext).carbonCatalog
        .lookupRelation1(identifier, None)(sqlContext).asInstanceOf[CarbonRelation]
    }
    val path = carbonTable.getMetaDataFilepath

    val segmentStatusManager =
      new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier)

    val invalidLoadIds = segmentStatusManager.updateDeletionStatus(loadids.asJava, path).asScala

    if (invalidLoadIds.isEmpty) {

      LOGGER.audit(s"Delete load by Id is successfull for $databaseName.$tableName.")
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
   databaseNameOp: Option[String],
  tableName: String,
  dateField: String,
  loadDate: String) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.tablemodel.tableSchema")

  def run(sqlContext: SQLContext): Seq[Row] = {

    LOGGER.audit("The delete load by load date request has been received.")
    val dbName = getDB.getDatabaseName(databaseNameOp, sqlContext)
    val identifier = TableIdentifier(tableName, Option(dbName))
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog
      .lookupRelation1(identifier, None)(sqlContext).asInstanceOf[CarbonRelation]
    if (relation == null) {
      LOGGER
        .audit(s"Delete load by load date is failed. Table $dbName.$tableName does not " +
         s"exist")
      sys.error(s"Table $dbName.$tableName does not exist")
    }

    val timeObj = Cast(Literal(loadDate), TimestampType).eval()
    if(null == timeObj) {
      val errorMessage = "Error: Invalid load start time format " + loadDate
      throw new MalformedCarbonCommandException(errorMessage)
    }

    val carbonTable = org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
      .getCarbonTable(dbName + '_' + tableName)
    val segmentStatusManager = new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier)

    if (null == carbonTable) {
      var relation = CarbonEnv.getInstance(sqlContext).carbonCatalog
        .lookupRelation1(identifier, None)(sqlContext).asInstanceOf[CarbonRelation]
    }
    val path = carbonTable.getMetaDataFilepath()

    val invalidLoadTimestamps = segmentStatusManager
      .updateDeletionStatus(loadDate, path, timeObj.asInstanceOf[java.lang.Long]).asScala
    if(invalidLoadTimestamps.isEmpty) {
      LOGGER.audit(s"Delete load by load date is successfull for $dbName.$tableName.")
    }
    else {
      sys.error("Delete load by load date is failed. No matching load found.")
    }
    Seq.empty

  }

}

private[sql] case class LoadTable(
    databaseNameOp: Option[String],
    tableName: String,
    factPathFromUser: String,
    dimFilesPath: Seq[DataLoadTableFileMapping],
    partionValues: Map[String, String],
    isOverwriteExist: Boolean = false,
    var inputSqlString: String = null) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)


  def run(sqlContext: SQLContext): Seq[Row] = {

    val dbName = getDB.getDatabaseName(databaseNameOp, sqlContext)
    val identifier = TableIdentifier(tableName, Option(dbName))
    if (isOverwriteExist) {
      sys.error("Overwrite is not supported for carbon table with " + dbName + "." + tableName)
    }
    if (null == org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
      .getCarbonTable(dbName + "_" + tableName)) {
      logError("Data loading failed. table not found: " + dbName + "." + tableName)
      LOGGER.audit("Data loading failed. table not found: " + dbName + "." + tableName)
      sys.error("Data loading failed. table not found: " + dbName + "." + tableName)
    }

    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog
        .lookupRelation1(Option(dbName), tableName)(sqlContext)
        .asInstanceOf[CarbonRelation]
    if (relation == null) {
        sys.error(s"Table $dbName.$tableName does not exist")
    }
    CarbonProperties.getInstance().addProperty("zookeeper.enable.lock", "false")
    val carbonLock = CarbonLockFactory
      .getCarbonLockObj(relation.tableMeta.carbonTable.getAbsoluteTableIdentifier
        .getCarbonTableIdentifier,
        LockUsage.METADATA_LOCK
      )
    try {
      if (carbonLock.lockWithRetries()) {
        logInfo("Successfully able to get the table metadata file lock")
      }
      else {
        sys.error("Table is locked for updation. Please try after some time")
      }

      val factPath = FileUtils.getPaths(CarbonUtil.checkAndAppendHDFSUrl(factPathFromUser))
      val carbonLoadModel = new CarbonLoadModel()
      carbonLoadModel.setTableName(relation.tableMeta.carbonTableIdentifier.getTableName)
      carbonLoadModel.setDatabaseName(relation.tableMeta.carbonTableIdentifier.getDatabaseName)
      carbonLoadModel.setStorePath(relation.tableMeta.storePath)
      if (dimFilesPath.isEmpty) {
        carbonLoadModel.setDimFolderPath(null)
      }
      else {
        val x = dimFilesPath.map(f => f.table + ":" + CarbonUtil.checkAndAppendHDFSUrl(f.loadPath))
        carbonLoadModel.setDimFolderPath(x.mkString(","))
      }

      val table = relation.tableMeta.carbonTable
      carbonLoadModel.setAggTables(table.getAggregateTablesName.asScala.toArray)
      carbonLoadModel.setTableName(table.getFactTableName)
      val dataLoadSchema = new CarbonDataLoadSchema(table)
      // Need to fill dimension relation
      carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
      var storeLocation = ""
      val configuredStore = CarbonLoaderUtil.getConfiguredLocalDirs(SparkEnv.get.conf)
      if (null != configuredStore && configuredStore.length > 0) {
        storeLocation = configuredStore(Random.nextInt(configuredStore.length))
      }
      if (storeLocation == null) {
        storeLocation = System.getProperty("java.io.tmpdir")
      }

      var partitionLocation = relation.tableMeta.storePath + "/partition/" +
                              relation.tableMeta.carbonTableIdentifier.getDatabaseName + "/" +
                              relation.tableMeta.carbonTableIdentifier.getTableName + "/"

      storeLocation = storeLocation + "/carbonstore/" + System.nanoTime()

      val columinar = sqlContext.getConf("carbon.is.columnar.storage", "true").toBoolean
      val kettleHomePath = CarbonScalaUtil.getKettleHome(sqlContext)

      val delimiter = partionValues.getOrElse("delimiter", ",")
      val quoteChar = partionValues.getOrElse("quotechar", "\"")
      val fileHeader = partionValues.getOrElse("fileheader", "")
      val escapeChar = partionValues.getOrElse("escapechar", "\\")
      val commentchar = partionValues.getOrElse("commentchar", "#")
      val columnDict = partionValues.getOrElse("columndict", null)
      val serializationNullFormat = partionValues.getOrElse("serialization_null_format", "\\N")
      val allDictionaryPath = partionValues.getOrElse("all_dictionary_path", "")
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
      val maxColumns = partionValues.getOrElse("maxcolumns", null)
      carbonLoadModel.setMaxColumns(maxColumns)
      carbonLoadModel.setEscapeChar(escapeChar)
      carbonLoadModel.setQuoteChar(quoteChar)
      carbonLoadModel.setCommentChar(commentchar)
      carbonLoadModel.setSerializationNullFormat("serialization_null_format" + "," +
        serializationNullFormat)
      if (delimiter.equalsIgnoreCase(complex_delimiter_level_1) ||
          complex_delimiter_level_1.equalsIgnoreCase(complex_delimiter_level_2) ||
          delimiter.equalsIgnoreCase(complex_delimiter_level_2)) {
        sys.error(s"Field Delimiter & Complex types delimiter are same")
      }
      else {
        carbonLoadModel.setComplexDelimiterLevel1(
          CarbonUtil.delimiterConverter(complex_delimiter_level_1))
        carbonLoadModel.setComplexDelimiterLevel2(
          CarbonUtil.delimiterConverter(complex_delimiter_level_2))
      }
      // set local dictionary path, and dictionary file extension
      carbonLoadModel.setAllDictPath(allDictionaryPath)

      var partitionStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
      try {
        // First system has to partition the data first and then call the load data
        if (null == relation.tableMeta.partitioner.partitionColumn ||
            relation.tableMeta.partitioner.partitionColumn(0).isEmpty) {
          LOGGER.info("Initiating Direct Load for the Table : (" +
                      dbName + "." + tableName + ")")
          carbonLoadModel.setFactFilePath(factPath)
          carbonLoadModel.setCsvDelimiter(CarbonUtil.unescapeChar(delimiter))
          carbonLoadModel.setCsvHeader(fileHeader)
          carbonLoadModel.setColDictFilePath(columnDict)
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
                      dbName + "." + tableName + ")")
          partitionStatus = CarbonContext.partitionData(
            dbName,
            tableName,
            factPath,
            partitionLocation,
            delimiter,
            quoteChar,
            fileHeader,
            escapeChar, multiLine)(sqlContext.asInstanceOf[HiveContext])
          carbonLoadModel.setFactFilePath(FileUtils.getPaths(partitionLocation))
          carbonLoadModel.setColDictFilePath(columnDict)
        }
        GlobalDictionaryUtil
          .generateGlobalDictionary(sqlContext, carbonLoadModel, relation.tableMeta.storePath)
        CarbonDataRDDFactory
          .loadCarbonData(sqlContext, carbonLoadModel, storeLocation, relation.tableMeta.storePath,
            kettleHomePath,
            relation.tableMeta.partitioner, columinar, isAgg = false, partitionStatus)
      }
      catch {
        case ex: Exception =>
          LOGGER.error(ex)
          LOGGER.audit(s"Dataload failure for $dbName.$tableName. Please check the logs")
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
            LOGGER.audit(s"Dataload failure for $dbName.$tableName. " +
              "Problem deleting the partition folder")
            throw ex
        }

      }
    } catch {
      case dle: DataLoadingException =>
        LOGGER.audit(s"Dataload failed for $dbName.$tableName. " + dle.getMessage)
        throw dle
      case mce: MalformedCarbonCommandException =>
        LOGGER.audit(s"Dataload failed for $dbName.$tableName. " + mce.getMessage)
        throw mce
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

private[sql] case class PartitionData(databaseName: String, tableName: String, factPath: String,
    targetPath: String, delimiter: String, quoteChar: String,
    fileHeader: String, escapeChar: String, multiLine: Boolean)
  extends RunnableCommand {

  var partitionStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS

  def run(sqlContext: SQLContext): Seq[Row] = {
    val identifier = TableIdentifier(tableName, Option(databaseName))
    val relation = CarbonEnv.getInstance(sqlContext)
      .carbonCatalog.lookupRelation1(identifier, None)(sqlContext).asInstanceOf[CarbonRelation]
    val dimNames = relation.tableMeta.carbonTable
      .getDimensionByTableName(tableName).asScala.map(_.getColName)
    val msrNames = relation.tableMeta.carbonTable
      .getDimensionByTableName(tableName).asScala.map(_.getColName)
    val targetFolder = targetPath
    partitionStatus = CarbonDataRDDFactory.partitionCarbonData(
      sqlContext.sparkContext, databaseName,
      tableName, factPath, targetFolder, (dimNames ++ msrNames).toArray
      , fileHeader, delimiter,
      quoteChar, escapeChar, multiLine, relation.tableMeta.partitioner)
    if (partitionStatus == CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS) {
      logInfo("Bad Record Found while partitioning data")
    }
    Seq.empty
  }
}

private[sql] case class DropTableCommand(ifExistsSet: Boolean, databaseNameOp: Option[String],
    tableName: String)
  extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val dbName = getDB.getDatabaseName(databaseNameOp, sqlContext)
    val identifier = TableIdentifier(tableName, Option(dbName))
    val tmpTable = org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
      .getCarbonTable(dbName + "_" + tableName)
    if (null == tmpTable) {
      if (!ifExistsSet) {
        LOGGER
          .audit(s"Dropping carbon table with Database name [$dbName] and Table name" +
                 "[$tableName] failed")
        LOGGER.error(s"Carbon Table $dbName.$tableName metadata does not exist")
      }
      if (sqlContext.tableNames(dbName).map(x => x.toLowerCase())
        .contains(tableName.toLowerCase())) {
          CarbonHiveMetadataUtil.invalidateAndDropTable(dbName, tableName, sqlContext)
      } else if (!ifExistsSet) {
        sys.error(s"Carbon Table $dbName.$tableName does not exist")
      }
    } else {
      CarbonProperties.getInstance().addProperty("zookeeper.enable.lock", "false")
      val carbonLock = CarbonLockFactory
        .getCarbonLockObj(tmpTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier,
          LockUsage.METADATA_LOCK
        )
      try {
        if (carbonLock.lockWithRetries()) {
          logInfo("Successfully able to get the table metadata file lock")
        } else {
          LOGGER.audit(s"Dropping table $dbName.$tableName failed as the Table is locked")
          sys.error("Table is locked for updation. Please try after some time")
        }

        val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog
          .lookupRelation1(identifier)(sqlContext).asInstanceOf[CarbonRelation]

        if (relation == null) {
          if (!ifExistsSet) {
            sys.error(s"Table $dbName.$tableName does not exist")
          }
        } else {
          LOGGER.audit(s"Deleting table [$tableName] under database [$dbName]")

          CarbonEnv.getInstance(sqlContext).carbonCatalog
            .dropTable(relation.tableMeta.partitioner.partitionCount,
              relation.tableMeta.storePath,
              TableIdentifier(relation.tableMeta.carbonTableIdentifier.getTableName,
                Some(relation.tableMeta.carbonTableIdentifier.getDatabaseName))
              )(sqlContext)
          CarbonDataRDDFactory
            .dropTable(sqlContext.sparkContext, dbName, tableName,
              relation.tableMeta.partitioner)
          QueryPartitionHelper.getInstance().removePartition(dbName, tableName)

          LOGGER.audit(s"Deleted table [$tableName] under database [$dbName]")
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
            // delete bad record log after drop table
            val badLogPath = CarbonUtil.getBadLogPath(dbName +  File.separator + tableName)
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

private[sql] case class ShowLoads(
    databaseNameOp: Option[String],
    tableName: String,
    limit: Option[String],
    override val output: Seq[Attribute]) extends RunnableCommand {


  override def run(sqlContext: SQLContext): Seq[Row] = {
    val databaseName = getDB.getDatabaseName(databaseNameOp, sqlContext)
    val tableUniqueName = databaseName + "_" + tableName
    // Here using checkSchemasModifiedTimeAndReloadTables in tableExists to reload metadata if
    // schema is changed by other process, so that tableInfoMap woulb be refilled.
    val tableExists = CarbonEnv.getInstance(sqlContext).carbonCatalog
      .tableExists(TableIdentifier(tableName, databaseNameOp))(sqlContext)
    if (!tableExists) {
      sys.error(s"$databaseName.$tableName is not found")
    }
    val carbonTable = org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
      .getCarbonTable(tableUniqueName)
    if (carbonTable == null) {
      sys.error(s"$databaseName.$tableName is not found")
    }
    val path = carbonTable.getMetaDataFilepath()

    val segmentStatusManager = new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier)

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

private[sql] case class DescribeCommandFormatted(
    child: SparkPlan,
    override val output: Seq[Attribute],
    tblIdentifier: TableIdentifier)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog
      .lookupRelation1(tblIdentifier)(sqlContext).asInstanceOf[CarbonRelation]
    val mapper = new ObjectMapper()
    val colProps = StringBuilder.newBuilder
    var results: Seq[(String, String, String)] = child.schema.fields.map { field =>
      val comment = if (relation.metaData.dims.contains(field.name)) {
        val dimension = relation.metaData.carbonTable.getDimensionByName(
            relation.tableMeta.carbonTableIdentifier.getTableName,
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
    results ++= Seq(("Database Name : ", relation.tableMeta.carbonTableIdentifier
      .getDatabaseName, "")
    )
    results ++= Seq(("Table Name : ", relation.tableMeta.carbonTableIdentifier.getTableName, ""))
    results ++= Seq(("CARBON Store Path : ", relation.tableMeta.storePath, ""))
    results ++= Seq(("", "", ""), ("#Aggregate Tables", "", ""))
    val carbonTable = relation.tableMeta.carbonTable
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
    val dimension = carbonTable
      .getDimensionByTableName(relation.tableMeta.carbonTableIdentifier.getTableName);
    results ++= getColumnGroups(dimension.asScala.toList)
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

private[sql] case class DeleteLoadByDate(
    databaseNameOp: Option[String],
    tableName: String,
    dateField: String,
    dateValue: String
) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sqlContext: SQLContext): Seq[Row] = {

    val dbName = getDB.getDatabaseName(databaseNameOp, sqlContext)
    LOGGER.audit(s"The delete load by date request has been received for $dbName.$tableName")
    val identifier = TableIdentifier(tableName, Option(dbName))
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog
      .lookupRelation1(identifier)(sqlContext).asInstanceOf[CarbonRelation]
    var level: String = ""
    val carbonTable = org.apache.carbondata.core.carbon.metadata.CarbonMetadata
         .getInstance().getCarbonTable(dbName + '_' + tableName)
    if (relation == null) {
      LOGGER.audit(s"The delete load by date is failed. Table $dbName.$tableName does not exist")
      sys.error(s"Table $dbName.$tableName does not exist")
    }

    val matches: Seq[AttributeReference] = relation.dimensionsAttr.filter(
      filter => filter.name.equalsIgnoreCase(dateField) &&
                filter.dataType.isInstanceOf[TimestampType]).toList

    if (matches.isEmpty) {
      LOGGER.audit(
        "The delete load by date is failed. " +
        "Table $dbName.$tableName does not contain date field :" + dateField)
      sys.error(s"Table $dbName.$tableName does not contain date field " + dateField)
    }
    else {
      level = matches.asJava.get(0).name
    }

    val actualColName = relation.metaData.carbonTable.getDimensionByName(tableName, level)
      .getColName
    CarbonDataRDDFactory.deleteLoadByDate(
      sqlContext,
      new CarbonDataLoadSchema(carbonTable),
      dbName,
      tableName,
      CarbonEnv.getInstance(sqlContext).carbonCatalog.storePath,
      level,
      actualColName,
      dateValue,
      relation.tableMeta.partitioner)
    LOGGER.audit(s"The delete load by date $dateValue is successful for $dbName.$tableName.")
    Seq.empty
  }
}

private[sql] case class CleanFiles(
    databaseNameOp: Option[String],
    tableName: String) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sqlContext: SQLContext): Seq[Row] = {
    val dbName = getDB.getDatabaseName(databaseNameOp, sqlContext)
    LOGGER.audit(s"The clean files request has been received for $dbName.$tableName")
    val identifier = TableIdentifier(tableName, Option(dbName))
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog
      .lookupRelation1(identifier)(sqlContext).
      asInstanceOf[CarbonRelation]
    if (relation == null) {
      LOGGER.audit(s"The clean files request is failed. Table $dbName.$tableName does not exist")
      sys.error(s"Table $dbName.$tableName does not exist")
    }

    val carbonLoadModel = new CarbonLoadModel()
    carbonLoadModel.setTableName(relation.tableMeta.carbonTableIdentifier.getTableName)
    carbonLoadModel.setDatabaseName(relation.tableMeta.carbonTableIdentifier.getDatabaseName)
    val table = relation.tableMeta.carbonTable
    carbonLoadModel.setAggTables(table.getAggregateTablesName.asScala.toArray)
    carbonLoadModel.setTableName(table.getFactTableName)
    carbonLoadModel.setStorePath(relation.tableMeta.storePath)
    val dataLoadSchema = new CarbonDataLoadSchema(table)
    carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
    CarbonDataRDDFactory.cleanFiles(
      sqlContext.sparkContext,
      carbonLoadModel,
      relation.tableMeta.storePath,
      relation.tableMeta.partitioner)
    LOGGER.audit("Clean files request is successfull.")
    Seq.empty
  }
}
