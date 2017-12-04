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
package org.apache.spark.sql.parser

import java.text.SimpleDateFormat

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.antlr.v4.runtime.tree.TerminalNode
import org.apache.spark.sql.{CarbonEnv, CarbonSession, SparkSession}
import org.apache.spark.sql.{CarbonOption, CarbonSession, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.{AbstractSqlParser, ParseException, SqlBaseParser}
import org.apache.spark.sql.catalyst.parser.ParserUtils._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlAstBuilder
import org.apache.spark.sql.execution.command.table.CarbonCreateTableCommand
import org.apache.spark.sql.internal.{SQLConf, VariableSubstitution}
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.util.{CarbonReflectionUtils, PartitionUtils}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.datatype.{DataTypes => CarbonDataTypes, StructField => CarbonStructField}
import org.apache.carbondata.core.metadata.schema.PartitionInfo
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.metadata.schema.table.{BucketFields, MalformedCarbonCommandException, PartitionerField}
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.util.{CarbonScalaUtil, CommonUtil}

/**
 * Concrete parser for Spark SQL stateENABLE_INMEMORY_MERGE_SORT_DEFAULTments and carbon specific
 * statements
 */
class CarbonSparkSqlParser(conf: SQLConf, sparkSession: SparkSession) extends AbstractSqlParser {

  val parser = new CarbonSpark2SqlParser
  val astBuilder = CarbonReflectionUtils.getAstBuilder(conf, parser, sparkSession)

  private val substitutor = new VariableSubstitution(conf)

  override def parsePlan(sqlText: String): LogicalPlan = {
    CarbonSession.updateSessionInfoToCurrentThread(sparkSession)
    try {
      super.parsePlan(sqlText)
    } catch {
      case ce: MalformedCarbonCommandException =>
        throw ce
      case ex =>
        try {
          parser.parse(sqlText)
        } catch {
          case mce: MalformedCarbonCommandException =>
            throw mce
          case e =>
            CarbonException.analysisException(
              s"""== Parse1 ==
                 |${ex.getMessage}
                 |== Parse2 ==
                 |${e.getMessage}
               """.stripMargin.trim)
        }
    }
  }

  protected override def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    super.parse(substitutor.substitute(command))(toResult)
  }
}

class CarbonHelperSqlAstBuilder(conf: SQLConf, parser: CarbonSpark2SqlParser)
  extends SparkSqlAstBuilder(conf) {

  def getFileStorage(createFileFormat: CreateFileFormatContext): String = {
    Option(createFileFormat) match {
      case Some(value) =>
        if (value.children.get(1).getText.equalsIgnoreCase("by")) {
          value.storageHandler().STRING().getSymbol.getText
        } else {
          // The case of "STORED AS PARQUET/ORC"
          ""
        }
      case _ => ""
    }
  }

  /**
   * This method will convert the database name to lower case
   *
   * @param dbName
   * @return Option of String
   */
  def convertDbNameToLowerCase(dbName: Option[String]): Option[String] = {
    dbName match {
      case Some(databaseName) => Some(databaseName.toLowerCase)
      case None => dbName
    }
  }

  def needToConvertToLowerCase(key: String): Boolean = {
    val noConvertList = Array("LIST_INFO", "RANGE_INFO")
    !noConvertList.exists(x => x.equalsIgnoreCase(key));
  }

  /**
   * Parse a key-value map from a [[TablePropertyListContext]], assuming all values are specified.
   */
  def visitPropertyKeyValues(ctx: TablePropertyListContext): Map[String, String] = {
    val props = visitTablePropertyList(ctx)
    val badKeys = props.filter { case (_, v) => v == null }.keys
    if (badKeys.nonEmpty) {
      operationNotAllowed(
        s"Values must be specified for key(s): ${ badKeys.mkString("[", ",", "]") }", ctx)
    }
    props.map { case (key, value) =>
      if (needToConvertToLowerCase(key)) {
        (key.toLowerCase, value.toLowerCase)
      } else {
        (key.toLowerCase, value)
      }
    }
  }

  def createCarbonTable(
      tableHeader: CreateTableHeaderContext,
      skewSpecContext: SkewSpecContext,
      bucketSpecContext: BucketSpecContext,
      partitionColumns: ColTypeListContext,
      columns : ColTypeListContext,
      tablePropertyList : TablePropertyListContext,
      locationSpecContext: SqlBaseParser.LocationSpecContext,
      tableComment : Option[String],
      ctas: TerminalNode) : LogicalPlan = {

    val (tableIdentifier, temp, ifNotExists, external) = visitCreateTableHeader(tableHeader)
    // TODO: implement temporary tables
    if (temp) {
      throw new ParseException(
        "CREATE TEMPORARY TABLE is not supported yet. " +
        "Please use CREATE TEMPORARY VIEW as an alternative.", tableHeader)
    }
    if (skewSpecContext != null) {
      operationNotAllowed("CREATE TABLE ... SKEWED BY", skewSpecContext)
    }
    if (bucketSpecContext != null) {
      operationNotAllowed("CREATE TABLE ... CLUSTERED BY", bucketSpecContext)
    }
    if (external) {
      operationNotAllowed("CREATE EXTERNAL TABLE", tableHeader)
    }
    if (ctas != null && columns != null) {
      operationNotAllowed("CREATE TABLE AS SELECT", tableHeader)
    }

    val tablePath = if (locationSpecContext != null) {
      Some(visitLocationSpec(locationSpecContext))
    } else {
      None
    }
    // validate schema
    val (colsStructFields, colNames) = validateAndExtractSchema(columns, tableIdentifier)

    val tableProperties = mutable.Map[String, String]()
    val properties = Option(tablePropertyList).map(visitPropertyKeyValues).getOrElse(Map.empty)
    properties.foreach { property => tableProperties.put(property._1, property._2) }

    val options = new CarbonOption(null, properties)

    // validate streaming table property
    validateStreamingProperty(options)

    // validate partition clause
    val (partitionByStructFields, partitionFields) =
      validateParitionFields(partitionColumns, colNames, tableProperties)
    val partitionInfo = createPartitionInfo(partitionFields, tableProperties)

    val fields = colsStructFields ++ partitionByStructFields
    fields.zipWithIndex.foreach { case (field, index) =>
      field.setSchemaOrdinal(index)
    }
    val schema = CarbonDataTypes.createStructType(fields.asJava)

    // update the schema to change all float field to double, since carbon does not support
    // float data type currently
    // TODO: remove this limitation
    val updatedFields = schema.getFields.asScala.map { field =>
      if (field.getDataType == CarbonDataTypes.FLOAT) {
        CarbonDataTypes.createStructField(field.getFieldName, CarbonDataTypes.DOUBLE)
      } else {
        field
      }
    }

    // validate and get bucket fields
    val bucketFields: Option[BucketFields] = parser.createBucketFields(tableProperties, options)

    val tableSchema = CarbonDataTypes.createStructType(updatedFields.asJava)

    CarbonCreateTableCommand(
      databaseNameOp = tableIdentifier.database,
      tableName = tableIdentifier.table,
      tableProperties = tableProperties,
      tableSchema = tableSchema,
      ifNotExists = ifNotExists,
      tableLocation = tablePath,
      bucketFields = bucketFields,
      partitionInfo = partitionInfo,
      tableComment = tableComment)
  }


  /**
   * @param partitionCols
   * @param tableProperties
   */
  private def createPartitionInfo(
      partitionCols: Seq[PartitionerField],
      tableProperties: mutable.Map[String, String]): Option[PartitionInfo] = {
    val timestampFormatter = new SimpleDateFormat(CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT))
    val dateFormatter = new SimpleDateFormat(CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT))
    if (partitionCols.isEmpty) {
      None
    } else {
      var partitionType: String = ""
      var numPartitions = 0
      var rangeInfo = List[String]()
      var listInfo = List[List[String]]()

      val columnDataType = partitionCols.head.getDataType
      if (tableProperties.get(CarbonCommonConstants.PARTITION_TYPE).isDefined) {
        partitionType = tableProperties(CarbonCommonConstants.PARTITION_TYPE)
      }
      if (tableProperties.get(CarbonCommonConstants.NUM_PARTITIONS).isDefined) {
        numPartitions = tableProperties(CarbonCommonConstants.NUM_PARTITIONS)
          .toInt
      }
      if (tableProperties.get(CarbonCommonConstants.RANGE_INFO).isDefined) {
        rangeInfo = tableProperties(CarbonCommonConstants.RANGE_INFO).split(",")
          .map(_.trim()).toList
        CommonUtil.validateRangeInfo(rangeInfo, columnDataType, timestampFormatter, dateFormatter)
      }
      if (tableProperties.get(CarbonCommonConstants.LIST_INFO).isDefined) {
        val originListInfo = tableProperties(CarbonCommonConstants.LIST_INFO)
        listInfo = PartitionUtils.getListInfo(originListInfo)
        CommonUtil.validateListInfo(listInfo)
      }
      val cols: ArrayBuffer[ColumnSchema] = new ArrayBuffer[ColumnSchema]()
      partitionCols.foreach(partition_col => {
        val columnSchema = new ColumnSchema
        columnSchema.setDataType(partition_col.getDataType)
        columnSchema.setColumnName(partition_col.getPartitionColumn)
        cols += columnSchema
      })

      var partitionInfo: PartitionInfo = null
      partitionType.toUpperCase() match {
        case "HASH" => partitionInfo = new PartitionInfo(cols.asJava, PartitionType.HASH)
          partitionInfo.initialize(numPartitions)
        case "RANGE" => partitionInfo = new PartitionInfo(cols.asJava, PartitionType.RANGE)
          partitionInfo.setRangeInfo(rangeInfo.asJava)
          partitionInfo.initialize(rangeInfo.size + 1)
        case "LIST" => partitionInfo = new PartitionInfo(cols.asJava, PartitionType.LIST)
          partitionInfo.setListInfo(listInfo.map(_.asJava).asJava)
          partitionInfo.initialize(listInfo.size + 1)
      }
      Some(partitionInfo)
    }
  }

  private def validateStreamingProperty(carbonOption: CarbonOption): Unit = {
    try {
      carbonOption.isStreaming
    } catch {
      case _: IllegalArgumentException =>
        throw new MalformedCarbonCommandException(
          "Table property 'streaming' should be either 'true' or 'false'")
    }
  }

  /**
   * Return partition column fields and PartitionerField object
   */
  private def validateParitionFields(
      partitionColumns: ColTypeListContext,
      colNames: Seq[String],
      tableProperties: mutable.Map[String, String]
  ): (Seq[CarbonStructField], Seq[PartitionerField]) = {
    val partitionByStructFields: Seq[CarbonStructField] =
      Option(partitionColumns)
        .toSeq
        .flatMap(visitColTypeList)
        .map { field =>
          CarbonDataTypes.createStructField(
            field.name,
            CarbonScalaUtil.convertSparkToCarbonDataType(field.dataType))
        }
    val partitionerFields = partitionByStructFields.map { structField =>
      new PartitionerField(structField.getFieldName, structField.getDataType, null)
    }
    if (partitionerFields.nonEmpty) {
      if (!CommonUtil.validatePartitionColumns(tableProperties, partitionerFields)) {
        throw new MalformedCarbonCommandException("Error: Invalid partition definition")
      }
      // partition columns should not be part of the schema
      val badPartCols = partitionerFields.map(_.getPartitionColumn).toSet.intersect(colNames.toSet)
      if (badPartCols.nonEmpty) {
        operationNotAllowed(s"Partition columns should not be specified in the schema: " +
                            badPartCols.map("\"" + _ + "\"").mkString("[", ",", "]")
          , partitionColumns: ColTypeListContext)
      }
    }
    (partitionByStructFields, partitionerFields)
  }

  /**
   * Return column fields and column names
   */
  private def validateAndExtractSchema(
      columns : ColTypeListContext,
      identifier: TableIdentifier): (Seq[CarbonStructField], Seq[String]) = {
    // Validate schema, ensuring whether no duplicate name is used in table definition
    val cols: Seq[CarbonStructField] =
      Option(columns)
        .toSeq
        .flatMap(visitColTypeList)
        .map { field =>
          CarbonDataTypes.createStructField(
            field.name,
            CarbonScalaUtil.convertSparkToCarbonDataType(field.dataType),
            field.getComment().getOrElse(""))
        }
    val colNames = cols.map(_.getFieldName)
    if (colNames.length != colNames.distinct.length) {
      val duplicateColumns = colNames.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => "\"" + x + "\""
      }
      throw new MalformedCarbonCommandException(
        s"Duplicated column names found in table definition of $identifier: " +
        duplicateColumns.mkString("[", ",", "]"))
    }
    (cols, colNames)
  }
}

trait CarbonAstTrait {
  def getFileStorage(createFileFormat: CreateFileFormatContext): String
}
