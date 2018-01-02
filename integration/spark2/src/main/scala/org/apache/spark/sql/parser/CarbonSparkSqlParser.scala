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

import scala.collection.mutable

import org.antlr.v4.runtime.tree.TerminalNode
import org.apache.spark.sql.{CarbonEnv, CarbonSession, SparkSession}
import org.apache.spark.sql.catalyst.parser.{AbstractSqlParser, ParseException, SqlBaseParser}
import org.apache.spark.sql.catalyst.parser.ParserUtils._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlAstBuilder
import org.apache.spark.sql.execution.command.{PartitionerField, TableModel, TableNewProcessor}
import org.apache.spark.sql.execution.command.table.{CarbonCreateTableAsSelectCommand, CarbonCreateTableCommand}
import org.apache.spark.sql.internal.{SQLConf, VariableSubstitution}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.util.CarbonReflectionUtils

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.hadoop.util.SchemaReader
import org.apache.carbondata.spark.CarbonOption
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.util.CommonUtil

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

class CarbonHelperSqlAstBuilder(conf: SQLConf,
    parser: CarbonSpark2SqlParser,
    sparkSession: SparkSession)
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
        s"Values must be specified for key(s): ${badKeys.mkString("[", ",", "]")}", ctx)
    }
    props.map { case (key, value) =>
      if (needToConvertToLowerCase(key)) {
        (key.toLowerCase, value.toLowerCase)
      } else {
        (key.toLowerCase, value)
      }
    }
  }

  def getPropertyKeyValues(ctx: TablePropertyListContext): Map[String, String]
  = {
    Option(ctx).map(visitPropertyKeyValues)
      .getOrElse(Map.empty)
  }

  def createCarbonTable(tableHeader: CreateTableHeaderContext,
      skewSpecContext: SkewSpecContext,
      bucketSpecContext: BucketSpecContext,
      partitionColumns: ColTypeListContext,
      columns : ColTypeListContext,
      tablePropertyList : TablePropertyListContext,
      locationSpecContext: SqlBaseParser.LocationSpecContext,
      tableComment : Option[String],
      ctas: TerminalNode,
      query: QueryContext) : LogicalPlan = {
    // val parser = new CarbonSpark2SqlParser

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

    val cols = Option(columns).toSeq.flatMap(visitColTypeList)
    val properties = getPropertyKeyValues(tablePropertyList)

    // Ensuring whether no duplicate name is used in table definition
    val colNames = cols.map(_.name)
    if (colNames.length != colNames.distinct.length) {
      val duplicateColumns = colNames.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => "\"" + x + "\""
      }
      operationNotAllowed(s"Duplicated column names found in table definition of " +
                          s"$tableIdentifier: ${duplicateColumns.mkString("[", ",", "]")}", columns)
    }

    val tablePath = if (locationSpecContext != null) {
      Some(visitLocationSpec(locationSpecContext))
    } else {
      None
    }

    val tableProperties = mutable.Map[String, String]()
    properties.foreach{property => tableProperties.put(property._1, property._2)}

    // validate partition clause
    val (partitionByStructFields, partitionFields) =
      validateParitionFields(partitionColumns, colNames, tableProperties)

    // validate partition clause
    if (partitionFields.nonEmpty) {
      if (!CommonUtil.validatePartitionColumns(tableProperties, partitionFields)) {
         throw new MalformedCarbonCommandException("Error: Invalid partition definition")
      }
      // partition columns should not be part of the schema
      val badPartCols = partitionFields
        .map(_.partitionColumn.toLowerCase)
        .toSet
        .intersect(colNames.map(_.toLowerCase).toSet)

      if (badPartCols.nonEmpty) {
        operationNotAllowed(s"Partition columns should not be specified in the schema: " +
                            badPartCols.map("\"" + _ + "\"").mkString("[", ",", "]"),
          partitionColumns)
      }
    }

    val options = new CarbonOption(properties)
    // validate streaming property
    validateStreamingProperty(options)
    var fields = parser.getFields(cols ++ partitionByStructFields)
    // validate for create table as select
    val selectQuery = Option(query).map(plan)
    selectQuery match {
      case Some(q) =>
        // create table as select does not allow creation of partitioned table
        if (partitionFields.nonEmpty) {
          val errorMessage = "A Create Table As Select (CTAS) statement is not allowed to " +
                             "create a partitioned table using Carbondata file formats."
          operationNotAllowed(errorMessage, partitionColumns)
        }
        // create table as select does not allow to explicitly specify schema
        if (fields.nonEmpty) {
          operationNotAllowed(
            "Schema may not be specified in a Create Table As Select (CTAS) statement", columns)
        }
        // external table is not allow
        if (external) {
          operationNotAllowed("Create external table as select", tableHeader)
        }
        fields = parser
          .getFields(CarbonEnv.getInstance(sparkSession).carbonMetastore
            .getSchemaFromUnresolvedRelation(sparkSession, Some(q).get))
      case _ =>
        // ignore this case
    }
    if (partitionFields.nonEmpty && options.isStreaming) {
      operationNotAllowed("Streaming is not allowed on partitioned table", partitionColumns)
    }
    // validate tblProperties
    val bucketFields = parser.getBucketFields(tableProperties, fields, options)

    val tableInfo = if (external) {
      // read table info from schema file in the provided table path
      val identifier = AbsoluteTableIdentifier.from(
        tablePath.get,
        CarbonEnv.getDatabaseName(tableIdentifier.database)(sparkSession),
        tableIdentifier.table)
      val table = try {
        SchemaReader.getTableInfo(identifier)
      } catch {
        case e: Throwable =>
          operationNotAllowed(s"Invalid table path provided: ${tablePath.get} ", tableHeader)
      }
      // set "_external" property, so that DROP TABLE will not delete the data
      table.getFactTable.getTableProperties.put("_external", "true")
      table
    } else {
      // prepare table model of the collected tokens
      val tableModel: TableModel = parser.prepareTableModel(
        ifNotExists,
        convertDbNameToLowerCase(tableIdentifier.database),
        tableIdentifier.table.toLowerCase,
        fields,
        partitionFields,
        tableProperties,
        bucketFields,
        isAlterFlow = false,
        tableComment)
      TableNewProcessor(tableModel)
    }
    selectQuery match {
      case query@Some(q) =>
        CarbonCreateTableAsSelectCommand(
          tableInfo = tableInfo,
          query = query.get,
          ifNotExistsSet = ifNotExists,
          tableLocation = tablePath)
      case _ =>
        CarbonCreateTableCommand(
          tableInfo = tableInfo,
          ifNotExistsSet = ifNotExists,
          tableLocation = tablePath)
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

  private def validateParitionFields(
      partitionColumns: ColTypeListContext,
      colNames: Seq[String],
      tableProperties: mutable.Map[String, String]): (Seq[StructField], Seq[PartitionerField]) = {
    val partitionByStructFields = Option(partitionColumns).toSeq.flatMap(visitColTypeList)
    val partitionerFields = partitionByStructFields.map { structField =>
      PartitionerField(structField.name, Some(structField.dataType.toString), null)
    }
    if (partitionerFields.nonEmpty) {
      if (!CommonUtil.validatePartitionColumns(tableProperties, partitionerFields)) {
         throw new MalformedCarbonCommandException("Error: Invalid partition definition")
      }
      // partition columns should not be part of the schema
      val badPartCols = partitionerFields.map(_.partitionColumn).toSet.intersect(colNames.toSet)
      if (badPartCols.nonEmpty) {
        operationNotAllowed(s"Partition columns should not be specified in the schema: " +
                            badPartCols.map("\"" + _ + "\"").mkString("[", ",", "]")
          , partitionColumns: ColTypeListContext)
      }
    }
    (partitionByStructFields, partitionerFields)
  }

}

trait CarbonAstTrait {
  def getFileStorage (createFileFormat : CreateFileFormatContext): String
}


