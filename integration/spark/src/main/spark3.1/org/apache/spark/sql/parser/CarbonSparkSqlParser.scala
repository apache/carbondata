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
import org.apache.spark.sql.{CarbonThreadUtil, CarbonToSparkAdapter, SparkSession}
import org.apache.spark.sql.catalyst.parser.{AbstractSqlParser, SqlBaseParser}
import org.apache.spark.sql.catalyst.parser.ParserUtils.operationNotAllowed
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkSqlAstBuilder, SparkSqlParser}
import org.apache.spark.sql.internal.{SQLConf, VariableSubstitution}
import org.apache.spark.sql.parser.CarbonSparkSqlParserUtil.convertPropertiesToLowercase
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.util.CarbonReflectionUtils

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.spark.util.CarbonScalaUtil

/**
 * Concrete parser for Spark SQL statements and carbon specific
 * statements
 */
class CarbonSparkSqlParser(conf: SQLConf, sparkSession: SparkSession) extends SparkSqlParser {

  val parser = new CarbonSpark2SqlParser

  override val astBuilder = CarbonReflectionUtils.getAstBuilder(conf, parser, sparkSession)

  private val substitutor = new VariableSubstitution

  override def parsePlan(sqlText: String): LogicalPlan = {
    CarbonThreadUtil.updateSessionInfoToCurrentThread(sparkSession)
    try {
      val parsedPlan = super.parsePlan(sqlText)
      CarbonScalaUtil.cleanParserThreadLocals
      parsedPlan
    } catch {
      case ce: MalformedCarbonCommandException =>
        CarbonScalaUtil.cleanParserThreadLocals
        throw ce
      case ex: Throwable =>
        try {
          parser.parse(sqlText)
        } catch {
          case mce: MalformedCarbonCommandException =>
            throw mce
          case e: Throwable =>
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
  extends SparkSqlAstBuilderWrapper(conf) {
  /**
   * Parse a key-value map from a [[TablePropertyListContext]], assuming all values are specified.
   */
  override def visitPropertyKeyValues(ctx: TablePropertyListContext): Map[String, String] = {
    val props = visitTablePropertyList(ctx)
    CarbonSparkSqlParserUtil.visitPropertyKeyValues(ctx, props)
  }

  def getPropertyKeyValues(ctx: TablePropertyListContext): Map[String, String] = {
    Option(ctx).map(visitPropertyKeyValues)
      .getOrElse(Map.empty)
  }

  def createCarbonTable(createTableTuple: (CreateTableHeaderContext, SkewSpecContext,
    BucketSpecContext, PartitionFieldListContext, ColTypeListContext, TablePropertyListContext,
    LocationSpecContext, Option[String], TerminalNode, QueryContext, String)): LogicalPlan = {

    val (tableHeader, skewSpecContext,
      bucketSpecContext,
      partitionColumns,
      columns,
      tablePropertyList,
      locationSpecContext,
      tableComment,
      ctas,
      query,
      provider) = createTableTuple

    val (tableIdent, temp, ifNotExists, external) = visitCreateTableHeader(tableHeader)
    val tableIdentifier = CarbonToSparkAdapter.getTableIdentifier(tableIdent)
    val cols: Seq[StructField] = Option(columns).toSeq.flatMap(visitColTypeList)
    val colNames: Seq[String] = CarbonSparkSqlParserUtil
      .validateCreateTableReqAndGetColumns(tableHeader,
        skewSpecContext,
        bucketSpecContext,
        columns,
        cols,
        tableIdentifier,
        temp)
    val tablePath: Option[String] = if (locationSpecContext != null) {
      Some(visitLocationSpec(locationSpecContext))
    } else {
      None
    }

    val properties: Map[String, String] = getPropertyKeyValues(tablePropertyList)
    val tableProperties = convertPropertiesToLowercase(properties)

    // validate partition clause
    // There can be two scenarios for creating partition table with spark 3.1.
    // Scenario 1: create partition columns with datatype.In this case we get struct fields from
    // visitPartitionFieldList and the transform list is empty.
    // Example syntax: create table example(col1 int) partitioned by(col2 int)
    // Scenario 2: create partition columns using column names from schema. Then struct fields will
    // be empty as datatype is not given and transform list consists of field references with
    // partition column names. Search the names in table columns to extract the struct fields.
    // Example syntax: create table example(col1 int, col2 int) partitioned by(col2)
    var (partitionTransformList,
    partitionByStructFields) = visitPartitionFieldList(partitionColumns)
    if (partitionByStructFields.isEmpty && partitionTransformList.nonEmpty) {
      val partitionNames = partitionTransformList
        .flatMap(_.references().flatMap(_.fieldNames()))
      partitionNames.foreach(partName => {
        val structFiled = cols.find(x => x.name.equals(partName))
        if (structFiled != None) {
          partitionByStructFields = partitionByStructFields :+ structFiled.get
        } else {
          operationNotAllowed(s"Partition columns not specified in the schema: " +
                              partitionNames.mkString("[", ",", "]")
            , partitionColumns: PartitionFieldListContext)
        }
      })
    }
    val partitionFields = CarbonToSparkAdapter.
      validatePartitionFields(partitionColumns, colNames, tableProperties,
      partitionByStructFields)

    // validate for create table as select
    val selectQuery = Option(query).map(plan)
    val extraTableTuple = (cols, external, tableIdentifier, ifNotExists, colNames, tablePath,
      tableProperties, properties, partitionByStructFields, partitionFields,
      parser, sparkSession, selectQuery)
    CarbonToSparkAdapter.createCarbonTable(createTableTuple, extraTableTuple)
  }
}

trait CarbonAstTrait {
  def getFileStorage (createFileFormat : CreateFileFormatContext): String
}


