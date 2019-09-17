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
import org.apache.spark.sql.{CarbonUtils, SparkSession}
import org.apache.spark.sql.catalyst.parser.{AbstractSqlParser, SqlBaseParser}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlAstBuilder
import org.apache.spark.sql.internal.{SQLConf, VariableSubstitution}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.util.CarbonReflectionUtils

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.spark.util.CarbonScalaUtil

/**
 * Concrete parser for Spark SQL statements and carbon specific
 * statements
 */
class CarbonSparkSqlParser(conf: SQLConf, sparkSession: SparkSession) extends AbstractSqlParser {

  val parser = new CarbonSpark2SqlParser
  val astBuilder = CarbonReflectionUtils.getAstBuilder(conf, parser, sparkSession)

  private val substitutor = new VariableSubstitution(conf)

  override def parsePlan(sqlText: String): LogicalPlan = {
    CarbonUtils.updateSessionInfoToCurrentThread(sparkSession)
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
  extends SparkSqlAstBuilder(conf) {
  /**
   * Parse a key-value map from a [[TablePropertyListContext]], assuming all values are specified.
   */
  def visitPropertyKeyValues(ctx: TablePropertyListContext): Map[String, String] = {
    val props = visitTablePropertyList(ctx)
    CarbonSparkSqlParserUtil.visitPropertyKeyValues(ctx, props)
  }

  def getPropertyKeyValues(ctx: TablePropertyListContext): Map[String, String]
  = {
    Option(ctx).map(visitPropertyKeyValues)
      .getOrElse(Map.empty)
  }

  def createCarbonTable(createTableTuple: (CreateTableHeaderContext, SkewSpecContext,
    BucketSpecContext, ColTypeListContext, ColTypeListContext, TablePropertyListContext,
    LocationSpecContext, Option[String], TerminalNode, QueryContext, String)): LogicalPlan = {
    // val parser = new CarbonSpark2SqlParser

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

    val (tableIdentifier, temp, ifNotExists, external) = visitCreateTableHeader(tableHeader)
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

    val tableProperties = mutable.Map[String, String]()
    val properties: Map[String, String] = getPropertyKeyValues(tablePropertyList)
    properties.foreach{property => tableProperties.put(property._1, property._2)}

    // validate partition clause
    val partitionByStructFields = Option(partitionColumns).toSeq.flatMap(visitColTypeList)
    val partitionFields = CarbonSparkSqlParserUtil.
      validatePartitionFields(partitionColumns, colNames, tableProperties,
      partitionByStructFields)

    // validate for create table as select
    val selectQuery = Option(query).map(plan)
    val extraTableTuple = (cols, external, tableIdentifier, ifNotExists, colNames, tablePath,
      tableProperties, properties, partitionByStructFields, partitionFields,
      parser, sparkSession, selectQuery)
    CarbonSparkSqlParserUtil
      .createCarbonTable(createTableTuple, extraTableTuple)
  }
}

trait CarbonAstTrait {
  def getFileStorage (createFileFormat : CreateFileFormatContext): String
}


