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

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.parser.{AbstractSqlParser, ParseException, SqlBaseParser}
import org.apache.spark.sql.catalyst.parser.ParserUtils._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.{CreateTableContext, TablePropertyListContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlAstBuilder
import org.apache.spark.sql.execution.command.{BucketFields, CarbonCreateTableCommand, Field, PartitionerField, TableModel}
import org.apache.spark.sql.internal.{SQLConf, VariableSubstitution}

import org.apache.carbondata.core.util.{CarbonSessionInfo, SessionParams, ThreadLocalSessionInfo}
import org.apache.carbondata.spark.CarbonOption
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.util.CommonUtil

/**
 * Concrete parser for Spark SQL stateENABLE_INMEMORY_MERGE_SORT_DEFAULTments and carbon specific
 * statements
 */
class CarbonSparkSqlParser(conf: SQLConf, sparkSession: SparkSession) extends AbstractSqlParser {

  val astBuilder = new CarbonSqlAstBuilder(conf)

  private val substitutor = new VariableSubstitution(conf)

  override def parsePlan(sqlText: String): LogicalPlan = {
    val carbonSessionInfo: CarbonSessionInfo = CarbonEnv.getInstance(sparkSession).carbonSessionInfo
    ThreadLocalSessionInfo.setCarbonSessionInfo(carbonSessionInfo)
    try {
      super.parsePlan(sqlText)
    } catch {
      case ce: MalformedCarbonCommandException =>
        throw ce
      case ex =>
        try {
          astBuilder.parser.parse(sqlText)
        } catch {
          case mce: MalformedCarbonCommandException =>
            throw mce
          case e =>
            sys
              .error("\n" + "BaseSqlParser>>>> " + ex.getMessage + "\n" + "CarbonSqlParser>>>> " +
                     e.getMessage)
        }
    }
  }

  protected override def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    super.parse(substitutor.substitute(command))(toResult)
  }
}

class CarbonSqlAstBuilder(conf: SQLConf) extends SparkSqlAstBuilder(conf) {

  val parser = new CarbonSpark2SqlParser

  override def visitCreateTable(ctx: CreateTableContext): LogicalPlan = {
    val fileStorage = Option(ctx.createFileFormat) match {
      case Some(value) =>
        if (value.children.get(1).getText.equalsIgnoreCase("by")) {
          value.storageHandler().STRING().getSymbol.getText
        } else {
          // The case of "STORED AS PARQUET/ORC"
          ""
        }
      case _ => ""
    }
    if (fileStorage.equalsIgnoreCase("'carbondata'") ||
        fileStorage.equalsIgnoreCase("'org.apache.carbondata.format'")) {
      val (name, temp, ifNotExists, external) = visitCreateTableHeader(ctx.createTableHeader)
      // TODO: implement temporary tables
      if (temp) {
        throw new ParseException(
          "CREATE TEMPORARY TABLE is not supported yet. " +
          "Please use CREATE TEMPORARY VIEW as an alternative.", ctx)
      }
      if (ctx.skewSpec != null) {
        operationNotAllowed("CREATE TABLE ... SKEWED BY", ctx)
      }
      if (ctx.bucketSpec != null) {
        operationNotAllowed("CREATE TABLE ... CLUSTERED BY", ctx)
      }
      val partitionByStructFields = Option(ctx.partitionColumns).toSeq.flatMap(visitColTypeList)
      val partitionerFields = partitionByStructFields.map { structField =>
        PartitionerField(structField.name, Some(structField.dataType.toString), null)
      }
      val cols = Option(ctx.columns).toSeq.flatMap(visitColTypeList)
      val properties = Option(ctx.tablePropertyList).map(visitPropertyKeyValues)
        .getOrElse(Map.empty)

      // Ensuring whether no duplicate name is used in table definition
      val colNames = cols.map(_.name)
      if (colNames.length != colNames.distinct.length) {
        val duplicateColumns = colNames.groupBy(identity).collect {
          case (x, ys) if ys.length > 1 => "\"" + x + "\""
        }
        operationNotAllowed(s"Duplicated column names found in table definition of $name: " +
                            duplicateColumns.mkString("[", ",", "]"), ctx)
      }

      val tableProperties = mutable.Map[String, String]()
      properties.foreach{property => tableProperties.put(property._1, property._2)}

      // validate partition clause
      if (partitionerFields.nonEmpty) {
        if (!CommonUtil.validatePartitionColumns(tableProperties, partitionerFields)) {
          throw new MalformedCarbonCommandException("Error: Invalid partition definition")
        }
        // partition columns should not be part of the schema
        val badPartCols = partitionerFields.map(_.partitionColumn).toSet.intersect(colNames.toSet)
        if (badPartCols.nonEmpty) {
          operationNotAllowed(s"Partition columns should not be specified in the schema: " +
                              badPartCols.map("\"" + _ + "\"").mkString("[", ",", "]"), ctx)
        }
      }
      val fields = parser.getFields(cols ++ partitionByStructFields)
      val options = new CarbonOption(properties)
      // validate tblProperties
      val bucketFields = parser.getBucketFields(tableProperties, fields, options)

      // prepare table model of the collected tokens
      val tableModel: TableModel = parser.prepareTableModel(ifNotExists,
        convertDbNameToLowerCase(name.database),
        name.table.toLowerCase,
        fields,
        partitionerFields,
        tableProperties,
        bucketFields)

      CarbonCreateTableCommand(tableModel)
    } else {
      super.visitCreateTable(ctx)
    }
  }

  /**
   * This method will convert the database name to lower case
   *
   * @param dbName
   * @return Option of String
   */
  protected def convertDbNameToLowerCase(dbName: Option[String]): Option[String] = {
    dbName match {
      case Some(databaseName) => Some(databaseName.toLowerCase)
      case None => dbName
    }
  }

  /**
   * Parse a key-value map from a [[TablePropertyListContext]], assuming all values are specified.
   */
  private def visitPropertyKeyValues(ctx: TablePropertyListContext): Map[String, String] = {
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

  private def needToConvertToLowerCase(key: String): Boolean = {
    val noConvertList = Array("LIST_INFO", "RANGE_INFO")
    !noConvertList.exists(x => x.equalsIgnoreCase(key));
  }


}
