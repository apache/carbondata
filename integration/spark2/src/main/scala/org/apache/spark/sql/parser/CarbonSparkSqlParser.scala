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

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.catalog.CatalogColumn
import org.apache.spark.sql.catalyst.parser.{AbstractSqlParser, ParseException, SqlBaseParser}
import org.apache.spark.sql.catalyst.parser.ParserUtils._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.{ColTypeListContext, CreateTableContext, TablePropertyListContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlAstBuilder
import org.apache.spark.sql.execution.command.{BucketFields, CreateTable, Field, TableModel}
import org.apache.spark.sql.internal.{SQLConf, VariableSubstitution}
import org.apache.spark.sql.types.DataType

import org.apache.carbondata.spark.CarbonOption
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.util.CommonUtil

/**
 * Concrete parser for Spark SQL statements and carbon specific statements
 */
class CarbonSparkSqlParser(conf: SQLConf) extends AbstractSqlParser {

  val astBuilder = new CarbonSqlAstBuilder(conf)

  private val substitutor = new VariableSubstitution(conf)

  protected override def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    super.parse(substitutor.substitute(command))(toResult)
  }

  override def parsePlan(sqlText: String): LogicalPlan = {
    try {
      super.parsePlan(sqlText)
    } catch {
      case e: Throwable =>
        astBuilder.parser.parse(sqlText)
    }
  }
}

class CarbonSqlAstBuilder(conf: SQLConf) extends SparkSqlAstBuilder(conf) {

  val parser = new CarbonSpark2SqlParser

  override def visitCreateTable(ctx: CreateTableContext): LogicalPlan = {
    val fileStorage = Option(ctx.createFileFormat) match {
      case Some(value) => value.storageHandler().STRING().getSymbol.getText
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
      val partitionCols = Option(ctx.partitionColumns).toSeq.flatMap(visitCatalogColumns)
      val cols = Option(ctx.columns).toSeq.flatMap(visitCatalogColumns)
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

      // For Hive tables, partition columns must not be part of the schema
      val badPartCols = partitionCols.map(_.name).toSet.intersect(colNames.toSet)
      if (badPartCols.nonEmpty) {
        operationNotAllowed(s"Partition columns may not be specified in the schema: " +
                            badPartCols.map("\"" + _ + "\"").mkString("[", ",", "]"), ctx)
      }

      // Note: Hive requires partition columns to be distinct from the schema, so we need
      // to include the partition columns here explicitly
      val schema = cols ++ partitionCols

      val fields = schema.map { col =>
        val x = col.name + ' ' + col.dataType
        val f: Field = parser.anyFieldDef(new parser.lexical.Scanner(x))
        match {
          case parser.Success(field, _) => field.asInstanceOf[Field]
          case failureOrError => throw new MalformedCarbonCommandException(
            s"Unsupported data type: $col.getType")
        }
        // the data type of the decimal type will be like decimal(10,0)
        // so checking the start of the string and taking the precision and scale.
        // resetting the data type with decimal
        if (f.dataType.getOrElse("").startsWith("decimal")) {
          val (precision, scale) = parser.getScaleAndPrecision(col.dataType)
          f.precision = precision
          f.scale = scale
          f.dataType = Some("decimal")
        }
        if(f.dataType.getOrElse("").startsWith("char")) {
          f.dataType = Some("char")
        }
        f.rawSchema = x
        f
      }

      // validate tblProperties
      if (!CommonUtil.validateTblProperties(properties.asJava.asScala, fields)) {
        throw new MalformedCarbonCommandException("Invalid table properties")
      }
      val options = new CarbonOption(properties)
      val bucketFields = {
        if (options.isBucketingEnabled) {
          Some(BucketFields(options.bucketColumns.split(","), options.bucketNumber))
        } else {
          None
        }
      }
      // prepare table model of the collected tokens
      val tableModel: TableModel = parser.prepareTableModel(ifNotExists,
        name.database,
        name.table,
        fields,
        Seq(),
        properties.asJava.asScala,
        bucketFields)

      CreateTable(tableModel)
    } else {
      super.visitCreateTable(ctx)
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
        s"Values must be specified for key(s): ${ badKeys.mkString("[", ",", "]") }", ctx)
    }
    props
  }

  private def visitCatalogColumns(ctx: ColTypeListContext): Seq[CatalogColumn] = {
    withOrigin(ctx) {
      ctx.colType.asScala.map { col =>
        CatalogColumn(
          col.identifier.getText.toLowerCase,
          // Note: for types like "STRUCT<myFirstName: STRING, myLastName: STRING>" we can't
          // just convert the whole type string to lower case, otherwise the struct field names
          // will no longer be case sensitive. Instead, we rely on our parser to get the proper
          // case before passing it to Hive.
          typedVisit[DataType](col.dataType).catalogString,
          nullable = true,
          Option(col.STRING).map(string))
      }
    }
  }
}
