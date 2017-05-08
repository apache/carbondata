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
import scala.collection.mutable

import org.apache.spark.sql.catalyst.parser.{AbstractSqlParser, ParseException, SqlBaseParser}
import org.apache.spark.sql.catalyst.parser.ParserUtils._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.{CreateTableContext,
TablePropertyListContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlAstBuilder
import org.apache.spark.sql.execution.command.{BucketFields, CreateTable, Field,
PartitionerField, TableModel}
import org.apache.spark.sql.internal.{SQLConf, VariableSubstitution}

import org.apache.carbondata.spark.CarbonOption
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.util.CommonUtil

/**
 * Concrete parser for Spark SQL statements and carbon specific statements
 */
class CarbonSparkSqlParser(conf: SQLConf) extends AbstractSqlParser {

  val astBuilder = new CarbonSqlAstBuilder(conf)

  private val substitutor = new VariableSubstitution(conf)

  override def parsePlan(sqlText: String): LogicalPlan = {
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
      properties.foreach{property => tableProperties.put(property._1, property._2.toLowerCase)}

      // validate partition clause
      if (partitionerFields.nonEmpty) {
        if (!CommonUtil.validatePartitionColumns(tableProperties, partitionerFields)) {
          throw new MalformedCarbonCommandException("Invalid partition definition")
        }
        // partition columns should not be part of the schema
        val badPartCols = partitionerFields.map(_.partitionColumn).toSet.intersect(colNames.toSet)
        if (badPartCols.nonEmpty) {
          operationNotAllowed(s"Partition columns should not be specified in the schema: " +
                              badPartCols.map("\"" + _ + "\"").mkString("[", ",", "]"), ctx)
        }
      }
      val schema = cols ++ partitionByStructFields
      val fields = schema.map { col =>
        val x = if (col.dataType.catalogString == "float") {
          '`' + col.name + '`' + " double"
        }
        else {
          '`' + col.name + '`' + ' ' + col.dataType.catalogString
        }
        val f: Field = parser.anyFieldDef(new parser.lexical.Scanner(x.toLowerCase))
        match {
          case parser.Success(field, _) => field.asInstanceOf[Field]
          case failureOrError => throw new MalformedCarbonCommandException(
            s"Unsupported data type: $col.getDataType")
        }
        // the data type of the decimal type will be like decimal(10,0)
        // so checking the start of the string and taking the precision and scale.
        // resetting the data type with decimal
        if (f.dataType.getOrElse("").startsWith("decimal")) {
          val (precision, scale) = parser.getScaleAndPrecision(col.dataType.catalogString)
          f.precision = precision
          f.scale = scale
          f.dataType = Some("decimal")
        }
        if (f.dataType.getOrElse("").startsWith("char")) {
          f.dataType = Some("char")
        }
        else if (f.dataType.getOrElse("").startsWith("float")) {
          f.dataType = Some("double")
        }
        f.rawSchema = x
        f
      }

      // validate tblProperties
      if (!CommonUtil.validateTblProperties(properties.asJava.asScala, fields)) {
        throw new MalformedCarbonCommandException("Invalid table properties")
      }
      val options = new CarbonOption(properties)
      val bucketFields = if (options.isBucketingEnabled) {
        if (options.bucketNumber.toString.contains("-") ||
            options.bucketNumber.toString.contains("+")) {
          throw new MalformedCarbonCommandException("INVALID NUMBER OF BUCKETS SPECIFIED")
        }
        else {
          Some(BucketFields(options.bucketColumns.toLowerCase.split(",").map(_.trim),
            options.bucketNumber))
        }
      } else {
        None
      }

      // prepare table model of the collected tokens
      val tableModel: TableModel = parser.prepareTableModel(ifNotExists,
        convertDbNameToLowerCase(name.database),
        name.table.toLowerCase,
        fields,
        partitionerFields,
        tableProperties,
        bucketFields)

      CreateTable(tableModel)
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
        s"Values must be specified for key(s): ${ badKeys.mkString("[", ",", "]") }", ctx)
    }
    props.map { case (key, value) =>
      (key.toLowerCase, value.toLowerCase)
    }
  }

}
