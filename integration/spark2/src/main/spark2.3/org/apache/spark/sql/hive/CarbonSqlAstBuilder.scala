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

package org.apache.spark.sql.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.ParserUtils.{string, withOrigin}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.{AddTableColumnsContext, ChangeColumnContext, CreateHiveTableContext, CreateTableContext, ShowTablesContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlAstBuilder
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsModel, AlterTableDataTypeChangeModel}
import org.apache.spark.sql.execution.command.schema.{CarbonAlterTableAddColumnCommand, CarbonAlterTableDataTypeChangeCommand}
import org.apache.spark.sql.execution.command.table.{CarbonExplainCommand, CarbonShowTablesCommand}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.parser.{CarbonHelperSqlAstBuilder, CarbonSpark2SqlParser}
import org.apache.spark.sql.types.DecimalType

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class CarbonSqlAstBuilder(conf: SQLConf, parser: CarbonSpark2SqlParser, sparkSession: SparkSession)
  extends SparkSqlAstBuilder(conf) {

  val helper = new CarbonHelperSqlAstBuilder(conf, parser, sparkSession)

  override def visitCreateHiveTable(ctx: CreateHiveTableContext): LogicalPlan = {
    val fileStorage = helper.getFileStorage(ctx.createFileFormat(0))

    if (fileStorage.equalsIgnoreCase("'carbondata'") ||
        fileStorage.equalsIgnoreCase("carbondata") ||
        fileStorage.equalsIgnoreCase("'carbonfile'") ||
        fileStorage.equalsIgnoreCase("'org.apache.carbondata.format'")) {
      val createTableTuple = (ctx.createTableHeader, ctx.skewSpec(0),
        ctx.bucketSpec(0), ctx.partitionColumns, ctx.columns, ctx.tablePropertyList(0),ctx.locationSpec(0),
        Option(ctx.STRING(0)).map(string), ctx.AS, ctx.query, fileStorage)
      helper.createCarbonTable(createTableTuple)
    } else {
      super.visitCreateHiveTable(ctx)
    }
  }

  override def visitChangeColumn(ctx: ChangeColumnContext): LogicalPlan = {

    val newColumn = visitColType(ctx.colType)
    if (!ctx.identifier.getText.equalsIgnoreCase(newColumn.name)) {
      throw new MalformedCarbonCommandException(
        "Column names provided are different. Both the column names should be same")
    }

    val (typeString, values) : (String, Option[List[(Int, Int)]]) = newColumn.dataType match {
      case d:DecimalType => ("decimal", Some(List((d.precision, d.scale))))
      case _ => (newColumn.dataType.typeName.toLowerCase, None)
    }

    val alterTableChangeDataTypeModel =
      AlterTableDataTypeChangeModel(new CarbonSpark2SqlParser().parseDataType(typeString, values),
        new CarbonSpark2SqlParser()
          .convertDbNameToLowerCase(Option(ctx.tableIdentifier().db).map(_.getText)),
        ctx.tableIdentifier().table.getText.toLowerCase,
        ctx.identifier.getText.toLowerCase,
        newColumn.name.toLowerCase)

    CarbonAlterTableDataTypeChangeCommand(alterTableChangeDataTypeModel)
  }


  override def visitAddTableColumns(ctx: AddTableColumnsContext): LogicalPlan = {
    val cols = Option(ctx.columns).toSeq.flatMap(visitColTypeList)
    val fields = parser.getFields(cols)
    val tblProperties = scala.collection.mutable.Map.empty[String, String]
    val tableModel = new CarbonSpark2SqlParser().prepareTableModel (false,
      new CarbonSpark2SqlParser().convertDbNameToLowerCase(Option(ctx.tableIdentifier().db)
        .map(_.getText)),
      ctx.tableIdentifier.table.getText.toLowerCase,
      fields,
      Seq.empty,
      tblProperties,
      None,
      true)

    val alterTableAddColumnsModel = AlterTableAddColumnsModel(
      Option(ctx.tableIdentifier().db).map(_.getText),
      ctx.tableIdentifier.table.getText,
      tblProperties.toMap,
      tableModel.dimCols,
      tableModel.msrCols,
      tableModel.highcardinalitydims.getOrElse(Seq.empty))

    CarbonAlterTableAddColumnCommand(alterTableAddColumnsModel)
  }

  override def visitCreateTable(ctx: CreateTableContext): LogicalPlan = {
    super.visitCreateTable(ctx)
  }

  override def visitShowTables(ctx: ShowTablesContext): LogicalPlan = {
    withOrigin(ctx) {
      if (CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_SHOW_DATAMAPS,
          CarbonCommonConstants.CARBON_SHOW_DATAMAPS_DEFAULT).toBoolean) {
        super.visitShowTables(ctx)
      } else {
        CarbonShowTablesCommand(
          Option(ctx.db).map(_.getText),
          Option(ctx.pattern).map(string))
      }
    }
  }

  override def visitExplain(ctx: SqlBaseParser.ExplainContext): LogicalPlan = {
    CarbonExplainCommand(super.visitExplain(ctx))
  }
}
