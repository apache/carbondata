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

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.CarbonToSparkAdapter
import org.apache.spark.sql.catalyst.CarbonParserUtil
import org.apache.spark.sql.catalyst.parser.SqlBaseParser
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.{AddTableColumnsContext, CreateTableContext, HiveChangeColumnContext}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, QualifiedColType}
import org.apache.spark.sql.execution.SparkSqlAstBuilder
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsModel, AlterTableDataTypeChangeModel}
import org.apache.spark.sql.execution.command.schema.{CarbonAlterTableAddColumnCommand, CarbonAlterTableColRenameDataTypeChangeCommand}
import org.apache.spark.sql.execution.command.table.CarbonExplainCommand
import org.apache.spark.sql.parser.CarbonSpark2SqlParser
import org.apache.spark.sql.types.{DecimalType, StructField}

trait SqlAstBuilderHelper extends SparkSqlAstBuilder {

  override def visitHiveChangeColumn(ctx: HiveChangeColumnContext): LogicalPlan = {

    val newColumn = visitColType(ctx.colType)
    val isColumnRename = !typedVisit[Seq[String]](ctx.colName).head.equalsIgnoreCase(newColumn
      .name)

    val (typeString, values): (String, Option[List[(Int, Int)]]) = newColumn.dataType match {
      case d: DecimalType => ("decimal", Some(List((d.precision, d.scale))))
      case _ => (newColumn.dataType.typeName.toLowerCase, None)
    }

    val fullTableName = visitMultipartIdentifier(ctx.table)
    val alterTableColRenameAndDataTypeChangeModel =
      AlterTableDataTypeChangeModel(
        CarbonParserUtil.parseDataType(newColumn.name, typeString, values),
        CarbonParserUtil.convertDbNameToLowerCase(Option(fullTableName.head)),
        fullTableName(1).toLowerCase,
        typedVisit[Seq[String]](ctx.colName).head.toLowerCase,
        newColumn.name.toLowerCase,
        isColumnRename)

    CarbonAlterTableColRenameDataTypeChangeCommand(alterTableColRenameAndDataTypeChangeModel)
  }


  def visitAddTableColumns(parser: CarbonSpark2SqlParser,
      ctx: AddTableColumnsContext): LogicalPlan = {

    val col = ctx.columns.qualifiedColTypeWithPosition.asScala
        .map(typedVisit[QualifiedColType]).toSeq
    val fields = CarbonToSparkAdapter.getField(parser, col)
    val tblProperties = scala.collection.mutable.Map.empty[String, String]
    val fullTableName = visitMultipartIdentifier(ctx.multipartIdentifier)
    val tableModel = CarbonParserUtil.prepareTableModel(ifNotExistPresent = false,
      CarbonParserUtil.convertDbNameToLowerCase(Option(fullTableName.head)),
      fullTableName(1).toLowerCase,
      fields,
      Seq.empty,
      tblProperties,
      None,
      isAlterFlow = true)

    val alterTableAddColumnsModel = AlterTableAddColumnsModel(
      Option(fullTableName.head),
      fullTableName(1).toLowerCase,
      tblProperties.toMap,
      tableModel.dimCols,
      tableModel.msrCols,
      tableModel.highCardinalityDims.getOrElse(Seq.empty))

    CarbonAlterTableAddColumnCommand(alterTableAddColumnsModel)
  }

  override def visitCreateTable(ctx: CreateTableContext): LogicalPlan = {
    super.visitCreateTable(ctx)
  }

  override def visitExplain(ctx: SqlBaseParser.ExplainContext): LogicalPlan = {
    CarbonExplainCommand(super.visitExplain(ctx))
  }
}
