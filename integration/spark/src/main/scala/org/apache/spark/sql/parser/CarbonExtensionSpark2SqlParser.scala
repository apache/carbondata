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

import scala.language.implicitConversions

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.CarbonParserUtil
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.management.CarbonLoadDataCommand
import org.apache.spark.sql.execution.command.schema.{CarbonAlterTableAddColumnCommand, CarbonAlterTableColRenameDataTypeChangeCommand, CarbonAlterTableDropColumnCommand}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants

/**
 * Parser for All Carbon DDL, DML cases in Unified context
 */
class CarbonExtensionSpark2SqlParser extends CarbonSpark2SqlParser {

  override protected lazy val extendedSparkSyntax: Parser[LogicalPlan] =
    loadDataNew | alterTableAddColumns

  /**
   * alter table add columns with TBLPROPERTIES
   */
  override protected lazy val alterTableAddColumns: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> (ident <~ ".").? ~ ident ~
    (ADD ~> COLUMNS ~> "(" ~> repsep(anyFieldDef, ",") <~ ")") ~
    (TBLPROPERTIES ~> "(" ~> repsep(options, ",") <~ ")") <~ opt(";") ^^ {
      case dbName ~ table ~ fields ~ tblProp =>
        CarbonSparkSqlParserUtil.alterTableAddColumns(
          dbName, table, fields, Option(tblProp))
    }

  /**
   * load data with OPTIONS
   */
  override protected lazy val loadDataNew: Parser[LogicalPlan] =
    LOAD ~> DATA ~> opt(LOCAL) ~> INPATH ~> stringLit ~ opt(OVERWRITE) ~
    (INTO ~> TABLE ~> (ident <~ ".").? ~ ident) ~
    (PARTITION ~> "(" ~> repsep(partitions, ",") <~ ")").? ~
    (OPTIONS ~> "(" ~> repsep(options, ",") <~ ")") <~ opt(";") ^^ {
      case filePath ~ isOverwrite ~ table ~ partitions ~ optionsList =>
        val (databaseNameOp, tableName) = table match {
          case databaseName ~ tableName => (databaseName, tableName.toLowerCase())
        }
        CarbonSparkSqlParserUtil.loadDataNew(
          databaseNameOp, tableName, Option(optionsList), partitions, filePath, isOverwrite)
    }
}
