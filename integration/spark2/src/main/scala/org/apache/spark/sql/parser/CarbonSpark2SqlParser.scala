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

import org.apache.spark.sql.ShowLoadsCommand
import org.apache.spark.sql.catalyst.CarbonDDLSqlParser
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command._

/**
 * TODO remove the duplicate code and add the common methods to common class.
 * Parser for All Carbon DDL, DML cases in Unified context
 */
class CarbonSpark2SqlParser extends CarbonDDLSqlParser {

  override def parse(input: String): LogicalPlan = {
    synchronized {
      // Initialize the Keywords.
      initLexical
      phrase(start)(new lexical.Scanner(input)) match {
        case Success(plan, _) => plan match {
          case x: LoadTable =>
            x.inputSqlString = input
            x
          case logicalPlan => logicalPlan
        }
        case failureOrError =>
          sys.error(failureOrError.toString)
      }
    }
  }


  protected lazy val start: Parser[LogicalPlan] = explainPlan | startCommand

  protected lazy val startCommand: Parser[LogicalPlan] =
    loadManagement| showLoads | alterTable

  protected lazy val loadManagement: Parser[LogicalPlan] =
    deleteLoadsByID | deleteLoadsByLoadDate | cleanFiles | loadDataNew


  protected lazy val alterTable: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> (ident <~ ".").? ~ ident ~ (COMPACT ~ stringLit) <~ opt(";")  ^^ {
      case dbName ~ table ~ (compact ~ compactType) =>
        val altertablemodel =
          AlterTableModel(convertDbNameToLowerCase(dbName), table, None, compactType,
          Some(System.currentTimeMillis()), null)
        AlterTableCompaction(altertablemodel)
    }


  protected lazy val loadDataNew: Parser[LogicalPlan] =
    LOAD ~> DATA ~> opt(LOCAL) ~> INPATH ~> stringLit ~ opt(OVERWRITE) ~
    (INTO ~> TABLE ~> (ident <~ ".").? ~ ident) ~
    (OPTIONS ~> "(" ~> repsep(loadOptions, ",") <~ ")").? <~ opt(";") ^^ {
      case filePath ~ isOverwrite ~ table ~ optionsList =>
        val (databaseNameOp, tableName) = table match {
          case databaseName ~ tableName => (databaseName, tableName.toLowerCase())
        }
        if (optionsList.isDefined) {
          validateOptions(optionsList)
        }
        val optionsMap = optionsList.getOrElse(List.empty[(String, String)]).toMap
        LoadTable(convertDbNameToLowerCase(databaseNameOp), tableName, filePath, Seq(), optionsMap,
          isOverwrite.isDefined)
    }

  protected lazy val deleteLoadsByID: Parser[LogicalPlan] =
    DELETE ~> SEGMENT ~> repsep(segmentId, ",") ~ (FROM ~> TABLE ~>
                                                   (ident <~ ".").? ~ ident) <~
    opt(";") ^^ {
      case loadids ~ table => table match {
        case databaseName ~ tableName =>
          DeleteLoadsById(loadids, convertDbNameToLowerCase(databaseName), tableName.toLowerCase())
      }
    }

  protected lazy val deleteLoadsByLoadDate: Parser[LogicalPlan] =
    DELETE ~> SEGMENTS ~> FROM ~> TABLE ~> (ident <~ ".").? ~ ident ~
    (WHERE ~> (STARTTIME <~ BEFORE) ~ stringLit) <~
    opt(";") ^^ {
      case schema ~ table ~ condition =>
        condition match {
          case dateField ~ dateValue =>
            DeleteLoadsByLoadDate(convertDbNameToLowerCase(schema),
              table.toLowerCase(),
              dateField,
              dateValue)
        }
    }

  protected lazy val cleanFiles: Parser[LogicalPlan] =
    CLEAN ~> FILES ~> FOR ~> TABLE ~> (ident <~ ".").? ~ ident <~ opt(";") ^^ {
      case databaseName ~ tableName =>
        CleanFiles(convertDbNameToLowerCase(databaseName), tableName.toLowerCase())
    }

  protected lazy val explainPlan: Parser[LogicalPlan] =
    (EXPLAIN ~> opt(EXTENDED)) ~ startCommand ^^ {
      case isExtended ~ logicalPlan =>
        logicalPlan match {
          case plan: CreateTable => ExplainCommand(logicalPlan, extended = isExtended.isDefined)
          case _ => ExplainCommand(OneRowRelation)
        }
    }

  protected lazy val showLoads: Parser[LogicalPlan] =
    SHOW ~> SEGMENTS ~> FOR ~> TABLE ~> (ident <~ ".").? ~ ident ~
    (LIMIT ~> numericLit).? <~
    opt(";") ^^ {
      case databaseName ~ tableName ~ limit =>
        ShowLoadsCommand(convertDbNameToLowerCase(databaseName), tableName.toLowerCase(), limit)
    }
}
