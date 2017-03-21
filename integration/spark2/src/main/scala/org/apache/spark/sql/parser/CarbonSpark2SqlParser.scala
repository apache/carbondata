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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.util.CommonUtil

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
    loadManagement| showLoads | alterTable | restructure

  protected lazy val loadManagement: Parser[LogicalPlan] =
    deleteLoadsByID | deleteLoadsByLoadDate | cleanFiles | loadDataNew

  protected lazy val restructure: Parser[LogicalPlan] =
    alterTableModifyDataType | alterTableDropColumn | alterTableAddColumns

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

  protected lazy val alterTableModifyDataType: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> (ident <~ ".").? ~ ident ~ CHANGE ~ ident ~ ident ~
    ident ~ opt("(" ~> rep1sep(valueOptions, ",") <~ ")") <~ opt(";") ^^ {
      case dbName ~ table ~ change ~ columnName ~ columnNameCopy ~ dataType ~ values =>
        // both the column names should be same
        if (!columnName.equalsIgnoreCase(columnNameCopy)) {
          throw new MalformedCarbonCommandException(
            "Column names provided are different. Both the column names should be same")
        }
        val alterTableChangeDataTypeModel =
          AlterTableDataTypeChangeModel(parseDataType(dataType.toLowerCase, values),
            convertDbNameToLowerCase(dbName),
            table.toLowerCase,
            columnName.toLowerCase,
            columnNameCopy.toLowerCase)
        AlterTableDataTypeChange(alterTableChangeDataTypeModel)
    }

  protected lazy val alterTableAddColumns: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> (ident <~ ".").? ~ ident ~
    (ADD ~> COLUMNS ~> "(" ~> repsep(anyFieldDef, ",") <~ ")") ~
    (TBLPROPERTIES ~> "(" ~> repsep(loadOptions, ",") <~ ")").? <~ opt(";") ^^ {
      case dbName ~ table ~ fields ~ tblProp =>
        fields.foreach{ f =>
          if (isComplexDimDictionaryExclude(f.dataType.get)) {
            throw new MalformedCarbonCommandException(
              s"Add column is unsupported for complex datatype column: ${f.column}")
          }
        }
        val tableProps = if (tblProp.isDefined) {
          tblProp.get.groupBy(_._1.toLowerCase).foreach(f =>
            if (f._2.size > 1) {
              val name = f._1.toLowerCase
              val colName = name.substring(14)
              if (name.startsWith("default.value.") &&
                  fields.filter(p => p.column.equalsIgnoreCase(colName)).size == 1) {
                LOGGER.error(s"Duplicate default value exist for new column: ${ colName }")
                LOGGER.audit(
                  s"Validation failed for Create/Alter Table Operation " +
                  s"for ${ table }. " +
                  s"Duplicate default value exist for new column: ${ colName }")
                sys.error(s"Duplicate default value exist for new column: ${ colName }")
              }
            }
          )
          // default value should not be converted to lower case
          val tblProps = tblProp.get
            .map(f => if (CarbonCommonConstants.TABLE_BLOCKSIZE.equalsIgnoreCase(f._1) ||
                          CarbonCommonConstants.NO_INVERTED_INDEX.equalsIgnoreCase(f._1) ||
                          CarbonCommonConstants.COLUMN_GROUPS.equalsIgnoreCase(f._1)) {
              throw new MalformedCarbonCommandException(
                s"Unsupported Table property in add column: ${ f._1 }")
            } else if (f._1.toLowerCase.startsWith("default.value.")) {
              f._1 -> f._2
            } else {
              f._1 -> f._2.toLowerCase
            })
          scala.collection.mutable.Map(tblProps: _*)
        } else {
          scala.collection.mutable.Map.empty[String, String]
        }

        val tableModel = prepareTableModel (false,
          convertDbNameToLowerCase(dbName),
          table.toLowerCase,
          fields.map(convertFieldNamesToLowercase),
          Seq.empty,
          tableProps,
          None,
          true)

        val alterTableAddColumnsModel = AlterTableAddColumnsModel(convertDbNameToLowerCase(dbName),
          table,
          tableProps,
          tableModel.dimCols,
          tableModel.msrCols,
          tableModel.highcardinalitydims.getOrElse(Seq.empty))
        AlterTableAddColumns(alterTableAddColumnsModel)
    }

  private def convertFieldNamesToLowercase(field: Field): Field = {
    val name = field.column.toLowerCase
    field.copy(column = name, name = Some(name))
  }
  protected lazy val alterTableDropColumn: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> (ident <~ ".").? ~ ident ~ DROP ~ COLUMNS ~
    ("(" ~> rep1sep(ident, ",") <~ ")") <~ opt(";") ^^ {
      case dbName ~ table ~ drop ~ columns ~ values =>
        // validate that same column name is not repeated
        values.map(_.toLowerCase).groupBy(identity).collect {
          case (x, ys) if ys.lengthCompare(1) > 0 =>
            throw new MalformedCarbonCommandException(s"$x is duplicate. Duplicate columns not " +
                                                      s"allowed")
        }
        val alterTableDropColumnModel = AlterTableDropColumnModel(convertDbNameToLowerCase(dbName),
          table.toLowerCase,
          values.map(_.toLowerCase))
        AlterTableDropColumns(alterTableDropColumnModel)
    }
}
