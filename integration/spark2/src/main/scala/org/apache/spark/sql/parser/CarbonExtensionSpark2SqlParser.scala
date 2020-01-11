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
 * TODO remove the duplicate code and add the common methods to common class.
 * Parser for All Carbon DDL, DML cases in Unified context
 */
class CarbonExtensionSpark2SqlParser extends CarbonSpark2SqlParser {

  override protected lazy val extendedSparkSyntax: Parser[LogicalPlan] =
    loadDataNew | alterTableColumnRenameAndModifyDataType | alterTableAddColumns

  /**
   * alter table change column with options
   */
  override protected lazy val alterTableColumnRenameAndModifyDataType: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> (ident <~ ".").? ~ ident ~ CHANGE ~ ident ~ ident ~
    ident ~ ("(" ~> rep1sep(valueOptions, ",") <~ ")") <~ opt(";") ^^ {
      case dbName ~ table ~ change ~ columnName ~ columnNameCopy ~ dataType ~ values =>
        var isColumnRename = false
        // If both the column name are not same, then its a call for column rename
        if (!columnName.equalsIgnoreCase(columnNameCopy)) {
          isColumnRename = true
        }
        val alterTableColRenameAndDataTypeChangeModel =
          AlterTableDataTypeChangeModel(CarbonParserUtil.parseDataType(dataType.toLowerCase,
            Option(values),
            isColumnRename),
            CarbonParserUtil.convertDbNameToLowerCase(dbName),
            table.toLowerCase,
            columnName.toLowerCase,
            columnNameCopy.toLowerCase,
            isColumnRename)
        CarbonAlterTableColRenameDataTypeChangeCommand(alterTableColRenameAndDataTypeChangeModel)
    }

  /**
   * alter table add columns with TBLPROPERTIES
   */
  override protected lazy val alterTableAddColumns: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> (ident <~ ".").? ~ ident ~
    (ADD ~> COLUMNS ~> "(" ~> repsep(anyFieldDef, ",") <~ ")") ~
    (TBLPROPERTIES ~> "(" ~> repsep(loadOptions, ",") <~ ")") <~ opt(";") ^^ {
      case dbName ~ table ~ fields ~ tblProp =>
        fields.foreach{ f =>
          if (CarbonParserUtil.isComplexType(f.dataType.get)) {
            throw new MalformedCarbonCommandException(
              s"Add column is unsupported for complex datatype column: ${f.column}")
          }
        }
        val tableProps = if (!tblProp.isEmpty) {
          tblProp.groupBy(_._1.toLowerCase).foreach(f =>
            if (f._2.size > 1) {
              val name = f._1.toLowerCase
              val colName = name.substring(14)
              if (name.startsWith("default.value.") &&
                  fields.count(p => p.column.equalsIgnoreCase(colName)) == 1) {
                sys.error(s"Duplicate default value exist for new column: ${ colName }")
              }
            }
          )
          // default value should not be converted to lower case
          val tblProps = tblProp
            .map(f => if (CarbonCommonConstants.TABLE_BLOCKSIZE.equalsIgnoreCase(f._1) ||
                          CarbonCommonConstants.SORT_COLUMNS.equalsIgnoreCase(f._1) ||
                          CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE.equalsIgnoreCase(f._1) ||
                          CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD.equalsIgnoreCase(f._1)) {
              throw new MalformedCarbonCommandException(
                s"Unsupported Table property in add column: ${ f._1 }")
            } else if (f._1.toLowerCase.startsWith("default.value.")) {
              if (fields.count(field => CarbonParserUtil.checkFieldDefaultValue(field.column,
                f._1.toLowerCase)) == 1) {
                f._1 -> f._2
              } else {
                throw new MalformedCarbonCommandException(
                  s"Default.value property does not matches with the columns in ALTER command. " +
                  s"Column name in property is: ${ f._1}")
              }
            } else {
              f._1 -> f._2.toLowerCase
            })
          scala.collection.mutable.Map(tblProps: _*)
        } else {
          scala.collection.mutable.Map.empty[String, String]
        }

        val tableModel = CarbonParserUtil.prepareTableModel (false,
          CarbonParserUtil.convertDbNameToLowerCase(dbName),
          table.toLowerCase,
          fields.map(CarbonParserUtil.convertFieldNamesToLowercase),
          Seq.empty,
          tableProps,
          None,
          true)

        val alterTableAddColumnsModel = AlterTableAddColumnsModel(
          CarbonParserUtil.convertDbNameToLowerCase(dbName),
          table,
          tableProps.toMap,
          tableModel.dimCols,
          tableModel.msrCols,
          tableModel.highcardinalitydims.getOrElse(Seq.empty))
        CarbonAlterTableAddColumnCommand(alterTableAddColumnsModel)
    }

  /**
   * load data with OPTIONS
   */
  override protected lazy val loadDataNew: Parser[LogicalPlan] =
    LOAD ~> DATA ~> opt(LOCAL) ~> INPATH ~> stringLit ~ opt(OVERWRITE) ~
    (INTO ~> TABLE ~> (ident <~ ".").? ~ ident) ~
    (PARTITION ~>"("~> repsep(partitions, ",") <~ ")").? ~
    (OPTIONS ~> "(" ~> repsep(loadOptions, ",") <~ ")") <~ opt(";") ^^ {
      case filePath ~ isOverwrite ~ table ~ partitions ~ optionsList =>
        val (databaseNameOp, tableName) = table match {
          case databaseName ~ tableName => (databaseName, tableName.toLowerCase())
        }
        CarbonParserUtil.validateOptions(Some(optionsList))
        val optionsMap = optionsList.toMap
        val partitionSpec = partitions.getOrElse(List.empty[(String, Option[String])]).toMap
        CarbonLoadDataCommand(
          databaseNameOp = CarbonParserUtil.convertDbNameToLowerCase(databaseNameOp),
          tableName = tableName,
          factPathFromUser = filePath,
          dimFilesPath = Seq(),
          options = optionsMap,
          isOverwriteTable = isOverwrite.isDefined,
          inputSqlString = null,
          dataFrame = None,
          updateModel = None,
          tableInfoOp = None,
          internalOptions = Map.empty,
          partition = partitionSpec)
    }
}
