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
import scala.language.implicitConversions

import org.apache.spark.sql.{DeleteRecords, ShowLoadsCommand, UpdateTable}
import org.apache.spark.sql.catalyst.CarbonDDLSqlParser
import org.apache.spark.sql.catalyst.CarbonTableIdentifierImplicit._
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.management.{AlterTableCompactionCommand, CleanFilesCommand, DeleteLoadByIdCommand, DeleteLoadByLoadDateCommand, LoadTableCommand}
import org.apache.spark.sql.execution.command.partition.{AlterTableDropCarbonPartitionCommand, AlterTableSplitCarbonPartitionCommand}
import org.apache.spark.sql.execution.command.schema.{AlterTableAddColumnCommand, AlterTableDataTypeChangeCommand, AlterTableDropColumnCommand}
import org.apache.spark.sql.types.StructField

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.spark.CarbonOption
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
          case x: LoadTableCommand =>
            x.inputSqlString = input
            x
          case x: AlterTableCompactionCommand =>
            x.alterTableModel.alterSql = input
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
    loadManagement|showLoads|alterTable|restructure|updateTable|deleteRecords|alterPartition

  protected lazy val loadManagement: Parser[LogicalPlan] =
    deleteLoadsByID | deleteLoadsByLoadDate | cleanFiles | loadDataNew

  protected lazy val restructure: Parser[LogicalPlan] =
    alterTableModifyDataType | alterTableDropColumn | alterTableAddColumns

  protected lazy val alterPartition: Parser[LogicalPlan] =
    alterAddPartition | alterSplitPartition | alterDropPartition

  protected lazy val alterAddPartition: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> (ident <~ ".").? ~ ident ~ (ADD ~> PARTITION ~>
      "(" ~> repsep(stringLit, ",") <~ ")") <~ opt(";") ^^ {
      case dbName ~ table ~ addInfo =>
        val alterTableAddPartitionModel =
          AlterTableSplitPartitionModel(dbName, table, "0", addInfo)
        AlterTableSplitCarbonPartitionCommand(alterTableAddPartitionModel)
    }

  protected lazy val alterSplitPartition: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> (ident <~ ".").? ~ ident ~ (SPLIT ~> PARTITION ~>
       "(" ~> numericLit <~ ")") ~ (INTO ~> "(" ~> repsep(stringLit, ",") <~ ")") <~ opt(";") ^^ {
      case dbName ~ table ~ partitionId ~ splitInfo =>
        val alterTableSplitPartitionModel =
          AlterTableSplitPartitionModel(dbName, table, partitionId, splitInfo)
        if (partitionId == 0) {
          sys.error("Please use [Alter Table Add Partition] statement to split default partition!")
        }
        AlterTableSplitCarbonPartitionCommand(alterTableSplitPartitionModel)
    }

  protected lazy val alterDropPartition: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> (ident <~ ".").? ~ ident ~ (DROP ~> PARTITION ~>
      "(" ~> numericLit <~ ")") ~ (WITH ~> DATA).? <~ opt(";") ^^ {
      case dbName ~ table ~ partitionId ~ withData =>
        val dropWithData = withData.getOrElse("NO") match {
          case "NO" => false
          case _ => true
        }
        val alterTableDropPartitionModel =
          AlterTableDropPartitionModel(dbName, table, partitionId, dropWithData)
        AlterTableDropCarbonPartitionCommand(alterTableDropPartitionModel)
    }


  protected lazy val alterTable: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> (ident <~ ".").? ~ ident ~ (COMPACT ~ stringLit) <~ opt(";")  ^^ {
      case dbName ~ table ~ (compact ~ compactType) =>
        val altertablemodel =
          AlterTableModel(convertDbNameToLowerCase(dbName), table, None, compactType,
          Some(System.currentTimeMillis()), null)
        AlterTableCompactionCommand(altertablemodel)
    }

  protected lazy val deleteRecords: Parser[LogicalPlan] =
    (DELETE ~> FROM ~> table) ~ restInput.? <~ opt(";") ^^ {
      case table ~ rest =>
        val tableName = getTableName(table.tableIdentifier)
        val alias = table.alias.getOrElse("")
        DeleteRecords("select tupleId from " + tableName + " " + alias + rest.getOrElse(""), table)
    }

  protected lazy val updateTable: Parser[LogicalPlan] =
    UPDATE ~> table ~
    (SET ~> "(" ~> repsep(element, ",") <~ ")") ~
    ("=" ~> restInput) <~ opt(";") ^^ {
      case tab ~ columns ~ rest =>
        val (sel, where) = splitQuery(rest)
        val (selectStmt, relation) =
          if (!sel.toLowerCase.startsWith("select ")) {
            if (sel.trim.isEmpty) {
              sys.error("At least one source column has to be specified ")
            }
            // only list of expression are given, need to convert that list of expressions into
            // select statement on destination table
            val relation = tab match {
              case r@UnresolvedRelation(tableIdentifier, alias) =>
                updateRelation(r, tableIdentifier, alias)
              case _ => tab
            }
            ("select " + sel + " from " + getTableName(relation.tableIdentifier) + " " +
             relation.alias.get, relation)
          } else {
            (sel, updateRelation(tab, tab.tableIdentifier, tab.alias))
          }
        UpdateTable(relation, columns, selectStmt, where)
    }

  private def updateRelation(
      r: UnresolvedRelation,
      tableIdentifier: Seq[String],
      alias: Option[String]): UnresolvedRelation = {
    alias match {
      case Some(_) => r
      case _ =>
        val tableAlias = tableIdentifier match {
          case Seq(dbName, tableName) => Some(tableName)
          case Seq(tableName) => Some(tableName)
        }
        UnresolvedRelation(tableIdentifier, tableAlias)
    }
  }

  protected lazy val element: Parser[String] =
    (ident <~ ".").? ~ ident ^^ {
      case table ~ column => column.toLowerCase
    }

  protected lazy val table: Parser[UnresolvedRelation] = {
    rep1sep(attributeName, ".") ~ opt(ident) ^^ {
      case tableIdent ~ alias => UnresolvedRelation(tableIdent, alias)
    }
  }

  private def splitQuery(query: String): (String, String) = {
    val stack = scala.collection.mutable.Stack[Char]()
    var foundSingleQuotes = false
    var foundDoubleQuotes = false
    var foundEscapeChar = false
    var ignoreChar = false
    var stop = false
    var bracketCount = 0
    val (selectStatement, where) = query.span {
      ch => {
        if (stop) {
          false
        } else {
          ignoreChar = false
          if (foundEscapeChar && (ch == '\'' || ch == '\"' || ch == '\\')) {
            foundEscapeChar = false
            ignoreChar = true
          }
          // If escaped single or double quotes found, no need to consider
          if (!ignoreChar) {
            if (ch == '\\') {
              foundEscapeChar = true
            } else if (ch == '\'') {
              foundSingleQuotes = !foundSingleQuotes
            } else if (ch == '\"') {
              foundDoubleQuotes = !foundDoubleQuotes
            }
            else if (ch == '(' && !foundSingleQuotes && !foundDoubleQuotes) {
              bracketCount = bracketCount + 1
              stack.push(ch)
            } else if (ch == ')' && !foundSingleQuotes && !foundDoubleQuotes) {
              bracketCount = bracketCount + 1
              stack.pop()
              if (0 == stack.size) {
                stop = true
              }
            }
          }
          true
        }
      }
    }
    if (bracketCount == 0 || bracketCount % 2 != 0) {
      sys.error("Parsing error, missing bracket ")
    }
    val select = selectStatement.trim
    (select.substring(1, select.length - 1).trim -> where.trim)
  }

  protected lazy val attributeName: Parser[String] = acceptMatch("attribute name", {
    case lexical.Identifier(str) => str.toLowerCase
    case lexical.Keyword(str) if !lexical.delimiters.contains(str) => str.toLowerCase
  })

  private def getTableName(tableIdentifier: Seq[String]): String = {
    if (tableIdentifier.size > 1) {
      tableIdentifier(0) + "." + tableIdentifier(1)
    } else {
      tableIdentifier(0)
    }
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
        LoadTableCommand(
          convertDbNameToLowerCase(databaseNameOp),
          tableName,
          filePath,
          Seq(),
          optionsMap,
          isOverwrite.isDefined)
    }

  protected lazy val deleteLoadsByID: Parser[LogicalPlan] =
    DELETE ~> FROM ~ TABLE ~> (ident <~ ".").? ~ ident ~
    (WHERE ~> (SEGMENT ~ "." ~ ID) ~> IN ~> "(" ~> repsep(segmentId, ",")) <~ ")" ~
    opt(";") ^^ {
      case dbName ~ tableName ~ loadids =>
        DeleteLoadByIdCommand(loadids, dbName, tableName.toLowerCase())
    }

  protected lazy val deleteLoadsByLoadDate: Parser[LogicalPlan] =
    DELETE ~> FROM ~> TABLE ~> (ident <~ ".").? ~ ident ~
    (WHERE ~> (SEGMENT ~ "." ~ STARTTIME ~> BEFORE) ~ stringLit) <~
    opt(";") ^^ {
      case database ~ table ~ condition =>
        condition match {
          case dateField ~ dateValue =>
            DeleteLoadByLoadDateCommand(convertDbNameToLowerCase(database),
              table.toLowerCase(),
              dateField,
              dateValue)
        }
    }

  protected lazy val cleanFiles: Parser[LogicalPlan] =
    CLEAN ~> FILES ~> FOR ~> TABLE ~> (ident <~ ".").? ~ ident <~ opt(";") ^^ {
      case databaseName ~ tableName =>
        CleanFilesCommand(convertDbNameToLowerCase(databaseName), tableName.toLowerCase())
    }

  protected lazy val explainPlan: Parser[LogicalPlan] =
    (EXPLAIN ~> opt(EXTENDED)) ~ startCommand ^^ {
      case isExtended ~ logicalPlan =>
        logicalPlan match {
          case _: CarbonCreateTableCommand =>
            ExplainCommand(logicalPlan, extended = isExtended.isDefined)
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
        AlterTableDataTypeChangeCommand(alterTableChangeDataTypeModel)
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
                          CarbonCommonConstants.COLUMN_GROUPS.equalsIgnoreCase(f._1) ||
            CarbonCommonConstants.SORT_COLUMNS.equalsIgnoreCase(f._1)) {
              throw new MalformedCarbonCommandException(
                s"Unsupported Table property in add column: ${ f._1 }")
            } else if (f._1.toLowerCase.startsWith("default.value.")) {
              if (fields.count(field => checkFieldDefaultValue(field.column,
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
        AlterTableAddColumnCommand(alterTableAddColumnsModel)
    }

  private def checkFieldDefaultValue(fieldName: String, defaultValueColumnName: String): Boolean = {
    defaultValueColumnName.equalsIgnoreCase("default.value." + fieldName)
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
        AlterTableDropColumnCommand(alterTableDropColumnModel)
    }

  def getFields(schema: Seq[StructField]): Seq[Field] = {
    schema.map { col =>
      val x = if (col.dataType.catalogString == "float") {
        '`' + col.name + '`' + " double"
      }
      else {
        '`' + col.name + '`' + ' ' + col.dataType.catalogString
      }
      val f: Field = anyFieldDef(new lexical.Scanner(x.toLowerCase))
      match {
        case Success(field, _) => field.asInstanceOf[Field]
        case failureOrError => throw new MalformedCarbonCommandException(
          s"Unsupported data type: $col.getType")
      }
      // the data type of the decimal type will be like decimal(10,0)
      // so checking the start of the string and taking the precision and scale.
      // resetting the data type with decimal
      if (f.dataType.getOrElse("").startsWith("decimal")) {
        val (precision, scale) = CommonUtil.getScaleAndPrecision(col.dataType.catalogString)
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
  }

  def getBucketFields(properties: mutable.Map[String, String],
      fields: Seq[Field],
      options: CarbonOption): Option[BucketFields] = {
    if (!CommonUtil.validateTblProperties(properties,
      fields)) {
      throw new MalformedCarbonCommandException("Invalid table properties")
    }
    if (options.isBucketingEnabled) {
      if (options.bucketNumber.toString.contains("-") ||
          options.bucketNumber.toString.contains("+") ||  options.bucketNumber == 0) {
        throw new MalformedCarbonCommandException("INVALID NUMBER OF BUCKETS SPECIFIED")
      }
      else {
        Some(BucketFields(options.bucketColumns.toLowerCase.split(",").map(_.trim),
          options.bucketNumber))
      }
    } else {
      None
    }
  }
}
