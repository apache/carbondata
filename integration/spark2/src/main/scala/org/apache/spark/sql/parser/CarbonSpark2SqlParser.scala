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

import org.apache.spark.sql.{CarbonToSparkAdapter, DeleteRecords, UpdateTable}
import org.apache.spark.sql.catalyst.{CarbonDDLSqlParser, TableIdentifier}
import org.apache.spark.sql.catalyst.CarbonTableIdentifierImplicit._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.datamap.{CarbonCreateDataMapCommand, CarbonDataMapRebuildCommand, CarbonDataMapShowCommand, CarbonDropDataMapCommand}
import org.apache.spark.sql.execution.command.management._
import org.apache.spark.sql.execution.command.partition.{CarbonAlterTableDropPartitionCommand, CarbonAlterTableSplitPartitionCommand}
import org.apache.spark.sql.execution.command.schema.{CarbonAlterTableAddColumnCommand, CarbonAlterTableColRenameDataTypeChangeCommand, CarbonAlterTableDropColumnCommand}
import org.apache.spark.sql.execution.command.table.CarbonCreateTableCommand
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.CarbonExpressions.CarbonUnresolvedRelation
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.execution.command.cache.{CarbonDropCacheCommand, CarbonShowCacheCommand}
import org.apache.spark.sql.execution.command.stream.{CarbonCreateStreamCommand, CarbonDropStreamCommand, CarbonShowStreamsCommand}
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.util.CarbonReflectionUtils

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.spark.CarbonOption
import org.apache.carbondata.spark.util.{CarbonScalaUtil, CommonUtil}

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
        case Success(plan, _) =>
          CarbonScalaUtil.cleanParserThreadLocals()
          plan match {
            case x: CarbonLoadDataCommand =>
              x.inputSqlString = input
              x
            case x: CarbonAlterTableCompactionCommand =>
              x.alterTableModel.alterSql = input
              x
            case logicalPlan => logicalPlan
        }
        case failureOrError =>
          CarbonScalaUtil.cleanParserThreadLocals()
          CarbonException.analysisException(failureOrError.toString)
      }
    }
  }


  protected lazy val start: Parser[LogicalPlan] = explainPlan | startCommand

  protected lazy val startCommand: Parser[LogicalPlan] =
    loadManagement | showLoads | alterTable | restructure | updateTable | deleteRecords |
    alterPartition | datamapManagement | alterTableFinishStreaming | stream | cli |
    cacheManagement | alterDataMap

  protected lazy val loadManagement: Parser[LogicalPlan] =
    deleteLoadsByID | deleteLoadsByLoadDate | cleanFiles | loadDataNew

  protected lazy val restructure: Parser[LogicalPlan] =
    alterTableColumnRenameAndModifyDataType | alterTableDropColumn | alterTableAddColumns

  protected lazy val alterPartition: Parser[LogicalPlan] =
    alterAddPartition | alterSplitPartition | alterDropPartition

  protected lazy val datamapManagement: Parser[LogicalPlan] =
    createDataMap | dropDataMap | showDataMap | refreshDataMap

  protected lazy val stream: Parser[LogicalPlan] =
    createStream | dropStream | showStreams

  protected lazy val cacheManagement: Parser[LogicalPlan] =
    showCache | dropCache

  protected lazy val alterAddPartition: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> (ident <~ ".").? ~ ident ~ (ADD ~> PARTITION ~>
      "(" ~> repsep(stringLit, ",") <~ ")") <~ opt(";") ^^ {
      case dbName ~ table ~ addInfo =>
        val alterTableAddPartitionModel = AlterTableSplitPartitionModel(dbName, table, "0", addInfo)
        CarbonAlterTableSplitPartitionCommand(alterTableAddPartitionModel)
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
        CarbonAlterTableSplitPartitionCommand(alterTableSplitPartitionModel)
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
        CarbonAlterTableDropPartitionCommand(alterTableDropPartitionModel)
    }


  protected lazy val alterTable: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> (ident <~ ".").? ~ ident ~ (COMPACT ~ stringLit) ~
      (WHERE ~> (SEGMENT ~ "." ~ ID) ~> IN ~> "(" ~> repsep(segmentId, ",") <~ ")").? <~
      opt(";") ^^ {
      case dbName ~ table ~ (compact ~ compactType) ~ segs =>
        val altertablemodel =
          AlterTableModel(convertDbNameToLowerCase(dbName), table, None, compactType,
          Some(System.currentTimeMillis()), null, segs)
        CarbonAlterTableCompactionCommand(altertablemodel)
    }

  /**
   * The below syntax is used to change the status of the segment
   * from "streaming" to "streaming finish".
   * ALTER TABLE tableName FINISH STREAMING
   */
  protected lazy val alterTableFinishStreaming: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> (ident <~ ".").? ~ ident <~ FINISH <~ STREAMING <~ opt(";") ^^ {
      case dbName ~ table =>
        CarbonAlterTableFinishStreaming(dbName, table)
    }

  /**
   * The syntax of CREATE STREAM
   * CREATE STREAM [IF NOT EXISTS] streamName ON TABLE [dbName.]tableName
   * [STMPROPERTIES('KEY'='VALUE')]
   * AS SELECT COUNT(COL1) FROM tableName
   */
  protected lazy val createStream: Parser[LogicalPlan] =
    CREATE ~> STREAM ~>  opt(IF ~> NOT ~> EXISTS) ~ ident ~
    (ON ~> TABLE ~> (ident <~ ".").?) ~ ident ~
    (STMPROPERTIES ~> "(" ~> repsep(loadOptions, ",") <~ ")").? ~
    (AS ~> restInput) <~ opt(";") ^^ {
      case ifNotExists ~ streamName ~ dbName ~ tableName ~ options ~ query =>
        val optionMap = options.getOrElse(List[(String, String)]()).toMap[String, String]
        CarbonCreateStreamCommand(
          streamName, dbName, tableName, ifNotExists.isDefined, optionMap, query)
    }

  /**
   * The syntax of DROP STREAM
   * DROP STREAM [IF EXISTS] streamName
   */
  protected lazy val dropStream: Parser[LogicalPlan] =
    DROP ~> STREAM ~> opt(IF ~> EXISTS) ~ ident <~ opt(";") ^^ {
      case ifExists ~ streamName =>
        CarbonDropStreamCommand(streamName, ifExists.isDefined)
    }

  /**
   * The syntax of SHOW STREAMS
   * SHOW STREAMS [ON TABLE dbName.tableName]
   */
  protected lazy val showStreams: Parser[LogicalPlan] =
    SHOW ~> STREAMS ~> opt(ontable) <~ opt(";") ^^ {
      case tableIdent =>
        CarbonShowStreamsCommand(tableIdent)
    }

  /**
   * The syntax of datamap creation is as follows.
   * CREATE DATAMAP IF NOT EXISTS datamapName [ON TABLE tableName]
   * USING 'DataMapProviderName'
   * [WITH DEFERRED REBUILD]
   * DMPROPERTIES('KEY'='VALUE') AS SELECT COUNT(COL1) FROM tableName
   */
  protected lazy val createDataMap: Parser[LogicalPlan] =
    CREATE ~> DATAMAP ~> opt(IF ~> NOT ~> EXISTS) ~ ident ~
    opt(ontable) ~
    (USING ~> stringLit) ~
    opt(WITH ~> DEFERRED ~> REBUILD) ~
    (DMPROPERTIES ~> "(" ~> repsep(loadOptions, ",") <~ ")").? ~
    (AS ~> restInput).? <~ opt(";") ^^ {
      case ifnotexists ~ dmname ~ tableIdent ~ dmProviderName ~ deferred ~ dmprops ~ query =>
        val map = dmprops.getOrElse(List[(String, String)]()).toMap[String, String]
        CarbonCreateDataMapCommand(dmname, tableIdent, dmProviderName, map, query,
          ifnotexists.isDefined, deferred.isDefined)
    }

  protected lazy val ontable: Parser[TableIdentifier] =
    ON ~> TABLE ~>  (ident <~ ".").? ~ ident ^^ {
      case dbName ~ tableName =>
        TableIdentifier(tableName, dbName)
    }

  /**
   * The below syntax is used to drop the datamap.
   * DROP DATAMAP IF EXISTS datamapName ON TABLE tablename
   */
  protected lazy val dropDataMap: Parser[LogicalPlan] =
    DROP ~> DATAMAP ~> opt(IF ~> EXISTS) ~ ident ~ opt(ontable) <~ opt(";")  ^^ {
      case ifexists ~ dmname ~ tableIdent =>
        CarbonDropDataMapCommand(dmname, ifexists.isDefined, tableIdent)
    }

  /**
   * The syntax of show datamap is used to show datamaps on the table
   * SHOW DATAMAP ON TABLE tableName
   */
  protected lazy val showDataMap: Parser[LogicalPlan] =
    SHOW ~> DATAMAP ~> opt(ontable) <~ opt(";") ^^ {
      case tableIdent =>
        CarbonDataMapShowCommand(tableIdent)
    }

  /**
   * The syntax of show datamap is used to show datamaps on the table
   * REBUILD DATAMAP datamapname [ON TABLE] tableName
   */
  protected lazy val refreshDataMap: Parser[LogicalPlan] =
    REBUILD ~> DATAMAP ~> ident ~ opt(ontable) <~ opt(";") ^^ {
      case datamap ~ tableIdent =>
        CarbonDataMapRebuildCommand(datamap, tableIdent)
    }

  protected lazy val alterDataMap: Parser[LogicalPlan] =
    ALTER ~> DATAMAP ~> (ident <~ ".").? ~ ident ~ (COMPACT ~ stringLit) ~
    (WHERE ~> (SEGMENT ~ "." ~ ID) ~> IN ~> "(" ~> repsep(segmentId, ",") <~ ")").? <~
    opt(";") ^^ {
      case dbName ~ datamap ~ (compact ~ compactType) ~ segs =>
        val altertablemodel =
          AlterTableModel(convertDbNameToLowerCase(dbName), datamap + "_table", None, compactType,
            Some(System.currentTimeMillis()), null, segs)
        CarbonAlterTableCompactionCommand(altertablemodel)
    }

  protected lazy val deleteRecords: Parser[LogicalPlan] =
    (DELETE ~> FROM ~> aliasTable) ~ restInput.? <~ opt(";") ^^ {
      case table ~ rest =>
        val tableName = getTableName(table._2)
        val relation: LogicalPlan = table._3 match {
          case Some(a) =>
            DeleteRecords(
              "select tupleId from " + tableName + " " + table._3.getOrElse("")
                + rest.getOrElse(""),
              Some(table._3.get),
              table._1)
          case None =>
            DeleteRecords(
              "select tupleId from " + tableName + " " + rest.getOrElse(""),
              None,
              table._1)
        }
        relation
    }

  protected lazy val updateTable: Parser[LogicalPlan] =
    UPDATE ~> aliasTable ~
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
            val relation : UnresolvedRelation = tab._1 match {
              case r@CarbonUnresolvedRelation(tableIdentifier) =>
                tab._3 match {
                  case Some(a) => updateRelation(r, tableIdentifier, tab._4, Some(tab._3.get))
                  case None => updateRelation(r, tableIdentifier, tab._4, None)
                }
              case _ => tab._1
            }

            tab._3 match {
              case Some(a) =>
                ("select " + sel + " from " + getTableName(tab._2) + " " + tab._3.get, relation)
              case None =>
                ("select " + sel + " from " + getTableName(tab._2), relation)
            }

          } else {
            (sel, updateRelation(tab._1, tab._2, tab._4, tab._3))
          }
        val rel = tab._3 match {
          case Some(a) => UpdateTable(relation, columns, selectStmt, Some(tab._3.get), where)
          case None => UpdateTable(relation,
            columns,
            selectStmt,
            Some(tab._1.tableIdentifier.table),
            where)
        }
        rel
    }



  private def updateRelation(
      r: UnresolvedRelation,
      tableIdent: Seq[String],
      tableIdentifier: TableIdentifier,
      alias: Option[String]): UnresolvedRelation = {
    alias match {
      case Some(_) => r
      case _ =>
        val tableAlias = tableIdent match {
          case Seq(dbName, tableName) => Some(tableName)
          case Seq(tableName) => Some(tableName)
        }
        // Use Reflection to choose between Spark2.1 and Spark2.2
        // Move UnresolvedRelation(tableIdentifier, tableAlias) to reflection.
        CarbonReflectionUtils.getUnresolvedRelation(tableIdentifier, tableAlias)
    }
  }

  protected lazy val element: Parser[String] =
    (ident <~ ".").? ~ ident ^^ {
      case table ~ column => column.toLowerCase
    }

  protected lazy val table: Parser[UnresolvedRelation] = {
    rep1sep(attributeName, ".") ~ opt(ident) ^^ {
      case tableIdent ~ alias => UnresolvedRelation(tableIdent)
    }
  }

  protected lazy val aliasTable: Parser[(UnresolvedRelation, List[String], Option[String],
    TableIdentifier)] = {
    rep1sep(attributeName, ".") ~ opt(ident) ^^ {
      case tableIdent ~ alias =>

        val tableIdentifier: TableIdentifier = toTableIdentifier(tableIdent)

        // Use Reflection to choose between Spark2.1 and Spark2.2
        // Move (UnresolvedRelation(tableIdent, alias), tableIdent, alias) to reflection.
        val unresolvedRelation = CarbonReflectionUtils.getUnresolvedRelation(tableIdentifier, alias)

        (unresolvedRelation, tableIdent, alias, tableIdentifier)
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
    select.substring(1, select.length - 1).trim -> where.trim
  }

  protected lazy val attributeName: Parser[String] = acceptMatch("attribute name", {
    case lexical.Identifier(str) => str.toLowerCase
    case lexical.Keyword(str) if !lexical.delimiters.contains(str) => str.toLowerCase
  })

  private def getTableName(tableIdentifier: Seq[String]): String = {
    if (tableIdentifier.size > 1) {
      tableIdentifier.head + "." + tableIdentifier(1)
    } else {
      tableIdentifier.head
    }
  }


  protected lazy val loadDataNew: Parser[LogicalPlan] =
    LOAD ~> DATA ~> opt(LOCAL) ~> INPATH ~> stringLit ~ opt(OVERWRITE) ~
    (INTO ~> TABLE ~> (ident <~ ".").? ~ ident) ~
    (PARTITION ~>"("~> repsep(partitions, ",") <~ ")").? ~
    (OPTIONS ~> "(" ~> repsep(loadOptions, ",") <~ ")").? <~ opt(";") ^^ {
      case filePath ~ isOverwrite ~ table ~ partitions ~ optionsList =>
        val (databaseNameOp, tableName) = table match {
          case databaseName ~ tableName => (databaseName, tableName.toLowerCase())
        }
        if (optionsList.isDefined) {
          validateOptions(optionsList)
        }
        val optionsMap = optionsList.getOrElse(List.empty[(String, String)]).toMap
        val partitionSpec = partitions.getOrElse(List.empty[(String, Option[String])]).toMap
        CarbonLoadDataCommand(
          databaseNameOp = convertDbNameToLowerCase(databaseNameOp),
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

  protected lazy val deleteLoadsByID: Parser[LogicalPlan] =
    DELETE ~> FROM ~ TABLE ~> (ident <~ ".").? ~ ident ~
    (WHERE ~> (SEGMENT ~ "." ~ ID) ~> IN ~> "(" ~> repsep(segmentId, ",")) <~ ")" ~
    opt(";") ^^ {
      case dbName ~ tableName ~ loadids =>
        CarbonDeleteLoadByIdCommand(loadids, dbName, tableName.toLowerCase())
    }

  protected lazy val deleteLoadsByLoadDate: Parser[LogicalPlan] =
    DELETE ~> FROM ~> TABLE ~> (ident <~ ".").? ~ ident ~
    (WHERE ~> (SEGMENT ~ "." ~ STARTTIME ~> BEFORE) ~ stringLit) <~
    opt(";") ^^ {
      case database ~ table ~ condition =>
        condition match {
          case dateField ~ dateValue =>
            CarbonDeleteLoadByLoadDateCommand(convertDbNameToLowerCase(database),
              table.toLowerCase(),
              dateField,
              dateValue)
        }
    }

  protected lazy val cleanFiles: Parser[LogicalPlan] =
    CLEAN ~> FILES ~> FOR ~> TABLE ~> (ident <~ ".").? ~ ident <~ opt(";") ^^ {
      case databaseName ~ tableName =>
        CarbonCleanFilesCommand(
          convertDbNameToLowerCase(databaseName),
          Option(tableName.toLowerCase()))
    }

  protected lazy val explainPlan: Parser[LogicalPlan] =
    (EXPLAIN ~> opt(EXTENDED)) ~ startCommand ^^ {
      case isExtended ~ logicalPlan =>
        logicalPlan match {
          case _: CarbonCreateTableCommand =>
            ExplainCommand(logicalPlan, extended = isExtended.isDefined)
          case _ => CarbonToSparkAdapter.getExplainCommandObj
        }
    }

  protected lazy val showLoads: Parser[LogicalPlan] =
    (SHOW ~> opt(HISTORY) <~ SEGMENTS <~ FOR <~ TABLE) ~ (ident <~ ".").? ~ ident ~
    (LIMIT ~> numericLit).? <~
    opt(";") ^^ {
      case showHistory ~ databaseName ~ tableName ~ limit =>
        CarbonShowLoadsCommand(
          convertDbNameToLowerCase(databaseName), tableName.toLowerCase(), limit,
          showHistory.isDefined)
    }

  protected lazy val showCache: Parser[LogicalPlan] =
    SHOW ~> METACACHE ~> opt(ontable) <~ opt(";") ^^ {
      case table =>
        CarbonShowCacheCommand(table)
    }

  protected lazy val dropCache: Parser[LogicalPlan] =
    DROP ~> METACACHE ~> ontable <~ opt(";") ^^ {
      case table =>
        CarbonDropCacheCommand(table)
    }

  protected lazy val cli: Parser[LogicalPlan] =
    (CARBONCLI ~> FOR ~> TABLE) ~> (ident <~ ".").? ~ ident ~
    (OPTIONS ~> "(" ~> commandOptions <~ ")").? <~
    opt(";") ^^ {
      case databaseName ~ tableName ~ commandList =>
        var commandOptions: String = null
        if (commandList.isDefined) {
          commandOptions = commandList.get
        }
        CarbonCliCommand(
          convertDbNameToLowerCase(databaseName),
          tableName.toLowerCase(),
          commandOptions)
    }


  protected lazy val alterTableColumnRenameAndModifyDataType: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> (ident <~ ".").? ~ ident ~ CHANGE ~ ident ~ ident ~
    ident ~ opt("(" ~> rep1sep(valueOptions, ",") <~ ")") <~ opt(";") ^^ {
      case dbName ~ table ~ change ~ columnName ~ columnNameCopy ~ dataType ~ values =>

        var isColumnRename = false
        // If both the column name are not same, then its a call for column rename
        if (!columnName.equalsIgnoreCase(columnNameCopy)) {
          isColumnRename = true
        }
        val alterTableColRenameAndDataTypeChangeModel =
          AlterTableDataTypeChangeModel(parseDataType(dataType.toLowerCase,
            values,
            isColumnRename),
            convertDbNameToLowerCase(dbName),
            table.toLowerCase,
            columnName.toLowerCase,
            columnNameCopy.toLowerCase,
            isColumnRename)
        CarbonAlterTableColRenameDataTypeChangeCommand(alterTableColRenameAndDataTypeChangeModel)
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
                  fields.count(p => p.column.equalsIgnoreCase(colName)) == 1) {
                sys.error(s"Duplicate default value exist for new column: ${ colName }")
              }
            }
          )
          // default value should not be converted to lower case
          val tblProps = tblProp.get
            .map(f => if (CarbonCommonConstants.TABLE_BLOCKSIZE.equalsIgnoreCase(f._1) ||
                          CarbonCommonConstants.SORT_COLUMNS.equalsIgnoreCase(f._1) ||
                          CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE.equalsIgnoreCase(f._1) ||
                          CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD.equalsIgnoreCase(f._1)) {
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

        val alterTableAddColumnsModel = AlterTableAddColumnsModel(
          convertDbNameToLowerCase(dbName),
          table,
          tableProps.toMap,
          tableModel.dimCols,
          tableModel.msrCols,
          tableModel.highcardinalitydims.getOrElse(Seq.empty))
        CarbonAlterTableAddColumnCommand(alterTableAddColumnsModel)
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
        CarbonAlterTableDropColumnCommand(alterTableDropColumnModel)
    }

  def getFields(schema: Seq[StructField]): Seq[Field] = {
    schema.map { col =>
      var columnComment: String = ""
      var plainComment: String = ""
      if (col.getComment().isDefined) {
        columnComment = " comment \"" + col.getComment().get + "\""
        plainComment = col.getComment().get
      }
      val x =
        if (col.dataType.catalogString == "float") {
          '`' + col.name + '`' + " double" + columnComment
        } else {
          '`' + col.name + '`' + ' ' + col.dataType.catalogString + columnComment
        }
      val f: Field = anyFieldDef(new lexical.Scanner(x.toLowerCase))
      match {
        case Success(field, _) => field.asInstanceOf[Field]
        case failureOrError => throw new MalformedCarbonCommandException(
          s"Unsupported data type: ${ col.dataType }")
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
      f.columnComment = plainComment
      f
    }
  }

  def addPreAggFunction(sql: String): String = {
    addPreAgg(new lexical.Scanner(sql)) match {
      case Success(query, _) => query
      case _ =>
        throw new MalformedCarbonCommandException(s"Unsupported query")
    }
  }

  def addPreAggLoadFunction(sql: String): String = {
    addPreAggLoad(new lexical.Scanner(sql)) match {
      case Success(query, _) => query
      case _ =>
        throw new MalformedCarbonCommandException(s"Unsupported query")
    }
  }

  def getBucketFields(
      properties: mutable.Map[String, String],
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
