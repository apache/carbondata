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

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{CarbonToSparkAdapter, Dataset, DeleteRecords, SparkSession, UpdateTable}
import org.apache.spark.sql.CarbonExpressions.CarbonUnresolvedRelation
import org.apache.spark.sql.catalyst.{CarbonDDLSqlParser, CarbonParserUtil, TableIdentifier}
import org.apache.spark.sql.catalyst.CarbonTableIdentifierImplicit._
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.cache.{CarbonDropCacheCommand, CarbonShowCacheCommand}
import org.apache.spark.sql.execution.command.datamap.{CarbonCreateDataMapCommand, CarbonDataMapRebuildCommand, CarbonDataMapShowCommand, CarbonDropDataMapCommand}
import org.apache.spark.sql.execution.command.management._
import org.apache.spark.sql.execution.command.schema.CarbonAlterTableDropColumnCommand
import org.apache.spark.sql.execution.command.stream.{CarbonCreateStreamCommand, CarbonDropStreamCommand, CarbonShowStreamsCommand}
import org.apache.spark.sql.execution.command.table.CarbonCreateTableCommand
import org.apache.spark.sql.secondaryindex.command._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.util.CarbonReflectionUtils

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.exception.InvalidConfigurationException
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


  protected lazy val start: Parser[LogicalPlan] = startCommand | extendedSparkSyntax

  protected lazy val startCommand: Parser[LogicalPlan] =
    loadManagement | showLoads | alterTable | restructure | updateTable | deleteRecords |
    datamapManagement | alterTableFinishStreaming | stream | cli |
    cacheManagement | alterDataMap | insertStageData | indexCommands

  protected lazy val loadManagement: Parser[LogicalPlan] =
    deleteLoadsByID | deleteLoadsByLoadDate | deleteStage | cleanFiles | addLoad

  protected lazy val restructure: Parser[LogicalPlan] = alterTableDropColumn

  protected lazy val datamapManagement: Parser[LogicalPlan] =
    createDataMap | dropDataMap | showDataMap | refreshDataMap

  protected lazy val stream: Parser[LogicalPlan] =
    createStream | dropStream | showStreams

  protected lazy val cacheManagement: Parser[LogicalPlan] =
    showCache | dropCache

  protected lazy val extendedSparkSyntax: Parser[LogicalPlan] =
    loadDataNew | explainPlan | alterTableColumnRenameAndModifyDataType |
    alterTableAddColumns

  protected lazy val alterTable: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> (ident <~ ".").? ~ ident ~ (COMPACT ~ stringLit) ~
      (WHERE ~> (SEGMENT ~ "." ~ ID) ~> IN ~> "(" ~> repsep(segmentId, ",") <~ ")").? <~
      opt(";") ^^ {
      case dbName ~ table ~ (compact ~ compactType) ~ segs =>
        val alterTableModel = AlterTableModel(CarbonParserUtil.convertDbNameToLowerCase(dbName),
          table, None, compactType, Some(System.currentTimeMillis()), null, segs)
        CarbonAlterTableCompactionCommand(alterTableModel)
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
    CREATE ~> STREAM ~> opt(IF ~> NOT ~> EXISTS) ~ ident ~
    (ON ~> TABLE ~> (ident <~ ".").?) ~ ident ~
    (STMPROPERTIES ~> "(" ~> repsep(options, ",") <~ ")").? ~
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
    (DMPROPERTIES ~> "(" ~> repsep(options, ",") <~ ")").? ~
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
        val alterTableModel = AlterTableModel(CarbonParserUtil.convertDbNameToLowerCase(dbName),
          datamap + "_table", None, compactType, Some(System.currentTimeMillis()), null, segs)
        CarbonAlterTableCompactionCommand(alterTableModel)
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
        val selectPattern = """^\s*select\s+""".r
        // In case of "update = (subquery) where something"
        // If subquery has join with main table, then only it should go to "update by join" flow.
        // Else it should go to "update by value" flow.
        // In update by value flow, we need values to update.
        // so need to execute plan and collect values from subquery if is not join with main table.
        var subQueryResults : String = ""
        if (selectPattern.findFirstIn(sel.toLowerCase).isDefined) {
          // subQuery starts with select
          val mainTableName = tab._4.table
          val mainTableAlias = if (tab._3.isDefined) {
            tab._3.get
          } else {
            ""
          }
          val session = SparkSession.getActiveSession.get
          val subQueryUnresolvedLogicalPlan = session.sessionState.sqlParser.parsePlan(sel)
          var isJoinWithMainTable : Boolean = false
          var isLimitPresent : Boolean = false
          subQueryUnresolvedLogicalPlan collect {
            case f: Filter =>
              f.condition.collect {
                case attr: UnresolvedAttribute =>
                  if ((!StringUtils.isEmpty(mainTableAlias) &&
                       attr.nameParts.head.equalsIgnoreCase(mainTableAlias)) ||
                      attr.nameParts.head.equalsIgnoreCase(mainTableName)) {
                    isJoinWithMainTable = true
                  }
              }
            case _: GlobalLimit =>
              isLimitPresent = true
          }
          if (isJoinWithMainTable && isLimitPresent) {
            throw new UnsupportedOperationException(
              "Update subquery has join with maintable and limit leads to multiple join for each " +
              "limit for each row")
          }
          if (!isJoinWithMainTable) {
            // Should go as value update, not as join update. So execute the sub query.
            val analyzedPlan = CarbonReflectionUtils.invokeAnalyzerExecute(session
              .sessionState
              .analyzer, subQueryUnresolvedLogicalPlan)
            val subQueryLogicalPlan = session.sessionState.optimizer.execute(analyzedPlan)
            val df = Dataset.ofRows(session, subQueryLogicalPlan)
            val rowsCount = df.count()
            if (rowsCount == 0L) {
              // if length = 0, update to null
              subQueryResults = "null"
            } else if (rowsCount != 1) {
              throw new UnsupportedOperationException(
                " update cannot be supported for 1 to N mapping, as more than one value present " +
                "for the update key")
            } else {
              subQueryResults = "'" + df.collect()(0).toSeq.mkString("','") + "'"
            }
          }
        }
        val (selectStmt, relation) =
          if (selectPattern.findFirstIn(sel.toLowerCase).isEmpty ||
              !StringUtils.isEmpty(subQueryResults)) {
            // if subQueryResults are not empty means, it is not join with main table.
            // so use subQueryResults in update with value flow.
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
            val newSel = if (!StringUtils.isEmpty(subQueryResults)) {
              subQueryResults
            } else {
              sel
            }
            tab._3 match {
              case Some(a) =>
                ("select " + newSel + " from " + getTableName(tab._2) + " " + tab._3.get, relation)
              case None =>
                ("select " + newSel + " from " + getTableName(tab._2), relation)
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
    (PARTITION ~> "(" ~> repsep(partitions, ",") <~ ")").? ~
    (OPTIONS ~> "(" ~> repsep(options, ",") <~ ")").? <~ opt(";") ^^ {
      case filePath ~ isOverwrite ~ table ~ partitions ~ optionsList =>
        val (databaseNameOp, tableName) = table match {
          case databaseName ~ tableName => (databaseName, tableName.toLowerCase())
        }
        CarbonSparkSqlParserUtil.loadDataNew(
          databaseNameOp, tableName, optionsList, partitions, filePath, isOverwrite)
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
            CarbonDeleteLoadByLoadDateCommand(
              CarbonParserUtil.convertDbNameToLowerCase(database),
              table.toLowerCase(),
              dateField,
              dateValue)
        }
    }

  /**
   * DELETE FROM TABLE [dbName.]tableName STAGE OPTIONS (key1=value1, key2=value2, ...)
   */
  protected lazy val deleteStage: Parser[LogicalPlan] =
    DELETE ~> FROM ~> TABLE ~> (ident <~ ".").? ~ ident ~
    STAGE ~ (OPTIONS ~> "(" ~> repsep(options, ",") <~ ")").? <~ opt(";") ^^ {
      case database ~ table ~ _ ~ options =>
            CarbonDeleteStageFilesCommand(database, table,
              options.getOrElse(List[(String, String)]()).toMap[String, String])
    }

  /**
   * ALTER TABLE [dbName.]tableName ADD SEGMENT
   * OPTIONS('path'='path','format'='format', ['partition'='schema list'])
   *
   * schema list format: column_name:data_type
   * for example: 'partition'='a:int,b:string'
   */
  protected lazy val addLoad: Parser[LogicalPlan] =
    ALTER ~ TABLE ~> (ident <~ ".").? ~ ident ~ (ADD ~> SEGMENT) ~
    (OPTIONS ~> "(" ~> repsep(options, ",") <~ ")") <~ opt(";") ^^ {
      case dbName ~ tableName ~ segment ~ optionsList =>
        CarbonAddLoadCommand(dbName, tableName, optionsList.toMap)
    }

  /**
   * INSERT INTO [dbName.]tableName STAGE [OPTIONS (key1=value1, key2=value2, ...)]
   */
  protected lazy val insertStageData: Parser[LogicalPlan] =
    INSERT ~ INTO ~> (ident <~ ".").? ~ ident ~ STAGE ~
    (OPTIONS ~> "(" ~> repsep(options, ",") <~ ")").? <~ opt(";") ^^ {
      case dbName ~ tableName ~ _ ~ options =>
        CarbonInsertFromStageCommand(dbName, tableName,
          options.getOrElse(List[(String, String)]()).toMap[String, String])
    }

  protected lazy val cleanFiles: Parser[LogicalPlan] =
    CLEAN ~> FILES ~> FOR ~> TABLE ~> (ident <~ ".").? ~ ident <~ opt(";") ^^ {
      case databaseName ~ tableName =>
        CarbonCleanFilesCommand(
          CarbonParserUtil.convertDbNameToLowerCase(databaseName),
          Option(tableName.toLowerCase()))
    }

  protected lazy val explainPlan: Parser[LogicalPlan] =
    (EXPLAIN ~> opt(EXTENDED)) ~ start ^^ {
      case isExtended ~ logicalPlan =>
        logicalPlan match {
          case _: CarbonCreateTableCommand =>
            ExplainCommand(logicalPlan, extended = isExtended.isDefined)
          case _ => CarbonToSparkAdapter.getExplainCommandObj()
        }
    }

  protected lazy val showLoads: Parser[LogicalPlan] =
    (SHOW ~> opt(HISTORY) <~ SEGMENTS <~ FOR <~ TABLE) ~ (ident <~ ".").? ~ ident ~
    (LIMIT ~> numericLit).? <~
    opt(";") ^^ {
      case showHistory ~ databaseName ~ tableName ~ limit =>
        CarbonShowLoadsCommand(
          CarbonParserUtil.convertDbNameToLowerCase(databaseName),
          tableName.toLowerCase(),
          limit,
          showHistory.isDefined)
    }

  protected lazy val showCache: Parser[LogicalPlan] =
    (SHOW ~> opt(EXECUTOR) <~ METACACHE) ~ opt(ontable) <~ opt(";") ^^ {
      case (executor ~ table) =>
        CarbonShowCacheCommand(executor.isDefined, table)
    }

  protected lazy val dropCache: Parser[LogicalPlan] =
    DROP ~> METACACHE ~> ontable <~ opt(";") ^^ {
      case table =>
        CarbonDropCacheCommand(table)
    }

  protected lazy val cli: Parser[LogicalPlan] =
    CARBONCLI ~> FOR ~> TABLE ~> (ident <~ ".").? ~ ident ~
    (OPTIONS ~> "(" ~> commandOptions <~ ")") <~ opt(";") ^^ {
      case databaseName ~ tableName ~ commandOptions =>
        CarbonCliCommand(
          CarbonParserUtil.convertDbNameToLowerCase(databaseName),
          tableName.toLowerCase(),
          commandOptions)
    }


  protected lazy val alterTableColumnRenameAndModifyDataType: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> (ident <~ ".").? ~ ident ~ CHANGE ~ ident ~ ident ~
    ident ~ opt("(" ~> rep1sep(valueOptions, ",") <~ ")") <~ opt(";") ^^ {
      case dbName ~ table ~ change ~ columnName ~ columnNameCopy ~ dataType ~ values =>
        CarbonSparkSqlParserUtil.alterTableColumnRenameAndModifyDataType(
          dbName, table, columnName, columnNameCopy, dataType, values)
    }

  protected lazy val alterTableAddColumns: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> (ident <~ ".").? ~ ident ~
    (ADD ~> COLUMNS ~> "(" ~> repsep(anyFieldDef, ",") <~ ")") ~
    (TBLPROPERTIES ~> "(" ~> repsep(options, ",") <~ ")").? <~ opt(";") ^^ {
      case dbName ~ table ~ fields ~ tblProp =>
        CarbonSparkSqlParserUtil.alterTableAddColumns(
          dbName, table, fields, tblProp)
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
        val alterTableDropColumnModel = AlterTableDropColumnModel(
          CarbonParserUtil.convertDbNameToLowerCase(dbName),
          table.toLowerCase,
          values.map(_.toLowerCase))
        CarbonAlterTableDropColumnCommand(alterTableDropColumnModel)
    }

  protected lazy val indexCommands: Parser[LogicalPlan] =
    showIndexes | createIndexTable | dropIndexTable | registerIndexes | rebuildIndex

  protected lazy val createIndexTable: Parser[LogicalPlan] =
    CREATE ~> INDEX ~> ident ~ (ON ~> TABLE ~> (ident <~ ".").? ~ ident) ~
    ("(" ~> repsep(ident, ",") <~ ")") ~ (AS ~> stringLit) ~
    (TBLPROPERTIES ~> "(" ~> repsep(options, ",") <~ ")").? <~ opt(";") ^^ {
      case indexTableName ~ table ~ cols ~ indexStoreType ~ tblProp =>

        if (!("carbondata".equalsIgnoreCase(indexStoreType))) {
          sys.error("Not a carbon format request")
        }

        val (dbName, tableName) = table match {
          case databaseName ~ tableName => (databaseName, tableName.toLowerCase())
        }

        val tableProperties = if (tblProp.isDefined) {
          val tblProps = tblProp.get.map(f => f._1 -> f._2)
          scala.collection.mutable.Map(tblProps: _*)
        } else {
          scala.collection.mutable.Map.empty[String, String]
        }
        // validate the tableBlockSize from table properties
        CommonUtil.validateSize(tableProperties, CarbonCommonConstants.TABLE_BLOCKSIZE)
        // validate for supported table properties
        validateTableProperties(tableProperties)
        // validate column_meta_cache proeperty if defined
        val tableColumns: List[String] = cols.map(f => f.toLowerCase)
        validateColumnMetaCacheAndCacheLevelProeprties(dbName,
          indexTableName.toLowerCase,
          tableColumns,
          tableProperties)
        validateColumnCompressorProperty(tableProperties
          .getOrElse(CarbonCommonConstants.COMPRESSOR, null))
        val indexTableModel = SecondaryIndex(dbName,
          tableName.toLowerCase,
          tableColumns,
          indexTableName.toLowerCase)
        CreateIndexTable(indexTableModel, tableProperties)
    }

  private def validateColumnMetaCacheAndCacheLevelProeprties(dbName: Option[String],
      tableName: String,
      tableColumns: Seq[String],
      tableProperties: scala.collection.mutable.Map[String, String]): Unit = {
    // validate column_meta_cache property
    if (tableProperties.get(CarbonCommonConstants.COLUMN_META_CACHE).isDefined) {
      CommonUtil.validateColumnMetaCacheFields(
        dbName.getOrElse(CarbonCommonConstants.DATABASE_DEFAULT_NAME),
        tableName,
        tableColumns,
        tableProperties.get(CarbonCommonConstants.COLUMN_META_CACHE).get,
        tableProperties)
    }
    // validate cache_level property
    if (tableProperties.get(CarbonCommonConstants.CACHE_LEVEL).isDefined) {
      CommonUtil.validateCacheLevel(
        tableProperties.get(CarbonCommonConstants.CACHE_LEVEL).get,
        tableProperties)
    }
  }

  private def validateColumnCompressorProperty(columnCompressor: String): Unit = {
    // Add validatation for column compressor when creating index table
    try {
      if (null != columnCompressor) {
        CompressorFactory.getInstance().getCompressor(columnCompressor)
      }
    } catch {
      case ex: UnsupportedOperationException =>
        throw new InvalidConfigurationException(ex.getMessage)
    }
  }

  /**
   * this method validates if index table properties contains other than supported ones
   *
   * @param tableProperties
   */
  private def validateTableProperties(tableProperties: scala.collection.mutable.Map[String,
    String]) = {
    val supportedPropertiesForIndexTable = Seq("TABLE_BLOCKSIZE",
      "COLUMN_META_CACHE",
      "CACHE_LEVEL",
      CarbonCommonConstants.COMPRESSOR.toUpperCase)
    tableProperties.foreach { property =>
      if (!supportedPropertiesForIndexTable.contains(property._1.toUpperCase)) {
        val errorMessage = "Unsupported Table property in index creation: " + property._1.toString
        throw new MalformedCarbonCommandException(errorMessage)
      }
    }
  }

  protected lazy val dropIndexTable: Parser[LogicalPlan] =
    DROP ~> INDEX ~> opt(IF ~> EXISTS) ~ ident ~ (ON ~> (ident <~ ".").? ~ ident) <~ opt(";") ^^ {
      case ifexist ~ indexTableName ~ table =>
        val (dbName, tableName) = table match {
          case databaseName ~ tableName => (databaseName, tableName.toLowerCase())
        }
        DropIndexCommand(ifexist.isDefined, dbName, indexTableName.toLowerCase, tableName)
    }

  protected lazy val showIndexes: Parser[LogicalPlan] =
    SHOW ~> INDEXES ~> ON ~> (ident <~ ".").? ~ ident <~ opt(";") ^^ {
      case databaseName ~ tableName =>
        ShowIndexesCommand(databaseName, tableName)
    }

  protected lazy val registerIndexes: Parser[LogicalPlan] =
    REGISTER ~> INDEX ~> TABLE ~> ident ~ (ON ~> (ident <~ ".").? ~ ident) <~ opt(";") ^^ {
      case indexTable ~ table =>
        val (dbName, tableName) = table match {
          case databaseName ~ tableName => (databaseName, tableName.toLowerCase())
        }
        RegisterIndexTableCommand(dbName, indexTable, tableName)
    }

  protected lazy val rebuildIndex: Parser[LogicalPlan] =
    REBUILD ~> INDEX ~> (ident <~ ".").? ~ ident ~
    (WHERE ~> (SEGMENT ~ "." ~ ID) ~> IN ~> "(" ~> repsep(segmentId, ",") <~ ")").? <~
    opt(";") ^^ {
      case dbName ~ table ~ segs =>
        val alterTableModel =
          AlterTableModel(CarbonParserUtil.convertDbNameToLowerCase(dbName), table, None, null,
            Some(System.currentTimeMillis()), null, segs)
        SIRebuildSegmentCommand(alterTableModel)
    }

  def getFields(schema: Seq[StructField], isExternal: Boolean = false): Seq[Field] = {
    schema.map { col =>
      var columnComment: String = ""
      var plainComment: String = ""
      if (col.getComment().isDefined) {
        columnComment = " comment \"" + col.getComment().get + "\""
        plainComment = col.getComment().get
      }
      // external table use float data type
      // normal table use double data type instead of float
      val x =
        if (col.dataType.catalogString == "float" && !isExternal) {
          '`' + col.name + '`' + " double" + columnComment
        } else {
          '`' + col.name + '`' + ' ' + col.dataType.catalogString + columnComment
        }
      val f: Field = anyFieldDef(new lexical.Scanner(x.toLowerCase))
      match {
        case Success(field, _) =>
          if (col.dataType.catalogString == "float" && isExternal) {
            field.dataType = Some("float")
          }
          field.asInstanceOf[Field]
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
      else if (f.dataType.getOrElse("").startsWith("float") && !isExternal) {
        f.dataType = Some("double")
      }
      f.rawSchema = x
      if (col.getComment().isDefined) {
        f.columnComment = plainComment
      }
      f
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
