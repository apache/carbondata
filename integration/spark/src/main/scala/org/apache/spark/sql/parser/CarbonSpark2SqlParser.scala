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

import java.util.regex.Pattern

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
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
import org.apache.spark.sql.execution.command.index.{CarbonCreateIndexCommand, CarbonRefreshIndexCommand, DropIndexCommand, IndexRepairCommand, ShowIndexesCommand}
import org.apache.spark.sql.execution.command.management._
import org.apache.spark.sql.execution.command.schema.CarbonAlterTableDropColumnCommand
import org.apache.spark.sql.execution.command.stream.{CarbonCreateStreamCommand, CarbonDropStreamCommand, CarbonShowStreamsCommand}
import org.apache.spark.sql.execution.command.table.{CarbonCreateTableCommand, CarbonDescribeColumnCommand, CarbonDescribeShortCommand}
import org.apache.spark.sql.execution.command.view.{CarbonCreateMVCommand, CarbonDropMVCommand, CarbonRefreshMVCommand, CarbonShowMVCommand}
import org.apache.spark.sql.execution.strategy.CarbonPlanHelper
import org.apache.spark.sql.secondaryindex.command.{CarbonCreateSecondaryIndexCommand, _}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField}
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
          plan
        case failureOrError =>
          CarbonScalaUtil.cleanParserThreadLocals()
          CarbonException.analysisException(failureOrError.toString)
      }
    }
  }


  protected lazy val start: Parser[LogicalPlan] =
    startCommand | extendedSparkSyntax

  protected lazy val startCommand: Parser[LogicalPlan] =
    segmentManagement | alterTable | restructure | updateTable | deleteRecords |
    alterTableFinishStreaming | stream | cli |
    cacheManagement | insertStageData | indexCommands | mvCommands | describeCommands

  protected lazy val segmentManagement: Parser[LogicalPlan] =
    deleteSegmentByID | deleteSegmentByLoadDate | deleteStage | cleanFiles | addSegment |
    showSegments

  protected lazy val restructure: Parser[LogicalPlan] = alterTableDropColumn

  protected lazy val stream: Parser[LogicalPlan] =
    createStream | dropStream | showStreams

  protected lazy val cacheManagement: Parser[LogicalPlan] =
    showCache | dropCache

  protected lazy val extendedSparkSyntax: Parser[LogicalPlan] =
    loadDataNew | explainPlan | alterTableColumnRenameAndModifyDataType |
    alterTableAddColumns

  protected lazy val mvCommands: Parser[LogicalPlan] =
    createMV | dropMV | showMV | refreshMV

  protected lazy val indexCommands: Parser[LogicalPlan] =
    createIndex | dropIndex | showIndexes | registerIndexes | refreshIndex | repairIndex |
      repairIndexDatabase

  protected lazy val describeCommands: Parser[LogicalPlan] = describeColumn | describeShort

  protected lazy val alterTable: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> (ident <~ ".").? ~ ident ~ (COMPACT ~ stringLit) ~
      (WHERE ~> (SEGMENT ~ "." ~ ID) ~> IN ~> "(" ~> repsep(segmentId, ",") <~ ")").? <~
      opt(";") ^^ {
      case dbName ~ table ~ (compact ~ compactType) ~ segs =>
        val alterTableModel = AlterTableModel(CarbonParserUtil.convertDbNameToLowerCase(dbName),
          table, None, compactType, Some(System.currentTimeMillis()), segs)
        CarbonAlterTableCompactionCommand(alterTableModel)
    }

  /**
   * describe complex column of table
   */
  protected lazy val describeColumn: Parser[LogicalPlan] =
    (DESCRIBE | DESC) ~> COLUMN ~> repsep(ident, ".") ~ ontable <~ opt(";") ^^ {
      case fields ~ table =>
        CarbonDescribeColumnCommand(
          table.database, table.table, fields.asJava)
    }

  /**
   * describe short version of table complex columns
   */
  protected lazy val describeShort: Parser[LogicalPlan] =
    (DESCRIBE | DESC) ~> SHORT ~> (ident <~ ".").? ~ ident <~ opt(";") ^^ {
      case dbName ~ table =>
        CarbonDescribeShortCommand(dbName, table)
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
   * CREATE INDEX [IF NOT EXISTS] index_name
   * ON TABLE [db_name.]table_name (column_name, ...)
   * AS carbondata/bloomfilter/lucene
   * [WITH DEFERRED REFRESH]
   * [PROPERTIES ('key'='value')]
   */
  protected lazy val createIndex: Parser[LogicalPlan] =
    CREATE ~> INDEX ~> opt(IF ~> NOT ~> EXISTS) ~ ident ~
    ontable ~
    ("(" ~> repsep(element, ",") <~ ")") ~
    (AS ~> stringLit) ~
    (WITH ~> DEFERRED ~> REFRESH).? ~
    (PROPERTIES ~> "(" ~> repsep(options, ",") <~ ")").? <~ opt(";") ^^ {
      case ifNotExists ~ indexName ~ table ~ cols ~ indexProvider ~ deferred ~ props =>
        val tableColumns = cols.map(f => f.toLowerCase)
        val indexModel = IndexModel(
          table.database, table.table.toLowerCase, tableColumns, indexName.toLowerCase)
        val propList = props.getOrElse(List[(String, String)]())
        val properties = mutable.Map[String, String](propList : _*)
        if ("carbondata".equalsIgnoreCase(indexProvider)) {
          // validate the tableBlockSize from table properties
          CommonUtil.validateSize(properties, CarbonCommonConstants.TABLE_BLOCKSIZE)
          // validate cache expiration time
          CommonUtil.validateCacheExpiration(properties,
            CarbonCommonConstants.INDEX_CACHE_EXPIRATION_TIME_IN_SECONDS)
          // validate for supported table properties
          validateTableProperties(properties)
          // validate column_meta_cache property if defined
          validateColumnMetaCacheAndCacheLevelProperties(
            table.database, indexName.toLowerCase, tableColumns, properties)
          CarbonSparkSqlParserUtil.validateColumnCompressorProperty(
            properties.getOrElse(CarbonCommonConstants.COMPRESSOR, null))
          // validate sort scope
          CommonUtil.validateSortScope(properties)
          // validate global_sort_partitions
          CommonUtil.validateGlobalSortPartitions(properties)
          CarbonCreateSecondaryIndexCommand(
            indexModel,
            properties,
            ifNotExists.isDefined,
            deferred.isDefined)
        } else {
          CarbonCreateIndexCommand(indexModel, indexProvider,
            properties.toMap, ifNotExists.isDefined, deferred.isDefined)
        }
    }

  protected lazy val ontable: Parser[TableIdentifier] =
    ON ~> TABLE.? ~ (ident <~ ".").? ~ ident ^^ {
      case ignored ~ dbName ~ tableName =>
        TableIdentifier(tableName, dbName)
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
        // If update is received for complex data types then throw exception
        var finalColumns = List.empty[String]
        val updateColumns = new ListBuffer[String]()
        columns.foreach { column =>
          if (column.contains('.')) {
            val columnFullName = column.split('.')
            if (columnFullName.length >= 3) {
              throw new UnsupportedOperationException("Unsupported operation on Complex data types")
            } else if ((tab._3.isDefined && tab._3.get.equalsIgnoreCase(columnFullName(0)))
                || tab._4.table.equalsIgnoreCase(columnFullName(0))) {
              updateColumns += columnFullName(1)
            } else {
              throw new UnsupportedOperationException("Unsupported operation on Complex data types")
            }
          } else {
            updateColumns += column
          }
        }
        finalColumns = updateColumns.toList
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
              "Update subquery has join with main table and limit leads to multiple join for " +
              "each limit for each row")
          }
          if (!isJoinWithMainTable) {
            // Should go as value update, not as join update. So execute the sub query.
            val analyzedPlan = CarbonToSparkAdapter.invokeAnalyzerExecute(session
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
          case Some(a) => UpdateTable(relation, finalColumns, selectStmt, Some(tab._3.get),
            where)
          case None => UpdateTable(relation,
            finalColumns,
            selectStmt,
            Some(CarbonToSparkAdapter.getTableIdentifier(tab._1).get.table),
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
      case table ~ column =>
        if (table.isDefined) {
          table.get.toLowerCase + "." + column.toLowerCase
        } else {
          column.toLowerCase
        }
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

  protected lazy val deleteSegmentByID: Parser[LogicalPlan] =
    DELETE ~> FROM ~ TABLE ~> (ident <~ ".").? ~ ident ~
    (WHERE ~> (SEGMENT ~ "." ~ ID) ~> IN ~> "(" ~> repsep(segmentId, ",")) <~ ")" ~
    opt(";") ^^ {
      case dbName ~ tableName ~ loadIds =>
        CarbonDeleteLoadByIdCommand(loadIds, dbName, tableName.toLowerCase())
    }

  protected lazy val deleteSegmentByLoadDate: Parser[LogicalPlan] =
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
  protected lazy val addSegment: Parser[LogicalPlan] =
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
    CLEAN ~> FILES ~> FOR ~> TABLE ~> (ident <~ ".").? ~ ident ~
      (OPTIONS ~> "(" ~> repsep(options, ",") <~ ")").? <~ opt(";") ^^ {
      case databaseName ~ tableName ~ optionList =>
        CarbonCleanFilesCommand(
          CarbonParserUtil.convertDbNameToLowerCase(databaseName),
          tableName.toLowerCase(),
          optionList.map(_.toMap).getOrElse(Map.empty))
    }

  protected lazy val explainPlan: Parser[LogicalPlan] =
    (EXPLAIN ~> opt(MODE)) ~ start ^^ {
      case mode ~ logicalPlan =>
        logicalPlan match {
          case _: CarbonCreateTableCommand =>
            CarbonToSparkAdapter.getExplainCommandObj(logicalPlan, mode)
          case _ => CarbonToSparkAdapter.getExplainCommandObj(mode)
        }
    }

  /**
   * SHOW [HISTORY] SEGMENTS
   * [FOR TABLE | ON] [db_name.]table_name
   * [AS (select query)]
   */
  protected lazy val showSegments: Parser[LogicalPlan] =
    (SHOW ~> opt(HISTORY) <~ SEGMENTS <~ ((FOR <~ TABLE) | ON)) ~ (ident <~ ".").? ~ ident ~
      opt(WITH <~ STAGE) ~ (LIMIT ~> numericLit).? ~ (AS  ~> restInput).? <~ opt(";") ^^ {
      case showHistory ~ databaseName ~ tableName ~ withStage ~ limit ~ queryOp =>
        if (queryOp.isEmpty) {
          CarbonShowSegmentsCommand(
            CarbonParserUtil.convertDbNameToLowerCase(databaseName),
            tableName.toLowerCase(),
            if (limit.isDefined) Some(Integer.valueOf(limit.get)) else None,
            showHistory.isDefined,
            withStage.isDefined)
        } else {
          CarbonShowSegmentsAsSelectCommand(
            CarbonParserUtil.convertDbNameToLowerCase(databaseName),
            tableName.toLowerCase(),
            queryOp.get,
            if (limit.isDefined) Some(Integer.valueOf(limit.get)) else None,
            showHistory.isDefined,
            withStage.isDefined)
        }
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
    ALTER ~> TABLE ~> (ident <~ ".").? ~ ident ~ (CHANGE ~> ident) ~ ident ~
    opt(primitiveTypes) ~ opt(nestedType) ~ opt(COMMENT ~> restInput) <~ opt(";") ^^ {
      case dbName ~ table ~ columnName ~ newColumnName ~ dataType ~ complexField ~
           comment if CarbonPlanHelper.isCarbonTable(TableIdentifier(table, dbName)) &&
                      (complexField.isDefined ^ dataType.isDefined) =>
        var primitiveType = dataType
        var newComment: Option[String] = comment
        var decimalValues = None: Option[List[(Int, Int)]]
        // if datatype is decimal then extract precision and scale
        if (!dataType.equals(None) && dataType.get.contains(CarbonCommonConstants.DECIMAL)) {
          val matcher = Pattern.compile("[0-9]+").matcher(dataType.get)
          val list = collection.mutable.ListBuffer.empty[Int]
          while ( { matcher.find }) {
            list += matcher.group.toInt
          }
          decimalValues = Some(List((list(0), list(1))))
          primitiveType = Some(CarbonCommonConstants.DECIMAL)
        }
        newComment = if (comment.isDefined) {
          Some(StringUtils.substringBetween(comment.get, "'", "'"))
        } else { None }

        CarbonSparkSqlParserUtil.alterTableColumnRenameAndModifyDataType(
          dbName, table, columnName, newColumnName, primitiveType, decimalValues, newComment,
          complexField)
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

  private def validateColumnMetaCacheAndCacheLevelProperties(dbName: Option[String],
      tableName: String,
      tableColumns: Seq[String],
      tableProperties: scala.collection.mutable.Map[String, String]): Unit = {
    // validate column_meta_cache property
    if (tableProperties.get(CarbonCommonConstants.COLUMN_META_CACHE).isDefined) {
      CommonUtil.validateColumnMetaCacheFields(
        dbName.getOrElse(CarbonCommonConstants.DATABASE_DEFAULT_NAME),
        tableName,
        tableColumns,
        tableProperties(CarbonCommonConstants.COLUMN_META_CACHE),
        tableProperties)
    }
    // validate cache_level property
    if (tableProperties.get(CarbonCommonConstants.CACHE_LEVEL).isDefined) {
      CommonUtil.validateCacheLevel(
        tableProperties(CarbonCommonConstants.CACHE_LEVEL),
        tableProperties)
    }
  }

  /**
   * this method validates if index table properties contains other than supported ones
   */
  private def validateTableProperties(tableProperties: scala.collection.mutable.Map[String,
    String]): Unit = {
    val supportedPropertiesForIndexTable = Seq("TABLE_BLOCKSIZE",
      "COLUMN_META_CACHE",
      "CACHE_LEVEL",
      CarbonCommonConstants.COMPRESSOR.toUpperCase,
      "SORT_SCOPE",
      "GLOBAL_SORT_PARTITIONS")
    tableProperties.foreach { property =>
      if (!supportedPropertiesForIndexTable.contains(property._1.toUpperCase)) {
        val errorMessage = "Unsupported Table property in index creation: " + property._1.toString
        throw new MalformedCarbonCommandException(errorMessage)
      }
    }
  }

  /**
   * DROP INDEX [IF EXISTS] index_name
   * ON [db_name.]table_name
   */
  protected lazy val dropIndex: Parser[LogicalPlan] =
    DROP ~> INDEX ~> opt(IF ~> EXISTS) ~ ident ~
    ontable <~ opt(";") ^^ {
      case ifExist ~ indexName ~ table =>
        DropIndexCommand(ifExist.isDefined, table.database, table.table, indexName.toLowerCase)
    }

  /**
   * SHOW INDEXES ON table_name
   */
  protected lazy val showIndexes: Parser[LogicalPlan] =
    SHOW ~> INDEXES ~> ontable <~ opt(";") ^^ {
      case table =>
        ShowIndexesCommand(table.database, table.table)
    }

  /**
   * REINDEX INDEX TABLE index_name
   * ON [db_name.]table_name
   * [WHERE SEGMENT.ID IN (segment_id, ...)] or
   *
   * REINDEX ON [db_name.]table_name
   * [WHERE SEGMENT.ID IN (segment_id, ...)]
   */
  protected lazy val repairIndex: Parser[LogicalPlan] =
    REINDEX ~> opt(INDEX ~> TABLE ~> ident)  ~ ontable ~
      (WHERE ~> (SEGMENT ~ "." ~ ID) ~> IN ~> "(" ~> repsep(segmentId, ",") <~ ")").? <~
    opt(";") ^^ {
      case indexName ~ tableIdent ~ segments =>
        IndexRepairCommand(indexName, tableIdent, null, segments)
    }

  /**
   * REINDEX DATABASEON db_name
   * [WHERE SEGMENT.ID IN (segment_id, ...)]
   */
  protected lazy val repairIndexDatabase: Parser[LogicalPlan] =
    REINDEX ~> DATABASE ~> ident ~
      (WHERE ~> (SEGMENT ~ "." ~ ID) ~> IN ~> "(" ~> repsep(segmentId, ",") <~ ")").? <~
      opt(";") ^^ {
      case dbName ~ segments =>
        IndexRepairCommand(None, null, dbName, segments)
    }

  protected lazy val registerIndexes: Parser[LogicalPlan] =
    REGISTER ~> INDEX ~> TABLE ~> ident ~ ontable <~ opt(";") ^^ {
      case indexTable ~ table =>
        RegisterIndexTableCommand(table.database, indexTable, table.table)
    }

  /**
   * REFRESH INDEX index_name
   * ON [db_name.]table_name
   * [WHERE SEGMENT.ID IN (segment_id, ...)]
   */
  protected lazy val refreshIndex: Parser[LogicalPlan] =
    REFRESH ~> INDEX ~>  ident ~
    ontable ~
    (WHERE ~> (SEGMENT ~ "." ~ ID) ~> IN ~> "(" ~> repsep(segmentId, ",") <~ ")").? <~
    opt(";") ^^ {
      case indexName ~ parentTableIdent ~ segments =>
        CarbonRefreshIndexCommand(indexName.toLowerCase(), parentTableIdent, segments)
    }

  def getFields(schema: Seq[StructField], isExternal: Boolean = false): Seq[Field] = {
    schema.map { col =>
      // TODO: Spark has started supporting CharType/VarChar types in Spark 3.1 but both are
      //  marked as experimental. Adding a hack to change to string for now.
      // Refer: https://issues.apache.org/jira/browse/CARBONDATA-4226
      if (CarbonToSparkAdapter.isCharType(col.dataType) || CarbonToSparkAdapter
          .isVarCharType(col.dataType)) {
        getFields(col.getComment(), col.name, DataTypes.StringType, isExternal)
      } else {
        getFields(col.getComment(), col.name, col.dataType, isExternal)
      }
    }
  }

  def getFields(comment: Option[String],
                name: String,
                dataType: DataType,
                isExternal: Boolean): Field = {
      var columnComment: String = ""
      var plainComment: String = ""
      if (comment.isDefined) {
        columnComment = " comment '" + comment.get + "'"
        plainComment = comment.get
      }
      val indexesToReplace = name.toCharArray.zipWithIndex.map {
        case (ch, index) => if (ch.equals('`')) index }.filter(_ != ())
      // external table use float data type
      // normal table use double data type instead of float
      var scannerInput = if (name.contains("`")) {
        // If the column name contains the character like `, then parsing fails, as ` char is used
        // to tokenize and it wont be able to differentiate between whether its actual character or
        // token character. So just for parsing temporarily replace with !, then once field is
        // prepared, just replace the original name.
        getScannerInput(dataType, columnComment, name.replaceAll("`", "!"), isExternal)
      } else {
        getScannerInput(dataType, columnComment, name, isExternal)
      }
      var field: Field = anyFieldDef(new lexical.Scanner(scannerInput.toLowerCase))
      match {
        case Success(field, _) =>
          if (dataType.catalogString == "float" && isExternal) {
            field.dataType = Some("float")
          }
          field.asInstanceOf[Field]
        case failureOrError => throw new MalformedCarbonCommandException(
          s"Unsupported data type: ${ dataType }")
      }
      if (name.contains("`")) {
        val actualName = indexesToReplace.foldLeft(field.column)((s, i) => s.updated(i
          .asInstanceOf[Int], '`'))
        field = field.copy(column = actualName, name = Some(actualName))
        scannerInput = getScannerInput(dataType, columnComment, name, isExternal)
      }
      // the data type of the decimal type will be like decimal(10,0)
      // so checking the start of the string and taking the precision and scale.
      // resetting the data type with decimal
      if (field.dataType.getOrElse("").startsWith("decimal")) {
        val (precision, scale) = CommonUtil.getScaleAndPrecision(dataType.catalogString)
        field.precision = precision
        field.scale = scale
        field.dataType = Some("decimal")
      }
      if (field.dataType.getOrElse("").startsWith("char")) {
        field.dataType = Some("char")
      }
      else if (field.dataType.getOrElse("").startsWith("float") && !isExternal) {
        field.dataType = Some("double")
      }
      field.rawSchema = scannerInput
      if (comment.isDefined) {
        field.columnComment = plainComment
      }
      field
  }

  def getScannerInput(dataType: DataType,
      columnComment: String,
      columnName: String,
      isExternal: Boolean): String = {
    if (dataType.catalogString == "float" && !isExternal) {
      '`' + columnName + '`' + " double" + columnComment
    } else {
      '`' + columnName + '`' + ' ' + dataType.catalogString +
        columnComment
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
      if (options.bucketNumber.isEmpty || options.bucketNumber.get <= 0) {
        throw new MalformedCarbonCommandException("INVALID NUMBER OF BUCKETS SPECIFIED")
      }
      else {
        Some(BucketFields(options.bucketColumns.toLowerCase.split(",").map(_.trim),
          options.bucketNumber.get))
      }
    } else {
      None
    }
  }

  // For materialized view
  /**
   * CREATE MATERIALIZED VIEW [IF NOT EXISTS] mv_name
   * [WITH DEFERRED REFRESH]
   * [PROPERTIES('KEY'='VALUE')]
   * AS mv_query_statement
   */
  private lazy val createMV: Parser[LogicalPlan] =
    CREATE ~> MATERIALIZED ~> VIEW ~> opt(IF ~> NOT ~> EXISTS) ~ (ident <~ ".").? ~ ident ~
    opt(WITH ~> DEFERRED ~> REFRESH) ~
    (PROPERTIES ~> "(" ~> repsep(options, ",") <~ ")").? ~
    AS ~ query <~ opt(";") ^^ {
      case ifNotExists ~ databaseName ~ name ~ deferredRefresh ~ properties ~ _ ~ query =>
        CarbonCreateMVCommand(
          databaseName,
          name,
          properties.getOrElse(List[(String, String)]()).toMap[String, String],
          query,
          ifNotExists.isDefined,
          deferredRefresh.isDefined)
    }

  /**
   * DROP MATERIALIZED VIEW [IF EXISTS] mv_name
   */
  private lazy val dropMV: Parser[LogicalPlan] =
    DROP ~> MATERIALIZED ~> VIEW ~> opt(IF ~> EXISTS) ~ (ident <~ ".").? ~ ident <~ opt(";") ^^ {
      case ifExits ~ databaseName ~ name =>
        CarbonDropMVCommand(databaseName, name, ifExits.isDefined)
    }

  /**
   * SHOW MATERIALIZED VIEWS [ON TABLE table_name]
   */
  private lazy val showMV: Parser[LogicalPlan] =
    SHOW ~> MATERIALIZED ~> VIEWS ~> opt(ontable) <~ opt(";") ^^ {
      case table =>
        CarbonShowMVCommand(None, table)
    }

  /**
   * REFRESH MATERIALIZED VIEW mv_name
   */
  private lazy val refreshMV: Parser[LogicalPlan] =
    REFRESH ~> MATERIALIZED ~> VIEW ~> (ident <~ ".").? ~ ident <~ opt(";") ^^ {
      case databaseName ~ name =>
        CarbonRefreshMVCommand(databaseName, name)
    }

  // Returns the rest of the input string that are not parsed yet
  private lazy val query: Parser[String] = new Parser[String] {
    def apply(in: Input): ParseResult[String] =
      Success(
        in.source.subSequence(in.offset, in.source.length()).toString,
        in.drop(in.source.length()))
  }

}
