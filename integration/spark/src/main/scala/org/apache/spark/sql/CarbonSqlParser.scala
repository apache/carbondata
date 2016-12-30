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

package org.apache.spark.sql

import java.util.regex.{Matcher, Pattern}

import scala.collection.JavaConverters._
import scala.collection.mutable.LinkedHashSet
import scala.collection.mutable.Map
import scala.language.implicitConversions
import scala.util.matching.Regex

import org.apache.hadoop.hive.ql.lib.Node
import org.apache.hadoop.hive.ql.parse._
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.CarbonTableIdentifierImplicit._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.execution.ExplainCommand
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.DescribeCommand
import org.apache.spark.sql.hive.HiveQlWrapper

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.carbon.metadata.datatype.DataType
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, DataTypeUtil}
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.util.CommonUtil

/**
 * Parser for All Carbon DDL, DML cases in Unified context
 */
class CarbonSqlParser() extends CarbonDDLSqlParser {

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
        case failureOrError => sys.error(failureOrError.toString)
      }
    }
  }

  override protected lazy val start: Parser[LogicalPlan] = explainPlan | startCommand

  protected lazy val startCommand: Parser[LogicalPlan] =
    createDatabase | dropDatabase | loadManagement | describeTable | showLoads | alterTable |
    updateTable | deleteRecords |  createTable

  protected lazy val loadManagement: Parser[LogicalPlan] =
    deleteLoadsByID | deleteLoadsByLoadDate | cleanFiles | loadDataNew

  protected lazy val createDatabase: Parser[LogicalPlan] =
    CREATE ~> (DATABASE | SCHEMA) ~> restInput ^^ {
      case statement =>
        val createDbSql = "CREATE DATABASE " + statement
        var dbName = ""
        // Get Ast node for create db command
        val node = HiveQlWrapper.getAst(createDbSql)
        node match {
          // get dbname
          case Token("TOK_CREATEDATABASE", children) =>
            dbName = BaseSemanticAnalyzer.unescapeIdentifier(children(0).getText)
        }
        CreateDatabase(dbName, createDbSql)
    }

  protected lazy val dropDatabase: Parser[LogicalPlan] =
    DROP ~> (DATABASE | SCHEMA) ~> restInput ^^ {
      case statement =>
        val dropDbSql = "DROP DATABASE " + statement
        var dbName = ""
        var isCascade = false
        // Get Ast node for drop db command
        val node = HiveQlWrapper.getAst(dropDbSql)
        node match {
          case Token("TOK_DROPDATABASE", children) =>
            dbName = BaseSemanticAnalyzer.unescapeIdentifier(children(0).getText)
            // check whether cascade drop db
            children.collect {
              case t@Token("TOK_CASCADE", _) =>
                isCascade = true
              case _ => // Unsupport features
            }
        }
        DropDatabase(dbName, isCascade, dropDbSql)
    }

  protected lazy val alterTable: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> restInput ^^ {
      case statement =>
        try {
          val alterSql = "alter table " + statement
          // DDl will be parsed and we get the AST tree from the HiveQl
          val node = HiveQlWrapper.getAst(alterSql)
          // processing the AST tree
          nodeToPlanForAlterTable(node, alterSql)
        } catch {
          // MalformedCarbonCommandException need to be throw directly, parser will catch it
          case ce: MalformedCarbonCommandException =>
            throw ce
        }
    }

  /**
   * For handling the create table DDl systax compatible to Hive syntax
   */
  protected lazy val createTable: Parser[LogicalPlan] =
  restInput ^^ {

    case statement =>
      try {
        // DDl will be parsed and we get the AST tree from the HiveQl
        val node = HiveQlWrapper.getAst(statement)
        // processing the AST tree
        nodeToPlan(node)
      } catch {
        // MalformedCarbonCommandException need to be throw directly, parser will catch it
        case ce: MalformedCarbonCommandException =>
          throw ce
        case e: Exception =>
          sys.error("Parsing error") // no need to do anything.
      }
  }

  /**
   * This function will traverse the tree and logical plan will be formed using that.
   *
   * @param node
   * @return LogicalPlan
   */
  protected def nodeToPlan(node: Node): LogicalPlan = {
    node match {
      // if create table taken is found then only we will handle.
      case Token("TOK_CREATETABLE", children) =>


        var fields: Seq[Field] = Seq[Field]()
        var tableComment: String = ""
        var tableProperties = Map[String, String]()
        var partitionCols: Seq[PartitionerField] = Seq[PartitionerField]()
        var likeTableName: String = ""
        var storedBy: String = ""
        var ifNotExistPresent: Boolean = false
        var dbName: Option[String] = None
        var tableName: String = ""
        var bucketFields: Option[BucketFields] = None

        try {

          // Checking whether create table request is carbon table
          children.collect {
            case Token("TOK_STORAGEHANDLER", child :: Nil) =>
              storedBy = BaseSemanticAnalyzer.unescapeSQLString(child.getText).trim.toLowerCase
            case _ =>
          }
          if (!(storedBy.equals(CarbonContext.datasourceName) ||
                storedBy.equals(CarbonContext.datasourceShortName))) {
            sys.error("Not a carbon format request")
          }

          children.collect {
            // collecting all the field  list
            case list@Token("TOK_TABCOLLIST", _) =>
              val cols = BaseSemanticAnalyzer.getColumns(list, true)
              if (cols != null) {
                val dupColsGrp = cols.asScala.groupBy(x => x.getName) filter {
                  case (_, colList) => colList.size > 1
                }
                if (dupColsGrp.nonEmpty) {
                  var columnName: String = ""
                  dupColsGrp.toSeq.foreach(columnName += _._1 + ", ")
                  columnName = columnName.substring(0, columnName.lastIndexOf(", "))
                  val errorMessage = "Duplicate column name: " + columnName + " found in table " +
                                     ".Please check create table statement."
                  throw new MalformedCarbonCommandException(errorMessage)
                }
                cols.asScala.map { col =>
                  val columnName = col.getName()
                  val dataType = Option(col.getType)
                  val name = Option(col.getName())
                  // This is to parse complex data types
                  val x = col.getName + ' ' + col.getType
                  val f: Field = anyFieldDef(new lexical.Scanner(x))
                  match {
                    case Success(field, _) => field
                    case failureOrError => throw new MalformedCarbonCommandException(
                      s"Unsupported data type: $col.getType")
                  }
                  // the data type of the decimal type will be like decimal(10,0)
                  // so checking the start of the string and taking the precision and scale.
                  // resetting the data type with decimal
                  if (f.dataType.getOrElse("").startsWith("decimal")) {
                    val (precision, scale) = getScaleAndPrecision(col.getType)
                    f.precision = precision
                    f.scale = scale
                    f.dataType = Some("decimal")
                  }
                  if(f.dataType.getOrElse("").startsWith("char")) {
                    f.dataType = Some("char")
                  }
                  f.rawSchema = x
                  fields ++= Seq(f)
                }
              }

            case Token("TOK_IFNOTEXISTS", _) =>
              ifNotExistPresent = true

            case t@Token("TOK_TABNAME", _) =>
              val (db, tblName) = extractDbNameTableName(t)
              dbName = db
              tableName = tblName.toLowerCase()

            case Token("TOK_TABLECOMMENT", child :: Nil) =>
              tableComment = BaseSemanticAnalyzer.unescapeSQLString(child.getText)

            case Token("TOK_TABLEPARTCOLS", list@Token("TOK_TABCOLLIST", _) :: Nil) =>
              val cols = BaseSemanticAnalyzer.getColumns(list(0), false)
              if (cols != null) {
                cols.asScala.map { col =>
                  val columnName = col.getName()
                  val dataType = Option(col.getType)
                  val comment = col.getComment
                  val partitionCol = new PartitionerField(columnName, dataType, comment)
                  partitionCols ++= Seq(partitionCol)
                }
              }
            case Token("TOK_TABLEPROPERTIES", list :: Nil) =>
              val propertySeq: Seq[(String, String)] = getProperties(list)
              val repeatedProperties = propertySeq.groupBy(_._1).filter(_._2.size > 1).keySet
              if (repeatedProperties.nonEmpty) {
                val repeatedPropStr: String = repeatedProperties.mkString(",")
                throw new MalformedCarbonCommandException("Table properties is repeated: " +
                                                          repeatedPropStr)
              }
              tableProperties ++= propertySeq

            case Token("TOK_LIKETABLE", child :: Nil) =>
              likeTableName = child.getChild(0).getText()
            case Token("TOK_ALTERTABLE_BUCKETS",
                  Token("TOK_TABCOLNAME", list)::numberOfBuckets) =>
              val cols = list.map(_.getText)
              if (cols != null) {
                bucketFields = Some(BucketFields(cols,
                  numberOfBuckets.head.getText.toInt))
              }

            case _ => // Unsupport features
          }


          // validate tblProperties
          if (!CommonUtil.validateTblProperties(tableProperties, fields)) {
            throw new MalformedCarbonCommandException("Invalid table properties")
          }
          // prepare table model of the collected tokens
          val tableModel: TableModel = prepareTableModel(ifNotExistPresent,
            dbName,
            tableName,
            fields,
            partitionCols,
            tableProperties,
            bucketFields)

          // get logical plan.
          CreateTable(tableModel)
        } catch {
          case ce: MalformedCarbonCommandException =>
            val message = if (tableName.isEmpty) {
              "Create table command failed. "
            }
            else if (dbName.isEmpty) {
              s"Create table command failed for $tableName. "
            }
            else {
              s"Create table command failed for ${ dbName.get }.$tableName. "
            }
            LOGGER.audit(message + ce.getMessage)
            throw ce
        }

    }
  }

  /**
   * This function will traverse the tree and logical plan will be formed using that.
   *
   * @param node
   * @return LogicalPlan
   */
  protected def nodeToPlanForAlterTable(node: Node, alterSql: String): LogicalPlan = {
    node match {
      // if create table taken is found then only we will handle.
      case Token("TOK_ALTERTABLE", children) =>

        var dbName: Option[String] = None
        var tableName: String = ""
        var compactionType: String = ""

        children.collect {

          case t@Token("TOK_TABNAME", _) =>
            val (db, tblName) = extractDbNameTableName(t)
            dbName = db
            tableName = tblName

          case Token("TOK_ALTERTABLE_COMPACT", child :: Nil) =>
            compactionType = BaseSemanticAnalyzer.unescapeSQLString(child.getText)

          case _ => // Unsupport features
        }

        val altertablemodel = AlterTableModel(dbName, tableName, compactionType, alterSql)
        AlterTableCompaction(altertablemodel)
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
        LoadTable(databaseNameOp, tableName, filePath, Seq(), optionsMap,
          isOverwrite.isDefined)
    }

  protected lazy val describeTable: Parser[LogicalPlan] =
    ((DESCRIBE | DESC) ~> opt(EXTENDED | FORMATTED)) ~ (ident <~ ".").? ~ ident ^^ {
      case ef ~ db ~ tbl =>
        val tblIdentifier = db match {
          case Some(dbName) =>
            TableIdentifier(tbl.toLowerCase, Some(dbName))
          case None =>
            TableIdentifier(tbl.toLowerCase, None)
        }
        if (ef.isDefined && "FORMATTED".equalsIgnoreCase(ef.get)) {
          new DescribeFormattedCommand("describe formatted " + tblIdentifier,
            tblIdentifier)
        } else {
          new DescribeCommand(UnresolvedRelation(tblIdentifier, None), ef.isDefined)
        }
    }

  protected lazy val showLoads: Parser[LogicalPlan] =
    SHOW ~> SEGMENTS ~> FOR ~> TABLE ~> (ident <~ ".").? ~ ident ~
    (LIMIT ~> numericLit).? <~
    opt(";") ^^ {
      case databaseName ~ tableName ~ limit =>
        ShowLoadsCommand(databaseName, tableName.toLowerCase(), limit)
    }

  protected lazy val deleteLoadsByID: Parser[LogicalPlan] =
    DELETE ~> SEGMENT ~> repsep(segmentId, ",") ~ (FROM ~> TABLE ~>
                                                   (ident <~ ".").? ~ ident) <~
    opt(";") ^^ {
      case loadids ~ table => table match {
        case databaseName ~ tableName =>
          DeleteLoadsById(loadids, databaseName, tableName.toLowerCase())
      }
    }

  protected lazy val deleteLoadsByLoadDate: Parser[LogicalPlan] =
    DELETE ~> SEGMENTS ~> FROM ~> TABLE ~> (ident <~ ".").? ~ ident ~
    (WHERE ~> (STARTTIME <~ BEFORE) ~ stringLit) <~
    opt(";") ^^ {
      case schema ~ table ~ condition =>
        condition match {
          case dateField ~ dateValue =>
            DeleteLoadsByLoadDate(schema, table.toLowerCase(), dateField, dateValue)
        }
    }

  protected lazy val cleanFiles: Parser[LogicalPlan] =
    CLEAN ~> FILES ~> FOR ~> TABLE ~> (ident <~ ".").? ~ ident <~ opt(";") ^^ {
      case databaseName ~ tableName => CleanFiles(databaseName, tableName.toLowerCase())
    }

  protected lazy val explainPlan: Parser[LogicalPlan] =
    (EXPLAIN ~> opt(EXTENDED)) ~ startCommand ^^ {
      case isExtended ~ logicalPlan =>
        logicalPlan match {
          case plan: CreateTable => ExplainCommand(logicalPlan, extended = isExtended.isDefined)
          case _ => ExplainCommand(OneRowRelation)
        }
    }

}
