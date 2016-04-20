/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
  *
  */
package org.apache.spark.sql

import java.util.regex.{Matcher, Pattern}

import org.apache.hadoop.hive.ql.lib.Node
import org.apache.hadoop.hive.ql.parse._
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.{SqlLexical, _}
import org.apache.spark.sql.cubemodel.{DimensionRelation, _}
import org.apache.spark.sql.execution.datasources.DescribeCommand
import org.apache.spark.sql.hive.HiveQlWrapper

import scala.collection.JavaConversions._
import scala.language.implicitConversions


/**
  * Parser for All Carbon DDL, DML cases in Unified context
  */
class CarbonSqlDDLParser()
  extends AbstractSparkSQLParser with Logging {

  protected val AGGREGATE = Keyword("AGGREGATE")
  protected val AS = Keyword("AS")
  protected val AGGREGATION = Keyword("AGGREGATION")
  protected val ALL = Keyword("ALL")
  protected val HIGH_CARDINALITY_DIMS = Keyword("NO_DICTIONARY")
  protected val BEFORE = Keyword("BEFORE")
  protected val BY = Keyword("BY")
  protected val CARDINALITY = Keyword("CARDINALITY")
  protected val CLASS = Keyword("CLASS")
  protected val CLEAN = Keyword("CLEAN")
  protected val COLS = Keyword("COLS")
  protected val COLUMNS = Keyword("COLUMNS")
  protected val CREATE = Keyword("CREATE")
  protected val CUBE = Keyword("CUBE")
  protected val CUBES = Keyword("CUBES")
  protected val DATA = Keyword("DATA")
  protected val DATABASES = Keyword("DATABASES")
  protected val DELETE = Keyword("DELETE")
  protected val DELIMITER = Keyword("DELIMITER")
  protected val DESCRIBE = Keyword("DESCRIBE")
  protected val DESC = Keyword("DESC")
  protected val DETAIL = Keyword("DETAIL")
  protected val DIMENSIONS = Keyword("DIMENSIONS")
  protected val DIMFOLDERPATH = Keyword("DIMFOLDERPATH")
  protected val DOUBLE = Keyword("DOUBLE")
  protected val DROP = Keyword("DROP")
  protected val ESCAPECHAR = Keyword("ESCAPECHAR")
  protected val EXCLUDE = Keyword("EXCLUDE")
  protected val EXTENDED = Keyword("EXTENDED")
  protected val FORMATTED = Keyword("FORMATTED")
  protected val FACT = Keyword("FACT")
  protected val FIELDS = Keyword("FIELDS")
  protected val FILEHEADER = Keyword("FILEHEADER")
  protected val FILES = Keyword("FILES")
  protected val FROM = Keyword("FROM")
  protected val HIERARCHIES = Keyword("HIERARCHIES")
  protected val IN = Keyword("IN")
  protected val INCLUDE = Keyword("INCLUDE")
  protected val INPATH = Keyword("INPATH")
  protected val INT = Keyword("INT")
  protected val INTEGER = Keyword("INTEGER")
  protected val INTO = Keyword("INTO")
  protected val LEVELS = Keyword("LEVELS")
  protected val LIKE = Keyword("LIKE")
  protected val LOAD = Keyword("LOAD")
  protected val LOADS = Keyword("LOADS")
  protected val LOCAL = Keyword("LOCAL")
  protected val LONG = Keyword("LONG")
  protected val MAPPED = Keyword("MAPPED")
  protected val MEASURES = Keyword("MEASURES")
  protected val MULTILINE = Keyword("MULTILINE")
  protected val COMPLEX_DELIMITER_LEVEL_1 = Keyword("COMPLEX_DELIMITER_LEVEL_1")
  protected val COMPLEX_DELIMITER_LEVEL_2 = Keyword("COMPLEX_DELIMITER_LEVEL_2")
  protected val NUMERIC = Keyword("NUMERIC")
  protected val ARRAY = Keyword("ARRAY")
  protected val STRUCT = Keyword("STRUCT")
  protected val BIGINT = Keyword("BIGINT")
  protected val DECIMAL = Keyword("DECIMAL")
  protected val OPTIONS = Keyword("OPTIONS")
  protected val OUTPATH = Keyword("OUTPATH")
  protected val OVERWRITE = Keyword("OVERWRITE")
  protected val PARTITION_COUNT = Keyword("PARTITION_COUNT")
  protected val PARTITIONDATA = Keyword("PARTITIONDATA")
  protected val PARTITIONER = Keyword("PARTITIONER")
  protected val QUOTECHAR = Keyword("QUOTECHAR")
  protected val RELATION = Keyword("RELATION")
  protected val SCHEMAS = Keyword("SCHEMAS")
  protected val SHOW = Keyword("SHOW")
  protected val STRING = Keyword("STRING")
  protected val TABLES = Keyword("TABLES")
  protected val TABLE = Keyword("TABLE")
  protected val TERMINATED = Keyword("TERMINATED")
  protected val TIMESTAMP = Keyword("TIMESTAMP")
  protected val TYPE = Keyword("TYPE")
  protected val USE = Keyword("USE")
  protected val WHERE = Keyword("WHERE")
  protected val WITH = Keyword("WITH")
  protected val AGGREGATETABLE = Keyword("AGGREGATETABLE")
  protected val SUM = Keyword("sum")
  protected val COUNT = Keyword("count")
  protected val AVG = Keyword("avg")
  protected val MAX = Keyword("max")
  protected val MIN = Keyword("min")
  protected val DISTINCT = Keyword("distinct")
  protected val DISTINCT_COUNT = Keyword("distinct-count")
  protected val SUM_DISTINCT = Keyword("sum-distinct")
  protected val ABS = Keyword("abs")

  //added for suggest aggregate table command
  protected val SUGGEST = Keyword("SUGGEST")
  protected val FOR = Keyword("FOR")
  protected val QUERY_STATS = Keyword("QUERY_STATS")
  protected val DATA_STATS = Keyword("DATA_STATS")
  protected val SCRIPTS = Keyword("SCRIPTS")
  protected val USING = Keyword("USING")
  protected val LIMIT = Keyword("LIMIT")
  protected val DEFAULTS = Keyword("DEFAULTS")
  protected val ALTER = Keyword("ALTER")
  protected val ADD = Keyword("ADD")

  protected val IF = Keyword("IF")
  protected val NOT = Keyword("NOT")
  protected val EXISTS = Keyword("EXISTS")
  protected val DIMENSION = Keyword("DIMENSION")

  protected val doubleQuotedString = "\"([^\"]+)\"".r
  protected val singleQuotedString = "'([^']+)'".r

  protected val newReservedWords =
    this.getClass
      .getMethods
      .filter(_.getReturnType == classOf[Keyword])
      .map(_.invoke(this).asInstanceOf[Keyword].str)

  override val lexical = {
    val sqllex = new SqlLexical()
    sqllex.initialize(newReservedWords)
    sqllex

  }

  override protected lazy val start: Parser[LogicalPlan] =
    createCube | showCreateCube | loadManagement | createAggregateTable | describeTable |
      suggestAggregates | showCube | showLoads | alterCube | showAllCubes | createTable

  protected lazy val loadManagement: Parser[LogicalPlan] = loadData | dropCubeOrTable |
    deleteLoadsByID | deleteLoadsByDate | cleanFiles

  protected lazy val suggestAggregates: Parser[LogicalPlan] =
  //SUGGEST AGGREGATES WITH SCRIPTS USING (DATA_STATS,QUERY STATS) FOR SCHEMA_NAME.CUBE_NAME
    SUGGEST ~> AGGREGATE ~> (WITH ~> SCRIPTS).? ~ (USING ~> (DATA_STATS | QUERY_STATS)).? ~ (FOR ~> CUBE ~> (ident <~ ".").? ~ ident) <~ opt(";") ^^ {
      case scriptsKey ~ usingStats ~ schemaDef => {
        val (schemaname, cubename) = schemaDef match {
          case schemaName ~ cubeName => (schemaName, cubeName)
        }
        SuggestAggregateCommand(scriptsKey, usingStats, schemaname, cubename)
      }
    }
  protected lazy val createAggregateTable: Parser[LogicalPlan] =
    CREATE ~> AGGREGATETABLE ~>
      (aggregates) ~
      (FROM ~> CUBE ~> (ident <~ ".").? ~ ident) <~ opt(";") ^^ {
      case aggregates ~ cube =>
        cube match {
          case schemaName ~ cubeName =>
            AddAggregatesToCube(schemaName, cubeName, aggregates)
        }
    }

  protected lazy val aggregates: Parser[Seq[AggregateTableAttributes]] =
    repsep((aggregateExpression | aggAttribute), ",")

  protected lazy val aggAttribute: Parser[AggregateTableAttributes] =
    (ident | stringLit) ^^ {
      case e1 => AggregateTableAttributes(e1)
    }

  protected lazy val aggregateExpression: Parser[AggregateTableAttributes] =
    (SUM ~> "(" ~> (ident | stringLit) <~ ")" ^^ { case e1 => AggregateTableAttributes(e1, SUM.str) } |
      COUNT ~> "(" ~> (ident | stringLit) <~ ")" ^^ { case e1 => AggregateTableAttributes(e1, COUNT.str) } |
      MAX ~> "(" ~> (ident | stringLit) <~ ")" ^^ { case e1 => AggregateTableAttributes(e1, MAX.str) } |
      MIN ~> "(" ~> (ident | stringLit) <~ ")" ^^ { case e1 => AggregateTableAttributes(e1, MIN.str) } |
      COUNT ~> "(" ~> DISTINCT ~> (ident | stringLit) <~ ")" ^^ { case e1 => AggregateTableAttributes(e1,
        DISTINCT_COUNT.str)
      } |
      DISTINCT ~> COUNT ~> "(" ~> (ident | stringLit) <~ ")" ^^ { case e1 => AggregateTableAttributes(e1,
        COUNT.str)
      } |
      SUM ~> "(" ~> DISTINCT ~> (ident | stringLit) <~ ")" ^^ { case e1 => AggregateTableAttributes(e1,
        SUM_DISTINCT.str)
      } |
      AVG ~> "(" ~> (ident | stringLit) <~ ")" ^^ { case e1 => AggregateTableAttributes(e1, AVG.str) }
      )

  protected lazy val defaultExpr =
    (ident | stringLit) ~ ("=" ~> (ident | stringLit | numericLit)) ^^ {
      case e1 ~ e2 => {
        Default(e1, e2.toString())
      }
    }

  protected lazy val defaultVals =
    rep1sep(defaultExpr, ",")

  protected lazy val defaultDefn =
    DEFAULTS ~> ("[" ~> defaultVals <~ "]")

  protected lazy val defaultOptions =
    ("(" ~> (noDictionaryDims).? ~ (aggregation).? ~ defaultDefn.? <~ ")")

  protected lazy val dropDefinition =
    DROP ~> "(" ~> rep1sep((stringLit | ident), ",") <~ ")"

  protected lazy val addDefinition =
    (ADD ~> cubeDefinition)

  protected lazy val cubeDefinition =
    ((DIMENSIONS ~> "(" ~> dimCols <~ ")").? ~
      (MEASURES ~> "(" ~> measureCols <~ ")").? ~
      (WITH ~ (simpleDimRelations)).?)

  protected lazy val showCubeDefinition =
    ((DIMENSIONS ~> "(" ~> dimCols <~ ")").? ~
      (MEASURES ~> "(" ~> measureCols <~ ")").? ~
      ((FACT ~> FROM ~ (dbTableIdentifier | stringLit) ~ (colsFilter).? ~ ("," ~> DIMENSION ~> FROM ~> dimRelations).?).?) ~
      (WITH ~ (simpleDimRelations)).?)

  protected lazy val aggOptionsForShowCreate =
    (aggregation).? ~ (",".? ~> partitioner).?
  protected lazy val aggOptions =
    (noDictionaryDims).? ~ (",".? ~> aggregation).? ~ (",".? ~> partitioner).?
  protected lazy val showcreateCubeOptionDef =
    ("(" ~> aggOptionsForShowCreate <~ ")")

  protected lazy val createCubeOptionDef =
    ("(" ~> aggOptions <~ ")")

  protected val escapedIdentifier = "`([^`]+)`".r


  protected lazy val showCreateCube: Parser[LogicalPlan] =
    SHOW ~> CREATE ~> CUBE ~> (IF ~> NOT ~> EXISTS).? ~ (ident <~ ".").? ~ ident ~
      showCubeDefinition ~
      (OPTIONS ~> showcreateCubeOptionDef).? <~ (";").? ^^ {
      case exists ~ schemaName ~ cubeName ~ cubeDefinition ~ options => {
        val (dimCols, msrCols, fromKeyword, withKeyword, source, factFieldsList, dimRelations, simpleDimRelations) =
          cubeDefinition match {
          case _ ~ _ ~ Some(_) ~ Some(_) => sys.error("FROM and WITH keywords can not be used together")

          case dimCols ~ msrCols ~ fromBody ~ withBody => {
            val (fromKeyword, source, factFieldsList, dimRelations) = fromBody match {
              case Some(fromKeyword ~ source ~ factFieldsList ~ dimRelations) =>
                (fromKeyword, source, factFieldsList, dimRelations)

              case _ => ("", "", None, None)
            }

            val (withKeyword, simpleDimRelations) = withBody match {
              case Some(withKeyword ~ simpleDimRelations) => (withKeyword, simpleDimRelations)
              case _ => ("", Seq())
            }

            (dimCols.getOrElse(Seq()), msrCols.getOrElse(Seq()), fromKeyword, withKeyword, source, factFieldsList,
              dimRelations.getOrElse(Seq()), simpleDimRelations)
          }
        }

        val (aggregation, partitioner) = options match {
          case Some(aggregation ~ partitioner) => (aggregation.getOrElse(Seq()), partitioner)
          case _ => (Seq(), None)
        }

        ShowCreateCubeCommand(tableModel(exists.isDefined,
          schemaName.getOrElse("default"), schemaName, cubeName, dimCols.map(f => normalizeType(f)).map(f => addParent(f)),
          msrCols.map(f => normalizeType(f)), fromKeyword, withKeyword, source,
          factFieldsList, dimRelations, simpleDimRelations, None, aggregation, partitioner, null))
      }
    }

  /**
    * For handling the create table DDl systax compatible to Hive syntax
    */
  protected lazy val createTable: Parser[LogicalPlan] =
    restInput ^^ {

      case statement => {
        try {
          //DDl will be parsed and we get the AST tree from the HiveQl
          val node = HiveQlWrapper.getAst(statement)
          // processing the AST tree
          nodeToPlan(node)
        } catch {
          case e: Exception => sys.error("Parsing error") // no need to do anything.
        }
      }
    }

  /**
    * This function will traverse the tree and logical plan will be formed using that.
    *
    * @param node
    * @return LogicalPlan
    */
  protected def nodeToPlan(node: Node): LogicalPlan = node match {
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

      children.collect {
        // collecting all the field  list
        case list@Token("TOK_TABCOLLIST", _) =>
          val cols = BaseSemanticAnalyzer.getColumns(list, true)
          if (cols != null) {

            cols.map { col =>
              val columnName = col.getName()
              val dataType = Option(col.getType)
              val comment = col.getComment
              val name = Option(col.getName())
              val f: Field = new Field(columnName, dataType, name, None, null, Some("columnar"))
              fields ++= Seq(f)
            }
          }

        case Token("TOK_IFNOTEXISTS", _) =>
          ifNotExistPresent = true

        case t@Token("TOK_TABNAME", _) =>
          val (db, tblName) = extractDbNameTableName(t)
          dbName = db
          tableName = tblName

        case Token("TOK_TABLECOMMENT", child :: Nil) =>
          tableComment = BaseSemanticAnalyzer.unescapeSQLString(child.getText)

        case Token("TOK_TABLEPARTCOLS", list@Token("TOK_TABCOLLIST", _) :: Nil) =>
          val cols = BaseSemanticAnalyzer.getColumns(list(0), false)
          if (cols != null) {
            cols.map { col =>
              val columnName = col.getName()
              val dataType = Option(col.getType)
              val comment = col.getComment
              //  HiveColumn(field.getName, field.getType, field.getComment)
              val partitionCol = new PartitionerField(columnName, dataType, comment)
              partitionCols ++= Seq(partitionCol)
            }
          }
        case Token("TOK_TABLEPROPERTIES", list :: Nil) =>
          tableProperties ++= getProperties(list)

        case Token("TOK_LIKETABLE", child :: Nil) =>
          likeTableName = child.getChild(0).getText()

        case Token("TOK_STORAGEHANDLER", child :: Nil) =>
          storedBy = BaseSemanticAnalyzer.unescapeSQLString(child.getText)

        case _ => // Unsupport features
      }

      if (!("org.apache.carbondata.format".equalsIgnoreCase(storedBy))) {
        sys.error("Not a carbon Format request")
      }

      // prepare table model of the collected tokens
      val tableModel: tableModel = prepareTableModel(ifNotExistPresent, dbName, tableName, fields, partitionCols,
        tableProperties)

      // get logical plan.
      CreateCube(tableModel)

  }

  /**
    *  This will prepate the Model from the Tree details.
    *
    * @param ifNotExistPresent
    * @param dbName
    * @param tableName
    * @param fields
    * @param partitionCols
    * @param tableProperties
    * @return
    */
  protected def prepareTableModel(ifNotExistPresent: Boolean, dbName: Option[String]
                                  , tableName: String, fields: Seq[Field], partitionCols: Seq[PartitionerField],
                                  tableProperties: Map[String, String]): tableModel
  = {

    // get column groups configuration from table properties.
    val groupCols: Seq[String] = updateColumnGroupsInField(tableProperties)

    var (dims: Seq[Field], noDictionaryDims: Seq[String]) = extractDimColsAndNoDictionaryFields(fields, tableProperties)
    val msrs: Seq[Field] = extractMsrColsFromFields(fields, tableProperties)


    val partitioner: Option[Partitioner] = getPartitionerObject(partitionCols, tableProperties)

    tableModel(ifNotExistPresent,
      dbName.getOrElse("default"), dbName, tableName, dims,
      msrs, "", null, "",
      None, Seq(), null, Option(noDictionaryDims), null, partitioner, groupCols)
  }

  /**
    * Extract the column groups configuration from table properties.
    * Based on this Row groups of fields will be determined.
    *
    * @param tableProperties
    * @param fields
    * @return
    */
  protected def updateColumnGroupsInField(tableProperties: Map[String, String]): Seq[String] = {
    if (None != tableProperties.get("COLUMN_GROUPS")) {

      var splittedColGrps: Seq[String] = Seq[String]()
      val nonSplitCols: String = tableProperties.get("COLUMN_GROUPS").get

      // row groups will be specified in table properties like -> "(col1,col2),(col3,col4)"
      // here first splitting the value by () . so that the above will be splitted into 2 strings.
      // [col1,col2] [col3,col4]
      val m: Matcher = Pattern.compile("\\(([^)]+)\\)").matcher(nonSplitCols)
      while (m.find()) {
        val oneGroup: String = m.group(1)

        splittedColGrps :+= oneGroup
      }
      // This will  be furthur handled.
      splittedColGrps
    }
    else {
      null
    }
  }

  /**
    * For getting the partitioner Object
    *
    * @param partitionCols
    * @param tableProperties
    * @return
    */
  protected def getPartitionerObject(partitionCols: Seq[PartitionerField], tableProperties: Map[String, String]):
  Option[Partitioner] = {

    // by default setting partition class empty. later in cube schema it is setting to default value.
    var partitionClass: String = ""
    var partitionCount: Int = 1
    var partitionColNames: Array[String] = Array[String]()
    if (None != tableProperties.get("PARTITIONCLASS")) {
      partitionClass = tableProperties.get("PARTITIONCLASS").get
    }

    if (None != tableProperties.get("PARTITIONCOUNT")) {
      try {
        partitionCount = tableProperties.get("PARTITIONCOUNT").get.toInt
      } catch {
        case e: Exception => // no need to do anything.
      }
    }

    partitionCols.foreach(col =>
      partitionColNames :+= col.partitionColumn
    )

    // this means user has given partition cols list
    if (!partitionColNames.isEmpty) {
      return Option(Partitioner(partitionClass, partitionColNames, partitionCount, null))
    }
    // if partition cols are not given then no need to do partition.
    None
  }

  /**
    * This will extract the Dimensions and NoDictionary Dimensions fields.
    * By default all string cols are dimensions.
    *
    * @param fields
    * @param tableProperties
    * @return
    */
  protected def extractDimColsAndNoDictionaryFields(fields: Seq[Field],
                                                    tableProperties: Map[String, String]): (Seq[Field], Seq[String]) = {
    var dimFields: Set[Field] = Set[Field]()
    var splittedCols: Array[String] = Array[String]()
    var noDictionaryDims: Seq[String] = Seq[String]()

    // check any column need to be excluded
    if (None != tableProperties.get("DICTIONARY_EXCLUDE")) {
      val dicExcludeCols: String = tableProperties.get("DICTIONARY_EXCLUDE").get
      splittedCols = dicExcludeCols.split(',')
    }

    // by default consider all String cols as dims and if any dictionary exclude is present then
    // add it to nodictionarydims list.
    fields.foreach(field => {
      if (field.dataType.get.equalsIgnoreCase("string")) {
        var isNoDictionary = false
        if (!splittedCols.isEmpty) {
          splittedCols.foreach(excludedCol =>
            if (field.column.equalsIgnoreCase(excludedCol)) {
              noDictionaryDims :+= field.column
              isNoDictionary = true
            }
          )
        }
        if (!isNoDictionary) {
          dimFields += (field)
        }
      }
    })


    // include the other columns into dims
    if (None != tableProperties.get("DICTIONARY_INCLUDE")) {
      val dicIncludeCols: String = tableProperties.get("DICTIONARY_INCLUDE").get
      val splittedCols = dicIncludeCols.split(',')
      fields.foreach(field => {
        splittedCols.foreach(col => {
          if (field.column.equalsIgnoreCase(col)) {
            dimFields += field
          }
        })
      })
    }

    (dimFields.toSeq, noDictionaryDims)
  }


  /**
    * Extract the Measure Cols fields. By default all non string cols will be measures.
    *
    * @param fields
    * @param tableProperties
    * @return
    */
  protected def extractMsrColsFromFields(fields: Seq[Field], tableProperties: Map[String, String]): Seq[Field] = {
    var msrFields: Seq[Field] = Seq[Field]()
    var splittedCols: Array[String] = Array[String]()

    // check any column need to be excluded
    if (None != tableProperties.get("DICTIONARY_INCLUDE")) {
      val dicIncludeCols: String = tableProperties.get("DICTIONARY_INCLUDE").get
      splittedCols = dicIncludeCols.split(',')
    }

    // by default consider all non string cols as msrs.
    fields.foreach(field => {
      if (!field.dataType.get.equalsIgnoreCase("string")) {
        if (!splittedCols.isEmpty) {
          splittedCols.foreach(dicIncludedCols =>
            if (!field.column.equalsIgnoreCase(dicIncludedCols)) {
              msrFields :+= field
            }
          )
        }
        else {
          msrFields :+= field
        }
      }
    })

    msrFields
  }

  /**
    * Extract the DbName and table name.
    *
    * @param tableNameParts
    * @return
    */
  protected def extractDbNameTableName(tableNameParts: Node): (Option[String], String) = {
    val (db, tableName) =
      tableNameParts.getChildren.map { case Token(part, Nil) => cleanIdentifier(part) } match {
        case Seq(tableOnly) => (None, tableOnly)
        case Seq(databaseName, table) => (Some(databaseName), table)
      }

    (db, tableName)
  }

  protected def cleanIdentifier(ident: String): String = ident match {
    case escapedIdentifier(i) => i
    case plainIdent => plainIdent
  }

  protected def getClauses(clauseNames: Seq[String], nodeList: Seq[ASTNode]): Seq[Option[Node]] = {
    var remainingNodes = nodeList
    val clauses = clauseNames.map { clauseName =>
      val (matches, nonMatches) = remainingNodes.partition(_.getText.toUpperCase == clauseName)
      remainingNodes = nonMatches ++ (if (matches.nonEmpty) matches.tail else Nil)
      matches.headOption
    }

    if (remainingNodes.nonEmpty) {
      sys.error(
        s"""Unhandled clauses:
            |You are likely trying to use an unsupported carbon feature."""".stripMargin)
    }
    clauses
  }

  object Token {
    /** @return matches of the form (tokenName, children). */
    def unapply(t: Any): Option[(String, Seq[ASTNode])] = t match {
      case t: ASTNode =>
        CurrentOrigin.setPosition(t.getLine, t.getCharPositionInLine)
        Some((t.getText,
          Option(t.getChildren).map(_.toList).getOrElse(Nil).asInstanceOf[Seq[ASTNode]]))
      case _ => None
    }
  }

  /**
    * Extract the table properties token
    *
    * @param node
    * @return
    */
  protected def getProperties(node: Node): Seq[(String, String)] = node match {
    case Token("TOK_TABLEPROPLIST", list) =>
      list.map {
        case Token("TOK_TABLEPROPERTY", Token(key, Nil) :: Token(value, Nil) :: Nil) =>
          (unquoteString(key) -> unquoteString(value))
      }
  }

  protected def unquoteString(str: String) = str match {
    case singleQuotedString(s) => s
    case doubleQuotedString(s) => s
    case other => other
  }

  protected lazy val createCube: Parser[LogicalPlan] =
    CREATE ~> CUBE ~> (IF ~> NOT ~> EXISTS).? ~ (ident <~ ".").? ~ ident ~
      cubeDefinition ~
      (OPTIONS ~> createCubeOptionDef).? <~ (";").? ^^ {
      case exists ~ schemaName ~ cubeName ~ cubeDefinition ~ options => {
        val (dimCols, msrCols, withKeyword, simpleDimRelations) = cubeDefinition match {

          case dimCols ~ msrCols ~ withBody => {
            val (withKeyword, simpleDimRelations) = withBody match {
              case Some(withKeyword ~ simpleDimRelations) => (withKeyword, simpleDimRelations)
              case _ => ("", Seq())
            }

            (dimCols.getOrElse(Seq()), msrCols.getOrElse(Seq()), withKeyword, simpleDimRelations)
          }
        }

        val (highCard, aggregation, partitioner) = options match {
          case Some(hc ~ agg ~ part) => (hc.getOrElse(Some(Seq())), agg.getOrElse(Seq()), part)
          case _ => (Some(Seq()), Seq(), None)
        }

        CreateCube(tableModel(exists.isDefined,
          schemaName.getOrElse("default"), schemaName, cubeName,
          dimCols.map(f => normalizeType(f)).map(f => addParent(f)),
          msrCols.map(f => normalizeType(f)), "", withKeyword, "",
          None, Seq(), simpleDimRelations, highCard, aggregation, partitioner, null))
      }
    }

  protected lazy val alterCube: Parser[LogicalPlan] =
    ALTER ~> CUBE ~> (ident <~ ".").? ~ ident ~
      (dropDefinition).? ~
      (addDefinition).? ~
      (OPTIONS ~> defaultOptions).? <~ opt(";") ^^ {
      case schemaName ~ cubeName ~ dropDefinition ~ addDefinition ~ options => {
        val (dimCols, msrCols, withKeyword, simpleDimRelations) = addDefinition match {

          case Some(dimCols ~ msrCols ~ withBody) => {
            val (withKeyword, simpleDimRelations) = withBody match {
              case Some(withKeyword ~ simpleDimRelations) => (withKeyword, simpleDimRelations)
              case _ => ("", Seq())
            }

            if (dimCols.isEmpty && msrCols.isEmpty) {
              sys.error("empty ADD definition found.Please provide the dimensions/measures to be added.")
            } else {
              (dimCols.getOrElse(Seq()), msrCols.getOrElse(Seq()), withKeyword, simpleDimRelations)
            }
          }

          case _ => {
            (Seq(), Seq(), "", Seq())
          }
        }

        val (noDictionary, aggregation, defaultVals) = options match {
          case Some(noDictionary ~ aggregation ~ defaultVals) => (noDictionary.getOrElse(Some(Seq())),
            aggregation.getOrElse(Seq()), defaultVals.getOrElse(Seq()))
          case _ => (Some(Seq()), Seq(), Seq())
        }

        val (dropCols) = dropDefinition match {
          case Some(dropCols) => (dropCols)
          case _ => (Seq())
        }

        AlterCube(tableModel(false, schemaName.getOrElse("default"), schemaName, cubeName,
          dimCols.map(f => normalizeType(f)),
          msrCols.map(f => normalizeType(f)), "", withKeyword, "",
          None, Seq(), simpleDimRelations, noDictionary, aggregation, None, null),
          dropCols, defaultVals)
      }

      case _ => {
        sys.error("Parsing error")
      }
    }


  protected lazy val loadData: Parser[LogicalPlan] =
    LOAD ~> DATA ~> FACT ~> FROM ~> stringLit ~
      (DIMENSION ~> FROM ~> repsep(tableFileMapping, ",")).? ~
      (opt(OVERWRITE) ~> INTO ~> CUBE ~> (ident <~ ".").? ~ ident) ~
      ((PARTITIONDATA | OPTIONS) ~> "(" ~> repsep(partitionOptions, ",") <~ ")") ~
      (FIELDS ~> TERMINATED ~> BY ~> stringLit).? <~ opt(";") ^^ {
      case filePath ~ dimFolderPath ~ cube ~ partionDataOptions ~ delimiter => {
        val (schema, cubename) = cube match {
          case schemaName ~ cubeName => (schemaName, cubeName)

        }
        val patitionOptionsMap = partionDataOptions.toMap
        //patitionOptionsMap.toList.foreach(a => println("Key="+a._1+"  value="+a._2))
        LoadCube(schema, cubename, filePath, dimFolderPath.getOrElse(Seq()), patitionOptionsMap)
      }
    }

  protected lazy val dbTableIdentifier: Parser[Seq[String]] =
    (ident <~ ".").? ~ (ident) ^^ {
      case databaseName ~ tableName =>
        if (databaseName.isDefined)
          Seq(databaseName.get, tableName)
        else Seq(tableName)
    }

  protected lazy val tableFileMapping: Parser[DataLoadTableFileMapping] =
    (ident <~ ":") ~ stringLit ^^ {
      case tableName ~ dataPath => DataLoadTableFileMapping(tableName, dataPath)
    }

  protected lazy val partitionOptions: Parser[(String, String)] =
    ((DELIMITER ~ stringLit) | (QUOTECHAR ~ stringLit) | (FILEHEADER ~ stringLit) |
      (ESCAPECHAR ~ stringLit) | (MULTILINE ~ stringLit) |
      (COMPLEX_DELIMITER_LEVEL_1 ~ stringLit) | (COMPLEX_DELIMITER_LEVEL_2 ~ stringLit)) ^^ {
      case opt ~ optvalue => (opt, optvalue)
      case _ => ("", "")
    }

  protected lazy val showAggregateTables: Parser[LogicalPlan] =
    SHOW ~> AGGREGATE ~> TABLES ~> (IN ~> ident).? <~ opt(";") ^^ {
      case schema =>
        ShowAggregateTablesCommand(schema)
    }

  protected lazy val showCube: Parser[LogicalPlan] =
    SHOW ~> CUBES ~> (IN ~> ident).? ~ (DETAIL).? <~ opt(";") ^^ {
      case schema ~ detail =>
        if (detail.isDefined) {
          ShowTablesDetailedCommand(schema)
        } else {
          ShowCubeCommand(schema)
        }
    }
  protected lazy val showAllCubes: Parser[LogicalPlan] =
    SHOW ~> ALL ~> CUBES <~ opt(";") ^^ {
      case _ => ShowAllCubeCommand()
    }

  protected lazy val dropCubeOrTable: Parser[LogicalPlan] =
    DROP ~> (CUBE | (AGGREGATE ~ TABLE)) ~ (IF ~> EXISTS).? ~ (ident <~ ".").? ~ ident <~ opt(";") ^^ {
      case tabletype ~ exists ~ schemaName ~ resourceName =>
        tabletype match {
          case agg ~ table => DropAggregateTableCommand(exists.isDefined, schemaName, resourceName)
          case _ => DropCubeCommand(exists.isDefined, schemaName, resourceName)
        }
    }

  protected lazy val cubeRelation: Parser[Relation] =
    RELATION ~> "(" ~> FACT ~> ("." ~> ident) ~ ("=" ~> ident) <~ ")" ^^ {
      case lcol ~ rcol => Relation(lcol, rcol)
    }

  protected lazy val simpleCubeRelation: Parser[Relation] =
    RELATION ~> "(" ~> FACT ~> ("." ~> ident) ~ ("=" ~> ident) <~ ")" ^^ {
      case lcol ~ rcol => Relation(lcol, rcol)
    }

  protected lazy val colsFilter: Parser[FilterCols] =
    (INCLUDE | EXCLUDE) ~ ("(" ~> repsep(ident | stringLit, ",") <~ ")") ^^ {
      case includeKey ~ fieldList => FilterCols(includeKey, fieldList)
    }

  protected lazy val dimRelation: Parser[DimensionRelation] =
    (ident <~ ":") ~ (dbTableIdentifier | stringLit) ~ cubeRelation ~ ((INCLUDE | EXCLUDE) ~ ("(" ~>
      repsep(ident | stringLit, ",") <~ ")")).? ^^ {
      case tableName ~ dimSource ~ relation ~ filterCols => {
        val (includeKey, fieldList) = filterCols match {
          case Some(includeKey ~ fieldList) => (includeKey, fieldList)
          case others => ("", Seq())
        }
        DimensionRelation(tableName, dimSource, relation, Some(includeKey), Some(fieldList))
      }
    }

  protected lazy val dimRelations: Parser[Seq[DimensionRelation]] = repsep(dimRelation, ",")

  protected lazy val simpleDimRelation: Parser[DimensionRelation] =
    ident ~ simpleCubeRelation ~ (INCLUDE ~> ("(" ~> repsep(ident | stringLit, ",") <~ ")")) ^^ {
      case tableName ~ relation ~ colList => {
        DimensionRelation(tableName, "", relation, Some("INCLUDE"), Some(colList))
      }
    }

  protected lazy val simpleDimRelations: Parser[Seq[DimensionRelation]] = repsep(simpleDimRelation, ",")

  protected lazy val dimCol: Parser[Field] = anyFieldDef

  protected lazy val primitiveTypes = STRING | INTEGER | TIMESTAMP | NUMERIC | BIGINT | DECIMAL
  protected lazy val nestedType: Parser[Field] = structFieldType | arrayFieldType | primitiveFieldType

  protected lazy val anyFieldDef: Parser[Field] =
    (ident | stringLit) ~ ((":").? ~> nestedType) ~ (IN ~> (ident | stringLit)).? ^^ {
      case e1 ~ e2 ~ e3 => {
        Field(e1, e2.dataType, Some(e1), e2.children, null, e3)
      }
    }

  protected lazy val primitiveFieldType: Parser[Field] =
    (primitiveTypes) ^^ {
      case e1 => {
        Field("unknown", Some(e1), Some("unknown"), Some(null))
      }
    }

  protected lazy val arrayFieldType: Parser[Field] =
    (ARRAY ~> "<" ~> nestedType <~ ">") ^^ {
      case e1 => {
        Field("unknown", Some("array"), Some("unknown"), Some(List(Field("val", e1.dataType, Some("val"),
          e1.children))))
      }
    }

  protected lazy val structFieldType: Parser[Field] =
    (STRUCT ~> "<" ~> repsep(anyFieldDef, ",") <~ ">") ^^ {
      case e1 => {
        Field("unknown", Some("struct"), Some("unknown"), Some(e1))
      }
    }

  protected lazy val measureCol: Parser[Field] =
    (ident | stringLit) ~ (INTEGER | NUMERIC | BIGINT | DECIMAL).? ~ (AS ~> (ident | stringLit)).? ~
      (IN ~> (ident | stringLit)).? ^^ {
      case e1 ~ e2 ~ e3 ~ e4 => Field(e1, e2, e3, Some(null))
    }

  protected lazy val dimCols: Parser[Seq[Field]] = rep1sep(dimCol, ",")

  protected lazy val measureCols: Parser[Seq[Field]] = rep1sep(measureCol, ",")

  protected lazy val expressions: Parser[Seq[FieldMapping]] = repsep(expsr, ",")

  protected lazy val aggExpressions: Parser[Seq[Aggregation]] = repsep(aggExpsr, ",")

  protected lazy val expsr: Parser[FieldMapping] =
    ((ident | stringLit) ~ ("=" ~> (ident | stringLit))) ^^ {
      case e1 ~ e2 => FieldMapping(e1, e2)
    }

  protected lazy val aggExpsr: Parser[Aggregation] =
    ((ident | stringLit) ~ ("=" ~> (SUM | COUNT | AVG | MIN | MAX | ABS))) ^^ {
      case e1 ~ e2 => Aggregation(e1, e2)
      case _ => sys.error("Invalid Aggregator type")
    }

  protected lazy val cardinalityExprs: Parser[Seq[Cardinality]] = repsep(numericExprs, ",")

  protected lazy val numericExprs: Parser[Cardinality] =
    ((ident | stringLit) ~ ("=" ~> numericLit)) ^^ {
      case e1 ~ e2 => Cardinality(e1, e2.toInt)
    }

  protected lazy val columns: Parser[Option[Seq[FieldMapping]]] =
    opt("(" ~> expressions <~ ")")

  protected lazy val cardinality: Parser[Option[Seq[Cardinality]]] =
    opt(CARDINALITY ~> ("[" ~> cardinalityExprs <~ "]"))

  protected lazy val aggregation: Parser[Seq[Aggregation]] =
    AGGREGATION ~> ("[" ~> aggExpressions <~ "]")
  protected lazy val noDictionaryDims: Parser[Option[Seq[String]]] =
    HIGH_CARDINALITY_DIMS ~> ("(" ~> repsep((ident | stringLit), ",") <~ ")") ^^ {
      case hc => {
        Some(hc)
      }
      case _ => None

    }

  protected lazy val partitioner: Parser[Partitioner] =
    PARTITIONER ~> "[" ~> opt(CLASS ~> "=" ~> stringLit) ~
      opt(",".? ~> COLUMNS ~> "=" ~> "(" ~> rep1sep((ident | stringLit), ",") <~ ")") ~
      (",".? ~> PARTITION_COUNT ~> "=" ~> numericLit) <~ "]" ^^ {
      case partitionerClass ~ columns ~ count =>
        Partitioner(partitionerClass.getOrElse(""),
          columns.getOrElse(List("")).toArray, count.toInt, null)
    }

  protected lazy val hierarchies: Parser[Option[Seq[HierarchyMapping]]] =
    opt(HIERARCHIES ~> ("[" ~> repsep(hierarchy, ",") <~ "]"))

  protected lazy val hierarchy: Parser[HierarchyMapping] =
    (ident ~ (TYPE ~> "=" ~> ident).?) ~
      (LEVELS ~> "{" ~> repsep((ident | stringLit), ",") <~ "}") ^^ {
      case hierName ~ hierType ~ levels =>
        var dimType = "StandardDimension"
        if (hierType.getOrElse(dimType).equalsIgnoreCase("time"))
          dimType = "TimeDimension"
        HierarchyMapping(hierName, dimType, levels)
    }

  protected lazy val describeTable: Parser[LogicalPlan] =
    ((DESCRIBE | DESC) ~> opt(EXTENDED | FORMATTED)) ~ (ident <~ ".").? ~ ident ^^ {
      case ef ~ db ~ tbl =>
        val tblIdentifier = db match {
          case Some(dbName) =>
            Seq(dbName, tbl)
          case None =>
            Seq(tbl)
        }
        if (ef.isDefined && "FORMATTED".equalsIgnoreCase(ef.get)) {
          new DescribeFormattedCommand("describe formatted " + tblIdentifier.mkString("."), tblIdentifier)
        }
        else {
          new DescribeCommand(UnresolvedRelation(tblIdentifier, None), ef.isDefined)
        }
    }

  private def normalizeType(field: Field): Field = {
    field.dataType.getOrElse("NIL") match {
      case "string" => Field(field.column, Some("String"), field.name, Some(null), field.parent, field.storeType)
      case "integer" => Field(field.column, Some("Integer"), field.name, Some(null), field.parent, field.storeType)
      case "long" => Field(field.column, Some("Long"), field.name, Some(null), field.parent, field.storeType)
      case "double" => Field(field.column, Some("Double"), field.name, Some(null), field.parent, field.storeType)
      case "timestamp" => Field(field.column, Some("Timestamp"), field.name, Some(null), field.parent, field.storeType)
      case "numeric" => Field(field.column, Some("Numeric"), field.name, Some(null), field.parent, field.storeType)
      case "array" => Field(field.column, Some("Array"), field.name, field.children.map(f => f.map(normalizeType(_))),
        field.parent, field.storeType)
      case "struct" => Field(field.column, Some("Struct"), field.name, field.children.map(f => f.map(normalizeType(_))),
        field.parent, field.storeType)
      case "bigint" => Field(field.column, Some("BigInt"), field.name, Some(null), field.parent, field.storeType)
      case "decimal" => Field(field.column, Some("Decimal"), field.name, Some(null), field.parent, field.storeType)
      case _ => field
    }
  }

  private def addParent(field: Field): Field = {
    field.dataType.getOrElse("NIL") match {
      case "Array" => Field(field.column, Some("Array"), field.name, field.children.map(f => f.map(appendParentForEachChild(_, field.column))), field.parent, field.storeType)
      case "Struct" => Field(field.column, Some("Struct"), field.name, field.children.map(f => f.map(appendParentForEachChild(_, field.column))), field.parent, field.storeType)
      case _ => field
    }
  }

  private def appendParentForEachChild(field: Field, parentName: String): Field = {
    field.dataType.getOrElse("NIL") match {
      case "String" => Field(parentName + "." + field.column, Some("String"), Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Integer" => Field(parentName + "." + field.column, Some("Integer"), Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Long" => Field(parentName + "." + field.column, Some("Long"), Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Double" => Field(parentName + "." + field.column, Some("Double"), Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Timestamp" => Field(parentName + "." + field.column, Some("Timestamp"), Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Numeric" => Field(parentName + "." + field.column, Some("Numeric"), Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Array" => Field(parentName + "." + field.column, Some("Array"), Some(parentName + "." + field.name.getOrElse(None)),
        field.children.map(f => f.map(appendParentForEachChild(_, parentName + "." + field.column))), parentName)
      case "Struct" => Field(parentName + "." + field.column, Some("Struct"), Some(parentName + "." + field.name.getOrElse(None)),
        field.children.map(f => f.map(appendParentForEachChild(_, parentName + "." + field.column))), parentName)
      case "BigInt" => Field(parentName + "." + field.column, Some("BigInt"), Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Decimal" => Field(parentName + "." + field.column, Some("Decimal"), Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case _ => field
    }
  }

  protected lazy val showLoads: Parser[LogicalPlan] =
    SHOW ~> LOADS ~> FOR ~> CUBE ~> (ident <~ ".").? ~ ident ~ (LIMIT ~> numericLit).? <~ opt(";") ^^ {
      case schemaName ~ cubeName ~ limit => ShowLoadsCommand(schemaName, cubeName, limit)
    }

  protected lazy val deleteLoadsByID: Parser[LogicalPlan] =
    DELETE ~> LOAD ~> repsep(numericLit, ",") ~ (FROM ~> CUBE ~> (ident <~ ".").? ~ ident) <~ opt(";") ^^ {
      case loadids ~ cube => cube match {
        case schemaName ~ cubeName => DeleteLoadsById(loadids, schemaName, cubeName)
      }
    }

  protected lazy val deleteLoadsByDate: Parser[LogicalPlan] =
    DELETE ~> FROM ~> CUBE ~> (ident <~ ".").? ~ ident ~ (WHERE ~> (ident <~ BEFORE) ~ stringLit) <~ opt(";") ^^ {
      case schema ~ cube ~ condition => {
        condition match {
          case dateField ~ dateValue => {
            println(schema, cube, dateField, dateValue)
            DeleteLoadByDate(schema, cube, dateField, dateValue)
          }
        }
      }
    }

  protected lazy val cleanFiles: Parser[LogicalPlan] =
    CLEAN ~> FILES ~> FOR ~> CUBE ~> (ident <~ ".").? ~ ident <~ opt(";") ^^ {
      case schemaName ~ cubeName => CleanFiles(schemaName, cubeName)
    }

}