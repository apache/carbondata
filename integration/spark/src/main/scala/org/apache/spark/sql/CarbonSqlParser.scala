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
import scala.language.implicitConversions
import scala.util.matching.Regex

import org.apache.hadoop.hive.ql.lib.Node
import org.apache.hadoop.hive.ql.parse._
import org.apache.spark.Logging
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
import org.apache.carbondata.core.util.DataTypeUtil
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.util.CommonUtil

/**
 * Parser for All Carbon DDL, DML cases in Unified context
 */
class CarbonSqlParser()
  extends AbstractSparkSQLParser with Logging {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  protected val AGGREGATE = carbonKeyWord("AGGREGATE")
  protected val AS = carbonKeyWord("AS")
  protected val AGGREGATION = carbonKeyWord("AGGREGATION")
  protected val ALL = carbonKeyWord("ALL")
  protected val HIGH_CARDINALITY_DIMS = carbonKeyWord("NO_DICTIONARY")
  protected val BEFORE = carbonKeyWord("BEFORE")
  protected val BY = carbonKeyWord("BY")
  protected val CARDINALITY = carbonKeyWord("CARDINALITY")
  protected val CASCADE = carbonKeyWord("CASCADE")
  protected val CLASS = carbonKeyWord("CLASS")
  protected val CLEAN = carbonKeyWord("CLEAN")
  protected val COLS = carbonKeyWord("COLS")
  protected val COLUMNS = carbonKeyWord("COLUMNS")
  protected val CREATE = carbonKeyWord("CREATE")
  protected val CUBE = carbonKeyWord("CUBE")
  protected val CUBES = carbonKeyWord("CUBES")
  protected val DATA = carbonKeyWord("DATA")
  protected val DATABASE = carbonKeyWord("DATABASE")
  protected val DATABASES = carbonKeyWord("DATABASES")
  protected val DELETE = carbonKeyWord("DELETE")
  protected val DELIMITER = carbonKeyWord("DELIMITER")
  protected val DESCRIBE = carbonKeyWord("DESCRIBE")
  protected val DESC = carbonKeyWord("DESC")
  protected val DETAIL = carbonKeyWord("DETAIL")
  protected val DIMENSIONS = carbonKeyWord("DIMENSIONS")
  protected val DIMFOLDERPATH = carbonKeyWord("DIMFOLDERPATH")
  protected val DROP = carbonKeyWord("DROP")
  protected val ESCAPECHAR = carbonKeyWord("ESCAPECHAR")
  protected val EXCLUDE = carbonKeyWord("EXCLUDE")
  protected val EXPLAIN = carbonKeyWord("EXPLAIN")
  protected val EXTENDED = carbonKeyWord("EXTENDED")
  protected val FORMATTED = carbonKeyWord("FORMATTED")
  protected val FACT = carbonKeyWord("FACT")
  protected val FIELDS = carbonKeyWord("FIELDS")
  protected val FILEHEADER = carbonKeyWord("FILEHEADER")
  protected val SERIALIZATION_NULL_FORMAT = carbonKeyWord("SERIALIZATION_NULL_FORMAT")
  protected val FILES = carbonKeyWord("FILES")
  protected val FROM = carbonKeyWord("FROM")
  protected val HIERARCHIES = carbonKeyWord("HIERARCHIES")
  protected val IN = carbonKeyWord("IN")
  protected val INCLUDE = carbonKeyWord("INCLUDE")
  protected val INPATH = carbonKeyWord("INPATH")
  protected val INTO = carbonKeyWord("INTO")
  protected val LEVELS = carbonKeyWord("LEVELS")
  protected val LIKE = carbonKeyWord("LIKE")
  protected val LOAD = carbonKeyWord("LOAD")
  protected val LOCAL = carbonKeyWord("LOCAL")
  protected val MAPPED = carbonKeyWord("MAPPED")
  protected val MEASURES = carbonKeyWord("MEASURES")
  protected val MULTILINE = carbonKeyWord("MULTILINE")
  protected val COMPLEX_DELIMITER_LEVEL_1 = carbonKeyWord("COMPLEX_DELIMITER_LEVEL_1")
  protected val COMPLEX_DELIMITER_LEVEL_2 = carbonKeyWord("COMPLEX_DELIMITER_LEVEL_2")
  protected val OPTIONS = carbonKeyWord("OPTIONS")
  protected val OUTPATH = carbonKeyWord("OUTPATH")
  protected val OVERWRITE = carbonKeyWord("OVERWRITE")
  protected val PARTITION_COUNT = carbonKeyWord("PARTITION_COUNT")
  protected val PARTITIONDATA = carbonKeyWord("PARTITIONDATA")
  protected val PARTITIONER = carbonKeyWord("PARTITIONER")
  protected val QUOTECHAR = carbonKeyWord("QUOTECHAR")
  protected val RELATION = carbonKeyWord("RELATION")
  protected val SCHEMA = carbonKeyWord("SCHEMA")
  protected val SCHEMAS = carbonKeyWord("SCHEMAS")
  protected val SHOW = carbonKeyWord("SHOW")
  protected val TABLES = carbonKeyWord("TABLES")
  protected val TABLE = carbonKeyWord("TABLE")
  protected val TERMINATED = carbonKeyWord("TERMINATED")
  protected val TYPE = carbonKeyWord("TYPE")
  protected val USE = carbonKeyWord("USE")
  protected val WHERE = carbonKeyWord("WHERE")
  protected val WITH = carbonKeyWord("WITH")
  protected val AGGREGATETABLE = carbonKeyWord("AGGREGATETABLE")
  protected val SUM = Keyword("sum")
  protected val COUNT = Keyword("count")
  protected val AVG = Keyword("avg")
  protected val MAX = Keyword("max")
  protected val MIN = Keyword("min")
  protected val DISTINCT = Keyword("distinct")
  protected val DISTINCT_COUNT = Keyword("distinct-count")
  protected val SUM_DISTINCT = Keyword("sum-distinct")
  protected val ABS = carbonKeyWord("abs")

  protected val FOR = carbonKeyWord("FOR")
  protected val SCRIPTS = carbonKeyWord("SCRIPTS")
  protected val USING = carbonKeyWord("USING")
  protected val LIMIT = carbonKeyWord("LIMIT")
  protected val DEFAULTS = carbonKeyWord("DEFAULTS")
  protected val ALTER = carbonKeyWord("ALTER")
  protected val ADD = carbonKeyWord("ADD")

  protected val IF = carbonKeyWord("IF")
  protected val NOT = carbonKeyWord("NOT")
  protected val EXISTS = carbonKeyWord("EXISTS")
  protected val DIMENSION = carbonKeyWord("DIMENSION")
  protected val STARTTIME = carbonKeyWord("STARTTIME")
  protected val SEGMENTS = carbonKeyWord("SEGMENTS")
  protected val SEGMENT = carbonKeyWord("SEGMENT")

  protected val STRING = carbonKeyWord("STRING")
  protected val INTEGER = carbonKeyWord("INTEGER")
  protected val TIMESTAMP = carbonKeyWord("TIMESTAMP")
  protected val NUMERIC = carbonKeyWord("NUMERIC")
  protected val DECIMAL = carbonKeyWord("DECIMAL")
  protected val DOUBLE = carbonKeyWord("DOUBLE")
  protected val INT = carbonKeyWord("INT")
  protected val BIGINT = carbonKeyWord("BIGINT")
  protected val ARRAY = carbonKeyWord("ARRAY")
  protected val STRUCT = carbonKeyWord("STRUCT")

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

  import lexical.Identifier
  implicit def regexToParser(regex: Regex): Parser[String] = acceptMatch(
    s"identifier matching regex ${regex}",
    { case Identifier(str) if regex.unapplySeq(str).isDefined => str }
  )
  override def parse(input: String): LogicalPlan = synchronized {
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

  /**
   * This will convert key word to regular expression.
   * @param keys
   * @return
   */
  private def carbonKeyWord(keys: String) =
    ("(?i)" + keys).r

  override protected lazy val start: Parser[LogicalPlan] = explainPlan | startCommand

  protected lazy val startCommand: Parser[LogicalPlan] =
    dropDatabaseCascade | loadManagement | describeTable | showLoads | alterTable | createTable

  protected lazy val loadManagement: Parser[LogicalPlan] = deleteLoadsByID | deleteLoadsByLoadDate |
    cleanFiles | loadDataNew

  protected lazy val aggregates: Parser[Seq[AggregateTableAttributes]] =
    repsep((aggregateExpression | aggAttribute), ",")

  protected lazy val aggAttribute: Parser[AggregateTableAttributes] =
    (ident | stringLit) ^^ {
      case e1 => AggregateTableAttributes(e1)
    }

  protected lazy val aggregateExpression: Parser[AggregateTableAttributes] =
    (SUM ~> "(" ~> (ident | stringLit) <~ ")" ^^
      { case e1 => AggregateTableAttributes(e1, SUM.str) } |
      COUNT ~> "(" ~> (ident | stringLit) <~ ")" ^^
        { case e1 => AggregateTableAttributes(e1, COUNT.str) } |
      MAX ~> "(" ~> (ident | stringLit) <~ ")" ^^
        { case e1 => AggregateTableAttributes(e1, MAX.str) } |
      MIN ~> "(" ~> (ident | stringLit) <~ ")" ^^
        { case e1 => AggregateTableAttributes(e1, MIN.str) } |
      COUNT ~> "(" ~> DISTINCT ~> (ident | stringLit) <~ ")" ^^
        { case e1 => AggregateTableAttributes(e1,
          DISTINCT_COUNT.str)
        } |
      DISTINCT ~> COUNT ~> "(" ~> (ident | stringLit) <~ ")" ^^
        { case e1 => AggregateTableAttributes(e1,
          COUNT.str)
        } |
      SUM ~> "(" ~> DISTINCT ~> (ident | stringLit) <~ ")" ^^
        { case e1 => AggregateTableAttributes(e1,
          SUM_DISTINCT.str)
        } |
      AVG ~> "(" ~> (ident | stringLit) <~ ")" ^^
        { case e1 => AggregateTableAttributes(e1, AVG.str) }
      )

  protected lazy val defaultExpr =
    (ident | stringLit) ~ ("=" ~> (ident | stringLit | numericLit)) ^^ {
      case e1 ~ e2 =>
        Default(e1, e2.toString())
    }

  protected lazy val defaultVals =
    rep1sep(defaultExpr, ",")

  protected lazy val defaultDefn =
    DEFAULTS ~> ("[" ~> defaultVals <~ "]")

  protected lazy val defaultOptions =
    ("(" ~> (noDictionaryDims).? ~ (aggregation).? ~ defaultDefn.? <~ ")")

  protected lazy val dropDefinition =
    DROP ~> "(" ~> rep1sep((stringLit | ident), ",") <~ ")"

  protected lazy val aggOptionsForShowCreate =
    (aggregation).? ~ (",".? ~> partitioner).?
  protected lazy val aggOptions =
    (noDictionaryDims).? ~ (",".? ~> aggregation).? ~ (",".? ~> partitioner).?
  protected lazy val showcreateTableOptionDef =
    ("(" ~> aggOptionsForShowCreate <~ ")")

  protected lazy val createTableOptionDef =
    ("(" ~> aggOptions <~ ")")

  protected val escapedIdentifier = "`([^`]+)`".r

  private def reorderDimensions(dims: Seq[Field]): Seq[Field] = {
    var complexDimensions: Seq[Field] = Seq()
    var dimensions: Seq[Field] = Seq()
    dims.foreach { dimension =>
      dimension.dataType.getOrElse("NIL") match {
        case "Array" => complexDimensions = complexDimensions:+dimension
        case "Struct" => complexDimensions = complexDimensions:+dimension
        case _ => dimensions = dimensions:+dimension
      }
    }
    dimensions ++ complexDimensions
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

  private def getScaleAndPrecision(dataType: String): (Int, Int) = {
    val m: Matcher = Pattern.compile("^decimal\\(([^)]+)\\)").matcher(dataType)
    m.find()
    val matchedString: String = m.group(1)
    val scaleAndPrecision = matchedString.split(",")
    (Integer.parseInt(scaleAndPrecision(0).trim), Integer.parseInt(scaleAndPrecision(1).trim))
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

          try {

          children.collect {
            // collecting all the field  list
            case list@Token("TOK_TABCOLLIST", _) =>
              val cols = BaseSemanticAnalyzer.getColumns(list, true)
              if (cols != null) {
                val dupColsGrp = cols.asScala.groupBy(x => x.getName) filter {
                  case (_, colList) => colList.size > 1
                }
                if (dupColsGrp.size > 0) {
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
                    case failureOrError => new Field(columnName, dataType, name, None, null,
                      Some("columnar"))
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
              tableProperties ++= getProperties(list)

            case Token("TOK_LIKETABLE", child :: Nil) =>
              likeTableName = child.getChild(0).getText()

          case Token("TOK_STORAGEHANDLER", child :: Nil) =>
            storedBy = BaseSemanticAnalyzer.unescapeSQLString(child.getText).trim.toLowerCase

            case _ => // Unsupport features
          }

        if (!(storedBy.equals(CarbonContext.datasourceName) ||
              storedBy.equals(CarbonContext.datasourceShortName))) {
          // TODO: should execute by Hive instead of error
          sys.error("Not a carbon format request")
        }

          // validate tblProperties
          if (!CommonUtil.validateTblProperties(tableProperties, fields)) {
            throw new MalformedCarbonCommandException("Invalid table properties")
          }
          // prepare table model of the collected tokens
          val tableModel: tableModel = prepareTableModel(ifNotExistPresent,
            dbName,
            tableName,
            fields,
            partitionCols,
            tableProperties)

          // get logical plan.
          CreateTable(tableModel)
        }
        catch {
          case ce: MalformedCarbonCommandException =>
            val message = if (tableName.isEmpty) "Create table command failed. "
            else if (!dbName.isDefined) s"Create table command failed for $tableName. "
            else s"Create table command failed for ${dbName.get}.$tableName. "
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

  /**
   * This will prepate the Model from the Tree details.
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
                                  , tableName: String, fields: Seq[Field],
                                  partitionCols: Seq[PartitionerField],
                                  tableProperties: Map[String, String]): tableModel
  = {

    val (dims: Seq[Field], noDictionaryDims: Seq[String]) = extractDimColsAndNoDictionaryFields(
      fields, tableProperties)
    if (dims.length == 0) {
      throw new MalformedCarbonCommandException(s"Table ${dbName.getOrElse(
        CarbonCommonConstants.DATABASE_DEFAULT_NAME)}.$tableName"
        + " can not be created without key columns. Please use DICTIONARY_INCLUDE or " +
        "DICTIONARY_EXCLUDE to set at least one key column " +
        "if all specified columns are numeric types")
    }
    val msrs: Seq[Field] = extractMsrColsFromFields(fields, tableProperties)

    // column properties
    val colProps = extractColumnProperties(fields, tableProperties)
    // get column groups configuration from table properties.
    val groupCols: Seq[String] = updateColumnGroupsInField(tableProperties,
        noDictionaryDims, msrs, dims)

    // get no inverted index columns from table properties.
    val noInvertedIdxCols = extractNoInvertedIndexColumns(fields, tableProperties)

    val partitioner: Option[Partitioner] = getPartitionerObject(partitionCols, tableProperties)

    tableModel(ifNotExistPresent,
      dbName.getOrElse("default"), dbName, tableName,
      reorderDimensions(dims.map(f => normalizeType(f)).map(f => addParent(f))),
      msrs.map(f => normalizeType(f)), "", null, "",
      None, Seq(), null, Option(noDictionaryDims), Option(noInvertedIdxCols), null, partitioner,
      groupCols, Some(colProps))
  }

  /**
   * Extract the column groups configuration from table properties.
   * Based on this Row groups of fields will be determined.
   *
   * @param tableProperties
   * @return
   */
  protected def updateColumnGroupsInField(tableProperties: Map[String, String],
      noDictionaryDims: Seq[String],
      msrs: Seq[Field],
      dims: Seq[Field]): Seq[String] = {
    if (None != tableProperties.get(CarbonCommonConstants.COLUMN_GROUPS)) {

      var splittedColGrps: Seq[String] = Seq[String]()
      val nonSplitCols: String = tableProperties.get(CarbonCommonConstants.COLUMN_GROUPS).get

      // row groups will be specified in table properties like -> "(col1,col2),(col3,col4)"
      // here first splitting the value by () . so that the above will be splitted into 2 strings.
      // [col1,col2] [col3,col4]
      val m: Matcher = Pattern.compile("\\(([^)]+)\\)").matcher(nonSplitCols)
      while (m.find()) {
        val oneGroup: String = m.group(1)
        CommonUtil.validateColumnGroup(oneGroup, noDictionaryDims, msrs, splittedColGrps, dims)
        val arrangedColGrp = rearrangedColumnGroup(oneGroup, dims)
        splittedColGrps :+= arrangedColGrp
      }
      // This will  be furthur handled.
      CommonUtil.arrangeColGrpsInSchemaOrder(splittedColGrps, dims)
    }
    else {
      null
    }
  }

  def rearrangedColumnGroup(colGroup: String, dims: Seq[Field]): String = {
    // if columns in column group is not in schema order than arrange it in schema order
    var colGrpFieldIndx: Seq[Int] = Seq[Int]()
    colGroup.split(',').map(_.trim).foreach { x =>
      dims.zipWithIndex.foreach { dim =>
        if (dim._1.column.equalsIgnoreCase(x)) {
          colGrpFieldIndx :+= dim._2
        }
      }
    }
    // sort it
    colGrpFieldIndx = colGrpFieldIndx.sorted
    // check if columns in column group is in schema order
    if (!checkIfInSequence(colGrpFieldIndx)) {
      throw new MalformedCarbonCommandException("Invalid column group:" + colGroup)
    }
    def checkIfInSequence(colGrpFieldIndx: Seq[Int]): Boolean = {
      for (i <- 0 until (colGrpFieldIndx.length - 1)) {
        if ((colGrpFieldIndx(i + 1) - colGrpFieldIndx(i)) != 1) {
          throw new MalformedCarbonCommandException(
            "Invalid column group,column in group should be contiguous as per schema.")
        }
      }
      true
    }
    val colGrpNames: StringBuilder = StringBuilder.newBuilder
    for (i <- 0 until colGrpFieldIndx.length) {
      colGrpNames.append(dims(colGrpFieldIndx(i)).column)
      if (i < (colGrpFieldIndx.length - 1)) {
        colGrpNames.append(",")
      }
    }
    colGrpNames.toString()
  }


  /**
   * For getting the partitioner Object
   *
   * @param partitionCols
   * @param tableProperties
   * @return
   */
  protected def getPartitionerObject(partitionCols: Seq[PartitionerField],
                                     tableProperties: Map[String, String]):
  Option[Partitioner] = {

    // by default setting partition class empty.
    // later in table schema it is setting to default value.
    var partitionClass: String = ""
    var partitionCount: Int = 1
    var partitionColNames: Array[String] = Array[String]()
    if (None != tableProperties.get(CarbonCommonConstants.PARTITIONCLASS)) {
      partitionClass = tableProperties.get(CarbonCommonConstants.PARTITIONCLASS).get
    }

    if (None != tableProperties.get(CarbonCommonConstants.PARTITIONCOUNT)) {
      try {
        partitionCount = tableProperties.get(CarbonCommonConstants.PARTITIONCOUNT).get.toInt
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

  protected def extractColumnProperties(fields: Seq[Field], tableProperties: Map[String, String]):
  java.util.Map[String, java.util.List[ColumnProperty]] = {
    val colPropMap = new java.util.HashMap[String, java.util.List[ColumnProperty]]()
    fields.foreach { field =>
      if (field.children.isDefined && field.children.get != null) {
        fillAllChildrenColumnProperty(field.column, field.children, tableProperties, colPropMap)
      } else {
        fillColumnProperty(None, field.column, tableProperties, colPropMap)
      }
    }
    colPropMap
  }

  protected def fillAllChildrenColumnProperty(parent: String, fieldChildren: Option[List[Field]],
    tableProperties: Map[String, String],
    colPropMap: java.util.HashMap[String, java.util.List[ColumnProperty]]) {
    fieldChildren.foreach(fields => {
      fields.foreach(field => {
        fillColumnProperty(Some(parent), field.column, tableProperties, colPropMap)
      }
      )
    }
    )
  }

  protected def fillColumnProperty(parentColumnName: Option[String],
    columnName: String,
    tableProperties: Map[String, String],
    colPropMap: java.util.HashMap[String, java.util.List[ColumnProperty]]) {
    val (tblPropKey, colProKey) = getKey(parentColumnName, columnName)
    val colProps = CommonUtil.getColumnProperties(tblPropKey, tableProperties)
    if (None != colProps) {
      colPropMap.put(colProKey, colProps.get)
    }
  }

  def getKey(parentColumnName: Option[String],
    columnName: String): (String, String) = {
    if (None != parentColumnName) {
      if (columnName == "val") {
        (parentColumnName.get, parentColumnName.get + "." + columnName)
      } else {
        (parentColumnName.get + "." + columnName, parentColumnName.get + "." + columnName)
      }
    } else {
      (columnName, columnName)
    }
  }
  /**
   * This will extract the no inverted columns fields.
   * By default all dimensions use inverted index.
   *
   * @param fields
   * @param tableProperties
   * @return
   */
  protected def extractNoInvertedIndexColumns(fields: Seq[Field],
    tableProperties: Map[String, String]):
  Seq[String] = {
    // check whether the column name is in fields
    var noInvertedIdxColsProps: Array[String] = Array[String]()
    var noInvertedIdxCols: Seq[String] = Seq[String]()

    if (tableProperties.get("NO_INVERTED_INDEX").isDefined) {
      noInvertedIdxColsProps =
        tableProperties.get("NO_INVERTED_INDEX").get.split(',').map(_.trim)
      noInvertedIdxColsProps
        .map { noInvertedIdxColProp =>
        if (!fields.exists(x => x.column.equalsIgnoreCase(noInvertedIdxColProp))) {
          val errormsg = "NO_INVERTED_INDEX column: " + noInvertedIdxColProp +
            " does not exist in table. Please check create table statement."
          throw new MalformedCarbonCommandException(errormsg)
        }
      }
    }
    // check duplicate columns and only 1 col left
    val distinctCols = noInvertedIdxColsProps.toSet
    // extract the no inverted index columns
    fields.foreach( field => {
      if (distinctCols.exists(x => x.equalsIgnoreCase(field.column))) {
        noInvertedIdxCols :+= field.column
      }
    }
    )
    noInvertedIdxCols
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
                                                    tableProperties: Map[String, String]):
  (Seq[Field], Seq[String]) = {
    var dimFields: LinkedHashSet[Field] = LinkedHashSet[Field]()
    var dictExcludeCols: Array[String] = Array[String]()
    var noDictionaryDims: Seq[String] = Seq[String]()
    var dictIncludeCols: Seq[String] = Seq[String]()

    // All excluded cols should be there in create table cols
    if (tableProperties.get(CarbonCommonConstants.DICTIONARY_EXCLUDE).isDefined) {
      dictExcludeCols =
        tableProperties.get(CarbonCommonConstants.DICTIONARY_EXCLUDE).get.split(',').map(_.trim)
      dictExcludeCols
        .map { dictExcludeCol =>
          if (!fields.exists(x => x.column.equalsIgnoreCase(dictExcludeCol))) {
            val errormsg = "DICTIONARY_EXCLUDE column: " + dictExcludeCol +
              " does not exist in table. Please check create table statement."
            throw new MalformedCarbonCommandException(errormsg)
          } else {
            val dataType = fields.find (x =>
              x.column.equalsIgnoreCase(dictExcludeCol)).get.dataType.get
            if (isComplexDimDictionaryExclude(dataType)) {
              val errormsg = "DICTIONARY_EXCLUDE is unsupported for complex datatype column: " +
                dictExcludeCol
              throw new MalformedCarbonCommandException(errormsg)
            } else if (isDoubleDecimalColDictionaryExclude(dataType)) {
              val errorMsg = "DICTIONARY_EXCLUDE is unsupported for " + dataType.toLowerCase() +
                " data type column: " + dictExcludeCol
              throw new MalformedCarbonCommandException(errorMsg)
            }
          }
        }
    }
    // All included cols should be there in create table cols
    if (tableProperties.get(CarbonCommonConstants.DICTIONARY_INCLUDE).isDefined) {
      dictIncludeCols =
        tableProperties.get(CarbonCommonConstants.DICTIONARY_INCLUDE).get.split(",").map(_.trim)
      dictIncludeCols.map { distIncludeCol =>
          if (!fields.exists(x => x.column.equalsIgnoreCase(distIncludeCol.trim))) {
            val errormsg = "DICTIONARY_INCLUDE column: " + distIncludeCol.trim +
              " does not exist in table. Please check create table statement."
            throw new MalformedCarbonCommandException(errormsg)
          }
        }
    }

    // include cols should contain exclude cols
    dictExcludeCols.foreach { dicExcludeCol =>
      if (dictIncludeCols.exists(x => x.equalsIgnoreCase(dicExcludeCol))) {
        val errormsg = "DICTIONARY_EXCLUDE can not contain the same column: " + dicExcludeCol +
          " with DICTIONARY_INCLUDE. Please check create table statement."
        throw new MalformedCarbonCommandException(errormsg)
      }
    }

    // by default consider all String cols as dims and if any dictionary exclude is present then
    // add it to noDictionaryDims list. consider all dictionary excludes/include cols as dims
    fields.foreach(field => {

      if (dictExcludeCols.toSeq.exists(x => x.equalsIgnoreCase(field.column))) {
        if (DataTypeUtil.getDataType(field.dataType.get.toUpperCase()) != DataType.TIMESTAMP) {
          noDictionaryDims :+= field.column
        }
        dimFields += field
      }
      else if (dictIncludeCols.exists(x => x.equalsIgnoreCase(field.column))) {
        dimFields += (field)
      }
      else if (isDetectAsDimentionDatatype(field.dataType.get)) {
        dimFields += (field)
      }
    }
    )

    (dimFields.toSeq, noDictionaryDims)
  }
  /**
   * It fills non string dimensions in dimFields
   */
  def fillNonStringDimension(dictIncludeCols: Seq[String],
    field: Field, dimFields: LinkedHashSet[Field]) {
    var dictInclude = false
    if (dictIncludeCols.nonEmpty) {
      dictIncludeCols.foreach(dictIncludeCol =>
        if (field.column.equalsIgnoreCase(dictIncludeCol)) {
          dictInclude = true
        })
    }
    if (dictInclude) {
      dimFields += field
    }
  }

  /**
   * detect dimention data type
   *
   * @param dimensionDatatype
   */
  def isDetectAsDimentionDatatype(dimensionDatatype: String): Boolean = {
    val dimensionType = Array("string", "array", "struct", "timestamp")
    dimensionType.exists(x => x.equalsIgnoreCase(dimensionDatatype))
  }

  /**
   * detects whether complex dimension is part of dictionary_exclude
   */
  def isComplexDimDictionaryExclude(dimensionDataType: String): Boolean = {
    val dimensionType = Array("array", "struct")
    dimensionType.exists(x => x.equalsIgnoreCase(dimensionDataType))
  }

   /**
    * detects whether double or decimal column is part of dictionary_exclude
    */
  def isDoubleDecimalColDictionaryExclude(columnDataType: String): Boolean = {
    val dataTypes = Array("double", "decimal")
    dataTypes.exists(x => x.equalsIgnoreCase(columnDataType))
  }

  /**
   * Extract the Measure Cols fields. By default all non string cols will be measures.
   *
   * @param fields
   * @param tableProperties
   * @return
   */
  protected def extractMsrColsFromFields(fields: Seq[Field],
                                         tableProperties: Map[String, String]): Seq[Field] = {
    var msrFields: Seq[Field] = Seq[Field]()
    var dictIncludedCols: Array[String] = Array[String]()
    var dictExcludedCols: Array[String] = Array[String]()

    // get all included cols
    if (None != tableProperties.get(CarbonCommonConstants.DICTIONARY_INCLUDE)) {
      dictIncludedCols =
        tableProperties.get(CarbonCommonConstants.DICTIONARY_INCLUDE).get.split(',').map(_.trim)
    }

    // get all excluded cols
    if (None != tableProperties.get(CarbonCommonConstants.DICTIONARY_EXCLUDE)) {
      dictExcludedCols =
        tableProperties.get(CarbonCommonConstants.DICTIONARY_EXCLUDE).get.split(',').map(_.trim)
    }

    // by default consider all non string cols as msrs. consider all include/ exclude cols as dims
    fields.foreach(field => {
      if (!isDetectAsDimentionDatatype(field.dataType.get)) {
          if (!dictIncludedCols.exists(x => x.equalsIgnoreCase(field.column)) &&
            !dictExcludedCols.exists(x => x.equalsIgnoreCase(field.column))) {
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
      tableNameParts.getChildren.asScala.map {
        case Token(part, Nil) => cleanIdentifier(part)
      } match {
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
          Option(t.getChildren).map(_.asScala.toList).getOrElse(Nil).asInstanceOf[Seq[ASTNode]]))
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
    case singleQuotedString(s) => s.toLowerCase()
    case doubleQuotedString(s) => s.toLowerCase()
    case other => other
  }

  protected lazy val loadDataNew: Parser[LogicalPlan] =
    LOAD ~> DATA ~> opt(LOCAL) ~> INPATH ~> stringLit ~ opt(OVERWRITE) ~
      (INTO ~> TABLE ~> (ident <~ ".").? ~ ident) ~
      (OPTIONS ~> "(" ~> repsep(loadOptions, ",") <~ ")").? <~ opt(";") ^^ {
        case filePath ~ isOverwrite ~ table ~ partionDataOptions =>
          val (schema, tablename) = table match {
            case databaseName ~ tableName => (databaseName, tableName.toLowerCase())
          }
          if(partionDataOptions.isDefined) {
            validateOptions(partionDataOptions)
          }
          val patitionOptionsMap = partionDataOptions.getOrElse(List.empty[(String, String)]).toMap
          LoadTable(schema, tablename, filePath, Seq(), patitionOptionsMap, isOverwrite.isDefined)
      }

  private def validateOptions(partionDataOptions: Option[List[(String, String)]]): Unit = {

    // validate with all supported options
    val options = partionDataOptions.get.groupBy(x => x._1)
    val supportedOptions = Seq("DELIMITER", "QUOTECHAR", "FILEHEADER", "ESCAPECHAR", "MULTILINE",
      "COMPLEX_DELIMITER_LEVEL_1", "COMPLEX_DELIMITER_LEVEL_2", "COLUMNDICT",
      "SERIALIZATION_NULL_FORMAT",
      "ALL_DICTIONARY_PATH", "MAXCOLUMNS"
    )
    var isSupported = true
    val invalidOptions = StringBuilder.newBuilder
    options.foreach(value => {
      if (!supportedOptions.exists(x => x.equalsIgnoreCase(value._1))) {
        isSupported = false
        invalidOptions.append(value._1)
      }

    }
    )
    if (!isSupported) {
      val errorMessage = "Error: Invalid option(s): " + invalidOptions.toString()
      throw new MalformedCarbonCommandException(errorMessage)
    }

    //  COLUMNDICT and ALL_DICTIONARY_PATH can not be used together.
    if (options.exists(_._1.equalsIgnoreCase("COLUMNDICT")) &&
      options.exists(_._1.equalsIgnoreCase("ALL_DICTIONARY_PATH"))) {
      val errorMessage = "Error: COLUMNDICT and ALL_DICTIONARY_PATH can not be used together" +
        " in options"
      throw new MalformedCarbonCommandException(errorMessage)
    }

    if (options.exists(_._1.equalsIgnoreCase("MAXCOLUMNS"))) {
      val maxColumns: String = options.get("maxcolumns").get(0)._2
      try {
        maxColumns.toInt
      } catch {
        case ex: NumberFormatException =>
          throw new MalformedCarbonCommandException(
            "option MAXCOLUMNS can only contain integer values")
      }
    }

    // check for duplicate options
    val duplicateOptions = options filter {
      case (_, optionlist) => optionlist.size > 1
    }
    val duplicates = StringBuilder.newBuilder
    if (duplicateOptions.size > 0) {
      duplicateOptions.foreach(x => {
        duplicates.append(x._1)
      }
      )
      val errorMessage = "Error: Duplicate option(s): " + duplicates.toString()
      throw new MalformedCarbonCommandException(errorMessage)
    }
  }

  protected lazy val dbTableIdentifier: Parser[Seq[String]] =
    (ident <~ ".").? ~ (ident) ^^ {
      case databaseName ~ tableName =>
        if (databaseName.isDefined) {
          Seq(databaseName.get, tableName)
        } else {
          Seq(tableName)
        }
    }

  protected lazy val tableFileMapping: Parser[DataLoadTableFileMapping] =
    (ident <~ ":") ~ stringLit ^^ {
      case tableName ~ dataPath => DataLoadTableFileMapping(tableName, dataPath)
    }

  protected lazy val loadOptions: Parser[(String, String)] =
    (stringLit <~ "=") ~ stringLit ^^ {
      case opt ~ optvalue => (opt.trim.toLowerCase(), optvalue)
      case _ => ("", "")
    }


  protected lazy val dimCol: Parser[Field] = anyFieldDef

  protected lazy val primitiveTypes =
    STRING ^^^ "string" | INTEGER ^^^ "integer" | TIMESTAMP ^^^
    "timestamp" | NUMERIC ^^^ "numeric" | BIGINT ^^^ "bigint" |
       INT ^^^ "int" | DOUBLE ^^^ "double" | decimalType

  /**
   * Matching the decimal(10,0) data type and returning the same.
   */
  private lazy val decimalType =
    DECIMAL ~ ("(" ~> numericLit <~",") ~ (numericLit <~ ")")  ^^ {
      case decimal ~ precision ~scale =>
        s"$decimal($precision, $scale)"
    }

  protected lazy val nestedType: Parser[Field] = structFieldType | arrayFieldType |
    primitiveFieldType

  protected lazy val anyFieldDef: Parser[Field] =
    (ident | stringLit) ~ ((":").? ~> nestedType) ~ (IN ~> (ident | stringLit)).? ^^ {
      case e1 ~ e2 ~ e3 =>
        Field(e1, e2.dataType, Some(e1), e2.children, null, e3)
    }

  protected lazy val primitiveFieldType: Parser[Field] =
    (primitiveTypes) ^^ {
      case e1 =>
        Field("unknown", Some(e1), Some("unknown"), Some(null))
    }

  protected lazy val arrayFieldType: Parser[Field] =
    ((ARRAY ^^^ "array") ~> "<" ~> nestedType <~ ">") ^^ {
      case e1 =>
        Field("unknown", Some("array"), Some("unknown"),
          Some(List(Field("val", e1.dataType, Some("val"),
            e1.children))))
    }

  protected lazy val structFieldType: Parser[Field] =
    ((STRUCT ^^^ "struct") ~> "<" ~> repsep(anyFieldDef, ",") <~ ">") ^^ {
      case e1 =>
        Field("unknown", Some("struct"), Some("unknown"), Some(e1))
    }

  protected lazy val measureCol: Parser[Field] =
    (ident | stringLit) ~ (INTEGER ^^^ "integer" | NUMERIC ^^^ "numeric" |
      BIGINT ^^^ "bigint" | DECIMAL ^^^ "decimal").? ~
      (AS ~> (ident | stringLit)).? ~ (IN ~> (ident | stringLit)).? ^^ {
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
      case hc =>
        Some(hc)
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
        if (hierType.getOrElse(dimType).equalsIgnoreCase("time")) {
          dimType = "TimeDimension"
        }
        HierarchyMapping(hierName, dimType, levels)
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
        }
        else {
          new DescribeCommand(UnresolvedRelation(tblIdentifier, None), ef.isDefined)
        }
    }
  private def normalizeType(field: Field): Field = {
    val dataType = field.dataType.getOrElse("NIL")
    dataType match {
      case "string" => Field(field.column, Some("String"), field.name, Some(null), field.parent,
        field.storeType
      )
      case "integer" | "int" => Field(field.column, Some("Integer"), field.name, Some(null),
        field.parent, field.storeType
      )
      case "long" => Field(field.column, Some("Long"), field.name, Some(null), field.parent,
        field.storeType
      )
      case "double" => Field(field.column, Some("Double"), field.name, Some(null), field.parent,
        field.storeType
      )
      case "timestamp" => Field(field.column, Some("Timestamp"), field.name, Some(null),
        field.parent, field.storeType
      )
      case "numeric" => Field(field.column, Some("Numeric"), field.name, Some(null), field.parent,
        field.storeType
      )
      case "array" => Field(field.column, Some("Array"), field.name,
        field.children.map(f => f.map(normalizeType(_))),
        field.parent, field.storeType
      )
      case "struct" => Field(field.column, Some("Struct"), field.name,
        field.children.map(f => f.map(normalizeType(_))),
        field.parent, field.storeType
      )
      case "bigint" => Field(field.column, Some("BigInt"), field.name, Some(null), field.parent,
        field.storeType
      )
      case "decimal" => Field(field.column, Some("Decimal"), field.name, Some(null), field.parent,
        field.storeType, field.precision, field.scale
      )
      // checking if the nested data type contains the child type as decimal(10,0),
      // if it is present then extracting the precision and scale. resetting the data type
      // with Decimal.
      case _ if (dataType.startsWith("decimal")) =>
        val (precision, scale) = getScaleAndPrecision(dataType)
        Field(field.column,
          Some("Decimal"),
          field.name,
          Some(null),
          field.parent,
          field.storeType, precision,
          scale
        )
      case _ =>
        field
    }
  }

  private def addParent(field: Field): Field = {
    field.dataType.getOrElse("NIL") match {
      case "Array" => Field(field.column, Some("Array"), field.name,
        field.children.map(f => f.map(appendParentForEachChild(_, field.column))), field.parent,
        field.storeType)
      case "Struct" => Field(field.column, Some("Struct"), field.name,
        field.children.map(f => f.map(appendParentForEachChild(_, field.column))), field.parent,
        field.storeType)
      case _ => field
    }
  }

  private def appendParentForEachChild(field: Field, parentName: String): Field = {
    field.dataType.getOrElse("NIL") match {
      case "String" => Field(parentName + "." + field.column, Some("String"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Integer" => Field(parentName + "." + field.column, Some("Integer"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Long" => Field(parentName + "." + field.column, Some("Long"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Double" => Field(parentName + "." + field.column, Some("Double"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Timestamp" => Field(parentName + "." + field.column, Some("Timestamp"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Numeric" => Field(parentName + "." + field.column, Some("Numeric"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Array" => Field(parentName + "." + field.column, Some("Array"),
        Some(parentName + "." + field.name.getOrElse(None)),
        field.children
          .map(f => f.map(appendParentForEachChild(_, parentName + "." + field.column))),
        parentName)
      case "Struct" => Field(parentName + "." + field.column, Some("Struct"),
        Some(parentName + "." + field.name.getOrElse(None)),
        field.children
          .map(f => f.map(appendParentForEachChild(_, parentName + "." + field.column))),
        parentName)
      case "BigInt" => Field(parentName + "." + field.column, Some("BigInt"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Decimal" => Field(parentName + "." + field.column, Some("Decimal"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName,
        field.storeType, field.precision, field.scale)
      case _ => field
    }
  }

  protected lazy val showLoads: Parser[LogicalPlan] =
    SHOW ~> SEGMENTS ~> FOR ~> TABLE ~> (ident <~ ".").? ~ ident ~
      (LIMIT ~> numericLit).? <~
      opt(";") ^^ {
      case databaseName ~ tableName ~ limit =>
        ShowLoadsCommand(databaseName, tableName.toLowerCase(), limit)
    }

  protected lazy val segmentId: Parser[String] =
    numericLit ^^ { u => u } |
      elem("decimal", p => {
        p.getClass.getSimpleName.equals("FloatLit") ||
        p.getClass.getSimpleName.equals("DecimalLit") } ) ^^ (_.chars)

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

  protected lazy val dropDatabaseCascade: Parser[LogicalPlan] =
    DROP ~> (DATABASE|SCHEMA) ~> opt(IF ~> EXISTS) ~> ident ~> CASCADE <~ opt(";") ^^ {
      case cascade => throw new MalformedCarbonCommandException(
          "Unsupported cascade operation in drop database/schema command")
    }
}
