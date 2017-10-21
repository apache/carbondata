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

package org.apache.spark.sql.catalyst

import java.text.SimpleDateFormat
import java.util.regex.{Matcher, Pattern}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, LinkedHashSet, Map}
import scala.language.implicitConversions
import scala.util.matching.Regex

import org.apache.hadoop.hive.ql.lib.Node
import org.apache.hadoop.hive.ql.parse._
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.execution.command._

import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.datatype.{DataType, DataTypes}
import org.apache.carbondata.core.metadata.schema.PartitionInfo
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil, DataTypeUtil}
import org.apache.carbondata.processing.loading.sort.SortScopeOptions
import org.apache.carbondata.store._
import org.apache.carbondata.store.util.{CommonUtil, DataTypeConverterUtil, PartitionUtils}

/**
 * TODO remove the duplicate code and add the common methods to common class.
 * Parser for All Carbon DDL cases
 */
abstract class CarbonDDLSqlParser extends AbstractCarbonSparkSQLParser {

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
  protected val COMPACT = carbonKeyWord("COMPACT")
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
  protected val BAD_RECORDS_LOGGER_ENABLE = carbonKeyWord("BAD_RECORDS_LOGGER_ENABLE")
  protected val BAD_RECORDS_ACTION = carbonKeyWord("BAD_RECORDS_ACTION")
  protected val IS_EMPTY_DATA_BAD_RECORD = carbonKeyWord("IS_EMPTY_DATA_BAD_RECORD")
  protected val IS_EMPTY_COMMA_DATA_BAD_RECORD = carbonKeyWord("IS_NULL_DATA_BAD_RECORD")
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
  protected val MERGE = carbonKeyWord("MERGE")
  protected val MULTILINE = carbonKeyWord("MULTILINE")
  protected val COMPLEX_DELIMITER_LEVEL_1 = carbonKeyWord("COMPLEX_DELIMITER_LEVEL_1")
  protected val COMPLEX_DELIMITER_LEVEL_2 = carbonKeyWord("COMPLEX_DELIMITER_LEVEL_2")
  protected val OPTIONS = carbonKeyWord("OPTIONS")
  protected val OUTPATH = carbonKeyWord("OUTPATH")
  protected val OVERWRITE = carbonKeyWord("OVERWRITE")
  protected val PARTITION = carbonKeyWord("PARTITION")
  protected val PARTITION_COUNT = carbonKeyWord("PARTITION_COUNT")
  protected val PARTITIONDATA = carbonKeyWord("PARTITIONDATA")
  protected val PARTITIONER = carbonKeyWord("PARTITIONER")
  protected val PARTITIONS = carbonKeyWord("PARTITIONS")
  protected val QUOTECHAR = carbonKeyWord("QUOTECHAR")
  protected val RELATION = carbonKeyWord("RELATION")
  protected val SCHEMA = carbonKeyWord("SCHEMA")
  protected val SCHEMAS = carbonKeyWord("SCHEMAS")
  protected val SET = Keyword("SET")
  protected val SHOW = carbonKeyWord("SHOW")
  protected val SPLIT = carbonKeyWord("SPLIT")
  protected val TABLES = carbonKeyWord("TABLES")
  protected val TABLE = carbonKeyWord("TABLE")
  protected val TERMINATED = carbonKeyWord("TERMINATED")
  protected val TYPE = carbonKeyWord("TYPE")
  protected val UPDATE = carbonKeyWord("UPDATE")
  protected val USE = carbonKeyWord("USE")
  protected val WHERE = Keyword("WHERE")
  protected val WITH = carbonKeyWord("WITH")
  protected val AGGREGATETABLE = carbonKeyWord("AGGREGATETABLE")
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
  protected val DATE = carbonKeyWord("DATE")
  protected val CHAR = carbonKeyWord("CHAR")
  protected val VARCHAR = carbonKeyWord("VARCHAR")
  protected val NUMERIC = carbonKeyWord("NUMERIC")
  protected val DECIMAL = carbonKeyWord("DECIMAL")
  protected val DOUBLE = carbonKeyWord("DOUBLE")
  protected val FLOAT = carbonKeyWord("FLOAT")
  protected val SHORT = carbonKeyWord("SHORT")
  protected val INT = carbonKeyWord("INT")
  protected val BOOLEAN = carbonKeyWord("BOOLEAN")
  protected val BIGINT = carbonKeyWord("BIGINT")
  protected val ARRAY = carbonKeyWord("ARRAY")
  protected val STRUCT = carbonKeyWord("STRUCT")
  protected val SMALLINT = carbonKeyWord("SMALLINT")
  protected val CHANGE = carbonKeyWord("CHANGE")
  protected val TBLPROPERTIES = carbonKeyWord("TBLPROPERTIES")
  protected val ID = carbonKeyWord("ID")

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

  implicit def regexToParser(regex: Regex): Parser[String] = {
    acceptMatch(
    s"identifier matching regex ${ regex }",
    { case Identifier(str) if regex.unapplySeq(str).isDefined => str }
    )
  }

  /**
   * This will convert key word to regular expression.
   *
   * @param keys
   * @return
   */
  private def carbonKeyWord(keys: String) = {
    ("(?i)" + keys).r
  }

  protected val escapedIdentifier = "`([^`]+)`".r

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
        case Seq(databaseName, table) => (Some(convertDbNameToLowerCase(databaseName)), table)
      }

    (db, tableName)
  }

  /**
   * This method will convert the database name to lower case
   *
   * @param dbName
   * @return String
   */
  protected def convertDbNameToLowerCase(dbName: String) = {
    dbName.toLowerCase
  }

  /**
   * This method will convert the database name to lower case
   *
   * @param dbName
   * @return Option of String
   */
  protected def convertDbNameToLowerCase(dbName: Option[String]): Option[String] = {
    dbName match {
      case Some(databaseName) => Some(convertDbNameToLowerCase(databaseName))
      case None => dbName
    }
  }

  protected def cleanIdentifier(ident: String): String = {
    ident match {
      case escapedIdentifier(i) => i
      case plainIdent => plainIdent
    }
  }

  protected def getClauses(clauseNames: Seq[String], nodeList: Seq[ASTNode]): Seq[Option[Node]] = {
    var remainingNodes = nodeList
    val clauses = clauseNames.map { clauseName =>
      val (matches, nonMatches) = remainingNodes.partition(_.getText.toUpperCase == clauseName)
      remainingNodes = nonMatches ++ (if (matches.nonEmpty) {
        matches.tail
      } else {
        Nil
      })
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
    def unapply(t: Any): Option[(String, Seq[ASTNode])] = {
      t match {
        case t: ASTNode =>
          CurrentOrigin.setPosition(t.getLine, t.getCharPositionInLine)
          Some((t.getText,
            Option(t.getChildren).map(_.asScala.toList).getOrElse(Nil).asInstanceOf[Seq[ASTNode]]))
        case _ => None
      }
    }
  }

  /**
   * Extract the table properties token
   *
   * @param node
   * @return
   */
  protected def getProperties(node: Node): Seq[(String, String)] = {
    node match {
      case Token("TOK_TABLEPROPLIST", list) =>
        list.map {
          case Token("TOK_TABLEPROPERTY", Token(key, Nil) :: Token(value, Nil) :: Nil) =>
            val reslovedKey = unquoteString(key)
            if (needToConvertToLowerCase(reslovedKey)) {
              (reslovedKey, unquoteString(value))
            } else {
              (reslovedKey, unquoteStringWithoutLowerConversion(value))
            }
        }
    }
  }

  protected def unquoteString(str: String) = {
    str match {
      case singleQuotedString(s) => s.toLowerCase()
      case doubleQuotedString(s) => s.toLowerCase()
      case other => other
    }
  }

  protected def unquoteStringWithoutLowerConversion(str: String) = {
    str match {
      case singleQuotedString(s) => s
      case doubleQuotedString(s) => s
      case other => other
    }
  }

  private def needToConvertToLowerCase(key: String): Boolean = {
    val noConvertList = Array("LIST_INFO", "RANGE_INFO")
    !noConvertList.exists(x => x.equalsIgnoreCase(key))
  }

  protected def validateOptions(optionList: Option[List[(String, String)]]): Unit = {

    // validate with all supported options
    val options = optionList.get.groupBy(x => x._1)
    val supportedOptions = Seq("DELIMITER", "QUOTECHAR", "FILEHEADER", "ESCAPECHAR", "MULTILINE",
      "COMPLEX_DELIMITER_LEVEL_1", "COMPLEX_DELIMITER_LEVEL_2", "COLUMNDICT",
      "SERIALIZATION_NULL_FORMAT", "BAD_RECORDS_LOGGER_ENABLE", "BAD_RECORDS_ACTION",
      "ALL_DICTIONARY_PATH", "MAXCOLUMNS", "COMMENTCHAR", "DATEFORMAT", "BAD_RECORD_PATH",
      "BATCH_SORT_SIZE_INMB", "GLOBAL_SORT_PARTITIONS", "SINGLE_PASS",
      "IS_EMPTY_DATA_BAD_RECORD", "HEADER"
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
      val maxColumns: String = options.get("maxcolumns").get.head._2
      try {
        maxColumns.toInt
      } catch {
        case ex: NumberFormatException =>
          throw new MalformedCarbonCommandException(
            "option MAXCOLUMNS can only contain integer values")
      }
    }

    if (options.exists(_._1.equalsIgnoreCase("BAD_RECORDS_ACTION"))) {
      val optionValue: String = options.get("bad_records_action").get.head._2
      try {
        LoggerAction.valueOf(optionValue.toUpperCase)
      }
      catch {
        case e: IllegalArgumentException =>
          throw new MalformedCarbonCommandException(
            "option BAD_RECORDS_ACTION can have only either FORCE or IGNORE or REDIRECT")
      }
    }
    if (options.exists(_._1.equalsIgnoreCase("IS_EMPTY_DATA_BAD_RECORD"))) {
      val optionValue: String = options.get("is_empty_data_bad_record").get.head._2
      if (!("true".equalsIgnoreCase(optionValue) || "false".equalsIgnoreCase(optionValue))) {
        throw new MalformedCarbonCommandException(
          "option IS_EMPTY_DATA_BAD_RECORD can have option either true or false")
      }
    }

    // check for duplicate options
    val duplicateOptions = options filter {
      case (_, optionlist) => optionlist.size > 1
    }
    val duplicates = StringBuilder.newBuilder
    if (duplicateOptions.nonEmpty) {
      duplicateOptions.foreach(x => {
        duplicates.append(x._1)
      }
      )
      val errorMessage = "Error: Duplicate option(s): " + duplicates.toString()
      throw new MalformedCarbonCommandException(errorMessage)
    }
  }

  protected lazy val dbTableIdentifier: Parser[Seq[String]] =
    (ident <~ ".").? ~ ident ^^ {
      case databaseName ~ tableName =>
        if (databaseName.isDefined) {
          Seq(databaseName.get, tableName)
        } else {
          Seq(tableName)
        }
    }

  protected lazy val loadOptions: Parser[(String, String)] =
    (stringLit <~ "=") ~ stringLit ^^ {
      case opt ~ optvalue => (opt.trim.toLowerCase(), optvalue)
      case _ => ("", "")
    }

  protected lazy val valueOptions: Parser[(Int, Int)] =
    (numericLit <~ ",") ~ numericLit ^^ {
      case opt ~ optvalue => (opt.toInt, optvalue.toInt)
      case _ => (0, 0)
    }

  protected lazy val columnOptions: Parser[(String, String)] =
    (stringLit <~ ",") ~ stringLit ^^ {
      case opt ~ optvalue => (opt, optvalue)
      case _ =>
        throw new MalformedCarbonCommandException(s"value cannot be empty")
    }

  protected lazy val dimCol: Parser[Field] = anyFieldDef

  protected lazy val primitiveTypes =
    STRING ^^^ "string" |BOOLEAN ^^^ "boolean" | INTEGER ^^^ "integer" |
    TIMESTAMP ^^^ "timestamp" | NUMERIC ^^^ "numeric" |
    BIGINT ^^^ "bigint" | (SHORT | SMALLINT) ^^^ "smallint" |
    INT ^^^ "int" | DOUBLE ^^^ "double" | FLOAT ^^^ "double" | decimalType |
    DATE ^^^ "date" | charType

  /**
   * Matching the decimal(10,0) data type and returning the same.
   */
  private lazy val charType =
    (CHAR | VARCHAR ) ~ ("(" ~>numericLit <~ ")") ^^ {
      case char ~ digit =>
        s"$char($digit)"
    }

  /**
   * Matching the decimal(10,0) data type and returning the same.
   */
  private lazy val decimalType =
  DECIMAL ~ (("(" ~> numericLit <~ ",") ~ (numericLit <~ ")")).? ^^ {
    case decimal ~ precisionAndScale => if (precisionAndScale.isDefined) {
      s"decimal(${ precisionAndScale.get._1 }, ${ precisionAndScale.get._2 })"
    } else {
      s"decimal(10,0)"
    }
  }

  protected lazy val nestedType: Parser[Field] = structFieldType | arrayFieldType |
                                                 primitiveFieldType

  lazy val anyFieldDef: Parser[Field] =
    (ident | stringLit) ~ (":".? ~> nestedType) ~ (IN ~> (ident | stringLit)).? ^^ {
      case e1 ~ e2 ~ e3 =>
        Field(e1, e2.dataType, Some(e1), e2.children, null, e3)
    }

  protected lazy val primitiveFieldType: Parser[Field] =
    primitiveTypes ^^ {
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
    (ident | stringLit) ~ (INTEGER ^^^ "integer" | NUMERIC ^^^ "numeric" | SHORT ^^^ "smallint" |
                           BIGINT ^^^ "bigint" | DECIMAL ^^^ "decimal").? ~
    (AS ~> (ident | stringLit)).? ~ (IN ~> (ident | stringLit)).? ^^ {
      case e1 ~ e2 ~ e3 ~ e4 => Field(e1, e2, e3, Some(null))
    }

  protected lazy val segmentId: Parser[String] =
    numericLit ^^ { u => u } |
    elem("decimal", p => {
      p.getClass.getSimpleName.equals("FloatLit") ||
      p.getClass.getSimpleName.equals("DecimalLit")
    }) ^^ (_.chars)

  /**
   * This method will parse the given data type and validate against the allowed data types
   *
   * @param dataType
   * @param values
   * @return
   */
  protected def parseDataType(dataType: String, values: Option[List[(Int, Int)]]): DataTypeInfo = {
    dataType match {
      case "bigint" | "long" =>
        if (values.isDefined) {
          throw new MalformedCarbonCommandException("Invalid data type")
        }
        DataTypeInfo(dataType)
      case "decimal" =>
        var precision: Int = 0
        var scale: Int = 0
        if (values.isDefined) {
          precision = values.get(0)._1
          scale = values.get(0)._2
        } else {
          throw new MalformedCarbonCommandException("Decimal format provided is invalid")
        }
        // precision should be > 0 and <= 38 and scale should be >= 0 and <= 38
        if (precision < 1 || precision > 38) {
          throw new MalformedCarbonCommandException("Invalid value for precision")
        } else if (scale < 0 || scale > 38) {
          throw new MalformedCarbonCommandException("Invalid value for scale")
        }
        DataTypeInfo("decimal", precision, scale)
      case _ =>
        throw new MalformedCarbonCommandException("Data type provided is invalid.")
    }
  }
}
