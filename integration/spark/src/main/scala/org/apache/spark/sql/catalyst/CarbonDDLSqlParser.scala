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

import scala.language.implicitConversions
import scala.util.matching.Regex

import org.apache.spark.sql.execution.command._

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory

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
  protected val CASCADE = carbonKeyWord("CASCADE")
  protected val CLASS = carbonKeyWord("CLASS")
  protected val CLEAN = carbonKeyWord("CLEAN")
  protected val COLS = carbonKeyWord("COLS")
  protected val COLUMNS = carbonKeyWord("COLUMNS")
  protected val COMPACT = carbonKeyWord("COMPACT")
  protected val FINISH = carbonKeyWord("FINISH")
  protected val STREAMING = carbonKeyWord("STREAMING")
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
  protected val SKIP_EMPTY_LINE = carbonKeyWord("SKIP_EMPTY_LINE")
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
  protected val COMPLEX_DELIMITER_LEVEL_3 = carbonKeyWord("COMPLEX_DELIMITER_LEVEL_3")
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
  protected val EXECUTOR = carbonKeyWord("EXECUTOR")

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
  protected val HISTORY = carbonKeyWord("HISTORY")
  protected val SEGMENTS = carbonKeyWord("SEGMENTS")
  protected val SEGMENT = carbonKeyWord("SEGMENT")
  protected val METACACHE = carbonKeyWord("METACACHE")

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
  protected val LONG = carbonKeyWord("LONG")
  protected val BIGINT = carbonKeyWord("BIGINT")
  protected val BINARY = carbonKeyWord("BINARY")
  protected val ARRAY = carbonKeyWord("ARRAY")
  protected val STRUCT = carbonKeyWord("STRUCT")
  protected val MAP = carbonKeyWord("MAP")
  protected val SMALLINT = carbonKeyWord("SMALLINT")
  protected val CHANGE = carbonKeyWord("CHANGE")
  protected val TBLPROPERTIES = carbonKeyWord("TBLPROPERTIES")
  protected val ID = carbonKeyWord("ID")
  protected val DATAMAP = carbonKeyWord("DATAMAP")
  protected val ON = carbonKeyWord("ON")
  protected val DMPROPERTIES = carbonKeyWord("DMPROPERTIES")
  protected val SELECT = carbonKeyWord("SELECT")
  protected val REBUILD = carbonKeyWord("REBUILD")
  protected val DEFERRED = carbonKeyWord("DEFERRED")
  protected val STREAM = carbonKeyWord("STREAM")
  protected val STREAMS = carbonKeyWord("STREAMS")
  protected val STMPROPERTIES = carbonKeyWord("STMPROPERTIES")
  protected val CARBONCLI = carbonKeyWord("CARBONCLI")
  protected val PATH = carbonKeyWord("PATH")
  protected val INSERT = carbonKeyWord("INSERT")
  protected val STAGE = carbonKeyWord("STAGE")
  protected val INDEX = carbonKeyWord("INDEX")
  protected val INDEXES = carbonKeyWord("INDEXES")
  protected val REGISTER = carbonKeyWord("REGISTER")

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
  def carbonKeyWord(keys: String): Regex = {
    ("(?i)" + keys).r
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

  protected lazy val options: Parser[(String, String)] =
    (stringLit <~ "=") ~ stringLit ^^ {
      case opt ~ optvalue => (opt.trim.toLowerCase(), optvalue)
      case _ => ("", "")
    }

  protected lazy val commandOptions: Parser[String] =
    stringLit ^^ {
      case optValue => optValue
      case _ => ""
    }


  protected lazy val partitions: Parser[(String, Option[String])] =
    (ident <~ "=".?) ~ stringLit.? ^^ {
      case opt ~ optvalue => (opt.trim, optvalue)
      case _ => ("", None)
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
    (LONG | BIGINT) ^^^ "bigint" | (SHORT | SMALLINT) ^^^ "smallint" |
    INT ^^^ "int" | DOUBLE ^^^ "double" | FLOAT ^^^ "double" | decimalType |
    DATE ^^^ "date" | charType

  protected lazy val miscType = BINARY ^^^ "binary"

  /**
   * Matching the char data type and returning the same.
   */
  private lazy val charType =
    (CHAR | VARCHAR ) ~ opt("(" ~>numericLit <~ ")") ^^ {
      case (char ~ _) =>
        s"$char"
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

  protected lazy val nestedType: Parser[Field] = structFieldType | arrayFieldType | mapFieldType |
                                                 primitiveFieldType | miscFieldType

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

  protected lazy val miscFieldType: Parser[Field] =
    miscType ^^ {
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

  //  Map<Key,Value> is represented as Map<Struct<Key,Value>>
  protected lazy val mapFieldType: Parser[Field] =
    (MAP ^^^ "map") ~> "<" ~> primitiveFieldType ~ ("," ~> nestedType) <~ ">" ^^ {
      case key ~ value =>
        Field("unknown", Some("map"), Some("unknown"),
          Some(List(
            Field("val", Some("struct"), Some("unknown"),
              Some(List(
                Field("key", key.dataType, Some("key"), key.children),
                Field("value", value.dataType, Some("value"), value.children)))))))
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
}
