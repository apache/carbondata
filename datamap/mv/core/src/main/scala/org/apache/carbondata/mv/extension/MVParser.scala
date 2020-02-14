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

package org.apache.carbondata.mv.extension

import scala.language.implicitConversions
import scala.util.matching.Regex
import scala.util.parsing.combinator.PackratParsers
import scala.util.parsing.combinator.syntactical.StandardTokenParsers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.{SqlLexical, TableIdentifier}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.hive.CarbonMVRules
import org.apache.spark.sql.util.{CarbonException, SparkSQLUtil}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.mv.extension.command.{CreateMaterializedViewCommand, DropMaterializedViewCommand, RefreshMaterializedViewCommand, ShowMaterializedViewCommand}
import org.apache.carbondata.mv.rewrite.MVUdf

class MVParser extends StandardTokenParsers with PackratParsers {

  // Keywords used in this parser
  protected val SELECT: Regex = carbonKeyWord("SELECT")
  protected val CREATE: Regex = carbonKeyWord("CREATE")
  protected val MATERIALIZED: Regex = carbonKeyWord("MATERIALIZED")
  protected val VIEW: Regex = carbonKeyWord("VIEW")
  protected val VIEWS: Regex = carbonKeyWord("VIEWS")
  protected val AS: Regex = carbonKeyWord("AS")
  protected val DROP: Regex = carbonKeyWord("DROP")
  protected val SHOW: Regex = carbonKeyWord("SHOW")
  protected val IF: Regex = carbonKeyWord("IF")
  protected val EXISTS: Regex = carbonKeyWord("EXISTS")
  protected val NOT: Regex = carbonKeyWord("NOT")
  protected val PROPERTIES: Regex = carbonKeyWord("PROPERTIES")
  protected val WITH: Regex = carbonKeyWord("WITH")
  protected val DEFERRED: Regex = carbonKeyWord("DEFERRED")
  protected val REFRESH: Regex = carbonKeyWord("REFRESH")
  protected val ON: Regex = carbonKeyWord("ON")
  protected val TABLE: Regex = carbonKeyWord("TABLE")

  /**
   * This will convert key word to regular expression.
   */
  private def carbonKeyWord(keys: String): Regex = {
    ("(?i)" + keys).r
  }

  implicit def regexToParser(regex: Regex): Parser[String] = {
    import lexical.Identifier
    acceptMatch(
      s"identifier matching regex ${ regex }",
      { case Identifier(str) if regex.unapplySeq(str).isDefined => str }
    )
  }

  // By default, use Reflection to find the reserved words defined in the sub class.
  // NOTICE, Since the Keyword properties defined by sub class, we couldn't call this
  // method during the parent class instantiation, because the sub class instance
  // isn't created yet.
  protected lazy val reservedWords: Seq[String] =
  this
    .getClass
    .getMethods
    .filter(_.getReturnType == classOf[Keyword])
    .map(_.invoke(this).asInstanceOf[Keyword].normalize)

  // Set the keywords as empty by default, will change that later.
  override val lexical = new SqlLexical

  protected case class Keyword(str: String) {
    def normalize: String = lexical.normalizeKeyword(str)
    def parser: Parser[String] = normalize
  }

  def parse(input: String): LogicalPlan = {
    synchronized {
      phrase(start)(new lexical.Scanner(input)) match {
        case Success(plan, _) =>
          plan
        case failureOrError =>
          CarbonException.analysisException(failureOrError.toString)
      }
    }
  }

  private lazy val start: Parser[LogicalPlan] = mvCommand

  private lazy val mvCommand: Parser[LogicalPlan] =
    createMV | dropMV | showMV | refreshMV

  /**
   * CREATE MATERIALIZED VIEW [IF NOT EXISTS] mv_name
   * [WITH DEFERRED REFRESH]
   * [PROPERTIES('KEY'='VALUE')]
   * AS mv_query_statement
   */
  private lazy val createMV: Parser[LogicalPlan] =
    CREATE ~> MATERIALIZED ~> VIEW ~> opt(IF ~> NOT ~> EXISTS) ~ ident ~
    opt(WITH ~> DEFERRED ~> REFRESH) ~
    (PROPERTIES ~> "(" ~> repsep(options, ",") <~ ")").? ~
    (AS ~> restInput).? <~ opt(";") ^^ {
      case ifNotExists ~ mvName ~ deferredRebuild ~ properties ~ query =>
        val map = properties.getOrElse(List[(String, String)]()).toMap[String, String]
        CreateMaterializedViewCommand(mvName, map, query,
          ifNotExists.isDefined, deferredRebuild.isDefined)
    }

  /**
   * DROP MATERIALIZED VIEW [IF EXISTS] mv_name
   */
  private lazy val dropMV: Parser[LogicalPlan] =
    DROP ~> MATERIALIZED ~> VIEW ~> opt(IF ~> EXISTS) ~ ident <~ opt(";") ^^ {
      case ifExits ~ mvName =>
        DropMaterializedViewCommand(mvName, ifExits.isDefined)
    }

  /**
   * SHOW MATERIALIZED VIEWS [ON TABLE table_name]
   */
  private lazy val showMV: Parser[LogicalPlan] =
    SHOW ~> MATERIALIZED ~> VIEWS ~> opt(onTable) <~ opt(";") ^^ {
      case tableIdent =>
        ShowMaterializedViewCommand(tableIdent)
    }

  /**
   * REFRESH MATERIALIZED VIEW mv_name
   */
  private lazy val refreshMV: Parser[LogicalPlan] =
    REFRESH ~> MATERIALIZED ~> VIEW ~> ident <~ opt(";") ^^ {
      case mvName =>
        RefreshMaterializedViewCommand(mvName)
    }

  // Returns the rest of the input string that are not parsed yet
  private lazy val restInput: Parser[String] = new Parser[String] {
    def apply(in: Input): ParseResult[String] =
      Success(
        in.source.subSequence(in.offset, in.source.length()).toString,
        in.drop(in.source.length()))
  }

  private lazy val options: Parser[(String, String)] =
    (stringLit <~ "=") ~ stringLit ^^ {
      case opt ~ optvalue => (opt.trim.toLowerCase(), optvalue)
      case _ => ("", "")
    }

  protected lazy val onTable: Parser[TableIdentifier] =
    ON ~> TABLE ~>  (ident <~ ".").? ~ ident ^^ {
      case dbName ~ tableName =>
        TableIdentifier(tableName, dbName)
    }

  protected lazy val segmentId: Parser[String] =
    numericLit ^^ { u => u } |
    elem("decimal", p => {
      p.getClass.getSimpleName.equals("FloatLit") ||
      p.getClass.getSimpleName.equals("DecimalLit")
    }) ^^ (_.chars)

  def addMVSkipFunction(sql: String): String = {
    lazy val addMVSkipUDF: Parser[String] =
      SELECT ~> restInput <~ opt(";") ^^ {
        case query =>
          "select mv() as mv, " + query
      }
    addMVSkipUDF(new lexical.Scanner(sql)) match {
      case Success(query, _) => query
      case _ =>
        throw new MalformedCarbonCommandException(s"Unsupported query")
    }
  }
}

object MVParser {

  def getMVQuery(query: String, sparkSession: SparkSession): DataFrame = {
    SparkSQLUtil
      .execute(getMVPlan(query, sparkSession), sparkSession)
      .drop(MVUdf.MV_SKIP_RULE_UDF)
  }

  def getMVPlan(query: String, sparkSession: SparkSession): LogicalPlan = {
    val updatedQuery = new MVParser().addMVSkipFunction(query)
    val analyzed = sparkSession.sql(updatedQuery).queryExecution.analyzed
    CarbonMVRules(sparkSession).apply(analyzed)
  }

}

