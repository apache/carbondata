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

import scala.language.implicitConversions
import scala.util.matching.Regex
import scala.util.parsing.combinator.PackratParsers
import scala.util.parsing.combinator.syntactical.StandardTokenParsers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.SqlLexical
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.optimizer.MVRewriteRule
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.view.MVFunctions

class MVQueryParser extends StandardTokenParsers with PackratParsers {

  // Keywords used in this parser
  protected val SELECT: Regex = carbonKeyWord("SELECT")

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

  // Returns the rest of the input string that are not parsed yet
  private lazy val query: Parser[String] = new Parser[String] {
    def apply(input: Input): ParseResult[String] =
      Success(
        input.source.subSequence(input.offset, input.source.length()).toString,
        input.drop(input.source.length()))
  }

  private lazy val queryAppendDummyFunction: Parser[String] =
    SELECT ~> query <~ opt(";") ^^ {
      case query =>
        "SELECT " +
        MVFunctions.DUMMY_FUNCTION + "() as " +
        MVFunctions.DUMMY_FUNCTION + ", " + query
    }

  def parseAndAppendDummyFunction(sql: String): String = {
    queryAppendDummyFunction(new lexical.Scanner(sql)) match {
      case Success(query, _) => query
      case _ =>
        throw new MalformedCarbonCommandException(s"Unsupported query")
    }
  }

}

object MVQueryParser {

  def getQuery(query: String, session: SparkSession): DataFrame = {
    SparkSQLUtil
      .execute(getQueryPlan(query, session), session)
      .drop(MVFunctions.DUMMY_FUNCTION)
  }

  def getQueryPlan(query: String, session: SparkSession): LogicalPlan = {
    val updatedQuery = new MVQueryParser().parseAndAppendDummyFunction(query)
    val analyzedPlan = session.sql(updatedQuery).queryExecution.analyzed
    analyzedPlan match {
      case _: Command => analyzedPlan
      case _ =>
        val optimizedRule = new MVRewriteRule(session)
        if (optimizedRule != null) {
          optimizedRule.apply(analyzedPlan)
        } else {
          analyzedPlan
        }
    }
  }

}
