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

package org.apache.spark.sql.common.util

import java.util.{Locale, TimeZone}

import org.apache.carbondata.common.logging.LogServiceFactory
import scala.collection.JavaConversions._

import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.command.LoadDataCommand
import org.apache.spark.sql.test.{ResourceRegisterAndCopier, TestQueryExecutor}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.scalatest.Suite

class QueryTest extends PlanTest with Suite {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  val DOLLAR = "$"

  // Timezone is fixed to America/Los_Angeles for those timezone sensitive tests (timestamp_*)
  TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
  // Add Locale setting
  Locale.setDefault(Locale.US)

  /**
   * Runs the plan and makes sure the answer contains all of the keywords, or the
   * none of keywords are listed in the answer
   * @param df the [[DataFrame]] to be executed
   * @param exists true for make sure the keywords are listed in the output, otherwise
   *               to make sure none of the keyword are not listed in the output
   * @param keywords keyword in string array
   */
  def checkExistence(df: DataFrame, exists: Boolean, keywords: String*) {
    val outputs = df.collect().map(_.mkString).mkString
    for (key <- keywords) {
      if (exists) {
        assert(outputs.contains(key), s"Failed for $df ($key doesn't exist in result)")
      } else {
        assert(!outputs.contains(key), s"Failed for $df ($key existed in the result)")
      }
    }
  }

  def sqlTest(sqlString: String, expectedAnswer: Seq[Row])(implicit sqlContext: SQLContext) {
    test(sqlString) {
      checkAnswer(sqlContext.sql(sqlString), expectedAnswer)
    }
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param df the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   */
  protected def checkAnswer(df: DataFrame, expectedAnswer: Seq[Row]): Unit = {
    QueryTest.checkAnswer(df, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  protected def checkAnswer(df: DataFrame, expectedAnswer: Row): Unit = {
    checkAnswer(df, Seq(expectedAnswer))
  }

  protected def checkAnswer(df: DataFrame, expectedAnswer: DataFrame): Unit = {
    checkAnswer(df, expectedAnswer.collect())
  }

  protected def checkAnswer(carbon: String, hive: String): Unit = {
    checkAnswer(sql(carbon), sql(hive))
  }

  def sql(sqlText: String): DataFrame = {
    val frame = TestQueryExecutor.INSTANCE.sql(sqlText)
    val plan = frame.queryExecution.logical
    if (TestQueryExecutor.hdfsUrl.startsWith("hdfs")) {
      plan match {
        case l: LoadDataCommand =>
          val copyPath = TestQueryExecutor.warehouse + "/" + l.table.table.toLowerCase +
                         l.path.substring(l.path.lastIndexOf("/"), l.path.length)
          ResourceRegisterAndCopier.copyLocalFile(l.path, copyPath)
        case _ =>
      }
    }
    frame
  }

  val sqlContext: SQLContext = TestQueryExecutor.INSTANCE.sqlContext

  val storeLocation = TestQueryExecutor.storeLocation
  val resourcesPath = TestQueryExecutor.resourcesPath
  val integrationPath = TestQueryExecutor.integrationPath
}

object QueryTest {

  def checkAnswer(df: DataFrame, expectedAnswer: java.util.List[Row]): String = {
    checkAnswer(df, expectedAnswer.toSeq) match {
      case Some(errorMessage) => errorMessage
      case None => null
    }
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * If there was exception during the execution or the contents of the DataFrame does not
   * match the expected result, an error message will be returned. Otherwise, a [[None]] will
   * be returned.
   * @param df the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   */
  def checkAnswer(df: DataFrame, expectedAnswer: Seq[Row]): Option[String] = {
    val isSorted = df.logicalPlan.collect { case s: logical.Sort => s }.nonEmpty
    def prepareAnswer(answer: Seq[Row]): Seq[Row] = {
      // Converts data to types that we can do equality comparison using Scala collections.
      // For BigDecimal type, the Scala type has a better definition of equality test (similar to
      // Java's java.math.BigDecimal.compareTo).
      // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
      // equality test.
      val converted: Seq[Row] = answer.map { s =>
        Row.fromSeq(s.toSeq.map {
          case d: java.math.BigDecimal => BigDecimal(d)
          case b: Array[Byte] => b.toSeq
          case o => o
        })
      }
      if (!isSorted) converted.sortBy(_.toString()) else converted
    }
    val sparkAnswer = try df.collect().toSeq catch {
      case e: Exception =>
        val errorMessage =
          s"""
             |Exception thrown while executing query:
             |== Exception ==
             |$e
             |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        return Some(errorMessage)
    }

    if (prepareAnswer(expectedAnswer) != prepareAnswer(sparkAnswer)) {
      val errorMessage =
        s"""
           |Results do not match for query:
           |== Results ==
           |${
          sideBySide(
            s"== Correct Answer - ${expectedAnswer.size} ==" +:
              prepareAnswer(expectedAnswer).map(_.toString()),
            s"== Spark Answer - ${sparkAnswer.size} ==" +:
              prepareAnswer(sparkAnswer).map(_.toString())).mkString("\n")
        }
      """.stripMargin
      return Some(errorMessage)
    }

    return None
  }
}
