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

package org.apache.spark.sql.test.util

import java.util.{Locale, ServiceLoader, TimeZone}

import scala.collection.JavaConverters._

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.test.{TestQueryExecutor, TestQueryExecutorRegister}
import org.apache.spark.util.Utils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class CarbonQueryTest extends PlanTest {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  // Timezone is fixed to America/Los_Angeles for those timezone sensitive tests (timestamp_*)
  TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
  // Add Locale setting
  Locale.setDefault(Locale.US)

  CarbonProperties.getInstance()
    .addProperty(CarbonCommonConstants.SUPPORT_DIRECT_QUERY_ON_DATAMAP, "true")

  /**
   * Runs the plan and makes sure the answer contains all of the keywords, or the
   * none of keywords are listed in the answer
   * @param df the [[DataFrame]] to be executed
   * @param exists true for make sure the keywords are listed in the output, otherwise
   *               to make sure none of the keyword are not listed in the output
   * @param keywords keyword in string array
   */
  def checkExistence(df: DataFrame, exists: Boolean, keywords: String*) {
    val outputs = df.collect().map(_.mkString(" ")).mkString(" ")
    for (key <- keywords) {
      if (exists) {
        assert(outputs.contains(key), s"Failed for $df ($key doesn't exist in result)")
      } else {
        assert(!outputs.contains(key), s"Failed for $df ($key existed in the result)")
      }
    }
  }

  /**
   * Runs the plan and counts the keyword in the answer
   * @param df the [[DataFrame]] to be executed
   * @param count expected count
   * @param keyword keyword to search
   */
  def checkExistenceCount(df: DataFrame, count: Long, keyword: String): Unit = {
    val outputs = df.collect().map(_.mkString).mkString
    assert(outputs.sliding(keyword.length).count(_ == keyword) === count)
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

  protected def dropTable(tableName: String): Unit = {
    sql(s"DROP TABLE IF EXISTS $tableName")
  }

  protected def dropDataMaps(tableName: String, dataMapNames: String*): Unit = {
    for (dataMapName <- dataMapNames) {
      sql(s"DROP DATAMAP IF EXISTS $dataMapName ON TABLE $tableName")
    }
  }

  val exec = {
    import scala.collection.JavaConverters._
    ServiceLoader.load(classOf[TestQueryExecutorRegister], Utils.getContextOrSparkClassLoader)
      .asScala
      .filter(instance => instance
        .getClass
        .getName.equals("org.apache.spark.sql.test.CarbonSpark2TestQueryExecutor"))
      .head.getClass.newInstance()
  }

  def sql(sqlText: String): DataFrame = exec.sql(sqlText)

  val sqlContext: SQLContext = exec.sqlContext

  lazy val warehouse = TestQueryExecutor.warehouse
  lazy val storeLocation = CarbonProperties.getInstance().
    getProperty(CarbonCommonConstants.STORE_LOCATION)
  val resourcesPath = TestQueryExecutor.resourcesPath
  val metaStoreDB = TestQueryExecutor.metaStoreDB
  val integrationPath = TestQueryExecutor.integrationPath
  val dblocation = TestQueryExecutor.location
  val defaultParallelism = sqlContext.sparkContext.defaultParallelism

}

object CarbonQueryTest {

  def checkAnswer(df: DataFrame, expectedAnswer: java.util.List[Row]): String = {
    checkAnswer(df, expectedAnswer.asScala) match {
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
          case d : Double =>
            if (!d.isInfinite && !d.isNaN) {
              var bd = BigDecimal(d)
              bd = bd.setScale(5, BigDecimal.RoundingMode.UP)
              bd.doubleValue()
            }
            else {
              d
            }
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
             |${df.queryExecution}
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
           |${df.queryExecution}
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
