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

package org.apache.carbondata.integration.spark.testsuite.dataload

import java.io.File

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.scalatest.BeforeAndAfterAll

class SparkDatasourceSuite extends QueryTest with BeforeAndAfterAll {

  var currentDirectory: String = _
  var df: DataFrame = _

  override def beforeAll {
    sql("DROP TABLE IF EXISTS carbon1")
    sql("DROP TABLE IF EXISTS carbon2")
    sql("DROP TABLE IF EXISTS carbon3")

    currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
        .getCanonicalPath
    import implicits._
    df = sc.parallelize(1 to 1000)
        .map(x => ("a", "b", x))
        .toDF("c1", "c2", "c3")

    // save dataframe to carbon file
    df.write
        .format("carbondata")
        .mode(SaveMode.Overwrite)
        .save(s"$storePath/${CarbonCommonConstants.DATABASE_DEFAULT_NAME}/carbon1")
  }

  test("read and write using CarbonContext") {
    val in = read
        .format("carbondata")
        .load(s"$storePath/${CarbonCommonConstants.DATABASE_DEFAULT_NAME}/carbon1")

    assert(in.where("c3 > 500").count() == 500)
  }

  test("read and write using CarbonContext with compression") {
    df.write
        .format("carbondata")
        .option("compress", "true")
        .mode(SaveMode.Overwrite)
        .save(s"$storePath/${CarbonCommonConstants.DATABASE_DEFAULT_NAME}/carbon2")

    val in = read
        .format("carbondata")
        .load(s"$storePath/${CarbonCommonConstants.DATABASE_DEFAULT_NAME}/carbon2")

    assert(in.where("c3 > 500").count() == 500)
  }

  test("saveAsCarbon API") {
    import org.apache.carbondata.spark._
    df.saveAsCarbonFile(Map("path" -> s"$storePath/${CarbonCommonConstants.DATABASE_DEFAULT_NAME}/carbon3"))

    checkAnswer(sql("SELECT count(*) FROM carbon3 WHERE c3 > 100"), Seq(Row(900)))
  }

  test("query using SQLContext") {
    val sqlContext = new SQLContext(sparkContext)
    sqlContext.sql(
      s"""
         | CREATE TEMPORARY TABLE temp
         | (c1 string, c2 string, c3 long)
         | USING org.apache.spark.sql.CarbonSource
         | OPTIONS (path '$storePath/default/carbon1')
      """.stripMargin)
    checkAnswer(sqlContext.sql(
      """
        | SELECT c1, c2, count(*)
        | FROM temp
        | WHERE c3 > 100
        | GROUP BY c1, c2
      """.stripMargin), Seq(Row("a", "b", 900)))
    sqlContext.dropTempTable("temp")
  }

  test("query using SQLContext without providing schema") {
    val sqlContext = new SQLContext(sparkContext)
    sqlContext.sql(
      s"""
         | CREATE TEMPORARY TABLE temp
         | USING org.apache.spark.sql.CarbonSource
         | OPTIONS (path '$storePath/default/carbon1')
      """.stripMargin)
    checkAnswer(sqlContext.sql(
      """
        | SELECT c1, c2, count(*)
        | FROM temp
        | WHERE c3 > 100
        | GROUP BY c1, c2
      """.stripMargin), Seq(Row("a", "b", 900)))
    sqlContext.dropTempTable("temp")
  }

  test("query using SQLContext, multiple load") {
    sql("DROP TABLE IF EXISTS test")
    sql(
      """
        | CREATE TABLE test(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    val testData = currentDirectory + "/src/test/resources/sample.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table test")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table test")

    val sqlContext = new SQLContext(sparkContext)
    sqlContext.sql(
      s"""
         | CREATE TEMPORARY TABLE temp
         | (id long, name string, city string, age long)
         | USING org.apache.spark.sql.CarbonSource
         | OPTIONS (path '$storePath/default/test')
      """.stripMargin)
    checkAnswer(sqlContext.sql(
      """
        | SELECT count(id)
        | FROM temp
      """.stripMargin), Seq(Row(8)))
    sqlContext.dropTempTable("temp")
    sql("DROP TABLE test")
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS carbon1")
    sql("DROP TABLE IF EXISTS carbon2")
    sql("DROP TABLE IF EXISTS carbon3")
  }
}
