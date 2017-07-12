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

package org.apache.carbondata.integration.spark.testsuite.dataload


import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.scalatest.BeforeAndAfterAll

class SparkDatasourceSuite extends QueryTest with BeforeAndAfterAll {

  var df: DataFrame = _

  override def beforeAll {
    sql("DROP TABLE IF EXISTS carbon1")

    import sqlContext.implicits._
    df = sqlContext.sparkContext.parallelize(1 to 1000)
        .map(x => ("a", "b", x))
        .toDF("c1", "c2", "c3")

    // save dataframe to carbon file
    df.write
        .format("carbondata")
        .option("tableName", "carbon1")
        .mode(SaveMode.Overwrite)
        .save()
  }

  test("read and write using CarbonContext") {
    val in = sqlContext.read
        .format("carbondata")
        .option("tableName", "carbon1")
        .load()

    assert(in.where("c3 > 500").count() == 500)
  }

  test("read and write using CarbonContext with compression") {
    val in = sqlContext.read
        .format("carbondata")
        .option("tableName", "carbon1")
        .option("compress", "true")
        .load()

    assert(in.where("c3 > 500").count() == 500)
  }

  test("test overwrite") {
    sql("DROP TABLE IF EXISTS carbon4")
    df.write
        .format("carbondata")
        .option("tableName", "carbon4")
        .mode(SaveMode.Overwrite)
        .save()
    df.write
        .format("carbondata")
        .option("tableName", "carbon4")
        .mode(SaveMode.Overwrite)
        .save()
    val in = sqlContext.read
        .format("carbondata")
        .option("tableName", "carbon4")
        .load()
    assert(in.where("c3 > 500").count() == 500)
    sql("DROP TABLE IF EXISTS carbon4")
  }

  test("read and write using CarbonContext, multiple load") {
    sql("DROP TABLE IF EXISTS carbon4")
    df.write
        .format("carbondata")
        .option("tableName", "carbon4")
        .mode(SaveMode.Overwrite)
        .save()
    df.write
        .format("carbondata")
        .option("tableName", "carbon4")
        .mode(SaveMode.Append)
        .save()
    val in = sqlContext.read
        .format("carbondata")
        .option("tableName", "carbon4")
        .load()
    assert(in.where("c3 > 500").count() == 1000)
    sql("DROP TABLE IF EXISTS carbon4")
  }
  
  test("query using SQLContext") {
    val newSQLContext = new SQLContext(sqlContext.sparkContext)
    newSQLContext.sql(
      s"""
         | CREATE TEMPORARY TABLE temp
         | (c1 string, c2 string, c3 int)
         | USING org.apache.spark.sql.CarbonSource
         | OPTIONS (path '$storeLocation/default/carbon1')
      """.stripMargin)
    checkAnswer(newSQLContext.sql(
      """
        | SELECT c1, c2, count(*)
        | FROM temp
        | WHERE c3 > 100
        | GROUP BY c1, c2
      """.stripMargin), Seq(Row("a", "b", 900)))
    newSQLContext.dropTempTable("temp")
  }

  test("query using SQLContext without providing schema") {
    val newSQLContext = new SQLContext(sqlContext.sparkContext)
    newSQLContext.sql(
      s"""
         | CREATE TEMPORARY TABLE temp
         | USING org.apache.spark.sql.CarbonSource
         | OPTIONS (path '$storeLocation/default/carbon1')
      """.stripMargin)
    checkAnswer(newSQLContext.sql(
      """
        | SELECT c1, c2, count(*)
        | FROM temp
        | WHERE c3 > 100
        | GROUP BY c1, c2
      """.stripMargin), Seq(Row("a", "b", 900)))
    newSQLContext.dropTempTable("temp")
  }

  test("query using SQLContext, multiple load") {
    sql("DROP TABLE IF EXISTS test")
    sql(
      """
        | CREATE TABLE test(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    val testData = s"${resourcesPath}/sample.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table test")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table test")

    val newSQLContext = new SQLContext(sqlContext.sparkContext)
    newSQLContext.sql(
      s"""
         | CREATE TEMPORARY TABLE temp
         | (id int, name string, city string, age int)
         | USING org.apache.spark.sql.CarbonSource
         | OPTIONS (path '$storeLocation/default/test')
      """.stripMargin)
    checkAnswer(newSQLContext.sql(
      """
        | SELECT count(id)
        | FROM temp
      """.stripMargin), Seq(Row(8)))
    newSQLContext.dropTempTable("temp")
    sql("DROP TABLE test")
  }

  test("json data with long datatype issue CARBONDATA-405") {
    val jsonDF = sqlContext.read.format("json").load(s"$resourcesPath/test.json")
    jsonDF.write
      .format("carbondata")
      .option("tableName", "dftesttable")
      .option("compress", "true")
      .mode(SaveMode.Overwrite)
      .save()
    val carbonDF = sqlContext
      .read
      .format("carbondata")
      .option("tableName", "dftesttable")
      .load()
    checkAnswer(
      carbonDF.select("age", "name"),
      jsonDF.select("age", "name"))
    sql("drop table dftesttable")
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS carbon1")
    sql("DROP TABLE IF EXISTS carbon2")
  }
}
