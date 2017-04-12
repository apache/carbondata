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

package org.apache.carbondata.spark.testsuite.dataload

import java.math.BigDecimal

import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.types.{DecimalType, DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.scalatest.BeforeAndAfterAll

class TestLoadDataFrame extends QueryTest with BeforeAndAfterAll {
  var df: DataFrame = _
  var dataFrame: DataFrame = _
  var df2: DataFrame = _


  def buildTestData() = {
    import sqlContext.implicits._
    df = sqlContext.sparkContext.parallelize(1 to 1000)
      .map(x => ("a", "b", x))
      .toDF("c1", "c2", "c3")

    val rdd = sqlContext.sparkContext.parallelize(
      Row(52.23, BigDecimal.valueOf(1234.4440), "Warsaw") ::
      Row(42.30, BigDecimal.valueOf(9999.9990), "Corte") :: Nil)

    val schema = StructType(
      StructField("double", DoubleType, nullable = false) ::
      StructField("decimal", DecimalType(9, 2), nullable = false) ::
      StructField("string", StringType, nullable = false) :: Nil)

    dataFrame = sqlContext.createDataFrame(rdd, schema)
    df2 = sqlContext.sparkContext.parallelize(1 to 1000)
      .map(x => ("key_" + x, "str_" + x, x, x * 2, x * 3))
      .toDF("c1", "c2", "c3", "c4", "c5")
  }

  def dropTable() = {
    sql("DROP TABLE IF EXISTS carbon1")
    sql("DROP TABLE IF EXISTS carbon2")
    sql("DROP TABLE IF EXISTS carbon3")
    sql("DROP TABLE IF EXISTS carbon4")
    sql("DROP TABLE IF EXISTS carbon5")
    sql("DROP TABLE IF EXISTS carbon6")
    sql("DROP TABLE IF EXISTS carbon7")
    sql("DROP TABLE IF EXISTS carbon8")
    sql("DROP TABLE IF EXISTS carbon9")
  }



  override def beforeAll {
    dropTable
    buildTestData
  }

  test("test load dataframe with saving compressed csv files") {
    // save dataframe to carbon file
    df.write
      .format("carbondata")
      .option("tableName", "carbon1")
      .option("tempCSV", "true")
      .option("compress", "true")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("select count(*) from carbon1 where c3 > 500"), Row(500)
    )
  }

  test("test load dataframe with saving csv uncompressed files") {
    // save dataframe to carbon file
    df.write
      .format("carbondata")
      .option("tableName", "carbon2")
      .option("tempCSV", "true")
      .option("compress", "false")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("select count(*) from carbon2 where c3 > 500"), Row(500)
    )
  }

  test("test load dataframe without saving csv files") {
    // save dataframe to carbon file
    df.write
      .format("carbondata")
      .option("tableName", "carbon3")
      .option("tempCSV", "false")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("select count(*) from carbon3 where c3 > 500"), Row(500)
    )
  }

  test("test decimal values for dataframe load"){
    dataFrame.write
      .format("carbondata")
      .option("tableName", "carbon4")
      .option("compress", "true")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("SELECT decimal FROM carbon4"),Seq(Row(BigDecimal.valueOf(10000.00)),Row(BigDecimal.valueOf(1234.44))))
  }

  test("test load dataframe with integer columns included in the dictionary"){
    df2.write
      .format("carbondata")
      .option("tableName", "carbon5")
      .option("compress", "true")
      .option("dictionary_include","c3,c4")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("select count(*) from carbon5 where c3 > 300"), Row(700)
    )
  }

  test("test load dataframe with string column excluded from the dictionary"){
    df2.write
      .format("carbondata")
      .option("tableName", "carbon6")
      .option("compress", "true")
      .option("dictionary_exclude","c2")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("select count(*) from carbon6 where c3 > 300"), Row(700)
    )
  }

  test("test load dataframe with both dictionary include and exclude specified"){
    df2.write
      .format("carbondata")
      .option("tableName", "carbon7")
      .option("compress", "true")
      .option("dictionary_include","c3,c4")
      .option("dictionary_exclude","c2")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("select count(*) from carbon7 where c3 > 300"), Row(700)
    )
  }

  test("test load dataframe with single pass enabled") {
    // save dataframe to carbon file
    df.write
      .format("carbondata")
      .option("tableName", "carbon8")
      .option("tempCSV", "false")
      .option("single_pass", "true")
      .option("compress", "false")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("select count(*) from carbon8 where c3 > 500"), Row(500)
    )
  }

  test("test load dataframe with single pass disabled") {
    // save dataframe to carbon file
    df.write
      .format("carbondata")
      .option("tableName", "carbon9")
      .option("tempCSV", "true")
      .option("single_pass", "false")
      .option("compress", "false")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("select count(*) from carbon9 where c3 > 500"), Row(500)
    )
  }

  override def afterAll {
    dropTable
  }
}

