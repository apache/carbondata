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
  }

  def dropTable() = {
    sql("DROP TABLE IF EXISTS carbon1")
    sql("DROP TABLE IF EXISTS carbon2")
    sql("DROP TABLE IF EXISTS carbon3")
    sql("DROP TABLE IF EXISTS carbon4")

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

  override def afterAll {
    dropTable
  }
}

