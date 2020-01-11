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
package org.apache.carbondata.integration.spark.testsuite.primitiveTypes

import java.util.Random

import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for filter query on Double datatypes
 */
class DoubleDataTypeTestCase extends QueryTest with BeforeAndAfterAll {

  lazy val df: DataFrame = generateDataFrame

  private def generateDataFrame(): DataFrame = {
    val r = new Random()
    val rdd = sqlContext.sparkContext
      .parallelize(1 to 10, 2)
      .map { x =>
        Row(x, "London" + (x % 2), x.toDouble / 13, x.toDouble / 11)
      }

    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("city", StringType, nullable = false),
        StructField("m1", DoubleType, nullable = false),
        StructField("m2", DoubleType, nullable = false)
      )
    )

    sqlContext.createDataFrame(rdd, schema)
  }

  override def beforeAll {
    sql("drop table if exists uniq_carbon")
    sql("drop table if exists uniq_hive")
    sql("drop table if exists doubleTypeCarbonTable")
    sql("drop table if exists doubleTypeHiveTable")

    df.write
      .format("carbondata")
      .option("tableName", "doubleTypeCarbonTable")
      .option("tempCSV", "false")
      .option("table_blocksize", "32")
      .mode(SaveMode.Overwrite)
      .save()

    df.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("doubleTypeHiveTable")

  }

  test("detail query") {
    checkAnswer(sql("select * from doubleTypeCarbonTable order by id"),
      sql("select * from doubleTypeHiveTable order by id"))

  }

  test("duplicate values") {
    sql("create table uniq_carbon(name string, double_column double) STORED AS carbondata ")
    sql(s"load data inpath '$resourcesPath/uniq.csv' into table uniq_carbon")
    sql("create table uniq_hive(name string, double_column double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    sql(s"load data local inpath '$resourcesPath/uniqwithoutheader.csv' into table uniq_hive")
    checkAnswer(sql("select * from uniq_carbon where double_column>=11"),
      sql("select * from uniq_hive where double_column>=11"))
  }

//  test("agg query") {
//    checkAnswer(sql("select city, sum(m1), avg(m1), count(m1), max(m1), min(m1) from doubleTypeCarbonTable group by city"),
//      sql("select city, sum(m1), avg(m1), count(m1), max(m1), min(m1) from doubleTypeHiveTable group by city"))
//
//    checkAnswer(sql("select city, sum(m2), avg(m2), count(m2), max(m2), min(m2) from doubleTypeCarbonTable group by city"),
//      sql("select city, sum(m2), avg(m2), count(m2), max(m2), min(m2) from doubleTypeHiveTable group by city"))
//  }

  override def afterAll {
    sql("drop table if exists uniq_carbon")
    sql("drop table if exists uniq_hive")
    sql("drop table if exists doubleTypeCarbonTable")
    sql("drop table if exists doubleTypeHiveTable")
  }
}