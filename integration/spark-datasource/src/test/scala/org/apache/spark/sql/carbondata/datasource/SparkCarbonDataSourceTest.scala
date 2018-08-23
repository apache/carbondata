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
package org.apache.spark.sql.carbondata.datasource


import org.apache.spark.sql.carbondata.datasource.TestUtil._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier

class SparkCarbonDataSourceTest extends FunSuite  with BeforeAndAfterAll {


  test("test write using dataframe") {
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")
    spark.sql("drop table if exists testformat")
    // Saves dataframe to carbon file
    df.write
      .format("carbon").saveAsTable("testformat")
    assert(spark.sql("select * from testformat").count() == 10)
    assert(spark.sql("select * from testformat where c1='a0'").count() == 1)
    assert(spark.sql("select * from testformat").count() == 10)
    spark.sql("drop table if exists testformat")
  }

  test("test write using ddl") {
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")
    spark.sql("drop table if exists testparquet")
    spark.sql("drop table if exists testformat")
    // Saves dataframe to carbon file
    df.write
      .format("parquet").saveAsTable("testparquet")
    spark.sql("create table carbon_table(c1 string, c2 string, number int) using carbon")
    spark.sql("insert into carbon_table select * from testparquet")
    TestUtil.checkAnswer(spark.sql("select * from carbon_table"), spark.sql("select * from testparquet"))
    val mapSize = DataMapStoreManager.getInstance().getAllDataMaps.size()
    DataMapStoreManager.getInstance().clearDataMaps(AbsoluteTableIdentifier.from(warehouse1+"/carbon_table", "", ""))
    assert(mapSize > DataMapStoreManager.getInstance().getAllDataMaps.size())
    spark.sql("drop table if exists testparquet")
    spark.sql("drop table if exists testformat")
  }

  test("test read with df write") {
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")

    // Saves dataframe to carbon file
    df.write.format("carbon").save(warehouse1 + "/test_folder/")

    val frame = spark.read.format("carbon").load(warehouse1 + "/test_folder")
    frame.show()
    assert(frame.count() == 10)
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
  }

  test("test write using subfolder") {
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")

    // Saves dataframe to carbon file
    df.write.format("carbon").save(warehouse1 + "/test_folder/"+System.nanoTime())
    df.write.format("carbon").save(warehouse1 + "/test_folder/"+System.nanoTime())
    df.write.format("carbon").save(warehouse1 + "/test_folder/"+System.nanoTime())

    val frame = spark.read.format("carbon").load(warehouse1 + "/test_folder")
    assert(frame.count() == 30)
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
  }

  test("test write using partition ddl") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists testparquet")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")

    // Saves dataframe to carbon file
    df.write
      .format("parquet").partitionBy("c2").saveAsTable("testparquet")
    spark.sql("create table carbon_table(c1 string, c2 string, number int) using carbon  PARTITIONED by (c2)")
    spark.sql("insert into carbon_table select * from testparquet")
    assert(spark.sql("select * from carbon_table").count() == 10)
    TestUtil.checkAnswer(spark.sql("select * from carbon_table"), spark.sql("select * from testparquet"))
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists testparquet")
  }

  test("test write with struct type") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, ("b", "c"), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    spark.sql("describe parquet_table").show(false)
    spark.sql("create table carbon_table(c1 string, c2 struct<a1:string, a2:string>, number int) using carbon")
    spark.sql("insert into carbon_table select * from parquet_table")
    assert(spark.sql("select * from carbon_table").count() == 10)
    TestUtil.checkAnswer(spark.sql("select * from carbon_table"), spark.sql("select * from parquet_table"))
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
  }

  test("test write with array type") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Array("b", "c"), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    spark.sql("describe parquet_table").show(false)
    spark.sql("create table carbon_table(c1 string, c2 array<string>, number int) using carbon")
    spark.sql("insert into carbon_table select * from parquet_table")
    assert(spark.sql("select * from carbon_table").count() == 10)
    TestUtil.checkAnswer(spark.sql("select * from carbon_table"), spark.sql("select * from parquet_table"))
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
  }

  test("test write with nested array and struct type") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Array(("1", "2"), ("3", "4")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    spark.sql("describe parquet_table").show(false)
    spark.sql("create table carbon_table(c1 string, c2 array<struct<a1:string, a2:string>>, number int) using carbon")
    spark.sql("insert into carbon_table select * from parquet_table")
    assert(spark.sql("select * from carbon_table").count() == 10)
    TestUtil.checkAnswer(spark.sql("select * from carbon_table"), spark.sql("select * from parquet_table"))
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
  }

  test("test write with nested struct and array type") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, (Array("1", "2"), ("3", "4")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    spark.sql("create table carbon_table(c1 string, c2 struct<a1:array<string>, a2:struct<a1:string, a2:string>>, number int) using carbon")
    spark.sql("insert into carbon_table select * from parquet_table")
    assert(spark.sql("select * from carbon_table").count() == 10)
    TestUtil.checkAnswer(spark.sql("select * from carbon_table"), spark.sql("select * from parquet_table"))
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
  }


  test("test write using ddl and options") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists testparquet")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")

    // Saves dataframe to carbon file
    df.write
      .format("parquet").saveAsTable("testparquet")
    spark.sql("create table carbon_table(c1 string, c2 string, number int) using carbon options('table_blocksize'='256')")
    TestUtil.checkExistence(spark.sql("describe formatted carbon_table"), true, "table_blocksize")
    spark.sql("insert into carbon_table select * from testparquet")
    spark.sql("select * from carbon_table").show()
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists testparquet")
  }

  test("test read with nested struct and array type without creating table") {
    FileFactory
      .deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_carbon_folder"))
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, (Array("1", "2"), ("3", "4")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    val frame = spark.sql("select * from parquet_table")
    frame.write.format("carbon").save(warehouse1 + "/test_carbon_folder")
    val dfread = spark.read.format("carbon").load(warehouse1 + "/test_carbon_folder")
    dfread.show(false)
    FileFactory
      .deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_carbon_folder"))
    spark.sql("drop table if exists parquet_table")
  }


  test("test read and write with date datatype") {
    spark.sql("drop table if exists date_table")
    spark.sql("drop table if exists date_parquet_table")
    spark.sql("create table date_table(empno int, empname string, projdate Date) using carbon")
    spark.sql("insert into  date_table select 11, 'ravi', '2017-11-11'")
    spark.sql("create table date_parquet_table(empno int, empname string, projdate Date) using parquet")
    spark.sql("insert into  date_parquet_table select 11, 'ravi', '2017-11-11'")
    checkAnswer(spark.sql("select * from date_table"), spark.sql("select * from date_parquet_table"))
    spark.sql("drop table if exists date_table")
    spark.sql("drop table if exists date_parquet_table")
  }

  test("test read and write with date datatype with wrong format") {
    spark.sql("drop table if exists date_table")
    spark.sql("drop table if exists date_parquet_table")
    spark.sql("create table date_table(empno int, empname string, projdate Date) using carbon")
    spark.sql("insert into  date_table select 11, 'ravi', '11-11-2017'")
    spark.sql("create table date_parquet_table(empno int, empname string, projdate Date) using parquet")
    spark.sql("insert into  date_parquet_table select 11, 'ravi', '11-11-2017'")
    checkAnswer(spark.sql("select * from date_table"), spark.sql("select * from date_parquet_table"))
    spark.sql("drop table if exists date_table")
    spark.sql("drop table if exists date_parquet_table")
  }

  test("test read and write with timestamp datatype") {
    spark.sql("drop table if exists date_table")
    spark.sql("drop table if exists date_parquet_table")
    spark.sql("create table date_table(empno int, empname string, projdate timestamp) using carbon")
    spark.sql("insert into  date_table select 11, 'ravi', '2017-11-11 00:00:01'")
    spark.sql("create table date_parquet_table(empno int, empname string, projdate timestamp) using parquet")
    spark.sql("insert into  date_parquet_table select 11, 'ravi', '2017-11-11 00:00:01'")
    checkAnswer(spark.sql("select * from date_table"), spark.sql("select * from date_parquet_table"))
    spark.sql("drop table if exists date_table")
    spark.sql("drop table if exists date_parquet_table")
  }

  test("test read and write with timestamp datatype with wrong format") {
    spark.sql("drop table if exists date_table")
    spark.sql("drop table if exists date_parquet_table")
    spark.sql("create table date_table(empno int, empname string, projdate timestamp) using carbon")
    spark.sql("insert into  date_table select 11, 'ravi', '11-11-2017 00:00:01'")
    spark.sql("create table date_parquet_table(empno int, empname string, projdate timestamp) using parquet")
    spark.sql("insert into  date_parquet_table select 11, 'ravi', '11-11-2017 00:00:01'")
    checkAnswer(spark.sql("select * from date_table"), spark.sql("select * from date_parquet_table"))
    spark.sql("drop table if exists date_table")
    spark.sql("drop table if exists date_parquet_table")
  }


  override protected def beforeAll(): Unit = {
    drop
  }

  override def afterAll():Unit = {
    drop
  }

  private def drop = {
    spark.sql("drop table if exists testformat")
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists testparquet")
  }
}
