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


import java.io.File
import java.util
import java.util.Arrays

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.carbondata.datasource.TestUtil._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.datatype.{DataTypes, StructField}
import org.apache.carbondata.hadoop.testutil.StoreCreator
import org.apache.carbondata.sdk.file.{CarbonWriter, Field, Schema}

class SparkCarbonDataSourceTest extends FunSuite with BeforeAndAfterAll {


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
    TestUtil.checkAnswer(spark.sql("select * from carbon_table where c1='a1'"), spark.sql("select * from testparquet where c1='a1'"))
    if (!spark.sparkContext.version.startsWith("2.1")) {
      val mapSize = DataMapStoreManager.getInstance().getAllDataMaps.size()
      DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(warehouse1 + "/carbon_table"))
      assert(mapSize > DataMapStoreManager.getInstance().getAllDataMaps.size())
    }
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
    if (!spark.sparkContext.version.startsWith("2.1")) {
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
      assert(frame.where("c1='a1'").count() == 3)

        val mapSize = DataMapStoreManager.getInstance().getAllDataMaps.size()
        DataMapStoreManager.getInstance()
          .clearDataMaps(AbsoluteTableIdentifier.from(warehouse1 + "/test_folder"))
        assert(mapSize > DataMapStoreManager.getInstance().getAllDataMaps.size())
      FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
    }
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
    // TODO fix in 2.1
    if (!spark.sparkContext.version.startsWith("2.1")) {
      assert(spark.sql("select * from carbon_table").count() == 10)
      TestUtil
        .checkAnswer(spark.sql("select * from carbon_table"),
          spark.sql("select * from testparquet"))
    }
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

  test("test write with array type with value as nested map type") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Array(Map("b" -> "c")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    spark.sql("create table carbon_table(c1 string, c2 array<map<string,string>>, number int) using carbon")
    spark.sql("insert into carbon_table select * from parquet_table")
    assert(spark.sql("select * from carbon_table").count() == 10)
    TestUtil.checkAnswer(spark.sql("select * from carbon_table"), spark.sql("select * from parquet_table"))
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
  }

  test("test write with array type with value as nested array<array<map>> type") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Array(Array(Map("b" -> "c"))), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    spark.sql("create table carbon_table(c1 string, c2 array<array<map<string,string>>>, number int) using carbon")
    spark.sql("insert into carbon_table select * from parquet_table")
    assert(spark.sql("select * from carbon_table").count() == 10)
    TestUtil.checkAnswer(spark.sql("select * from carbon_table"), spark.sql("select * from parquet_table"))
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
  }

  test("test write with struct type with value as nested map type") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, ("a", Map("b" -> "c")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    spark.sql("create table carbon_table(c1 string, c2 struct<a1:string, a2:map<string,string>>, number int) using carbon")
    spark.sql("insert into carbon_table select * from parquet_table")
    assert(spark.sql("select * from carbon_table").count() == 10)
    TestUtil.checkAnswer(spark.sql("select * from carbon_table"), spark.sql("select * from parquet_table"))
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
  }

  test("test write with struct type with value as nested struct<array<map>> type") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, ("a", Array(Map("b" -> "c"))), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    spark.sql("create table carbon_table(c1 string, c2 struct<a1:string, a2:array<map<string,string>>>, number int) using carbon")
    spark.sql("insert into carbon_table select * from parquet_table")
    assert(spark.sql("select * from carbon_table").count() == 10)
    TestUtil.checkAnswer(spark.sql("select * from carbon_table"), spark.sql("select * from parquet_table"))
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
  }

  test("test write with map type") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Map("b" -> "c"), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    spark.sql("create table carbon_table(c1 string, c2 map<string, string>, number int) using carbon")
    spark.sql("insert into carbon_table select * from parquet_table")
    assert(spark.sql("select * from carbon_table").count() == 10)
    TestUtil.checkAnswer(spark.sql("select * from carbon_table"), spark.sql("select * from parquet_table"))
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
  }

  test("test write with map type with Int data type as key") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Map(99 -> "c"), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    spark.sql("create table carbon_table(c1 string, c2 map<int, string>, number int) using carbon")
    spark.sql("insert into carbon_table select * from parquet_table")
    assert(spark.sql("select * from carbon_table").count() == 10)
    TestUtil.checkAnswer(spark.sql("select * from carbon_table"), spark.sql("select * from parquet_table"))
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
  }

  test("test write with map type with value as nested map type") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Map("a" -> Map("b" -> "c")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    spark.sql("create table carbon_table(c1 string, c2 map<string, map<string, string>>, number int) using carbon")
    spark.sql("insert into carbon_table select * from parquet_table")
    assert(spark.sql("select * from carbon_table").count() == 10)
    TestUtil.checkAnswer(spark.sql("select * from carbon_table"), spark.sql("select * from parquet_table"))
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
  }

  test("test write with map type with value as nested struct type") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Map("a" -> ("b", "c")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    spark.sql("create table carbon_table(c1 string, c2 map<string, struct<a1:string, a2:string>>, number int) using carbon")
    spark.sql("insert into carbon_table select * from parquet_table")
    assert(spark.sql("select * from carbon_table").count() == 10)
    TestUtil.checkAnswer(spark.sql("select * from carbon_table"), spark.sql("select * from parquet_table"))
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
  }

  test("test write with map type with value as nested array type") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Map("a" -> Array("b", "c")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    spark.sql("create table carbon_table(c1 string, c2 map<string, array<string>>, number int) using carbon")
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

  test("test write with array type with filter") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Array("b", "c"), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    spark.sql("create table carbon_table(c1 string, c2 array<string>, number int) using carbon")
    spark.sql("insert into carbon_table select * from parquet_table")
    assert(spark.sql("select * from carbon_table").count() == 10)
    TestUtil.checkAnswer(spark.sql("select * from carbon_table where c1='a1' and c2[0]='b'"), spark.sql("select * from parquet_table where c1='a1' and c2[0]='b'"))
    TestUtil.checkAnswer(spark.sql("select * from carbon_table"), spark.sql("select * from parquet_table"))
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
  }

  test("test write with struct type with filter") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, (Array("1", "2"), ("3", "4")),Array(("1", 1), ("2", 2)), x))
      .toDF("c1", "c2", "c3",  "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    spark.sql("create table carbon_table(c1 string, c2 struct<a1:array<string>, a2:struct<a1:string, a2:string>>, c3 array<struct<a1:string, a2:int>>, number int) using carbon")
    spark.sql("insert into carbon_table select * from parquet_table")
    assert(spark.sql("select * from carbon_table").count() == 10)
    TestUtil.checkAnswer(spark.sql("select * from carbon_table"), spark.sql("select * from parquet_table"))
    TestUtil.checkAnswer(spark.sql("select * from carbon_table where c2.a1[0]='1' and c1='a1'"), spark.sql("select * from parquet_table where c2._1[0]='1' and c1='a1'"))
    TestUtil.checkAnswer(spark.sql("select * from carbon_table where c2.a1[0]='1' and c3[0].a2=1"), spark.sql("select * from parquet_table where c2._1[0]='1' and c3[0]._2=1"))
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
  }


  test("test read with df write string issue") {
    spark.sql("drop table if exists test123")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x.toShort , x, x.toLong, x.toDouble, BigDecimal.apply(x),  Array(x+1, x), ("b", BigDecimal.apply(x))))
      .toDF("c1", "c2", "shortc", "intc", "longc", "doublec", "bigdecimalc", "arrayc", "structc")


    // Saves dataframe to carbon file
    df.write.format("carbon").save(warehouse1 + "/test_folder/")
    if (!spark.sparkContext.version.startsWith("2.1")) {
      spark
        .sql(s"create table test123 (c1 string, c2 string, shortc smallint,intc int, longc bigint,  doublec double, bigdecimalc decimal(38,18), arrayc array<int>, structc struct<_1:string, _2:decimal(38,18)>) using carbon location '$warehouse1/test_folder/'")

      checkAnswer(spark.sql("select * from test123"),
        spark.read.format("carbon").load(warehouse1 + "/test_folder/"))
    }
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
    spark.sql("drop table if exists test123")
  }

  test("test read with df write with empty data") {
    spark.sql("drop table if exists test123")
    spark.sql("drop table if exists test123_par")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
    // Saves dataframe to carbon file
    if (!spark.sparkContext.version.startsWith("2.1")) {
      spark
        .sql(s"create table test123 (c1 string, c2 string, arrayc array<int>, structc struct<_1:string, _2:decimal(38,18)>, shortc smallint,intc int, longc bigint,  doublec double, bigdecimalc decimal(38,18)) using carbon location '$warehouse1/test_folder/'")

      spark
        .sql(s"create table test123_par (c1 string, c2 string, arrayc array<int>, structc struct<_1:string, _2:decimal(38,18)>, shortc smallint,intc int, longc bigint,  doublec double, bigdecimalc decimal(38,18)) using carbon location '$warehouse1/test_folder/'")
      TestUtil
        .checkAnswer(spark.sql("select count(*) from test123"),
          spark.sql("select count(*) from test123_par"))
    }
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
    spark.sql("drop table if exists test123")
    spark.sql("drop table if exists test123_par")
  }

  test("test write with nosort columns") {
    spark.sql("drop table if exists test123")
    spark.sql("drop table if exists test123_par")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x.toShort , x, x.toLong, x.toDouble, BigDecimal.apply(x),  Array(x+1, x), ("b", BigDecimal.apply(x))))
      .toDF("c1", "c2", "shortc", "intc", "longc", "doublec", "bigdecimalc", "arrayc", "structc")


    // Saves dataframe to carbon file
    df.write.format("parquet").saveAsTable("test123_par")
    if (!spark.sparkContext.version.startsWith("2.1")) {
      spark
        .sql(s"create table test123 (c1 string, c2 string, shortc smallint,intc int, longc bigint,  doublec double, bigdecimalc decimal(38,18), arrayc array<int>, structc struct<_1:string, _2:decimal(38,18)>) using carbon options('sort_columns'='') location '$warehouse1/test_folder/'")

      spark.sql(s"insert into test123 select * from test123_par")
      checkAnswer(spark.sql("select * from test123"), spark.sql(s"select * from test123_par"))
    }
    spark.sql("drop table if exists test123")
    spark.sql("drop table if exists test123_par")
  }

  test("test complex columns mismatch") {
    spark.sql("drop table if exists array_com_hive")
    spark.sql(s"drop table if exists array_com")
    spark.sql("create table array_com_hive (CUST_ID string, YEAR int, MONTH int, AGE int, GENDER string, EDUCATED string, IS_MARRIED string, ARRAY_INT array<int>,ARRAY_STRING array<string>,ARRAY_DATE array<timestamp>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT double, HQ_DEPOSIT double) row format delimited fields terminated by ',' collection items terminated by '$'")
    spark.sql(s"load data local inpath '$resource/Array.csv' into table array_com_hive")
    spark.sql("create table Array_com (CUST_ID string, YEAR int, MONTH int, AGE int, GENDER string, EDUCATED string, IS_MARRIED string, ARRAY_INT array<int>,ARRAY_STRING array<string>,ARRAY_DATE array<timestamp>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT double, HQ_DEPOSIT double) using carbon")
    spark.sql("insert into Array_com select * from array_com_hive")
    TestUtil.checkAnswer(spark.sql("select * from Array_com order by CUST_ID ASC limit 3"), spark.sql("select * from array_com_hive order by CUST_ID ASC limit 3"))
    spark.sql("drop table if exists array_com_hive")
    spark.sql(s"drop table if exists array_com")
  }

  test("test complex columns fail while insert ") {
    spark.sql("drop table if exists STRUCT_OF_ARRAY_com_hive")
    spark.sql(s"drop table if exists STRUCT_OF_ARRAY_com")
    spark.sql(" create table STRUCT_OF_ARRAY_com_hive (CUST_ID string, YEAR int, MONTH int, AGE int, GENDER string, EDUCATED string, IS_MARRIED string, STRUCT_OF_ARRAY struct<ID: int,CHECK_DATE: timestamp ,SNo: array<int>,sal1: array<double>,state: array<string>,date1: array<timestamp>>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT float, HQ_DEPOSIT double) row format delimited fields terminated by ',' collection items terminated by '$' map keys terminated by '&'")
    spark.sql(s"load data local inpath '$resource/structofarray.csv' into table STRUCT_OF_ARRAY_com_hive")
    spark.sql("create table STRUCT_OF_ARRAY_com (CUST_ID string, YEAR int, MONTH int, AGE int, GENDER string, EDUCATED string, IS_MARRIED string, STRUCT_OF_ARRAY struct<ID: int,CHECK_DATE: timestamp,SNo: array<int>,sal1: array<double>,state: array<string>,date1: array<timestamp>>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT double, HQ_DEPOSIT double) using carbon")
    spark.sql(" insert into STRUCT_OF_ARRAY_com select * from STRUCT_OF_ARRAY_com_hive")
    TestUtil.checkAnswer(spark.sql("select * from STRUCT_OF_ARRAY_com  order by CUST_ID ASC"), spark.sql("select * from STRUCT_OF_ARRAY_com_hive  order by CUST_ID ASC"))
    spark.sql("drop table if exists STRUCT_OF_ARRAY_com_hive")
    spark.sql(s"drop table if exists STRUCT_OF_ARRAY_com")
  }

  test("test partition error in carbon") {
    spark.sql("drop table if exists carbon_par")
    spark.sql("drop table if exists parquet_par")
    spark.sql("create table carbon_par (name string, age int, country string) using carbon partitioned by (country)")
    spark.sql("insert into carbon_par select 'b', '12', 'aa'")
    spark.sql("create table parquet_par (name string, age int, country string) using carbon partitioned by (country)")
    spark.sql("insert into parquet_par select 'b', '12', 'aa'")
    checkAnswer(spark.sql("select * from carbon_par"), spark.sql("select * from parquet_par"))
    spark.sql("drop table if exists carbon_par")
    spark.sql("drop table if exists parquet_par")
  }

  test("test more cols error in carbon") {
    spark.sql("drop table if exists h_jin")
    spark.sql("drop table if exists c_jin")
    spark.sql(s"""create table h_jin(RECORD_ID string,
      CDR_ID string,LOCATION_CODE int,SYSTEM_ID string,
      CLUE_ID string,HIT_ELEMENT string,CARRIER_CODE string,CAP_TIME date,
      DEVICE_ID string,DATA_CHARACTER string,
      NETCELL_ID string,NETCELL_TYPE int,EQU_CODE string,CLIENT_MAC string,
      SERVER_MAC string,TUNNEL_TYPE string,TUNNEL_IP_CLIENT string,TUNNEL_IP_SERVER string,
      TUNNEL_ID_CLIENT string,TUNNEL_ID_SERVER string,SIDE_ONE_TUNNEL_ID string,SIDE_TWO_TUNNEL_ID string,
      CLIENT_IP string,SERVER_IP string,TRANS_PROTOCOL string,CLIENT_PORT int,SERVER_PORT int,APP_PROTOCOL string,
      CLIENT_AREA bigint,SERVER_AREA bigint,LANGUAGE string,STYPE string,SUMMARY string,FILE_TYPE string,FILENAME string,
      FILESIZE string,BILL_TYPE string,ORIG_USER_NUM string,USER_NUM string,USER_IMSI string,
      USER_IMEI string,USER_BELONG_AREA_CODE string,USER_BELONG_COUNTRY_CODE string,
      USER_LONGITUDE double,USER_LATITUDE double,USER_MSC string,USER_BASE_STATION string,
      USER_CURR_AREA_CODE string,USER_CURR_COUNTRY_CODE string,USER_SIGNAL_POINT string,USER_IP string,
      ORIG_OPPO_NUM string,OPPO_NUM string,OPPO_IMSI string,OPPO_IMEI string,OPPO_BELONG_AREA_CODE string,
      OPPO_BELONG_COUNTRY_CODE string,OPPO_LONGITUDE double,OPPO_LATITUDE double,OPPO_MSC string,OPPO_BASE_STATION string,
      OPPO_CURR_AREA_CODE string,OPPO_CURR_COUNTRY_CODE string,OPPO_SIGNAL_POINT string,OPPO_IP string,RING_TIME timestamp,
      CALL_ESTAB_TIME timestamp,END_TIME timestamp,CALL_DURATION bigint,CALL_STATUS_CODE int,DTMF string,ORIG_OTHER_NUM string,
      OTHER_NUM string,ROAM_NUM string,SEND_TIME timestamp,ORIG_SMS_CONTENT string,ORIG_SMS_CODE int,SMS_CONTENT string,SMS_NUM int,
      SMS_COUNT int,REMARK string,CONTENT_STATUS int,VOC_LENGTH bigint,FAX_PAGE_COUNT int,COM_OVER_CAUSE int,ROAM_TYPE int,SGSN_ADDR string,GGSN_ADDR string,
      PDP_ADDR string,APN_NI string,APN_OI string,CARD_ID string,TIME_OUT int,LOGIN_TIME timestamp,USER_IMPU string,OPPO_IMPU string,USER_LAST_IMPI string,
      USER_CURR_IMPI string,SUPSERVICE_TYPE bigint,SUPSERVICE_TYPE_SUBCODE bigint,SMS_CENTERNUM string,USER_LAST_LONGITUDE double,USER_LAST_LATITUDE double,
      USER_LAST_MSC string,USER_LAST_BASE_STATION string,LOAD_ID bigint,P_CAP_TIME string)  ROW format delimited FIELDS terminated by '|'""".stripMargin)
    spark.sql(s"load data local inpath '$resource/j2.csv' into table h_jin")
    spark.sql(s"""create table c_jin(RECORD_ID string,
      CDR_ID string,LOCATION_CODE int,SYSTEM_ID string,
      CLUE_ID string,HIT_ELEMENT string,CARRIER_CODE string,CAP_TIME date,
      DEVICE_ID string,DATA_CHARACTER string,
      NETCELL_ID string,NETCELL_TYPE int,EQU_CODE string,CLIENT_MAC string,
      SERVER_MAC string,TUNNEL_TYPE string,TUNNEL_IP_CLIENT string,TUNNEL_IP_SERVER string,
      TUNNEL_ID_CLIENT string,TUNNEL_ID_SERVER string,SIDE_ONE_TUNNEL_ID string,SIDE_TWO_TUNNEL_ID string,
      CLIENT_IP string,SERVER_IP string,TRANS_PROTOCOL string,CLIENT_PORT int,SERVER_PORT int,APP_PROTOCOL string,
      CLIENT_AREA string,SERVER_AREA string,LANGUAGE string,STYPE string,SUMMARY string,FILE_TYPE string,FILENAME string,
      FILESIZE string,BILL_TYPE string,ORIG_USER_NUM string,USER_NUM string,USER_IMSI string,
      USER_IMEI string,USER_BELONG_AREA_CODE string,USER_BELONG_COUNTRY_CODE string,
      USER_LONGITUDE double,USER_LATITUDE double,USER_MSC string,USER_BASE_STATION string,
      USER_CURR_AREA_CODE string,USER_CURR_COUNTRY_CODE string,USER_SIGNAL_POINT string,USER_IP string,
      ORIG_OPPO_NUM string,OPPO_NUM string,OPPO_IMSI string,OPPO_IMEI string,OPPO_BELONG_AREA_CODE string,
      OPPO_BELONG_COUNTRY_CODE string,OPPO_LONGITUDE double,OPPO_LATITUDE double,OPPO_MSC string,OPPO_BASE_STATION string,
      OPPO_CURR_AREA_CODE string,OPPO_CURR_COUNTRY_CODE string,OPPO_SIGNAL_POINT string,OPPO_IP string,RING_TIME timestamp,
      CALL_ESTAB_TIME timestamp,END_TIME timestamp,CALL_DURATION string,CALL_STATUS_CODE int,DTMF string,ORIG_OTHER_NUM string,
      OTHER_NUM string,ROAM_NUM string,SEND_TIME timestamp,ORIG_SMS_CONTENT string,ORIG_SMS_CODE int,SMS_CONTENT string,SMS_NUM int,
      SMS_COUNT int,REMARK string,CONTENT_STATUS int,VOC_LENGTH string,FAX_PAGE_COUNT int,COM_OVER_CAUSE int,ROAM_TYPE int,SGSN_ADDR string,GGSN_ADDR string,
      PDP_ADDR string,APN_NI string,APN_OI string,CARD_ID string,TIME_OUT int,LOGIN_TIME timestamp,USER_IMPU string,OPPO_IMPU string,USER_LAST_IMPI string,
      USER_CURR_IMPI string,SUPSERVICE_TYPE string,SUPSERVICE_TYPE_SUBCODE string,SMS_CENTERNUM string,USER_LAST_LONGITUDE double,USER_LAST_LATITUDE double,
      USER_LAST_MSC string,USER_LAST_BASE_STATION string,LOAD_ID string,P_CAP_TIME string) using carbon""".stripMargin)
    spark.sql(s"""insert into c_jin
      select
      RECORD_ID,CDR_ID,LOCATION_CODE,SYSTEM_ID,
      CLUE_ID,HIT_ELEMENT,CARRIER_CODE,CAP_TIME,
      DEVICE_ID,DATA_CHARACTER,NETCELL_ID,NETCELL_TYPE,EQU_CODE,CLIENT_MAC,
      SERVER_MAC,TUNNEL_TYPE,TUNNEL_IP_CLIENT,TUNNEL_IP_SERVER,
      TUNNEL_ID_CLIENT,TUNNEL_ID_SERVER,SIDE_ONE_TUNNEL_ID,SIDE_TWO_TUNNEL_ID,
      CLIENT_IP,SERVER_IP,TRANS_PROTOCOL,CLIENT_PORT,SERVER_PORT,APP_PROTOCOL,
      CLIENT_AREA,SERVER_AREA,LANGUAGE,STYPE,SUMMARY,FILE_TYPE,FILENAME,
      FILESIZE,BILL_TYPE,ORIG_USER_NUM,USER_NUM,USER_IMSI,
      USER_IMEI,USER_BELONG_AREA_CODE,USER_BELONG_COUNTRY_CODE,
      USER_LONGITUDE,USER_LATITUDE,USER_MSC,USER_BASE_STATION,
      USER_CURR_AREA_CODE,USER_CURR_COUNTRY_CODE,USER_SIGNAL_POINT,USER_IP,
      ORIG_OPPO_NUM,OPPO_NUM,OPPO_IMSI,OPPO_IMEI,OPPO_BELONG_AREA_CODE,
      OPPO_BELONG_COUNTRY_CODE,OPPO_LONGITUDE,OPPO_LATITUDE,OPPO_MSC,OPPO_BASE_STATION,
      OPPO_CURR_AREA_CODE,OPPO_CURR_COUNTRY_CODE,OPPO_SIGNAL_POINT,OPPO_IP,RING_TIME,
      CALL_ESTAB_TIME,END_TIME,CALL_DURATION,CALL_STATUS_CODE,DTMF,ORIG_OTHER_NUM,
      OTHER_NUM,ROAM_NUM,SEND_TIME,ORIG_SMS_CONTENT,ORIG_SMS_CODE,SMS_CONTENT,SMS_NUM,
      SMS_COUNT,REMARK,CONTENT_STATUS,VOC_LENGTH,FAX_PAGE_COUNT,COM_OVER_CAUSE,ROAM_TYPE,SGSN_ADDR,GGSN_ADDR,
      PDP_ADDR,APN_NI,APN_OI,CARD_ID,TIME_OUT,LOGIN_TIME,USER_IMPU,OPPO_IMPU,USER_LAST_IMPI,
      USER_CURR_IMPI,SUPSERVICE_TYPE,SUPSERVICE_TYPE_SUBCODE,SMS_CENTERNUM,USER_LAST_LONGITUDE,USER_LAST_LATITUDE,
      USER_LAST_MSC,USER_LAST_BASE_STATION,LOAD_ID,P_CAP_TIME
      from h_jin""".stripMargin)
    assert(spark.sql("select * from c_jin").collect().length == 1)
    spark.sql("drop table if exists h_jin")
    spark.sql("drop table if exists c_jin")
  }

  test("test write and create table with sort columns not allow") {
    spark.sql("drop table if exists test123")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", "c" + x, "d" + x, x.toShort, x, x.toLong, x.toDouble, BigDecimal
        .apply(x)))
      .toDF("c1", "c2", "c3", "c4", "shortc", "intc", "longc", "doublec", "bigdecimalc")

    // Saves dataframe to carbon file
    df.write.format("carbon").save(s"$warehouse1/test_folder/")
    if (!spark.sparkContext.version.startsWith("2.1")) {
      intercept[UnsupportedOperationException] {
        spark
          .sql(s"create table test123 using carbon options('sort_columns'='shortc,c2') location " +
               s"'$warehouse1/test_folder/'")
      }
    }
    spark.sql("drop table if exists test123")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
  }

  test("valdate if path not specified during table creation") {
    spark.sql("drop table if exists test123")
    val ex = intercept[AnalysisException] {
      spark.sql(s"create table test123 using carbon options('sort_columns'='shortc,c2')")
    }
    assert(ex.getMessage().contains("Unable to infer schema for carbon"))
  }

  test("test double boundary") {
    spark.sql("drop table if exists par")
    spark.sql("drop table if exists car")

    spark.sql("create table par (c1 string, c2 double, n int) using parquet")
    spark.sql("create table car (c1 string, c2 double, n int) using carbon")
    spark.sql("insert into par select 'a', 1.7986931348623157E308, 215565665556")
        spark.sql("insert into car select 'a', 1.7986931348623157E308, 215565665556")

    checkAnswer(spark.sql("select * from car"), spark.sql("select * from par"))
    spark.sql("drop table if exists par")
    spark.sql("drop table if exists car")
  }

  test("test clearing datamaps") {
    if (!spark.sparkContext.version.startsWith("2.1")) {
      import spark.implicits._
      val df = spark.sparkContext.parallelize(1 to 10)
        .map(x => ("a" + x % 10, "b", x))
        .toDF("c1", "c2", "number")
      spark.sql("drop table if exists testparquet")
      spark.sql("drop table if exists carbon_table")
      spark.sql("drop table if exists carbon_table1")
      // Saves dataframe to carbon file
      df.write
        .format("parquet").saveAsTable("testparquet")
      spark.sql("create table carbon_table(c1 string, c2 string, number int) using carbon")
      spark.sql("create table carbon_table1(c1 string, c2 string, number int) using carbon")
      spark.sql("insert into carbon_table select * from testparquet")
      spark.sql("insert into carbon_table1 select * from testparquet")
      DataMapStoreManager.getInstance().getAllDataMaps.clear()
      spark.sql("select * from carbon_table where c1='a1'").collect()
      assert(DataMapStoreManager.getInstance().getAllDataMaps.size() == 1)
      spark.sql("select * from carbon_table where c1='a2'").collect()
      assert(DataMapStoreManager.getInstance().getAllDataMaps.size() == 1)
      spark.sql("select * from carbon_table1 where c1='a1'").collect()
      assert(DataMapStoreManager.getInstance().getAllDataMaps.size() == 2)
      spark.sql("select * from carbon_table1 where c1='a2'").collect()
      assert(DataMapStoreManager.getInstance().getAllDataMaps.size() == 2)
      DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(warehouse1 + "/carbon_table"))
      assert(DataMapStoreManager.getInstance().getAllDataMaps.size() == 1)
      DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(warehouse1 + "/carbon_table1"))
      assert(DataMapStoreManager.getInstance().getAllDataMaps.size() == 0)
      spark.sql("drop table if exists testparquet")
      spark.sql("drop table if exists carbon_table")
      spark.sql("drop table if exists carbon_table1")
    }
  }

  test("test write using multi subfolder") {
    if (!spark.sparkContext.version.startsWith("2.1")) {
      FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
      import spark.implicits._
      val df = spark.sparkContext.parallelize(1 to 10)
        .map(x => ("a" + x % 10, "b", x))
        .toDF("c1", "c2", "number")

      // Saves dataframe to carbon file
      df.write.format("carbon").save(warehouse1 + "/test_folder/1/" + System.nanoTime())
      df.write.format("carbon").save(warehouse1 + "/test_folder/2/" + System.nanoTime())
      df.write.format("carbon").save(warehouse1 + "/test_folder/3/" + System.nanoTime())

      val frame = spark.read.format("carbon").load(warehouse1 + "/test_folder")
      assert(frame.count() == 30)
      assert(frame.where("c1='a1'").count() == 3)
      val mapSize = DataMapStoreManager.getInstance().getAllDataMaps.size()
      DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(warehouse1 + "/test_folder"))
      assert(mapSize > DataMapStoreManager.getInstance().getAllDataMaps.size())
      FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
    }
  }

  test("test read using old data") {
    val store = new StoreCreator(new File(warehouse1).getAbsolutePath,
      new File(warehouse1 + "../../../../../hadoop/src/test/resources/data.csv").getCanonicalPath,
      false)
    store.createCarbonStore()
    FileFactory.deleteAllFilesOfDir(new File(warehouse1+"/testdb/testtable/Fact/Part0/Segment_0/0"))
    val dfread = spark.read.format("carbon").load(warehouse1+"/testdb/testtable/Fact/Part0/Segment_0")
    dfread.show(false)
    spark.sql("drop table if exists parquet_table")
  }

  test("test read using different sort order data") {
    if (!spark.sparkContext.version.startsWith("2.1")) {
      spark.sql("drop table if exists old_comp")
      FileFactory.deleteAllFilesOfDir(new File(warehouse1 + "/testdb"))
      val store = new StoreCreator(new File(warehouse1).getAbsolutePath,
        new File(warehouse1 + "../../../../../hadoop/src/test/resources/data.csv").getCanonicalPath,
        false)
      store.setSortColumns(new util.ArrayList[String](Seq("name").asJava))
      var model = store.createTableAndLoadModel(false)
      model.setSegmentId("0")
      store.createCarbonStore(model)
      FileFactory.deleteAllFilesOfDir(new File(warehouse1 + "/testdb/testtable/Fact/Part0/Segment_0/0"))
      store.setSortColumns(new util.ArrayList[String](Seq("country","phonetype").asJava))
      model = store.createTableAndLoadModel(false)
      model.setSegmentId("1")
      store.createCarbonStore(model)
      FileFactory.deleteAllFilesOfDir(new File(warehouse1 + "/testdb/testtable/Fact/Part0/Segment_1/0"))
      store.setSortColumns(new util.ArrayList[String](Seq("date").asJava))
      model = store.createTableAndLoadModel(false)
      model.setSegmentId("2")
      store.createCarbonStore(model)
      FileFactory.deleteAllFilesOfDir(new File(warehouse1 + "/testdb/testtable/Fact/Part0/Segment_2/0"))
      store.setSortColumns(new util.ArrayList[String](Seq("serialname").asJava))
      model = store.createTableAndLoadModel(false)
      model.setSegmentId("3")
      store.createCarbonStore(model)
      FileFactory.deleteAllFilesOfDir(new File(warehouse1 + "/testdb/testtable/Fact/Part0/Segment_3/0"))
      spark.sql(s"create table old_comp(id int, date string, country string, name string, phonetype string, serialname string, salary int) using carbon options(path='$warehouse1/testdb/testtable/Fact/Part0/', 'sort_columns'='name')")

      assert(spark.sql("select * from old_comp where country='china'").count() == 3396)
      assert(spark.sql("select * from old_comp ").count() == 4000)
      spark.sql("drop table if exists old_comp")

      spark.sql(s"create table old_comp1 using carbon options(path='$warehouse1/testdb/testtable/Fact/Part0/')")
      assert(spark.sql("select * from old_comp1 where country='china'").count() == 3396)
      assert(spark.sql("select * from old_comp1 ").count() == 4000)
      spark.sql("drop table if exists old_comp1")
      FileFactory.deleteAllFilesOfDir(new File(warehouse1 + "/testdb"))
    }
  }


  test("test write sdk and read with spark using different sort order data") {
    spark.sql("drop table if exists sdkout")
    FileFactory.deleteAllFilesOfDir(new File(warehouse1+"/sdk"))
    buildTestDataOtherDataType(5, Array("age", "address"), warehouse1+"/sdk")
    spark.sql(s"create table sdkout using carbon options(path='$warehouse1/sdk')")
    assert(spark.sql("select * from sdkout").collect().length == 5)
    buildTestDataOtherDataType(5, Array("name","salary"), warehouse1+"/sdk")
    spark.sql("refresh table sdkout")
    assert(spark.sql("select * from sdkout where name = 'name1'").collect().length == 2)
    assert(spark.sql("select * from sdkout where salary=100").collect().length == 2)
    buildTestDataOtherDataType(5, Array("name","age"), warehouse1+"/sdk")
    spark.sql("refresh table sdkout")
    assert(spark.sql("select * from sdkout where name='name0'").collect().length == 3)
    assert(spark.sql("select * from sdkout").collect().length == 15)
    assert(spark.sql("select * from sdkout where salary=100").collect().length == 3)
    assert(spark.sql("select * from sdkout where address='address1'").collect().length == 3)
    buildTestDataOtherDataType(5, Array("name","salary"), warehouse1+"/sdk")
    spark.sql("refresh table sdkout")
    assert(spark.sql("select * from sdkout where name='name0'").collect().length == 4)
    assert(spark.sql("select * from sdkout").collect().length == 20)
    assert(spark.sql("select * from sdkout where salary=100").collect().length == 4)
    assert(spark.sql("select * from sdkout where address='address1'").collect().length == 4)
    FileFactory.deleteAllFilesOfDir(new File(warehouse1+"/sdk"))
  }

  test("test write sdk with different schema and read with spark") {
    spark.sql("drop table if exists sdkout")
    FileFactory.deleteAllFilesOfDir(new File(warehouse1+"/sdk1"))
    buildTestDataOtherDataType(5, Array("age", "address"), warehouse1+"/sdk1")
    spark.sql(s"create table sdkout using carbon options(path='$warehouse1/sdk1')")
    assert(spark.sql("select * from sdkout").collect().length == 5)
    buildTestDataOtherDataType(5, null, warehouse1+"/sdk1", 2)
    spark.sql("refresh table sdkout")
    intercept[Exception] {
      spark.sql("select * from sdkout").show()
    }
    intercept[Exception] {
      spark.sql("select * from sdkout where salary=100").show()
    }
    FileFactory.deleteAllFilesOfDir(new File(warehouse1+"/sdk1"))
  }

  test("test Float data type by giving schema explicitly and desc formatted") {
    spark.sql("drop table if exists sdkout")
    FileFactory.deleteAllFilesOfDir(new File(warehouse1+"/sdk1"))
    buildTestDataOtherDataType(5, Array("age", "address"), warehouse1+"/sdk1")
    spark.sql(s"create table sdkout(male boolean, age int, height double, name string, address " +
              s"string," +
              s"salary long, floatField float, bytefield byte) using carbon options " +
              s"(path='$warehouse1/sdk1')")
    assert(spark.sql("desc formatted sdkout").collect().take(7).reverse.head.get(1).equals("float"))
    assert(spark.sql("desc formatted sdkout").collect().take(8).reverse.head.get(1).equals
    ("tinyint"))
  }

  test("test select * on table with float data type") {
    spark.sql("drop table if exists sdkout")
    FileFactory.deleteAllFilesOfDir(new File(warehouse1+"/sdk1"))
    buildTestDataOtherDataType(11, Array("age", "address"), warehouse1 + "/sdk1")
    spark.sql(s"create table sdkout(male boolean, age int, height double, name string, address " +
              s"string," +
              s"salary long, floatField float, bytefield byte) using carbon options (path='$warehouse1/sdk1')")
    checkAnswer(spark.sql("select * from par_table"), spark.sql("select * from sdkout"))
    checkAnswer(spark.sql("select floatfield from par_table"), spark.sql("select floatfield from sdkout"))
  }

  test("test various filters on float data") {
    spark.sql("drop table if exists sdkout")
    FileFactory.deleteAllFilesOfDir(new File(warehouse1+"/sdk1"))
    buildTestDataOtherDataType(11, Array("age", "address"), warehouse1 + "/sdk1")
    spark.sql(s"create table sdkout(male boolean, age int, height double, name string, address " +
              s"string," +
              s"salary long, floatField float, bytefield byte) using carbon options (path='$warehouse1/sdk1')")
    checkAnswer(spark.sql("select * from par_table where floatfield < 10"),
      spark.sql("select * from sdkout where floatfield < 10"))
    checkAnswer(spark.sql("select * from par_table where floatfield > 5.3"),
      spark.sql("select * from sdkout where floatfield > 5.3"))
    checkAnswer(spark.sql("select * from par_table where floatfield >= 4.1"),
      spark.sql("select * from sdkout where floatfield >= 4.1"))
    checkAnswer(spark.sql("select * from par_table where floatfield != 5.5"),
      spark.sql("select * from sdkout where floatfield != 5.5"))
    checkAnswer(spark.sql("select * from par_table where floatfield <= 5"),
      spark.sql("select * from sdkout where floatfield <= 5"))
    checkAnswer(spark.sql("select * from par_table where floatfield >= 5"),
      spark.sql("select * from sdkout where floatfield >= 5"))
    checkAnswer(spark.sql("select * from par_table where floatfield IN ('5.5','6.6')"),
      spark.sql("select * from sdkout where floatfield IN ('5.5','6.6')"))
    checkAnswer(spark.sql("select * from par_table where floatfield NOT IN ('5.5','6.6')"),
      spark.sql("select * from sdkout where floatfield NOT IN ('5.5','6.6')"))
    checkAnswer(spark.sql("select * from par_table where floatfield = cast('6.6' as float)"),
      spark.sql("select * from sdkout where floatfield = cast('6.6' as float)"))
  }

  test("test select * on table with byte data type") {
    spark.sql("drop table if exists sdkout")
    FileFactory.deleteAllFilesOfDir(new File(warehouse1+"/sdk1"))
    buildTestDataOtherDataType(11, Array("age", "address"), warehouse1 + "/sdk1")
    spark.sql(s"create table sdkout(male boolean, age int, height double, name string, address " +
              s"string," +
              s"salary long, floatField float, bytefield byte) using carbon options " +
              s"(path='$warehouse1/sdk1')")
    checkAnswer(spark.sql("select * from par_table"), spark.sql("select * from sdkout"))
    checkAnswer(spark.sql("select byteField from par_table"), spark.sql("select bytefield from sdkout"))
  }

  test("test various filters on byte data") {
    spark.sql("drop table if exists sdkout")
    FileFactory.deleteAllFilesOfDir(new File(warehouse1+"/sdk1"))
    buildTestDataOtherDataType(11, Array("age", "address"), warehouse1 + "/sdk1")
    spark.sql(s"create table sdkout(male boolean, age int, height double, name string, address " +
              s"string," +
              s"salary long, floatField float, bytefield byte) using carbon options " +
              s"(path='$warehouse1/sdk1')")
    checkAnswer(spark.sql("select * from par_table where bytefield < 10"),
      spark.sql("select * from sdkout where bytefield < 10"))
    checkAnswer(spark.sql("select * from par_table where bytefield > 5"),
      spark.sql("select * from sdkout where bytefield > 5"))
    checkAnswer(spark.sql("select * from par_table where bytefield >= 4"),
      spark.sql("select * from sdkout where bytefield >= 4"))
    checkAnswer(spark.sql("select * from par_table where bytefield != 5"),
      spark.sql("select * from sdkout where bytefield != 5"))
    checkAnswer(spark.sql("select * from par_table where bytefield <= 5"),
      spark.sql("select * from sdkout where bytefield <= 5"))
    checkAnswer(spark.sql("select * from par_table where bytefield >= 5"),
      spark.sql("select * from sdkout where bytefield >= 5"))
    checkAnswer(spark.sql("select * from par_table where bytefield IN ('5','6')"),
      spark.sql("select * from sdkout where bytefield IN ('5','6')"))
    checkAnswer(spark.sql("select * from par_table where bytefield NOT IN ('5','6')"),
      spark.sql("select * from sdkout where bytefield NOT IN ('5','6')"))
  }

  test("test struct of float type and byte type") {
    import scala.collection.JavaConverters._
    val path = new File(warehouse1+"/sdk1").getAbsolutePath
    FileFactory.deleteAllFilesOfDir(new File(warehouse1+"/sdk1"))
    spark.sql("drop table if exists complextable")
    val fields = List(new StructField
    ("byteField", DataTypes.BYTE), new StructField("floatField", DataTypes.FLOAT))
    val structType = Array(new Field("stringfield", DataTypes.STRING), new Field
    ("structField", "struct", fields.asJava))


    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(path)
          .uniqueIdentifier(System.nanoTime()).withBlockSize(2)
          .withCsvInput(new Schema(structType)).build()

      var i = 0
      while (i < 11) {
        val array = Array[String](s"name$i", s"$i" + "$" +s"$i.${i}12")
        writer.write(array)
        i += 1
      }
      writer.close()
      spark.sql("create table complextable (stringfield string, structfield struct<bytefield: " +
                "byte, floatfield: float>) " +
                s"using carbon location '$path'")
    } catch {
      case ex: Exception => throw new RuntimeException(ex)
      case _ => None
    }
    checkAnswer(spark.sql("select * from complextable limit 1"), Seq(Row("name0", Row(0
      .asInstanceOf[Byte], 0.012.asInstanceOf[Float]))))
    checkAnswer(spark.sql("select * from complextable where structfield.bytefield > 9"), Seq(Row
    ("name10", Row(10.asInstanceOf[Byte], 10.1012.asInstanceOf[Float]))))
    checkAnswer(spark.sql("select * from complextable where structfield.bytefield > 9"), Seq(Row
    ("name10", Row(10.asInstanceOf[Byte], 10.1012.asInstanceOf[Float]))))
    checkAnswer(spark.sql("select * from complextable where structfield.floatfield > 9.912"), Seq
    (Row
    ("name10", Row(10.asInstanceOf[Byte], 10.1012.asInstanceOf[Float]))))
    checkAnswer(spark.sql("select * from complextable where structfield.floatfield > 9.912 and " +
                          "structfield.bytefield < 11"), Seq(Row("name10", Row(10.asInstanceOf[Byte], 10.1012.asInstanceOf[Float]))))
  }

  test("test bytefield as sort column") {
    import scala.collection.JavaConverters._
    val path = new File(warehouse1+"/sdk1").getAbsolutePath
    FileFactory.deleteAllFilesOfDir(new File(warehouse1+"/sdk1"))
    var fields: Array[Field] = new Array[Field](8)
    // same column name, but name as boolean type
    fields(0) = new Field("age", DataTypes.INT)
    fields(1) = new Field("height", DataTypes.DOUBLE)
    fields(2) = new Field("name", DataTypes.STRING)
    fields(3) = new Field("address", DataTypes.STRING)
    fields(4) = new Field("salary", DataTypes.LONG)
    fields(5) = new Field("bytefield", DataTypes.BYTE)

    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(path)
          .uniqueIdentifier(System.nanoTime()).withBlockSize(2).sortBy(Array("bytefield"))
          .withCsvInput(new Schema(fields)).build()

      var i = 0
      while (i < 11) {
        val array = Array[String](
          String.valueOf(i),
          String.valueOf(i.toDouble / 2),
          "name" + i,
          "address" + i,
          (i * 100).toString,
          s"${10 - i}")
        writer.write(array)
        i += 1
      }
      writer.close()
      spark.sql("drop table if exists sorted_par")
      spark.sql("drop table if exists sort_table")
      spark.sql(s"create table sort_table (age int, height double, name string, address string," +
                s" salary long, bytefield byte) using carbon location '$path'")
      FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(s"$warehouse1/../warehouse2"))
      spark.sql(s"create table sorted_par(age int, height double, name string, address " +
                s"string," +
                s"salary long, bytefield byte) using parquet location " +
                s"'$warehouse1/../warehouse2'")
      (0 to 10).foreach {
        i =>
          spark.sql(s"insert into sorted_par select '$i', ${ i.toDouble / 2 }, 'name$i', " +
                    s"'address$i', ${ i * 100 }, '${ 10 - i }'")
      }
      checkAnswer(spark.sql("select * from sorted_par order by bytefield"),
        spark.sql("select * from sort_table"))
    } catch {
      case ex: Exception => throw new RuntimeException(ex)
      case _ => None
    }
  }

  test("test array of float type and byte type") {
    import scala.collection.JavaConverters._
    val path = new File(warehouse1+"/sdk1").getAbsolutePath
    FileFactory.deleteAllFilesOfDir(new File(warehouse1+"/sdk1"))
    spark.sql("drop table if exists complextable")
    val structType =
      Array(new Field("stringfield", DataTypes.STRING),
        new Field("bytearray", "array", List(new StructField("byteField", DataTypes.BYTE))
          .asJava),
        new Field("floatarray", "array", List(new StructField("floatfield", DataTypes.FLOAT))
          .asJava))

    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(path)
          .uniqueIdentifier(System.nanoTime()).withBlockSize(2)
          .withCsvInput(new Schema(structType)).build()

      var i = 0
      while (i < 10) {
        val array = Array[String](s"name$i",s"$i" + "$" + s"${i*2}", s"${i/2}" + "$" + s"${i/3}")
        writer.write(array)
        i += 1
      }
      writer.close()
      spark.sql(s"create table complextable (stringfield string, bytearray " +
                s"array<byte>, floatarray array<float>) using carbon " +
                s"location " +
                s"'$path'")
    } catch {
      case ex: Exception => throw new RuntimeException(ex)
      case _ => None
    }
    checkAnswer(spark.sql("select * from complextable limit 1"), Seq(Row("name0", mutable
      .WrappedArray.make(Array[Byte](0, 0)), mutable.WrappedArray.make(Array[Float](0.0f, 0.0f)))))
    checkAnswer(spark.sql("select * from complextable where bytearray[0] = 1"), Seq(Row("name1",
      mutable.WrappedArray.make(Array[Byte](1, 2)), mutable.WrappedArray.make(Array[Float](0.0f,
        0.0f)))))
    checkAnswer(spark.sql("select * from complextable where bytearray[0] > 8"), Seq(Row("name9",
      mutable.WrappedArray.make(Array[Byte](9, 18)), mutable.WrappedArray.make(Array[Float](4.0f,
        3.0f)))))
    checkAnswer(spark.sql("select * from complextable where floatarray[0] IN (4.0) and stringfield = 'name8'"), Seq(Row
    ("name8",
      mutable.WrappedArray.make(Array[Byte](8, 16)), mutable.WrappedArray.make(Array[Float](4.0f,
      2.0f)))))
  }

  private def createParquetTable {
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(s"$warehouse1/../warehouse2"))
    spark.sql(s"create table par_table(male boolean, age int, height double, name string, address " +
              s"string," +
              s"salary long, floatField float, bytefield byte) using parquet location " +
              s"'$warehouse1/../warehouse2'")
    (0 to 10).foreach {
      i => spark.sql(s"insert into par_table select 'true','$i', ${i.toDouble / 2}, 'name$i', " +
                     s"'address$i', ${i*100}, $i.$i, '$i'")
    }
  }

  // prepare sdk writer output with other schema
  def buildTestDataOtherDataType(rows: Int, sortColumns: Array[String], writerPath: String, colCount: Int = -1): Any = {
    var fields: Array[Field] = new Array[Field](8)
    // same column name, but name as boolean type
    fields(0) = new Field("male", DataTypes.BOOLEAN)
    fields(1) = new Field("age", DataTypes.INT)
    fields(2) = new Field("height", DataTypes.DOUBLE)
    fields(3) = new Field("name", DataTypes.STRING)
    fields(4) = new Field("address", DataTypes.STRING)
    fields(5) = new Field("salary", DataTypes.LONG)
    fields(6) = new Field("floatField", DataTypes.FLOAT)
    fields(7) = new Field("bytefield", DataTypes.BYTE)

    if (colCount > 0) {
      val fieldsToWrite: Array[Field] = new Array[Field](colCount)
      var i = 0
      while (i < colCount) {
        fieldsToWrite(i) = fields(i)
        i += 1
      }
      fields = fieldsToWrite
    }

    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(writerPath)
          .uniqueIdentifier(System.nanoTime()).withBlockSize(2).sortBy(sortColumns)
          .withCsvInput(new Schema(fields)).build()

      var i = 0
      while (i < rows) {
        val array = Array[String]("true",
          String.valueOf(i),
          String.valueOf(i.toDouble / 2),
          "name" + i,
          "address" + i,
          (i * 100).toString,
          s"$i.$i", s"$i")
        if (colCount > 0) {
          writer.write(array.slice(0, colCount))
        } else {
          writer.write(array)
        }
        i += 1
      }
      writer.close()
    } catch {
      case ex: Exception => throw new RuntimeException(ex)
      case _ => None
    }
  }

  test("test byte and float for multiple pages") {
    val path = new File(warehouse1+"/sdk1").getAbsolutePath
    FileFactory.deleteAllFilesOfDir(new File(warehouse1+"/sdk1"))
    spark.sql("drop table if exists multi_page")
    var fields: Array[Field] = new Array[Field](8)
    // same column name, but name as boolean type
    fields(0) = new Field("a", DataTypes.STRING)
    fields(1) = new Field("b", DataTypes.FLOAT)
    fields(2) = new Field("c", DataTypes.BYTE)

    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(path)
          .uniqueIdentifier(System.nanoTime()).withBlockSize(2)
          .withCsvInput(new Schema(fields)).build()

      var i = 0
      while (i < 33000) {
        val array = Array[String](
          String.valueOf(i),
          s"$i.3200", "32")
        writer.write(array)
        i += 1
      }
      writer.close()
      spark.sql(s"create table multi_page (a string, b float, c byte) using carbon location " +
                s"'$path'")
      assert(spark.sql("select * from multi_page").count() == 33000)
    } catch {
      case ex: Exception => throw new RuntimeException(ex)
    } finally {
      FileFactory.deleteAllFilesOfDir(new File(warehouse1+"/sdk1"))
    }
  }

  override protected def beforeAll(): Unit = {
    drop
    createParquetTable
  }

  override def afterAll(): Unit = {
    drop
  }

  private def drop = {
    spark.sql("drop table if exists testformat")
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists testparquet")
    spark.sql("drop table if exists par_table")
    spark.sql("drop table if exists sdkout")
  }
}
