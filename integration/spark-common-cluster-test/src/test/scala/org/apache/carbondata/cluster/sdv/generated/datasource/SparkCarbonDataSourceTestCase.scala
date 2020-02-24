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
package org.apache.carbondata.cluster.sdv.generated.datasource

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, File, InputStream}

import scala.collection.mutable

import org.apache.avro
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, Encoder}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.common.util.DataSourceTestUtil._
import org.apache.spark.sql.test.TestQueryExecutor
import org.junit.Assert
import org.scalatest.{BeforeAndAfterAll,FunSuite}

import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.datatype.{DataTypes, StructField}
import org.apache.carbondata.hadoop.testutil.StoreCreator
import org.apache.carbondata.sdk.file.{CarbonWriter, Field, Schema}

class SparkCarbonDataSourceTestCase extends FunSuite with BeforeAndAfterAll {
  import spark._

  val warehouse1 = s"${TestQueryExecutor.projectPath}/integration/spark/target/warehouse"

  test("test write using dataframe") {
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")
    sql("drop table if exists testformat")
    // Saves dataframe to carbon file
    df.write
      .format("carbon").saveAsTable("testformat")
    assert(sql("select * from testformat").count() == 10)
    assert(sql("select * from testformat where c1='a0'").count() == 1)
    checkAnswer(sql("select c1 from testformat where number = 7"), Seq(Row("a7")))
    sql("drop table if exists testformat")
  }

  test("test write using ddl") {
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")
    sql("drop table if exists testparquet")
    sql("drop table if exists testformat")
    // Saves dataframe to carbon file
    df.write
      .format("parquet").saveAsTable("testparquet")
    sql("create table carbon_table(c1 string, c2 string, number int) using carbon")
    sql("insert into carbon_table select * from testparquet")
    checkAnswer(sql("select * from carbon_table where c1='a1'"),
      sql("select * from testparquet where c1='a1'"))
    if (!sqlContext.sparkContext.version.startsWith("2.1")) {
      val mapSize = DataMapStoreManager.getInstance().getAllDataMaps.size()
      DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(warehouse1 + "/carbon_table"))
      assert(mapSize >= DataMapStoreManager.getInstance().getAllDataMaps.size())
    }
    sql("drop table if exists testparquet")
    sql("drop table if exists testformat")
  }

  test("test read with df write") {
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")

    // Saves dataframe to carbon file
    df.write.format("carbon").save(warehouse1 + "/test_folder/")

    val frame = sqlContext.read.format("carbon").load(warehouse1 + "/test_folder")
    assert(frame.count() == 10)
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
  }

  test("test write using subfolder") {
    if (!sqlContext.sparkContext.version.startsWith("2.1")) {
      FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
      import sqlContext.implicits._
      val df = sqlContext.sparkContext.parallelize(1 to 10)
        .map(x => ("a" + x % 10, "b", x))
        .toDF("c1", "c2", "number")

      // Saves dataframe to carbon file
      df.write.format("carbon").save(warehouse1 + "/test_folder/" + System.nanoTime())
      df.write.format("carbon").save(warehouse1 + "/test_folder/" + System.nanoTime())
      df.write.format("carbon").save(warehouse1 + "/test_folder/" + System.nanoTime())

      val frame = sqlContext.read.format("carbon").load(warehouse1 + "/test_folder")
      assert(frame.where("c1='a1'").count() == 3)

      val mapSize = DataMapStoreManager.getInstance().getAllDataMaps.size()
      DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(warehouse1 + "/test_folder"))
      assert(mapSize > DataMapStoreManager.getInstance().getAllDataMaps.size())
      FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
    }
  }

  test("test write using partition ddl") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists testparquet")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")

    // Saves dataframe to carbon file
    df.write
      .format("parquet").partitionBy("c2").saveAsTable("testparquet")
    sql(
      "create table carbon_table(c1 string, c2 string, number int) using carbon  PARTITIONED by " +
      "(c2)")
    sql("insert into carbon_table select * from testparquet")
    // TODO fix in 2.1
    if (!sqlContext.sparkContext.version.startsWith("2.1")) {
      assert(sql("select * from carbon_table").count() == 10)
      checkAnswer(sql("select * from carbon_table"),
        sql("select * from testparquet"))
    }
    sql("drop table if exists carbon_table")
    sql("drop table if exists testparquet")
  }

  test("test write with struct type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, ("b", "c"), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql(
      "create table carbon_table(c1 string, c2 struct<a1:string, a2:string>, number int) using " +
      "carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"), sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with array type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Array("b", "c"), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql("create table carbon_table(c1 string, c2 array<string>, number int) using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"), sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with nested array and struct type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Array(("1", "2"), ("3", "4")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql(
      "create table carbon_table(c1 string, c2 array<struct<a1:string, a2:string>>, number int) " +
      "using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"), sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with nested struct and array type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, (Array("1", "2"), ("3", "4")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql(
      "create table carbon_table(c1 string, c2 struct<a1:array<string>, a2:struct<a1:string, " +
      "a2:string>>, number int) using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"), sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with array type with value as nested map type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Array(Map("b" -> "c")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql(
      "create table carbon_table(c1 string, c2 array<map<string,string>>, number int) using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"), sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with array type with value as nested array<array<map>> type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Array(Array(Map("b" -> "c"))), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql(
      "create table carbon_table(c1 string, c2 array<array<map<string,string>>>, number int) " +
      "using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"), sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with struct type with value as nested map type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, ("a", Map("b" -> "c")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql(
      "create table carbon_table(c1 string, c2 struct<a1:string, a2:map<string,string>>, number " +
      "int) using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"), sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with struct type with value as nested struct<array<map>> type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, ("a", Array(Map("b" -> "c"))), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql(
      "create table carbon_table(c1 string, c2 struct<a1:string, a2:array<map<string,string>>>, " +
      "number int) using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"), sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with map type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Map("b" -> "c"), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql("create table carbon_table(c1 string, c2 map<string, string>, number int) using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"), sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with map type with Int data type as key") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Map(99 -> "c"), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql("create table carbon_table(c1 string, c2 map<int, string>, number int) using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"), sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with map type with value as nested map type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Map("a" -> Map("b" -> "c")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql(
      "create table carbon_table(c1 string, c2 map<string, map<string, string>>, number int) " +
      "using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"), sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with map type with value as nested struct type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Map("a" -> ("b", "c")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql(
      "create table carbon_table(c1 string, c2 map<string, struct<a1:string, a2:string>>, number " +
      "int) using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"), sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with map type with value as nested array type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Map("a" -> Array("b", "c")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql(
      "create table carbon_table(c1 string, c2 map<string, array<string>>, number int) using " +
      "carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"), sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write using ddl and options") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists testparquet")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")

    // Saves dataframe to carbon file
    df.write
      .format("parquet").saveAsTable("testparquet")
    sql(
      "create table carbon_table(c1 string, c2 string, number int) using carbon options" +
      "('table_blocksize'='256','inverted_index'='c1')")
    checkExistence(sql("describe formatted carbon_table"), true, "table_blocksize")
    checkExistence(sql("describe formatted carbon_table"), true, "inverted_index")
    sql("insert into carbon_table select * from testparquet")
    checkAnswer(sql("select * from carbon_table"), sql("select * from testparquet"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists testparquet")
  }

  test("test read with nested struct and array type without creating table") {
    FileFactory
      .deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_carbon_folder"))
    sql("drop table if exists parquet_table")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, (Array("1", "2"), ("3", "4")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    val frame = sql("select * from parquet_table")
    frame.write.format("carbon").save(warehouse1 + "/test_carbon_folder")
    val dfread = sqlContext.read.format("carbon").load(warehouse1 + "/test_carbon_folder")
    FileFactory
      .deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_carbon_folder"))
    sql("drop table if exists parquet_table")
  }

  test("test read and write with date datatype") {
    sql("drop table if exists date_table")
    sql("drop table if exists date_parquet_table")
    sql("create table date_table(empno int, empname string, projdate Date) using carbon")
    sql("insert into  date_table select 11, 'ravi', '2017-11-11'")
    sql("create table date_parquet_table(empno int, empname string, projdate Date) using parquet")
    sql("insert into  date_parquet_table select 11, 'ravi', '2017-11-11'")
    checkAnswer(sql("select * from date_table"), sql("select * from date_parquet_table"))
    sql("drop table if exists date_table")
    sql("drop table if exists date_parquet_table")
  }

  test("test read and write with date datatype with wrong format") {
    sql("drop table if exists date_table")
    sql("drop table if exists date_parquet_table")
    sql("create table date_table(empno int, empname string, projdate Date) using carbon")
    sql("insert into  date_table select 11, 'ravi', '11-11-2017'")
    sql("create table date_parquet_table(empno int, empname string, projdate Date) using parquet")
    sql("insert into  date_parquet_table select 11, 'ravi', '11-11-2017'")
    checkAnswer(sql("select * from date_table"), sql("select * from date_parquet_table"))
    sql("drop table if exists date_table")
    sql("drop table if exists date_parquet_table")
  }

  test("test read and write with timestamp datatype") {
    sql("drop table if exists date_table")
    sql("drop table if exists date_parquet_table")
    sql("create table date_table(empno int, empname string, projdate timestamp) using carbon")
    sql("insert into  date_table select 11, 'ravi', '2017-11-11 00:00:01'")
    sql(
      "create table date_parquet_table(empno int, empname string, projdate timestamp) using " +
      "parquet")
    sql("insert into  date_parquet_table select 11, 'ravi', '2017-11-11 00:00:01'")
    checkAnswer(sql("select * from date_table"), sql("select * from date_parquet_table"))
    sql("drop table if exists date_table")
    sql("drop table if exists date_parquet_table")
  }

  test("test read and write with timestamp datatype with wrong format") {
    sql("drop table if exists date_table")
    sql("drop table if exists date_parquet_table")
    sql("create table date_table(empno int, empname string, projdate timestamp) using carbon")
    sql("insert into  date_table select 11, 'ravi', '11-11-2017 00:00:01'")
    sql(
      "create table date_parquet_table(empno int, empname string, projdate timestamp) using " +
      "parquet")
    sql("insert into  date_parquet_table select 11, 'ravi', '11-11-2017 00:00:01'")
    checkAnswer(sql("select * from date_table"), sql("select * from date_parquet_table"))
    sql("drop table if exists date_table")
    sql("drop table if exists date_parquet_table")
  }

  test("test write with array type with filter") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Array("b", "c"), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql("create table carbon_table(c1 string, c2 array<string>, number int) using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table where c1='a1' and c2[0]='b'"),
      sql("select * from parquet_table where c1='a1' and c2[0]='b'"))
    checkAnswer(sql("select * from carbon_table"), sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with struct type with filter") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, (Array("1", "2"), ("3", "4")), Array(("1", 1), ("2", 2)), x))
      .toDF("c1", "c2", "c3", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql(
      "create table carbon_table(c1 string, c2 struct<a1:array<string>, a2:struct<a1:string, " +
      "a2:string>>, c3 array<struct<a1:string, a2:int>>, number int) using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"), sql("select * from parquet_table"))
    checkAnswer(sql("select * from carbon_table where c2.a1[0]='1' and c1='a1'"),
      sql("select * from parquet_table where c2._1[0]='1' and c1='a1'"))
    checkAnswer(sql("select * from carbon_table where c2.a1[0]='1' and c3[0].a2=1"),
      sql("select * from parquet_table where c2._1[0]='1' and c3[0]._2=1"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test read with df write string issue") {
    sql("drop table if exists test123")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x.toShort, x, x.toLong, x.toDouble, BigDecimal.apply(x),
        Array(x + 1,
          x), ("b", BigDecimal.apply(x))))
      .toDF("c1", "c2", "shortc", "intc", "longc", "doublec", "bigdecimalc", "arrayc", "structc")

    // Saves dataframe to carbon file
    df.write.format("carbon").save(warehouse1 + "/test_folder/")
    if (!sqlContext.sparkContext.version.startsWith("2.1")) {
      sql(s"create table test123 (c1 string, c2 string, shortc smallint,intc int, longc bigint,  " +
          s"doublec double, bigdecimalc decimal(38,18), arrayc array<int>, structc " +
          s"struct<_1:string, _2:decimal(38,18)>) using carbon location '$warehouse1/test_folder/'")

      checkAnswer(sql("select * from test123"),
        sqlContext.read.format("carbon").load(warehouse1 + "/test_folder/"))
    }
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
    sql("drop table if exists test123")
  }

  test("test read with df write with empty data") {
    sql("drop table if exists test123")
    sql("drop table if exists test123_par")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
    // Saves dataframe to carbon file
    if (!sqlContext.sparkContext.version.startsWith("2.1")) {
      sql(s"create table test123 (c1 string, c2 string, arrayc array<int>, structc " +
          s"struct<_1:string, _2:decimal(38,18)>, shortc smallint,intc int, longc bigint,  " +
          s"doublec double, bigdecimalc decimal(38,18)) using carbon location " +
          s"'$warehouse1/test_folder/'")

      sql(s"create table test123_par (c1 string, c2 string, arrayc array<int>, structc " +
          s"struct<_1:string, _2:decimal(38,18)>, shortc smallint,intc int, longc bigint,  " +
          s"doublec double, bigdecimalc decimal(38,18)) using carbon location " +
          s"'$warehouse1/test_folder/'")
      checkAnswer(sql("select count(*) from test123"),
        sql("select count(*) from test123_par"))
    }
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
    sql("drop table if exists test123")
    sql("drop table if exists test123_par")
  }

  test("test write with nosort columns") {
    sql("drop table if exists test123")
    sql("drop table if exists test123_par")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x.toShort, x, x.toLong, x.toDouble, BigDecimal.apply(x),
        Array(x + 1,
          x), ("b", BigDecimal.apply(x))))
      .toDF("c1", "c2", "shortc", "intc", "longc", "doublec", "bigdecimalc", "arrayc", "structc")

    // Saves dataframe to carbon file
    df.write.format("parquet").saveAsTable("test123_par")
    if (!sqlContext.sparkContext.version.startsWith("2.1")) {
      sql(s"create table test123 (c1 string, c2 string, shortc smallint,intc int, longc bigint,  " +
          s"doublec double, bigdecimalc decimal(38,18), arrayc array<int>, structc " +
          s"struct<_1:string, _2:decimal(38,18)>) using carbon options('sort_columns'='') " +
          s"location '$warehouse1/test_folder/'")

      sql(s"insert into test123 select * from test123_par")
      checkAnswer(sql("select * from test123"), sql(s"select * from test123_par"))
    }
    sql("drop table if exists test123")
    sql("drop table if exists test123_par")
  }

  test("test complex columns mismatch") {
    sql("drop table if exists array_com_hive")
    sql(s"drop table if exists array_com")
    sql(
      "create table array_com_hive (CUST_ID string, YEAR int, MONTH int, AGE int, GENDER string, " +
      "EDUCATED string, IS_MARRIED string, ARRAY_INT array<int>,ARRAY_STRING array<string>," +
      "ARRAY_DATE array<timestamp>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT " +
      "double, HQ_DEPOSIT double) row format delimited fields terminated by ',' collection items " +
      "terminated by '$'")
    val sourceFile = FileFactory
      .getPath(s"$resource" + "../../../../../spark/src/test/resources/Array.csv")
      .toString
    sql(s"load data local inpath '$sourceFile' into table array_com_hive")
    sql(
      "create table Array_com (CUST_ID string, YEAR int, MONTH int, AGE int, GENDER string, " +
      "EDUCATED string, IS_MARRIED string, ARRAY_INT array<int>,ARRAY_STRING array<string>," +
      "ARRAY_DATE array<timestamp>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT " +
      "double, HQ_DEPOSIT double) using carbon")
    sql("insert into Array_com select * from array_com_hive")
    checkAnswer(sql("select * from Array_com order by CUST_ID ASC limit 3"),
      sql("select * from array_com_hive order by CUST_ID ASC limit 3"))
    sql("drop table if exists array_com_hive")
    sql(s"drop table if exists array_com")
  }

  test("test complex columns fail while insert ") {
    sql("drop table if exists STRUCT_OF_ARRAY_com_hive")
    sql(s"drop table if exists STRUCT_OF_ARRAY_com")
    sql(
      " create table STRUCT_OF_ARRAY_com_hive (CUST_ID string, YEAR int, MONTH int, AGE int, " +
      "GENDER string, EDUCATED string, IS_MARRIED string, STRUCT_OF_ARRAY struct<ID: int," +
      "CHECK_DATE: timestamp ,SNo: array<int>,sal1: array<double>,state: array<string>," +
      "date1: array<timestamp>>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT float, " +
      "HQ_DEPOSIT double) row format delimited fields terminated by ',' collection items " +
      "terminated by '$' map keys terminated by '&'")
    val sourceFile = FileFactory
      .getPath(
        s"$resource" + "../../../../../spark/src/test/resources/structofarray.csv")
      .toString
    sql(s"load data local inpath '$sourceFile' into table STRUCT_OF_ARRAY_com_hive")
    sql(
      "create table STRUCT_OF_ARRAY_com (CUST_ID string, YEAR int, MONTH int, AGE int, GENDER " +
      "string, EDUCATED string, IS_MARRIED string, STRUCT_OF_ARRAY struct<ID: int," +
      "CHECK_DATE: timestamp,SNo: array<int>,sal1: array<double>,state: array<string>," +
      "date1: array<timestamp>>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT double," +
      " HQ_DEPOSIT double) using carbon")
    sql(" insert into STRUCT_OF_ARRAY_com select * from STRUCT_OF_ARRAY_com_hive")
    checkAnswer(sql("select * from STRUCT_OF_ARRAY_com  order by CUST_ID ASC"),
      sql("select * from STRUCT_OF_ARRAY_com_hive  order by CUST_ID ASC"))
    sql("drop table if exists STRUCT_OF_ARRAY_com_hive")
    sql(s"drop table if exists STRUCT_OF_ARRAY_com")
  }

  test("test partition error in carbon") {
    sql("drop table if exists carbon_par")
    sql("drop table if exists parquet_par")
    sql(
      "create table carbon_par (name string, age int, country string) using carbon partitioned by" +
      " (country)")
    sql("insert into carbon_par select 'b', '12', 'aa'")
    sql(
      "create table parquet_par (name string, age int, country string) using carbon partitioned " +
      "by (country)")
    sql("insert into parquet_par select 'b', '12', 'aa'")
    checkAnswer(sql("select * from carbon_par"), sql("select * from parquet_par"))
    sql("drop table if exists carbon_par")
    sql("drop table if exists parquet_par")
  }

  test("test write and create table with sort columns not allow") {
    sql("drop table if exists test123")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", "c" + x, "d" + x, x.toShort, x, x.toLong, x.toDouble, BigDecimal
        .apply(x)))
      .toDF("c1", "c2", "c3", "c4", "shortc", "intc", "longc", "doublec", "bigdecimalc")

    // Saves dataframe to carbon file
    df.write.format("carbon").save(s"$warehouse1/test_folder/")
    if (!sqlContext.sparkContext.version.startsWith("2.1")) {
      intercept[UnsupportedOperationException] {
        sql(s"create table test123 using carbon options('sort_columns'='shortc,c2') location " +
            s"'$warehouse1/test_folder/'")
      }
    }
    sql("drop table if exists test123")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
  }

  test("valdate if path not specified during table creation") {
    sql("drop table if exists test123")
    val ex = intercept[AnalysisException] {
      sql(s"create table test123 using carbon options('sort_columns'='shortc,c2')")
    }
    assert(ex.getMessage().contains("Unable to infer schema for carbon"))
  }

  test("test double boundary") {
    sql("drop table if exists par")
    sql("drop table if exists car")

    sql("create table par (c1 string, c2 double, n int) using parquet")
    sql("create table car (c1 string, c2 double, n int) using carbon")
    sql("insert into par select 'a', 1.7986931348623157E308, 215565665556")
    sql("insert into car select 'a', 1.7986931348623157E308, 215565665556")

    checkAnswer(sql("select * from car"), sql("select * from par"))
    sql("drop table if exists par")
    sql("drop table if exists car")
  }

  test("test write using multi subfolder") {
    if (!sqlContext.sparkContext.version.startsWith("2.1")) {
      FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
      import sqlContext.implicits._
      val df = sqlContext.sparkContext.parallelize(1 to 10)
        .map(x => ("a" + x % 10, "b", x))
        .toDF("c1", "c2", "number")

      // Saves dataframe to carbon file
      df.write.format("carbon").save(warehouse1 + "/test_folder/1/" + System.nanoTime())
      df.write.format("carbon").save(warehouse1 + "/test_folder/2/" + System.nanoTime())
      df.write.format("carbon").save(warehouse1 + "/test_folder/3/" + System.nanoTime())

      val frame = sqlContext.read.format("carbon").load(warehouse1 + "/test_folder")
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
      new File(warehouse1 + "../../../../../hadoop/src/test/resources/data.csv").getCanonicalPath)
    store.createCarbonStore()
    FileFactory
      .deleteAllFilesOfDir(new File(warehouse1 + "/testdb/testtable/Fact/Part0/Segment_0/0"))
    val dfread = sqlContext.read.format("carbon")
      .load(warehouse1 + "/testdb/testtable/Fact/Part0/Segment_0")
    sql("drop table if exists parquet_table")
  }

  test("test write sdk and read with spark using different sort order data") {
    sql("drop table if exists sdkout")
    FileFactory.deleteAllFilesOfDir(new File(warehouse1 + "/sdk"))
    buildTestDataOtherDataType(5, Array("age", "address"), warehouse1 + "/sdk")
    sql(s"create table sdkout using carbon options(path='$warehouse1/sdk')")
    assert(sql("select * from sdkout").collect().length == 5)
    buildTestDataOtherDataType(5, Array("name", "salary"), warehouse1 + "/sdk")
    sql("refresh table sdkout")
    assert(sql("select * from sdkout where name = 'name1'").collect().length == 2)
    assert(sql("select * from sdkout where salary=100").collect().length == 2)
    buildTestDataOtherDataType(5, Array("name", "age"), warehouse1 + "/sdk")
    sql("refresh table sdkout")
    assert(sql("select * from sdkout where name='name0'").collect().length == 3)
    assert(sql("select * from sdkout").collect().length == 15)
    assert(sql("select * from sdkout where salary=100").collect().length == 3)
    assert(sql("select * from sdkout where address='address1'").collect().length == 3)
    buildTestDataOtherDataType(5, Array("name", "salary"), warehouse1 + "/sdk")
    sql("refresh table sdkout")
    assert(sql("select * from sdkout where name='name0'").collect().length == 4)
    assert(sql("select * from sdkout").collect().length == 20)
    assert(sql("select * from sdkout where salary=100").collect().length == 4)
    assert(sql("select * from sdkout where address='address1'").collect().length == 4)
    FileFactory.deleteAllFilesOfDir(new File(warehouse1 + "/sdk"))
  }

  test("test Float data type by giving schema explicitly and desc formatted") {
    sql("drop table if exists sdkout")
    FileFactory.deleteAllFilesOfDir(new File(warehouse1 + "/sdk1"))
    buildTestDataOtherDataType(5, Array("age", "address"), warehouse1 + "/sdk1")
    sql(s"create table sdkout(male boolean, age int, height double, name string, address " +
        s"string," +
        s"salary long, floatField float, bytefield byte) using carbon options " +
        s"(path='$warehouse1/sdk1')")
    assert(sql("desc formatted sdkout").collect().take(7).reverse.head.get(1).equals("float"))
    assert(sql("desc formatted sdkout").collect().take(8).reverse.head.get(1).equals
    ("tinyint"))
  }

  test("test select * on table with float data type") {
    sql("drop table if exists sdkout")
    FileFactory.deleteAllFilesOfDir(new File(warehouse1 + "/sdk1"))
    buildTestDataOtherDataType(11, Array("age", "address"), warehouse1 + "/sdk1")
    sql(s"create table sdkout(male boolean, age int, height double, name string, address " +
        s"string," +
        s"salary long, floatField float, bytefield byte) using carbon options " +
        s"(path='$warehouse1/sdk1')")
    checkAnswer(sql("select * from par_table"), sql("select * from sdkout"))
    checkAnswer(sql("select floatfield from par_table"), sql("select floatfield from sdkout"))
  }

  test("test various filters on float data") {
    sql("drop table if exists sdkout")
    FileFactory.deleteAllFilesOfDir(new File(warehouse1 + "/sdk1"))
    buildTestDataOtherDataType(11, Array("age", "address"), warehouse1 + "/sdk1")
    sql(s"create table sdkout(male boolean, age int, height double, name string, address " +
        s"string," +
        s"salary long, floatField float, bytefield byte) using carbon options " +
        s"(path='$warehouse1/sdk1')")
    checkAnswer(sql("select * from par_table where floatfield < 10"),
      sql("select * from sdkout where floatfield < 10"))
    checkAnswer(sql("select * from par_table where floatfield > 5.3"),
      sql("select * from sdkout where floatfield > 5.3"))
    checkAnswer(sql("select * from par_table where floatfield >= 4.1"),
      sql("select * from sdkout where floatfield >= 4.1"))
    checkAnswer(sql("select * from par_table where floatfield != 5.5"),
      sql("select * from sdkout where floatfield != 5.5"))
    checkAnswer(sql("select * from par_table where floatfield <= 5"),
      sql("select * from sdkout where floatfield <= 5"))
    checkAnswer(sql("select * from par_table where floatfield >= 5"),
      sql("select * from sdkout where floatfield >= 5"))
    checkAnswer(sql("select * from par_table where floatfield IN ('5.5','6.6')"),
      sql("select * from sdkout where floatfield IN ('5.5','6.6')"))
    checkAnswer(sql("select * from par_table where floatfield NOT IN ('5.5','6.6')"),
      sql("select * from sdkout where floatfield NOT IN ('5.5','6.6')"))
    checkAnswer(sql("select * from par_table where floatfield = cast('6.6' as float)"),
      sql("select * from sdkout where floatfield = cast('6.6' as float)"))
  }

  test("test select * on table with byte data type") {
    sql("drop table if exists sdkout")
    FileFactory.deleteAllFilesOfDir(new File(warehouse1 + "/sdk1"))
    buildTestDataOtherDataType(11, Array("age", "address"), warehouse1 + "/sdk1")
    sql(s"create table sdkout(male boolean, age int, height double, name string, address " +
        s"string," +
        s"salary long, floatField float, bytefield byte) using carbon options " +
        s"(path='$warehouse1/sdk1')")
    checkAnswer(sql("select * from par_table"), sql("select * from sdkout"))
    checkAnswer(sql("select byteField from par_table"), sql("select bytefield from sdkout"))
  }

  test("test various filters on byte data") {
    sql("drop table if exists sdkout")
    FileFactory.deleteAllFilesOfDir(new File(warehouse1 + "/sdk1"))
    buildTestDataOtherDataType(11, Array("age", "address"), warehouse1 + "/sdk1")
    sql(s"create table sdkout(male boolean, age int, height double, name string, address " +
        s"string," +
        s"salary long, floatField float, bytefield byte) using carbon options " +
        s"(path='$warehouse1/sdk1')")
    checkAnswer(sql("select * from par_table where bytefield < 10"),
      sql("select * from sdkout where bytefield < 10"))
    checkAnswer(sql("select * from par_table where bytefield > 5"),
      sql("select * from sdkout where bytefield > 5"))
    checkAnswer(sql("select * from par_table where bytefield >= 4"),
      sql("select * from sdkout where bytefield >= 4"))
    checkAnswer(sql("select * from par_table where bytefield != 5"),
      sql("select * from sdkout where bytefield != 5"))
    checkAnswer(sql("select * from par_table where bytefield <= 5"),
      sql("select * from sdkout where bytefield <= 5"))
    checkAnswer(sql("select * from par_table where bytefield >= 5"),
      sql("select * from sdkout where bytefield >= 5"))
    checkAnswer(sql("select * from par_table where bytefield IN ('5','6')"),
      sql("select * from sdkout where bytefield IN ('5','6')"))
    checkAnswer(sql("select * from par_table where bytefield NOT IN ('5','6')"),
      sql("select * from sdkout where bytefield NOT IN ('5','6')"))
  }

  test("test struct of float type and byte type") {
    import scala.collection.JavaConverters._
    val path = FileFactory.getPath(warehouse1 + "/sdk1").toString
    FileFactory.deleteAllFilesOfDir(new File(warehouse1 + "/sdk1"))
    sql("drop table if exists complextable")
    val fields = List(new StructField
    ("byteField", DataTypes.BYTE), new StructField("floatField", DataTypes.FLOAT))
    val structType = Array(new Field("stringfield", DataTypes.STRING), new Field
    ("structField", "struct", fields.asJava))


    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(path)
          .uniqueIdentifier(System.nanoTime()).withBlockSize(2)
          .withCsvInput(new Schema(structType)).writtenBy("SparkCarbonDataSourceTestCase").build()

      var i = 0
      while (i < 11) {
        val array = Array[String](s"name$i", s"$i" + "\001" + s"$i.${ i }12")
        writer.write(array)
        i += 1
      }
      writer.close()
      sql("create table complextable (stringfield string, structfield struct<bytefield: " +
          "byte, floatfield: float>) " +
          s"using carbon location '$path'")
    } catch {
      case ex: Exception => throw new RuntimeException(ex)
      case _ => None
    }
    checkAnswer(sql("select * from complextable limit 1"), Seq(Row("name0", Row(0
      .asInstanceOf[Byte], 0.012.asInstanceOf[Float]))))
    checkAnswer(sql("select * from complextable where structfield.bytefield > 9"), Seq(Row
    ("name10", Row(10.asInstanceOf[Byte], 10.1012.asInstanceOf[Float]))))
    checkAnswer(sql("select * from complextable where structfield.bytefield > 9"), Seq(Row
    ("name10", Row(10.asInstanceOf[Byte], 10.1012.asInstanceOf[Float]))))
    checkAnswer(sql("select * from complextable where structfield.floatfield > 9.912"), Seq
    (Row
    ("name10", Row(10.asInstanceOf[Byte], 10.1012.asInstanceOf[Float]))))
    checkAnswer(sql("select * from complextable where structfield.floatfield > 9.912 and " +
                    "structfield.bytefield < 11"),
      Seq(Row("name10", Row(10.asInstanceOf[Byte], 10.1012.asInstanceOf[Float]))))
  }

  test("test bytefield as sort column") {
    val path = FileFactory.getPath(warehouse1 + "/sdk1").toString
    FileFactory.deleteAllFilesOfDir(new File(warehouse1 + "/sdk1"))
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
          .withCsvInput(new Schema(fields)).writtenBy("SparkCarbonDataSourceTestCase").build()

      var i = 0
      while (i < 11) {
        val array = Array[String](
          String.valueOf(i),
          String.valueOf(i.toDouble / 2),
          "name" + i,
          "address" + i,
          (i * 100).toString,
          s"${ 10 - i }")
        writer.write(array)
        i += 1
      }
      writer.close()
      sql("drop table if exists sorted_par")
      sql("drop table if exists sort_table")
      sql(s"create table sort_table (age int, height double, name string, address string," +
          s" salary long, bytefield byte) using carbon location '$path'")
      FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(s"$warehouse1/../warehouse2"))
      sql(s"create table sorted_par(age int, height double, name string, address " +
          s"string," +
          s"salary long, bytefield byte) using parquet location " +
          s"'$warehouse1/../warehouse2'")
      (0 to 10).foreach {
        i =>
          sql(s"insert into sorted_par select '$i', ${ i.toDouble / 2 }, 'name$i', " +
              s"'address$i', ${ i * 100 }, '${ 10 - i }'")
      }
      checkAnswer(sql("select * from sorted_par order by bytefield"),
        sql("select * from sort_table"))
    } catch {
      case ex: Exception => throw new RuntimeException(ex)
      case _ => None
    }
  }

  test("test array of float type and byte type") {
    import scala.collection.JavaConverters._
    val path = FileFactory.getPath(warehouse1 + "/sdk1").toString
    FileFactory.deleteAllFilesOfDir(new File(warehouse1 + "/sdk1"))
    sql("drop table if exists complextable")
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
          .withCsvInput(new Schema(structType)).writtenBy("SparkCarbonDataSourceTestCase").build()

      var i = 0
      while (i < 10) {
        val array = Array[String](s"name$i",
          s"$i" + "\001" + s"${ i * 2 }",
          s"${ i / 2 }" + "\001" + s"${ i / 3 }")
        writer.write(array)
        i += 1
      }
      writer.close()
      sql(s"create table complextable (stringfield string, bytearray " +
          s"array<byte>, floatarray array<float>) using carbon " +
          s"location " +
          s"'$path'")
    } catch {
      case ex: Exception => throw new RuntimeException(ex)
      case _ => None
    }
    checkAnswer(sql("select * from complextable limit 1"), Seq(Row("name0", mutable
      .WrappedArray.make(Array[Byte](0, 0)), mutable.WrappedArray.make(Array[Float](0.0f, 0.0f)))))
    checkAnswer(sql("select * from complextable where bytearray[0] = 1"), Seq(Row("name1",
      mutable.WrappedArray.make(Array[Byte](1, 2)), mutable.WrappedArray.make(Array[Float](0.0f,
        0.0f)))))
    checkAnswer(sql("select * from complextable where bytearray[0] > 8"), Seq(Row("name9",
      mutable.WrappedArray.make(Array[Byte](9, 18)), mutable.WrappedArray.make(Array[Float](4.0f,
        3.0f)))))
    checkAnswer(sql(
      "select * from complextable where floatarray[0] IN (4.0) and stringfield = 'name8'"), Seq(Row
    ("name8",
      mutable.WrappedArray.make(Array[Byte](8, 16)), mutable.WrappedArray.make(Array[Float](4.0f,
      2.0f)))))
  }

  private def createParquetTable {
    val path = FileFactory.getUpdatedFilePath(s"$warehouse1/../warehouse2")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(s"$path"))
    sql(s"create table par_table(male boolean, age int, height double, name string, address " +
        s"string," +
        s"salary long, floatField float, bytefield byte) using parquet location '$path'")
    (0 to 10).foreach {
      i =>
        sql(s"insert into par_table select 'true','$i', ${ i.toDouble / 2 }, 'name$i', " +
            s"'address$i', ${ i * 100 }, $i.$i, '$i'")
    }
  }

  // prepare sdk writer output with other schema
  def buildTestDataOtherDataType(rows: Int,
      sortColumns: Array[String],
      writerPath: String,
      colCount: Int = -1): Any = {
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
          .withCsvInput(new Schema(fields)).writtenBy("SparkCarbonDataSourceTestCase").build()

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

  def buildStructSchemaWithNestedArrayOfMapTypeAsValue(writerPath: String, rows: Int): Unit = {
    FileFactory.deleteAllFilesOfDir(new File(writerPath))
    val mySchema =
      """
        |{
        |  "name": "address",
        |  "type": "record",
        |  "fields": [
        |    {
        |      "name": "name",
        |      "type": "string"
        |    },
        |    {
        |      "name": "age",
        |      "type": "int"
        |    },
        |    {
        |      "name": "structRecord",
        |      "type": {
        |        "type": "record",
        |        "name": "my_address",
        |        "fields": [
        |          {
        |            "name": "street",
        |            "type": "string"
        |          },
        |          {
        |            "name": "houseDetails",
        |            "type": {
        |               "type": "array",
        |               "items": {
        |                   "name": "memberDetails",
        |                   "type": "map",
        |                   "values": "string"
        |                }
        |             }
        |          }
        |        ]
        |      }
        |    }
        |  ]
        |}
      """.stripMargin
    val json =
      """ {"name":"bob", "age":10, "structRecord": {"street":"street1",
        |"houseDetails": [{"101": "Rahul", "102": "Pawan"}]}} """
        .stripMargin
    WriteFilesWithAvroWriter(writerPath, rows, mySchema, json)
  }

  test("test external table with struct type with value as nested struct<array<map>> type") {
    val writerPath: String = FileFactory.getUpdatedFilePath(warehouse1 + "/sdk1")
    val rowCount = 3
    buildStructSchemaWithNestedArrayOfMapTypeAsValue(writerPath, rowCount)
    sql("drop table if exists carbon_external")
    sql(s"create table carbon_external using carbon location '$writerPath'")
    assert(sql("select * from carbon_external").count() == rowCount)
    sql("drop table if exists carbon_external")
  }

  test("test byte and float for multiple pages") {
    val path = FileFactory.getPath(warehouse1 + "/sdk1").toString
    FileFactory.deleteAllFilesOfDir(new File(warehouse1 + "/sdk1"))
    sql("drop table if exists multi_page")
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
          .withCsvInput(new Schema(fields)).writtenBy("SparkCarbonDataSourceTestCase").build()

      var i = 0
      while (i < 33000) {
        val array = Array[String](
          String.valueOf(i),
          s"$i.3200", "32")
        writer.write(array)
        i += 1
      }
      writer.close()
      sql(s"create table multi_page (a string, b float, c byte) using carbon location " +
          s"'$path'")
      assert(sql("select * from multi_page").count() == 33000)
    } catch {
      case ex: Exception => throw new RuntimeException(ex)
    } finally {
      FileFactory.deleteAllFilesOfDir(new File(warehouse1 + "/sdk1"))
    }
  }

  test("test partition issue with add location") {
    sql("drop table if exists partitionTable_obs")
    sql("drop table if exists partitionTable_obs_par")
    sql(s"create table partitionTable_obs (id int,name String,email String) using carbon " +
        s"partitioned by(email) ")
    sql(s"create table partitionTable_obs_par (id int,name String,email String) using parquet " +
        s"partitioned by(email) ")
    sql("insert into partitionTable_obs select 1,'huawei','abc'")
    sql("insert into partitionTable_obs select 1,'huawei','bcd'")
    sql(s"alter table partitionTable_obs add partition (email='def') location " +
        s"'$warehouse1/test_folder121/'")
    sql("insert into partitionTable_obs select 1,'huawei','def'")

    sql("insert into partitionTable_obs_par select 1,'huawei','abc'")
    sql("insert into partitionTable_obs_par select 1,'huawei','bcd'")
    sql(s"alter table partitionTable_obs_par add partition (email='def') location " +
        s"'$warehouse1/test_folder122/'")
    sql("insert into partitionTable_obs_par select 1,'huawei','def'")

    checkAnswer(sql("select * from partitionTable_obs"),
      sql("select * from partitionTable_obs_par"))
    sql("drop table if exists partitionTable_obs")
    sql("drop table if exists partitionTable_obs_par")
  }

  test("test multiple partition  select issue") {
    sql("drop table if exists t_carbn01b_hive")
    sql(s"drop table if exists t_carbn01b")
    sql(
      "create table t_carbn01b_hive(Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep " +
      "DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String," +
      "Create_date String,Active_status String,Item_type_cd INT, Update_time TIMESTAMP, " +
      "Discount_price DOUBLE)  using parquet partitioned by (Active_status,Item_type_cd, " +
      "Update_time, Discount_price)")
    sql(
      "create table t_carbn01b(Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep " +
      "DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String," +
      "Create_date String,Active_status String,Item_type_cd INT, Update_time TIMESTAMP, " +
      "Discount_price DOUBLE)  using carbon partitioned by (Active_status,Item_type_cd, " +
      "Update_time, Discount_price)")
    sql(
      "insert into t_carbn01b partition(Active_status, Item_type_cd,Update_time,Discount_price) " +
      "select * from t_carbn01b_hive")
    sql(
      "alter table t_carbn01b add partition (active_status='xyz',Item_type_cd=12," +
      "Update_time=NULL,Discount_price='3000')")
    sql(
      "insert overwrite table t_carbn01b select 'xyz', 12, 74,3000,20000000,121.5,4.99,2.44," +
      "'RE3423ee','dddd', 'ssss','2012-01-02 23:04:05.12', '2012-01-20'")
    sql(
      "insert overwrite table t_carbn01b_hive select 'xyz', 12, 74,3000,20000000,121.5,4.99,2.44," +
      "'RE3423ee','dddd', 'ssss','2012-01-02 23:04:05.12', '2012-01-20'")
    checkAnswer(sql("select * from t_carbn01b_hive"), sql("select * from t_carbn01b"))
    sql("drop table if exists t_carbn01b_hive")
    sql(s"drop table if exists t_carbn01b")
  }

  test("Test Float value by having negative exponents") {
    sql("DROP TABLE IF EXISTS float_p")
    sql("DROP TABLE IF EXISTS float_c")
    sql("CREATE TABLE float_p(f float) using parquet")
    sql("CREATE TABLE float_c(f float) using carbon")
    sql("INSERT INTO float_p select \"1.4E-3\"")
    sql("INSERT INTO float_p select \"1.4E-38\"")
    sql("INSERT INTO float_c select \"1.4E-3\"")
    sql("INSERT INTO float_c select \"1.4E-38\"")
    checkAnswer(sql("SELECT * FROM float_p"),
      sql("SELECT * FROM float_c"))
    sql("DROP TABLE float_p")
    sql("DROP TABLE float_c")
  }

  test("test fileformat flow with drop and query on same table") {
    sql("drop table if exists fileformat_drop")
    sql("drop table if exists fileformat_drop_hive")
    sql(
      "create table fileformat_drop (imei string,AMSize string,channelsId string,ActiveCountry " +
      "string, Activecity string,gamePointId double,deviceInformationId double,productionDate " +
      "Timestamp,deliveryDate timestamp,deliverycharge double) using carbon options" +
      "('table_blocksize'='1','LOCAL_DICTIONARY_ENABLE'='TRUE'," +
      "'LOCAL_DICTIONARY_THRESHOLD'='1000')")
    sql(
      "create table fileformat_drop_hive(imei string,deviceInformationId double,AMSize string," +
      "channelsId string,ActiveCountry string,Activecity string,gamePointId double,productionDate" +
      " Timestamp,deliveryDate timestamp,deliverycharge double)row format delimited FIELDS " +
      "terminated by ',' LINES terminated by '\n' stored as textfile")
    val sourceFile = FileFactory
      .getPath(s"$resource" +
               "../../../../../spark/src/test/resources/vardhandaterestruct.csv")
      .toString
    sql(s"load data local inpath '$sourceFile' into table fileformat_drop_hive")
    sql(
      "insert into fileformat_drop select imei ,deviceInformationId ,AMSize ,channelsId ," +
      "ActiveCountry ,Activecity ,gamePointId ,productionDate ,deliveryDate ,deliverycharge from " +
      "fileformat_drop_hive")
    assert(sql("select count(*) from fileformat_drop where imei='1AA10000'").collect().length == 1)

    sql("drop table if exists fileformat_drop")
    sql(
      "create table fileformat_drop (imei string,deviceInformationId double,AMSize string," +
      "channelsId string,ActiveCountry string,Activecity string,gamePointId float,productionDate " +
      "timestamp,deliveryDate timestamp,deliverycharge decimal(10,2)) using carbon options" +
      "('table_blocksize'='1','LOCAL_DICTIONARY_ENABLE'='true'," +
      "'local_dictionary_threshold'='1000')")
    sql(
      "insert into fileformat_drop select imei ,deviceInformationId ,AMSize ,channelsId ," +
      "ActiveCountry ,Activecity ,gamePointId ,productionDate ,deliveryDate ,deliverycharge from " +
      "fileformat_drop_hive")
    assert(sql("select count(*) from fileformat_drop where imei='1AA10000'").collect().length == 1)
    sql("drop table if exists fileformat_drop")
    sql("drop table if exists fileformat_drop_hive")
  }

  test("validate the columns not present in schema") {
    sql("drop table if exists validate")
    sql(
      "create table validate (name string, age int, address string) using carbon options" +
      "('inverted_index'='abc')")
    val ex = intercept[Exception] {
      sql("insert into validate select 'abc',4,'def'")
    }
    assert(ex.getMessage
      .contains("column: abc specified in inverted index columns does not exist in schema"))
  }

  override protected def beforeAll(): Unit = {
    drop
    createParquetTable
  }

  override def afterAll(): Unit = {
    drop
  }

  private def drop = {
    sql("drop table if exists testformat")
    sql("drop table if exists carbon_table")
    sql("drop table if exists testparquet")
    sql("drop table if exists par_table")
    sql("drop table if exists sdkout")
    sql("drop table if exists validate")
  }

  private def jsonToAvro(json: String, avroSchema: String): GenericRecord = {
    var input: InputStream = null
    var writer: DataFileWriter[GenericRecord] = null
    var encoder: Encoder = null
    var output: ByteArrayOutputStream = null
    try {
      val schema = new org.apache.avro.Schema.Parser().parse(avroSchema)
      val reader = new GenericDatumReader[GenericRecord](schema)
      input = new ByteArrayInputStream(json.getBytes())
      output = new ByteArrayOutputStream()
      val din = new DataInputStream(input)
      writer = new DataFileWriter[GenericRecord](new GenericDatumWriter[GenericRecord]())
      writer.create(schema, output)
      val decoder = DecoderFactory.get().jsonDecoder(schema, din)
      var datum: GenericRecord = reader.read(null, decoder)
      return datum
    } finally {
      input.close()
      writer.close()
    }
  }

  def WriteFilesWithAvroWriter(writerPath: String,
      rows: Int,
      mySchema: String,
      json: String) = {
    // conversion to GenericData.Record
    val nn = new avro.Schema.Parser().parse(mySchema)
    val record = jsonToAvro(json, mySchema)
    try {
      val writer = CarbonWriter.builder
        .outputPath(writerPath)
        .uniqueIdentifier(System.currentTimeMillis()).withAvroInput(nn).writtenBy("DataSource")
        .build()
      var i = 0
      while (i < rows) {
        writer.write(record)
        i = i + 1
      }
      writer.close()
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        Assert.fail(e.getMessage)
      }
    }
  }
}
